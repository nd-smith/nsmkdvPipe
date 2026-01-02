"""
Delta table operations with retry and auth refresh support.

Read/write operations for Delta tables on OneLake/ADLS. All datetimes are UTC-aware.
Note: Query timeouts not supported; use time-bounded filters and partition pruning.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
from deltalake import DeltaTable, write_deltalake

from verisk_pipeline.common.auth import get_storage_options, clear_token_cache
from verisk_pipeline.common.retry import RetryConfig, with_retry
from verisk_pipeline.common.circuit_breaker import (
    CircuitBreakerConfig,
)
from verisk_pipeline.common.config.xact import get_config
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.decorators import logged_operation, LoggedClass
from verisk_pipeline.common.logging.utilities import log_with_context

logger = get_logger(__name__)

# Retry config for Delta operations
DELTA_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=10.0,
)

# Circuit breaker config for Delta operations
DELTA_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    timeout_seconds=30.0,
)


@dataclass
class WriteOperation:
    """Write operation for idempotency tracking. Token is a UUID uniquely identifying the operation."""

    token: str
    table_path: str
    timestamp: datetime
    row_count: int


def _on_auth_error() -> None:
    """Callback for auth errors - clears token cache."""
    clear_token_cache()


class DeltaTableReader(LoggedClass):
    """
    Reader for Delta tables with auth retry support.

    Usage:
        reader = DeltaTableReader("abfss://workspace@onelake/lakehouse/Tables/events")
        df = reader.read()
        df = reader.read(columns=["trace_id", "ingested_at"])
    """

    def __init__(
        self, table_path: str, storage_options: Optional[Dict[str, str]] = None
    ):
        """
        Args:
            table_path: Full abfss:// path to Delta table
            storage_options: Optional storage options (default: from get_storage_options())
        """
        self.table_path = table_path
        self.storage_options = storage_options
        super().__init__()

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def read(
        self,
        columns: Optional[List[str]] = None,
    ) -> pl.DataFrame:
        """
        Read entire Delta table.

        Args:
            columns: Optional list of columns to read (None = all)

        Returns:
            Polars DataFrame
        """
        opts = self.storage_options or get_storage_options()

        df = pl.read_delta(
            self.table_path,
            storage_options=opts,
            columns=columns,
        )

        # Batch size validation (Task E.1)
        config = get_config()
        max_batch_size = config.lakehouse.max_batch_size_read
        if len(df) > max_batch_size:
            self._log(
                logging.WARNING,
                "Read batch exceeds configured limit",
                rows_read=len(df),
                max_batch_size=max_batch_size,
                table_path=self.table_path,
            )

        self._log(logging.DEBUG, "Read complete", rows_read=len(df))
        return df

    @logged_operation(level=logging.DEBUG, include_args=["columns"])
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def scan(
        self,
        columns: Optional[List[str]] = None,
    ) -> pl.LazyFrame:
        """
        Create lazy scan of Delta table.

        Args:
            columns: Optional list of columns to select

        Returns:
            Polars LazyFrame for query building
        """
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)

        if columns:
            lf = lf.select(columns)

        return lf

    @logged_operation(level=logging.DEBUG, include_args=["limit"])
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def read_filtered(
        self,
        filter_expr: pl.Expr,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
    ) -> pl.DataFrame:
        """
        Read Delta table with filter pushdown.

        Args:
            filter_expr: Polars filter expression
            columns: Optional list of columns
            limit: Optional row limit
            order_by: Optional column to sort by (applied before limit)
            descending: Sort descending if True

        Returns:
            Filtered Polars DataFrame
        """
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)
        lf = lf.filter(filter_expr)

        if columns:
            lf = lf.select(columns)

        if order_by:
            lf = lf.sort(order_by, descending=descending)

        if limit:
            lf = lf.head(limit)
        if limit and not order_by:
            self._log(
                logging.WARNING,
                "read_filtered called with limit but no order_by - results may be non-deterministic",
                limit=limit,
            )

        # streaming=True can cause "invalid SlotMap key used" panic with sort()
        use_streaming = order_by is None
        df = lf.collect(streaming=use_streaming)
        self._log(logging.DEBUG, "Read filtered complete", rows_read=len(df))
        return df

    @logged_operation(operation_name="read_as_polars")
    def read_as_polars(
        self,
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
    ) -> pl.DataFrame:
        """
        Read Delta table as Polars DataFrame with optional filters.

        Args:
            filters: Optional list of (column, operator, value) tuples.
                    Operators: '=', '!=', '<', '>', '<=', '>='
            columns: Optional list of columns to select

        Returns:
            Polars DataFrame
        """
        opts = self.storage_options or get_storage_options()

        lf = pl.scan_delta(self.table_path, storage_options=opts)

        # Apply filters with robust type casting
        if filters:
            for col, op, value in filters:
                try:
                    # Cast column to match value type for numeric comparisons
                    col_expr = pl.col(col)
                    if isinstance(value, int):
                        # Cast to int, fill nulls with 0 to handle conversion failures
                        col_expr = col_expr.cast(pl.Int64, strict=False).fill_null(0)
                    elif isinstance(value, float):
                        # Cast to float, fill nulls with 0.0
                        col_expr = col_expr.cast(pl.Float64, strict=False).fill_null(
                            0.0
                        )

                    if op == "=":
                        lf = lf.filter(col_expr == value)
                    elif op == "!=":
                        lf = lf.filter(col_expr != value)
                    elif op == "<":
                        lf = lf.filter(col_expr < value)
                    elif op == ">":
                        lf = lf.filter(col_expr > value)
                    elif op == "<=":
                        lf = lf.filter(col_expr <= value)
                    elif op == ">=":
                        lf = lf.filter(col_expr >= value)
                    else:
                        raise ValueError(f"Unsupported filter operator: {op}")
                except Exception as e:
                    # Provide detailed error for debugging
                    raise TypeError(
                        f"Filter error on column '{col}' {op} {value} (type: {type(value).__name__}): {str(e)}"
                    ) from e

        if columns:
            lf = lf.select(columns)

        return lf.collect(streaming=True)

    def exists(self) -> bool:
        """Check if table exists and is readable."""
        try:
            # Try to read schema only
            opts = self.storage_options or get_storage_options()
            pl.scan_delta(self.table_path, storage_options=opts).collect_schema()
            return True
        except Exception as e:
            self._log(
                logging.DEBUG,
                "Table does not exist or is not readable",
                error_message=str(e)[:200],
            )
            return False


class DeltaTableWriter(LoggedClass):
    """
    Writer for Delta tables with auth retry and deduplication support.

    Usage:
        writer = DeltaTableWriter("abfss://workspace@onelake/lakehouse/Tables/events")
        rows_written = writer.append(df)
    """

    def __init__(
        self,
        table_path: str,
        dedupe_column: Optional[str] = None,
        dedupe_window_hours: int = 24,
        timestamp_column: str = "ingested_at",
        z_order_columns: Optional[List[str]] = None,
    ):
        self.table_path = table_path
        self.dedupe_column = dedupe_column
        self.dedupe_window_hours = dedupe_window_hours
        self.timestamp_column = timestamp_column
        self.z_order_columns = z_order_columns or []
        self._reader = DeltaTableReader(table_path)
        self._optimization_scheduler: Optional[Any] = None
        super().__init__()

    def _table_exists(self, opts: Dict[str, str]) -> bool:
        """Check if Delta table exists."""
        try:
            DeltaTable(self.table_path, storage_options=opts)
            return True
        except Exception:
            return False

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def append(
        self,
        df: pl.DataFrame,
        dedupe: bool = True,
    ) -> int:
        """
        Append DataFrame to Delta table.

        Args:
            df: Data to append
            dedupe: Whether to deduplicate against existing data

        Returns:
            Number of rows written
        """
        if df.is_empty():
            self._log(logging.DEBUG, "No data to write")
            return 0

        initial_count = len(df)

        # Dedupe within batch
        if self.dedupe_column and self.dedupe_column in df.columns:
            df = df.unique(subset=[self.dedupe_column])
            if len(df) < initial_count:
                self._log(
                    logging.DEBUG,
                    "Removed duplicates within batch",
                    records_processed=initial_count - len(df),
                )

        # Dedupe against existing data
        if dedupe and self.dedupe_column:
            df = self._filter_existing(df)
            if df.is_empty():
                self._log(logging.DEBUG, "No new records after deduplication")
                return 0

        # Write to Delta - convert to Arrow for write_deltalake
        opts = get_storage_options()
        write_deltalake(
            self.table_path,
            df.to_arrow(),
            mode="append",
            schema_mode="merge",
            storage_options=opts,
        )  # type: ignore[call-overload]

        rows_written = len(df)
        # rows_written auto-captured by decorator since we return int
        return rows_written

    @logged_operation(level=logging.INFO, include_args=["merge_keys"])
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def merge(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        preserve_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Merge DataFrame into table (true upsert via Delta merge API).

        - Matching rows: UPDATE all columns except merge_keys and preserve_columns
        - Non-matching rows: INSERT all columns
        - Multiple rows for same key in batch: Combined (later non-null values win)

        Args:
            df: Data to merge
            merge_keys: Columns forming primary key
            preserve_columns: Columns to preserve on update (default: ["created_at"])

        Returns:
            Number of rows affected
        """
        if df.is_empty():
            return 0

        if preserve_columns is None:
            preserve_columns = ["created_at"]

        initial_len = len(df)

        # Check if we need to dedupe within batch (avoid expensive dict ops if not needed)
        unique_keys = df.select(merge_keys).unique()
        needs_dedupe = len(unique_keys) < initial_len
        del unique_keys  # Free immediately

        if needs_dedupe:
            # Combine rows within batch by merge keys (later non-null values overlay earlier)
            # This is expensive but necessary when batch has duplicate keys
            rows = df.to_dicts()
            merged: Dict[tuple, dict] = {}
            for row in rows:
                key = tuple(row.get(k) for k in merge_keys)
                if key in merged:
                    for col, val in row.items():
                        if val is not None:
                            merged[key][col] = val
                else:
                    merged[key] = row.copy()

            del rows  # Free original list
            df = pl.DataFrame(list(merged.values()), infer_schema_length=None)
            del merged  # Free dict
            self._log(
                logging.DEBUG,
                "Deduped batch",
                records_processed=initial_len,
                rows_written=len(df),
            )

        # Cast any null-typed columns to string (they'll get cast again by Delta)
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

        opts = get_storage_options()

        # Create table if doesn't exist
        if not self._table_exists(opts):
            arrow_table = df.to_arrow()
            write_deltalake(
                self.table_path,
                arrow_table,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=opts,
            )
            del arrow_table
            self._log(
                logging.INFO,
                "Created table",
                rows_written=len(df),
            )
            return len(df)

        # Build merge predicate
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

        # Build update dict - all columns except merge keys and preserved
        update_cols = [
            c for c in df.columns if c not in merge_keys and c not in preserve_columns
        ]
        update_dict = {c: f"source.{c}" for c in update_cols}

        # Build insert dict - all columns
        insert_dict = {c: f"source.{c}" for c in df.columns}

        # Convert to PyArrow for merge, then free polars df
        pa_df = df.to_arrow()
        del df

        # Perform merge
        dt = DeltaTable(self.table_path, storage_options=opts)

        result = (
            dt.merge(
                source=pa_df,
                predicate=predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(update_dict)
            .when_not_matched_insert(insert_dict)
            .execute()
        )

        # Free PyArrow table and DeltaTable reference
        del pa_df, dt

        # Result contains metrics
        rows_updated = result.get("num_target_rows_updated", 0) or 0
        rows_inserted = result.get("num_target_rows_inserted", 0) or 0

        self._log(
            logging.DEBUG,
            "Merge complete",
            rows_merged=rows_inserted + rows_updated,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
        )

        return rows_inserted + rows_updated

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def upsert_with_idempotency(
        self,
        df: pl.DataFrame,
        merge_key: str,
        event_id_field: str = "_event_id",
    ) -> int:
        """
        Upsert with deduplication by event ID.

        Useful for idempotent processing where the same event may be
        processed multiple times but should only result in one row.

        Args:
            df: Data to upsert
            merge_key: Primary key column for merge (e.g., "project_id")
            event_id_field: Column containing event ID for dedup

        Returns:
            Number of rows written
        """
        if df.is_empty():
            self._log(logging.DEBUG, "No data to upsert")
            return 0

        initial_count = len(df)

        # Phase 1: Dedupe within batch by event_id (keep last occurrence)
        if event_id_field in df.columns:
            df = df.unique(subset=[event_id_field], keep="last")
            if len(df) < initial_count:
                self._log(
                    logging.DEBUG,
                    "Removed duplicates by event_id",
                    dedupe_field=event_id_field,
                    records_processed=initial_count - len(df),
                )

        # Phase 2: Dedupe within batch by merge_key (keep last occurrence)
        if merge_key in df.columns:
            before = len(df)
            df = df.unique(subset=[merge_key], keep="last")
            if len(df) < before:
                self._log(
                    logging.DEBUG,
                    "Removed duplicates by merge_key",
                    dedupe_field=merge_key,
                    records_processed=before - len(df),
                )

        if df.is_empty():
            return 0

        # Write using append mode with dedup against existing data
        rows_written: int = self.append(df, dedupe=True)  # type: ignore[assignment]
        return rows_written

    def enable_auto_optimization(
        self, interval_hours: int = 24, target_file_size_mb: int = 128
    ) -> None:
        """
        Enable automatic optimization scheduling (P3.1).

        When enabled, optimization will be checked after each write operation.
        This helps maintain query performance by periodically compacting small
        files and applying Z-ordering.

        Args:
            interval_hours: Hours between optimization runs (default: 24)
            target_file_size_mb: Target file size after optimization (default: 128MB)

        Example:
            writer = DeltaTableWriter(
                table_path="path/to/table",
                z_order_columns=["created_date", "status"]
            )
            writer.enable_auto_optimization(interval_hours=24)
            writer.append(df)  # Optimization checked automatically
        """
        from verisk_pipeline.storage.optimization import (
            OptimizationScheduler,
            OptimizationConfig,
        )

        config = OptimizationConfig(
            optimization_interval_hours=interval_hours,
            target_file_size_mb=target_file_size_mb,
        )
        self._optimization_scheduler = OptimizationScheduler(config)

        logger.info(
            "Auto-optimization enabled",
            extra={
                "table": self.table_path,
                "interval_hours": interval_hours,
                "target_file_size_mb": target_file_size_mb,
                "z_order_columns": self.z_order_columns,
            },
        )

    @logged_operation(level=logging.INFO, include_args=["z_order_columns"])
    def optimize_with_zorder(self, z_order_columns: Optional[List[str]] = None) -> dict:
        """
        Run OPTIMIZE with Z-ordering on specified columns (P3.2).

        Z-ordering co-locates related data in files, dramatically improving
        query performance for filtered queries. For example, if you frequently
        filter by date and status, Z-ordering on ["date", "status"] can provide
        3-10x speedup by reducing files scanned.

        IMPORTANT: Follow with VACUUM after 7-day retention to reclaim storage.

        Args:
            z_order_columns: Columns to Z-order by (uses default if not provided)

        Returns:
            Optimization metrics dictionary

        Example:
            writer = DeltaTableWriter(
                "path/to/table",
                z_order_columns=["created_date", "status"]
            )

            # Run periodic optimization
            result = writer.optimize_with_zorder()

            # Override Z-order columns for specific optimization
            result = writer.optimize_with_zorder(z_order_columns=["trace_id"])
        """
        columns = z_order_columns or self.z_order_columns

        if not columns:
            logger.warning(
                "No Z-order columns specified, running regular OPTIMIZE",
                extra={"table": self.table_path},
            )

        logger.info(
            "Running OPTIMIZE with Z-ordering",
            extra={"table": self.table_path, "z_order_columns": columns},
        )

        from verisk_pipeline.storage.optimization import (
            DeltaTableOptimizer,
            OptimizationConfig,
        )

        optimizer = DeltaTableOptimizer(OptimizationConfig())
        result = optimizer.optimize(self.table_path, z_order_columns=columns)

        return result

    def _filter_existing(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter out records that already exist in table."""
        if not self.dedupe_column:
            return df

        try:
            existing_ids = self._get_recent_ids()
        except Exception as e:
            self._log_exception(
                e,
                "Could not get recent IDs for deduplication",
                level=logging.WARNING,
            )
            return df

        if not existing_ids:
            return df

        before_count = len(df)
        df = df.filter(~pl.col(self.dedupe_column).is_in(existing_ids))

        filtered_count = before_count - len(df)
        if filtered_count > 0:
            self._log(
                logging.DEBUG,
                "Filtered existing records",
                records_processed=filtered_count,
            )

        return df

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def _get_recent_ids(self) -> Set[str]:
        """Get dedupe column values from recent window using lazy scan."""
        if not self.dedupe_column:
            return set()

        if not self._reader.exists():
            return set()

        cutoff = datetime.now(timezone.utc) - timedelta(
            hours=self.dedupe_window_hours
        )

        # Use lazy scan with predicate pushdown - only reads relevant parquet files
        opts = get_storage_options()
        df = (
            pl.scan_delta(self.table_path, storage_options=opts)
            .filter(pl.col(self.timestamp_column) > cutoff)
            .select(self.dedupe_column)
            .collect()
        )

        if df is None or df.is_empty():
            return set()

        return set(df[self.dedupe_column].to_list())

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def deduplicate_with_merge(
        self,
        new_df: pl.DataFrame,
        dedupe_column: str = "trace_id",
        when_matched: str = "do_nothing",
    ) -> int:
        """
        Use Delta Lake native MERGE for memory-efficient deduplication (P2.1).

        Eliminates need to load existing IDs into memory by using Delta Lake's
        native MERGE operation to handle deduplication at the storage layer.

        Args:
            new_df: New data to deduplicate and write
            dedupe_column: Column to use for deduplication matching
            when_matched: Action when key exists ('do_nothing' or 'update')

        Returns:
            Number of new rows inserted
        """
        if new_df.is_empty():
            self._log(logging.DEBUG, "No data to deduplicate")
            return 0

        import tempfile
        import shutil

        initial_count = len(new_df)

        # Dedupe within batch first
        new_df = new_df.unique(subset=[dedupe_column])
        if len(new_df) < initial_count:
            self._log(
                logging.DEBUG,
                "Removed duplicates within batch",
                records_processed=initial_count - len(new_df),
            )

        # Write new data to temporary table
        temp_dir = tempfile.mkdtemp(prefix="delta_merge_")
        temp_table = f"{temp_dir}/temp_merge"

        try:
            opts = get_storage_options()

            # Check if target table exists
            if not self._table_exists(opts):
                # Table doesn't exist, just write directly
                write_deltalake(
                    self.table_path,
                    new_df.to_arrow(),
                    mode="overwrite",
                    schema_mode="overwrite",
                    storage_options=opts,
                )
                self._log(
                    logging.INFO,
                    "Created table (no existing data to deduplicate against)",
                    rows_written=len(new_df),
                )
                return len(new_df)

            # Write temp table
            write_deltalake(
                temp_table,
                new_df.to_arrow(),
                mode="overwrite",
                storage_options={},  # Local temp table, no auth needed
            )

            # Perform MERGE operation
            dt = DeltaTable(self.table_path, storage_options=opts)

            predicate = f"target.{dedupe_column} = source.{dedupe_column}"

            merge_builder = dt.merge(
                source=temp_table,
                predicate=predicate,
                source_alias="source",
                target_alias="target",
            )

            if when_matched == "do_nothing":
                # Skip existing records (deduplication)
                merge_builder = merge_builder.when_not_matched_insert_all()
            elif when_matched == "update":
                # Update existing records
                merge_builder = (
                    merge_builder.when_matched_update_all().when_not_matched_insert_all()
                )
            else:
                raise ValueError(f"Invalid when_matched value: {when_matched}")

            result = merge_builder.execute()

            # Cleanup temp table
            shutil.rmtree(temp_dir, ignore_errors=True)

            rows_inserted = result.get("num_target_rows_inserted", 0) or 0
            rows_updated = result.get("num_target_rows_updated", 0) or 0

            self._log(
                logging.INFO,
                "Merge deduplication complete",
                rows_inserted=rows_inserted,
                rows_updated=rows_updated,
                initial_batch_size=initial_count,
            )

            return rows_inserted + rows_updated

        except Exception as e:
            # Cleanup on error
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def merge_batched(
        self,
        df: pl.DataFrame,
        merge_keys: List[str],
        batch_size: Optional[int] = None,
        when_matched: str = "update",
        when_not_matched: str = "insert",
        preserve_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Perform batched MERGE operations for large datasets (P2.3).

        Args:
            df: DataFrame to merge
            merge_keys: Columns to use for matching
            batch_size: Maximum rows per batch (uses config if None)
            when_matched: Action for matched rows ('update', 'delete', 'do_nothing')
            when_not_matched: Action for unmatched rows ('insert', 'do_nothing')
            preserve_columns: Columns to preserve on update

        Returns:
            Total rows merged
        """
        if df.is_empty():
            self._log(logging.DEBUG, "No data to merge")
            return 0

        # Get batch size from config if not provided
        if batch_size is None:
            config = get_config()
            batch_size = config.lakehouse.max_batch_size_merge

        total_rows = len(df)

        # Single batch optimization
        if total_rows <= batch_size:
            self._log(logging.INFO, "Single batch merge", total_rows=total_rows)
            return self.merge(
                df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
            )

        # Multi-batch merge
        num_batches = (total_rows + batch_size - 1) // batch_size
        self._log(
            logging.INFO,
            "Batched merge starting",
            total_rows=total_rows,
            batch_size=batch_size,
            num_batches=num_batches,
        )

        rows_merged = 0
        for batch_num, batch_start in enumerate(
            range(0, total_rows, batch_size), start=1
        ):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.slice(batch_start, batch_end - batch_start)

            self._log(
                logging.INFO,
                f"Merging batch {batch_num}/{num_batches}",
                batch_start=batch_start,
                batch_end=batch_end,
                batch_rows=len(batch_df),
            )

            # Perform merge for this batch
            result = self.merge(
                batch_df,
                merge_keys=merge_keys,
                preserve_columns=preserve_columns,
            )

            rows_merged += result

            # Progress logging
            progress_pct = (batch_end / total_rows) * 100
            self._log(
                logging.INFO,
                "Merge progress",
                rows_processed=batch_end,
                total_rows=total_rows,
                progress_pct=round(progress_pct, 1),
            )

        self._log(
            logging.INFO,
            "Batched merge complete",
            rows_merged=rows_merged,
            total_rows=total_rows,
        )
        return rows_merged

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_rows(self, rows: List[dict], schema: Optional[dict] = None) -> int:
        """
        Write list of dicts to Delta table.

        Args:
            rows: List of row dictionaries
            schema: Optional Polars schema dict

        Returns:
            Number of rows written
        """
        if not rows:
            return 0

        if schema:
            df = pl.DataFrame(rows, schema=schema)
        else:
            df = pl.DataFrame(rows)

        result: int = self.append(df, dedupe=False)  # type: ignore[assignment]
        return result

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_with_idempotency(
        self,
        df: pl.DataFrame,
        operation_token: Optional[str] = None,
    ) -> WriteOperation:
        """
        Write data with idempotency token to prevent duplicate writes (Task G.3).

        Uses a separate idempotency tracking table to record write tokens.
        If the same token is used again, the write is skipped.

        Args:
            df: Data to write
            operation_token: Optional idempotency token (UUID generated if not provided)

        Returns:
            WriteOperation containing token and write metadata

        Raises:
            Exception: On write failure after retries
        """
        # Generate token if not provided
        if operation_token is None:
            operation_token = str(uuid.uuid4())

        # Check if this operation was already completed
        if self._is_duplicate(operation_token):
            self._log(
                logging.INFO,
                "Skipping duplicate write operation",
                operation_token=operation_token,
                table_path=self.table_path,
            )
            return WriteOperation(
                token=operation_token,
                table_path=self.table_path,
                timestamp=datetime.now(timezone.utc),
                row_count=0,
            )

        # Perform the write
        rows_written = self.append(df, dedupe=True)

        # Record token to prevent future duplicates
        write_op = WriteOperation(
            token=operation_token,
            table_path=self.table_path,
            timestamp=datetime.now(timezone.utc),
            row_count=rows_written,
        )
        self._record_token(write_op)

        return write_op

    def _is_duplicate(self, token: str) -> bool:
        """Check if operation token exists in idempotency table (Task G.3).

        Args:
            token: Operation token to check

        Returns:
            True if token exists (duplicate operation), False otherwise
        """
        try:
            # Idempotency table path
            idempotency_table = f"{self.table_path}_idempotency"

            # Check if idempotency table exists
            opts = get_storage_options()
            try:
                DeltaTable(idempotency_table, storage_options=opts)
            except Exception:
                # Table doesn't exist yet
                return False

            # Read idempotency table and check for token
            reader = DeltaTableReader(idempotency_table)
            if not reader.exists():
                return False

            df = reader.read(columns=["token"])
            if df.is_empty():
                return False

            return token in df["token"].to_list()

        except Exception as e:
            self._log_exception(
                e,
                "Error checking idempotency token",
                level=logging.WARNING,
                token=token,
            )
            # On error, assume not duplicate (fail open for writes)
            return False

    def _record_token(self, operation: WriteOperation) -> None:
        """Record operation token in idempotency table (Task G.3).

        Args:
            operation: WriteOperation to record
        """
        try:
            # Idempotency table path
            idempotency_table = f"{self.table_path}_idempotency"

            # Create record
            record_df = pl.DataFrame(
                {
                    "token": [operation.token],
                    "table_path": [operation.table_path],
                    "timestamp": [operation.timestamp],
                    "row_count": [operation.row_count],
                }
            )

            # Write to idempotency table
            opts = get_storage_options()
            write_deltalake(
                idempotency_table,
                record_df.to_arrow(),
                mode="append",
                schema_mode="merge",
                storage_options=opts,
            )  # type: ignore[call-overload]

            self._log(
                logging.DEBUG,
                "Recorded idempotency token",
                token=operation.token,
                idempotency_table=idempotency_table,
            )

        except Exception as e:
            self._log_exception(
                e,
                "Error recording idempotency token",
                level=logging.WARNING,
                token=operation.token,
            )
            # Don't raise - write succeeded, token recording is best-effort


class EventsTableReader(DeltaTableReader):
    """
    Specialized reader for the events table.

    Provides convenience methods for common event queries.
    """

    def __init__(self, table_path: str):
        super().__init__(table_path)

    def get_max_timestamp(
        self, timestamp_col: str = "ingested_at"
    ) -> Optional[datetime]:
        """Get maximum timestamp from table."""
        try:
            df: pl.DataFrame = self.read(columns=[timestamp_col])  # type: ignore[assignment]
            if df is None or df.is_empty():
                return None

            # Column is UTC-aware, just get max directly
            max_ts = df.select(pl.col(timestamp_col).max()).item()

            return max_ts
        except Exception as e:
            self._log_exception(
                e,
                "Could not get max timestamp",
                level=logging.WARNING,
            )
            return None

    def read_after_watermark(
        self,
        watermark: datetime,
        timestamp_col: str = "ingested_at",
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Read events after watermark timestamp.

        Args:
            watermark: Read events after this timestamp (UTC-aware)
            timestamp_col: Timestamp column name
            limit: Optional row limit

        Returns:
            DataFrame of events
        """
        # Both watermark and column are UTC-aware, comparison works directly
        filter_expr = pl.col(timestamp_col) > pl.lit(watermark)
        result: pl.DataFrame = self.read_filtered(filter_expr, limit=limit)  # type: ignore[assignment]
        return result

    @logged_operation(operation_name="read_by_status_subtypes")
    def read_by_status_subtypes(
        self,
        status_subtypes: List[str],
        watermark: Optional[datetime] = None,
        timestamp_col: str = "ingested_at",
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        columns: Optional[List[str]] = None,
        require_attachments: bool = False,
    ) -> pl.DataFrame:
        """
        Read events filtered by status_subtypes and optionally after a watermark.

        Uses Delta Lake's native partition pruning via filter pushdown.
        The event_date partition column is used to limit data scanned.

        Memory optimization: Use `columns` parameter to project only needed columns
        early in the query plan, significantly reducing memory for wide tables.

        Args:
            status_subtypes: List of status_subtype values to filter on
            watermark: Only read events after this timestamp (required)
            timestamp_col: Timestamp column name (default: ingested_at)
            limit: Optional row limit
            order_by: Optional column to sort by
            columns: Optional list of columns to select (projects early for memory efficiency)
            require_attachments: If True, filter to only rows with non-null, non-empty attachments

        Returns:
            Filtered DataFrame
        """
        if watermark is None:
            raise ValueError("Watermark required for this method")

        self._log(
            logging.DEBUG,
            "Reading events by status subtypes",
            status_subtypes=status_subtypes,
            watermark=watermark.isoformat(),
            columns=columns,
            require_attachments=require_attachments,
        )

        opts = self.storage_options or get_storage_options()

        # Calculate date range for partition pruning
        # Ensure we use date type for partition column comparison
        watermark_date = watermark.date()
        today = datetime.now(timezone.utc).date()

        # Build lazy scan with filter pushdown
        # Delta Lake will automatically prune partitions based on event_date filter
        lf = pl.scan_delta(self.table_path, storage_options=opts)

        # OPTIMIZATION: Project columns early to reduce memory footprint
        # This must happen before any operations that might materialize data
        if columns:
            # Ensure partition and filter columns are included for pushdown
            required_cols = {"event_date", timestamp_col, "status_subtype"}
            if require_attachments:
                required_cols.add("attachments")
            all_cols = list(set(columns) | required_cols)
            lf = lf.select(all_cols)

        # Build filter expression - separate partition filters for better pushdown
        # Partition filter (file-level pruning)
        partition_filter = (pl.col("event_date") >= watermark_date) & (
            pl.col("event_date") <= today
        )

        # Row-level filters
        row_filter = (pl.col(timestamp_col) > watermark) & (
            pl.col("status_subtype").is_in(status_subtypes)
        )

        # Optional attachments filter - push into query instead of post-filtering
        if require_attachments:
            row_filter = (
                row_filter
                & pl.col("attachments").is_not_null()
                & (pl.col("attachments") != "")
            )

        # Apply filters - partition filter first for better optimization hints
        lf = lf.filter(partition_filter).filter(row_filter)

        # OPTIMIZATION: When sorting with a limit, we must materialize all matching
        # rows to sort them. For large datasets, consider:
        # 1. Using a tighter time window (reduce watermark lookback)
        # 2. Skipping sort if approximate ordering is acceptable
        # 3. Using a two-phase approach for very large datasets
        if order_by and limit:
            self._log(
                logging.DEBUG,
                "Sort with limit: will materialize all matching rows for sort",
                order_by=order_by,
                limit=limit,
            )
            lf = lf.sort(order_by).head(limit)
        elif order_by:
            lf = lf.sort(order_by)
        elif limit:
            # No sort - can apply limit early without full materialization
            lf = lf.head(limit)

        # Collect - streaming mode helps when no blocking operations (like sort)
        # CRITICAL: streaming=True can cause "invalid SlotMap key used" panic when
        # combined with sort(), so only enable streaming when no sort is present
        use_streaming = order_by is None
        result = lf.collect(streaming=use_streaming)

        self._log(
            logging.DEBUG,
            "Read by status subtypes complete",
            rows_read=len(result),
            status_subtypes=status_subtypes,
            watermark_date=str(watermark_date),
            today=str(today),
        )

        return result


# Z-Ordering Helper Functions and Constants (P3.2)

RECOMMENDED_Z_ORDER_COLUMNS = {
    "inventory": [
        "created_date",  # Time-based queries
        "status",  # Status filtering
        "trace_id",  # Lookup by trace
    ],
    "retry_queue": [
        "retry_count",  # Retry filtering
        "error_type",  # Error analysis
        "created_date",  # Time-based queries
    ],
    "attachments": [
        "download_status",  # Status filtering
        "attachment_url",  # Lookup by URL
        "created_date",  # Time-based queries
    ],
}
"""Recommended Z-order columns by table type for query optimization."""


def suggest_z_order_columns(
    table_path: str, query_patterns: Optional[List[str]] = None
) -> List[str]:
    """
    Suggest Z-order columns based on query patterns or table type (P3.2).

    Z-ordering improves query performance by co-locating related data.
    This function suggests appropriate columns based on common query
    patterns for known table types.

    Args:
        table_path: Path to Delta table
        query_patterns: Optional list of common filter columns

    Returns:
        Suggested Z-order columns

    Example:
        # Use explicit query patterns
        columns = suggest_z_order_columns(
            "path/to/table",
            query_patterns=["created_date", "status"]
        )

        # Infer from table name
        columns = suggest_z_order_columns("inventory_table")
        # Returns: ["created_date", "status", "trace_id"]
    """
    # If query patterns provided, use those
    if query_patterns:
        return query_patterns

    # Otherwise, use common patterns for this domain
    table_path_lower = table_path.lower()

    # Try to infer from table name
    for pattern_name, columns in RECOMMENDED_Z_ORDER_COLUMNS.items():
        if pattern_name in table_path_lower:
            logger.info(
                "Suggested Z-order columns for table",
                extra={
                    "table": table_path,
                    "pattern": pattern_name,
                    "columns": columns,
                },
            )
            return columns

    # Default: use timestamp columns
    logger.info(
        "No specific pattern matched, using default Z-order columns",
        extra={"table": table_path, "default_columns": ["created_date"]},
    )
    return ["created_date"]
