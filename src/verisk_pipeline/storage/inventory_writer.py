"""
Inventory table writer for success-only download tracking.

Append-only writes with automatic status/error field stripping. Split-table architecture:
inventory (successes) and retry queue (failures). All datetimes are UTC-aware.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import polars as pl
from deltalake import write_deltalake, DeltaTable

from verisk_pipeline.storage.delta import DeltaTableReader
from verisk_pipeline.common.config.xact import get_config
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.decorators import logged_operation, LoggedClass
from verisk_pipeline.common.retry import with_retry, RetryConfig
from verisk_pipeline.common.auth import get_storage_options, clear_token_cache


logger = get_logger(__name__)
DELTA_RETRY_CONFIG = RetryConfig(max_attempts=3, base_delay=1.0, max_delay=30.0)


def _on_auth_error():
    """Clear token cache on auth errors."""
    clear_token_cache()


@dataclass
class BatchWriteResult:
    """Result of batch write operation with partial success tracking (Task E.6).

    Tracks both successful and failed writes within a batch operation,
    enabling graceful degradation and retry of only failed items.
    """

    total_attempted: int
    rows_written: int
    rows_failed: int
    failed_rows: List[Dict[str, Any]]
    error_summary: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_attempted == 0:
            return 0.0
        return (self.rows_written / self.total_attempted) * 100


class InventoryTableWriter(LoggedClass):
    """
    Inventory table writer for success-only download tracking.

    Key characteristics:
    - Append-only (no UPDATE/DELETE operations)
    - Automatically strips status/error fields from writes
    - Provides get_completed_ids() for dedup checking
    - NO retry logic (handled by RetryQueueWriter)
    """

    # Fields to strip from inventory writes (retry-specific)
    RETRY_FIELDS = {
        "status",
        "retry_count",
        "error_category",
        "error_message",
        "http_status",
        "next_retry_at",
        "expires_at",
        "expired_at_ingest",
        "refresh_count",
    }

    def __init__(
        self,
        table_path: str,
        primary_keys: List[str],
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self.table_path = table_path
        self.primary_keys = primary_keys
        self._reader = DeltaTableReader(table_path)
        self._completed_ids_cache: Optional[Set[str]] = None
        self._cache_valid = False
        self._cache_size_exceeded = False
        self._cycle_id: Optional[str] = None
        super().__init__()

    def get_storage_options(self) -> Dict[str, str]:
        """Get storage options for Delta access."""
        return get_storage_options()

    def _make_composite_key(self, row: Dict[str, Any]) -> str:
        """Create composite key string from row for set lookups."""
        if len(self.primary_keys) == 1:
            return str(row[self.primary_keys[0]])
        return "|".join(str(row[k]) for k in self.primary_keys)

    def _strip_retry_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remove retry-specific fields from DataFrame."""
        cols_to_drop = [c for c in self.RETRY_FIELDS if c in df.columns]
        if cols_to_drop:
            self._logger.debug(f"Stripping retry fields: {cols_to_drop}")
            df = df.drop(cols_to_drop)
        return df

    @logged_operation(operation_name="write_inventory")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_inventory(self, rows: List[Dict[str, Any]]) -> int:
        """
        Write successful downloads to inventory (append-only).

        Automatically strips status/error fields - inventory table
        has NO status column (success is implicit).

        Args:
            rows: List of download success records

        Returns:
            Number of rows written
        """
        if not rows:
            self._logger.debug("No inventory rows to write")
            return 0

        # Convert to Polars DataFrame
        df = pl.DataFrame(rows)

        # Strip retry-specific fields
        df = self._strip_retry_fields(df)

        # Add created_at if not present
        if "created_at" not in df.columns:
            df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("created_at"))

        # Add created_date if not present
        if "created_date" not in df.columns:
            now = datetime.now(timezone.utc)
            df = df.with_columns(pl.lit(now.date()).alias("created_date"))

        # Write to Delta table (append-only)
        try:
            write_deltalake(
                self.table_path,
                df.to_arrow(),
                mode="append",
                schema_mode="merge",
                storage_options=self.get_storage_options(),
            )

            self._logger.info(
                f"Wrote {len(df)} inventory records",
                extra={"table": self.table_path, "row_count": len(df)},
            )

            # Invalidate completed IDs cache
            self._completed_ids_cache = None

            return len(df)

        except Exception as e:
            self._logger.error(
                f"Failed to write inventory rows: {e}",
                extra={"table": self.table_path, "error": str(e)},
            )
            raise

    @logged_operation(operation_name="get_completed_ids")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_completed_ids(self, use_cache: bool = True) -> Set[str]:
        """
        Get composite keys of all records in inventory (for dedup) with size monitoring (P2.2).

        Since inventory is success-only, all records in the table
        represent completed downloads.

        Args:
            use_cache: Whether to use cached result

        Returns:
            Set of composite key strings
        """
        # Get cache size limit from config
        from verisk_pipeline.common.config.xact import get_config

        config = get_config()
        max_cache_size = config.lakehouse.max_cache_size_completed_ids

        if use_cache and self._cache_valid and not self._cache_size_exceeded:
            self._logger.debug(
                f"Cache hit: completed IDs (size={len(self._completed_ids_cache)})"
            )
            return self._completed_ids_cache

        try:
            if not self._reader.exists():
                self._logger.debug("Inventory table does not exist yet")
                return set()

            completed = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .select(self.primary_keys)
                .unique()
                .collect(streaming=True)
            )

        except Exception as e:
            self._logger.warning(
                f"Could not read completed IDs: {e}",
                extra={"table": self.table_path, "error": str(e)},
            )
            return set()

        ids = set(
            self._make_composite_key(row) for row in completed.iter_rows(named=True)
        )

        # Check cache size limit (P2.2)
        if len(ids) > max_cache_size:
            cache_size_mb = len(ids) * 8 / (1024 * 1024)
            self._logger.warning(
                f"Completed IDs cache size {len(ids):,} exceeds limit {max_cache_size:,}",
                extra={
                    "cache_size": len(ids),
                    "limit": max_cache_size,
                    "cache_size_mb": round(cache_size_mb, 2),
                },
            )
            self._cache_size_exceeded = True
            # Don't cache, return directly
            return ids

        # Cache is within limits
        if use_cache:
            self._completed_ids_cache = ids
            self._cache_valid = True
            self._cache_size_exceeded = False

            cache_size_mb = len(ids) * 8 / (1024 * 1024)
            self._logger.info(
                f"Cached {len(ids):,} completed IDs",
                extra={
                    "cache_size": len(ids),
                    "cache_size_mb": round(cache_size_mb, 2),
                },
            )

        return ids

    def start_cycle(self, cycle_id: str) -> None:
        """Start processing cycle - enables caching."""
        self._cycle_id = cycle_id
        self.clear_cache()
        self._logger.debug(f"Started inventory cycle: {cycle_id}")

    def end_cycle(self) -> None:
        """End processing cycle - clears cache."""
        if self._cycle_id:
            self._logger.debug(f"Ended inventory cycle: {self._cycle_id}")
        self._cycle_id = None
        self.clear_cache()

    def clear_cache(self) -> None:
        """Clear completed IDs cache."""
        self._completed_ids_cache = None
        self._cache_valid = False
        self._cache_size_exceeded = False
        self._logger.debug("Cleared inventory cache")

    def get_row_count(self) -> int:
        """Get total row count in inventory table."""
        try:
            if not self._reader.exists():
                return 0

            dt = DeltaTable(self.table_path, storage_options=self.get_storage_options())
            return dt.to_pyarrow_dataset().count_rows()

        except Exception as e:
            self._logger.warning(
                f"Could not get row count: {e}",
                extra={"table": self.table_path, "error": str(e)},
            )
            return 0

    @contextmanager
    def cycle_context(self, cycle_id: str):
        """Context manager for processing cycles with automatic cleanup (Task E.4).

        Ensures cache is properly cleared even if exceptions occur during processing.

        Args:
            cycle_id: Unique identifier for this processing cycle

        Yields:
            None

        Example:
            with writer.cycle_context("cycle_001"):
                # Processing logic here
                writer.write_inventory(rows)
                # Cache automatically cleared on exit
        """
        self.start_cycle(cycle_id)
        try:
            yield
        finally:
            self.end_cycle()

    @logged_operation(operation_name="write_inventory_batch")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_inventory_batch(
        self,
        rows: List[Dict[str, Any]],
        max_batch_size: Optional[int] = None,
    ) -> BatchWriteResult:
        """
        Write inventory batch with partial success tracking (Task E.6).

        Attempts to write all rows, tracking successes and failures separately.
        Enables retry of only failed rows rather than entire batch.

        Args:
            rows: List of download success records
            max_batch_size: Optional batch size limit (defaults to config value)

        Returns:
            BatchWriteResult with success/failure breakdown
        """
        if not rows:
            self._log(logging.DEBUG, "No inventory rows to write")
            return BatchWriteResult(
                total_attempted=0,
                rows_written=0,
                rows_failed=0,
                failed_rows=[],
            )

        total_attempted = len(rows)

        # Get batch size limit from config if not provided
        if max_batch_size is None:
            config = get_config()
            max_batch_size = config.lakehouse.max_batch_size_inventory

        # Validate batch size (Task E.1)
        if total_attempted > max_batch_size:
            self._log(
                logging.WARNING,
                "Batch size exceeds configured limit",
                total_rows=total_attempted,
                max_batch_size=max_batch_size,
            )

        # Attempt to write all rows
        try:
            rows_written = self.write_inventory(rows)

            return BatchWriteResult(
                total_attempted=total_attempted,
                rows_written=rows_written,
                rows_failed=0,
                failed_rows=[],
            )

        except Exception as e:
            # Partial failure - try to write rows individually to identify failures
            self._log(
                logging.WARNING,
                "Batch write failed, attempting individual writes",
                total_rows=total_attempted,
                error=str(e)[:200],
            )

            successful = 0
            failed_rows = []

            for row in rows:
                try:
                    # Write single row
                    result = self.write_inventory([row])
                    successful += result
                except Exception as row_error:
                    self._log(
                        logging.DEBUG,
                        "Individual row write failed",
                        error=str(row_error)[:100],
                    )
                    failed_rows.append(row)

            rows_failed = len(failed_rows)
            success_rate = (
                (successful / total_attempted) * 100 if total_attempted > 0 else 0
            )

            self._log(
                logging.INFO,
                "Batch write completed with partial success",
                total_attempted=total_attempted,
                rows_written=successful,
                rows_failed=rows_failed,
                success_rate=round(success_rate, 1),
            )

            return BatchWriteResult(
                total_attempted=total_attempted,
                rows_written=successful,
                rows_failed=rows_failed,
                failed_rows=failed_rows,
                error_summary=str(e)[:200],
            )
