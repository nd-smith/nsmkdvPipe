"""
Tracking table writer for pipeline retry operations.

Provides optimized query patterns for tracking download status:
- Filter pushdown to Delta (only read 'failed' rows)
- Column projection (only read needed columns)
- Vectorized backoff calculation in Polars
- Per-cycle caching to avoid redundant scans
- Pre-computed next_retry_at for maximum efficiency

All datetimes are UTC-aware (timezone.utc).

Usage:
    from verisk_pipeline.storage.table_writers import TrackingTableWriter

    writer = TrackingTableWriter(
        table_path=path,
        primary_keys=["trace_id", "download_url"],
        retry_columns=["trace_id", "download_url", "status", ...],
    )

    # With cycle caching (recommended for retry stages)
    writer.start_cycle(cycle_id)
    try:
        pending = writer.get_pending_retries(limit=100)
        # ... process ...
    finally:
        writer.end_cycle()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import polars as pl
from deltalake import DeltaTable, write_deltalake

from verisk_pipeline.storage.delta import DeltaTableReader
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
class CycleCache:
    """Per-cycle cache to avoid redundant scans."""

    cycle_id: str
    completed_ids: Optional[Set[str]] = None
    permanent_ids: Optional[Set[str]] = None
    failed_df: Optional[pl.DataFrame] = None


class TrackingTableWriter(LoggedClass):
    """
    Tracking table writer with optimized queries for retry operations.

    Memory optimizations applied:
    - Column projection: only reads needed columns
    - Filter pushdown: only reads failed/completed rows as needed
    - Cycle caching: avoids redundant scans within a cycle
    - Vectorized operations: no Python loops for backoff calculation
    """

    def __init__(
        self,
        table_path: str,
        primary_keys: List[str],
        retry_columns: List[str],
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self.table_path = table_path
        self.primary_keys = primary_keys
        self.retry_columns = retry_columns

        self._reader = DeltaTableReader(table_path)
        self._cycle_cache: Optional[CycleCache] = None
        self._available_columns: Optional[Set[str]] = None
        super().__init__()

    def get_storage_options(self) -> Dict[str, str]:
        """Get storage options for Delta access."""
        return get_storage_options()

    def _make_composite_key(self, row: Dict[str, Any]) -> str:
        """Create composite key string from row for set lookups."""
        if len(self.primary_keys) == 1:
            return str(row[self.primary_keys[0]])
        return "|".join(str(row[k]) for k in self.primary_keys)

    def start_cycle(self, cycle_id: str) -> None:
        """Start processing cycle - enables caching."""
        self._cycle_cache = CycleCache(cycle_id=cycle_id)
        self._available_columns = None
        self._log(logging.DEBUG, "Started tracking cycle", cycle_id=cycle_id)

    def end_cycle(self) -> None:
        """End processing cycle - clears cache."""
        if self._cycle_cache:
            self._log(
                logging.DEBUG,
                "Ended tracking cycle",
                cycle_id=self._cycle_cache.cycle_id,
            )
        self._cycle_cache = None

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def _get_available_columns(self) -> Set[str]:
        """Get available columns in table (cached)."""
        if self._available_columns is not None:
            return self._available_columns

        try:
            schema_df = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .head(0)
                .collect()
            )
            self._available_columns = set(schema_df.columns)
        except Exception:
            self._available_columns = set()

        return self._available_columns

    def _select_available_columns(self, requested: List[str]) -> List[str]:
        """Filter requested columns to those that exist in table."""
        available = self._get_available_columns()
        return [c for c in requested if c in available]

    def _table_exists(self) -> bool:
        """Check if Delta table exists."""
        try:
            DeltaTable(self.table_path, storage_options=self.get_storage_options())
            self._log(logging.DEBUG, "Table exists check passed")
            return True
        except Exception as e:
            self._log(logging.DEBUG, "Table does not exist", error_message=str(e)[:200])
            return False

    @logged_operation(level=logging.DEBUG, include_args=["max_retries", "limit"])
    def get_pending_retries(
        self,
        max_retries: int = 3,
        min_age_seconds: int = 60,
        backoff_base_seconds: float = 30.0,
        backoff_multiplier: float = 2.0,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Get records eligible for retry with optimized queries.

        Optimizations:
        1. Filter pushdown - only reads 'failed' rows from Delta
        2. Column projection - only reads needed columns
        3. Vectorized backoff - Polars expressions, not Python loops
        4. Caching - reuses failed_df within cycle
        """
        now = datetime.now(timezone.utc)

        select_cols = self._select_available_columns(self.retry_columns)
        if not select_cols:
            self._log(logging.WARNING, "No retry columns available in tracking table")
            return pl.DataFrame()

        if "next_retry_at" in self._get_available_columns():
            if "next_retry_at" not in select_cols:
                select_cols.append("next_retry_at")

        failed_df = self._get_failed_rows(select_cols)

        if failed_df.is_empty():
            self._log(logging.DEBUG, "No failed records found")
            return failed_df

        self._log(
            logging.DEBUG,
            "Read failed rows with filter pushdown",
            records_processed=len(failed_df),
        )

        latest = (
            failed_df.sort("created_at", descending=True)
            .group_by(self.primary_keys)
            .first()
        )

        self._log(logging.DEBUG, "After dedup", records_processed=len(latest))

        eligible = latest.filter(
            (pl.col("retry_count") < max_retries)
            & (
                (pl.col("error_category").is_null())
                | (pl.col("error_category") != "permanent")
            )
        )

        if eligible.is_empty():
            self._log(logging.DEBUG, "No retry-eligible records")
            return eligible

        if "next_retry_at" in eligible.columns:
            # Compare UTC-aware datetimes directly
            ready = eligible.filter(
                (pl.col("next_retry_at").is_null())
                | (pl.col("next_retry_at") <= pl.lit(now))
            )
        else:
            ready = self._apply_backoff_filter(
                eligible, now, min_age_seconds, backoff_base_seconds, backoff_multiplier
            )

        self._log(logging.DEBUG, "Ready for retry", records_processed=len(ready))

        if limit:
            ready = ready.head(limit)

        return ready

    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def _get_failed_rows(self, columns: List[str]) -> pl.DataFrame:
        """Get failed rows with caching support."""
        if self._cycle_cache and self._cycle_cache.failed_df is not None:
            cached_cols = [
                c for c in columns if c in self._cycle_cache.failed_df.columns
            ]
            return self._cycle_cache.failed_df.select(cached_cols)

        try:
            failed_df = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .filter(pl.col("status") == "failed")
                .select(columns)
                .collect()
            )
        except Exception as e:
            self._log_exception(
                e,
                "Could not scan tracking table",
                level=logging.WARNING,
            )
            return pl.DataFrame()

        if self._cycle_cache:
            self._cycle_cache.failed_df = failed_df

        return failed_df

    def _apply_backoff_filter(
        self,
        df: pl.DataFrame,
        now: datetime,
        min_age_seconds: int,
        backoff_base_seconds: float,
        backoff_multiplier: float,
    ) -> pl.DataFrame:
        """Apply exponential backoff filter using Polars expressions."""
        # now is already UTC-aware

        if df["created_at"].dtype in (pl.Utf8, pl.String):
            # Parse string to datetime, assuming UTC
            try:
                df = df.with_columns(
                    pl.col("created_at")
                    .str.to_datetime(time_zone="UTC")
                    .alias("_created_dt")
                )
            except Exception:
                try:
                    # Fallback: parse without timezone, then assume UTC
                    df = df.with_columns(
                        pl.col("created_at")
                        .str.slice(0, 26)
                        .str.to_datetime()
                        .dt.replace_time_zone("UTC")
                        .alias("_created_dt")
                    )
                except Exception as e:
                    self._log_exception(
                        e,
                        "Could not parse created_at",
                        level=logging.WARNING,
                    )
                    return df
        else:
            # Datetime column - ensure UTC-aware for comparison
            col_dtype = df["created_at"].dtype
            if hasattr(col_dtype, "time_zone") and col_dtype.time_zone is None:
                # Naive datetime column - assume UTC
                df = df.with_columns(
                    pl.col("created_at")
                    .dt.replace_time_zone("UTC")
                    .alias("_created_dt")
                )
            else:
                # Already UTC-aware
                df = df.with_columns(pl.col("created_at").alias("_created_dt"))

        return (
            df.with_columns(
                (
                    min_age_seconds
                    + backoff_base_seconds
                    * (backoff_multiplier ** pl.col("retry_count").fill_null(0))
                ).alias("_wait_seconds")
            )
            .with_columns(
                (
                    pl.col("_created_dt") + pl.duration(seconds=pl.col("_wait_seconds"))
                ).alias("_next_retry")
            )
            .filter(pl.col("_next_retry") <= pl.lit(now))
            .drop(["_created_dt", "_wait_seconds", "_next_retry"])
        )

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_latest_status(self) -> pl.DataFrame:
        """
        Get latest status for each entity (for exclusion checks).

        OPTIMIZED: Only selects primary_keys + status + created_at columns.
        """
        try:
            if not self._reader.exists():
                return pl.DataFrame()

            # OPTIMIZATION: Only select needed columns
            select_cols = self.primary_keys + ["status", "created_at"]

            df = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .select(select_cols)
                .filter(
                    pl.col("status").is_in(
                        ["completed", "failed", "failed_permanent", "deferred"]
                    )
                )
                .collect()
            )

            if df is None or df.is_empty():
                return pl.DataFrame()

            return (
                df.sort("created_at", descending=True)
                .group_by(self.primary_keys)
                .first()
            )
        except Exception as e:
            self._log_exception(
                e,
                "Could not get latest status",
                level=logging.WARNING,
            )
            return pl.DataFrame()

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_completed_ids(self) -> Set[str]:
        """Get composite keys that have completed successfully."""
        if self._cycle_cache and self._cycle_cache.completed_ids is not None:
            self._log(
                logging.DEBUG,
                "Cache hit: completed IDs",
                cache_size=len(self._cycle_cache.completed_ids),
            )
            return self._cycle_cache.completed_ids

        try:
            completed = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .filter(pl.col("status") == "completed")
                .select(self.primary_keys)
                .unique()
                .collect()
            )
        except Exception as e:
            self._log_exception(
                e,
                "Could not read completed IDs",
                level=logging.WARNING,
            )
            return set()

        ids = set(
            self._make_composite_key(row) for row in completed.iter_rows(named=True)
        )

        self._log(
            logging.DEBUG,
            "Loaded completed IDs",
            ids_count=len(ids),
        )

        if self._cycle_cache:
            self._cycle_cache.completed_ids = ids

        return ids

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_permanent_failure_ids(self) -> Set[str]:
        """Get composite keys with permanent failures."""
        if self._cycle_cache and self._cycle_cache.permanent_ids is not None:
            self._log(
                logging.DEBUG,
                "Cache hit: permanent failure IDs",
                cache_size=len(self._cycle_cache.permanent_ids),
            )
            return self._cycle_cache.permanent_ids

        try:
            permanent = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .filter(pl.col("status") == "failed_permanent")
                .select(self.primary_keys)
                .unique()
                .collect()
            )
        except Exception as e:
            self._log_exception(
                e,
                "Could not read permanent failure IDs",
                level=logging.WARNING,
            )
            return set()

        ids = set(
            self._make_composite_key(row) for row in permanent.iter_rows(named=True)
        )

        self._log(
            logging.DEBUG,
            "Loaded permanent failure IDs",
            ids_count=len(ids),
        )

        if self._cycle_cache:
            self._cycle_cache.permanent_ids = ids

        return ids

    @logged_operation(level=logging.DEBUG)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_retry_statistics(self, lookback_days: int = 7) -> Dict[str, int]:
        """
        Get retry queue statistics by status.
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

            stats = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .filter(pl.col("created_at") >= pl.lit(cutoff))
                .select(self.primary_keys + ["status", "created_at"])
                .sort("created_at", descending=True)
                .group_by(self.primary_keys)
                .first()
                .group_by("status")
                .len()
                .collect()
            )
            return {row["status"]: row["len"] for row in stats.iter_rows(named=True)}
        except Exception as e:
            self._log_exception(
                e,
                "Could not get retry statistics",
                level=logging.WARNING,
            )
            return {"pending": 0, "completed": 0, "failed": 0, "failed_permanent": 0}

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_tracking_rows(
        self,
        rows: List[Dict[str, Any]],
        compute_next_retry: bool = True,
    ) -> int:
        """Write tracking rows to Delta table."""
        if not rows:
            return 0

        for row in rows:
            if "error_category" not in row:
                row["error_category"] = None

        df = pl.DataFrame(rows, infer_schema_length=None)

        if (
            compute_next_retry
            and "status" in df.columns
            and "retry_count" in df.columns
        ):
            now = datetime.now(timezone.utc)
            min_age = 60
            base = 30.0
            mult = 2.0

            df = df.with_columns(
                pl.when(pl.col("status") == "failed")
                .then(
                    pl.lit(now)
                    + pl.duration(
                        seconds=(
                            min_age
                            + base * (mult ** pl.col("retry_count").fill_null(0))
                        )
                    )
                )
                .otherwise(pl.lit(None))
                .alias("next_retry_at")
            )

        try:
            write_deltalake(
                self.table_path,
                df.to_arrow(),
                mode="append",
                schema_mode="merge",
                storage_options=self.get_storage_options(),
            )

            # rows_written auto-captured by decorator since we return int
            return len(df)

        except Exception as e:
            self._log_exception(e, "Failed to write tracking rows")
            raise

    @logged_operation(level=logging.INFO)
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def mark_permanent_failures(
        self,
        predicate: str,
        error_message: str = "Marked as permanent failure",
    ) -> int:
        """Mark records as permanently failed using Delta UPDATE."""
        if not self._table_exists():
            self._log(logging.DEBUG, "Table does not exist, nothing to mark")
            return 0

        self._log(
            logging.INFO,
            "Marking permanent failures",
            predicate=predicate[:200] if predicate else None,
        )

        try:
            dt = DeltaTable(self.table_path, storage_options=self.get_storage_options())
            result = dt.update(
                predicate=predicate,
                updates={
                    "status": "'failed_permanent'",
                    "error_message": f"'{error_message}'",
                },
            )

            rows_updated = result.get("num_updated_rows", 0) or 0
            self._log(
                logging.INFO,
                "Marked permanent failures complete",
                rows_updated=rows_updated,
            )
            return rows_updated

        except Exception as e:
            self._log_exception(
                e,
                "Failed to mark permanent failures",
                predicate=predicate[:200] if predicate else None,
            )
            raise
