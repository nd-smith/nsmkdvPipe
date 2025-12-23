"""
Retry queue writer for managing bounded retry table lifecycle.

Provides optimized MERGE/DELETE operations for split-table architecture:
- MERGE for upsert (new failures or retry count increments)
- DELETE for lifecycle management (success, exhausted, expired)
- Exponential backoff calculation
- Retention policy enforcement
- Queue health monitoring

All datetimes are UTC-aware (timezone.utc).

Usage:
    from verisk_pipeline.storage.retry_queue_writer import RetryQueueWriter

    writer = RetryQueueWriter(
        table_path=path,
        primary_keys=["trace_id", "attachment_url"],
    )

    # Write new failures or update existing
    writer.write_retry(
        rows=[...],
        max_retries=3,
        backoff_base_seconds=300,
        backoff_multiplier=2.0
    )

    # Delete completed retries
    writer.delete_by_keys([{"trace_id": "...", "attachment_url": "..."}])

    # Cleanup expired records
    deleted = writer.cleanup_expired(retention_days=7)
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import polars as pl
from deltalake import DeltaTable

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


class RetryQueueWriter(LoggedClass):
    """
    Retry queue writer with MERGE/DELETE lifecycle management.

    Manages bounded retry queue with:
    - MERGE-based upsert for failures and retry increments
    - DELETE-based cleanup for completed/exhausted/expired records
    - Exponential backoff with vectorized calculation
    - Retention policy enforcement
    """

    def __init__(
        self,
        table_path: str,
        primary_keys: List[str],
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self.table_path = table_path
        self.primary_keys = primary_keys
        self._reader = DeltaTableReader(table_path)
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

    def _calculate_next_retry_at(
        self, retry_count: int, backoff_base_seconds: float, backoff_multiplier: float
    ) -> datetime:
        """Calculate next retry timestamp with exponential backoff."""
        delay_seconds = backoff_base_seconds * (backoff_multiplier**retry_count)
        return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    @logged_operation(operation_name="write_retry")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def write_retry(
        self,
        rows: List[Dict[str, Any]],
        max_retries: int,
        backoff_base_seconds: float,
        backoff_multiplier: float,
    ) -> int:
        """
        Write or update retry records using MERGE.

        IMPORTANT: Caller must provide retry_count and next_retry_at values.
        This method performs a simple MERGE - it does NOT auto-increment retry_count.

        For new records (INSERT):
        - Caller should set retry_count = 0
        - Caller should calculate next_retry_at
        - This method sets status = 'failed', created_at, updated_at

        For existing records (UPDATE from retry stage):
        - Caller should read current retry_count from queue
        - Caller should increment retry_count
        - Caller should recalculate next_retry_at based on new retry_count
        - This method updates all provided fields

        Args:
            rows: List of retry records with ALL fields populated by caller
            max_retries: Maximum retry attempts (for logging only)
            backoff_base_seconds: Base delay (for logging only)
            backoff_multiplier: Multiplier (for logging only)

        Returns:
            Number of rows written/updated
        """
        if not rows:
            self._logger.debug("No retry rows to write")
            return 0

        # Convert to Polars DataFrame
        df = pl.DataFrame(rows)

        # Add/update metadata timestamps
        now = datetime.now(timezone.utc)

        # Set status if not provided
        if "status" not in df.columns:
            df = df.with_columns(pl.lit("failed").alias("status"))

        # Set retry_count if not provided (new records)
        if "retry_count" not in df.columns:
            df = df.with_columns(pl.lit(0).alias("retry_count"))

        # Calculate next_retry_at if not provided
        if "next_retry_at" not in df.columns:
            df = df.with_columns(
                [
                    pl.col("retry_count")
                    .map_elements(
                        lambda rc: self._calculate_next_retry_at(
                            rc, backoff_base_seconds, backoff_multiplier
                        ),
                        return_dtype=pl.Datetime,
                    )
                    .alias("next_retry_at")
                ]
            )

        # Set timestamps
        if "created_at" not in df.columns:
            df = df.with_columns(pl.lit(now).alias("created_at"))

        df = df.with_columns(
            [
                pl.lit(now).alias("updated_at"),
                pl.lit(now.date()).alias("created_date"),
            ]
        )

        # Build MERGE predicate (composite key match)
        predicate_parts = [f"target.{pk} = source.{pk}" for pk in self.primary_keys]
        merge_predicate = " AND ".join(predicate_parts)

        # Build UPDATE SET clause - update ALL columns from source
        # This allows retry stage to pass incremented retry_count
        all_cols = df.columns
        update_set = {
            col: f"source.{col}" for col in all_cols if col not in self.primary_keys
        }

        # Execute MERGE
        dt = DeltaTable(self.table_path, storage_options=self.get_storage_options())

        dt.merge(
            source=df.to_arrow(),
            predicate=merge_predicate,
            source_alias="source",
            target_alias="target",
        ).when_matched_update(set=update_set).when_not_matched_insert_all().execute()

        self._logger.info(
            f"Wrote/updated {len(df)} retry records",
            extra={"table": self.table_path, "row_count": len(df)},
        )

        return len(df)

    @logged_operation(operation_name="delete_by_keys")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def delete_by_keys(self, key_dicts: List[Dict[str, Any]]) -> int:
        """
        Delete retry records by composite primary keys.

        Used for lifecycle management:
        - Delete after successful retry (now in inventory)
        - Delete after exhausted retries (failed_permanent)
        - Delete after manual intervention

        Args:
            key_dicts: List of dicts containing primary key values
                      Example: [{"trace_id": "x", "attachment_url": "y"}]

        Returns:
            Number of rows deleted
        """
        if not key_dicts:
            self._logger.debug("No keys to delete")
            return 0

        # Build DELETE predicate (composite key IN list)
        # For single key: WHERE key IN (val1, val2, ...)
        # For composite: WHERE (key1 = val1 AND key2 = val2) OR (key1 = val3 AND key2 = val4) OR ...

        if len(self.primary_keys) == 1:
            # Single key optimization
            pk = self.primary_keys[0]
            values = [str(kd[pk]) for kd in key_dicts]
            values_str = ", ".join(f"'{v}'" for v in values)
            predicate = f"{pk} IN ({values_str})"
        else:
            # Composite key - build OR'd predicates
            predicates = []
            for kd in key_dicts:
                parts = [f"{pk} = '{kd[pk]}'" for pk in self.primary_keys]
                predicates.append(f"({' AND '.join(parts)})")
            predicate = " OR ".join(predicates)

        # Execute DELETE
        dt = DeltaTable(self.table_path, storage_options=self.get_storage_options())

        # Get count before delete for logging
        before_count = dt.to_pyarrow_dataset().count_rows()

        dt.delete(predicate)

        after_count = dt.to_pyarrow_dataset().count_rows()
        deleted = before_count - after_count

        self._logger.info(
            f"Deleted {deleted} retry records",
            extra={"table": self.table_path, "deleted_count": deleted},
        )

        return deleted

    @logged_operation(operation_name="get_pending_retries")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_pending_retries(
        self,
        max_retries: int,
        min_age_seconds: int = 0,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Get retry-eligible records from queue.

        Filters:
        - status = 'failed'
        - retry_count < max_retries
        - next_retry_at <= now (respects backoff)
        - Optional: created_at >= now - min_age_seconds

        Args:
            max_retries: Maximum retry attempts
            min_age_seconds: Minimum age before retry (additional safety)
            limit: Maximum records to return

        Returns:
            DataFrame of retry-eligible records
        """
        now = datetime.now(timezone.utc)
        min_created_at = (
            now - timedelta(seconds=min_age_seconds) if min_age_seconds > 0 else None
        )

        # Read with filter pushdown (exclude next_retry_at - need NULL handling)
        filters = [
            ("status", "=", "failed"),
            ("retry_count", "<", max_retries),
        ]

        if min_created_at:
            filters.append(("created_at", ">=", min_created_at))

        df = self._reader.read_as_polars(filters=filters)

        # Apply next_retry_at filter with NULL handling
        # NULL next_retry_at = immediately eligible (legacy records)
        if not df.is_empty() and "next_retry_at" in df.columns:
            df = df.filter(
                pl.col("next_retry_at").is_null() | (pl.col("next_retry_at") <= now)
            )

        # Ensure retry_count is integer (handle schema inconsistencies)
        if "retry_count" in df.columns:
            df = df.with_columns(
                pl.col("retry_count").cast(pl.Int64, strict=False).fill_null(0)
            )

        # Sort by next_retry_at (oldest first) and limit
        df = df.sort("next_retry_at")

        if limit:
            df = df.head(limit)

        # Batch size validation (Task E.1)
        config = get_config()
        max_batch_size = config.lakehouse.max_batch_size_retry_queue
        if len(df) > max_batch_size:
            self._logger.warning(
                f"Retry queue batch exceeds configured limit",
                extra={
                    "rows_read": len(df),
                    "max_batch_size": max_batch_size,
                    "table": self.table_path,
                },
            )

        self._logger.debug(
            f"Found {len(df)} pending retries",
            extra={"table": self.table_path, "count": len(df), "limit": limit},
        )

        return df

    @logged_operation(operation_name="get_queued_ids")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def get_queued_ids(self) -> set:
        """
        Get primary key values of all records currently in retry queue.

        Used by download stage to exclude media already queued for retry,
        preventing duplicate processing and retry_count resets.

        Returns:
            Set of primary key strings (composite keys joined by '|')
        """
        try:
            if not self._reader.exists():
                self._logger.debug("Retry queue table does not exist yet")
                return set()

            queued = (
                pl.scan_delta(
                    self.table_path, storage_options=self.get_storage_options()
                )
                .select(self.primary_keys)
                .unique()
                .collect(streaming=True)
            )

        except Exception as e:
            self._logger.warning(
                f"Could not read queued IDs: {e}",
                extra={"table": self.table_path, "error": str(e)},
            )
            return set()

        ids = set(
            self._make_composite_key(row) for row in queued.iter_rows(named=True)
        )

        self._logger.debug(
            f"Found {len(ids)} IDs in retry queue",
            extra={"table": self.table_path, "count": len(ids)},
        )

        return ids

    @logged_operation(operation_name="cleanup_expired")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def cleanup_expired(self, retention_days: int) -> int:
        """
        Delete retry records older than retention period.

        Implements retention policy to keep queue bounded.
        Deletes records where created_at < (now - retention_days).

        Args:
            retention_days: Number of days to retain records

        Returns:
            Number of rows deleted
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        cutoff_iso = cutoff.isoformat()

        # Execute DELETE
        dt = DeltaTable(self.table_path, storage_options=self.get_storage_options())

        # Get count before delete for logging
        before_count = dt.to_pyarrow_dataset().count_rows()

        predicate = f"created_at < '{cutoff_iso}'"
        dt.delete(predicate)

        after_count = dt.to_pyarrow_dataset().count_rows()
        deleted = before_count - after_count

        self._logger.info(
            f"Cleaned up {deleted} expired retry records",
            extra={
                "table": self.table_path,
                "deleted_count": deleted,
                "retention_days": retention_days,
                "cutoff": cutoff_iso,
            },
        )

        return deleted

    @logged_operation(operation_name="get_queue_statistics")
    @with_retry(config=DELTA_RETRY_CONFIG, on_auth_error=_on_auth_error)
    def start_cycle(self, cycle_id: str) -> None:
        """Start processing cycle - enables logging and tracking."""
        self._cycle_id = cycle_id
        self._logger.debug(f"Started retry queue cycle: {cycle_id}")

    def end_cycle(self) -> None:
        """End processing cycle - clears cycle state."""
        if self._cycle_id:
            self._logger.debug(f"Ended retry queue cycle: {self._cycle_id}")
        self._cycle_id = None

    @contextmanager
    def cycle_context(self, cycle_id: str):
        """Context manager for processing cycles with automatic cleanup (Task E.4).

        Ensures cycle state is properly cleared even if exceptions occur.

        Args:
            cycle_id: Unique identifier for this processing cycle

        Yields:
            None

        Example:
            with writer.cycle_context("cycle_001"):
                # Processing logic here
                pending = writer.get_pending_retries(max_retries=3)
                # Cycle automatically ended on exit
        """
        self.start_cycle(cycle_id)
        try:
            yield
        finally:
            self.end_cycle()

    def get_queue_statistics(self) -> Dict[str, int]:
        """
        Get retry queue health metrics.

        Returns:
            Dict with queue statistics:
            - total_rows: Total records in queue
            - status_failed: Count with status='failed'
            - status_permanent: Count with status='failed_permanent'
            - retry_0: Count with retry_count=0
            - retry_1: Count with retry_count=1
            - retry_2+: Count with retry_count>=2
        """
        df = self._reader.read_as_polars()

        stats = {
            "total_rows": len(df),
            "status_failed": len(df.filter(pl.col("status") == "failed")),
            "status_permanent": len(df.filter(pl.col("status") == "failed_permanent")),
            "retry_0": len(df.filter(pl.col("retry_count") == 0)),
            "retry_1": len(df.filter(pl.col("retry_count") == 1)),
            "retry_2_plus": len(df.filter(pl.col("retry_count") >= 2)),
        }

        # Warning if queue is getting large
        if stats["total_rows"] > 10000:
            self._logger.warning(
                f"Retry queue is large: {stats['total_rows']} rows",
                extra={"table": self.table_path, "stats": stats},
            )

        return stats
