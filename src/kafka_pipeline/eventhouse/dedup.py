"""
Deduplication logic for Eventhouse polling.

Uses xact_events Delta table as the source of truth for already-processed events.
Performs anti-join in KQL for efficient filtering of new events.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl

from core.auth.credentials import get_storage_options

logger = logging.getLogger(__name__)


@dataclass
class DedupConfig:
    """Configuration for deduplication."""

    # Path to xact_events Delta table
    xact_events_table_path: str

    # Time window for querying xact_events (hours)
    # Only trace_ids from this window are used for deduplication
    xact_events_window_hours: int = 24

    # Time window for Eventhouse query (hours)
    # Only query events from this window
    eventhouse_query_window_hours: int = 1

    # Overlap window for safety (minutes)
    # Re-check events from the last N minutes even if marked as processed
    overlap_minutes: int = 5

    # Maximum trace_ids per KQL query batch
    # If more than this, chunk into multiple queries
    max_trace_ids_per_query: int = 50_000

    # Column name for trace_id in Eventhouse (may differ from Delta table)
    # Default is "traceId" to match common Eventhouse schema
    eventhouse_trace_id_column: str = "traceId"


class EventhouseDeduplicator:
    """
    Deduplicates Eventhouse events against xact_events Delta table.

    Workflow:
    1. On first call, query xact_events for recently processed trace_ids
    2. Cache trace_ids locally for subsequent polls
    3. Build KQL query with anti-join filter (trace_id !in dynamic([...]))
    4. Update cache when new events are written

    This ensures:
    - Time filter is FIRST in KQL (limits scan of Eventhouse table)
    - Anti-join happens IN KQL (not Python)
    - Delta query happens ONCE per cycle (not every poll)

    Example:
        >>> dedup = EventhouseDeduplicator(config)
        >>> query = dedup.build_deduped_query(
        ...     base_table="Events",
        ...     poll_from=datetime.now() - timedelta(hours=1),
        ... )
        >>> # After writing events:
        >>> dedup.add_to_cache(["trace-1", "trace-2"])
    """

    def __init__(self, config: DedupConfig):
        """Initialize deduplicator.

        Args:
            config: Deduplication configuration
        """
        self.config = config
        # Local cache of trace_ids - populated on first query, updated on writes
        self._trace_id_cache: set[str] = set()
        self._cache_initialized: bool = False

    def _get_storage_options(self) -> dict:
        """Get Azure storage options for Delta table access.

        Note: No instance-level caching here - the auth module handles caching
        and will detect when the token file is modified to refresh tokens.
        """
        return get_storage_options()

    def _load_cache_from_delta(
        self,
        window_hours: Optional[int] = None,
    ) -> set[str]:
        """
        Load trace_ids from xact_events Delta table into cache.

        Uses partition pruning on event_date for efficient file-level filtering,
        then applies ingested_at filter for precise time window.

        Args:
            window_hours: Hours to look back (defaults to config.xact_events_window_hours)

        Returns:
            Set of trace_ids that have been processed recently
        """
        window = window_hours or self.config.xact_events_window_hours
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window)
        cutoff_date = cutoff.date()
        today = datetime.now(timezone.utc).date()

        start_time = time.perf_counter()

        try:
            # Use Polars lazy scan with partition pruning
            # CRITICAL: Filter on event_date FIRST for partition pruning,
            # then filter on ingested_at for precise time window
            df = (
                pl.scan_delta(
                    self.config.xact_events_table_path,
                    storage_options=self._get_storage_options(),
                )
                # Partition filter - enables file-level pruning
                .filter(pl.col("event_date") >= cutoff_date)
                .filter(pl.col("event_date") <= today)
                # Row-level filter for precise time window
                .filter(pl.col("ingested_at") > cutoff)
                .select("trace_id")
                .unique()
                .collect()
            )

            trace_ids = set(df["trace_id"].to_list())

            duration_ms = (time.perf_counter() - start_time) * 1000

            logger.info(
                "Loaded trace_id cache from xact_events",
                extra={
                    "trace_id_count": len(trace_ids),
                    "window_hours": window,
                    "duration_ms": round(duration_ms, 2),
                    "table_path": self.config.xact_events_table_path,
                    "partition_filter": f"{cutoff_date} to {today}",
                },
            )

            return trace_ids

        except FileNotFoundError:
            # Table doesn't exist yet - first run, no deduplication needed
            logger.warning(
                "xact_events table not found, starting with empty cache",
                extra={"table_path": self.config.xact_events_table_path},
            )
            return set()

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(
                "Failed to load trace_id cache from xact_events",
                extra={
                    "error": str(e)[:200],
                    "duration_ms": round(duration_ms, 2),
                    "table_path": self.config.xact_events_table_path,
                },
            )
            # Re-raise to let caller handle
            raise

    def get_recent_trace_ids(
        self,
        window_hours: Optional[int] = None,
        force_refresh: bool = False,
    ) -> list[str]:
        """
        Get trace_ids for deduplication from local cache.

        On first call (or force_refresh), loads from Delta table.
        Subsequent calls return cached values.

        Args:
            window_hours: Hours to look back (only used on cache load)
            force_refresh: If True, reload cache from Delta

        Returns:
            List of trace_ids that have been processed recently
        """
        if not self._cache_initialized or force_refresh:
            self._trace_id_cache = self._load_cache_from_delta(window_hours)
            self._cache_initialized = True
            logger.info(
                "Initialized trace_id cache",
                extra={"cache_size": len(self._trace_id_cache)},
            )

        return list(self._trace_id_cache)

    def add_to_cache(self, trace_ids: list[str]) -> int:
        """
        Add newly written trace_ids to the local cache.

        Call this after successfully writing events to Delta.

        Args:
            trace_ids: List of trace_ids that were just written

        Returns:
            Number of new trace_ids added (excludes duplicates)
        """
        if not trace_ids:
            return 0

        before_size = len(self._trace_id_cache)
        self._trace_id_cache.update(trace_ids)
        added = len(self._trace_id_cache) - before_size

        if added > 0:
            logger.debug(
                "Added trace_ids to cache",
                extra={
                    "added": added,
                    "total_size": len(self._trace_id_cache),
                    "duplicates_skipped": len(trace_ids) - added,
                },
            )

        return added

    def get_cache_size(self) -> int:
        """Return current cache size."""
        return len(self._trace_id_cache)

    def is_cache_initialized(self) -> bool:
        """Return whether cache has been loaded from Delta."""
        return self._cache_initialized

    def build_kql_anti_join_filter(
        self,
        trace_ids: list[str],
        batch_index: int = 0,
    ) -> str:
        """
        Build KQL anti-join filter clause.

        Args:
            trace_ids: List of trace_ids to exclude
            batch_index: Batch index for logging (when chunking)

        Returns:
            KQL filter clause: "traceId !in dynamic(['id1', 'id2', ...])"
            or empty string if no trace_ids
        """
        if not trace_ids:
            return ""

        # Escape single quotes in trace_ids
        escaped_ids = [tid.replace("'", "\\'") for tid in trace_ids]

        # Build dynamic array
        id_list = ", ".join(f"'{tid}'" for tid in escaped_ids)

        # Use configurable column name (default: traceId for Eventhouse schema)
        column_name = self.config.eventhouse_trace_id_column
        filter_clause = f"{column_name} !in (dynamic([{id_list}]))"

        logger.debug(
            "Built KQL anti-join filter",
            extra={
                "trace_id_count": len(trace_ids),
                "batch_index": batch_index,
                "filter_length": len(filter_clause),
                "column_name": column_name,
            },
        )

        return filter_clause

    def build_deduped_query(
        self,
        base_table: str,
        poll_from: datetime,
        poll_to: Optional[datetime] = None,
        additional_filters: Optional[str] = None,
        limit: int = 1000,
        order_by: str = "ingestion_time() asc",
    ) -> str:
        """
        Build complete KQL query with time filter and anti-join.

        IMPORTANT: Time filter comes FIRST to limit the scan.

        Args:
            base_table: Source table name in Eventhouse
            poll_from: Start of time window
            poll_to: End of time window (defaults to now)
            additional_filters: Optional additional KQL filter clauses
            limit: Max rows to return (default: 1000)
            order_by: Order clause (default: ingestion_time() asc)

        Returns:
            Complete KQL query string

        Raises:
            ValueError: If trace_ids exceed max batch size
        """
        # Get recent trace_ids from cache (loads from Delta on first call)
        trace_ids = self.get_recent_trace_ids()

        # Check batch size
        if len(trace_ids) > self.config.max_trace_ids_per_query:
            logger.warning(
                "trace_ids exceed max batch size, query may need chunking",
                extra={
                    "trace_id_count": len(trace_ids),
                    "max_per_query": self.config.max_trace_ids_per_query,
                },
            )
            # For now, truncate with warning; WP-503 will handle proper chunking
            trace_ids = trace_ids[: self.config.max_trace_ids_per_query]

        # Build query parts
        query_parts = [base_table]

        # TIME FILTER FIRST - this is critical for performance
        poll_to_dt = poll_to or datetime.now(timezone.utc)

        # Format datetime for KQL
        from_str = poll_from.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_str = poll_to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        query_parts.append(f"| where ingestion_time() >= datetime({from_str})")
        query_parts.append(f"| where ingestion_time() < datetime({to_str})")

        # Anti-join filter (if we have trace_ids)
        anti_join_filter = self.build_kql_anti_join_filter(trace_ids)
        if anti_join_filter:
            query_parts.append(f"| where {anti_join_filter}")

        # Additional filters
        if additional_filters:
            query_parts.append(f"| where {additional_filters}")

        # Order and limit
        query_parts.append(f"| order by {order_by}")
        query_parts.append(f"| take {limit}")

        query = "\n".join(query_parts)

        logger.debug(
            "Built deduped KQL query",
            extra={
                "base_table": base_table,
                "poll_from": from_str,
                "poll_to": to_str,
                "trace_id_count": len(trace_ids),
                "limit": limit,
                "query_length": len(query),
                "cache_initialized": self._cache_initialized,
            },
        )

        return query

    def get_poll_window(
        self,
        last_poll_time: Optional[datetime] = None,
    ) -> tuple[datetime, datetime]:
        """
        Calculate poll time window with overlap.

        Args:
            last_poll_time: Last successful poll timestamp (if any)

        Returns:
            Tuple of (poll_from, poll_to) datetimes
        """
        now = datetime.now(timezone.utc)
        poll_to = now

        if last_poll_time is None:
            # First poll - use configured window
            poll_from = now - timedelta(hours=self.config.eventhouse_query_window_hours)
        else:
            # Subsequent poll - from last poll time with overlap
            poll_from = last_poll_time - timedelta(minutes=self.config.overlap_minutes)

        logger.debug(
            "Calculated poll window",
            extra={
                "poll_from": poll_from.isoformat(),
                "poll_to": poll_to.isoformat(),
                "last_poll_time": (
                    last_poll_time.isoformat() if last_poll_time else None
                ),
                "overlap_minutes": self.config.overlap_minutes,
            },
        )

        return poll_from, poll_to


def get_recent_trace_ids_sync(
    table_path: str,
    hours: int = 24,
    storage_options: Optional[dict] = None,
) -> list[str]:
    """
    Standalone function to get trace_ids from xact_events.

    Uses partition pruning on event_date for efficient file-level filtering.

    Args:
        table_path: Path to xact_events Delta table
        hours: Hours to look back
        storage_options: Azure storage options (defaults to getting from credentials)

    Returns:
        List of trace_ids
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_date = cutoff.date()
    today = datetime.now(timezone.utc).date()

    if storage_options is None:
        storage_options = get_storage_options()

    try:
        return (
            pl.scan_delta(table_path, storage_options=storage_options)
            # Partition filter for file-level pruning
            .filter(pl.col("event_date") >= cutoff_date)
            .filter(pl.col("event_date") <= today)
            # Row-level filter for precise time window
            .filter(pl.col("ingested_at") > cutoff)
            .select("trace_id")
            .unique()
            .collect()["trace_id"]
            .to_list()
        )
    except FileNotFoundError:
        return []
