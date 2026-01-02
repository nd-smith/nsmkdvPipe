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
    1. Query xact_events for recently processed trace_ids (Polars lazy scan)
    2. Build KQL query with anti-join filter (trace_id !in dynamic([...]))
    3. Return query with trace_ids embedded for execution

    This ensures:
    - Time filter is FIRST in KQL (limits scan of Eventhouse table)
    - Anti-join happens IN KQL (not Python)
    - trace_ids query from Delta is time-bounded

    Example:
        >>> dedup = EventhouseDeduplicator(config)
        >>> query = await dedup.build_query_with_anti_join(
        ...     base_table="Events",
        ...     poll_from=datetime.now() - timedelta(hours=1),
        ... )
        >>> # query contains: Events | where ingestion_time() > ... | where trace_id !in dynamic([...])
    """

    def __init__(self, config: DedupConfig):
        """Initialize deduplicator.

        Args:
            config: Deduplication configuration
        """
        self.config = config

    def _get_storage_options(self) -> dict:
        """Get Azure storage options for Delta table access.

        Note: No instance-level caching here - the auth module handles caching
        and will detect when the token file is modified to refresh tokens.
        """
        return get_storage_options()

    def get_recent_trace_ids(
        self,
        window_hours: Optional[int] = None,
    ) -> list[str]:
        """
        Get trace_ids from xact_events within time window.

        Uses Polars lazy scan with predicate pushdown - only reads relevant
        parquet files, not the entire table.

        Args:
            window_hours: Hours to look back (defaults to config.xact_events_window_hours)

        Returns:
            List of trace_ids that have been processed recently
        """
        window = window_hours or self.config.xact_events_window_hours
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window)

        start_time = time.perf_counter()

        try:
            # Use Polars lazy scan with predicate pushdown
            df = (
                pl.scan_delta(
                    self.config.xact_events_table_path,
                    storage_options=self._get_storage_options(),
                )
                .filter(pl.col("ingested_at") > cutoff)  # Pushes down to Delta
                .select("trace_id")  # Only reads one column
                .collect()
            )

            trace_ids = df["trace_id"].to_list()

            duration_ms = (time.perf_counter() - start_time) * 1000

            logger.info(
                "Retrieved recent trace_ids from xact_events",
                extra={
                    "trace_id_count": len(trace_ids),
                    "window_hours": window,
                    "duration_ms": round(duration_ms, 2),
                    "table_path": self.config.xact_events_table_path,
                },
            )

            return trace_ids

        except FileNotFoundError:
            # Table doesn't exist yet - first run, no deduplication needed
            logger.warning(
                "xact_events table not found, skipping deduplication",
                extra={"table_path": self.config.xact_events_table_path},
            )
            return []

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(
                "Failed to retrieve trace_ids from xact_events",
                extra={
                    "error": str(e)[:200],
                    "duration_ms": round(duration_ms, 2),
                    "table_path": self.config.xact_events_table_path,
                },
            )
            # Re-raise to let caller handle
            raise

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
        # Get recent trace_ids
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

    Uses lazy scan with predicate pushdown - only reads relevant parquet files.

    Args:
        table_path: Path to xact_events Delta table
        hours: Hours to look back
        storage_options: Azure storage options (defaults to getting from credentials)

    Returns:
        List of trace_ids
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    if storage_options is None:
        storage_options = get_storage_options()

    try:
        return (
            pl.scan_delta(table_path, storage_options=storage_options)
            .filter(pl.col("ingested_at") > cutoff)
            .select("trace_id")
            .collect()["trace_id"]
            .to_list()
        )
    except FileNotFoundError:
        return []
