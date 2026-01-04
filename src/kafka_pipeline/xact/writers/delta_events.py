"""
Delta Lake writer for event analytics table.

Writes events to the xact_events Delta table with:
- Flattening of nested JSON structures using xact transform module
- Deduplication by trace_id
- Async/non-blocking writes
- Schema compatibility with legacy xact_events table

Uses flatten_events() from kafka_pipeline.xact.writers.transform.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter
from kafka_pipeline.xact.writers.transform import flatten_events


class DeltaEventsWriter(BaseDeltaWriter):
    """
    Writer for xact_events Delta table with deduplication and async support.

    Uses flatten_events() from kafka_pipeline.xact.writers.transform to transform
    raw Eventhouse rows into the correct xact_events schema with all 28 columns.

    Features:
    - Flattening of nested JSON using xact transform module
    - Deduplication by trace_id within configurable time window
    - Non-blocking writes using asyncio.to_thread
    - Schema compatibility with legacy xact_events table

    Input format (raw Eventhouse rows):
        - type: Full event type string (e.g., "verisk.claims.property.xn.documentsReceived")
        - version: Event version
        - utcDateTime: Event timestamp
        - traceId: Trace identifier
        - data: JSON string with nested event data

    Output schema matches kafka_pipeline.xact.writers.transform.FLATTENED_SCHEMA:
        - type, status_subtype, version, ingested_at, event_date, trace_id
        - description, assignment_id, original_assignment_id, xn_address, carrier_id
        - estimate_version, note, author, sender_reviewer_name, sender_reviewer_email
        - carrier_reviewer_name, carrier_reviewer_email, event_datetime_mdt
        - attachments (comma-joined), claim_number, contact_* fields, raw_json

    Usage:
        >>> writer = DeltaEventsWriter(table_path="abfss://.../xact_events")
        >>> await writer.write_raw_events([{"type": "...", "traceId": "...", ...}])
    """

    def __init__(
        self,
        table_path: str,
        dedupe_window_hours: int = 24,
    ):
        """
        Initialize Delta events writer.

        Args:
            table_path: Full abfss:// path to xact_events Delta table
            dedupe_window_hours: Hours to check for duplicate trace_ids (default: 24)
        """
        # Initialize base class with deduplication on trace_id
        # Use created_at (write time) not ingested_at (source event time) for dedup window
        # This ensures we find recently-written records even when backfilling old events
        super().__init__(
            table_path=table_path,
            dedupe_column="trace_id",
            dedupe_window_hours=dedupe_window_hours,
            timestamp_column="created_at",
        )

        self.dedupe_window_hours = dedupe_window_hours

    async def write_raw_events(self, raw_events: List[Dict[str, Any]]) -> bool:
        """
        Write raw Eventhouse events to Delta table (non-blocking).

        Transforms raw Eventhouse rows using flatten_events() from verisk_pipeline
        to ensure schema compatibility with xact_events table.

        Args:
            raw_events: List of raw event dicts with Eventhouse schema:
                - type: Full event type string
                - version: Event version
                - utcDateTime: Event timestamp
                - traceId: Trace identifier
                - data: JSON string with nested event data

        Returns:
            True if write succeeded, False otherwise
        """
        if not raw_events:
            return True

        try:
            # Create DataFrame from raw events in Eventhouse format
            raw_df = pl.DataFrame(raw_events)

            # Use flatten_events() from verisk_pipeline to transform
            # This ensures schema compatibility with xact_events table
            flattened_df = flatten_events(raw_df)

            # Add created_at column (pipeline processing timestamp)
            now = datetime.now(timezone.utc)
            flattened_df = flattened_df.with_columns(
                pl.lit(now).alias("created_at")
            )

            # Use base class async append method
            success = await self._async_append(flattened_df, dedupe=True)

            if success:
                self.logger.info(
                    "Successfully wrote events to Delta",
                    extra={
                        "event_count": len(raw_events),
                        "columns": len(flattened_df.columns),
                        "table_path": self.table_path,
                    },
                )

            return success

        except Exception as e:
            self.logger.error(
                "Failed to write events to Delta",
                extra={
                    "event_count": len(raw_events),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False


__all__ = ["DeltaEventsWriter"]
