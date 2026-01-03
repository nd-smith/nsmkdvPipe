"""
Delta Lake writer for ClaimX events table.

Writes ClaimX events to the claimx_events Delta table with:
- Deduplication by event_id
- Async/non-blocking writes
- Schema compatibility with verisk_pipeline claimx_events table

Unlike xact events, ClaimX events are not flattened - they maintain
the simple event structure from Eventhouse/webhooks.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

import polars as pl

from kafka_pipeline.common.writers.base import BaseDeltaWriter


class ClaimXEventsDeltaWriter(BaseDeltaWriter):
    """
    Writer for claimx_events Delta table with deduplication and async support.

    ClaimX events have a simpler structure than xact events - no flattening needed.
    Events are written directly from Eventhouse rows to the Delta table.

    Features:
    - Deduplication by event_id within configurable time window
    - Non-blocking writes using asyncio.to_thread
    - Schema compatibility with verisk_pipeline claimx_events table

    Input format (raw Eventhouse rows from ClaimXEventMessage):
        - event_id: Unique event identifier
        - event_type: Event type string (e.g., "PROJECT_CREATED", "PROJECT_FILE_ADDED")
        - project_id: ClaimX project identifier
        - ingested_at: Event ingestion timestamp
        - media_id: Optional media file identifier
        - task_assignment_id: Optional task assignment identifier
        - video_collaboration_id: Optional video collaboration identifier
        - master_file_name: Optional master file name
        - raw_data: Raw event payload (JSON object)

    Output schema:
        Same as input, plus:
        - event_date: Date partition column (from ingested_at)
        - created_at: Pipeline processing timestamp

    Usage:
        >>> writer = ClaimXEventsDeltaWriter(table_path="abfss://.../claimx_events")
        >>> await writer.write_events([{"event_id": "...", "event_type": "...", ...}])
    """

    def __init__(
        self,
        table_path: str,
        dedupe_window_hours: int = 24,
    ):
        """
        Initialize ClaimX events writer.

        Args:
            table_path: Full abfss:// path to claimx_events Delta table
            dedupe_window_hours: Hours to check for duplicate event_ids (default: 24)
        """
        # Initialize base class with deduplication on event_id
        super().__init__(
            table_path=table_path,
            dedupe_column="event_id",
            dedupe_window_hours=dedupe_window_hours,
            timestamp_column="ingested_at",
            partition_column="event_date",
        )

        self.dedupe_window_hours = dedupe_window_hours

    async def write_events(self, events: List[Dict[str, Any]]) -> bool:
        """
        Write ClaimX events to Delta table (non-blocking).

        Args:
            events: List of event dicts from ClaimXEventMessage.model_dump():
                - event_id: Unique event identifier
                - event_type: Event type string
                - project_id: ClaimX project ID
                - ingested_at: Event ingestion timestamp
                - media_id: Optional media file ID
                - task_assignment_id: Optional task assignment ID
                - video_collaboration_id: Optional video collaboration ID
                - master_file_name: Optional master file name
                - raw_data: Optional raw event payload

        Returns:
            True if write succeeded, False otherwise
        """
        if not events:
            return True

        try:
            # Create DataFrame from events
            df = pl.DataFrame(events)

            # Add event_date partition column (from ingested_at)
            df = df.with_columns(
                pl.col("ingested_at").dt.date().alias("event_date")
            )

            # Add created_at column (pipeline processing timestamp)
            now = datetime.now(timezone.utc)
            df = df.with_columns(
                pl.lit(now).alias("created_at")
            )

            # Use base class async append method
            success = await self._async_append(df, dedupe=True)

            if success:
                self.logger.info(
                    "Successfully wrote ClaimX events to Delta",
                    extra={
                        "event_count": len(events),
                        "columns": len(df.columns),
                        "table_path": self.table_path,
                    },
                )

            return success

        except Exception as e:
            self.logger.error(
                "Failed to write ClaimX events to Delta",
                extra={
                    "event_count": len(events),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False


__all__ = ["ClaimXEventsDeltaWriter"]
