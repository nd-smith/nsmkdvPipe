"""
Delta Lake writer for event analytics table.

Writes EventMessage records to the xact_events Delta table with:
- Deduplication by trace_id
- Async/non-blocking writes
- Metrics tracking
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional

import polars as pl
from pydantic import BaseModel

from kafka_pipeline.schemas.events import EventMessage
from verisk_pipeline.storage.delta import DeltaTableWriter

logger = logging.getLogger(__name__)


class DeltaEventsWriter:
    """
    Writer for xact_events Delta table with deduplication and async support.

    Features:
    - Deduplication by trace_id within configurable time window
    - Non-blocking writes using asyncio.to_thread
    - Metrics for write success/failure
    - Schema compatibility with existing xact_events table

    Usage:
        >>> writer = DeltaEventsWriter(table_path="abfss://.../xact_events")
        >>> await writer.write_event(event_message)
        >>> # Or batch writes
        >>> await writer.write_events([event1, event2, event3])
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
        self.table_path = table_path
        self.dedupe_window_hours = dedupe_window_hours

        # Initialize underlying Delta writer with deduplication on trace_id
        self._delta_writer = DeltaTableWriter(
            table_path=table_path,
            dedupe_column="trace_id",
            dedupe_window_hours=dedupe_window_hours,
            timestamp_column="ingested_at",
        )

        logger.info(
            "Initialized DeltaEventsWriter",
            extra={
                "table_path": table_path,
                "dedupe_window_hours": dedupe_window_hours,
            },
        )

    async def write_event(self, event: EventMessage) -> bool:
        """
        Write a single event to Delta table (non-blocking).

        Args:
            event: EventMessage to write

        Returns:
            True if write succeeded, False otherwise
        """
        return await self.write_events([event])

    async def write_events(self, events: List[EventMessage]) -> bool:
        """
        Write multiple events to Delta table (non-blocking).

        Converts EventMessage objects to DataFrame and writes to Delta.
        Uses asyncio.to_thread to avoid blocking the event loop.

        Args:
            events: List of EventMessage objects to write

        Returns:
            True if write succeeded, False otherwise
        """
        if not events:
            return True

        try:
            # Convert events to DataFrame
            df = self._events_to_dataframe(events)

            # Perform write in thread pool to avoid blocking
            await asyncio.to_thread(
                self._delta_writer.append,
                df,
                dedupe=True,
            )

            logger.info(
                "Successfully wrote events to Delta",
                extra={
                    "event_count": len(events),
                    "table_path": self.table_path,
                },
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to write events to Delta",
                extra={
                    "event_count": len(events),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    def _events_to_dataframe(self, events: List[EventMessage]) -> pl.DataFrame:
        """
        Convert EventMessage objects to Polars DataFrame.

        Schema matches xact_events table:
        - trace_id: str
        - event_type: str
        - event_subtype: str (optional)
        - source_system: str
        - timestamp: datetime (event timestamp)
        - ingested_at: datetime (current UTC time)
        - attachments: list[str] (optional)
        - payload: dict
        - metadata: dict (optional)

        Args:
            events: List of EventMessage objects

        Returns:
            Polars DataFrame with xact_events schema
        """
        # Current time for ingested_at
        now = datetime.now(timezone.utc)

        # Convert to list of dicts matching table schema
        rows = []
        for event in events:
            row = {
                "trace_id": event.trace_id,
                "event_type": event.event_type,
                "event_subtype": event.event_subtype,
                "source_system": event.source_system,
                "timestamp": event.timestamp,
                "ingested_at": now,
                "attachments": event.attachments or [],
                "payload": event.payload or {},
                "metadata": event.metadata or {},
            }
            rows.append(row)

        # Create DataFrame with explicit schema
        df = pl.DataFrame(
            rows,
            schema={
                "trace_id": pl.Utf8,
                "event_type": pl.Utf8,
                "event_subtype": pl.Utf8,
                "source_system": pl.Utf8,
                "timestamp": pl.Datetime(time_zone="UTC"),
                "ingested_at": pl.Datetime(time_zone="UTC"),
                "attachments": pl.List(pl.Utf8),
                "payload": pl.Struct,
                "metadata": pl.Struct,
            },
        )

        return df


__all__ = ["DeltaEventsWriter"]
