"""
Delta Lake writer for event analytics table.

Writes events to the xact_events Delta table with:
- Flattening of nested JSON structures using verisk_pipeline transform
- Deduplication by trace_id
- Async/non-blocking writes
- Schema compatibility with verisk_pipeline xact_events table

Uses flatten_events() from verisk_pipeline to ensure schema compatibility.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import polars as pl

from verisk_pipeline.storage.delta import DeltaTableWriter
from verisk_pipeline.xact.stages.transform import flatten_events, FLATTENED_SCHEMA

logger = logging.getLogger(__name__)


class DeltaEventsWriter:
    """
    Writer for xact_events Delta table with deduplication and async support.

    Uses flatten_events() from verisk_pipeline to transform raw Eventhouse rows
    into the correct xact_events schema with all 28 columns.

    Features:
    - Flattening of nested JSON using verisk_pipeline transform
    - Deduplication by trace_id within configurable time window
    - Non-blocking writes using asyncio.to_thread
    - Schema compatibility with verisk_pipeline xact_events table

    Input format (raw Eventhouse rows):
        - type: Full event type string (e.g., "verisk.claims.property.xn.documentsReceived")
        - version: Event version
        - utcDateTime: Event timestamp
        - traceId: Trace identifier
        - data: JSON string with nested event data

    Output schema matches verisk_pipeline FLATTENED_SCHEMA:
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

            # Perform write in thread pool to avoid blocking
            await asyncio.to_thread(
                self._delta_writer.append,
                flattened_df,
                dedupe=True,
            )

            logger.info(
                "Successfully wrote events to Delta",
                extra={
                    "event_count": len(raw_events),
                    "columns": len(flattened_df.columns),
                    "table_path": self.table_path,
                },
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to write events to Delta",
                extra={
                    "event_count": len(raw_events),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    async def write_dataframe(self, df: pl.DataFrame) -> bool:
        """
        Write a pre-flattened DataFrame to Delta table.

        Use this when you already have a DataFrame in the correct schema
        (e.g., from a direct Eventhouse query that was already flattened).

        Args:
            df: Polars DataFrame with xact_events schema

        Returns:
            True if write succeeded, False otherwise
        """
        if df.is_empty():
            return True

        try:
            # Add created_at if not present
            if "created_at" not in df.columns:
                now = datetime.now(timezone.utc)
                df = df.with_columns(pl.lit(now).alias("created_at"))

            # Perform write in thread pool to avoid blocking
            await asyncio.to_thread(
                self._delta_writer.append,
                df,
                dedupe=True,
            )

            logger.info(
                "Successfully wrote DataFrame to Delta",
                extra={
                    "row_count": len(df),
                    "table_path": self.table_path,
                },
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to write DataFrame to Delta",
                extra={
                    "row_count": len(df),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    def get_expected_schema(self) -> Dict[str, Any]:
        """
        Get the expected output schema for xact_events table.

        Returns:
            Dict mapping column names to Polars data types
        """
        return FLATTENED_SCHEMA.copy()


__all__ = ["DeltaEventsWriter"]
