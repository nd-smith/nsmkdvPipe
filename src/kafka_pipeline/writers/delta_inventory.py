"""
Delta Lake writer for attachment inventory table.

Writes download results to the xact_attachments Delta table with:
- Idempotency via merge on (trace_id, attachment_url)
- Async/non-blocking writes
- Metrics tracking for batch size and latency
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List

import polars as pl

from kafka_pipeline.schemas.results import DownloadResultMessage
from verisk_pipeline.storage.delta import DeltaTableWriter

logger = logging.getLogger(__name__)


class DeltaInventoryWriter:
    """
    Writer for xact_attachments Delta table with idempotency and async support.

    Features:
    - Merge on (trace_id, attachment_url) for idempotency
    - Non-blocking writes using asyncio.to_thread
    - Metrics for batch size and write latency
    - Schema compatibility with existing xact_attachments table

    Usage:
        >>> writer = DeltaInventoryWriter(table_path="abfss://.../xact_attachments")
        >>> await writer.write_results([result1, result2, result3])
    """

    def __init__(self, table_path: str):
        """
        Initialize Delta inventory writer.

        Args:
            table_path: Full abfss:// path to xact_attachments Delta table
        """
        self.table_path = table_path

        # Initialize underlying Delta writer with merge support
        self._delta_writer = DeltaTableWriter(
            table_path=table_path,
            z_order_columns=["trace_id", "downloaded_at"],
        )

        logger.info(
            "Initialized DeltaInventoryWriter",
            extra={"table_path": table_path},
        )

    async def write_result(self, result: DownloadResultMessage) -> bool:
        """
        Write a single download result to Delta table (non-blocking).

        Args:
            result: DownloadResultMessage to write

        Returns:
            True if write succeeded, False otherwise
        """
        return await self.write_results([result])

    async def write_results(self, results: List[DownloadResultMessage]) -> bool:
        """
        Write multiple download results to Delta table (non-blocking).

        Converts DownloadResultMessage objects to DataFrame and merges into Delta.
        Uses asyncio.to_thread to avoid blocking the event loop.

        Merge strategy:
        - Match on (trace_id, attachment_url) for idempotency
        - UPDATE existing rows with new data
        - INSERT new rows that don't match

        Args:
            results: List of DownloadResultMessage objects to write

        Returns:
            True if write succeeded, False otherwise
        """
        if not results:
            return True

        batch_size = len(results)
        start_time = time.monotonic()

        try:
            # Convert results to DataFrame
            df = self._results_to_dataframe(results)

            # Perform merge in thread pool to avoid blocking
            # Merge on (trace_id, attachment_url) for idempotency
            rows_affected = await asyncio.to_thread(
                self._delta_writer.merge,
                df,
                merge_keys=["trace_id", "attachment_url"],
                preserve_columns=["created_at"],
            )

            # Calculate latency metric
            latency_ms = (time.monotonic() - start_time) * 1000

            logger.info(
                "Successfully wrote results to Delta inventory",
                extra={
                    "batch_size": batch_size,
                    "rows_affected": rows_affected,
                    "latency_ms": round(latency_ms, 2),
                    "table_path": self.table_path,
                },
            )
            return True

        except Exception as e:
            latency_ms = (time.monotonic() - start_time) * 1000

            logger.error(
                "Failed to write results to Delta inventory",
                extra={
                    "batch_size": batch_size,
                    "latency_ms": round(latency_ms, 2),
                    "table_path": self.table_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    def _results_to_dataframe(self, results: List[DownloadResultMessage]) -> pl.DataFrame:
        """
        Convert DownloadResultMessage objects to Polars DataFrame.

        Schema matches xact_attachments table:
        - trace_id: str
        - attachment_url: str
        - blob_path: str (destination_path from result)
        - bytes_downloaded: int
        - downloaded_at: datetime (completed_at from result)
        - processing_time_ms: int
        - created_at: datetime (current UTC time)
        - created_date: date (current UTC date)

        Args:
            results: List of DownloadResultMessage objects

        Returns:
            Polars DataFrame with xact_attachments schema
        """
        # Current time for created_at
        now = datetime.now(timezone.utc)

        # Convert to list of dicts matching table schema
        rows = []
        for result in results:
            row = {
                "trace_id": result.trace_id,
                "attachment_url": result.attachment_url,
                "blob_path": result.destination_path,
                "bytes_downloaded": result.bytes_downloaded,
                "downloaded_at": result.completed_at,
                "processing_time_ms": result.processing_time_ms,
                "created_at": now,
                "created_date": now.date(),
            }
            rows.append(row)

        # Create DataFrame with explicit schema
        df = pl.DataFrame(
            rows,
            schema={
                "trace_id": pl.Utf8,
                "attachment_url": pl.Utf8,
                "blob_path": pl.Utf8,
                "bytes_downloaded": pl.Int64,
                "downloaded_at": pl.Datetime(time_zone="UTC"),
                "processing_time_ms": pl.Int64,
                "created_at": pl.Datetime(time_zone="UTC"),
                "created_date": pl.Date,
            },
        )

        return df


__all__ = ["DeltaInventoryWriter"]
