"""
Delta Lake writer for attachment inventory table.

Writes download results to the xact_attachments Delta table with:
- Idempotency via merge on (trace_id, attachment_url)
- Async/non-blocking writes
- Schema compatibility with verisk_pipeline xact_attachments table

Schema aligned with verisk_pipeline Task.to_tracking_row() for compatibility.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List

import polars as pl

from kafka_pipeline.schemas.results import DownloadResultMessage
from verisk_pipeline.storage.delta import DeltaTableWriter
from verisk_pipeline.xact.xact_models import XACT_PRIMARY_KEYS

logger = logging.getLogger(__name__)


# Schema for xact_attachments table (matches verisk_pipeline)
INVENTORY_SCHEMA = {
    "trace_id": pl.Utf8,
    "attachment_url": pl.Utf8,
    "blob_path": pl.Utf8,
    "status_subtype": pl.Utf8,
    "file_type": pl.Utf8,
    "assignment_id": pl.Utf8,
    "status": pl.Utf8,
    "http_status": pl.Int64,
    "bytes_downloaded": pl.Int64,
    "retry_count": pl.Int64,
    "error_message": pl.Utf8,
    "created_at": pl.Utf8,
    "expires_at": pl.Datetime(time_zone="UTC"),
    "expired_at_ingest": pl.Boolean,
}


class DeltaInventoryWriter:
    """
    Writer for xact_attachments Delta table with idempotency and async support.

    Schema matches verisk_pipeline Task.to_tracking_row() output:
    - trace_id, attachment_url: Primary keys for merge
    - blob_path: Destination path in OneLake
    - status_subtype: Event type suffix (e.g., "documentsReceived")
    - file_type: File extension (e.g., "pdf", "esx")
    - assignment_id: Assignment ID from event
    - status: completed/failed/failed_permanent
    - http_status: HTTP response code
    - bytes_downloaded: Size of downloaded file
    - retry_count: Number of retry attempts
    - error_message: Error description (if failed)
    - created_at: Timestamp of result creation
    - expires_at: URL expiration time (optional)
    - expired_at_ingest: Whether URL was expired at ingest

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
            z_order_columns=["trace_id", "created_at"],
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

        Schema matches verisk_pipeline xact_attachments table:
        - trace_id: str (primary key)
        - attachment_url: str (primary key)
        - blob_path: str
        - status_subtype: str
        - file_type: str
        - assignment_id: str
        - status: str (completed/failed/failed_permanent)
        - http_status: int
        - bytes_downloaded: int
        - retry_count: int
        - error_message: str
        - created_at: str (ISO format)
        - expires_at: datetime (optional)
        - expired_at_ingest: bool (optional)

        Args:
            results: List of DownloadResultMessage objects

        Returns:
            Polars DataFrame with xact_attachments schema
        """
        # Convert to list of dicts using to_tracking_row() for consistency
        rows = [result.to_tracking_row() for result in results]

        # Create DataFrame with explicit schema
        df = pl.DataFrame(rows, schema=INVENTORY_SCHEMA)

        return df


class DeltaFailedAttachmentsWriter:
    """
    Writer for xact_attachments_failed Delta table.

    Records permanent download failures for tracking and potential replay.

    Features:
    - Merge on (trace_id, attachment_url) for idempotency
    - Non-blocking writes using asyncio.to_thread
    - Captures error details for debugging and replay decisions

    Usage:
        >>> writer = DeltaFailedAttachmentsWriter(table_path="abfss://.../xact_attachments_failed")
        >>> await writer.write_results([failed_result1, failed_result2])
    """

    def __init__(self, table_path: str):
        """
        Initialize Delta failed attachments writer.

        Args:
            table_path: Full abfss:// path to xact_attachments_failed Delta table
        """
        self.table_path = table_path

        # Initialize underlying Delta writer with merge support
        self._delta_writer = DeltaTableWriter(
            table_path=table_path,
            z_order_columns=["trace_id", "failed_at"],
        )

        logger.info(
            "Initialized DeltaFailedAttachmentsWriter",
            extra={"table_path": table_path},
        )

    async def write_result(self, result: DownloadResultMessage) -> bool:
        """
        Write a single failed result to Delta table (non-blocking).

        Args:
            result: DownloadResultMessage with failed_permanent status

        Returns:
            True if write succeeded, False otherwise
        """
        return await self.write_results([result])

    async def write_results(self, results: List[DownloadResultMessage]) -> bool:
        """
        Write multiple failed results to Delta table (non-blocking).

        Converts DownloadResultMessage objects to DataFrame and merges into Delta.
        Uses asyncio.to_thread to avoid blocking the event loop.

        Merge strategy:
        - Match on (trace_id, attachment_url) for idempotency
        - UPDATE existing rows with new data (e.g., if replayed and failed again)
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
                "Successfully wrote failed results to Delta",
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
                "Failed to write failed results to Delta",
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
        Convert failed DownloadResultMessage objects to Polars DataFrame.

        Schema for xact_attachments_failed table:
        - trace_id: str
        - attachment_url: str
        - error_message: str
        - error_category: str
        - failed_at: datetime (completed_at from result)
        - processing_time_ms: int
        - created_at: datetime (current UTC time)
        - created_date: date (current UTC date, for partitioning)

        Args:
            results: List of DownloadResultMessage objects

        Returns:
            Polars DataFrame with xact_attachments_failed schema
        """
        # Current time for created_at
        now = datetime.now(timezone.utc)

        # Convert to list of dicts matching table schema
        rows = []
        for result in results:
            row = {
                "trace_id": result.trace_id,
                "attachment_url": result.attachment_url,
                "error_message": result.error_message or "Unknown error",
                "error_category": result.error_category or "unknown",
                "failed_at": result.completed_at,
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
                "error_message": pl.Utf8,
                "error_category": pl.Utf8,
                "failed_at": pl.Datetime(time_zone="UTC"),
                "processing_time_ms": pl.Int64,
                "created_at": pl.Datetime(time_zone="UTC"),
                "created_date": pl.Date,
            },
        )

        return df


__all__ = ["DeltaInventoryWriter", "DeltaFailedAttachmentsWriter"]
