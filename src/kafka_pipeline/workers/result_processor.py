"""
Result processor worker for batching download results.

Consumes from downloads.results topic and batches results
for efficient Delta table writes.

Features:
- Size-based batching (default: 100 records)
- Timeout-based batching (default: 5 seconds)
- Thread-safe batch accumulation
- Graceful shutdown with pending batch flush
- Writes successful downloads to xact_attachments table
- Writes permanent failures to xact_attachments_failed table (optional)
"""

import asyncio
import json
import time
from typing import List, Optional

from aiokafka.structs import ConsumerRecord

from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.writers.delta_inventory import (
    DeltaInventoryWriter,
    DeltaFailedAttachmentsWriter,
)

logger = get_logger(__name__)


class ResultProcessor:
    """
    Consumes download results and batches for Delta table writes.

    The result processor is the final stage of the pipeline:
    1. Consumes from downloads.results topic
    2. Routes results by status:
       - success → xact_attachments table
       - failed_permanent → xact_attachments_failed table (if configured)
       - failed_transient → skipped (still retrying)
    3. Batches results by size or timeout
    4. Flushes batches to Delta tables

    Batching Strategy:
    - Size flush: When batch reaches BATCH_SIZE (default: 100)
    - Timeout flush: When BATCH_TIMEOUT_SECONDS elapsed (default: 5s)
    - Shutdown flush: Pending batches flushed on graceful shutdown

    Thread Safety:
    - Uses asyncio.Lock for batch accumulation
    - Safe for concurrent message processing

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> processor = ResultProcessor(
        ...     config,
        ...     inventory_table_path="abfss://.../xact_attachments",
        ...     failed_table_path="abfss://.../xact_attachments_failed",
        ... )
        >>> await processor.start()
        >>> # Processor runs until stopped
        >>> await processor.stop()
    """

    # Batching configuration
    BATCH_SIZE = 100
    BATCH_TIMEOUT_SECONDS = 5

    def __init__(
        self,
        config: KafkaConfig,
        inventory_table_path: str,
        failed_table_path: Optional[str] = None,
        batch_size: Optional[int] = None,
        batch_timeout_seconds: Optional[float] = None,
    ):
        """
        Initialize result processor.

        Args:
            config: Kafka configuration
            inventory_table_path: Full abfss:// path to xact_attachments Delta table
            failed_table_path: Optional path to xact_attachments_failed Delta table.
                               If provided, permanent failures will be written here.
            batch_size: Optional custom batch size (default: 100)
            batch_timeout_seconds: Optional custom timeout (default: 5.0)
        """
        self.config = config
        self.batch_size = batch_size or self.BATCH_SIZE
        self.batch_timeout_seconds = batch_timeout_seconds or self.BATCH_TIMEOUT_SECONDS

        # Delta writers
        self._inventory_writer = DeltaInventoryWriter(table_path=inventory_table_path)
        self._failed_writer: Optional[DeltaFailedAttachmentsWriter] = None
        if failed_table_path:
            self._failed_writer = DeltaFailedAttachmentsWriter(table_path=failed_table_path)

        # Batching state - separate batches for success and failed
        self._batch: List[DownloadResultMessage] = []
        self._failed_batch: List[DownloadResultMessage] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()

        # Kafka consumer
        self._consumer = BaseKafkaConsumer(
            config=config,
            topics=[config.downloads_results_topic],
            group_id="xact-result-processor",
            message_handler=self._handle_result,
        )

        # Background flush task
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

        logger.info(
            "Initialized result processor",
            extra={
                "batch_size": self.batch_size,
                "batch_timeout_seconds": self.batch_timeout_seconds,
                "results_topic": config.downloads_results_topic,
                "inventory_table_path": inventory_table_path,
                "failed_table_path": failed_table_path,
                "failed_tracking_enabled": failed_table_path is not None,
            },
        )

    async def start(self) -> None:
        """
        Start the result processor.

        Starts consuming from results topic and begins background flush timer.
        This method runs until stop() is called.

        Raises:
            Exception: If consumer fails to start
        """
        if self._running:
            logger.warning("Result processor already running, ignoring duplicate start")
            return

        logger.info("Starting result processor")
        self._running = True

        # Start background flush task for timeout-based flushing
        self._flush_task = asyncio.create_task(self._periodic_flush())

        try:
            # Start consumer (blocks until stopped)
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("Result processor cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Result processor terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the result processor gracefully.

        Flushes any pending batch before stopping consumer.
        Safe to call multiple times.
        """
        if not self._running:
            logger.debug("Result processor not running or already stopped")
            return

        logger.info("Stopping result processor")
        self._running = False

        try:
            # Cancel periodic flush task
            if self._flush_task and not self._flush_task.done():
                self._flush_task.cancel()
                try:
                    await self._flush_task
                except asyncio.CancelledError:
                    pass

            # Flush any pending batches
            async with self._batch_lock:
                if self._batch:
                    logger.info(
                        "Flushing pending success batch on shutdown",
                        extra={"batch_size": len(self._batch)},
                    )
                    await self._flush_batch()
                if self._failed_batch:
                    logger.info(
                        "Flushing pending failed batch on shutdown",
                        extra={"batch_size": len(self._failed_batch)},
                    )
                    await self._flush_failed_batch()

            # Stop consumer
            await self._consumer.stop()

            logger.info("Result processor stopped successfully")

        except Exception as e:
            logger.error(
                "Error stopping result processor",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

    async def _handle_result(self, message: ConsumerRecord) -> None:
        """
        Handle a single result message.

        Routes results by status:
        - success → inventory batch
        - failed_permanent → failed batch (if tracking enabled)
        - failed_transient → skip (still retrying)

        Triggers flush if size or timeout threshold reached.

        Args:
            message: ConsumerRecord from Kafka

        Raises:
            Exception: If message parsing or batch flush fails
        """
        # Parse message
        try:
            result = DownloadResultMessage.model_validate_json(message.value)
        except Exception as e:
            logger.error(
                "Failed to parse result message",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Route by status
        if result.status == "success":
            # Add to success batch
            async with self._batch_lock:
                self._batch.append(result)

                # Check if flush needed (size-based)
                if len(self._batch) >= self.batch_size:
                    logger.debug(
                        "Success batch size threshold reached, flushing",
                        extra={"batch_size": len(self._batch)},
                    )
                    await self._flush_batch()

        elif result.status == "failed_permanent" and self._failed_writer:
            # Add to failed batch (only if tracking enabled)
            async with self._batch_lock:
                self._failed_batch.append(result)

                # Check if flush needed (size-based)
                if len(self._failed_batch) >= self.batch_size:
                    logger.debug(
                        "Failed batch size threshold reached, flushing",
                        extra={"batch_size": len(self._failed_batch)},
                    )
                    await self._flush_failed_batch()

        else:
            # Skip transient failures (still retrying) or permanent without writer
            logger.debug(
                "Skipping result",
                extra={
                    "trace_id": result.trace_id,
                    "status": result.status,
                    "reason": "transient" if result.status == "failed_transient" else "no_failed_writer",
                    "attachment_url": result.attachment_url[:100],  # truncate for logging
                },
            )

    async def _periodic_flush(self) -> None:
        """
        Background task for timeout-based batch flushing.

        Runs continuously while processor is active.
        Flushes batches when timeout threshold exceeded.
        """
        logger.debug("Starting periodic flush task")

        try:
            while self._running:
                # Sleep for check interval (1 second)
                await asyncio.sleep(1)

                # Check if timeout threshold exceeded
                async with self._batch_lock:
                    elapsed = time.monotonic() - self._last_flush

                    if elapsed >= self.batch_timeout_seconds:
                        # Flush success batch if not empty
                        if self._batch:
                            logger.debug(
                                "Success batch timeout threshold reached, flushing",
                                extra={
                                    "batch_size": len(self._batch),
                                    "elapsed_seconds": elapsed,
                                },
                            )
                            await self._flush_batch()

                        # Flush failed batch if not empty
                        if self._failed_batch:
                            logger.debug(
                                "Failed batch timeout threshold reached, flushing",
                                extra={
                                    "batch_size": len(self._failed_batch),
                                    "elapsed_seconds": elapsed,
                                },
                            )
                            await self._flush_failed_batch()

        except asyncio.CancelledError:
            logger.debug("Periodic flush task cancelled")
            raise
        except Exception as e:
            logger.error(
                "Error in periodic flush task",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

    async def _flush_batch(self) -> None:
        """
        Flush current batch (internal, assumes lock held).

        Converts batch to inventory records and writes to Delta.
        Resets batch and updates flush timestamp.

        Note: This method assumes the caller holds self._batch_lock
        """
        if not self._batch:
            return

        # Snapshot current batch and reset
        batch = self._batch
        self._batch = []
        self._last_flush = time.monotonic()

        batch_size = len(batch)
        logger.info(
            "Flushing inventory batch",
            extra={
                "batch_size": batch_size,
                "first_trace_id": batch[0].trace_id,
                "last_trace_id": batch[-1].trace_id,
            },
        )

        # Write to Delta inventory table
        # Uses asyncio.to_thread internally for non-blocking I/O
        success = await self._inventory_writer.write_results(batch)

        if not success:
            logger.error(
                "Failed to write batch to Delta inventory",
                extra={
                    "batch_size": batch_size,
                    "first_trace_id": batch[0].trace_id,
                    "last_trace_id": batch[-1].trace_id,
                },
            )

    async def _flush_failed_batch(self) -> None:
        """
        Flush current failed batch (internal, assumes lock held).

        Converts failed batch to records and writes to Delta.
        Resets failed batch and updates flush timestamp.

        Note: This method assumes the caller holds self._batch_lock
        """
        if not self._failed_batch or not self._failed_writer:
            return

        # Snapshot current batch and reset
        batch = self._failed_batch
        self._failed_batch = []
        self._last_flush = time.monotonic()

        batch_size = len(batch)
        logger.info(
            "Flushing failed attachments batch",
            extra={
                "batch_size": batch_size,
                "first_trace_id": batch[0].trace_id,
                "last_trace_id": batch[-1].trace_id,
            },
        )

        # Write to Delta failed attachments table
        # Uses asyncio.to_thread internally for non-blocking I/O
        success = await self._failed_writer.write_results(batch)

        if not success:
            logger.error(
                "Failed to write batch to Delta failed attachments table",
                extra={
                    "batch_size": batch_size,
                    "first_trace_id": batch[0].trace_id,
                    "last_trace_id": batch[-1].trace_id,
                },
            )

    @property
    def is_running(self) -> bool:
        """Check if result processor is running."""
        return self._running and self._consumer.is_running


__all__ = [
    "ResultProcessor",
]
