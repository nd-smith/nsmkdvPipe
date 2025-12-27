"""
Result processor worker for batching download results.

Consumes from downloads.results topic and batches successful results
for efficient Delta table writes. Filters out failures (transient and permanent)
as those are handled separately by retry/DLQ infrastructure.

Features:
- Size-based batching (default: 100 records)
- Timeout-based batching (default: 5 seconds)
- Thread-safe batch accumulation
- Graceful shutdown with pending batch flush
- Only processes successful downloads
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.writers.delta_inventory import DeltaInventoryWriter

logger = logging.getLogger(__name__)


class ResultProcessor:
    """
    Consumes download results and batches for Delta inventory writes.

    The result processor is the final stage of the pipeline:
    1. Consumes from downloads.results topic
    2. Filters for successful downloads only
    3. Batches results by size or timeout
    4. Flushes batches for inventory writes (Delta integration in WP-309)

    Batching Strategy:
    - Size flush: When batch reaches BATCH_SIZE (default: 100)
    - Timeout flush: When BATCH_TIMEOUT_SECONDS elapsed (default: 5s)
    - Shutdown flush: Pending batch flushed on graceful shutdown

    Thread Safety:
    - Uses asyncio.Lock for batch accumulation
    - Safe for concurrent message processing

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> processor = ResultProcessor(config)
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
        batch_size: Optional[int] = None,
        batch_timeout_seconds: Optional[float] = None,
    ):
        """
        Initialize result processor.

        Args:
            config: Kafka configuration
            inventory_table_path: Full abfss:// path to xact_attachments Delta table
            batch_size: Optional custom batch size (default: 100)
            batch_timeout_seconds: Optional custom timeout (default: 5.0)
        """
        self.config = config
        self.batch_size = batch_size or self.BATCH_SIZE
        self.batch_timeout_seconds = batch_timeout_seconds or self.BATCH_TIMEOUT_SECONDS

        # Delta inventory writer
        self._inventory_writer = DeltaInventoryWriter(table_path=inventory_table_path)

        # Batching state
        self._batch: List[DownloadResultMessage] = []
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

            # Flush any pending batch
            async with self._batch_lock:
                if self._batch:
                    logger.info(
                        "Flushing pending batch on shutdown",
                        extra={"batch_size": len(self._batch)},
                    )
                    await self._flush_batch()

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

        Filters for successful downloads and adds to batch.
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

        # Filter: only process successful downloads
        if result.status != "success":
            logger.debug(
                "Skipping non-success result",
                extra={
                    "trace_id": result.trace_id,
                    "status": result.status,
                    "attachment_url": result.attachment_url[:100],  # truncate for logging
                },
            )
            return

        # Add to batch with thread-safe accumulation
        async with self._batch_lock:
            self._batch.append(result)

            # Check if flush needed (size-based)
            if len(self._batch) >= self.batch_size:
                logger.debug(
                    "Batch size threshold reached, flushing",
                    extra={"batch_size": len(self._batch)},
                )
                await self._flush_batch()

    async def _periodic_flush(self) -> None:
        """
        Background task for timeout-based batch flushing.

        Runs continuously while processor is active.
        Flushes batch when timeout threshold exceeded.
        """
        logger.debug("Starting periodic flush task")

        try:
            while self._running:
                # Sleep for check interval (1 second)
                await asyncio.sleep(1)

                # Check if timeout threshold exceeded
                async with self._batch_lock:
                    if not self._batch:
                        continue

                    elapsed = time.monotonic() - self._last_flush
                    if elapsed >= self.batch_timeout_seconds:
                        logger.debug(
                            "Batch timeout threshold reached, flushing",
                            extra={
                                "batch_size": len(self._batch),
                                "elapsed_seconds": elapsed,
                            },
                        )
                        await self._flush_batch()

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

    @property
    def is_running(self) -> bool:
        """Check if result processor is running."""
        return self._running and self._consumer.is_running


__all__ = [
    "ResultProcessor",
]
