"""
Delta Events Worker - Writes events to Delta Lake xact_events table.

This worker consumes events from the events.raw topic and writes them to
the xact_events Delta table for analytics and deduplication tracking.

Separated from EventIngesterWorker to follow single-responsibility principle:
- EventIngesterWorker: Parse events → produce download tasks
- DeltaEventsWorker: Parse events → write to Delta Lake

Features:
- Batch accumulation for efficient Delta writes
- Configurable batch size via delta_events_batch_size
- Optional batch limit for testing via delta_events_max_batches

Consumer group: {prefix}-delta-events
Input topic: events.raw
Output: Delta table xact_events (no Kafka output)
"""

import asyncio
import json
from typing import Any, Dict, List, Optional

from aiokafka.structs import ConsumerRecord

from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.xact.writers import DeltaEventsWriter

logger = get_logger(__name__)


class DeltaEventsWorker:
    """
    Worker to consume events and write them to Delta Lake in batches.

    Processes EventMessage records from the events.raw topic and writes
    them to the xact_events Delta table using the flatten_events() transform.

    This worker runs independently of the EventIngesterWorker, consuming
    from the same topic but with a different consumer group. This allows:
    - Independent scaling of Delta writes vs download task creation
    - Fault isolation between Delta writes and Kafka pipeline
    - Batching optimization for Delta writes

    Features:
    - Batch accumulation for efficient Delta writes
    - Configurable batch size via config.delta_events_batch_size
    - Optional batch limit for testing via config.delta_events_max_batches
    - Deduplication by trace_id within configurable time window
    - Graceful shutdown with pending batch flush
    - Schema compatibility with xact_events table (29 columns)

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> worker = DeltaEventsWorker(config, events_table_path="abfss://...")
        >>> await worker.start()
        >>> # Worker runs until stopped or max_batches reached
        >>> await worker.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        events_table_path: str,
        dedupe_window_hours: int = 24,
    ):
        """
        Initialize Delta events worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings).
                    Also provides delta_events_batch_size and delta_events_max_batches.
            events_table_path: Full abfss:// path to xact_events Delta table
            dedupe_window_hours: Hours to check for duplicate trace_ids (default: 24)
        """
        self.config = config
        self.consumer: Optional[BaseKafkaConsumer] = None
        self.delta_writer: Optional[DeltaEventsWriter] = None

        # Consumer group for delta events writing (separate from event ingester)
        self.consumer_group = f"{config.consumer_group_prefix}-delta-events"

        # Batch configuration
        self.batch_size = config.delta_events_batch_size
        self.max_batches = config.delta_events_max_batches  # None = unlimited

        # Batch state
        self._batch: List[Dict[str, Any]] = []
        self._batches_written = 0
        self._total_events_written = 0

        # Initialize Delta writer
        if events_table_path:
            self.delta_writer = DeltaEventsWriter(
                table_path=events_table_path,
                dedupe_window_hours=dedupe_window_hours,
            )
        else:
            raise ValueError("events_table_path is required for DeltaEventsWorker")

        logger.info(
            "Initialized DeltaEventsWorker",
            extra={
                "consumer_group": self.consumer_group,
                "events_topic": config.events_topic,
                "events_table_path": events_table_path,
                "dedupe_window_hours": dedupe_window_hours,
                "batch_size": self.batch_size,
                "max_batches": self.max_batches,
            },
        )

    async def start(self) -> None:
        """
        Start the delta events worker.

        Initializes consumer and begins consuming events from the events.raw topic.
        Runs until stop() is called or max_batches is reached (if configured).

        Raises:
            Exception: If consumer fails to start
        """
        logger.info(
            "Starting DeltaEventsWorker",
            extra={
                "batch_size": self.batch_size,
                "max_batches": self.max_batches,
            },
        )

        # Create and start consumer with message handler
        self.consumer = BaseKafkaConsumer(
            config=self.config,
            topics=[self.config.events_topic],
            group_id=self.consumer_group,
            message_handler=self._handle_event_message,
        )

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the delta events worker.

        Flushes any pending batch, then gracefully shuts down consumer,
        committing any pending offsets.
        """
        logger.info("Stopping DeltaEventsWorker")

        # Flush any remaining events in the batch
        if self._batch:
            logger.info(
                "Flushing remaining batch on shutdown",
                extra={"batch_size": len(self._batch)},
            )
            await self._flush_batch()

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        logger.info(
            "DeltaEventsWorker stopped successfully",
            extra={
                "batches_written": self._batches_written,
                "total_events_written": self._total_events_written,
            },
        )

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        """
        Process a single event message from Kafka.

        Adds the event to the batch and flushes when batch is full.

        Args:
            record: ConsumerRecord containing EventMessage JSON
        """
        # Check if we've reached max batches limit
        if self.max_batches is not None and self._batches_written >= self.max_batches:
            logger.info(
                "Reached max_batches limit, stopping consumer",
                extra={
                    "max_batches": self.max_batches,
                    "batches_written": self._batches_written,
                },
            )
            # Request consumer to stop
            if self.consumer:
                await self.consumer.stop()
            return

        # Decode message - keep as raw dict, don't convert to EventMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(
                "Failed to parse message JSON",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Add to batch (raw dict with data already as dict)
        self._batch.append(message_data)

        logger.debug(
            "Added event to batch",
            extra={
                "trace_id": message_data.get("traceId"),
                "batch_size": len(self._batch),
                "batch_threshold": self.batch_size,
            },
        )

        # Flush batch if full
        if len(self._batch) >= self.batch_size:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        """
        Write the accumulated batch to Delta Lake.

        Clears the batch after writing and updates counters.
        """
        if not self._batch:
            return

        batch_size = len(self._batch)
        batch_to_write = self._batch
        self._batch = []  # Clear batch immediately to accept new events

        try:
            # Write batch using flatten_events() transformation
            # Data is already in dict format, no JSON parsing needed
            success = await self.delta_writer.write_raw_events(batch_to_write)

            self._batches_written += 1
            if success:
                self._total_events_written += batch_size

            record_delta_write(
                table="xact_events",
                event_count=batch_size,
                success=success,
            )

            logger.info(
                "Flushed batch to Delta",
                extra={
                    "batch_size": batch_size,
                    "batches_written": self._batches_written,
                    "total_events_written": self._total_events_written,
                    "max_batches": self.max_batches,
                    "success": success,
                },
            )

            if not success:
                logger.warning(
                    "Delta batch write failed",
                    extra={"batch_size": batch_size},
                )

        except Exception as e:
            logger.error(
                "Unexpected error writing batch to Delta",
                extra={
                    "batch_size": batch_size,
                    "error": str(e),
                },
                exc_info=True,
            )
            record_delta_write(
                table="xact_events",
                event_count=batch_size,
                success=False,
            )


__all__ = ["DeltaEventsWorker"]
