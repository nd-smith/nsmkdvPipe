"""
ClaimX Delta Events Worker - Writes events to Delta Lake claimx_events table.

This worker consumes events from the claimx events.raw topic and writes them to
the claimx_events Delta table for analytics.

Separated from ClaimXEventIngesterWorker to follow single-responsibility principle:
- ClaimXEventIngesterWorker: Parse events -> produce enrichment tasks
- ClaimXDeltaEventsWorker: Parse events -> write to Delta Lake

Features:
- Batch accumulation for efficient Delta writes
- Configurable batch size via delta_events_batch_size
- Configurable flush timeout to ensure pending events are written
- Optional batch limit for testing via delta_events_max_batches
- Retry via Kafka topics with exponential backoff

Consumer group: {prefix}-delta-events
Input topic: claimx.events.raw
Output: Delta table claimx_events (no Kafka output)
Retry topics: claimx-delta-events.retry.{delay}m
DLQ topic: claimx-delta-events.dlq
"""

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.writers import ClaimXEventsDeltaWriter

logger = get_logger(__name__)


class ClaimXDeltaEventsWorker:
    """
    Worker to consume ClaimX events and write them to Delta Lake in batches.

    Processes ClaimXEventMessage records from the claimx events.raw topic and writes
    them to the claimx_events Delta table using the ClaimXEventsDeltaWriter.

    This worker runs independently of the ClaimXEventIngesterWorker, consuming
    from the same topic but with a different consumer group. This allows:
    - Independent scaling of Delta writes vs enrichment task creation
    - Fault isolation between Delta writes and Kafka pipeline
    - Batching optimization for Delta writes

    Features:
    - Batch accumulation for efficient Delta writes
    - Configurable batch size via config.delta_events_batch_size
    - Configurable flush timeout for low-traffic periods
    - Optional batch limit for testing via config.delta_events_max_batches
    - Graceful shutdown with pending batch flush
    - Failed batches route to Kafka retry topics
    - Deduplication handled by daily Fabric maintenance job

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>> worker = ClaimXDeltaEventsWorker(
        ...     config=config,
        ...     producer=producer,
        ...     events_table_path="abfss://..."
        ... )
        >>> await worker.start()
    """

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = 30

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        events_table_path: str,
        domain: str = "claimx",
    ):
        """
        Initialize ClaimX Delta events worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings).
                    Also provides delta_events_batch_size and delta_events_max_batches.
            producer: Kafka producer for retry topic routing (required).
            events_table_path: Full abfss:// path to claimx_events Delta table
            domain: Domain identifier (default: "claimx")
        """
        self.config = config
        self.domain = domain
        self.events_table_path = events_table_path
        self.consumer: Optional[BaseKafkaConsumer] = None
        self.producer = producer

        # Batch configuration - use worker-specific config
        processing_config = config.get_worker_config(domain, "delta_events_writer", "processing")
        self.batch_size = processing_config.get("batch_size", 100)
        self.max_batches = processing_config.get("max_batches")  # None = unlimited

        # Flush timeout configuration - flush pending events after N seconds
        self._flush_timeout_seconds = processing_config.get("flush_timeout_seconds", 120)

        # Retry configuration from worker processing settings
        self._retry_delays = processing_config.get("retry_delays", [300, 600, 1200, 2400])
        self._retry_topic_prefix = processing_config.get(
            "retry_topic_prefix", "claimx-delta-events.retry"
        )
        self._dlq_topic = processing_config.get("dlq_topic", "claimx-delta-events.dlq")

        # Batch state
        self._batch: List[Dict[str, Any]] = []
        self._batches_written = 0
        self._total_events_written = 0
        self._last_flush_time = time.monotonic()  # Track last flush for timeout

        # Cycle output tracking
        self._events_received = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None
        self._running = False

        # Initialize Delta writer
        if not events_table_path:
            raise ValueError("events_table_path is required for ClaimXDeltaEventsWorker")

        self.delta_writer = ClaimXEventsDeltaWriter(
            table_path=events_table_path,
        )

        logger.info(
            "Initialized ClaimXDeltaEventsWorker",
            extra={
                "domain": domain,
                "worker_name": "delta_events_writer",
                "consumer_group": config.get_consumer_group(domain, "delta_events_writer"),
                "events_topic": config.get_topic(domain, "events"),
                "events_table_path": events_table_path,
                "batch_size": self.batch_size,
                "max_batches": self.max_batches,
                "flush_timeout_seconds": self._flush_timeout_seconds,
                "retry_delays": self._retry_delays,
                "retry_topic_prefix": self._retry_topic_prefix,
                "dlq_topic": self._dlq_topic,
            },
        )

    async def start(self) -> None:
        """
        Start the ClaimX delta events worker.

        Initializes consumer and begins consuming events from the events.raw topic.
        Runs until stop() is called or max_batches is reached (if configured).

        Raises:
            Exception: If consumer fails to start
        """
        logger.info(
            "Starting ClaimXDeltaEventsWorker",
            extra={
                "batch_size": self.batch_size,
                "max_batches": self.max_batches,
                "flush_timeout_seconds": self._flush_timeout_seconds,
            },
        )
        self._running = True

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Create and start consumer with message handler
        # Disable per-message commits - we commit after batch writes to ensure
        # offsets are only committed after data is durably written to Delta Lake
        self.consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name="delta_events_writer",
            topics=[self.config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
            enable_message_commit=False,
        )

        try:
            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the ClaimX delta events worker.

        Flushes any pending batch, then gracefully shuts down consumer,
        committing any pending offsets.
        """
        logger.info("Stopping ClaimXDeltaEventsWorker")
        self._running = False

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

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
            "ClaimXDeltaEventsWorker stopped successfully",
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
            record: ConsumerRecord containing ClaimXEventMessage JSON
        """
        # Track events received for cycle output
        self._events_received += 1

        # Check if we've reached max batches limit
        if self.max_batches is not None and self._batches_written >= self.max_batches:
            logger.info(
                "Reached max_batches limit, stopping consumer",
                extra={
                    "max_batches": self.max_batches,
                    "batches_written": self._batches_written,
                },
            )
            if self.consumer:
                await self.consumer.stop()
            return

        # Decode and parse message
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            # Parse as ClaimXEventMessage to validate, then convert to dict
            event = ClaimXEventMessage.from_eventhouse_row(message_data)
            event_dict = event.model_dump()
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimX message",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Add to batch
        self._batch.append(event_dict)

        logger.debug(
            "Added ClaimX event to batch",
            extra={
                "event_id": event.event_id,
                "batch_size": len(self._batch),
                "batch_threshold": self.batch_size,
            },
        )

        # Flush batch if full
        if len(self._batch) >= self.batch_size:
            await self._flush_batch()

    async def _flush_batch(self, timeout_flush: bool = False) -> None:
        """
        Write the accumulated batch to Delta Lake.

        On success: clears batch and updates counters.
        On failure: routes batch to Kafka retry topic.

        Args:
            timeout_flush: If True, this flush was triggered by flush timeout
                          rather than reaching batch_size
        """
        if not self._batch:
            return

        # Generate short batch ID for log correlation
        batch_id = uuid.uuid4().hex[:8]
        batch_size = len(self._batch)
        batch_to_write = self._batch
        self._batch = []  # Clear immediately to accept new events

        success = await self._write_batch(batch_to_write, batch_id)

        if success:
            self._batches_written += 1
            self._total_events_written += batch_size
            self._last_flush_time = time.monotonic()  # Track successful flush time

            # Commit offsets after successful Delta write
            # This ensures at-least-once semantics: offsets are only committed
            # after data is durably written to Delta Lake
            if self.consumer:
                await self.consumer.commit()

            # Build progress message with timeout indicator
            if self.max_batches:
                progress = f"Batch {self._batches_written}/{self.max_batches}"
            else:
                progress = f"Batch {self._batches_written}"

            flush_reason = " (timeout flush)" if timeout_flush else ""

            logger.info(
                f"{progress}: Successfully wrote {batch_size} ClaimX events to Delta{flush_reason}",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "batches_written": self._batches_written,
                    "total_events_written": self._total_events_written,
                    "max_batches": self.max_batches,
                    "timeout_flush": timeout_flush,
                },
            )

            # Log to console when a timeout flush happens
            if timeout_flush:
                print(
                    f"[ClaimXDeltaEventsWorker] Timeout flush: "
                    f"wrote {batch_size} pending events to Delta"
                )

            # Stop immediately if we've reached max_batches
            if self.max_batches and self._batches_written >= self.max_batches:
                logger.info(
                    "Reached max_batches limit, stopping consumer",
                    extra={
                        "batch_id": batch_id,
                        "max_batches": self.max_batches,
                        "batches_written": self._batches_written,
                    },
                )
                if self.consumer:
                    await self.consumer.stop()
        else:
            # Route to Kafka retry topic
            event_ids = [
                e.get("event_id") for e in batch_to_write[:10] if e.get("event_id")
            ]
            logger.warning(
                "ClaimX batch write failed, routing to retry topic",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "event_ids": event_ids,
                },
            )
            await self._route_to_retry(batch_to_write, batch_id)

    async def _write_batch(self, batch: List[Dict[str, Any]], batch_id: str) -> bool:
        """
        Attempt to write a batch to Delta Lake.

        Args:
            batch: List of event dictionaries to write
            batch_id: Short identifier for log correlation

        Returns:
            True if write succeeded, False otherwise
        """
        batch_size = len(batch)

        try:
            success = await self.delta_writer.write_events(batch)

            record_delta_write(
                table="claimx_events",
                event_count=batch_size,
                success=success,
            )

            return success

        except Exception as e:
            event_ids = [evt.get("event_id") for evt in batch[:10] if evt.get("event_id")]
            logger.error(
                "Unexpected error writing ClaimX batch to Delta",
                extra={
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "error": str(e),
                    "event_ids": event_ids,
                },
                exc_info=True,
            )
            record_delta_write(
                table="claimx_events",
                event_count=batch_size,
                success=False,
            )
            return False

    async def _route_to_retry(
        self,
        batch: List[Dict[str, Any]],
        batch_id: str,
        retry_count: int = 0,
    ) -> None:
        """
        Route failed batch to retry topic.

        Args:
            batch: List of event dictionaries that failed
            batch_id: Short identifier for log correlation
            retry_count: Current retry count (0-indexed)
        """
        if retry_count >= len(self._retry_delays):
            # Exceeded max retries, route to DLQ
            logger.error(
                "ClaimX batch exceeded max retries, routing to DLQ",
                extra={
                    "batch_id": batch_id,
                    "batch_size": len(batch),
                    "retry_count": retry_count,
                    "max_retries": len(self._retry_delays),
                },
            )
            topic = self._dlq_topic
        else:
            # Route to appropriate retry topic
            delay_seconds = self._retry_delays[retry_count]
            delay_minutes = delay_seconds // 60
            topic = f"{self._retry_topic_prefix}.{delay_minutes}m"

        try:
            # Serialize batch for retry
            retry_message = {
                "batch_id": batch_id,
                "retry_count": retry_count + 1,
                "events": batch,
            }
            await self.producer.send(
                topic=topic,
                key=batch_id,
                value=retry_message,
            )
            logger.info(
                "Routed ClaimX failed batch to retry topic",
                extra={
                    "batch_id": batch_id,
                    "batch_size": len(batch),
                    "retry_topic": topic,
                    "retry_count": retry_count + 1,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to route ClaimX batch to retry topic",
                extra={
                    "batch_id": batch_id,
                    "batch_size": len(batch),
                    "retry_topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging and flush timeout checking.

        Logs processing statistics at regular intervals for operational visibility.
        Also checks if pending events should be flushed due to timeout.
        """
        logger.info(
            "Cycle 0: events=0, batches_written=0, pending=0 [cycle output every %ds, flush timeout %ds]",
            self.CYCLE_LOG_INTERVAL_SECONDS,
            self._flush_timeout_seconds,
        )
        self._last_cycle_log = time.monotonic()

        try:
            while self._running:
                await asyncio.sleep(1)

                # Check for flush timeout - if we have pending events and haven't flushed recently
                if self._batch:
                    time_since_last_flush = time.monotonic() - self._last_flush_time
                    if time_since_last_flush >= self._flush_timeout_seconds:
                        pending_count = len(self._batch)
                        logger.info(
                            f"Flush timeout reached ({self._flush_timeout_seconds}s), "
                            f"flushing {pending_count} pending ClaimX events",
                            extra={
                                "pending_count": pending_count,
                                "time_since_last_flush": time_since_last_flush,
                                "flush_timeout_seconds": self._flush_timeout_seconds,
                            },
                        )
                        print(
                            f"[ClaimXDeltaEventsWorker] Flush timeout: {pending_count} events pending "
                            f"for {time_since_last_flush:.0f}s, triggering flush"
                        )
                        await self._flush_batch(timeout_flush=True)

                # Log cycle output at regular intervals
                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= self.CYCLE_LOG_INTERVAL_SECONDS:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    logger.info(
                        f"Cycle {self._cycle_count}: events={self._events_received}, "
                        f"batches_written={self._batches_written}, pending={len(self._batch)}",
                        extra={
                            "cycle": self._cycle_count,
                            "events_received": self._events_received,
                            "batches_written": self._batches_written,
                            "total_events_written": self._total_events_written,
                            "pending_batch_size": len(self._batch),
                            "cycle_interval_seconds": self.CYCLE_LOG_INTERVAL_SECONDS,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXDeltaEventsWorker"]
