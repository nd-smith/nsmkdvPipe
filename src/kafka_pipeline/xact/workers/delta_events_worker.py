"""
Delta Events Worker - Writes events to Delta Lake xact_events table.

This worker consumes events from the events.raw topic and writes them to
the xact_events Delta table for analytics and deduplication tracking.

Separated from EventIngesterWorker to follow single-responsibility principle:
- EventIngesterWorker: Parse events → produce download tasks
- DeltaEventsWorker: Parse events → write to Delta Lake

Consumer group: {prefix}-delta-events
Input topic: events.raw
Output: Delta table xact_events (no Kafka output)
"""

import asyncio
import json
from typing import Optional, Set

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.writers import DeltaEventsWriter

logger = get_logger(__name__)


class DeltaEventsWorker:
    """
    Worker to consume events and write them to Delta Lake.

    Processes EventMessage records from the events.raw topic and writes
    them to the xact_events Delta table using the flatten_events() transform.

    This worker runs independently of the EventIngesterWorker, consuming
    from the same topic but with a different consumer group. This allows:
    - Independent scaling of Delta writes vs download task creation
    - Fault isolation between Delta writes and Kafka pipeline
    - Batching optimization for Delta writes

    Features:
    - Non-blocking Delta writes using asyncio background tasks
    - Deduplication by trace_id within configurable time window
    - Graceful shutdown with pending task completion
    - Schema compatibility with xact_events table (28 columns)

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> worker = DeltaEventsWorker(config, events_table_path="abfss://...")
        >>> await worker.start()
        >>> # Worker runs until stopped
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
            config: Kafka configuration for consumer (topic names, connection settings)
            events_table_path: Full abfss:// path to xact_events Delta table
            dedupe_window_hours: Hours to check for duplicate trace_ids (default: 24)
        """
        self.config = config
        self.consumer: Optional[BaseKafkaConsumer] = None
        self.delta_writer: Optional[DeltaEventsWriter] = None

        # Consumer group for delta events writing (separate from event ingester)
        self.consumer_group = f"{config.consumer_group_prefix}-delta-events"

        # Initialize Delta writer
        if events_table_path:
            self.delta_writer = DeltaEventsWriter(
                table_path=events_table_path,
                dedupe_window_hours=dedupe_window_hours,
            )
        else:
            raise ValueError("events_table_path is required for DeltaEventsWorker")

        # Background task tracking for graceful shutdown
        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0

        logger.info(
            "Initialized DeltaEventsWorker",
            extra={
                "consumer_group": self.consumer_group,
                "events_topic": config.events_topic,
                "events_table_path": events_table_path,
                "dedupe_window_hours": dedupe_window_hours,
            },
        )

    async def start(self) -> None:
        """
        Start the delta events worker.

        Initializes consumer and begins consuming events from the events.raw topic.
        This method runs until stop() is called.

        Raises:
            Exception: If consumer fails to start
        """
        logger.info("Starting DeltaEventsWorker")

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

        Waits for pending background tasks (with timeout), then gracefully
        shuts down consumer, committing any pending offsets.
        """
        logger.info("Stopping DeltaEventsWorker")

        # Wait for pending background tasks with timeout
        await self._wait_for_pending_tasks(timeout_seconds=30)

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        logger.info("DeltaEventsWorker stopped successfully")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[dict] = None,
    ) -> asyncio.Task:
        """
        Create a background task with tracking and lifecycle logging.

        The task is added to _pending_tasks and automatically removed on completion.

        Args:
            coro: Coroutine to run as a task
            task_name: Descriptive name for the task
            context: Optional dict of context info for logging

        Returns:
            The created asyncio.Task
        """
        self._task_counter += 1
        full_name = f"{task_name}-{self._task_counter}"
        context = context or {}

        task = asyncio.create_task(coro, name=full_name)
        self._pending_tasks.add(task)

        logger.debug(
            "Background task created",
            extra={
                "task_name": full_name,
                "pending_tasks": len(self._pending_tasks),
                **context,
            },
        )

        def _on_task_done(t: asyncio.Task) -> None:
            """Callback when task completes."""
            self._pending_tasks.discard(t)

            if t.cancelled():
                logger.debug(
                    "Background task cancelled",
                    extra={"task_name": t.get_name()},
                )
            elif t.exception() is not None:
                exc = t.exception()
                logger.error(
                    "Background task failed",
                    extra={
                        "task_name": t.get_name(),
                        "error": str(exc)[:200],
                        **context,
                    },
                )
            else:
                logger.debug(
                    "Background task completed",
                    extra={"task_name": t.get_name()},
                )

        task.add_done_callback(_on_task_done)
        return task

    async def _wait_for_pending_tasks(self, timeout_seconds: float = 30) -> None:
        """
        Wait for pending background tasks to complete with timeout.

        Args:
            timeout_seconds: Maximum time to wait for tasks (default: 30s)
        """
        if not self._pending_tasks:
            logger.debug("No pending background tasks to wait for")
            return

        pending_count = len(self._pending_tasks)
        task_names = [t.get_name() for t in self._pending_tasks]

        logger.info(
            "Waiting for pending background tasks to complete",
            extra={
                "pending_count": pending_count,
                "task_names": task_names,
                "timeout_seconds": timeout_seconds,
            },
        )

        tasks_to_wait = list(self._pending_tasks)

        try:
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                pending_names = [t.get_name() for t in pending]
                logger.warning(
                    "Cancelling background tasks that did not complete in time",
                    extra={
                        "pending_count": len(pending),
                        "pending_task_names": pending_names,
                    },
                )

                for task in pending:
                    task.cancel()

                await asyncio.gather(*pending, return_exceptions=True)

            completed_count = len(done)
            failed_count = sum(1 for t in done if t.exception() is not None)

            logger.info(
                "Background task cleanup complete",
                extra={
                    "completed": completed_count,
                    "failed": failed_count,
                    "cancelled": len(pending),
                },
            )

        except Exception as e:
            logger.error(
                "Error waiting for pending tasks",
                extra={"error": str(e)[:200]},
                exc_info=True,
            )

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        """
        Process a single event message from Kafka.

        Parses the EventMessage and writes it to Delta Lake.

        Args:
            record: ConsumerRecord containing EventMessage JSON
        """
        # Decode and parse EventMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            event = EventMessage.from_eventhouse_row(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse EventMessage",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        logger.debug(
            "Processing event for Delta write",
            extra={
                "trace_id": event.trace_id,
                "type": event.type,
            },
        )

        # Write event to Delta Lake (non-blocking background task)
        self._create_tracked_task(
            self._write_event_to_delta(event),
            task_name="delta_write",
            context={"trace_id": event.trace_id},
        )

    async def _write_event_to_delta(self, event: EventMessage) -> None:
        """
        Write event to Delta Lake table (background task).

        Uses flatten_events() to transform the event into the correct
        xact_events schema with all 28 columns.

        Args:
            event: EventMessage to write to Delta
        """
        try:
            # Convert to Eventhouse row format for flatten_events()
            raw_event = event.to_eventhouse_row()

            # Write using flatten_events() transformation
            success = await self.delta_writer.write_raw_events([raw_event])
            record_delta_write(
                table="xact_events",
                event_count=1,
                success=success,
            )

            if not success:
                logger.warning(
                    "Delta write failed for event",
                    extra={"trace_id": event.trace_id},
                )

        except Exception as e:
            logger.error(
                "Unexpected error writing event to Delta",
                extra={
                    "trace_id": event.trace_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            record_delta_write(
                table="xact_events",
                event_count=1,
                success=False,
            )


__all__ = ["DeltaEventsWorker"]
