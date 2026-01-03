"""
Event Ingester Worker - Consumes events and produces download tasks.

This worker is the entry point to the download pipeline:
1. Consumes EventMessage from events.raw topic
2. Validates attachment URLs against domain allowlist
3. Generates blob storage paths for each attachment
4. Produces DownloadTaskMessage to downloads.pending topic
5. Writes events to Delta Lake (xact_events table) using flatten_events()

Schema compatibility:
- EventMessage matches verisk_pipeline EventRecord
- DownloadTaskMessage matches verisk_pipeline Task
- DeltaEventsWriter uses flatten_events() for xact_events table

Consumer group: {prefix}-event-ingester
Input topic: events.raw
Output topic: downloads.pending
Delta table: xact_events (with all 28 flattened columns)
"""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, Optional, Set

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.setup import get_logger
from core.paths.resolver import generate_blob_path
from core.security.url_validation import validate_download_url
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.writers import DeltaEventsWriter
from verisk_pipeline.common.security import sanitize_url

logger = get_logger(__name__)


class EventIngesterWorker:
    """
    Worker to consume events and produce download tasks.

    Processes EventMessage records from the events.raw topic, validates
    attachment URLs, generates storage paths, and produces DownloadTaskMessage
    records to the downloads.pending topic for processing by download workers.

    Also writes events to Delta Lake xact_events table for analytics and
    deduplication tracking.

    Features:
    - URL validation with domain allowlist
    - Automatic blob path generation
    - Event deduplication ready (trace_id preservation)
    - Graceful handling of events without attachments
    - Sanitized logging of validation failures
    - Non-blocking Delta Lake writes for analytics

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> worker = EventIngesterWorker(config)
        >>> await worker.start()
        >>> # Worker runs until stopped
        >>> await worker.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        enable_delta_writes: bool = True,
        events_table_path: str = "",
        domain: str = "xact",
        producer_config: Optional[KafkaConfig] = None,
    ):
        """
        Initialize event ingester worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings)
            enable_delta_writes: Whether to enable Delta Lake writes (default: True)
            events_table_path: Full abfss:// path to xact_events Delta table
            domain: Domain identifier for OneLake routing (e.g., "xact", "claimx")
            producer_config: Optional separate Kafka config for producer. If not provided,
                uses the consumer config. This is needed when reading from Event Hub
                but writing to local Kafka.
        """
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[BaseKafkaConsumer] = None
        self.delta_writer: Optional[DeltaEventsWriter] = None
        self.enable_delta_writes = enable_delta_writes

        # Consumer group for event ingestion
        self.consumer_group = f"{config.consumer_group_prefix}-event-ingester"

        # Initialize Delta writer if enabled and path provided
        if self.enable_delta_writes and events_table_path:
            self.delta_writer = DeltaEventsWriter(
                table_path=events_table_path,
                dedupe_window_hours=24,
            )
        elif self.enable_delta_writes:
            logger.warning(
                "Delta writes enabled but no events_table_path provided, skipping"
            )

        # Background task tracking for graceful shutdown
        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0  # For unique task naming

        logger.info(
            "Initialized EventIngesterWorker",
            extra={
                "consumer_group": self.consumer_group,
                "events_topic": config.events_topic,
                "pending_topic": self.producer_config.downloads_pending_topic,
                "delta_writes_enabled": self.enable_delta_writes,
                "pipeline_domain": self.domain,
                "separate_producer_config": producer_config is not None,
            },
        )

    @property
    def config(self) -> KafkaConfig:
        """Backward-compatible property returning consumer_config."""
        return self.consumer_config

    async def start(self) -> None:
        """
        Start the event ingester worker.

        Initializes producer and consumer, then begins consuming events
        from the events.raw topic. This method runs until stop() is called.

        Raises:
            Exception: If producer or consumer fails to start
        """
        logger.info("Starting EventIngesterWorker")

        # Start producer first (uses producer_config for local Kafka)
        self.producer = BaseKafkaProducer(self.producer_config)
        await self.producer.start()

        # Create and start consumer with message handler (uses consumer_config)
        self.consumer = BaseKafkaConsumer(
            config=self.consumer_config,
            topics=[self.consumer_config.events_topic],
            group_id=self.consumer_group,
            message_handler=self._handle_event_message,
        )

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the event ingester worker.

        Waits for pending background tasks (with timeout), then gracefully
        shuts down consumer and producer, committing any pending offsets
        and flushing pending messages.
        """
        logger.info("Stopping EventIngesterWorker")

        # Wait for pending background tasks with timeout
        await self._wait_for_pending_tasks(timeout_seconds=30)

        # Stop consumer first (stops receiving new messages)
        if self.consumer:
            await self.consumer.stop()

        # Then stop producer (flushes pending messages)
        if self.producer:
            await self.producer.stop()

        logger.info("EventIngesterWorker stopped successfully")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> asyncio.Task:
        """
        Create a background task with tracking and lifecycle logging.

        The task is added to _pending_tasks and automatically removed on completion.
        Task lifecycle events (creation, completion, error) are logged.

        Args:
            coro: Coroutine to run as a task
            task_name: Descriptive name for the task (e.g., "delta_write")
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
            """Callback when task completes (success, error, or cancelled)."""
            self._pending_tasks.discard(t)

            if t.cancelled():
                logger.debug(
                    "Background task cancelled",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )
            elif t.exception() is not None:
                exc = t.exception()
                logger.error(
                    "Background task failed",
                    extra={
                        "task_name": t.get_name(),
                        "error": str(exc)[:200],
                        "pending_tasks": len(self._pending_tasks),
                        **context,
                    },
                )
            else:
                logger.debug(
                    "Background task completed",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )

        task.add_done_callback(_on_task_done)
        return task

    async def _wait_for_pending_tasks(self, timeout_seconds: float = 30) -> None:
        """
        Wait for pending background tasks to complete with timeout.

        Logs task information and handles cancellation of tasks that
        don't complete within the timeout.

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

        # Copy the set since it may be modified by callbacks during gather
        tasks_to_wait = list(self._pending_tasks)

        try:
            # Wait with timeout
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                # Log and cancel tasks that didn't complete
                pending_names = [t.get_name() for t in pending]
                logger.warning(
                    "Cancelling background tasks that did not complete in time",
                    extra={
                        "pending_count": len(pending),
                        "pending_task_names": pending_names,
                        "timeout_seconds": timeout_seconds,
                    },
                )

                for task in pending:
                    task.cancel()
                    logger.warning(
                        "Cancelled pending task",
                        extra={"task_name": task.get_name()},
                    )

                # Wait briefly for cancellations to propagate
                await asyncio.gather(*pending, return_exceptions=True)

            # Log summary of completed tasks
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

        Parses the EventMessage (matching verisk_pipeline EventRecord schema),
        validates attachments, generates download tasks, and produces them to
        the pending topic. Also writes event to Delta Lake for analytics.

        Args:
            record: ConsumerRecord containing EventMessage JSON

        Raises:
            Exception: If message processing fails (will be handled by consumer error routing)
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

        logger.info(
            "Processing event",
            extra={
                "trace_id": event.trace_id,
                "type": event.type,
                "status_subtype": event.status_subtype,
                "attachment_count": len(event.attachments) if event.attachments else 0,
            },
        )

        # Write event to Delta Lake for analytics (non-blocking)
        # Uses flatten_events() from verisk_pipeline for proper schema
        # Tracked for graceful shutdown
        if self.delta_writer:
            self._create_tracked_task(
                self._write_event_to_delta(event),
                task_name="delta_write",
                context={"trace_id": event.trace_id},
            )

        # Skip events without attachments (many events are just status updates)
        if not event.attachments:
            logger.debug(
                "Event has no attachments, skipping download task creation",
                extra={"trace_id": event.trace_id},
            )
            return

        # Extract assignment_id from data (required for path generation)
        assignment_id = event.assignment_id
        if not assignment_id:
            logger.warning(
                "Event missing assignmentId in data, cannot generate paths",
                extra={
                    "trace_id": event.trace_id,
                    "type": event.type,
                },
            )
            return

        # Process each attachment
        for attachment_url in event.attachments:
            await self._process_attachment(
                event=event,
                attachment_url=attachment_url,
                assignment_id=assignment_id,
            )

    async def _process_attachment(
        self,
        event: EventMessage,
        attachment_url: str,
        assignment_id: str,
    ) -> None:
        """
        Process a single attachment from an event.

        Validates the URL, generates a blob path, creates a download task
        matching verisk_pipeline Task schema, and produces it to pending topic.

        Args:
            event: Source EventMessage (matches verisk_pipeline EventRecord)
            attachment_url: URL of the attachment to download
            assignment_id: Assignment ID for path generation
        """
        # Validate attachment URL
        is_valid, error_message = validate_download_url(attachment_url)
        if not is_valid:
            logger.warning(
                "Invalid attachment URL, skipping",
                extra={
                    "trace_id": event.trace_id,
                    "type": event.type,
                    "url": sanitize_url(attachment_url),
                    "validation_error": error_message,
                },
            )
            return

        # Generate blob storage path (using status_subtype from event type)
        try:
            blob_path, file_type = generate_blob_path(
                status_subtype=event.status_subtype,
                trace_id=event.trace_id,
                assignment_id=assignment_id,
                download_url=attachment_url,
                estimate_version=event.estimate_version,
            )
        except Exception as e:
            logger.error(
                "Failed to generate blob path",
                extra={
                    "trace_id": event.trace_id,
                    "status_subtype": event.status_subtype,
                    "assignment_id": assignment_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Parse original timestamp from event
        original_timestamp = datetime.fromisoformat(
            event.utc_datetime.replace("Z", "+00:00")
        )

        # Create download task message matching verisk_pipeline Task schema
        download_task = DownloadTaskMessage(
            trace_id=event.trace_id,
            attachment_url=attachment_url,
            blob_path=blob_path,
            status_subtype=event.status_subtype,
            file_type=file_type,
            assignment_id=assignment_id,
            estimate_version=event.estimate_version,
            retry_count=0,
            event_type=self.domain,  # Use configured domain for OneLake routing
            event_subtype=event.status_subtype,
            original_timestamp=original_timestamp,
        )

        # Produce download task to pending topic
        try:
            metadata = await self.producer.send(
                topic=self.producer_config.downloads_pending_topic,
                key=event.trace_id,
                value=download_task,
                headers={"trace_id": event.trace_id},
            )

            logger.info(
                "Created download task",
                extra={
                    "trace_id": event.trace_id,
                    "blob_path": blob_path,
                    "status_subtype": event.status_subtype,
                    "file_type": file_type,
                    "assignment_id": assignment_id,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to produce download task",
                extra={
                    "trace_id": event.trace_id,
                    "blob_path": blob_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

    async def _write_event_to_delta(self, event: EventMessage) -> None:
        """
        Write event to Delta Lake table (background task).

        Uses flatten_events() from verisk_pipeline to transform the event
        into the correct xact_events schema with all 28 columns.

        This method runs as a background task and doesn't block Kafka processing.
        Failures are logged but don't affect the main event processing flow.

        Args:
            event: EventMessage to write to Delta (matches EventRecord schema)
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
            # Catch all exceptions to prevent background task from crashing
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


__all__ = ["EventIngesterWorker"]
