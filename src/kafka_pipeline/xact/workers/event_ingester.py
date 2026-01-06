"""
Event Ingester Worker - Consumes events and produces download tasks.

This worker is the entry point to the download pipeline:
1. Consumes EventMessage from events.raw topic
2. Validates attachment URLs against domain allowlist
3. Generates blob storage paths for each attachment
4. Produces DownloadTaskMessage to downloads.pending topic

Note: Delta Lake writes are handled separately by DeltaEventsWorker,
which consumes from the same topic with a different consumer group.

Schema compatibility:
- EventMessage matches verisk_pipeline EventRecord
- DownloadTaskMessage matches verisk_pipeline Task

Consumer group: {prefix}-event-ingester
Input topic: events.raw
Output topic: downloads.pending
"""

import json
from datetime import datetime
from typing import Optional

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.setup import get_logger
from core.paths.resolver import generate_blob_path
from core.security.url_validation import validate_download_url
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.common.security import sanitize_url

logger = get_logger(__name__)


class EventIngesterWorker:
    """
    Worker to consume events and produce download tasks.

    Processes EventMessage records from the events.raw topic, validates
    attachment URLs, generates storage paths, and produces DownloadTaskMessage
    records to the downloads.pending topic for processing by download workers.

    Note: Delta Lake writes are handled by a separate DeltaEventsWorker that
    consumes from the same topic with a different consumer group.

    Features:
    - URL validation with domain allowlist
    - Automatic blob path generation
    - Event deduplication ready (trace_id preservation)
    - Graceful handling of events without attachments
    - Sanitized logging of validation failures

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
        domain: str = "xact",
        producer_config: Optional[KafkaConfig] = None,
    ):
        """
        Initialize event ingester worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings)
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

        logger.info(
            "Initialized EventIngesterWorker",
            extra={
                "domain": domain,
                "worker_name": "event_ingester",
                "events_topic": config.get_topic(domain, "events"),
                "pending_topic": self.producer_config.get_topic(domain, "downloads_pending"),
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
        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="event_ingester",
        )
        await self.producer.start()

        # Create and start consumer with message handler (uses consumer_config)
        self.consumer = BaseKafkaConsumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
        )

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the event ingester worker.

        Gracefully shuts down consumer and producer, committing any pending
        offsets and flushing pending messages.
        """
        logger.info("Stopping EventIngesterWorker")

        # Stop consumer first (stops receiving new messages)
        if self.consumer:
            await self.consumer.stop()

        # Then stop producer (flushes pending messages)
        if self.producer:
            await self.producer.stop()

        logger.info("EventIngesterWorker stopped successfully")

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        """
        Process a single event message from Kafka.

        Parses the EventMessage (matching verisk_pipeline EventRecord schema),
        validates attachments, generates download tasks, and produces them to
        the pending topic.

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
                topic=self.producer_config.get_topic(self.domain, "downloads_pending"),
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


__all__ = ["EventIngesterWorker"]
