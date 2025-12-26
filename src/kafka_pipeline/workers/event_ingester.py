"""
Event Ingester Worker - Consumes events and produces download tasks.

This worker is the entry point to the download pipeline:
1. Consumes EventMessage from events.raw topic
2. Validates attachment URLs against domain allowlist
3. Generates blob storage paths for each attachment
4. Produces DownloadTaskMessage to downloads.pending topic

Consumer group: {prefix}-event-ingester
Input topic: events.raw
Output topic: downloads.pending
"""

import json
import logging
from typing import Optional

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.paths.resolver import generate_blob_path
from core.security.url_validation import validate_download_url
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.producer import BaseKafkaProducer
from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from verisk_pipeline.common.security import sanitize_url

logger = logging.getLogger(__name__)


class EventIngesterWorker:
    """
    Worker to consume events and produce download tasks.

    Processes EventMessage records from the events.raw topic, validates
    attachment URLs, generates storage paths, and produces DownloadTaskMessage
    records to the downloads.pending topic for processing by download workers.

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

    def __init__(self, config: KafkaConfig):
        """
        Initialize event ingester worker.

        Args:
            config: Kafka configuration with topic names and connection settings
        """
        self.config = config
        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[BaseKafkaConsumer] = None

        # Consumer group for event ingestion
        self.consumer_group = f"{config.consumer_group_prefix}-event-ingester"

        logger.info(
            "Initialized EventIngesterWorker",
            extra={
                "consumer_group": self.consumer_group,
                "events_topic": config.events_topic,
                "pending_topic": config.downloads_pending_topic,
            },
        )

    async def start(self) -> None:
        """
        Start the event ingester worker.

        Initializes producer and consumer, then begins consuming events
        from the events.raw topic. This method runs until stop() is called.

        Raises:
            Exception: If producer or consumer fails to start
        """
        logger.info("Starting EventIngesterWorker")

        # Start producer first
        self.producer = BaseKafkaProducer(self.config)
        await self.producer.start()

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

        Parses the EventMessage, validates attachments, generates download tasks,
        and produces them to the pending topic.

        Args:
            record: ConsumerRecord containing EventMessage JSON

        Raises:
            Exception: If message processing fails (will be handled by consumer error routing)
        """
        # Decode and parse EventMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            event = EventMessage(**message_data)
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
                "event_type": event.event_type,
                "event_subtype": event.event_subtype,
                "attachment_count": len(event.attachments) if event.attachments else 0,
            },
        )

        # Skip events without attachments
        if not event.attachments:
            logger.debug(
                "Event has no attachments, skipping",
                extra={"trace_id": event.trace_id},
            )
            return

        # Extract assignment_id from payload (required for path generation)
        assignment_id = event.payload.get("assignment_id")
        if not assignment_id:
            logger.warning(
                "Event missing assignment_id in payload, cannot generate paths",
                extra={
                    "trace_id": event.trace_id,
                    "event_type": event.event_type,
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

        Validates the URL, generates a blob path, creates a download task,
        and produces it to the pending topic.

        Args:
            event: Source EventMessage
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
                    "event_type": event.event_type,
                    "url": sanitize_url(attachment_url),
                    "validation_error": error_message,
                },
            )
            return

        # Generate blob storage path
        try:
            blob_path, file_type = generate_blob_path(
                status_subtype=event.event_subtype,
                trace_id=event.trace_id,
                assignment_id=assignment_id,
                download_url=attachment_url,
                estimate_version=event.payload.get("estimate_version"),
            )
        except Exception as e:
            logger.error(
                "Failed to generate blob path",
                extra={
                    "trace_id": event.trace_id,
                    "event_subtype": event.event_subtype,
                    "assignment_id": assignment_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Create download task message
        download_task = DownloadTaskMessage(
            trace_id=event.trace_id,
            attachment_url=attachment_url,
            destination_path=blob_path,
            event_type=event.event_type,
            event_subtype=event.event_subtype,
            retry_count=0,
            original_timestamp=event.timestamp,
            metadata={
                "assignment_id": assignment_id,
                "file_type": file_type,
                "source_system": event.source_system,
            },
        )

        # Produce download task to pending topic
        try:
            metadata = await self.producer.send(
                topic=self.config.downloads_pending_topic,
                key=event.trace_id,
                value=download_task,
                headers={"trace_id": event.trace_id},
            )

            logger.info(
                "Created download task",
                extra={
                    "trace_id": event.trace_id,
                    "destination_path": blob_path,
                    "file_type": file_type,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to produce download task",
                extra={
                    "trace_id": event.trace_id,
                    "destination_path": blob_path,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise


__all__ = ["EventIngesterWorker"]
