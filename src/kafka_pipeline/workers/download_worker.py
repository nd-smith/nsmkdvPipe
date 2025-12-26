"""
Download worker for processing download tasks.

Consumes DownloadTaskMessage from pending and retry topics,
downloads attachments using AttachmentDownloader, and tracks
processing metrics.

This is the core download worker implementation (WP-304).
Upload integration (WP-305) and error handling (WP-306) are
added in subsequent work packages.
"""

import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Optional

from aiokafka.structs import ConsumerRecord

from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadTask, DownloadOutcome
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)


class DownloadWorker:
    """
    Worker that processes download tasks from Kafka.

    Consumes DownloadTaskMessage from:
    - downloads.pending (new tasks from event ingester)
    - downloads.retry.* (retried tasks with exponential backoff)

    For each task:
    1. Parse DownloadTaskMessage from Kafka
    2. Convert to DownloadTask for AttachmentDownloader
    3. Download attachment to temporary location
    4. Track processing time and outcome
    5. Log result (upload integration in WP-305)

    Usage:
        config = KafkaConfig.from_env()
        worker = DownloadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    CONSUMER_GROUP = "xact-download-worker"

    # Retry topics with exponential backoff delays
    RETRY_TOPICS = [
        "xact.downloads.retry.5m",
        "xact.downloads.retry.10m",
        "xact.downloads.retry.20m",
        "xact.downloads.retry.40m",
    ]

    def __init__(self, config: KafkaConfig, temp_dir: Optional[Path] = None):
        """
        Initialize download worker.

        Args:
            config: Kafka configuration
            temp_dir: Optional directory for temporary downloads (None = system temp)
        """
        self.config = config
        self.temp_dir = temp_dir or Path(tempfile.gettempdir()) / "download_worker"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Build list of topics to consume from
        topics = [config.downloads_pending_topic] + self.RETRY_TOPICS

        # Create consumer with message handler
        self.consumer = BaseKafkaConsumer(
            config=config,
            topics=topics,
            group_id=self.CONSUMER_GROUP,
            message_handler=self._handle_task_message,
        )

        # Create downloader instance (reused across tasks)
        self.downloader = AttachmentDownloader()

        logger.info(
            "Initialized download worker",
            extra={
                "consumer_group": self.CONSUMER_GROUP,
                "topics": topics,
                "temp_dir": str(self.temp_dir),
            },
        )

    async def start(self) -> None:
        """
        Start the download worker.

        Begins consuming messages from pending and retry topics.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer fails to start
        """
        logger.info("Starting download worker")
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the download worker.

        Stops consuming messages and cleans up resources.
        Safe to call multiple times.
        """
        logger.info("Stopping download worker")
        await self.consumer.stop()

    async def _handle_task_message(self, message: ConsumerRecord) -> None:
        """
        Process a single download task message.

        Called by BaseKafkaConsumer for each message consumed from
        pending or retry topics.

        Args:
            message: ConsumerRecord with DownloadTaskMessage as value

        Raises:
            Exception: On processing failures (handled by consumer error routing)
        """
        start_time = time.perf_counter()

        # Parse message value as DownloadTaskMessage
        try:
            task_message = DownloadTaskMessage.model_validate_json(message.value)
        except Exception as e:
            logger.error(
                "Failed to parse DownloadTaskMessage",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        logger.info(
            "Processing download task",
            extra={
                "trace_id": task_message.trace_id,
                "attachment_url": task_message.attachment_url,
                "destination_path": task_message.destination_path,
                "retry_count": task_message.retry_count,
                "topic": message.topic,
            },
        )

        # Convert DownloadTaskMessage to DownloadTask
        download_task = self._convert_to_download_task(task_message)

        # Perform download
        outcome = await self.downloader.download(download_task)

        # Calculate processing time
        processing_time_ms = int((time.perf_counter() - start_time) * 1000)

        # Log outcome (upload integration in WP-305)
        if outcome.success:
            logger.info(
                "Download completed successfully",
                extra={
                    "trace_id": task_message.trace_id,
                    "attachment_url": task_message.attachment_url,
                    "bytes_downloaded": outcome.bytes_downloaded,
                    "content_type": outcome.content_type,
                    "processing_time_ms": processing_time_ms,
                    "local_path": str(outcome.file_path),
                },
            )
            # TODO (WP-305): Upload to OneLake and produce result message
        else:
            logger.warning(
                "Download failed",
                extra={
                    "trace_id": task_message.trace_id,
                    "attachment_url": task_message.attachment_url,
                    "error_message": outcome.error_message,
                    "error_category": outcome.error_category.value if outcome.error_category else None,
                    "status_code": outcome.status_code,
                    "processing_time_ms": processing_time_ms,
                },
            )
            # TODO (WP-306): Route to retry or DLQ based on error_category

    def _convert_to_download_task(self, task_message: DownloadTaskMessage) -> DownloadTask:
        """
        Convert DownloadTaskMessage to DownloadTask for downloader.

        Creates a temporary file path based on destination_path to avoid
        conflicts between concurrent downloads.

        Args:
            task_message: Kafka message with download task details

        Returns:
            DownloadTask configured for AttachmentDownloader
        """
        # Create temporary file path (unique per trace_id)
        # Use destination_path to preserve file extension
        destination_filename = Path(task_message.destination_path).name
        temp_file = self.temp_dir / task_message.trace_id / destination_filename

        return DownloadTask(
            url=task_message.attachment_url,
            destination=temp_file,
            timeout=60,  # TODO: Make configurable
            validate_url=True,
            validate_file_type=True,
            # Use default allowed domains and extensions from security module
            allowed_domains=None,
            allowed_extensions=None,
            max_size=None,  # TODO: Make configurable
        )

    @property
    def is_running(self) -> bool:
        """Check if worker is running and processing messages."""
        return self.consumer.is_running


__all__ = ["DownloadWorker"]
