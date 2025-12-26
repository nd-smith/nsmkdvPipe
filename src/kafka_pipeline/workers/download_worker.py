"""
Download worker for processing download tasks.

Consumes DownloadTaskMessage from pending and retry topics,
downloads attachments using AttachmentDownloader, uploads to
OneLake, and produces result messages.

This implementation includes:
- WP-304: Core download processing
- WP-305: OneLake upload and result production
- WP-306: Error handling (to be added)
"""

import logging
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from aiokafka.structs import ConsumerRecord

from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadTask, DownloadOutcome
from core.errors.exceptions import CircuitOpenError
from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.producer import BaseKafkaProducer
from kafka_pipeline.retry.handler import RetryHandler
from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.storage import OneLakeClient

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

        Raises:
            ValueError: If ONELAKE_BASE_PATH is not configured
        """
        self.config = config
        self.temp_dir = temp_dir or Path(tempfile.gettempdir()) / "download_worker"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Validate OneLake configuration
        if not config.onelake_base_path:
            raise ValueError(
                "ONELAKE_BASE_PATH environment variable is required for download worker"
            )

        # Build list of topics to consume from
        topics = [config.downloads_pending_topic] + self.RETRY_TOPICS

        # Create consumer with message handler
        self.consumer = BaseKafkaConsumer(
            config=config,
            topics=topics,
            group_id=self.CONSUMER_GROUP,
            message_handler=self._handle_task_message,
        )

        # Create producer for result messages
        self.producer = BaseKafkaProducer(config=config)

        # Create downloader instance (reused across tasks)
        self.downloader = AttachmentDownloader()

        # Create OneLake client (lazy initialized in start())
        self.onelake_client: Optional[OneLakeClient] = None

        # Create retry handler for error routing (lazy initialized in start())
        self.retry_handler: Optional[RetryHandler] = None

        logger.info(
            "Initialized download worker",
            extra={
                "consumer_group": self.CONSUMER_GROUP,
                "topics": topics,
                "temp_dir": str(self.temp_dir),
                "onelake_base_path": config.onelake_base_path,
            },
        )

    async def start(self) -> None:
        """
        Start the download worker.

        Begins consuming messages from pending and retry topics.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer or producer fails to start
        """
        logger.info("Starting download worker")

        # Start producer
        await self.producer.start()

        # Initialize retry handler (requires producer to be started)
        self.retry_handler = RetryHandler(self.config, self.producer)

        # Initialize OneLake client
        self.onelake_client = OneLakeClient(self.config.onelake_base_path)
        await self.onelake_client.__aenter__()

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the download worker.

        Stops consuming messages and cleans up resources.
        Safe to call multiple times.
        """
        logger.info("Stopping download worker")

        # Stop consumer first (stops message processing)
        await self.consumer.stop()

        # Stop producer
        await self.producer.stop()

        # Close OneLake client
        if self.onelake_client is not None:
            await self.onelake_client.close()
            self.onelake_client = None

        # Clear retry handler reference
        self.retry_handler = None

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

        # Handle outcome: upload and produce result
        if outcome.success:
            await self._handle_success(task_message, outcome, processing_time_ms)
        else:
            await self._handle_failure(task_message, outcome, processing_time_ms)

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

    async def _handle_success(
        self,
        task_message: DownloadTaskMessage,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """
        Handle successful download: upload to OneLake and produce result.

        Args:
            task_message: Original task message
            outcome: Download outcome with file path
            processing_time_ms: Total processing time in milliseconds

        Raises:
            Exception: On upload or produce failures
        """
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

        try:
            # Upload to OneLake
            assert self.onelake_client is not None, "OneLake client not initialized"
            assert outcome.file_path is not None, "File path missing in successful outcome"

            blob_path = await self.onelake_client.upload_file(
                relative_path=task_message.destination_path,
                local_path=outcome.file_path,
                overwrite=True,
            )

            logger.info(
                "Uploaded file to OneLake",
                extra={
                    "trace_id": task_message.trace_id,
                    "destination_path": task_message.destination_path,
                    "blob_path": blob_path,
                },
            )

            # Produce result message
            result_message = DownloadResultMessage(
                trace_id=task_message.trace_id,
                attachment_url=task_message.attachment_url,
                status="success",
                destination_path=task_message.destination_path,
                bytes_downloaded=outcome.bytes_downloaded,
                error_message=None,
                error_category=None,
                processing_time_ms=processing_time_ms,
                completed_at=datetime.now(timezone.utc),
            )

            await self.producer.send(
                topic=self.config.downloads_results_topic,
                key=task_message.trace_id,
                value=result_message,
            )

            logger.info(
                "Produced success result message",
                extra={
                    "trace_id": task_message.trace_id,
                    "topic": self.config.downloads_results_topic,
                },
            )

        finally:
            # Clean up temporary file
            await self._cleanup_temp_file(outcome.file_path)

    async def _handle_failure(
        self,
        task_message: DownloadTaskMessage,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """
        Handle failed download: route to retry/DLQ and produce result.

        Routes failures based on error category:
        - CIRCUIT_OPEN: Re-raise exception (don't commit offset, reprocess when circuit closes)
        - PERMANENT: Send to DLQ and commit offset (no retry)
        - TRANSIENT: Send to retry topic and commit offset
        - AUTH: Send to retry topic and commit offset (credentials may refresh)
        - UNKNOWN: Send to retry topic and commit offset (conservative retry)

        Args:
            task_message: Original task message
            outcome: Download outcome with error details
            processing_time_ms: Total processing time in milliseconds

        Raises:
            CircuitOpenError: If circuit breaker is open (offset not committed)
        """
        assert self.retry_handler is not None, "RetryHandler not initialized"

        error_category = outcome.error_category or ErrorCategory.UNKNOWN

        logger.warning(
            "Download failed",
            extra={
                "trace_id": task_message.trace_id,
                "attachment_url": task_message.attachment_url,
                "error_message": outcome.error_message,
                "error_category": error_category.value,
                "status_code": outcome.status_code,
                "processing_time_ms": processing_time_ms,
                "retry_count": task_message.retry_count,
            },
        )

        # Handle circuit breaker errors specially: re-raise to prevent offset commit
        # Message will be reprocessed when circuit closes
        if error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - re-raising to prevent offset commit",
                extra={
                    "trace_id": task_message.trace_id,
                    "attachment_url": task_message.attachment_url,
                },
            )
            # Clean up temporary file before re-raising
            if outcome.file_path:
                await self._cleanup_temp_file(outcome.file_path)

            # Re-raise circuit open error to prevent offset commit
            raise CircuitOpenError(
                circuit_name="download_worker",
                retry_after=60.0,  # Circuit breaker will handle timing
                cause=Exception(outcome.error_message or "Circuit breaker open"),
            )

        # For all other errors, route through RetryHandler
        # This will send to retry topics or DLQ based on error category and retry count
        try:
            # Create exception from outcome
            error_message = outcome.error_message or "Download failed"
            error = Exception(error_message)

            # Route through retry handler (sends to retry topic or DLQ)
            await self.retry_handler.handle_failure(
                task=task_message,
                error=error,
                error_category=error_category,
            )

            logger.info(
                "Routed failed task through retry handler",
                extra={
                    "trace_id": task_message.trace_id,
                    "error_category": error_category.value,
                    "retry_count": task_message.retry_count,
                },
            )

        except Exception as e:
            # If retry routing fails, log but don't fail the message processing
            # The message will be reprocessed on next poll
            logger.error(
                "Failed to route task through retry handler",
                extra={
                    "trace_id": task_message.trace_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise  # Re-raise to prevent offset commit

        # Determine status for result message
        if error_category == ErrorCategory.PERMANENT:
            status = "failed_permanent"
        else:
            status = "failed_transient"

        # Produce result message for observability
        result_message = DownloadResultMessage(
            trace_id=task_message.trace_id,
            attachment_url=task_message.attachment_url,
            status=status,
            destination_path=None,
            bytes_downloaded=None,
            error_message=outcome.error_message,
            error_category=error_category.value,
            processing_time_ms=processing_time_ms,
            completed_at=datetime.now(timezone.utc),
        )

        await self.producer.send(
            topic=self.config.downloads_results_topic,
            key=task_message.trace_id,
            value=result_message,
        )

        logger.info(
            "Produced failure result message",
            extra={
                "trace_id": task_message.trace_id,
                "status": status,
                "topic": self.config.downloads_results_topic,
            },
        )

        # Clean up temporary file if it exists
        if outcome.file_path:
            await self._cleanup_temp_file(outcome.file_path)

    async def _cleanup_temp_file(self, file_path: Path) -> None:
        """
        Clean up temporary download file and parent directory.

        Args:
            file_path: Path to temporary file

        Note:
            Runs in thread pool since file deletion is blocking I/O.
            Errors are logged but not raised.
        """
        try:
            import asyncio

            def _delete():
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(
                        "Deleted temporary file",
                        extra={"file_path": str(file_path)},
                    )

                # Clean up parent directory if empty (trace_id directory)
                parent = file_path.parent
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    logger.debug(
                        "Deleted empty temporary directory",
                        extra={"directory": str(parent)},
                    )

            await asyncio.to_thread(_delete)

        except Exception as e:
            logger.warning(
                "Failed to clean up temporary file",
                extra={
                    "file_path": str(file_path),
                    "error": str(e),
                },
            )

    @property
    def is_running(self) -> bool:
        """Check if worker is running and processing messages."""
        return self.consumer.is_running


__all__ = ["DownloadWorker"]
