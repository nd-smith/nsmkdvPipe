"""
Download worker for processing download tasks with concurrent processing.

Consumes DownloadTaskMessage from pending and retry topics,
downloads attachments using AttachmentDownloader, caches to local
filesystem, and produces CachedDownloadMessage for upload worker.

This implementation includes:
- WP-304: Core download processing
- WP-305: Cache file and produce cached message (upload decoupled)
- WP-306: Error handling
- WP-313: Concurrent processing (FR-2.6 requirement)

Architecture:
- Downloads are cached locally before upload (decoupled from upload worker)
- Upload worker consumes from downloads.cached topic
- This allows independent scaling of download vs upload workers

Concurrent Processing (WP-313):
- Fetches batches of messages from Kafka
- Processes downloads concurrently using asyncio.Semaphore
- Uses HTTP connection pooling via shared aiohttp.ClientSession
- Configurable concurrency via DOWNLOAD_CONCURRENCY (default: 10, max: 50)
- Graceful shutdown waits for in-flight downloads to complete
"""

import asyncio
import logging
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import aiohttp
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadTask, DownloadOutcome
from core.errors.exceptions import CircuitOpenError
from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.producer import BaseKafkaProducer
from kafka_pipeline.retry.handler import RetryHandler
from kafka_pipeline.schemas.cached import CachedDownloadMessage
from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    update_downloads_concurrent,
    update_downloads_batch_size,
    message_processing_duration_seconds,
)

logger = logging.getLogger(__name__)


@dataclass
class TaskResult:
    """Result of processing a single download task."""
    message: ConsumerRecord
    task_message: DownloadTaskMessage
    outcome: DownloadOutcome
    processing_time_ms: int
    success: bool
    error: Optional[Exception] = None


class DownloadWorker:
    """
    Worker that processes download tasks from Kafka with concurrent processing.

    Consumes DownloadTaskMessage from:
    - downloads.pending (new tasks from event ingester)
    - downloads.retry.* (retried tasks with exponential backoff)

    Architecture:
    - Downloads files to local cache directory
    - Produces CachedDownloadMessage to downloads.cached topic
    - Upload Worker (separate) handles OneLake uploads
    - This decoupling allows independent scaling of download vs upload

    Concurrent Processing (WP-313 - FR-2.6):
    - Fetches batches of messages using Kafka's getmany()
    - Processes downloads concurrently with configurable parallelism
    - Uses semaphore to control max concurrent downloads (default: 10)
    - Shares HTTP connection pool across concurrent downloads
    - Tracks in-flight downloads for graceful shutdown

    For each task:
    1. Parse DownloadTaskMessage from Kafka
    2. Convert to DownloadTask for AttachmentDownloader
    3. Download attachment to cache location (concurrent)
    4. Produce CachedDownloadMessage to cached topic
    5. Commit offsets after batch processing

    Usage:
        config = KafkaConfig.from_env()
        worker = DownloadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    CONSUMER_GROUP = "xact-download-worker"
    WORKER_NAME = "download_worker"

    def __init__(self, config: KafkaConfig, temp_dir: Optional[Path] = None):
        """
        Initialize download worker.

        Args:
            config: Kafka configuration
            temp_dir: Optional directory for temporary downloads (None = system temp)
        """
        self.config = config

        # Temp dir for in-progress downloads
        self.temp_dir = temp_dir or Path(tempfile.gettempdir()) / "download_worker"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Cache dir for completed downloads awaiting upload
        self.cache_dir = Path(config.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Build list of topics to consume from (pending + retry topics)
        # Retry topics are dynamically constructed from config using get_retry_topic()
        retry_topics = [
            config.get_retry_topic(i) for i in range(len(config.retry_delays))
        ]
        self.topics = [config.downloads_pending_topic] + retry_topics

        # Consumer will be created in start()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # Concurrency control (WP-313)
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight_tasks: Set[str] = set()  # Track by trace_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: Optional[asyncio.Event] = None

        # Shared HTTP session for connection pooling (WP-313)
        self._http_session: Optional[aiohttp.ClientSession] = None

        # Create producer for result messages
        self.producer = BaseKafkaProducer(config=config)

        # Create downloader instance (reused across tasks)
        self.downloader = AttachmentDownloader()

        # Create retry handler for error routing (lazy initialized in start())
        self.retry_handler: Optional[RetryHandler] = None

        logger.info(
            "Initialized download worker with concurrent processing",
            extra={
                "consumer_group": self.CONSUMER_GROUP,
                "topics": self.topics,
                "temp_dir": str(self.temp_dir),
                "cache_dir": str(self.cache_dir),
                "download_concurrency": config.download_concurrency,
                "download_batch_size": config.download_batch_size,
            },
        )

    async def start(self) -> None:
        """
        Start the download worker with concurrent processing.

        Begins consuming messages from pending and retry topics.
        Processes messages in concurrent batches.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer or producer fails to start
        """
        if self._running:
            logger.warning("Worker already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting download worker with concurrent processing",
            extra={
                "download_concurrency": self.config.download_concurrency,
                "download_batch_size": self.config.download_batch_size,
            },
        )

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.config.download_concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Create shared HTTP session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=self.config.download_concurrency,
            limit_per_host=self.config.download_concurrency,
        )
        self._http_session = aiohttp.ClientSession(connector=connector)

        # Start producer
        await self.producer.start()

        # Initialize retry handler (requires producer to be started)
        self.retry_handler = RetryHandler(self.config, self.producer)

        # Create Kafka consumer
        await self._create_consumer()

        self._running = True

        # Update connection status
        update_connection_status("consumer", connected=True)
        partition_count = len(self._consumer.assignment()) if self._consumer else 0
        update_assigned_partitions(self.CONSUMER_GROUP, partition_count)

        logger.info(
            "Download worker started successfully",
            extra={
                "topics": self.topics,
                "partitions": partition_count,
            },
        )

        # Start batch consumption loop
        try:
            await self._consume_batch_loop()
        except asyncio.CancelledError:
            logger.info("Worker cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Worker terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def _create_consumer(self) -> None:
        """Create and start the Kafka consumer."""
        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.CONSUMER_GROUP,
            "enable_auto_commit": False,  # Manual commit after batch processing
            "auto_offset_reset": self.config.auto_offset_reset,
            "max_poll_records": self.config.download_batch_size,
            "max_poll_interval_ms": self.config.max_poll_interval_ms,
            "session_timeout_ms": self.config.session_timeout_ms,
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                consumer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(*self.topics, **consumer_config)
        await self._consumer.start()

    async def stop(self) -> None:
        """
        Stop the download worker and wait for in-flight downloads.

        Performs graceful shutdown:
        1. Signals shutdown to stop accepting new batches
        2. Waits for in-flight downloads to complete (up to 30s)
        3. Commits final offsets
        4. Closes all resources

        Safe to call multiple times.
        """
        if not self._running:
            logger.debug("Worker not running or already stopped")
            return

        logger.info("Stopping download worker, waiting for in-flight downloads")
        self._running = False

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        # Wait for in-flight downloads to complete (with timeout)
        await self._wait_for_in_flight(timeout=30.0)

        # Stop consumer
        if self._consumer:
            try:
                await self._consumer.commit()
                await self._consumer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping consumer",
                    extra={"error": str(e)},
                    exc_info=True,
                )
            finally:
                self._consumer = None

        # Close HTTP session
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

        # Stop producer
        await self.producer.stop()

        # Clear retry handler reference
        self.retry_handler = None

        # Update metrics
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self.CONSUMER_GROUP, 0)
        update_downloads_concurrent(self.WORKER_NAME, 0)
        update_downloads_batch_size(self.WORKER_NAME, 0)

        logger.info("Download worker stopped successfully")

    async def _wait_for_in_flight(self, timeout: float = 30.0) -> None:
        """
        Wait for in-flight downloads to complete.

        Args:
            timeout: Maximum time to wait in seconds
        """
        start_time = time.perf_counter()
        while True:
            async with self._in_flight_lock:
                count = len(self._in_flight_tasks)

            if count == 0:
                logger.info("All in-flight downloads completed")
                return

            elapsed = time.perf_counter() - start_time
            if elapsed >= timeout:
                logger.warning(
                    "Timeout waiting for in-flight downloads",
                    extra={
                        "remaining_tasks": count,
                        "timeout_seconds": timeout,
                    },
                )
                return

            logger.debug(
                "Waiting for in-flight downloads",
                extra={"remaining_tasks": count},
            )
            await asyncio.sleep(0.5)

    async def _consume_batch_loop(self) -> None:
        """
        Main batch consumption loop.

        Fetches batches of messages and processes them concurrently.
        """
        logger.info("Starting batch consumption loop")

        while self._running and self._consumer:
            try:
                # Fetch batch of messages
                data = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.config.download_batch_size,
                )

                if not data:
                    continue

                # Flatten messages from all partitions
                messages: List[ConsumerRecord] = []
                for topic_partition, records in data.items():
                    messages.extend(records)

                if not messages:
                    continue

                # Update batch size metric
                update_downloads_batch_size(self.WORKER_NAME, len(messages))

                logger.info(
                    "Processing message batch",
                    extra={
                        "batch_size": len(messages),
                        "download_concurrency": self.config.download_concurrency,
                    },
                )

                # Process batch concurrently
                results = await self._process_batch(messages)

                # Handle results and determine commit strategy
                should_commit = await self._handle_batch_results(results)

                if should_commit:
                    await self._consumer.commit()
                    logger.debug(
                        "Committed offsets for batch",
                        extra={"batch_size": len(messages)},
                    )

                # Reset batch size metric
                update_downloads_batch_size(self.WORKER_NAME, 0)

            except asyncio.CancelledError:
                logger.info("Batch consumption loop cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error in batch consumption loop",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(1)

    async def _process_batch(self, messages: List[ConsumerRecord]) -> List[TaskResult]:
        """
        Process a batch of messages concurrently.

        Uses asyncio.Semaphore to control concurrency and asyncio.gather()
        to process downloads in parallel.

        Args:
            messages: List of ConsumerRecords to process

        Returns:
            List of TaskResult objects with processing outcomes
        """
        async def bounded_process(message: ConsumerRecord) -> TaskResult:
            """Process a single message with semaphore control."""
            async with self._semaphore:
                return await self._process_single_task(message)

        # Update concurrent downloads metric as tasks start
        update_downloads_concurrent(self.WORKER_NAME, len(messages))

        # Process all messages concurrently
        tasks = [bounded_process(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to TaskResult objects
        processed_results: List[TaskResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Create error result for unhandled exceptions
                message = messages[i]
                logger.error(
                    "Unhandled exception processing message",
                    extra={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "error": str(result),
                    },
                    exc_info=result,
                )
                # We can't create a full TaskResult without parsing, so we'll handle this
                # as a transient error that will be retried on next poll
                processed_results.append(None)  # type: ignore
            else:
                processed_results.append(result)

        # Update metric - all downloads complete
        update_downloads_concurrent(self.WORKER_NAME, 0)

        # Log batch summary
        succeeded = sum(1 for r in processed_results if r and r.success)
        failed = sum(1 for r in processed_results if r and not r.success)
        errors = sum(1 for r in processed_results if r is None)

        logger.info(
            "Batch processing complete",
            extra={
                "batch_size": len(messages),
                "succeeded": succeeded,
                "failed": failed,
                "unhandled_errors": errors,
            },
        )

        return [r for r in processed_results if r is not None]

    async def _process_single_task(self, message: ConsumerRecord) -> TaskResult:
        """
        Process a single download task message.

        Args:
            message: ConsumerRecord with DownloadTaskMessage as value

        Returns:
            TaskResult with processing outcome
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
            # Return error result
            return TaskResult(
                message=message,
                task_message=None,  # type: ignore
                outcome=DownloadOutcome(
                    success=False,
                    error_message=f"Failed to parse message: {str(e)}",
                    error_category=ErrorCategory.PERMANENT,
                ),
                processing_time_ms=int((time.perf_counter() - start_time) * 1000),
                success=False,
                error=e,
            )

        # Track in-flight task
        async with self._in_flight_lock:
            self._in_flight_tasks.add(task_message.trace_id)

        try:
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

            # Record processing duration metric
            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(
                topic=message.topic, consumer_group=self.CONSUMER_GROUP
            ).observe(duration)

            # Handle outcome: upload and produce result
            if outcome.success:
                await self._handle_success(task_message, outcome, processing_time_ms)
                record_message_consumed(
                    message.topic, self.CONSUMER_GROUP, len(message.value), success=True
                )
                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=outcome,
                    processing_time_ms=processing_time_ms,
                    success=True,
                )
            else:
                await self._handle_failure(task_message, outcome, processing_time_ms)
                record_message_consumed(
                    message.topic, self.CONSUMER_GROUP, len(message.value), success=False
                )
                # Check if this is a circuit breaker error that should prevent commit
                is_circuit_error = outcome.error_category == ErrorCategory.CIRCUIT_OPEN
                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=outcome,
                    processing_time_ms=processing_time_ms,
                    success=False,
                    error=CircuitOpenError("download_worker", 60.0) if is_circuit_error else None,
                )

        finally:
            # Remove from in-flight tracking
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(task_message.trace_id)

    async def _handle_batch_results(self, results: List[TaskResult]) -> bool:
        """
        Handle batch results and determine if offsets should be committed.

        Returns False if any result has a circuit breaker error, which means
        those messages should be reprocessed when the circuit closes.

        Args:
            results: List of TaskResult from batch processing

        Returns:
            True if offsets should be committed, False otherwise
        """
        # Check for circuit breaker errors
        circuit_errors = [r for r in results if r.error and isinstance(r.error, CircuitOpenError)]

        if circuit_errors:
            logger.warning(
                "Circuit breaker errors in batch - not committing offsets",
                extra={"circuit_error_count": len(circuit_errors)},
            )
            return False

        return True

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
        Handle successful download: move to cache and produce cached message.

        Moves the downloaded file to the cache directory and produces a
        CachedDownloadMessage for the Upload Worker to process.

        Args:
            task_message: Original task message
            outcome: Download outcome with file path
            processing_time_ms: Total processing time in milliseconds

        Raises:
            Exception: On cache move or produce failures
        """
        assert outcome.file_path is not None, "File path missing in successful outcome"

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

        # Move file to cache directory with stable path
        # Path structure: cache_dir/trace_id/filename
        cache_subdir = self.cache_dir / task_message.trace_id
        cache_subdir.mkdir(parents=True, exist_ok=True)

        # Use original filename from destination_path
        filename = Path(task_message.destination_path).name
        cache_path = cache_subdir / filename

        # Move file from temp to cache (atomic on same filesystem)
        await asyncio.to_thread(shutil.move, str(outcome.file_path), str(cache_path))

        logger.info(
            "Cached file for upload",
            extra={
                "trace_id": task_message.trace_id,
                "cache_path": str(cache_path),
            },
        )

        # Clean up empty temp directory
        try:
            if outcome.file_path.parent.exists():
                await asyncio.to_thread(outcome.file_path.parent.rmdir)
        except OSError:
            pass  # Directory not empty or already removed

        # Produce cached message for upload worker
        cached_message = CachedDownloadMessage(
            trace_id=task_message.trace_id,
            attachment_url=task_message.attachment_url,
            destination_path=task_message.destination_path,
            local_cache_path=str(cache_path),
            bytes_downloaded=outcome.bytes_downloaded or 0,
            content_type=outcome.content_type,
            event_type=task_message.event_type,
            event_subtype=task_message.event_subtype,
            original_timestamp=task_message.original_timestamp,
            downloaded_at=datetime.now(timezone.utc),
            metadata=task_message.metadata,
        )

        await self.producer.send(
            topic=self.config.downloads_cached_topic,
            key=task_message.trace_id,
            value=cached_message,
        )

        logger.info(
            "Produced cached download message",
            extra={
                "trace_id": task_message.trace_id,
                "topic": self.config.downloads_cached_topic,
                "cache_path": str(cache_path),
            },
        )

    async def _handle_failure(
        self,
        task_message: DownloadTaskMessage,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """
        Handle failed download: route to retry/DLQ and produce result.

        Routes failures based on error category:
        - CIRCUIT_OPEN: Don't process further (will be retried on next poll)
        - PERMANENT: Send to DLQ and commit offset (no retry)
        - TRANSIENT: Send to retry topic and commit offset
        - AUTH: Send to retry topic and commit offset (credentials may refresh)
        - UNKNOWN: Send to retry topic and commit offset (conservative retry)

        Args:
            task_message: Original task message
            outcome: Download outcome with error details
            processing_time_ms: Total processing time in milliseconds
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

        # Record error metric
        record_processing_error(
            self.config.downloads_pending_topic,
            self.CONSUMER_GROUP,
            error_category.value,
        )

        # Handle circuit breaker errors specially - don't route, will reprocess
        if error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess on next poll",
                extra={
                    "trace_id": task_message.trace_id,
                    "attachment_url": task_message.attachment_url,
                },
            )
            # Clean up temporary file before returning
            if outcome.file_path:
                await self._cleanup_temp_file(outcome.file_path)
            return

        # For all other errors, route through RetryHandler
        try:
            error_message = outcome.error_message or "Download failed"
            error = Exception(error_message)

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
            logger.error(
                "Failed to route task through retry handler",
                extra={
                    "trace_id": task_message.trace_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            # Don't re-raise - we'll still produce result message

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
        return self._running

    @property
    def in_flight_count(self) -> int:
        """Return the number of downloads currently in progress."""
        return len(self._in_flight_tasks)


__all__ = ["DownloadWorker"]
