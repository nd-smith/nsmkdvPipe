"""
Upload worker for processing cached downloads and uploading to OneLake.

Consumes CachedDownloadMessage from downloads.cached topic,
uploads files to OneLake, and produces DownloadResultMessage.

This worker is decoupled from the Download Worker to allow:
- Independent scaling of download vs upload workers
- Downloads not blocked by slow OneLake uploads
- Cache buffer if OneLake has temporary issues

Architecture:
- Download Worker: downloads → local cache → CachedDownloadMessage
- Upload Worker: CachedDownloadMessage → OneLake → DownloadResultMessage
"""

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.producer import BaseKafkaProducer
from kafka_pipeline.schemas.cached import CachedDownloadMessage
from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.storage import OneLakeClient
from kafka_pipeline.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


@dataclass
class UploadResult:
    """Result of processing a single upload task."""
    message: ConsumerRecord
    cached_message: CachedDownloadMessage
    processing_time_ms: int
    success: bool
    error: Optional[Exception] = None


class UploadWorker:
    """
    Worker that uploads cached downloads to OneLake.

    Consumes CachedDownloadMessage from downloads.cached topic,
    uploads files to OneLake, produces DownloadResultMessage,
    and cleans up the local cache.

    Concurrent Processing:
    - Fetches batches of messages using Kafka's getmany()
    - Processes uploads concurrently with configurable parallelism
    - Uses semaphore to control max concurrent uploads (default: 10)
    - Tracks in-flight uploads for graceful shutdown

    For each message:
    1. Parse CachedDownloadMessage from Kafka
    2. Verify cached file exists
    3. Upload to OneLake
    4. Produce DownloadResultMessage to results topic
    5. Delete local cached file
    6. Commit offsets after batch processing

    Usage:
        config = KafkaConfig.from_env()
        worker = UploadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    CONSUMER_GROUP = "xact-upload-worker"
    WORKER_NAME = "upload_worker"

    def __init__(self, config: KafkaConfig):
        """
        Initialize upload worker.

        Args:
            config: Kafka configuration

        Raises:
            ValueError: If no OneLake path is configured (neither domain paths nor base path)
        """
        self.config = config

        # Validate OneLake configuration - need either domain paths or base path
        if not config.onelake_domain_paths and not config.onelake_base_path:
            raise ValueError(
                "OneLake path configuration required. Set either:\n"
                "  - onelake_domain_paths in config.yaml (preferred), or\n"
                "  - ONELAKE_XACT_PATH / ONELAKE_CLAIMX_PATH env vars, or\n"
                "  - ONELAKE_BASE_PATH env var (fallback for all domains)"
            )

        # Topic to consume from
        self.topic = config.downloads_cached_topic

        # Consumer will be created in start()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # Concurrency control
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight_tasks: Set[str] = set()  # Track by trace_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: Optional[asyncio.Event] = None

        # Create producer for result messages
        self.producer = BaseKafkaProducer(config=config)

        # OneLake clients by domain (lazy initialized in start())
        self.onelake_clients: Dict[str, OneLakeClient] = {}

        # Log configured domains
        configured_domains = list(config.onelake_domain_paths.keys())
        logger.info(
            "Initialized upload worker",
            extra={
                "consumer_group": self.CONSUMER_GROUP,
                "topic": self.topic,
                "configured_domains": configured_domains,
                "fallback_path": config.onelake_base_path or "(none)",
                "upload_concurrency": config.upload_concurrency,
                "upload_batch_size": config.upload_batch_size,
            },
        )

    async def start(self) -> None:
        """
        Start the upload worker with concurrent processing.

        Begins consuming messages from cached topic.
        Processes messages in concurrent batches.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer or producer fails to start
        """
        if self._running:
            logger.warning("Worker already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting upload worker",
            extra={
                "upload_concurrency": self.config.upload_concurrency,
                "upload_batch_size": self.config.upload_batch_size,
            },
        )

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.config.upload_concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        # Initialize OneLake clients for each configured domain
        for domain, path in self.config.onelake_domain_paths.items():
            client = OneLakeClient(path)
            await client.__aenter__()
            self.onelake_clients[domain] = client
            logger.info(
                "Initialized OneLake client for domain",
                extra={
                    "domain": domain,
                    "onelake_base_path": path,
                },
            )

        # Initialize fallback client if base_path is configured
        if self.config.onelake_base_path:
            fallback_client = OneLakeClient(self.config.onelake_base_path)
            await fallback_client.__aenter__()
            self.onelake_clients["_fallback"] = fallback_client
            logger.info(
                "Initialized fallback OneLake client",
                extra={"onelake_base_path": self.config.onelake_base_path},
            )

        # Create Kafka consumer
        await self._create_consumer()

        self._running = True

        # Update connection status
        update_connection_status("consumer", connected=True)

        logger.info("Upload worker started successfully")

        try:
            await self._consume_batch_loop()
        except asyncio.CancelledError:
            logger.info("Upload worker cancelled")
        except Exception as e:
            logger.error(f"Upload worker error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """
        Stop the upload worker gracefully.

        Waits for in-flight uploads to complete before stopping.
        """
        if not self._running:
            return

        logger.info("Stopping upload worker...")
        self._running = False

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        # Wait for in-flight uploads (with timeout)
        if self._in_flight_tasks:
            logger.info(
                f"Waiting for {len(self._in_flight_tasks)} in-flight uploads to complete..."
            )
            wait_start = time.time()
            while self._in_flight_tasks and (time.time() - wait_start) < 30:
                await asyncio.sleep(0.5)

            if self._in_flight_tasks:
                logger.warning(
                    f"Forcing shutdown with {len(self._in_flight_tasks)} uploads still in progress"
                )

        # Stop consumer
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

        # Stop producer
        await self.producer.stop()

        # Close all OneLake clients
        for domain, client in self.onelake_clients.items():
            try:
                await client.close()
                logger.debug(f"Closed OneLake client for domain '{domain}'")
            except Exception as e:
                logger.warning(f"Error closing OneLake client for '{domain}': {e}")
        self.onelake_clients.clear()

        # Update metrics
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self.CONSUMER_GROUP, 0)

        logger.info("Upload worker stopped")

    async def _create_consumer(self) -> None:
        """Create and start Kafka consumer."""
        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.CONSUMER_GROUP,
            "auto_offset_reset": self.config.auto_offset_reset,
            "enable_auto_commit": False,
            "max_poll_records": self.config.upload_batch_size,
            "session_timeout_ms": self.config.session_timeout_ms,
            "max_poll_interval_ms": self.config.max_poll_interval_ms,
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Add security configuration
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism == "OAUTHBEARER":
                consumer_config["sasl_oauth_token_provider"] = create_kafka_oauth_callback()
            elif self.config.sasl_mechanism == "PLAIN":
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(self.topic, **consumer_config)
        await self._consumer.start()

        logger.info(
            "Consumer started",
            extra={
                "topic": self.topic,
                "consumer_group": self.CONSUMER_GROUP,
            },
        )

    async def _consume_batch_loop(self) -> None:
        """Main consumption loop with batch processing."""
        assert self._consumer is not None

        while self._running:
            try:
                # Fetch batch of messages
                batch: Dict[str, List[ConsumerRecord]] = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.config.upload_batch_size,
                )

                if not batch:
                    continue

                # Flatten messages from all partitions
                messages = []
                for topic_partition, records in batch.items():
                    messages.extend(records)

                if messages:
                    await self._process_batch(messages)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consume loop: {e}", exc_info=True)
                record_processing_error(self.topic, self.CONSUMER_GROUP, "consume_error")
                await asyncio.sleep(1)  # Brief pause before retry

    async def _process_batch(self, messages: List[ConsumerRecord]) -> None:
        """Process a batch of messages concurrently."""
        assert self._consumer is not None
        assert self._semaphore is not None

        logger.debug(f"Processing batch of {len(messages)} messages")

        # Process all messages concurrently
        tasks = [
            asyncio.create_task(self._process_single_with_semaphore(msg))
            for msg in messages
        ]

        results: List[UploadResult] = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Unexpected error in upload: {result}", exc_info=True)
                record_processing_error(self.topic, self.CONSUMER_GROUP, "unexpected_error")

        # Commit offsets after batch
        try:
            await self._consumer.commit()
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}", exc_info=True)

    async def _process_single_with_semaphore(self, message: ConsumerRecord) -> UploadResult:
        """Process single message with semaphore for concurrency control."""
        assert self._semaphore is not None

        async with self._semaphore:
            return await self._process_single_upload(message)

    async def _process_single_upload(self, message: ConsumerRecord) -> UploadResult:
        """Process a single cached download message."""
        start_time = time.time()
        trace_id = "unknown"

        try:
            # Parse message
            cached_message = CachedDownloadMessage.model_validate_json(message.value)
            trace_id = cached_message.trace_id

            # Track in-flight
            async with self._in_flight_lock:
                self._in_flight_tasks.add(trace_id)

            record_message_consumed(
                self.topic, self.CONSUMER_GROUP, len(message.value), success=True
            )

            # Verify cached file exists
            cache_path = Path(cached_message.local_cache_path)
            if not cache_path.exists():
                raise FileNotFoundError(f"Cached file not found: {cache_path}")

            # Get domain from event_type to route to correct OneLake path
            # event_type should be the domain (e.g., "xact", "claimx") set by EventIngester
            raw_event_type = cached_message.event_type
            domain = raw_event_type.lower()

            # Log domain lookup for debugging
            available_domains = list(self.onelake_clients.keys())
            logger.debug(
                "Domain lookup for upload",
                extra={
                    "trace_id": trace_id,
                    "raw_event_type": raw_event_type,
                    "domain": domain,
                    "available_domains": available_domains,
                },
            )

            # Get appropriate OneLake client for this domain
            onelake_client = self.onelake_clients.get(domain)
            if onelake_client is None:
                # Fall back to default client
                onelake_client = self.onelake_clients.get("_fallback")
                if onelake_client is None:
                    raise ValueError(
                        f"No OneLake client configured for domain '{domain}' "
                        f"(event_type='{raw_event_type}') and no fallback configured. "
                        f"Available domains: {available_domains}"
                    )
                logger.warning(
                    "Using fallback OneLake client - domain not found",
                    extra={
                        "trace_id": trace_id,
                        "raw_event_type": raw_event_type,
                        "domain": domain,
                        "available_domains": available_domains,
                    },
                )

            # Upload to OneLake (domain-specific path)
            blob_path = await onelake_client.upload_file(
                relative_path=cached_message.destination_path,
                local_path=cache_path,
                overwrite=True,
            )

            logger.info(
                "Uploaded file to OneLake",
                extra={
                    "trace_id": trace_id,
                    "domain": domain,
                    "raw_event_type": raw_event_type,
                    "destination_path": cached_message.destination_path,
                    "onelake_base_path": onelake_client.base_path,
                    "blob_path": blob_path,
                    "bytes": cached_message.bytes_downloaded,
                },
            )

            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)

            # Produce success result
            result_message = DownloadResultMessage(
                trace_id=trace_id,
                attachment_url=cached_message.attachment_url,
                blob_path=cached_message.destination_path,
                status_subtype=cached_message.status_subtype,
                file_type=cached_message.file_type,
                assignment_id=cached_message.assignment_id,
                status="completed",
                bytes_downloaded=cached_message.bytes_downloaded,
                created_at=datetime.now(timezone.utc),
            )

            await self.producer.send(
                topic=self.config.downloads_results_topic,
                key=trace_id,
                value=result_message,
            )

            # Clean up cached file
            await self._cleanup_cache_file(cache_path)

            return UploadResult(
                message=message,
                cached_message=cached_message,
                processing_time_ms=processing_time_ms,
                success=True,
            )

        except Exception as e:
            processing_time_ms = int((time.time() - start_time) * 1000)
            logger.error(
                f"Upload failed: {e}",
                extra={"trace_id": trace_id},
                exc_info=True,
            )
            record_processing_error(self.topic, self.CONSUMER_GROUP, "upload_error")

            # For upload failures, we produce a failure result
            # The file stays in cache for manual review/retry
            try:
                # Re-parse message in case it wasn't parsed yet
                if 'cached_message' not in locals() or cached_message is None:
                    cached_message = CachedDownloadMessage.model_validate_json(message.value)

                result_message = DownloadResultMessage(
                    trace_id=cached_message.trace_id,
                    attachment_url=cached_message.attachment_url,
                    blob_path=cached_message.destination_path,
                    status_subtype=cached_message.status_subtype,
                    file_type=cached_message.file_type,
                    assignment_id=cached_message.assignment_id,
                    status="failed_permanent",
                    bytes_downloaded=0,
                    error_message=str(e)[:500],
                    created_at=datetime.now(timezone.utc),
                )

                await self.producer.send(
                    topic=self.config.downloads_results_topic,
                    key=cached_message.trace_id,
                    value=result_message,
                )
            except Exception as produce_error:
                logger.error(f"Failed to produce failure result: {produce_error}")

            return UploadResult(
                message=message,
                cached_message=cached_message if 'cached_message' in locals() else None,
                processing_time_ms=processing_time_ms,
                success=False,
                error=e,
            )

        finally:
            # Remove from in-flight tracking
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(trace_id)

    async def _cleanup_cache_file(self, cache_path: Path) -> None:
        """Clean up cached file and its parent directory if empty."""
        try:
            # Delete the file
            if cache_path.exists():
                await asyncio.to_thread(os.remove, str(cache_path))

            # Try to remove parent directory if empty
            parent = cache_path.parent
            if parent.exists():
                try:
                    await asyncio.to_thread(parent.rmdir)
                except OSError:
                    pass  # Directory not empty

            logger.debug(f"Cleaned up cache file: {cache_path}")

        except Exception as e:
            logger.warning(f"Failed to clean up cache file {cache_path}: {e}")
