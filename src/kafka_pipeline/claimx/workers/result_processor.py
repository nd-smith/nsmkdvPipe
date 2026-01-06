"""
ClaimX Result Processor - Processes download/upload results and tracks outcomes.

This worker is the final stage of the ClaimX download pipeline:
1. Consumes ClaimXUploadResultMessage from results topic
2. Logs outcomes for monitoring and alerting
3. Emits metrics on success/failure rates
4. Optionally writes to Delta Lake for audit trail

Consumer group: {prefix}-claimx-result-processor
Input topic: claimx.downloads.results
Delta table (optional): claimx_download_results
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
)
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage

logger = get_logger(__name__)


class ClaimXResultProcessor:
    """
    Worker to consume and process ClaimX download/upload results.

    Processes ClaimXUploadResultMessage records from the results topic,
    logs outcomes, emits metrics, and tracks success/failure rates for monitoring.

    Features:
    - Success/failure rate tracking
    - Detailed logging for operational monitoring
    - Metrics emission for dashboards
    - Graceful shutdown

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> processor = ClaimXResultProcessor(config)
        >>> await processor.start()
        >>> # Processor runs until stopped
        >>> await processor.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        results_topic: str = "",
    ):
        """
        Initialize ClaimX result processor.

        Args:
            config: Kafka configuration for consumer
            results_topic: Topic name for upload results (e.g., "claimx.downloads.results")
        """
        self.config = config
        self.domain = "claimx"
        self.worker_name = "result_processor"

        # Get topic from hierarchical config or use provided/default
        self.results_topic = results_topic or config.get_topic(self.domain, "downloads_results")
        self.consumer: Optional[BaseKafkaConsumer] = None

        # Consumer group from hierarchical config
        self.consumer_group = config.get_consumer_group(self.domain, self.worker_name)

        # Running statistics (reset periodically for monitoring windows)
        self._stats_lock = asyncio.Lock()
        self._stats: Dict[str, int] = {
            "total_processed": 0,
            "completed": 0,
            "failed": 0,
            "failed_permanent": 0,
            "bytes_uploaded_total": 0,
        }
        self._stats_window_start = datetime.now(timezone.utc)

        logger.info(
            "Initialized ClaimXResultProcessor",
            extra={
                "consumer_group": self.consumer_group,
                "results_topic": self.results_topic,
            },
        )

    async def start(self) -> None:
        """
        Start the ClaimX result processor.

        Begins consuming result messages from the results topic.
        This method runs until stop() is called.

        Raises:
            Exception: If consumer fails to start
        """
        logger.info("Starting ClaimXResultProcessor")

        # Create and start consumer with message handler
        self.consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self.results_topic],
            message_handler=self._handle_result_message,
        )

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the ClaimX result processor.

        Gracefully shuts down the consumer and logs final statistics.
        """
        logger.info("Stopping ClaimXResultProcessor")

        # Log final statistics
        await self._log_statistics(final=True)

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        logger.info("ClaimXResultProcessor stopped successfully")

    async def _handle_result_message(self, record: ConsumerRecord) -> None:
        """
        Process a single upload result message from Kafka.

        Logs the outcome, updates statistics, and emits metrics.

        Args:
            record: ConsumerRecord containing ClaimXUploadResultMessage JSON

        Raises:
            Exception: If message processing fails (will be handled by consumer error routing)
        """
        # Decode and parse ClaimXUploadResultMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            result = ClaimXUploadResultMessage.model_validate(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimXUploadResultMessage",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            record_processing_error(record.topic, self.consumer_group, "parse_error")
            raise

        # Update statistics
        async with self._stats_lock:
            self._stats["total_processed"] += 1

            if result.status == "completed":
                self._stats["completed"] += 1
                self._stats["bytes_uploaded_total"] += result.bytes_uploaded

                logger.info(
                    "Upload completed successfully",
                    extra={
                        "correlation_id": result.source_event_id,
                        "media_id": result.media_id,
                        "project_id": result.project_id,
                        "file_name": result.file_name,
                        "file_type": result.file_type,
                        "bytes_uploaded": result.bytes_uploaded,
                        "blob_path": result.blob_path,
                        "source_event_id": result.source_event_id,
                    },
                )

            elif result.status == "failed_permanent":
                self._stats["failed_permanent"] += 1

                logger.error(
                    "Upload failed permanently",
                    extra={
                        "correlation_id": result.source_event_id,
                        "media_id": result.media_id,
                        "project_id": result.project_id,
                        "file_name": result.file_name,
                        "error_message": result.error_message,
                        "error_category": "permanent",
                        "source_event_id": result.source_event_id,
                    },
                )

            elif result.status == "failed":
                self._stats["failed"] += 1

                logger.warning(
                    "Upload failed (transient)",
                    extra={
                        "correlation_id": result.source_event_id,
                        "media_id": result.media_id,
                        "project_id": result.project_id,
                        "file_name": result.file_name,
                        "error_message": result.error_message,
                        "error_category": "transient",
                        "source_event_id": result.source_event_id,
                    },
                )

            # Log statistics periodically (every 100 messages)
            if self._stats["total_processed"] % 100 == 0:
                await self._log_statistics()

        # Record message consumption metric
        record_message_consumed(
            record.topic,
            self.consumer_group,
            len(record.value),
            success=True,
        )

    async def _log_statistics(self, final: bool = False) -> None:
        """
        Log processing statistics for monitoring.

        Args:
            final: Whether this is the final stats log before shutdown
        """
        async with self._stats_lock:
            elapsed = (datetime.now(timezone.utc) - self._stats_window_start).total_seconds()

            total = self._stats["total_processed"]
            completed = self._stats["completed"]
            failed = self._stats["failed"]
            failed_permanent = self._stats["failed_permanent"]
            bytes_total = self._stats["bytes_uploaded_total"]

            success_rate = (completed / total * 100) if total > 0 else 0.0
            failure_rate = ((failed + failed_permanent) / total * 100) if total > 0 else 0.0

            log_level = "info" if not final else "info"
            logger.log(
                getattr(logging, log_level.upper(), logging.INFO),
                "Result processor statistics" + (" (final)" if final else ""),
                extra={
                    "total_processed": total,
                    "completed": completed,
                    "failed_transient": failed,
                    "failed_permanent": failed_permanent,
                    "success_rate_pct": round(success_rate, 2),
                    "failure_rate_pct": round(failure_rate, 2),
                    "bytes_uploaded_total": bytes_total,
                    "elapsed_seconds": round(elapsed, 2),
                    "messages_per_second": round(total / elapsed, 2) if elapsed > 0 else 0,
                },
            )

            # Reset stats for next window (if not final)
            if not final:
                self._stats = {
                    "total_processed": 0,
                    "completed": 0,
                    "failed": 0,
                    "failed_permanent": 0,
                    "bytes_uploaded_total": 0,
                }
                self._stats_window_start = datetime.now(timezone.utc)


__all__ = ["ClaimXResultProcessor"]
