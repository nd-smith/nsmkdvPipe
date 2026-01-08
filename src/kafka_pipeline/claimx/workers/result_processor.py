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
from kafka_pipeline.common.writers.base import BaseDeltaWriter
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
        inventory_table_path: str = "",
    ):
        """
        Initialize ClaimX result processor.

        Args:
            config: Kafka configuration for consumer
            results_topic: Topic name for upload results (e.g., "claimx.downloads.results")
            inventory_table_path: Full abfss:// path to claimx_attachments table (optional)
        """
        self.config = config
        self.domain = "claimx"
        self.worker_name = "result_processor"

        # Get topic from hierarchical config or use provided/default
        self.results_topic = results_topic or config.get_topic(self.domain, "downloads_results")
        self.consumer: Optional[BaseKafkaConsumer] = None

        # Consumer group from hierarchical config
        self.consumer_group = config.get_consumer_group(self.domain, self.worker_name)

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        # Initialize Delta writer for inventory if path is provided
        self.inventory_writer: Optional[BaseDeltaWriter] = None
        if inventory_table_path:
            self.inventory_writer = BaseDeltaWriter(
                table_path=inventory_table_path,
                partition_column="project_id",
            )

        logger.info(
            "Initialized ClaimXResultProcessor",
            extra={
                "consumer_group": self.consumer_group,
                "results_topic": self.results_topic,
                "inventory_table": inventory_table_path,
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

         # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the ClaimX result processor.

        Gracefully shuts down the consumer and logs final statistics.
        """
        logger.info("Stopping ClaimXResultProcessor")

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

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
        self._records_processed += 1

        if result.status == "completed":
            self._records_succeeded += 1

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

            # Write to inventory table
            if self.inventory_writer:
                # Prepare record for inventory
                # Note: Schema should match table definition
                # Flattening relevant fields for querying
                import polars as pl
                now = datetime.now(timezone.utc)
                
                inventory_row = {
                    "media_id": result.media_id,
                    "project_id": result.project_id,
                    "file_name": result.file_name,
                    "file_type": result.file_type,
                    "blob_path": result.blob_path,
                    "bytes": result.bytes_uploaded,
                    "source_event_id": result.source_event_id,
                    "created_at": now,
                    "updated_at": now,
                }
                
                df = pl.DataFrame([inventory_row])
                # Using merge to prevent duplicates if reprocessing
                await self.inventory_writer._async_merge(
                    df,
                    merge_keys=["media_id"],
                    preserve_columns=["created_at"],
                )

        elif result.status == "failed_permanent":
            self._records_failed += 1

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
            self._records_failed += 1

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

        # Record message consumption metric
        record_message_consumed(
            record.topic,
            self.consumer_group,
            len(record.value),
            success=True,
        )

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.
        """
        logger.info(
            "Cycle 0: processed=0 (succeeded=0, failed=0, skipped=0) "
            "[cycle output every %ds]",
            30,
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:  # Runs until cancelled
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:  # 30 matches standard interval
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    logger.info(
                        f"Cycle {self._cycle_count}: processed={self._records_processed} "
                        f"(succeeded={self._records_succeeded}, failed={self._records_failed}, "
                        f"skipped={self._records_skipped})",
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "cycle_interval_seconds": 30,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXResultProcessor"]
