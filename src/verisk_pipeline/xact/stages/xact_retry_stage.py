"""
Retry stage: Reprocess failed attachments with exponential backoff.

Queries transient failures from retry tracking table, respects backoff timing,
and processes retries in batches with circuit breaker protection.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import polars as pl

from verisk_pipeline.common.async_utils import run_async_with_shutdown
from verisk_pipeline.common.auth import get_storage_options
from verisk_pipeline.common.config.xact import PipelineConfig
from verisk_pipeline.common.exceptions import ErrorCategory
from verisk_pipeline.common.memory import MemoryMonitor
from verisk_pipeline.xact.xact_models import (
    BatchResult,
    DownloadResult,
    Task,
    TaskStatus,
    StageResult,
    StageStatus,
    XACT_PRIMARY_KEYS,
    XACT_RETRY_COLUMNS,
)
from verisk_pipeline.common.url_expiration import check_presigned_url
from verisk_pipeline.storage.onelake import OneLakeClient
from verisk_pipeline.xact.stages.xact_download import (
    download_batch,
)
from verisk_pipeline.xact.services.path_resolver import generate_blob_path
from verisk_pipeline.common.circuit_breaker import (
    get_circuit_breaker,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
)
from verisk_pipeline.common.security import validate_download_url
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.decorators import extract_log_context
from verisk_pipeline.common.logging.context_managers import log_phase
from verisk_pipeline.common.logging.utilities import (
    log_memory_checkpoint,
    log_with_context,
    log_exception,
)

logger = get_logger(__name__)


class RetryStage:
    """
    Retry stage for failed attachment downloads.

    Processes transient failures with exponential backoff.
    Runs independently of main download stage.
    Uses split-table architecture: inventory for successes, retry queue for failures.
    """

    # Retry configuration defaults
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_MIN_AGE_SECONDS = 60
    DEFAULT_BACKOFF_BASE = 60.0
    DEFAULT_BACKOFF_MULTIPLIER = 2.0
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_MAX_CONCURRENT = 10
    DEFAULT_RETENTION_DAYS = 7

    def __init__(self, config: PipelineConfig):
        """Initialize retry stage with configuration."""
        self.config = config

        self._max_retries = int(config.retry.max_retries)
        self._min_age = int(config.retry.min_retry_age_seconds)
        self._backoff_base = float(config.retry.backoff_base_seconds)
        self._backoff_mult = float(config.retry.backoff_multiplier)
        self._batch_size = int(config.retry.batch_size)
        self._max_concurrent = int(config.retry.max_concurrent)
        self.retention_days = int(config.retry.retention_days)

        from verisk_pipeline.storage.inventory_writer import InventoryTableWriter
        from verisk_pipeline.storage.retry_queue_writer import RetryQueueWriter

        self.inventory_writer = InventoryTableWriter(
            table_path=config.lakehouse.attachments_table_path,
            primary_keys=XACT_PRIMARY_KEYS,
            storage_options=get_storage_options(),
        )

        self.retry_writer = RetryQueueWriter(
            table_path=config.lakehouse.retry_table_path,
            primary_keys=XACT_PRIMARY_KEYS,
            storage_options=get_storage_options(),
        )

        self.onelake_client = OneLakeClient(
            config.lakehouse.files_path, max_pool_size=config.retry.max_concurrent + 5
        )

        self._download_breaker = get_circuit_breaker(
            "attachment_download", EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG
        )
        self._upload_breaker = get_circuit_breaker(
            "onelake_upload", ONELAKE_CIRCUIT_CONFIG
        )

        self.memory_monitor = MemoryMonitor(
            enabled=config.observability.memory_profiling_enabled
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "RetryStage initialized (split-table architecture)",
            inventory_table=config.lakehouse.attachments_table_path,
            retry_table=config.lakehouse.retry_table_path,
            files_path=config.lakehouse.files_path,
            max_concurrent=self._max_concurrent,
            max_retries=self._max_retries,
            min_retry_age_seconds=self._min_age,
            backoff_base_seconds=self._backoff_base,
            backoff_multiplier=self._backoff_mult,
            retry_batch_size=self._batch_size,
        )

    def run(self) -> StageResult:
        """
        Main execution entry point.

        Execute complete retry workflow:
        1. Check if retry stage is enabled
        2. Get pending retries from retry queue
        3. Process retry batch (download + upload)
        4. Write tracking records
        5. Return stage result with metrics
        """
        start_time = datetime.now(timezone.utc)

        try:
            # Check if retry stage should be skipped
            if not getattr(self.config.retry, "enabled", True):
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Retry stage disabled",
                )
                return StageResult(
                    stage_name="retry",
                    status=StageStatus.SKIPPED,
                    duration_seconds=0.0,
                )

            # Memory checkpoint: Stage start
            self.memory_monitor.snapshot("XACT Retry - Stage start")

            # Generate cycle ID for tracking
            cycle_id = generate_cycle_id()
            self._cycle_id = cycle_id
            self.inventory_writer.start_cycle(cycle_id)
            self.retry_writer.start_cycle(cycle_id)

            obs_config = self.config.observability
            log_memory_checkpoint(logger, "stage_start", config=obs_config)

            # Log circuit breaker status at start
            log_with_context(
                logger,
                logging.DEBUG,
                "Circuit breaker status at stage start",
                download_circuit=self._download_breaker.get_diagnostics(),
                upload_circuit=self._upload_breaker.get_diagnostics(),
            )

            # Step 1: Get pending retries from retry queue
            pending = self._get_pending_retries()

            # Step 2: Check if there are any items to retry
            if self._is_empty_batch(pending):
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "No items ready for retry",
                )
                self._cleanup_after_execution(
                    StageResult(
                        stage_name="retry",
                        status=StageStatus.SUCCESS,
                        duration_seconds=0.0,
                        records_processed=0,
                    )
                )
                return StageResult(
                    stage_name="retry",
                    status=StageStatus.SUCCESS,
                    duration_seconds=0.0,
                    records_processed=0,
                )

            # Step 3: Process retry batch (download + upload)
            result_dict = self._process_retry_batch(pending)

            # Step 4: Write tracking records (split-table architecture)
            self._write_retry_results(result_dict, pending)

            # Step 5: Calculate duration and build result
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            stage_result = StageResult(
                stage_name="retry",
                status=StageStatus.SUCCESS,
                duration_seconds=duration,
                records_processed=result_dict.get("total", 0),
                records_succeeded=result_dict.get("succeeded", 0),
                records_failed=result_dict.get("failed", 0),
                metadata=result_dict.get("metadata", {}),
            )

            # Cleanup and finalization
            self._cleanup_after_execution(stage_result)

            return stage_result

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            log_exception(
                logger,
                e,
                "Stage 'retry' failed",
                stage="retry",
            )

            return StageResult(
                stage_name="retry",
                status=StageStatus.FAILED,
                duration_seconds=duration,
                started_at=start_time,
                error=str(e),
            )

    def _get_pending_retries(self) -> pl.DataFrame:
        """
        Get pending retries from retry queue.

        Queries retry queue for transient failures under max_retries limit
        and past backoff wait time.
        """
        # Run cleanup first if enabled
        if (
            hasattr(self.config.retry, "cleanup_on_start")
            and self.config.retry.cleanup_on_start
        ):
            retention_days = getattr(self.config.retry, "retention_days", 7)
            try:
                deleted = self.retry_writer.cleanup_expired(retention_days)
                if deleted > 0:
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Cleaned up expired retry records",
                        deleted_count=deleted,
                        retention_days=retention_days,
                    )
            except Exception as e:
                # Cleanup is housekeeping - don't fail the stage if unavailable
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Cleanup failed, continuing without cleanup",
                    error_message=str(e),
                    retention_days=retention_days,
                )

        log_with_context(
            logger,
            logging.DEBUG,
            "Querying pending retries from retry queue",
            max_retries=self._max_retries,
            min_age_seconds=self._min_age,
            batch_limit=self._batch_size,
        )

        with log_phase(logger, "get_pending_retries"):
            try:
                retry_df = self.retry_writer.get_pending_retries(
                    max_retries=self._max_retries,
                    min_age_seconds=self._min_age,
                    limit=self._batch_size,
                )
            except Exception as e:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Could not read retry queue, treating as empty",
                    error_message=str(e),
                )
                retry_df = pl.DataFrame()

        obs_config = self.config.observability
        log_memory_checkpoint(
            logger, "after_pending_read", config=obs_config, df=retry_df
        )

        if not retry_df.is_empty():
            # Log retry count distribution
            if "retry_count" in retry_df.columns:
                retry_count_dist = retry_df.group_by("retry_count").len().to_dicts()
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Retry count distribution",
                    distribution=retry_count_dist,
                )

        log_with_context(
            logger,
            logging.DEBUG,
            "Retrieved pending retries",
            count=len(retry_df),
        )

        return retry_df

    def _process_retry_batch(self, pending: pl.DataFrame) -> Dict:
        """
        Process failed attachments eligible for retry.

        Validates URLs, checks expiration, downloads batch concurrently,
        and uploads to OneLake.
        """
        batch_result = BatchResult()
        obs_config = self.config.observability

        # Build retry counts map
        retry_counts: Dict[Tuple[str, str], int] = {
            (row["trace_id"], row["attachment_url"]): row["retry_count"] or 0
            for row in pending.iter_rows(named=True)
        }

        # Build tasks from retry records, filtering out invalid/expired URLs
        tasks = []
        invalid_url_results = []
        expired_url_results = []

        log_with_context(
            logger,
            logging.DEBUG,
            "Building tasks from retry records",
            total_records=len(pending),
        )

        for row in pending.iter_rows(named=True):
            attachment_url = row["attachment_url"]
            current_retry_count = row["retry_count"] or 0

            # Generate blob_path from available fields (not stored in retry queue)
            blob_path, file_type = generate_blob_path(
                event_type=row["status_subtype"],
                trace_id=row["trace_id"],
                assignment_id=row["assignment_id"],
                download_url=attachment_url,
            )

            task = Task(
                trace_id=row["trace_id"],
                attachment_url=attachment_url,
                blob_path=blob_path,
                status_subtype=row["status_subtype"],
                file_type=file_type,
                assignment_id=row["assignment_id"],
                retry_count=current_retry_count,
            )

            url_info = check_presigned_url(attachment_url)
            if url_info.is_expired:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Skipping expired URL on retry",
                    url_type=url_info.url_type,
                    expired_at=(
                        url_info.expires_at.isoformat() if url_info.expires_at else None
                    ),
                    retry_count=current_retry_count,
                    **extract_log_context(task),
                )
                expired_url_results.append(
                    DownloadResult(
                        task=task,
                        success=False,
                        error="URL expired - cannot refresh Xact presigned URLs",
                        error_category=ErrorCategory.PERMANENT,
                        is_retryable=False,
                    )
                )
                continue

            is_valid, validation_error = validate_download_url(attachment_url)

            if not is_valid:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Blocked unsafe URL for retry",
                    error_category="ssrf_blocked",
                    error_message=validation_error,
                    retry_count=current_retry_count,
                    **extract_log_context(task),
                )
                invalid_url_results.append(
                    DownloadResult(
                        task=task,
                        success=False,
                        error=f"URL validation failed: {validation_error}",
                        error_category=ErrorCategory.PERMANENT,
                        is_retryable=False,
                    )
                )
            else:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "URL validation passed",
                    retry_count=current_retry_count,
                    **extract_log_context(task),
                )
                tasks.append(task)

        log_with_context(
            logger,
            logging.DEBUG,
            "Task building complete",
            valid_tasks=len(tasks),
            expired_urls=len(expired_url_results),
            invalid_urls=len(invalid_url_results),
        )

        if not tasks and not invalid_url_results and not expired_url_results:
            return batch_result

        log_with_context(
            logger,
            logging.INFO,
            "Processing retry attachments",
            batch_size=len(tasks),
            blocked_by_validation=len(invalid_url_results),
            expired_urls=len(expired_url_results),
        )

        results = []

        if tasks:
            with self.onelake_client:
                results = run_async_with_shutdown(
                    download_batch(
                        tasks=tasks,
                        onelake_client=self.onelake_client,
                        max_concurrent=self._max_concurrent,
                        timeout=self.config.download.timeout_seconds,
                        download_breaker=self._download_breaker,
                        upload_breaker=self._upload_breaker,
                    )
                )

        log_memory_checkpoint(logger, "after_processing", config=obs_config)

        all_results = results + invalid_url_results + expired_url_results

        self._retry_counts = retry_counts
        self._all_results = all_results

        for r in all_results:
            batch_result.add_result(r)

        return {
            "total": batch_result.total,
            "succeeded": batch_result.succeeded,
            "failed": batch_result.failed,
            "failed_permanent": batch_result.failed_permanent,
            "failed_transient": batch_result.failed_transient,
            "metadata": {
                "circuit_download": self._download_breaker.get_diagnostics(),
                "circuit_upload": self._upload_breaker.get_diagnostics(),
            },
        }

    def _write_retry_results(self, result: Dict, pending: pl.DataFrame) -> None:
        """Write tracking records for retry results."""
        exhausted_count = self._write_tracking_records(
            self._all_results, self._retry_counts
        )
        self._log_summary_from_dict(result, exhausted_count)

    def _write_tracking_records(
        self,
        results: List[DownloadResult],
        retry_counts: Dict[Tuple[str, str], int],
    ) -> int:
        """
        Write tracking records using split-table architecture.

        Success → inventory + DELETE from retry queue
        Exhausted/Permanent → DELETE from retry queue only
        Still transient → UPDATE retry queue (increment retry_count)
        """
        if not results:
            return 0

        inventory_rows = []
        retry_update_rows = []
        delete_keys = []
        exhausted_count = 0
        status_transitions = {
            "success": 0,
            "still_transient": 0,
            "now_permanent": 0,
            "exhausted": 0,
        }

        for result in results:
            task = result.task
            key = (task.trace_id, task.attachment_url)
            current_retry_count = retry_counts.get(key, task.retry_count) or 0
            new_retry_count = current_retry_count + 1

            if result.success:
                status_transitions["success"] += 1

                row = task.to_tracking_row(
                    status=TaskStatus.COMPLETED,
                    http_status=result.http_status,
                    error_message=None,
                )
                inventory_rows.append(row)
                delete_keys.append(
                    {"trace_id": task.trace_id, "attachment_url": task.attachment_url}
                )

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Retry succeeded - moving to inventory",
                    retry_count=new_retry_count,
                    attempts_total=new_retry_count,
                    **extract_log_context(result),
                )

            elif not result.is_retryable:
                status_transitions["now_permanent"] += 1
                delete_keys.append(
                    {"trace_id": task.trace_id, "attachment_url": task.attachment_url}
                )

                log_with_context(
                    logger,
                    logging.WARNING,
                    "Retry revealed permanent failure - removing from queue",
                    retry_count=new_retry_count,
                    error_category=(
                        result.error_category.value if result.error_category else None
                    ),
                    error=result.error[:200] if result.error else None,
                    **extract_log_context(result),
                )

            else:
                if self.is_retry_exhausted(new_retry_count):
                    exhausted_count += 1
                    status_transitions["exhausted"] += 1
                    delete_keys.append(
                        {"trace_id": task.trace_id, "attachment_url": task.attachment_url}
                    )

                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Max retries exhausted - removing from queue",
                        retry_count=new_retry_count,
                        max_retries=self._max_retries,
                        last_error=result.error[:200] if result.error else None,
                        trace_id=task.trace_id,
                        **extract_log_context(result),
                    )
                else:
                    status_transitions["still_transient"] += 1

                    row = task.to_tracking_row(
                        status=TaskStatus.FAILED,
                        http_status=result.http_status,
                        error_message=result.error,
                    )
                    row["retry_count"] = new_retry_count
                    row["error_category"] = (
                        result.error_category.value if result.error_category else None
                    )
                    retry_update_rows.append(row)

                    next_backoff = self.calculate_backoff_seconds(new_retry_count)
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Retry failed - updating queue",
                        retry_count=new_retry_count,
                        max_retries=self._max_retries,
                        retries_remaining=self._max_retries - new_retry_count,
                        next_retry_after_seconds=round(next_backoff, 1),
                        **extract_log_context(result),
                    )

        log_with_context(
            logger,
            logging.DEBUG,
            "Writing split-table retry records",
            total_records=len(results),
            inventory_rows=len(inventory_rows),
            retry_updates=len(retry_update_rows),
            deletes=len(delete_keys),
            status_transitions=status_transitions,
        )

        if inventory_rows:
            self.inventory_writer.write_inventory(inventory_rows)

        if retry_update_rows:
            self.retry_writer.write_retry(
                rows=retry_update_rows,
                max_retries=self._max_retries,
                backoff_base_seconds=self._backoff_base,
                backoff_multiplier=self._backoff_mult,
            )

        if delete_keys:
            self.retry_writer.delete_by_keys(delete_keys)

        return exhausted_count

    def _log_summary_from_dict(self, result: Dict, exhausted_count: int) -> None:
        """Log retry batch summary from result dict."""
        log_with_context(
            logger,
            logging.INFO,
            "Retry batch complete",
            records_processed=result["total"],
            records_succeeded=result["succeeded"],
            records_failed=result["failed"],
            failed_permanent=result.get("failed_permanent", 0),
            failed_transient=result.get("failed_transient", 0),
            exhausted_max_retries=exhausted_count,
        )

    def _cleanup_after_execution(self, stage_result: StageResult) -> None:
        """Post-execution cleanup and finalization."""
        self.memory_monitor.snapshot("XACT Retry - Stage complete")

        obs_config = self.config.observability
        log_memory_checkpoint(logger, "stage_end", config=obs_config)

        log_with_context(
            logger,
            logging.INFO,
            "Stage complete",
            stage="retry",
            status="success",
            records_processed=stage_result.records_processed,
            records_succeeded=stage_result.records_succeeded,
            records_failed=stage_result.records_failed,
            duration_ms=stage_result.duration_seconds * 1000,
        )

        self.inventory_writer.end_cycle()
        self.retry_writer.end_cycle()

    def _is_empty_batch(self, batch: pl.DataFrame) -> bool:
        """Check if batch is empty."""
        if batch is None:
            return True
        if hasattr(batch, "is_empty"):
            return batch.is_empty()
        if hasattr(batch, "__len__"):
            return len(batch) == 0
        return False

    def calculate_backoff_seconds(self, retry_count: int) -> float:
        """
        Calculate exponential backoff wait time.

        Formula: backoff_base * (backoff_multiplier ** retry_count)
        For retry_count=0, uses min_retry_age.
        """
        if retry_count == 0:
            return float(self._min_age)

        return self._backoff_base * (self._backoff_mult**retry_count)

    def is_retry_exhausted(self, retry_count: int) -> bool:
        """Check if retry count has reached max_retries."""
        return retry_count >= self._max_retries

    def get_stage_name(self) -> str:
        """Get unique identifier for this stage."""
        return "retry"

    def get_circuit_status(self) -> dict:
        """Get circuit breaker status for health checks."""
        return {
            "download": self._download_breaker.get_diagnostics(),
            "upload": self._upload_breaker.get_diagnostics(),
        }

    def get_retry_statistics(self) -> dict:
        """Get retry queue statistics."""
        return self.retry_writer.get_queue_statistics()

    def close(self) -> None:
        """Clean up stage resources."""
        log_with_context(logger, logging.DEBUG, "Closing RetryStage resources")
        if self.onelake_client:
            self.onelake_client.close()
