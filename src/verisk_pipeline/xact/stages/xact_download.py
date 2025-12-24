"""
Download stage: Delta table -> OneLake files.

Reads events with attachments, downloads files, and tracks status.
"""

import asyncio
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import aiofiles
import aiohttp
import polars as pl

from verisk_pipeline.common.auth import get_storage_options
from verisk_pipeline.common.circuit_breaker import (
    get_circuit_breaker,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
    CircuitBreaker,
)
from verisk_pipeline.common.config.xact import PipelineConfig
from verisk_pipeline.common.exceptions import ErrorCategory
from verisk_pipeline.common.logging.context_managers import log_phase, StageLogContext
from verisk_pipeline.common.logging.decorators import extract_log_context
from verisk_pipeline.common.logging.setup import get_logger, generate_cycle_id
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_memory_checkpoint,
    log_with_context,
)
from verisk_pipeline.common.memory import MemoryMonitor
from verisk_pipeline.common.security import (
    sanitize_error_message,
    sanitize_url,
    validate_download_url,
)
from verisk_pipeline.common.url_expiration import check_presigned_url
from verisk_pipeline.storage.delta import EventsTableReader
from verisk_pipeline.storage.inventory_writer import InventoryTableWriter
from verisk_pipeline.storage.onelake import OneLakeClient
from verisk_pipeline.storage.retry_queue_writer import RetryQueueWriter
from verisk_pipeline.storage.watermark import WatermarkManager
from verisk_pipeline.storage.watermark_helpers import WatermarkSession
from verisk_pipeline.xact.services.path_resolver import generate_blob_path
from verisk_pipeline.xact.xact_models import (
    DownloadResult,
    StageResult,
    StageStatus,
    Task,
    TaskStatus,
    XACT_PRIMARY_KEYS,
)

logger = get_logger(__name__)


class DownloadStage:
    """
    Download stage: Delta table -> OneLake files.

    Processes NEW events only. Records detailed failure info.
    Retry logic handled by separate retry stage.
    """

    # Download configuration constants
    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
    STREAM_THRESHOLD = 50 * 1024 * 1024  # Stream files > 50MB

    def __init__(self, config: PipelineConfig):
        """Initialize download stage."""
        self.config = config

        self.events_reader = EventsTableReader(
            f"{config.lakehouse.abfss_path}/{config.lakehouse.events_table}"
        )
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
            config.lakehouse.files_path,
            max_pool_size=config.download.max_concurrent + 5,
        )

        self.download_breaker = get_circuit_breaker(
            "download_download",
            EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
        )
        self.upload_breaker = get_circuit_breaker(
            "download_upload",
            ONELAKE_CIRCUIT_CONFIG,
        )

        self.watermark = WatermarkManager("download_attachments")

        self.memory_monitor = MemoryMonitor(
            enabled=config.observability.memory_profiling_enabled
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "DownloadStage initialized (flattened implementation)",
            events_table=f"{config.lakehouse.abfss_path}/{config.lakehouse.events_table}",
            inventory_table=config.lakehouse.attachments_table_path,
            retry_table=config.lakehouse.retry_table_path,
            files_path=config.lakehouse.files_path,
            max_concurrent=config.download.max_concurrent,
        )

    def get_stage_name(self) -> str:
        """Get stage name for logging."""
        return "download"

    def run(self) -> StageResult:
        """Main execution entry point for download workflow."""
        cycle_id = generate_cycle_id()
        start = datetime.now(timezone.utc)

        with StageLogContext(self.get_stage_name(), cycle_id=cycle_id):
            self.memory_monitor.snapshot("Stage start")

            # Log circuit breaker status
            log_with_context(
                logger,
                logging.DEBUG,
                "Circuit breaker status at stage start",
                download_circuit=self.download_breaker.get_diagnostics(),
                upload_circuit=self.upload_breaker.get_diagnostics(),
            )

            try:
                # Watermark session handles retrieval and updates
                with WatermarkSession(
                    self.watermark,
                    lookback_days=self.config.processing.lookback_days,
                    timestamp_column="_max_ingested_at",
                    logger=logger,
                ) as wm_session:

                    with log_phase(logger, "query_events"):
                        items_df = self._query_events(wm_session.start_watermark)

                    log_memory_checkpoint(
                        logger,
                        "after_query",
                        config=self.config.observability,
                        df=items_df,
                    )

                    if items_df.is_empty():
                        log_with_context(logger, logging.DEBUG, "No items to download")
                        return self._build_empty_result(start)

                    with log_phase(logger, "build_tasks"):
                        tasks = self._build_tasks(items_df, wm_session)

                    if not tasks:
                        log_with_context(logger, logging.DEBUG, "No valid tasks to process")
                        return self._build_empty_result(start)

                    log_with_context(
                        logger,
                        logging.INFO,
                        "Processing downloads",
                        batch_size=len(tasks),
                    )

                    with self.onelake_client:
                        results = asyncio.run(self._download_batch(tasks))

                    log_memory_checkpoint(
                        logger,
                        "after_downloads",
                        config=self.config.observability,
                    )

                    with log_phase(logger, "write_tracking"):
                        self._write_tracking_records(results)

                    self.memory_monitor.snapshot("Stage complete")
                    return self._build_result(results, start)

            except Exception as e:
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                log_exception(
                    logger,
                    e,
                    "Stage failed",
                    stage=self.get_stage_name(),
                    duration_ms=duration * 1000,
                )
                log_memory_checkpoint(
                    logger, "stage_error", config=self.config.observability
                )

                return StageResult(
                    stage_name=self.get_stage_name(),
                    status=StageStatus.FAILED,
                    duration_seconds=duration,
                    error=str(e),
                )

    # Columns required for download task building - project early for memory efficiency
    REQUIRED_COLUMNS = [
        "trace_id",
        "status_subtype",
        "assignment_id",
        "attachments",
        "estimate_version",
        "ingested_at",
    ]

    def _query_events(self, watermark: datetime) -> pl.DataFrame:
        """Query events with attachments, time-bounded to lookback window."""
        log_with_context(
            logger,
            logging.DEBUG,
            "Querying events table",
            watermark=watermark.isoformat(),
            lookback_days=self.config.processing.lookback_days,
            status_subtypes=self.config.processing.status_subtypes,
            max_events_to_scan=self.config.processing.max_events_to_scan,
        )

        # Read events by status subtypes with memory optimizations:
        # - columns: Project only needed columns early (reduces memory 60-70%)
        # - require_attachments: Push attachments filter into Delta query
        # - order_by: Sort by ingested_at to process oldest first
        events_df = self.events_reader.read_by_status_subtypes(
            status_subtypes=self.config.processing.status_subtypes,
            watermark=watermark,
            limit=self.config.processing.max_events_to_scan,
            order_by="ingested_at",
            columns=self.REQUIRED_COLUMNS,
            require_attachments=True,
        )

        if events_df.is_empty():
            log_with_context(
                logger,
                logging.DEBUG,
                "No new events found since watermark",
                watermark=watermark.isoformat(),
            )
            return events_df

        log_with_context(
            logger,
            logging.DEBUG,
            "Events with attachments read from table",
            event_count=len(events_df),
        )

        # Capture max timestamp for watermark update
        max_ts = events_df.select(pl.col("ingested_at").max()).item()

        # Explode comma-separated attachments into individual rows
        events_df = (
            events_df.with_columns(
                pl.col("attachments").str.split(",").alias("attachment_list")
            )
            .explode("attachment_list")
            .with_columns(
                pl.col("attachment_list").str.strip_chars().alias("attachment_url")
            )
            .drop(["attachments", "attachment_list"])
            .filter(pl.col("attachment_url") != "")
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "Attachments exploded",
            total_attachment_urls=len(events_df),
        )

        # Add max timestamp column for watermark update
        events_df = events_df.with_columns(pl.lit(max_ts).alias("_max_ingested_at"))

        # Exclude already processed URLs (with time-based optimization)
        events_df = self._filter_processed_urls(events_df)

        # Limit to batch size
        final_df = events_df.head(self.config.processing.batch_size)

        log_with_context(
            logger,
            logging.DEBUG,
            "Unprocessed attachments ready",
            batch_size=len(final_df),
        )

        return final_df

    def _filter_processed_urls(self, events_df: pl.DataFrame) -> pl.DataFrame:
        trace_ids = events_df["trace_id"].unique().to_list()
        if not trace_ids:
            return events_df

        try:
            # Time-bound the query to lookback window
            lookback_cutoff = datetime.now(timezone.utc) - timedelta(
                days=self.config.processing.lookback_days
            )

            log_with_context(
                logger,
                logging.DEBUG,
                "Checking tracking table for processed URLs",
                trace_id_count=len(trace_ids),
                lookback_cutoff=lookback_cutoff.isoformat(),
            )

            # Query tracking table with filter pushdown
            processed = (
                pl.scan_delta(
                    f"{self.config.lakehouse.abfss_path}/{self.config.lakehouse.tracking_table}",
                    storage_options=get_storage_options(),
                )
                .filter(
                    pl.col("trace_id").is_in(trace_ids)
                    & (pl.col("created_at") >= pl.lit(lookback_cutoff))
                )
                .select(["trace_id", "attachment_url", "created_at"])
                .collect()
            )

            log_with_context(
                logger,
                logging.DEBUG,
                "Tracking table query complete",
                processed_records_found=len(processed),
            )

            if not processed.is_empty():
                # Get latest per (trace_id, attachment_url)
                processed = (
                    processed.sort("created_at", descending=True)
                    .group_by(["trace_id", "attachment_url"])
                    .first()
                )

                before_filter = len(events_df)
                events_df = events_df.join(
                    processed.select(["trace_id", "attachment_url"]),
                    on=["trace_id", "attachment_url"],
                    how="anti",
                )

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Filtered out already processed URLs",
                    before_filter=before_filter,
                    after_filter=len(events_df),
                    filtered_out=before_filter - len(events_df),
                )

        except Exception as e:
            log_exception(
                logger,
                e,
                "Could not check processed status",
                level=logging.WARNING,
                include_traceback=False,
            )

        return events_df

    def _build_tasks(
        self, items_df: pl.DataFrame, wm_session: WatermarkSession
    ) -> List[Task]:
        """Build download tasks with expiration checking and blob path generation."""
        tasks = []
        expired_results = []
        expiring_soon_count = 0

        log_with_context(
            logger,
            logging.DEBUG,
            "Building tasks from events",
            event_count=len(items_df),
        )

        for row in items_df.iter_rows(named=True):
            # Extract row data
            trace_id = row["trace_id"]
            status_subtype = row["status_subtype"]
            assignment_id = row["assignment_id"]
            attachment_url = row["attachment_url"]
            estimate_version = row.get("estimate_version")

            # Check URL expiration
            url_info = check_presigned_url(attachment_url)

            # Generate blob path and file type
            blob_path, file_type = generate_blob_path(
                status_subtype=status_subtype,
                trace_id=trace_id,
                assignment_id=assignment_id,
                download_url=attachment_url,
                estimate_version=estimate_version,
            )

            # Create task object
            task = Task(
                trace_id=trace_id,
                attachment_url=attachment_url,
                blob_path=blob_path,
                status_subtype=status_subtype,
                file_type=file_type,
                assignment_id=assignment_id,
                estimate_version=estimate_version,
            )

            # Handle expired URLs
            if url_info.is_expired:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Skipping expired URL",
                    url_type=url_info.url_type,
                    expired_at=(
                        url_info.expires_at.isoformat() if url_info.expires_at else None
                    ),
                    **extract_log_context(task),
                )
                expired_results.append(
                    DownloadResult(
                        task=task,
                        success=False,
                        error="URL expired before download attempt",
                        error_category=ErrorCategory.PERMANENT,
                        is_retryable=False,
                    )
                )
            elif url_info.expires_within(300):  # 5 minutes
                # Warn but still process
                expiring_soon_count += 1
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "URL expiring soon",
                    seconds_remaining=url_info.seconds_remaining,
                    **extract_log_context(task),
                )
                tasks.append(task)
            else:
                tasks.append(task)

        # Write tracking records for expired URLs
        if expired_results:
            log_with_context(
                logger,
                logging.INFO,
                "Writing tracking records for expired URLs",
                expired_count=len(expired_results),
            )
            self._write_tracking_records(expired_results, expired_at_ingest=True)

        # Update watermark (even if some/all URLs expired)
        if "_max_ingested_at" in items_df.columns:
            wm_session.update_from_dataframe(items_df)

        log_with_context(
            logger,
            logging.DEBUG,
            "Tasks built",
            total_events=len(items_df),
            valid_tasks=len(tasks),
            expired_urls=len(expired_results),
            expiring_soon=expiring_soon_count,
        )

        return tasks

    async def _download_batch(self, tasks: List[Task]) -> List[DownloadResult]:
        """
        Download batch of files concurrently with circuit breaker protection.

        Uses semaphore for concurrency control.
        """
        if not tasks:
            return []

        max_concurrent = self.config.download.max_concurrent

        log_with_context(
            logger,
            logging.DEBUG,
            "Starting batch download",
            batch_size=len(tasks),
            max_concurrent=max_concurrent,
        )

        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_download(task: Task) -> DownloadResult:
            async with semaphore:
                return await self._download_single(task)

        # Create HTTP session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=max_concurrent,
            limit_per_host=max_concurrent,
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            self._session = session  # Make available to _download_single
            coros = [bounded_download(task) for task in tasks]
            all_results = await asyncio.gather(*coros, return_exceptions=True)

        # Convert exceptions to result objects
        results = []
        for i, result in enumerate(all_results):
            if isinstance(result, Exception):
                task = tasks[i]
                log_exception(
                    logger,
                    result,
                    "Unhandled exception in download batch",
                    **extract_log_context(task),
                )
                results.append(
                    DownloadResult(
                        task=task,
                        success=False,
                        error=f"Unexpected error: {sanitize_error_message(str(result))}",
                        error_category=ErrorCategory.TRANSIENT,
                        is_retryable=True,
                    )
                )
            else:
                results.append(result)

        # Log batch summary
        succeeded = sum(1 for r in results if r.success)
        failed_permanent = sum(1 for r in results if not r.success and not r.is_retryable)
        failed_transient = sum(1 for r in results if not r.success and r.is_retryable)
        circuit_rejected = sum(
            1 for r in results if r.error_category == ErrorCategory.CIRCUIT_OPEN
        )

        log_with_context(
            logger,
            logging.INFO,
            "Batch download complete",
            batch_size=len(tasks),
            records_succeeded=succeeded,
            failed_permanent=failed_permanent,
            failed_transient=failed_transient,
            circuit_rejected=circuit_rejected if circuit_rejected else None,
        )

        return results

    async def _download_single(self, task: Task) -> DownloadResult:
        """
        Download single file and upload to OneLake.

        Handles URL validation, circuit breaker checks, HTTP download with
        streaming for large files, OneLake upload, and error classification.
        """
        is_valid, validation_error = validate_download_url(task.attachment_url)
        if not is_valid:
            log_with_context(
                logger,
                logging.WARNING,
                "Blocked unsafe URL",
                error_category="ssrf_blocked",
                error_message=validation_error,
                **extract_log_context(task),
            )
            return DownloadResult(
                task=task,
                success=False,
                error=f"URL validation failed: {validation_error}",
                error_category=ErrorCategory.PERMANENT,
                is_retryable=False,
            )

        # Check download circuit breaker
        if self.download_breaker.is_open:
            retry_after = self.download_breaker._get_retry_after()
            log_with_context(
                logger,
                logging.DEBUG,
                "Download circuit breaker open",
                retry_after_seconds=retry_after,
                **extract_log_context(task),
            )
            return DownloadResult(
                task=task,
                success=False,
                error=f"Download circuit open, retry after {retry_after:.0f}s",
                error_category=ErrorCategory.CIRCUIT_OPEN,
                is_retryable=True,
            )

        # Check upload circuit breaker
        if self.upload_breaker.is_open:
            retry_after = self.upload_breaker._get_retry_after()
            log_with_context(
                logger,
                logging.DEBUG,
                "Upload circuit breaker open",
                retry_after_seconds=retry_after,
                **extract_log_context(task),
            )
            return DownloadResult(
                task=task,
                success=False,
                error=f"Upload circuit open, retry after {retry_after:.0f}s",
                error_category=ErrorCategory.CIRCUIT_OPEN,
                is_retryable=True,
            )

        tmp_path = None
        download_start = datetime.now(timezone.utc)

        try:
            # Download from source (disable redirects for security)
            async with self._session.get(
                task.attachment_url,
                timeout=aiohttp.ClientTimeout(total=self.config.download.timeout_seconds),
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    error_msg, error_category, is_retryable = self._classify_http_error(
                        response.status, task.attachment_url
                    )
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Download failed",
                        http_status=response.status,
                        error_category=error_category.value,
                        **extract_log_context(task),
                    )
                    self.download_breaker.record_failure(Exception(error_msg))
                    return DownloadResult(
                        task=task,
                        success=False,
                        http_status=response.status,
                        error=error_msg,
                        error_category=error_category,
                        is_retryable=is_retryable,
                    )

                content_length = response.content_length or 0

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Download response received",
                    http_status=200,
                    content_length=content_length,
                    streaming=(content_length > self.STREAM_THRESHOLD),
                    **extract_log_context(task),
                )

                # Choose streaming vs in-memory based on size
                if content_length > self.STREAM_THRESHOLD:
                    # Stream large files to temp file
                    tmp_path = tempfile.mktemp(suffix=f".{task.file_type}")
                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(self.CHUNK_SIZE):
                            await f.write(chunk)
                    self.onelake_client.upload_file(task.blob_path, tmp_path)
                else:
                    # Small files: read into memory
                    content = await response.read()
                    self.onelake_client.upload_bytes(task.blob_path, content)

                # Record success with circuit breakers
                self.download_breaker.record_success()
                self.upload_breaker.record_success()

                download_duration_ms = (
                    datetime.now(timezone.utc) - download_start
                ).total_seconds() * 1000

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Download and upload complete",
                    bytes_downloaded=content_length,
                    blob_path=task.blob_path,
                    duration_ms=round(download_duration_ms, 2),
                    **extract_log_context(task),
                )

                return DownloadResult(
                    task=task,
                    success=True,
                    http_status=200,
                    bytes_downloaded=content_length,
                )

        except asyncio.TimeoutError:
            log_with_context(
                logger,
                logging.WARNING,
                "Download timeout",
                timeout_seconds=self.config.download.timeout_seconds,
                error_category=ErrorCategory.TRANSIENT.value,
                **extract_log_context(task),
            )
            self.download_breaker.record_failure(asyncio.TimeoutError("Download timeout"))
            return DownloadResult(
                task=task,
                success=False,
                error="Download timeout",
                error_category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        except aiohttp.ClientError as e:
            log_with_context(
                logger,
                logging.WARNING,
                "Connection error",
                error_category=ErrorCategory.TRANSIENT.value,
                error_message=sanitize_error_message(str(e)),
                **extract_log_context(task),
            )
            self.download_breaker.record_failure(e)
            return DownloadResult(
                task=task,
                success=False,
                error=f"Connection error: {sanitize_error_message(str(e))}",
                error_category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        except Exception as e:
            log_exception(
                logger,
                e,
                "Unexpected download error",
                **extract_log_context(task),
            )
            return DownloadResult(
                task=task,
                success=False,
                error=f"Unexpected error: {sanitize_error_message(str(e))}",
                error_category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _classify_http_error(
        self, status: int, url: str
    ) -> tuple[str, ErrorCategory, bool]:
        """Classify HTTP error for retry logic."""
        safe_url = sanitize_url(url)

        if status == 404:
            return (f"Not found (404): {safe_url}", ErrorCategory.PERMANENT, False)
        if status == 403:
            return (f"Forbidden (403): {safe_url}", ErrorCategory.TRANSIENT, True)
        if status == 401:
            return (f"Unauthorized (401): {safe_url}", ErrorCategory.TRANSIENT, True)
        if status == 429:
            return (f"Rate limited (429): {safe_url}", ErrorCategory.TRANSIENT, True)
        if status in (500, 502, 503, 504):
            return (
                f"Server error ({status}): {safe_url}",
                ErrorCategory.TRANSIENT,
                True,
            )
        if 400 <= status < 500:
            return (
                f"Client error ({status}): {safe_url}",
                ErrorCategory.PERMANENT,
                False,
            )

        return (f"HTTP error ({status}): {safe_url}", ErrorCategory.TRANSIENT, True)

    def _write_tracking_records(
        self,
        results: List[DownloadResult],
        expired_at_ingest: bool = False,
    ) -> None:
        """
        Write tracking records using split-table architecture.

        Success → inventory table
        Transient failure → retry queue
        Permanent failure → logged only
        """
        if not results:
            return

        inventory_rows = []
        retry_rows = []
        status_counts = {"completed": 0, "failed": 0, "failed_permanent": 0}

        for result in results:
            task = result.task

            if result.success:
                # SUCCESS → Inventory table (no status/error fields)
                status_counts["completed"] += 1

                row = task.to_tracking_row(
                    status=TaskStatus.COMPLETED,
                    http_status=result.http_status,
                    error_message=None,
                )
                inventory_rows.append(row)

            elif not result.is_retryable:
                status_counts["failed_permanent"] += 1
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Permanent failure - not retryable",
                    trace_id=task.trace_id,
                    attachment_url=task.attachment_url,
                    error=result.error,
                    error_category=(
                        result.error_category.value if result.error_category else None
                    ),
                )

            else:
                status_counts["failed"] += 1

                row = task.to_tracking_row(
                    status=TaskStatus.FAILED,
                    http_status=result.http_status,
                    error_message=(
                        sanitize_error_message(result.error) if result.error else None
                    ),
                )
                row["error_category"] = (
                    result.error_category.value if result.error_category else None
                )
                row["retry_count"] = 0

                url_info = check_presigned_url(task.attachment_url)
                row["expires_at"] = (
                    url_info.expires_at.isoformat() if url_info.expires_at else None
                )
                row["expired_at_ingest"] = expired_at_ingest

                retry_rows.append(row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Writing split-table records",
            total_records=len(results),
            inventory_rows=len(inventory_rows),
            retry_rows=len(retry_rows),
            completed=status_counts["completed"],
            failed=status_counts["failed"],
            failed_permanent=status_counts["failed_permanent"],
        )

        if inventory_rows:
            self.inventory_writer.write_inventory(inventory_rows)

        if retry_rows:
            self.retry_writer.write_retry(
                rows=retry_rows,
                max_retries=self.config.retry.max_retries,
                backoff_base_seconds=self.config.retry.backoff_base_seconds,
                backoff_multiplier=self.config.retry.backoff_multiplier,
            )

    def _build_result(
        self, results: List[DownloadResult], start: datetime
    ) -> StageResult:
        """Build StageResult from download results."""
        duration = (datetime.now(timezone.utc) - start).total_seconds()

        records_processed = len(results)
        records_succeeded = sum(1 for r in results if r.success)
        records_failed = sum(1 for r in results if not r.success)
        records_failed_permanent = sum(
            1 for r in results if not r.success and not r.is_retryable
        )

        log_with_context(
            logger,
            logging.INFO,
            "Stage complete",
            stage=self.get_stage_name(),
            status="success",
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
            records_failed_permanent=records_failed_permanent,
            duration_ms=duration * 1000,
        )

        return StageResult(
            stage_name=self.get_stage_name(),
            status=StageStatus.SUCCESS,
            duration_seconds=duration,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
            metadata={
                "circuit_download": self.download_breaker.get_diagnostics(),
                "circuit_upload": self.upload_breaker.get_diagnostics(),
                "failed_permanent": records_failed_permanent,
            },
        )

    def _build_empty_result(self, start: datetime) -> StageResult:
        """Build empty StageResult when no items to process."""
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        return StageResult(
            stage_name=self.get_stage_name(),
            status=StageStatus.SUCCESS,
            duration_seconds=duration,
            records_processed=0,
            records_succeeded=0,
            records_failed=0,
        )

    def close(self) -> None:
        """Clean up resources."""
        log_with_context(
            logger,
            logging.DEBUG,
            "Closing DownloadStage resources",
        )
        if self.onelake_client:
            self.onelake_client.close()

    def get_circuit_status(self) -> dict:
        """Get circuit breaker status for health checks."""
        return {
            "download": self.download_breaker.get_diagnostics(),
            "upload": self.upload_breaker.get_diagnostics(),
        }


async def download_single(
    task: Task,
    session: aiohttp.ClientSession,
    onelake_client: OneLakeClient,
    timeout: int,
    download_breaker: CircuitBreaker,
    upload_breaker: CircuitBreaker,
) -> DownloadResult:
    """
    Download single attachment and upload to OneLake.

    Standalone utility function used by both DownloadStage and RetryStage.
    """
    CHUNK_SIZE = 8 * 1024 * 1024
    STREAM_THRESHOLD = 50 * 1024 * 1024

    is_valid, validation_error = validate_download_url(task.attachment_url)
    if not is_valid:
        log_with_context(
            logger,
            logging.WARNING,
            "Blocked unsafe URL",
            error_category="ssrf_blocked",
            error_message=validation_error,
            **extract_log_context(task),
        )
        return DownloadResult(
            task=task,
            success=False,
            error=f"URL validation failed: {validation_error}",
            error_category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    # Check download circuit breaker
    if download_breaker.is_open:
        retry_after = download_breaker._get_retry_after()
        log_with_context(
            logger,
            logging.DEBUG,
            "Download circuit breaker open",
            retry_after_seconds=retry_after,
            **extract_log_context(task),
        )
        return DownloadResult(
            task=task,
            success=False,
            error=f"Download circuit open, retry after {retry_after:.0f}s",
            error_category=ErrorCategory.CIRCUIT_OPEN,
            is_retryable=True,
        )

    # Check upload circuit breaker
    if upload_breaker.is_open:
        retry_after = upload_breaker._get_retry_after()
        log_with_context(
            logger,
            logging.DEBUG,
            "Upload circuit breaker open",
            retry_after_seconds=retry_after,
            **extract_log_context(task),
        )
        return DownloadResult(
            task=task,
            success=False,
            error=f"Upload circuit open, retry after {retry_after:.0f}s",
            error_category=ErrorCategory.CIRCUIT_OPEN,
            is_retryable=True,
        )

    tmp_path = None
    download_start = datetime.now(timezone.utc)

    try:
        # Download from source (disable redirects for security)
        async with session.get(
            task.attachment_url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=False,
        ) as response:
            if response.status != 200:
                # Xact URLs cannot be refreshed, so most client errors are permanent.
                # Only server errors (5xx) and rate limits (429) are transient.
                error_category = ErrorCategory.TRANSIENT
                is_retryable = True
                if response.status in (400, 401, 403, 404, 410):
                    error_category = ErrorCategory.PERMANENT
                    is_retryable = False

                log_with_context(
                    logger,
                    logging.WARNING,
                    "Download failed",
                    http_status=response.status,
                    error_category=error_category.value,
                    **extract_log_context(task),
                )
                download_breaker.record_failure(Exception(f"HTTP {response.status}"))
                return DownloadResult(
                    task=task,
                    success=False,
                    http_status=response.status,
                    error=f"HTTP error: {response.status}",
                    error_category=error_category,
                    is_retryable=is_retryable,
                )

            content_length = response.content_length or 0

            log_with_context(
                logger,
                logging.DEBUG,
                "Download response received",
                http_status=200,
                content_length=content_length,
                streaming=(content_length > STREAM_THRESHOLD),
                **extract_log_context(task),
            )

            if content_length > STREAM_THRESHOLD:
                # Stream large files to temp file
                tmp_path = tempfile.mktemp(suffix=f".{task.file_type}")
                async with aiofiles.open(tmp_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        await f.write(chunk)
                onelake_client.upload_file(task.blob_path, tmp_path)
            else:
                # Small files: read into memory
                content = await response.read()
                onelake_client.upload_bytes(task.blob_path, content)

            download_breaker.record_success()
            upload_breaker.record_success()

            download_duration_ms = (
                datetime.now(timezone.utc) - download_start
            ).total_seconds() * 1000

            log_with_context(
                logger,
                logging.DEBUG,
                "Download and upload complete",
                bytes_downloaded=content_length,
                blob_path=task.blob_path,
                duration_ms=round(download_duration_ms, 2),
                **extract_log_context(task),
            )

            return DownloadResult(
                task=task,
                success=True,
                http_status=200,
                bytes_downloaded=content_length,
            )

    except asyncio.TimeoutError:
        log_with_context(
            logger,
            logging.WARNING,
            "Download timeout",
            timeout_seconds=timeout,
            error_category=ErrorCategory.TRANSIENT.value,
            **extract_log_context(task),
        )
        download_breaker.record_failure(asyncio.TimeoutError("Download timeout"))
        return DownloadResult(
            task=task,
            success=False,
            error="Download timeout",
            error_category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )
    except aiohttp.ClientError as e:
        log_with_context(
            logger,
            logging.WARNING,
            "Connection error",
            error_category=ErrorCategory.TRANSIENT.value,
            error_message=sanitize_error_message(str(e)),
            **extract_log_context(task),
        )
        download_breaker.record_failure(e)
        return DownloadResult(
            task=task,
            success=False,
            error=f"Connection error: {sanitize_error_message(str(e))}",
            error_category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )
    except Exception as e:
        log_exception(
            logger,
            e,
            "Unexpected download error",
            **extract_log_context(task),
        )
        return DownloadResult(
            task=task,
            success=False,
            error=f"Unexpected error: {sanitize_error_message(str(e))}",
            error_category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


async def download_batch(
    tasks: List[Task],
    onelake_client: OneLakeClient,
    max_concurrent: int,
    timeout: int,
    download_breaker: CircuitBreaker,
    upload_breaker: CircuitBreaker,
) -> List[DownloadResult]:
    """
    Download batch of attachments concurrently.

    Standalone utility function used by both DownloadStage and RetryStage.
    """
    if not tasks:
        return []

    log_with_context(
        logger,
        logging.DEBUG,
        "Starting batch download",
        batch_size=len(tasks),
        max_concurrent=max_concurrent,
        timeout_seconds=timeout,
    )

    semaphore = asyncio.Semaphore(max_concurrent)

    async def bounded_download(
        task: Task, session: aiohttp.ClientSession
    ) -> DownloadResult:
        async with semaphore:
            return await download_single(
                task, session, onelake_client, timeout, download_breaker, upload_breaker
            )

    connector = aiohttp.TCPConnector(
        limit=max_concurrent, limit_per_host=max_concurrent
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        coros = [bounded_download(task, session) for task in tasks]
        all_results = await asyncio.gather(*coros, return_exceptions=True)

    results = []
    for i, result in enumerate(all_results):
        if isinstance(result, Exception):
            task = tasks[i]
            log_exception(
                logger,
                result,
                "Unhandled exception in download batch",
                **extract_log_context(task),
            )
            results.append(
                DownloadResult(
                    task=task,
                    success=False,
                    error=f"Unexpected error: {sanitize_error_message(str(result))}",
                    error_category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )
            )
        else:
            results.append(result)

    succeeded = sum(1 for r in results if r.success)
    failed_permanent = sum(1 for r in results if not r.success and not r.is_retryable)
    failed_transient = sum(1 for r in results if not r.success and r.is_retryable)
    circuit_rejected = sum(
        1 for r in results if r.error_category == ErrorCategory.CIRCUIT_OPEN
    )

    log_with_context(
        logger,
        logging.INFO,
        "Batch complete",
        batch_size=len(tasks),
        records_succeeded=succeeded,
        failed_permanent=failed_permanent,
        failed_transient=failed_transient,
        circuit_rejected=circuit_rejected if circuit_rejected else None,
    )

    return results
