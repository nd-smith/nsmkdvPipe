"""
Download stage: Media metadata -> OneLake files.

Reads media metadata with download URLs, downloads files, uploads to OneLake.
Supports automatic URL refresh for expired presigned URLs via ClaimX API.
"""

import asyncio
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import aiofiles
import aiohttp
import polars as pl

from verisk_pipeline.common.auth import get_storage_options
from verisk_pipeline.common.config.claimx import ClaimXConfig
from verisk_pipeline.common.exceptions import ErrorCategory
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.decorators import extract_log_context
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_with_context,
    log_memory_checkpoint,
)
from verisk_pipeline.common.logging.context_managers import StageLogContext
from verisk_pipeline.common.memory import MemoryMonitor
from verisk_pipeline.claimx.api_client import ClaimXApiClient
from verisk_pipeline.common.security import (
    sanitize_error_message,
    sanitize_filename,
    validate_download_url,
)
from verisk_pipeline.common.circuit_breaker import (
    CircuitBreaker,
    get_circuit_breaker,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
)
from verisk_pipeline.storage.onelake import OneLakeClient
from verisk_pipeline.common.url_expiration import check_presigned_url
from verisk_pipeline.claimx.claimx_models import (
    BatchResult,
    MediaDownloadResult,
    MediaTask,
    StageResult,
    StageStatus,
    CLAIMX_PRIMARY_KEYS,
)
from verisk_pipeline.storage.delta import DeltaTableReader
from verisk_pipeline.storage.inventory_writer import InventoryTableWriter
from verisk_pipeline.storage.retry_queue_writer import RetryQueueWriter
from verisk_pipeline.xact.xact_models import TaskStatus

logger = get_logger(__name__)


def generate_blob_path(
    project_id: str,
    media_id: str,
    file_type: str,
    file_name: Optional[str] = None,
) -> str:
    """
    Generate OneLake blob path for media file.

    Path format: claimx/attachments/{project_id}/{media_id}_{filename}
    """
    if file_name:
        filename = sanitize_filename(file_name)
    else:
        filename = f"media_{media_id}"

    ext = file_type.lower() if file_type else "bin"
    if not filename.lower().endswith(f".{ext}"):
        filename = f"{filename}.{ext}"

    return f"/claimx/attachments/{project_id}/{media_id}_{filename}"


class DownloadStage:
    """
    Download stage: Media metadata -> OneLake files.

    Reads media metadata with download URLs, downloads files, uploads to OneLake, and tracks status.
    Supports automatic URL refresh for expired presigned URLs via ClaimX API.
    """

    # Download configuration constants
    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
    STREAM_THRESHOLD = 50 * 1024 * 1024  # Stream files > 50MB

    def __init__(self, config: ClaimXConfig):
        """
        Initialize download stage with configuration.

        Args:
            config: ClaimX pipeline configuration
        """
        self.config = config
        self._media_reader = DeltaTableReader(
            config.lakehouse.table_path(config.lakehouse.media_metadata_table)
        )

        # Split-table architecture: separate inventory and retry writers
        self.inventory_writer = InventoryTableWriter(
            table_path=config.lakehouse.attachments_table_path,
            primary_keys=CLAIMX_PRIMARY_KEYS,
            storage_options=get_storage_options(),
        )

        self.retry_writer = RetryQueueWriter(
            table_path=config.lakehouse.retry_table_path,
            primary_keys=CLAIMX_PRIMARY_KEYS,
            storage_options=get_storage_options(),
        )

        # Initialize OneLake client
        self.onelake_client = OneLakeClient(
            config.lakehouse.files_path,
            max_pool_size=config.download.max_concurrent + 5,
        )

        # Initialize circuit breakers
        self._download_breaker = get_circuit_breaker(
            "media_download", EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG
        )
        self._upload_breaker = get_circuit_breaker(
            "onelake_upload", ONELAKE_CIRCUIT_CONFIG
        )

        # Store allowed domains if configured
        self._allowed_domains: Optional[Set[str]] = None
        if hasattr(config.download, "allowed_domains"):
            self._allowed_domains = set(config.download.allowed_domains)

        # Initialize memory monitor for production profiling
        self.memory_monitor = MemoryMonitor(
            enabled=config.observability.memory_profiling_enabled
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "ClaimX DownloadStage initialized (split-table architecture)",
            media_metadata_table=config.lakehouse.table_path(
                config.lakehouse.media_metadata_table
            ),
            attachments_table=config.lakehouse.attachments_table_path,
            retry_table=config.lakehouse.retry_table_path,
            files_path=config.lakehouse.files_path,
            max_concurrent=config.download.max_concurrent,
        )

    def run(self) -> StageResult:
        """
        Main execution entry point - LINEAR FLOW, NO PARENT CALLS.

        Execute complete download workflow:
        1. Query media metadata with download URLs
        2. Build tasks (with URL refresh if expired)
        3. Download files concurrently
        4. Upload to OneLake
        5. Write tracking records (split-table)
        6. Return stage result

        Returns:
            StageResult with execution metrics
        """
        cycle_id = generate_cycle_id()
        start = datetime.now(timezone.utc)

        with StageLogContext(self.get_stage_name(), cycle_id=cycle_id):
            try:
                # Memory checkpoint: Stage start
                self.memory_monitor.snapshot("ClaimX Download - Stage start")
                obs_config = self.config.observability
                log_memory_checkpoint(logger, "stage_start", config=obs_config)

                # Start write cycles
                self.inventory_writer.start_cycle(cycle_id)
                self.retry_writer.start_cycle(cycle_id)

                # Log circuit breaker status
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Circuit breaker status at stage start",
                    download_circuit=self._download_breaker.get_diagnostics(),
                    upload_circuit=self._upload_breaker.get_diagnostics(),
                )

                # Step 1: Get media pending download
                items_df = self._get_download_items()

                if items_df.is_empty():
                    log_with_context(
                        logger,
                        logging.INFO,
                        "No media pending download",
                    )
                    return self._build_empty_result(start)

                log_with_context(
                    logger,
                    logging.INFO,
                    "Processing media batch",
                    batch_size=len(items_df),
                )

                # Step 2: Build tasks (with URL refresh if expired)
                tasks = self._build_tasks(items_df)

                if not tasks:
                    log_with_context(
                        logger,
                        logging.INFO,
                        "No valid tasks after URL validation",
                    )
                    return self._build_empty_result(start)

                log_with_context(
                    logger,
                    logging.INFO,
                    "Tasks built successfully",
                    task_count=len(tasks),
                )

                # Memory checkpoint: After task building
                log_memory_checkpoint(logger, "after_task_build", config=obs_config)

                # Step 3: Download batch concurrently
                with self.onelake_client:
                    results = asyncio.run(
                        download_batch(
                            tasks=tasks,
                            onelake_client=self.onelake_client,
                            max_concurrent=self.config.download.max_concurrent,
                            timeout=self.config.download.timeout_seconds,
                            download_breaker=self._download_breaker,
                            upload_breaker=self._upload_breaker,
                            allowed_domains=self._allowed_domains,
                            proxy=getattr(self.config.download, "proxy", None),
                        )
                    )

                # Memory checkpoint: After download
                log_memory_checkpoint(logger, "after_download", config=obs_config)

                # Step 4: Write tracking records (split-table)
                self._write_tracking_records(results)

                # Memory checkpoint: After writing
                log_memory_checkpoint(logger, "after_write", config=obs_config)

                # Step 5: Build result with metrics
                stage_result = self._build_result(results, start)

                # Cleanup
                self._cleanup_after_execution(stage_result)

                return stage_result

            except Exception as e:
                # Error handling - inline, no parent call
                duration = (datetime.now(timezone.utc) - start).total_seconds()

                log_exception(
                    logger,
                    e,
                    "Stage 'download' failed",
                    stage="download",
                )

                # End cycles on error
                self.inventory_writer.end_cycle()
                self.retry_writer.end_cycle()

                return StageResult(
                    stage_name="download",
                    status=StageStatus.FAILED,
                    duration_seconds=duration,
                    started_at=start,
                    error=str(e),
                )

    def _get_download_items(self) -> pl.DataFrame:
        """
        Get media pending download.

        Queries media metadata table and excludes already downloaded items.

        Returns:
            DataFrame with media metadata (may be empty)
        """
        # Memory checkpoint: Getting download items
        self.memory_monitor.snapshot("ClaimX Download - Getting download items")

        try:
            # Read media metadata
            media_df = self._media_reader.read(
                columns=[
                    "media_id",
                    "project_id",
                    "file_type",
                    "file_name",
                    "full_download_link",
                    "source_event_id",
                    "created_at",
                ]
            )

            if media_df.is_empty():
                return media_df

            # Exclude already downloaded (inventory = success)
            try:
                completed_ids = self.inventory_writer.get_completed_ids()

                if completed_ids:
                    media_df = media_df.filter(
                        ~pl.col("media_id").cast(pl.Utf8).is_in(list(completed_ids))
                    )
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Could not check download status",
                    level=logging.WARNING,
                    include_traceback=False,
                )

            # Exclude items already in retry queue (let retry stage handle them)
            try:
                queued_ids = self.retry_writer.get_queued_ids()

                if queued_ids:
                    media_df = media_df.filter(
                        ~pl.col("media_id").cast(pl.Utf8).is_in(list(queued_ids))
                    )
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Could not check retry queue",
                    level=logging.WARNING,
                    include_traceback=False,
                )

            # Limit batch
            return media_df.head(self.config.processing.batch_size)

        except Exception as e:
            log_exception(logger, e, "Error reading pending media")
            return pl.DataFrame()

    def _build_tasks(self, items_df: pl.DataFrame) -> List[MediaTask]:
        """
        Convert DataFrame rows to MediaTask objects.

        Checks URL expiration - expired URLs trigger refresh attempt via ClaimX API.

        Args:
            items_df: DataFrame with media metadata

        Returns:
            List of valid tasks (with refreshed URLs if needed)
        """
        tasks = []
        expired_needing_refresh: List[Dict] = []

        for row in items_df.iter_rows(named=True):
            media_id = row.get("media_id")
            project_id = row.get("project_id")
            download_url = row.get("full_download_link")

            if not all([media_id, project_id, download_url]):
                continue

            # Check URL expiration
            url_info = check_presigned_url(download_url)

            if url_info.is_expired or url_info.expires_within(60):
                # Mark for refresh
                expired_needing_refresh.append(
                    {
                        "media_id": media_id,
                        "project_id": project_id,
                        "row": row,
                    }
                )
                continue

            file_type = row.get("file_type") or ""
            file_name = row.get("file_name") or ""

            blob_path = generate_blob_path(
                project_id=project_id,
                media_id=media_id,
                file_type=file_type,
                file_name=file_name,
            )

            task = MediaTask(
                media_id=media_id,
                project_id=project_id,
                download_url=download_url,
                blob_path=blob_path,
                file_type=file_type,
                file_name=file_name,
                source_event_id=row.get("source_event_id") or "",
                expires_at=(
                    url_info.expires_at.isoformat() if url_info.expires_at else None
                ),
            )
            tasks.append(task)

        # Handle expired URLs - attempt refresh via ClaimX API
        if expired_needing_refresh:
            refreshed_tasks = self._refresh_and_build_tasks(expired_needing_refresh)
            tasks.extend(refreshed_tasks)

        return tasks

    def _refresh_and_build_tasks(self, expired_items: List[Dict]) -> List[MediaTask]:
        """
        Refresh expired URLs via ClaimX API and build tasks.

        Args:
            expired_items: List of dicts with media_id, project_id, row

        Returns:
            List of MediaTask with fresh URLs
        """
        if not expired_items:
            return []

        # Group by project to minimize API calls
        by_project: Dict[str, List[Dict]] = {}
        for item in expired_items:
            pid = item["project_id"]
            if pid not in by_project:
                by_project[pid] = []
            by_project[pid].append(item)

        log_with_context(
            logger,
            logging.INFO,
            "Refreshing expired URLs",
            media_count=len(expired_items),
            project_count=len(by_project),
        )

        # Refresh URLs via ClaimX API
        fresh_urls = asyncio.run(self._fetch_fresh_urls(list(by_project.keys())))

        # Build tasks with fresh URLs
        tasks = []
        for item in expired_items:
            media_id = item["media_id"]
            project_id = item["project_id"]
            row = item["row"]

            fresh_url = fresh_urls.get(media_id)
            if not fresh_url:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Could not refresh URL for media",
                    media_id=media_id,
                    project_id=project_id,
                )
                continue

            url_info = check_presigned_url(fresh_url)
            file_type = row.get("file_type") or ""
            file_name = row.get("file_name") or ""

            blob_path = generate_blob_path(
                project_id=project_id,
                media_id=media_id,
                file_type=file_type,
                file_name=file_name,
            )

            task = MediaTask(
                media_id=media_id,
                project_id=project_id,
                download_url=fresh_url,
                blob_path=blob_path,
                file_type=file_type,
                file_name=file_name,
                source_event_id=row.get("source_event_id") or "",
                expires_at=(
                    url_info.expires_at.isoformat() if url_info.expires_at else None
                ),
                refresh_count=1,
            )
            tasks.append(task)

        log_with_context(
            logger,
            logging.DEBUG,
            "Refreshed URLs",
            tasks_created=len(tasks),
            tasks_failed=len(expired_items) - len(tasks),
        )

        return tasks

    async def _fetch_fresh_urls(self, project_ids: List[str]) -> Dict[str, str]:
        """
        Fetch fresh presigned URLs for projects via ClaimX API.

        Args:
            project_ids: Projects to refresh

        Returns:
            Dict mapping media_id -> fresh_url
        """
        fresh_urls: Dict[str, str] = {}

        client = ClaimXApiClient(
            base_url=self.config.api.base_url,
            auth_token=self.config.api.auth_token,
            timeout_seconds=self.config.api.timeout_seconds,
            max_concurrent=self.config.download.max_concurrent,
        )

        async with client:
            for project_id in project_ids:
                try:
                    media_list = await client.get_project_media(int(project_id))
                    if media_list:
                        for item in media_list:
                            mid = str(item.get("mediaID") or item.get("media_id") or "")
                            url = item.get("fullDownloadLink") or item.get(
                                "full_download_link"
                            )
                            if mid and url:
                                fresh_urls[mid] = url
                except Exception as e:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Could not refresh URLs for project",
                        project_id=project_id,
                        error_message=str(e),
                    )

        return fresh_urls

    def _write_tracking_records(self, results: List[MediaDownloadResult]) -> None:
        """
        Write download results to split tables.

        Split logic:
        - Success → inventory table (status/error fields auto-stripped)
        - Transient failure → retry queue (with error tracking, retry_count=0)
        - Permanent failure → log only (no table write)

        Args:
            results: List of download results
        """
        if not results:
            return

        inventory_rows = []
        retry_rows = []
        status_counts = {"completed": 0, "failed": 0, "failed_permanent": 0}

        for result in results:
            task = result.task

            if result.success:
                # SUCCESS → Inventory table (no status/error fields needed)
                status_counts["completed"] += 1
                row = task.to_tracking_row(
                    status=TaskStatus.COMPLETED,
                    http_status=result.http_status,
                    error_message=None,
                    bytes_downloaded=result.bytes_downloaded,
                )
                # Note: inventory_writer will auto-strip status/error fields
                inventory_rows.append(row)

            elif not result.is_retryable:
                # PERMANENT FAILURE → Log only (no table write)
                status_counts["failed_permanent"] += 1
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Permanent failure - not retryable",
                    media_id=task.media_id,
                    project_id=task.project_id,
                    error_category=(
                        result.error_category.value if result.error_category else None
                    ),
                    error_message=(
                        sanitize_error_message(result.error) if result.error else None
                    ),
                )

            else:
                # TRANSIENT FAILURE → Retry queue
                status_counts["failed"] += 1
                row = task.to_tracking_row(
                    status=TaskStatus.FAILED,
                    http_status=result.http_status,
                    error_message=(
                        sanitize_error_message(result.error) if result.error else None
                    ),
                    bytes_downloaded=result.bytes_downloaded,
                )
                row["error_category"] = (
                    result.error_category.value if result.error_category else None
                )
                row["retry_count"] = 0  # First attempt
                retry_rows.append(row)

        # Write to split tables
        if inventory_rows:
            self.inventory_writer.write_inventory(inventory_rows)

        if retry_rows:
            self.retry_writer.write_retry(
                rows=retry_rows,
                max_retries=self.config.retry.max_retries,
                backoff_base_seconds=self.config.retry.backoff_base_seconds,
                backoff_multiplier=self.config.retry.backoff_multiplier,
            )

        # Log summary
        log_with_context(
            logger,
            logging.INFO,
            "Download batch processing complete",
            **status_counts,
        )

    def _build_result(
        self, results: List[MediaDownloadResult], start: datetime
    ) -> StageResult:
        """
        Build stage result from download results.

        Args:
            results: List of download results
            start: Stage start time

        Returns:
            StageResult with metrics
        """
        batch_result = BatchResult()
        for r in results:
            batch_result.add_result(r)

        duration = (datetime.now(timezone.utc) - start).total_seconds()

        return StageResult(
            stage_name="download",
            status=StageStatus.SUCCESS,
            duration_seconds=duration,
            records_processed=batch_result.total,
            records_succeeded=batch_result.succeeded,
            records_failed=batch_result.failed,
            metadata={
                "circuit_download": self._download_breaker.get_diagnostics(),
                "circuit_upload": self._upload_breaker.get_diagnostics(),
            },
        )

    def _build_empty_result(self, start: datetime) -> StageResult:
        """
        Build empty result for no-work scenarios.

        Args:
            start: Stage start time

        Returns:
            StageResult with no records processed
        """
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        return StageResult(
            stage_name="download",
            status=StageStatus.SUCCESS,
            duration_seconds=duration,
            records_processed=0,
        )

    def _cleanup_after_execution(self, stage_result: StageResult) -> None:
        """
        Post-execution cleanup and finalization.

        Inline replacement for _after_execute() hook:
        - Memory checkpoint
        - Log completion
        - End write cycles

        Args:
            stage_result: The stage result from execution
        """
        # Memory checkpoint: Stage complete
        self.memory_monitor.snapshot("ClaimX Download - Stage complete")

        obs_config = self.config.observability
        log_memory_checkpoint(logger, "stage_end", config=obs_config)

        log_with_context(
            logger,
            logging.INFO,
            "Stage complete",
            stage="download",
            status="success",
            records_processed=stage_result.records_processed,
            records_succeeded=stage_result.records_succeeded,
            records_failed=stage_result.records_failed,
            duration_ms=stage_result.duration_seconds * 1000,
        )

        self.inventory_writer.end_cycle()
        self.retry_writer.end_cycle()

    # =============================================================================
    # Public API Methods
    # =============================================================================

    def get_stage_name(self) -> str:
        """
        Get unique identifier for this stage.

        Returns:
            Stage name "download"
        """
        return "download"

    def get_circuit_status(self) -> dict:
        """
        Get circuit breaker status for health checks.

        Returns:
            Dict mapping circuit names to their diagnostics
        """
        return {
            "download": self._download_breaker.get_diagnostics(),
            "upload": self._upload_breaker.get_diagnostics(),
        }

    def close(self) -> None:
        """
        Clean up stage resources.

        Closes OneLake client connection pool.
        """
        log_with_context(logger, logging.DEBUG, "Closing DownloadStage resources")
        if self.onelake_client:
            self.onelake_client.close()


# Standalone Download Functions (for use by RetryStage)
# Shared utilities reused by retry stage to avoid code duplication.


async def download_single(
    task: MediaTask,
    session: aiohttp.ClientSession,
    onelake_client: OneLakeClient,
    timeout: int,
    download_breaker: CircuitBreaker,
    upload_breaker: CircuitBreaker,
    allowed_domains: Optional[Set[str]] = None,
    proxy: Optional[str] = None,
) -> MediaDownloadResult:
    """
    Download single media file and upload to OneLake.

    Standalone utility function used by both DownloadStage and RetryStage.
    Keeps download logic consistent across stages without inheritance.

    Args:
        task: Media download task
        session: aiohttp session for HTTP requests
        onelake_client: OneLake client for uploads
        timeout: Download timeout in seconds
        download_breaker: Circuit breaker for downloads
        upload_breaker: Circuit breaker for uploads
        allowed_domains: Optional set of allowed download domains
        proxy: Optional proxy URL

    Returns:
        MediaDownloadResult with success/failure status
    """
    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
    STREAM_THRESHOLD = 50 * 1024 * 1024  # Stream files > 50MB

    # Validate URL before any network calls (SSRF prevention)
    is_valid, validation_error = validate_download_url(
        task.download_url, allowed_domains=allowed_domains
    )
    if not is_valid:
        log_with_context(
            logger,
            logging.WARNING,
            "Blocked unsafe URL",
            error_category="ssrf_blocked",
            error_message=validation_error,
            **extract_log_context(task),
        )
        return MediaDownloadResult(
            task=task,
            success=False,
            error=f"URL validation failed: {validation_error}",
            error_category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    # Check download circuit breaker
    if download_breaker.is_open:
        retry_after = download_breaker._get_retry_after()
        return MediaDownloadResult(
            task=task,
            success=False,
            error=f"Download circuit open, retry after {retry_after:.0f}s",
            error_category=ErrorCategory.CIRCUIT_OPEN,
            is_retryable=True,
        )

    # Check upload circuit breaker
    if upload_breaker.is_open:
        retry_after = upload_breaker._get_retry_after()
        return MediaDownloadResult(
            task=task,
            success=False,
            error=f"Upload circuit open, retry after {retry_after:.0f}s",
            error_category=ErrorCategory.CIRCUIT_OPEN,
            is_retryable=True,
        )

    tmp_path = None
    download_start = datetime.now(timezone.utc)

    try:
        # Download from source with secure redirect handling
        # Follow redirects but validate each redirect target for SSRF
        current_url = task.download_url
        max_redirects = 5
        redirect_count = 0

        while redirect_count < max_redirects:
            async with session.get(
                current_url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=False,
                proxy=proxy,
            ) as response:
                # Handle redirects securely
                if response.status in (301, 302, 303, 307, 308):
                    redirect_count += 1
                    location = response.headers.get("Location")
                    if not location:
                        log_with_context(
                            logger,
                            logging.WARNING,
                            "Redirect without Location header",
                            http_status=response.status,
                            **extract_log_context(task),
                        )
                        return MediaDownloadResult(
                            task=task,
                            success=False,
                            http_status=response.status,
                            error=f"Redirect {response.status} without Location header",
                            error_category=ErrorCategory.PERMANENT,
                            is_retryable=False,
                        )

                    # Validate redirect destination (SSRF protection)
                    is_valid, validation_error = validate_download_url(
                        location, allowed_domains=allowed_domains
                    )
                    if not is_valid:
                        log_with_context(
                            logger,
                            logging.WARNING,
                            "Blocked redirect to unsafe URL",
                            redirect_url=location,
                            error_category="ssrf_blocked",
                            error_message=validation_error,
                            **extract_log_context(task),
                        )
                        return MediaDownloadResult(
                            task=task,
                            success=False,
                            http_status=response.status,
                            error=f"Redirect blocked: {validation_error}",
                            error_category=ErrorCategory.PERMANENT,
                            is_retryable=False,
                        )

                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Following redirect",
                        http_status=response.status,
                        redirect_count=redirect_count,
                        **extract_log_context(task),
                    )
                    current_url = location
                    continue

                if response.status != 200:
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
                    return MediaDownloadResult(
                        task=task,
                        success=False,
                        http_status=response.status,
                        error=f"HTTP error: {response.status}",
                        error_category=error_category,
                        is_retryable=is_retryable,
                    )

                # Success - process the response
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

                return MediaDownloadResult(
                    task=task,
                    success=True,
                    http_status=200,
                    bytes_downloaded=content_length,
                )

        # Too many redirects
        log_with_context(
            logger,
            logging.WARNING,
            "Too many redirects",
            redirect_count=redirect_count,
            **extract_log_context(task),
        )
        return MediaDownloadResult(
            task=task,
            success=False,
            error=f"Too many redirects ({redirect_count})",
            error_category=ErrorCategory.PERMANENT,
            is_retryable=False,
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
        return MediaDownloadResult(
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
        return MediaDownloadResult(
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
        return MediaDownloadResult(
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
    tasks: List[MediaTask],
    onelake_client: OneLakeClient,
    max_concurrent: int,
    timeout: int,
    download_breaker: CircuitBreaker,
    upload_breaker: CircuitBreaker,
    allowed_domains: Optional[Set[str]] = None,
    proxy: Optional[str] = None,
) -> List[MediaDownloadResult]:
    """
    Download batch of media files with concurrency control.

    Standalone utility function used by both DownloadStage and RetryStage.
    Keeps download logic consistent across stages without inheritance.

    Args:
        tasks: List of media download tasks
        onelake_client: OneLake client for uploads
        max_concurrent: Maximum concurrent downloads
        timeout: Download timeout in seconds
        download_breaker: Circuit breaker for downloads
        upload_breaker: Circuit breaker for uploads
        allowed_domains: Optional set of allowed download domains
        proxy: Optional proxy URL

    Returns:
        List of MediaDownloadResult objects
    """
    if not tasks:
        return []

    # Early exit if circuits open
    if download_breaker.is_open:
        log_with_context(
            logger, logging.WARNING, "Download circuit open, rejecting batch"
        )
        return [
            MediaDownloadResult(
                task=task,
                success=False,
                error="Download circuit open",
                error_category=ErrorCategory.CIRCUIT_OPEN,
                is_retryable=True,
            )
            for task in tasks
        ]

    if upload_breaker.is_open:
        log_with_context(
            logger, logging.WARNING, "Upload circuit open, rejecting batch"
        )
        return [
            MediaDownloadResult(
                task=task,
                success=False,
                error="Upload circuit open",
                error_category=ErrorCategory.CIRCUIT_OPEN,
                is_retryable=True,
            )
            for task in tasks
        ]

    semaphore = asyncio.Semaphore(max_concurrent)

    connector = aiohttp.TCPConnector(
        limit=max_concurrent,
        limit_per_host=max_concurrent,
    )

    async with aiohttp.ClientSession(connector=connector) as session:

        async def download_with_semaphore(task: MediaTask) -> MediaDownloadResult:
            if download_breaker.is_open or upload_breaker.is_open:
                return MediaDownloadResult(
                    task=task,
                    success=False,
                    error="Circuit opened during batch",
                    error_category=ErrorCategory.CIRCUIT_OPEN,
                    is_retryable=True,
                )

            async with semaphore:
                return await download_single(
                    task,
                    session,
                    onelake_client,
                    timeout,
                    download_breaker,
                    upload_breaker,
                    allowed_domains,
                    proxy,
                )

        download_tasks = [download_with_semaphore(t) for t in tasks]
        all_results = await asyncio.gather(*download_tasks, return_exceptions=True)

    # Convert exceptions to results
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
                MediaDownloadResult(
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
    failed_perm = sum(1 for r in results if not r.success and not r.is_retryable)
    failed_trans = sum(1 for r in results if not r.success and r.is_retryable)

    log_with_context(
        logger,
        logging.INFO,
        "Batch complete",
        records_succeeded=succeeded,
        failed_permanent=failed_perm,
        failed_transient=failed_trans,
    )

    return results
