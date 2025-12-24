"""
Retry stage: Retry failed enrichments and downloads.

Retries transient failures with exponential backoff.
"""

import asyncio
import gc
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, cast

import polars as pl

from verisk_pipeline.claimx.stages.enrich import EntityTableWriter
from verisk_pipeline.claimx.api_client import ClaimXApiClient
from verisk_pipeline.common.config.claimx import ClaimXConfig
from verisk_pipeline.common.memory import MemoryMonitor
from verisk_pipeline.claimx.claimx_models import (
    ClaimXEvent,
    EntityRows,
    MediaDownloadResult,
    MediaTask,
    CLAIMX_PRIMARY_KEYS,
)
from verisk_pipeline.claimx.stages.handlers.base import get_handler_registry
from verisk_pipeline.claimx.stages.claimx_download import (
    download_batch,
    generate_blob_path,
)
from verisk_pipeline.common.auth import get_storage_options
from verisk_pipeline.common.url_expiration import (
    check_presigned_url,
    extract_expires_at_iso,
)
from verisk_pipeline.storage.inventory_writer import InventoryTableWriter
from verisk_pipeline.storage.retry_queue_writer import RetryQueueWriter
from verisk_pipeline.common.exceptions import ErrorCategory
from verisk_pipeline.common.security import sanitize_error_message
from verisk_pipeline.storage.delta import DeltaTableReader, DeltaTableWriter
from verisk_pipeline.storage.onelake import OneLakeClient
from verisk_pipeline.xact.xact_models import TaskStatus
from verisk_pipeline.common.circuit_breaker import (
    get_circuit_breaker,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
)
from verisk_pipeline.common.logging.context_managers import log_phase, StageLogContext
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_memory_checkpoint,
    log_with_context,
)
from verisk_pipeline.claimx.claimx_models import StageResult, StageStatus

logger = get_logger(__name__)


class RetryStage:
    """
    Retry stage for ClaimX pipeline.

    Retries:
    1. Failed API enrichments (from claimx_event_log)
    2. Failed media downloads (from claimx_media_downloads retry queue)

    Uses exponential backoff based on retry_count.
    """

    # Columns required for claimx retry processing - project early for memory efficiency
    # These are the columns that exist in claimx_retry table and are needed by _build_retry_tasks()
    RETRY_PROCESSING_COLUMNS = [
        "media_id",
        "project_id",
        "download_url",
        "status",
        "retry_count",
        "expires_at",
        "refresh_count",
        "file_name",
        "file_type",
        "next_retry_at",
        "created_at",
    ]

    def __init__(self, config: ClaimXConfig):
        """Initialize retry stage."""
        self.config = config

        self._max_retries = config.retry.max_retries
        self._min_age = config.retry.min_retry_age_seconds
        self._backoff_base = config.retry.backoff_base_seconds
        self._backoff_mult = config.retry.backoff_multiplier
        self._batch_size = config.retry.batch_size
        self._max_concurrent = config.download.max_concurrent

        # Event log for enrichment retries
        self._event_log_reader = DeltaTableReader(
            config.lakehouse.table_path(config.lakehouse.event_log_table)
        )
        self._event_log_writer = DeltaTableWriter(
            config.lakehouse.table_path(config.lakehouse.event_log_table),
            dedupe_column=None,
        )

        # Events table for re-fetching event data
        self._events_reader = DeltaTableReader(
            config.lakehouse.table_path(config.lakehouse.events_table)
        )

        # Media metadata for download retries
        self._media_reader = DeltaTableReader(
            config.lakehouse.table_path(config.lakehouse.media_metadata_table)
        )

        # Split-table architecture
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

        self._entity_writer = EntityTableWriter(config)

        # OneLake client for downloads
        self._onelake_client = OneLakeClient(
            config.lakehouse.files_path,
            max_pool_size=self._max_concurrent + 5,
        )

        # Circuit breakers
        self._download_breaker = get_circuit_breaker(
            "claimx_media_download", EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG
        )
        self._upload_breaker = get_circuit_breaker(
            "claimx_media_upload", ONELAKE_CIRCUIT_CONFIG
        )

        # Handler registry for enrichment
        self._registry = get_handler_registry()

        # Initialize memory monitor for production profiling
        self.memory_monitor = MemoryMonitor(
            enabled=config.observability.memory_profiling_enabled
        )

        # Log initialization with config
        log_with_context(
            logger,
            logging.DEBUG,
            "RetryStage initialized",
            max_retries=self._max_retries,
            min_retry_age_seconds=self._min_age,
            backoff_base_seconds=self._backoff_base,
            backoff_multiplier=self._backoff_mult,
            batch_size=self._batch_size,
            max_concurrent=self._max_concurrent,
        )

    def get_stage_name(self) -> str:
        """Return stage name for logging."""
        return "retry"

    def run(self) -> StageResult:
        """
        Main execution entry point - LINEAR FLOW, NO PARENT CALLS.

        Execute complete retry workflow:
        1. Check if retry stage is enabled
        2. Get pending retries (enrichments + downloads)
        3. Process retry batch (dual paths)
        4. Write tracking records
        5. Return stage result
        6. Cleanup after execution
        """
        cycle_id = generate_cycle_id()
        start = datetime.now(timezone.utc)

        with StageLogContext(self.get_stage_name(), cycle_id=cycle_id):
            try:
                # Step 1: Check if enabled
                if not self.config.retry.enabled:
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Retry stage disabled - skipping",
                        stage="retry",
                    )
                    return StageResult(
                        stage_name="retry",
                        status=StageStatus.SKIPPED,
                        duration_seconds=duration,
                        started_at=start,
                    )

                # Memory checkpoint: Stage start
                self.memory_monitor.snapshot("ClaimX Retry - Stage start")

                # Start write cycles
                self._cycle_id = cycle_id
                self.inventory_writer.start_cycle(cycle_id)
                self.retry_writer.start_cycle(cycle_id)

                obs_config = self.config.observability
                log_memory_checkpoint(logger, "stage_start", config=obs_config)

                # Log circuit breaker status
                download_circuit = self._download_breaker.get_diagnostics()
                upload_circuit = self._upload_breaker.get_diagnostics()
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Circuit breaker status",
                    download_circuit_state=download_circuit.get("state"),
                    download_failure_count=download_circuit.get("failure_count"),
                    upload_circuit_state=upload_circuit.get("state"),
                    upload_failure_count=upload_circuit.get("failure_count"),
                )

                # Step 2: Get pending retries (dual paths)
                pending = self._get_pending_retries()
                if self._is_empty_batch(pending):
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    log_with_context(
                        logger,
                        logging.INFO,
                        "No pending retries - skipping",
                        stage="retry",
                    )

                    # End cycles
                    self.inventory_writer.end_cycle()
                    self.retry_writer.end_cycle()

                    return StageResult(
                        stage_name="retry",
                        status=StageStatus.COMPLETED,
                        duration_seconds=duration,
                        started_at=start,
                        records_processed=0,
                    )

                # Step 3: Process retry batch (dual paths: enrichments + downloads)
                results = self._process_retry_batch(pending)

                # Step 4: Write retry results (already done in individual methods)
                self._write_retry_results(results, pending)

                # Step 5: Build result
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                stage_result = StageResult(
                    stage_name="retry",
                    status=StageStatus.COMPLETED,
                    duration_seconds=duration,
                    started_at=start,
                    records_processed=results["total"],
                    records_succeeded=results["succeeded"],
                    records_failed=results["failed"],
                    metadata=results.get("metadata", {}),
                )

                # Step 6: Cleanup
                self._cleanup_after_execution(stage_result)

                return stage_result

            except Exception as e:
                # Error handling - inline, no parent call
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                log_exception(logger, e, "Stage 'retry' failed", stage="retry")

                # End cycles on error
                self.inventory_writer.end_cycle()
                self.retry_writer.end_cycle()

                return StageResult(
                    stage_name="retry",
                    status=StageStatus.FAILED,
                    duration_seconds=duration,
                    started_at=start,
                    error=str(e),
                )

    def _cleanup_after_execution(self, result: StageResult) -> None:
        """
        Post-execution cleanup - replaces _after_execute() hook.

        Memory profiling, logging, and cycle finalization.
        """
        # Memory checkpoint: Stage complete
        self.memory_monitor.snapshot("ClaimX Retry - Stage complete")

        obs_config = self.config.observability
        log_memory_checkpoint(logger, "stage_end", config=obs_config)

        log_with_context(
            logger,
            logging.INFO,
            "Stage complete",
            stage="retry",
            status="success",
            records_processed=result.records_processed,
            records_succeeded=result.records_succeeded,
            records_failed=result.records_failed,
            duration_ms=result.duration_seconds * 1000,
            enrich_retried=result.metadata.get("enrich_retried", 0),
            enrich_succeeded=result.metadata.get("enrich_succeeded", 0),
            download_retried=result.metadata.get("download_retried", 0),
            download_succeeded=result.metadata.get("download_succeeded", 0),
        )

        # End write cycles
        self.inventory_writer.end_cycle()
        self.retry_writer.end_cycle()

    # =============================================================================
    # Retry-Specific Helpers (Domain-Specific Logic)
    # =============================================================================

    def calculate_backoff_seconds(self, retry_count: int) -> float:
        """
        Calculate exponential backoff delay in seconds.

        Formula: backoff_base * (backoff_multiplier ** retry_count)
        """
        if retry_count <= 0:
            return self._min_age
        return self._backoff_base * (self._backoff_mult ** retry_count)

    def is_retry_exhausted(self, retry_count: int) -> bool:
        """Check if retry count has reached maximum."""
        return retry_count >= self._max_retries

    def _get_pending_retries(self) -> Dict:
        """
        Get pending retries for both enrichments and downloads.

        Returns dict with 'enrichments' and 'downloads' DataFrames.
        """
        return {
            "enrichments": self._get_pending_enrichment_retries(),
            "downloads": self._get_pending_download_retries(),
        }

    def _process_retry_batch(self, pending: Dict) -> Dict:
        """
        Process retry batch for both enrichments and downloads.

        ClaimX has dual retry paths, so we process both and aggregate results.
        """
        # Retry enrichments
        with log_phase(logger, "retry_enrichments"):
            enrich_stats = asyncio.run(
                self._retry_enrichments_batch(pending["enrichments"])
            )
        gc.collect()

        # Retry downloads
        with log_phase(logger, "retry_downloads"):
            download_stats = self._retry_downloads_batch(pending["downloads"])
        gc.collect()

        # Aggregate results
        total_processed = enrich_stats["total"] + download_stats["total"]
        total_succeeded = enrich_stats["succeeded"] + download_stats["succeeded"]
        total_failed = enrich_stats["failed"] + download_stats["failed"]
        total_failed_permanent = enrich_stats.get(
            "failed_permanent", 0
        ) + download_stats.get("failed_permanent", 0)

        return {
            "total": total_processed,
            "succeeded": total_succeeded,
            "failed": total_failed,
            "failed_permanent": total_failed_permanent,
            "metadata": {
                "enrich_retried": enrich_stats["total"],
                "enrich_succeeded": enrich_stats["succeeded"],
                "download_retried": download_stats["total"],
                "download_succeeded": download_stats["succeeded"],
            },
        }

    def _write_retry_results(self, result: Dict, pending: Dict) -> None:
        """Write retry results (no additional tracking needed, done in individual methods)."""
        pass

    def _is_empty_batch(self, batch: Dict) -> bool:
        """Check if both enrichments and downloads are empty."""
        enrich_df = batch.get("enrichments")
        download_df = batch.get("downloads")

        enrich_empty = enrich_df is None or enrich_df.is_empty()
        download_empty = download_df is None or download_df.is_empty()

        return enrich_empty and download_empty

    # =============================================================================
    # Enrichment Retry Logic
    # =============================================================================

    def _get_pending_enrichment_retries(self) -> pl.DataFrame:
        """
        Get failed enrichments eligible for retry.

        OPTIMIZED: Uses vectorized Polars expressions for backoff calculation
        instead of Python loop with datetime parsing per row.
        """
        try:
            if not self._event_log_reader.exists():
                return pl.DataFrame()

            now = datetime.now(timezone.utc)
            now_lit = pl.lit(now).cast(pl.Datetime("us", "UTC"))

            log_with_context(
                logger,
                logging.DEBUG,
                "Querying enrichment retries",
                max_retries=self._max_retries,
                min_retry_age=self._min_age,
                backoff_base=self._backoff_base,
                backoff_multiplier=self._backoff_mult,
                batch_size=self._batch_size,
            )

            # Read ALL recent event logs (not just failed)
            df = cast(
                pl.DataFrame,
                self._event_log_reader.read_filtered(
                    filter_expr=pl.col("event_id").is_not_null(),
                    columns=[
                        "event_id",
                        "status",
                        "retry_count",
                        "error_category",
                        "processed_at",
                    ],
                    limit=self._batch_size * 10,  # Get more since we filter after
                    order_by="processed_at",
                ),
            )

            if df.is_empty():
                return df

            # Get latest row per event FIRST
            df = df.sort("processed_at", descending=True).group_by("event_id").first()

            # THEN filter to failed, under retry limit, non-permanent
            df = df.filter(
                (pl.col("status") == "failed")
                & (pl.col("retry_count") < self._max_retries)
                & (
                    (pl.col("error_category").is_null())
                    | (pl.col("error_category") != "permanent")
                )
            )

            if df.is_empty():
                return df

            log_with_context(
                logger,
                logging.DEBUG,
                "Found failed enrichments",
                failed_count=len(df),
            )

            # OPTIMIZED: Apply backoff filter using vectorized Polars expressions
            # Convert processed_at to datetime if it's a string
            if df["processed_at"].dtype in (pl.Utf8, pl.String):
                try:
                    df = df.with_columns(
                        pl.col("processed_at")
                        .str.to_datetime(time_zone="UTC")
                        .alias("_processed_dt")
                    )
                except Exception:
                    # Fallback: handle various ISO formats
                    try:
                        df = df.with_columns(
                            pl.col("processed_at")
                            .str.replace("Z", "+00:00")
                            .str.slice(0, 26)
                            .str.to_datetime()
                            .dt.replace_time_zone("UTC")
                            .alias("_processed_dt")
                        )
                    except Exception as e:
                        log_with_context(
                            logger,
                            logging.WARNING,
                            "Could not parse processed_at",
                            error_message=str(e),
                        )
                        return pl.DataFrame()
            else:
                # Already datetime, ensure UTC
                df = df.with_columns(
                    pl.col("processed_at")
                    .dt.replace_time_zone("UTC")
                    .alias("_processed_dt")
                )

            # Vectorized backoff calculation:
            # wait_seconds = min_age when retry_count == 0
            # wait_seconds = backoff_base * (backoff_mult ** retry_count) otherwise
            df = df.with_columns(
                pl.when(pl.col("retry_count").fill_null(0) == 0)
                .then(pl.lit(float(self._min_age)))
                .otherwise(
                    self._backoff_base
                    * (self._backoff_mult ** pl.col("retry_count").fill_null(0))
                )
                .alias("_wait_seconds")
            )

            # Calculate next retry time and filter to ready records
            ready = (
                df.with_columns(
                    (
                        pl.col("_processed_dt")
                        + pl.duration(seconds=pl.col("_wait_seconds"))
                    ).alias("_next_retry")
                )
                .filter(pl.col("_next_retry") <= now_lit)
                .drop(["_processed_dt", "_wait_seconds", "_next_retry"])
                .head(self._batch_size)
            )

            log_with_context(
                logger,
                logging.DEBUG,
                "Enrichment retries ready",
                ready_count=len(ready),
            )

            return ready

        except Exception as e:
            log_with_context(
                logger,
                logging.WARNING,
                "Error getting enrichment retries",
                error_message=str(e),
            )
            return pl.DataFrame()

    async def _retry_enrichments_batch(self, pending: pl.DataFrame) -> Dict:
        """Retry failed enrichment API calls."""
        stats = {"total": 0, "succeeded": 0, "failed": 0, "failed_permanent": 0}
        obs_config = self.config.observability

        if pending is None or pending.is_empty():
            log_with_context(logger, logging.DEBUG, "No pending enrichment retries")
            return stats

        stats["total"] = len(pending)

        # Log retry_count distribution
        try:
            retry_counts = pending.group_by("retry_count").len().to_dicts()
            retry_distribution = {
                str(row["retry_count"]): row["len"] for row in retry_counts
            }
            log_with_context(
                logger,
                logging.DEBUG,
                "Enrichment retry distribution",
                retry_count_distribution=retry_distribution,
            )
        except Exception:
            pass

        log_with_context(
            logger,
            logging.INFO,
            "Retrying failed enrichments",
            batch_size=len(pending),
        )
        # Get event IDs to retry
        event_ids = pending["event_id"].to_list()

        # Fetch full event data
        events = self._fetch_events_by_id(event_ids)
        if not events:
            log_with_context(
                logger, logging.WARNING, "Could not fetch event data for retries"
            )
            return stats

        # Build retry count lookup
        retry_counts = self._build_retry_counts(pending)

        # Clear pending df - no longer needed
        del pending
        gc.collect()

        # Create API client
        client = ClaimXApiClient(
            base_url=self.config.api.base_url,
            auth_token=self.config.api.auth_token or "",
            timeout_seconds=self.config.api.timeout_seconds,
            max_concurrent=self._max_concurrent,
        )

        all_rows = EntityRows()
        event_logs = []

        try:
            async with client:
                # Group by handler
                grouped = self._registry.group_events_by_handler(events)

                for handler_class, handler_events in grouped.items():
                    handler_name = handler_class.__name__
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Processing handler events",
                        handler_name=handler_name,
                        batch_size=len(handler_events),
                    )

                    handler = handler_class(client)
                    result = await handler.process(handler_events)

                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Handler complete",
                        handler_name=handler_name,
                        records_succeeded=result.succeeded,
                        records_failed=result.failed,
                        failed_permanent=result.failed_permanent,
                    )

                    stats["succeeded"] += result.succeeded
                    stats["failed"] += result.failed
                    stats["failed_permanent"] += result.failed_permanent

                    all_rows.merge(result.rows)

                    # Update retry counts in event logs
                    for log in result.event_logs:
                        log["retry_count"] = retry_counts.get(log["event_id"], 1)
                    event_logs.extend(result.event_logs)

        except Exception as e:
            log_exception(
                logger, e, "Error during handler processing", handler_name=handler_name
            )
            raise
        log_memory_checkpoint(logger, "after_processing", config=obs_config)

        # Write results
        if not all_rows.is_empty():
            self._entity_writer.write_all(all_rows)

        if event_logs:
            self._write_event_logs(event_logs)

        # Cleanup
        del all_rows, event_logs, events, retry_counts
        gc.collect()

        log_with_context(
            logger,
            logging.INFO,
            "Enrichment retry complete",
            records_succeeded=stats["succeeded"],
            records_failed=stats["failed"],
            records_failed_permanent=stats["failed_permanent"],
        )

        return stats

    def _build_retry_counts(self, pending: pl.DataFrame) -> Dict[str, int]:
        """Build retry count lookup from pending df."""
        try:
            import io
            import json

            buffer = io.BytesIO()
            pending.write_ndjson(buffer)
            buffer.seek(0)
            retry_counts = {}
            for line in buffer:
                row = json.loads(line.decode("utf-8", errors="replace"))
                retry_counts[row["event_id"]] = (row.get("retry_count") or 0) + 1
            return retry_counts
        except Exception as e:
            log_with_context(
                logger,
                logging.WARNING,
                "Error building retry counts, using defaults",
                error_message=str(e),
            )
            return {eid: 1 for eid in pending["event_id"].to_list()}

    def _fetch_events_by_id(self, event_ids: List[str]) -> List[ClaimXEvent]:
        """Fetch event data for given IDs."""
        try:
            # Batch to avoid Polars is_in() bug with large lists
            BATCH_SIZE = 200
            all_events = []

            log_with_context(
                logger,
                logging.DEBUG,
                "Fetching events by ID",
                event_count=len(event_ids),
                batch_size=BATCH_SIZE,
            )

            for i in range(0, len(event_ids), BATCH_SIZE):
                batch_ids = event_ids[i : i + BATCH_SIZE]

                try:
                    df = self._events_reader.read_filtered(
                        filter_expr=pl.col("event_id").is_in(batch_ids),
                        columns=[
                            "event_id",
                            "event_type",
                            "project_id",
                            "media_id",
                            "task_assignment_id",
                            "video_collaboration_id",
                            "master_file_name",
                            "ingested_at",
                        ],
                    )
                except Exception as e:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Error reading batch",
                        batch_start=i,
                        batch_end=i + BATCH_SIZE,
                        error_message=str(e),
                    )
                    continue

                if df.is_empty():
                    continue

                for row in df.to_dicts():
                    try:
                        event = ClaimXEvent.from_kusto_row(row)
                        all_events.append(event)
                    except Exception as e:
                        log_with_context(
                            logger,
                            logging.WARNING,
                            "Error parsing event",
                            event_id=row.get("event_id"),
                            error_message=str(e),
                        )

                # Clear batch df
                del df

            log_with_context(
                logger,
                logging.DEBUG,
                "Fetched events",
                requested=len(event_ids),
                fetched=len(all_events),
            )

            return all_events

        except Exception as e:
            log_with_context(
                logger,
                logging.WARNING,
                "Error fetching events",
                error_message=str(e),
            )
            return []

    def _write_event_logs(self, logs: List[Dict]) -> None:
        """Write event log entries."""
        if not logs:
            return

        try:
            df = pl.DataFrame(logs)
            self._event_log_writer.append(df, dedupe=False)
            log_with_context(
                logger,
                logging.DEBUG,
                "Wrote event logs",
                records_written=len(logs),
            )
        except Exception as e:
            log_with_context(
                logger,
                logging.WARNING,
                "Error writing event logs",
                error_message=str(e),
            )

    # =============================================================================
    # Download Retry Logic
    # =============================================================================

    def _get_pending_download_retries(self) -> Optional[pl.DataFrame]:
        """Get pending download retries from retry queue."""
        # Run cleanup first if enabled
        if (
            hasattr(self.config.retry, "cleanup_on_start")
            and self.config.retry.cleanup_on_start
        ):
            retention_days = getattr(self.config.retry, "retention_days", 7)
            deleted = self.retry_writer.cleanup_expired(retention_days)
            if deleted > 0:
                log_with_context(
                    logger,
                    logging.INFO,
                    "Cleaned up expired retry records",
                    deleted_count=deleted,
                    retention_days=retention_days,
                )

        # Query from retry queue
        pending = self.retry_writer.get_pending_retries(
            max_retries=self._max_retries,
            min_age_seconds=self._min_age,
            limit=self._batch_size,
            columns=self.RETRY_PROCESSING_COLUMNS,
        )

        obs_config = self.config.observability
        log_memory_checkpoint(
            logger, "after_pending_read", config=obs_config, df=pending
        )

        return pending

    def _retry_downloads_batch(self, pending: Optional[pl.DataFrame]) -> Dict:
        """Retry failed media downloads."""
        stats = {"total": 0, "succeeded": 0, "failed": 0, "failed_permanent": 0}
        obs_config = self.config.observability

        if pending is None or pending.is_empty():
            log_with_context(logger, logging.DEBUG, "No pending download retries")
            return stats

        stats["total"] = len(pending)

        # Log retry_count distribution
        try:
            retry_counts = pending.group_by("retry_count").len().to_dicts()
            retry_distribution = {
                str(row["retry_count"]): row["len"] for row in retry_counts
            }
            log_with_context(
                logger,
                logging.DEBUG,
                "Download retry distribution",
                retry_count_distribution=retry_distribution,
            )
        except Exception:
            pass

        log_with_context(
            logger,
            logging.INFO,
            "Retrying failed downloads",
            batch_size=len(pending),
        )

        # Refresh presigned URLs before retry (they may have expired)
        project_ids = pending["project_id"].unique().to_list()
        with log_phase(logger, "refresh_urls"):
            fresh_urls = asyncio.run(self._refresh_media_urls(project_ids))

        # Build tasks with fresh URLs from media table
        tasks = self._build_retry_tasks(pending, fresh_urls)
        if not tasks:
            log_with_context(logger, logging.WARNING, "Could not build retry tasks")
            return stats

        log_with_context(
            logger,
            logging.DEBUG,
            "Built retry tasks",
            tasks_created=len(tasks),
            pending_count=len(pending),
        )

        # Execute downloads
        with log_phase(logger, "download_batch"):
            with self._onelake_client:
                results = asyncio.run(
                    download_batch(
                        tasks=tasks,
                        onelake_client=self._onelake_client,
                        max_concurrent=self._max_concurrent,
                        timeout=self.config.download.timeout_seconds,
                        download_breaker=self._download_breaker,
                        upload_breaker=self._upload_breaker,
                        allowed_domains=set(
                            self.config.security.allowed_download_domains
                        ),
                        proxy=self.config.download.proxy,
                    )
                )
        log_memory_checkpoint(logger, "after_processing", config=obs_config)

        # Write tracking records
        with log_phase(logger, "write_tracking"):
            self._write_download_tracking(results, pending)

        # Aggregate stats and collect failed sample
        failed_items = []
        for r in results:
            if r.success:
                stats["succeeded"] += 1
            else:
                stats["failed"] += 1
                failed_items.append(r)
                if not r.is_retryable:
                    stats["failed_permanent"] += 1

        failed_sample = (
            [
                {
                    "media_id": r.task.media_id,
                    "error": r.error[:100] if r.error else None,
                }
                for r in failed_items[:3]
            ]
            if failed_items
            else None
        )

        # Cleanup
        del pending, tasks, results
        gc.collect()

        log_with_context(
            logger,
            logging.INFO,
            "Download retry complete",
            records_succeeded=stats["succeeded"],
            records_failed=stats["failed"],
            records_failed_permanent=stats["failed_permanent"],
            failed_sample=failed_sample,
        )

        return stats

    async def _refresh_media_urls(self, project_ids: List[str]) -> Dict[str, Dict]:
        """
        Fetch fresh presigned URLs for projects.

        Returns:
            Dict mapping media_id -> {project_id, download_url, expires_at}
        """
        if not project_ids:
            return {}

        log_with_context(
            logger,
            logging.DEBUG,
            "Refreshing media URLs",
            project_count=len(project_ids),
        )

        client = ClaimXApiClient(
            base_url=self.config.api.base_url,
            auth_token=self.config.api.auth_token,
            timeout_seconds=self.config.api.timeout_seconds,
            max_concurrent=self._max_concurrent,
        )

        # Collect fresh URLs in memory (don't write to Delta)
        fresh_urls: Dict[str, Dict] = {}
        api_calls = 0
        api_errors = 0

        async with client:
            for project_id in project_ids:
                try:
                    media_list = await client.get_project_media(int(project_id))
                    api_calls += 1
                    if media_list:
                        for item in media_list:
                            media_id = str(
                                item.get("mediaID") or item.get("media_id") or ""
                            )
                            download_url = item.get("fullDownloadLink") or item.get(
                                "full_download_link"
                            )

                            if media_id and download_url:
                                fresh_urls[media_id] = {
                                    "project_id": str(project_id),
                                    "download_url": download_url,
                                    "expires_at": extract_expires_at_iso(download_url),
                                }

                        log_with_context(
                            logger,
                            logging.DEBUG,
                            "Collected media URLs",
                            project_id=project_id,
                            media_count=len(media_list),
                        )
                except Exception as e:
                    api_errors += 1
                    log_exception(
                        logger,
                        e,
                        "Could not refresh URLs for project",
                        level=logging.WARNING,
                        include_traceback=False,
                        project_id=project_id,
                    )

        log_with_context(
            logger,
            logging.DEBUG,
            "Refreshed URLs",
            total_urls=len(fresh_urls),
            api_calls=api_calls,
            api_errors=api_errors,
        )

        return fresh_urls

    def _build_retry_tasks(
        self, pending: pl.DataFrame, fresh_urls: Dict[str, Dict]
    ) -> List[MediaTask]:
        """
        Build MediaTask objects for retry using fresh URLs.

        Args:
            pending: DataFrame with retry candidates (has all metadata)
            fresh_urls: Dict mapping media_id -> {download_url, expires_at}
        """
        tasks = []
        still_expired = []
        missing_urls = []
        valid_count = 0

        for row in pending.iter_rows(named=True):
            media_id = row.get("media_id")
            if not media_id:
                continue

            # Get fresh URL (or fall back to stored URL)
            if media_id in fresh_urls:
                download_url = fresh_urls[media_id]["download_url"]
                expires_at = fresh_urls[media_id].get("expires_at")
                refresh_count = (row.get("refresh_count") or 0) + 1
            else:
                # No fresh URL - use stored URL from tracking table
                download_url = row.get("download_url")
                expires_at = row.get("expires_at")
                refresh_count = row.get("refresh_count") or 0
                missing_urls.append(media_id)

            if not download_url:
                continue

            # Check if URL is expired
            url_info = check_presigned_url(download_url)
            retry_count = (row.get("retry_count") or 0) + 1

            if url_info.is_expired:
                still_expired.append(
                    {
                        "media_id": media_id,
                        "project_id": row.get("project_id"),
                        "download_url": download_url,
                        "blob_path": row.get("blob_path") or "",
                        "file_type": row.get("file_type") or "",
                        "file_name": row.get("file_name") or "",
                        "source_event_id": row.get("source_event_id") or "",
                        "status": TaskStatus.FAILED.value,  # Will retry again
                        "error_message": "URL expired",
                        "error_category": ErrorCategory.TRANSIENT.value,
                        "retry_count": retry_count,
                        "refresh_count": refresh_count,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue

            valid_count += 1

            # Use metadata from pending (tracking table already has it)
            project_id = row.get("project_id") or ""
            file_type = row.get("file_type") or ""
            file_name = row.get("file_name") or ""

            # Use existing blob_path or generate new one
            blob_path = row.get("blob_path") or generate_blob_path(
                project_id=project_id,
                media_id=media_id,
                file_type=file_type,
                file_name=file_name,
            )

            tasks.append(
                MediaTask(
                    media_id=media_id,
                    project_id=project_id,
                    download_url=download_url,
                    blob_path=blob_path,
                    file_type=file_type,
                    file_name=file_name,
                    source_event_id=row.get("source_event_id") or "",
                    retry_count=retry_count,
                    expires_at=expires_at,
                    refresh_count=refresh_count,
                )
            )

        # Log task building results
        log_with_context(
            logger,
            logging.DEBUG,
            "Task building results",
            valid_tasks=valid_count,
            expired_urls=len(still_expired),
            missing_urls=len(missing_urls),
        )

        # Log warnings
        if missing_urls:
            log_with_context(
                logger,
                logging.DEBUG,
                "Media IDs not in refresh response (using stored URL)",
                count=len(missing_urls),
            )

        # Write still-expired URLs to retry queue
        if still_expired:
            try:
                self.retry_writer.write_retry(
                    rows=still_expired,
                    max_retries=self.config.retry.max_retries,
                    backoff_base_seconds=self.config.retry.backoff_base_seconds,
                    backoff_multiplier=self.config.retry.backoff_multiplier,
                )
                log_with_context(
                    logger,
                    logging.WARNING,
                    "URLs still expired after refresh",
                    count=len(still_expired),
                )
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Could not write expired URLs to retry queue",
                    level=logging.WARNING,
                )

        return tasks

    def _write_download_tracking(
        self,
        results: List[MediaDownloadResult],
        pending: pl.DataFrame,
    ) -> None:
        """
        Write download retry results to split tables.

        Split logic with DELETE operations:
        - Success → Write to inventory + DELETE from retry queue
        - Permanent failure → DELETE from retry queue only
        - Exhausted (retry_count >= max) → DELETE from retry queue only
        - Still transient → UPDATE retry queue with incremented retry_count
        """
        if not results:
            return

        # Build retry count lookup (current retry_count from queue)
        retry_counts = {
            (row["media_id"], row["project_id"]): row.get("retry_count") or 0
            for row in pending.iter_rows(named=True)
        }

        inventory_rows = []
        retry_update_rows = []
        delete_keys = []
        exhausted_count = 0

        for result in results:
            task = result.task
            key = (task.media_id, task.project_id)
            current_retry_count = retry_counts.get(key, task.retry_count) or 0
            new_retry_count = current_retry_count + 1

            if result.success:
                # SUCCESS → Inventory + DELETE from retry queue
                row = task.to_tracking_row(
                    status=TaskStatus.COMPLETED,
                    http_status=result.http_status,
                    error_message=None,
                    bytes_downloaded=result.bytes_downloaded,
                )
                # Note: inventory_writer will auto-strip status/error fields
                inventory_rows.append(row)
                delete_keys.append(
                    {
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                    }
                )

            elif not result.is_retryable:
                # PERMANENT FAILURE → DELETE from retry queue only
                delete_keys.append(
                    {
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                    }
                )
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Permanent failure - removing from queue",
                    media_id=task.media_id,
                    project_id=task.project_id,
                    error_category=(
                        result.error_category.value if result.error_category else None
                    ),
                )

            elif (
                result.http_status in (401, 403, 404, 410)
                and task.refresh_count > 0
            ):
                # URL-related error AFTER refresh → truly permanent
                # We already tried with a fresh URL, so this is not a stale URL issue.
                delete_keys.append(
                    {
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                    }
                )
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Permanent failure after URL refresh - removing from queue",
                    media_id=task.media_id,
                    project_id=task.project_id,
                    http_status=result.http_status,
                    refresh_count=task.refresh_count,
                    error_category=(
                        result.error_category.value if result.error_category else None
                    ),
                )

            else:
                if self.is_retry_exhausted(new_retry_count):
                    # EXHAUSTED → DELETE from retry queue only
                    exhausted_count += 1
                    delete_keys.append(
                        {
                            "media_id": task.media_id,
                            "project_id": task.project_id,
                        }
                    )
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Retry exhausted - removing from queue",
                        media_id=task.media_id,
                        project_id=task.project_id,
                        retry_count=new_retry_count,
                        max_retries=self._max_retries,
                    )
                else:
                    # STILL TRANSIENT → UPDATE retry queue
                    row = task.to_tracking_row(
                        status=TaskStatus.FAILED,
                        http_status=result.http_status,
                        error_message=(
                            sanitize_error_message(result.error)
                            if result.error
                            else None
                        ),
                        bytes_downloaded=result.bytes_downloaded,
                    )
                    row["error_category"] = (
                        result.error_category.value if result.error_category else None
                    )
                    row["retry_count"] = new_retry_count
                    row["refresh_count"] = task.refresh_count
                    retry_update_rows.append(row)

                    # Log next backoff
                    next_backoff = self._backoff_base * (
                        self._backoff_mult**new_retry_count
                    )
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Will retry again",
                        media_id=task.media_id,
                        retry_count=new_retry_count,
                        retries_remaining=self._max_retries - new_retry_count,
                        next_backoff_seconds=next_backoff,
                    )

        # Write to split tables
        if inventory_rows:
            self.inventory_writer.write_inventory(inventory_rows)

        if retry_update_rows:
            self.retry_writer.write_retry(
                rows=retry_update_rows,
                max_retries=self._max_retries,
                backoff_base_seconds=self._backoff_base,
                backoff_multiplier=self._backoff_mult,
            )

        # DELETE completed/exhausted/permanent from retry queue
        if delete_keys:
            self.retry_writer.delete_by_keys(delete_keys)

        # Log summary
        log_with_context(
            logger,
            logging.INFO,
            "Download retry processing complete",
            succeeded=len(inventory_rows),
            still_pending=len(retry_update_rows),
            removed_from_queue=len(delete_keys),
            exhausted=exhausted_count,
        )

    # =============================================================================
    # Utility Methods
    # =============================================================================

    def get_retry_statistics(self) -> Dict:
        """Get retry queue statistics."""
        try:
            # Enrichment retries
            enrich_pending = self._get_pending_enrichment_retries()
            enrich_count = len(enrich_pending) if not enrich_pending.is_empty() else 0

            # Download retries (from retry queue)
            download_pending = self.retry_writer.get_pending_retries(
                max_retries=self._max_retries,
                min_age_seconds=self._min_age,
                columns=self.RETRY_PROCESSING_COLUMNS,
            )
            download_count = (
                len(download_pending) if not download_pending.is_empty() else 0
            )

            return {
                "pending_enrichment": enrich_count,
                "pending_download": download_count,
                "pending_total": enrich_count + download_count,
            }

        except Exception as e:
            log_exception(
                logger,
                e,
                "Error getting retry stats",
                level=logging.WARNING,
                include_traceback=False,
            )
            return {"pending_enrichment": 0, "pending_download": 0, "pending_total": 0}

    def get_circuit_status(self) -> Dict:
        """Get circuit breaker diagnostics."""
        return {
            "download": self._download_breaker.get_diagnostics(),
            "upload": self._upload_breaker.get_diagnostics(),
        }

    def close(self) -> None:
        """Clean up resources."""
        log_with_context(logger, logging.DEBUG, "Closing RetryStage resources")
        if self._onelake_client:
            self._onelake_client.close()
