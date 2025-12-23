"""
Enrich stage: Events -> API -> Entity tables.

Reads pending events, processes through handlers, writes to Delta tables.

This module provides the EnrichStage coordinator which orchestrates enrichment
through focused service components extracted to verisk_pipeline.claimx.services.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import polars as pl

from verisk_pipeline.claimx.api_client import ClaimXApiClient
from verisk_pipeline.common.config.claimx import ClaimXConfig
from verisk_pipeline.claimx.claimx_models import (
    ClaimXEvent,
    EntityRows,
)
from verisk_pipeline.claimx.stages.handlers.base import (
    get_handler_registry,
)

# Import handlers to trigger registration
from verisk_pipeline.claimx.stages.handlers import (  # noqa: F401
    project,
    media,
    task,
    contact,
    video,
    xa_link,
    project_conversations,
    personal_property,
)

# Import services - extracted from this module for separation of concerns
from verisk_pipeline.claimx.services import (
    ProcessedEventCache,
    EventLogWriter,
    EntityTableWriter,
)

from verisk_pipeline.storage.delta import DeltaTableReader
from verisk_pipeline.storage.watermark import WatermarkManager
from verisk_pipeline.storage.watermark_helpers import (
    WatermarkSession,
    extract_max_from_events,
)
from verisk_pipeline.xact.xact_models import StageResult, StageStatus
from verisk_pipeline.claimx.stages.handlers.project_cache import ProjectCache
from verisk_pipeline.claimx.stages.handlers.project import ProjectHandler
from verisk_pipeline.common.logging.context_managers import StageLogContext, log_phase
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_memory_checkpoint,
    log_with_context,
)

logger = get_logger(__name__)


class EnrichStage:
    """
    Enrich stage coordinator: Events -> API -> Entity tables.

    Orchestrates enrichment through focused services:
    - ProcessedEventCache: Deduplication tracking
    - EntityTableWriter: Entity persistence
    - EventLogWriter: Processing status tracking
    """

    def __init__(self, config: ClaimXConfig):
        self.config = config

        self._project_cache = ProjectCache()
        self._cache_loaded = False

        self._events_reader = DeltaTableReader(
            config.lakehouse.table_path(config.lakehouse.events_table)
        )

        self._event_log_reader = DeltaTableReader(
            config.lakehouse.table_path(config.lakehouse.event_log_table)
        )
        self._event_log_writer = EventLogWriter(
            config.lakehouse.table_path(config.lakehouse.event_log_table)
        )

        self._processed_cache = ProcessedEventCache(
            event_log_reader=self._event_log_reader,
            lookback_days=config.processing.lookback_days,
        )

        self._entity_writer = EntityTableWriter(config)
        self._watermark = WatermarkManager("claimx_enrich")
        self._registry = get_handler_registry()
        self._client: Optional[ClaimXApiClient] = None

        log_with_context(
            logger,
            logging.DEBUG,
            "EnrichStage initialized",
            events_table=config.lakehouse.table_path(config.lakehouse.events_table),
            event_log_table=config.lakehouse.table_path(
                config.lakehouse.event_log_table
            ),
            watermark_name="claimx_enrich",
            event_types=config.processing.event_types,
            batch_size=config.processing.batch_size,
            lookback_days=config.processing.lookback_days,
            skip_dedup_check=config.processing.skip_dedup_check,
        )

    def run(self) -> StageResult:
        """Execute enrich stage."""
        cycle_id = generate_cycle_id()

        with StageLogContext("enrich", cycle_id=cycle_id):
            start = datetime.now(timezone.utc)
            obs_config = self.config.observability

            log_memory_checkpoint(logger, "stage_start", config=obs_config)

            try:
                result = asyncio.run(self._process_events())

                duration = (datetime.now(timezone.utc) - start).total_seconds()
                duration_ms = duration * 1000

                log_memory_checkpoint(logger, "stage_end", config=obs_config)

                log_with_context(
                    logger,
                    logging.INFO,
                    "Stage complete",
                    stage="enrich",
                    status="success",
                    records_processed=result["total"],
                    records_succeeded=result["succeeded"],
                    records_failed=result["failed"],
                    records_failed_permanent=result["failed_permanent"],
                    duration_ms=duration_ms,
                    api_calls=result["api_calls"],
                )

                return StageResult(
                    stage_name="enrich",
                    status=(
                        StageStatus.SUCCESS
                        if result["failed"] == 0
                        else StageStatus.SUCCESS
                    ),
                    duration_seconds=duration,
                    records_processed=result["total"],
                    records_succeeded=result["succeeded"],
                    records_failed=result["failed"],
                    metadata={
                        "api_calls": result["api_calls"],
                        "failed_permanent": result["failed_permanent"],
                        "tables_written": result["tables_written"],
                    },
                )

            except Exception as e:
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                log_exception(
                    logger,
                    e,
                    "Stage failed",
                    stage="enrich",
                    duration_ms=duration * 1000,
                )

                log_memory_checkpoint(logger, "stage_end", config=obs_config)

                return StageResult(
                    stage_name="enrich",
                    status=StageStatus.FAILED,
                    duration_seconds=duration,
                    error=str(e),
                )

    async def _load_project_cache(self) -> None:
        """Load project IDs from Delta table into cache."""
        if self._cache_loaded:
            return

        try:
            projects_path = self.config.lakehouse.table_path(
                self.config.lakehouse.projects_table
            )
            reader = DeltaTableReader(projects_path)

            if reader.exists():
                df: pl.DataFrame = reader.read(columns=["project_id"])
                if df is not None and not df.is_empty():
                    project_ids = df["project_id"].to_list()
                    self._project_cache.bulk_load(project_ids)

            self._cache_loaded = True
            log_with_context(
                logger,
                logging.INFO,
                "Loaded project cache",
                cache_size=self._project_cache.size(),
            )

        except Exception as e:
            log_exception(
                logger,
                e,
                "Could not load project cache",
                level=logging.WARNING,
                include_traceback=False,
            )
            self._cache_loaded = True

    async def _ensure_projects_exist(
        self,
        events: List[ClaimXEvent],
    ) -> Dict[str, bool]:
        """Ensure all projects referenced by events exist."""
        if self._client is None:
            return {e.project_id: False for e in events if e.project_id}

        project_ids = {e.project_id for e in events if e.project_id}
        missing = [pid for pid in project_ids if not self._project_cache.exists(pid)]

        if not missing:
            log_with_context(
                logger,
                logging.DEBUG,
                "All projects exist in cache",
                project_count=len(project_ids),
            )
            return {pid: True for pid in project_ids}

        log_with_context(
            logger,
            logging.INFO,
            "Fetching missing projects",
            total_projects=len(project_ids),
            missing_projects=len(missing),
        )

        results: Dict[str, bool] = {pid: True for pid in project_ids}
        all_rows = EntityRows()

        project_handler: ProjectHandler = ProjectHandler(self._client)

        async def fetch_one(pid: str):
            success, rows, _ = await project_handler.ensure_exists(
                pid,
                self._project_cache,
            )
            return pid, success, rows

        tasks = [fetch_one(pid) for pid in missing]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        failed = 0
        succeeded = 0
        for item in fetched:
            if isinstance(item, Exception):
                log_exception(
                    logger,
                    item,
                    "Project fetch failed",
                    level=logging.WARNING,
                    include_traceback=False,
                )
                failed += 1
                continue
            pid, success, rows = item
            results[pid] = success
            all_rows.merge(rows)
            if success:
                succeeded += 1
            else:
                failed += 1

        log_with_context(
            logger,
            logging.INFO,
            "Project fetch complete",
            records_succeeded=succeeded,
            records_failed=failed,
        )

        if not all_rows.is_empty():
            self._entity_writer.write_all(all_rows)

        return results

    async def _process_events(self) -> Dict:
        """Process pending events through handlers."""
        stats: Dict = {
            "total": 0,
            "succeeded": 0,
            "failed": 0,
            "failed_permanent": 0,
            "api_calls": 0,
            "tables_written": {},
        }

        obs_config = self.config.observability

        # Watermark session handles retrieval and updates
        with WatermarkSession(
            self._watermark,
            lookback_days=self.config.processing.lookback_days,
            timestamp_column="ingested_at",
            logger=logger,
        ) as wm_session:
            with log_phase(logger, "load_project_cache"):
                await self._load_project_cache()

            with log_phase(logger, "get_events"):
                events = self._get_pending_events(wm_session.start_watermark)

            log_memory_checkpoint(logger, "after_events_read", config=obs_config)

            if not events:
                log_with_context(logger, logging.DEBUG, "No pending events to process")
                return stats

            stats["total"] = len(events)

            # Log event type distribution
            event_types = {}
            for e in events:
                event_types[e.event_type] = event_types.get(e.event_type, 0) + 1

            log_with_context(
                logger,
                logging.INFO,
                "Processing pending events",
                batch_size=len(events),
                event_type_distribution=event_types,
            )

            self._client = ClaimXApiClient(
                base_url=self.config.api.base_url,
                auth_token=self.config.api.auth_token or "",
                timeout_seconds=self.config.api.timeout_seconds,
                max_concurrent=self.config.api.max_concurrent,
            )

            all_rows = EntityRows()
            all_event_logs: List[Dict] = []

            try:
                async with self._client:
                    with log_phase(logger, "ensure_projects"):
                        project_status = await self._ensure_projects_exist(events)

                    valid_events = [
                        e for e in events if project_status.get(e.project_id, False)
                    ]
                    skipped_events = len(events) - len(valid_events)

                    if skipped_events > 0:
                        log_with_context(
                            logger,
                            logging.WARNING,
                            "Skipped events due to missing projects",
                            skipped_count=skipped_events,
                        )

                    with log_phase(logger, "process_handlers"):
                        grouped = self._registry.group_events_by_handler(valid_events)

                        for handler_class, handler_events in grouped.items():
                            handler_name = handler_class.__name__
                            log_with_context(
                                logger,
                                logging.DEBUG,
                                "Processing handler",
                                handler_name=handler_name,
                                events_count=len(handler_events),
                            )

                            handler = handler_class(self._client)
                            result = await handler.process(handler_events)

                            log_with_context(
                                logger,
                                logging.DEBUG,
                                "Handler complete",
                                handler_name=handler_name,
                                records_succeeded=result.succeeded,
                                records_failed=result.failed,
                                failed_permanent=result.failed_permanent,
                                api_calls=result.api_calls,
                            )

                            stats["succeeded"] += result.succeeded
                            stats["failed"] += result.failed
                            stats["failed_permanent"] += result.failed_permanent
                            stats["api_calls"] += result.api_calls

                            all_rows.merge(result.rows)
                            all_event_logs.extend(result.event_logs)

                log_memory_checkpoint(logger, "after_api_calls", config=obs_config)

            finally:
                self._client = None

            with log_phase(logger, "write_entities"):
                if not all_rows.is_empty():
                    table_counts = self._entity_writer.write_all(all_rows)
                    stats["tables_written"] = table_counts
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Wrote to entity tables",
                        tables_written=table_counts,
                    )

            with log_phase(logger, "write_event_logs"):
                if all_event_logs:
                    log_count = self._event_log_writer.write_event_logs(all_event_logs)
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Wrote event log records",
                        records_written=log_count,
                    )

                    processed_ids = [
                        log["event_id"]
                        for log in all_event_logs
                        if log.get("status") in ("success", "failed_permanent")
                    ]
                    if processed_ids:
                        self._processed_cache.mark_processed_batch(processed_ids)

            log_memory_checkpoint(logger, "after_writes", config=obs_config)

            # Update watermark using session with extracted max timestamp
            max_time = extract_max_from_events(events, "ingested_at")
            if max_time:
                wm_session.update_from_timestamp(max_time)

            return stats

    def _get_pending_events(self, watermark: datetime) -> List[ClaimXEvent]:
        """Get events pending enrichment."""
        log_with_context(
            logger,
            logging.DEBUG,
            "Getting pending events",
            watermark=watermark.isoformat(),
            event_types=self.config.processing.event_types,
            lookback_days=self.config.processing.lookback_days,
            max_events_to_scan=self.config.processing.max_events_to_scan,
            batch_size=self.config.processing.batch_size,
        )

        try:
            events_df: Optional[pl.DataFrame] = self._events_reader.read_filtered(
                filter_expr=(pl.col("ingested_at") > pl.lit(watermark))
                & (pl.col("event_type").is_in(self.config.processing.event_types)),
                limit=self.config.processing.max_events_to_scan,
                order_by="ingested_at",
            )

            if events_df is None or events_df.is_empty():
                return []

            log_with_context(
                logger,
                logging.DEBUG,
                "Read events from Delta",
                events_read=len(events_df),
            )

            if not self.config.processing.skip_dedup_check:
                before_dedup = len(events_df)
                events_df = self._exclude_processed(events_df)
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Excluded processed events",
                    before_dedup=before_dedup,
                    after_dedup=len(events_df),
                )
                if events_df.is_empty():
                    return []

            events_df = events_df.head(self.config.processing.batch_size)

            events: List[ClaimXEvent] = []
            parse_failed = 0
            parse_failures: List[Dict] = []
            for row in events_df.iter_rows(named=True):
                try:
                    event = ClaimXEvent.from_kusto_row(row)
                    events.append(event)
                except Exception as e:
                    parse_failed += 1
                    event_id = (
                        row.get("event_id")
                        if isinstance(row, dict)
                        else getattr(row, "event_id", None)
                    )
                    project_id = (
                        row.get("project_id")
                        if isinstance(row, dict)
                        else getattr(row, "project_id", None)
                    )
                    log_exception(
                        logger,
                        e,
                        "Error parsing event row",
                        level=logging.WARNING,
                        include_traceback=False,
                        event_id=event_id,
                        project_id=project_id,
                    )
                    if len(parse_failures) < 3:
                        parse_failures.append(
                            {
                                "event_id": event_id,
                                "project_id": project_id,
                                "error": str(e)[:100],
                            }
                        )

            if parse_failed > 0:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Some events failed to parse",
                    parse_failed=parse_failed,
                    parse_succeeded=len(events),
                    parse_failure_sample=parse_failures if parse_failures else None,
                )

            return events

        except Exception as e:
            log_exception(
                logger,
                e,
                "Error reading events",
                level=logging.WARNING,
                include_traceback=False,
            )
            return []

    def _exclude_processed(self, events_df: pl.DataFrame) -> pl.DataFrame:
        """Exclude events already processed."""
        return self._processed_cache.filter_unprocessed(events_df)

    def get_circuit_status(self) -> Dict:
        """Get API circuit breaker status."""
        if self._client:
            return self._client.get_circuit_status()
        return {"state": "unknown"}

    def close(self) -> None:
        """Clean up resources."""
        log_with_context(logger, logging.DEBUG, "Closing EnrichStage resources")
