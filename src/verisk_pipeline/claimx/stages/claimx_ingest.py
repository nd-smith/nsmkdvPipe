"""
CLAIMX
Ingest stage: Kusto -> Delta table.
Reads ClaimX events from Kusto/Eventhouse and writes to Delta table.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import polars as pl

from verisk_pipeline.common.auth import clear_token_cache
from verisk_pipeline.common.config.claimx import ClaimXConfig
from verisk_pipeline.common.retry import with_retry
from verisk_pipeline.core.base_stage import BaseStage
from verisk_pipeline.storage.kusto import (
    KustoReader,
    KUSTO_RETRY_CONFIG,
    KUSTO_CIRCUIT_CONFIG,
)
from verisk_pipeline.storage.delta import DeltaTableWriter
from verisk_pipeline.storage.watermark import WatermarkManager
from verisk_pipeline.storage.watermark_helpers import WatermarkSession
from verisk_pipeline.xact.xact_models import StageResult, StageStatus
from verisk_pipeline.common.logging.context_managers import StageLogContext, log_phase
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.decorators import extract_log_context
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_memory_checkpoint,
    log_with_context,
)

logger = get_logger(__name__)


class ClaimXKustoReader(KustoReader):
    """
    ClaimX-specific Kusto reader.

    Extends base KustoReader with event type filtering for ClaimX events.
    """

    def __init__(self, config: ClaimXConfig):
        super().__init__(
            cluster_uri=config.kusto.cluster_uri,
            database=config.kusto.database,
            circuit_name="kusto_claimx",
            circuit_config=KUSTO_CIRCUIT_CONFIG,
        )
        self.table = config.kusto.events_table
        self.event_types = config.processing.event_types

        # Log initialization with config details
        log_with_context(
            logger,
            logging.DEBUG,
            "ClaimXKustoReader initialized",
            cluster_uri=config.kusto.cluster_uri,
            database=config.kusto.database,
            table=self.table,
            event_types=self.event_types,
        )

    def _build_type_filter(self) -> str:
        """Build KQL filter for event types."""
        type_filters = [f"eventType == '{et}'" for et in self.event_types]
        return " or ".join(type_filters)

    @with_retry(config=KUSTO_RETRY_CONFIG, on_auth_error=clear_token_cache)
    def fetch_events(self, watermark: datetime, limit: int = 5000) -> pl.DataFrame:
        """
        Fetch ClaimX events after watermark.

        Args:
            watermark: Fetch events after this timestamp
            limit: Maximum rows to return

        Returns:
            DataFrame with event data
        """
        watermark_str = watermark.strftime("%Y-%m-%dT%H:%M:%S")
        type_filter = self._build_type_filter()

        query = f"""
        {self.table}
        | where ingestion_time() > datetime({watermark_str})
        | where {type_filter}
        | extend IngestionTime = ingestion_time()
        | order by IngestionTime asc
        | take {limit}
        """

        # Log the query at DEBUG level for troubleshooting
        log_with_context(
            logger,
            logging.DEBUG,
            "Executing Kusto query",
            watermark=watermark_str,
            limit=limit,
            event_types_count=len(self.event_types),
            query=query.strip(),
        )

        df = self._circuit_breaker.call(lambda: self._execute_raw(query))

        # Log event type distribution if we have results
        if not df.is_empty():
            try:
                type_counts = df.group_by("eventType").len().to_dicts()
                type_distribution = {
                    row["eventType"]: row["len"] for row in type_counts
                }
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Event type distribution",
                    event_type_counts=type_distribution,
                    total_events=len(df),
                )
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Error processing event in loop",
                    level=logging.WARNING,
                    include_traceback=False,
                )

        log_with_context(
            logger,
            logging.INFO,
            "Fetched events from Kusto",
            records_processed=len(df),
            watermark=watermark_str,
        )
        return df


def flatten_event(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Flatten Kusto event row for Delta table."""
    now = datetime.now(timezone.utc)
    now_str = now.isoformat()

    ingested_at = row.get("IngestionTime")

    # Handle NULL ingestion time - use current time as fallback
    if ingested_at is None:
        ingested_at = now

    # Convert to ISO string if datetime object
    if hasattr(ingested_at, "isoformat"):
        ingested_at_str = ingested_at.isoformat()
        event_date = ingested_at.strftime("%Y-%m-%d")
    else:
        # Already a string
        ingested_at_str = str(ingested_at)
        event_date = ingested_at_str[:10]  # "2025-12-13T10:30:00" -> "2025-12-13"

    # Extract fields
    event_type = _safe_str(row.get("eventType")) or ""
    project_id = _safe_str(row.get("projectId")) or ""
    master_file_name = _safe_str(row.get("masterFileName")) or ""
    media_id = _safe_str(row.get("mediaId")) or ""
    task_id = _safe_str(row.get("taskAssignmentId")) or ""
    video_id = _safe_str(row.get("videoCollaborationId")) or ""

    # Create composite key - master_file_name first for visibility
    key_parts = [
        master_file_name,
        event_type,
        project_id,
        media_id,
        task_id,
        video_id,
        ingested_at_str,
    ]
    event_id = "|".join(p for p in key_parts if p)

    return {
        "event_id": event_id,
        "event_type": event_type,
        "project_id": project_id,
        "media_id": media_id,
        "task_assignment_id": task_id,
        "video_collaboration_id": video_id,
        "ingested_at": ingested_at_str,
        "event_date": event_date,
        "master_file_name": master_file_name,
        "created_at": now_str,
    }


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


class IngestStage(BaseStage):
    """Ingest stage: Kusto -> Delta table."""

    def __init__(self, config: ClaimXConfig):
        super().__init__(config)
        self._kusto_reader = ClaimXKustoReader(config)
        self._delta_writer = DeltaTableWriter(
            config.lakehouse.table_path(config.lakehouse.events_table),
            dedupe_column="event_id",
            dedupe_window_hours=24,
            timestamp_column="ingested_at",
        )
        self._watermark = WatermarkManager("claimx_ingest")

        # Log initialization with config details
        log_with_context(
            logger,
            logging.DEBUG,
            "IngestStage initialized",
            events_table_path=config.lakehouse.table_path(
                config.lakehouse.events_table
            ),
            dedupe_column="event_id",
            dedupe_window_hours=24,
            watermark_name="claimx_ingest",
            event_types=config.processing.event_types,
            lookback_days=config.processing.lookback_days,
            max_events_to_scan=config.processing.max_events_to_scan,
        )

    def get_stage_name(self) -> str:
        """Return stage identifier."""
        return "ingest"

    def _execute(self) -> StageResult:
        """Execute ingest stage."""
        cycle_id = generate_cycle_id()
        with StageLogContext("ingest", cycle_id=cycle_id):
            start = datetime.now(timezone.utc)
            records_processed = 0
            records_written = 0
            flatten_failed = 0
            obs_config = self.config.observability

            log_memory_checkpoint(logger, "stage_start", config=obs_config)

            try:
                # Check circuit breaker - fail fast
                circuit_status = self._kusto_reader.get_circuit_status()
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Circuit breaker status",
                    circuit_state=circuit_status.get("state"),
                    failure_count=circuit_status.get("failure_count"),
                    success_count=circuit_status.get("success_count"),
                )

                if circuit_status["state"] == "open":
                    duration_ms = (
                        datetime.now(timezone.utc) - start
                    ).total_seconds() * 1000
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Kusto circuit is open, skipping ingest",
                        circuit_state=circuit_status["state"],
                    )
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Stage complete",
                        stage="ingest",
                        status="skipped",
                        records_processed=0,
                        records_succeeded=0,
                        records_failed=0,
                        records_failed_permanent=0,
                        duration_ms=duration_ms,
                    )
                    return StageResult(
                        stage_name="ingest",
                        status=StageStatus.SKIPPED,
                        duration_seconds=(
                            datetime.now(timezone.utc) - start
                        ).total_seconds(),
                        error="Kusto circuit breaker open",
                        metadata={"circuit": circuit_status},
                    )

                # Watermark session handles retrieval and updates
                with WatermarkSession(
                    self._watermark,
                    lookback_days=self.config.processing.lookback_days,
                    timestamp_column="IngestionTime",
                    logger=logger,
                ) as wm_session:
                    current_watermark = wm_session.start_watermark

                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Reading events with watermark",
                        watermark=(
                            current_watermark.isoformat() if current_watermark else None
                        ),
                        lookback_days=self.config.processing.lookback_days,
                        max_events_to_scan=self.config.processing.max_events_to_scan,
                    )

                    # Read from Kusto
                    with log_phase(logger, "kusto_fetch"):
                        events_df = self._kusto_reader.fetch_events(
                            watermark=current_watermark,
                            limit=self.config.processing.max_events_to_scan,
                        )

                    log_memory_checkpoint(
                        logger, "after_kusto_read", config=obs_config, df=events_df
                    )

                    if events_df.is_empty():
                        duration_ms = (
                            datetime.now(timezone.utc) - start
                        ).total_seconds() * 1000
                        log_with_context(
                            logger, logging.DEBUG, "No new events from Kusto"
                        )
                        log_memory_checkpoint(logger, "stage_end", config=obs_config)
                        log_with_context(
                            logger,
                            logging.INFO,
                            "Stage complete",
                            stage="ingest",
                            status="success",
                            records_processed=0,
                            records_succeeded=0,
                            records_failed=0,
                            records_failed_permanent=0,
                            duration_ms=duration_ms,
                        )
                        return StageResult(
                            stage_name="ingest",
                            status=StageStatus.SUCCESS,
                            duration_seconds=(
                                datetime.now(timezone.utc) - start
                            ).total_seconds(),
                            records_processed=0,
                            records_succeeded=0,
                            metadata={"circuit": circuit_status},
                        )

                    records_processed = len(events_df)
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Read events from Kusto",
                        records_processed=records_processed,
                    )

                    # Flatten events
                    with log_phase(logger, "flatten_events"):
                        flattened_rows = []
                        for row in events_df.iter_rows(named=True):
                            try:
                                flat = flatten_event(row)
                                if flat.get("event_id"):
                                    flattened_rows.append(flat)
                            except Exception as e:
                                flatten_failed += 1
                                log_exception(
                                    logger,
                                    e,
                                    "Error flattening event",
                                    level=logging.WARNING,
                                    include_traceback=False,
                                    **extract_log_context(row),
                                )

                    # Log transform statistics
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Transform statistics",
                        raw_events=records_processed,
                        flattened_events=len(flattened_rows),
                        flatten_failed=flatten_failed,
                    )

                    if not flattened_rows:
                        duration_ms = (
                            datetime.now(timezone.utc) - start
                        ).total_seconds() * 1000
                        log_with_context(
                            logger, logging.WARNING, "No valid events after flattening"
                        )
                        log_memory_checkpoint(logger, "stage_end", config=obs_config)
                        log_with_context(
                            logger,
                            logging.INFO,
                            "Stage complete",
                            stage="ingest",
                            status="success",
                            records_processed=records_processed,
                            records_succeeded=0,
                            records_failed=flatten_failed,
                            records_failed_permanent=0,
                            duration_ms=duration_ms,
                        )
                        return StageResult(
                            stage_name="ingest",
                            status=StageStatus.SUCCESS,
                            duration_seconds=(
                                datetime.now(timezone.utc) - start
                            ).total_seconds(),
                            records_processed=records_processed,
                            records_succeeded=0,
                        )

                    # Write to Delta
                    write_df = pl.DataFrame(flattened_rows)

                    log_memory_checkpoint(
                        logger, "after_transform", config=obs_config, df=write_df
                    )

                    with log_phase(logger, "delta_write"):
                        records_written = self._delta_writer.append(
                            write_df, dedupe=True
                        )

                    log_with_context(
                        logger,
                        logging.INFO,
                        "Wrote events to Delta table",
                        records_succeeded=records_written,
                        records_deduplicated=len(flattened_rows)
                        - (records_written or 0),
                    )

                    # Update watermark using session (automatic logging and persistence)
                    wm_session.update_from_dataframe(events_df)

                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    duration_ms = duration * 1000

                    log_memory_checkpoint(logger, "stage_end", config=obs_config)

                    log_with_context(
                        logger,
                        logging.INFO,
                        "Stage complete",
                        stage="ingest",
                        status="success",
                        records_processed=records_processed,
                        records_succeeded=records_written or 0,
                        records_failed=flatten_failed,
                        records_failed_permanent=0,
                        duration_ms=duration_ms,
                    )

                    return StageResult(
                        stage_name="ingest",
                        status=StageStatus.SUCCESS,
                        duration_seconds=duration,
                        records_processed=records_processed,
                        records_succeeded=records_written or 0,
                        watermark=wm_session.new_watermark or current_watermark,
                        metadata={"circuit": self._kusto_reader.get_circuit_status()},
                    )

            except Exception as e:
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                log_exception(
                    logger,
                    e,
                    "Stage failed",
                    stage="ingest",
                    duration_ms=duration * 1000,
                )

                log_memory_checkpoint(logger, "stage_end", config=obs_config)

                return StageResult(
                    stage_name="ingest",
                    status=StageStatus.FAILED,
                    duration_seconds=duration,
                    error=str(e),
                    records_processed=records_processed,
                )

    def get_circuit_status(self) -> Dict[str, Any]:
        return self._kusto_reader.get_circuit_status()

    def close(self) -> None:
        log_with_context(logger, logging.DEBUG, "Closing IngestStage resources")
        self._kusto_reader.close()
