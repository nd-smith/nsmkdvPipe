"""
XACT
Ingest stage: Kusto/Eventhouse -> Delta table.
Reads xact events from Kusto, transforms them, and writes to Delta table.

Error handling:
- Circuit breaker protects against Kusto outages
- Auth errors trigger token refresh and retry
- Transient errors (throttling, timeout) retry with backoff
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, cast

import polars as pl

from verisk_pipeline.common.auth import clear_token_cache
from verisk_pipeline.common.config.xact import PipelineConfig
from verisk_pipeline.common.retry import with_retry
from verisk_pipeline.core.base_stage import BaseStage
from verisk_pipeline.storage.kusto import (
    KustoReader,
    KUSTO_RETRY_CONFIG,
)
from verisk_pipeline.common.circuit_breaker import KUSTO_CIRCUIT_CONFIG
from verisk_pipeline.storage.delta import DeltaTableWriter
from verisk_pipeline.storage.watermark import WatermarkManager
from verisk_pipeline.storage.watermark_helpers import WatermarkSession
from verisk_pipeline.xact.xact_models import StageResult, StageStatus
from verisk_pipeline.xact.stages.transform import flatten_events
from verisk_pipeline.common.logging.context_managers import StageLogContext, log_phase
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_memory_checkpoint,
    log_with_context,
)

logger = get_logger(__name__)


class XactKustoReader(KustoReader):
    """
    Xact-specific Kusto reader.

    Extends base KustoReader with event type filtering for xact events.
    """

    def __init__(self, config: PipelineConfig):
        """
        Args:
            config: Pipeline configuration containing Kusto settings
        """
        super().__init__(
            cluster_uri=config.kusto.cluster_uri,
            database=config.kusto.database,
            circuit_name="kusto_xact",
            circuit_config=KUSTO_CIRCUIT_CONFIG,
        )
        self.table = config.kusto.table_name
        self.event_types = config.processing.event_types

        log_with_context(
            logger,
            logging.DEBUG,
            "XactKustoReader initialized",
            cluster_uri=config.kusto.cluster_uri,
            database=config.kusto.database,
            table=self.table,
            event_types=self.event_types,
        )

    def _build_type_filter(self) -> str:
        """Build KQL filter for event types."""
        type_filters = []
        for event_type in self.event_types:
            if event_type.endswith("*"):
                type_filters.append(f"type startswith '{event_type[:-1]}'")
            else:
                type_filters.append(f"type == '{event_type}'")
        return " or ".join(type_filters)

    @with_retry(config=KUSTO_RETRY_CONFIG, on_auth_error=clear_token_cache)
    def fetch_events(self, watermark: datetime) -> pl.DataFrame:
        """
        Fetch xact events after watermark.

        Uses circuit breaker to protect against Kusto outages.

        Args:
            watermark: Fetch events after this timestamp

        Returns:
            DataFrame with columns: type, version, utcDateTime, traceId, data

        Raises:
            CircuitOpenError: If Kusto circuit breaker is open
            KustoError: On query failure after retries
        """
        watermark_str = watermark.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        type_filter = self._build_type_filter()

        query = f"""
        {self.table}
        | where utcDateTime > datetime({watermark_str})
        | where {type_filter}
        | project type, version, utcDateTime, traceId, data
        | order by utcDateTime asc
        """

        log_with_context(
            logger,
            logging.INFO,
            "Fetching events from Kusto",
            watermark=watermark_str,
            table=self.table,
            event_type_filter=type_filter,
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "Kusto query",
            query=query.strip(),
        )

        # Execute through circuit breaker (inherited from base class)
        df = self._circuit_breaker.call(lambda: self._execute_raw(query))

        # Log event type distribution if we have results
        if not df.is_empty() and "type" in df.columns:
            type_counts = df.group_by("type").len().to_dicts()
            log_with_context(
                logger,
                logging.INFO,
                "Fetched events by type",
                records_processed=len(df),
                event_type_distribution=type_counts,
            )
        else:
            log_with_context(
                logger,
                logging.INFO,
                "Fetched events",
                records_processed=len(df),
            )

        return df

    @with_retry(config=KUSTO_RETRY_CONFIG, on_auth_error=clear_token_cache)
    def fetch_events_range(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> pl.DataFrame:
        """
        Fetch xact events within a time range.

        Uses circuit breaker to protect against Kusto outages.

        Args:
            start_time: Range start (inclusive)
            end_time: Range end (exclusive)

        Returns:
            DataFrame with event data
        """
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        type_filter = self._build_type_filter()

        query = f"""
        {self.table}
        | where utcDateTime >= datetime({start_str}) and utcDateTime < datetime({end_str})
        | where {type_filter}
        | project type, version, utcDateTime, traceId, data
        | order by utcDateTime asc
        """

        log_with_context(
            logger,
            logging.INFO,
            "Fetching events in range from Kusto",
            start_time=start_str,
            end_time=end_str,
            table=self.table,
            event_type_filter=type_filter,
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "Kusto range query",
            query=query.strip(),
        )

        # Execute through circuit breaker
        df = self._circuit_breaker.call(lambda: self._execute_raw(query))

        # Log event type distribution if we have results
        if not df.is_empty() and "type" in df.columns:
            type_counts = df.group_by("type").len().to_dicts()
            log_with_context(
                logger,
                logging.INFO,
                "Fetched events by type",
                records_processed=len(df),
                event_type_distribution=type_counts,
            )
        else:
            log_with_context(
                logger,
                logging.INFO,
                "Fetched events",
                records_processed=len(df),
            )

        return df


class IngestStage(BaseStage):
    """
    Orchestrates the ingest pipeline stage.

    Features:
    - Circuit breaker protection for Kusto
    - Intelligent retry with auth refresh
    - Chunked fetching for catch-up scenarios
    """

    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.kusto_reader = XactKustoReader(config)
        self.delta_writer = DeltaTableWriter(
            table_path=config.lakehouse.events_table_path,
            dedupe_column="trace_id",
            dedupe_window_hours=1,
        )
        self.watermark = WatermarkManager("ingest_events")

        log_with_context(
            logger,
            logging.DEBUG,
            "IngestStage initialized",
            events_table_path=config.lakehouse.events_table_path,
            dedupe_column="trace_id",
            dedupe_window_hours=1,
            watermark_name="ingest_events",
        )

    def get_stage_name(self) -> str:
        """Return stage identifier."""
        return "ingest"

    def _execute(self) -> StageResult:
        """
        Execute the ingest stage.

        Returns:
            StageResult with execution details
        """
        cycle_id = generate_cycle_id()

        with StageLogContext("ingest", cycle_id=cycle_id):
            start_time = datetime.now(timezone.utc)
            records_fetched = 0
            records_written = 0
            obs_config = self.config.observability

            log_memory_checkpoint(logger, "stage_start", config=obs_config)

            try:
                # Check circuit breaker status
                circuit_status = self.kusto_reader.get_circuit_status()
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Circuit breaker status",
                    circuit_name="kusto_xact",
                    circuit_state=circuit_status["state"],
                    failure_count=circuit_status.get("failure_count"),
                    success_count=circuit_status.get("success_count"),
                )

                if circuit_status["state"] == "open":
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Kusto circuit is open, skipping ingest stage",
                        circuit_state=circuit_status["state"],
                        last_failure_time=circuit_status.get("last_failure_time"),
                    )
                    return StageResult(
                        stage_name="ingest",
                        status=StageStatus.SKIPPED,
                        duration_seconds=(
                            datetime.now(timezone.utc) - start_time
                        ).total_seconds(),
                        error="Kusto circuit breaker open",
                        metadata={"circuit": circuit_status},
                    )

                # Calculate lookback days from start_date for watermark session
                start_date = datetime.fromisoformat(self.config.processing.start_date)
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=timezone.utc)
                lookback_days = (datetime.now(timezone.utc) - start_date).days

                # Watermark session handles retrieval and updates
                with WatermarkSession(
                    self.watermark,
                    lookback_days=max(lookback_days, 1),  # Minimum 1 day
                    timestamp_column="ingested_at",
                    logger=logger,
                ) as wm_session:
                    current_watermark = wm_session.start_watermark

                    log_with_context(
                        logger,
                        logging.INFO,
                        "Ingest stage starting",
                        watermark=current_watermark.isoformat(),
                        start_date=self.config.processing.start_date,
                        using_default_watermark=(current_watermark >= start_date),
                    )

                    # Fetch events (chunked if behind)
                    with log_phase(logger, "fetch_events"):
                        df = self._fetch_with_chunking(current_watermark)  # type: ignore

                    log_memory_checkpoint(
                        logger, "after_kusto_read", config=obs_config, df=df
                    )

                    if df.is_empty():
                        duration = (
                            datetime.now(timezone.utc) - start_time
                        ).total_seconds()
                        log_with_context(
                            logger,
                            logging.INFO,
                            "No new events to process",
                            watermark=current_watermark.isoformat(),
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
                            duration_ms=duration * 1000,
                        )

                        return StageResult(
                            stage_name="ingest",
                            status=StageStatus.SUCCESS,
                            records_processed=0,
                            records_succeeded=0,
                            records_failed=0,
                            duration_seconds=duration,
                            watermark=current_watermark,
                            metadata={
                                "circuit": self.kusto_reader.get_circuit_status()
                            },
                        )

                    # Transform
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Starting event transformation",
                        raw_event_count=len(df),
                        columns=df.columns,
                    )

                with log_phase(logger, "flatten_events"):
                    df_flattened = flatten_events(df)

                records_fetched = len(df_flattened)

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Events flattened",
                    raw_event_count=len(df),
                    flattened_count=records_fetched,
                    output_columns=(
                        df_flattened.columns if not df_flattened.is_empty() else []
                    ),
                )

                # Add pipeline columns
                with log_phase(logger, "add_pipeline_columns"):
                    df_flattened = self._add_pipeline_columns(df_flattened)

                log_memory_checkpoint(
                    logger, "after_transform", config=obs_config, df=df_flattened
                )

                # Write to Delta
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Writing to Delta table",
                    rows_to_write=len(df_flattened),
                    table_path=self.config.lakehouse.events_table_path,
                )

                with log_phase(logger, "delta_write"):
                    records_written = self.delta_writer.append(
                        df_flattened, dedupe=False
                    )

                log_with_context(
                    logger,
                    logging.INFO,
                    "Delta write complete",
                    rows_written=records_written,
                    rows_deduplicated=records_fetched - (records_written or 0),
                )

                # Update watermark using session (automatic logging and persistence)
                wm_session.update_from_dataframe(df_flattened)

                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                records_failed = records_fetched - (records_written or 0)

                log_memory_checkpoint(logger, "stage_end", config=obs_config)

                log_with_context(
                    logger,
                    logging.INFO,
                    "Stage complete",
                    stage="ingest",
                    status="success",
                    records_processed=records_fetched,
                    records_succeeded=records_written or 0,
                    records_failed=records_failed,
                    records_failed_permanent=0,
                    duration_ms=duration * 1000,
                    watermark_final=(
                        wm_session.new_watermark.isoformat()
                        if wm_session.new_watermark
                        else current_watermark.isoformat()
                    ),
                )

                return StageResult(
                    stage_name="ingest",
                    status=StageStatus.SUCCESS,
                    records_processed=records_fetched,
                    records_succeeded=records_written or 0,
                    records_failed=records_failed,
                    duration_seconds=duration,
                    watermark=wm_session.new_watermark or current_watermark,
                    metadata={"circuit": self.kusto_reader.get_circuit_status()},
                )

            except Exception as e:
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                log_exception(
                    logger,
                    e,
                    "Stage failed",
                    stage="ingest",
                    duration_ms=duration * 1000,
                    records_fetched=records_fetched,
                    records_written=records_written,
                )

                log_memory_checkpoint(logger, "stage_end", config=obs_config)

                # Include circuit status in error metadata
                try:
                    circuit_status = self.kusto_reader.get_circuit_status()
                except Exception:
                    circuit_status = {"state": "unknown"}

                return StageResult(
                    stage_name="ingest",
                    status=StageStatus.FAILED,
                    duration_seconds=duration,
                    error=str(e),
                    metadata={"circuit": circuit_status},
                    records_processed=records_fetched,
                    records_succeeded=records_written or 0,
                    records_failed=records_fetched - (records_written or 0),
                )

    def _add_pipeline_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add pipeline-controlled columns before writing to Delta.

        Adds:
        - created_at: Current timestamp (when pipeline writes the row)
        - event_date: Date partition column (derived from ingested_at)

        Args:
            df: Flattened DataFrame from transform

        Returns:
            DataFrame with added pipeline columns
        """
        now = datetime.now(timezone.utc)

        return df.with_columns(
            [
                # created_at: when pipeline writes this row
                pl.lit(now).alias("created_at"),
                # event_date: partition column derived from ingested_at
                pl.col("ingested_at").dt.date().alias("event_date"),
            ]
        )

    def _fetch_with_chunking(self, watermark: datetime) -> pl.DataFrame:
        """
        Fetch events, chunking by hour if significantly behind.

        Args:
            watermark: Starting watermark (UTC-aware)

        Returns:
            DataFrame with fetched events
        """
        now = datetime.now(timezone.utc)

        # Watermark is now always UTC-aware from WatermarkManager
        # No defensive conversion needed

        hours_behind = (now - watermark).total_seconds() / 3600

        log_with_context(
            logger,
            logging.DEBUG,
            "Evaluating fetch strategy",
            watermark=watermark.isoformat(),
            current_time=now.isoformat(),
            hours_behind=round(hours_behind, 2),
            chunking_threshold_hours=1.0,
        )

        if hours_behind > 1:
            # Fetch one hour chunk to avoid overwhelming memory
            end_time = watermark + timedelta(hours=1)
            log_with_context(
                logger,
                logging.INFO,
                "Using chunked fetch (behind schedule)",
                hours_behind=round(hours_behind, 1),
                chunk_start=watermark.isoformat(),
                chunk_end=end_time.isoformat(),
                chunk_duration_hours=1.0,
            )
            return cast(
                pl.DataFrame, self.kusto_reader.fetch_events_range(watermark, end_time)
            )
        else:
            log_with_context(
                logger,
                logging.DEBUG,
                "Using standard fetch (current)",
                hours_behind=round(hours_behind, 2),
            )
            return cast(pl.DataFrame, self.kusto_reader.fetch_events(watermark))

    def get_circuit_status(self) -> dict:
        """Get circuit breaker status for health checks."""
        return self.kusto_reader.get_circuit_status()

    def close(self) -> None:
        """Close resources."""
        log_with_context(logger, logging.DEBUG, "Closing IngestStage resources")
        if self.kusto_reader:
            self.kusto_reader.close()
