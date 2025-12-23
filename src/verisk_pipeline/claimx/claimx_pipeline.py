"""
ClaimX pipeline orchestrator.

Coordinates stage execution, scheduling, and lifecycle management.
"""

import logging
import signal
import threading
from datetime import datetime
from typing import Dict, Optional

from verisk_pipeline.common.config.claimx import ClaimXConfig
from verisk_pipeline.api.health import get_health_state
from verisk_pipeline.claimx.stages.claimx_ingest import IngestStage
from verisk_pipeline.claimx.stages.enrich import EnrichStage
from verisk_pipeline.claimx.stages.claimx_download import DownloadStage
from verisk_pipeline.claimx.stages.claimx_retry_stage import RetryStage
from verisk_pipeline.common.logging.context_managers import (
    StageLogContext,
    set_log_context,
)
from verisk_pipeline.common.logging.setup import generate_cycle_id, get_logger
from verisk_pipeline.common.logging.utilities import (
    log_exception,
    log_memory_checkpoint,
    log_with_context,
)
from verisk_pipeline.metrics import (
    get_pipeline_metrics,
    record_circuit_state,
    record_error_category,
)
from verisk_pipeline.storage.log_uploader import LogUploader
from verisk_pipeline.xact.xact_models import CycleResult, StageResult, StageStatus

logger = get_logger(__name__)


class Pipeline:
    """
    ClaimX pipeline orchestrator.

    Manages stage execution, scheduling, health checks, and graceful shutdown.

    Usage:
        config = ClaimXConfig(...)
        pipeline = Pipeline(config)
        pipeline.run()  # Blocks until shutdown
    """

    name = "claimx"

    def __init__(self, config: ClaimXConfig):
        self.config = config

        # Stages
        self._ingest_stage: Optional[IngestStage] = None
        self._enrich_stage: Optional[EnrichStage] = None
        self._download_stage: Optional[DownloadStage] = None
        self._retry_stage: Optional[RetryStage] = None

        # Observability
        self._log_uploader: Optional[LogUploader] = None

        # Health and metrics
        self._health_state = get_health_state()
        self._metrics = get_pipeline_metrics()

        # Lifecycle
        self._shutdown_event = threading.Event()
        self._cycle_count = 0
        self._started_at: Optional[datetime] = None
        self._last_success: Optional[datetime] = None
        self._consecutive_errors = 0

    def _init_stages(self) -> None:
        """Initialize enabled pipeline stages."""
        enabled = self.config.schedule.enabled_stages

        if "ingest" in enabled:
            self._ingest_stage = IngestStage(self.config)
            log_with_context(logger, logging.INFO, "Stage initialized", stage="ingest")

        if "enrich" in enabled:
            self._enrich_stage = EnrichStage(self.config)
            log_with_context(logger, logging.INFO, "Stage initialized", stage="enrich")

        if "download" in enabled:
            self._download_stage = DownloadStage(self.config)
            log_with_context(
                logger, logging.INFO, "Stage initialized", stage="download"
            )

        if "retry" in enabled and self.config.retry.enabled:
            self._retry_stage = RetryStage(self.config)
            log_with_context(logger, logging.INFO, "Stage initialized", stage="retry")

        # Log uploader
        if self.config.observability.log_upload_enabled:
            self._log_uploader = LogUploader(self.config)
            log_with_context(logger, logging.INFO, "Log uploader initialized")

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown signal handlers."""

        def handler(signum: int, frame: object) -> None:
            sig_name = signal.Signals(signum).name
            log_with_context(
                logger,
                logging.INFO,
                "Shutdown signal received",
                signal=sig_name,
            )
            self._shutdown_event.set()
            self._health_state.set_shutting_down()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def _print_banner(self) -> None:
        """Print startup banner."""
        print("\n" + "=" * 60)
        print("CLAIMX PIPELINE")
        print("=" * 60)
        print(f"  Started:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Worker ID:     {self.config.worker_id}")
        print(f"  Stages:        {', '.join(self.config.schedule.enabled_stages)}")
        print(f"  Poll interval: {self.config.schedule.interval_seconds}s")
        if self.config.test_mode:
            print("  Mode:          TEST (single cycle)")
        else:
            print("  Mode:          CONTINUOUS")
        print("=" * 60 + "\n")

    def _print_cycle_summary(self, result: CycleResult) -> None:
        """Print cycle summary to console."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "OK" if result.is_success else "FAILED"

        print("-" * 60)
        print(
            f"CYCLE #{result.cycle_number} [{result.cycle_id}] | {timestamp} | {result.duration_seconds:.1f}s | {status}"
        )
        print("-" * 60)

        for stage in result.stages:
            if stage.status == StageStatus.SKIPPED:
                icon = "-"
            elif stage.is_success:
                icon = "+"
            else:
                icon = "x"

            print(f"  {icon} {stage.stage_name}: ", end="")

            if stage.status == StageStatus.SKIPPED:
                print("skipped")
            elif stage.is_success:
                print(
                    f"processed={stage.records_processed}, succeeded={stage.records_succeeded}"
                )
            else:
                print(f"ERROR - {stage.error}")

        print("-" * 60)

    def _print_shutdown(self) -> None:
        """Print shutdown banner."""
        print("\n" + "=" * 60)
        print("SHUTDOWN")
        print("=" * 60)
        print(f"  Stopped:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Cycles:   {self._cycle_count}")
        if self._started_at:
            uptime = (datetime.now() - self._started_at).total_seconds()
            print(f"  Uptime:   {uptime:.0f}s")
        print("=" * 60 + "\n")

    def run_cycle(self) -> CycleResult:
        """Execute a single pipeline cycle."""
        self._cycle_count += 1
        cycle_start = datetime.now()
        cycle_id = generate_cycle_id()

        set_log_context(cycle_id=cycle_id)

        # Memory checkpoint at cycle start
        log_memory_checkpoint(logger, "cycle_start", config=self.config.observability)

        log_with_context(
            logger,
            logging.INFO,
            "Cycle starting",
            cycle_id=cycle_id,
            cycle_number=self._cycle_count,
        )

        result = CycleResult(
            cycle_number=self._cycle_count,
            started_at=cycle_start,
            cycle_id=cycle_id,
        )

        self._health_state.set_running(cycle_id)

        # Run ingest stage
        if self._ingest_stage:
            with StageLogContext("ingest", cycle_id):
                stage_result = self._ingest_stage.run()
                stage_result.started_at = cycle_start
            result.add_stage_result(stage_result)
            self._update_ingest_metrics(stage_result)

        # Run enrich stage
        if self._enrich_stage:
            with StageLogContext("enrich", cycle_id):
                stage_result = self._enrich_stage.run()
                stage_result.started_at = datetime.now()
            result.add_stage_result(stage_result)
            self._update_enrich_metrics(stage_result)

        # Run download stage
        if self._download_stage:
            with StageLogContext("download", cycle_id):
                stage_result = self._download_stage.run()
                stage_result.started_at = datetime.now()
            result.add_stage_result(stage_result)
            self._update_download_metrics(stage_result)

        # Run retry stage
        if self._retry_stage:
            with StageLogContext("retry", cycle_id):
                stage_result = self._retry_stage.run()
                stage_result.started_at = datetime.now()
            result.add_stage_result(stage_result)
            self._update_retry_metrics(stage_result)

        # Calculate duration
        result.duration_seconds = (datetime.now() - cycle_start).total_seconds()

        # Update health state
        if result.is_success:
            self._last_success = datetime.now()
            self._consecutive_errors = 0
            self._health_state.set_cycle_success()
        else:
            self._consecutive_errors += 1
            errors = [s.error for s in result.stages if s.error]
            self._health_state.set_cycle_error("; ".join(errors))

        # Update circuit breaker health
        self._update_circuit_breaker_health()

        self._health_state.set_idle()

        # Update metrics
        self._metrics.counter("cycles_total").inc()
        if not result.is_success:
            self._metrics.counter("cycles_failed").inc()
        self._metrics.gauge("last_cycle_duration_seconds").set(result.duration_seconds)

        # Maybe upload logs
        self._maybe_upload_logs()

        # Memory checkpoint at cycle end
        log_memory_checkpoint(logger, "cycle_end", config=self.config.observability)

        log_with_context(
            logger,
            logging.INFO,
            "Cycle complete",
            cycle_id=cycle_id,
            cycle_number=self._cycle_count,
            duration_seconds=result.duration_seconds,
            success=result.is_success,
            records_processed=result.total_records_processed,
        )

        return result

    def _maybe_upload_logs(self) -> None:
        """Upload logs if interval reached."""
        if not self._log_uploader:
            return

        interval = self.config.observability.log_upload_interval_cycles
        if self._cycle_count % interval == 0:
            try:
                uploaded = self._log_uploader.upload_logs()
                deleted = self._log_uploader.cleanup_old_logs()
                if uploaded > 0 or deleted > 0:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Log maintenance complete",
                        logs_uploaded=uploaded,
                        logs_deleted=deleted,
                    )
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Log upload failed",
                    level=logging.WARNING,
                    include_traceback=False,
                )

    def _update_circuit_breaker_health(self) -> None:
        """Update health state with circuit breaker status."""
        # Ingest stage - Kusto circuit
        if self._ingest_stage:
            try:
                status = self._ingest_stage.get_circuit_status()
                state = status.get("state", "unknown")
                self._health_state.set_circuit_state("kusto", state)
                record_circuit_state(self._metrics, "claimx", "kusto", state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get Kusto circuit status",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

        # Enrich stage - API circuit
        if self._enrich_stage:
            try:
                status = self._enrich_stage.get_circuit_status()
                state = status.get("state", "unknown")
                self._health_state.set_circuit_state("api", state)
                record_circuit_state(self._metrics, "claimx", "api", state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get API circuit status",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

        # Download stage circuits
        if self._download_stage:
            try:
                dl_status = self._download_stage.get_circuit_status()
                download_state = dl_status.get("download", {}).get("state", "unknown")
                upload_state = dl_status.get("upload", {}).get("state", "unknown")

                self._health_state.set_circuit_state("download", download_state)
                self._health_state.set_circuit_state("upload", upload_state)

                record_circuit_state(
                    self._metrics, "claimx", "download", download_state
                )
                record_circuit_state(self._metrics, "claimx", "upload", upload_state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get download circuit status",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

    def _update_ingest_metrics(self, result: StageResult) -> None:
        """Update metrics from ingest stage."""
        if result.is_success:
            self._metrics.counter("ingest_events_total").inc(result.records_processed)
            self._health_state.set_component_status("kusto", "ok")
            self._health_state.set_component_status("delta_write", "ok")
        else:
            self._health_state.set_component_status("kusto", "error")

    def _update_enrich_metrics(self, result: StageResult) -> None:
        """Update metrics from enrich stage."""
        self._metrics.counter("enrich_events_total").inc(result.records_processed)
        self._metrics.counter("enrich_success_total").inc(result.records_succeeded)

        if result.records_failed > 0:
            self._metrics.counter("enrich_failures_total").inc(result.records_failed)

        api_calls = result.metadata.get("api_calls", 0)
        if api_calls:
            self._metrics.counter("api_calls_total").inc(api_calls)

        if result.is_success:
            self._health_state.set_component_status("api", "ok")
        else:
            self._health_state.set_component_status("api", "error")

        failed_permanent = result.metadata.get("failed_permanent", 0)
        if failed_permanent > 0:
            for _ in range(failed_permanent):
                record_error_category(self._metrics, "claimx", "permanent")

    def _update_download_metrics(self, result: StageResult) -> None:
        """Update metrics from download stage."""
        self._metrics.counter("download_attempts_total").inc(result.records_processed)
        self._metrics.counter("download_success_total").inc(result.records_succeeded)

        if result.records_failed > 0:
            self._metrics.counter("download_failures_total").inc(result.records_failed)

        if result.is_success:
            self._health_state.set_component_status("onelake", "ok")
        else:
            self._health_state.set_component_status("onelake", "error")

        failed_permanent = result.metadata.get("failed_permanent", 0)
        failed_transient = result.metadata.get("failed_transient", 0)

        if failed_permanent > 0:
            for _ in range(failed_permanent):
                record_error_category(self._metrics, "claimx", "permanent")

        if failed_transient > 0:
            for _ in range(failed_transient):
                record_error_category(self._metrics, "claimx", "transient")

    def _update_retry_metrics(self, result: StageResult) -> None:
        """Update metrics from retry stage."""
        self._metrics.counter("retry_attempts_total").inc(result.records_processed)
        self._metrics.counter("retry_success_total").inc(result.records_succeeded)

        if result.records_failed > 0:
            self._metrics.counter("retry_failures_total").inc(result.records_failed)

        # Track error categories from metadata
        failed_permanent = result.metadata.get("failed_permanent", 0)
        failed_transient = result.metadata.get("failed_transient", 0)

        if failed_permanent > 0:
            self._metrics.counter("retry_permanent_total").inc(failed_permanent)
            for _ in range(failed_permanent):
                record_error_category(self._metrics, "claimx", "permanent")

        if failed_transient > 0:
            for _ in range(failed_transient):
                record_error_category(self._metrics, "claimx", "transient")

    def run_once(self) -> int:
        """Run single pipeline cycle and exit."""
        errors = self.config.validate()
        if errors:
            for err in errors:
                log_with_context(logger, logging.ERROR, "Config error", error=err)
            return 1

        self._init_stages()
        self._print_banner()

        result = self.run_cycle()
        self._print_cycle_summary(result)

        self._cleanup_stages()

        return 0 if result.is_success else 1

    def run(self) -> int:
        """Run pipeline continuously until shutdown signal."""
        errors = self.config.validate()
        if errors:
            for err in errors:
                log_with_context(logger, logging.ERROR, "Config error", error=err)
            return 1

        self._started_at = datetime.now()
        self._setup_signal_handlers()
        self._init_stages()
        self._print_banner()

        try:
            while not self._shutdown_event.is_set():
                try:
                    result = self.run_cycle()
                    self._print_cycle_summary(result)

                    if self.config.test_mode:
                        log_with_context(
                            logger, logging.INFO, "Test mode: exiting after first cycle"
                        )
                        break

                except Exception as e:
                    log_exception(
                        logger,
                        e,
                        "Error in cycle",
                        cycle_number=self._cycle_count,
                    )
                    self._consecutive_errors += 1
                    print(f"\n!!! Error in cycle: {e}")

                # Wait for next cycle
                interval = self.config.schedule.interval_seconds
                print(f"  Next cycle in {interval}s...")

                if self._shutdown_event.wait(timeout=interval):
                    break

        except Exception as e:
            log_exception(
                logger,
                e,
                "Fatal error",
            )
            print(f"\nFATAL ERROR: {e}")
            return 1

        finally:
            self._cleanup_stages()
            self._print_shutdown()

        return 0

    def shutdown(self) -> None:
        """Request graceful shutdown."""
        log_with_context(logger, logging.INFO, "Shutdown requested")
        self._shutdown_event.set()

    def _cleanup_stages(self) -> None:
        """Cleanup resources for all stages."""
        stages = [
            (self._ingest_stage, "ingest"),
            (self._enrich_stage, "enrich"),
            (self._download_stage, "download"),
            (self._retry_stage, "retry"),
        ]

        for stage, name in stages:
            if stage is not None:
                try:
                    stage.close()
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Stage resources cleaned up",
                        stage=name,
                    )
                except Exception as e:
                    log_exception(
                        logger,
                        e,
                        "Error cleaning up stage",
                        stage=name,
                        level=logging.WARNING,
                        include_traceback=False,
                    )

    def is_healthy(self) -> bool:
        """Check if pipeline is healthy."""
        return self._consecutive_errors < 3

    def get_status(self) -> Dict:
        """Get pipeline status for health endpoint."""
        status = {
            "healthy": self.is_healthy(),
            "cycles_completed": self._cycle_count,
            "consecutive_errors": self._consecutive_errors,
            "last_success": (
                self._last_success.isoformat() if self._last_success else None
            ),
            "uptime_seconds": (
                (datetime.now() - self._started_at).total_seconds()
                if self._started_at
                else 0
            ),
            "circuits": {},
        }

        # Collect circuit breaker states
        if self._enrich_stage:
            try:
                status["circuits"]["api"] = self._enrich_stage.get_circuit_status()
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get API circuit status for health",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

        if self._download_stage:
            try:
                dl_status = self._download_stage.get_circuit_status()
                status["circuits"]["download"] = dl_status.get("download", {})
                status["circuits"]["upload"] = dl_status.get("upload", {})
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get download circuit status for health",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

        return status
