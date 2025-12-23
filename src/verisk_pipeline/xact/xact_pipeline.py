"""
Pipeline orchestrator.

Coordinates stage execution, scheduling, and lifecycle management.
"""

import logging
import signal
import threading
from datetime import datetime
from typing import Optional
import os

os.environ["RUST_LOG"] = "error"
os.environ["POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY"] = "1"
from verisk_pipeline.api.health import HealthServer, get_health_state
from verisk_pipeline.common.config.xact import PipelineConfig, get_config
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
    Timer,
    get_pipeline_metrics,
    record_circuit_state,
    record_error_category,
)
from verisk_pipeline.xact.xact_models import CycleResult, StageResult, StageStatus
from verisk_pipeline.xact.stages.xact_ingest import IngestStage
from verisk_pipeline.xact.stages.xact_download import DownloadStage
from verisk_pipeline.xact.stages.xact_retry_stage import RetryStage
from verisk_pipeline.storage.log_uploader import LogUploader


logger = get_logger(__name__)


class Pipeline:
    """
    Main pipeline orchestrator.

    Manages stage execution, scheduling, health checks, and graceful shutdown.

    Usage:
        config = get_config()
        pipeline = Pipeline(config)
        pipeline.run()  # Blocks until shutdown
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or get_config()

        # Stages
        self._ingest_stage: Optional[IngestStage] = None
        self._download_stage: Optional[DownloadStage] = None
        self._retry_stage: Optional[RetryStage] = None

        # Observability
        self._log_uploader: Optional[LogUploader] = None

        # Health and metrics
        self._health_server: Optional[HealthServer] = None
        self._health_state = get_health_state()
        self._metrics = get_pipeline_metrics()

        # Lifecycle
        self._shutdown_event = threading.Event()
        self._cycle_count = 0
        self._started_at: Optional[datetime] = None

    def _init_stages(self) -> None:
        """Initialize enabled pipeline stages."""
        enabled = self.config.schedule.enabled_stages

        if "ingest" in enabled:
            self._ingest_stage = IngestStage(self.config)
            log_with_context(logger, logging.INFO, "Stage initialized", stage="ingest")

        if "download" in enabled:
            self._download_stage = DownloadStage(self.config)
            log_with_context(
                logger, logging.INFO, "Stage initialized", stage="download"
            )

        if "retry" in enabled:
            self._retry_stage = RetryStage(self.config)
            log_with_context(logger, logging.INFO, "Stage initialized", stage="retry")

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
        print("XACT PIPELINE")
        print("=" * 60)
        print(f"  Started:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Worker ID:     {self.config.worker_id}")
        print(f"  Stages:        {', '.join(self.config.schedule.enabled_stages)}")
        print(f"  Poll interval: {self.config.schedule.interval_seconds}s")
        print(
            f"  Health port:   {self.config.health.port if self.config.health.enabled else 'disabled'}"
        )
        print(f"  JSON logs:     {self.config.observability.json_logs}")
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
            status_icon = "+" if stage.is_success else "x"
            if stage.status == StageStatus.SKIPPED:
                status_icon = "-"

            print(f"  {status_icon} {stage.stage_name}: ", end="")

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
                record_circuit_state(self._metrics, "xact", "kusto", state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get Kusto circuit status",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

        # Download stage circuits
        if self._download_stage:
            try:
                download_status = (
                    self._download_stage._download_breaker.get_diagnostics()
                )
                upload_status = self._download_stage._upload_breaker.get_diagnostics()

                download_state = download_status.get("state", "unknown")
                upload_state = upload_status.get("state", "unknown")

                self._health_state.set_circuit_state("download", download_state)
                self._health_state.set_circuit_state("upload", upload_state)

                record_circuit_state(self._metrics, "xact", "download", download_state)
                record_circuit_state(self._metrics, "xact", "upload", upload_state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get download circuit status",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

        # Retry stage circuits (same as download)
        if self._retry_stage:
            try:
                status = self._retry_stage.get_circuit_status()
                download_state = status.get("download", {}).get("state", "unknown")
                upload_state = status.get("upload", {}).get("state", "unknown")

                # Only update if not already set by download stage
                if not self._download_stage:
                    self._health_state.set_circuit_state("download", download_state)
                    self._health_state.set_circuit_state("upload", upload_state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Failed to get retry circuit status",
                    level=logging.DEBUG,
                    include_traceback=False,
                )

    def run_cycle(self) -> CycleResult:
        """
        Execute a single pipeline cycle.

        Returns:
            CycleResult with execution details
        """
        self._cycle_count += 1
        cycle_start = datetime.now()
        cycle_id = generate_cycle_id()

        # Set cycle context for all logs
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
                with Timer(self._metrics.histogram("cycle_duration_seconds")):
                    stage_result = self._ingest_stage.run()
                    stage_result.started_at = cycle_start

            result.add_stage_result(stage_result)
            self._update_ingest_metrics(stage_result)

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
            self._health_state.set_cycle_success()
        else:
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

    def _update_retry_metrics(self, result: StageResult) -> None:
        """Update metrics from retry stage result."""
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
                record_error_category(self._metrics, "xact", "permanent")

        if failed_transient > 0:
            for _ in range(failed_transient):
                record_error_category(self._metrics, "xact", "transient")

    def _update_ingest_metrics(self, result: StageResult) -> None:
        """Update metrics from ingest stage result."""
        if result.is_success:
            self._metrics.counter("ingest_events_total").inc(result.records_processed)
            self._metrics.counter("ingest_events_written").inc(result.records_succeeded)
            self._health_state.set_component_status("kusto", "ok")
            self._health_state.set_component_status("delta_write", "ok")
        else:
            self._health_state.set_component_status("kusto", "error")

            # Check if circuit breaker tripped
            circuit_status = result.metadata.get("circuit", {})
            if circuit_status.get("state") == "open":
                self._metrics.counter("circuit_breaker_trips_total").inc()

    def _update_download_metrics(self, result: StageResult) -> None:
        """Update metrics from download stage result."""
        self._metrics.counter("download_attempts_total").inc(result.records_processed)
        self._metrics.counter("download_success_total").inc(result.records_succeeded)

        if result.records_failed > 0:
            self._metrics.counter("download_failures_total").inc(result.records_failed)

        if result.is_success:
            self._health_state.set_component_status("onelake", "ok")
        else:
            self._health_state.set_component_status("onelake", "error")

        # Track error categories from metadata
        failed_permanent = result.metadata.get("failed_permanent", 0)
        failed_transient = result.metadata.get("failed_transient", 0)
        failed_circuit = result.metadata.get("failed_circuit", 0)

        if failed_permanent > 0:
            for _ in range(failed_permanent):
                record_error_category(self._metrics, "xact", "permanent")

        if failed_transient > 0:
            for _ in range(failed_transient):
                record_error_category(self._metrics, "xact", "transient")

        if failed_circuit > 0:
            self._metrics.counter("circuit_breaker_trips_total").inc()
            for _ in range(failed_circuit):
                record_error_category(self._metrics, "xact", "circuit_open")

    def run_once(self) -> int:
        """
        Run a single pipeline cycle and exit.

        Returns:
            Exit code (0 for success, 1 for failure)
        """
        # Note: logging setup moved to main.py

        # Validate config
        errors = self.config.validate()
        if errors:
            for err in errors:
                log_with_context(logger, logging.ERROR, "Config error", error=err)
            return 1

        self._init_stages()
        self._print_banner()

        result = self.run_cycle()
        self._print_cycle_summary(result)

        return 0 if result.is_success else 1

    def run(self) -> int:
        """
        Run pipeline continuously until shutdown signal.

        Returns:
            Exit code (0 for clean shutdown, 1 for error)
        """
        # Note: logging setup moved to main.py

        # Validate config
        errors = self.config.validate()
        if errors:
            for err in errors:
                log_with_context(logger, logging.ERROR, "Config error", error=err)
            return 1

        self._started_at = datetime.now()
        self._setup_signal_handlers()
        self._init_stages()

        # Start health server
        if self.config.health.enabled:
            self._health_server = HealthServer(self.config.health)
            self._health_server.start()

        self._print_banner()

        try:
            while not self._shutdown_event.is_set():
                try:
                    result = self.run_cycle()
                    self._print_cycle_summary(result)

                    # Test mode: exit after first cycle
                    if self.config.test_mode:
                        log_with_context(
                            logger,
                            logging.INFO,
                            "Test mode exit",
                            cycles_completed=1,
                        )
                        break

                except Exception as e:
                    log_exception(
                        logger,
                        e,
                        "Error in cycle",
                        cycle_number=self._cycle_count,
                    )
                    self._health_state.set_cycle_error(str(e))
                    print(f"\n!!! Error in cycle: {e}")

                # Wait for next cycle or shutdown
                interval = self.config.schedule.interval_seconds
                print(f"  Next cycle in {interval}s...")

                if self._shutdown_event.wait(timeout=interval):
                    break  # Shutdown requested

        except Exception as e:
            log_exception(
                logger,
                e,
                "Fatal error",
            )
            print(f"\nFATAL ERROR: {e}")
            return 1

        finally:
            # Final log upload on shutdown (with timeout)
            if self._log_uploader:
                try:
                    upload_thread = threading.Thread(
                        target=self._log_uploader.force_upload_all
                    )
                    upload_thread.start()
                    upload_thread.join(timeout=30)
                    if upload_thread.is_alive():
                        log_with_context(
                            logger,
                            logging.WARNING,
                            "Log upload timed out during shutdown",
                        )
                except Exception as e:
                    log_exception(
                        logger,
                        e,
                        "Final log upload failed",
                        level=logging.WARNING,
                        include_traceback=False,
                    )
                try:
                    self._log_uploader.close()
                except Exception:
                    pass

            # Cleanup
            self._cleanup_stages()
            if self._health_server:
                self._health_server.stop()
            self._print_shutdown()

        return 0

    def shutdown(self) -> None:
        """Request graceful shutdown."""
        log_with_context(logger, logging.INFO, "Shutdown requested")
        self._shutdown_event.set()

    def _cleanup_stages(self) -> None:
        """Cleanup resources for all stages."""
        for stage, name in [
            (self._ingest_stage, "ingest"),
            (self._download_stage, "download"),
            (self._retry_stage, "retry"),
        ]:
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


def run_pipeline(
    config: Optional[PipelineConfig] = None,
    mode: str = "continuous",
) -> int:
    """
    Convenience function to run pipeline.

    Args:
        config: Pipeline configuration (uses default if None)
        mode: "continuous" or "once"

    Returns:
        Exit code
    """
    pipeline = Pipeline(config)

    if mode == "once":
        return pipeline.run_once()
    else:
        return pipeline.run()
