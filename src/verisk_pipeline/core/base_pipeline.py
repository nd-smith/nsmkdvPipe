"""
Base pipeline orchestrator using template method pattern.

Provides common pipeline lifecycle management with abstract hooks for
domain-specific stage execution and metrics handling.
"""

import logging
import signal
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional, Tuple

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


logger = get_logger(__name__)


class BasePipeline(ABC):
    """
    Abstract base class for pipeline orchestrators.

    Implements the template method pattern for pipeline execution, providing
    common lifecycle management while delegating domain-specific logic to
    subclasses.

    Template Methods:
        - run_cycle(): Executes pipeline stages using get_stages()
        - _print_banner(): Uses get_pipeline_name() for display
        - _update_circuit_breaker_health(): Uses subclass circuit tracking

    Abstract Methods (must be implemented by subclasses):
        - get_pipeline_name(): Return pipeline display name
        - get_stages(): Return list of (name, stage) tuples for execution
        - _update_stage_metrics(stage_name, result): Update domain metrics
        - _update_circuit_breaker_health(): Update health with circuit states
        - _init_stages(): Initialize pipeline stages
        - _cleanup_stages(): Cleanup stage resources

    Concrete Methods (shared implementation):
        - _setup_signal_handlers(): Signal handling for graceful shutdown
        - _print_cycle_summary(): Console output formatting
        - _print_shutdown(): Shutdown banner
        - _maybe_upload_logs(): Log upload coordination
        - run_once(): Single cycle execution
        - run(): Continuous execution loop
        - shutdown(): Graceful shutdown coordination

    Usage:
        class XACTPipeline(BasePipeline):
            def get_pipeline_name(self) -> str:
                return "XACT"

            def get_stages(self) -> List[Tuple[str, Optional[Any]]]:
                return [
                    ("ingest", self._ingest_stage),
                    ("download", self._download_stage),
                ]

            def _update_stage_metrics(self, stage_name: str, result) -> None:
                if stage_name == "ingest":
                    self._update_ingest_metrics(result)
    """

    def __init__(self, config: Any):
        """
        Initialize base pipeline components.

        Args:
            config: Pipeline configuration object
        """
        self.config = config

        # Import here to avoid circular imports - these are optional
        self._log_uploader: Optional[Any] = None
        self._health_server: Optional[Any] = None
        self._health_state: Optional[Any] = None
        self._metrics: Optional[Any] = None

        # Lifecycle
        self._shutdown_event = threading.Event()
        self._cycle_count = 0
        self._started_at: Optional[datetime] = None

    def _init_observability(self) -> None:
        """Initialize health and metrics - call from subclass after imports available."""
        try:
            from verisk_pipeline.api.health import get_health_state
            from verisk_pipeline.metrics import get_pipeline_metrics

            self._health_state = get_health_state()
            self._metrics = get_pipeline_metrics()
        except ImportError:
            pass

    @abstractmethod
    def get_pipeline_name(self) -> str:
        """
        Get the display name for this pipeline.

        Returns:
            Pipeline name (e.g., "XACT", "CLAIMX")
        """
        pass

    @abstractmethod
    def get_stages(self) -> List[Tuple[str, Optional[Any]]]:
        """
        Get the list of stages to execute in order.

        Returns:
            List of (stage_name, stage_instance) tuples.
            stage_instance may be None if stage is disabled.

        Example:
            [
                ("ingest", self._ingest_stage),
                ("download", self._download_stage),
                ("retry", self._retry_stage),
            ]
        """
        pass

    @abstractmethod
    def _update_stage_metrics(self, stage_name: str, result: Any) -> None:
        """
        Update domain-specific metrics for a stage.

        Args:
            stage_name: Name of the stage
            result: Stage execution result

        Example:
            if stage_name == "ingest":
                self._metrics.counter("ingest_events_total").inc(result.records_processed)
        """
        pass

    @abstractmethod
    def _update_circuit_breaker_health(self) -> None:
        """
        Update health state with circuit breaker status.

        Each pipeline has different circuit breakers to track.
        Implementation should query stage circuits and update health state.

        Example:
            if self._ingest_stage:
                status = self._ingest_stage.get_circuit_status()
                state = status.get("state", "unknown")
                self._health_state.set_circuit_state("kusto", state)
        """
        pass

    @abstractmethod
    def _init_stages(self) -> None:
        """
        Initialize pipeline stages.

        Subclasses should create stage instances and set up log uploader.

        Example:
            enabled = self.config.schedule.enabled_stages
            if "ingest" in enabled:
                self._ingest_stage = IngestStage(self.config)
            if self.config.observability.log_upload_enabled:
                self._log_uploader = LogUploader(self.config)
        """
        pass

    @abstractmethod
    def _cleanup_stages(self) -> None:
        """
        Cleanup resources for all stages.

        Subclasses should iterate through stages and call close() on each.

        Example:
            for name, stage in self.get_stages():
                if stage is not None:
                    try:
                        stage.close()
                    except Exception as e:
                        log_exception(logger, e, "Error cleaning up stage", stage=name)
        """
        pass

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
            if self._health_state:
                self._health_state.set_shutting_down()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def _print_banner(self) -> None:
        """Print startup banner."""
        print("\n" + "=" * 60)
        print(f"{self.get_pipeline_name()} PIPELINE")
        print("=" * 60)
        print(f"  Started:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Worker ID:     {self.config.worker_id}")
        print(f"  Stages:        {', '.join(self.config.schedule.enabled_stages)}")
        print(f"  Poll interval: {self.config.schedule.interval_seconds}s")

        # Health port (optional, may not exist in all configs)
        if hasattr(self.config, "health") and self.config.health.enabled:
            print(f"  Health port:   {self.config.health.port}")

        # JSON logs (if available in observability config)
        if hasattr(self.config.observability, "json_logs"):
            print(f"  JSON logs:     {self.config.observability.json_logs}")

        if self.config.test_mode:
            print("  Mode:          TEST (single cycle)")
        else:
            print("  Mode:          CONTINUOUS")
        print("=" * 60 + "\n")

    def _print_cycle_summary(self, result: Any) -> None:
        """Print cycle summary to console."""
        from verisk_pipeline.xact.xact_models import StageStatus

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

    def run_cycle(self) -> Any:
        """
        Execute a single pipeline cycle (template method).

        This method implements the common cycle structure and delegates
        stage-specific logic to get_stages() and _update_stage_metrics().

        Returns:
            CycleResult with execution details
        """
        from verisk_pipeline.xact.xact_models import CycleResult

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

        if self._health_state:
            self._health_state.set_running(cycle_id)

        # Execute all stages from subclass
        stages = self.get_stages()
        for stage_name, stage_instance in stages:
            if stage_instance is not None:
                with StageLogContext(stage_name, cycle_id):
                    stage_result = stage_instance.run()
                    stage_result.started_at = datetime.now()

                result.add_stage_result(stage_result)
                self._update_stage_metrics(stage_name, stage_result)

        # Calculate duration
        result.duration_seconds = (datetime.now() - cycle_start).total_seconds()

        # Update health state
        if self._health_state:
            if result.is_success:
                self._health_state.set_cycle_success()
            else:
                errors = [s.error for s in result.stages if s.error]
                self._health_state.set_cycle_error("; ".join(errors))

        # Update circuit breaker health
        self._update_circuit_breaker_health()

        if self._health_state:
            self._health_state.set_idle()

        # Update metrics
        if self._metrics:
            self._metrics.counter("cycles_total").inc()
            if not result.is_success:
                self._metrics.counter("cycles_failed").inc()
            self._metrics.gauge("last_cycle_duration_seconds").set(
                result.duration_seconds
            )

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

    def run_once(self) -> int:
        """
        Run a single pipeline cycle and exit.

        Returns:
            Exit code (0 for success, 1 for failure)
        """
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

        self._cleanup_stages()

        return 0 if result.is_success else 1

    def run(self) -> int:
        """
        Run pipeline continuously until shutdown signal.

        Returns:
            Exit code (0 for clean shutdown, 1 for error)
        """
        # Validate config
        errors = self.config.validate()
        if errors:
            for err in errors:
                log_with_context(logger, logging.ERROR, "Config error", error=err)
            return 1

        self._started_at = datetime.now()
        self._setup_signal_handlers()
        self._init_stages()

        # Start health server (if configured)
        if hasattr(self.config, "health") and self.config.health.enabled:
            try:
                from verisk_pipeline.api.health import HealthServer

                self._health_server = HealthServer(self.config.health)
                self._health_server.start()
            except ImportError:
                pass

        self._print_banner()

        cycle_id = None  # For exception handling scope

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
                            "Test mode: exiting after first cycle",
                        )
                        break

                except Exception as e:
                    log_exception(
                        logger,
                        e,
                        "Error in cycle",
                        cycle_id=cycle_id if cycle_id else None,
                        cycle_number=self._cycle_count,
                    )
                    if self._health_state:
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
                cycle_id=cycle_id if cycle_id else None,
            )
            print(f"\nFATAL ERROR: {e}")
            return 1

        finally:
            # Final log upload on shutdown (if configured)
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
