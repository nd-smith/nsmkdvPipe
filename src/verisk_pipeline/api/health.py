"""
Health check HTTP endpoint.

Provides /health, /ready, and /metrics endpoints for monitoring and orchestration.
Supports multiple pipeline domains.
"""

import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, List, Optional, Tuple
import json

from verisk_pipeline.common.config.xact import HealthConfig
from verisk_pipeline.common.logging.utilities import log_with_context
from verisk_pipeline.xact.xact_models import HealthStatus
from verisk_pipeline.common.security import sanitize_error_message

logger = logging.getLogger(__name__)


class HealthState:
    """
    Thread-safe health state container.

    Updated by pipeline, read by health endpoint.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._started_at = datetime.now(timezone.utc)
        self._healthy = True
        self._status = "starting"
        self._current_state = "idle"
        self._last_successful_cycle: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._components: Dict[str, str] = {}
        self._cycle_count = 0
        self._error_count = 0
        self._consecutive_errors = 0

        # Track current cycle and circuit breaker states
        self._current_cycle_id: Optional[str] = None
        self._circuit_states: Dict[str, str] = {}
        self._retry_queue_depth: int = 0

    def set_running(self, cycle_id: Optional[str] = None) -> None:
        """Mark pipeline as running."""
        with self._lock:
            self._status = "healthy"
            self._current_state = "running"
            if cycle_id:
                self._current_cycle_id = cycle_id

    def set_idle(self) -> None:
        """Mark pipeline as idle between cycles."""
        with self._lock:
            self._current_state = "idle"
            self._current_cycle_id = None

    def set_cycle_success(self) -> None:
        """Record successful cycle completion."""
        with self._lock:
            self._last_successful_cycle = datetime.now(timezone.utc)
            self._cycle_count += 1
            self._healthy = True
            self._status = "healthy"
            self._last_error = None
            self._consecutive_errors = 0

    def set_cycle_error(self, error: str) -> None:
        """Record cycle error."""
        with self._lock:
            self._error_count += 1
            self._consecutive_errors += 1
            self._last_error = sanitize_error_message(error)
            # Degraded after 1 error, unhealthy after 3 consecutive
            if self._consecutive_errors >= 3:
                self._healthy = False
                self._status = "unhealthy"
            else:
                self._status = "degraded"

    def set_component_status(self, component: str, status: str) -> None:
        """Update component status."""
        with self._lock:
            self._components[component] = status

    def set_circuit_state(self, circuit_name: str, state: str) -> None:
        """Update circuit breaker state."""
        with self._lock:
            self._circuit_states[circuit_name] = state

    def set_retry_queue_depth(self, depth: int) -> None:
        """Update retry queue depth."""
        with self._lock:
            self._retry_queue_depth = depth

    def set_shutting_down(self) -> None:
        """Mark pipeline as shutting down (draining)."""
        with self._lock:
            self._status = "draining"
            self._healthy = False
            self._current_state = "shutting_down"

    @property
    def is_healthy(self) -> bool:
        """Check if healthy."""
        with self._lock:
            return self._healthy

    def get_health_status(self) -> HealthStatus:
        """Get current health status."""
        with self._lock:
            uptime = (datetime.now(timezone.utc) - self._started_at).total_seconds()

            # Build components dict with circuit states
            components = dict(self._components)
            for circuit, state in self._circuit_states.items():
                components[f"circuit_{circuit}"] = state

            return HealthStatus(
                healthy=self._healthy,
                status=self._status,
                uptime_seconds=uptime,
                last_successful_cycle=self._last_successful_cycle,
                current_state=self._current_state,
                components=components,
            )

    def get_metrics(self) -> Dict[str, Any]:
        """Get metrics for monitoring."""
        with self._lock:
            uptime = (datetime.now(timezone.utc) - self._started_at).total_seconds()
            return {
                "uptime_seconds": uptime,
                "cycle_count": self._cycle_count,
                "error_count": self._error_count,
                "consecutive_errors": self._consecutive_errors,
                "retry_queue_depth": self._retry_queue_depth,
                "last_successful_cycle": (
                    self._last_successful_cycle.isoformat()
                    if self._last_successful_cycle
                    else None
                ),
                "current_cycle_id": self._current_cycle_id,
                "circuit_states": dict(self._circuit_states),
            }

    def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed status for debugging."""
        with self._lock:
            health = self.get_health_status()
            metrics = self.get_metrics()
            return {
                "health": health.to_dict(),
                "metrics": metrics,
                "last_error": self._last_error,
            }


# Global health state instance
_health_state: Optional[HealthState] = None


def get_health_state() -> HealthState:
    """Get or create global health state."""
    global _health_state
    if _health_state is None:
        _health_state = HealthState()
    return _health_state


def reset_health_state() -> None:
    """Reset health state (for testing)."""
    global _health_state
    _health_state = None


class HealthRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health endpoints."""

    # Reference to server's pipeline list
    pipelines: List[Tuple[str, Any]] = []

    # Suppress default logging
    def log_message(self, format: str, *args: Any) -> None:
        logger.debug(f"Health endpoint: {args[0]}")

    def _send_json_response(self, status_code: int, data: Dict[str, Any]) -> None:
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _send_text_response(self, status_code: int, text: str) -> None:
        """Send plain text response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(text.encode())

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/ready":
            self._handle_ready()
        elif self.path == "/metrics":
            self._handle_metrics()
        elif self.path == "/status":
            self._handle_status()
        else:
            self.send_error(404, "Not Found")

    def _is_all_healthy(self) -> bool:
        """Check if all pipelines are healthy."""
        if not self.pipelines:
            return get_health_state().is_healthy

        return all(
            p.is_healthy() if hasattr(p, "is_healthy") else True
            for _, p in self.pipelines
        )

    def _handle_health(self) -> None:
        """
        Health check endpoint.

        Returns 200 if all pipelines healthy, 503 if any unhealthy.
        """
        state = get_health_state()
        health = state.get_health_status()

        # Check all pipelines if available
        all_healthy = self._is_all_healthy() and health.healthy
        status_code = 200 if all_healthy else 503

        response = health.to_dict()
        response["all_domains_healthy"] = all_healthy

        self._send_json_response(status_code, response)

    def _handle_ready(self) -> None:
        """
        Readiness check endpoint.

        Returns 200 if ready to receive traffic, 503 if not.
        """
        state = get_health_state()
        health = state.get_health_status()

        # Ready if not in error state
        is_ready = health.status not in ("unhealthy", "draining")
        status_code = 200 if is_ready else 503

        self._send_json_response(
            status_code,
            {
                "ready": is_ready,
                "status": health.status,
            },
        )

    def _handle_metrics(self) -> None:
        """
        Prometheus-format metrics endpoint.

        Returns all pipeline metrics in Prometheus text format.
        """
        from verisk_pipeline.metrics import get_pipeline_metrics

        registry = get_pipeline_metrics()
        prometheus_output = registry.to_prometheus()

        # Append health state metrics
        state = get_health_state()
        state_metrics = state.get_metrics()

        extra_metrics = [
            "",
            "# HELP pipeline_uptime_seconds Pipeline uptime in seconds",
            "# TYPE pipeline_uptime_seconds gauge",
            f"pipeline_uptime_seconds {state_metrics['uptime_seconds']:.1f}",
            "",
            "# HELP pipeline_consecutive_errors Consecutive cycle errors",
            "# TYPE pipeline_consecutive_errors gauge",
            f"pipeline_consecutive_errors {state_metrics['consecutive_errors']}",
        ]

        full_output = prometheus_output + "\n" + "\n".join(extra_metrics)
        self._send_text_response(200, full_output)

    def _handle_status(self) -> None:
        """
        Detailed status endpoint for debugging.

        Returns comprehensive status including per-domain health.
        """
        state = get_health_state()
        detailed = state.get_detailed_status()

        # Add per-domain status
        domains = {}
        for name, pipeline in self.pipelines:
            domain_status = {
                "healthy": (
                    pipeline.is_healthy() if hasattr(pipeline, "is_healthy") else True
                ),
            }
            if hasattr(pipeline, "get_status"):
                domain_status.update(pipeline.get_status())
            if hasattr(pipeline, "get_circuit_status"):
                domain_status["circuits"] = pipeline.get_circuit_status()
            domains[name] = domain_status

        detailed["domains"] = domains

        # Add registry metrics summary
        from verisk_pipeline.metrics import get_pipeline_metrics

        registry = get_pipeline_metrics()
        detailed["registry"] = registry.get_all()

        self._send_json_response(200, detailed)


class HealthServer:
    """
    Health check HTTP server.

    Runs in background thread, provides health endpoints.
    Supports multiple pipeline domains.

    Endpoints:
        /health  - Health check (200/503)
        /ready   - Readiness check (200/503)
        /metrics - Prometheus-format metrics
        /status  - Detailed JSON status for debugging

    Usage:
        server = HealthServer(config, pipelines=[("xact", xact_pipeline)])
        server.start()
        # ... pipeline runs ...
        server.stop()
    """

    def __init__(
        self,
        config: HealthConfig,
        pipelines: Optional[List[Tuple[str, Any]]] = None,
    ):
        self.host = config.host
        self.port = config.port
        self.enabled = config.enabled
        self._pipelines = pipelines or []

        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        """Start health server in background thread."""
        if not self.enabled:
            logger.info("Health server disabled")
            return

        if self._running:
            logger.warning("Health server already running")
            return

        try:
            # Set pipelines on handler class
            HealthRequestHandler.pipelines = self._pipelines

            self._server = HTTPServer((self.host, self.port), HealthRequestHandler)
            self._thread = threading.Thread(target=self._serve, daemon=True)
            self._thread.start()
            self._running = True

            log_with_context(
                logger,
                logging.INFO,
                f"Health server started on {self.host}:{self.port}",
                http_status=200,
            )
        except Exception as e:
            logger.error(f"Failed to start health server: {e}")
            raise

    def _serve(self) -> None:
        """Serve requests (runs in thread)."""
        if self._server:
            self._server.serve_forever()

    def stop(self) -> None:
        """Stop health server."""
        if not self._running:
            return

        if self._server:
            self._server.shutdown()
            self._server = None

        self._running = False
        logger.info("Health server stopped")

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running
