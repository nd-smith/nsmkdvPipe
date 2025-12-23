"""Observability infrastructure for metrics and monitoring."""

import logging
from typing import Dict, Any
from collections import defaultdict
from threading import Lock

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Simple metrics collector for circuit breaker and other components.

    Collects metrics in memory for export to monitoring systems.
    """

    _instance = None
    _lock = Lock()

    def __new__(cls):
        """Singleton pattern for metrics collector."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize metrics storage."""
        self._gauges: Dict[str, float] = {}
        self._counters: Dict[str, int] = defaultdict(int)
        self._lock = Lock()

    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set gauge metric value.

        Args:
            name: Metric name
            value: Metric value
            labels: Optional labels for metric
        """
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value

            logger.debug(
                f"Gauge metric: {name}={value}",
                extra={"metric_name": name, "value": value, "labels": labels},
            )

    def increment_counter(
        self, name: str, value: int = 1, labels: Dict[str, str] = None
    ):
        """Increment counter metric.

        Args:
            name: Metric name
            value: Increment value (default 1)
            labels: Optional labels for metric
        """
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value

            logger.debug(
                f"Counter metric: {name}+={value}",
                extra={"metric_name": name, "value": value, "labels": labels},
            )

    @staticmethod
    def _make_key(name: str, labels: Dict[str, str] = None) -> str:
        """Create metric key from name and labels.

        Args:
            name: Metric name
            labels: Optional labels

        Returns:
            Combined key string
        """
        if not labels:
            return name

        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics.

        Returns:
            Dictionary of all metrics
        """
        with self._lock:
            return {"gauges": dict(self._gauges), "counters": dict(self._counters)}

    def reset(self):
        """Reset all metrics."""
        with self._lock:
            self._gauges.clear()
            self._counters.clear()


def log_all_metrics():
    """Log all collected metrics.

    Call this periodically (e.g., every 60 seconds) to export metrics.
    """
    collector = MetricsCollector()
    metrics = collector.get_all_metrics()

    logger.info(
        "Metrics snapshot",
        extra={
            "gauges": metrics["gauges"],
            "counters": metrics["counters"],
            "metric_count": len(metrics["gauges"]) + len(metrics["counters"]),
        },
    )
