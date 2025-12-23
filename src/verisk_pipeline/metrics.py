"""
Simple metrics collection for pipeline observability.

Provides counters, gauges, and histograms with optional label support.
Optional Prometheus-format export.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class MetricValue:
    """Single metric with metadata."""

    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


class LabeledMetric:
    """Base class for metrics with label support."""

    def __init__(
        self, name: str, description: str = "", label_names: Tuple[str, ...] = ()
    ):
        self.name = name
        self.description = description
        self.label_names = label_names
        self._lock = threading.Lock()

    def _label_key(self, labels: Dict[str, str]) -> Tuple:
        """Create hashable key from labels."""
        return tuple(labels.get(k, "") for k in self.label_names)


class Counter(LabeledMetric):
    """
    Monotonically increasing counter with optional labels.

    Usage:
        # Without labels
        events_processed = Counter("events_processed", "Total events processed")
        events_processed.inc()

        # With labels
        events = Counter("events_total", "Events by domain", ("domain", "status"))
        events.labels(domain="xact", status="success").inc()
    """

    def __init__(
        self, name: str, description: str = "", label_names: Tuple[str, ...] = ()
    ):
        super().__init__(name, description, label_names)
        self._values: Dict[Tuple, float] = {}
        if not label_names:
            self._values[()] = 0.0

    def inc(self, amount: float = 1.0) -> None:
        """Increment counter (for unlabeled use)."""
        if amount < 0:
            raise ValueError("Counter can only be incremented")
        with self._lock:
            self._values[()] = self._values.get((), 0.0) + amount

    def labels(self, **kwargs) -> "CounterChild":
        """Get labeled counter child."""
        return CounterChild(self, kwargs)

    def get(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current value."""
        with self._lock:
            key = self._label_key(labels) if labels else ()
            return self._values.get(key, 0.0)

    def get_all(self) -> Dict[Tuple, float]:
        """Get all values with their label keys."""
        with self._lock:
            return dict(self._values)

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self._values = {(): 0.0} if not self.label_names else {}

    def _inc_labeled(self, labels: Dict[str, str], amount: float) -> None:
        """Internal: increment with labels."""
        if amount < 0:
            raise ValueError("Counter can only be incremented")
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount


class CounterChild:
    """Child counter with bound labels."""

    def __init__(self, parent: Counter, labels: Dict[str, str]):
        self._parent = parent
        self._labels = labels

    def inc(self, amount: float = 1.0) -> None:
        """Increment counter."""
        self._parent._inc_labeled(self._labels, amount)


class Gauge(LabeledMetric):
    """
    Gauge that can go up or down, with optional labels.

    Usage:
        # Without labels
        active_downloads = Gauge("active_downloads", "Current active downloads")
        active_downloads.set(5)

        # With labels
        queue = Gauge("queue_depth", "Queue depth by domain", ("domain",))
        queue.labels(domain="xact").set(10)
    """

    def __init__(
        self, name: str, description: str = "", label_names: Tuple[str, ...] = ()
    ):
        super().__init__(name, description, label_names)
        self._values: Dict[Tuple, float] = {}
        if not label_names:
            self._values[()] = 0.0

    def set(self, value: float) -> None:
        """Set gauge to value (for unlabeled use)."""
        with self._lock:
            self._values[()] = value

    def inc(self, amount: float = 1.0) -> None:
        """Increment gauge."""
        with self._lock:
            self._values[()] = self._values.get((), 0.0) + amount

    def dec(self, amount: float = 1.0) -> None:
        """Decrement gauge."""
        with self._lock:
            self._values[()] = self._values.get((), 0.0) - amount

    def labels(self, **kwargs) -> "GaugeChild":
        """Get labeled gauge child."""
        return GaugeChild(self, kwargs)

    def get(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current value."""
        with self._lock:
            key = self._label_key(labels) if labels else ()
            return self._values.get(key, 0.0)

    def get_all(self) -> Dict[Tuple, float]:
        """Get all values with their label keys."""
        with self._lock:
            return dict(self._values)

    def _set_labeled(self, labels: Dict[str, str], value: float) -> None:
        """Internal: set with labels."""
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = value

    def _inc_labeled(self, labels: Dict[str, str], amount: float) -> None:
        """Internal: increment with labels."""
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount


class GaugeChild:
    """Child gauge with bound labels."""

    def __init__(self, parent: Gauge, labels: Dict[str, str]):
        self._parent = parent
        self._labels = labels

    def set(self, value: float) -> None:
        """Set gauge value."""
        self._parent._set_labeled(self._labels, value)

    def inc(self, amount: float = 1.0) -> None:
        """Increment gauge."""
        self._parent._inc_labeled(self._labels, amount)

    def dec(self, amount: float = 1.0) -> None:
        """Decrement gauge."""
        self._parent._inc_labeled(self._labels, -amount)


class Histogram:
    """
    Simple histogram for tracking distributions.

    Tracks count, sum, min, max, and configurable buckets.

    Usage:
        download_duration = Histogram("download_duration_seconds", buckets=[0.1, 0.5, 1, 5, 10])
        download_duration.observe(0.35)
    """

    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    def __init__(
        self,
        name: str,
        description: str = "",
        buckets: Optional[tuple] = None,
    ):
        self.name = name
        self.description = description
        self.buckets = buckets or self.DEFAULT_BUCKETS

        self._lock = threading.Lock()
        self._count = 0
        self._sum = 0.0
        self._min: Optional[float] = None
        self._max: Optional[float] = None
        self._bucket_counts = {b: 0 for b in self.buckets}

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._count += 1
            self._sum += value

            if self._min is None or value < self._min:
                self._min = value
            if self._max is None or value > self._max:
                self._max = value

            for bucket in self.buckets:
                if value <= bucket:
                    self._bucket_counts[bucket] += 1

    def get_stats(self) -> Dict:
        """Get histogram statistics."""
        with self._lock:
            return {
                "count": self._count,
                "sum": self._sum,
                "min": self._min,
                "max": self._max,
                "avg": self._sum / self._count if self._count > 0 else 0,
                "buckets": dict(self._bucket_counts),
            }

    def reset(self) -> None:
        """Reset histogram."""
        with self._lock:
            self._count = 0
            self._sum = 0.0
            self._min = None
            self._max = None
            self._bucket_counts = {b: 0 for b in self.buckets}


class Timer:
    """
    Context manager for timing operations.

    Usage:
        with Timer(histogram):
            do_operation()

        # Or manually:
        timer = Timer(histogram)
        timer.start()
        do_operation()
        timer.stop()
    """

    def __init__(self, histogram: Optional[Histogram] = None):
        self.histogram = histogram
        self._start_time: Optional[float] = None
        self._duration: Optional[float] = None

    def start(self) -> "Timer":
        """Start timer."""
        self._start_time = time.time()
        return self

    def stop(self) -> float:
        """Stop timer and return duration."""
        if self._start_time is None:
            raise RuntimeError("Timer not started")

        self._duration = time.time() - self._start_time

        if self.histogram:
            self.histogram.observe(self._duration)

        return self._duration

    @property
    def duration(self) -> Optional[float]:
        """Get recorded duration."""
        return self._duration

    def __enter__(self) -> "Timer":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


class MetricsRegistry:
    """
    Central registry for all metrics.

    Usage:
        registry = MetricsRegistry()
        counter = registry.counter("events_total", "Total events", ("domain",))
        counter.labels(domain="xact").inc()
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}

    def counter(
        self,
        name: str,
        description: str = "",
        label_names: Tuple[str, ...] = (),
    ) -> Counter:
        """Get or create a counter."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, description, label_names)
            return self._counters[name]

    def gauge(
        self,
        name: str,
        description: str = "",
        label_names: Tuple[str, ...] = (),
    ) -> Gauge:
        """Get or create a gauge."""
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, description, label_names)
            return self._gauges[name]

    def histogram(
        self,
        name: str,
        description: str = "",
        buckets: Optional[tuple] = None,
    ) -> Histogram:
        """Get or create a histogram."""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, description, buckets)
            return self._histograms[name]

    def get_all(self) -> Dict[str, Dict]:
        """Get all metrics as dictionary."""
        with self._lock:
            result = {
                "counters": {},
                "gauges": {},
                "histograms": {},
            }

            for name, c in self._counters.items():
                all_values = c.get_all()
                if c.label_names:
                    result["counters"][name] = {
                        "description": c.description,
                        "labels": c.label_names,
                        "values": {str(k): v for k, v in all_values.items()},
                    }
                else:
                    result["counters"][name] = {
                        "value": c.get(),
                        "description": c.description,
                    }

            for name, g in self._gauges.items():
                all_values = g.get_all()
                if g.label_names:
                    result["gauges"][name] = {
                        "description": g.description,
                        "labels": g.label_names,
                        "values": {str(k): v for k, v in all_values.items()},
                    }
                else:
                    result["gauges"][name] = {
                        "value": g.get(),
                        "description": g.description,
                    }

            for name, h in self._histograms.items():
                result["histograms"][name] = {
                    **h.get_stats(),
                    "description": h.description,
                }

            return result

    def to_prometheus(self) -> str:
        """
        Export metrics in Prometheus text format.

        Returns:
            Prometheus-compatible metrics string
        """
        lines = []

        with self._lock:
            # Counters
            for name, counter in self._counters.items():
                if counter.description:
                    lines.append(f"# HELP {name} {counter.description}")
                lines.append(f"# TYPE {name} counter")

                all_values = counter.get_all()
                if counter.label_names:
                    for label_key, value in all_values.items():
                        label_str = ",".join(
                            f'{k}="{v}"' for k, v in zip(counter.label_names, label_key)
                        )
                        lines.append(f"{name}{{{label_str}}} {value}")
                else:
                    lines.append(f"{name} {counter.get()}")

            # Gauges
            for name, gauge in self._gauges.items():
                if gauge.description:
                    lines.append(f"# HELP {name} {gauge.description}")
                lines.append(f"# TYPE {name} gauge")

                all_values = gauge.get_all()
                if gauge.label_names:
                    for label_key, value in all_values.items():
                        label_str = ",".join(
                            f'{k}="{v}"' for k, v in zip(gauge.label_names, label_key)
                        )
                        lines.append(f"{name}{{{label_str}}} {value}")
                else:
                    lines.append(f"{name} {gauge.get()}")

            # Histograms
            for name, hist in self._histograms.items():
                stats = hist.get_stats()
                if hist.description:
                    lines.append(f"# HELP {name} {hist.description}")
                lines.append(f"# TYPE {name} histogram")

                cumulative = 0
                for bucket, count in sorted(stats["buckets"].items()):
                    cumulative += count
                    lines.append(f'{name}_bucket{{le="{bucket}"}} {cumulative}')
                lines.append(f'{name}_bucket{{le="+Inf"}} {stats["count"]}')
                lines.append(f"{name}_sum {stats['sum']}")
                lines.append(f"{name}_count {stats['count']}")

        return "\n".join(lines)

    def reset_all(self) -> None:
        """Reset all metrics."""
        with self._lock:
            for c in self._counters.values():
                c.reset()
            for g in self._gauges.values():
                with g._lock:
                    g._values = {(): 0.0} if not g.label_names else {}
            for h in self._histograms.values():
                h.reset()


# Global registry instance
_registry: Optional[MetricsRegistry] = None


def get_registry() -> MetricsRegistry:
    """Get or create global metrics registry."""
    global _registry
    if _registry is None:
        _registry = MetricsRegistry()
    return _registry


def reset_registry() -> None:
    """Reset global registry (for testing)."""
    global _registry
    _registry = None


def get_pipeline_metrics() -> MetricsRegistry:
    """
    Get registry with pre-defined pipeline metrics.

    All metrics include domain label for multi-pipeline support.

    Returns:
        MetricsRegistry with standard pipeline metrics
    """
    registry = get_registry()

    # Cycle metrics with domain label
    registry.counter(
        "pipeline_cycles_total", "Total pipeline cycles", ("domain", "stage")
    )
    registry.counter(
        "pipeline_cycles_failed", "Failed pipeline cycles", ("domain", "stage")
    )

    # Ingest stage metrics
    registry.counter(
        "pipeline_ingest_events_total", "Events ingested from Kusto", ("domain",)
    )
    registry.counter(
        "pipeline_ingest_events_written", "Events written to Delta", ("domain",)
    )

    # Download stage metrics
    registry.counter(
        "pipeline_download_attempts_total", "Download attempts", ("domain", "status")
    )
    registry.counter(
        "pipeline_download_success_total", "Successful downloads", ("domain",)
    )
    registry.counter(
        "pipeline_download_failures_total", "Failed downloads", ("domain",)
    )

    # Retry stage metrics
    registry.counter("pipeline_retry_attempts_total", "Retry attempts", ("domain",))
    registry.counter("pipeline_retry_success_total", "Successful retries", ("domain",))
    registry.counter("pipeline_retry_failures_total", "Failed retries", ("domain",))
    registry.counter("pipeline_retry_exhausted_total", "Retries exhausted", ("domain",))
    registry.counter(
        "pipeline_retry_permanent_total", "Permanent failures discovered", ("domain",)
    )

    # Error category breakdown
    registry.counter(
        "pipeline_errors_total", "Errors by category", ("domain", "category")
    )

    # Circuit breaker metrics
    registry.counter(
        "pipeline_circuit_trips_total", "Circuit breaker trips", ("domain", "circuit")
    )
    registry.gauge(
        "pipeline_circuit_state",
        "Circuit state (0=closed, 1=open, 2=half-open)",
        ("domain", "circuit"),
    )

    # Queue depth gauges
    registry.gauge("pipeline_retry_queue_depth", "Pending retry count", ("domain",))
    registry.gauge("pipeline_active_downloads", "Active downloads", ("domain",))

    # Duration histograms (without domain label for simplicity)
    registry.histogram(
        "pipeline_download_duration_seconds",
        "Download duration in seconds",
        buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
    )
    registry.histogram(
        "pipeline_cycle_duration_seconds",
        "Pipeline cycle duration in seconds",
        buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
    )

    return registry


# Circuit breaker state mapping for metrics
CIRCUIT_STATE_VALUES = {
    "closed": 0,
    "open": 1,
    "half_open": 2,
    "half-open": 2,
}


def record_circuit_state(
    registry: MetricsRegistry,
    domain: str,
    circuit_name: str,
    state: str,
) -> None:
    """
    Record circuit breaker state as gauge value.

    Args:
        registry: Metrics registry
        domain: Pipeline domain (xact, claimx)
        circuit_name: Circuit breaker name (kusto, download, upload)
        state: Circuit state string
    """
    state_value = CIRCUIT_STATE_VALUES.get(state.lower(), -1)
    registry.gauge("pipeline_circuit_state", "", ("domain", "circuit")).labels(
        domain=domain, circuit=circuit_name
    ).set(state_value)


def record_error_category(
    registry: MetricsRegistry, domain: str, category: str
) -> None:
    """
    Increment error counter by category.

    Args:
        registry: Metrics registry
        domain: Pipeline domain
        category: Error category (transient, permanent, auth, circuit_open)
    """
    registry.counter("pipeline_errors_total", "", ("domain", "category")).labels(
        domain=domain, category=category
    ).inc()


# Alias for convenience
record_error = record_error_category


# Convenience functions for common operations
def inc_cycle(domain: str, stage: str) -> None:
    """Increment cycle counter."""
    get_registry().counter("pipeline_cycles_total", "", ("domain", "stage")).labels(
        domain=domain, stage=stage
    ).inc()


def inc_cycle_failed(domain: str, stage: str) -> None:
    """Increment failed cycle counter."""
    get_registry().counter("pipeline_cycles_failed", "", ("domain", "stage")).labels(
        domain=domain, stage=stage
    ).inc()


def inc_ingest_events(domain: str, count: int = 1) -> None:
    """Increment ingest events counter."""
    get_registry().counter("pipeline_ingest_events_total", "", ("domain",)).labels(
        domain=domain
    ).inc(count)


def inc_download(domain: str, status: str) -> None:
    """Increment download counter."""
    get_registry().counter(
        "pipeline_download_attempts_total", "", ("domain", "status")
    ).labels(domain=domain, status=status).inc()


def set_retry_queue_depth(domain: str, depth: int) -> None:
    """Set retry queue depth gauge."""
    get_registry().gauge("pipeline_retry_queue_depth", "", ("domain",)).labels(
        domain=domain
    ).set(depth)
