# verisk_pipeline/common/memory.py
import gc
from typing import List, Dict, Any, Optional
import psutil
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context
import logging

try:
    import objgraph

    OBJGRAPH_AVAILABLE = True
except ImportError:
    OBJGRAPH_AVAILABLE = False

logger = get_logger(__name__)


def get_memory_mb() -> float:
    """Current process memory in MB."""
    return psutil.Process().memory_info().rss / 1024 / 1024


class MemoryTracker:
    """Track memory across multiple checkpoints within a stage."""

    def __init__(self, stage_name: str, warn_threshold_mb: float = 500):
        self.stage_name = stage_name
        self.warn_threshold_mb = warn_threshold_mb
        self.baseline = get_memory_mb()
        self.peak = self.baseline

    def checkpoint(self, label: str, **extra):
        """Log memory at a checkpoint, track peak."""
        current = get_memory_mb()
        self.peak = max(self.peak, current)
        delta_from_baseline = current - self.baseline

        level = (
            logging.WARNING
            if delta_from_baseline > self.warn_threshold_mb
            else logging.DEBUG
        )
        log_with_context(
            logger,
            level,
            f"Memory checkpoint: {label}",
            stage=self.stage_name,
            memory_mb=round(current, 1),
            memory_delta_mb=round(delta_from_baseline, 1),
            memory_peak_mb=round(self.peak, 1),
            **extra,
        )

    def summary(self) -> dict:
        """Final summary stats."""
        current = get_memory_mb()
        return {
            "memory_baseline_mb": round(self.baseline, 1),
            "memory_final_mb": round(current, 1),
            "memory_peak_mb": round(self.peak, 1),
            "memory_growth_mb": round(current - self.baseline, 1),
        }


class MemoryMonitor:
    """Monitor memory usage with enable/disable capability for production profiling."""

    def __init__(
        self,
        enabled: bool = True,
        stage_name: str = "default",
        warn_threshold_mb: float = 500,
    ):
        """Initialize memory monitor.

        Args:
            enabled: Whether to actively monitor memory
            stage_name: Name of the stage being monitored
            warn_threshold_mb: Warning threshold in MB
        """
        self.enabled = enabled
        self._tracker = (
            MemoryTracker(stage_name, warn_threshold_mb) if enabled else None
        )

    def checkpoint(self, label: str, **extra):
        """Log memory at a checkpoint if monitoring is enabled."""
        if self.enabled and self._tracker:
            self._tracker.checkpoint(label, **extra)

    def snapshot(self, label: str, **extra):
        """Take a memory snapshot if monitoring is enabled (alias for checkpoint)."""
        if self.enabled and self._tracker:
            self._tracker.checkpoint(label, **extra)

    def summary(self) -> dict:
        """Get memory summary if monitoring is enabled."""
        if self.enabled and self._tracker:
            return self._tracker.summary()
        return {}


class LeakDetector:
    """Detect memory leaks in long-running processes (Task E.3).

    Uses objgraph to track object growth over time.
    Suitable for cycle-based processing where memory should be stable.

    Example:
        detector = LeakDetector()
        detector.set_baseline()

        for cycle in range(num_cycles):
            process_cycle(cycle)

            if cycle % 10 == 0:
                detector.check_for_leaks()
    """

    def __init__(self, check_interval: int = 10):
        """Initialize leak detector.

        Args:
            check_interval: Check for leaks every N cycles
        """
        self.check_interval = check_interval
        self.baseline = None
        self.cycle_count = 0

        if not OBJGRAPH_AVAILABLE:
            logger.warning(
                "objgraph not available - leak detection disabled. "
                "Install with: pip install objgraph"
            )

    def set_baseline(self):
        """Establish baseline object counts.

        Call this after initial warmup, before main processing loop.
        """
        if not OBJGRAPH_AVAILABLE:
            return

        gc.collect()  # Force garbage collection
        self.baseline = objgraph.typestats()
        logger.info("Leak detector baseline established")

    def check_for_leaks(self, cycle_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Compare current state to baseline and detect leaks.

        Args:
            cycle_id: Optional cycle identifier for logging

        Returns:
            List of detected leaks with type and growth count
        """
        if not OBJGRAPH_AVAILABLE:
            return []

        if self.baseline is None:
            logger.warning("Leak detector baseline not set, setting now")
            self.set_baseline()
            return []

        self.cycle_count += 1

        # Only check at intervals
        if self.cycle_count % self.check_interval != 0:
            return []

        gc.collect()

        # Get growth since baseline
        growth = objgraph.growth(limit=10, baseline=self.baseline)

        leaks_detected = []
        for typename, delta in growth:
            if delta > 1000:  # Significant growth threshold
                leaks_detected.append({"type": typename, "growth": delta})

                logger.warning(
                    f"Potential memory leak detected: {typename}",
                    extra={
                        "object_type": typename,
                        "object_count_increase": delta,
                        "cycle_id": cycle_id,
                        "cycle_count": self.cycle_count,
                    },
                )

        if not leaks_detected:
            logger.info(
                "Leak check passed",
                extra={"cycle_id": cycle_id, "cycle_count": self.cycle_count},
            )

        return leaks_detected

    def get_most_common_types(self, limit: int = 10) -> List[tuple]:
        """Get most common object types currently in memory.

        Useful for understanding memory composition.

        Args:
            limit: Number of types to return

        Returns:
            List of (typename, count) tuples
        """
        if not OBJGRAPH_AVAILABLE:
            return []

        gc.collect()
        return objgraph.most_common_types(limit)
