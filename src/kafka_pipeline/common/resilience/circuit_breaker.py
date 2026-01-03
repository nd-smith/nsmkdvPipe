"""
Circuit breaker pattern for resilience against cascading failures.

Protects against scenarios like:
- VPN disconnects causing thousands of failures
- Upstream service outages
- Network partitions

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Failing, requests rejected immediately (fast-fail)
- HALF_OPEN: Testing recovery, limited requests allowed

Usage:
    # Decorator style (recommended)
    @circuit_protected("api")
    def call_api():
        ...

    # Manual style
    breaker = get_circuit_breaker("downloads")
    result = breaker.call(lambda: download_file())
"""

import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Callable, Dict, Optional, TypeVar

from kafka_pipeline.common.exceptions import (
    CircuitOpenError,
    ErrorCategory,
    PipelineError,
    classify_exception,
)
from kafka_pipeline.common.logging import log_exception, log_with_context

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Rejecting requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    # Number of failures before opening circuit
    failure_threshold: int = 5

    # Number of successes in half-open before closing
    success_threshold: int = 2

    # Seconds to wait in open state before testing
    timeout_seconds: float = 30.0

    # Max concurrent calls allowed in half-open
    half_open_max_calls: int = 3

    # Error categories that count as failures (None = all errors)
    failure_categories: Optional[tuple] = None

    # If True, auth errors don't count toward failure threshold
    # (they should trigger token refresh, not circuit open)
    ignore_auth_errors: bool = True


# Circuit breaker config for ClaimX API
# - Single service, all endpoints share fate
# - 5 failures: API issues are usually systematic
# - 60s timeout: Give time for service recovery
CLAIMX_API_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    success_threshold=2,
    timeout_seconds=60.0,
    half_open_max_calls=3,
)


@dataclass
class CircuitStats:
    """Statistics for circuit breaker monitoring."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    last_state_change_time: Optional[float] = None
    current_state: str = "closed"


class CircuitBreaker:
    """
    Circuit breaker implementation with exception-aware failure tracking.

    Thread-safe for concurrent access.
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.on_state_change = on_state_change

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0

        self._stats = CircuitStats()
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state (may transition on access)."""
        with self._lock:
            self._check_state_transition()
            return self._state

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (rejecting)."""
        return self.state == CircuitState.OPEN

    @property
    def stats(self) -> CircuitStats:
        """Get copy of current statistics."""
        with self._lock:
            self._check_state_transition()
            return CircuitStats(
                total_calls=self._stats.total_calls,
                successful_calls=self._stats.successful_calls,
                failed_calls=self._stats.failed_calls,
                rejected_calls=self._stats.rejected_calls,
                state_changes=self._stats.state_changes,
                last_failure_time=self._stats.last_failure_time,
                last_success_time=self._stats.last_success_time,
                last_state_change_time=self._stats.last_state_change_time,
                current_state=self._state.value,
            )

    def _should_count_failure(self, exc: Exception) -> bool:
        """Determine if exception should count toward failure threshold."""
        # Get error category
        if isinstance(exc, PipelineError):
            category = exc.category
        else:
            category = classify_exception(exc)

        # Ignore auth errors if configured
        if self.config.ignore_auth_errors and category == ErrorCategory.AUTH:
            log_with_context(
                logger,
                logging.DEBUG,
                "Ignoring auth error for failure count",
                circuit_name=self.name,
                error_category=category.value,
            )
            return False

        # Check against allowed failure categories
        if self.config.failure_categories:
            return category in self.config.failure_categories

        # By default, only count transient and unknown errors
        # Permanent errors are application bugs, not service issues
        return category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.UNKNOWN,
            ErrorCategory.CIRCUIT_OPEN,  # Downstream circuit issues
        )

    def _check_state_transition(self) -> None:
        """Check if state should transition (called under lock)."""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.config.timeout_seconds:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Circuit breaker timeout elapsed, transitioning to half-open",
                        circuit_name=self.name,
                        elapsed_seconds=round(elapsed, 2),
                        timeout_seconds=self.config.timeout_seconds,
                        failure_count=self._failure_count,
                    )
                    self._transition_to(CircuitState.HALF_OPEN)

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to new state (called under lock)."""
        old_state = self._state
        if old_state == new_state:
            return

        self._state = new_state
        self._stats.state_changes += 1
        self._stats.last_state_change_time = time.time()
        self._stats.current_state = new_state.value

        # Reset counters on state change
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            log_with_context(
                logger,
                logging.INFO,
                "Circuit closed",
                circuit_name=self.name,
                circuit_state="closed",
            )
        elif new_state == CircuitState.HALF_OPEN:
            self._success_count = 0
            self._half_open_calls = 0
            log_with_context(
                logger,
                logging.INFO,
                "Circuit half-open",
                circuit_name=self.name,
                circuit_state="half_open",
            )
        elif new_state == CircuitState.OPEN:
            self._success_count = 0
            log_with_context(
                logger,
                logging.WARNING,
                "Circuit open",
                circuit_name=self.name,
                circuit_state="open",
                timeout_seconds=self.config.timeout_seconds,
            )

        if self.on_state_change:
            try:
                self.on_state_change(old_state, new_state)
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Error in circuit state change callback",
                    circuit_name=self.name,
                    level=logging.WARNING,
                    include_traceback=False,
                )

    def _record_success(self) -> None:
        """Record successful call (called under lock)."""
        self._stats.successful_calls += 1
        self._stats.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._transition_to(CircuitState.CLOSED)
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success (consecutive failure tracking)
            self._failure_count = 0

    def _record_failure(self, exc: Exception) -> None:
        """Record failed call (called under lock)."""
        self._stats.failed_calls += 1
        self._stats.last_failure_time = time.time()

        # Check if this error should count
        if not self._should_count_failure(exc):
            log_with_context(
                logger,
                logging.DEBUG,
                "Circuit breaker failure not counted",
                circuit_name=self.name,
                circuit_state=self._state.value,
                error_type=type(exc).__name__,
                error_message=str(exc)[:200],
            )
            return

        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Any counted failure in half-open goes back to open
            log_with_context(
                logger,
                logging.DEBUG,
                "Circuit breaker failure recorded (half-open)",
                circuit_name=self.name,
                error_type=type(exc).__name__,
                error_message=str(exc)[:200],
                action="transitioning to open",
            )
            self._transition_to(CircuitState.OPEN)
        elif self._state == CircuitState.CLOSED:
            self._failure_count += 1
            log_with_context(
                logger,
                logging.DEBUG,
                "Circuit breaker failure recorded",
                circuit_name=self.name,
                circuit_state=self._state.value,
                error_type=type(exc).__name__,
                error_message=str(exc)[:200],
                failure_count=self._failure_count,
                failure_threshold=self.config.failure_threshold,
            )
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def _can_execute(self) -> bool:
        """Check if call can proceed (called under lock)."""
        self._check_state_transition()

        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            return False

        # HALF_OPEN: allow limited calls for testing
        if self._half_open_calls < self.config.half_open_max_calls:
            self._half_open_calls += 1
            return True

        return False

    def _get_retry_after(self) -> float:
        """Get seconds until circuit might close."""
        if self._last_failure_time is None:
            return 0.0
        elapsed = time.time() - self._last_failure_time
        return max(0, self.config.timeout_seconds - elapsed)

    def call(self, func: Callable[[], T]) -> T:
        """
        Execute function through circuit breaker.

        Args:
            func: Function to execute

        Returns:
            Result of function

        Raises:
            CircuitOpenError: If circuit is open
            Exception: Any exception from func (also recorded as failure)
        """
        with self._lock:
            self._stats.total_calls += 1

            if not self._can_execute():
                self._stats.rejected_calls += 1
                retry_after = self._get_retry_after()
                raise CircuitOpenError(self.name, retry_after)

        # Execute outside lock
        try:
            result = func()
            with self._lock:
                self._record_success()
            return result
        except Exception as e:
            with self._lock:
                self._record_failure(e)
            raise

    def record_success(self) -> None:
        """Manually record a success (for context manager pattern)."""
        with self._lock:
            self._stats.total_calls += 1
            self._record_success()

    def record_failure(self, exc: Exception) -> None:
        """Manually record a failure (for context manager pattern)."""
        with self._lock:
            self._stats.total_calls += 1
            self._record_failure(exc)

    def reset(self) -> None:
        """Manually reset circuit to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0
            self._last_failure_time = None
            log_with_context(
                logger,
                logging.INFO,
                "Circuit manually reset",
                circuit_name=self.name,
            )

    def get_diagnostics(self) -> dict:
        """Get diagnostic info for health checks."""
        with self._lock:
            self._check_state_transition()
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "config": {
                    "failure_threshold": self.config.failure_threshold,
                    "success_threshold": self.config.success_threshold,
                    "timeout_seconds": self.config.timeout_seconds,
                },
                "stats": {
                    "total_calls": self._stats.total_calls,
                    "successful_calls": self._stats.successful_calls,
                    "failed_calls": self._stats.failed_calls,
                    "rejected_calls": self._stats.rejected_calls,
                    "state_changes": self._stats.state_changes,
                },
            }


# =============================================================================
# Circuit Breaker Registry
# =============================================================================

_breakers: Dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_circuit_breaker(
    name: str,
    config: Optional[CircuitBreakerConfig] = None,
) -> CircuitBreaker:
    """
    Get or create a named circuit breaker.

    Args:
        name: Unique name for the circuit breaker
        config: Configuration (only used on first creation)

    Returns:
        CircuitBreaker instance
    """
    with _registry_lock:
        if name not in _breakers:
            _breakers[name] = CircuitBreaker(name, config)
            log_with_context(
                logger,
                logging.DEBUG,
                "Created circuit breaker",
                circuit_name=name,
            )
        return _breakers[name]


def circuit_protected(
    name: str,
    config: Optional[CircuitBreakerConfig] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to protect a function with a circuit breaker.

    Args:
        name: Name of the circuit breaker to use
        config: Configuration (only used on first creation)

    Returns:
        Decorated function that executes through circuit breaker

    Raises:
        CircuitOpenError: If circuit is open
        Exception: Any exception from the wrapped function

    Usage:
        @circuit_protected("api")
        def call_api():
            ...

        @circuit_protected("downloads", CLAIMX_API_CIRCUIT_CONFIG)
        def download_file(url: str):
            ...
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            breaker = get_circuit_breaker(name, config)
            return breaker.call(lambda: func(*args, **kwargs))

        return wrapper

    return decorator
