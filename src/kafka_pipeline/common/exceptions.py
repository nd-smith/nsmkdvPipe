"""
Common exception types and error classification for kafka_pipeline.

Provides:
- ErrorCategory enum for retry decisions
- Typed exception hierarchy for pipeline errors
- Error classification utilities
"""

from enum import Enum
from typing import Optional


class ErrorCategory(Enum):
    """
    Classification of error types for handling decisions.

    This enum is used throughout the pipeline to classify errors and determine
    appropriate retry/recovery strategies.

    Categories:
        TRANSIENT: Temporary failures that should retry with backoff
                   (e.g., network timeouts, 429/503 errors)
        AUTH: Authentication failures requiring credential refresh
              (e.g., 401 errors, expired tokens)
        PERMANENT: Non-retriable failures that won't succeed on retry
                   (e.g., 404, validation errors, configuration issues)
        CIRCUIT_OPEN: Circuit breaker is open, rejecting fast without attempting
        UNKNOWN: Unclassified errors, may retry conservatively
    """

    TRANSIENT = "transient"
    AUTH = "auth"
    PERMANENT = "permanent"
    CIRCUIT_OPEN = "circuit_open"
    UNKNOWN = "unknown"


class PipelineError(Exception):
    """
    Base exception for all pipeline errors.

    Attributes:
        message: Human-readable error description
        category: Error classification for retry decisions
        cause: Original exception if wrapping
        context: Additional context dict for debugging
    """

    category: ErrorCategory = ErrorCategory.UNKNOWN

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        context: Optional[dict] = None,
    ):
        self.message = message
        self.cause = cause
        self.context = context or {}
        super().__init__(message)

    @property
    def is_retryable(self) -> bool:
        """Whether this error should trigger a retry."""
        return self.category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.AUTH,
            ErrorCategory.UNKNOWN,
        )

    @property
    def should_refresh_auth(self) -> bool:
        """Whether this error should trigger auth refresh."""
        return self.category == ErrorCategory.AUTH

    def __str__(self) -> str:
        parts = [self.message]
        if self.cause:
            parts.append(f"Caused by: {self.cause}")
        return " | ".join(parts)


# =============================================================================
# Authentication Errors
# =============================================================================


class AuthError(PipelineError):
    """Base class for authentication errors."""

    category = ErrorCategory.AUTH


class TokenExpiredError(AuthError):
    """Token has expired, needs refresh."""

    pass


# =============================================================================
# Network/Connection Errors (Transient)
# =============================================================================


class TransientError(PipelineError):
    """Base class for transient/retriable errors."""

    category = ErrorCategory.TRANSIENT


class ConnectionError(TransientError):
    """Network connection failed (VPN drop, DNS, etc)."""

    pass


class TimeoutError(TransientError):
    """Operation timed out."""

    pass


class ThrottlingError(TransientError):
    """Rate limited (429) - should back off."""

    def __init__(
        self,
        message: str,
        retry_after: Optional[float] = None,
        cause: Optional[Exception] = None,
        context: Optional[dict] = None,
    ):
        super().__init__(message, cause, context)
        self.retry_after = retry_after  # Seconds to wait if provided


class ServiceUnavailableError(TransientError):
    """Service temporarily unavailable (503)."""

    pass


# =============================================================================
# Permanent Errors (Don't Retry)
# =============================================================================


class PermanentError(PipelineError):
    """Base class for permanent/non-retriable errors."""

    category = ErrorCategory.PERMANENT


class NotFoundError(PermanentError):
    """Resource not found (404)."""

    pass


class ForbiddenError(PermanentError):
    """Access denied (403) - permissions issue, not auth."""

    pass


class ValidationError(PermanentError):
    """Data validation failed."""

    pass


class ConfigurationError(PermanentError):
    """Invalid configuration."""

    pass


# =============================================================================
# Circuit Breaker Errors
# =============================================================================


class CircuitOpenError(PipelineError):
    """Circuit breaker is open, rejecting requests."""

    category = ErrorCategory.CIRCUIT_OPEN

    def __init__(
        self,
        circuit_name: str,
        retry_after: float,
        cause: Optional[Exception] = None,
    ):
        message = f"Circuit '{circuit_name}' is open"
        super().__init__(message, cause, {"circuit_name": circuit_name})
        self.circuit_name = circuit_name
        self.retry_after = retry_after


# =============================================================================
# Error Classification Utilities
# =============================================================================


def classify_http_status(status_code: int) -> ErrorCategory:
    """
    Classify HTTP status code into error category.

    Args:
        status_code: HTTP response status

    Returns:
        Appropriate ErrorCategory
    """
    if 200 <= status_code < 300:
        return ErrorCategory.UNKNOWN  # Not an error

    # Auth redirects (302 = redirect to login page)
    if status_code == 302:
        return ErrorCategory.AUTH

    if status_code == 401:
        return ErrorCategory.AUTH

    if status_code == 429:
        return ErrorCategory.TRANSIENT  # Rate limited

    if status_code in (403, 404, 400, 405, 410, 422):
        return ErrorCategory.PERMANENT  # Client errors, won't fix with retry

    if 400 <= status_code < 500:
        return ErrorCategory.PERMANENT  # Other 4xx

    if status_code in (500, 502, 503, 504):
        return ErrorCategory.TRANSIENT  # Server errors, may recover

    if status_code >= 500:
        return ErrorCategory.TRANSIENT  # Other 5xx

    return ErrorCategory.UNKNOWN


def classify_exception(exc: Exception) -> ErrorCategory:
    """
    Classify an exception into error category.

    Args:
        exc: Exception to classify

    Returns:
        Appropriate ErrorCategory
    """
    # Already classified
    if isinstance(exc, PipelineError):
        return exc.category

    # Check exception type
    exc_type = type(exc).__name__.lower()
    exc_str = str(exc).lower()

    # Delta Lake commit conflicts (delta-rs error code 15 = version conflict)
    # These are transient and should be retried with backoff
    delta_conflict_markers = (
        "commitfailederror",
        "failed to commit transaction",
        "transaction conflict",
        "version conflict",
        "concurrent modification",
    )
    if any(m in exc_type or m in exc_str for m in delta_conflict_markers):
        return ErrorCategory.TRANSIENT

    # Connection errors
    connection_markers = (
        "connectionerror",
        "connection refused",
        "connection reset",
        "connection aborted",
        "no route to host",
        "network unreachable",
        "name resolution",
        "dns",
        "socket",
        "broken pipe",
    )
    if any(m in exc_type or m in exc_str for m in connection_markers):
        return ErrorCategory.TRANSIENT

    # Timeout errors
    if "timeout" in exc_type or "timeout" in exc_str:
        return ErrorCategory.TRANSIENT

    # Auth errors (302 = redirect to login, 401 = unauthorized)
    auth_markers = (
        "302",
        "401",
        "unauthorized",
        "authentication",
        "token expired",
        "invalid token",
    )
    if any(m in exc_str for m in auth_markers):
        return ErrorCategory.AUTH

    # Throttling
    if "429" in exc_str or "throttl" in exc_str or "rate limit" in exc_str:
        return ErrorCategory.TRANSIENT

    # Server errors
    if "503" in exc_str or "502" in exc_str or "504" in exc_str:
        return ErrorCategory.TRANSIENT

    # Permission errors (not auth - actual permissions)
    if "403" in exc_str or "forbidden" in exc_str or "access denied" in exc_str:
        return ErrorCategory.PERMANENT

    # Not found
    if "404" in exc_str or "not found" in exc_str:
        return ErrorCategory.PERMANENT

    return ErrorCategory.UNKNOWN


def wrap_exception(
    exc: Exception,
    default_class: type = PipelineError,
    context: Optional[dict] = None,
) -> PipelineError:
    """
    Wrap a generic exception in appropriate PipelineError subclass.

    Args:
        exc: Exception to wrap
        default_class: Class to use if can't classify
        context: Additional context to include

    Returns:
        Appropriate PipelineError subclass instance
    """
    if isinstance(exc, PipelineError):
        if context:
            exc.context.update(context)
        return exc

    category = classify_exception(exc)
    exc_str = str(exc).lower()

    # Map to specific exception types
    if category == ErrorCategory.AUTH:
        if "expired" in exc_str:
            return TokenExpiredError(str(exc), cause=exc, context=context)
        return AuthError(str(exc), cause=exc, context=context)

    if category == ErrorCategory.TRANSIENT:
        if "timeout" in exc_str:
            return TimeoutError(str(exc), cause=exc, context=context)
        if "429" in exc_str or "throttl" in exc_str:
            return ThrottlingError(str(exc), cause=exc, context=context)
        if "503" in exc_str:
            return ServiceUnavailableError(str(exc), cause=exc, context=context)
        return ConnectionError(str(exc), cause=exc, context=context)

    if category == ErrorCategory.PERMANENT:
        if "404" in exc_str or "not found" in exc_str:
            return NotFoundError(str(exc), cause=exc, context=context)
        if "403" in exc_str or "forbidden" in exc_str:
            return ForbiddenError(str(exc), cause=exc, context=context)
        return PermanentError(str(exc), cause=exc, context=context)

    # Default wrapper
    return default_class(str(exc), cause=exc, context=context)
