"""
Backwards-compatibility shim for kafka_pipeline.common.exceptions.

This module re-exports exception types and utilities from their canonical
location in core.errors.exceptions to maintain compatibility with existing
imports like `from kafka_pipeline.common.exceptions import ErrorCategory`.

New code should import directly from core.errors.exceptions or core.types.
"""

# Re-export ErrorCategory from canonical source
from core.types import ErrorCategory

# Re-export all exception classes and utilities from core.errors.exceptions
from core.errors.exceptions import (
    # Base exception
    PipelineError,
    # Auth errors
    AuthError,
    TokenExpiredError,
    # Transient errors
    TransientError,
    ConnectionError,
    TimeoutError,
    ThrottlingError,
    ServiceUnavailableError,
    # Permanent errors
    PermanentError,
    NotFoundError,
    ForbiddenError,
    ValidationError,
    ConfigurationError,
    # Circuit breaker
    CircuitOpenError,
    # Classification utilities
    classify_http_status,
    classify_exception,
    wrap_exception,
    # Error checking utilities
    is_auth_error,
    is_transient_error,
    is_retryable_error,
)

__all__ = [
    # Error category enum
    "ErrorCategory",
    # Base exception
    "PipelineError",
    # Auth errors
    "AuthError",
    "TokenExpiredError",
    # Transient errors
    "TransientError",
    "ConnectionError",
    "TimeoutError",
    "ThrottlingError",
    "ServiceUnavailableError",
    # Permanent errors
    "PermanentError",
    "NotFoundError",
    "ForbiddenError",
    "ValidationError",
    "ConfigurationError",
    # Circuit breaker
    "CircuitOpenError",
    # Classification utilities
    "classify_http_status",
    "classify_exception",
    "wrap_exception",
    # Error checking utilities
    "is_auth_error",
    "is_transient_error",
    "is_retryable_error",
]
