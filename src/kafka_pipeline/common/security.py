"""
Backwards-compatibility shim for kafka_pipeline security utilities.

This module re-exports security functions from the canonical location.
New code should import directly from core.security.url_validation:

    from core.security.url_validation import (
        extract_filename_from_url,
        sanitize_url,
        sanitize_error_message,
    )
"""

from core.security.url_validation import (
    extract_filename_from_url,
    sanitize_url,
    sanitize_error_message,
    SENSITIVE_PARAMS,
    SENSITIVE_PATTERNS,
)

__all__ = [
    "extract_filename_from_url",
    "sanitize_url",
    "sanitize_error_message",
    "SENSITIVE_PARAMS",
    "SENSITIVE_PATTERNS",
]
