"""
Retry handling for Kafka pipeline.

Provides:
- Retry decorator with intelligent backoff for transient failures (common)
- Delayed redelivery scheduling (common)

NOTE: RetryHandler has been moved to kafka_pipeline.xact.retry.
      Import from there instead: from kafka_pipeline.xact.retry import RetryHandler
      The backwards-compatible re-export here will be removed in a future version.
"""

import warnings

# Generic retry utilities - remain in common
from kafka_pipeline.common.retry.scheduler import DelayedRedeliveryScheduler
from kafka_pipeline.common.retry.decorators import RetryConfig, with_retry, DEFAULT_RETRY, AUTH_RETRY

# Re-export RetryHandler from new location for backwards compatibility
from kafka_pipeline.xact.retry import RetryHandler

# Emit deprecation warning for RetryHandler import from common
warnings.warn(
    "Importing RetryHandler from kafka_pipeline.common.retry is deprecated. "
    "Use 'from kafka_pipeline.xact.retry import RetryHandler' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    # Deprecated - use kafka_pipeline.xact.retry instead
    "RetryHandler",
    # Generic utilities - stay in common
    "DelayedRedeliveryScheduler",
    "RetryConfig",
    "with_retry",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
]
