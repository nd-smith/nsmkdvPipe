"""
Backwards-compatibility shim for retry utilities.

This module redirects to the canonical implementation in core.resilience.retry.
All retry functionality has been consolidated into the core package.

For new code, import directly from core.resilience.retry:
    from core.resilience.retry import RetryConfig, with_retry

This module is maintained for backwards compatibility with existing imports:
    from kafka_pipeline.common.retry.decorators import RetryConfig, with_retry
"""

from core.resilience.retry import (
    AUTH_RETRY,
    DEFAULT_RETRY,
    RetryBudget,
    RetryConfig,
    RetryStats,
    with_retry,
)

__all__ = [
    "RetryConfig",
    "RetryStats",
    "RetryBudget",
    "with_retry",
    "DEFAULT_RETRY",
    "AUTH_RETRY",
]
