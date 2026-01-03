"""
Retry handling for Kafka pipeline.

DEPRECATED: This module has been moved to kafka_pipeline.common.retry.
This location provides backward compatibility and will be removed in a future version.
Please update imports to use kafka_pipeline.common.retry instead.
"""

# Re-export from new location for backward compatibility
from kafka_pipeline.common.retry import (
    DelayedRedeliveryScheduler,
    RetryHandler,
)

__all__ = [
    "RetryHandler",
    "DelayedRedeliveryScheduler",
]
