"""
Dead-letter queue (DLQ) handler for manual review and replay.

DEPRECATED: This module has moved to kafka_pipeline.common.dlq
Import from kafka_pipeline.common.dlq instead.
These re-exports are for backward compatibility only.
"""

# Re-export from new location in common/
from kafka_pipeline.common.dlq.handler import DLQHandler

__all__ = [
    "DLQHandler",
]
