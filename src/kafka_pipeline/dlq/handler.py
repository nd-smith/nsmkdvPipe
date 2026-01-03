"""
DEPRECATED: This module has moved to kafka_pipeline.common.dlq.handler
Import from kafka_pipeline.common.dlq.handler instead.
This file is for backward compatibility only.
"""

# Re-export everything from new location
from kafka_pipeline.common.dlq.handler import *  # noqa: F401, F403

__all__ = [
    "DLQHandler",
]
