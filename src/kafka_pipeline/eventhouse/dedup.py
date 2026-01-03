"""
Deduplication - DEPRECATED

This module has moved to kafka_pipeline.common.eventhouse.dedup
This file provides backward compatibility re-exports.
"""

# Re-export from new location for backward compatibility
from kafka_pipeline.common.eventhouse.dedup import (
    DedupConfig,
    EventhouseDeduplicator,
    get_recent_trace_ids_sync,
)

__all__ = [
    "DedupConfig",
    "EventhouseDeduplicator",
    "get_recent_trace_ids_sync",
]
