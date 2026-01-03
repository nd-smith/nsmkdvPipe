"""
Eventhouse integration module for polling events from Microsoft Fabric Eventhouse.

This module provides KQL client and polling capabilities for consuming events
from Eventhouse (KQL tables) as an alternative to Event Hub.

DEPRECATED: This module has moved to kafka_pipeline.common.eventhouse
Import from kafka_pipeline.common.eventhouse instead.
This module provides backward compatibility re-exports.
"""

# Re-export from new location for backward compatibility
from kafka_pipeline.common.eventhouse import (
    DedupConfig,
    EventhouseConfig,
    EventhouseDeduplicator,
    KQLClient,
    KQLEventPoller,
    KQLQueryResult,
    PollerConfig,
    get_recent_trace_ids_sync,
)

__all__ = [
    # KQL Client
    "EventhouseConfig",
    "KQLClient",
    "KQLQueryResult",
    # Deduplication
    "DedupConfig",
    "EventhouseDeduplicator",
    "get_recent_trace_ids_sync",
    # Poller
    "KQLEventPoller",
    "PollerConfig",
]
