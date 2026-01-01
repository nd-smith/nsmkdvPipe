"""
Eventhouse integration module for polling events from Microsoft Fabric Eventhouse.

This module provides KQL client and polling capabilities for consuming events
from Eventhouse (KQL tables) as an alternative to Event Hub.
"""

from kafka_pipeline.eventhouse.dedup import (
    DedupConfig,
    EventhouseDeduplicator,
    get_recent_trace_ids_sync,
)
from kafka_pipeline.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)
from kafka_pipeline.eventhouse.poller import (
    KQLEventPoller,
    PollerConfig,
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
