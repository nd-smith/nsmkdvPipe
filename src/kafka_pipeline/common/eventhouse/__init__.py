"""Eventhouse/KQL infrastructure."""

from kafka_pipeline.common.eventhouse.dedup import (
    DedupConfig,
    EventhouseDeduplicator,
    get_recent_trace_ids_sync,
)
from kafka_pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)
from kafka_pipeline.common.eventhouse.poller import (
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
