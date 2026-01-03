"""
Kafka message schemas - re-exports from xact domain.

All schema definitions have been moved to kafka_pipeline.xact.schemas.
This module provides convenient re-exports.

New code should import directly from kafka_pipeline.xact.schemas:
    from kafka_pipeline.xact.schemas import EventMessage
    from kafka_pipeline.xact.schemas import DownloadTaskMessage
    etc.
"""

# Re-export from xact domain schemas
from kafka_pipeline.xact.schemas import (
    CachedDownloadMessage,
    DownloadResultMessage,
    DownloadTaskMessage,
    EventMessage,
    FailedDownloadMessage,
)

__all__ = [
    "EventMessage",
    "DownloadTaskMessage",
    "CachedDownloadMessage",
    "DownloadResultMessage",
    "FailedDownloadMessage",
]
