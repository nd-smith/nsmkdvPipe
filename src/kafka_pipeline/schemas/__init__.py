"""
Kafka message schemas.

Pydantic models for all Kafka message types with validation and serialization.

Schemas:
    events.py   - EventMessage (raw events from source)
    tasks.py    - DownloadTaskMessage (work items for download workers)
    cached.py   - CachedDownloadMessage (files cached locally, awaiting upload)
    results.py  - DownloadResultMessage, FailedDownloadMessage (outcomes and DLQ)

Design Decisions:
    - Pydantic for validation and JSON serialization
    - Explicit schemas (no dynamic/dict-based messages)
    - Backward-compatible evolution (additive changes only)
    - Datetime fields as ISO 8601 strings

Future Consideration:
    - Avro/Protobuf for schema registry integration
    - Schema versioning strategy
"""

from kafka_pipeline.schemas.cached import CachedDownloadMessage
from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.schemas.results import DownloadResultMessage, FailedDownloadMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage

__all__ = [
    "EventMessage",
    "DownloadTaskMessage",
    "CachedDownloadMessage",
    "DownloadResultMessage",
    "FailedDownloadMessage",
]
