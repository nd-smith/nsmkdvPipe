"""
Xact domain schemas.

Pydantic models for xact events, tasks, results, and cached data.

Schemas:
    events.py   - EventMessage (raw xact events from Eventhouse)
    tasks.py    - DownloadTaskMessage (work items for download workers)
    cached.py   - CachedDownloadMessage (files cached locally, awaiting upload)
    results.py  - DownloadResultMessage, FailedDownloadMessage (outcomes and DLQ)

Design Decisions:
    - Pydantic for validation and JSON serialization
    - Explicit schemas (no dynamic/dict-based messages)
    - Backward-compatible evolution (additive changes only)
    - Datetime fields as ISO 8601 strings
"""

from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.models import EventRecord, Task, XACT_PRIMARY_KEYS
from kafka_pipeline.xact.schemas.results import DownloadResultMessage, FailedDownloadMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

__all__ = [
    "EventMessage",
    "DownloadTaskMessage",
    "CachedDownloadMessage",
    "DownloadResultMessage",
    "FailedDownloadMessage",
    "EventRecord",
    "Task",
    "XACT_PRIMARY_KEYS",
]
