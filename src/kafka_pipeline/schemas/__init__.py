"""
Kafka message schemas.

Pydantic models for all Kafka message types with validation and serialization.

Schemas:
    events.py   - EventMessage (raw events from source)
    tasks.py    - DownloadTaskMessage (work items for download workers)
    results.py  - DownloadResultMessage (download outcomes)
    dlq.py      - FailedDownloadMessage (dead-letter queue entries)

Design Decisions:
    - Pydantic for validation and JSON serialization
    - Explicit schemas (no dynamic/dict-based messages)
    - Backward-compatible evolution (additive changes only)
    - Datetime fields as ISO 8601 strings

Future Consideration:
    - Avro/Protobuf for schema registry integration
    - Schema versioning strategy
"""
