"""
Kafka consumer workers.

Implements the business logic for each stage of the pipeline.

Workers:
    event_ingester.py   - Consumes raw events, produces download tasks
    download_worker.py  - Downloads attachments, uploads to OneLake
    result_processor.py - Batches successful results, writes to Delta inventory
    dlq_handler.py      - Handles dead-letter queue for manual review

Scaling:
    - Each worker type is a separate consumer group
    - Horizontal scaling by adding instances (up to partition count)
    - Download workers are the primary scaling target

Dependencies:
    - kafka_pipeline.kafka: Base producer/consumer classes
    - kafka_pipeline.schemas: Message schemas
    - core.*: Reusable business logic
"""

from kafka_pipeline.workers.event_ingester import EventIngesterWorker
from kafka_pipeline.workers.result_processor import ResultProcessor

__all__ = [
    "EventIngesterWorker",
    "ResultProcessor",
]
