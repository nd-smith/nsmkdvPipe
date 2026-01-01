"""
Kafka consumer workers.

Implements the business logic for each stage of the pipeline.

Workers:
    event_ingester.py   - Consumes raw events, produces download tasks
    download_worker.py  - Downloads attachments to local cache
    upload_worker.py    - Uploads cached files to OneLake
    result_processor.py - Batches successful results, writes to Delta inventory
    dlq_handler.py      - Handles dead-letter queue for manual review

Architecture:
    Download and Upload are decoupled for independent scaling:
    - Download Worker: downloads.pending → local cache → downloads.cached
    - Upload Worker: downloads.cached → OneLake → downloads.results

Scaling:
    - Each worker type is a separate consumer group
    - Horizontal scaling by adding instances (up to partition count)
    - Download and upload workers can be scaled independently

Dependencies:
    - kafka_pipeline.kafka: Base producer/consumer classes
    - kafka_pipeline.schemas: Message schemas
    - core.*: Reusable business logic
"""

from kafka_pipeline.workers.event_ingester import EventIngesterWorker
from kafka_pipeline.workers.result_processor import ResultProcessor
from kafka_pipeline.workers.upload_worker import UploadWorker

__all__ = [
    "EventIngesterWorker",
    "ResultProcessor",
    "UploadWorker",
]
