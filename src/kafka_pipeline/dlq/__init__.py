"""Dead-letter queue (DLQ) handler for manual review and replay."""

from kafka_pipeline.dlq.handler import DLQHandler

__all__ = [
    "DLQHandler",
]
