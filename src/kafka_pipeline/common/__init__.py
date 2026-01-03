"""Common infrastructure shared across all pipeline domains."""

from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer

__all__ = [
    "BaseKafkaConsumer",
    "BaseKafkaProducer",
]
