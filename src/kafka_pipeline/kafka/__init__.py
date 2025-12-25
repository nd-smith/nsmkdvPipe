"""
Kafka infrastructure module.

Provides base classes and utilities for Kafka producers and consumers.

Modules:
    config.py      - KafkaConfig dataclass with environment loading
    producer.py    - BaseKafkaProducer with batching and error handling
    consumer.py    - BaseKafkaConsumer with offset management
    retry.py       - RetryHandler for routing failures to retry topics

Design Decisions:
    - Use aiokafka for async operations
    - Manual offset commits (no auto-commit) for at-least-once
    - Circuit breaker integration for resilience
    - Structured logging with Kafka context (topic, partition, offset)

Configuration:
    - SASL/OAUTHBEARER for Azure Event Hubs compatibility
    - Configurable consumer group prefix
    - Tunable batch sizes and timeouts
"""
