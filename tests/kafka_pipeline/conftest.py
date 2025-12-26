"""
Pytest fixtures for Kafka pipeline integration tests.

Provides fixtures for:
- Docker-based Kafka test containers
- Test Kafka configuration
- Producer and consumer instances
- Topic management
"""

import asyncio
import os
from typing import AsyncGenerator, Generator

import pytest
from testcontainers.kafka import KafkaContainer

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.producer import BaseKafkaProducer


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    """
    Provide a Kafka container for integration tests.

    Uses Testcontainers to start a real Kafka instance in Docker.
    The container runs for the entire test session and is shared across tests.

    Yields:
        KafkaContainer: Started Kafka container instance
    """
    # Create and start Kafka container
    # Uses Confluent Kafka image which includes Zookeeper
    kafka = KafkaContainer()
    kafka.start()

    # Set environment variable for KafkaConfig.from_env()
    # This allows tests to use the test container's bootstrap server
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka.get_bootstrap_server()
    # Disable auth for local testing
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
    os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"

    yield kafka

    # Cleanup: stop container after all tests complete
    kafka.stop()

    # Clean up environment variables
    os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
    os.environ.pop("KAFKA_SECURITY_PROTOCOL", None)
    os.environ.pop("KAFKA_SASL_MECHANISM", None)


@pytest.fixture
def kafka_config(kafka_container: KafkaContainer) -> KafkaConfig:
    """
    Provide test Kafka configuration.

    Creates configuration that points to the test container with
    simplified security settings for local testing.

    Args:
        kafka_container: Test Kafka container fixture

    Returns:
        KafkaConfig: Configuration for test environment
    """
    # Create config from environment (which includes container bootstrap server)
    config = KafkaConfig.from_env()

    # Override security settings for local testing
    config.security_protocol = "PLAINTEXT"
    config.sasl_mechanism = "PLAIN"

    return config


@pytest.fixture
async def kafka_producer(
    kafka_config: KafkaConfig,
) -> AsyncGenerator[BaseKafkaProducer, None]:
    """
    Provide a started Kafka producer for tests.

    Creates and starts a producer instance that connects to the test container.
    Automatically stops and cleans up after the test.

    Args:
        kafka_config: Test Kafka configuration

    Yields:
        BaseKafkaProducer: Started producer instance
    """
    producer = BaseKafkaProducer(config=kafka_config)
    await producer.start()

    yield producer

    # Cleanup: stop producer after test
    await producer.stop()


@pytest.fixture
def unique_topic_prefix(request) -> str:
    """
    Generate unique topic prefix for test isolation.

    Creates a unique prefix based on the test name to ensure
    tests don't interfere with each other by using different topics.

    Args:
        request: Pytest request fixture for test metadata

    Returns:
        str: Unique topic prefix (e.g., "test_produce_consume_123")
    """
    # Use test name to create unique prefix
    test_name = request.node.name
    # Remove special characters that aren't allowed in Kafka topic names
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in test_name)
    return safe_name.lower()[:100]  # Limit length


@pytest.fixture
def test_topics(
    kafka_config: KafkaConfig, unique_topic_prefix: str
) -> dict[str, str]:
    """
    Provide unique test topic names.

    Creates a mapping of logical topic names to actual unique topic names
    for test isolation. Each test gets its own set of topics.

    Args:
        kafka_config: Test Kafka configuration
        unique_topic_prefix: Unique prefix for this test

    Returns:
        dict: Mapping of logical name to actual topic name
    """
    return {
        "pending": f"{unique_topic_prefix}.downloads.pending",
        "results": f"{unique_topic_prefix}.downloads.results",
        "retry_5m": f"{unique_topic_prefix}.downloads.pending.retry.5m",
        "retry_10m": f"{unique_topic_prefix}.downloads.pending.retry.10m",
        "retry_20m": f"{unique_topic_prefix}.downloads.pending.retry.20m",
        "retry_40m": f"{unique_topic_prefix}.downloads.pending.retry.40m",
        "dlq": f"{unique_topic_prefix}.downloads.dlq",
    }


@pytest.fixture
async def kafka_consumer_factory(
    kafka_config: KafkaConfig,
) -> AsyncGenerator[callable, None]:
    """
    Factory fixture for creating Kafka consumers.

    Provides a factory function that creates and tracks consumer instances.
    All created consumers are automatically stopped and cleaned up after the test.

    Args:
        kafka_config: Test Kafka configuration

    Yields:
        callable: Factory function that creates consumers
    """
    created_consumers = []

    async def create_consumer(
        topics: list[str],
        group_id: str,
        message_handler,
    ) -> BaseKafkaConsumer:
        """
        Create and start a consumer for testing.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            message_handler: Async callback for processing messages

        Returns:
            BaseKafkaConsumer: Started consumer instance
        """
        consumer = BaseKafkaConsumer(
            config=kafka_config,
            topics=topics,
            group_id=group_id,
            message_handler=message_handler,
        )
        # Note: Don't start() here - tests control when to start
        # This allows tests to set up handlers before consumption begins
        created_consumers.append(consumer)
        return consumer

    yield create_consumer

    # Cleanup: stop all created consumers
    for consumer in created_consumers:
        if consumer.is_running:
            await consumer.stop()


@pytest.fixture
async def message_collector() -> callable:
    """
    Provide a message collector for testing.

    Creates a collector that accumulates messages consumed by a consumer.
    Useful for asserting on consumed messages in tests.

    Returns:
        callable: Message handler that collects messages
    """
    messages = []

    async def collect(record):
        """Collect consumed message."""
        messages.append(record)

    # Attach messages list to the function for easy access in tests
    collect.messages = messages

    return collect


@pytest.fixture(scope="session", autouse=True)
def wait_for_kafka_ready(kafka_container: KafkaContainer):
    """
    Wait for Kafka to be ready before running tests.

    Ensures the Kafka container is fully started and accepting connections.
    Applied automatically to all tests.

    Args:
        kafka_container: Test Kafka container fixture
    """
    # The container's start() method waits for basic readiness
    # No additional wait needed - testcontainers handles this
    pass
