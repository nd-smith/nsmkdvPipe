"""
Kafka producer with circuit breaker integration.

Provides async Kafka producer functionality with:
- Circuit breaker protection for resilience
- OAUTHBEARER authentication for Azure EventHub
- Batch sending support
- Header support for message routing
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from pydantic import BaseModel

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging import get_logger, log_with_context, log_exception
from core.resilience.circuit_breaker import (
    CircuitBreaker,
    KAFKA_CIRCUIT_CONFIG,
    get_circuit_breaker,
)
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.metrics import (
    record_message_produced,
    record_producer_error,
    update_connection_status,
    batch_processing_duration_seconds,
)

logger = get_logger(__name__)


class BaseKafkaProducer:
    """
    Async Kafka producer with circuit breaker and authentication.

    Provides reliable message production with:
    - Circuit breaker protection against broker failures
    - Azure AD authentication via OAUTHBEARER
    - Batching support for efficient throughput
    - Message headers for routing metadata

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(config)
        >>> await producer.start()
        >>> try:
        ...     metadata = await producer.send(
        ...         topic="my-topic",
        ...         key="key-123",
        ...         value=my_pydantic_model,
        ...         headers={"trace_id": "evt-456"}
        ...     )
        ... finally:
        ...     await producer.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        """
        Initialize Kafka producer.

        Args:
            config: Kafka configuration
            circuit_breaker: Optional custom circuit breaker (uses default if None)
        """
        self.config = config
        self._producer: Optional[AIOKafkaProducer] = None
        self._circuit_breaker = circuit_breaker or get_circuit_breaker(
            "kafka_producer", KAFKA_CIRCUIT_CONFIG
        )
        self._started = False

        log_with_context(
            logger,
            logging.INFO,
            "Initialized Kafka producer",
            bootstrap_servers=config.bootstrap_servers,
            security_protocol=config.security_protocol,
            sasl_mechanism=config.sasl_mechanism,
        )

    async def start(self) -> None:
        """
        Start the Kafka producer and establish connection.

        Creates the underlying aiokafka producer with authentication
        and connects to the Kafka cluster.

        Raises:
            Exception: If producer fails to start or connect
        """
        if self._started:
            logger.warning("Producer already started, ignoring duplicate start call")
            return

        logger.info("Starting Kafka producer")

        # Create aiokafka producer configuration
        producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "acks": self.config.acks,
            "value_serializer": lambda v: v,  # We'll handle serialization in send()
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Note: aiokafka handles retries internally - we don't pass explicit retry config

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            producer_config["security_protocol"] = self.config.security_protocol
            producer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Add authentication based on mechanism
            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                producer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                # SASL_PLAIN for Event Hubs or basic auth
                producer_config["sasl_plain_username"] = self.config.sasl_plain_username
                producer_config["sasl_plain_password"] = self.config.sasl_plain_password

        # Create aiokafka producer
        self._producer = AIOKafkaProducer(**producer_config)

        await self._producer.start()
        self._started = True

        # Update connection status metric
        update_connection_status("producer", connected=True)

        log_with_context(
            logger,
            logging.INFO,
            "Kafka producer started successfully",
            bootstrap_servers=self.config.bootstrap_servers,
            acks=self.config.acks,
        )

    async def stop(self) -> None:
        """
        Stop the Kafka producer and cleanup resources.

        Flushes any pending messages and closes the connection gracefully.
        Safe to call multiple times.
        """
        if not self._started or self._producer is None:
            logger.debug("Producer not started or already stopped")
            return

        logger.info("Stopping Kafka producer")

        try:
            # Flush any pending messages
            await self._producer.flush()
            # Stop the producer
            await self._producer.stop()
            logger.info("Kafka producer stopped successfully")
        except Exception as e:
            log_exception(
                logger,
                e,
                "Error stopping Kafka producer",
            )
            raise
        finally:
            # Update connection status metric
            update_connection_status("producer", connected=False)
            self._producer = None
            self._started = False

    async def send(
        self,
        topic: str,
        key: str,
        value: BaseModel,
        headers: Optional[Dict[str, str]] = None,
    ) -> RecordMetadata:
        """
        Send a single message to Kafka topic.

        The message is serialized to JSON and sent with the specified key.
        Headers can be provided for routing metadata.

        Args:
            topic: Kafka topic name
            key: Message key (used for partitioning)
            value: Pydantic model to serialize as message value
            headers: Optional key-value pairs for message headers

        Returns:
            RecordMetadata with topic, partition, offset information

        Raises:
            CircuitOpenError: If circuit breaker is open
            Exception: If send operation fails
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        # Serialize value to JSON bytes
        value_bytes = value.model_dump_json().encode("utf-8")

        # Convert headers to list of tuples with byte values
        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        log_with_context(
            logger,
            logging.DEBUG,
            "Sending message to Kafka",
            topic=topic,
            key=key,
            headers=headers,
            value_size=len(value_bytes),
        )

        # Send through circuit breaker
        async def _send():
            return await self._producer.send_and_wait(
                topic,
                key=key.encode("utf-8"),
                value=value_bytes,
                headers=headers_list,
            )

        try:
            metadata = await self._circuit_breaker.call_async(_send)

            # Record successful message production
            record_message_produced(topic, len(value_bytes), success=True)

            log_with_context(
                logger,
                logging.DEBUG,
                "Message sent successfully",
                topic=metadata.topic,
                partition=metadata.partition,
                offset=metadata.offset,
            )

            return metadata

        except Exception as e:
            # Record failed message production
            record_message_produced(topic, len(value_bytes), success=False)
            record_producer_error(topic, type(e).__name__)

            log_exception(
                logger,
                e,
                "Failed to send message",
                topic=topic,
                key=key,
            )
            raise

    async def send_batch(
        self,
        topic: str,
        messages: List[Tuple[str, BaseModel]],
        headers: Optional[Dict[str, str]] = None,
    ) -> List[RecordMetadata]:
        """
        Send a batch of messages to Kafka topic.

        All messages are sent to the same topic. Each message can have
        a different key for partitioning. Headers are applied to all messages.

        Args:
            topic: Kafka topic name
            messages: List of (key, value) tuples to send
            headers: Optional headers applied to all messages

        Returns:
            List of RecordMetadata in same order as input messages

        Raises:
            CircuitOpenError: If circuit breaker is open
            Exception: If any send operation fails
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        if not messages:
            logger.warning("send_batch called with empty message list")
            return []

        log_with_context(
            logger,
            logging.INFO,
            "Sending batch to Kafka",
            topic=topic,
            message_count=len(messages),
            headers=headers,
        )

        # Convert headers to list of tuples
        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        # Track batch processing time
        start_time = time.perf_counter()

        # Send all messages and collect futures
        futures = []
        total_bytes = 0
        for key, value in messages:
            # Serialize value to JSON bytes
            value_bytes = value.model_dump_json().encode("utf-8")
            total_bytes += len(value_bytes)

            # Send message (returns Future)
            future = await self._producer.send(
                topic,
                key=key.encode("utf-8"),
                value=value_bytes,
                headers=headers_list,
            )
            futures.append(future)

        # Wait for all sends to complete through circuit breaker
        async def _wait_for_batch():
            # Await all futures
            results = []
            for future in futures:
                metadata = await future
                results.append(metadata)
            return results

        try:
            results = await self._circuit_breaker.call_async(_wait_for_batch)

            # Record batch metrics
            duration = time.perf_counter() - start_time
            batch_processing_duration_seconds.labels(topic=topic).observe(duration)

            # Record each message as successfully produced
            for _ in results:
                record_message_produced(topic, total_bytes // len(results), success=True)

            log_with_context(
                logger,
                logging.INFO,
                "Batch sent successfully",
                topic=topic,
                message_count=len(results),
                partitions=list({r.partition for r in results}),
                duration_ms=round(duration * 1000, 2),
            )

            return results

        except Exception as e:
            # Record batch failure
            duration = time.perf_counter() - start_time
            batch_processing_duration_seconds.labels(topic=topic).observe(duration)

            # Record failures for each message
            for _ in messages:
                record_message_produced(topic, total_bytes // len(messages), success=False)
            record_producer_error(topic, type(e).__name__)

            log_exception(
                logger,
                e,
                "Failed to send batch",
                topic=topic,
                message_count=len(messages),
                duration_ms=round(duration * 1000, 2),
            )
            raise

    async def flush(self) -> None:
        """
        Flush any pending messages to Kafka.

        Blocks until all buffered messages have been sent.

        Raises:
            RuntimeError: If producer not started
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        logger.debug("Flushing producer")
        await self._producer.flush()

    @property
    def is_started(self) -> bool:
        """Check if producer is started and ready to send messages."""
        return self._started and self._producer is not None


__all__ = [
    "BaseKafkaProducer",
]
