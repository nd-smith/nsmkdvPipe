"""
Kafka consumer with circuit breaker integration.

Provides async Kafka consumer functionality with:
- Manual offset commit for at-least-once processing
- Circuit breaker protection for resilience
- OAUTHBEARER authentication for Azure EventHub
- Multiple topic subscription
- Graceful shutdown handling
- Message handler pattern for processing logic
- Intelligent error classification and routing
"""

import asyncio
import logging
from typing import Awaitable, Callable, List, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.errors.exceptions import CircuitOpenError, ErrorCategory
from core.errors.kafka_classifier import KafkaErrorClassifier
from core.resilience.circuit_breaker import (
    CircuitBreaker,
    KAFKA_CIRCUIT_CONFIG,
    get_circuit_breaker,
)
from kafka_pipeline.config import KafkaConfig

logger = logging.getLogger(__name__)


class BaseKafkaConsumer:
    """
    Async Kafka consumer with circuit breaker and authentication.

    Provides reliable message consumption with:
    - Manual offset commit for at-least-once processing
    - Circuit breaker protection against broker failures
    - Azure AD authentication via OAUTHBEARER
    - Multiple topic subscription
    - Graceful shutdown handling
    - Custom message handler pattern

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> async def handle_message(record: ConsumerRecord):
        ...     # Process message
        ...     print(f"Received: {record.value}")
        >>>
        >>> consumer = BaseKafkaConsumer(
        ...     config=config,
        ...     topics=["my-topic"],
        ...     group_id="my-consumer-group",
        ...     message_handler=handle_message
        ... )
        >>> await consumer.start()
        >>> # Consumer runs until stopped
        >>> await consumer.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        topics: List[str],
        group_id: str,
        message_handler: Callable[[ConsumerRecord], Awaitable[None]],
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        """
        Initialize Kafka consumer.

        Args:
            config: Kafka configuration
            topics: List of topics to subscribe to
            group_id: Consumer group ID for offset management
            message_handler: Async callback function to process messages
            circuit_breaker: Optional custom circuit breaker (uses default if None)
        """
        if not topics:
            raise ValueError("At least one topic must be specified")

        self.config = config
        self.topics = topics
        self.group_id = group_id
        self.message_handler = message_handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._circuit_breaker = circuit_breaker or get_circuit_breaker(
            f"kafka_consumer_{group_id}", KAFKA_CIRCUIT_CONFIG
        )

        logger.info(
            "Initialized Kafka consumer",
            extra={
                "topics": topics,
                "group_id": group_id,
                "bootstrap_servers": config.bootstrap_servers,
            },
        )

    async def start(self) -> None:
        """
        Start the Kafka consumer and begin processing messages.

        Creates the underlying aiokafka consumer, connects to the cluster,
        and starts the message consumption loop. This method runs until
        stop() is called.

        The consumer will:
        1. Connect to Kafka cluster with authentication
        2. Subscribe to configured topics
        3. Consume messages and call message_handler for each
        4. Manually commit offsets after successful processing

        Raises:
            Exception: If consumer fails to start or connect
        """
        if self._running:
            logger.warning("Consumer already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting Kafka consumer",
            extra={"topics": self.topics, "group_id": self.group_id},
        )

        # Create OAuth callback for authentication
        oauth_callback = create_kafka_oauth_callback()

        # Create aiokafka consumer
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.group_id,
            security_protocol=self.config.security_protocol,
            sasl_mechanism=self.config.sasl_mechanism,
            sasl_oauth_token_provider=oauth_callback,
            enable_auto_commit=self.config.enable_auto_commit,
            auto_offset_reset=self.config.auto_offset_reset,
            max_poll_records=self.config.max_poll_records,
            max_poll_interval_ms=self.config.max_poll_interval_ms,
            session_timeout_ms=self.config.session_timeout_ms,
        )

        await self._consumer.start()
        self._running = True

        logger.info(
            "Kafka consumer started successfully",
            extra={
                "topics": self.topics,
                "group_id": self.group_id,
                "partitions": len(self._consumer.assignment()),
            },
        )

        # Start message consumption loop
        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Consumer loop terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """
        Stop the Kafka consumer and cleanup resources.

        Commits any pending offsets and closes the connection gracefully.
        Safe to call multiple times.
        """
        if not self._running or self._consumer is None:
            logger.debug("Consumer not running or already stopped")
            return

        logger.info("Stopping Kafka consumer")
        self._running = False

        try:
            # Commit any pending offsets
            if self._consumer:
                await self._consumer.commit()
                # Stop the consumer
                await self._consumer.stop()
            logger.info("Kafka consumer stopped successfully")
        except Exception as e:
            logger.error(
                "Error stopping Kafka consumer",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._consumer = None

    async def _consume_loop(self) -> None:
        """
        Main message consumption loop.

        Continuously fetches messages, processes them through the handler,
        and commits offsets after successful processing.
        """
        logger.info("Starting message consumption loop")

        while self._running and self._consumer:
            try:
                # Fetch messages (blocking with timeout)
                # This is wrapped in circuit breaker to protect against broker failures
                async def _fetch_messages():
                    # getmany() returns a dict of TopicPartition -> list of records
                    # timeout_ms controls how long to wait for messages
                    return await self._consumer.getmany(timeout_ms=1000)

                data = await self._circuit_breaker.call_async(_fetch_messages)

                # Process messages from all partitions
                for topic_partition, messages in data.items():
                    for message in messages:
                        if not self._running:
                            logger.info("Consumer stopped, breaking message loop")
                            return

                        await self._process_message(message)

            except asyncio.CancelledError:
                logger.info("Consumption loop cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error in consumption loop",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                # Continue processing - the circuit breaker will handle repeated failures
                await asyncio.sleep(1)

    async def _process_message(self, message: ConsumerRecord) -> None:
        """
        Process a single message with error classification and routing.

        Handles errors according to their category:
        - TRANSIENT: Will be retried (don't commit, message reprocessed)
        - PERMANENT: Should go to DLQ (don't commit for now, logged)
        - AUTH: Token refresh needed (don't commit, will reprocess)
        - CIRCUIT_OPEN: Circuit breaker open (don't commit, will reprocess)

        Args:
            message: ConsumerRecord to process
        """
        logger.debug(
            "Processing message",
            extra={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key.decode("utf-8") if message.key else None,
            },
        )

        try:
            # Call user-provided message handler
            await self.message_handler(message)

            # Commit offset after successful processing (at-least-once semantics)
            await self._consumer.commit()

            logger.debug(
                "Message processed successfully",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )

        except Exception as e:
            # Classify the error to determine routing
            await self._handle_processing_error(message, e)

    async def _handle_processing_error(
        self, message: ConsumerRecord, error: Exception
    ) -> None:
        """
        Handle message processing errors with intelligent routing.

        Classifies the error and determines appropriate action:
        - TRANSIENT: Log and don't commit (message will be retried on next poll)
        - PERMANENT: Log for DLQ routing (future: send to DLQ topic)
        - AUTH: Log and don't commit (will reprocess after token refresh)
        - CIRCUIT_OPEN: Log and don't commit (will reprocess when circuit closes)
        - UNKNOWN: Log and don't commit (conservative retry)

        Args:
            message: The ConsumerRecord that failed processing
            error: The exception that occurred during processing
        """
        # Classify the error using Kafka error classifier
        classified_error = KafkaErrorClassifier.classify_consumer_error(
            error,
            context={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "group_id": self.group_id,
            },
        )

        error_category = classified_error.category

        # Build log context
        log_extra = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "error_type": type(error).__name__,
            "error_category": error_category.value,
            "error_message": str(error),
            "classified_as": type(classified_error).__name__,
        }

        # Route based on error category
        if error_category == ErrorCategory.TRANSIENT:
            logger.warning(
                "Transient error processing message - will retry on next poll",
                extra=log_extra,
                exc_info=True,
            )
            # Don't commit offset - message will be reprocessed
            # TODO (WP-209): Send to retry topic with exponential backoff

        elif error_category == ErrorCategory.PERMANENT:
            logger.error(
                "Permanent error processing message - should route to DLQ",
                extra=log_extra,
                exc_info=True,
            )
            # Don't commit offset yet
            # TODO (WP-211): Send to DLQ topic for manual review
            # For now, just log - message will be reprocessed (not ideal)

        elif error_category == ErrorCategory.AUTH:
            logger.warning(
                "Authentication error - will reprocess after token refresh",
                extra=log_extra,
                exc_info=True,
            )
            # Don't commit offset - message will be reprocessed after auth refresh
            # The token cache will refresh automatically on next attempt

        elif error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess when circuit closes",
                extra=log_extra,
                exc_info=True,
            )
            # Don't commit offset - message will be reprocessed when circuit recovers
            # Circuit breaker will track failure rate and open/close accordingly

        else:  # UNKNOWN or other categories
            logger.error(
                "Unknown error category - applying conservative retry",
                extra=log_extra,
                exc_info=True,
            )
            # Don't commit offset - conservative retry for unknown errors

        # Note: We don't re-raise the exception here because we want to continue
        # processing other messages. The offset won't be committed, so this message
        # will be retried on the next poll.

    @property
    def is_running(self) -> bool:
        """Check if consumer is running and processing messages."""
        return self._running and self._consumer is not None


__all__ = [
    "BaseKafkaConsumer",
]
