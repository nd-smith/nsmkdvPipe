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
import time
from typing import Awaitable, Callable, List, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging import get_logger, log_with_context, log_exception, KafkaLogContext
from core.errors.exceptions import CircuitOpenError, ErrorCategory
from core.errors.kafka_classifier import KafkaErrorClassifier
from core.resilience.circuit_breaker import (
    CircuitBreaker,
    KAFKA_CIRCUIT_CONFIG,
    get_circuit_breaker,
)
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


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
        max_batches: Optional[int] = None,
    ):
        """
        Initialize Kafka consumer.

        Args:
            config: Kafka configuration
            topics: List of topics to subscribe to
            group_id: Consumer group ID for offset management
            message_handler: Async callback function to process messages
            circuit_breaker: Optional custom circuit breaker (uses default if None)
            max_batches: Optional limit on number of batches to process (None = unlimited).
                        Useful for testing. A batch is one getmany() poll result.
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

        # Batch limiting for testing
        self.max_batches = max_batches
        self._batch_count = 0

        log_with_context(
            logger,
            logging.INFO,
            "Initialized Kafka consumer",
            topics=topics,
            group_id=group_id,
            bootstrap_servers=config.bootstrap_servers,
            max_batches=max_batches,
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

        log_with_context(
            logger,
            logging.INFO,
            "Starting Kafka consumer",
            topics=self.topics,
            group_id=self.group_id,
        )

        # Create aiokafka consumer configuration
        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.group_id,
            "enable_auto_commit": self.config.enable_auto_commit,
            "auto_offset_reset": self.config.auto_offset_reset,
            "max_poll_records": self.config.max_poll_records,
            "max_poll_interval_ms": self.config.max_poll_interval_ms,
            "session_timeout_ms": self.config.session_timeout_ms,
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Add authentication based on mechanism
            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                consumer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                # SASL_PLAIN for Event Hubs or basic auth
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        # Create aiokafka consumer
        self._consumer = AIOKafkaConsumer(*self.topics, **consumer_config)

        await self._consumer.start()
        self._running = True

        # Update connection status and partition assignment metrics
        update_connection_status("consumer", connected=True)
        partition_count = len(self._consumer.assignment())
        update_assigned_partitions(self.group_id, partition_count)

        log_with_context(
            logger,
            logging.INFO,
            "Kafka consumer started successfully",
            topics=self.topics,
            group_id=self.group_id,
            partitions=partition_count,
        )

        # Start message consumption loop
        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            log_exception(
                logger,
                e,
                "Consumer loop terminated with error",
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
            log_exception(
                logger,
                e,
                "Error stopping Kafka consumer",
            )
            raise
        finally:
            # Update connection status metrics
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.group_id, 0)
            self._consumer = None

    async def _consume_loop(self) -> None:
        """
        Main message consumption loop.

        Continuously fetches messages, processes them through the handler,
        and commits offsets after successful processing.

        If max_batches is set, exits after processing that many batches.
        """
        log_with_context(
            logger,
            logging.INFO,
            "Starting message consumption loop",
            max_batches=self.max_batches,
            topics=self.topics,
            group_id=self.group_id,
        )

        # Track partition assignment status for logging
        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
            try:
                # Check if we've reached max_batches limit
                if self.max_batches is not None and self._batch_count >= self.max_batches:
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Reached max_batches limit, stopping consumer",
                        max_batches=self.max_batches,
                        batches_processed=self._batch_count,
                    )
                    return

                # Check partition assignment before fetching
                # During consumer group rebalances, getmany() can block indefinitely
                # waiting for partition assignment (ignoring timeout_ms).
                # This check ensures we don't hang during startup with multiple consumers.
                assignment = self._consumer.assignment()
                if not assignment:
                    if not _logged_waiting_for_assignment:
                        log_with_context(
                            logger,
                            logging.INFO,
                            "Waiting for partition assignment (consumer group rebalance in progress)",
                            group_id=self.group_id,
                            topics=self.topics,
                        )
                        _logged_waiting_for_assignment = True
                    # Sleep briefly and retry - don't call getmany() during rebalance
                    await asyncio.sleep(0.5)
                    continue

                # Log when we first receive partition assignment
                if not _logged_assignment_received:
                    partition_info = [
                        f"{tp.topic}:{tp.partition}" for tp in assignment
                    ]
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Partition assignment received, starting message consumption",
                        group_id=self.group_id,
                        partition_count=len(assignment),
                        partitions=partition_info,
                    )
                    _logged_assignment_received = True
                    # Update partition assignment metric
                    update_assigned_partitions(self.group_id, len(assignment))

                # Fetch messages with timeout
                # Note: We call getmany() directly instead of through the circuit breaker
                # because the circuit breaker uses threading.RLock which can cause deadlocks
                # when multiple async consumers share the same lock in a single event loop.
                logger.debug(
                    "Calling getmany()",
                    extra={"group_id": self.group_id, "timeout_ms": 1000},
                )
                try:
                    data = await self._consumer.getmany(timeout_ms=1000)
                    logger.debug(
                        "getmany() returned",
                        extra={
                            "group_id": self.group_id,
                            "partition_count": len(data) if data else 0,
                            "message_count": sum(len(msgs) for msgs in data.values()) if data else 0,
                        },
                    )
                    self._circuit_breaker.record_success()
                except Exception as fetch_error:
                    logger.debug(
                        "getmany() raised exception",
                        extra={"group_id": self.group_id, "error": str(fetch_error)},
                    )
                    self._circuit_breaker.record_failure(fetch_error)
                    raise

                # Count this as a batch if we got any messages
                if data:
                    self._batch_count += 1

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
                log_exception(
                    logger,
                    e,
                    "Error in consumption loop",
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
        # Use KafkaLogContext to automatically include Kafka context in all logs
        with KafkaLogContext(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            key=message.key.decode("utf-8") if message.key else None,
            consumer_group=self.group_id,
        ):
            log_with_context(
                logger,
                logging.DEBUG,
                "Processing message",
                message_size=len(message.value) if message.value else 0,
            )

            # Track processing time
            start_time = time.perf_counter()
            message_size = len(message.value) if message.value else 0

            try:
                # Call user-provided message handler
                await self.message_handler(message)

                # Record processing duration
                duration = time.perf_counter() - start_time
                message_processing_duration_seconds.labels(
                    topic=message.topic, consumer_group=self.group_id
                ).observe(duration)

                # Commit offset after successful processing (at-least-once semantics)
                await self._consumer.commit()

                # Update offset and lag metrics
                self._update_partition_metrics(message)

                # Record successful message consumption
                record_message_consumed(
                    message.topic, self.group_id, message_size, success=True
                )

                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Message processed successfully",
                    duration_ms=round(duration * 1000, 2),
                )

            except Exception as e:
                # Record processing duration even for failures
                duration = time.perf_counter() - start_time
                message_processing_duration_seconds.labels(
                    topic=message.topic, consumer_group=self.group_id
                ).observe(duration)

                # Record failed message consumption
                record_message_consumed(
                    message.topic, self.group_id, message_size, success=False
                )

                # Classify the error to determine routing
                await self._handle_processing_error(message, e, duration)

    async def _handle_processing_error(
        self, message: ConsumerRecord, error: Exception, duration: float
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
            duration: Processing duration in seconds before error occurred
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

        # Record error metric
        record_processing_error(message.topic, self.group_id, error_category.value)

        # Common log context - Kafka context is automatically included via KafkaLogContext
        common_context = {
            "error_category": error_category.value,
            "classified_as": type(classified_error).__name__,
            "duration_ms": round(duration * 1000, 2),
        }

        # Route based on error category
        if error_category == ErrorCategory.TRANSIENT:
            log_exception(
                logger,
                error,
                "Transient error processing message - will retry on next poll",
                level=logging.WARNING,
                **common_context,
            )
            # Don't commit offset - message will be reprocessed
            # TODO (WP-209): Send to retry topic with exponential backoff

        elif error_category == ErrorCategory.PERMANENT:
            log_exception(
                logger,
                error,
                "Permanent error processing message - should route to DLQ",
                **common_context,
            )
            # Don't commit offset yet
            # TODO (WP-211): Send to DLQ topic for manual review
            # For now, just log - message will be reprocessed (not ideal)

        elif error_category == ErrorCategory.AUTH:
            log_exception(
                logger,
                error,
                "Authentication error - will reprocess after token refresh",
                level=logging.WARNING,
                **common_context,
            )
            # Don't commit offset - message will be reprocessed after auth refresh
            # The token cache will refresh automatically on next attempt

        elif error_category == ErrorCategory.CIRCUIT_OPEN:
            log_exception(
                logger,
                error,
                "Circuit breaker open - will reprocess when circuit closes",
                level=logging.WARNING,
                **common_context,
            )
            # Don't commit offset - message will be reprocessed when circuit recovers
            # Circuit breaker will track failure rate and open/close accordingly

        else:  # UNKNOWN or other categories
            log_exception(
                logger,
                error,
                "Unknown error category - applying conservative retry",
                **common_context,
            )
            # Don't commit offset - conservative retry for unknown errors

        # Note: We don't re-raise the exception here because we want to continue
        # processing other messages. The offset won't be committed, so this message
        # will be retried on the next poll.

    def _update_partition_metrics(self, message: ConsumerRecord) -> None:
        """
        Update offset and lag metrics for the partition.

        Args:
            message: The consumed message with partition and offset info
        """
        if not self._consumer:
            return

        try:
            # Update current offset
            update_consumer_offset(
                message.topic, message.partition, self.group_id, message.offset
            )

            # Calculate and update lag (high watermark - current offset)
            # Get high watermark for the partition
            tp = TopicPartition(message.topic, message.partition)
            partition_metadata = self._consumer.highwater(tp)

            if partition_metadata is not None:
                # Lag = high watermark - (current offset + 1)
                # +1 because we've consumed this message
                lag = partition_metadata - (message.offset + 1)
                update_consumer_lag(
                    message.topic, message.partition, self.group_id, lag
                )

        except Exception as e:
            # Don't fail message processing due to metrics issues
            log_with_context(
                logger,
                logging.DEBUG,
                "Failed to update partition metrics",
                topic=message.topic,
                partition=message.partition,
                error=str(e),
            )

    @property
    def is_running(self) -> bool:
        """Check if consumer is running and processing messages."""
        return self._running and self._consumer is not None


__all__ = [
    "BaseKafkaConsumer",
    "AIOKafkaConsumer",
    "get_circuit_breaker",
    "CircuitBreaker",
    "ConsumerRecord",
    "create_kafka_oauth_callback",
]
