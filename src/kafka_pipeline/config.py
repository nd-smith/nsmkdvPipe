"""Kafka pipeline configuration from environment variables."""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class KafkaConfig:
    """Kafka connection and behavior configuration.

    Load from environment using KafkaConfig.from_env().
    All timing values in milliseconds unless otherwise noted.
    """

    # Connection
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "OAUTHBEARER"

    # SASL_PLAIN credentials (for Event Hubs or basic auth)
    sasl_plain_username: str = ""
    sasl_plain_password: str = ""

    # Consumer defaults
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 100
    max_poll_interval_ms: int = 300000  # 5 minutes
    session_timeout_ms: int = 30000

    # Producer defaults
    acks: str = "all"
    retries: int = 3
    retry_backoff_ms: int = 1000

    # Topics
    events_topic: str = "xact.events.raw"
    downloads_pending_topic: str = "xact.downloads.pending"
    downloads_results_topic: str = "xact.downloads.results"
    dlq_topic: str = "xact.downloads.dlq"

    # Consumer group prefix
    consumer_group_prefix: str = "xact"

    # Retry configuration (delays in seconds)
    retry_delays: List[int] = field(default_factory=lambda: [300, 600, 1200, 2400])
    max_retries: int = 4

    # Storage configuration
    onelake_base_path: str = ""  # abfss:// path to OneLake files directory

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Load configuration from environment variables.

        Required environment variables:
            KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses

        Optional environment variables (with defaults):
            KAFKA_SECURITY_PROTOCOL: SASL_SSL (default)
            KAFKA_SASL_MECHANISM: OAUTHBEARER (default)
            KAFKA_EVENTS_TOPIC: xact.events.raw (default)
            KAFKA_DOWNLOADS_PENDING_TOPIC: xact.downloads.pending (default)
            KAFKA_DOWNLOADS_RESULTS_TOPIC: xact.downloads.results (default)
            KAFKA_DLQ_TOPIC: xact.downloads.dlq (default)
            KAFKA_CONSUMER_GROUP_PREFIX: xact (default)
            KAFKA_MAX_POLL_RECORDS: 100 (default)
            KAFKA_SESSION_TIMEOUT_MS: 30000 (default)
            RETRY_DELAYS: 300,600,1200,2400 (default, comma-separated seconds)
            MAX_RETRIES: 4 (default)
            ONELAKE_BASE_PATH: OneLake abfss:// path (required for upload)

        Raises:
            ValueError: If required environment variables are missing
        """
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

        # Parse retry delays from comma-separated string
        retry_delays_str = os.getenv("RETRY_DELAYS", "300,600,1200,2400")
        retry_delays = [int(d.strip()) for d in retry_delays_str.split(",")]

        return cls(
            # Connection
            bootstrap_servers=bootstrap_servers,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "OAUTHBEARER"),
            sasl_plain_username=os.getenv("KAFKA_SASL_PLAIN_USERNAME", ""),
            sasl_plain_password=os.getenv("KAFKA_SASL_PLAIN_PASSWORD", ""),

            # Consumer defaults
            max_poll_records=int(os.getenv("KAFKA_MAX_POLL_RECORDS", "100")),
            session_timeout_ms=int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000")),

            # Topics
            events_topic=os.getenv("KAFKA_EVENTS_TOPIC", "xact.events.raw"),
            downloads_pending_topic=os.getenv(
                "KAFKA_DOWNLOADS_PENDING_TOPIC", "xact.downloads.pending"
            ),
            downloads_results_topic=os.getenv(
                "KAFKA_DOWNLOADS_RESULTS_TOPIC", "xact.downloads.results"
            ),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "xact.downloads.dlq"),

            # Consumer group
            consumer_group_prefix=os.getenv("KAFKA_CONSUMER_GROUP_PREFIX", "xact"),

            # Retry configuration
            retry_delays=retry_delays,
            max_retries=int(os.getenv("MAX_RETRIES", "4")),

            # Storage configuration
            onelake_base_path=os.getenv("ONELAKE_BASE_PATH", ""),
        )

    def get_retry_topic(self, attempt: int) -> str:
        """Get retry topic name for a specific retry attempt.

        Args:
            attempt: Retry attempt number (0-indexed)

        Returns:
            Topic name for this retry level (e.g., "xact.downloads.retry.5m")
        """
        if attempt >= len(self.retry_delays):
            raise ValueError(
                f"Retry attempt {attempt} exceeds max retries {len(self.retry_delays)}"
            )

        delay_seconds = self.retry_delays[attempt]
        delay_minutes = delay_seconds // 60

        return f"{self.downloads_pending_topic}.retry.{delay_minutes}m"

    def get_consumer_group(self, worker_type: str) -> str:
        """Get consumer group name for a worker type.

        Args:
            worker_type: Type of worker (e.g., "download", "event-ingester")

        Returns:
            Full consumer group name (e.g., "xact-download-worker")
        """
        return f"{self.consumer_group_prefix}-{worker_type}-worker"
