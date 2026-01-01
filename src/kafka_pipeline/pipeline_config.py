"""
Pipeline configuration for hybrid Event Hub + Local Kafka setup.

Architecture:
    - Event Hub (Azure): Source of truth for raw events
    - Local Kafka: Internal pipeline communication between workers

Workers:
    - EventIngesterWorker: Reads from Event Hub, writes to Local Kafka
    - DownloadWorker: Reads/writes Local Kafka only
    - ResultProcessor: Reads from Local Kafka only
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional

from kafka_pipeline.config import KafkaConfig


@dataclass
class EventHubConfig:
    """Configuration for Azure Event Hub connection (Kafka-compatible).

    Event Hubs uses Kafka protocol with SASL_SSL + OAUTHBEARER or SASL_PLAIN.
    Connection string format for SASL_PLAIN:
        Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...
    """

    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"  # Event Hubs uses PLAIN with connection string
    sasl_username: str = "$ConnectionString"
    sasl_password: str = ""  # Full connection string

    # Consumer settings
    events_topic: str = "xact.events.raw"
    consumer_group: str = "xact-event-ingester"
    auto_offset_reset: str = "earliest"

    @classmethod
    def from_env(cls) -> "EventHubConfig":
        """Load Event Hub configuration from environment variables.

        Required:
            EVENTHUB_BOOTSTRAP_SERVERS: Event Hub namespace (e.g., namespace.servicebus.windows.net:9093)
            EVENTHUB_CONNECTION_STRING: Full Event Hub connection string

        Optional:
            EVENTHUB_EVENTS_TOPIC: Topic name (default: xact.events.raw)
            EVENTHUB_CONSUMER_GROUP: Consumer group (default: xact-event-ingester)
        """
        bootstrap_servers = os.getenv("EVENTHUB_BOOTSTRAP_SERVERS")
        connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")

        if not bootstrap_servers:
            raise ValueError("EVENTHUB_BOOTSTRAP_SERVERS environment variable is required")
        if not connection_string:
            raise ValueError("EVENTHUB_CONNECTION_STRING environment variable is required")

        return cls(
            bootstrap_servers=bootstrap_servers,
            sasl_password=connection_string,
            events_topic=os.getenv("EVENTHUB_EVENTS_TOPIC", "xact.events.raw"),
            consumer_group=os.getenv("EVENTHUB_CONSUMER_GROUP", "xact-event-ingester"),
            auto_offset_reset=os.getenv("EVENTHUB_AUTO_OFFSET_RESET", "earliest"),
        )

    def to_kafka_config(self) -> KafkaConfig:
        """Convert to KafkaConfig for use with BaseKafkaConsumer."""
        return KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password,
            events_topic=self.events_topic,
            auto_offset_reset=self.auto_offset_reset,
        )


@dataclass
class LocalKafkaConfig:
    """Configuration for local Kafka instance (internal pipeline communication).

    Uses PLAINTEXT for local development/testing.
    """

    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = ""  # Not used for PLAINTEXT

    # Topics for internal pipeline
    downloads_pending_topic: str = "xact.downloads.pending"
    downloads_results_topic: str = "xact.downloads.results"
    dlq_topic: str = "xact.downloads.dlq"

    # Consumer group prefix
    consumer_group_prefix: str = "xact"

    # Retry configuration (delays in seconds)
    retry_delays: List[int] = field(default_factory=lambda: [300, 600, 1200, 2400])
    max_retries: int = 4

    # Storage
    onelake_base_path: str = ""

    @classmethod
    def from_env(cls) -> "LocalKafkaConfig":
        """Load local Kafka configuration from environment variables.

        Optional (all have defaults for local development):
            LOCAL_KAFKA_BOOTSTRAP_SERVERS: Kafka broker (default: localhost:9092)
            LOCAL_KAFKA_SECURITY_PROTOCOL: Protocol (default: PLAINTEXT)
            KAFKA_DOWNLOADS_PENDING_TOPIC: Pending topic (default: xact.downloads.pending)
            KAFKA_DOWNLOADS_RESULTS_TOPIC: Results topic (default: xact.downloads.results)
            KAFKA_DLQ_TOPIC: DLQ topic (default: xact.downloads.dlq)
            ONELAKE_BASE_PATH: OneLake path for uploads
        """
        retry_delays_str = os.getenv("RETRY_DELAYS", "300,600,1200,2400")
        retry_delays = [int(d.strip()) for d in retry_delays_str.split(",")]

        return cls(
            bootstrap_servers=os.getenv("LOCAL_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            security_protocol=os.getenv("LOCAL_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            downloads_pending_topic=os.getenv(
                "KAFKA_DOWNLOADS_PENDING_TOPIC", "xact.downloads.pending"
            ),
            downloads_results_topic=os.getenv(
                "KAFKA_DOWNLOADS_RESULTS_TOPIC", "xact.downloads.results"
            ),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "xact.downloads.dlq"),
            consumer_group_prefix=os.getenv("KAFKA_CONSUMER_GROUP_PREFIX", "xact"),
            retry_delays=retry_delays,
            max_retries=int(os.getenv("MAX_RETRIES", "4")),
            onelake_base_path=os.getenv("ONELAKE_BASE_PATH", ""),
        )

    def to_kafka_config(self) -> KafkaConfig:
        """Convert to KafkaConfig for use with workers."""
        return KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            downloads_pending_topic=self.downloads_pending_topic,
            downloads_results_topic=self.downloads_results_topic,
            dlq_topic=self.dlq_topic,
            consumer_group_prefix=self.consumer_group_prefix,
            retry_delays=self.retry_delays,
            max_retries=self.max_retries,
            onelake_base_path=self.onelake_base_path,
            # Set a placeholder for events_topic (not used by local kafka workers)
            events_topic="",
        )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration.

    Combines Event Hub (source) and Local Kafka (internal) configurations.
    """

    eventhub: EventHubConfig
    local_kafka: LocalKafkaConfig

    # Delta Lake configuration
    enable_delta_writes: bool = True
    events_table_path: str = ""
    inventory_table_path: str = ""
    failed_table_path: str = ""  # Optional: for tracking permanent failures

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Load complete pipeline configuration from environment."""
        return cls(
            eventhub=EventHubConfig.from_env(),
            local_kafka=LocalKafkaConfig.from_env(),
            enable_delta_writes=os.getenv("ENABLE_DELTA_WRITES", "true").lower() == "true",
            events_table_path=os.getenv("DELTA_EVENTS_TABLE_PATH", ""),
            inventory_table_path=os.getenv("DELTA_INVENTORY_TABLE_PATH", ""),
            failed_table_path=os.getenv("DELTA_FAILED_TABLE_PATH", ""),
        )


def get_pipeline_config() -> PipelineConfig:
    """Get pipeline configuration from environment.

    This is the main entry point for loading configuration.
    """
    return PipelineConfig.from_env()
