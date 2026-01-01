"""Tests for pipeline configuration."""

import os
from unittest.mock import patch

import pytest

from kafka_pipeline.pipeline_config import (
    EventHubConfig,
    EventhouseSourceConfig,
    EventSourceType,
    LocalKafkaConfig,
    PipelineConfig,
    get_event_source_type,
    get_pipeline_config,
)


class TestEventSourceType:
    """Tests for EventSourceType enum."""

    def test_eventhub_value(self):
        """Test eventhub enum value."""
        assert EventSourceType.EVENTHUB.value == "eventhub"

    def test_eventhouse_value(self):
        """Test eventhouse enum value."""
        assert EventSourceType.EVENTHOUSE.value == "eventhouse"

    def test_string_conversion(self):
        """Test string conversion."""
        assert str(EventSourceType.EVENTHUB) == "EventSourceType.EVENTHUB"
        assert EventSourceType("eventhub") == EventSourceType.EVENTHUB
        assert EventSourceType("eventhouse") == EventSourceType.EVENTHOUSE


class TestEventhouseSourceConfig:
    """Tests for EventhouseSourceConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = EventhouseSourceConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
        )

        assert config.source_table == "Events"
        assert config.poll_interval_seconds == 30
        assert config.batch_size == 1000
        assert config.query_timeout_seconds == 120
        assert config.xact_events_table_path == ""
        assert config.xact_events_window_hours == 24
        assert config.eventhouse_query_window_hours == 1
        assert config.overlap_minutes == 5

    def test_custom_values(self):
        """Test custom configuration values."""
        config = EventhouseSourceConfig(
            cluster_url="https://prod.kusto.windows.net",
            database="proddb",
            source_table="CustomEvents",
            poll_interval_seconds=60,
            batch_size=500,
            query_timeout_seconds=180,
            xact_events_table_path="abfss://container@storage.dfs.core.windows.net/xact_events",
            xact_events_window_hours=48,
            eventhouse_query_window_hours=2,
            overlap_minutes=10,
        )

        assert config.cluster_url == "https://prod.kusto.windows.net"
        assert config.database == "proddb"
        assert config.source_table == "CustomEvents"
        assert config.poll_interval_seconds == 60
        assert config.batch_size == 500
        assert config.query_timeout_seconds == 180
        assert config.xact_events_table_path == "abfss://container@storage.dfs.core.windows.net/xact_events"
        assert config.xact_events_window_hours == 48
        assert config.eventhouse_query_window_hours == 2
        assert config.overlap_minutes == 10

    def test_from_env_success(self):
        """Test loading from environment variables."""
        env = {
            "EVENTHOUSE_CLUSTER_URL": "https://test.kusto.windows.net",
            "EVENTHOUSE_DATABASE": "testdb",
            "EVENTHOUSE_SOURCE_TABLE": "MyEvents",
            "POLL_INTERVAL_SECONDS": "45",
            "POLL_BATCH_SIZE": "750",
            "EVENTHOUSE_QUERY_TIMEOUT": "150",
            "XACT_EVENTS_TABLE_PATH": "abfss://test/xact_events",
            "DEDUP_XACT_EVENTS_WINDOW_HOURS": "36",
            "DEDUP_EVENTHOUSE_WINDOW_HOURS": "3",
            "DEDUP_OVERLAP_MINUTES": "8",
        }

        with patch.dict(os.environ, env, clear=False):
            config = EventhouseSourceConfig.from_env()

        assert config.cluster_url == "https://test.kusto.windows.net"
        assert config.database == "testdb"
        assert config.source_table == "MyEvents"
        assert config.poll_interval_seconds == 45
        assert config.batch_size == 750
        assert config.query_timeout_seconds == 150
        assert config.xact_events_table_path == "abfss://test/xact_events"
        assert config.xact_events_window_hours == 36
        assert config.eventhouse_query_window_hours == 3
        assert config.overlap_minutes == 8

    def test_from_env_missing_cluster_url(self):
        """Test error when cluster URL is missing."""
        env = {
            "EVENTHOUSE_DATABASE": "testdb",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="EVENTHOUSE_CLUSTER_URL is required"):
                EventhouseSourceConfig.from_env()

    def test_from_env_missing_database(self):
        """Test error when database is missing."""
        env = {
            "EVENTHOUSE_CLUSTER_URL": "https://test.kusto.windows.net",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="EVENTHOUSE_DATABASE is required"):
                EventhouseSourceConfig.from_env()


class TestPipelineConfig:
    """Tests for PipelineConfig."""

    def test_eventhub_source_properties(self):
        """Test helper properties for Event Hub source."""
        config = PipelineConfig(
            event_source=EventSourceType.EVENTHUB,
            eventhub=EventHubConfig(
                bootstrap_servers="namespace.servicebus.windows.net:9093",
                sasl_password="connection-string",
            ),
        )

        assert config.is_eventhub_source is True
        assert config.is_eventhouse_source is False

    def test_eventhouse_source_properties(self):
        """Test helper properties for Eventhouse source."""
        config = PipelineConfig(
            event_source=EventSourceType.EVENTHOUSE,
            eventhouse=EventhouseSourceConfig(
                cluster_url="https://test.kusto.windows.net",
                database="testdb",
            ),
        )

        assert config.is_eventhub_source is False
        assert config.is_eventhouse_source is True

    def test_from_env_eventhub_default(self):
        """Test loading Event Hub config (default)."""
        env = {
            "EVENTHUB_BOOTSTRAP_SERVERS": "namespace.servicebus.windows.net:9093",
            "EVENTHUB_CONNECTION_STRING": "Endpoint=sb://...",
            "LOCAL_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        }

        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.from_env()

        assert config.event_source == EventSourceType.EVENTHUB
        assert config.eventhub is not None
        assert config.eventhouse is None
        assert config.is_eventhub_source is True

    def test_from_env_eventhub_explicit(self):
        """Test loading Event Hub config with explicit EVENT_SOURCE."""
        env = {
            "EVENT_SOURCE": "eventhub",
            "EVENTHUB_BOOTSTRAP_SERVERS": "namespace.servicebus.windows.net:9093",
            "EVENTHUB_CONNECTION_STRING": "Endpoint=sb://...",
        }

        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.from_env()

        assert config.event_source == EventSourceType.EVENTHUB
        assert config.is_eventhub_source is True

    def test_from_env_eventhouse(self):
        """Test loading Eventhouse config."""
        env = {
            "EVENT_SOURCE": "eventhouse",
            "EVENTHOUSE_CLUSTER_URL": "https://test.kusto.windows.net",
            "EVENTHOUSE_DATABASE": "testdb",
        }

        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.from_env()

        assert config.event_source == EventSourceType.EVENTHOUSE
        assert config.eventhub is None
        assert config.eventhouse is not None
        assert config.is_eventhouse_source is True

    def test_from_env_invalid_source(self):
        """Test error with invalid event source."""
        env = {
            "EVENT_SOURCE": "invalid",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="Invalid EVENT_SOURCE 'invalid'"):
                PipelineConfig.from_env()

    def test_from_env_case_insensitive(self):
        """Test EVENT_SOURCE is case-insensitive."""
        env = {
            "EVENT_SOURCE": "EVENTHOUSE",
            "EVENTHOUSE_CLUSTER_URL": "https://test.kusto.windows.net",
            "EVENTHOUSE_DATABASE": "testdb",
        }

        with patch.dict(os.environ, env, clear=True):
            config = PipelineConfig.from_env()

        assert config.event_source == EventSourceType.EVENTHOUSE


class TestGetEventSourceType:
    """Tests for get_event_source_type helper."""

    def test_default_is_eventhub(self):
        """Test default event source type."""
        with patch.dict(os.environ, {}, clear=True):
            source = get_event_source_type()

        assert source == EventSourceType.EVENTHUB

    def test_eventhouse(self):
        """Test eventhouse event source."""
        with patch.dict(os.environ, {"EVENT_SOURCE": "eventhouse"}, clear=True):
            source = get_event_source_type()

        assert source == EventSourceType.EVENTHOUSE

    def test_case_insensitive(self):
        """Test case insensitivity."""
        with patch.dict(os.environ, {"EVENT_SOURCE": "EVENTHUB"}, clear=True):
            source = get_event_source_type()

        assert source == EventSourceType.EVENTHUB


class TestLocalKafkaConfig:
    """Tests for LocalKafkaConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = LocalKafkaConfig()

        assert config.bootstrap_servers == "localhost:9092"
        assert config.security_protocol == "PLAINTEXT"
        assert config.downloads_pending_topic == "xact.downloads.pending"
        assert config.downloads_results_topic == "xact.downloads.results"
        assert config.dlq_topic == "xact.downloads.dlq"

    def test_to_kafka_config(self):
        """Test conversion to KafkaConfig."""
        config = LocalKafkaConfig()
        kafka_config = config.to_kafka_config()

        assert kafka_config.bootstrap_servers == "localhost:9092"
        assert kafka_config.security_protocol == "PLAINTEXT"
        assert kafka_config.downloads_pending_topic == "xact.downloads.pending"
