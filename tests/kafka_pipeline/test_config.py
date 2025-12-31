"""Tests for Kafka configuration."""

import os
import pytest

from kafka_pipeline.config import KafkaConfig


class TestKafkaConfig:
    """Test KafkaConfig dataclass and environment loading."""

    def test_from_env_minimal_required(self, monkeypatch):
        """Test loading with only required environment variables."""
        # Clear any env vars set by other fixtures to test true defaults
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        monkeypatch.delenv("KAFKA_SASL_MECHANISM", raising=False)
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9093,kafka2:9093")

        config = KafkaConfig.from_env()

        assert config.bootstrap_servers == "kafka1:9093,kafka2:9093"
        assert config.security_protocol == "SASL_SSL"
        assert config.sasl_mechanism == "OAUTHBEARER"

    def test_from_env_missing_required_raises(self, monkeypatch):
        """Test that missing required variables raises ValueError."""
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

        with pytest.raises(ValueError, match="KAFKA_BOOTSTRAP_SERVERS"):
            KafkaConfig.from_env()

    def test_from_env_all_variables(self, monkeypatch):
        """Test loading all environment variables."""
        env_vars = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka.example.com:9093",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "KAFKA_SASL_MECHANISM": "PLAIN",
            "KAFKA_EVENTS_TOPIC": "custom.events",
            "KAFKA_DOWNLOADS_PENDING_TOPIC": "custom.pending",
            "KAFKA_DOWNLOADS_RESULTS_TOPIC": "custom.results",
            "KAFKA_DLQ_TOPIC": "custom.dlq",
            "KAFKA_CONSUMER_GROUP_PREFIX": "custom",
            "KAFKA_MAX_POLL_RECORDS": "200",
            "KAFKA_SESSION_TIMEOUT_MS": "45000",
            "RETRY_DELAYS": "60,120,240",
            "MAX_RETRIES": "3",
        }

        for key, value in env_vars.items():
            monkeypatch.setenv(key, value)

        config = KafkaConfig.from_env()

        assert config.bootstrap_servers == "kafka.example.com:9093"
        assert config.security_protocol == "PLAINTEXT"
        assert config.sasl_mechanism == "PLAIN"
        assert config.events_topic == "custom.events"
        assert config.downloads_pending_topic == "custom.pending"
        assert config.downloads_results_topic == "custom.results"
        assert config.dlq_topic == "custom.dlq"
        assert config.consumer_group_prefix == "custom"
        assert config.max_poll_records == 200
        assert config.session_timeout_ms == 45000
        assert config.retry_delays == [60, 120, 240]
        assert config.max_retries == 3

    def test_default_consumer_settings(self, monkeypatch):
        """Test default consumer configuration values."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()

        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
        assert config.max_poll_records == 100
        assert config.max_poll_interval_ms == 300000
        assert config.session_timeout_ms == 30000

    def test_default_producer_settings(self, monkeypatch):
        """Test default producer configuration values."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()

        assert config.acks == "all"
        assert config.retries == 3
        assert config.retry_backoff_ms == 1000

    def test_default_topics(self, monkeypatch):
        """Test default topic names."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()

        assert config.events_topic == "xact.events.raw"
        assert config.downloads_pending_topic == "xact.downloads.pending"
        assert config.downloads_results_topic == "xact.downloads.results"
        assert config.dlq_topic == "xact.downloads.dlq"

    def test_default_retry_configuration(self, monkeypatch):
        """Test default retry delays and max retries."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()

        assert config.retry_delays == [300, 600, 1200, 2400]
        assert config.max_retries == 4

    def test_retry_delays_parsing(self, monkeypatch):
        """Test parsing retry delays from comma-separated string."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        monkeypatch.setenv("RETRY_DELAYS", "30,60,120,240,480")

        config = KafkaConfig.from_env()

        assert config.retry_delays == [30, 60, 120, 240, 480]

    def test_retry_delays_with_whitespace(self, monkeypatch):
        """Test retry delays parsing handles whitespace."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        monkeypatch.setenv("RETRY_DELAYS", "100, 200, 300")

        config = KafkaConfig.from_env()

        assert config.retry_delays == [100, 200, 300]

    def test_get_retry_topic_first_retry(self, monkeypatch):
        """Test get_retry_topic for first retry (5 minutes)."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()
        retry_topic = config.get_retry_topic(0)

        assert retry_topic == "xact.downloads.pending.retry.5m"

    def test_get_retry_topic_second_retry(self, monkeypatch):
        """Test get_retry_topic for second retry (10 minutes)."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()
        retry_topic = config.get_retry_topic(1)

        assert retry_topic == "xact.downloads.pending.retry.10m"

    def test_get_retry_topic_all_levels(self, monkeypatch):
        """Test get_retry_topic for all retry levels."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()

        expected = [
            "xact.downloads.pending.retry.5m",
            "xact.downloads.pending.retry.10m",
            "xact.downloads.pending.retry.20m",
            "xact.downloads.pending.retry.40m",
        ]

        for attempt, expected_topic in enumerate(expected):
            assert config.get_retry_topic(attempt) == expected_topic

    def test_get_retry_topic_exceeds_max_raises(self, monkeypatch):
        """Test get_retry_topic raises ValueError when attempt exceeds max."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()

        with pytest.raises(ValueError, match="exceeds max retries"):
            config.get_retry_topic(4)

    def test_get_retry_topic_custom_delays(self, monkeypatch):
        """Test get_retry_topic with custom retry delays."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        monkeypatch.setenv("RETRY_DELAYS", "60,180")

        config = KafkaConfig.from_env()

        assert config.get_retry_topic(0) == "xact.downloads.pending.retry.1m"
        assert config.get_retry_topic(1) == "xact.downloads.pending.retry.3m"

    def test_get_consumer_group_download_worker(self, monkeypatch):
        """Test get_consumer_group for download worker."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()
        group = config.get_consumer_group("download")

        assert group == "xact-download-worker"

    def test_get_consumer_group_event_ingester(self, monkeypatch):
        """Test get_consumer_group for event ingester."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()
        group = config.get_consumer_group("event-ingester")

        assert group == "xact-event-ingester-worker"

    def test_get_consumer_group_custom_prefix(self, monkeypatch):
        """Test get_consumer_group with custom prefix."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        monkeypatch.setenv("KAFKA_CONSUMER_GROUP_PREFIX", "custom")

        config = KafkaConfig.from_env()
        group = config.get_consumer_group("download")

        assert group == "custom-download-worker"

    def test_direct_instantiation(self):
        """Test creating KafkaConfig directly without environment."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            events_topic="test.events",
        )

        assert config.bootstrap_servers == "kafka:9093"
        assert config.events_topic == "test.events"
        assert config.security_protocol == "SASL_SSL"  # default

    def test_dataclass_immutability_after_creation(self, monkeypatch):
        """Test that config values can be modified after creation."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        config = KafkaConfig.from_env()
        original_servers = config.bootstrap_servers

        # Dataclasses are mutable by default
        config.bootstrap_servers = "kafka2:9093"
        assert config.bootstrap_servers == "kafka2:9093"
        assert config.bootstrap_servers != original_servers
