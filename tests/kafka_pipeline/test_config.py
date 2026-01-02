"""Tests for Kafka configuration."""

import os
from pathlib import Path
import pytest
import tempfile

from kafka_pipeline.config import (
    KafkaConfig,
    load_config,
    get_config,
    set_config,
    reset_config,
    DEFAULT_CONFIG_PATH,
)


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


class TestYamlConfigLoading:
    """Test YAML configuration loading with environment overrides."""

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        """Clear Kafka-related environment variables before each test."""
        env_vars = [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_SECURITY_PROTOCOL",
            "KAFKA_SASL_MECHANISM",
            "KAFKA_EVENTS_TOPIC",
            "RETRY_DELAYS",
            "MAX_RETRIES",
            "DOWNLOAD_CONCURRENCY",
        ]
        for var in env_vars:
            monkeypatch.delenv(var, raising=False)
        # Reset singleton
        reset_config()

    def test_load_config_from_yaml(self, tmp_path):
        """Test loading configuration from YAML file."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "yaml-kafka:9092"
  security_protocol: "PLAINTEXT"
  events_topic: "yaml.events"
  download_concurrency: 25
""")

        config = load_config(config_path=config_file)

        assert config.bootstrap_servers == "yaml-kafka:9092"
        assert config.security_protocol == "PLAINTEXT"
        assert config.events_topic == "yaml.events"
        assert config.download_concurrency == 25

    def test_load_config_flat_structure(self, tmp_path):
        """Test loading config without nested 'kafka:' key."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
bootstrap_servers: "flat-kafka:9092"
security_protocol: "SASL_SSL"
""")

        config = load_config(config_path=config_file)

        assert config.bootstrap_servers == "flat-kafka:9092"
        assert config.security_protocol == "SASL_SSL"

    def test_env_overrides_yaml(self, tmp_path, monkeypatch):
        """Test that environment variables override YAML values."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "yaml-kafka:9092"
  security_protocol: "PLAINTEXT"
""")

        # Environment should override YAML
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-kafka:9093")
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")

        config = load_config(config_path=config_file)

        assert config.bootstrap_servers == "env-kafka:9093"
        assert config.security_protocol == "SASL_SSL"

    def test_overrides_parameter(self, tmp_path):
        """Test that overrides parameter works."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "yaml-kafka:9092"
  download_concurrency: 10
""")

        config = load_config(
            config_path=config_file,
            overrides={"download_concurrency": 30}
        )

        assert config.bootstrap_servers == "yaml-kafka:9092"
        assert config.download_concurrency == 30

    def test_missing_bootstrap_servers_raises(self, tmp_path):
        """Test that missing bootstrap_servers raises ValueError."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  security_protocol: "PLAINTEXT"
""")

        with pytest.raises(ValueError, match="bootstrap_servers is required"):
            load_config(config_path=config_file)

    def test_missing_config_file_with_env(self, tmp_path, monkeypatch):
        """Test that missing config file is ok if env vars are set."""
        config_file = tmp_path / "nonexistent.yaml"
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-kafka:9093")

        config = load_config(config_path=config_file)

        assert config.bootstrap_servers == "env-kafka:9093"

    def test_retry_delays_from_yaml(self, tmp_path):
        """Test loading retry_delays list from YAML."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "kafka:9092"
  retry_delays: [60, 120, 240]
""")

        config = load_config(config_path=config_file)

        assert config.retry_delays == [60, 120, 240]

    def test_concurrency_constraints(self, tmp_path):
        """Test that concurrency values are constrained to valid range."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "kafka:9092"
  download_concurrency: 100
  upload_concurrency: 0
""")

        config = load_config(config_path=config_file)

        # Should be clamped to valid range (1-50)
        assert config.download_concurrency == 50
        assert config.upload_concurrency == 1


class TestConfigSingleton:
    """Test singleton config pattern."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self, monkeypatch):
        """Reset singleton and clear env before and after each test."""
        # Clear env vars that might override our test values
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        reset_config()
        yield
        reset_config()

    def test_get_config_returns_same_instance(self, tmp_path, monkeypatch):
        """Test that get_config returns the same instance."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "singleton-kafka:9092"
""")
        # Point to our test config
        monkeypatch.setattr(
            "kafka_pipeline.config.DEFAULT_CONFIG_PATH",
            config_file
        )

        config1 = get_config()
        config2 = get_config()

        assert config1 is config2
        assert config1.bootstrap_servers == "singleton-kafka:9092"

    def test_set_config_overrides_singleton(self, monkeypatch):
        """Test that set_config sets the singleton instance."""
        custom_config = KafkaConfig(bootstrap_servers="custom:9092")

        set_config(custom_config)
        retrieved = get_config()

        assert retrieved is custom_config
        assert retrieved.bootstrap_servers == "custom:9092"

    def test_reset_config_clears_singleton(self, tmp_path, monkeypatch):
        """Test that reset_config clears the singleton."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
kafka:
  bootstrap_servers: "first:9092"
""")
        monkeypatch.setattr(
            "kafka_pipeline.config.DEFAULT_CONFIG_PATH",
            config_file
        )

        first = get_config()
        assert first.bootstrap_servers == "first:9092"

        # Update config file
        config_file.write_text("""
kafka:
  bootstrap_servers: "second:9092"
""")

        reset_config()
        second = get_config()

        assert second.bootstrap_servers == "second:9092"
        assert first is not second
