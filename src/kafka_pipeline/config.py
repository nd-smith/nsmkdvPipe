"""Kafka pipeline configuration from YAML and environment variables.

Configuration priority (highest to lowest):
1. Environment variables
2. config.yaml file
3. Dataclass defaults
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# Default config path: config.yaml in src/ directory
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@dataclass
class KafkaConfig:
    """Kafka connection and behavior configuration.

    Load from YAML using load_config() or get_config().
    For backwards compatibility, from_env() is still available.
    All timing values in milliseconds unless otherwise noted.
    """

    # Connection
    bootstrap_servers: str = ""
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

    # Connection settings (prevent timeout during long operations)
    request_timeout_ms: int = 120000  # 2 minutes for requests
    metadata_max_age_ms: int = 300000  # 5 minutes between metadata refreshes
    connections_max_idle_ms: int = 540000  # 9 minutes idle before close

    # Topics
    events_topic: str = "xact.events.raw"
    downloads_pending_topic: str = "xact.downloads.pending"
    downloads_cached_topic: str = "xact.downloads.cached"
    downloads_results_topic: str = "xact.downloads.results"
    dlq_topic: str = "xact.downloads.dlq"

    # Consumer group prefix
    consumer_group_prefix: str = "xact"

    # Retry configuration (delays in seconds)
    retry_delays: List[int] = field(default_factory=lambda: [300, 600, 1200, 2400])
    max_retries: int = 4

    # Storage configuration
    onelake_base_path: str = ""  # abfss:// path to OneLake files directory (fallback)
    onelake_domain_paths: Dict[str, str] = field(default_factory=dict)  # Domain -> OneLake path

    # Download concurrency settings (FR-2.6)
    download_concurrency: int = 10  # Max concurrent downloads (default: 10, range: 1-50)
    download_batch_size: int = 20  # Messages to fetch per batch

    # Upload concurrency settings
    upload_concurrency: int = 10  # Max concurrent uploads (default: 10, range: 1-50)
    upload_batch_size: int = 20  # Messages to fetch per batch

    # Cache directory for downloaded files awaiting upload
    cache_dir: str = "/tmp/kafka_pipeline_cache"

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Load configuration from environment variables only.

        DEPRECATED: Use load_config() or get_config() instead.
        This method is kept for backwards compatibility.

        Required environment variables:
            KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses

        Optional environment variables (with defaults):
            KAFKA_SECURITY_PROTOCOL: SASL_SSL (default)
            KAFKA_SASL_MECHANISM: OAUTHBEARER (default)
            KAFKA_EVENTS_TOPIC: xact.events.raw (default)
            KAFKA_DOWNLOADS_PENDING_TOPIC: xact.downloads.pending (default)
            KAFKA_DOWNLOADS_CACHED_TOPIC: xact.downloads.cached (default)
            KAFKA_DOWNLOADS_RESULTS_TOPIC: xact.downloads.results (default)
            KAFKA_DLQ_TOPIC: xact.downloads.dlq (default)
            KAFKA_CONSUMER_GROUP_PREFIX: xact (default)
            KAFKA_MAX_POLL_RECORDS: 100 (default)
            KAFKA_SESSION_TIMEOUT_MS: 30000 (default)
            RETRY_DELAYS: 300,600,1200,2400 (default, comma-separated seconds)
            MAX_RETRIES: 4 (default)
            ONELAKE_BASE_PATH: OneLake abfss:// path (fallback for upload)
            ONELAKE_XACT_PATH: OneLake path for xact domain
            ONELAKE_CLAIMX_PATH: OneLake path for claimx domain
            DOWNLOAD_CONCURRENCY: 10 (default, max concurrent downloads, range 1-50)
            DOWNLOAD_BATCH_SIZE: 20 (default, messages to fetch per batch)
            UPLOAD_CONCURRENCY: 10 (default, max concurrent uploads, range 1-50)
            UPLOAD_BATCH_SIZE: 20 (default, messages to fetch per batch)
            CACHE_DIR: /tmp/kafka_pipeline_cache (default, local cache for downloads)

        Raises:
            ValueError: If required environment variables are missing
        """
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

        # Parse retry delays from comma-separated string
        retry_delays_str = os.getenv("RETRY_DELAYS", "300,600,1200,2400")
        retry_delays = [int(d.strip()) for d in retry_delays_str.split(",")]

        # Build domain paths from environment variables
        onelake_domain_paths: Dict[str, str] = {}
        if os.getenv("ONELAKE_XACT_PATH"):
            onelake_domain_paths["xact"] = os.getenv("ONELAKE_XACT_PATH", "")
        if os.getenv("ONELAKE_CLAIMX_PATH"):
            onelake_domain_paths["claimx"] = os.getenv("ONELAKE_CLAIMX_PATH", "")

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
            downloads_cached_topic=os.getenv(
                "KAFKA_DOWNLOADS_CACHED_TOPIC", "xact.downloads.cached"
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
            onelake_domain_paths=onelake_domain_paths,
            # Cache directory
            cache_dir=os.getenv("CACHE_DIR", "/tmp/kafka_pipeline_cache"),
            # Download concurrency settings
            download_concurrency=min(
                50,  # Max allowed
                max(1, int(os.getenv("DOWNLOAD_CONCURRENCY", "10"))),  # Min 1
            ),
            download_batch_size=max(1, int(os.getenv("DOWNLOAD_BATCH_SIZE", "20"))),
            # Upload concurrency settings
            upload_concurrency=min(
                50,  # Max allowed
                max(1, int(os.getenv("UPLOAD_CONCURRENCY", "10"))),  # Min 1
            ),
            upload_batch_size=max(1, int(os.getenv("UPLOAD_BATCH_SIZE", "20"))),
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


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge overlay into base dict."""
    result = base.copy()
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _apply_env_overrides(data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply environment variable overrides to config data.

    Environment variables take precedence over YAML values.
    """
    result = data.copy()

    # Map environment variables to config keys
    env_mapping = {
        "KAFKA_BOOTSTRAP_SERVERS": "bootstrap_servers",
        "KAFKA_SECURITY_PROTOCOL": "security_protocol",
        "KAFKA_SASL_MECHANISM": "sasl_mechanism",
        "KAFKA_SASL_PLAIN_USERNAME": "sasl_plain_username",
        "KAFKA_SASL_PLAIN_PASSWORD": "sasl_plain_password",
        "KAFKA_MAX_POLL_RECORDS": ("max_poll_records", int),
        "KAFKA_SESSION_TIMEOUT_MS": ("session_timeout_ms", int),
        "KAFKA_EVENTS_TOPIC": "events_topic",
        "KAFKA_DOWNLOADS_PENDING_TOPIC": "downloads_pending_topic",
        "KAFKA_DOWNLOADS_CACHED_TOPIC": "downloads_cached_topic",
        "KAFKA_DOWNLOADS_RESULTS_TOPIC": "downloads_results_topic",
        "KAFKA_DLQ_TOPIC": "dlq_topic",
        "KAFKA_CONSUMER_GROUP_PREFIX": "consumer_group_prefix",
        "MAX_RETRIES": ("max_retries", int),
        "ONELAKE_BASE_PATH": "onelake_base_path",
        "CACHE_DIR": "cache_dir",
        "DOWNLOAD_CONCURRENCY": ("download_concurrency", int),
        "DOWNLOAD_BATCH_SIZE": ("download_batch_size", int),
        "UPLOAD_CONCURRENCY": ("upload_concurrency", int),
        "UPLOAD_BATCH_SIZE": ("upload_batch_size", int),
    }

    for env_var, config_key in env_mapping.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            if isinstance(config_key, tuple):
                key, converter = config_key
                result[key] = converter(env_value)
            else:
                result[config_key] = env_value

    # Special handling for retry_delays (comma-separated list)
    retry_delays_str = os.getenv("RETRY_DELAYS")
    if retry_delays_str is not None:
        result["retry_delays"] = [int(d.strip()) for d in retry_delays_str.split(",")]

    # Special handling for domain-specific OneLake paths
    domain_paths = result.get("onelake_domain_paths", {}).copy()
    if os.getenv("ONELAKE_XACT_PATH"):
        domain_paths["xact"] = os.getenv("ONELAKE_XACT_PATH", "")
    if os.getenv("ONELAKE_CLAIMX_PATH"):
        domain_paths["claimx"] = os.getenv("ONELAKE_CLAIMX_PATH", "")
    if domain_paths:
        result["onelake_domain_paths"] = domain_paths

    # Apply concurrency constraints
    if "download_concurrency" in result:
        result["download_concurrency"] = min(50, max(1, result["download_concurrency"]))
    if "upload_concurrency" in result:
        result["upload_concurrency"] = min(50, max(1, result["upload_concurrency"]))
    if "download_batch_size" in result:
        result["download_batch_size"] = max(1, result["download_batch_size"])
    if "upload_batch_size" in result:
        result["upload_batch_size"] = max(1, result["upload_batch_size"])

    return result


def load_config(
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> KafkaConfig:
    """Load Kafka configuration from YAML file with environment variable overrides.

    Configuration priority (highest to lowest):
    1. Environment variables
    2. overrides parameter
    3. config.yaml file (under 'kafka:' key)
    4. Dataclass defaults

    Args:
        config_path: Path to YAML config file. Defaults to config.yaml in project root.
        overrides: Dict of overrides to apply (after YAML, before env vars)

    Returns:
        KafkaConfig instance

    Example config.yaml:
        kafka:
          bootstrap_servers: "localhost:9092"
          security_protocol: "PLAINTEXT"
          events_topic: "xact.events.raw"
          download_concurrency: 20
    """
    config_path = config_path or DEFAULT_CONFIG_PATH

    # Load YAML
    data: Dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, "r") as f:
            yaml_data = yaml.safe_load(f) or {}

        # Extract kafka section if present
        if "kafka" in yaml_data:
            data = yaml_data["kafka"]
        else:
            # Allow flat structure for backwards compatibility
            data = yaml_data

    # Apply overrides
    if overrides:
        data = _deep_merge(data, overrides)

    # Apply environment variable overrides
    data = _apply_env_overrides(data)

    # Validate required fields
    if not data.get("bootstrap_servers"):
        raise ValueError(
            "bootstrap_servers is required. "
            "Set in config.yaml under 'kafka:' key or via KAFKA_BOOTSTRAP_SERVERS env var."
        )

    # Build config from data
    return KafkaConfig(
        bootstrap_servers=data.get("bootstrap_servers", ""),
        security_protocol=data.get("security_protocol", "SASL_SSL"),
        sasl_mechanism=data.get("sasl_mechanism", "OAUTHBEARER"),
        sasl_plain_username=data.get("sasl_plain_username", ""),
        sasl_plain_password=data.get("sasl_plain_password", ""),
        auto_offset_reset=data.get("auto_offset_reset", "earliest"),
        enable_auto_commit=data.get("enable_auto_commit", False),
        max_poll_records=data.get("max_poll_records", 100),
        max_poll_interval_ms=data.get("max_poll_interval_ms", 300000),
        session_timeout_ms=data.get("session_timeout_ms", 30000),
        acks=data.get("acks", "all"),
        retries=data.get("retries", 3),
        retry_backoff_ms=data.get("retry_backoff_ms", 1000),
        events_topic=data.get("events_topic", "xact.events.raw"),
        downloads_pending_topic=data.get("downloads_pending_topic", "xact.downloads.pending"),
        downloads_cached_topic=data.get("downloads_cached_topic", "xact.downloads.cached"),
        downloads_results_topic=data.get("downloads_results_topic", "xact.downloads.results"),
        dlq_topic=data.get("dlq_topic", "xact.downloads.dlq"),
        consumer_group_prefix=data.get("consumer_group_prefix", "xact"),
        retry_delays=data.get("retry_delays", [300, 600, 1200, 2400]),
        max_retries=data.get("max_retries", 4),
        onelake_base_path=data.get("onelake_base_path", ""),
        onelake_domain_paths=data.get("onelake_domain_paths", {}),
        download_concurrency=data.get("download_concurrency", 10),
        download_batch_size=data.get("download_batch_size", 20),
        upload_concurrency=data.get("upload_concurrency", 10),
        upload_batch_size=data.get("upload_batch_size", 20),
        cache_dir=data.get("cache_dir", "/tmp/kafka_pipeline_cache"),
    )


# Singleton instance
_kafka_config: Optional[KafkaConfig] = None


def get_config() -> KafkaConfig:
    """Get or load the singleton Kafka config instance.

    Uses load_config() on first call, then returns cached instance.
    """
    global _kafka_config
    if _kafka_config is None:
        _kafka_config = load_config()
    return _kafka_config


def set_config(config: KafkaConfig) -> None:
    """Set the singleton Kafka config instance.

    Useful for testing or programmatic configuration.
    """
    global _kafka_config
    _kafka_config = config


def reset_config() -> None:
    """Reset the singleton config instance.

    Forces reload on next get_config() call.
    """
    global _kafka_config
    _kafka_config = None
