"""Kafka pipeline configuration from YAML and environment variables.

Configuration priority (highest to lowest):
1. Environment variables
2. config.yaml file
3. Dataclass defaults
"""

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def _get_default_cache_dir() -> str:
    """Get cross-platform default cache directory.

    Uses the system temp directory to ensure the path is valid on both
    Windows and Unix systems.

    Returns:
        Absolute path to the default cache directory.
    """
    return str(Path(tempfile.gettempdir()) / "kafka_pipeline_cache")


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

    # Domain-specific topic prefixes
    xact_topic_prefix: str = "xact"
    claimx_topic_prefix: str = "claimx"

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
    # Default is computed at runtime via _get_default_cache_dir() for cross-platform support
    cache_dir: str = field(default_factory=_get_default_cache_dir)

    # ClaimX API settings
    claimx_api_url: str = ""  # Base URL for ClaimX API
    claimx_api_username: str = ""  # Basic auth username
    claimx_api_password: str = ""  # Basic auth password
    claimx_api_timeout_seconds: int = 30  # Request timeout in seconds
    claimx_api_max_retries: int = 3  # Max retries for API calls
    claimx_api_concurrency: int = 10  # Max concurrent API requests

    # Delta events writer settings
    delta_events_batch_size: int = 1000  # Events per batch before writing to Delta
    delta_events_max_batches: Optional[int] = None  # Optional limit for testing (None = unlimited)

    # Delta events retry settings
    delta_events_retry_delays: List[int] = field(
        default_factory=lambda: [300, 600, 1200, 2400]  # 5m, 10m, 20m, 40m
    )
    delta_events_max_retries: int = 4
    delta_events_retry_topic_prefix: str = "delta-events.retry"
    delta_events_dlq_topic: str = "delta-events.dlq"

    # Consumer batch limiting for testing
    consumer_max_batches: Optional[int] = None  # Optional limit on poll batches (None = unlimited)

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
            KAFKA_XACT_TOPIC_PREFIX: xact (default)
            KAFKA_CLAIMX_TOPIC_PREFIX: claimx (default)
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
            CLAIMX_API_URL: ClaimX API base URL
            CLAIMX_API_USERNAME: ClaimX API username for Basic auth
            CLAIMX_API_PASSWORD: ClaimX API password for Basic auth
            CLAIMX_API_TIMEOUT_SECONDS: 30 (default, API request timeout in seconds)
            CLAIMX_API_MAX_RETRIES: 3 (default, max retries for API calls)
            CLAIMX_API_CONCURRENCY: 10 (default, max concurrent API requests)

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
            # Domain topic prefixes
            xact_topic_prefix=os.getenv("KAFKA_XACT_TOPIC_PREFIX", "xact"),
            claimx_topic_prefix=os.getenv("KAFKA_CLAIMX_TOPIC_PREFIX", "claimx"),
            # Consumer group
            consumer_group_prefix=os.getenv("KAFKA_CONSUMER_GROUP_PREFIX", "xact"),
            # Retry configuration
            retry_delays=retry_delays,
            max_retries=int(os.getenv("MAX_RETRIES", "4")),
            # Storage configuration
            onelake_base_path=os.getenv("ONELAKE_BASE_PATH", ""),
            onelake_domain_paths=onelake_domain_paths,
            # Cache directory
            cache_dir=os.getenv("CACHE_DIR") or _get_default_cache_dir(),
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
            # ClaimX API settings
            claimx_api_url=os.getenv("CLAIMX_API_URL", ""),
            claimx_api_username=os.getenv("CLAIMX_API_USERNAME", ""),
            claimx_api_password=os.getenv("CLAIMX_API_PASSWORD", ""),
            claimx_api_timeout_seconds=int(os.getenv("CLAIMX_API_TIMEOUT_SECONDS", "30")),
            claimx_api_max_retries=int(os.getenv("CLAIMX_API_MAX_RETRIES", "3")),
            claimx_api_concurrency=int(os.getenv("CLAIMX_API_CONCURRENCY", "10")),
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

    def get_topic(self, domain: str, topic_type: str) -> str:
        """Get topic name for a specific domain and topic type.

        Args:
            domain: Domain name ("xact" or "claimx")
            topic_type: Type of topic (e.g., "events.raw", "downloads.pending",
                       "downloads.cached", "downloads.results", "downloads.dlq",
                       "enrichment.pending")

        Returns:
            Full topic name (e.g., "xact.events.raw", "claimx.enrichment.pending")

        Raises:
            ValueError: If domain is not "xact" or "claimx"

        Examples:
            >>> config.get_topic("xact", "events.raw")
            "xact.events.raw"
            >>> config.get_topic("claimx", "enrichment.pending")
            "claimx.enrichment.pending"
        """
        if domain == "xact":
            prefix = self.xact_topic_prefix
        elif domain == "claimx":
            prefix = self.claimx_topic_prefix
        else:
            raise ValueError(f"Unknown domain: {domain}. Must be 'xact' or 'claimx'")

        return f"{prefix}.{topic_type}"


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
        "KAFKA_XACT_TOPIC_PREFIX": "xact_topic_prefix",
        "KAFKA_CLAIMX_TOPIC_PREFIX": "claimx_topic_prefix",
        "KAFKA_CONSUMER_GROUP_PREFIX": "consumer_group_prefix",
        "MAX_RETRIES": ("max_retries", int),
        "ONELAKE_BASE_PATH": "onelake_base_path",
        "CACHE_DIR": "cache_dir",
        "DOWNLOAD_CONCURRENCY": ("download_concurrency", int),
        "DOWNLOAD_BATCH_SIZE": ("download_batch_size", int),
        "UPLOAD_CONCURRENCY": ("upload_concurrency", int),
        "UPLOAD_BATCH_SIZE": ("upload_batch_size", int),
        "CLAIMX_API_URL": "claimx_api_url",
        "CLAIMX_API_USERNAME": "claimx_api_username",
        "CLAIMX_API_PASSWORD": "claimx_api_password",
        "CLAIMX_API_TIMEOUT_SECONDS": ("claimx_api_timeout_seconds", int),
        "CLAIMX_API_MAX_RETRIES": ("claimx_api_max_retries", int),
        "CLAIMX_API_CONCURRENCY": ("claimx_api_concurrency", int),
        "DELTA_EVENTS_BATCH_SIZE": ("delta_events_batch_size", int),
        "DELTA_EVENTS_MAX_RETRIES": ("delta_events_max_retries", int),
        "DELTA_EVENTS_RETRY_TOPIC_PREFIX": "delta_events_retry_topic_prefix",
        "DELTA_EVENTS_DLQ_TOPIC": "delta_events_dlq_topic",
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

    # Special handling for delta_events_retry_delays (comma-separated list)
    delta_retry_delays_str = os.getenv("DELTA_EVENTS_RETRY_DELAYS")
    if delta_retry_delays_str is not None:
        result["delta_events_retry_delays"] = [
            int(d.strip()) for d in delta_retry_delays_str.split(",")
        ]

    # Special handling for domain-specific OneLake paths
    domain_paths = result.get("onelake_domain_paths", {}).copy()
    if os.getenv("ONELAKE_XACT_PATH"):
        domain_paths["xact"] = os.getenv("ONELAKE_XACT_PATH", "")
    if os.getenv("ONELAKE_CLAIMX_PATH"):
        domain_paths["claimx"] = os.getenv("ONELAKE_CLAIMX_PATH", "")
    if domain_paths:
        result["onelake_domain_paths"] = domain_paths

    # Special handling for delta_events_max_batches (optional int)
    max_batches_str = os.getenv("DELTA_EVENTS_MAX_BATCHES")
    if max_batches_str is not None and max_batches_str.strip():
        result["delta_events_max_batches"] = int(max_batches_str)

    # Special handling for consumer_max_batches (optional int)
    consumer_max_batches_str = os.getenv("CONSUMER_MAX_BATCHES")
    if consumer_max_batches_str is not None and consumer_max_batches_str.strip():
        result["consumer_max_batches"] = int(consumer_max_batches_str)

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
        request_timeout_ms=data.get("request_timeout_ms", 120000),
        metadata_max_age_ms=data.get("metadata_max_age_ms", 300000),
        connections_max_idle_ms=data.get("connections_max_idle_ms", 540000),
        events_topic=data.get("events_topic", "xact.events.raw"),
        downloads_pending_topic=data.get("downloads_pending_topic", "xact.downloads.pending"),
        downloads_cached_topic=data.get("downloads_cached_topic", "xact.downloads.cached"),
        downloads_results_topic=data.get("downloads_results_topic", "xact.downloads.results"),
        dlq_topic=data.get("dlq_topic", "xact.downloads.dlq"),
        xact_topic_prefix=data.get("xact_topic_prefix", "xact"),
        claimx_topic_prefix=data.get("claimx_topic_prefix", "claimx"),
        consumer_group_prefix=data.get("consumer_group_prefix", "xact"),
        retry_delays=data.get("retry_delays", [300, 600, 1200, 2400]),
        max_retries=data.get("max_retries", 4),
        onelake_base_path=data.get("onelake_base_path", ""),
        onelake_domain_paths=data.get("onelake_domain_paths", {}),
        download_concurrency=data.get("download_concurrency", 10),
        download_batch_size=data.get("download_batch_size", 20),
        upload_concurrency=data.get("upload_concurrency", 10),
        upload_batch_size=data.get("upload_batch_size", 20),
        cache_dir=data.get("cache_dir") or _get_default_cache_dir(),
        claimx_api_url=data.get("claimx_api_url", ""),
        claimx_api_username=data.get("claimx_api_username", ""),
        claimx_api_password=data.get("claimx_api_password", ""),
        claimx_api_timeout_seconds=data.get("claimx_api_timeout_seconds", 30),
        claimx_api_max_retries=data.get("claimx_api_max_retries", 3),
        claimx_api_concurrency=data.get("claimx_api_concurrency", 10),
        delta_events_batch_size=data.get("delta_events_batch_size", 1000),
        delta_events_max_batches=data.get("delta_events_max_batches"),
        delta_events_retry_delays=data.get(
            "delta_events_retry_delays", [300, 600, 1200, 2400]
        ),
        delta_events_max_retries=data.get("delta_events_max_retries", 4),
        delta_events_retry_topic_prefix=data.get(
            "delta_events_retry_topic_prefix", "delta-events.retry"
        ),
        delta_events_dlq_topic=data.get("delta_events_dlq_topic", "delta-events.dlq"),
        consumer_max_batches=data.get("consumer_max_batches"),
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
