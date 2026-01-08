"""Kafka pipeline configuration from YAML file.

Configuration loads ONLY from config.yaml.
Environment variables are NOT supported (except for ClaimX API credentials).
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
    """Kafka pipeline configuration.

    Loads from YAML file with hierarchical structure organized by domain and worker.
    Each consumer and producer can be individually configured.

    Configuration structure:
        kafka:
          connection: {...}           # Shared connection settings
          consumer_defaults: {...}    # Default consumer settings
          producer_defaults: {...}    # Default producer settings
          xact:                       # XACT domain
            topics: {...}
            event_ingester:
              consumer: {...}
              producer: {...}
              processing: {...}
            download_worker: {...}
            upload_worker: {...}
          claimx:                     # ClaimX domain
            (same structure as xact)
          storage: {...}              # OneLake paths

    All timing values in milliseconds unless otherwise noted.
    """

    # =========================================================================
    # CONNECTION SETTINGS (shared across all consumers/producers)
    # =========================================================================
    bootstrap_servers: str = ""
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "OAUTHBEARER"
    sasl_plain_username: str = ""
    sasl_plain_password: str = ""
    request_timeout_ms: int = 120000  # 2 minutes
    metadata_max_age_ms: int = 300000  # 5 minutes
    connections_max_idle_ms: int = 540000  # 9 minutes

    # =========================================================================
    # DEFAULT SETTINGS (applied to all consumers/producers unless overridden)
    # =========================================================================
    consumer_defaults: Dict[str, Any] = field(default_factory=dict)
    producer_defaults: Dict[str, Any] = field(default_factory=dict)

    # =========================================================================
    # DOMAIN CONFIGURATIONS (xact and claimx)
    # =========================================================================
    xact: Dict[str, Any] = field(default_factory=dict)
    claimx: Dict[str, Any] = field(default_factory=dict)

    # =========================================================================
    # STORAGE CONFIGURATION
    # =========================================================================
    onelake_base_path: str = ""  # Fallback path
    onelake_domain_paths: Dict[str, str] = field(default_factory=dict)
    cache_dir: str = field(default_factory=_get_default_cache_dir)

    # =========================================================================
    # CLAIMX API CONFIGURATION
    # =========================================================================
    claimx_api_url: str = ""
    claimx_api_username: str = ""
    claimx_api_password: str = ""
    claimx_api_timeout_seconds: int = 30
    claimx_api_concurrency: int = 20

    def get_worker_config(
        self,
        domain: str,
        worker_name: str,
        component: str,  # "consumer", "producer", or "processing"
    ) -> Dict[str, Any]:
        """Get merged configuration for a specific worker's component.

        Merges defaults with worker-specific overrides.

        Merge priority (highest to lowest):
        1. Worker-specific config (e.g., xact.download_worker.consumer)
        2. Default config (consumer_defaults or producer_defaults)

        Args:
            domain: "xact" or "claimx"
            worker_name: Worker name (e.g., "download_worker", "event_ingester")
            component: Component type ("consumer", "producer", or "processing")

        Returns:
            Merged configuration dict with all settings resolved

        Examples:
            >>> config.get_worker_config("xact", "download_worker", "consumer")
            {'max_poll_records': 50, 'session_timeout_ms': 60000, ...}

            >>> config.get_worker_config("xact", "download_worker", "processing")
            {'concurrency': 10, 'batch_size': 20, 'timeout_seconds': 60}
        """
        # Get domain config
        domain_config = self.xact if domain == "xact" else self.claimx
        if not domain_config:
            raise ValueError(f"No configuration found for domain: {domain}")

        # Start with defaults for consumer/producer (no defaults for processing)
        if component == "consumer":
            result = self.consumer_defaults.copy()
        elif component == "producer":
            result = self.producer_defaults.copy()
        elif component == "processing":
            result = {}
        else:
            raise ValueError(
                f"Invalid component: {component}. Must be 'consumer', 'producer', or 'processing'"
            )

        # Get worker-specific overrides
        worker_config = domain_config.get(worker_name, {})
        component_config = worker_config.get(component, {})

        # Merge worker-specific settings over defaults
        result.update(component_config)

        return result

    def get_topic(self, domain: str, topic_key: str) -> str:
        """Get topic name for a specific domain and topic key.

        Args:
            domain: "xact" or "claimx"
            topic_key: Topic key (e.g., "events", "downloads_pending", "downloads_cached")

        Returns:
            Full topic name

        Examples:
            >>> config.get_topic("xact", "events")
            "xact.events.raw"

            >>> config.get_topic("claimx", "enrichment_pending")
            "claimx.enrichment.pending"

        Raises:
            ValueError: If domain or topic_key not found
        """
        domain_config = self.xact if domain == "xact" else self.claimx
        if not domain_config:
            raise ValueError(f"No configuration found for domain: {domain}")

        topics = domain_config.get("topics", {})
        if topic_key not in topics:
            raise ValueError(
                f"Topic '{topic_key}' not found in {domain} domain. "
                f"Available topics: {list(topics.keys())}"
            )

        return topics[topic_key]

    def get_consumer_group(self, domain: str, worker_name: str) -> str:
        """Get consumer group name for a worker.

        First checks if worker has a custom group_id in its consumer config.
        Otherwise, constructs from consumer_group_prefix pattern.

        Args:
            domain: "xact" or "claimx"
            worker_name: Worker name (e.g., "download_worker")

        Returns:
            Consumer group name

        Examples:
            >>> config.get_consumer_group("xact", "download_worker")
            "xact-download-worker"
        """
        # Check if worker has custom group_id
        worker_config = self.get_worker_config(domain, worker_name, "consumer")
        if "group_id" in worker_config:
            return worker_config["group_id"]

        # Otherwise construct from prefix
        domain_config = self.xact if domain == "xact" else self.claimx
        prefix = domain_config.get("consumer_group_prefix", domain)
        return f"{prefix}-{worker_name}"

    def get_retry_topic(self, domain: str, attempt: int) -> str:
        """Get retry topic name for a specific retry attempt.

        Args:
            domain: "xact" or "claimx"
            attempt: Retry attempt number (0-indexed)

        Returns:
            Topic name for this retry level (e.g., "xact.downloads.retry.5m")

        Raises:
            ValueError: If attempt exceeds configured max retries
        """
        domain_config = self.xact if domain == "xact" else self.claimx
        retry_delays = domain_config.get("retry_delays", [])

        if attempt >= len(retry_delays):
            raise ValueError(
                f"Retry attempt {attempt} exceeds max retries {len(retry_delays)}"
            )

        delay_seconds = retry_delays[attempt]
        delay_minutes = delay_seconds // 60

        pending_topic = self.get_topic(domain, "downloads_pending")
        return f"{pending_topic}.retry.{delay_minutes}m"

    def get_retry_delays(self, domain: str) -> List[int]:
        """Get retry delays for a domain.

        Args:
            domain: "xact" or "claimx"

        Returns:
            List of retry delays in seconds
        """
        domain_config = self.xact if domain == "xact" else self.claimx
        return domain_config.get("retry_delays", [300, 600, 1200, 2400])

    def get_max_retries(self, domain: str) -> int:
        """Get max retries for a domain.

        Args:
            domain: "xact" or "claimx"

        Returns:
            Maximum number of retries (derived from retry_delays length)
        """
        return len(self.get_retry_delays(domain))

    def validate(self) -> None:
        """Validate configuration for correctness and constraints.

        Raises:
            ValueError: If configuration is invalid

        Validations:
        - Required fields present
        - Consumer timeout constraints (Kafka requirements)
        - Producer settings valid
        - Numeric ranges
        - Enum values
        """
        # Required connection settings
        if not self.bootstrap_servers:
            raise ValueError("bootstrap_servers is required in kafka.connection section")

        # Validate consumer defaults
        self._validate_consumer_settings(self.consumer_defaults, "consumer_defaults")

        # Validate producer defaults
        self._validate_producer_settings(self.producer_defaults, "producer_defaults")

        # Validate each domain
        for domain_name in ["xact", "claimx"]:
            domain_config = getattr(self, domain_name)
            if not domain_config:
                continue  # Domain not configured, skip

            # Validate each worker in domain
            for worker_name, worker_config in domain_config.items():
                if worker_name in ["topics", "consumer_group_prefix", "retry_delays", "max_retries"]:
                    continue  # Skip non-worker keys

                # Validate worker consumer settings
                if "consumer" in worker_config:
                    self._validate_consumer_settings(
                        worker_config["consumer"],
                        f"{domain_name}.{worker_name}.consumer"
                    )

                # Validate worker producer settings
                if "producer" in worker_config:
                    self._validate_producer_settings(
                        worker_config["producer"],
                        f"{domain_name}.{worker_name}.producer"
                    )

                # Validate worker processing settings
                if "processing" in worker_config:
                    self._validate_processing_settings(
                        worker_config["processing"],
                        f"{domain_name}.{worker_name}.processing"
                    )

    def _validate_consumer_settings(self, settings: Dict[str, Any], context: str) -> None:
        """Validate consumer settings.

        Args:
            settings: Consumer settings dict
            context: Context string for error messages

        Raises:
            ValueError: If settings are invalid
        """
        # Kafka requirement: heartbeat_interval_ms < session_timeout_ms / 3
        if "heartbeat_interval_ms" in settings and "session_timeout_ms" in settings:
            heartbeat = settings["heartbeat_interval_ms"]
            session_timeout = settings["session_timeout_ms"]
            if heartbeat >= session_timeout / 3:
                raise ValueError(
                    f"{context}: heartbeat_interval_ms ({heartbeat}) must be < "
                    f"session_timeout_ms/3 ({session_timeout/3:.0f}). "
                    f"Recommended: heartbeat_interval_ms <= {session_timeout // 3}"
                )

        # Logical requirement: session_timeout_ms < max_poll_interval_ms
        if "session_timeout_ms" in settings and "max_poll_interval_ms" in settings:
            session_timeout = settings["session_timeout_ms"]
            max_poll_interval = settings["max_poll_interval_ms"]
            if session_timeout >= max_poll_interval:
                raise ValueError(
                    f"{context}: session_timeout_ms ({session_timeout}) must be < "
                    f"max_poll_interval_ms ({max_poll_interval})"
                )

        # Validate numeric ranges
        if "max_poll_records" in settings:
            if settings["max_poll_records"] < 1:
                raise ValueError(
                    f"{context}: max_poll_records must be >= 1, got {settings['max_poll_records']}"
                )

        # Validate auto_offset_reset
        if "auto_offset_reset" in settings:
            valid_values = ["earliest", "latest", "none"]
            if settings["auto_offset_reset"] not in valid_values:
                raise ValueError(
                    f"{context}: auto_offset_reset must be one of {valid_values}, "
                    f"got '{settings['auto_offset_reset']}'"
                )

        # Validate partition assignment strategy
        if "partition_assignment_strategy" in settings:
            valid_strategies = ["RoundRobin", "Range", "Sticky"]
            if settings["partition_assignment_strategy"] not in valid_strategies:
                raise ValueError(
                    f"{context}: partition_assignment_strategy must be one of {valid_strategies}, "
                    f"got '{settings['partition_assignment_strategy']}'"
                )

    def _validate_producer_settings(self, settings: Dict[str, Any], context: str) -> None:
        """Validate producer settings.

        Args:
            settings: Producer settings dict
            context: Context string for error messages

        Raises:
            ValueError: If settings are invalid
        """
        # Validate acks
        if "acks" in settings:
            valid_acks = ["0", "1", "all", 0, 1]
            if settings["acks"] not in valid_acks:
                raise ValueError(
                    f"{context}: acks must be one of [0, 1, 'all'], got '{settings['acks']}'"
                )

        # Validate compression_type
        if "compression_type" in settings:
            valid_compression = ["none", "gzip", "snappy", "lz4", "zstd"]
            if settings["compression_type"] not in valid_compression:
                raise ValueError(
                    f"{context}: compression_type must be one of {valid_compression}, "
                    f"got '{settings['compression_type']}'"
                )

        # Validate numeric ranges
        if "retries" in settings and settings["retries"] < 0:
            raise ValueError(
                f"{context}: retries must be >= 0, got {settings['retries']}"
            )

        if "batch_size" in settings and settings["batch_size"] < 0:
            raise ValueError(
                f"{context}: batch_size must be >= 0, got {settings['batch_size']}"
            )

        if "linger_ms" in settings and settings["linger_ms"] < 0:
            raise ValueError(
                f"{context}: linger_ms must be >= 0, got {settings['linger_ms']}"
            )

    def _validate_processing_settings(self, settings: Dict[str, Any], context: str) -> None:
        """Validate processing settings.

        Args:
            settings: Processing settings dict
            context: Context string for error messages

        Raises:
            ValueError: If settings are invalid
        """
        # Validate concurrency range (1-50)
        if "concurrency" in settings:
            concurrency = settings["concurrency"]
            if not (1 <= concurrency <= 50):
                raise ValueError(
                    f"{context}: concurrency must be between 1 and 50, got {concurrency}"
                )

        # Validate batch_size
        if "batch_size" in settings:
            if settings["batch_size"] < 1:
                raise ValueError(
                    f"{context}: batch_size must be >= 1, got {settings['batch_size']}"
                )

        # Validate timeout_seconds
        if "timeout_seconds" in settings:
            if settings["timeout_seconds"] <= 0:
                raise ValueError(
                    f"{context}: timeout_seconds must be > 0, got {settings['timeout_seconds']}"
                )

        # Validate flush_timeout_seconds (for delta writers)
        if "flush_timeout_seconds" in settings:
            if settings["flush_timeout_seconds"] <= 0:
                raise ValueError(
                    f"{context}: flush_timeout_seconds must be > 0, got {settings['flush_timeout_seconds']}"
                )


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge overlay into base dict.

    Args:
        base: Base dictionary
        overlay: Dictionary to merge on top

    Returns:
        Merged dictionary
    """
    result = base.copy()
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> KafkaConfig:
    """Load Kafka configuration from YAML file.

    Configuration loads ONLY from YAML file.
    Environment variables are NOT supported.

    Args:
        config_path: Path to YAML config file. Defaults to config.yaml in src/
        overrides: Optional dict of overrides to apply (for testing)

    Returns:
        KafkaConfig instance with validated configuration

    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist

    Example config.yaml:
        kafka:
          connection:
            bootstrap_servers: "localhost:9092"
            security_protocol: "PLAINTEXT"
          consumer_defaults:
            max_poll_records: 100
            session_timeout_ms: 30000
          producer_defaults:
            acks: "all"
            retries: 3
          xact:
            topics:
              events: "xact.events.raw"
              downloads_pending: "xact.downloads.pending"
            download_worker:
              consumer:
                max_poll_records: 50
              processing:
                concurrency: 10
    """
    config_path = config_path or DEFAULT_CONFIG_PATH

    # Load YAML
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Create config.yaml based on config.yaml.example"
        )

    with open(config_path, "r") as f:
        yaml_data = yaml.safe_load(f) or {}

    # Extract kafka section
    if "kafka" not in yaml_data:
        raise ValueError(
            "Invalid config file: missing 'kafka:' section\n"
            "See config.yaml.example for correct structure"
        )

    kafka_config = yaml_data["kafka"]

    # Apply overrides (for testing)
    if overrides:
        kafka_config = _deep_merge(kafka_config, overrides)

    # Extract connection settings (support both flat and nested structure)
    connection = kafka_config.get("connection", {})
    if not connection:
        # Fallback: connection settings directly under kafka (flat structure)
        connection = kafka_config

    # Extract defaults
    consumer_defaults = kafka_config.get("consumer_defaults", {})
    producer_defaults = kafka_config.get("producer_defaults", {})

    # Extract domain configs
    xact_config = kafka_config.get("xact", {})
    claimx_config = kafka_config.get("claimx", {})

    # Extract storage settings (support multiple locations for flexibility)
    # Merge sources with priority: kafka.storage > root storage > flat kafka structure
    # This allows users to split config across locations (e.g., onelake paths at root,
    # cache_dir under kafka.storage) and have them properly merged.
    storage = {}

    # Start with flat kafka structure as base (lowest priority)
    for key in ["onelake_base_path", "onelake_domain_paths", "cache_dir"]:
        if key in kafka_config:
            storage[key] = kafka_config[key]

    # Merge root-level storage section (medium priority)
    root_storage = yaml_data.get("storage", {})
    if root_storage:
        storage = _deep_merge(storage, root_storage)

    # Merge kafka.storage section (highest priority)
    kafka_storage = kafka_config.get("storage", {})
    if kafka_storage:
        storage = _deep_merge(storage, kafka_storage)

    # Extract ClaimX API settings (from root-level claimx section, not kafka.claimx)
    claimx_root = yaml_data.get("claimx", {})
    claimx_api = claimx_root.get("api", {})

    # Load ClaimX API credentials from environment variables
    claimx_api_username = os.getenv("CLAIMX_API_USERNAME", "")
    claimx_api_password = os.getenv("CLAIMX_API_PASSWORD", "")

    # Build KafkaConfig instance
    config = KafkaConfig(
        # Connection settings
        bootstrap_servers=connection.get("bootstrap_servers", ""),
        security_protocol=connection.get("security_protocol", "PLAINTEXT"),
        sasl_mechanism=connection.get("sasl_mechanism", "OAUTHBEARER"),
        sasl_plain_username=connection.get("sasl_plain_username", ""),
        sasl_plain_password=connection.get("sasl_plain_password", ""),
        request_timeout_ms=connection.get("request_timeout_ms", 120000),
        metadata_max_age_ms=connection.get("metadata_max_age_ms", 300000),
        connections_max_idle_ms=connection.get("connections_max_idle_ms", 540000),
        # Defaults
        consumer_defaults=consumer_defaults,
        producer_defaults=producer_defaults,
        # Domain configs
        xact=xact_config,
        claimx=claimx_config,
        # Storage
        onelake_base_path=storage.get("onelake_base_path", ""),
        onelake_domain_paths=storage.get("onelake_domain_paths", {}),
        cache_dir=storage.get("cache_dir") or _get_default_cache_dir(),
        # ClaimX API
        claimx_api_url=claimx_api.get("base_url", ""),
        claimx_api_username=claimx_api_username,
        claimx_api_password=claimx_api_password,
        claimx_api_timeout_seconds=claimx_api.get("timeout_seconds", 30),
        claimx_api_concurrency=claimx_api.get("max_concurrent", 20),
    )

    # Validate configuration
    config.validate()

    return config


# Singleton instance
_kafka_config: Optional[KafkaConfig] = None


def get_config() -> KafkaConfig:
    """Get or load the singleton Kafka config instance.

    Uses load_config() on first call, then returns cached instance.

    Returns:
        Singleton KafkaConfig instance
    """
    global _kafka_config
    if _kafka_config is None:
        _kafka_config = load_config()
    return _kafka_config


def set_config(config: KafkaConfig) -> None:
    """Set the singleton Kafka config instance.

    Useful for testing or programmatic configuration.

    Args:
        config: KafkaConfig instance to use as singleton
    """
    global _kafka_config
    _kafka_config = config


def reset_config() -> None:
    """Reset the singleton config instance.

    Forces reload on next get_config() call.
    """
    global _kafka_config
    _kafka_config = None
