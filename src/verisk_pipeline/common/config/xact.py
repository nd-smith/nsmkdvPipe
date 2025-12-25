"""
XACT Pipeline configuration classes.

Provides dataclass-based configuration for the XACT attachment processing pipeline.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from verisk_pipeline.common.config.base import DEFAULT_CONFIG_PATH, _deep_merge


@dataclass
class KustoConfig:
    """Kusto/Eventhouse configuration."""

    cluster_uri: str = ""
    database: str = ""
    table_name: str = "events"

    def __post_init__(self):
        # Env overrides
        self.cluster_uri = os.getenv("EVENTHOUSE_CLUSTER_URI", self.cluster_uri)
        self.database = os.getenv("KQL_DATABASE", self.database)


@dataclass
class LakehouseConfig:
    """OneLake/Lakehouse configuration."""

    abfss_path: str = ""
    events_table: str = "xact_events"
    attachments_table: str = "xact_attachments"  # Success-only inventory
    retry_table: str = "xact_retry"  # Active retry queue
    files_path: str = ""

    # Batch size limits for memory management (Task E.1)
    max_batch_size_retry_queue: int = 1000
    max_batch_size_inventory: int = 10000
    max_batch_size_merge: int = 100000
    max_batch_size_read: int = 50000

    # Upload concurrency parameters (Task E.5)
    upload_max_concurrency: int = 16  # Parallel block uploads
    upload_block_size_mb: int = 4  # 4 MB blocks
    upload_max_single_put_mb: int = 64  # 64 MB threshold for multipart

    # Cache limits (P2.2)
    max_cache_size_completed_ids: int = 1_000_000  # ~8MB in memory

    def __post_init__(self):
        # Env overrides
        self.abfss_path = os.getenv("LAKEHOUSE_ABFSS_PATH", self.abfss_path)
        self.files_path = os.getenv("FILES_BASE_PATH", self.files_path)

        # Derived paths
        if self.abfss_path and not self.files_path:
            self.files_path = f"{self.abfss_path}/Files"

    @property
    def events_table_path(self) -> str:
        """Full path to events Delta table."""
        return f"{self.abfss_path}/{self.events_table}"

    @property
    def attachments_table_path(self) -> str:
        """Full path to attachments inventory Delta table (success only)."""
        return f"{self.abfss_path}/{self.attachments_table}"

    @property
    def retry_table_path(self) -> str:
        """Full path to retry queue Delta table."""
        return f"{self.abfss_path}/{self.retry_table}"

    @property
    def tracking_table_path(self) -> str:
        """Deprecated: Use attachments_table_path instead."""
        return self.attachments_table_path

    @property
    def connection_pool_size(self) -> int:
        """Calculate optimal connection pool size (P2.4)."""
        import os

        cpu_cores = os.cpu_count() or 4
        # Formula: min(cpu_cores * 10, upload_max_concurrency, 250)
        return min(cpu_cores * 10, self.upload_max_concurrency, 250)


@dataclass
class ProcessingConfig:
    """Event processing configuration."""

    event_types: List[str] = field(default_factory=lambda: ["xact.assignment.*"])
    status_subtypes: List[str] = field(
        default_factory=lambda: [
            "documentsReceived",
            "firstNoticeOfLossReceived",
            "estimatePackageReceived",
        ]
    )
    start_date: str = "2024-01-01T00:00:00"
    batch_size: int = 100
    max_events_to_scan: int = 5000
    lookback_days: int = 7

    def __post_init__(self):
        """Ensure proper types from YAML/env vars."""
        self.batch_size = int(self.batch_size)
        self.max_events_to_scan = int(self.max_events_to_scan)
        self.lookback_days = int(self.lookback_days)


@dataclass
class DownloadConfig:
    """Attachment download configuration."""

    max_concurrent: int = 10
    timeout_seconds: int = 60
    max_retries: int = 3

    def __post_init__(self):
        """Ensure proper types from YAML/env vars."""
        self.max_concurrent = int(self.max_concurrent)
        self.timeout_seconds = int(self.timeout_seconds)
        self.max_retries = int(self.max_retries)


@dataclass
class RetryStageConfig:
    """Configuration for retry stage."""

    enabled: bool = True
    batch_size: int = 50
    min_retry_age_seconds: int = 300  # 5 minutes
    max_retries: int = 3
    backoff_base_seconds: int = 300  # 5 minutes
    backoff_multiplier: float = 2.0
    max_concurrent: int = 5
    retention_days: int = 30  # Default retention period for retry records
    cleanup_on_start: bool = (
        True  # Run cleanup when retry stage starts  # Default retention period for retry records
    )

    def __post_init__(self):
        """Ensure proper types from YAML/env vars."""
        self.enabled = (
            bool(self.enabled) if not isinstance(self.enabled, bool) else self.enabled
        )
        self.batch_size = int(self.batch_size)
        self.min_retry_age_seconds = int(self.min_retry_age_seconds)
        self.max_retries = int(self.max_retries)
        self.backoff_base_seconds = int(self.backoff_base_seconds)
        self.backoff_multiplier = float(self.backoff_multiplier)
        self.max_concurrent = int(self.max_concurrent)
        self.retention_days = int(self.retention_days)
        self.cleanup_on_start = (
            bool(self.cleanup_on_start)
            if not isinstance(self.cleanup_on_start, bool)
            else self.cleanup_on_start
        )


@dataclass
class ScheduleConfig:
    """Scheduling configuration."""

    interval_seconds: int = 60
    enabled_stages: List[str] = field(
        default_factory=lambda: ["ingest", "download", "retry"]
    )


@dataclass
class HealthConfig:
    """Health check endpoint configuration."""

    enabled: bool = True
    # Default to localhost for security - use 0.0.0.0 only when external access needed
    host: str = "127.0.0.1"
    port: int = 8080


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "logs"


@dataclass
class ObservabilityConfig:
    """Observability settings for logging and monitoring."""

    # JSON structured logging
    json_logs: bool = True

    # Log upload to OneLake
    log_upload_enabled: bool = True
    log_upload_interval_cycles: int = 10
    log_retention_days: int = 7

    # Memory checkpoint logging
    memory_checkpoints_enabled: bool = True
    memory_checkpoint_level: str = "DEBUG"  # DEBUG or INFO

    # Memory profiling (production monitoring with ~10% overhead)
    memory_profiling_enabled: bool = True  # Enable tracemalloc monitoring
    memory_snapshot_interval: int = 5  # Minutes between automatic snapshots
    memory_alert_threshold_mb: int = 4096  # Alert if memory usage exceeds 4GB


@dataclass
class TimeoutConfig:
    """Timeout settings for external services and operations.

    All timeouts in seconds. Prevents hanging operations and enables fast-fail behavior.
    """

    # Kusto (Azure Data Explorer) query timeouts
    kusto_query_timeout: int = 300  # 5 minutes for complex analytical queries
    kusto_request_timeout: int = 30  # 30s for API request completion

    # OneLake/ADLS Gen2 operation timeouts
    onelake_connection_timeout: int = 300  # 5 minutes for slow networks
    onelake_request_timeout: int = 300  # 5 minutes for large file uploads
    onelake_read_timeout: int = 300  # 5 minutes for large file downloads

    # HTTP client timeouts
    http_connection_timeout: int = 30  # 30s to establish connections
    http_read_timeout: int = 300  # 5 minutes for response body

    # Delta Lake operations (documentation only - no timeout control)
    delta_query_timeout_note: str = (
        "Delta Lake queries timeout via Polars collect() - no direct control"
    )

    def __post_init__(self):
        """Validate timeout values are reasonable."""
        # Ensure all numeric timeouts are positive
        for field_name, field_value in self.__dict__.items():
            if isinstance(field_value, int) and not field_name.endswith("_note"):
                if field_value <= 0:
                    raise ValueError(
                        f"Timeout {field_name} must be positive, got {field_value}"
                    )
                # Warn if timeout is extremely large (>1 hour)
                if field_value > 3600:
                    import warnings

                    warnings.warn(
                        f"Timeout {field_name} is very large: {field_value}s (>1 hour)"
                    )


@dataclass
class SecurityConfig:
    """Security settings."""

    allowed_download_domains: List[str] = field(
        default_factory=lambda: [
            "usw2-prod-xn-exportreceiver-publish.s3.us-west-2.amazonaws.com"
        ]
    )

    # Temp file encryption (for large file downloads that exceed memory threshold)
    # Encrypts files at rest using Fernet (AES-128-CBC). Disabled by default for dev.
    encrypt_temp_files: bool = False

    # Audit logging (Task H.1)
    audit_logging_enabled: bool = True
    audit_log_path: str = "/var/log/verisk_pipeline/audit.log"

    # Key Vault settings (Task H.2)
    key_vault_url: str = ""
    service_principal_secret_name: str = "service-principal-secret"

    # Azure subscription (Task H.3)
    subscription_id: str = ""
    resource_group: str = ""

    # MSAL settings (Task H.5)
    client_id: str = ""
    tenant_id: str = ""


@dataclass
class PipelineConfig:
    """
    Root configuration for the xact pipeline.

    Loads from YAML file with environment variable overrides.
    """

    kusto: KustoConfig = field(default_factory=KustoConfig)
    lakehouse: LakehouseConfig = field(default_factory=LakehouseConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    retry: RetryStageConfig = field(default_factory=RetryStageConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    security: SecurityConfig = field(default_factory=lambda: SecurityConfig())

    # Runtime metadata
    worker_id: str = "worker-01"
    test_mode: bool = False

    def __post_init__(self):
        # Env overrides for runtime settings
        self.worker_id = os.getenv("WORKER_ID", self.worker_id)
        self.test_mode = os.getenv("TEST_MODE", "").lower() == "true"
        if self.security.allowed_download_domains:
            os.environ.setdefault(
                "allowed_download_domains",
                ",".join(self.security.allowed_download_domains),
            )

    def validate(self) -> List[str]:
        """
        Validate configuration.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Required for ingest stage
        if "ingest" in self.schedule.enabled_stages:
            if not self.kusto.cluster_uri:
                errors.append("kusto.cluster_uri is required for ingest stage")
            if not self.kusto.database:
                errors.append("kusto.database is required for ingest stage")
            if not self.lakehouse.abfss_path:
                errors.append("lakehouse.abfss_path is required for ingest stage")

        # Required for download stage
        if "download" in self.schedule.enabled_stages:
            if not self.lakehouse.abfss_path:
                errors.append("lakehouse.abfss_path is required for download stage")
            if not self.lakehouse.files_path:
                errors.append("lakehouse.files_path is required for download stage")

        # Lower bounds
        if self.processing.batch_size < 1:
            errors.append("processing.batch_size must be >= 1")
        if self.download.max_concurrent < 1:
            errors.append("download.max_concurrent must be >= 1")
        if self.schedule.interval_seconds < 1:
            errors.append("schedule.interval_seconds must be >= 1")
        if self.observability.log_retention_days < 1:
            errors.append("observability.log_retention_days must be >= 1")
        # Upper bounds
        if self.download.max_concurrent > 100:
            errors.append("download.max_concurrent must be <= 100")
        if self.retry.max_concurrent > 100:
            errors.append("retry.max_concurrent must be <= 100")
        if self.processing.batch_size > 10000:
            errors.append("processing.batch_size must be <= 10000")
        if self.processing.max_events_to_scan > 100000:
            errors.append("processing.max_events_to_scan must be <= 100000")
        # Security settings
        if not self.security.allowed_download_domains:
            errors.append(
                "security.allowed_download_domains cannot be empty - "
                "specify allowed domains for attachment downloads"
            )
        else:
            for domain in self.security.allowed_download_domains:
                if not domain or not domain.strip():
                    errors.append(
                        "security.allowed_download_domains contains empty value"
                    )
                elif " " in domain:
                    errors.append(
                        f"security.allowed_download_domains has invalid domain: '{domain}'"
                    )

        return errors

    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return len(self.validate()) == 0


def _dict_to_config(data: Dict[str, Any]) -> PipelineConfig:
    """Convert dict to PipelineConfig with nested dataclasses."""
    return PipelineConfig(
        kusto=KustoConfig(**data.get("kusto", {})),
        lakehouse=LakehouseConfig(**data.get("lakehouse", {})),
        processing=ProcessingConfig(**data.get("processing", {})),
        download=DownloadConfig(**data.get("download", {})),
        retry=RetryStageConfig(**data.get("retry", {})),
        schedule=ScheduleConfig(**data.get("schedule", {})),
        health=HealthConfig(**data.get("health", {})),
        logging=LoggingConfig(**data.get("logging", {})),
        observability=ObservabilityConfig(**data.get("observability", {})),
        worker_id=data.get("worker_id", "worker-01"),
        test_mode=data.get("test_mode", False),
        security=SecurityConfig(**data.get("security", {})),
    )


def load_config(
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> PipelineConfig:
    """
    Load configuration from YAML file with optional overrides.

    Args:
        config_path: Path to YAML config file (default: config.yaml in project root)
        overrides: Dict of overrides to apply after loading

    Returns:
        PipelineConfig instance

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid YAML
    """
    config_path = config_path or DEFAULT_CONFIG_PATH

    # Load base config from YAML
    if config_path.exists():
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    # Apply overrides
    if overrides:
        data = _deep_merge(data, overrides)

    return _dict_to_config(data)


def load_config_from_dict(data: Dict[str, Any]) -> PipelineConfig:
    """
    Load configuration from a dictionary.

    Useful for testing or programmatic config.

    Args:
        data: Configuration dictionary

    Returns:
        PipelineConfig instance
    """
    return _dict_to_config(data)


# Module-level cached config
_config: Optional[PipelineConfig] = None


def get_config() -> PipelineConfig:
    """
    Get or load the singleton config instance.

    Returns:
        Cached PipelineConfig instance
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset cached config (primarily for testing)."""
    global _config
    _config = None


def set_config(config: PipelineConfig) -> None:
    """Set the cached config instance (primarily for testing)."""
    global _config
    _config = config
