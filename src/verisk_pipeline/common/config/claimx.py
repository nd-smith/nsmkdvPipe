"""
ClaimX Pipeline configuration classes.

Provides dataclass-based configuration for the ClaimX event processing pipeline.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from verisk_pipeline.common.config.xact import ObservabilityConfig


@dataclass
class ClaimXKustoConfig:
    """Kusto/Eventhouse configuration for ClaimX events."""

    cluster_uri: str = ""
    database: str = ""
    events_table: str = "tbl_CLAIMXPERIENCE_EVENTS"

    def __post_init__(self):
        self.cluster_uri = os.getenv("CLAIMX_KUSTO_URI", self.cluster_uri)
        self.database = os.getenv("CLAIMX_DATABASE", self.database)


@dataclass
class ClaimXApiConfig:
    """ClaimX REST API configuration."""

    base_url: str = ""
    _auth_token: str = ""
    auth_token_env: str = "CLAIMX_API_TOKEN"  # Env var containing Basic auth token
    timeout_seconds: int = 30
    max_concurrent: int = 20

    def __post_init__(self):
        self.base_url = os.getenv("CLAIMX_API_URL", self.base_url)
        if not self._auth_token:
            self._auth_token = os.getenv(self.auth_token_env, "")

    @property
    def auth_token(self) -> Optional[str]:
        """Get auth token from environment or instance variable."""
        return self._auth_token or os.getenv(self.auth_token_env)


@dataclass
class ClaimXLakehouseConfig:
    """OneLake/Lakehouse configuration for ClaimX."""

    abfss_path: str = ""
    files_path: str = ""

    # Event log (from Kusto)
    events_table: str = "claimx_events"

    # Entity tables (from API enrichment)
    projects_table: str = "claimx_projects"
    contacts_table: str = "claimx_contacts"
    media_metadata_table: str = "claimx_attachment_metadata"  # API metadata source
    tasks_table: str = "claimx_tasks"
    task_templates_table: str = "claimx_task_templates"
    external_links_table: str = "claimx_external_links"
    video_collab_table: str = "claimx_video_collab"

    # Processing tracking
    event_log_table: str = "claimx_event_log"
    attachments_table: str = "claimx_attachments"  # Success-only inventory
    retry_table: str = "claimx_retry"  # Active retry queue

    def __post_init__(self):
        self.abfss_path = os.getenv("CLAIMX_LAKEHOUSE_PATH", self.abfss_path)
        self.files_path = os.getenv("CLAIMX_FILES_PATH", self.files_path)

        if self.abfss_path and not self.files_path:
            self.files_path = f"{self.abfss_path}/Files"

    def table_path(self, table_name: str) -> str:
        """Get full path for a table."""
        return f"{self.abfss_path}/{table_name}"

    @property
    def attachments_table_path(self) -> str:
        """Full path to attachments inventory Delta table (success only)."""
        return self.table_path(self.attachments_table)

    @property
    def retry_table_path(self) -> str:
        """Full path to retry queue Delta table."""
        return self.table_path(self.retry_table)


@dataclass
class ClaimXProcessingConfig:
    """Event processing configuration."""

    event_types: List[str] = field(
        default_factory=lambda: [
            "PROJECT_CREATED",
            "PROJECT_FILE_ADDED",
            "PROJECT_MFN_ADDED",
            "CUSTOM_TASK_ASSIGNED",
            "CUSTOM_TASK_COMPLETED",
            "POLICYHOLDER_INVITED",
            "POLICYHOLDER_JOINED",
            "VIDEO_COLLABORATION_INVITE_SENT",
            "VIDEO_COLLABORATION_COMPLETED",
        ]
    )
    batch_size: int = 100
    max_events_to_scan: int = 5000
    lookback_days: int = 7
    start_date: str = "2024-01-01T00:00:00"
    skip_dedup_check: bool = False  # Skip dedup check when enriching events


@dataclass
class ClaimXDownloadConfig:
    """Media download configuration."""

    max_concurrent: int = 10
    timeout_seconds: int = 60
    proxy: Optional[str] = None


@dataclass
class ClaimXRetryConfig:
    """Retry stage configuration."""

    enabled: bool = True
    batch_size: int = 50
    min_retry_age_seconds: int = 300  # 5 minutes
    max_retries: int = 4
    backoff_base_seconds: int = 300
    backoff_multiplier: float = 2.0
    max_concurrent: int = 5
    retention_days: int = 30  # Default retention period for retry records
    cleanup_on_start: bool = (
        True  # Run cleanup when retry stage starts  # Default retention period for retry records
    )


@dataclass
class ClaimXScheduleConfig:
    """Scheduling configuration."""

    interval_seconds: int = 60
    enabled_stages: List[str] = field(
        default_factory=lambda: ["ingest", "enrich", "download", "retry"]
    )


@dataclass
class ClaimXSecurityConfig:
    """Security settings for ClaimX."""

    # Allowed domains for media download URLs (S3 presigned URLs from API)
    allowed_download_domains: List[str] = field(
        default_factory=lambda: [
            "claimxperience.s3.amazonaws.com",
            "claimxperience.s3.us-east-1.amazonaws.com",
            "www.claimxperience.com",
            "claimxperience.com",
            "xactware-claimx-us-prod.s3.us-west-1.amazonaws.com",
            "www.claimxperience.com",
        ]
    )

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
class ClaimXConfig:
    """
    Root configuration for the ClaimX pipeline.

    Loads from YAML with environment variable overrides.
    """

    kusto: ClaimXKustoConfig = field(default_factory=ClaimXKustoConfig)
    api: ClaimXApiConfig = field(default_factory=ClaimXApiConfig)
    lakehouse: ClaimXLakehouseConfig = field(default_factory=ClaimXLakehouseConfig)
    processing: ClaimXProcessingConfig = field(default_factory=ClaimXProcessingConfig)
    download: ClaimXDownloadConfig = field(default_factory=ClaimXDownloadConfig)
    retry: ClaimXRetryConfig = field(default_factory=ClaimXRetryConfig)
    schedule: ClaimXScheduleConfig = field(default_factory=ClaimXScheduleConfig)
    security: ClaimXSecurityConfig = field(default_factory=ClaimXSecurityConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    # Runtime metadata
    worker_id: str = "worker-01"
    test_mode: bool = False

    def __post_init__(self):
        self.worker_id = os.getenv("WORKER_ID", self.worker_id)
        self.test_mode = os.getenv("TEST_MODE", "").lower() == "true"

    def validate(self) -> List[str]:
        """Validate configuration."""
        errors = []

        # Ingest stage requirements
        if "ingest" in self.schedule.enabled_stages:
            if not self.kusto.cluster_uri:
                errors.append("kusto.cluster_uri is required for ingest stage")
            if not self.kusto.database:
                errors.append("kusto.database is required for ingest stage")
            if not self.lakehouse.abfss_path:
                errors.append("lakehouse.abfss_path is required for ingest stage")

        # Enrich stage requirements
        if "enrich" in self.schedule.enabled_stages:
            if not self.api.base_url:
                errors.append("api.base_url is required for enrich stage")
            if not self.api.auth_token:
                errors.append(
                    f"Environment variable {self.api.auth_token_env} is required for enrich stage"
                )
            if not self.lakehouse.abfss_path:
                errors.append("lakehouse.abfss_path is required for enrich stage")

        # Download stage requirements
        if "download" in self.schedule.enabled_stages:
            if not self.lakehouse.abfss_path:
                errors.append("lakehouse.abfss_path is required for download stage")
            if not self.lakehouse.files_path:
                errors.append("lakehouse.files_path is required for download stage")

        # Bounds checking
        if self.processing.batch_size < 1:
            errors.append("processing.batch_size must be >= 1")
        if self.processing.batch_size > 10000:
            errors.append("processing.batch_size must be <= 10000")
        if self.download.max_concurrent < 1:
            errors.append("download.max_concurrent must be >= 1")
        if self.download.max_concurrent > 100:
            errors.append("download.max_concurrent must be <= 100")
        if self.api.max_concurrent < 1:
            errors.append("api.max_concurrent must be >= 1")
        if self.api.max_concurrent > 100:
            errors.append("api.max_concurrent must be <= 100")
        if self.schedule.interval_seconds < 1:
            errors.append("schedule.interval_seconds must be >= 1")

        # Security
        if not self.security.allowed_download_domains:
            errors.append(
                "security.allowed_download_domains cannot be empty - "
                "specify allowed S3 domains for media downloads"
            )

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0


def load_claimx_config_from_dict(data: Dict[str, Any]) -> ClaimXConfig:
    """
    Build ClaimXConfig from a dictionary.

    Args:
        data: Dictionary with claimx config values

    Returns:
        ClaimXConfig instance
    """
    return ClaimXConfig(
        kusto=ClaimXKustoConfig(**data.get("kusto", {})),
        api=ClaimXApiConfig(**data.get("api", {})),
        lakehouse=ClaimXLakehouseConfig(**data.get("lakehouse", {})),
        processing=ClaimXProcessingConfig(**data.get("processing", {})),
        download=ClaimXDownloadConfig(**data.get("download", {})),
        retry=ClaimXRetryConfig(**data.get("retry", {})),
        schedule=ClaimXScheduleConfig(**data.get("schedule", {})),
        security=ClaimXSecurityConfig(**data.get("security", {})),
        observability=ObservabilityConfig(**data.get("observability", {})),
        worker_id=data.get("worker_id", "worker-01"),
        test_mode=data.get("test_mode", False),
    )
