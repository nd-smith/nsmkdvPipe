"""
Root configuration for multi-domain pipeline.

Requires nested config format with domain keys (xact, claimx).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from verisk_pipeline.common.config.base import DEFAULT_CONFIG_PATH
from verisk_pipeline.common.config.xact import (
    HealthConfig,
    LoggingConfig,
    ObservabilityConfig,
    PipelineConfig as XactPipelineConfig,
    load_config_from_dict,
)


@dataclass
class RootConfig:
    """
    Root configuration for multi-domain pipeline.

    Supports running xact, claimx, or both pipelines concurrently.
    """

    # Domain configs (None = disabled)
    xact: Optional[XactPipelineConfig] = None
    # Future: claimx: Optional[ClaimXConfig] = None

    # Shared configs
    health: HealthConfig = field(default_factory=HealthConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    # Runtime metadata
    worker_id: str = "worker-01"
    test_mode: bool = False

    def __post_init__(self):
        self.worker_id = os.getenv("WORKER_ID", self.worker_id)
        self.test_mode = os.getenv("TEST_MODE", "").lower() == "true"

    @property
    def enabled_domains(self) -> List[str]:
        """List of enabled domain names."""
        domains = []
        if self.xact is not None:
            domains.append("xact")
        # Future: if self.claimx is not None: domains.append("claimx")
        return domains

    def validate(self) -> List[str]:
        """Validate configuration."""
        errors = []

        if not self.enabled_domains:
            errors.append("No pipeline domains enabled (need 'xact:' key in config)")

        if self.xact is not None:
            xact_errors = self.xact.validate()
            errors.extend([f"xact: {e}" for e in xact_errors])

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge overlay into base dict."""
    result = base.copy()
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_root_config(
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> RootConfig:
    """
    Load root configuration from YAML.

    Expects nested format with domain keys:
        xact:
          kusto: ...
          lakehouse: ...
        health: ...

    Args:
        config_path: Path to YAML config file
        overrides: Dict of overrides to apply

    Returns:
        RootConfig instance

    Raises:
        ValueError: If config missing required domain key
    """
    config_path = config_path or DEFAULT_CONFIG_PATH

    # Load YAML
    if config_path.exists():
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    # Apply overrides
    if overrides:
        data = _deep_merge(data, overrides)

    # Check for nested format
    has_domain_key = "xact" in data or "claimx" in data

    if not has_domain_key:
        raise ValueError(
            "Config must have 'xact:' or 'claimx:' top-level key. "
            "Migrate config.yaml by wrapping existing content under 'xact:' key."
        )

    # Build xact config if present
    xact_config = None
    if "xact" in data and data["xact"] is not None:
        xact_data = data["xact"].copy()
        # Inherit shared settings if not specified in domain
        for key in ["health", "logging", "observability", "security"]:
            if key not in xact_data and key in data:
                xact_data[key] = data[key]
        xact_config = load_config_from_dict(xact_data)

    return RootConfig(
        xact=xact_config,
        health=HealthConfig(**data.get("health", {})),
        logging=LoggingConfig(**data.get("logging", {})),
        observability=ObservabilityConfig(**data.get("observability", {})),
        worker_id=data.get("worker_id", "worker-01"),
        test_mode=data.get("test_mode", False),
    )


# Module-level cached config
_root_config: Optional[RootConfig] = None


def get_root_config() -> RootConfig:
    """Get or load the singleton root config instance."""
    global _root_config
    if _root_config is None:
        _root_config = load_root_config()
    return _root_config


def reset_root_config() -> None:
    """Reset cached config (primarily for testing)."""
    global _root_config
    _root_config = None


def set_root_config(config: RootConfig) -> None:
    """Set the cached config instance (primarily for testing)."""
    global _root_config
    _root_config = config
