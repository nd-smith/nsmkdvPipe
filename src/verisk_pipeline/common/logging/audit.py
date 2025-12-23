"""
Audit logging for security-sensitive operations (Task H.1).

Provides structured audit trail for:
- Authentication events (login, token refresh, failures)
- Authorization checks (RBAC validation)
- Credential operations (rotation, expiration warnings)
- Security configuration changes

All audit logs are written to a dedicated audit log file with structured JSON format
for compliance and security monitoring.
"""

import json
import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

from verisk_pipeline.common.config.xact import get_config


class AuditEventType(Enum):
    """Types of auditable security events."""

    # Authentication events
    AUTH_TOKEN_ACQUIRED = "auth.token.acquired"
    AUTH_TOKEN_REFRESH = "auth.token.refresh"
    AUTH_TOKEN_EXPIRED = "auth.token.expired"
    AUTH_LOGIN_SUCCESS = "auth.login.success"
    AUTH_LOGIN_FAILURE = "auth.login.failure"
    AUTH_LOGOUT = "auth.logout"
    AUTH_CACHE_CLEARED = "auth.cache.cleared"

    # Authorization events
    AUTHZ_RBAC_CHECK = "authz.rbac.check"
    AUTHZ_RBAC_DENIED = "authz.rbac.denied"
    AUTHZ_PERMISSION_GRANTED = "authz.permission.granted"
    AUTHZ_PERMISSION_DENIED = "authz.permission.denied"

    # Credential events
    CRED_ROTATION_SUCCESS = "cred.rotation.success"
    CRED_ROTATION_FAILURE = "cred.rotation.failure"
    CRED_EXPIRATION_WARNING = "cred.expiration.warning"
    CRED_EXPIRED = "cred.expired"

    # Configuration events
    CONFIG_SECURITY_CHANGED = "config.security.changed"
    CONFIG_AUDIT_ENABLED = "config.audit.enabled"
    CONFIG_AUDIT_DISABLED = "config.audit.disabled"


class AuditLogger:
    """
    Audit logger for security-sensitive operations.

    Writes structured JSON audit logs to a dedicated audit log file.
    Audit logs are separate from application logs for security/compliance.

    Usage:
        audit = AuditLogger()
        audit.log_auth_event(
            event_type=AuditEventType.AUTH_TOKEN_ACQUIRED,
            auth_mode="cli",
            resource="https://storage.azure.com/",
            success=True
        )
    """

    _instance: Optional["AuditLogger"] = None
    _lock = None

    def __new__(cls):
        """Singleton pattern for centralized audit logging."""
        if cls._instance is None:
            from threading import Lock

            if cls._lock is None:
                cls._lock = Lock()
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize audit logger with configuration."""
        if self._initialized:
            return

        config = get_config()
        self.enabled = config.security.audit_logging_enabled
        self.audit_log_path = config.security.audit_log_path

        # Create audit log directory if needed
        if self.enabled:
            log_dir = Path(self.audit_log_path).parent
            log_dir.mkdir(parents=True, exist_ok=True)

        # Set up Python logger for audit events
        self._logger = logging.getLogger("verisk_pipeline.audit")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False  # Don't propagate to root logger

        # Add file handler if enabled
        if self.enabled:
            handler = logging.FileHandler(self.audit_log_path)
            handler.setLevel(logging.INFO)
            handler.setFormatter(
                logging.Formatter("%(message)s")
            )  # JSON only, no formatting
            self._logger.addHandler(handler)

        self._initialized = True

    def _create_audit_record(
        self, event_type: AuditEventType, success: bool, **kwargs: Any
    ) -> Dict[str, Any]:
        """Create structured audit record."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type.value,
            "success": success,
            "component": "verisk_pipeline",
        }

        # Add all additional context
        for key, value in kwargs.items():
            if value is not None:
                record[key] = value

        return record

    def _log_record(self, record: Dict[str, Any]) -> None:
        """Write audit record to log file."""
        if not self.enabled:
            return

        # Write as single-line JSON for easy parsing
        self._logger.info(json.dumps(record))

    def log_auth_event(
        self,
        event_type: AuditEventType,
        auth_mode: str,
        success: bool,
        resource: Optional[str] = None,
        user_id: Optional[str] = None,
        error_message: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Log authentication event.

        Args:
            event_type: Type of auth event
            auth_mode: Authentication mode (cli, spn, file)
            success: Whether operation succeeded
            resource: Resource being accessed (optional)
            user_id: User identifier (optional)
            error_message: Error message if failed (optional)
            **kwargs: Additional context
        """
        record = self._create_audit_record(
            event_type=event_type,
            success=success,
            auth_mode=auth_mode,
            resource=resource,
            user_id=user_id,
            error_message=error_message,
            **kwargs
        )
        self._log_record(record)

    def log_authz_event(
        self,
        event_type: AuditEventType,
        permission: str,
        success: bool,
        resource: Optional[str] = None,
        user_id: Optional[str] = None,
        reason: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Log authorization (RBAC) event.

        Args:
            event_type: Type of authz event
            permission: Permission being checked
            success: Whether check passed
            resource: Resource being accessed (optional)
            user_id: User identifier (optional)
            reason: Reason for denial if failed (optional)
            **kwargs: Additional context
        """
        record = self._create_audit_record(
            event_type=event_type,
            success=success,
            permission=permission,
            resource=resource,
            user_id=user_id,
            reason=reason,
            **kwargs
        )
        self._log_record(record)

    def log_credential_event(
        self,
        event_type: AuditEventType,
        credential_type: str,
        success: bool,
        expires_at: Optional[str] = None,
        error_message: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Log credential management event.

        Args:
            event_type: Type of credential event
            credential_type: Type of credential (service_principal, token, etc.)
            success: Whether operation succeeded
            expires_at: Expiration timestamp (optional)
            error_message: Error message if failed (optional)
            **kwargs: Additional context
        """
        record = self._create_audit_record(
            event_type=event_type,
            success=success,
            credential_type=credential_type,
            expires_at=expires_at,
            error_message=error_message,
            **kwargs
        )
        self._log_record(record)

    def log_config_event(
        self,
        event_type: AuditEventType,
        config_key: str,
        old_value: Optional[Any] = None,
        new_value: Optional[Any] = None,
        **kwargs: Any
    ) -> None:
        """
        Log security configuration change.

        Args:
            event_type: Type of config event
            config_key: Configuration key changed
            old_value: Previous value (optional)
            new_value: New value (optional)
            **kwargs: Additional context
        """
        record = self._create_audit_record(
            event_type=event_type,
            success=True,
            config_key=config_key,
            old_value=str(old_value) if old_value is not None else None,
            new_value=str(new_value) if new_value is not None else None,
            **kwargs
        )
        self._log_record(record)


# Module-level singleton accessor
def get_audit_logger() -> AuditLogger:
    """Get the singleton audit logger instance."""
    return AuditLogger()
