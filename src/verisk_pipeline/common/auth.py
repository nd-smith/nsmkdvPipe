"""
Unified Azure authentication with token caching.
Supports Azure CLI (interactive) and Service Principal.
"""

import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
import shutil

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context
from verisk_pipeline.common.logging.audit import get_audit_logger, AuditEventType

logger = get_logger(__name__)
audit = get_audit_logger()

# Token timing constants
TOKEN_REFRESH_MINS = 50  # Refresh before expiry
TOKEN_EXPIRY_MINS = 60  # Azure token lifetime


@dataclass
class CachedToken:
    """Token with acquisition timestamp."""

    value: str
    acquired_at: datetime

    def is_valid(self, buffer_mins: int = TOKEN_REFRESH_MINS) -> bool:
        """Check if token is still valid with buffer."""
        age = datetime.now(timezone.utc) - self.acquired_at
        return age < timedelta(minutes=buffer_mins)


class TokenCache:
    """Token cache for multiple resources."""

    def __init__(self):
        self._tokens: Dict[str, CachedToken] = {}

    def get(self, resource: str) -> Optional[str]:
        """Get cached token if still valid."""
        cached = self._tokens.get(resource)
        if cached and cached.is_valid():
            return cached.value
        return None

    def set(self, resource: str, token: str) -> None:
        """Cache a token."""
        self._tokens[resource] = CachedToken(
            value=token, acquired_at=datetime.now(timezone.utc)
        )

    def clear(self, resource: Optional[str] = None) -> None:
        """Clear one or all cached tokens."""
        if resource:
            self._tokens.pop(resource, None)
            log_with_context(
                logger, logging.DEBUG, "Cleared token cache", resource=resource
            )
        else:
            self._tokens.clear()
            log_with_context(logger, logging.DEBUG, "Cleared all token caches")

    def get_age(self, resource: str) -> Optional[timedelta]:
        """Get age of cached token (for diagnostics)."""
        cached = self._tokens.get(resource)
        if cached:
            return datetime.now(timezone.utc) - cached.acquired_at
        return None


class AzureAuthError(Exception):
    """Raised when Azure authentication fails."""

    pass


class AzureAuth:
    """
    Azure authentication provider.

    Supports three modes:
    - CLI: Uses `az account get-access-token` (set AZURE_AUTH_INTERACTIVE=true)
    - SPN: Uses client credentials (set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    - File: Reads token from file (set AZURE_TOKEN_FILE=/path/to/token.txt)
    """

    STORAGE_RESOURCE = "https://storage.azure.com/"

    def __init__(self, cache: Optional[TokenCache] = None):
        self._cache = cache or TokenCache()
        self._load_config()

    def _load_config(self) -> None:
        """Load auth configuration from environment."""
        self.use_cli = os.getenv("AZURE_AUTH_INTERACTIVE", "").lower() == "true"
        self.token_file = os.getenv("AZURE_TOKEN_FILE")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")

    @property
    def has_spn_credentials(self) -> bool:
        """Check if SPN credentials are fully configured."""
        return all([self.client_id, self.client_secret, self.tenant_id])

    @property
    def auth_mode(self) -> str:
        """Return current auth mode for diagnostics."""
        if self.token_file:
            return "file"
        if self.use_cli:
            return "cli"
        if self.has_spn_credentials:
            return "spn"
        return "none"

    def _read_token_file(self, resource: Optional[str] = None) -> str:
        """Read token from file.

        Supports two formats:
        1. JSON file with resource-keyed tokens: {"https://storage.azure.com/": "token", ...}
        2. Plain text file with single token (legacy)

        Args:
            resource: Optional resource URL to look up in JSON format.
                    If None, uses STORAGE_RESOURCE for storage operations.
        """
        if not self.token_file:
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                auth_mode="file",
                success=False,
                error_message="AZURE_TOKEN_FILE not set",
            )
            raise AzureAuthError("AZURE_TOKEN_FILE not set")

        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                content = f.read().strip()

            if not content:
                audit.log_auth_event(
                    event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                    auth_mode="file",
                    success=False,
                    error_message="Token file is empty",
                    token_file=self.token_file,
                )
                raise AzureAuthError(f"Token file is empty: {self.token_file}")

            # Try to parse as JSON (multi-resource token file from token_refresher)
            try:
                import json

                tokens = json.loads(content)
                if isinstance(tokens, dict):
                    # JSON format: {"resource_url": "token", ...}
                    lookup_resource = resource or self.STORAGE_RESOURCE

                    # Try exact match first
                    if lookup_resource in tokens:
                        token = tokens[lookup_resource]
                        log_with_context(
                            logger,
                            logging.DEBUG,
                            "Read token from JSON file",
                            token_file=self.token_file,
                            resource=lookup_resource,
                        )
                        audit.log_auth_event(
                            event_type=AuditEventType.AUTH_TOKEN_ACQUIRED,
                            auth_mode="file",
                            success=True,
                            token_file=self.token_file,
                            resource=lookup_resource,
                        )
                        return token

                    # For Kusto clusters, check if cluster URI is in tokens
                    # (token_refresher may have been run with --kusto-cluster)
                    for key in tokens:
                        if lookup_resource.rstrip("/") == key.rstrip("/"):
                            token = tokens[key]
                            log_with_context(
                                logger,
                                logging.DEBUG,
                                "Read token from JSON file (normalized match)",
                                token_file=self.token_file,
                                resource=lookup_resource,
                                matched_key=key,
                            )
                            audit.log_auth_event(
                                event_type=AuditEventType.AUTH_TOKEN_ACQUIRED,
                                auth_mode="file",
                                success=True,
                                token_file=self.token_file,
                                resource=lookup_resource,
                            )
                            return token

                    # Resource not found in JSON
                    available_resources = list(tokens.keys())
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "Resource not found in token file",
                        token_file=self.token_file,
                        requested_resource=lookup_resource,
                        available_resources=available_resources,
                    )
                    audit.log_auth_event(
                        event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                        auth_mode="file",
                        success=False,
                        error_message=f"Resource not found in token file: {lookup_resource}",
                        token_file=self.token_file,
                    )
                    raise AzureAuthError(
                        f"Resource '{lookup_resource}' not found in token file. "
                        f"Available: {available_resources}"
                    )
            except json.JSONDecodeError:
                # Not JSON - treat as plain text token (legacy format)
                pass

            # Plain text format (legacy): entire file content is the token
            log_with_context(
                logger,
                logging.DEBUG,
                "Read token from plain text file",
                token_file=self.token_file,
            )
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_TOKEN_ACQUIRED,
                auth_mode="file",
                success=True,
                token_file=self.token_file,
            )
            return content

        except FileNotFoundError as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Token file not found",
                token_file=self.token_file,
            )
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                auth_mode="file",
                success=False,
                error_message="Token file not found",
                token_file=self.token_file,
            )
            raise AzureAuthError(f"Token file not found: {self.token_file}") from e
        except IOError as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Failed to read token file",
                token_file=self.token_file,
                error_message=str(e)[:200],
            )
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                auth_mode="file",
                success=False,
                error_message=str(e)[:200],
                token_file=self.token_file,
            )
            raise AzureAuthError(f"Failed to read token file: {self.token_file}") from e

    def _fetch_cli_token(self, resource: str) -> str:
        """Fetch fresh token from Azure CLI with timeout retry."""
        az_path = shutil.which("az")
        if not az_path:
            raise AzureAuthError(
                "Azure CLI not found. Ensure 'az' is installed and in PATH."
            )

        cmd = [az_path, "account", "get-access-token", "--resource", resource]
        if self.tenant_id:
            cmd.extend(["--tenant", self.tenant_id])
        cmd.extend(["--query", "accessToken", "-o", "tsv"])

        max_attempts = 2
        last_error = None

        for attempt in range(max_attempts):
            proc = None
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, stderr = proc.communicate(timeout=60)

                class _Result:
                    pass

                result = _Result()
                result.returncode = proc.returncode
                result.stdout = stdout
                result.stderr = stderr
                break

            except subprocess.TimeoutExpired:
                last_error = f"Azure CLI token request timed out for {resource}"
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Azure CLI token request timed out",
                    resource=resource,
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                )
                if proc:
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                if attempt < max_attempts - 1:
                    import time

                    time.sleep(5)
                continue
        else:
            raise AzureAuthError(last_error)

        if result.returncode != 0:
            stderr = result.stderr.strip()
            if "az login" in stderr.lower() or "please run" in stderr.lower():
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Azure CLI session expired",
                    resource=resource,
                )
                audit.log_auth_event(
                    event_type=AuditEventType.AUTH_TOKEN_EXPIRED,
                    auth_mode="cli",
                    success=False,
                    resource=resource,
                    error_message="Azure CLI session expired",
                )
                raise AzureAuthError(
                    f"Azure CLI session expired. Run 'az login' to re-authenticate.\nDetails: {stderr}"
                )
            log_with_context(
                logger,
                logging.ERROR,
                "Azure CLI token fetch failed",
                resource=resource,
                error_message=stderr[:200],
            )
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                auth_mode="cli",
                success=False,
                resource=resource,
                error_message=stderr[:200],
            )
            raise AzureAuthError(f"Azure CLI token fetch failed: {stderr}")

        token = result.stdout.strip()
        if not token:
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                auth_mode="cli",
                success=False,
                resource=resource,
                error_message="Azure CLI returned empty token",
            )
            raise AzureAuthError("Azure CLI returned empty token")

        audit.log_auth_event(
            event_type=AuditEventType.AUTH_TOKEN_ACQUIRED,
            auth_mode="cli",
            success=True,
            resource=resource,
        )
        return token

    def get_cli_token(self, resource: str, force_refresh: bool = False) -> str:
        """Get token from Azure CLI (with caching)."""
        if not force_refresh:
            cached = self._cache.get(resource)
            if cached:
                log_with_context(
                    logger, logging.DEBUG, "Using cached CLI token", resource=resource
                )
                return cached

        log_with_context(
            logger, logging.DEBUG, "Fetching fresh CLI token", resource=resource
        )
        # Audit token refresh if force_refresh was requested
        if force_refresh:
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_TOKEN_REFRESH,
                auth_mode="cli",
                success=True,
                resource=resource,
            )
        token = self._fetch_cli_token(resource)
        self._cache.set(resource, token)
        return token

    def get_storage_token(self, force_refresh: bool = False) -> Optional[str]:
        """Get token for OneLake/ADLS storage operations."""
        if self.token_file:
            try:
                return self._read_token_file(resource=self.STORAGE_RESOURCE)
            except AzureAuthError as e:
                # Log warning but don't fail - return None to trigger fallback
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Token file configured but unavailable",
                    token_file=self.token_file,
                    error=str(e),
                )
                return None
        if self.use_cli:
            return self.get_cli_token(self.STORAGE_RESOURCE, force_refresh)
        return None

    def get_storage_options(self, force_refresh: bool = False) -> Dict[str, str]:
        """Get delta-rs / object_store compatible auth options."""
        if self.token_file:
            try:
                token = self._read_token_file(resource=self.STORAGE_RESOURCE)
                return {"azure_storage_token": token}
            except AzureAuthError as e:
                # Log warning but don't fail - let auth fail at access time
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Token file configured but unavailable - auth will fail at access time",
                    token_file=self.token_file,
                    error=str(e),
                )
                return {}

        if self.use_cli:
            token = self.get_storage_token(force_refresh)
            if token:
                return {"azure_storage_token": token}
            return {}

        if self.has_spn_credentials:
            return {
                "azure_client_id": self.client_id,
                "azure_client_secret": self.client_secret,
                "azure_tenant_id": self.tenant_id,
            }

        log_with_context(
            logger,
            logging.ERROR,
            "No Azure credentials configured - authentication will fail",
            resource=self.STORAGE_RESOURCE,
        )
        return {}

    def get_kusto_token(self, cluster_uri: str, force_refresh: bool = False) -> str:
        """Get token for Kusto/Eventhouse."""
        if self.token_file:
            try:
                return self._read_token_file(resource=cluster_uri)
            except AzureAuthError as e:
                # Log warning and try to fall back to CLI if available
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Kusto token file unavailable, attempting CLI fallback",
                    token_file=self.token_file,
                    error=str(e),
                    has_cli_fallback=self.use_cli,
                )
                # If CLI is not available, re-raise the error
                if not self.use_cli:
                    raise AzureAuthError(
                        f"Kusto token file unavailable and no CLI fallback: {self.token_file}"
                    ) from e
                # Fall through to CLI auth below

        if not self.use_cli:
            raise AzureAuthError(
                "Kusto CLI token requested but AZURE_AUTH_INTERACTIVE not set. "
                "For SPN auth, use KustoConnectionStringBuilder directly."
            )
        return self.get_cli_token(cluster_uri, force_refresh)

    def clear_cache(self, resource: Optional[str] = None) -> None:
        """Clear cached tokens."""
        self._cache.clear(resource)
        audit.log_auth_event(
            event_type=AuditEventType.AUTH_CACHE_CLEARED,
            auth_mode=self.auth_mode,
            success=True,
            resource=resource if resource else "all",
        )

    def check_credential_expiry(self, warn_days: int = 30) -> Optional[Dict]:
        """
        Check credential expiration and warn if approaching (Task H.2).

        Checks service principal secret expiry from Key Vault metadata.
        Logs audit events for expiration warnings.

        Args:
            warn_days: Number of days before expiry to start warning

        Returns:
            Dict with expiry info if credentials found, None otherwise
        """
        # Only check for SPN mode with Key Vault configuration
        config_available = False
        key_vault_url = None
        secret_name = None

        try:
            from verisk_pipeline.common.config.xact import get_config

            config = get_config()
            key_vault_url = config.security.key_vault_url
            secret_name = config.security.service_principal_secret_name
            config_available = bool(key_vault_url and secret_name)
        except Exception:
            pass

        if not config_available or not self.has_spn_credentials:
            return None

        try:
            # Extract vault name from URL
            from urllib.parse import urlparse

            vault_name = urlparse(key_vault_url).netloc.split(".")[0]

            # Get secret expiry from Key Vault using Azure CLI
            import subprocess

            result = subprocess.run(
                [
                    "az",
                    "keyvault",
                    "secret",
                    "show",
                    "--vault-name",
                    vault_name,
                    "--name",
                    secret_name,
                    "--query",
                    "attributes.expires",
                    "-o",
                    "tsv",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode != 0 or not result.stdout.strip():
                return None

            # Parse expiry timestamp
            expiry_str = result.stdout.strip()
            expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))

            # Calculate days until expiry
            days_until_expiry = (expiry_dt - datetime.now(timezone.utc)).days

            expiry_info = {
                "expires_at": expiry_str,
                "days_until_expiry": days_until_expiry,
                "expired": days_until_expiry < 0,
            }

            # Log warnings based on days until expiry
            if days_until_expiry < 0:
                log_with_context(
                    logger,
                    logging.CRITICAL,
                    "Service principal credentials EXPIRED",
                    **expiry_info,
                )
                audit.log_credential_event(
                    event_type=AuditEventType.CRED_EXPIRED,
                    credential_type="service_principal",
                    success=False,
                    expires_at=expiry_str,
                )
            elif days_until_expiry <= 7:
                log_with_context(
                    logger,
                    logging.CRITICAL,
                    "Service principal credentials expire in 7 days or less",
                    **expiry_info,
                )
                audit.log_credential_event(
                    event_type=AuditEventType.CRED_EXPIRATION_WARNING,
                    credential_type="service_principal",
                    success=True,
                    expires_at=expiry_str,
                    days_remaining=days_until_expiry,
                )
            elif days_until_expiry <= 14:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Service principal credentials expire in 14 days or less",
                    **expiry_info,
                )
                audit.log_credential_event(
                    event_type=AuditEventType.CRED_EXPIRATION_WARNING,
                    credential_type="service_principal",
                    success=True,
                    expires_at=expiry_str,
                    days_remaining=days_until_expiry,
                )
            elif days_until_expiry <= warn_days:
                log_with_context(
                    logger,
                    logging.INFO,
                    f"Service principal credentials expire in {days_until_expiry} days",
                    **expiry_info,
                )
                audit.log_credential_event(
                    event_type=AuditEventType.CRED_EXPIRATION_WARNING,
                    credential_type="service_principal",
                    success=True,
                    expires_at=expiry_str,
                    days_remaining=days_until_expiry,
                )

            return expiry_info

        except Exception as e:
            log_with_context(
                logger,
                logging.DEBUG,
                "Could not check credential expiry",
                error=str(e)[:200],
            )
            return None

    def get_diagnostics(self) -> Dict:
        """Get auth state for health checks / debugging."""
        diag = {
            "auth_mode": self.auth_mode,
            "cli_enabled": self.use_cli,
            "spn_configured": self.has_spn_credentials,
            "token_file": self.token_file,
        }
        storage_age = self._cache.get_age(self.STORAGE_RESOURCE)
        if storage_age:
            diag["storage_token_age_seconds"] = storage_age.total_seconds()

        # Add credential expiry info (Task H.2)
        expiry_info = self.check_credential_expiry()
        if expiry_info:
            diag["credential_expiry"] = expiry_info

        return diag


class ProactiveTokenRefresher:
    """
    Background thread for proactive token refresh (P2.13).

    Refreshes tokens at 45-minute mark (before 60-minute expiration)
    to prevent mid-operation auth failures.
    """

    def __init__(
        self,
        auth: AzureAuth,
        resource: str,
        refresh_interval_minutes: int = 45,
    ):
        """
        Args:
            auth: AzureAuth instance to refresh tokens for
            resource: Azure resource to refresh tokens for
            refresh_interval_minutes: Minutes between refreshes (default: 45)
        """
        self.auth = auth
        self.resource = resource
        self.refresh_interval = refresh_interval_minutes * 60  # Convert to seconds
        self._stop_event = None
        self._thread = None
        self._running = False

    def start(self) -> None:
        """Start background token refresh thread."""
        if self._running:
            return

        import threading

        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()
        self._running = True

        log_with_context(
            logger,
            logging.INFO,
            "Started proactive token refresher",
            resource=self.resource,
            refresh_interval_minutes=self.refresh_interval / 60,
        )

    def stop(self) -> None:
        """Stop background token refresh thread."""
        if not self._running:
            return

        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._running = False

        log_with_context(
            logger,
            logging.INFO,
            "Stopped proactive token refresher",
            resource=self.resource,
        )

    def _refresh_loop(self) -> None:
        """Background loop for periodic token refresh."""
        while not self._stop_event.wait(self.refresh_interval):
            try:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Proactive token refresh triggered",
                    resource=self.resource,
                )

                # Force refresh the token
                if self.auth.use_cli:
                    self.auth.get_cli_token(self.resource, force_refresh=True)
                else:
                    # For SPN, tokens are managed by azure-identity library
                    # Just clear cache to force next access to get fresh token
                    self.auth.clear_cache(self.resource)

                log_with_context(
                    logger,
                    logging.INFO,
                    "Proactive token refresh completed",
                    resource=self.resource,
                )

            except Exception as e:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Proactive token refresh failed",
                    resource=self.resource,
                    error=str(e)[:200],
                )


def _get_spn_credential(
    tenant_id: str,
    client_id: str,
    client_secret: Optional[str] = None,
    certificate_path: Optional[str] = None,
):
    """
    Get Azure service principal credential with certificate support (P2.14).

    Supports both client secret and certificate-based authentication.
    Certificate auth provides 1-2 year validity vs 90-day client secret expiry.

    Args:
        tenant_id: Azure tenant ID
        client_id: Service principal client ID
        client_secret: Optional client secret (for secret-based auth)
        certificate_path: Optional path to certificate file (for cert-based auth)

    Returns:
        Azure credential object (ClientSecretCredential or CertificateCredential)

    Raises:
        ValueError: If neither client_secret nor certificate_path provided
    """
    from azure.identity import ClientSecretCredential, CertificateCredential

    if certificate_path:
        # Certificate-based authentication (P2.14)
        log_with_context(
            logger,
            logging.INFO,
            "Using certificate-based authentication",
            tenant_id=tenant_id,
            client_id=client_id,
            certificate_path=certificate_path,
        )

        try:
            credential = CertificateCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                certificate_path=certificate_path,
            )

            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_SUCCESS,
                auth_mode="spn_certificate",
                success=True,
                tenant_id=tenant_id,
                client_id=client_id,
            )

            return credential

        except Exception as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Certificate authentication failed",
                error=str(e)[:200],
            )
            audit.log_auth_event(
                event_type=AuditEventType.AUTH_LOGIN_FAILURE,
                auth_mode="spn_certificate",
                success=False,
                error_message=str(e)[:200],
            )
            raise

    elif client_secret:
        # Client secret authentication (traditional)
        log_with_context(
            logger,
            logging.DEBUG,
            "Using client secret authentication",
            tenant_id=tenant_id,
            client_id=client_id,
        )

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

        audit.log_auth_event(
            event_type=AuditEventType.AUTH_LOGIN_SUCCESS,
            auth_mode="spn_secret",
            success=True,
            tenant_id=tenant_id,
            client_id=client_id,
        )

        return credential

    else:
        raise ValueError(
            "Must provide either client_secret or certificate_path for SPN authentication"
        )


# Module-level singleton instance
_auth_instance: Optional[AzureAuth] = None


def get_auth() -> AzureAuth:
    """Get or create the singleton AzureAuth instance."""
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = AzureAuth()
    return _auth_instance


# Convenience functions (delegate to singleton)
def get_storage_options(force_refresh: bool = False) -> Dict[str, str]:
    """Get storage auth options from singleton."""
    return get_auth().get_storage_options(force_refresh)


def clear_token_cache(resource: Optional[str] = None) -> None:
    """Clear token cache on singleton."""
    get_auth().clear_cache(resource)
