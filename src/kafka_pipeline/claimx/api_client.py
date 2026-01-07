"""
ClaimX REST API client.

Async HTTP client for the ClaimXperience API with circuit breaker protection,
rate limiting, and comprehensive error handling.

Supports file-backed credentials for automatic token refresh (following xact pattern).
"""

import asyncio
import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from kafka_pipeline.common.resilience import (
    get_circuit_breaker,
    CLAIMX_API_CIRCUIT_CONFIG,
)
from kafka_pipeline.common.exceptions import ErrorCategory
from kafka_pipeline.common.logging import get_logger, logged_operation, LoggedClass

logger = get_logger(__name__)

# Registry of FileBackedClaimXCredential instances for coordinated refresh
_claimx_credential_registry: list = []


def _register_claimx_credential(credential: "FileBackedClaimXCredential") -> None:
    """Register a FileBackedClaimXCredential for coordinated refresh."""
    _claimx_credential_registry.append(credential)


def _refresh_all_claimx_credentials() -> None:
    """Force refresh all registered ClaimX credentials on auth error."""
    for cred in _claimx_credential_registry:
        try:
            cred.clear_cache()
        except Exception:
            pass  # Best effort


class FileBackedClaimXCredential:
    """
    Credential that reads ClaimX API credentials from file with automatic refresh.

    Similar to FileBackedKustoCredential used by xact, this class re-reads
    credentials from file periodically, allowing token_refresher to keep
    credentials updated externally.

    The credential file should be JSON with username and password:
    {
        "claimx": {
            "username": "user@example.com",
            "password": "secret"
        }
    }

    Or environment variable references:
    {
        "claimx": {
            "username_env": "CLAIMX_API_USERNAME",
            "password_env": "CLAIMX_API_PASSWORD"
        }
    }
    """

    # Default refresh threshold: re-read credentials every 10 minutes
    DEFAULT_REFRESH_THRESHOLD_MINUTES = 10

    def __init__(
        self,
        credential_file: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        refresh_threshold_minutes: int = DEFAULT_REFRESH_THRESHOLD_MINUTES,
    ):
        """
        Initialize file-backed ClaimX credential.

        Args:
            credential_file: Path to JSON credential file (if None, uses env vars)
            username: Static username (fallback if file not available)
            password: Static password (fallback if file not available)
            refresh_threshold_minutes: Re-read file if credentials older than this
        """
        self._credential_file = Path(credential_file) if credential_file else None
        self._static_username = username
        self._static_password = password
        self._refresh_threshold = timedelta(minutes=refresh_threshold_minutes)

        self._cached_auth_token: Optional[str] = None
        self._credentials_acquired_at: Optional[datetime] = None

        # Register for coordinated refresh on auth errors
        _register_claimx_credential(self)

    def _should_refresh(self) -> bool:
        """Check if credentials should be refreshed from file."""
        if self._cached_auth_token is None or self._credentials_acquired_at is None:
            return True
        age = datetime.now(timezone.utc) - self._credentials_acquired_at
        return age >= self._refresh_threshold

    def _read_credentials_from_file(self) -> tuple[str, str]:
        """Read username/password from credential file."""
        if not self._credential_file or not self._credential_file.exists():
            raise RuntimeError(f"Credential file not found: {self._credential_file}")

        content = self._credential_file.read_text(encoding="utf-8-sig").strip()
        if not content:
            raise RuntimeError(f"Credential file is empty: {self._credential_file}")

        try:
            data = json.loads(content)
            claimx_creds = data.get("claimx", data)

            # Check for direct username/password
            if "username" in claimx_creds and "password" in claimx_creds:
                return claimx_creds["username"], claimx_creds["password"]

            # Check for environment variable references
            if "username_env" in claimx_creds and "password_env" in claimx_creds:
                username = os.getenv(claimx_creds["username_env"], "")
                password = os.getenv(claimx_creds["password_env"], "")
                if username and password:
                    return username, password
                raise RuntimeError(
                    f"Environment variables not set: {claimx_creds['username_env']}, "
                    f"{claimx_creds['password_env']}"
                )

            raise RuntimeError(
                "Credential file missing username/password or username_env/password_env"
            )
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON in credential file: {e}") from e

    def _read_credentials_from_env(self) -> tuple[str, str]:
        """Read credentials from environment variables."""
        username = os.getenv("CLAIMX_API_USERNAME", self._static_username or "")
        password = os.getenv("CLAIMX_API_PASSWORD", self._static_password or "")

        if not username or not password:
            raise RuntimeError(
                "ClaimX credentials not available: set CLAIMX_API_USERNAME and "
                "CLAIMX_API_PASSWORD environment variables or provide credential_file"
            )
        return username, password

    def _encode_credentials(self, username: str, password: str) -> str:
        """Encode username:password to Base64 for Basic auth."""
        credentials = f"{username}:{password}"
        return base64.b64encode(credentials.encode()).decode()

    def get_auth_token(self) -> str:
        """
        Return Base64-encoded auth token, refreshing from file if near expiry.

        This method should be called for each request to ensure fresh credentials.
        """
        if self._should_refresh():
            try:
                if self._credential_file:
                    username, password = self._read_credentials_from_file()
                else:
                    username, password = self._read_credentials_from_env()

                self._cached_auth_token = self._encode_credentials(username, password)
                self._credentials_acquired_at = datetime.now(timezone.utc)
                logger.debug(
                    "Refreshed ClaimX credentials",
                    extra={"source": str(self._credential_file) if self._credential_file else "env"},
                )
            except Exception as e:
                # If we have cached credentials, log warning but continue
                if self._cached_auth_token:
                    logger.warning(
                        f"Failed to refresh ClaimX credentials, using cached: {e}"
                    )
                else:
                    raise

        return self._cached_auth_token

    def clear_cache(self) -> None:
        """Clear cached credentials to force re-read on next get_auth_token()."""
        self._cached_auth_token = None
        self._credentials_acquired_at = None
        logger.debug("Cleared ClaimX credential cache")


class ClaimXApiError(Exception):
    """Base exception for ClaimX API errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        category: ErrorCategory = ErrorCategory.TRANSIENT,
        is_retryable: bool = True,
        should_refresh_auth: bool = False,
    ):
        """
        Initialize ClaimX API error.

        Args:
            message: Error description
            status_code: HTTP status code if applicable
            category: Error category for classification
            is_retryable: Whether this error can be retried
            should_refresh_auth: Whether auth credentials should be refreshed
        """
        super().__init__(message)
        self.status_code = status_code
        self.category = category
        self.is_retryable = is_retryable
        self.should_refresh_auth = should_refresh_auth


def classify_api_error(status: int, url: str) -> ClaimXApiError:
    """
    Create appropriate exception for HTTP status code.

    Classifies HTTP status codes into appropriate error categories:
    - 401: Authentication error (retryable with credential refresh)
    - 403: Authorization error (not retryable)
    - 404: Not found (permanent, not retryable)
    - 429: Rate limit (transient, retryable)
    - 5xx: Server error (transient, retryable)
    - 4xx: Client error (permanent, not retryable)

    Args:
        status: HTTP status code
        url: Request URL for context

    Returns:
        ClaimXApiError with proper classification
    """
    if status == 401:
        # Auth errors ARE retryable if credentials can be refreshed
        return ClaimXApiError(
            f"Unauthorized (401): {url}",
            status_code=status,
            category=ErrorCategory.AUTH,
            is_retryable=True,  # Now retryable with credential refresh
            should_refresh_auth=True,
        )

    if status == 403:
        return ClaimXApiError(
            f"Forbidden (403): {url}",
            status_code=status,
            category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    if status == 404:
        return ClaimXApiError(
            f"Not found (404): {url}",
            status_code=status,
            category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    if status == 429:
        return ClaimXApiError(
            f"Rate limited (429): {url}",
            status_code=status,
            category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )

    if status in (500, 502, 503, 504):
        return ClaimXApiError(
            f"Server error ({status}): {url}",
            status_code=status,
            category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )

    if 400 <= status < 500:
        return ClaimXApiError(
            f"Client error ({status}): {url}",
            status_code=status,
            category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    return ClaimXApiError(
        f"HTTP error ({status}): {url}",
        status_code=status,
        category=ErrorCategory.TRANSIENT,
        is_retryable=True,
    )


class ClaimXApiClient(LoggedClass):
    """
    Async client for ClaimXperience REST API.

    Provides methods for fetching projects, media, tasks, contacts, and other
    entities from the ClaimX API with built-in:
    - Circuit breaker pattern for resilience
    - Rate limiting via semaphore
    - Comprehensive error handling and classification
    - Request/response logging
    - Automatic credential refresh (via FileBackedClaimXCredential)

    Usage:
        # With file-backed credentials (recommended for long-running workers)
        credential = FileBackedClaimXCredential(username="user", password="pass")
        async with ClaimXApiClient(base_url, credential=credential) as client:
            project = await client.get_project(123)

        # With static credentials (legacy)
        async with ClaimXApiClient(base_url, auth_token="token") as client:
            project = await client.get_project(123)

    Configuration:
        base_url: ClaimX API base URL (e.g., https://www.claimxperience.com/service/cxedirest)
        credential: FileBackedClaimXCredential for auto-refreshing credentials
        auth_token: Basic authentication token (static, legacy)
        timeout_seconds: Request timeout (default: 30)
        max_concurrent: Maximum concurrent requests (default: 20)
    """

    log_component = "claimx_api"

    def __init__(
        self,
        base_url: str,
        credential: Optional[FileBackedClaimXCredential] = None,
        auth_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout_seconds: int = 30,
        max_concurrent: int = 20,
        sender_username: str = "user@example.com",
    ):
        """
        Initialize ClaimX API client.

        Args:
            base_url: API base URL (e.g., https://www.claimxperience.com/service/cxedirest)
            credential: FileBackedClaimXCredential for auto-refreshing credentials (recommended)
            auth_token: Pre-encoded Basic auth token (base64, static - legacy)
            username: ClaimX API username (creates FileBackedClaimXCredential)
            password: ClaimX API password (creates FileBackedClaimXCredential)
            timeout_seconds: Request timeout in seconds
            max_concurrent: Max concurrent requests
            sender_username: Default sender username for video collaboration

        Raises:
            ValueError: If no valid credential source provided
        """
        self.base_url = base_url.rstrip("/")

        # Use provided credential, or create one from username/password, or use static token
        if credential:
            self._credential = credential
        elif username and password:
            # Create file-backed credential that reads from env vars as fallback
            self._credential = FileBackedClaimXCredential(
                username=username,
                password=password,
            )
        elif auth_token:
            # Legacy: static auth token (no refresh capability)
            self._credential = None
            self._static_auth_token = auth_token
            logger.warning(
                "Using static auth_token - credentials will not auto-refresh. "
                "Consider using credential= or username/password= for auto-refresh."
            )
        else:
            raise ValueError(
                "ClaimXApiClient requires 'credential', 'username' and 'password', "
                "or 'auth_token'"
            )

        self.timeout_seconds = timeout_seconds
        self.max_concurrent = max_concurrent
        self.sender_username = sender_username

        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._circuit = get_circuit_breaker("claimx_api", CLAIMX_API_CIRCUIT_CONFIG)

        super().__init__()

    def _get_auth_token(self) -> str:
        """Get current auth token, refreshing if needed."""
        if self._credential:
            return self._credential.get_auth_token()
        return self._static_auth_token

    async def __aenter__(self) -> "ClaimXApiClient":
        """Create session on context enter."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close session on context exit."""
        await self.close()

    async def _ensure_session(self) -> None:
        """Create aiohttp session if not exists."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
            )
            # Don't bake auth header into session - we pass it per-request for refresh support
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        _auth_retry: bool = False,
    ) -> Dict[str, Any]:
        """
        Make an API request with circuit breaker protection and rate limiting.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path (will be joined with base_url)
            params: Query parameters
            json_body: JSON body for POST requests
            _auth_retry: Internal flag - True if this is a retry after auth refresh

        Returns:
            Parsed JSON response

        Raises:
            ClaimXApiError: On API errors, timeouts, or circuit breaker open
        """
        await self._ensure_session()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Check circuit breaker
        if self._circuit.is_open:
            retry_after = self._circuit._get_retry_after()
            error = ClaimXApiError(
                f"Circuit open, retry after {retry_after:.0f}s",
                category=ErrorCategory.CIRCUIT_OPEN,
                is_retryable=True,
            )
            self._log(
                logging.WARNING,
                "Circuit breaker open",
                api_endpoint=endpoint,
                api_method=method,
                circuit_state="open",
            )
            raise error

        # Get fresh auth token for each request (supports credential refresh)
        auth_token = self._get_auth_token()
        request_headers = {"Authorization": f"Basic {auth_token}"}

        async with self._semaphore:
            try:
                assert self._session is not None  # for mypy
                async with self._session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                ) as response:
                    if response.status != 200:
                        error = classify_api_error(response.status, url)

                        # Handle auth errors with credential refresh and retry
                        if error.should_refresh_auth and not _auth_retry and self._credential:
                            self._log(
                                logging.INFO,
                                "Auth error detected, refreshing credentials and retrying",
                                api_endpoint=endpoint,
                                api_method=method,
                            )
                            # Clear credential cache to force re-read from file/env
                            _refresh_all_claimx_credentials()
                            # Retry once with fresh credentials
                            return await self._request(
                                method, endpoint, params, json_body, _auth_retry=True
                            )

                        if error.is_retryable:
                            self._circuit.record_failure(error)
                        self._log(
                            logging.WARNING,
                            "API request failed",
                            api_endpoint=endpoint,
                            api_method=method,
                            http_status=response.status,
                            error_category=error.category.value,
                            auth_retry=_auth_retry,
                        )
                        raise error

                    self._circuit.record_success()

                    data = await response.json()
                    return data

            except asyncio.TimeoutError:
                error = ClaimXApiError(
                    f"Timeout after {self.timeout_seconds}s: {url}",
                    category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )
                self._circuit.record_failure(error)
                self._log(
                    logging.WARNING,
                    "API request timeout",
                    api_endpoint=endpoint,
                    api_method=method,
                    error_category="transient",
                )
                raise error

            except aiohttp.ClientError as e:
                error = ClaimXApiError(
                    f"Connection error: {e}",
                    category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )
                self._circuit.record_failure(error)
                self._log_exception(
                    e,
                    "API connection error",
                    level=logging.WARNING,
                    api_endpoint=endpoint,
                    api_method=method,
                )
                raise error

    # =========================================================================
    # Project Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG)
    async def get_project(self, project_id: int) -> Dict[str, Any]:
        """
        Get full project details.

        Used for: PROJECT_CREATED, PROJECT_MFN_ADDED events

        Args:
            project_id: Project ID

        Returns:
            Project data dict with full project details

        Raises:
            ClaimXApiError: On API errors
        """
        return await self._request("GET", f"/export/project/{project_id}")

    @logged_operation(level=logging.DEBUG)
    async def get_project_media(
        self,
        project_id: int,
        media_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get media metadata for a project.

        Used for: PROJECT_FILE_ADDED events

        Args:
            project_id: Project ID
            media_ids: Optional list of specific media IDs to fetch

        Returns:
            List of media metadata dicts (includes full_download_link for attachments)

        Raises:
            ClaimXApiError: On API errors
        """
        params = {}
        if media_ids:
            params["mediaIds"] = ",".join(str(m) for m in media_ids)

        response = await self._request(
            "GET",
            f"/export/project/{project_id}/media",
            params=params if params else None,
        )

        # API may return single object or list - normalize to list
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            # Might be wrapped in a data/media key
            if "data" in response:
                return response["data"]
            if "media" in response:
                return response["media"]
            return [response]
        return []

    # =========================================================================
    # Contact Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG)
    async def get_project_contacts(self, project_id: int) -> List[Dict[str, Any]]:
        """
        Get all contacts (policyholders) for a project.

        Args:
            project_id: Project ID

        Returns:
            List of contact data dicts

        Raises:
            ClaimXApiError: On API errors
        """
        response = await self._request(
            "GET",
            f"/export/project/{project_id}/contacts",
        )

        # Normalize response to list
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            if "data" in response:
                return response["data"]
            if "contacts" in response:
                return response["contacts"]
            return [response]
        return []

    # =========================================================================
    # Custom Task Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG)
    async def get_custom_task(self, assignment_id: int) -> Dict[str, Any]:
        """
        Get custom task assignment details.

        Used for: CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED events

        Args:
            assignment_id: Task assignment ID

        Returns:
            Task assignment data with nested customTask and externalLinkData

        Raises:
            ClaimXApiError: On API errors
        """
        return await self._request(
            "GET",
            f"/customTasks/assignment/{assignment_id}",
            params={"full": "true"},
        )

    @logged_operation(level=logging.DEBUG)
    async def get_project_tasks(self, project_id: int) -> List[Dict[str, Any]]:
        """
        Get all custom tasks for a project.

        Args:
            project_id: Project ID

        Returns:
            List of task assignment data

        Raises:
            ClaimXApiError: On API errors
        """
        response = await self._request(
            "GET",
            f"/export/project/{project_id}/tasks",
        )

        # Normalize response to list
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            if "data" in response:
                return response["data"]
            if "tasks" in response:
                return response["tasks"]
            return [response]
        return []

    # =========================================================================
    # Video Collaboration Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG)
    async def get_video_collaboration(
        self,
        project_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sender_username: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get video collaboration report for a project.

        Used for: VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED events

        Args:
            project_id: Project ID
            start_date: Optional filter start date
            end_date: Optional filter end date
            sender_username: Sender username (defaults to instance default)

        Returns:
            Video collaboration data

        Raises:
            ClaimXApiError: On API errors
        """
        body: Dict[str, Any] = {
            "reportType": "VIDEO_COLLABORATION",
            "projectId": int(project_id),
            "senderUsername": sender_username or self.sender_username,
        }

        if start_date:
            body["startDate"] = start_date.isoformat()
        if end_date:
            body["endDate"] = end_date.isoformat()

        return await self._request("POST", "/data", json_body=body)

    # =========================================================================
    # Conversation Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG)
    async def get_project_conversations(
        self, project_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all conversations for a project.

        Args:
            project_id: Project ID

        Returns:
            List of conversation data dicts

        Raises:
            ClaimXApiError: On API errors
        """
        response = await self._request(
            "GET",
            f"/export/project/{project_id}/conversations",
        )

        # Normalize response to list
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            if "data" in response:
                return response["data"]
            if "conversations" in response:
                return response["conversations"]
            return [response]
        return []

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_circuit_status(self) -> Dict[str, Any]:
        """
        Get circuit breaker diagnostics.

        Returns:
            Dict with circuit breaker state and metrics
        """
        return self._circuit.get_diagnostics()

    @property
    def is_circuit_open(self) -> bool:
        """
        Check if circuit breaker is open.

        Returns:
            True if circuit is open (requests will be rejected)
        """
        return self._circuit.is_open
