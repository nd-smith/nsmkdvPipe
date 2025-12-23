"""
ClaimX REST API client.

Async HTTP client for the ClaimXperience API with circuit breaker protection.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

from verisk_pipeline.common.circuit_breaker import (
    get_circuit_breaker,
    CLAIMX_API_CIRCUIT_CONFIG,
)
from verisk_pipeline.common.exceptions import (
    ErrorCategory,
)
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.decorators import logged_operation, LoggedClass

logger = get_logger(__name__)


class ClaimXApiError(Exception):
    """Base exception for ClaimX API errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        category: ErrorCategory = ErrorCategory.TRANSIENT,
        is_retryable: bool = True,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.category = category
        self.is_retryable = is_retryable


def classify_api_error(status: int, url: str) -> ClaimXApiError:
    """
    Create appropriate exception for HTTP status code.

    Args:
        status: HTTP status code
        url: Request URL for context

    Returns:
        ClaimXApiError with proper classification
    """
    if status == 401:
        return ClaimXApiError(
            f"Unauthorized (401): {url}",
            status_code=status,
            category=ErrorCategory.AUTH,
            is_retryable=False,
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

    Usage:
        async with ClaimXApiClient(config) as client:
            project = await client.get_project(123)
            media = await client.get_project_media(123, media_ids=[456, 789])
    """

    log_component = "api"

    def __init__(
        self,
        base_url: str,
        auth_token: str,
        timeout_seconds: int = 30,
        max_concurrent: int = 20,
        senderUsername: str = "user@example.com",
    ):
        """
        Args:
            base_url: API base URL (e.g., https://www.claimxperience.com/service/cxedirest)
            auth_token: Basic auth token
            timeout_seconds: Request timeout
            max_concurrent: Max concurrent requests
            senderUsername: Default sender username for video collaboration
        """
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.timeout_seconds = timeout_seconds
        self.max_concurrent = max_concurrent
        self.senderUsername = senderUsername

        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._circuit = get_circuit_breaker("claimx_api", CLAIMX_API_CIRCUIT_CONFIG)

        super().__init__()

    async def __aenter__(self) -> "ClaimXApiClient":
        """Create session on context enter."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close session on context exit."""
        await self.close()

    async def _ensure_session(self) -> None:
        """Create session if not exists."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Authorization": f"Basic {self.auth_token}",
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
    ) -> Dict[str, Any]:
        """
        Make an API request with circuit breaker protection.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            params: Query parameters
            json_body: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            ClaimXApiError: On API errors
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

        async with self._semaphore:
            try:
                assert self._session is not None  # for mypy
                async with self._session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                ) as response:
                    if response.status != 200:
                        error = classify_api_error(response.status, url)
                        if error.is_retryable:
                            self._circuit.record_failure(error)
                        self._log(
                            logging.WARNING,
                            "API request failed",
                            api_endpoint=endpoint,
                            api_method=method,
                            http_status=response.status,
                            error_category=error.category.value,
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

    @logged_operation(level=logging.DEBUG, include_args=["project_id"])
    async def get_project(self, project_id: int) -> Dict[str, Any]:
        """
        Get full project details.

        Used for: PROJECT_CREATED, PROJECT_MFN_ADDED

        Args:
            project_id: Project ID

        Returns:
            Project data dict
        """
        return await self._request("GET", f"/export/project/{project_id}")

    @logged_operation(level=logging.DEBUG, include_args=["project_id"])
    async def get_project_media(
        self,
        project_id: int,
        media_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get media metadata for a project.

        Used for: PROJECT_FILE_ADDED

        Args:
            project_id: Project ID
            media_ids: Optional list of specific media IDs to fetch

        Returns:
            List of media metadata dicts (includes full_download_link)
        """
        params = {}
        if media_ids:
            params["mediaIds"] = ",".join(str(m) for m in media_ids)

        response = await self._request(
            "GET",
            f"/export/project/{project_id}/media",
            params=params if params else None,
        )

        # API may return single object or list
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            # Might be wrapped in a key
            if "data" in response:
                return response["data"]
            if "media" in response:
                return response["media"]
            return [response]
        return []

    # =========================================================================
    # Custom Task Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG, include_args=["assignment_id"])
    async def get_custom_task(self, assignment_id: int) -> Dict[str, Any]:
        """
        Get custom task assignment details.

        Used for: CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED

        Args:
            assignment_id: Task assignment ID

        Returns:
            Task assignment data with nested customTask and externalLinkData
        """
        return await self._request(
            "GET",
            f"/customTasks/assignment/{assignment_id}",
            params={"full": "true"},
        )

    # =========================================================================
    # Video Collaboration Endpoints
    # =========================================================================

    @logged_operation(level=logging.DEBUG, include_args=["project_id"])
    async def get_video_collaboration(
        self,
        project_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        senderUsername: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get video collaboration report for a project.

        Used for: VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED

        Args:
            project_id: Project ID
            start_date: Optional filter start
            end_date: Optional filter end
            senderUsername: Sender username (defaults to instance default)

        Returns:
            Video collaboration data
        """
        body: Dict[str, Any] = {
            "reportType": "VIDEO_COLLABORATION",
            "projectId": int(project_id),
            "senderUsername": senderUsername or self.senderUsername,
        }

        if start_date:
            body["startDate"] = start_date.isoformat()
        if end_date:
            body["endDate"] = end_date.isoformat()

        return await self._request("POST", "/data", json_body=body)

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_circuit_status(self) -> Dict[str, Any]:
        """Get circuit breaker diagnostics."""
        return self._circuit.get_diagnostics()

    @property
    def is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self._circuit.is_open
