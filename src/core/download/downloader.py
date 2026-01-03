"""
Unified attachment downloader with clean interface.

Provides AttachmentDownloader class that orchestrates:
- URL validation (SSRF prevention)
- File type validation (extension and MIME type)
- HTTP download (in-memory or streaming based on size)
- Error classification and reporting

Clean interface: DownloadTask -> DownloadOutcome
"""

import asyncio
from pathlib import Path
from typing import Optional

import aiohttp

from core.download.http_client import create_session, download_url
from core.download.models import DownloadOutcome, DownloadTask
from core.download.streaming import download_to_file, should_stream
from core.errors.exceptions import ErrorCategory
from core.security.file_validation import validate_file_type
from core.security.url_validation import validate_download_url


class AttachmentDownloader:
    """
    Unified downloader for attachments with validation and error handling.

    This class orchestrates the complete download process:
    1. URL validation (if enabled)
    2. File type validation (if enabled)
    3. HTTP download (streaming or in-memory based on size)
    4. Error classification and reporting

    Usage:
        downloader = AttachmentDownloader()
        task = DownloadTask(
            url="https://example.com/file.pdf",
            destination=Path("output.pdf")
        )
        outcome = await downloader.download(task)
        if outcome.success:
            print(f"Downloaded {outcome.bytes_downloaded} bytes")
        else:
            print(f"Failed: {outcome.error_message}")

    Session management:
        By default, creates a new session for each download.
        For batch downloads, pass a shared session to the constructor:

        async with create_session() as session:
            downloader = AttachmentDownloader(session=session)
            for task in tasks:
                outcome = await downloader.download(task)
    """

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        max_connections: int = 100,
        max_connections_per_host: int = 10,
    ):
        """
        Initialize AttachmentDownloader.

        Args:
            session: Optional aiohttp session (None = create per download)
            max_connections: Total connection pool size (default: 100)
            max_connections_per_host: Per-host connection limit (default: 10)
        """
        self._session = session
        self._owns_session = session is None
        self._max_connections = max_connections
        self._max_connections_per_host = max_connections_per_host

    async def download(self, task: DownloadTask) -> DownloadOutcome:
        """
        Download attachment according to task specification.

        Orchestrates the complete download process with validation and error handling.

        Args:
            task: Download task specification

        Returns:
            DownloadOutcome with success/failure and metadata

        Steps:
            1. Validate URL (if task.validate_url=True)
            2. Validate file type from URL (if task.validate_file_type=True)
            3. Perform HTTP download (streaming or in-memory)
            4. Validate Content-Type from response (if task.validate_file_type=True)
            5. Return outcome with metadata

        Example:
            task = DownloadTask(
                url="https://example.com/file.pdf",
                destination=Path("file.pdf"),
                timeout=30,
                validate_url=True,
                validate_file_type=True
            )
            outcome = await downloader.download(task)
        """
        # Step 1: Validate URL
        if task.validate_url:
            is_valid, error = validate_download_url(
                task.url, allowed_domains=task.allowed_domains
            )
            if not is_valid:
                return DownloadOutcome.validation_failure(
                    validation_error=f"URL validation failed: {error}",
                    error_category=ErrorCategory.PERMANENT,
                )

        # Step 2: Validate file type from URL
        if task.validate_file_type:
            is_valid, error = validate_file_type(
                task.url, allowed_extensions=task.allowed_extensions
            )
            if not is_valid:
                return DownloadOutcome.validation_failure(
                    validation_error=f"File type validation failed: {error}",
                    error_category=ErrorCategory.PERMANENT,
                )

        # Step 3: Perform HTTP download
        session = self._session
        should_close_session = False

        try:
            # Create session if needed
            if session is None:
                session = create_session(
                    max_connections=self._max_connections,
                    max_connections_per_host=self._max_connections_per_host,
                )
                should_close_session = True

            # HEAD request to check Content-Length for streaming decision
            # (Optional optimization - could also just try streaming)
            content_length = await self._get_content_length(
                task.url, session, task.timeout
            )

            # Check max size if specified
            if task.max_size and content_length and content_length > task.max_size:
                return DownloadOutcome.validation_failure(
                    validation_error=f"File size {content_length} exceeds maximum {task.max_size}",
                    error_category=ErrorCategory.PERMANENT,
                )

            # Decide on streaming vs in-memory based on size
            use_streaming = should_stream(content_length)

            if use_streaming:
                # Use streaming download for large files
                outcome = await self._download_streaming(task, session)
            else:
                # Use in-memory download for small files
                outcome = await self._download_in_memory(task, session)

            # Step 4: Validate Content-Type from response
            if outcome.success and task.validate_file_type and outcome.content_type:
                is_valid, error = validate_file_type(
                    task.url,
                    content_type=outcome.content_type,
                    allowed_extensions=task.allowed_extensions,
                )
                if not is_valid:
                    # Delete downloaded file on validation failure
                    if outcome.file_path and outcome.file_path.exists():
                        outcome.file_path.unlink()

                    return DownloadOutcome.validation_failure(
                        validation_error=f"Content-Type validation failed: {error}",
                        error_category=ErrorCategory.PERMANENT,
                    )

            return outcome

        finally:
            # Clean up session if we created it
            if should_close_session and session:
                await session.close()

    async def _get_content_length(
        self, url: str, session: aiohttp.ClientSession, timeout: int
    ) -> Optional[int]:
        """
        Get Content-Length from HEAD request.

        Args:
            url: URL to check
            session: aiohttp session
            timeout: Request timeout

        Returns:
            Content-Length in bytes, or None if unavailable
        """
        try:
            async with session.head(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
            ) as response:
                return response.content_length
        except Exception:
            # HEAD request failed or not supported - continue with download
            return None

    async def _download_in_memory(
        self, task: DownloadTask, session: aiohttp.ClientSession
    ) -> DownloadOutcome:
        """
        Download file in-memory (for files < 50MB).

        Args:
            task: Download task
            session: aiohttp session

        Returns:
            DownloadOutcome
        """
        response, error = await download_url(
            url=task.url,
            session=session,
            timeout=task.timeout,
        )

        if error:
            return DownloadOutcome.download_failure(
                error_message=error.error_message,
                error_category=error.error_category,
                status_code=error.status_code,
            )

        # Write content to file
        try:
            # Use asyncio.to_thread for mkdir to ensure proper synchronization
            # on Windows, where synchronous mkdir may not be immediately visible
            await asyncio.to_thread(
                task.destination.parent.mkdir, parents=True, exist_ok=True
            )
            await asyncio.to_thread(task.destination.write_bytes, response.content)

            return DownloadOutcome.success_outcome(
                file_path=task.destination,
                bytes_downloaded=len(response.content),
                content_type=response.content_type,
                status_code=response.status_code,
            )

        except OSError as e:
            return DownloadOutcome.download_failure(
                error_message=f"File write error: {str(e)}",
                error_category=ErrorCategory.PERMANENT,
            )

    async def _download_streaming(
        self, task: DownloadTask, session: aiohttp.ClientSession
    ) -> DownloadOutcome:
        """
        Download file using streaming (for files > 50MB).

        Args:
            task: Download task
            session: aiohttp session

        Returns:
            DownloadOutcome
        """
        # Ensure parent directory exists
        # Use asyncio.to_thread for mkdir to ensure proper synchronization
        # on Windows, where synchronous mkdir may not be immediately visible
        await asyncio.to_thread(
            task.destination.parent.mkdir, parents=True, exist_ok=True
        )

        result, error = await download_to_file(
            url=task.url,
            output_path=task.destination,
            session=session,
            timeout=task.timeout,
        )

        if error:
            return DownloadOutcome.download_failure(
                error_message=error.error_message,
                error_category=error.error_category,
                status_code=error.status_code,
            )

        return DownloadOutcome.success_outcome(
            file_path=task.destination,
            bytes_downloaded=result.bytes_written,
            content_type=result.content_type,
            status_code=200,
        )

    async def _get_content_type(
        self, url: str, session: aiohttp.ClientSession, timeout: int
    ) -> Optional[str]:
        """
        Get Content-Type from HEAD request.

        Args:
            url: URL to check
            session: aiohttp session
            timeout: Request timeout

        Returns:
            Content-Type header value, or None if unavailable
        """
        try:
            async with session.head(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
            ) as response:
                return response.headers.get("Content-Type")
        except Exception:
            # HEAD request failed - return None
            return None


__all__ = ["AttachmentDownloader"]
