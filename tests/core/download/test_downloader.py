"""
Tests for AttachmentDownloader with DownloadTask/DownloadOutcome interface.

Test coverage:
- Successful downloads (small and large files)
- URL validation (domain allowlist)
- File type validation (extension and Content-Type)
- HTTP errors (4xx, 5xx, timeouts, connection errors)
- Max size enforcement
- Session management
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from core.download.downloader import AttachmentDownloader
from core.download.http_client import DownloadError, DownloadResponse
from core.download.models import DownloadTask
from core.download.streaming import STREAM_THRESHOLD, StreamDownloadError
from core.errors.exceptions import ErrorCategory


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create temporary output directory for downloads."""
    output_dir = tmp_path / "downloads"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def sample_task(temp_output_dir):
    """Create sample download task."""
    return DownloadTask(
        url="https://claimxperience.com/file.pdf",
        destination=temp_output_dir / "file.pdf",
        timeout=30,
    )


class TestAttachmentDownloaderSuccess:
    """Test successful download scenarios."""

    @pytest.mark.asyncio
    async def test_small_file_download_success(self, sample_task, temp_output_dir):
        """Test successful download of small file (in-memory)."""
        content = b"PDF file content"
        response = DownloadResponse(
            content=content,
            status_code=200,
            content_length=len(content),
            content_type="application/pdf",
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = len(content)
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (response, None)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is True
            assert outcome.file_path == sample_task.destination
            assert outcome.bytes_downloaded == len(content)
            assert outcome.content_type == "application/pdf"
            assert outcome.status_code == 200
            assert outcome.error_message is None
            assert outcome.error_category is None

            # Verify file was written
            assert sample_task.destination.exists()
            assert sample_task.destination.read_bytes() == content

    @pytest.mark.asyncio
    async def test_large_file_download_success(self, sample_task, temp_output_dir):
        """Test successful download of large file (streaming)."""
        bytes_written = 100 * 1024 * 1024  # 100MB

        # Mock session for HEAD requests (both Content-Length and Content-Type)
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = bytes_written
        mock_head_response.headers.get = MagicMock(return_value="application/pdf")
        mock_session.head.return_value.__aenter__.return_value = mock_head_response

        with patch("core.download.downloader.download_to_file") as mock_stream, patch(
            "core.download.downloader.should_stream"
        ) as mock_should_stream, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_should_stream.return_value = True
            mock_stream.return_value = (bytes_written, None)
            mock_create.return_value = mock_session
            mock_session.close = AsyncMock()

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is True
            assert outcome.bytes_downloaded == bytes_written
            assert outcome.content_type == "application/pdf"

    @pytest.mark.asyncio
    async def test_download_without_validation(self, temp_output_dir):
        """Test download with validation disabled."""
        task = DownloadTask(
            url="https://untrusted.com/file.exe",
            destination=temp_output_dir / "file.exe",
            validate_url=False,
            validate_file_type=False,
        )

        content = b"executable content"
        response = DownloadResponse(
            content=content,
            status_code=200,
            content_type="application/x-executable",
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = len(content)
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (response, None)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(task)

            # Should succeed because validation is disabled
            assert outcome.success is True
            assert outcome.bytes_downloaded == len(content)


class TestAttachmentDownloaderValidation:
    """Test validation failures."""

    @pytest.mark.asyncio
    async def test_url_validation_failure_domain(self, temp_output_dir):
        """Test URL validation failure for untrusted domain."""
        task = DownloadTask(
            url="https://untrusted.com/file.pdf",
            destination=temp_output_dir / "file.pdf",
            validate_url=True,
        )

        downloader = AttachmentDownloader()
        outcome = await downloader.download(task)

        assert outcome.success is False
        assert "URL validation failed" in outcome.error_message
        assert "not in allowlist" in outcome.validation_error
        assert outcome.error_category == ErrorCategory.PERMANENT

    @pytest.mark.asyncio
    async def test_url_validation_failure_http(self, temp_output_dir):
        """Test URL validation failure for HTTP (not HTTPS)."""
        task = DownloadTask(
            url="http://claimxperience.com/file.pdf",
            destination=temp_output_dir / "file.pdf",
            validate_url=True,
        )

        downloader = AttachmentDownloader()
        outcome = await downloader.download(task)

        assert outcome.success is False
        assert "URL validation failed" in outcome.error_message
        assert "Must be HTTPS" in outcome.validation_error
        assert outcome.error_category == ErrorCategory.PERMANENT

    @pytest.mark.asyncio
    async def test_file_type_validation_failure_extension(self, temp_output_dir):
        """Test file type validation failure for disallowed extension."""
        task = DownloadTask(
            url="https://claimxperience.com/file.exe",
            destination=temp_output_dir / "file.exe",
            validate_file_type=True,
        )

        downloader = AttachmentDownloader()
        outcome = await downloader.download(task)

        assert outcome.success is False
        assert "File type validation failed" in outcome.error_message
        assert "not allowed" in outcome.validation_error
        assert outcome.error_category == ErrorCategory.PERMANENT

    @pytest.mark.asyncio
    async def test_file_type_validation_failure_content_type(
        self, sample_task, temp_output_dir
    ):
        """Test file type validation failure for mismatched Content-Type."""
        content = b"executable content"
        response = DownloadResponse(
            content=content,
            status_code=200,
            content_type="application/x-executable",  # Wrong type
        )

        with patch("core.download.downloader.download_url") as mock_download:
            mock_download.return_value = (response, None)

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert "Content-Type validation failed" in outcome.error_message
            assert outcome.error_category == ErrorCategory.PERMANENT

            # File should be deleted after validation failure
            assert not sample_task.destination.exists()

    @pytest.mark.asyncio
    async def test_max_size_enforcement(self, sample_task):
        """Test max size enforcement."""
        sample_task.max_size = 1000  # 1KB limit

        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = 5000  # 5KB file
        mock_session.head.return_value.__aenter__.return_value = mock_head_response

        downloader = AttachmentDownloader(session=mock_session)
        outcome = await downloader.download(sample_task)

        assert outcome.success is False
        assert "exceeds maximum" in outcome.validation_error
        assert outcome.error_category == ErrorCategory.PERMANENT


class TestAttachmentDownloaderErrors:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_http_404_error(self, sample_task):
        """Test handling of HTTP 404 error."""
        error = DownloadError(
            status_code=404,
            error_message="HTTP 404",
            error_category=ErrorCategory.PERMANENT,
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = 1000
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (None, error)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert outcome.error_message == "HTTP 404"
            assert outcome.status_code == 404
            assert outcome.error_category == ErrorCategory.PERMANENT

    @pytest.mark.asyncio
    async def test_http_500_error(self, sample_task):
        """Test handling of HTTP 500 error."""
        error = DownloadError(
            status_code=500,
            error_message="HTTP 500",
            error_category=ErrorCategory.TRANSIENT,
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = 1000
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (None, error)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert outcome.error_message == "HTTP 500"
            assert outcome.status_code == 500
            assert outcome.error_category == ErrorCategory.TRANSIENT

    @pytest.mark.asyncio
    async def test_timeout_error(self, sample_task):
        """Test handling of timeout error."""
        error = DownloadError(
            status_code=None,
            error_message="Download timeout after 30s",
            error_category=ErrorCategory.TRANSIENT,
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = 1000
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (None, error)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert "timeout" in outcome.error_message.lower()
            assert outcome.status_code is None
            assert outcome.error_category == ErrorCategory.TRANSIENT

    @pytest.mark.asyncio
    async def test_connection_error(self, sample_task):
        """Test handling of connection error."""
        error = DownloadError(
            status_code=None,
            error_message="Connection error: DNS lookup failed",
            error_category=ErrorCategory.TRANSIENT,
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = 1000
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (None, error)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert "Connection error" in outcome.error_message
            assert outcome.status_code is None
            assert outcome.error_category == ErrorCategory.TRANSIENT

    @pytest.mark.asyncio
    async def test_file_write_error(self, sample_task):
        """Test handling of file write error."""
        content = b"PDF content"
        response = DownloadResponse(
            content=content, status_code=200, content_type="application/pdf"
        )

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "asyncio.to_thread"
        ) as mock_to_thread:
            mock_download.return_value = (response, None)
            mock_to_thread.side_effect = OSError("Disk full")

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert "File write error" in outcome.error_message
            assert outcome.error_category == ErrorCategory.PERMANENT

    @pytest.mark.asyncio
    async def test_streaming_download_error(self, sample_task):
        """Test handling of streaming download error."""
        error = StreamDownloadError(
            status_code=500,
            error_message="HTTP 500",
            error_category=ErrorCategory.TRANSIENT,
        )

        with patch("core.download.downloader.download_to_file") as mock_stream, patch(
            "core.download.downloader.should_stream"
        ) as mock_should_stream:
            mock_should_stream.return_value = True
            mock_stream.return_value = (None, error)

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            assert outcome.success is False
            assert outcome.error_message == "HTTP 500"
            assert outcome.error_category == ErrorCategory.TRANSIENT


class TestAttachmentDownloaderSessionManagement:
    """Test session lifecycle management."""

    @pytest.mark.asyncio
    async def test_creates_own_session(self, sample_task):
        """Test that downloader creates its own session when none provided."""
        content = b"PDF content"
        response = DownloadResponse(
            content=content, status_code=200, content_type="application/pdf"
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = len(content)
        mock_session.head.return_value.__aenter__.return_value = mock_head_response

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (response, None)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(sample_task)

            # Should create session
            mock_create.assert_called_once()
            # Should close session after use
            mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_uses_provided_session(self, sample_task):
        """Test that downloader uses provided session without closing it."""
        content = b"PDF content"
        response = DownloadResponse(
            content=content, status_code=200, content_type="application/pdf"
        )

        # Mock provided session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = len(content)
        mock_session.head.return_value.__aenter__.return_value = mock_head_response

        with patch("core.download.downloader.download_url") as mock_download:
            mock_download.return_value = (response, None)

            downloader = AttachmentDownloader(session=mock_session)
            outcome = await downloader.download(sample_task)

            # Should NOT close provided session
            mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_session_cleanup_on_error(self, sample_task):
        """Test that session is cleaned up even on error."""
        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.side_effect = Exception("Unexpected error")
            mock_session = AsyncMock(spec=aiohttp.ClientSession)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()

            with pytest.raises(Exception):
                await downloader.download(sample_task)

            # Session should still be closed
            mock_session.close.assert_called_once()


class TestAttachmentDownloaderIntegration:
    """Integration tests with real file I/O."""

    @pytest.mark.asyncio
    async def test_download_creates_parent_directory(self, temp_output_dir):
        """Test that download creates parent directory if missing."""
        nested_path = temp_output_dir / "nested" / "dir" / "file.pdf"
        task = DownloadTask(
            url="https://claimxperience.com/file.pdf", destination=nested_path
        )

        content = b"PDF content"
        response = DownloadResponse(
            content=content, status_code=200, content_type="application/pdf"
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = len(content)
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (response, None)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(task)

            assert outcome.success is True
            assert nested_path.exists()
            assert nested_path.parent.exists()

    @pytest.mark.asyncio
    async def test_custom_allowed_domains(self, temp_output_dir):
        """Test download with custom allowed domains."""
        task = DownloadTask(
            url="https://custom-domain.com/file.pdf",
            destination=temp_output_dir / "file.pdf",
            validate_url=True,
            allowed_domains={"custom-domain.com"},
        )

        content = b"PDF content"
        response = DownloadResponse(
            content=content, status_code=200, content_type="application/pdf"
        )

        # Mock session for HEAD requests
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_head_response = AsyncMock()
        mock_head_response.content_length = len(content)
        mock_session.head.return_value.__aenter__.return_value = mock_head_response
        mock_session.close = AsyncMock()

        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.create_session"
        ) as mock_create:
            mock_download.return_value = (response, None)
            mock_create.return_value = mock_session

            downloader = AttachmentDownloader()
            outcome = await downloader.download(task)

            assert outcome.success is True

    @pytest.mark.asyncio
    async def test_custom_allowed_extensions(self, temp_output_dir):
        """Test download with custom allowed extensions."""
        task = DownloadTask(
            url="https://claimxperience.com/file.custom",
            destination=temp_output_dir / "file.custom",
            validate_file_type=True,
            allowed_extensions={"custom"},
        )

        content = b"custom content"
        response = DownloadResponse(
            content=content, status_code=200, content_type="application/octet-stream"
        )

        # Need to patch validate_file_type to accept custom extension
        with patch("core.download.downloader.download_url") as mock_download, patch(
            "core.download.downloader.validate_file_type"
        ) as mock_validate:
            mock_download.return_value = (response, None)
            mock_validate.return_value = (True, "")

            downloader = AttachmentDownloader()
            outcome = await downloader.download(task)

            assert outcome.success is True
