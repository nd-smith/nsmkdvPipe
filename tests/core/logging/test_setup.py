"""Tests for logging setup functions."""

import logging
import tempfile
from pathlib import Path

import pytest

from core.logging.context import clear_log_context, get_log_context, set_log_context
from core.logging.filters import StageContextFilter
from core.logging.setup import setup_multi_worker_logging


class TestSetupMultiWorkerLogging:
    """Tests for setup_multi_worker_logging function."""

    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test."""
        clear_log_context()
        yield
        clear_log_context()
        # Clear root logger handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

    def test_creates_per_worker_file_handlers(self, tmp_path):
        """Creates separate file handlers for each worker."""
        workers = ["download", "upload"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
        )

        root_logger = logging.getLogger()

        # Should have: 1 console + 2 worker files + 1 combined = 4 handlers
        assert len(root_logger.handlers) == 4

        # Count file handlers with filters
        filtered_handlers = [
            h for h in root_logger.handlers
            if hasattr(h, 'filters') and any(
                isinstance(f, StageContextFilter) for f in h.filters
            )
        ]
        assert len(filtered_handlers) == 2

    def test_creates_log_files_in_correct_structure(self, tmp_path):
        """Log files are created with correct domain/date/filename structure."""
        workers = ["download"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
        )

        # Find created log files
        log_files = list(tmp_path.rglob("*.log"))

        # Should have download worker file + combined pipeline file
        assert len(log_files) == 2

        # Check that kafka domain folder exists
        kafka_dir = tmp_path / "kafka"
        assert kafka_dir.exists()

    def test_filters_route_logs_correctly(self, tmp_path):
        """Logs are routed to correct files based on stage context."""
        workers = ["download", "upload"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
        )

        # Set download context and log
        set_log_context(stage="download")
        download_logger = logging.getLogger("test.download")
        download_logger.info("Download message")

        # Set upload context and log
        set_log_context(stage="upload")
        upload_logger = logging.getLogger("test.upload")
        upload_logger.info("Upload message")

        # Force handlers to flush
        for handler in logging.getLogger().handlers:
            handler.flush()

        # Find log files
        log_files = list(tmp_path.rglob("*.log"))

        # Read each file and verify content
        download_file = None
        upload_file = None
        combined_file = None

        for log_file in log_files:
            content = log_file.read_text()
            if "download" in log_file.name and "pipeline" not in log_file.name:
                download_file = content
            elif "upload" in log_file.name:
                upload_file = content
            elif "pipeline" in log_file.name:
                combined_file = content

        # Download file should have download message, not upload
        assert download_file is not None
        assert "Download message" in download_file
        assert "Upload message" not in download_file

        # Upload file should have upload message, not download
        assert upload_file is not None
        assert "Upload message" in upload_file
        assert "Download message" not in upload_file

        # Combined file should have both
        assert combined_file is not None
        assert "Download message" in combined_file
        assert "Upload message" in combined_file

    def test_suppresses_noisy_loggers(self, tmp_path):
        """Noisy loggers are set to WARNING level."""
        setup_multi_worker_logging(
            workers=["download"],
            domain="kafka",
            log_dir=tmp_path,
            suppress_noisy=True,
        )

        # Check that noisy loggers are suppressed
        azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
        assert azure_logger.level == logging.WARNING

    def test_console_handler_receives_all_logs(self, tmp_path, capsys):
        """Console handler receives logs from all workers."""
        workers = ["download", "upload"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
            console_level=logging.INFO,
        )

        # Log from both contexts
        set_log_context(stage="download")
        logging.getLogger("test").info("Download test")

        set_log_context(stage="upload")
        logging.getLogger("test").info("Upload test")

        # Capture console output
        captured = capsys.readouterr()

        # Console should have both messages
        assert "Download test" in captured.out
        assert "Upload test" in captured.out
