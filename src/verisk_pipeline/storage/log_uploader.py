"""
Upload log files to OneLake for long-term storage and parsing.

Uploads completed log files organized by domain and date to OneLake,
mirroring the local directory structure.

Local:   logs/xact/2025-01-15/xact_download_20250115.log
OneLake: Files/veriskPipeline/logs/xact/2025-01-15/xact_download_20250115.log
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_exception, log_with_context
from verisk_pipeline.common.logging.context_managers import log_operation
from verisk_pipeline.common.logging.decorators import LoggedClass
from verisk_pipeline.storage.onelake import OneLakeClient

logger = get_logger(__name__)


class LogUploader(LoggedClass):
    """
    Upload log files to OneLake organized by domain and date.

    Directory structure:
        Local:   {log_dir}/{domain}/{date}/{domain}_{stage}_{date}.log
        OneLake: {onelake_path}/logs/{domain}/{date}/{domain}_{stage}_{date}.log

    Handles:
    - Uploading completed (not current) log files
    - Tracking which files have been uploaded via marker files
    - Cleaning up old local logs after retention period
    - Supporting multiple domains (xact, claimx)

    Usage:
        uploader = LogUploader(config, domain="xact")
        uploaded = uploader.upload_logs()
        deleted = uploader.cleanup_old_logs()
    """

    def __init__(
        self,
        config,
        domain: Optional[str] = None,
        log_dir: Optional[Path] = None,
    ):
        """
        Args:
            config: Pipeline configuration (PipelineConfig or ClaimXConfig)
            domain: Pipeline domain (xact, claimx). If None, uploads all domains.
            log_dir: Override log directory (default: from config or ./logs)
        """
        self.config = config
        self.domain = domain

        # Determine log directory
        if log_dir:
            self._log_dir = Path(log_dir)
        elif hasattr(config, "logging") and hasattr(config.logging, "log_dir"):
            self._log_dir = Path(config.logging.log_dir)
        else:
            self._log_dir = Path("logs")

        # OneLake destination - under veriskPipeline/logs
        self._onelake_base = config.lakehouse.files_path.rstrip("/")
        self._onelake_logs_path = f"{self._onelake_base}/veriskPipeline/logs"

        # Settings from config
        self._retention_days = config.observability.log_retention_days
        self._enabled = config.observability.log_upload_enabled

        # Client created lazily
        self._client: Optional[OneLakeClient] = None

        super().__init__()

    def _get_client(self) -> OneLakeClient:
        """Get or create OneLake client."""
        if self._client is None:
            self._client = OneLakeClient(self._onelake_base)
        return self._client

    def _get_domains(self) -> List[str]:
        """Get list of domains to process."""
        if self.domain:
            return [self.domain]

        # Auto-detect domains from directory structure
        domains = []
        if self._log_dir.exists():
            for item in self._log_dir.iterdir():
                if item.is_dir() and item.name in ("xact", "claimx"):
                    domains.append(item.name)
        return domains

    def _is_current_log(self, log_file: Path) -> bool:
        """
        Check if file is a current (active) log file.

        Current logs are from today's date folder.
        """
        today = datetime.now().strftime("%Y-%m-%d")

        # Check if file is in today's date folder
        if log_file.parent.name == today:
            return True

        return False

    def _is_uploaded(self, log_file: Path) -> bool:
        """Check if file has been uploaded (marker exists)."""
        marker = log_file.with_suffix(log_file.suffix + ".uploaded")
        return marker.exists()

    def _mark_uploaded(self, log_file: Path) -> None:
        """Create upload marker for file."""
        marker = log_file.with_suffix(log_file.suffix + ".uploaded")
        marker.touch()

    def _get_uploadable_logs(self, domain: str) -> List[Path]:
        """
        Get list of log files ready for upload for a domain.

        Excludes:
        - Current day's active logs
        - Already uploaded files
        - Upload markers
        - Rotated backup files (.log.1, .log.2, etc.)

        Args:
            domain: Domain to scan (xact, claimx)

        Returns:
            List of log file paths ready for upload
        """
        domain_dir = self._log_dir / domain
        if not domain_dir.exists():
            return []

        ready = []

        # Iterate through date folders
        for date_folder in sorted(domain_dir.iterdir()):
            if not date_folder.is_dir():
                continue

            # Skip folders that don't look like dates
            try:
                datetime.strptime(date_folder.name, "%Y-%m-%d")
            except ValueError:
                continue

            # Find log files in this date folder
            for log_file in date_folder.glob("*.log"):
                # Skip markers
                if ".uploaded" in log_file.name:
                    continue

                # Skip rotated backups (.log.1, .log.2)
                if log_file.suffix != ".log":
                    continue

                # Skip current active logs
                if self._is_current_log(log_file):
                    continue

                # Skip already uploaded
                if self._is_uploaded(log_file):
                    continue

                ready.append(log_file)

        return ready

    def _get_onelake_path(self, local_path: Path) -> str:
        """
        Convert local log path to OneLake destination path.

        Local:   logs/xact/2025-01-15/xact_download_20250115.log
        OneLake: veriskPipeline/logs/xact/2025-01-15/xact_download_20250115.log

        Args:
            local_path: Local log file path

        Returns:
            Relative path for OneLake upload
        """
        # Get path relative to log_dir
        # e.g., xact/2025-01-15/xact_download_20250115.log
        rel_path = local_path.relative_to(self._log_dir)

        # Build OneLake path
        return f"veriskPipeline/logs/{rel_path.as_posix()}"

    def upload_logs(self) -> int:
        """
        Upload completed log files to OneLake.

        Uploads all non-current logs from configured domain(s).

        Returns:
            Number of files uploaded
        """
        if not self._enabled:
            log_with_context(logger, logging.DEBUG, "Log upload disabled")
            return 0

        domains = self._get_domains()
        if not domains:
            log_with_context(logger, logging.DEBUG, "No log domains found")
            return 0

        total_uploaded = 0
        client = self._get_client()

        with client:
            for domain in domains:
                logs_to_upload = self._get_uploadable_logs(domain)

                if not logs_to_upload:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "No logs to upload",
                        domain=domain,
                    )
                    continue

                for log_file in logs_to_upload:
                    onelake_path = self._get_onelake_path(log_file)

                    with log_operation(
                        logger,
                        "upload_log",
                        blob_path=onelake_path,
                        domain=domain,
                    ):
                        try:
                            content = log_file.read_bytes()
                            client.upload_bytes(onelake_path, content)
                            self._mark_uploaded(log_file)
                            total_uploaded += 1

                            log_with_context(
                                logger,
                                logging.INFO,
                                "Uploaded log file",
                                blob_path=onelake_path,
                                bytes_written=len(content),
                                domain=domain,
                            )

                        except Exception as e:
                            log_exception(
                                logger,
                                e,
                                "Failed to upload log file",
                                blob_path=str(log_file),
                                domain=domain,
                                level=logging.WARNING,
                            )

        if total_uploaded > 0:
            log_with_context(
                logger,
                logging.INFO,
                "Log upload complete",
                records_processed=total_uploaded,
            )

        return total_uploaded

    def cleanup_old_logs(self) -> int:
        """
        Delete local logs older than retention period.

        Only deletes files that:
        - Have been successfully uploaded (marker exists)
        - Are older than retention_days

        Returns:
            Number of files deleted
        """
        domains = self._get_domains()
        if not domains:
            return 0

        cutoff_date = datetime.now() - timedelta(days=self._retention_days)
        cutoff_folder = cutoff_date.strftime("%Y-%m-%d")
        deleted = 0

        for domain in domains:
            domain_dir = self._log_dir / domain
            if not domain_dir.exists():
                continue

            # Find old date folders
            for date_folder in sorted(domain_dir.iterdir()):
                if not date_folder.is_dir():
                    continue

                # Skip non-date folders
                try:
                    datetime.strptime(date_folder.name, "%Y-%m-%d")
                except ValueError:
                    continue

                # Skip recent folders
                if date_folder.name >= cutoff_folder:
                    continue

                # Delete uploaded files in old folders
                for log_file in date_folder.glob("*.log"):
                    if ".uploaded" in log_file.name:
                        continue

                    marker = log_file.with_suffix(log_file.suffix + ".uploaded")

                    # Only delete if uploaded
                    if not marker.exists():
                        continue

                    try:
                        log_file.unlink()
                        marker.unlink()
                        deleted += 1

                        log_with_context(
                            logger,
                            logging.DEBUG,
                            "Deleted old log",
                            blob_path=str(log_file),
                            domain=domain,
                        )

                    except Exception as e:
                        log_exception(
                            logger,
                            e,
                            "Failed to delete log file",
                            blob_path=str(log_file),
                            domain=domain,
                            level=logging.WARNING,
                        )

                # Remove empty date folders
                try:
                    remaining = list(date_folder.iterdir())
                    if not remaining:
                        date_folder.rmdir()
                        log_with_context(
                            logger,
                            logging.DEBUG,
                            "Removed empty date folder",
                            blob_path=str(date_folder),
                            domain=domain,
                        )
                except Exception:
                    pass  # Folder not empty or other issue

        if deleted > 0:
            log_with_context(
                logger,
                logging.INFO,
                "Cleaned up old log files",
                records_processed=deleted,
            )

        return deleted

    def get_upload_status(self) -> dict:
        """
        Get status of log uploads for monitoring.

        Returns:
            Dict with upload statistics by domain
        """
        domains = self._get_domains()
        status = {}

        for domain in domains:
            domain_dir = self._log_dir / domain
            if not domain_dir.exists():
                status[domain] = {"pending": 0, "uploaded": 0, "current": 0}
                continue

            pending = 0
            uploaded = 0
            current = 0

            for date_folder in domain_dir.iterdir():
                if not date_folder.is_dir():
                    continue

                for log_file in date_folder.glob("*.log"):
                    if ".uploaded" in log_file.name:
                        continue

                    if self._is_current_log(log_file):
                        current += 1
                    elif self._is_uploaded(log_file):
                        uploaded += 1
                    else:
                        pending += 1

            status[domain] = {
                "pending": pending,
                "uploaded": uploaded,
                "current": current,
            }

            self._log(
                logging.DEBUG,
                "Upload status checked",
                domain=domain,
                pending=pending,
                uploaded=uploaded,
                current=current,
            )

        return status

    def force_upload_all(self) -> int:
        """
        Force upload all log files including current ones.

        Use for debugging or shutdown scenarios.

        Returns:
            Number of files uploaded
        """
        if not self._log_dir.exists():
            return 0

        domains = self._get_domains()
        uploaded = 0
        client = self._get_client()

        with client:
            for domain in domains:
                domain_dir = self._log_dir / domain
                if not domain_dir.exists():
                    continue

                for date_folder in domain_dir.iterdir():
                    if not date_folder.is_dir():
                        continue

                    for log_file in date_folder.glob("*.log"):
                        if ".uploaded" in log_file.name:
                            continue
                        if self._is_uploaded(log_file):
                            continue

                        onelake_path = self._get_onelake_path(log_file)

                        try:
                            content = log_file.read_bytes()
                            client.upload_bytes(onelake_path, content)
                            self._mark_uploaded(log_file)
                            uploaded += 1

                            log_with_context(
                                logger,
                                logging.DEBUG,
                                "Force uploaded log file",
                                blob_path=onelake_path,
                                bytes_written=len(content),
                                domain=domain,
                            )

                        except Exception as e:
                            log_exception(
                                logger,
                                e,
                                "Failed to force upload log file",
                                blob_path=str(log_file),
                                domain=domain,
                                level=logging.WARNING,
                            )

        return uploaded

    def close(self) -> None:
        """Close resources."""
        if self._client is not None:
            try:
                self._client.close()
                log_with_context(logger, logging.DEBUG, "OneLake client closed")
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Error closing OneLake client",
                    level=logging.WARNING,
                )
            self._client = None
