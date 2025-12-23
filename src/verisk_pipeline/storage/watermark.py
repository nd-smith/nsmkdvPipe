"""
Watermark management for tracking processing progress.

Supports file-based persistence with optional Delta table fallback.

All datetimes are UTC-aware (timezone.utc).
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.decorators import logged_operation, LoggedClass

logger = get_logger(__name__)

# Default watermark directory
DEFAULT_WATERMARK_DIR = Path(__file__).parent.parent.parent


class WatermarkManager(LoggedClass):
    """
    Manages watermark timestamps for incremental processing.

    Watermarks track the last successfully processed timestamp,
    enabling resumable processing after restarts.

    All datetimes returned are UTC-aware. Naive datetimes passed to set()
    are assumed to be UTC.

    Usage:
        wm = WatermarkManager("xact_events")

        # Get last processed time (returns UTC-aware datetime)
        last = wm.get(default=datetime(2024, 1, 1, tzinfo=timezone.utc))

        # Process events after 'last'...

        # Update watermark (accepts UTC-aware datetime)
        wm.set(new_max_timestamp)
    """

    log_component = "watermark"

    def __init__(
        self,
        name: str,
        storage_dir: Optional[Path] = None,
    ):
        """
        Args:
            name: Unique name for this watermark (used in filename)
            storage_dir: Storage directory (default: project root)
        """
        self.name = name
        self.storage_dir = storage_dir or DEFAULT_WATERMARK_DIR
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self._file_path = self.storage_dir / f".watermark_{name}.txt"
        self._cached_value: Optional[datetime] = None

        super().__init__()

    @property
    def file_path(self) -> Path:
        """Path to watermark file."""
        return self._file_path

    @logged_operation(level=logging.DEBUG)
    def get(self, default: Optional[datetime] = None) -> Optional[datetime]:
        """
        Get current watermark value as UTC-aware datetime.

        Args:
            default: Value to return if no watermark exists.
                     Will be made UTC-aware if naive.

        Returns:
            UTC-aware datetime or None
        """
        # Return cached if available
        if self._cached_value is not None:
            self._log(
                logging.DEBUG,
                "Using cached watermark",
                value=self._cached_value.isoformat(),
            )
            return self._cached_value

        # Try to read from file
        value = self._read_file()
        if value is not None:
            self._cached_value = value
            return value

        # Return default, ensuring it's UTC-aware
        if default is not None and default.tzinfo is None:
            default = default.replace(tzinfo=timezone.utc)
        return default

    @logged_operation(level=logging.DEBUG)
    def set(self, timestamp: datetime) -> None:
        """
        Update watermark to new timestamp.

        Args:
            timestamp: New watermark value. Naive datetimes are assumed UTC.
        """
        # Ensure UTC-aware for storage
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC if different timezone
            timestamp = timestamp.astimezone(timezone.utc)

        self._write_file(timestamp)
        self._cached_value = timestamp
        self._log(
            logging.INFO,
            "Watermark updated",
            watermark_name=self.name,
            value=timestamp.isoformat(),
        )

    @logged_operation(level=logging.DEBUG)
    def clear(self) -> None:
        """Clear watermark (delete file and cache)."""
        try:
            if self._file_path.exists():
                self._file_path.unlink()
            self._cached_value = None
            self._log(
                logging.DEBUG,
                "Watermark cleared",
                watermark_name=self.name,
            )
        except Exception as e:
            self._log_exception(
                e,
                "Failed to clear watermark",
                level=logging.WARNING,
                watermark_name=self.name,
            )

    def exists(self) -> bool:
        """Check if watermark file exists."""
        return self._file_path.exists()

    def _read_file(self) -> Optional[datetime]:
        """Read watermark from file, always returning UTC-aware datetime."""
        try:
            if not self._file_path.exists():
                return None

            text = self._file_path.read_text().strip()
            if not text:
                return None

            # Parse ISO format
            timestamp = datetime.fromisoformat(text)

            # Ensure UTC-aware (handle old naive watermarks gracefully)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            self._log(
                logging.DEBUG,
                "Read watermark from file",
                watermark_name=self.name,
                value=timestamp.isoformat(),
            )
            return timestamp

        except ValueError as e:
            self._log_exception(
                e,
                "Invalid watermark format",
                level=logging.WARNING,
                watermark_name=self.name,
                file_path=str(self._file_path),
            )
            return None
        except Exception as e:
            self._log_exception(
                e,
                "Failed to read watermark",
                level=logging.WARNING,
                watermark_name=self.name,
            )
            return None

    def _write_file(self, timestamp: datetime) -> None:
        """Write watermark atomically. Timestamp should already be UTC-aware."""
        temp_path = self._file_path.with_suffix(".tmp")
        try:
            temp_path.write_text(timestamp.isoformat())
            temp_path.replace(self._file_path)  # Atomic on POSIX
        except Exception as e:
            self._log_exception(
                e,
                "Failed to write watermark",
                watermark_name=self.name,
            )
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise

    def get_age(self) -> Optional[timedelta]:
        """
        Get age of current watermark (time since watermark timestamp).

        Returns:
            Timedelta since watermark, or None if no watermark
        """
        value = self.get()
        if value is None:
            return None
        return datetime.now(timezone.utc) - value

    def is_stale(self, max_age: timedelta) -> bool:
        """
        Check if watermark is older than max_age.

        Args:
            max_age: Maximum acceptable age

        Returns:
            True if watermark is stale or doesn't exist
        """
        age = self.get_age()
        if age is None:
            return True
        return age > max_age

    def get_diagnostics(self) -> dict:
        """Get diagnostic info for health checks."""
        value = self.get()
        age = self.get_age()

        return {
            "name": self.name,
            "file_path": str(self._file_path),
            "exists": self.exists(),
            "value": value.isoformat() if value else None,
            "age_seconds": age.total_seconds() if age else None,
        }


# Registry for named watermark managers
_managers: dict[str, WatermarkManager] = {}


def get_watermark_manager(
    name: str,
    storage_dir: Optional[Path] = None,
) -> WatermarkManager:
    """
    Get or create a named watermark manager.

    Args:
        name: Unique name for the watermark
        storage_dir: Storage directory (only used on first creation)

    Returns:
        WatermarkManager instance
    """
    if name not in _managers:
        _managers[name] = WatermarkManager(name, storage_dir)
    return _managers[name]
