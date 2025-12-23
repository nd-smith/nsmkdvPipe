"""Log formatters for JSON and console output."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from verisk_pipeline.common.logging.context import get_log_context


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter with context injection.

    Produces one JSON object per line for easy parsing with jq/grep.
    Sanitizes URLs to remove sensitive tokens before logging.
    """

    # Fields to extract from LogRecord extras
    EXTRA_FIELDS = [
        # Existing
        "trace_id",
        "duration_ms",
        "http_status",
        "error_category",
        "error_message",
        "records_processed",
        "records_succeeded",
        "records_failed",
        "batch_size",
        "retry_count",
        "circuit_state",
        "download_url",
        "blob_path",
        # Operation tracking
        "operation",
        "table",
        "primary_keys",
        "rows_read",
        "rows_written",
        "rows_merged",
        "rows_inserted",
        "rows_updated",
        "columns",
        "limit",
        # Identifiers
        "event_id",
        "event_type",
        "project_id",
        "media_id",
        "assignment_id",
        "status_subtype",
        "resource",
        # Memory tracking
        "checkpoint",
        "memory_mb",
        "df_rows",
        "df_cols",
        # API tracking
        "api_endpoint",
        "api_method",
        "api_calls",
    ]

    # Fields that contain URLs and should be sanitized
    URL_FIELDS = ["download_url", "blob_path", "url"]

    def __init__(self):
        super().__init__()
        # Lazy import to avoid circular dependency
        self._sanitize_url = None

    def _get_sanitizer(self):
        """Lazy load URL sanitizer to avoid circular import."""
        if self._sanitize_url is None:
            try:
                from verisk_pipeline.common.security import sanitize_url

                self._sanitize_url = sanitize_url
            except ImportError:
                # Fallback: no sanitization if security module not available
                self._sanitize_url = lambda x: x
        return self._sanitize_url

    def _sanitize_value(self, key: str, value: Any) -> Any:
        """Sanitize value if it's a URL field."""
        if key in self.URL_FIELDS and isinstance(value, str):
            return self._get_sanitizer()(value)
        return value

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON with sanitized URLs."""
        log_entry: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Inject context variables
        ctx = get_log_context()
        if ctx["domain"]:
            log_entry["domain"] = ctx["domain"]
        if ctx["stage"]:
            log_entry["stage"] = ctx["stage"]
        if ctx["cycle_id"]:
            log_entry["cycle_id"] = ctx["cycle_id"]
        if ctx["worker_id"]:
            log_entry["worker_id"] = ctx["worker_id"]

        # Add source location for DEBUG/ERROR
        if record.levelno in (logging.DEBUG, logging.ERROR, logging.CRITICAL):
            log_entry["file"] = f"{record.filename}:{record.lineno}"

        # Extract extra fields with sanitization
        for field in self.EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                log_entry[field] = self._sanitize_value(field, value)

        # Include exception info
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable console formatter.

    Includes context when available.
    """

    def format(self, record: logging.LogRecord) -> str:
        ctx = get_log_context()

        # Build prefix
        parts = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            record.levelname,
        ]

        if ctx["domain"]:
            parts.append(f"[{ctx['domain']}]")
        if ctx["stage"]:
            parts.append(f"[{ctx['stage']}]")

        prefix = " - ".join(parts)

        # Add trace_id if present
        trace_id = getattr(record, "trace_id", None)
        if trace_id:
            return f"{prefix} - [{trace_id[:8]}] {record.getMessage()}"

        return f"{prefix} - {record.getMessage()}"
