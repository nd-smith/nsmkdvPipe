"""Logging setup and configuration."""

import io
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from verisk_pipeline.common.logging.constants import (
    DEFAULT_BACKUP_COUNT,
    DEFAULT_CONSOLE_LEVEL,
    DEFAULT_FILE_LEVEL,
    DEFAULT_LOG_DIR,
    DEFAULT_MAX_BYTES,
    NOISY_LOGGERS,
)
from verisk_pipeline.common.logging.context import set_log_context
from verisk_pipeline.common.logging.formatters import ConsoleFormatter, JSONFormatter


def get_log_file_path(
    log_dir: Path,
    domain: Optional[str] = None,
    stage: Optional[str] = None,
) -> Path:
    """
    Build log file path with domain/date subfolder structure.

    Structure: {log_dir}/{domain}/{YYYY-MM-DD}/{domain}_{stage}_{YYYYMMDD}.log

    Args:
        log_dir: Base log directory
        domain: Pipeline domain (xact, claimx)
        stage: Stage name (ingest, download, etc.)

    Returns:
        Full path to log file
    """
    date_folder = datetime.now().strftime("%Y-%m-%d")
    date_str = datetime.now().strftime("%Y%m%d")

    # Build filename
    if domain and stage:
        filename = f"{domain}_{stage}_{date_str}.log"
    elif domain:
        filename = f"{domain}_{date_str}.log"
    elif stage:
        filename = f"{stage}_{date_str}.log"
    else:
        filename = f"verisk_pipeline_{date_str}.log"

    # Build path with subfolders
    if domain:
        log_path = log_dir / domain / date_folder / filename
    else:
        log_path = log_dir / date_folder / filename

    return log_path


def setup_logging(
    name: str = "verisk_pipeline",
    stage: Optional[str] = None,
    domain: Optional[str] = None,
    log_dir: Optional[Path] = None,
    json_format: bool = True,
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    suppress_noisy: bool = True,
    worker_id: Optional[str] = None,
) -> logging.Logger:
    """
    Configure logging with console and rotating file handlers.

    Log files are organized by domain and date:
        logs/xact/2025-01-15/xact_download_20250115.log
        logs/claimx/2025-01-15/claimx_enrich_20250115.log

    Args:
        name: Logger name and log file prefix
        stage: Stage name for per-stage log files (ingest/download/retry)
        domain: Pipeline domain (xact, claimx)
        log_dir: Directory for log files (default: ./logs)
        json_format: Use JSON format for file logs (default: True)
        console_level: Console handler level (default: INFO)
        file_level: File handler level (default: DEBUG)
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
        suppress_noisy: Quiet down Azure SDK and HTTP client loggers
        worker_id: Worker identifier for context

    Returns:
        Configured logger instance
    """
    log_dir = log_dir or DEFAULT_LOG_DIR

    # Set context variables
    if worker_id:
        set_log_context(worker_id=worker_id)
    if stage:
        set_log_context(stage=stage)
    if domain:
        set_log_context(domain=domain)

    # Build log file path with subfolders
    log_file = get_log_file_path(log_dir, domain=domain, stage=stage)

    # Ensure directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create formatters
    if json_format:
        file_formatter = JSONFormatter()
    else:
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
        )
    console_formatter = ConsoleFormatter()

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)

    # Console handler
    if sys.platform == "win32":
        safe_stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )
        console_handler = logging.StreamHandler(safe_stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture all, handlers filter

    # Remove existing handlers to avoid duplicates on re-init
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Suppress noisy loggers
    if suppress_noisy:
        for logger_name in NOISY_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger = logging.getLogger(name)
    logger.debug(
        f"Logging initialized: file={log_file}, json={json_format}",
        extra={"stage": stage or "pipeline", "domain": domain or "unknown"},
    )

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Use this instead of logging.getLogger() to ensure consistent naming.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def generate_cycle_id() -> str:
    """
    Generate unique cycle identifier.

    Format: c-YYYYMMDD-HHMMSS-XXXX where XXXX is random hex.

    Returns:
        Unique cycle ID string
    """
    import secrets

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = secrets.token_hex(2)
    return f"c-{ts}-{suffix}"
