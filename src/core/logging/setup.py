"""Logging setup and configuration."""

import io
import logging
import os
import secrets
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional

from core.logging.context import set_log_context
from core.logging.filters import StageContextFilter
from core.logging.formatters import ConsoleFormatter, JSONFormatter

# Default settings
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_BACKUP_COUNT = 5
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG

# Noisy loggers to suppress
NOISY_LOGGERS = [
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.identity",
    "urllib3",
    "aiohttp",
    "aiokafka",
]


def get_log_file_path(
    log_dir: Path,
    domain: Optional[str] = None,
    stage: Optional[str] = None,
    instance_id: Optional[str] = None,
) -> Path:
    """
    Build log file path with domain/date subfolder structure.

    Structure: {log_dir}/{domain}/{YYYY-MM-DD}/{domain}_{stage}_{YYYYMMDD}[_instance].log

    When instance_id is provided, it's appended to the filename to prevent
    file locking conflicts when multiple workers of the same type run
    concurrently.

    Args:
        log_dir: Base log directory
        domain: Pipeline domain (xact, claimx, kafka)
        stage: Stage name (ingest, download, etc.)
        instance_id: Unique instance identifier (e.g., process ID) for
            multi-worker scenarios

    Returns:
        Full path to log file
    """
    date_folder = datetime.now().strftime("%Y-%m-%d")
    date_str = datetime.now().strftime("%Y%m%d")

    # Build base filename
    if domain and stage:
        base_name = f"{domain}_{stage}_{date_str}"
    elif domain:
        base_name = f"{domain}_{date_str}"
    elif stage:
        base_name = f"{stage}_{date_str}"
    else:
        base_name = f"pipeline_{date_str}"

    # Append instance ID if provided (for multi-worker isolation)
    if instance_id:
        filename = f"{base_name}_{instance_id}.log"
    else:
        filename = f"{base_name}.log"

    # Build path with subfolders
    if domain:
        log_path = log_dir / domain / date_folder / filename
    else:
        log_path = log_dir / date_folder / filename

    return log_path


def setup_logging(
    name: str = "pipeline",
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
    use_instance_id: bool = True,
) -> logging.Logger:
    """
    Configure logging with console and rotating file handlers.

    Log files are organized by domain and date:
        logs/xact/2025-01-15/xact_download_20250115.log
        logs/kafka/2025-01-15/kafka_consumer_20250115.log

    When use_instance_id is True (default), the process ID is appended to
    the log filename to prevent file locking conflicts when multiple
    workers of the same type run concurrently:
        logs/kafka/2025-01-15/kafka_consumer_20250115_p12345.log

    Args:
        name: Logger name and log file prefix
        stage: Stage name for per-stage log files (ingest/download/retry)
        domain: Pipeline domain (xact, claimx, kafka)
        log_dir: Directory for log files (default: ./logs)
        json_format: Use JSON format for file logs (default: True)
        console_level: Console handler level (default: INFO)
        file_level: File handler level (default: DEBUG)
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
        suppress_noisy: Quiet down Azure SDK and HTTP client loggers
        worker_id: Worker identifier for context
        use_instance_id: Append process ID to log filename for multi-worker
            isolation (default: True). Set to False for single-worker
            deployments or when log aggregation is preferred.

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

    # Generate instance ID from process ID for multi-worker isolation
    instance_id = f"p{os.getpid()}" if use_instance_id else None

    # Build log file path with subfolders
    log_file = get_log_file_path(
        log_dir, domain=domain, stage=stage, instance_id=instance_id
    )

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


def setup_multi_worker_logging(
    workers: List[str],
    domain: str = "kafka",
    log_dir: Optional[Path] = None,
    json_format: bool = True,
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    suppress_noisy: bool = True,
    use_instance_id: bool = True,
) -> logging.Logger:
    """
    Configure logging with per-worker file handlers.

    Creates one RotatingFileHandler per worker type, each filtered
    to only receive logs from that worker's context. Also creates
    a combined log file that receives all logs.

    Log files are organized by domain and date:
        logs/kafka/2025-01-15/kafka_download_20250115.log
        logs/kafka/2025-01-15/kafka_upload_20250115.log
        logs/kafka/2025-01-15/kafka_pipeline_20250115.log  (combined)

    When use_instance_id is True (default), the process ID is appended to
    log filenames to prevent file locking conflicts when multiple instances
    of the same worker configuration run concurrently.

    Args:
        workers: List of worker stage names (e.g., ["download", "upload"])
        domain: Pipeline domain (default: "kafka")
        log_dir: Directory for log files (default: ./logs)
        json_format: Use JSON format for file logs (default: True)
        console_level: Console handler level (default: INFO)
        file_level: File handler level (default: DEBUG)
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
        suppress_noisy: Quiet down Azure SDK and HTTP client loggers
        use_instance_id: Append process ID to log filenames for multi-instance
            isolation (default: True)

    Returns:
        Configured logger instance
    """
    log_dir = log_dir or DEFAULT_LOG_DIR

    # Generate instance ID from process ID for multi-instance isolation
    instance_id = f"p{os.getpid()}" if use_instance_id else None

    # Create formatters
    if json_format:
        file_formatter = JSONFormatter()
    else:
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
        )
    console_formatter = ConsoleFormatter()

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture all, handlers filter
    root_logger.handlers.clear()

    # Add console handler (receives all logs)
    if sys.platform == "win32":
        safe_stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )
        console_handler = logging.StreamHandler(safe_stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Add per-worker file handlers
    for worker in workers:
        log_file = get_log_file_path(
            log_dir, domain=domain, stage=worker, instance_id=instance_id
        )
        log_file.parent.mkdir(parents=True, exist_ok=True)

        handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setLevel(file_level)
        handler.setFormatter(file_formatter)
        handler.addFilter(StageContextFilter(worker))
        root_logger.addHandler(handler)

    # Add combined file handler (no filter - receives all logs)
    combined_file = get_log_file_path(
        log_dir, domain=domain, stage="pipeline", instance_id=instance_id
    )
    combined_file.parent.mkdir(parents=True, exist_ok=True)
    combined_handler = RotatingFileHandler(
        combined_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    combined_handler.setLevel(file_level)
    combined_handler.setFormatter(file_formatter)
    root_logger.addHandler(combined_handler)

    # Suppress noisy loggers
    if suppress_noisy:
        for logger_name in NOISY_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger = logging.getLogger("kafka_pipeline")
    logger.debug(
        f"Multi-worker logging initialized: workers={workers}, domain={domain}",
        extra={"stage": "pipeline", "domain": domain},
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
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = secrets.token_hex(2)
    return f"c-{ts}-{suffix}"
