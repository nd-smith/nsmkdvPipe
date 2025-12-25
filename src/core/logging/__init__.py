"""
Structured logging module.

Provides JSON logging with correlation IDs and context propagation.
"""

from core.logging.context import (
    clear_log_context,
    get_log_context,
    set_log_context,
)
from core.logging.formatters import ConsoleFormatter, JSONFormatter
from core.logging.setup import (
    generate_cycle_id,
    get_log_file_path,
    get_logger,
    setup_logging,
)

__all__ = [
    # Setup
    "setup_logging",
    "get_logger",
    "generate_cycle_id",
    "get_log_file_path",
    # Formatters
    "JSONFormatter",
    "ConsoleFormatter",
    # Context
    "set_log_context",
    "get_log_context",
    "clear_log_context",
]
