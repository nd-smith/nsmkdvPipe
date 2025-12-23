"""
Shared utilities for ClaimX event handlers.

Consolidates type conversion, timestamp handling, and timing utilities
previously duplicated across handler modules.
"""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


def safe_int(value: Any) -> Optional[int]:
    """Safely convert to int, return None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_str(value: Any) -> Optional[str]:
    """Safely convert to string, return None for empty."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_bool(value: Any) -> Optional[bool]:
    """Safely convert to boolean."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def safe_float(value: Any) -> Optional[float]:
    """Safely convert to float, return None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_decimal_str(value: Any) -> Optional[str]:
    """
    Safely convert to decimal string for precise storage.

    Returns string representation to avoid float precision issues
    when storing in Delta tables.
    """
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_timestamp(value: Any) -> Optional[str]:
    """
    Parse timestamp to ISO format string.

    Handles datetime objects, ISO strings, and Z-suffix normalization.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        return s.replace("Z", "+00:00") if s else None
    return None


def now_iso() -> str:
    """Return current UTC time as ISO format string."""
    return datetime.now(timezone.utc).isoformat()


def elapsed_ms(start: datetime) -> int:
    """Calculate milliseconds elapsed since start time."""
    return int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
