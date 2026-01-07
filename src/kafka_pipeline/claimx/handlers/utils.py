"""
Shared utilities for ClaimX event handlers.

Consolidates type conversion, timestamp handling, and timing utilities
used across handler modules.
"""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


def safe_int(value: Any) -> Optional[int]:
    """
    Safely convert to int, return None on failure.

    Args:
        value: Value to convert

    Returns:
        Integer value or None
    """
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_str(value: Any) -> Optional[str]:
    """
    Safely convert to string, return None for empty.

    Args:
        value: Value to convert

    Returns:
        String value or None if empty
    """
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_bool(value: Any) -> Optional[bool]:
    """
    Safely convert to boolean.

    Args:
        value: Value to convert

    Returns:
        Boolean value or None
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def safe_float(value: Any) -> Optional[float]:
    """
    Safely convert to float, return None on failure.

    Args:
        value: Value to convert

    Returns:
        Float value or None
    """
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

    Args:
        value: Value to convert

    Returns:
        Decimal string or None
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

    Args:
        value: Timestamp value to parse

    Returns:
        ISO format string or None
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
    """
    Return current UTC time as ISO format string.

    Returns:
        Current UTC time in ISO format
    """
    return datetime.now(timezone.utc).isoformat()


def elapsed_ms(start: datetime) -> int:
    """
    Calculate milliseconds elapsed since start time.

    Args:
        start: Start time

    Returns:
        Elapsed milliseconds as integer
    """
    return int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
