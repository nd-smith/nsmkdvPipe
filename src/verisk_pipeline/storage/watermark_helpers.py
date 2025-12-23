"""
Watermark management helpers for incremental processing.

Provides context manager and utilities to centralize common watermark patterns:
- Retrieval with lookback defaults
- Timestamp extraction from DataFrames and events
- Update with automatic change logging

All datetimes are UTC-aware (timezone.utc).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any

import polars as pl

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context
from verisk_pipeline.storage.watermark import WatermarkManager

logger = get_logger(__name__)


def extract_max_timestamp(
    df: pl.DataFrame,
    column: str = "ingested_at",
    default: Optional[datetime] = None,
) -> Optional[datetime]:
    """
    Extract maximum timestamp from DataFrame column.

    Args:
        df: DataFrame to extract from
        column: Timestamp column name
        default: Value to return if extraction fails

    Returns:
        Maximum timestamp or default

    Example:
        >>> df = pl.DataFrame({"ingested_at": [dt1, dt2, dt3]})
        >>> max_ts = extract_max_timestamp(df, "ingested_at")
    """
    if df.is_empty():
        return default

    try:
        max_ts = df.select(pl.col(column).max()).item()
        return max_ts if max_ts else default
    except Exception as e:
        logger.warning(f"Failed to extract max timestamp from column '{column}': {e}")
        return default


def extract_max_from_events(
    events: List[Any],
    attr: str = "ingested_at",
    default: Optional[datetime] = None,
) -> Optional[datetime]:
    """
    Extract max timestamp from list of event objects.

    Handles both datetime objects and ISO format strings.
    Automatically converts string timestamps with 'Z' suffix.

    Args:
        events: List of event objects
        attr: Attribute name containing timestamp
        default: Value to return if extraction fails

    Returns:
        Maximum timestamp or default

    Example:
        >>> events = [Event(ingested_at=dt1), Event(ingested_at=dt2)]
        >>> max_ts = extract_max_from_events(events, "ingested_at")
    """
    if not events:
        return default

    try:
        max_time = max(
            (getattr(e, attr) for e in events if getattr(e, attr, None) is not None),
            default=default,
        )

        if max_time is None:
            return default

        # Handle string timestamps
        if isinstance(max_time, str):
            max_time_str = str(max_time).replace("Z", "+00:00")
            max_time = datetime.fromisoformat(max_time_str)

        return max_time
    except Exception as e:
        logger.warning(f"Failed to extract max from events (attr='{attr}'): {e}")
        return default


class WatermarkSession:
    """
    Context manager for watermark-based processing with automatic updates.

    Encapsulates common watermark lifecycle:
    1. Retrieve watermark with lookback default on enter
    2. Track new maximum timestamp during processing
    3. Update watermark and log changes on successful exit

    Usage:
        with WatermarkSession(
            watermark_manager,
            lookback_days=7,
            timestamp_column="ingested_at",
            logger=stage_logger
        ) as wm_session:
            watermark = wm_session.start_watermark
            events = fetch_events_after(watermark)
            # ... process events ...
            wm_session.update_from_dataframe(events_df)
            # Automatic watermark update and logging on exit

    The watermark will only be updated if:
    - No exception occurred during processing
    - A new watermark was provided via update_from_*() methods
    - The new watermark is different from the start watermark
    """

    def __init__(
        self,
        watermark_manager: WatermarkManager,
        lookback_days: int,
        timestamp_column: str = "ingested_at",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize watermark session.

        Args:
            watermark_manager: WatermarkManager instance
            lookback_days: Days to look back for default watermark
            timestamp_column: Column name for timestamp extraction from DataFrames
            logger: Logger instance (defaults to module logger)
        """
        self.watermark = watermark_manager
        self.lookback_days = lookback_days
        self.timestamp_column = timestamp_column
        self.logger = logger or globals()["logger"]
        self.start_watermark: Optional[datetime] = None
        self.new_watermark: Optional[datetime] = None

    def __enter__(self) -> "WatermarkSession":
        """
        Get watermark with lookback default and log retrieval.

        Returns:
            Self for context manager protocol
        """
        lookback = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        self.start_watermark = self.watermark.get(default=lookback)

        # Handle None case (shouldn't happen with default, but be defensive)
        if self.start_watermark is None:
            self.start_watermark = lookback

        log_with_context(
            self.logger,
            logging.DEBUG,
            "Watermark session starting",
            watermark=self.start_watermark.isoformat(),
            lookback_days=self.lookback_days,
        )
        return self

    def update_from_dataframe(self, df: pl.DataFrame) -> None:
        """
        Extract and store max timestamp from DataFrame.

        The watermark will be updated on context exit if this timestamp
        is different from the start watermark.

        Args:
            df: DataFrame containing timestamp column
        """
        if df.is_empty():
            return

        max_ts = extract_max_timestamp(df, self.timestamp_column)
        if max_ts:
            self.new_watermark = max_ts

    def update_from_timestamp(self, timestamp: datetime) -> None:
        """
        Manually set new watermark timestamp.

        The watermark will be updated on context exit if this timestamp
        is different from the start watermark.

        Args:
            timestamp: New watermark value
        """
        if timestamp:
            self.new_watermark = timestamp

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Update watermark and log changes on successful exit.

        Watermark is only updated if:
        - No exception occurred (exc_type is None)
        - A new watermark was set
        - The new watermark differs from start watermark

        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised
        """
        if exc_type is None and self.new_watermark:
            if self.new_watermark != self.start_watermark:
                advance_seconds = (
                    (self.new_watermark - self.start_watermark).total_seconds()
                    if self.start_watermark
                    else None
                )

                self.watermark.set(self.new_watermark)

                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Watermark updated",
                    old_watermark=(
                        self.start_watermark.isoformat()
                        if self.start_watermark
                        else None
                    ),
                    new_watermark=self.new_watermark.isoformat(),
                    advance_seconds=advance_seconds,
                )
            else:
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Watermark unchanged",
                    watermark=(
                        self.start_watermark.isoformat()
                        if self.start_watermark
                        else None
                    ),
                )
