"""
Processed event caching service.

Provides in-memory cache of processed event IDs to avoid expensive Delta table reads.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import polars as pl

from verisk_pipeline.storage.delta import DeltaTableReader
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_exception, log_with_context

logger = get_logger(__name__)


class ProcessedEventCache:
    """
    In-memory cache of processed event IDs.

    Avoids expensive Delta table reads every cycle.
    """

    def __init__(
        self,
        event_log_reader: DeltaTableReader,
        lookback_days: int = 7,
    ):
        self._reader = event_log_reader
        self._lookback_days = lookback_days
        self._processed_ids: set[str] = set()
        self._initialized: bool = False
        self._last_bootstrap: Optional[datetime] = None

    def _bootstrap(self) -> None:
        """Load processed IDs from Delta table."""
        if self._initialized:
            return

        start_time = time.time()

        try:
            if not self._reader.exists():
                log_with_context(
                    logger,
                    logging.INFO,
                    "Event log table does not exist, starting with empty cache",
                )
                self._initialized = True
                self._last_bootstrap = datetime.now(timezone.utc)
                return

            lookback_cutoff = datetime.now(timezone.utc) - timedelta(
                days=self._lookback_days
            )

            log_with_context(
                logger,
                logging.INFO,
                "Bootstrapping processed event cache from Delta",
                lookback_days=self._lookback_days,
                lookback_cutoff=lookback_cutoff.isoformat(),
            )

            log_df: Optional[pl.DataFrame] = self._reader.read_filtered(
                filter_expr=(
                    pl.col("status").is_in(["success", "failed_permanent"])
                    & (pl.col("processed_at") >= lookback_cutoff.isoformat())
                ),
                columns=["event_id"],
            )

            if log_df is not None and not log_df.is_empty():
                self._processed_ids = set(log_df["event_id"].to_list())

            elapsed_ms = (time.time() - start_time) * 1000
            self._initialized = True
            self._last_bootstrap = datetime.now(timezone.utc)

            log_with_context(
                logger,
                logging.INFO,
                "Processed event cache bootstrapped",
                cache_size=len(self._processed_ids),
                duration_ms=round(elapsed_ms, 2),
                lookback_days=self._lookback_days,
            )

        except Exception as e:
            log_exception(
                logger,
                e,
                "Failed to bootstrap processed event cache, proceeding with empty cache",
                level=logging.WARNING,
                include_traceback=False,
            )
            self._initialized = True
            self._last_bootstrap = datetime.now(timezone.utc)

    def is_processed(self, event_id: str) -> bool:
        self._bootstrap()
        return event_id in self._processed_ids

    def filter_unprocessed(self, events_df: pl.DataFrame) -> pl.DataFrame:
        self._bootstrap()

        if not self._processed_ids:
            return events_df

        before_count = len(events_df)
        filtered = events_df.filter(
            ~pl.col("event_id").is_in(list(self._processed_ids))
        )
        after_count = len(filtered)

        log_with_context(
            logger,
            logging.DEBUG,
            "Filtered processed events from cache",
            before_filter=before_count,
            after_filter=after_count,
            filtered_out=before_count - after_count,
            cache_size=len(self._processed_ids),
        )

        return filtered

    def mark_processed(self, event_id: str) -> None:
        self._processed_ids.add(event_id)

    def mark_processed_batch(self, event_ids: List[str]) -> None:
        if not event_ids:
            return

        self._processed_ids.update(event_ids)
        log_with_context(
            logger,
            logging.DEBUG,
            "Marked events as processed in cache",
            batch_size=len(event_ids),
            cache_size=len(self._processed_ids),
        )

    def size(self) -> int:
        return len(self._processed_ids)

    def is_initialized(self) -> bool:
        return self._initialized

    def get_diagnostics(self) -> Dict[str, Any]:
        return {
            "initialized": self._initialized,
            "cache_size": len(self._processed_ids),
            "lookback_days": self._lookback_days,
            "last_bootstrap": (
                self._last_bootstrap.isoformat() if self._last_bootstrap else None
            ),
        }
