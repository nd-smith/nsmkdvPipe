"""
Retry handling for xact domain.

Provides retry routing and scheduling for:
- Download tasks: DownloadRetryHandler routes to retry topics or DLQ
- Delta batch writes: DeltaRetryHandler routes failed batches to retry or DLQ
- Delayed redelivery: DeltaBatchRetryScheduler handles scheduled retry attempts
"""

from kafka_pipeline.xact.retry.handler import DeltaRetryHandler
from kafka_pipeline.xact.retry.scheduler import DeltaBatchRetryScheduler
from kafka_pipeline.xact.retry.download_retry import DownloadRetryHandler, RetryHandler

__all__ = [
    # Download task retry handling
    "DownloadRetryHandler",
    "RetryHandler",  # Backwards compatibility alias for DownloadRetryHandler
    # Delta batch retry handling
    "DeltaRetryHandler",
    "DeltaBatchRetryScheduler",
]
