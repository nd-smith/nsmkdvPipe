"""
Result message schemas - moved to xact domain.

This module re-exports from kafka_pipeline.xact.schemas.results.
"""

from kafka_pipeline.xact.schemas.results import DownloadResultMessage, FailedDownloadMessage

__all__ = ["DownloadResultMessage", "FailedDownloadMessage"]
