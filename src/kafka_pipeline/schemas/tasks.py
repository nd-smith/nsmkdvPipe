"""
Task message schemas - moved to xact domain.

This module re-exports from kafka_pipeline.xact.schemas.tasks.
"""

from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

__all__ = ["DownloadTaskMessage"]
