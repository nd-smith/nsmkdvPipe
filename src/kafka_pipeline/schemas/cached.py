"""
Cached message schemas - moved to xact domain.

This module re-exports from kafka_pipeline.xact.schemas.cached.
"""

from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage

__all__ = ["CachedDownloadMessage"]
