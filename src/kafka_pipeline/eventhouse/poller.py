"""
KQL Event Poller - DEPRECATED

This module has moved to kafka_pipeline.common.eventhouse.poller
This file provides backward compatibility re-exports.
"""

# Re-export everything from new location for backward compatibility
# This includes internal imports that tests may patch
from kafka_pipeline.common.eventhouse.poller import *  # noqa: F401, F403

__all__ = [
    "KQLEventPoller",
    "PollerConfig",
]
