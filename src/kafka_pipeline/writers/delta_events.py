"""
DEPRECATED: This module has moved to kafka_pipeline.xact.writers.delta_events

This is a temporary backward compatibility shim.
Please update your imports to use:
    from kafka_pipeline.xact.writers import DeltaEventsWriter
"""

import warnings

# Re-export from new location for backward compatibility
from kafka_pipeline.xact.writers.delta_events import DeltaEventsWriter

# Warn about deprecated import path
warnings.warn(
    "Importing from kafka_pipeline.writers.delta_events is deprecated. "
    "Please use kafka_pipeline.xact.writers.delta_events instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["DeltaEventsWriter"]
