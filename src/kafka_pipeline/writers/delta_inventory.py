"""
DEPRECATED: This module has moved to kafka_pipeline.xact.writers.delta_inventory

This is a temporary backward compatibility shim.
Please update your imports to use:
    from kafka_pipeline.xact.writers import DeltaInventoryWriter, DeltaFailedAttachmentsWriter
"""

import warnings

# Re-export from new location for backward compatibility
from kafka_pipeline.xact.writers.delta_inventory import (
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
)

# Warn about deprecated import path
warnings.warn(
    "Importing from kafka_pipeline.writers.delta_inventory is deprecated. "
    "Please use kafka_pipeline.xact.writers.delta_inventory instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["DeltaInventoryWriter", "DeltaFailedAttachmentsWriter"]
