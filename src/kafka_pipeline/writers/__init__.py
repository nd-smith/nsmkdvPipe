"""
DEPRECATED: Writers have moved to domain-specific packages.

This is a temporary backward compatibility shim.
Please update your imports to use:
    from kafka_pipeline.xact.writers import DeltaEventsWriter, DeltaInventoryWriter

Xact writers are now in kafka_pipeline.xact.writers
"""

import warnings

# Re-export from new location for backward compatibility
from kafka_pipeline.xact.writers import (
    DeltaEventsWriter,
    DeltaFailedAttachmentsWriter,
    DeltaInventoryWriter,
)

# Warn about deprecated import path
warnings.warn(
    "Importing from kafka_pipeline.writers is deprecated. "
    "Please use kafka_pipeline.xact.writers instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["DeltaEventsWriter", "DeltaInventoryWriter", "DeltaFailedAttachmentsWriter"]
