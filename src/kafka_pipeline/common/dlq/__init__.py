"""
Dead letter queue handling - DEPRECATED.

This module has been moved to the xact domain. Import from the new location:
    from kafka_pipeline.xact.dlq import DLQHandler

This backwards-compatible re-export will be removed in a future version.
"""

import warnings

# Re-export from new location for backwards compatibility
from kafka_pipeline.xact.dlq import DLQHandler

# Emit deprecation warning on import
warnings.warn(
    "Importing DLQHandler from kafka_pipeline.common.dlq is deprecated. "
    "Use 'from kafka_pipeline.xact.dlq import DLQHandler' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "DLQHandler",
]
