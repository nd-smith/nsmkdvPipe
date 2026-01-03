"""
DEPRECATED: This module has moved to kafka_pipeline.common.storage.onelake_client.

This re-export is for backward compatibility only and will be removed in a future version.
Please update your imports to use kafka_pipeline.common.storage.onelake_client instead.
"""

# Re-export everything from the new location
from kafka_pipeline.common.storage.onelake_client import *  # noqa: F401, F403

# For test mocking compatibility - expose LegacyOneLakeClient
# We import the module itself and reference the attribute to ensure
# patches to this module will work
try:
    import kafka_pipeline.common.storage.onelake_client as _common
    LegacyOneLakeClient = _common.LegacyOneLakeClient  # noqa: F405
except (ImportError, AttributeError, PermissionError):
    # If we can't import it (environment issues), create a placeholder
    # Tests will mock this anyway before it's used
    LegacyOneLakeClient = None  # noqa: F405
