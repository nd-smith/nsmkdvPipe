"""
KQL Client - DEPRECATED

This module has moved to kafka_pipeline.common.eventhouse.kql_client
This file provides backward compatibility re-exports.
"""

# Re-export everything from new location for backward compatibility
# This includes internal imports that tests may patch
from kafka_pipeline.common.eventhouse.kql_client import *  # noqa: F401, F403

__all__ = [
    "DEFAULT_CONFIG_PATH",
    "KUSTO_RESOURCE",
    "EventhouseConfig",
    "FileBackedKustoCredential",
    "KQLClient",
    "KQLQueryResult",
]
