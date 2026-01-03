"""
Storage operations for Kafka pipeline workers.

Provides async-friendly storage clients for uploading downloaded
attachments to OneLake (Azure Data Lake Storage Gen2).

DEPRECATED: This module is deprecated. Import from kafka_pipeline.common.storage instead.
Re-exports provided for backward compatibility.
"""

# Re-export from new location for backward compatibility
from kafka_pipeline.common.storage.onelake_client import OneLakeClient

__all__ = ["OneLakeClient"]
