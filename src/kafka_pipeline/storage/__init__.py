"""
Storage operations for Kafka pipeline workers.

Provides async-friendly storage clients for uploading downloaded
attachments to OneLake (Azure Data Lake Storage Gen2).
"""

from kafka_pipeline.storage.onelake_client import OneLakeClient

__all__ = ["OneLakeClient"]
