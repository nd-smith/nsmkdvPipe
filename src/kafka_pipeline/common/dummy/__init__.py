"""
Dummy data source for testing the Kafka pipeline without external dependencies.

Provides realistic-looking insurance claim data for both XACT and ClaimX domains,
allowing pipeline testing without access to production data sources.
"""

from kafka_pipeline.common.dummy.source import DummyDataSource, DummySourceConfig
from kafka_pipeline.common.dummy.file_server import DummyFileServer

__all__ = ["DummyDataSource", "DummySourceConfig", "DummyFileServer"]
