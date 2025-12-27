"""
Pytest fixtures for end-to-end integration tests.

Provides fixtures for:
- All worker instances (event ingester, download worker, result processor)
- Mock OneLake client (in-memory file storage for testing)
- Mock Delta Lake writers (in-memory verification)
- Worker lifecycle management
- Test environment validation
"""

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

# Set test mode environment variables at module import time
# This prevents audit logger from trying to create /var/log/verisk_pipeline
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"
if "AUDIT_LOG_PATH" not in os.environ:
    os.environ["AUDIT_LOG_PATH"] = "/tmp/test_audit.log"

# NOTE: Worker imports are done lazily inside fixtures to ensure
# environment variables are set before module initialization happens
from kafka_pipeline.config import KafkaConfig


class MockOneLakeClient:
    """
    Mock OneLake client for testing without Azure dependencies.

    Stores files in memory and provides verification methods for tests.
    Mimics the async interface of the real OneLakeClient.
    """

    def __init__(self, base_path: str):
        """
        Initialize mock client.

        Args:
            base_path: abfss:// path (ignored in mock, stored for verification)
        """
        self.base_path = base_path
        self.uploaded_files: Dict[str, bytes] = {}
        self.upload_count = 0
        self.is_open = False

    async def __aenter__(self):
        """Async context manager entry."""
        self.is_open = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.is_open = False
        return False

    async def upload_file(self, blob_path: str, local_path: Path) -> None:
        """
        Mock file upload - reads local file and stores in memory.

        Args:
            blob_path: Destination path in OneLake
            local_path: Path to local file to upload

        Raises:
            FileNotFoundError: If local file doesn't exist
        """
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        # Read file content
        content = local_path.read_bytes()

        # Store in memory
        self.uploaded_files[blob_path] = content
        self.upload_count += 1

    async def exists(self, blob_path: str) -> bool:
        """Check if file exists in mock storage."""
        return blob_path in self.uploaded_files

    async def close(self) -> None:
        """Close client connection."""
        self.is_open = False

    def get_uploaded_content(self, blob_path: str) -> Optional[bytes]:
        """Get uploaded file content for verification."""
        return self.uploaded_files.get(blob_path)

    def clear(self) -> None:
        """Clear all uploaded files (useful between tests)."""
        self.uploaded_files.clear()
        self.upload_count = 0


class MockDeltaEventsWriter:
    """
    Mock Delta events writer for testing without Delta Lake dependencies.

    Stores written events in memory and provides verification methods.
    Tracks deduplication behavior.
    """

    def __init__(self, table_path: str, dedupe_window_hours: int = 24):
        """
        Initialize mock writer.

        Args:
            table_path: abfss:// path to Delta table (stored for verification)
            dedupe_window_hours: Deduplication window (affects behavior)
        """
        self.table_path = table_path
        self.dedupe_window_hours = dedupe_window_hours
        self.written_events: List[Dict] = []
        self.write_count = 0
        self.dedupe_hits = 0
        self._seen_trace_ids = set()

    async def write_event(self, event: Dict) -> None:
        """
        Mock event write - stores in memory with deduplication.

        Args:
            event: Event dictionary to write
        """
        trace_id = event.get("trace_id")

        # Simulate deduplication
        if trace_id in self._seen_trace_ids:
            self.dedupe_hits += 1
            return  # Skip duplicate

        self._seen_trace_ids.add(trace_id)
        self.written_events.append(event)
        self.write_count += 1

    def get_events_by_trace_id(self, trace_id: str) -> List[Dict]:
        """Get all events with given trace_id."""
        return [e for e in self.written_events if e.get("trace_id") == trace_id]

    def clear(self) -> None:
        """Clear all written events."""
        self.written_events.clear()
        self.write_count = 0
        self.dedupe_hits = 0
        self._seen_trace_ids.clear()


class MockDeltaInventoryWriter:
    """
    Mock Delta inventory writer for testing without Delta Lake dependencies.

    Stores inventory records in memory with merge-based idempotency simulation.
    """

    def __init__(self, table_path: str):
        """
        Initialize mock writer.

        Args:
            table_path: abfss:// path to Delta table (stored for verification)
        """
        self.table_path = table_path
        self.inventory_records: List[Dict] = []
        self.write_count = 0
        self.merge_count = 0
        # Track unique keys for merge simulation: (trace_id, attachment_url)
        self._record_keys = {}

    async def write_batch(self, records: List[Dict]) -> None:
        """
        Mock batch write with merge-based idempotency.

        Args:
            records: List of inventory records to write
        """
        for record in records:
            trace_id = record.get("trace_id")
            attachment_url = record.get("attachment_url")
            key = (trace_id, attachment_url)

            # Simulate merge behavior: update if exists, insert if new
            if key in self._record_keys:
                # Update existing record
                idx = self._record_keys[key]
                self.inventory_records[idx] = record
                self.merge_count += 1
            else:
                # Insert new record
                self._record_keys[key] = len(self.inventory_records)
                self.inventory_records.append(record)

        self.write_count += 1

    def get_records_by_trace_id(self, trace_id: str) -> List[Dict]:
        """Get all inventory records with given trace_id."""
        return [r for r in self.inventory_records if r.get("trace_id") == trace_id]

    def clear(self) -> None:
        """Clear all inventory records."""
        self.inventory_records.clear()
        self.write_count = 0
        self.merge_count = 0
        self._record_keys.clear()


@pytest.fixture
def mock_onelake_client(kafka_config: KafkaConfig) -> MockOneLakeClient:
    """
    Provide mock OneLake client for testing.

    Args:
        kafka_config: Kafka configuration with onelake_base_path

    Returns:
        MockOneLakeClient: In-memory mock client
    """
    client = MockOneLakeClient(base_path=kafka_config.onelake_base_path)
    return client


@pytest.fixture
def mock_delta_events_writer() -> MockDeltaEventsWriter:
    """
    Provide mock Delta events writer for testing.

    Returns:
        MockDeltaEventsWriter: In-memory mock writer
    """
    writer = MockDeltaEventsWriter(
        table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_events",
        dedupe_window_hours=24,
    )
    return writer


@pytest.fixture
def mock_delta_inventory_writer() -> MockDeltaInventoryWriter:
    """
    Provide mock Delta inventory writer for testing.

    Returns:
        MockDeltaInventoryWriter: In-memory mock writer
    """
    writer = MockDeltaInventoryWriter(
        table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_attachments"
    )
    return writer


@pytest.fixture
async def event_ingester_worker(
    test_kafka_config: KafkaConfig,
    mock_delta_events_writer: MockDeltaEventsWriter,
    monkeypatch,
) -> AsyncGenerator["EventIngesterWorker", None]:
    """
    Provide event ingester worker with mocked Delta writer.

    Args:
        test_kafka_config: Test Kafka configuration
        mock_delta_events_writer: Mock Delta writer
        monkeypatch: Pytest monkeypatch fixture

    Yields:
        EventIngesterWorker: Worker instance with mocked dependencies
    """
    # Lazy import to ensure env vars are set
    from kafka_pipeline.workers.event_ingester import EventIngesterWorker

    # Patch DeltaEventsWriter to return mock
    monkeypatch.setattr(
        "kafka_pipeline.workers.event_ingester.DeltaEventsWriter",
        lambda *args, **kwargs: mock_delta_events_writer
    )

    # Create worker with Delta writes enabled
    worker = EventIngesterWorker(
        config=test_kafka_config,
        enable_delta_writes=True
    )

    yield worker

    # Cleanup: stop worker if running
    if worker.consumer and worker.consumer.is_running:
        await worker.stop()


@pytest.fixture
async def download_worker(
    test_kafka_config: KafkaConfig,
    mock_onelake_client: MockOneLakeClient,
    monkeypatch,
    tmp_path: Path,
) -> AsyncGenerator["DownloadWorker", None]:
    """
    Provide download worker with mocked OneLake client.

    Args:
        test_kafka_config: Test Kafka configuration
        mock_onelake_client: Mock OneLake client
        monkeypatch: Pytest monkeypatch fixture
        tmp_path: Pytest temp directory

    Yields:
        DownloadWorker: Worker instance with mocked dependencies
    """
    # Lazy import to ensure env vars are set
    from kafka_pipeline.workers.download_worker import DownloadWorker

    # Patch OneLakeClient to return mock
    monkeypatch.setattr(
        "kafka_pipeline.workers.download_worker.OneLakeClient",
        lambda *args, **kwargs: mock_onelake_client
    )

    # Create worker with temp directory
    worker = DownloadWorker(
        config=test_kafka_config,
        temp_dir=tmp_path / "downloads"
    )

    yield worker

    # Cleanup: stop worker if running
    if worker.consumer.is_running:
        await worker.stop()


@pytest.fixture
async def result_processor(
    test_kafka_config: KafkaConfig,
    mock_delta_inventory_writer: MockDeltaInventoryWriter,
    monkeypatch,
) -> AsyncGenerator["ResultProcessor", None]:
    """
    Provide result processor with mocked Delta writer.

    Args:
        test_kafka_config: Test Kafka configuration
        mock_delta_inventory_writer: Mock Delta inventory writer
        monkeypatch: Pytest monkeypatch fixture

    Yields:
        ResultProcessor: Worker instance with mocked dependencies
    """
    # Lazy import to ensure env vars are set
    from kafka_pipeline.workers.result_processor import ResultProcessor

    # Patch DeltaInventoryWriter to return mock
    monkeypatch.setattr(
        "kafka_pipeline.workers.result_processor.DeltaInventoryWriter",
        lambda *args, **kwargs: mock_delta_inventory_writer
    )

    # Create processor
    processor = ResultProcessor(
        config=test_kafka_config,
        inventory_table_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_attachments",
        batch_size=10,  # Smaller batch for faster testing
        batch_timeout_seconds=1.0,  # Shorter timeout for faster testing
    )

    yield processor

    # Cleanup: stop processor if running
    if processor._consumer.is_running:
        await processor.stop()


@pytest.fixture
def all_workers(
    event_ingester_worker,
    download_worker,
    result_processor,
) -> Dict[str, object]:
    """
    Provide all workers as a dict for E2E tests.

    Args:
        event_ingester_worker: Event ingester worker instance
        download_worker: Download worker instance
        result_processor: Result processor instance

    Returns:
        dict: Mapping of worker name to instance
    """
    return {
        "event_ingester": event_ingester_worker,
        "download_worker": download_worker,
        "result_processor": result_processor,
    }


@pytest.fixture
def mock_storage(
    mock_onelake_client: MockOneLakeClient,
    mock_delta_events_writer: MockDeltaEventsWriter,
    mock_delta_inventory_writer: MockDeltaInventoryWriter,
) -> Dict[str, object]:
    """
    Provide all mock storage components as a dict.

    Args:
        mock_onelake_client: Mock OneLake client
        mock_delta_events_writer: Mock Delta events writer
        mock_delta_inventory_writer: Mock Delta inventory writer

    Returns:
        dict: Mapping of storage component name to mock instance
    """
    return {
        "onelake": mock_onelake_client,
        "delta_events": mock_delta_events_writer,
        "delta_inventory": mock_delta_inventory_writer,
    }
