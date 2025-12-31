"""
Integration tests for Download Worker end-to-end flow.

Tests the complete download pipeline from consuming download tasks
to uploading to OneLake and producing result messages.

These tests use Testcontainers to run a real Kafka instance and verify:
- Pending → download → upload → result flow
- Transient failure → retry topic routing
- Permanent failure → DLQ routing
- Retry exhaustion → DLQ routing
- Error handling and recovery

Note: OneLake client is mocked for all tests to avoid Azure credential requirements.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord

from core.download.models import DownloadOutcome
from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.consumer import BaseKafkaConsumer
from kafka_pipeline.producer import BaseKafkaProducer
from kafka_pipeline.schemas.results import DownloadResultMessage, FailedDownloadMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.workers.download_worker import DownloadWorker


# Module-level patch for OneLakeClient to avoid Azure credential requirements
@pytest.fixture(autouse=True)
def mock_onelake_module():
    """Mock OneLakeClient for all tests in this module."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.upload_file = AsyncMock(return_value="uploaded/path.pdf")

    with patch("kafka_pipeline.workers.download_worker.OneLakeClient", return_value=mock_client):
        yield mock_client


@pytest.fixture
def sample_download_task():
    """Create sample download task message for testing."""
    return DownloadTaskMessage(
        trace_id="test-flow-001",
        attachment_url="https://claimxperience.com/files/document.pdf",
        destination_path="claims/C-12345/document.pdf",
        event_type="claim",
        event_subtype="documentsReceived",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"test": "data"},
    )


@pytest.fixture
def kafka_config_with_downloads(kafka_config: KafkaConfig, unique_topic_prefix: str):
    """
    Provide Kafka config with download-specific topic names.

    Args:
        kafka_config: Base test Kafka configuration
        unique_topic_prefix: Unique prefix for test isolation

    Returns:
        KafkaConfig: Configuration with test-specific topics
    """
    config = kafka_config
    config.downloads_pending_topic = f"{unique_topic_prefix}.downloads.pending"
    config.downloads_results_topic = f"{unique_topic_prefix}.downloads.results"
    config.dlq_topic = f"{unique_topic_prefix}.downloads.dlq"
    config.consumer_group_prefix = unique_topic_prefix

    # Configure retry topics
    config.retry_delays = [300, 600, 1200, 2400]  # 5m, 10m, 20m, 40m
    config.max_retries = 4

    # Configure OneLake (required for worker initialization)
    config.onelake_base_path = "abfss://test@test.dfs.core.windows.net/Files"

    return config


@pytest.mark.asyncio
@pytest.mark.integration
async def test_successful_download_flow(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_downloads: KafkaConfig,
    sample_download_task: DownloadTaskMessage,
    unique_topic_prefix: str,
    tmp_path: Path,
    mock_onelake_module: AsyncMock,
):
    """
    Test complete successful download flow: pending → download → upload → result.

    Verifies:
    - Download task is consumed from pending topic
    - Attachment is downloaded successfully
    - File is uploaded to OneLake
    - Success result is produced to results topic
    """
    pending_topic = kafka_config_with_downloads.downloads_pending_topic
    results_topic = kafka_config_with_downloads.downloads_results_topic

    # Send download task to pending topic
    await kafka_producer.send(
        topic=pending_topic,
        key=sample_download_task.trace_id,
        value=sample_download_task,
        headers={"source": "test"},
    )

    # Collect result messages
    result_messages: List[DownloadResultMessage] = []

    async def result_collector(record: ConsumerRecord):
        result = DownloadResultMessage.model_validate_json(record.value)
        result_messages.append(result)

    # Start consumer for results topic
    result_consumer = await kafka_consumer_factory(
        topics=[results_topic],
        group_id=f"{unique_topic_prefix}.result-collector",
        message_handler=result_collector,
    )
    result_consumer_task = asyncio.create_task(result_consumer.start())

    # Create download worker
    worker = DownloadWorker(
        config=kafka_config_with_downloads,
        temp_dir=tmp_path / "downloads",
    )

    # Mock successful download
    temp_file = tmp_path / "downloads" / "test-flow-001" / "document.pdf"
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file.write_text("fake pdf content")

    successful_outcome = DownloadOutcome(
        success=True,
        file_path=temp_file,
        bytes_downloaded=1024,
        content_type="application/pdf",
        error_message=None,
        error_category=None,
        status_code=200,
    )

    with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
        mock_download.return_value = successful_outcome

        # Reset upload mock for this test
        mock_onelake_module.upload_file.reset_mock()
        mock_onelake_module.upload_file.return_value = "claims/C-12345/document.pdf"

        # Start worker in background
        worker_task = asyncio.create_task(worker.start())

        # Wait for result message to be produced
        for _ in range(100):  # 10 seconds max
            if len(result_messages) >= 1:
                break
            await asyncio.sleep(0.1)

        # Stop worker and consumer
        await worker.stop()
        await result_consumer.stop()

        # Cancel background tasks
        worker_task.cancel()
        result_consumer_task.cancel()
        try:
            await asyncio.gather(worker_task, result_consumer_task)
        except asyncio.CancelledError:
            pass

        # Verify download was called
        mock_download.assert_called_once()

        # Verify upload was called
        mock_onelake_module.upload_file.assert_called_once()

    # Verify result message
    assert len(result_messages) == 1, "Should produce one result message"

    result = result_messages[0]
    assert result.trace_id == sample_download_task.trace_id
    assert result.attachment_url == sample_download_task.attachment_url
    assert result.status == "success"
    assert result.destination_path == "claims/C-12345/document.pdf"
    assert result.bytes_downloaded == 1024
    # Note: content_type not tracked in DownloadResultMessage schema
    assert result.error_message is None
    assert result.error_category is None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_transient_failure_routes_to_retry(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_downloads: KafkaConfig,
    sample_download_task: DownloadTaskMessage,
    unique_topic_prefix: str,
    tmp_path: Path,
):
    """
    Test transient failure routing: pending → download fails → retry topic.

    Verifies:
    - Download task is consumed from pending topic
    - Download fails with transient error
    - Task is routed to retry topic (not DLQ)
    - Retry count is incremented
    - Error context is preserved
    """
    pending_topic = kafka_config_with_downloads.downloads_pending_topic
    retry_topic_5m = f"{unique_topic_prefix}.downloads.retry.5m"

    # Send download task to pending topic
    await kafka_producer.send(
        topic=pending_topic,
        key=sample_download_task.trace_id,
        value=sample_download_task,
        headers={"source": "test"},
    )

    # Collect retry messages
    retry_messages: List[DownloadTaskMessage] = []

    async def retry_collector(record: ConsumerRecord):
        task = DownloadTaskMessage.model_validate_json(record.value)
        retry_messages.append(task)

    # Start consumer for retry topic
    retry_consumer = await kafka_consumer_factory(
        topics=[retry_topic_5m],
        group_id=f"{unique_topic_prefix}.retry-collector",
        message_handler=retry_collector,
    )
    retry_consumer_task = asyncio.create_task(retry_consumer.start())

    # Create download worker
    worker = DownloadWorker(
        config=kafka_config_with_downloads,
        temp_dir=tmp_path / "downloads",
    )

    # Mock transient download failure
    failed_outcome = DownloadOutcome(
        success=False,
        file_path=None,
        bytes_downloaded=None,
        content_type=None,
        error_message="Connection timeout after 60 seconds",
        error_category=ErrorCategory.TRANSIENT,
        status_code=None,
    )

    with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
        mock_download.return_value = failed_outcome

        # Start worker in background
        worker_task = asyncio.create_task(worker.start())

        # Wait for retry message to be produced
        for _ in range(100):  # 10 seconds max
            if len(retry_messages) >= 1:
                break
            await asyncio.sleep(0.1)

        # Stop worker and consumer
        await worker.stop()
        await retry_consumer.stop()

        # Cancel background tasks
        worker_task.cancel()
        retry_consumer_task.cancel()
        try:
            await asyncio.gather(worker_task, retry_consumer_task)
        except asyncio.CancelledError:
            pass

    # Verify retry message was produced
    assert len(retry_messages) == 1, "Should produce one retry message"

    retry_task = retry_messages[0]
    assert retry_task.trace_id == sample_download_task.trace_id
    assert retry_task.retry_count == 1, "Retry count should be incremented"
    assert "timeout" in retry_task.metadata.get("last_error", "").lower()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_permanent_failure_routes_to_dlq(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_downloads: KafkaConfig,
    sample_download_task: DownloadTaskMessage,
    unique_topic_prefix: str,
    tmp_path: Path,
):
    """
    Test permanent failure routing: pending → download fails → DLQ.

    Verifies:
    - Download task is consumed from pending topic
    - Download fails with permanent error (404)
    - Task is routed to DLQ (not retry)
    - Error details are preserved in DLQ message
    """
    pending_topic = kafka_config_with_downloads.downloads_pending_topic
    dlq_topic = kafka_config_with_downloads.dlq_topic

    # Send download task to pending topic
    await kafka_producer.send(
        topic=pending_topic,
        key=sample_download_task.trace_id,
        value=sample_download_task,
        headers={"source": "test"},
    )

    # Collect DLQ messages
    dlq_messages: List[FailedDownloadMessage] = []

    async def dlq_collector(record: ConsumerRecord):
        msg = FailedDownloadMessage.model_validate_json(record.value)
        dlq_messages.append(msg)

    # Start consumer for DLQ topic
    dlq_consumer = await kafka_consumer_factory(
        topics=[dlq_topic],
        group_id=f"{unique_topic_prefix}.dlq-collector",
        message_handler=dlq_collector,
    )
    dlq_consumer_task = asyncio.create_task(dlq_consumer.start())

    # Create download worker
    worker = DownloadWorker(
        config=kafka_config_with_downloads,
        temp_dir=tmp_path / "downloads",
    )

    # Mock permanent download failure (404)
    failed_outcome = DownloadOutcome(
        success=False,
        file_path=None,
        bytes_downloaded=None,
        content_type=None,
        error_message="File not found - 404",
        error_category=ErrorCategory.PERMANENT,
        status_code=404,
    )

    with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
        mock_download.return_value = failed_outcome

        # Start worker in background
        worker_task = asyncio.create_task(worker.start())

        # Wait for DLQ message to be produced
        for _ in range(100):  # 10 seconds max
            if len(dlq_messages) >= 1:
                break
            await asyncio.sleep(0.1)

        # Stop worker and consumer
        await worker.stop()
        await dlq_consumer.stop()

        # Cancel background tasks
        worker_task.cancel()
        dlq_consumer_task.cancel()
        try:
            await asyncio.gather(worker_task, dlq_consumer_task)
        except asyncio.CancelledError:
            pass

    # Verify DLQ message was produced
    assert len(dlq_messages) == 1, "Should produce one DLQ message"

    dlq_msg = dlq_messages[0]
    assert dlq_msg.trace_id == sample_download_task.trace_id
    assert dlq_msg.attachment_url == sample_download_task.attachment_url
    assert dlq_msg.failure_reason == "permanent"
    assert "404" in dlq_msg.error_message


@pytest.mark.asyncio
@pytest.mark.integration
async def test_retry_exhaustion_routes_to_dlq(
    kafka_producer: BaseKafkaProducer,
    kafka_consumer_factory: callable,
    kafka_config_with_downloads: KafkaConfig,
    unique_topic_prefix: str,
    tmp_path: Path,
):
    """
    Test retry exhaustion: retry topic → download fails → DLQ.

    Verifies:
    - Task with max retry count is consumed
    - Download fails with transient error
    - Task is routed to DLQ (not another retry)
    - DLQ message indicates retry exhaustion
    """
    retry_topic = f"{unique_topic_prefix}.downloads.retry.40m"
    dlq_topic = kafka_config_with_downloads.dlq_topic

    # Create task with max retry count (exhausted)
    exhausted_task = DownloadTaskMessage(
        trace_id="test-exhausted-001",
        attachment_url="https://claimxperience.com/files/document.pdf",
        destination_path="claims/C-99999/document.pdf",
        event_type="claim",
        event_subtype="documentsReceived",
        retry_count=4,  # At max retries
        original_timestamp=datetime.now(timezone.utc),
        metadata={"previous_errors": ["timeout", "timeout", "timeout", "timeout"]},
    )

    # Send exhausted task to retry topic
    await kafka_producer.send(
        topic=retry_topic,
        key=exhausted_task.trace_id,
        value=exhausted_task,
        headers={"source": "test"},
    )

    # Collect DLQ messages
    dlq_messages: List[FailedDownloadMessage] = []

    async def dlq_collector(record: ConsumerRecord):
        msg = FailedDownloadMessage.model_validate_json(record.value)
        dlq_messages.append(msg)

    # Start consumer for DLQ topic
    dlq_consumer = await kafka_consumer_factory(
        topics=[dlq_topic],
        group_id=f"{unique_topic_prefix}.dlq-exhausted-collector",
        message_handler=dlq_collector,
    )
    dlq_consumer_task = asyncio.create_task(dlq_consumer.start())

    # Create download worker
    worker = DownloadWorker(
        config=kafka_config_with_downloads,
        temp_dir=tmp_path / "downloads",
    )

    # Mock transient failure (but retries exhausted)
    failed_outcome = DownloadOutcome(
        success=False,
        file_path=None,
        bytes_downloaded=None,
        content_type=None,
        error_message="Still timing out",
        error_category=ErrorCategory.TRANSIENT,
        status_code=None,
    )

    with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
        mock_download.return_value = failed_outcome

        # Start worker in background
        worker_task = asyncio.create_task(worker.start())

        # Wait for DLQ message to be produced
        for _ in range(100):  # 10 seconds max
            if len(dlq_messages) >= 1:
                break
            await asyncio.sleep(0.1)

        # Stop worker and consumer
        await worker.stop()
        await dlq_consumer.stop()

        # Cancel background tasks
        worker_task.cancel()
        dlq_consumer_task.cancel()
        try:
            await asyncio.gather(worker_task, dlq_consumer_task)
        except asyncio.CancelledError:
            pass

    # Verify DLQ message was produced (retries exhausted)
    assert len(dlq_messages) == 1, "Should produce one DLQ message"

    dlq_msg = dlq_messages[0]
    assert dlq_msg.trace_id == exhausted_task.trace_id
    assert dlq_msg.retry_count == 4, "Should preserve final retry count"
    assert dlq_msg.failure_reason == "retry_exhausted"
