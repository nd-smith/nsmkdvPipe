"""
Unit tests for DownloadWorker.

Tests download task consumption, AttachmentDownloader integration,
task conversion, and processing metrics.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.structs import ConsumerRecord

import pytest

from core.download.models import DownloadOutcome, DownloadTask
from core.errors.exceptions import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.workers.download_worker import DownloadWorker


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        events_topic="test.events.raw",
        downloads_pending_topic="test.downloads.pending",
        downloads_results_topic="test.downloads.results",
        dlq_topic="test.downloads.dlq",
        consumer_group_prefix="test",
    )


@pytest.fixture
def sample_download_task_message():
    """Create sample DownloadTaskMessage for testing."""
    return DownloadTaskMessage(
        trace_id="evt-123",
        attachment_url="https://claimxperience.com/files/document.pdf",
        destination_path="claims/C-456/document.pdf",
        event_type="claim",
        event_subtype="documentsReceived",
        retry_count=0,
        original_timestamp=datetime.now(timezone.utc),
        metadata={"expected_size": 1024},
    )


@pytest.fixture
def sample_consumer_record(sample_download_task_message):
    """Create sample ConsumerRecord with DownloadTaskMessage."""
    return ConsumerRecord(
        topic="test.downloads.pending",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"evt-123",
        value=sample_download_task_message.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=7,
        serialized_value_size=len(sample_download_task_message.model_dump_json()),
    )


@pytest.fixture
def temp_download_dir(tmp_path):
    """Create temporary download directory."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir


@pytest.mark.asyncio
class TestDownloadWorker:
    """Test suite for DownloadWorker."""

    async def test_initialization(self, kafka_config, temp_download_dir):
        """Test worker initialization with correct configuration."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        assert worker.config == kafka_config
        assert worker.temp_dir == temp_download_dir
        assert worker.CONSUMER_GROUP == "xact-download-worker"
        assert worker.downloader is not None

    async def test_topics_subscription(self, kafka_config, temp_download_dir):
        """Test worker subscribes to correct topics."""
        with patch("kafka_pipeline.workers.download_worker.BaseKafkaConsumer") as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer

            worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

            # Verify consumer created with correct topics
            mock_consumer_class.assert_called_once()
            call_kwargs = mock_consumer_class.call_args.kwargs

            expected_topics = [
                kafka_config.downloads_pending_topic,
                "xact.downloads.retry.5m",
                "xact.downloads.retry.10m",
                "xact.downloads.retry.20m",
                "xact.downloads.retry.40m",
            ]

            assert call_kwargs["topics"] == expected_topics
            assert call_kwargs["group_id"] == "xact-download-worker"
            assert call_kwargs["config"] == kafka_config

    async def test_start_and_stop(self, kafka_config, temp_download_dir):
        """Test worker start and stop lifecycle."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Mock consumer
        worker.consumer = AsyncMock()

        # Test start
        await worker.start()
        worker.consumer.start.assert_called_once()

        # Test stop
        await worker.stop()
        worker.consumer.stop.assert_called_once()

    async def test_convert_to_download_task(
        self, kafka_config, temp_download_dir, sample_download_task_message
    ):
        """Test conversion from DownloadTaskMessage to DownloadTask."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        download_task = worker._convert_to_download_task(sample_download_task_message)

        assert isinstance(download_task, DownloadTask)
        assert download_task.url == sample_download_task_message.attachment_url
        assert download_task.timeout == 60
        assert download_task.validate_url is True
        assert download_task.validate_file_type is True

        # Verify temp file path includes trace_id and preserves filename
        assert sample_download_task_message.trace_id in str(download_task.destination)
        assert download_task.destination.name == "document.pdf"
        assert download_task.destination.parent == temp_download_dir / sample_download_task_message.trace_id

    async def test_handle_task_message_success(
        self, kafka_config, temp_download_dir, sample_consumer_record, sample_download_task_message
    ):
        """Test successful download task processing."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Mock successful download
        mock_outcome = DownloadOutcome.success_outcome(
            file_path=temp_download_dir / "evt-123" / "document.pdf",
            bytes_downloaded=2048,
            content_type="application/pdf",
            status_code=200,
        )

        with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = mock_outcome

            # Process message
            await worker._handle_task_message(sample_consumer_record)

            # Verify download was called with correct task
            mock_download.assert_called_once()
            call_args = mock_download.call_args.args[0]
            assert isinstance(call_args, DownloadTask)
            assert call_args.url == sample_download_task_message.attachment_url
            assert "evt-123" in str(call_args.destination)

    async def test_handle_task_message_failure(
        self, kafka_config, temp_download_dir, sample_consumer_record
    ):
        """Test failed download task processing."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Mock failed download
        mock_outcome = DownloadOutcome.download_failure(
            error_message="Connection timeout",
            error_category=ErrorCategory.TRANSIENT,
            status_code=None,
        )

        with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = mock_outcome

            # Process message should not raise (just logs)
            await worker._handle_task_message(sample_consumer_record)

            # Verify download was called
            mock_download.assert_called_once()

    async def test_handle_task_message_invalid_json(self, kafka_config, temp_download_dir):
        """Test handling of invalid message JSON."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Create record with invalid JSON
        invalid_record = ConsumerRecord(
            topic="test.downloads.pending",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=b"invalid json {{{",
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=15,
        )

        # Should raise exception (handled by consumer error routing)
        with pytest.raises(Exception):
            await worker._handle_task_message(invalid_record)

    async def test_handle_task_message_missing_fields(
        self, kafka_config, temp_download_dir
    ):
        """Test handling of message with missing required fields."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Create record with incomplete data
        incomplete_data = {
            "trace_id": "evt-123",
            # Missing required fields
        }

        invalid_record = ConsumerRecord(
            topic="test.downloads.pending",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=json.dumps(incomplete_data).encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(json.dumps(incomplete_data)),
        )

        # Should raise validation exception
        with pytest.raises(Exception):
            await worker._handle_task_message(invalid_record)

    async def test_handle_retry_topic_message(
        self, kafka_config, temp_download_dir, sample_download_task_message
    ):
        """Test processing message from retry topic."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Create message from retry topic
        retry_task = sample_download_task_message.model_copy(update={"retry_count": 2})

        retry_record = ConsumerRecord(
            topic="xact.downloads.retry.10m",  # Retry topic
            partition=0,
            offset=5,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=retry_task.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(retry_task.model_dump_json()),
        )

        # Mock successful download
        mock_outcome = DownloadOutcome.success_outcome(
            file_path=temp_download_dir / "evt-123" / "document.pdf",
            bytes_downloaded=2048,
            content_type="application/pdf",
            status_code=200,
        )

        with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = mock_outcome

            # Should process normally
            await worker._handle_task_message(retry_record)

            # Verify download was called
            mock_download.assert_called_once()

    async def test_processing_time_tracking(
        self, kafka_config, temp_download_dir, sample_consumer_record
    ):
        """Test that processing time is measured and logged."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Mock successful download with small delay
        mock_outcome = DownloadOutcome.success_outcome(
            file_path=temp_download_dir / "evt-123" / "document.pdf",
            bytes_downloaded=2048,
            content_type="application/pdf",
            status_code=200,
        )

        async def slow_download(task):
            import asyncio
            await asyncio.sleep(0.01)  # 10ms delay
            return mock_outcome

        with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
            mock_download.side_effect = slow_download

            # Mock logger to capture processing time
            with patch("kafka_pipeline.workers.download_worker.logger") as mock_logger:
                await worker._handle_task_message(sample_consumer_record)

                # Verify logger was called with processing_time_ms
                info_calls = [call for call in mock_logger.info.call_args_list]
                assert any(
                    "processing_time_ms" in str(call) for call in info_calls
                ), "Processing time should be logged"

    async def test_temp_dir_creation(self, kafka_config, tmp_path):
        """Test that temp directory is created if it doesn't exist."""
        temp_dir = tmp_path / "new_downloads"
        assert not temp_dir.exists()

        worker = DownloadWorker(kafka_config, temp_dir=temp_dir)

        assert temp_dir.exists()
        assert temp_dir.is_dir()

    async def test_is_running_property(self, kafka_config, temp_download_dir):
        """Test is_running property delegates to consumer."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Mock consumer
        worker.consumer = MagicMock()
        worker.consumer.is_running = True

        assert worker.is_running is True

        worker.consumer.is_running = False
        assert worker.is_running is False
