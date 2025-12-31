"""
Unit tests for DownloadWorker with concurrent processing.

Tests download task consumption, AttachmentDownloader integration,
task conversion, batch processing, and concurrency control.

Updated for WP-313: Concurrent Processing support.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from aiokafka.structs import ConsumerRecord

import pytest

from core.download.models import DownloadOutcome, DownloadTask
from core.errors.exceptions import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.workers.download_worker import DownloadWorker, TaskResult


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
        onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
        security_protocol="PLAINTEXT",  # Use PLAINTEXT for unit tests
        sasl_mechanism="PLAIN",
        download_concurrency=10,  # WP-313
        download_batch_size=20,  # WP-313
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
        # WP-313: Verify concurrency settings
        assert worker.config.download_concurrency == 10
        assert worker.config.download_batch_size == 20

    async def test_topics_list(self, kafka_config, temp_download_dir):
        """Test worker has correct topic list (dynamically constructed from config)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Retry topics are dynamically constructed from config
        expected_topics = [kafka_config.downloads_pending_topic] + [
            kafka_config.get_retry_topic(i) for i in range(len(kafka_config.retry_delays))
        ]

        assert worker.topics == expected_topics
        # Verify the retry topic naming pattern
        assert f"{kafka_config.downloads_pending_topic}.retry.5m" in worker.topics

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

    async def test_process_single_task_success(
        self, kafka_config, temp_download_dir, sample_consumer_record, sample_download_task_message
    ):
        """Test successful download task processing (WP-313: _process_single_task)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies that are normally initialized in start()
        worker.onelake_client = AsyncMock()
        worker.onelake_client.upload_file = AsyncMock(return_value="claims/C-456/document.pdf")
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Mock successful download
        mock_outcome = DownloadOutcome.success_outcome(
            file_path=temp_download_dir / "evt-123" / "document.pdf",
            bytes_downloaded=2048,
            content_type="application/pdf",
            status_code=200,
        )

        # Create the temp file so cleanup doesn't fail
        (temp_download_dir / "evt-123").mkdir(parents=True, exist_ok=True)
        (temp_download_dir / "evt-123" / "document.pdf").touch()

        with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = mock_outcome

            # Process message using new method
            result = await worker._process_single_task(sample_consumer_record)

            # Verify result
            assert isinstance(result, TaskResult)
            assert result.success is True
            assert result.task_message.trace_id == "evt-123"
            assert result.outcome.success is True

            # Verify download was called with correct task
            mock_download.assert_called_once()
            call_args = mock_download.call_args.args[0]
            assert isinstance(call_args, DownloadTask)
            assert call_args.url == sample_download_task_message.attachment_url
            assert "evt-123" in str(call_args.destination)

    async def test_process_single_task_failure(
        self, kafka_config, temp_download_dir, sample_consumer_record
    ):
        """Test failed download task processing (WP-313: _process_single_task)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies that are normally initialized in start()
        worker.onelake_client = AsyncMock()
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Mock failed download
        mock_outcome = DownloadOutcome.download_failure(
            error_message="Connection timeout",
            error_category=ErrorCategory.TRANSIENT,
            status_code=None,
        )

        with patch.object(worker.downloader, "download", new_callable=AsyncMock) as mock_download:
            mock_download.return_value = mock_outcome

            # Process message
            result = await worker._process_single_task(sample_consumer_record)

            # Verify result
            assert isinstance(result, TaskResult)
            assert result.success is False
            assert result.outcome.error_category == ErrorCategory.TRANSIENT

            # Verify download was called
            mock_download.assert_called_once()

    async def test_process_single_task_invalid_json(self, kafka_config, temp_download_dir):
        """Test handling of invalid message JSON (WP-313: _process_single_task)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

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

        # Should return error result, not raise exception
        result = await worker._process_single_task(invalid_record)

        assert isinstance(result, TaskResult)
        assert result.success is False
        assert result.error is not None
        assert "Failed to parse message" in result.outcome.error_message

    async def test_process_batch_concurrent(
        self, kafka_config, temp_download_dir, sample_download_task_message
    ):
        """Test concurrent batch processing (WP-313)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.onelake_client = AsyncMock()
        worker.onelake_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Create 5 messages
        messages = []
        for i in range(5):
            task = sample_download_task_message.model_copy(
                update={"trace_id": f"evt-{i}", "destination_path": f"claims/C-{i}/doc.pdf"}
            )
            record = ConsumerRecord(
                topic="test.downloads.pending",
                partition=0,
                offset=i,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                timestamp_type=0,
                key=f"evt-{i}".encode("utf-8"),
                value=task.model_dump_json().encode("utf-8"),
                headers=[],
                checksum=None,
                serialized_key_size=5,
                serialized_value_size=len(task.model_dump_json()),
            )
            messages.append(record)

            # Create temp files
            (temp_download_dir / f"evt-{i}").mkdir(parents=True, exist_ok=True)
            (temp_download_dir / f"evt-{i}" / "doc.pdf").touch()

        # Track concurrent execution
        concurrent_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()

        async def mock_download(task):
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

            await asyncio.sleep(0.01)  # Simulate some work

            async with lock:
                concurrent_count -= 1

            return DownloadOutcome.success_outcome(
                file_path=temp_download_dir / task.destination.parent.name / task.destination.name,
                bytes_downloaded=1024,
                content_type="application/pdf",
                status_code=200,
            )

        with patch.object(worker.downloader, "download", side_effect=mock_download):
            results = await worker._process_batch(messages)

        # Verify all messages processed
        assert len(results) == 5
        assert all(r.success for r in results)

        # Verify concurrency was limited by semaphore
        assert max_concurrent <= 3, f"Max concurrent was {max_concurrent}, should be <= 3"

    async def test_process_batch_with_failures(
        self, kafka_config, temp_download_dir, sample_download_task_message
    ):
        """Test batch processing with mixed success/failure (WP-313)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.onelake_client = AsyncMock()
        worker.onelake_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Create 3 messages
        messages = []
        for i in range(3):
            task = sample_download_task_message.model_copy(
                update={"trace_id": f"evt-{i}", "destination_path": f"claims/C-{i}/doc.pdf"}
            )
            record = ConsumerRecord(
                topic="test.downloads.pending",
                partition=0,
                offset=i,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                timestamp_type=0,
                key=f"evt-{i}".encode("utf-8"),
                value=task.model_dump_json().encode("utf-8"),
                headers=[],
                checksum=None,
                serialized_key_size=5,
                serialized_value_size=len(task.model_dump_json()),
            )
            messages.append(record)

            # Create temp files
            (temp_download_dir / f"evt-{i}").mkdir(parents=True, exist_ok=True)
            (temp_download_dir / f"evt-{i}" / "doc.pdf").touch()

        call_count = 0

        async def mock_download(task):
            nonlocal call_count
            call_count += 1

            # First task fails, others succeed
            if "evt-0" in str(task.destination):
                return DownloadOutcome.download_failure(
                    error_message="Download failed",
                    error_category=ErrorCategory.TRANSIENT,
                    status_code=503,
                )

            return DownloadOutcome.success_outcome(
                file_path=temp_download_dir / task.destination.parent.name / task.destination.name,
                bytes_downloaded=1024,
                content_type="application/pdf",
                status_code=200,
            )

        with patch.object(worker.downloader, "download", side_effect=mock_download):
            results = await worker._process_batch(messages)

        # Verify all messages processed
        assert len(results) == 3
        assert call_count == 3

        # Check results
        succeeded = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        assert succeeded == 2
        assert failed == 1

    async def test_in_flight_tracking(
        self, kafka_config, temp_download_dir, sample_consumer_record
    ):
        """Test in-flight task tracking (WP-313)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initialize concurrency control
        worker._semaphore = asyncio.Semaphore(10)
        worker._in_flight_tasks = set()
        worker._in_flight_lock = asyncio.Lock()

        # Mock dependencies
        worker.onelake_client = AsyncMock()
        worker.onelake_client.upload_file = AsyncMock(return_value="path/to/file.pdf")
        worker.producer = AsyncMock()
        worker.retry_handler = AsyncMock()

        # Create temp files
        (temp_download_dir / "evt-123").mkdir(parents=True, exist_ok=True)
        (temp_download_dir / "evt-123" / "document.pdf").touch()

        in_flight_during_download = None

        async def mock_download(task):
            nonlocal in_flight_during_download
            # Capture in-flight count during download
            async with worker._in_flight_lock:
                in_flight_during_download = len(worker._in_flight_tasks)

            return DownloadOutcome.success_outcome(
                file_path=temp_download_dir / "evt-123" / "document.pdf",
                bytes_downloaded=1024,
                content_type="application/pdf",
                status_code=200,
            )

        with patch.object(worker.downloader, "download", side_effect=mock_download):
            result = await worker._process_single_task(sample_consumer_record)

        # Verify in-flight was tracked during download
        assert in_flight_during_download == 1

        # Verify in-flight is cleared after completion
        assert len(worker._in_flight_tasks) == 0
        assert worker.in_flight_count == 0

    async def test_handle_batch_results_with_circuit_error(self, kafka_config, temp_download_dir):
        """Test that circuit breaker errors prevent offset commit (WP-313)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        from core.errors.exceptions import CircuitOpenError

        # Create results with one circuit breaker error
        results = [
            TaskResult(
                message=MagicMock(),
                task_message=MagicMock(),
                outcome=MagicMock(),
                processing_time_ms=100,
                success=True,
                error=None,
            ),
            TaskResult(
                message=MagicMock(),
                task_message=MagicMock(),
                outcome=MagicMock(),
                processing_time_ms=100,
                success=False,
                error=CircuitOpenError("test", 60.0),
            ),
        ]

        should_commit = await worker._handle_batch_results(results)
        assert should_commit is False

    async def test_handle_batch_results_all_success(self, kafka_config, temp_download_dir):
        """Test that successful results allow offset commit (WP-313)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        results = [
            TaskResult(
                message=MagicMock(),
                task_message=MagicMock(),
                outcome=MagicMock(),
                processing_time_ms=100,
                success=True,
                error=None,
            ),
            TaskResult(
                message=MagicMock(),
                task_message=MagicMock(),
                outcome=MagicMock(),
                processing_time_ms=100,
                success=False,
                error=Exception("Regular error"),  # Not a circuit error
            ),
        ]

        should_commit = await worker._handle_batch_results(results)
        assert should_commit is True

    async def test_temp_dir_creation(self, kafka_config, tmp_path):
        """Test that temp directory is created if it doesn't exist."""
        temp_dir = tmp_path / "new_downloads"
        assert not temp_dir.exists()

        worker = DownloadWorker(kafka_config, temp_dir=temp_dir)

        assert temp_dir.exists()
        assert temp_dir.is_dir()

    async def test_is_running_property(self, kafka_config, temp_download_dir):
        """Test is_running property."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)

        # Initially not running
        assert worker.is_running is False

        # Simulate running state
        worker._running = True
        assert worker.is_running is True

    async def test_in_flight_count_property(self, kafka_config, temp_download_dir):
        """Test in_flight_count property (WP-313)."""
        worker = DownloadWorker(kafka_config, temp_dir=temp_download_dir)
        worker._in_flight_tasks = {"evt-1", "evt-2", "evt-3"}

        assert worker.in_flight_count == 3


@pytest.mark.asyncio
class TestDownloadWorkerConfig:
    """Test suite for download concurrency configuration (WP-313)."""

    async def test_default_concurrency_config(self):
        """Test default concurrency settings."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
        )

        assert config.download_concurrency == 10
        assert config.download_batch_size == 20

    async def test_custom_concurrency_config(self):
        """Test custom concurrency settings."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            onelake_base_path="abfss://test@test.dfs.core.windows.net/Files",
            download_concurrency=25,
            download_batch_size=50,
        )

        assert config.download_concurrency == 25
        assert config.download_batch_size == 50

    async def test_concurrency_from_env(self):
        """Test loading concurrency settings from environment."""
        import os

        with patch.dict(os.environ, {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "ONELAKE_BASE_PATH": "abfss://test@test.dfs.core.windows.net/Files",
            "DOWNLOAD_CONCURRENCY": "30",
            "DOWNLOAD_BATCH_SIZE": "40",
        }):
            config = KafkaConfig.from_env()

            assert config.download_concurrency == 30
            assert config.download_batch_size == 40

    async def test_concurrency_max_limit(self):
        """Test that concurrency is capped at 50."""
        import os

        with patch.dict(os.environ, {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "ONELAKE_BASE_PATH": "abfss://test@test.dfs.core.windows.net/Files",
            "DOWNLOAD_CONCURRENCY": "100",  # Over the limit
        }):
            config = KafkaConfig.from_env()

            assert config.download_concurrency == 50  # Capped at max

    async def test_concurrency_min_limit(self):
        """Test that concurrency is at least 1."""
        import os

        with patch.dict(os.environ, {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "ONELAKE_BASE_PATH": "abfss://test@test.dfs.core.windows.net/Files",
            "DOWNLOAD_CONCURRENCY": "0",  # Under the limit
        }):
            config = KafkaConfig.from_env()

            assert config.download_concurrency == 1  # At least 1
