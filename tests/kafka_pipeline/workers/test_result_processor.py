"""
Unit tests for ResultProcessor.

Tests result consumption, batch accumulation, size-based flushing,
timeout-based flushing, and graceful shutdown.
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.workers.result_processor import ResultProcessor


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
    )


@pytest.fixture
def inventory_table_path():
    """Create test inventory table path."""
    return "abfss://test@storage.dfs.core.windows.net/xact_attachments"


@pytest.fixture
def mock_inventory_writer():
    """Create mock DeltaInventoryWriter."""
    with patch("kafka_pipeline.workers.result_processor.DeltaInventoryWriter") as mock:
        # Mock instance returned when DeltaInventoryWriter() is called
        mock_instance = AsyncMock()
        mock_instance.write_results = AsyncMock(return_value=True)
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_success_result():
    """Create sample successful DownloadResultMessage."""
    return DownloadResultMessage(
        trace_id="evt-123",
        attachment_url="https://storage.example.com/file.pdf",
        status="success",
        destination_path="claims/C-456/file.pdf",
        bytes_downloaded=2048576,
        processing_time_ms=1250,
        completed_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_failed_transient_result():
    """Create sample failed (transient) DownloadResultMessage."""
    return DownloadResultMessage(
        trace_id="evt-456",
        attachment_url="https://storage.example.com/timeout.pdf",
        status="failed_transient",
        destination_path=None,
        bytes_downloaded=None,
        error_message="Connection timeout",
        error_category="transient",
        processing_time_ms=30000,
        completed_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_failed_permanent_result():
    """Create sample failed (permanent) DownloadResultMessage."""
    return DownloadResultMessage(
        trace_id="evt-789",
        attachment_url="https://storage.example.com/invalid.exe",
        status="failed_permanent",
        destination_path=None,
        bytes_downloaded=None,
        error_message="File type not allowed",
        error_category="permanent",
        processing_time_ms=50,
        completed_at=datetime.now(timezone.utc),
    )


def create_consumer_record(result: DownloadResultMessage, offset: int = 0) -> ConsumerRecord:
    """Create ConsumerRecord from DownloadResultMessage."""
    return ConsumerRecord(
        topic="test.downloads.results",
        partition=0,
        offset=offset,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=result.trace_id.encode("utf-8"),
        value=result.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=len(result.trace_id),
        serialized_value_size=len(result.model_dump_json()),
    )


@pytest.mark.asyncio
class TestResultProcessor:
    """Test suite for ResultProcessor."""

    async def test_initialization(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test processor initialization with correct configuration."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        assert processor.config == kafka_config
        assert processor.batch_size == 100
        assert processor.batch_timeout_seconds == 5
        assert processor._batch == []
        assert not processor.is_running

    async def test_initialization_custom_batch_config(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test processor initialization with custom batch configuration."""
        processor = ResultProcessor(
            kafka_config,
            inventory_table_path,
            batch_size=50,
            batch_timeout_seconds=10.0,
        )

        assert processor.batch_size == 50
        assert processor.batch_timeout_seconds == 10.0

    async def test_handle_successful_result(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test handling successful download result."""
        processor = ResultProcessor(kafka_config, inventory_table_path)
        record = create_consumer_record(sample_success_result)

        await processor._handle_result(record)

        # Verify result added to batch
        assert len(processor._batch) == 1
        assert processor._batch[0].trace_id == "evt-123"
        assert processor._batch[0].status == "success"

    async def test_filter_failed_transient_result(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_failed_transient_result
    ):
        """Test that failed_transient results are filtered out."""
        processor = ResultProcessor(kafka_config, inventory_table_path)
        record = create_consumer_record(sample_failed_transient_result)

        await processor._handle_result(record)

        # Verify result NOT added to batch
        assert len(processor._batch) == 0

    async def test_filter_failed_permanent_result(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_failed_permanent_result
    ):
        """Test that failed_permanent results are filtered out."""
        processor = ResultProcessor(kafka_config, inventory_table_path)
        record = create_consumer_record(sample_failed_permanent_result)

        await processor._handle_result(record)

        # Verify result NOT added to batch
        assert len(processor._batch) == 0

    async def test_batch_size_flush(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that batch flushes when size threshold reached."""
        processor = ResultProcessor(kafka_config, inventory_table_path, batch_size=3)

        # Add results up to batch size
        for i in range(3):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should be empty after automatic flush
        assert len(processor._batch) == 0

    async def test_batch_accumulation_below_threshold(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that batch accumulates when below size threshold."""
        processor = ResultProcessor(kafka_config, inventory_table_path, batch_size=10)

        # Add 5 results (below threshold)
        for i in range(5):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should contain 5 results (no flush yet)
        assert len(processor._batch) == 5

    async def test_periodic_flush_timeout(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that periodic flush triggers after timeout."""
        processor = ResultProcessor(kafka_config, inventory_table_path, batch_timeout_seconds=0.5)

        # Add one result to batch
        record = create_consumer_record(sample_success_result)
        await processor._handle_result(record)

        # Verify batch has 1 result
        assert len(processor._batch) == 1

        # Start periodic flush task
        flush_task = asyncio.create_task(processor._periodic_flush())
        processor._running = True

        try:
            # Wait for timeout + buffer
            await asyncio.sleep(1.5)

            # Batch should be flushed
            assert len(processor._batch) == 0
        finally:
            processor._running = False
            flush_task.cancel()
            try:
                await flush_task
            except asyncio.CancelledError:
                pass

    async def test_graceful_shutdown_flushes_pending_batch(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that graceful shutdown flushes pending batch."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Mock the consumer
        mock_consumer = AsyncMock()
        mock_consumer.is_running = False
        processor._consumer = mock_consumer
        processor._running = True

        # Add results to batch
        for i in range(3):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Verify batch has 3 results
        assert len(processor._batch) == 3

        # Stop processor (should flush)
        await processor.stop()

        # Batch should be empty after shutdown flush
        assert len(processor._batch) == 0
        assert not processor.is_running

    async def test_empty_batch_no_flush(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that empty batch doesn't trigger flush."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Call flush with empty batch
        async with processor._batch_lock:
            await processor._flush_batch()

        # Should complete without error
        assert len(processor._batch) == 0

    async def test_invalid_message_raises_exception(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that invalid message parsing raises exception."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Create invalid consumer record
        invalid_record = ConsumerRecord(
            topic="test.downloads.results",
            partition=0,
            offset=0,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-invalid",
            value=b'{"invalid": "json structure"}',
            headers=[],
            checksum=None,
            serialized_key_size=11,
            serialized_value_size=30,
        )

        # Should raise validation error
        with pytest.raises(Exception):
            await processor._handle_result(invalid_record)

    async def test_thread_safe_batch_accumulation(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that concurrent batch accumulation is thread-safe."""
        processor = ResultProcessor(kafka_config, inventory_table_path, batch_size=1000)

        # Create multiple results
        async def add_result(i):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Add 50 results concurrently
        tasks = [add_result(i) for i in range(50)]
        await asyncio.gather(*tasks)

        # Batch should contain exactly 50 results
        assert len(processor._batch) == 50

    async def test_flush_batch_converts_to_inventory_records(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that flush_batch converts results to inventory record format."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Add result to batch
        record = create_consumer_record(sample_success_result)
        await processor._handle_result(record)

        # Verify batch has 1 result before flush
        assert len(processor._batch) == 1

        # Flush batch
        async with processor._batch_lock:
            await processor._flush_batch()

        # Batch should be empty after flush
        assert len(processor._batch) == 0

    async def test_multiple_flush_cycles(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that processor handles multiple flush cycles correctly."""
        processor = ResultProcessor(kafka_config, inventory_table_path, batch_size=2)

        # First batch
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-batch1-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Batch should be flushed
        assert len(processor._batch) == 0

        # Second batch
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-batch2-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i + 10)
            await processor._handle_result(record)

        # Second batch should also be flushed
        assert len(processor._batch) == 0

    async def test_is_running_property(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test is_running property reflects processor state."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Initially not running
        assert not processor.is_running

        # Mock consumer as running
        mock_consumer = MagicMock()
        mock_consumer.is_running = True
        processor._consumer = mock_consumer
        processor._running = True

        assert processor.is_running

        # Stop processor
        processor._running = False
        mock_consumer.is_running = False

        assert not processor.is_running

    async def test_delta_writer_called_on_flush(
        self, kafka_config, inventory_table_path, mock_inventory_writer, sample_success_result
    ):
        """Test that Delta writer is called when batch is flushed."""
        processor = ResultProcessor(kafka_config, inventory_table_path, batch_size=2)

        # Add 2 results to trigger flush
        for i in range(2):
            result = DownloadResultMessage(
                trace_id=f"evt-{i}",
                attachment_url=f"https://storage.example.com/file{i}.pdf",
                status="success",
                destination_path=f"claims/C-{i}/file{i}.pdf",
                bytes_downloaded=1024 * i,
                processing_time_ms=100,
                completed_at=datetime.now(timezone.utc),
            )
            record = create_consumer_record(result, offset=i)
            await processor._handle_result(record)

        # Verify Delta writer was called
        mock_inventory_writer.write_results.assert_called_once()

        # Verify batch was passed to writer
        call_args = mock_inventory_writer.write_results.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0].trace_id == "evt-0"
        assert call_args[1].trace_id == "evt-1"

    async def test_delta_write_failure_logged(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that Delta write failures are logged but don't crash processor."""
        # Configure mock to return False (failure)
        mock_inventory_writer.write_results = AsyncMock(return_value=False)

        processor = ResultProcessor(kafka_config, inventory_table_path, batch_size=1)

        # Add result to trigger flush
        result = DownloadResultMessage(
            trace_id="evt-fail",
            attachment_url="https://storage.example.com/file.pdf",
            status="success",
            destination_path="claims/C-123/file.pdf",
            bytes_downloaded=1024,
            processing_time_ms=100,
            completed_at=datetime.now(timezone.utc),
        )
        record = create_consumer_record(result)

        # Should not raise exception even though write failed
        await processor._handle_result(record)

        # Verify batch was still cleared
        assert len(processor._batch) == 0

    async def test_duplicate_start_warning(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that starting already-running processor logs warning."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Mock as already running
        processor._running = True

        # Calling start again should return immediately
        await processor.start()

        # Should not have created new consumer
        assert processor._running

    async def test_stop_already_stopped_processor(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that stopping already-stopped processor is safe."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Processor not running
        assert not processor._running

        # Calling stop should be safe
        await processor.stop()

        # Should still not be running
        assert not processor._running

    async def test_stop_with_error_in_consumer_stop(
        self, kafka_config, inventory_table_path, mock_inventory_writer
    ):
        """Test that errors during consumer stop are propagated."""
        processor = ResultProcessor(kafka_config, inventory_table_path)

        # Mock the consumer to raise exception on stop
        mock_consumer = AsyncMock()
        mock_consumer.is_running = True
        mock_consumer.stop = AsyncMock(side_effect=Exception("Consumer stop failed"))
        processor._consumer = mock_consumer
        processor._running = True

        # Should raise the exception
        with pytest.raises(Exception, match="Consumer stop failed"):
            await processor.stop()

        # Processor should still be marked as not running
        assert not processor._running
