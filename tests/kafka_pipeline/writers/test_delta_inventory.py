"""
Tests for Delta inventory writer.

Tests cover:
- Result to DataFrame conversion
- Merge operation for idempotency
- Async write operations
- Error handling
- Batch metrics tracking (size and latency)
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from kafka_pipeline.schemas.results import DownloadResultMessage
from kafka_pipeline.writers.delta_inventory import DeltaInventoryWriter


@pytest.fixture
def sample_result():
    """Create a sample DownloadResultMessage for testing."""
    return DownloadResultMessage(
        trace_id="test-trace-123",
        attachment_url="https://example.com/file1.pdf",
        destination_path="attachments/2024/01/test-trace-123/file1.pdf",
        status="success",
        bytes_downloaded=12345,
        completed_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        processing_time_ms=1500,
    )


@pytest.fixture
def delta_writer():
    """Create a DeltaInventoryWriter with mocked Delta backend."""
    with patch("kafka_pipeline.writers.delta_inventory.DeltaTableWriter"):
        writer = DeltaInventoryWriter(
            table_path="abfss://test@onelake/lakehouse/xact_attachments",
        )
        yield writer


class TestDeltaInventoryWriter:
    """Test suite for DeltaInventoryWriter."""

    def test_initialization(self, delta_writer):
        """Test writer initialization."""
        assert delta_writer.table_path == "abfss://test@onelake/lakehouse/xact_attachments"
        assert delta_writer._delta_writer is not None

    def test_results_to_dataframe_single_result(self, delta_writer, sample_result):
        """Test converting a single result to DataFrame."""
        df = delta_writer._results_to_dataframe([sample_result])

        # Check DataFrame shape
        assert len(df) == 1

        # Check schema
        assert "trace_id" in df.columns
        assert "attachment_url" in df.columns
        assert "blob_path" in df.columns
        assert "bytes_downloaded" in df.columns
        assert "downloaded_at" in df.columns
        assert "processing_time_ms" in df.columns
        assert "created_at" in df.columns
        assert "created_date" in df.columns

        # Check data types
        assert df.schema["trace_id"] == pl.Utf8
        assert df.schema["attachment_url"] == pl.Utf8
        assert df.schema["blob_path"] == pl.Utf8
        assert df.schema["bytes_downloaded"] == pl.Int64
        assert df.schema["downloaded_at"] == pl.Datetime(time_zone="UTC")
        assert df.schema["processing_time_ms"] == pl.Int64
        assert df.schema["created_at"] == pl.Datetime(time_zone="UTC")
        assert df.schema["created_date"] == pl.Date

        # Check values
        assert df["trace_id"][0] == "test-trace-123"
        assert df["attachment_url"][0] == "https://example.com/file1.pdf"
        assert df["blob_path"][0] == "attachments/2024/01/test-trace-123/file1.pdf"
        assert df["bytes_downloaded"][0] == 12345
        assert df["processing_time_ms"][0] == 1500

    def test_results_to_dataframe_multiple_results(self, delta_writer):
        """Test converting multiple results to DataFrame."""
        results = [
            DownloadResultMessage(
                trace_id=f"trace-{i}",
                attachment_url=f"https://example.com/file{i}.pdf",
                destination_path=f"attachments/2024/01/trace-{i}/file{i}.pdf",
                status="success",
                bytes_downloaded=1000 * i,
                completed_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                processing_time_ms=1000 + i,
            )
            for i in range(3)
        ]

        df = delta_writer._results_to_dataframe(results)

        assert len(df) == 3
        assert df["trace_id"].to_list() == ["trace-0", "trace-1", "trace-2"]
        assert df["bytes_downloaded"].to_list() == [0, 1000, 2000]

    @pytest.mark.asyncio
    async def test_write_result_success(self, delta_writer, sample_result):
        """Test successful single result write."""
        # Mock asyncio.to_thread to actually call the function synchronously
        with patch("asyncio.to_thread", side_effect=lambda f, *args, **kwargs: f(*args, **kwargs)):
            # Mock the underlying Delta writer merge
            delta_writer._delta_writer.merge = MagicMock(return_value=1)

            result = await delta_writer.write_result(sample_result)

            assert result is True
            delta_writer._delta_writer.merge.assert_called_once()

            # Verify DataFrame was created correctly
            call_args = delta_writer._delta_writer.merge.call_args
            df = call_args[0][0]
            assert len(df) == 1
            assert df["trace_id"][0] == "test-trace-123"

            # Verify merge keys (passed as keyword argument)
            assert call_args[1]["merge_keys"] == ["trace_id", "attachment_url"]

    @pytest.mark.asyncio
    async def test_write_results_multiple(self, delta_writer):
        """Test writing multiple results in batch."""
        results = [
            DownloadResultMessage(
                trace_id=f"trace-{i}",
                attachment_url=f"https://example.com/file{i}.pdf",
                destination_path=f"attachments/2024/01/trace-{i}/file{i}.pdf",
                status="success",
                bytes_downloaded=1000 * i,
                completed_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                processing_time_ms=1000,
            )
            for i in range(5)
        ]

        delta_writer._delta_writer.merge = MagicMock(return_value=5)

        result = await delta_writer.write_results(results)

        assert result is True
        delta_writer._delta_writer.merge.assert_called_once()

        # Verify DataFrame has all results
        call_args = delta_writer._delta_writer.merge.call_args
        df = call_args[0][0]
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_write_results_empty_list(self, delta_writer):
        """Test writing empty result list returns True."""
        result = await delta_writer.write_results([])

        assert result is True
        delta_writer._delta_writer.merge.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_result_failure(self, delta_writer, sample_result):
        """Test write failure handling."""
        # Mock merge to raise an exception
        delta_writer._delta_writer.merge = MagicMock(
            side_effect=Exception("Delta merge failed")
        )

        result = await delta_writer.write_result(sample_result)

        assert result is False

    @pytest.mark.asyncio
    async def test_write_results_merge_keys(self, delta_writer, sample_result):
        """Test that correct merge keys are used for idempotency."""
        # Mock asyncio.to_thread to actually call the function synchronously
        with patch("asyncio.to_thread", side_effect=lambda f, *args, **kwargs: f(*args, **kwargs)):
            delta_writer._delta_writer.merge = MagicMock(return_value=1)

            await delta_writer.write_result(sample_result)

            # Verify merge keys: (trace_id, attachment_url)
            call_args = delta_writer._delta_writer.merge.call_args
            merge_keys = call_args[1]["merge_keys"]
            assert merge_keys == ["trace_id", "attachment_url"]

    @pytest.mark.asyncio
    async def test_write_results_preserve_columns(self, delta_writer, sample_result):
        """Test that created_at is preserved during merge updates."""
        delta_writer._delta_writer.merge = MagicMock(return_value=1)

        await delta_writer.write_result(sample_result)

        # Verify preserve_columns includes created_at
        call_args = delta_writer._delta_writer.merge.call_args
        preserve_columns = call_args[1]["preserve_columns"]
        assert preserve_columns == ["created_at"]

    @pytest.mark.asyncio
    async def test_write_results_async_execution(self, delta_writer, sample_result):
        """Test that write operations use asyncio.to_thread for non-blocking I/O."""
        # Track if asyncio.to_thread was called
        to_thread_called = []

        def mock_to_thread(f, *args, **kwargs):
            to_thread_called.append(True)
            return f(*args, **kwargs)

        # Patch asyncio.to_thread to track calls
        with patch("asyncio.to_thread", side_effect=mock_to_thread):
            delta_writer._delta_writer.merge = MagicMock(return_value=1)

            result = await delta_writer.write_result(sample_result)

            assert result is True
            # Verify asyncio.to_thread was called for non-blocking I/O
            assert len(to_thread_called) == 1

    @pytest.mark.asyncio
    async def test_write_results_latency_metrics(self, delta_writer, sample_result):
        """Test that latency metrics are logged."""
        delta_writer._delta_writer.merge = MagicMock(return_value=1)

        # Capture log output to verify latency_ms is logged
        with patch("kafka_pipeline.writers.delta_inventory.logger") as mock_logger:
            await delta_writer.write_result(sample_result)

            # Verify info log was called with latency_ms
            assert mock_logger.info.called
            log_call = mock_logger.info.call_args
            extra = log_call[1]["extra"]
            assert "latency_ms" in extra
            assert "batch_size" in extra
            assert extra["batch_size"] == 1

    def test_created_at_timestamp(self, delta_writer, sample_result):
        """Test that created_at is set to current UTC time."""
        before = datetime.now(timezone.utc)
        df = delta_writer._results_to_dataframe([sample_result])
        after = datetime.now(timezone.utc)

        created_at = df["created_at"][0]

        # created_at should be between before and after
        assert before <= created_at <= after

    def test_created_date_field(self, delta_writer, sample_result):
        """Test that created_date is set to current UTC date."""
        before_date = datetime.now(timezone.utc).date()
        df = delta_writer._results_to_dataframe([sample_result])
        after_date = datetime.now(timezone.utc).date()

        created_date = df["created_date"][0]

        # created_date should be today's date
        assert created_date == before_date or created_date == after_date

    def test_timezone_handling(self, delta_writer, sample_result):
        """Test that all timestamps are timezone-aware (UTC)."""
        df = delta_writer._results_to_dataframe([sample_result])

        # Both downloaded_at and created_at should be UTC-aware
        assert df.schema["downloaded_at"] == pl.Datetime(time_zone="UTC")
        assert df.schema["created_at"] == pl.Datetime(time_zone="UTC")

    def test_blob_path_mapping(self, delta_writer, sample_result):
        """Test that destination_path is correctly mapped to blob_path."""
        df = delta_writer._results_to_dataframe([sample_result])

        # destination_path from result should become blob_path in DataFrame
        assert df["blob_path"][0] == sample_result.destination_path


@pytest.mark.asyncio
async def test_delta_writer_integration():
    """Integration test with actual Delta writer (mocked storage)."""
    with patch(
        "kafka_pipeline.writers.delta_inventory.DeltaTableWriter"
    ) as mock_delta_writer_class:
        # Setup mock
        mock_writer_instance = MagicMock()
        mock_writer_instance.merge = MagicMock(return_value=1)
        mock_delta_writer_class.return_value = mock_writer_instance

        # Create writer and write result
        writer = DeltaInventoryWriter(
            table_path="abfss://test@onelake/lakehouse/xact_attachments",
        )

        result = DownloadResultMessage(
            trace_id="integration-test",
            attachment_url="https://example.com/integration.pdf",
            destination_path="attachments/2024/01/integration-test/integration.pdf",
            status="success",
            bytes_downloaded=54321,
            completed_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            processing_time_ms=2000,
        )

        write_result = await writer.write_result(result)

        assert write_result is True
        mock_writer_instance.merge.assert_called_once()

        # Verify DeltaTableWriter was initialized with correct params
        mock_delta_writer_class.assert_called_once_with(
            table_path="abfss://test@onelake/lakehouse/xact_attachments",
            z_order_columns=["trace_id", "downloaded_at"],
        )
