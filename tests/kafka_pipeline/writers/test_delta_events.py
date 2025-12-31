"""
Tests for Delta events writer.

Tests cover:
- Event to DataFrame conversion
- Deduplication behavior
- Async write operations
- Error handling
- Metrics tracking
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.writers.delta_events import DeltaEventsWriter


@pytest.fixture
def sample_event():
    """Create a sample EventMessage for testing."""
    return EventMessage(
        trace_id="test-trace-123",
        event_type="xact.assignment.status",
        event_subtype="documentsReceived",
        source_system="xact",
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        attachments=["https://example.com/file1.pdf", "https://example.com/file2.pdf"],
        payload={"assignment_id": "A12345", "claim_id": "C67890"},
        metadata={"version": "1.0"},
    )


@pytest.fixture
def delta_writer():
    """Create a DeltaEventsWriter with mocked Delta backend."""
    with patch("kafka_pipeline.writers.delta_events.DeltaTableWriter"):
        writer = DeltaEventsWriter(
            table_path="abfss://test@onelake/lakehouse/xact_events",
            dedupe_window_hours=24,
        )
        yield writer


class TestDeltaEventsWriter:
    """Test suite for DeltaEventsWriter."""

    def test_initialization(self, delta_writer):
        """Test writer initialization."""
        assert delta_writer.table_path == "abfss://test@onelake/lakehouse/xact_events"
        assert delta_writer.dedupe_window_hours == 24
        assert delta_writer._delta_writer is not None

    def test_events_to_dataframe_single_event(self, delta_writer, sample_event):
        """Test converting a single event to DataFrame."""
        df = delta_writer._events_to_dataframe([sample_event])

        # Check DataFrame shape
        assert len(df) == 1

        # Check schema
        assert "trace_id" in df.columns
        assert "event_type" in df.columns
        assert "event_subtype" in df.columns
        assert "source_system" in df.columns
        assert "timestamp" in df.columns
        assert "ingested_at" in df.columns
        assert "attachments" in df.columns
        assert "payload" in df.columns
        assert "metadata" in df.columns

        # Check data types
        assert df.schema["trace_id"] == pl.Utf8
        assert df.schema["event_type"] == pl.Utf8
        assert df.schema["timestamp"] == pl.Datetime(time_zone="UTC")
        assert df.schema["ingested_at"] == pl.Datetime(time_zone="UTC")

        # Check values
        assert df["trace_id"][0] == "test-trace-123"
        assert df["event_type"][0] == "xact.assignment.status"
        assert df["event_subtype"][0] == "documentsReceived"
        assert df["source_system"][0] == "xact"
        assert len(df["attachments"][0]) == 2

    def test_events_to_dataframe_multiple_events(self, delta_writer):
        """Test converting multiple events to DataFrame."""
        events = [
            EventMessage(
                trace_id=f"trace-{i}",
                event_type="xact.assignment.status",
                event_subtype="documentsReceived",
                source_system="xact",
                timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                attachments=[f"https://example.com/file{i}.pdf"],
                payload={"assignment_id": f"A{i}"},
            )
            for i in range(3)
        ]

        df = delta_writer._events_to_dataframe(events)

        assert len(df) == 3
        assert df["trace_id"].to_list() == ["trace-0", "trace-1", "trace-2"]

    def test_events_to_dataframe_optional_fields(self, delta_writer):
        """Test DataFrame conversion with optional fields missing."""
        event = EventMessage(
            trace_id="test-trace",
            event_type="xact.assignment.status",
            event_subtype="documentsReceived",
            source_system="xact",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            payload={"claim_id": "C123"},
            # No attachments or metadata (optional fields)
        )

        df = delta_writer._events_to_dataframe([event])

        assert len(df) == 1
        assert df["trace_id"][0] == "test-trace"
        assert df["attachments"][0].to_list() == []  # Empty list for None
        assert df["metadata"][0] == {}  # Empty dict for None

    @pytest.mark.asyncio
    async def test_write_event_success(self, delta_writer, sample_event):
        """Test successful single event write."""
        # Mock the underlying Delta writer
        delta_writer._delta_writer.append = MagicMock(return_value=1)

        result = await delta_writer.write_event(sample_event)

        assert result is True
        delta_writer._delta_writer.append.assert_called_once()

        # Verify DataFrame was created correctly
        call_args = delta_writer._delta_writer.append.call_args
        df = call_args[0][0]
        assert len(df) == 1
        assert df["trace_id"][0] == "test-trace-123"

    @pytest.mark.asyncio
    async def test_write_events_multiple(self, delta_writer):
        """Test writing multiple events in batch."""
        events = [
            EventMessage(
                trace_id=f"trace-{i}",
                event_type="xact.assignment.status",
                event_subtype="documentsReceived",
                source_system="xact",
                timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                payload={"assignment_id": f"A{i}"},
            )
            for i in range(5)
        ]

        delta_writer._delta_writer.append = MagicMock(return_value=5)

        result = await delta_writer.write_events(events)

        assert result is True
        delta_writer._delta_writer.append.assert_called_once()

        # Verify DataFrame has all events
        call_args = delta_writer._delta_writer.append.call_args
        df = call_args[0][0]
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_write_events_empty_list(self, delta_writer):
        """Test writing empty event list returns True."""
        result = await delta_writer.write_events([])

        assert result is True
        delta_writer._delta_writer.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_event_failure(self, delta_writer, sample_event):
        """Test write failure handling."""
        # Mock append to raise an exception
        delta_writer._delta_writer.append = MagicMock(
            side_effect=Exception("Delta write failed")
        )

        result = await delta_writer.write_event(sample_event)

        assert result is False

    @pytest.mark.asyncio
    async def test_write_events_deduplication_enabled(self, delta_writer, sample_event):
        """Test that deduplication is enabled in write calls."""
        delta_writer._delta_writer.append = MagicMock(return_value=1)

        await delta_writer.write_event(sample_event)

        # Verify dedupe=True was passed
        call_args = delta_writer._delta_writer.append.call_args
        assert call_args[1]["dedupe"] is True

    @pytest.mark.asyncio
    async def test_write_events_async_execution(self, delta_writer, sample_event):
        """Test that write operations use asyncio.to_thread for non-blocking execution."""
        # Mock the underlying Delta writer
        delta_writer._delta_writer.append = MagicMock(return_value=1)

        # Patch asyncio.to_thread to verify it's called
        async def mock_to_thread_impl(*args, **kwargs):
            return None

        with patch("kafka_pipeline.writers.delta_events.asyncio.to_thread", side_effect=mock_to_thread_impl) as mock_to_thread:
            result = await delta_writer.write_event(sample_event)

            assert result is True
            # Verify to_thread was called (proving async execution)
            mock_to_thread.assert_called_once()

    def test_ingested_at_timestamp(self, delta_writer, sample_event):
        """Test that ingested_at is set to current UTC time."""
        before = datetime.now(timezone.utc)
        df = delta_writer._events_to_dataframe([sample_event])
        after = datetime.now(timezone.utc)

        ingested_at = df["ingested_at"][0]

        # ingested_at should be between before and after
        assert before <= ingested_at <= after

    def test_timezone_handling(self, delta_writer, sample_event):
        """Test that all timestamps are timezone-aware (UTC)."""
        df = delta_writer._events_to_dataframe([sample_event])

        # Both timestamp and ingested_at should be UTC-aware
        timestamp = df["timestamp"][0]
        ingested_at = df["ingested_at"][0]

        # Polars Datetime with time_zone="UTC" are timezone-aware
        assert df.schema["timestamp"] == pl.Datetime(time_zone="UTC")
        assert df.schema["ingested_at"] == pl.Datetime(time_zone="UTC")


@pytest.mark.asyncio
async def test_delta_writer_integration():
    """Integration test with actual Delta writer (mocked storage)."""
    with patch("kafka_pipeline.writers.delta_events.DeltaTableWriter") as mock_delta_writer_class:
        # Setup mock
        mock_writer_instance = MagicMock()
        mock_writer_instance.append = MagicMock(return_value=1)
        mock_delta_writer_class.return_value = mock_writer_instance

        # Create writer and write event
        writer = DeltaEventsWriter(
            table_path="abfss://test@onelake/lakehouse/xact_events",
            dedupe_window_hours=24,
        )

        event = EventMessage(
            trace_id="integration-test",
            event_type="xact.assignment.status",
            event_subtype="documentsReceived",
            source_system="xact",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            payload={"assignment_id": "A12345"},
        )

        result = await writer.write_event(event)

        assert result is True
        mock_writer_instance.append.assert_called_once()

        # Verify DeltaTableWriter was initialized with correct params
        mock_delta_writer_class.assert_called_once_with(
            table_path="abfss://test@onelake/lakehouse/xact_events",
            dedupe_column="trace_id",
            dedupe_window_hours=24,
            timestamp_column="ingested_at",
        )
