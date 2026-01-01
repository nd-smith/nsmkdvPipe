"""Tests for KQL Event Poller."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.eventhouse.dedup import DedupConfig
from kafka_pipeline.eventhouse.kql_client import EventhouseConfig, KQLQueryResult
from kafka_pipeline.eventhouse.poller import KQLEventPoller, PollerConfig
from kafka_pipeline.schemas.events import EventMessage


class TestPollerConfig:
    """Tests for PollerConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        eventhouse = EventhouseConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
        )
        kafka = KafkaConfig(bootstrap_servers="localhost:9092")
        dedup = DedupConfig(xact_events_table_path="abfss://test/xact_events")

        config = PollerConfig(
            eventhouse=eventhouse,
            kafka=kafka,
            dedup=dedup,
        )

        assert config.poll_interval_seconds == 30
        assert config.batch_size == 1000
        assert config.source_table == "Events"
        assert config.max_kafka_lag == 10_000

    def test_custom_values(self):
        """Test custom configuration values."""
        eventhouse = EventhouseConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
        )
        kafka = KafkaConfig(bootstrap_servers="localhost:9092")
        dedup = DedupConfig(xact_events_table_path="abfss://test/xact_events")

        config = PollerConfig(
            eventhouse=eventhouse,
            kafka=kafka,
            dedup=dedup,
            poll_interval_seconds=60,
            batch_size=500,
            source_table="CustomEvents",
            max_kafka_lag=5000,
        )

        assert config.poll_interval_seconds == 60
        assert config.batch_size == 500
        assert config.source_table == "CustomEvents"
        assert config.max_kafka_lag == 5000


class TestKQLEventPoller:
    """Tests for KQLEventPoller."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        eventhouse = EventhouseConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
        )
        kafka = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="PLAINTEXT",
        )
        dedup = DedupConfig(xact_events_table_path="abfss://test/xact_events")

        return PollerConfig(
            eventhouse=eventhouse,
            kafka=kafka,
            dedup=dedup,
            poll_interval_seconds=1,  # Fast for testing
            batch_size=100,
        )

    @pytest.fixture
    def mock_kql_client(self):
        """Create mock KQL client."""
        mock = AsyncMock()
        mock.connect = AsyncMock()
        mock.close = AsyncMock()
        mock.execute_query = AsyncMock()
        return mock

    @pytest.fixture
    def mock_producer(self):
        """Create mock Kafka producer."""
        mock = AsyncMock()
        mock.start = AsyncMock()
        mock.stop = AsyncMock()
        mock.send = AsyncMock(
            return_value=MagicMock(partition=0, offset=1)
        )
        return mock

    @pytest.fixture
    def mock_deduplicator(self):
        """Create mock deduplicator."""
        mock = MagicMock()
        mock.get_poll_window = MagicMock(
            return_value=(
                datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
            )
        )
        mock.build_deduped_query = MagicMock(return_value="test query")
        return mock

    def test_row_to_event_basic(self, config):
        """Test converting a simple row to EventMessage."""
        poller = KQLEventPoller(config)

        row = {
            "trace_id": "test-trace-123",
            "event_type": "claim",
            "event_subtype": "created",
            "timestamp": "2024-01-15T10:30:00+00:00",
            "source_system": "claimx",
            "payload": {"claim_id": "C-123"},
            "attachments": ["https://example.com/file1.pdf"],
        }

        event = poller._row_to_event(row)

        assert event.trace_id == "test-trace-123"
        assert event.event_type == "claim"
        assert event.event_subtype == "created"
        assert event.source_system == "claimx"
        assert event.payload == {"claim_id": "C-123"}
        assert event.attachments == ["https://example.com/file1.pdf"]

    def test_row_to_event_with_datetime_object(self, config):
        """Test converting row with datetime object."""
        poller = KQLEventPoller(config)

        row = {
            "trace_id": "test-trace-123",
            "event_type": "claim",
            "event_subtype": "created",
            "timestamp": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "source_system": "claimx",
            "payload": {},
        }

        event = poller._row_to_event(row)

        assert event.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_row_to_event_with_string_payload(self, config):
        """Test converting row with JSON string payload."""
        poller = KQLEventPoller(config)

        row = {
            "trace_id": "test-trace-123",
            "event_type": "claim",
            "event_subtype": "created",
            "timestamp": "2024-01-15T10:30:00Z",
            "source_system": "claimx",
            "payload": '{"claim_id": "C-123"}',
        }

        event = poller._row_to_event(row)

        assert event.payload == {"claim_id": "C-123"}

    def test_row_to_event_with_string_attachments(self, config):
        """Test converting row with JSON string attachments."""
        poller = KQLEventPoller(config)

        row = {
            "trace_id": "test-trace-123",
            "event_type": "claim",
            "event_subtype": "created",
            "timestamp": "2024-01-15T10:30:00Z",
            "source_system": "claimx",
            "payload": {},
            "attachments": '["https://example.com/file1.pdf", "https://example.com/file2.pdf"]',
        }

        event = poller._row_to_event(row)

        assert event.attachments == [
            "https://example.com/file1.pdf",
            "https://example.com/file2.pdf",
        ]

    def test_row_to_event_custom_column_mapping(self, config):
        """Test converting row with custom column mapping."""
        config.column_mapping = {
            "trace_id": "custom_trace",
            "event_type": "custom_type",
            "event_subtype": "custom_subtype",
            "timestamp": "custom_time",
            "source_system": "custom_source",
            "payload": "custom_data",
        }

        poller = KQLEventPoller(config)

        row = {
            "custom_trace": "test-trace-123",
            "custom_type": "claim",
            "custom_subtype": "created",
            "custom_time": "2024-01-15T10:30:00Z",
            "custom_source": "claimx",
            "custom_data": {},
        }

        event = poller._row_to_event(row)

        assert event.trace_id == "test-trace-123"
        assert event.event_type == "claim"

    @pytest.mark.asyncio
    async def test_process_results_empty(self, config):
        """Test processing empty results."""
        poller = KQLEventPoller(config)

        result = KQLQueryResult(rows=[], row_count=0)

        count = await poller._process_results(result)

        assert count == 0

    @pytest.mark.asyncio
    async def test_process_results_with_events(self, config):
        """Test processing results with events."""
        poller = KQLEventPoller(config)
        poller._producer = AsyncMock()
        poller._producer.send = AsyncMock(
            return_value=MagicMock(partition=0, offset=1)
        )

        result = KQLQueryResult(
            rows=[
                {
                    "trace_id": "test-1",
                    "event_type": "claim",
                    "event_subtype": "created",
                    "timestamp": "2024-01-15T10:30:00Z",
                    "source_system": "claimx",
                    "payload": {"assignment_id": "A-123"},
                    "attachments": ["https://verisk.com/file1.pdf"],
                },
            ],
            row_count=1,
        )

        with patch(
            "kafka_pipeline.eventhouse.poller.validate_download_url",
            return_value=(True, None),
        ), patch(
            "kafka_pipeline.eventhouse.poller.generate_blob_path",
            return_value=("/path/to/file.pdf", "pdf"),
        ):
            count = await poller._process_results(result)

        assert count == 1
        poller._producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_event_without_attachments(self, config):
        """Test processing event without attachments."""
        poller = KQLEventPoller(config)
        poller._producer = AsyncMock()

        event = EventMessage(
            trace_id="test-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload={},
            attachments=None,
        )

        await poller._process_event_attachments(event)

        # Producer should not be called
        poller._producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_event_without_assignment_id(self, config):
        """Test processing event without assignment_id."""
        poller = KQLEventPoller(config)
        poller._producer = AsyncMock()

        event = EventMessage(
            trace_id="test-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload={},  # No assignment_id
            attachments=["https://example.com/file.pdf"],
        )

        await poller._process_event_attachments(event)

        # Producer should not be called due to missing assignment_id
        poller._producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_and_stop(self, config):
        """Test starting and stopping the poller."""
        with (
            patch(
                "kafka_pipeline.eventhouse.poller.KQLClient"
            ) as mock_kql_class,
            patch(
                "kafka_pipeline.eventhouse.poller.BaseKafkaProducer"
            ) as mock_producer_class,
            patch(
                "kafka_pipeline.eventhouse.poller.EventhouseDeduplicator"
            ),
        ):
            mock_kql = AsyncMock()
            mock_kql_class.return_value = mock_kql

            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer

            poller = KQLEventPoller(config)

            await poller.start()

            assert poller.is_running
            mock_kql.connect.assert_called_once()
            mock_producer.start.assert_called_once()

            await poller.stop()

            assert not poller.is_running
            mock_kql.close.assert_called_once()
            mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test async context manager."""
        with (
            patch(
                "kafka_pipeline.eventhouse.poller.KQLClient"
            ) as mock_kql_class,
            patch(
                "kafka_pipeline.eventhouse.poller.BaseKafkaProducer"
            ) as mock_producer_class,
            patch(
                "kafka_pipeline.eventhouse.poller.EventhouseDeduplicator"
            ),
        ):
            mock_kql = AsyncMock()
            mock_kql_class.return_value = mock_kql

            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer

            async with KQLEventPoller(config) as poller:
                assert poller.is_running

            assert not poller.is_running

    @pytest.mark.asyncio
    async def test_poll_cycle(self, config):
        """Test a single poll cycle."""
        with (
            patch(
                "kafka_pipeline.eventhouse.poller.KQLClient"
            ) as mock_kql_class,
            patch(
                "kafka_pipeline.eventhouse.poller.BaseKafkaProducer"
            ) as mock_producer_class,
            patch(
                "kafka_pipeline.eventhouse.poller.EventhouseDeduplicator"
            ) as mock_dedup_class,
        ):
            # Setup mock KQL client
            mock_kql = AsyncMock()
            mock_kql.execute_query = AsyncMock(
                return_value=KQLQueryResult(rows=[], row_count=0)
            )
            mock_kql_class.return_value = mock_kql

            # Setup mock producer
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer

            # Setup mock deduplicator
            mock_dedup = MagicMock()
            mock_dedup.get_poll_window.return_value = (
                datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
            )
            mock_dedup.build_deduped_query.return_value = "test query"
            mock_dedup_class.return_value = mock_dedup

            async with KQLEventPoller(config) as poller:
                count = await poller._poll_cycle()

            assert count == 0
            mock_kql.execute_query.assert_called_once_with("test query")

    def test_stats(self, config):
        """Test getting poller statistics."""
        poller = KQLEventPoller(config)

        stats = poller.stats

        assert stats["running"] is False
        assert stats["total_polls"] == 0
        assert stats["total_events_fetched"] == 0
        assert stats["consecutive_empty_polls"] == 0
        assert stats["last_poll_time"] is None

    @pytest.mark.asyncio
    async def test_consecutive_empty_polls_tracking(self, config):
        """Test tracking of consecutive empty polls."""
        with (
            patch(
                "kafka_pipeline.eventhouse.poller.KQLClient"
            ) as mock_kql_class,
            patch(
                "kafka_pipeline.eventhouse.poller.BaseKafkaProducer"
            ) as mock_producer_class,
            patch(
                "kafka_pipeline.eventhouse.poller.EventhouseDeduplicator"
            ) as mock_dedup_class,
        ):
            # Setup mock KQL client
            mock_kql = AsyncMock()
            mock_kql.execute_query = AsyncMock(
                return_value=KQLQueryResult(rows=[], row_count=0)
            )
            mock_kql_class.return_value = mock_kql

            # Setup mock producer
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer

            # Setup mock deduplicator
            mock_dedup = MagicMock()
            mock_dedup.get_poll_window.return_value = (
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            )
            mock_dedup.build_deduped_query.return_value = "test query"
            mock_dedup_class.return_value = mock_dedup

            async with KQLEventPoller(config) as poller:
                # Run multiple empty poll cycles
                await poller._poll_cycle()
                assert poller._consecutive_empty_polls == 1

                await poller._poll_cycle()
                assert poller._consecutive_empty_polls == 2

                await poller._poll_cycle()
                assert poller._consecutive_empty_polls == 3
