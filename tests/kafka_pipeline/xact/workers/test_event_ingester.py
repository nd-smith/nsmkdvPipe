"""
Unit tests for EventIngesterWorker.

Tests event consumption, URL validation, path generation, and download task production.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.structs import ConsumerRecord, RecordMetadata

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker


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
def sample_event():
    """Create sample EventMessage for testing."""
    import json
    data = {
        "assignmentId": "A-456",
        "claim_id": "C-789",
        "attachments": [
            "https://claimxperience.com/files/document1.pdf",
            "https://claimxperience.com/files/document2.pdf",
        ],
    }
    return EventMessage(
        type="verisk.claims.property.xn.documentsReceived",
        version=1,
        utcDateTime=datetime.now(timezone.utc).isoformat(),
        traceId="evt-123",
        data=json.dumps(data),
    )


@pytest.fixture
def sample_consumer_record(sample_event):
    """Create sample ConsumerRecord with EventMessage."""
    return ConsumerRecord(
        topic="test.events.raw",
        partition=0,
        offset=10,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        timestamp_type=0,
        key=b"evt-123",
        value=sample_event.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=7,
        serialized_value_size=len(sample_event.model_dump_json()),
    )


@pytest.mark.asyncio
class TestEventIngesterWorker:
    """Test suite for EventIngesterWorker."""

    async def test_initialization(self, kafka_config):
        """Test worker initialization with correct configuration."""
        worker = EventIngesterWorker(kafka_config)

        assert worker.config == kafka_config
        assert worker.consumer_group == "test-event-ingester"
        assert worker.producer is None
        assert worker.consumer is None

    async def test_start_and_stop(self, kafka_config):
        """Test worker start and stop lifecycle."""
        worker = EventIngesterWorker(kafka_config)

        # Mock producer and consumer
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        with patch(
            "kafka_pipeline.workers.event_ingester.BaseKafkaProducer"
        ) as mock_producer_class, patch(
            "kafka_pipeline.workers.event_ingester.BaseKafkaConsumer"
        ) as mock_consumer_class:
            mock_producer_class.return_value = mock_producer
            mock_consumer_class.return_value = mock_consumer

            # Start should initialize producer and consumer
            # We can't test the full start() because consumer.start() blocks
            # Instead, test that components are created correctly
            worker.producer = mock_producer
            worker.consumer = mock_consumer

            # Test stop
            await worker.stop()

            # Verify stop was called on both
            mock_consumer.stop.assert_called_once()
            mock_producer.stop.assert_called_once()

    async def test_handle_event_with_attachments(
        self, kafka_config, sample_consumer_record, sample_event
    ):
        """Test processing event with valid attachments."""
        worker = EventIngesterWorker(kafka_config)

        # Mock producer
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "test.downloads.pending"
        mock_metadata.partition = 0
        mock_metadata.offset = 20
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Process the message
        await worker._handle_event_message(sample_consumer_record)

        # Verify producer.send was called twice (two attachments)
        assert mock_producer.send.call_count == 2

        # Verify first download task
        first_call = mock_producer.send.call_args_list[0]
        assert first_call.kwargs["topic"] == "test.downloads.pending"
        assert first_call.kwargs["key"] == "evt-123"
        assert first_call.kwargs["headers"] == {"trace_id": "evt-123"}

        # Verify download task content
        download_task = first_call.kwargs["value"]
        assert isinstance(download_task, DownloadTaskMessage)
        assert download_task.trace_id == "evt-123"
        assert download_task.event_type == "claim"
        assert download_task.event_subtype == "documentsReceived"
        assert download_task.retry_count == 0
        assert "claimxperience.com" in download_task.attachment_url
        assert "documentsReceived/A-456/evt-123/" in download_task.destination_path
        assert download_task.metadata["assignment_id"] == "A-456"
        assert download_task.metadata["source_system"] == "claimx"

    async def test_handle_event_without_attachments(self, kafka_config, sample_event):
        """Test that events without attachments are skipped."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Create event without attachments
        event_no_attachments = sample_event.model_copy()
        event_no_attachments.attachments = None

        record = ConsumerRecord(
            topic="test.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=event_no_attachments.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(event_no_attachments.model_dump_json()),
        )

        # Process the message
        await worker._handle_event_message(record)

        # Verify no tasks were produced
        mock_producer.send.assert_not_called()

    async def test_handle_event_missing_assignment_id(
        self, kafka_config, sample_event
    ):
        """Test that events without assignment_id are skipped."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Create event without assignment_id
        event_no_assignment = sample_event.model_copy()
        event_no_assignment.payload = {"claim_id": "C-789"}  # No assignment_id

        record = ConsumerRecord(
            topic="test.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=event_no_assignment.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(event_no_assignment.model_dump_json()),
        )

        # Process the message
        await worker._handle_event_message(record)

        # Verify no tasks were produced
        mock_producer.send.assert_not_called()

    async def test_handle_event_invalid_url(self, kafka_config, sample_event):
        """Test that invalid URLs are skipped with warning."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        worker.producer = mock_producer

        # Create event with invalid URL (not in allowlist)
        event_invalid_url = sample_event.model_copy()
        event_invalid_url.attachments = ["https://evil.com/malware.exe"]

        record = ConsumerRecord(
            topic="test.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=event_invalid_url.model_dump_json().encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(event_invalid_url.model_dump_json()),
        )

        # Process the message
        await worker._handle_event_message(record)

        # Verify no tasks were produced
        mock_producer.send.assert_not_called()

    async def test_handle_event_invalid_json(self, kafka_config):
        """Test that invalid JSON raises exception."""
        worker = EventIngesterWorker(kafka_config)

        record = ConsumerRecord(
            topic="test.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=b"invalid json{{{",
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=16,
        )

        # Should raise JSONDecodeError
        with pytest.raises(json.JSONDecodeError):
            await worker._handle_event_message(record)

    async def test_handle_event_invalid_schema(self, kafka_config):
        """Test that schema validation errors raise exception."""
        worker = EventIngesterWorker(kafka_config)

        # Create invalid event data (missing required fields)
        invalid_event = {"trace_id": "evt-123"}  # Missing many required fields

        record = ConsumerRecord(
            topic="test.events.raw",
            partition=0,
            offset=10,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            timestamp_type=0,
            key=b"evt-123",
            value=json.dumps(invalid_event).encode("utf-8"),
            headers=[],
            checksum=None,
            serialized_key_size=7,
            serialized_value_size=len(json.dumps(invalid_event)),
        )

        # Should raise ValidationError
        with pytest.raises(Exception):  # ValidationError from Pydantic
            await worker._handle_event_message(record)

    async def test_process_attachment_path_generation(self, kafka_config, sample_event):
        """Test blob path generation for different event subtypes."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "test.downloads.pending"
        mock_metadata.partition = 0
        mock_metadata.offset = 20
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Test documentsReceived
        await worker._process_attachment(
            event=sample_event,
            attachment_url="https://claimxperience.com/files/doc.pdf",
            assignment_id="A-456",
        )

        call_args = mock_producer.send.call_args
        download_task = call_args.kwargs["value"]
        assert "documentsReceived/A-456/evt-123/" in download_task.destination_path
        assert download_task.metadata["file_type"] == "PDF"

    async def test_process_attachment_fnol_event(self, kafka_config, sample_event):
        """Test path generation for FNOL events."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "test.downloads.pending"
        mock_metadata.partition = 0
        mock_metadata.offset = 20
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Update event to FNOL subtype
        fnol_event = sample_event.model_copy()
        fnol_event.event_subtype = "firstNoticeOfLossReceived"

        await worker._process_attachment(
            event=fnol_event,
            attachment_url="https://claimxperience.com/files/report.pdf",
            assignment_id="A-456",
        )

        call_args = mock_producer.send.call_args
        download_task = call_args.kwargs["value"]
        assert "firstNoticeOfLossReceived/A-456/evt-123/" in download_task.destination_path
        assert "_FNOL_" in download_task.destination_path

    async def test_process_attachment_estimate_package(self, kafka_config, sample_event):
        """Test path generation for estimate package events."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "test.downloads.pending"
        mock_metadata.partition = 0
        mock_metadata.offset = 20
        mock_producer.send.return_value = mock_metadata
        worker.producer = mock_producer

        # Update event to estimate package subtype with version
        est_event = sample_event.model_copy()
        est_event.event_subtype = "estimatePackageReceived"
        est_event.payload["estimate_version"] = "v2"

        await worker._process_attachment(
            event=est_event,
            attachment_url="https://claimxperience.com/files/estimate.pdf",
            assignment_id="A-456",
        )

        call_args = mock_producer.send.call_args
        download_task = call_args.kwargs["value"]
        assert "estimatePackageReceived/A-456/evt-123/" in download_task.destination_path
        assert "_v2_" in download_task.destination_path

    async def test_process_attachment_producer_failure(
        self, kafka_config, sample_event
    ):
        """Test that producer failures are propagated."""
        worker = EventIngesterWorker(kafka_config)
        mock_producer = AsyncMock()
        mock_producer.send.side_effect = Exception("Kafka broker unavailable")
        worker.producer = mock_producer

        # Should raise the producer exception
        with pytest.raises(Exception, match="Kafka broker unavailable"):
            await worker._process_attachment(
                event=sample_event,
                attachment_url="https://claimxperience.com/files/doc.pdf",
                assignment_id="A-456",
            )
