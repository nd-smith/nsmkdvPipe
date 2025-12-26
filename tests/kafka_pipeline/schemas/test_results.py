"""
Tests for DownloadResultMessage and FailedDownloadMessage schemas.

Validates Pydantic model behavior, JSON serialization, status enum handling,
error truncation, and DLQ message structure.
"""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.schemas.results import DownloadResultMessage, FailedDownloadMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage


class TestDownloadResultMessageCreation:
    """Test DownloadResultMessage instantiation with valid data."""

    def test_create_success_result(self):
        """DownloadResultMessage can be created for successful download."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="success",
            destination_path="claims/C-456/file.pdf",
            bytes_downloaded=2048576,
            processing_time_ms=1250,
            completed_at=datetime(2024, 12, 25, 10, 31, 15, tzinfo=timezone.utc)
        )

        assert result.trace_id == "evt-123"
        assert result.status == "success"
        assert result.destination_path == "claims/C-456/file.pdf"
        assert result.bytes_downloaded == 2048576
        assert result.error_message is None
        assert result.error_category is None

    def test_create_failed_transient_result(self):
        """DownloadResultMessage can be created for transient failure."""
        result = DownloadResultMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_transient",
            error_message="Connection timeout after 30 seconds",
            error_category="transient",
            processing_time_ms=30150,
            completed_at=datetime.now(timezone.utc)
        )

        assert result.status == "failed_transient"
        assert result.destination_path is None
        assert result.bytes_downloaded is None
        assert result.error_message == "Connection timeout after 30 seconds"
        assert result.error_category == "transient"

    def test_create_failed_permanent_result(self):
        """DownloadResultMessage can be created for permanent failure."""
        result = DownloadResultMessage(
            trace_id="evt-789",
            attachment_url="https://storage.example.com/invalid.exe",
            status="failed_permanent",
            error_message="File type .exe not allowed",
            error_category="permanent",
            processing_time_ms=50,
            completed_at=datetime.now(timezone.utc)
        )

        assert result.status == "failed_permanent"
        assert result.error_message == "File type .exe not allowed"
        assert result.error_category == "permanent"


class TestDownloadResultMessageValidation:
    """Test field validation rules for DownloadResultMessage."""

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadResultMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                status="success"
                # Missing processing_time_ms and completed_at
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('processing_time_ms',) for e in errors)
        assert any(e['loc'] == ('completed_at',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="",
                attachment_url="https://storage.example.com/file.pdf",
                status="success",
                processing_time_ms=1000,
                completed_at=datetime.now(timezone.utc)
            )

    def test_whitespace_trace_id_raises_error(self):
        """Whitespace-only trace_id raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="   ",
                attachment_url="https://storage.example.com/file.pdf",
                status="success",
                processing_time_ms=1000,
                completed_at=datetime.now(timezone.utc)
            )

    def test_invalid_status_raises_error(self):
        """Invalid status value raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                status="invalid_status",
                processing_time_ms=1000,
                completed_at=datetime.now(timezone.utc)
            )

    def test_negative_bytes_downloaded_raises_error(self):
        """Negative bytes_downloaded raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                status="success",
                bytes_downloaded=-100,
                processing_time_ms=1000,
                completed_at=datetime.now(timezone.utc)
            )

    def test_negative_processing_time_raises_error(self):
        """Negative processing_time_ms raises ValidationError."""
        with pytest.raises(ValidationError):
            DownloadResultMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf",
                status="success",
                processing_time_ms=-1000,
                completed_at=datetime.now(timezone.utc)
            )

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        result = DownloadResultMessage(
            trace_id="  evt-123  ",
            attachment_url="  https://storage.example.com/file.pdf  ",
            status="success",
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )

        assert result.trace_id == "evt-123"
        assert result.attachment_url == "https://storage.example.com/file.pdf"


class TestDownloadResultMessageStatusEnum:
    """Test status field values and validation."""

    def test_success_status_is_valid(self):
        """Status 'success' is accepted."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="success",
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )
        assert result.status == "success"

    def test_failed_transient_status_is_valid(self):
        """Status 'failed_transient' is accepted."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_transient",
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )
        assert result.status == "failed_transient"

    def test_failed_permanent_status_is_valid(self):
        """Status 'failed_permanent' is accepted."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_permanent",
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )
        assert result.status == "failed_permanent"


class TestDownloadResultMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_serialize_success_to_json(self):
        """DownloadResultMessage serializes to valid JSON."""
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="success",
            destination_path="claims/C-456/file.pdf",
            bytes_downloaded=2048576,
            processing_time_ms=1250,
            completed_at=datetime(2024, 12, 25, 10, 31, 15, tzinfo=timezone.utc)
        )

        json_str = result.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-123"
        assert parsed["status"] == "success"
        assert parsed["destination_path"] == "claims/C-456/file.pdf"
        assert parsed["bytes_downloaded"] == 2048576
        assert parsed["processing_time_ms"] == 1250
        assert parsed["completed_at"] == "2024-12-25T10:31:15+00:00"

    def test_serialize_failure_to_json(self):
        """Failed result serializes with error fields."""
        result = DownloadResultMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_transient",
            error_message="Connection timeout",
            error_category="transient",
            processing_time_ms=30000,
            completed_at=datetime(2024, 12, 25, 10, 31, 45, tzinfo=timezone.utc)
        )

        json_str = result.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["status"] == "failed_transient"
        assert parsed["error_message"] == "Connection timeout"
        assert parsed["error_category"] == "transient"
        assert parsed["destination_path"] is None
        assert parsed["bytes_downloaded"] is None

    def test_deserialize_from_json(self):
        """DownloadResultMessage can be created from JSON."""
        json_data = {
            "trace_id": "evt-789",
            "attachment_url": "https://storage.example.com/doc.pdf",
            "status": "success",
            "destination_path": "policies/P-001/doc.pdf",
            "bytes_downloaded": 1024,
            "error_message": None,
            "error_category": None,
            "processing_time_ms": 500,
            "completed_at": "2024-12-25T15:45:00Z"
        }

        json_str = json.dumps(json_data)
        result = DownloadResultMessage.model_validate_json(json_str)

        assert result.trace_id == "evt-789"
        assert result.status == "success"
        assert result.bytes_downloaded == 1024

    def test_round_trip_serialization(self):
        """Data survives JSON serialization round-trip."""
        original = DownloadResultMessage(
            trace_id="evt-round-trip",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_permanent",
            error_message="Invalid file type",
            error_category="permanent",
            processing_time_ms=100,
            completed_at=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        json_str = original.model_dump_json()
        restored = DownloadResultMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.status == original.status
        assert restored.error_message == original.error_message
        assert restored.processing_time_ms == original.processing_time_ms


class TestErrorMessageTruncation:
    """Test error message truncation to prevent huge messages."""

    def test_error_message_under_limit_not_truncated(self):
        """Error messages under 500 chars are not truncated."""
        short_error = "Connection timeout after 30 seconds"
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_transient",
            error_message=short_error,
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )

        assert result.error_message == short_error

    def test_error_message_over_limit_is_truncated(self):
        """Error messages over 500 chars are truncated."""
        long_error = "x" * 600
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_transient",
            error_message=long_error,
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )

        assert len(result.error_message) == 500
        assert result.error_message.endswith("...")

    def test_error_message_exactly_500_chars_not_truncated(self):
        """Error message exactly 500 chars is not truncated."""
        exact_error = "x" * 500
        result = DownloadResultMessage(
            trace_id="evt-123",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_transient",
            error_message=exact_error,
            processing_time_ms=1000,
            completed_at=datetime.now(timezone.utc)
        )

        assert len(result.error_message) == 500
        assert result.error_message == exact_error


class TestFailedDownloadMessageCreation:
    """Test FailedDownloadMessage instantiation for DLQ."""

    def test_create_dlq_message(self):
        """FailedDownloadMessage can be created with original task."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/bad.pdf",
            destination_path="claims/C-789/bad.pdf",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc)
        )

        dlq = FailedDownloadMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/bad.pdf",
            original_task=task,
            final_error="File not found after 4 retries",
            error_category="permanent",
            retry_count=4,
            failed_at=datetime(2024, 12, 25, 10, 45, 0, tzinfo=timezone.utc)
        )

        assert dlq.trace_id == "evt-456"
        assert dlq.original_task == task
        assert dlq.final_error == "File not found after 4 retries"
        assert dlq.error_category == "permanent"
        assert dlq.retry_count == 4

    def test_create_dlq_with_truncated_error(self):
        """FailedDownloadMessage truncates long error messages."""
        task = DownloadTaskMessage(
            trace_id="evt-error",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-001/file.pdf",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime.now(timezone.utc)
        )

        long_error = "Error: " + "x" * 600
        dlq = FailedDownloadMessage(
            trace_id="evt-error",
            attachment_url="https://storage.example.com/file.pdf",
            original_task=task,
            final_error=long_error,
            error_category="transient",
            retry_count=4,
            failed_at=datetime.now(timezone.utc)
        )

        assert len(dlq.final_error) == 500
        assert dlq.final_error.endswith("...")


class TestFailedDownloadMessageValidation:
    """Test field validation rules for FailedDownloadMessage."""

    def test_missing_required_fields_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            FailedDownloadMessage(
                trace_id="evt-123",
                attachment_url="https://storage.example.com/file.pdf"
                # Missing original_task, final_error, error_category, retry_count, failed_at
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('original_task',) for e in errors)
        assert any(e['loc'] == ('final_error',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-789/file.pdf",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="",
                attachment_url="https://storage.example.com/file.pdf",
                original_task=task,
                final_error="Error occurred",
                error_category="permanent",
                retry_count=4,
                failed_at=datetime.now(timezone.utc)
            )

    def test_empty_final_error_raises_error(self):
        """Empty final_error raises ValidationError."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-789/file.pdf",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="evt-456",
                attachment_url="https://storage.example.com/file.pdf",
                original_task=task,
                final_error="",
                error_category="permanent",
                retry_count=4,
                failed_at=datetime.now(timezone.utc)
            )

    def test_whitespace_final_error_raises_error(self):
        """Whitespace-only final_error raises ValidationError."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-789/file.pdf",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        with pytest.raises(ValidationError):
            FailedDownloadMessage(
                trace_id="evt-456",
                attachment_url="https://storage.example.com/file.pdf",
                original_task=task,
                final_error="   ",
                error_category="permanent",
                retry_count=4,
                failed_at=datetime.now(timezone.utc)
            )

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        task = DownloadTaskMessage(
            trace_id="evt-456",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-789/file.pdf",
            event_type="claim",
            event_subtype="created",
            original_timestamp=datetime.now(timezone.utc)
        )

        dlq = FailedDownloadMessage(
            trace_id="  evt-456  ",
            attachment_url="  https://storage.example.com/file.pdf  ",
            original_task=task,
            final_error="  Error occurred  ",
            error_category="  permanent  ",
            retry_count=4,
            failed_at=datetime.now(timezone.utc)
        )

        assert dlq.trace_id == "evt-456"
        assert dlq.attachment_url == "https://storage.example.com/file.pdf"
        assert dlq.final_error == "Error occurred"
        assert dlq.error_category == "permanent"


class TestFailedDownloadMessageSerialization:
    """Test JSON serialization and deserialization for DLQ messages."""

    def test_serialize_to_json(self):
        """FailedDownloadMessage serializes to valid JSON with nested task."""
        task = DownloadTaskMessage(
            trace_id="evt-dlq",
            attachment_url="https://storage.example.com/missing.pdf",
            destination_path="claims/C-999/missing.pdf",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc),
            metadata={"last_error": "File not found (404)"}
        )

        dlq = FailedDownloadMessage(
            trace_id="evt-dlq",
            attachment_url="https://storage.example.com/missing.pdf",
            original_task=task,
            final_error="File not found (404) - exhausted retries",
            error_category="permanent",
            retry_count=4,
            failed_at=datetime(2024, 12, 25, 10, 45, 0, tzinfo=timezone.utc)
        )

        json_str = dlq.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-dlq"
        assert parsed["final_error"] == "File not found (404) - exhausted retries"
        assert parsed["retry_count"] == 4
        assert "original_task" in parsed
        assert parsed["original_task"]["trace_id"] == "evt-dlq"
        assert parsed["original_task"]["retry_count"] == 4

    def test_deserialize_from_json(self):
        """FailedDownloadMessage can be created from JSON."""
        json_data = {
            "trace_id": "evt-dlq",
            "attachment_url": "https://storage.example.com/missing.pdf",
            "original_task": {
                "trace_id": "evt-dlq",
                "attachment_url": "https://storage.example.com/missing.pdf",
                "destination_path": "claims/C-999/missing.pdf",
                "event_type": "claim",
                "event_subtype": "created",
                "retry_count": 4,
                "original_timestamp": "2024-12-25T10:00:00Z",
                "metadata": {}
            },
            "final_error": "File not found",
            "error_category": "permanent",
            "retry_count": 4,
            "failed_at": "2024-12-25T10:45:00Z"
        }

        json_str = json.dumps(json_data)
        dlq = FailedDownloadMessage.model_validate_json(json_str)

        assert dlq.trace_id == "evt-dlq"
        assert dlq.final_error == "File not found"
        assert dlq.retry_count == 4
        assert isinstance(dlq.original_task, DownloadTaskMessage)
        assert dlq.original_task.trace_id == "evt-dlq"

    def test_round_trip_serialization(self):
        """DLQ message survives JSON serialization round-trip."""
        task = DownloadTaskMessage(
            trace_id="evt-round",
            attachment_url="https://storage.example.com/file.pdf",
            destination_path="claims/C-123/file.pdf",
            event_type="claim",
            event_subtype="created",
            retry_count=4,
            original_timestamp=datetime(2024, 12, 25, 10, 0, 0, tzinfo=timezone.utc)
        )

        original = FailedDownloadMessage(
            trace_id="evt-round",
            attachment_url="https://storage.example.com/file.pdf",
            original_task=task,
            final_error="Connection failed",
            error_category="transient",
            retry_count=4,
            failed_at=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        )

        json_str = original.model_dump_json()
        restored = FailedDownloadMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.final_error == original.final_error
        assert restored.retry_count == original.retry_count
        assert restored.original_task.trace_id == original.original_task.trace_id


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_bytes_downloaded_is_valid(self):
        """Zero bytes downloaded is a valid value."""
        result = DownloadResultMessage(
            trace_id="evt-zero",
            attachment_url="https://storage.example.com/empty.txt",
            status="success",
            bytes_downloaded=0,
            processing_time_ms=100,
            completed_at=datetime.now(timezone.utc)
        )
        assert result.bytes_downloaded == 0

    def test_zero_processing_time_is_valid(self):
        """Zero processing time is a valid value."""
        result = DownloadResultMessage(
            trace_id="evt-instant",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_permanent",
            processing_time_ms=0,
            completed_at=datetime.now(timezone.utc)
        )
        assert result.processing_time_ms == 0

    def test_very_large_bytes_downloaded(self):
        """Very large bytes_downloaded values are accepted."""
        result = DownloadResultMessage(
            trace_id="evt-large",
            attachment_url="https://storage.example.com/huge.zip",
            status="success",
            bytes_downloaded=10_737_418_240,  # 10 GB
            processing_time_ms=60000,
            completed_at=datetime.now(timezone.utc)
        )
        assert result.bytes_downloaded == 10_737_418_240

    def test_unicode_in_error_message(self):
        """Error messages can contain Unicode characters."""
        result = DownloadResultMessage(
            trace_id="evt-unicode",
            attachment_url="https://storage.example.com/file.pdf",
            status="failed_permanent",
            error_message="Datei nicht gefunden: Schädenübersicht.pdf 重要",
            processing_time_ms=100,
            completed_at=datetime.now(timezone.utc)
        )
        assert "Schädenübersicht" in result.error_message
        assert "重要" in result.error_message

    def test_naive_datetime_is_accepted(self):
        """Naive datetime (without timezone) is accepted."""
        naive_dt = datetime(2024, 12, 25, 10, 30, 0)
        result = DownloadResultMessage(
            trace_id="evt-naive",
            attachment_url="https://storage.example.com/file.pdf",
            status="success",
            processing_time_ms=1000,
            completed_at=naive_dt
        )
        assert result.completed_at == naive_dt
