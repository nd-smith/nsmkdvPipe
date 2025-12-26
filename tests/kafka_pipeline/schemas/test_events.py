"""
Tests for EventMessage schema.

Validates Pydantic model behavior, JSON serialization, and field validation.
"""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kafka_pipeline.schemas.events import EventMessage


class TestEventMessageCreation:
    """Test EventMessage instantiation with valid data."""

    def test_create_with_all_fields(self):
        """EventMessage can be created with all fields populated."""
        event = EventMessage(
            trace_id="evt-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            source_system="claimx",
            payload={"claim_id": "C-456", "amount": 10000},
            attachments=["https://storage.example.com/file1.pdf"]
        )

        assert event.trace_id == "evt-123"
        assert event.event_type == "claim"
        assert event.event_subtype == "created"
        assert event.source_system == "claimx"
        assert event.payload == {"claim_id": "C-456", "amount": 10000}
        assert event.attachments == ["https://storage.example.com/file1.pdf"]

    def test_create_without_attachments(self):
        """EventMessage can be created without attachments (optional field)."""
        event = EventMessage(
            trace_id="evt-456",
            event_type="policy",
            event_subtype="updated",
            timestamp=datetime.now(timezone.utc),
            source_system="policysys",
            payload={"policy_id": "P-789"}
        )

        assert event.attachments is None

    def test_create_with_empty_attachments_list(self):
        """Empty attachments list is normalized to None."""
        event = EventMessage(
            trace_id="evt-789",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload={},
            attachments=[]
        )

        assert event.attachments is None

    def test_create_with_complex_payload(self):
        """Payload can contain nested structures."""
        complex_payload = {
            "claim_id": "C-001",
            "details": {
                "amount": 50000,
                "category": "property",
                "metadata": {
                    "filed_by": "agent-123",
                    "priority": "high"
                }
            },
            "items": [
                {"item_id": "I-1", "value": 1000},
                {"item_id": "I-2", "value": 2000}
            ]
        }

        event = EventMessage(
            trace_id="evt-complex",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload=complex_payload
        )

        assert event.payload == complex_payload
        assert event.payload["details"]["metadata"]["priority"] == "high"


class TestEventMessageValidation:
    """Test field validation rules."""

    def test_missing_required_field_raises_error(self):
        """Missing required fields raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EventMessage(
                event_type="claim",
                event_subtype="created",
                timestamp=datetime.now(timezone.utc),
                source_system="claimx",
                payload={}
                # Missing trace_id
            )

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('trace_id',) for e in errors)

    def test_empty_trace_id_raises_error(self):
        """Empty trace_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EventMessage(
                trace_id="",
                event_type="claim",
                event_subtype="created",
                timestamp=datetime.now(timezone.utc),
                source_system="claimx",
                payload={}
            )

        errors = exc_info.value.errors()
        assert any('trace_id' in str(e) for e in errors)

    def test_whitespace_trace_id_raises_error(self):
        """Whitespace-only trace_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            EventMessage(
                trace_id="   ",
                event_type="claim",
                event_subtype="created",
                timestamp=datetime.now(timezone.utc),
                source_system="claimx",
                payload={}
            )

        errors = exc_info.value.errors()
        assert any('trace_id' in str(e) for e in errors)

    def test_whitespace_is_trimmed(self):
        """Leading/trailing whitespace is trimmed from string fields."""
        event = EventMessage(
            trace_id="  evt-123  ",
            event_type="  claim  ",
            event_subtype="  created  ",
            timestamp=datetime.now(timezone.utc),
            source_system="  claimx  ",
            payload={}
        )

        assert event.trace_id == "evt-123"
        assert event.event_type == "claim"
        assert event.event_subtype == "created"
        assert event.source_system == "claimx"

    def test_invalid_attachments_type_raises_error(self):
        """Attachments must be a list if provided."""
        with pytest.raises(ValidationError):
            EventMessage(
                trace_id="evt-123",
                event_type="claim",
                event_subtype="created",
                timestamp=datetime.now(timezone.utc),
                source_system="claimx",
                payload={},
                attachments="not-a-list"  # Invalid: should be list
            )

    def test_attachments_filters_empty_strings(self):
        """Empty strings in attachments list are filtered out."""
        event = EventMessage(
            trace_id="evt-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload={},
            attachments=["https://example.com/file1.pdf", "", "  ", "https://example.com/file2.pdf"]
        )

        assert event.attachments == ["https://example.com/file1.pdf", "https://example.com/file2.pdf"]


class TestEventMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_serialize_to_json(self):
        """EventMessage serializes to valid JSON."""
        event = EventMessage(
            trace_id="evt-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            source_system="claimx",
            payload={"claim_id": "C-456"},
            attachments=["https://example.com/file1.pdf"]
        )

        json_str = event.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["trace_id"] == "evt-123"
        assert parsed["event_type"] == "claim"
        assert parsed["timestamp"] == "2024-12-25T10:30:00+00:00"
        assert parsed["payload"] == {"claim_id": "C-456"}

    def test_deserialize_from_json(self):
        """EventMessage can be created from JSON string."""
        json_data = {
            "trace_id": "evt-789",
            "event_type": "policy",
            "event_subtype": "updated",
            "timestamp": "2024-12-25T15:45:00Z",
            "source_system": "policysys",
            "payload": {"policy_id": "P-001"},
            "attachments": None
        }

        json_str = json.dumps(json_data)
        event = EventMessage.model_validate_json(json_str)

        assert event.trace_id == "evt-789"
        assert event.event_type == "policy"
        assert event.payload == {"policy_id": "P-001"}
        assert event.attachments is None

    def test_datetime_serialization_iso_format(self):
        """Datetime fields serialize to ISO 8601 format."""
        event = EventMessage(
            trace_id="evt-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime(2024, 12, 25, 10, 30, 45, tzinfo=timezone.utc),
            source_system="claimx",
            payload={}
        )

        json_str = event.model_dump_json()
        parsed = json.loads(json_str)

        # Verify ISO 8601 format
        assert "2024-12-25" in parsed["timestamp"]
        assert "10:30:45" in parsed["timestamp"]

        # Verify it can be parsed back
        timestamp_str = parsed["timestamp"]
        datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

    def test_round_trip_serialization(self):
        """Data survives JSON serialization round-trip."""
        original = EventMessage(
            trace_id="evt-round-trip",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            source_system="claimx",
            payload={"claim_id": "C-456", "amount": 10000},
            attachments=["https://example.com/file1.pdf", "https://example.com/file2.pdf"]
        )

        # Serialize to JSON and back
        json_str = original.model_dump_json()
        restored = EventMessage.model_validate_json(json_str)

        assert restored.trace_id == original.trace_id
        assert restored.event_type == original.event_type
        assert restored.event_subtype == original.event_subtype
        assert restored.timestamp == original.timestamp
        assert restored.source_system == original.source_system
        assert restored.payload == original.payload
        assert restored.attachments == original.attachments

    def test_model_dump_dict(self):
        """model_dump() returns dict with serialized datetime."""
        event = EventMessage(
            trace_id="evt-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            source_system="claimx",
            payload={"claim_id": "C-456"}
        )

        data = event.model_dump()

        assert isinstance(data, dict)
        assert data["trace_id"] == "evt-123"
        # field_serializer applies even in regular model_dump()
        assert isinstance(data["timestamp"], str)
        assert "2024-12-25" in data["timestamp"]

    def test_model_dump_with_mode_json(self):
        """model_dump(mode='json') converts datetime to string."""
        event = EventMessage(
            trace_id="evt-123",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime(2024, 12, 25, 10, 30, 0, tzinfo=timezone.utc),
            source_system="claimx",
            payload={"claim_id": "C-456"}
        )

        data = event.model_dump(mode='json')

        assert isinstance(data, dict)
        assert isinstance(data["timestamp"], str)
        assert "2024-12-25" in data["timestamp"]


class TestEventMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_payload_is_valid(self):
        """Payload can be an empty dict."""
        event = EventMessage(
            trace_id="evt-empty",
            event_type="test",
            event_subtype="empty",
            timestamp=datetime.now(timezone.utc),
            source_system="testsys",
            payload={}
        )

        assert event.payload == {}

    def test_very_long_trace_id(self):
        """Very long trace_id is accepted."""
        long_id = "evt-" + "x" * 1000
        event = EventMessage(
            trace_id=long_id,
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload={}
        )

        assert event.trace_id == long_id

    def test_unicode_in_payload(self):
        """Payload can contain Unicode characters."""
        event = EventMessage(
            trace_id="evt-unicode",
            event_type="claim",
            event_subtype="created",
            timestamp=datetime.now(timezone.utc),
            source_system="claimx",
            payload={
                "description": "Schäden an Gebäude",
                "note": "重要な情報",
                "emoji": "🔥💧"
            }
        )

        assert event.payload["description"] == "Schäden an Gebäude"
        assert event.payload["note"] == "重要な情報"
        assert event.payload["emoji"] == "🔥💧"

    def test_naive_datetime_is_accepted(self):
        """Naive datetime (without timezone) is accepted."""
        naive_dt = datetime(2024, 12, 25, 10, 30, 0)
        event = EventMessage(
            trace_id="evt-naive",
            event_type="claim",
            event_subtype="created",
            timestamp=naive_dt,
            source_system="claimx",
            payload={}
        )

        assert event.timestamp == naive_dt
