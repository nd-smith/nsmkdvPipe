"""
Event message schemas for Kafka pipeline.

Contains Pydantic models for raw event messages consumed from source topics.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


class EventMessage(BaseModel):
    """Schema for raw event messages from source system.

    Represents the initial event data consumed from the source Kafka topic,
    before processing and extraction into download tasks.

    Attributes:
        trace_id: Unique identifier for the event, used for deduplication
        event_type: High-level event category (e.g., "claim", "policy")
        event_subtype: Specific event action (e.g., "created", "updated")
        timestamp: When the event occurred in the source system
        source_system: Identifier of the originating system
        payload: Event-specific data (flexible structure)
        attachments: Optional list of attachment URLs to download

    Example:
        >>> event = EventMessage(
        ...     trace_id="evt-123",
        ...     event_type="claim",
        ...     event_subtype="created",
        ...     timestamp=datetime.now(timezone.utc),
        ...     source_system="claimx",
        ...     payload={"claim_id": "C-456", "amount": 10000},
        ...     attachments=["https://storage.example.com/file1.pdf"]
        ... )
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier for deduplication",
        min_length=1
    )
    event_type: str = Field(
        ...,
        description="High-level event category",
        min_length=1
    )
    event_subtype: str = Field(
        ...,
        description="Specific event action or subtype",
        min_length=1
    )
    timestamp: datetime = Field(
        ...,
        description="Event occurrence timestamp (UTC)"
    )
    source_system: str = Field(
        ...,
        description="Originating system identifier",
        min_length=1
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="Event-specific data (flexible structure)"
    )
    attachments: Optional[List[str]] = Field(
        default=None,
        description="Optional list of attachment URLs"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional metadata for additional context"
    )

    @field_validator('trace_id', 'event_type', 'event_subtype', 'source_system')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('attachments')
    @classmethod
    def validate_attachments(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Ensure attachments list contains valid URLs if provided."""
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("attachments must be a list")
            # Filter out None or empty strings
            v = [url for url in v if url and isinstance(url, str) and url.strip()]
            return v if v else None
        return v

    @field_serializer('timestamp')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'evt-2024-001',
                    'event_type': 'claim',
                    'event_subtype': 'created',
                    'timestamp': '2024-12-25T10:30:00Z',
                    'source_system': 'claimx',
                    'payload': {
                        'claim_id': 'C-12345',
                        'amount': 10000,
                        'status': 'pending'
                    },
                    'attachments': [
                        'https://storage.example.com/claims/C-12345/document1.pdf'
                    ]
                }
            ]
        }
    }
