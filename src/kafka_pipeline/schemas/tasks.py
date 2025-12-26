"""
Download task message schemas for Kafka pipeline.

Contains Pydantic models for download work items sent to download workers.
"""

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, Field, field_serializer, field_validator


class DownloadTaskMessage(BaseModel):
    """Schema for download work items sent to download workers.

    Represents a single attachment download task derived from an event.
    Includes retry tracking and metadata for observability.

    Attributes:
        trace_id: Unique identifier from the source event (for correlation)
        attachment_url: URL of the attachment to download
        destination_path: Target path in blob storage for the downloaded file
        event_type: High-level event category from source event
        event_subtype: Specific event action from source event
        retry_count: Number of times this task has been retried (starts at 0)
        original_timestamp: Timestamp from the original event (preserved through retries)
        metadata: Extensible metadata dict for additional context

    Example:
        >>> from datetime import datetime, timezone
        >>> task = DownloadTaskMessage(
        ...     trace_id="evt-123",
        ...     attachment_url="https://storage.example.com/file.pdf",
        ...     destination_path="claims/C-456/file.pdf",
        ...     event_type="claim",
        ...     event_subtype="created",
        ...     retry_count=0,
        ...     original_timestamp=datetime.now(timezone.utc),
        ...     metadata={"file_size": 1024, "content_type": "application/pdf"}
        ... )
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier for correlation",
        min_length=1
    )
    attachment_url: str = Field(
        ...,
        description="URL of the attachment to download",
        min_length=1
    )
    destination_path: str = Field(
        ...,
        description="Target path in blob storage",
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
    retry_count: int = Field(
        default=0,
        description="Number of retry attempts (starts at 0)",
        ge=0
    )
    original_timestamp: datetime = Field(
        ...,
        description="Timestamp from original event (preserved through retries)"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Extensible metadata for additional context"
    )

    @field_validator('trace_id', 'attachment_url', 'destination_path', 'event_type', 'event_subtype')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('retry_count')
    @classmethod
    def validate_retry_count(cls, v: int) -> int:
        """Ensure retry_count is non-negative."""
        if v < 0:
            raise ValueError("retry_count must be non-negative")
        return v

    @field_serializer('original_timestamp')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'evt-2024-001',
                    'attachment_url': 'https://storage.example.com/claims/C-12345/document.pdf',
                    'destination_path': 'claims/C-12345/document.pdf',
                    'event_type': 'claim',
                    'event_subtype': 'created',
                    'retry_count': 0,
                    'original_timestamp': '2024-12-25T10:30:00Z',
                    'metadata': {
                        'expected_size': 2048576,
                        'content_type': 'application/pdf',
                        'source_system': 'claimx'
                    }
                },
                {
                    'trace_id': 'evt-2024-002',
                    'attachment_url': 'https://storage.example.com/policies/P-67890/image.jpg',
                    'destination_path': 'policies/P-67890/image.jpg',
                    'event_type': 'policy',
                    'event_subtype': 'updated',
                    'retry_count': 2,
                    'original_timestamp': '2024-12-25T09:15:00Z',
                    'metadata': {
                        'retry_reason': 'transient_network_error'
                    }
                }
            ]
        }
    }
