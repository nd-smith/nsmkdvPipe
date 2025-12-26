"""
Download result and failure message schemas for Kafka pipeline.

Contains Pydantic models for download outcomes sent to results topic
and failure messages sent to dead-letter queue (DLQ).
"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator

from kafka_pipeline.schemas.tasks import DownloadTaskMessage


class DownloadResultMessage(BaseModel):
    """Schema for download outcomes (success or failure).

    Sent to results topic after download worker processes a task.
    Contains both successful downloads and failed attempts for observability.

    Attributes:
        trace_id: Unique event identifier for correlation
        attachment_url: URL of the attachment that was processed
        status: Outcome status (success, failed_transient, failed_permanent)
        destination_path: Target path in blob storage (None if failed)
        bytes_downloaded: Number of bytes downloaded (None if failed)
        error_message: Error description if failed (truncated to 500 chars)
        error_category: Error classification (transient, permanent, auth, etc.)
        processing_time_ms: Total processing time in milliseconds
        completed_at: Timestamp when processing completed

    Example:
        >>> from datetime import datetime, timezone
        >>> result = DownloadResultMessage(
        ...     trace_id="evt-123",
        ...     attachment_url="https://storage.example.com/file.pdf",
        ...     status="success",
        ...     destination_path="claims/C-456/file.pdf",
        ...     bytes_downloaded=2048576,
        ...     processing_time_ms=1250,
        ...     completed_at=datetime.now(timezone.utc)
        ... )
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier for correlation",
        min_length=1
    )
    attachment_url: str = Field(
        ...,
        description="URL of the attachment that was processed",
        min_length=1
    )
    status: Literal["success", "failed_transient", "failed_permanent"] = Field(
        ...,
        description="Outcome status: success, failed_transient, or failed_permanent"
    )
    destination_path: Optional[str] = Field(
        default=None,
        description="Target path in blob storage (None if download failed)"
    )
    bytes_downloaded: Optional[int] = Field(
        default=None,
        description="Number of bytes downloaded (None if failed)",
        ge=0
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error description if failed (truncated to 500 chars)"
    )
    error_category: Optional[str] = Field(
        default=None,
        description="Error classification (transient, permanent, auth, etc.)"
    )
    processing_time_ms: int = Field(
        ...,
        description="Total processing time in milliseconds",
        ge=0
    )
    completed_at: datetime = Field(
        ...,
        description="Timestamp when processing completed"
    )

    @field_validator('trace_id', 'attachment_url')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('error_message')
    @classmethod
    def truncate_error_message(cls, v: Optional[str]) -> Optional[str]:
        """Truncate error message to prevent huge messages."""
        if v and len(v) > 500:
            return v[:497] + "..."
        return v

    @field_serializer('completed_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'evt-2024-001',
                    'attachment_url': 'https://storage.example.com/claims/C-12345/document.pdf',
                    'status': 'success',
                    'destination_path': 'claims/C-12345/document.pdf',
                    'bytes_downloaded': 2048576,
                    'error_message': None,
                    'error_category': None,
                    'processing_time_ms': 1250,
                    'completed_at': '2024-12-25T10:31:15Z'
                },
                {
                    'trace_id': 'evt-2024-002',
                    'attachment_url': 'https://storage.example.com/policies/P-67890/image.jpg',
                    'status': 'failed_transient',
                    'destination_path': None,
                    'bytes_downloaded': None,
                    'error_message': 'Connection timeout after 30 seconds',
                    'error_category': 'transient',
                    'processing_time_ms': 30150,
                    'completed_at': '2024-12-25T10:31:45Z'
                },
                {
                    'trace_id': 'evt-2024-003',
                    'attachment_url': 'https://storage.example.com/invalid/file.exe',
                    'status': 'failed_permanent',
                    'destination_path': None,
                    'bytes_downloaded': None,
                    'error_message': 'File type .exe not allowed',
                    'error_category': 'permanent',
                    'processing_time_ms': 50,
                    'completed_at': '2024-12-25T10:32:00Z'
                }
            ]
        }
    }


class FailedDownloadMessage(BaseModel):
    """Schema for messages sent to dead-letter queue (DLQ).

    Sent when a download task has exhausted all retry attempts.
    Preserves complete context for manual review and potential replay.

    Attributes:
        trace_id: Unique event identifier for correlation
        attachment_url: URL of the attachment that failed
        original_task: Complete original task message for replay capability
        final_error: Final error message (truncated to 500 chars)
        error_category: Error classification from final attempt
        retry_count: Total number of retry attempts made
        failed_at: Timestamp when task was sent to DLQ

    Example:
        >>> from datetime import datetime, timezone
        >>> task = DownloadTaskMessage(
        ...     trace_id="evt-456",
        ...     attachment_url="https://storage.example.com/bad.pdf",
        ...     destination_path="claims/C-789/bad.pdf",
        ...     event_type="claim",
        ...     event_subtype="created",
        ...     retry_count=4,
        ...     original_timestamp=datetime.now(timezone.utc)
        ... )
        >>> dlq = FailedDownloadMessage(
        ...     trace_id="evt-456",
        ...     attachment_url="https://storage.example.com/bad.pdf",
        ...     original_task=task,
        ...     final_error="File not found after 4 retries",
        ...     error_category="permanent",
        ...     retry_count=4,
        ...     failed_at=datetime.now(timezone.utc)
        ... )
    """

    trace_id: str = Field(
        ...,
        description="Unique event identifier for correlation",
        min_length=1
    )
    attachment_url: str = Field(
        ...,
        description="URL of the attachment that failed",
        min_length=1
    )
    original_task: DownloadTaskMessage = Field(
        ...,
        description="Complete original task message for replay capability"
    )
    final_error: str = Field(
        ...,
        description="Final error message (truncated to 500 chars)"
    )
    error_category: str = Field(
        ...,
        description="Error classification from final attempt",
        min_length=1
    )
    retry_count: int = Field(
        ...,
        description="Total number of retry attempts made",
        ge=0
    )
    failed_at: datetime = Field(
        ...,
        description="Timestamp when task was sent to DLQ"
    )

    @field_validator('trace_id', 'attachment_url', 'error_category')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_validator('final_error')
    @classmethod
    def truncate_final_error(cls, v: str) -> str:
        """Truncate error message to prevent huge messages."""
        if not v or not v.strip():
            raise ValueError("final_error cannot be empty or whitespace")
        v = v.strip()
        if len(v) > 500:
            return v[:497] + "..."
        return v

    @field_serializer('failed_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'trace_id': 'evt-2024-004',
                    'attachment_url': 'https://storage.example.com/claims/C-99999/missing.pdf',
                    'original_task': {
                        'trace_id': 'evt-2024-004',
                        'attachment_url': 'https://storage.example.com/claims/C-99999/missing.pdf',
                        'destination_path': 'claims/C-99999/missing.pdf',
                        'event_type': 'claim',
                        'event_subtype': 'created',
                        'retry_count': 4,
                        'original_timestamp': '2024-12-25T10:00:00Z',
                        'metadata': {
                            'last_error': 'File not found (404)',
                            'retry_at': '2024-12-25T10:40:00Z'
                        }
                    },
                    'final_error': 'File not found (404) - URL returned 404 Not Found after 4 retry attempts',
                    'error_category': 'permanent',
                    'retry_count': 4,
                    'failed_at': '2024-12-25T10:45:00Z'
                }
            ]
        }
    }
