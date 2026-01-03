"""
Test data generators for Kafka message schemas.

Provides factory functions for creating valid test messages with sensible defaults.
All generators support customization via keyword arguments for test-specific scenarios.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.schemas.results import (
    DownloadResultMessage,
    FailedDownloadMessage,
)
from kafka_pipeline.schemas.tasks import DownloadTaskMessage


def create_event_message(
    trace_id: Optional[str] = None,
    event_type: str = "claim",
    event_subtype: str = "documentsReceived",
    source_system: str = "xn",
    attachments: Optional[List[str]] = None,
    timestamp: Optional[datetime] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> EventMessage:
    """
    Create test EventMessage with sensible defaults.

    Args:
        trace_id: Unique event identifier (auto-generated if None)
        event_type: High-level event category (default: "claim")
        event_subtype: Specific event action (default: "documentsReceived")
        source_system: Source system identifier (default: "xn")
        attachments: List of attachment URLs (default: single test PDF)
        timestamp: Event timestamp (default: current UTC time)
        payload: Event-specific data (default: minimal claim payload)

    Returns:
        EventMessage: Valid event message for testing

    Example:
        >>> event = create_event_message(trace_id="test-001")
        >>> event = create_event_message(
        ...     trace_id="test-002",
        ...     attachments=["https://example.com/file1.pdf", "https://example.com/file2.pdf"]
        ... )
    """
    import json

    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-evt-{uuid.uuid4().hex[:8]}"

    # Default attachments
    if attachments is None:
        attachments = ["https://example.com/attachment.pdf"]

    # Default timestamp
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    # Default payload - include attachments in data
    if payload is None:
        payload = {
            "assignmentId": f"A-{trace_id[-6:]}",
            "claim_id": f"C-{trace_id[-6:]}",
        }

    # Add attachments to data payload
    data_payload = {**payload, "attachments": attachments}

    # Build the event type string
    type_str = f"verisk.claims.property.{source_system}.{event_subtype}"

    return EventMessage(
        type=type_str,
        version=1,
        utcDateTime=timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
        traceId=trace_id,
        data=json.dumps(data_payload),
    )


def create_download_task_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    blob_path: Optional[str] = None,
    status_subtype: str = "documentsReceived",
    file_type: Optional[str] = None,
    assignment_id: str = "A12345",
    event_type: str = "xact",
    event_subtype: str = "documentsReceived",
    retry_count: int = 0,
    original_timestamp: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> DownloadTaskMessage:
    """
    Create test DownloadTaskMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: URL to download (default: example.com PDF)
        blob_path: OneLake blob path (auto-generated if None)
        status_subtype: Event status subtype (default: "documentsReceived")
        file_type: File type from URL (auto-detected if None)
        assignment_id: Assignment ID (default: "A12345")
        event_type: Event category (default: "xact")
        event_subtype: Event action (default: "documentsReceived")
        retry_count: Number of retry attempts (default: 0)
        original_timestamp: Original event timestamp (default: current UTC)
        metadata: Additional metadata dict (default: empty)

    Returns:
        DownloadTaskMessage: Valid download task for testing

    Example:
        >>> task = create_download_task_message(trace_id="test-001")
        >>> task = create_download_task_message(
        ...     trace_id="test-002",
        ...     attachment_url="https://example.com/large-file.pdf",
        ...     retry_count=2
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-task-{uuid.uuid4().hex[:8]}"

    # Extract file_type from URL if not provided
    if file_type is None:
        filename = attachment_url.split("/")[-1]
        if "." in filename:
            file_type = filename.rsplit(".", 1)[-1].lower()
        else:
            file_type = "pdf"

    # Auto-generate blob_path if not provided
    if blob_path is None:
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"

    # Default timestamp
    if original_timestamp is None:
        original_timestamp = datetime.now(timezone.utc)

    # Default metadata
    if metadata is None:
        metadata = {}

    return DownloadTaskMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        blob_path=blob_path,
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id=assignment_id,
        event_type=event_type,
        event_subtype=event_subtype,
        retry_count=retry_count,
        original_timestamp=original_timestamp,
        metadata=metadata,
    )


def create_download_result_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    blob_path: Optional[str] = None,
    status_subtype: str = "documentsReceived",
    file_type: Optional[str] = None,
    assignment_id: str = "A12345",
    status: str = "completed",
    http_status: Optional[int] = 200,
    bytes_downloaded: int = 1024,
    retry_count: int = 0,
    created_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    expires_at: Optional[datetime] = None,
    expired_at_ingest: Optional[bool] = None,
) -> DownloadResultMessage:
    """
    Create test DownloadResultMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: Downloaded URL
        blob_path: OneLake path where file was uploaded
        status_subtype: Event status subtype (default: "documentsReceived")
        file_type: File extension (auto-detected from URL if None)
        assignment_id: Assignment ID (default: "A12345")
        status: Download status (default: "completed")
        http_status: HTTP response status code (default: 200)
        bytes_downloaded: Number of bytes downloaded (default: 1024, 0 for failures)
        retry_count: Number of retry attempts (default: 0)
        created_at: Creation timestamp (default: current UTC)
        error_message: Error description (for failures)
        expires_at: URL expiration timestamp (optional)
        expired_at_ingest: Whether URL was expired at ingest (optional)

    Returns:
        DownloadResultMessage: Valid result message for testing

    Example:
        >>> result = create_download_result_message(trace_id="test-001")
        >>> result = create_download_result_message(
        ...     trace_id="test-002",
        ...     status="failed",
        ...     error_message="Connection timeout",
        ...     bytes_downloaded=0
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-result-{uuid.uuid4().hex[:8]}"

    # Extract file_type from URL if not provided
    if file_type is None:
        filename = attachment_url.split("/")[-1]
        if "." in filename:
            file_type = filename.rsplit(".", 1)[-1].lower()
        else:
            file_type = "pdf"

    # Auto-generate blob_path if not provided (only for successful downloads)
    if blob_path is None and status == "completed":
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"
    elif blob_path is None:
        # For failures, still need a blob_path
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"

    # Default creation timestamp
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    # For failed downloads, bytes_downloaded should be 0
    if status != "completed":
        bytes_downloaded = 0
        http_status = None

    return DownloadResultMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        blob_path=blob_path,
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id=assignment_id,
        status=status,
        http_status=http_status,
        bytes_downloaded=bytes_downloaded,
        retry_count=retry_count,
        error_message=error_message,
        created_at=created_at,
        expires_at=expires_at,
        expired_at_ingest=expired_at_ingest,
    )


def create_failed_download_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    blob_path: Optional[str] = None,
    status_subtype: str = "documentsReceived",
    file_type: Optional[str] = None,
    assignment_id: str = "A12345",
    final_error: str = "Download failed after max retries",
    error_category: str = "PERMANENT",
    retry_count: int = 3,
    event_type: str = "xact",
    event_subtype: str = "documentsReceived",
    original_timestamp: Optional[datetime] = None,
    failed_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> FailedDownloadMessage:
    """
    Create test FailedDownloadMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: Failed download URL
        blob_path: OneLake blob path (auto-generated if None)
        status_subtype: Event status subtype (default: "documentsReceived")
        file_type: File type from URL (auto-detected if None)
        assignment_id: Assignment ID (default: "A12345")
        final_error: Error description
        error_category: Error classification (default: "PERMANENT")
        retry_count: Number of attempts made (default: 3)
        event_type: Event category (default: "xact")
        event_subtype: Event action (default: "documentsReceived")
        original_timestamp: Original event timestamp
        failed_at: Timestamp when task was sent to DLQ (default: current UTC)
        metadata: Additional error context

    Returns:
        FailedDownloadMessage: Valid DLQ message for testing

    Example:
        >>> failed = create_failed_download_message(trace_id="test-001")
        >>> failed = create_failed_download_message(
        ...     trace_id="test-002",
        ...     final_error="404 Not Found",
        ...     error_category="PERMANENT"
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-failed-{uuid.uuid4().hex[:8]}"

    # Extract file_type from URL if not provided
    if file_type is None:
        filename = attachment_url.split("/")[-1]
        if "." in filename:
            file_type = filename.rsplit(".", 1)[-1].lower()
        else:
            file_type = "pdf"

    # Auto-generate blob_path if not provided
    if blob_path is None:
        filename = attachment_url.split("/")[-1]
        blob_path = f"{status_subtype}/{assignment_id}/{file_type}/{filename}"

    # Default timestamps
    if original_timestamp is None:
        original_timestamp = datetime.now(timezone.utc)

    if failed_at is None:
        failed_at = datetime.now(timezone.utc)

    # Default metadata with error context
    if metadata is None:
        metadata = {
            "error_history": [
                "Attempt 1: Connection timeout",
                "Attempt 2: Connection timeout",
                "Attempt 3: Connection timeout",
            ]
        }

    # Create original task for reference
    original_task = DownloadTaskMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        blob_path=blob_path,
        status_subtype=status_subtype,
        file_type=file_type,
        assignment_id=assignment_id,
        event_type=event_type,
        event_subtype=event_subtype,
        retry_count=retry_count,
        original_timestamp=original_timestamp,
        metadata=metadata,
    )

    return FailedDownloadMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        original_task=original_task,
        final_error=final_error,
        error_category=error_category,
        retry_count=retry_count,
        failed_at=failed_at,
    )
