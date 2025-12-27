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
    event_subtype: str = "created",
    source_system: str = "claimx",
    attachments: Optional[List[str]] = None,
    timestamp: Optional[datetime] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> EventMessage:
    """
    Create test EventMessage with sensible defaults.

    Args:
        trace_id: Unique event identifier (auto-generated if None)
        event_type: High-level event category (default: "claim")
        event_subtype: Specific event action (default: "created")
        source_system: Originating system (default: "claimx")
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

    # Default payload
    if payload is None:
        payload = {
            "claim_id": f"C-{trace_id[-6:]}",
            "amount": 10000,
            "status": "pending",
        }

    return EventMessage(
        trace_id=trace_id,
        event_type=event_type,
        event_subtype=event_subtype,
        timestamp=timestamp,
        source_system=source_system,
        payload=payload,
        attachments=attachments,
    )


def create_download_task_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    destination_path: Optional[str] = None,
    event_type: str = "claim",
    event_subtype: str = "created",
    retry_count: int = 0,
    original_timestamp: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> DownloadTaskMessage:
    """
    Create test DownloadTaskMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: URL to download (default: example.com PDF)
        destination_path: OneLake destination path (auto-generated if None)
        event_type: Event category (default: "claim")
        event_subtype: Event action (default: "created")
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

    # Auto-generate destination path if not provided
    if destination_path is None:
        filename = attachment_url.split("/")[-1]
        destination_path = f"test/{trace_id}/{filename}"

    # Default timestamp
    if original_timestamp is None:
        original_timestamp = datetime.now(timezone.utc)

    # Default metadata
    if metadata is None:
        metadata = {}

    return DownloadTaskMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        destination_path=destination_path,
        event_type=event_type,
        event_subtype=event_subtype,
        retry_count=retry_count,
        original_timestamp=original_timestamp,
        metadata=metadata,
    )


def create_download_result_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    destination_path: Optional[str] = None,
    status: str = "success",
    bytes_downloaded: Optional[int] = 1024,
    processing_time_ms: int = 100,
    completed_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    error_category: Optional[str] = None,
) -> DownloadResultMessage:
    """
    Create test DownloadResultMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: Downloaded URL
        destination_path: OneLake path where file was uploaded
        status: Download status (default: "success")
        bytes_downloaded: Number of bytes downloaded (default: 1024, None for failures)
        processing_time_ms: Processing time in ms (default: 100)
        completed_at: Completion timestamp (default: current UTC)
        error_message: Error description (for failures)
        error_category: Error classification (for failures)

    Returns:
        DownloadResultMessage: Valid result message for testing

    Example:
        >>> result = create_download_result_message(trace_id="test-001")
        >>> result = create_download_result_message(
        ...     trace_id="test-002",
        ...     status="failed_transient",
        ...     error_message="Connection timeout",
        ...     error_category="TRANSIENT",
        ...     bytes_downloaded=None
        ... )
    """
    # Auto-generate trace_id if not provided
    if trace_id is None:
        import uuid
        trace_id = f"test-result-{uuid.uuid4().hex[:8]}"

    # Auto-generate destination path if not provided (only for successful downloads)
    if destination_path is None and status == "success":
        filename = attachment_url.split("/")[-1]
        destination_path = f"test/{trace_id}/{filename}"

    # Default completion timestamp
    if completed_at is None:
        completed_at = datetime.now(timezone.utc)

    # For failed downloads, bytes_downloaded should be None
    if status != "success" and bytes_downloaded is not None:
        bytes_downloaded = None

    return DownloadResultMessage(
        trace_id=trace_id,
        attachment_url=attachment_url,
        destination_path=destination_path,
        status=status,
        bytes_downloaded=bytes_downloaded,
        processing_time_ms=processing_time_ms,
        completed_at=completed_at,
        error_message=error_message,
        error_category=error_category,
    )


def create_failed_download_message(
    trace_id: Optional[str] = None,
    attachment_url: str = "https://example.com/file.pdf",
    destination_path: Optional[str] = None,
    final_error: str = "Download failed after max retries",
    error_category: str = "PERMANENT",
    retry_count: int = 3,
    event_type: str = "claim",
    event_subtype: str = "created",
    original_timestamp: Optional[datetime] = None,
    failed_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> FailedDownloadMessage:
    """
    Create test FailedDownloadMessage with sensible defaults.

    Args:
        trace_id: Event identifier (auto-generated if None)
        attachment_url: Failed download URL
        destination_path: Intended destination path
        final_error: Error description
        error_category: Error classification (default: "PERMANENT")
        retry_count: Number of attempts made (default: 3)
        event_type: Event category
        event_subtype: Event action
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

    # Auto-generate destination path if not provided
    if destination_path is None:
        filename = attachment_url.split("/")[-1]
        destination_path = f"test/{trace_id}/{filename}"

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
        destination_path=destination_path,
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
