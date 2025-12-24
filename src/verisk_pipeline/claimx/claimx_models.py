"""
ClaimX pipeline data models.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from verisk_pipeline.common.exceptions import ErrorCategory
from verisk_pipeline.common.security import sanitize_error_message

# Reuse generic stage/cycle models from xact
from verisk_pipeline.xact.xact_models import (
    TaskStatus,
)


# ClaimX tracking table configuration
CLAIMX_PRIMARY_KEYS = ["media_id"]
CLAIMX_RETRY_COLUMNS = [
    "media_id",
    "project_id",
    "download_url",
    "blob_path",
    "file_type",
    "file_name",
    "source_event_id",
    "status",
    "retry_count",
    "error_category",
    "created_at",
    "expires_at",
    "refresh_count",
    "http_status",
    "bytes_downloaded",
]


class EventStatus(str, Enum):
    """Processing status for a ClaimX event."""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    FAILED_PERMANENT = "failed_permanent"
    SKIPPED = "skipped"  # e.g., duplicate event


@dataclass
class ClaimXEvent:
    """
    Parsed event from Kusto.

    Represents a single ClaimX webhook event to be processed.
    """

    event_id: str
    event_type: str
    project_id: str
    ingested_at: datetime

    # Optional fields depending on event type
    media_id: Optional[str] = None  # Changed from int
    task_assignment_id: Optional[str] = None  # Changed from int
    video_collaboration_id: Optional[str] = None  # Added
    master_file_name: Optional[str] = None  # Added

    # Raw payload for handler-specific parsing
    raw_data: Optional[Dict[str, Any]] = None

    @classmethod
    def from_kusto_row(cls, row: Dict[str, Any]) -> "ClaimXEvent":
        """Create event from Kusto row dict."""
        return cls(
            event_id=row.get("event_id") or row.get("eventId") or "",
            event_type=row.get("event_type") or row.get("eventType") or "",
            project_id=row.get("project_id") or row.get("projectId") or "",
            ingested_at=row.get("ingested_at") or row.get("IngestionTime"),
            media_id=row.get("media_id") or row.get("mediaId"),
            task_assignment_id=row.get("task_assignment_id")
            or row.get("taskAssignmentId"),
            video_collaboration_id=row.get("video_collaboration_id")
            or row.get("videoCollaborationId"),
            master_file_name=row.get("master_file_name") or row.get("masterFileName"),
            raw_data=row,
        )


@dataclass
class EntityRows:
    """
    Rows to write to entity tables.

    Handlers populate the relevant lists based on event type.
    """

    projects: List[Dict[str, Any]] = field(default_factory=list)
    contacts: List[Dict[str, Any]] = field(default_factory=list)
    media: List[Dict[str, Any]] = field(default_factory=list)
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    task_templates: List[Dict[str, Any]] = field(default_factory=list)
    external_links: List[Dict[str, Any]] = field(default_factory=list)
    video_collab: List[Dict[str, Any]] = field(default_factory=list)

    def is_empty(self) -> bool:
        """Check if all row lists are empty."""
        return not any(
            [
                self.projects,
                self.contacts,
                self.media,
                self.tasks,
                self.task_templates,
                self.external_links,
                self.video_collab,
            ]
        )

    def merge(self, other: "EntityRows") -> None:
        """Merge another EntityRows into this one."""
        self.projects.extend(other.projects)
        self.contacts.extend(other.contacts)
        self.media.extend(other.media)
        self.tasks.extend(other.tasks)
        self.task_templates.extend(other.task_templates)
        self.external_links.extend(other.external_links)
        self.video_collab.extend(other.video_collab)


@dataclass
class EnrichmentResult:
    """
    Result of enriching a single event.

    Returned by handlers for each event processed.
    """

    event: ClaimXEvent
    success: bool
    rows: EntityRows = field(default_factory=EntityRows)

    # Error details
    error: Optional[str] = None
    error_category: Optional[ErrorCategory] = None
    is_retryable: bool = True

    # API call stats
    api_calls: int = 0
    duration_ms: int = 0

    @property
    def is_permanent_failure(self) -> bool:
        """Check if this is a non-retryable failure."""
        return not self.success and not self.is_retryable

    def to_event_log_row(self) -> Dict[str, Any]:
        """Convert to event log table row."""
        status = (
            "success"
            if self.success
            else ("failed_permanent" if self.is_permanent_failure else "failed")
        )
        return {
            "event_id": self.event.event_id,
            "event_type": self.event.event_type,
            "project_id": self.event.project_id,
            "status": status,
            "error_message": sanitize_error_message(self.error) if self.error else None,
            "error_category": (
                self.error_category.value if self.error_category else None
            ),
            "api_calls": self.api_calls,
            "duration_ms": self.duration_ms,
            "retry_count": 0,  # Set by retry stage
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }


@dataclass
class HandlerResult:
    """
    Result of a handler processing a batch of events.

    Aggregates results from multiple EnrichmentResults.
    """

    handler_name: str
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    failed_permanent: int = 0
    skipped: int = 0

    rows: EntityRows = field(default_factory=EntityRows)
    event_logs: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    duration_seconds: float = 0.0
    api_calls: int = 0

    def add_result(self, result: EnrichmentResult) -> None:
        """Add an enrichment result to totals."""
        self.total += 1
        self.api_calls += result.api_calls

        if result.success:
            self.succeeded += 1
            self.rows.merge(result.rows)
        else:
            self.failed += 1
            if result.is_permanent_failure:
                self.failed_permanent += 1
            if result.error:
                self.errors.append(f"{result.event.event_id}: {result.error}")

        self.event_logs.append(result.to_event_log_row())

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100


@dataclass
class MediaTask:
    """
    Media download task.

    Similar to xact's Task, but for ClaimX media files.
    """

    media_id: str
    project_id: str
    download_url: str
    blob_path: str

    # Metadata for tracking
    file_type: str = ""
    file_name: str = ""
    source_event_id: str = ""
    retry_count: int = 0

    # URL expiration tracking
    expires_at: Optional[str] = None  # ISO datetime
    refresh_count: int = 0  # Number of times URL was refreshed

    def to_tracking_row(
        self,
        status: TaskStatus,
        http_status: Optional[int] = None,
        error_message: Optional[str] = None,
        bytes_downloaded: int = 0,
    ) -> Dict[str, Any]:
        """Convert to tracking table row."""
        return {
            "media_id": self.media_id,
            "project_id": self.project_id,
            "download_url": self.download_url,
            "blob_path": self.blob_path,
            "file_type": self.file_type,
            "file_name": self.file_name,
            "source_event_id": self.source_event_id,
            "status": status.value if isinstance(status, TaskStatus) else status,
            "http_status": http_status,
            "bytes_downloaded": bytes_downloaded,
            "retry_count": self.retry_count,
            "error_message": error_message,
            "created_at": datetime.now(timezone.utc).isoformat(),
            # New columns
            "expires_at": self.expires_at,
            "refresh_count": self.refresh_count,
        }


@dataclass
class MediaDownloadResult:
    """
    Result of a single media download attempt.
    """

    task: MediaTask
    success: bool
    http_status: Optional[int] = None
    error: Optional[str] = None
    bytes_downloaded: int = 0
    error_category: Optional[ErrorCategory] = None
    is_retryable: bool = True

    @property
    def is_permanent_failure(self) -> bool:
        """Check if this is a non-retryable failure."""
        return not self.success and not self.is_retryable


@dataclass
class BatchDownloadResult:
    """Result of downloading a batch of media files."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    failed_permanent: int = 0
    failed_transient: int = 0
    failed_circuit: int = 0

    duration_seconds: float = 0.0
    bytes_downloaded: int = 0
    errors: List[str] = field(default_factory=list)

    def add_result(self, result: MediaDownloadResult) -> None:
        """Add a download result to batch totals."""
        self.total += 1
        if result.success:
            self.succeeded += 1
            self.bytes_downloaded += result.bytes_downloaded
        else:
            self.failed += 1
            if result.error:
                self.errors.append(f"{result.task.media_id}: {result.error}")

            if result.error_category == ErrorCategory.CIRCUIT_OPEN:
                self.failed_circuit += 1
            elif result.is_retryable:
                self.failed_transient += 1
            else:
                self.failed_permanent += 1

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100


class StageStatus(str, Enum):
    """Status of a pipeline stage execution."""

    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StageResult:
    """Result of a pipeline stage execution."""

    stage_name: str
    status: StageStatus
    records_processed: int = 0
    records_succeeded: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    error: Optional[str] = None
    watermark: Optional[datetime] = None
    started_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Check if stage succeeded."""
        return self.status in (StageStatus.SUCCESS, StageStatus.COMPLETED)


@dataclass
class BatchResult:
    """Result of processing a batch of tasks."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    failed_transient: int = 0
    failed_permanent: int = 0
    failed_circuit: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100

    def add_result(self, result: MediaDownloadResult) -> None:
        """Add a download result to batch totals."""
        self.total += 1
        if result.success:
            self.succeeded += 1
        else:
            self.failed += 1
            if result.error:
                self.errors.append(f"{result.task.media_id}: {result.error}")

            if result.error_category == ErrorCategory.CIRCUIT_OPEN:
                self.failed_circuit += 1
            elif result.is_retryable:
                self.failed_transient += 1
            else:
                self.failed_permanent += 1
