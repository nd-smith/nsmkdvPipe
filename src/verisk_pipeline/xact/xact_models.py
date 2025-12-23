"""
Shared data models for the xact pipeline.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from verisk_pipeline.common.security import sanitize_error_message
from verisk_pipeline.common.exceptions import ErrorCategory


# XACT tracking table configuration
XACT_PRIMARY_KEYS = ["trace_id", "attachment_url"]
XACT_RETRY_COLUMNS = [
    "trace_id",
    "attachment_url",
    "status",
    "retry_count",
    "error_category",
    "created_at",
    "blob_path",
    "status_subtype",
    "file_type",
    "assignment_id",
    "expires_at",
    "expired_at_ingest",
]


class TaskStatus(str, Enum):
    """Status of an attachment download task."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    FAILED_PERMANENT = "failed_permanent"  # Won't retry (404, 403, etc.)


class StageStatus(str, Enum):
    """Status of a pipeline stage execution."""

    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Task:
    """
    Attachment download task.

    Represents a single attachment to be downloaded and stored.
    """

    trace_id: str
    attachment_url: str
    blob_path: str
    status_subtype: str
    file_type: str
    assignment_id: str
    estimate_version: Optional[str] = None
    retry_count: int = 0

    def to_tracking_row(
        self,
        status: TaskStatus,
        http_status: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> dict:
        """
        Convert task to tracking table row.

        Args:
            status: Current task status
            http_status: HTTP response status code
            error_message: Error message if failed

        Returns:
            Dict suitable for tracking table insert
        """
        return {
            "trace_id": self.trace_id,
            "attachment_url": self.attachment_url,
            "blob_path": self.blob_path,
            "status_subtype": self.status_subtype,
            "file_type": self.file_type,
            "assignment_id": self.assignment_id,
            "status": status.value if isinstance(status, TaskStatus) else status,
            "http_status": http_status,
            "retry_count": self.retry_count,
            "error_message": error_message,
            "created_at": datetime.now(timezone.utc).isoformat(),
            # New columns - set by caller or default to None
            "expires_at": None,
            "expired_at_ingest": None,
        }


@dataclass
class DownloadResult:
    """
    Result of a single attachment download attempt.

    Includes error categorization for retry decisions.
    """

    task: Task
    success: bool
    http_status: Optional[int] = None
    error: Optional[str] = None
    bytes_downloaded: int = 0
    # New fields for error categorization
    error_category: Optional[ErrorCategory] = None
    is_retryable: bool = True  # Default to retriable for safety

    @property
    def is_permanent_failure(self) -> bool:
        """Check if this is a permanent (non-retriable) failure."""
        return not self.success and not self.is_retryable


@dataclass
class BatchResult:
    """Result of processing a batch of tasks."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    # New fields for error categorization
    failed_transient: int = 0
    failed_permanent: int = 0
    failed_circuit: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100

    def add_result(self, result: DownloadResult) -> None:
        """Add a download result to batch totals."""
        self.total += 1
        if result.success:
            self.succeeded += 1
        else:
            self.failed += 1
            if result.error:
                self.errors.append(f"{result.task.trace_id}: {result.error}")

            # Track by error category
            if result.error_category == ErrorCategory.CIRCUIT_OPEN:
                self.failed_circuit += 1
            elif result.is_retryable:
                self.failed_transient += 1
            else:
                self.failed_permanent += 1


@dataclass
class EventRecord:
    """
    Parsed event record from Kusto.

    Represents raw event before transformation.
    """

    type: str
    version: str
    utc_datetime: str
    trace_id: str
    data: str  # JSON string

    @property
    def status_subtype(self) -> str:
        """Extract status subtype from event type."""
        return self.type.split(".")[-1] if "." in self.type else self.type


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
        return self.status == StageStatus.SUCCESS


@dataclass
class CycleResult:
    """Result of a complete pipeline cycle (all stages)."""

    cycle_number: int
    started_at: datetime
    cycle_id: str = ""
    duration_seconds: float = 0.0
    stages: List[StageResult] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        """Check if all stages succeeded."""
        return all(s.is_success or s.status == StageStatus.SKIPPED for s in self.stages)

    @property
    def total_records_processed(self) -> int:
        """Sum of records processed across all stages."""
        return sum(s.records_processed for s in self.stages)

    def add_stage_result(self, result: StageResult) -> None:
        """Add a stage result to the cycle."""
        self.stages.append(result)

    def get_stage(self, name: str) -> Optional[StageResult]:
        """Get result for a specific stage."""
        for stage in self.stages:
            if stage.stage_name == name:
                return stage
        return None


def _sanitize_error_for_persistence(
    error: Optional[str], max_length: int = 500
) -> Optional[str]:
    """
    Sanitize error message for persistence to Delta table.

    Removes sensitive data like tokens, keys, and credentials.

    Args:
        error: Raw error message
        max_length: Maximum length of output

    Returns:
        Sanitized and truncated error message
    """
    if not error:
        return None

    try:
        return sanitize_error_message(error, max_length=max_length)
    except ImportError:
        # Fallback: simple truncation if security module not available
        if len(error) > max_length:
            return error[: max_length - 3] + "..."
        return error


@dataclass
class CycleSummary:
    """
    Cycle execution summary for Delta table persistence.

    Captures key metrics from each cycle for queryable history.
    """

    cycle_id: str
    worker_id: str
    stage: str
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    status: str  # success / failed / partial

    # Counts
    records_processed: int = 0
    records_succeeded: int = 0
    records_failed: int = 0
    records_failed_permanent: int = 0

    # Error info
    error_count: int = 0
    error_sample: Optional[str] = None  # First error message (truncated + sanitized)

    # Retry-specific
    retry_queue_depth: int = 0  # Remaining retries after this cycle

    # Circuit breaker state
    circuit_breaker_trips: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for Delta table insert."""
        return {
            "cycle_id": self.cycle_id,
            "worker_id": self.worker_id,
            "stage": self.stage,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_succeeded": self.records_succeeded,
            "records_failed": self.records_failed,
            "records_failed_permanent": self.records_failed_permanent,
            "error_count": self.error_count,
            "error_sample": self.error_sample,
            "retry_queue_depth": self.retry_queue_depth,
            "circuit_breaker_trips": self.circuit_breaker_trips,
        }

    @classmethod
    def from_stage_result(
        cls,
        cycle_id: str,
        worker_id: str,
        stage_result: StageResult,
        retry_queue_depth: int = 0,
        circuit_breaker_trips: int = 0,
    ) -> "CycleSummary":
        """
        Build CycleSummary from StageResult.

        Args:
            cycle_id: Cycle identifier
            worker_id: Worker identifier
            stage_result: Stage execution result
            retry_queue_depth: Remaining retry queue size
            circuit_breaker_trips: Number of CB trips this cycle

        Returns:
            CycleSummary instance
        """
        now = datetime.now(timezone.utc)
        started = stage_result.started_at or now

        # Determine status
        if stage_result.is_success:
            status = "success"
        elif stage_result.status == StageStatus.SKIPPED:
            status = "skipped"
        else:
            status = "failed"

        # Sanitize and truncate error sample
        error_sample = _sanitize_error_for_persistence(stage_result.error)

        return cls(
            cycle_id=cycle_id,
            worker_id=worker_id,
            stage=stage_result.stage_name,
            started_at=started,
            ended_at=now,
            duration_seconds=stage_result.duration_seconds,
            status=status,
            records_processed=stage_result.records_processed,
            records_succeeded=stage_result.records_succeeded,
            records_failed=stage_result.records_failed,
            records_failed_permanent=stage_result.metadata.get("failed_permanent", 0),
            error_count=1 if stage_result.error else 0,
            error_sample=error_sample,
            retry_queue_depth=retry_queue_depth,
            circuit_breaker_trips=circuit_breaker_trips,
        )


@dataclass
class HealthStatus:
    """Health check status for the pipeline."""

    healthy: bool
    status: str  # "healthy", "degraded", "unhealthy"
    uptime_seconds: float
    last_successful_cycle: Optional[datetime] = None
    current_state: str = "idle"  # "idle", "running", "error"
    components: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "healthy": self.healthy,
            "status": self.status,
            "uptime_seconds": self.uptime_seconds,
            "last_successful_cycle": (
                self.last_successful_cycle.isoformat()
                if self.last_successful_cycle
                else None
            ),
            "current_state": self.current_state,
            "components": self.components,
        }
