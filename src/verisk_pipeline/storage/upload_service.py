"""
Centralized async upload service for OneLake file uploads.

Provides a unified upload queue that can be shared across pipelines (claimx, xact),
with a thread pool for concurrent uploads and backpressure support.

Usage:
    # Initialize with base paths for each domain
    service = UploadService({
        "claimx": "abfss://workspace@onelake.../claimx/Files",
        "xact": "abfss://workspace@onelake.../xact/Files",
    })

    # Start the background workers
    async with service:
        # Submit uploads (non-blocking)
        await service.submit(UploadTask(
            source=content_bytes,
            destination="attachments/file.jpg",
            domain="claimx",
        ))

        # Or wait for completion
        result = await service.submit_and_wait(task)
"""

import asyncio
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from verisk_pipeline.common.logging.decorators import LoggedClass
from verisk_pipeline.common.logging.utilities import log_with_context
from verisk_pipeline.storage.onelake import OneLakeClient


logger = logging.getLogger(__name__)


class UploadStatus(Enum):
    """Status of an upload task."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class UploadTask:
    """
    Represents a file upload task.

    Attributes:
        source: Either bytes content or Path to local file
        destination: Relative path in OneLake (within domain's base path)
        domain: Pipeline domain ("claimx" or "xact") for routing
        trace_id: Optional trace ID for correlation
        metadata: Optional metadata dict for logging/callbacks
    """
    source: Union[bytes, Path, str]  # bytes, Path, or str path
    destination: str
    domain: str
    trace_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Normalize string paths to Path objects
        if isinstance(self.source, str) and not isinstance(self.source, bytes):
            self.source = Path(self.source)

    @property
    def is_file(self) -> bool:
        """True if source is a file path, False if bytes."""
        return isinstance(self.source, Path)

    @property
    def size_bytes(self) -> int:
        """Size of the upload in bytes."""
        if isinstance(self.source, bytes):
            return len(self.source)
        elif isinstance(self.source, Path) and self.source.exists():
            return self.source.stat().st_size
        return 0


@dataclass
class UploadResult:
    """Result of an upload operation."""
    task: UploadTask
    success: bool
    blob_path: Optional[str] = None
    error: Optional[str] = None
    duration_ms: float = 0.0


class UploadServiceMetrics:
    """Thread-safe metrics for upload service."""

    def __init__(self):
        self._lock = threading.Lock()
        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._bytes_uploaded = 0
        self._total_duration_ms = 0.0
        self._start_time = datetime.now(timezone.utc)

    def record_submit(self):
        with self._lock:
            self._submitted += 1

    def record_complete(self, bytes_uploaded: int, duration_ms: float):
        with self._lock:
            self._completed += 1
            self._bytes_uploaded += bytes_uploaded
            self._total_duration_ms += duration_ms

    def record_failure(self):
        with self._lock:
            self._failed += 1

    @property
    def queue_depth(self) -> int:
        """Approximate pending uploads."""
        with self._lock:
            return self._submitted - self._completed - self._failed

    def snapshot(self) -> Dict[str, Any]:
        """Return current metrics snapshot."""
        with self._lock:
            return {
                "submitted": self._submitted,
                "completed": self._completed,
                "failed": self._failed,
                "pending": self._submitted - self._completed - self._failed,
                "bytes_uploaded": self._bytes_uploaded,
                "avg_duration_ms": (
                    self._total_duration_ms / self._completed
                    if self._completed > 0 else 0
                ),
                "uptime_seconds": (
                    datetime.now(timezone.utc) - self._start_time
                ).total_seconds(),
            }


class UploadService(LoggedClass):
    """
    Centralized upload service for OneLake file uploads.

    Features:
    - Single connection pool shared across all pipelines
    - Async queue with backpressure (maxsize limits)
    - Thread pool for concurrent blocking uploads
    - Unified metrics and logging
    - Graceful shutdown with drain support
    """

    log_component = "upload_service"

    def __init__(
        self,
        base_paths: Dict[str, str],
        max_workers: int = 10,
        max_queue_size: int = 100,
        drain_timeout_seconds: float = 30.0,
    ):
        """
        Initialize upload service.

        Args:
            base_paths: Dict mapping domain names to OneLake base paths
                        e.g. {"claimx": "abfss://...", "xact": "abfss://..."}
            max_workers: Maximum concurrent upload threads
            max_queue_size: Maximum queue depth (for backpressure)
            drain_timeout_seconds: Timeout for draining queue on shutdown
        """
        super().__init__()

        self._base_paths = base_paths
        self._max_workers = max_workers
        self._max_queue_size = max_queue_size
        self._drain_timeout = drain_timeout_seconds

        # Lazy-initialized resources
        self._clients: Dict[str, OneLakeClient] = {}
        self._queue: Optional[asyncio.Queue] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._workers: list[asyncio.Task] = []
        self._running = False
        self._shutdown_event: Optional[asyncio.Event] = None

        # Metrics
        self._metrics = UploadServiceMetrics()

        self._log(
            logging.INFO,
            "UploadService created",
            domains=list(base_paths.keys()),
            max_workers=max_workers,
            max_queue_size=max_queue_size,
        )

    def _get_client(self, domain: str) -> OneLakeClient:
        """Get or create OneLakeClient for domain."""
        if domain not in self._clients:
            if domain not in self._base_paths:
                raise ValueError(f"Unknown domain: {domain}. Known: {list(self._base_paths.keys())}")

            self._clients[domain] = OneLakeClient(
                self._base_paths[domain],
                max_pool_size=self._max_workers + 5,
            )
            self._log(
                logging.DEBUG,
                "Created OneLakeClient for domain",
                domain=domain,
                base_path=self._base_paths[domain][:50] + "...",
            )

        return self._clients[domain]

    async def start(self) -> None:
        """Start the upload service workers."""
        if self._running:
            return

        self._queue = asyncio.Queue(maxsize=self._max_queue_size)
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="upload_worker",
        )
        self._shutdown_event = asyncio.Event()
        self._running = True

        # Start worker tasks
        for i in range(self._max_workers):
            worker = asyncio.create_task(
                self._worker_loop(i),
                name=f"upload_worker_{i}",
            )
            self._workers.append(worker)

        self._log(
            logging.INFO,
            "UploadService started",
            workers=self._max_workers,
            queue_size=self._max_queue_size,
        )

    async def stop(self, drain: bool = True) -> None:
        """
        Stop the upload service.

        Args:
            drain: If True, wait for pending uploads to complete
        """
        if not self._running:
            return

        self._running = False

        if drain and self._queue and not self._queue.empty():
            self._log(
                logging.INFO,
                "Draining upload queue",
                pending=self._queue.qsize(),
                timeout_seconds=self._drain_timeout,
            )

            # Wait for queue to drain or timeout
            try:
                await asyncio.wait_for(
                    self._wait_for_drain(),
                    timeout=self._drain_timeout,
                )
            except asyncio.TimeoutError:
                self._log(
                    logging.WARNING,
                    "Drain timeout, stopping with pending uploads",
                    remaining=self._queue.qsize() if self._queue else 0,
                )

        # Signal workers to stop
        if self._shutdown_event:
            self._shutdown_event.set()

        # Cancel worker tasks
        for worker in self._workers:
            worker.cancel()

        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

        # Shutdown executor
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

        # Close clients
        for domain, client in self._clients.items():
            try:
                client.__exit__(None, None, None)
            except Exception:
                pass
        self._clients.clear()

        self._log(
            logging.INFO,
            "UploadService stopped",
            **self._metrics.snapshot(),
        )

    async def _wait_for_drain(self) -> None:
        """Wait for queue to empty."""
        while self._queue and not self._queue.empty():
            await asyncio.sleep(0.1)

    async def _worker_loop(self, worker_id: int) -> None:
        """Background worker that processes upload tasks."""
        self._log(logging.DEBUG, "Upload worker started", worker_id=worker_id)

        while self._running or (self._queue and not self._queue.empty()):
            try:
                # Use wait_for to allow checking _running periodically
                try:
                    item = await asyncio.wait_for(
                        self._queue.get(),  # type: ignore
                        timeout=1.0,
                    )
                except asyncio.TimeoutError:
                    continue

                task, future = item
                result = await self._process_upload(task)

                if not future.done():
                    if result.success:
                        future.set_result(result)
                    else:
                        future.set_exception(Exception(result.error))

                self._queue.task_done()  # type: ignore

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log(
                    logging.ERROR,
                    "Worker error",
                    worker_id=worker_id,
                    error=str(e)[:200],
                )

        self._log(logging.DEBUG, "Upload worker stopped", worker_id=worker_id)

    async def _process_upload(self, task: UploadTask) -> UploadResult:
        """Process a single upload task."""
        start_time = datetime.now(timezone.utc)

        try:
            client = self._get_client(task.domain)

            # Run sync upload in thread pool
            loop = asyncio.get_event_loop()

            if task.is_file:
                source_path = task.source
                blob_path = await loop.run_in_executor(
                    self._executor,
                    client.upload_file,
                    task.destination,
                    str(source_path),
                )
            else:
                blob_path = await loop.run_in_executor(
                    self._executor,
                    client.upload_bytes,
                    task.destination,
                    task.source,
                )

            duration_ms = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds() * 1000

            self._metrics.record_complete(task.size_bytes, duration_ms)

            self._log(
                logging.DEBUG,
                "Upload complete",
                domain=task.domain,
                destination=task.destination,
                bytes=task.size_bytes,
                duration_ms=round(duration_ms, 2),
                trace_id=task.trace_id,
            )

            return UploadResult(
                task=task,
                success=True,
                blob_path=blob_path,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds() * 1000

            self._metrics.record_failure()

            self._log(
                logging.WARNING,
                "Upload failed",
                domain=task.domain,
                destination=task.destination,
                error=str(e)[:200],
                duration_ms=round(duration_ms, 2),
                trace_id=task.trace_id,
            )

            return UploadResult(
                task=task,
                success=False,
                error=str(e),
                duration_ms=duration_ms,
            )

    async def submit(self, task: UploadTask) -> asyncio.Future:
        """
        Submit an upload task (non-blocking).

        Returns a Future that resolves to UploadResult on completion.
        May block if queue is full (backpressure).

        Args:
            task: Upload task to submit

        Returns:
            Future that resolves to UploadResult
        """
        if not self._running:
            raise RuntimeError("UploadService not running. Call start() first.")

        self._metrics.record_submit()

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        await self._queue.put((task, future))  # type: ignore

        return future

    async def submit_and_wait(self, task: UploadTask) -> UploadResult:
        """
        Submit an upload task and wait for completion.

        Args:
            task: Upload task to submit

        Returns:
            UploadResult with success/failure details
        """
        future = await self.submit(task)
        try:
            return await future
        except Exception as e:
            return UploadResult(
                task=task,
                success=False,
                error=str(e),
            )

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        return self._metrics.snapshot()

    @property
    def queue_depth(self) -> int:
        """Current number of pending uploads."""
        return self._queue.qsize() if self._queue else 0

    @property
    def is_running(self) -> bool:
        """True if service is running."""
        return self._running

    async def __aenter__(self) -> "UploadService":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop(drain=True)


# Singleton instance for cross-pipeline sharing
_upload_service: Optional[UploadService] = None
_upload_service_lock = threading.Lock()


def get_upload_service() -> Optional[UploadService]:
    """Get the shared upload service instance (if initialized)."""
    return _upload_service


def init_upload_service(
    base_paths: Dict[str, str],
    max_workers: int = 10,
    max_queue_size: int = 100,
) -> UploadService:
    """
    Initialize the shared upload service singleton.

    Should be called once at application startup.

    Args:
        base_paths: Dict mapping domain names to OneLake base paths
        max_workers: Maximum concurrent upload threads
        max_queue_size: Maximum queue depth

    Returns:
        The initialized UploadService
    """
    global _upload_service

    with _upload_service_lock:
        if _upload_service is not None:
            logger.warning("UploadService already initialized, returning existing instance")
            return _upload_service

        _upload_service = UploadService(
            base_paths=base_paths,
            max_workers=max_workers,
            max_queue_size=max_queue_size,
        )

        return _upload_service


async def shutdown_upload_service(drain: bool = True) -> None:
    """Shutdown the shared upload service."""
    global _upload_service

    with _upload_service_lock:
        if _upload_service is not None:
            await _upload_service.stop(drain=drain)
            _upload_service = None
