"""
KQL Event Poller for polling events from Microsoft Fabric Eventhouse.

Polls Eventhouse at configurable intervals, applies deduplication,
and produces events to Kafka for processing by download workers.
"""

import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set, Type

import yaml

from core.logging.setup import get_logger
from kafka_pipeline.config import KafkaConfig, load_config as load_kafka_config
from kafka_pipeline.common.eventhouse.kql_client import (
    DEFAULT_CONFIG_PATH,
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)
from kafka_pipeline.common.producer import BaseKafkaProducer

logger = get_logger(__name__)


# Default checkpoint directory
DEFAULT_CHECKPOINT_DIR = Path(".checkpoints")


@dataclass
class PollerCheckpoint:
    """
    Checkpoint state for resuming the poller after restart.

    Stores the composite key (ingestion_time, trace_id) of the last processed
    record to enable exact resume without duplicates or gaps.

    The checkpoint uses deterministic ordering (ingestion_time asc, traceId asc)
    to ensure consistent resume behavior.
    """

    last_ingestion_time: str  # ISO format UTC timestamp
    last_trace_id: str  # trace_id of the last processed record
    updated_at: str  # When checkpoint was written (for debugging)

    @classmethod
    def from_file(cls, path: Path) -> Optional["PollerCheckpoint"]:
        """
        Load checkpoint from JSON file.

        Args:
            path: Path to checkpoint file

        Returns:
            PollerCheckpoint if file exists and is valid, None otherwise
        """
        if not path.exists():
            logger.info("No checkpoint file found", extra={"path": str(path)})
            return None

        try:
            with open(path, "r") as f:
                data = json.load(f)

            checkpoint = cls(
                last_ingestion_time=data["last_ingestion_time"],
                last_trace_id=data["last_trace_id"],
                updated_at=data.get("updated_at", ""),
            )
            logger.info(
                "Loaded checkpoint",
                extra={
                    "last_ingestion_time": checkpoint.last_ingestion_time,
                    "last_trace_id": checkpoint.last_trace_id[:20] + "..."
                    if len(checkpoint.last_trace_id) > 20
                    else checkpoint.last_trace_id,
                    "updated_at": checkpoint.updated_at,
                },
            )
            return checkpoint

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(
                "Failed to load checkpoint, starting fresh",
                extra={"path": str(path), "error": str(e)},
            )
            return None

    def save(self, path: Path) -> bool:
        """
        Save checkpoint to JSON file.

        Creates parent directories if they don't exist.

        Args:
            path: Path to checkpoint file

        Returns:
            True if save succeeded, False otherwise
        """
        try:
            # Ensure directory exists
            path.parent.mkdir(parents=True, exist_ok=True)

            # Update timestamp
            self.updated_at = datetime.now(timezone.utc).isoformat()

            # Write atomically (write to temp, then replace)
            # Use os.replace() for cross-platform atomic replace (works on Windows)
            temp_path = path.with_suffix(".tmp")
            with open(temp_path, "w") as f:
                json.dump(asdict(self), f, indent=2)

            os.replace(temp_path, path)

            logger.debug(
                "Saved checkpoint",
                extra={
                    "last_ingestion_time": self.last_ingestion_time,
                    "last_trace_id": self.last_trace_id[:20] + "..."
                    if len(self.last_trace_id) > 20
                    else self.last_trace_id,
                },
            )
            return True

        except (OSError, IOError) as e:
            logger.error(
                "Failed to save checkpoint",
                extra={"path": str(path), "error": str(e)},
            )
            return False

    def to_datetime(self) -> datetime:
        """Parse last_ingestion_time to datetime."""
        return datetime.fromisoformat(self.last_ingestion_time.replace("Z", "+00:00"))

# Default backfill window (hours) when no start time is configured
# Keep small to avoid memory issues on first startup - use explicit backfill for historical data
DEFAULT_BACKFILL_WINDOW_HOURS = 1


@dataclass
class PollerConfig:
    """Configuration for KQL Event Poller."""

    # Eventhouse connection
    eventhouse: EventhouseConfig

    # Kafka configuration
    kafka: KafkaConfig

    # Event schema class for row-to-event conversion
    # Must have a from_eventhouse_row(row: Dict[str, Any]) classmethod
    event_schema_class: Optional[Type] = None

    # Domain identifier for OneLake routing (e.g., "xact", "claimx")
    domain: str = "xact"

    # Polling settings
    poll_interval_seconds: int = 30  # Seconds between poll cycles
    batch_size: int = 1000  # Max events per poll cycle
    source_table: str = "Events"  # Eventhouse table name

    # Schema mapping (Eventhouse column -> EventMessage field)
    # Override defaults if Eventhouse has different column names
    column_mapping: dict[str, str] = field(
        default_factory=lambda: {
            "trace_id": "trace_id",
            "event_type": "event_type",
            "event_subtype": "event_subtype",
            "timestamp": "timestamp",
            "source_system": "source_system",
            "payload": "payload",
            "attachments": "attachments",
        }
    )

    # Delta Lake settings
    events_table_path: str = ""  # Path to events table (xact_events or claimx_events)

    # Backpressure settings
    max_kafka_lag: int = 10_000  # Pause polling if lag exceeds this
    lag_check_interval_seconds: int = 60  # How often to check lag

    # Backfill settings
    # If backfill_start_stamp is set, starts backfill from that exact timestamp
    # Format: "YYYY-MM-DD HH:MM:SS.ffffff" (UTC)
    backfill_start_stamp: Optional[str] = None
    # If backfill_stop_stamp is set, stops backfill at that timestamp (otherwise uses current time)
    backfill_stop_stamp: Optional[str] = None
    # If True, fetches all backfill data in one query instead of paginating
    bulk_backfill: bool = False

    # Checkpoint settings for resuming after restart
    # Path to checkpoint file (default: .checkpoints/poller_{domain}.json)
    checkpoint_path: Optional[Path] = None

    @classmethod
    def load_config(
        cls,
        config_path: Optional[Path] = None,
    ) -> "PollerConfig":
        """Load configuration from YAML file with environment variable overrides.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. config.yaml file (under 'eventhouse.poller:' key)
        3. Dataclass defaults

        Args:
            config_path: Path to YAML config file. Defaults to src/config.yaml.

        Returns:
            PollerConfig instance
        """
        config_path = config_path or DEFAULT_CONFIG_PATH

        # Load sub-configs
        eventhouse_config = EventhouseConfig.load_config(config_path)
        kafka_config = load_kafka_config(config_path)

        # Load YAML for poller-specific settings
        poller_data: Dict[str, Any] = {}
        dedup_data: Dict[str, Any] = {}
        if config_path.exists():
            with open(config_path, "r") as f:
                yaml_data = yaml.safe_load(f) or {}
            eventhouse_section = yaml_data.get("eventhouse", {})
            poller_data = eventhouse_section.get("poller", {})

        # Apply environment variable overrides for poller
        if os.getenv("POLL_INTERVAL_SECONDS"):
            poller_data["poll_interval_seconds"] = os.getenv("POLL_INTERVAL_SECONDS")
        if os.getenv("POLL_BATCH_SIZE"):
            poller_data["batch_size"] = os.getenv("POLL_BATCH_SIZE")
        if os.getenv("EVENTHOUSE_SOURCE_TABLE"):
            poller_data["source_table"] = os.getenv("EVENTHOUSE_SOURCE_TABLE")
        if os.getenv("PIPELINE_DOMAIN"):
            poller_data["domain"] = os.getenv("PIPELINE_DOMAIN")
        if os.getenv("XACT_EVENTS_TABLE_PATH"):
            poller_data["events_table_path"] = os.getenv("XACT_EVENTS_TABLE_PATH")
        if os.getenv("MAX_KAFKA_LAG"):
            poller_data["max_kafka_lag"] = os.getenv("MAX_KAFKA_LAG")

        # Apply environment variable overrides for backfill
        if os.getenv("BACKFILL_START_TIMESTAMP"):
            poller_data["backfill_start_stamp"] = os.getenv("BACKFILL_START_TIMESTAMP")
        if os.getenv("BACKFILL_STOP_TIMESTAMP"):
            poller_data["backfill_stop_stamp"] = os.getenv("BACKFILL_STOP_TIMESTAMP")
        if os.getenv("BULK_BACKFILL"):
            poller_data["bulk_backfill"] = os.getenv("BULK_BACKFILL").lower() in (
                "true", "1", "yes"
            )

        xact_events_path = poller_data.get("events_table_path", "")

        # Handle column_mapping from YAML
        column_mapping = poller_data.get("column_mapping", {
            "trace_id": "trace_id",
            "event_type": "event_type",
            "event_subtype": "event_subtype",
            "timestamp": "timestamp",
            "source_system": "source_system",
            "payload": "payload",
            "attachments": "attachments",
        })

        # Checkpoint path - env var overrides config
        domain = poller_data.get("domain", "xact")
        checkpoint_path_env = os.getenv("POLLER_CHECKPOINT_PATH")
        if checkpoint_path_env:
            checkpoint_path = Path(checkpoint_path_env)
        elif poller_data.get("checkpoint_path"):
            checkpoint_path = Path(poller_data["checkpoint_path"])
        else:
            checkpoint_path = DEFAULT_CHECKPOINT_DIR / f"poller_{domain}.json"

        return cls(
            eventhouse=eventhouse_config,
            kafka=kafka_config,
            domain=domain,
            poll_interval_seconds=int(poller_data.get("poll_interval_seconds", 30)),
            batch_size=int(poller_data.get("batch_size", 1000)),
            source_table=poller_data.get("source_table", "Events"),
            column_mapping=column_mapping,
            events_table_path=xact_events_path,
            max_kafka_lag=int(poller_data.get("max_kafka_lag", 10_000)),
            lag_check_interval_seconds=int(
                poller_data.get("lag_check_interval_seconds", 60)
            ),
            backfill_start_stamp=poller_data.get("backfill_start_stamp"),
            backfill_stop_stamp=poller_data.get("backfill_stop_stamp"),
            bulk_backfill=bool(poller_data.get("bulk_backfill", False)),
            checkpoint_path=checkpoint_path,
        )

    @classmethod
    def from_env(cls) -> "PollerConfig":
        """Load configuration from environment variables.

        DEPRECATED: Use load_config() instead.
        This method is kept for backwards compatibility.

        Required environment variables:
            EVENTHOUSE_CLUSTER_URL: Kusto cluster URL
            EVENTHOUSE_DATABASE: Database name
            KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses
            XACT_EVENTS_TABLE_PATH: Path to xact_events Delta table

        Optional environment variables:
            EVENTHOUSE_SOURCE_TABLE: Table name (default: Events)
            POLL_INTERVAL_SECONDS: Poll interval (default: 30)
            POLL_BATCH_SIZE: Max events per poll (default: 1000)
            MAX_KAFKA_LAG: Pause polling threshold (default: 10000)
        """
        eventhouse_config = EventhouseConfig.from_env()
        kafka_config = KafkaConfig.from_env()

        xact_events_path = os.getenv("XACT_EVENTS_TABLE_PATH", "")

        # Parse bulk_backfill boolean
        bulk_backfill_env = os.getenv("BULK_BACKFILL", "").lower()
        bulk_backfill = bulk_backfill_env in ("true", "1", "yes")

        # Checkpoint path - default to .checkpoints/poller_{domain}.json
        domain = os.getenv("PIPELINE_DOMAIN", "xact")
        checkpoint_path_env = os.getenv("POLLER_CHECKPOINT_PATH")
        checkpoint_path = (
            Path(checkpoint_path_env)
            if checkpoint_path_env
            else DEFAULT_CHECKPOINT_DIR / f"poller_{domain}.json"
        )

        return cls(
            eventhouse=eventhouse_config,
            kafka=kafka_config,
            domain=domain,
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "30")),
            batch_size=int(os.getenv("POLL_BATCH_SIZE", "1000")),
            source_table=os.getenv("EVENTHOUSE_SOURCE_TABLE", "Events"),
            events_table_path=xact_events_path,
            max_kafka_lag=int(os.getenv("MAX_KAFKA_LAG", "10000")),
            backfill_start_stamp=os.getenv("BACKFILL_START_TIMESTAMP"),
            backfill_stop_stamp=os.getenv("BACKFILL_STOP_TIMESTAMP"),
            bulk_backfill=bulk_backfill,
            checkpoint_path=checkpoint_path,
        )


class KQLEventPoller:
    """
    Polls Eventhouse for new events and produces them to Kafka.

    Workflow:
    1. Query Eventhouse with time filter and deduplication
    2. Convert KQL results to EventMessage format
    3. Publish events to events.raw topic (EventIngester handles attachments)
    4. Write events to Delta Lake for deduplication tracking

    Features:
    - Configurable poll interval
    - Deduplication via xact_events Delta table
    - Schema mapping for flexible Eventhouse schemas
    - Graceful shutdown (finish current batch)
    - Backpressure: pause if Kafka lag is high
    - Metrics for observability

    Example:
        >>> config = PollerConfig.from_env()
        >>> async with KQLEventPoller(config) as poller:
        ...     await poller.run()
    """

    def __init__(self, config: PollerConfig):
        """Initialize the poller.

        Args:
            config: Poller configuration
        """
        self.config = config
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Event schema class - default to xact EventMessage if not specified
        if config.event_schema_class is None:
            from kafka_pipeline.xact.schemas.events import EventMessage
            self._event_schema_class = EventMessage
        else:
            self._event_schema_class = config.event_schema_class

        # Components (initialized on start)
        self._kql_client: Optional[KQLClient] = None
        self._producer: Optional[BaseKafkaProducer] = None

        # State
        self._last_poll_time: Optional[datetime] = None
        self._consecutive_empty_polls = 0
        self._total_events_fetched = 0
        self._total_polls = 0

        # Stats tracking for backfill
        self._seen_trace_ids: set[str] = set()
        self._duplicate_count = 0

        # Backfill mode - stays true until we get a partial batch
        self._backfill_mode = True
        self._backfill_start_time: Optional[datetime] = None
        self._backfill_stop_time: Optional[datetime] = None
        self._bulk_backfill_complete = False

        # Parse backfill timestamps from config
        if config.backfill_start_stamp:
            self._backfill_start_time = self._parse_timestamp(config.backfill_start_stamp)
            logger.info(
                "Using configured backfill start timestamp",
                extra={"backfill_start": self._backfill_start_time.isoformat()},
            )
        if config.backfill_stop_stamp:
            self._backfill_stop_time = self._parse_timestamp(config.backfill_stop_stamp)
            logger.info(
                "Using configured backfill stop timestamp",
                extra={"backfill_stop": self._backfill_stop_time.isoformat()},
            )

        # Pagination state - track last batch for efficient queries
        self._last_ingestion_time: Optional[datetime] = None
        self._last_trace_id: Optional[str] = None  # For checkpoint resume

        # Checkpoint for persisting state across restarts
        self._checkpoint_path = config.checkpoint_path or (
            DEFAULT_CHECKPOINT_DIR / f"poller_{config.domain}.json"
        )
        self._checkpoint: Optional[PollerCheckpoint] = None
        self._load_checkpoint()

        # Background task tracking for graceful shutdown
        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0  # For unique task naming

        logger.info(
            "Initialized KQLEventPoller",
            extra={
                "source_table": config.source_table,
                "poll_interval": config.poll_interval_seconds,
                "batch_size": config.batch_size,
                "pipeline_domain": config.domain,
                "event_schema": self._event_schema_class.__name__,
                "checkpoint_path": str(self._checkpoint_path),
            },
        )

    async def __aenter__(self) -> "KQLEventPoller":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    @staticmethod
    def _parse_timestamp(timestamp_str: str) -> datetime:
        """Parse a timestamp string into a datetime object.

        Supports formats:
        - "YYYY-MM-DD HH:MM:SS.ffffff"
        - "YYYY-MM-DDTHH:MM:SS.ffffffZ"
        - ISO format variations

        Args:
            timestamp_str: Timestamp string to parse

        Returns:
            datetime object in UTC
        """
        # Try common formats
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                # Ensure UTC timezone
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        # Try ISO format as fallback
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            raise ValueError(f"Could not parse timestamp: {timestamp_str}")

    def _load_checkpoint(self) -> None:
        """Load checkpoint from file and initialize state.

        If checkpoint exists and is valid, sets:
        - _last_ingestion_time: Starting point for queries
        - _last_trace_id: For skipping already-processed records
        - _backfill_start_time: Overridden by checkpoint if present

        If no checkpoint or backfill_start_stamp is configured, falls back to:
        - Configured backfill_start_stamp, or
        - Default window (24 hours ago)
        """
        self._checkpoint = PollerCheckpoint.from_file(self._checkpoint_path)

        if self._checkpoint is not None:
            # Checkpoint exists - use it as starting point
            self._last_ingestion_time = self._checkpoint.to_datetime()
            self._last_trace_id = self._checkpoint.last_trace_id

            # Override backfill start time with checkpoint
            # (checkpoint takes precedence over configured backfill_start_stamp)
            if self._backfill_start_time is None:
                self._backfill_start_time = self._last_ingestion_time

            logger.info(
                "Resuming from checkpoint",
                extra={
                    "last_ingestion_time": self._last_ingestion_time.isoformat(),
                    "last_trace_id": self._last_trace_id[:20] + "..."
                    if len(self._last_trace_id) > 20
                    else self._last_trace_id,
                },
            )
        else:
            logger.info(
                "No checkpoint found, will use configured start or default window"
            )

    def _save_checkpoint(
        self, ingestion_time: datetime, trace_id: str
    ) -> None:
        """Save checkpoint after successful processing.

        Args:
            ingestion_time: Ingestion time of the last processed record
            trace_id: Trace ID of the last processed record
        """
        checkpoint = PollerCheckpoint(
            last_ingestion_time=ingestion_time.isoformat(),
            last_trace_id=trace_id,
            updated_at="",  # Will be set by save()
        )
        checkpoint.save(self._checkpoint_path)

        # Update in-memory state
        self._last_ingestion_time = ingestion_time
        self._last_trace_id = trace_id
        self._checkpoint = checkpoint

    async def start(self) -> None:
        """Initialize all components."""
        logger.info("Starting KQLEventPoller components")

        # Initialize KQL client
        self._kql_client = KQLClient(self.config.eventhouse)
        await self._kql_client.connect()

        # Initialize Kafka producer
        self._producer = BaseKafkaProducer(
            config=self.config.kafka,
            domain=self.config.domain,
            worker_name="eventhouse_poller",
        )
        await self._producer.start()

        self._running = True
        logger.info("KQLEventPoller components started")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> asyncio.Task:
        """
        Create a background task with tracking and lifecycle logging.

        The task is added to _pending_tasks and automatically removed on completion.
        Task lifecycle events (creation, completion, error) are logged.

        Args:
            coro: Coroutine to run as a task
            task_name: Descriptive name for the task (e.g., "delta_write")
            context: Optional dict of context info for logging

        Returns:
            The created asyncio.Task
        """
        self._task_counter += 1
        full_name = f"{task_name}-{self._task_counter}"
        context = context or {}

        task = asyncio.create_task(coro, name=full_name)
        self._pending_tasks.add(task)

        logger.debug(
            "Background task created",
            extra={
                "task_name": full_name,
                "pending_tasks": len(self._pending_tasks),
                **context,
            },
        )

        def _on_task_done(t: asyncio.Task) -> None:
            """Callback when task completes (success, error, or cancelled)."""
            self._pending_tasks.discard(t)

            if t.cancelled():
                logger.debug(
                    "Background task cancelled",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )
            elif t.exception() is not None:
                exc = t.exception()
                logger.error(
                    "Background task failed",
                    extra={
                        "task_name": t.get_name(),
                        "error": str(exc)[:200],
                        "pending_tasks": len(self._pending_tasks),
                        **context,
                    },
                )
            else:
                logger.debug(
                    "Background task completed",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )

        task.add_done_callback(_on_task_done)
        return task

    async def stop(self) -> None:
        """Gracefully shutdown all components.

        Waits for pending background tasks (with timeout), then cleans up
        components. Errors during cleanup are logged but do not prevent
        other components from being cleaned up.
        """
        logger.info("Stopping KQLEventPoller")
        self._running = False
        self._shutdown_event.set()

        # Wait for pending background tasks with timeout
        await self._wait_for_pending_tasks(timeout_seconds=30)

        # Stop producer - errors are logged but don't prevent other cleanup
        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning(
                    "Error stopping producer during poller shutdown",
                    extra={"error": str(e)[:200]},
                )
            finally:
                self._producer = None

        # Close KQL client
        if self._kql_client:
            try:
                await self._kql_client.close()
            except Exception as e:
                logger.warning(
                    "Error closing KQL client during poller shutdown",
                    extra={"error": str(e)[:200]},
                )
            finally:
                self._kql_client = None

        logger.info("KQLEventPoller stopped")

    async def _wait_for_pending_tasks(self, timeout_seconds: float = 30) -> None:
        """
        Wait for pending background tasks to complete with timeout.

        Logs task information and handles cancellation of tasks that
        don't complete within the timeout.

        Args:
            timeout_seconds: Maximum time to wait for tasks (default: 30s)
        """
        if not self._pending_tasks:
            logger.debug("No pending background tasks to wait for")
            return

        pending_count = len(self._pending_tasks)
        task_names = [t.get_name() for t in self._pending_tasks]

        logger.info(
            "Waiting for pending background tasks to complete",
            extra={
                "pending_count": pending_count,
                "task_names": task_names,
                "timeout_seconds": timeout_seconds,
            },
        )

        # Copy the set since it may be modified by callbacks during gather
        tasks_to_wait = list(self._pending_tasks)

        try:
            # Wait with timeout
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                # Log and cancel tasks that didn't complete
                pending_names = [t.get_name() for t in pending]
                logger.warning(
                    "Cancelling background tasks that did not complete in time",
                    extra={
                        "pending_count": len(pending),
                        "pending_task_names": pending_names,
                        "timeout_seconds": timeout_seconds,
                    },
                )

                for task in pending:
                    task.cancel()
                    logger.warning(
                        "Cancelled pending task",
                        extra={"task_name": task.get_name()},
                    )

                # Wait briefly for cancellations to propagate
                await asyncio.gather(*pending, return_exceptions=True)

            # Log summary of completed tasks
            completed_count = len(done)
            failed_count = sum(1 for t in done if t.exception() is not None)

            logger.info(
                "Background task cleanup complete",
                extra={
                    "completed": completed_count,
                    "failed": failed_count,
                    "cancelled": len(pending),
                },
            )

        except Exception as e:
            logger.error(
                "Error waiting for pending tasks",
                extra={"error": str(e)[:200]},
                exc_info=True,
            )

    def _print_backfill_stats(self) -> None:
        """Print backfill statistics to terminal."""
        stats = self.stats
        print("\n" + "=" * 60)
        print("BACKFILL COMPLETE")
        print("=" * 60)
        print(f"  Total messages sent to Kafka:  {stats['total_events_fetched']:,}")
        print(f"  Unique trace IDs:              {stats['unique_trace_ids']:,}")
        print(f"  Duplicate events detected:     {stats['duplicate_count']:,}")
        if stats['unique_trace_ids'] > 0:
            dupe_rate = (stats['duplicate_count'] / stats['total_events_fetched']) * 100
            print(f"  Duplicate rate:                {dupe_rate:.2f}%")
        print("=" * 60 + "\n")

    async def run(self) -> None:
        """
        Main polling loop.

        Runs until stop() is called or shutdown event is set.
        In bulk_backfill mode, exits after backfill with stats display.
        """
        logger.info("Starting polling loop")

        # Check if bulk backfill is configured
        if self.config.bulk_backfill and self._backfill_mode:
            logger.info(
                "Bulk backfill mode enabled",
                extra={
                    "start_stamp": self.config.backfill_start_stamp,
                    "stop_stamp": self.config.backfill_stop_stamp,
                },
            )
            try:
                await self._bulk_backfill()
                # Print stats and exit - don't continue to real-time mode
                self._print_backfill_stats()
                logger.info("Bulk backfill complete, exiting")
                return
            except asyncio.CancelledError:
                logger.info("Bulk backfill cancelled")
                self._print_backfill_stats()
                return
            except Exception as e:
                logger.error(
                    "Error in bulk backfill",
                    extra={"error": str(e)[:200]},
                    exc_info=True,
                )
                self._print_backfill_stats()
                return

        # Continue with normal polling loop (real-time mode)
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._poll_cycle()
            except asyncio.CancelledError:
                logger.info("Polling cancelled")
                break
            except Exception as e:
                logger.error(
                    "Error in poll cycle",
                    extra={"error": str(e)[:200]},
                    exc_info=True,
                )

            # Wait for next poll interval or shutdown
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.poll_interval_seconds,
                )
                # Shutdown event was set
                break
            except asyncio.TimeoutError:
                # Normal timeout, continue polling
                pass

        logger.info(
            "Polling loop ended",
            extra={
                "total_polls": self._total_polls,
                "total_events_fetched": self._total_events_fetched,
            },
        )

    async def _bulk_backfill(self) -> int:
        """
        Execute bulk backfill using paginated KQL queries to stream data to Kafka.

        Uses ingestion_time-based pagination to avoid KQL result size limits (64 MB).
        Each page fetches batch_size rows, then uses the last row's ingestion_time
        to fetch the next page.

        Returns:
            Total number of events processed
        """
        now = datetime.now(timezone.utc)

        # Use configured start time or fall back to default window
        if self._backfill_start_time is None:
            self._backfill_start_time = now - timedelta(
                hours=DEFAULT_BACKFILL_WINDOW_HOURS
            )

        # Use configured stop time or current time
        poll_to = self._backfill_stop_time or now

        # Get trace_id column name from config mapping
        trace_id_column = self.config.column_mapping.get("trace_id", "trace_id")

        logger.info(
            "Starting bulk backfill",
            extra={
                "backfill_start": self._backfill_start_time.isoformat(),
                "backfill_stop": poll_to.isoformat(),
                "has_checkpoint": self._checkpoint is not None,
            },
        )

        # Paginate using checkpoint (ingestion_time, trace_id) for deterministic resume
        total_processed = 0
        batch_num = 0
        batch_size = self.config.batch_size
        current_start = self._backfill_start_time
        current_trace_id: Optional[str] = None

        # If we have a checkpoint, use it as starting point
        if self._checkpoint:
            current_start = self._checkpoint.to_datetime()
            current_trace_id = self._checkpoint.last_trace_id
            logger.info(
                "Resuming bulk backfill from checkpoint",
                extra={
                    "checkpoint_time": current_start.isoformat(),
                    "checkpoint_trace_id": current_trace_id[:20] + "..."
                    if len(current_trace_id) > 20
                    else current_trace_id,
                },
            )

        backfill_start_time = time.perf_counter()

        while True:
            batch_num += 1

            # Format timestamps for KQL
            start_str = current_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            stop_str = poll_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            # Build paginated query with deterministic ordering
            # Use composite key comparison for exact resume:
            # (ingestion_time > start) OR (ingestion_time == start AND traceId > last_trace_id)
            if current_trace_id:
                # Resume from checkpoint - skip records at/before checkpoint
                escaped_trace_id = current_trace_id.replace("'", "\\'")
                # Note: wrap trace_id in guid() for correct comparison
                where_clause = f"""| where ingestion_time() > datetime({start_str})
    or (ingestion_time() == datetime({start_str}) and {trace_id_column} > guid('{escaped_trace_id}'))
| where ingestion_time() < datetime({stop_str})"""
            else:
                # No checkpoint - start from beginning of window
                where_clause = f"""| where ingestion_time() >= datetime({start_str})
| where ingestion_time() < datetime({stop_str})"""

            query = f"""{self.config.source_table}
{where_clause}
| extend ingestion_time = ingestion_time()
| order by ingestion_time asc, {trace_id_column} asc
| take {batch_size}"""

            logger.debug(
                "Executing bulk backfill page",
                extra={
                    "batch_num": batch_num,
                    "start": start_str,
                    "stop": stop_str,
                    "limit": batch_size,
                    "has_trace_id_filter": current_trace_id is not None,
                },
            )

            # Execute paginated query
            query_start = time.perf_counter()
            result = await self._kql_client.execute_query(query)
            query_duration_ms = (time.perf_counter() - query_start) * 1000

            rows_fetched = len(result.rows) if result.rows else 0

            if not result.rows:
                logger.info(
                    "Bulk backfill pagination complete - no more rows",
                    extra={
                        "total_batches": batch_num - 1,
                        "total_processed": total_processed,
                    },
                )
                break

            # Process this batch
            events_count = await self._process_results(result)

            # Extract last row info for checkpoint and next query
            last_row = result.rows[-1]
            last_trace_id = last_row.get("traceId", last_row.get(trace_id_column, ""))
            ingestion_time_val = last_row.get("ingestion_time", last_row.get("$IngestionTime"))

            if ingestion_time_val and last_trace_id:
                if isinstance(ingestion_time_val, datetime):
                    last_ingestion_time = ingestion_time_val
                else:
                    try:
                        last_ingestion_time = datetime.fromisoformat(
                            str(ingestion_time_val).replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        logger.warning(
                            "Could not parse ingestion_time for pagination",
                            extra={"raw_value": str(ingestion_time_val)[:100]},
                        )
                        last_ingestion_time = current_start

                # Save checkpoint after each batch
                self._save_checkpoint(last_ingestion_time, last_trace_id)

                # Update pagination state for next query
                current_start = last_ingestion_time
                current_trace_id = last_trace_id
            else:
                logger.warning("No ingestion_time or trace_id in last row, cannot paginate further")
                break
            total_processed += events_count

            logger.info(
                "Bulk backfill batch processed",
                extra={
                    "batch_num": batch_num,
                    "batch_rows": rows_fetched,
                    "batch_events": events_count,
                    "total_processed": total_processed,
                    "query_duration_ms": round(query_duration_ms, 2),
                    "next_start": next_start.isoformat() if next_start else None,
                    "boundary_trace_ids": len(last_batch_trace_ids),
                },
            )

            # Check for shutdown between batches
            if self._shutdown_event.is_set():
                logger.info(
                    "Bulk backfill interrupted by shutdown",
                    extra={"processed_before_shutdown": total_processed},
                )
                break

            # If we got fewer rows than requested, we're done
            if rows_fetched < batch_size:
                logger.info(
                    "Bulk backfill complete - last page was partial",
                    extra={
                        "total_batches": batch_num,
                        "total_processed": total_processed,
                        "last_batch_size": rows_fetched,
                    },
                )
                break

            # Move pagination cursor forward
            current_start = next_start

        # Mark backfill as complete
        self._backfill_mode = False
        self._bulk_backfill_complete = True
        self._total_events_fetched += total_processed
        total_duration_ms = (time.perf_counter() - backfill_start_time) * 1000

        logger.info(
            "Bulk backfill complete",
            extra={
                "total_events": total_processed,
                "total_batches": batch_num,
                "total_duration_ms": round(total_duration_ms, 2),
            },
        )

        return total_processed

    async def _poll_cycle(self) -> int:
        """
        Execute a single poll cycle.

        Returns:
            Number of events processed
        """
        self._total_polls += 1
        poll_start = time.perf_counter()

        # Calculate poll window
        now = datetime.now(timezone.utc)

        if self._backfill_mode:
            # During backfill, paginate by ingestion_time for efficiency
            if self._backfill_start_time is None:
                # First poll - calculate backfill start time from default window
                self._backfill_start_time = now - timedelta(
                    hours=DEFAULT_BACKFILL_WINDOW_HOURS
                )
                logger.info(
                    "Starting backfill mode (calculated from window)",
                    extra={
                        "backfill_start": self._backfill_start_time.isoformat(),
                        "window_hours": DEFAULT_BACKFILL_WINDOW_HOURS,
                    },
                )

            # Use last_ingestion_time for pagination if available
            if self._last_ingestion_time is not None:
                poll_from = self._last_ingestion_time
            else:
                poll_from = self._backfill_start_time

            # Use configured stop time or current time
            poll_to = self._backfill_stop_time or now
        else:
            # Normal mode - use last ingestion time/poll time
            poll_to = now
            if self._last_ingestion_time is not None:
                 # Resume exactly from last checkpoint
                 poll_from = self._last_ingestion_time
            elif self._last_poll_time is None:
                # First poll (no checkpoint) - use default window (1 hour)
                poll_from = now - timedelta(hours=1)
            else:
                # Fallback to last poll time (should have checkpoint if we ran efficiently)
                poll_from = self._last_poll_time

        # Build KQL query with cursor-based pagination (using trace_id and ingestion_time)
        query = self._build_query(
            base_table=self.config.source_table,
            poll_from=poll_from,
            poll_to=poll_to,
            limit=self.config.batch_size,
            checkpoint_trace_id=self._last_trace_id,
        )

        # Execute query
        query_start = time.perf_counter()
        result = await self._kql_client.execute_query(query)
        query_duration_ms = (time.perf_counter() - query_start) * 1000

        # Filter rows to skip those at/before checkpoint (deterministic resume)
        rows_to_process = self._filter_checkpoint_rows(result.rows) if result.rows else []

        # Process results (only rows after checkpoint)
        events_count = await self._process_filtered_results(rows_to_process)

        # Save checkpoint after successful processing (use last row from original result)
        if result.rows:
            last_row = result.rows[-1]
            trace_id_col = "traceId"
            last_trace_id = last_row.get("traceId", last_row.get(trace_id_col, ""))
            ingestion_time_str = last_row.get("ingestion_time", last_row.get("$IngestionTime"))

            if ingestion_time_str and last_trace_id:
                if isinstance(ingestion_time_str, datetime):
                    last_ingestion_time = ingestion_time_str
                else:
                    try:
                        last_ingestion_time = datetime.fromisoformat(
                            str(ingestion_time_str).replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        last_ingestion_time = None

                if last_ingestion_time:
                    self._save_checkpoint(last_ingestion_time, last_trace_id)

        # Update state
        self._last_poll_time = poll_to
        self._total_events_fetched += events_count

        # Check if backfill is complete (got fewer events than batch_size)
        if self._backfill_mode and events_count < self.config.batch_size:
            self._backfill_mode = False
            logger.info(
                "Backfill complete, switching to real-time mode",
                extra={
                    "total_backfilled": self._total_events_fetched,
                    "final_batch_size": events_count,
                },
            )

        poll_duration_ms = (time.perf_counter() - poll_start) * 1000

        # Track consecutive empty polls
        if events_count == 0:
            self._consecutive_empty_polls += 1
        else:
            self._consecutive_empty_polls = 0

        # Log metrics
        logger.info(
            "Poll cycle completed",
            extra={
                "events_fetched": events_count,
                "poll_duration_ms": round(poll_duration_ms, 2),
                "query_duration_ms": round(query_duration_ms, 2),
                "poll_from": poll_from.isoformat(),
                "poll_to": poll_to.isoformat(),
                "consecutive_empty_polls": self._consecutive_empty_polls,
                "backfill_mode": self._backfill_mode,
            },
        )

        # Warn on extended period of no events
        if self._consecutive_empty_polls >= 10:
            logger.warning(
                "No events fetched for extended period",
                extra={"consecutive_empty_polls": self._consecutive_empty_polls},
            )

        return events_count

    def _filter_checkpoint_rows(self, rows: list[dict]) -> list[dict]:
        """
        Filter rows to skip those at/before the checkpoint.

        Uses deterministic ordering (ingestion_time asc, traceId asc) to determine
        which rows to skip. A row is skipped if:
        - Its ingestion_time < checkpoint.last_ingestion_time, OR
        - Its ingestion_time == checkpoint.last_ingestion_time AND
          its trace_id <= checkpoint.last_trace_id

        Args:
            rows: List of row dicts from KQL query result

        Returns:
            List of rows that should be processed (after checkpoint)
        """
        if not self._checkpoint or not rows:
            return rows

        checkpoint_time = self._checkpoint.to_datetime()
        checkpoint_trace_id = self._checkpoint.last_trace_id
        trace_id_col = "traceId"

        filtered_rows = []
        skipped_count = 0

        for row in rows:
            # Get row's ingestion_time
            ingestion_time_str = row.get("ingestion_time", row.get("$IngestionTime"))
            if isinstance(ingestion_time_str, datetime):
                row_time = ingestion_time_str
            elif ingestion_time_str:
                try:
                    row_time = datetime.fromisoformat(
                        str(ingestion_time_str).replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError):
                    # Can't parse time - include row to be safe
                    filtered_rows.append(row)
                    continue
            else:
                # No time - include row to be safe
                filtered_rows.append(row)
                continue

            # Get row's trace_id
            row_trace_id = row.get("traceId", row.get(trace_id_col, ""))

            # Deterministic comparison: skip if (time, trace_id) <= checkpoint
            if row_time < checkpoint_time:
                skipped_count += 1
                continue
            elif row_time == checkpoint_time and row_trace_id <= checkpoint_trace_id:
                skipped_count += 1
                continue

            # Row is after checkpoint - include it
            filtered_rows.append(row)

        if skipped_count > 0:
            logger.debug(
                "Skipped rows at/before checkpoint",
                extra={
                    "skipped_count": skipped_count,
                    "remaining_count": len(filtered_rows),
                    "checkpoint_time": checkpoint_time.isoformat(),
                    "checkpoint_trace_id": checkpoint_trace_id[:20] + "..."
                    if len(checkpoint_trace_id) > 20
                    else checkpoint_trace_id,
                },
            )

        return filtered_rows

    async def _process_filtered_results(self, rows: list[dict]) -> int:
        """
        Process filtered rows into events and publish to events.raw topic.

        Events are published to the events_topic (xact.events.raw) for consumption
        by the EventIngester, which handles attachment processing and produces
        download tasks.

        Args:
            rows: List of row dicts to process (already filtered by checkpoint)

        Returns:
            Number of events processed
        """
        if not rows:
            return 0

        events_processed = 0
        events_for_delta = []

        for row in rows:
            try:
                # Convert row to EventMessage
                event = self._row_to_event(row)

                # Write to Delta (batch for efficiency)
                events_for_delta.append(event)

                # Publish EventMessage to events.raw topic
                # EventIngester will consume and create download tasks
                await self._publish_event(event)

                events_processed += 1

            except Exception as e:
                logger.error(
                    "Failed to process row",
                    extra={
                        "error": str(e)[:200],
                        "row_trace_id": row.get(self.config.column_mapping["trace_id"]),
                    },
                    exc_info=True,
                )

        # Track stats for backfill reporting
        if events_for_delta:
            for event in events_for_delta:
                # Get event ID based on schema type
                if hasattr(event, 'trace_id'):
                    event_id = event.trace_id
                elif hasattr(event, 'event_id'):
                    event_id = event.event_id
                else:
                    event_id = str(hash(str(event)))

                if event_id in self._seen_trace_ids:
                    self._duplicate_count += 1
                else:
                    self._seen_trace_ids.add(event_id)

        return events_processed

    async def _process_results(self, result: KQLQueryResult) -> int:
        """
        Process query results into events and publish to events.raw topic.

        DEPRECATED: Use _process_filtered_results instead.
        Kept for backwards compatibility with bulk backfill.

        Args:
            result: KQL query result

        Returns:
            Number of events processed
        """
        if result.is_empty:
            return 0

        return await self._process_filtered_results(result.rows)

    def _build_query(
        self,
        base_table: str,
        poll_from: datetime,
        poll_to: datetime,
        limit: int = 1000,
        checkpoint_trace_id: Optional[str] = None,
    ) -> str:
        """
        Build KQL query with cursor-based pagination.

        Uses deterministic ordering (ingestion_time asc, traceId asc) to enable
        exact resume from checkpoint.

        Args:
            base_table: Source table name in Eventhouse
            poll_from: Start of time window / Checkpoint time
            poll_to: End of time window
            limit: Max rows to return (default: 1000)
            checkpoint_trace_id: Last processed trace_id for cursor pagination

        Returns:
            Complete KQL query string
        """
        # Format datetime for KQL
        from_str = poll_from.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        to_str = poll_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Get trace_id column name from config mapping
        # Default to 'trace_id' if not found, but many Kusto tables use 'traceId'
        trace_id_column = "traceId"

        # Construct Where Clause
        if checkpoint_trace_id:
             # Cursor-based pagination:
             # (time > checkpoint_time) OR (time == checkpoint_time AND traceId > checkpoint_traceId)
             escaped_trace_id = checkpoint_trace_id.replace("'", "\\'")
             # Note: wrap trace_id in guid() for correct comparison if the column is a guid type
             where_clause = f"""| where ingestion_time() > datetime({from_str})
    or (ingestion_time() == datetime({from_str}) and {trace_id_column} > guid('{escaped_trace_id}'))"""
        else:
             # Standard time window (inclusive start)
             where_clause = f"| where ingestion_time() >= datetime({from_str})"

        # Deterministic ordering enables exact checkpoint resume
        query = f"""
{base_table}
{where_clause}
| where ingestion_time() < datetime({to_str})
| extend ingestion_time = ingestion_time()
| order by ingestion_time asc, {trace_id_column} asc
| take {limit}
"""

        logger.debug(
            "Built KQL query",
            extra={
                "base_table": base_table,
                "poll_from": from_str,
                "poll_to": to_str,
                "limit": limit,
                "has_cursor": checkpoint_trace_id is not None,
            },
        )

        return query.strip()

    async def _publish_event(self, event: Any) -> None:
        """
        Publish event message to the events.raw topic.

        Supports both xact EventMessage and claimx ClaimXEventMessage.

        Args:
            event: Event message to publish (EventMessage or ClaimXEventMessage)
        """
        try:
            # Determine event key based on schema type
            if hasattr(event, 'trace_id'):
                # xact EventMessage
                event_key = event.trace_id
                event_id = event.trace_id
            elif hasattr(event, 'event_id'):
                # claimx ClaimXEventMessage
                event_key = event.event_id
                event_id = event.event_id
            else:
                # Fallback
                event_key = str(hash(str(event)))
                event_id = event_key

            await self._producer.send(
                topic=self.config.kafka.get_topic(self.config.domain, "events"),
                key=event_key,
                value=event,
                headers={"event_id": event_id},
            )

            logger.debug(
                "Published event to events.raw",
                extra={
                    "event_id": event_id,
                    "domain": self.config.domain,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to publish event",
                extra={
                    "error": str(e)[:200],
                },
            )

    def _row_to_event(self, row: dict[str, Any]) -> Any:
        """
        Convert a KQL result row to event message using configured schema class.

        Uses the event_schema_class from config which must have a
        from_eventhouse_row(row: Dict[str, Any]) classmethod.

        Args:
            row: Dictionary from KQL result

        Returns:
            Event message instance (EventMessage or ClaimXEventMessage)
        """
        # Use the configured event schema class to convert the row
        return self._event_schema_class.from_eventhouse_row(row)

    @property
    def is_running(self) -> bool:
        """Check if poller is running."""
        return self._running

    @property
    def stats(self) -> dict[str, Any]:
        """Get current poller statistics."""
        return {
            "running": self._running,
            "total_polls": self._total_polls,
            "total_events_fetched": self._total_events_fetched,
            "unique_trace_ids": len(self._seen_trace_ids),
            "duplicate_count": self._duplicate_count,
            "consecutive_empty_polls": self._consecutive_empty_polls,
            "last_poll_time": (
                self._last_poll_time.isoformat() if self._last_poll_time else None
            ),
        }
