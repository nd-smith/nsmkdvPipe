"""
KQL Event Poller for polling events from Microsoft Fabric Eventhouse.

Polls Eventhouse at configurable intervals, applies deduplication,
and produces events to Kafka for processing by download workers.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
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

        return cls(
            eventhouse=eventhouse_config,
            kafka=kafka_config,
            domain=poller_data.get("domain", "xact"),
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

        return cls(
            eventhouse=eventhouse_config,
            kafka=kafka_config,
            domain=os.getenv("PIPELINE_DOMAIN", "xact"),
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "30")),
            batch_size=int(os.getenv("POLL_BATCH_SIZE", "1000")),
            source_table=os.getenv("EVENTHOUSE_SOURCE_TABLE", "Events"),
            events_table_path=xact_events_path,
            max_kafka_lag=int(os.getenv("MAX_KAFKA_LAG", "10000")),
            backfill_start_stamp=os.getenv("BACKFILL_START_TIMESTAMP"),
            backfill_stop_stamp=os.getenv("BACKFILL_STOP_TIMESTAMP"),
            bulk_backfill=bulk_backfill,
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
        self._last_batch_trace_ids: list[str] = []

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

        # Use configured start time or fall back to window-based calculation
        if self._backfill_start_time is None:
            self._backfill_start_time = now - timedelta(
                hours=self.config.dedup.eventhouse_query_window_hours
            )

        # Use configured stop time or current time
        poll_to = self._backfill_stop_time or now

        logger.info(
            "Starting bulk backfill",
            extra={
                "backfill_start": self._backfill_start_time.isoformat(),
                "backfill_stop": poll_to.isoformat(),
            },
        )

        # Paginate using ingestion_time to avoid KQL 64 MB result limit
        total_processed = 0
        batch_num = 0
        batch_size = self.config.batch_size
        current_start = self._backfill_start_time
        last_batch_trace_ids: list[str] = []
        backfill_start_time = time.perf_counter()

        while True:
            batch_num += 1

            # Format timestamps for KQL
            start_str = current_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            stop_str = poll_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            # Build paginated query with limit
            # Use anti-join on last batch's trace_ids to handle events with same ingestion_time
            trace_id_column = self.config.dedup.eventhouse_trace_id_column
            if last_batch_trace_ids:
                escaped_ids = [tid.replace("'", "\\'") for tid in last_batch_trace_ids]
                id_list = ", ".join(f"'{tid}'" for tid in escaped_ids)
                anti_join_clause = f"\n| where {trace_id_column} !in (dynamic([{id_list}]))"
            else:
                anti_join_clause = ""

            query = f"""{self.config.source_table}
| where ingestion_time() >= datetime({start_str})
| where ingestion_time() < datetime({stop_str}){anti_join_clause}
| extend ingestion_time = ingestion_time()
| order by ingestion_time asc
| take {batch_size}"""

            logger.debug(
                "Executing bulk backfill page",
                extra={
                    "batch_num": batch_num,
                    "start": start_str,
                    "stop": stop_str,
                    "limit": batch_size,
                    "anti_join_count": len(last_batch_trace_ids),
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

            # Extract pagination info for next query
            last_row = result.rows[-1]
            ingestion_time_val = last_row.get("ingestion_time", last_row.get("$IngestionTime"))

            if ingestion_time_val:
                if isinstance(ingestion_time_val, datetime):
                    next_start = ingestion_time_val
                else:
                    try:
                        next_start = datetime.fromisoformat(
                            str(ingestion_time_val).replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        logger.warning(
                            "Could not parse ingestion_time for pagination",
                            extra={"raw_value": str(ingestion_time_val)[:100]},
                        )
                        next_start = current_start
            else:
                logger.warning("No ingestion_time in last row, cannot paginate further")
                next_start = current_start

            # Collect trace_ids from rows at the boundary (same ingestion_time as last row)
            # These are used for anti-join in next query to prevent duplicates
            last_batch_trace_ids = [
                row.get("traceId", row.get("trace_id", ""))
                for row in result.rows
                if row.get("ingestion_time", row.get("$IngestionTime")) == ingestion_time_val
            ]

            # Process this batch
            events_count = await self._process_results(result)
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
                # First poll - calculate backfill start time from window config
                self._backfill_start_time = now - timedelta(
                    hours=self.config.dedup.eventhouse_query_window_hours
                )
                logger.info(
                    "Starting backfill mode (calculated from window)",
                    extra={
                        "backfill_start": self._backfill_start_time.isoformat(),
                        "window_hours": self.config.dedup.eventhouse_query_window_hours,
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
            # Normal mode - use last poll time with overlap
            # Simple time window without deduplication (dedup happens in download worker)
            poll_to = now
            if self._last_poll_time is None:
                # First poll - use default window (1 hour)
                poll_from = now - timedelta(hours=1)
            else:
                # Subsequent poll - from last poll time with 5-minute overlap
                poll_from = self._last_poll_time - timedelta(minutes=5)

        # Build simple KQL query (no deduplication at polling level)
        query = self._build_query(
            base_table=self.config.source_table,
            poll_from=poll_from,
            poll_to=poll_to,
            limit=self.config.batch_size,
        )

        # Execute query
        query_start = time.perf_counter()
        result = await self._kql_client.execute_query(query)
        query_duration_ms = (time.perf_counter() - query_start) * 1000

        # Extract pagination info from result before processing
        if self._backfill_mode and result.rows:
            # Get trace_ids from this batch for next query's anti-join
            self._last_batch_trace_ids = [
                row.get("traceId", row.get("trace_id", ""))
                for row in result.rows
            ]

            # Get max ingestion_time for pagination (last row since ordered asc)
            last_row = result.rows[-1]
            ingestion_time_str = last_row.get("ingestion_time", last_row.get("$IngestionTime"))
            if ingestion_time_str:
                if isinstance(ingestion_time_str, datetime):
                    self._last_ingestion_time = ingestion_time_str
                else:
                    # Parse string timestamp
                    try:
                        self._last_ingestion_time = datetime.fromisoformat(
                            ingestion_time_str.replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        logger.warning(
                            "Could not parse ingestion_time for pagination",
                            extra={"ingestion_time": str(ingestion_time_str)[:50]},
                        )

        # Process results
        events_count = await self._process_results(result)

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

    async def _process_results(self, result: KQLQueryResult) -> int:
        """
        Process query results into events and publish to events.raw topic.

        Events are published to the events_topic (xact.events.raw) for consumption
        by the EventIngester, which handles attachment processing and produces
        download tasks. This ensures consistent behavior between EventHub and
        Eventhouse modes.

        Args:
            result: KQL query result

        Returns:
            Number of events processed
        """
        if result.is_empty:
            return 0

        events_processed = 0
        events_for_delta = []

        for row in result.rows:
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

    def _build_query(
        self,
        base_table: str,
        poll_from: datetime,
        poll_to: datetime,
        limit: int = 1000,
    ) -> str:
        """
        Build simple KQL query with time filter.

        No deduplication at polling level - duplicates are handled by
        download worker's in-memory cache.

        Args:
            base_table: Source table name in Eventhouse
            poll_from: Start of time window
            poll_to: End of time window
            limit: Max rows to return (default: 1000)

        Returns:
            Complete KQL query string
        """
        # Format datetime for KQL
        from_str = poll_from.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_str = poll_to.strftime("%Y-%m-%dT%H:%M:%SZ")

        query = f"""
{base_table}
| where ingestion_time() >= datetime({from_str})
| where ingestion_time() < datetime({to_str})
| extend ingestion_time = ingestion_time()
| order by ingestion_time() asc
| take {limit}
"""

        logger.debug(
            "Built KQL query",
            extra={
                "base_table": base_table,
                "poll_from": from_str,
                "poll_to": to_str,
                "limit": limit,
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
