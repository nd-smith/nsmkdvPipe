"""
KQL Event Poller for polling events from Microsoft Fabric Eventhouse.

Polls Eventhouse at configurable intervals, applies deduplication,
and produces events to Kafka for processing by download workers.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yaml

from kafka_pipeline.config import KafkaConfig, load_config as load_kafka_config
from kafka_pipeline.eventhouse.dedup import DedupConfig, EventhouseDeduplicator
from kafka_pipeline.eventhouse.kql_client import (
    DEFAULT_CONFIG_PATH,
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)
from kafka_pipeline.producer import BaseKafkaProducer
from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.writers.delta_events import DeltaEventsWriter

from core.paths.resolver import generate_blob_path
from core.security.url_validation import validate_download_url
from verisk_pipeline.common.security import sanitize_url

logger = logging.getLogger(__name__)


@dataclass
class PollerConfig:
    """Configuration for KQL Event Poller."""

    # Eventhouse connection
    eventhouse: EventhouseConfig

    # Kafka configuration
    kafka: KafkaConfig

    # Deduplication configuration
    dedup: DedupConfig

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
    events_table_path: str = ""  # Path to xact_events table

    # Backpressure settings
    max_kafka_lag: int = 10_000  # Pause polling if lag exceeds this
    lag_check_interval_seconds: int = 60  # How often to check lag

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
            dedup_data = eventhouse_section.get("dedup", {})

        # Apply environment variable overrides for poller
        if os.getenv("POLL_INTERVAL_SECONDS"):
            poller_data["poll_interval_seconds"] = os.getenv("POLL_INTERVAL_SECONDS")
        if os.getenv("POLL_BATCH_SIZE"):
            poller_data["batch_size"] = os.getenv("POLL_BATCH_SIZE")
        if os.getenv("EVENTHOUSE_SOURCE_TABLE"):
            poller_data["source_table"] = os.getenv("EVENTHOUSE_SOURCE_TABLE")
        if os.getenv("XACT_EVENTS_TABLE_PATH"):
            poller_data["events_table_path"] = os.getenv("XACT_EVENTS_TABLE_PATH")
        if os.getenv("MAX_KAFKA_LAG"):
            poller_data["max_kafka_lag"] = os.getenv("MAX_KAFKA_LAG")

        # Apply environment variable overrides for dedup
        if os.getenv("XACT_EVENTS_TABLE_PATH"):
            dedup_data["xact_events_table_path"] = os.getenv("XACT_EVENTS_TABLE_PATH")
        if os.getenv("DEDUP_XACT_EVENTS_WINDOW_HOURS"):
            dedup_data["xact_events_window_hours"] = os.getenv(
                "DEDUP_XACT_EVENTS_WINDOW_HOURS"
            )
        if os.getenv("DEDUP_EVENTHOUSE_WINDOW_HOURS"):
            dedup_data["eventhouse_query_window_hours"] = os.getenv(
                "DEDUP_EVENTHOUSE_WINDOW_HOURS"
            )
        if os.getenv("DEDUP_OVERLAP_MINUTES"):
            dedup_data["overlap_minutes"] = os.getenv("DEDUP_OVERLAP_MINUTES")

        xact_events_path = poller_data.get("events_table_path", "")

        dedup_config = DedupConfig(
            xact_events_table_path=dedup_data.get(
                "xact_events_table_path", xact_events_path
            ),
            xact_events_window_hours=int(
                dedup_data.get("xact_events_window_hours", 24)
            ),
            eventhouse_query_window_hours=int(
                dedup_data.get("eventhouse_query_window_hours", 1)
            ),
            overlap_minutes=int(dedup_data.get("overlap_minutes", 5)),
            max_trace_ids_per_query=int(
                dedup_data.get("max_trace_ids_per_query", 50_000)
            ),
        )

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
            dedup=dedup_config,
            poll_interval_seconds=int(poller_data.get("poll_interval_seconds", 30)),
            batch_size=int(poller_data.get("batch_size", 1000)),
            source_table=poller_data.get("source_table", "Events"),
            column_mapping=column_mapping,
            events_table_path=xact_events_path,
            max_kafka_lag=int(poller_data.get("max_kafka_lag", 10_000)),
            lag_check_interval_seconds=int(
                poller_data.get("lag_check_interval_seconds", 60)
            ),
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

        dedup_config = DedupConfig(
            xact_events_table_path=xact_events_path,
            xact_events_window_hours=int(
                os.getenv("DEDUP_XACT_EVENTS_WINDOW_HOURS", "24")
            ),
            eventhouse_query_window_hours=int(
                os.getenv("DEDUP_EVENTHOUSE_WINDOW_HOURS", "1")
            ),
            overlap_minutes=int(os.getenv("DEDUP_OVERLAP_MINUTES", "5")),
        )

        return cls(
            eventhouse=eventhouse_config,
            kafka=kafka_config,
            dedup=dedup_config,
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "30")),
            batch_size=int(os.getenv("POLL_BATCH_SIZE", "1000")),
            source_table=os.getenv("EVENTHOUSE_SOURCE_TABLE", "Events"),
            events_table_path=xact_events_path,
            max_kafka_lag=int(os.getenv("MAX_KAFKA_LAG", "10000")),
        )


class KQLEventPoller:
    """
    Polls Eventhouse for new events and produces them to Kafka.

    Workflow:
    1. Query Eventhouse with time filter and deduplication
    2. Convert KQL results to EventMessage format
    3. For each attachment, create DownloadTaskMessage
    4. Produce to downloads.pending topic
    5. Write events to Delta Lake for deduplication tracking

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

        # Components (initialized on start)
        self._kql_client: Optional[KQLClient] = None
        self._deduplicator: Optional[EventhouseDeduplicator] = None
        self._producer: Optional[BaseKafkaProducer] = None
        self._delta_writer: Optional[DeltaEventsWriter] = None

        # State
        self._last_poll_time: Optional[datetime] = None
        self._consecutive_empty_polls = 0
        self._total_events_fetched = 0
        self._total_polls = 0

        logger.info(
            "Initialized KQLEventPoller",
            extra={
                "source_table": config.source_table,
                "poll_interval": config.poll_interval_seconds,
                "batch_size": config.batch_size,
            },
        )

    async def __aenter__(self) -> "KQLEventPoller":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    async def start(self) -> None:
        """Initialize all components."""
        logger.info("Starting KQLEventPoller components")

        # Initialize KQL client
        self._kql_client = KQLClient(self.config.eventhouse)
        await self._kql_client.connect()

        # Initialize deduplicator
        self._deduplicator = EventhouseDeduplicator(self.config.dedup)

        # Initialize Kafka producer
        self._producer = BaseKafkaProducer(self.config.kafka)
        await self._producer.start()

        # Initialize Delta writer if path configured
        if self.config.events_table_path:
            self._delta_writer = DeltaEventsWriter(
                table_path=self.config.events_table_path,
                dedupe_window_hours=24,
            )

        self._running = True
        logger.info("KQLEventPoller components started")

    async def stop(self) -> None:
        """Gracefully shutdown all components."""
        logger.info("Stopping KQLEventPoller")
        self._running = False
        self._shutdown_event.set()

        if self._producer:
            await self._producer.stop()

        if self._kql_client:
            await self._kql_client.close()

        logger.info("KQLEventPoller stopped")

    async def run(self) -> None:
        """
        Main polling loop.

        Runs until stop() is called or shutdown event is set.
        """
        logger.info("Starting polling loop")

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

    async def _poll_cycle(self) -> int:
        """
        Execute a single poll cycle.

        Returns:
            Number of events processed
        """
        self._total_polls += 1
        poll_start = time.perf_counter()

        # Calculate poll window
        poll_from, poll_to = self._deduplicator.get_poll_window(self._last_poll_time)

        # Build query with deduplication
        query = self._deduplicator.build_deduped_query(
            base_table=self.config.source_table,
            poll_from=poll_from,
            poll_to=poll_to,
            limit=self.config.batch_size,
        )

        # Execute query
        query_start = time.perf_counter()
        result = await self._kql_client.execute_query(query)
        query_duration_ms = (time.perf_counter() - query_start) * 1000

        # Process results
        events_count = await self._process_results(result)

        # Update state
        self._last_poll_time = poll_to
        self._total_events_fetched += events_count

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

        # Write events to Delta Lake (batch)
        if events_for_delta and self._delta_writer:
            asyncio.create_task(self._write_events_to_delta(events_for_delta))

        return events_processed

    async def _publish_event(self, event: EventMessage) -> None:
        """
        Publish EventMessage to the events.raw topic.

        EventMessage schema matches verisk_pipeline EventRecord.

        Args:
            event: EventMessage to publish
        """
        try:
            await self._producer.send(
                topic=self.config.kafka.events_topic,
                key=event.trace_id,
                value=event,
                headers={"trace_id": event.trace_id},
            )

            logger.debug(
                "Published event to events.raw",
                extra={
                    "trace_id": event.trace_id,
                    "type": event.type,
                    "status_subtype": event.status_subtype,
                    "attachment_count": len(event.attachments) if event.attachments else 0,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to publish event",
                extra={
                    "trace_id": event.trace_id,
                    "error": str(e)[:200],
                },
            )

    def _row_to_event(self, row: dict[str, Any]) -> EventMessage:
        """
        Convert a KQL result row to EventMessage.

        Creates EventMessage matching verisk_pipeline EventRecord schema:
        - type: Full event type string
        - version: Event version
        - utc_datetime: Event timestamp
        - trace_id: Trace identifier
        - data: JSON string with nested event data

        Eventhouse source columns:
        - type: "verisk.claims.property.xn.documentsReceived"
        - version: Event version number
        - utcDateTime: Event timestamp
        - traceId: UUID trace identifier (camelCase!)
        - data: JSON object containing payload and nested attachments

        Args:
            row: Dictionary from KQL result

        Returns:
            EventMessage matching verisk_pipeline EventRecord schema
        """
        import json

        # Extract trace_id (supports both traceId and trace_id column names)
        trace_id = row.get("traceId", row.get("trace_id", ""))

        # Extract full event type
        full_type = row.get("type", "")

        # Extract version
        version = str(row.get("version", ""))

        # Extract timestamp as string
        timestamp_val = row.get("utcDateTime", row.get("timestamp", ""))
        if isinstance(timestamp_val, datetime):
            utc_datetime = timestamp_val.isoformat()
        else:
            utc_datetime = str(timestamp_val) if timestamp_val else ""

        # Extract data field as JSON string
        data_val = row.get("data", {})
        if isinstance(data_val, dict):
            data = json.dumps(data_val)
        elif isinstance(data_val, str):
            data = data_val
        else:
            data = "{}"

        # Create EventMessage using from_eventhouse_row for proper parsing
        return EventMessage.from_eventhouse_row({
            "type": full_type,
            "version": version,
            "utcDateTime": utc_datetime,
            "traceId": trace_id,
            "data": data,
        })

    async def _process_event_attachments(self, event: EventMessage) -> None:
        """
        Process attachments from an event (legacy method).

        Note: This is now handled by EventIngester after events are published
        to the events.raw topic. Kept for backwards compatibility.

        Args:
            event: EventMessage to process (matches verisk_pipeline EventRecord)
        """
        if not event.attachments:
            logger.debug(
                "Event has no attachments",
                extra={"trace_id": event.trace_id},
            )
            return

        # Extract assignment_id from data
        assignment_id = event.assignment_id
        if not assignment_id:
            logger.warning(
                "Event missing assignmentId, cannot generate paths",
                extra={"trace_id": event.trace_id},
            )
            return

        for attachment_url in event.attachments:
            await self._process_single_attachment(
                event=event,
                attachment_url=attachment_url,
                assignment_id=assignment_id,
            )

    async def _process_single_attachment(
        self,
        event: EventMessage,
        attachment_url: str,
        assignment_id: str,
    ) -> None:
        """
        Process a single attachment (legacy method).

        Note: This is now handled by EventIngester after events are published
        to the events.raw topic. Kept for backwards compatibility.

        Args:
            event: Source event (matches verisk_pipeline EventRecord)
            attachment_url: URL to download
            assignment_id: For path generation
        """
        # Validate URL
        is_valid, error_message = validate_download_url(attachment_url)
        if not is_valid:
            logger.warning(
                "Invalid attachment URL",
                extra={
                    "trace_id": event.trace_id,
                    "url": sanitize_url(attachment_url),
                    "error": error_message,
                },
            )
            return

        # Generate blob path using status_subtype
        try:
            blob_path, file_type = generate_blob_path(
                status_subtype=event.status_subtype,
                trace_id=event.trace_id,
                assignment_id=assignment_id,
                download_url=attachment_url,
                estimate_version=event.estimate_version,
            )
        except Exception as e:
            logger.error(
                "Failed to generate blob path",
                extra={
                    "trace_id": event.trace_id,
                    "error": str(e)[:200],
                },
            )
            return

        # Create download task matching verisk_pipeline Task schema
        download_task = DownloadTaskMessage(
            trace_id=event.trace_id,
            attachment_url=attachment_url,
            blob_path=blob_path,
            status_subtype=event.status_subtype,
            file_type=file_type,
            assignment_id=assignment_id,
            estimate_version=event.estimate_version,
            retry_count=0,
        )

        # Produce to Kafka
        try:
            await self._producer.send(
                topic=self.config.kafka.downloads_pending_topic,
                key=event.trace_id,
                value=download_task,
                headers={"trace_id": event.trace_id},
            )

            logger.debug(
                "Created download task",
                extra={
                    "trace_id": event.trace_id,
                    "blob_path": blob_path,
                    "file_type": file_type,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to produce download task",
                extra={
                    "trace_id": event.trace_id,
                    "error": str(e)[:200],
                },
            )

    async def _write_events_to_delta(self, events: list[EventMessage]) -> None:
        """
        Write events to Delta Lake for deduplication tracking.

        Uses flatten_events() from verisk_pipeline to transform events
        into the correct xact_events schema with all 28 columns.

        Runs as background task, failures don't affect main processing.

        Args:
            events: Events to write (matching verisk_pipeline EventRecord schema)
        """
        if not self._delta_writer:
            return

        try:
            # Convert EventMessage objects to Eventhouse row format
            raw_events = [event.to_eventhouse_row() for event in events]

            # Write using flatten_events() transformation
            success = await self._delta_writer.write_raw_events(raw_events)

            if not success:
                logger.warning(
                    "Delta write failed for events batch",
                    extra={"event_count": len(events)},
                )
        except Exception as e:
            logger.error(
                "Error writing events to Delta",
                extra={
                    "event_count": len(events),
                    "error": str(e)[:200],
                },
            )

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
            "consecutive_empty_polls": self._consecutive_empty_polls,
            "last_poll_time": (
                self._last_poll_time.isoformat() if self._last_poll_time else None
            ),
        }
