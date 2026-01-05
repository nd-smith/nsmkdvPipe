"""
ClaimX Enrichment Worker - Enriches events with API data and produces download tasks.

This worker is the core of the ClaimX enrichment pipeline:
1. Consumes ClaimXEnrichmentTask from enrichment pending topic
2. Routes events to appropriate handlers based on event_type
3. Handlers call ClaimX API to fetch entity data
4. Writes entity rows to Delta Lake tables
5. Produces ClaimXDownloadTask for media files with download URLs

Consumer group: {prefix}-claimx-enrichment-worker
Input topic: claimx.enrichment.pending
Output topic: claimx.downloads.pending
Delta tables: claimx_projects, claimx_contacts, claimx_attachment_metadata, claimx_tasks,
              claimx_task_templates, claimx_external_links, claimx_video_collab
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.setup import get_logger
from core.types import ErrorCategory
from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.metrics import record_delta_write
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from kafka_pipeline.claimx.handlers import get_handler_registry, HandlerRegistry
from kafka_pipeline.claimx.monitoring import HealthCheckServer
from kafka_pipeline.claimx.retry import EnrichmentRetryHandler
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import (
    ClaimXEnrichmentTask,
    ClaimXDownloadTask,
)
from kafka_pipeline.claimx.writers import ClaimXEntityWriter

logger = get_logger(__name__)


class ClaimXEnrichmentWorker:
    """
    Worker to consume ClaimX enrichment tasks and enrich them with API data.

    Processes ClaimXEnrichmentTask records from the enrichment pending topic,
    routes them to appropriate handlers, fetches entity data from ClaimX API,
    writes entity rows to Delta tables, and produces download tasks for media files.

    Features:
    - Event routing via handler registry
    - Concurrent API calls with rate limiting (via ClaimXApiClient)
    - Batch processing by handler type
    - Entity data writes to 7 Delta tables
    - Download task generation for media files
    - Graceful shutdown with background task tracking

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> writer = ClaimXEntityWriter(...)
        >>> worker = ClaimXEnrichmentWorker(
        ...     config=config,
        ...     entity_writer=writer,
        ... )
        >>> await worker.start()
        >>> # Worker runs until stopped
        >>> await worker.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        entity_writer: ClaimXEntityWriter,
        enable_delta_writes: bool = True,
        enrichment_topic: str = "",
        download_topic: str = "",
        producer_config: Optional[KafkaConfig] = None,
        batch_size: int = 100,
        batch_timeout_seconds: float = 5.0,
    ):
        """
        Initialize ClaimX enrichment worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings)
            entity_writer: ClaimXEntityWriter for writing entity rows to Delta tables
            enable_delta_writes: Whether to enable Delta Lake writes (default: True)
            enrichment_topic: Topic name for enrichment tasks (e.g., "claimx.enrichment.pending")
            download_topic: Topic name for download tasks (e.g., "claimx.downloads.pending")
            producer_config: Optional separate Kafka config for producer
            batch_size: Number of tasks to process in a batch (default: 100)
            batch_timeout_seconds: Max seconds to wait for batch to fill (default: 5.0)
        """
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.enrichment_topic = enrichment_topic or "claimx.enrichment.pending"
        self.download_topic = download_topic or "claimx.downloads.pending"
        self.entity_writer = entity_writer
        self.enable_delta_writes = enable_delta_writes
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds

        # Consumer group for ClaimX enrichment
        self.consumer_group = (
            f"{config.consumer_group_prefix}-claimx-enrichment-worker"
        )

        # Build list of topics to consume from (pending + retry topics)
        retry_topics = [
            self._get_retry_topic(i) for i in range(len(config.retry_delays))
        ]
        self.topics = [self.enrichment_topic] + retry_topics

        # Kafka components (initialized in start())
        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[BaseKafkaConsumer] = None
        self.api_client: Optional[ClaimXApiClient] = None
        self.retry_handler: Optional[EnrichmentRetryHandler] = None

        # Handler registry for routing events
        self.handler_registry: HandlerRegistry = get_handler_registry()

        # Health check server
        health_port = config.claimx_enrichment_health_port if hasattr(config, 'claimx_enrichment_health_port') else 8081
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-enricher",
        )

        # Background task tracking for graceful shutdown
        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0

        # Batch accumulation
        self._batch: List[ClaimXEnrichmentTask] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: Optional[asyncio.Task] = None

        logger.info(
            "Initialized ClaimXEnrichmentWorker",
            extra={
                "consumer_group": self.consumer_group,
                "topics": self.topics,
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "delta_writes_enabled": self.enable_delta_writes,
                "batch_size": self.batch_size,
                "batch_timeout_seconds": self.batch_timeout_seconds,
                "retry_delays": config.retry_delays,
                "max_retries": config.max_retries,
            },
        )

    @property
    def config(self) -> KafkaConfig:
        """Backward-compatible property returning consumer_config."""
        return self.consumer_config

    async def start(self) -> None:
        """
        Start the ClaimX enrichment worker.

        Initializes producer, consumer, and API client, then begins consuming
        enrichment tasks. This method runs until stop() is called.

        Raises:
            Exception: If producer, consumer, or API client fails to start
        """
        logger.info("Starting ClaimXEnrichmentWorker")

        # Start health check server first
        await self.health_server.start()

        # Initialize API client
        self.api_client = ClaimXApiClient(
            base_url=self.consumer_config.claimx_api_url,
            username=self.consumer_config.claimx_api_username,
            password=self.consumer_config.claimx_api_password,
            timeout_seconds=self.consumer_config.claimx_api_timeout_seconds,
            max_concurrent=self.consumer_config.claimx_api_concurrency,
        )
        await self.api_client._ensure_session()

        # Check API reachability
        api_reachable = not self.api_client.is_circuit_open()
        self.health_server.set_ready(
            kafka_connected=False,  # Not connected yet
            api_reachable=api_reachable,
        )

        # Start producer first (uses producer_config for local Kafka)
        self.producer = BaseKafkaProducer(self.producer_config)
        await self.producer.start()

        # Initialize retry handler (requires producer to be started)
        self.retry_handler = EnrichmentRetryHandler(
            config=self.consumer_config,
            producer=self.producer,
        )
        logger.info(
            "Retry handler initialized",
            extra={
                "retry_topics": [t for t in self.topics if "retry" in t],
                "dlq_topic": self.retry_handler.dlq_topic,
            },
        )

        # Create and start consumer with message handler (uses consumer_config)
        # Consumes from pending topic + all retry topics
        self.consumer = BaseKafkaConsumer(
            config=self.consumer_config,
            topics=self.topics,
            group_id=self.consumer_group,
            message_handler=self._handle_enrichment_task,
            max_batches=self.consumer_config.consumer_max_batches,
        )

        # Update readiness: Kafka will be connected when consumer starts
        self.health_server.set_ready(
            kafka_connected=True,
            api_reachable=api_reachable,
            circuit_open=self.api_client.is_circuit_open(),
        )

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the ClaimX enrichment worker.

        Processes any remaining batch, waits for pending background tasks
        (with timeout), then gracefully shuts down consumer, producer, and
        API client.
        """
        logger.info("Stopping ClaimXEnrichmentWorker")

        # Process any remaining batch
        async with self._batch_lock:
            if self._batch:
                logger.info(
                    "Processing final batch before shutdown",
                    extra={"batch_size": len(self._batch)},
                )
                await self._process_batch(self._batch.copy())
                self._batch.clear()

        # Cancel batch timer if running
        if self._batch_timer and not self._batch_timer.done():
            self._batch_timer.cancel()
            try:
                await self._batch_timer
            except asyncio.CancelledError:
                pass

        # Wait for pending background tasks with timeout
        await self._wait_for_pending_tasks(timeout_seconds=30)

        # Stop consumer first (stops receiving new messages)
        if self.consumer:
            await self.consumer.stop()

        # Then stop producer (flushes pending messages)
        if self.producer:
            await self.producer.stop()

        # Close API client
        if self.api_client:
            await self.api_client.close()

        # Stop health check server
        await self.health_server.stop()

        logger.info("ClaimXEnrichmentWorker stopped successfully")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> asyncio.Task:
        """
        Create a background task with tracking and lifecycle logging.

        Args:
            coro: Coroutine to run as a task
            task_name: Descriptive name for the task
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
            """Callback when task completes."""
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

    async def _wait_for_pending_tasks(self, timeout_seconds: float = 30) -> None:
        """
        Wait for pending background tasks to complete with timeout.

        Args:
            timeout_seconds: Maximum time to wait for tasks
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

        tasks_to_wait = list(self._pending_tasks)

        try:
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
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

                await asyncio.gather(*pending, return_exceptions=True)

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

    async def _handle_enrichment_task(self, record: ConsumerRecord) -> None:
        """
        Process a single enrichment task message from Kafka.

        Accumulates tasks into a batch and processes when batch is full
        or timeout expires.

        Args:
            record: ConsumerRecord containing ClaimXEnrichmentTask JSON
        """
        # Decode and parse ClaimXEnrichmentTask
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            task = ClaimXEnrichmentTask.model_validate(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimXEnrichmentTask",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        logger.debug(
            "Received enrichment task",
            extra={
                "event_id": task.event_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
            },
        )

        # Add to batch
        async with self._batch_lock:
            self._batch.append(task)

            # Start batch timer if this is the first task
            if len(self._batch) == 1 and not (
                self._batch_timer and not self._batch_timer.done()
            ):
                self._batch_timer = self._create_tracked_task(
                    self._batch_timeout(),
                    task_name="batch_timeout",
                )

            # Process batch if full
            if len(self._batch) >= self.batch_size:
                batch_to_process = self._batch.copy()
                self._batch.clear()

                # Cancel timer
                if self._batch_timer and not self._batch_timer.done():
                    self._batch_timer.cancel()
                    try:
                        await self._batch_timer
                    except asyncio.CancelledError:
                        pass

                # Process batch
                await self._process_batch(batch_to_process)

    async def _batch_timeout(self) -> None:
        """
        Timer for batch processing timeout.

        Processes the current batch when timeout expires.
        """
        try:
            await asyncio.sleep(self.batch_timeout_seconds)

            async with self._batch_lock:
                if self._batch:
                    batch_to_process = self._batch.copy()
                    self._batch.clear()

                    logger.debug(
                        "Batch timeout expired, processing batch",
                        extra={"batch_size": len(batch_to_process)},
                    )

                    await self._process_batch(batch_to_process)

        except asyncio.CancelledError:
            logger.debug("Batch timeout cancelled")
            raise

    async def _ensure_projects_exist(
        self,
        project_ids: List[str],
    ) -> None:
        """
        Pre-flight check: Ensure all projects exist in Delta table.

        For projects not in the Delta table, fetches them from the API
        and writes them to claimx_projects before processing the batch.

        This prevents referential integrity issues when writing child entities
        (contacts, media, tasks) that reference project_id.

        Args:
            project_ids: List of project IDs that need to exist

        Note:
            Failures are non-fatal - missing projects will be handled during
            event processing via ProjectHandler.
        """
        if not project_ids or not self.enable_delta_writes:
            return

        unique_project_ids = list(set(project_ids))

        logger.debug(
            "Pre-flight: Checking project existence",
            extra={
                "project_count": len(unique_project_ids),
                "project_ids": unique_project_ids[:10],  # Sample
            },
        )

        try:
            # Import here to avoid circular dependencies and ensure optional dependency
            import polars as pl
            from deltalake import DeltaTable

            # Query existing projects from Delta table
            projects_writer = self.entity_writer._writers.get("projects")
            if not projects_writer:
                logger.warning("No projects writer available, skipping pre-flight check")
                return

            table_path = projects_writer.table_path

            # Check if table exists
            try:
                dt = DeltaTable(table_path)
                df = dt.to_pyarrow_dataset().to_table().to_pandas()
                existing_project_ids = set(df["project_id"].astype(str).unique())
            except Exception as e:
                # Table might not exist yet - that's OK
                logger.debug(
                    "Projects table not yet initialized",
                    extra={"table_path": table_path, "error": str(e)[:100]},
                )
                existing_project_ids = set()

            # Find missing projects
            missing_project_ids = [
                pid for pid in unique_project_ids if pid not in existing_project_ids
            ]

            if not missing_project_ids:
                logger.debug("Pre-flight: All projects exist in Delta table")
                return

            logger.info(
                "Pre-flight: Fetching missing projects from API",
                extra={
                    "missing_count": len(missing_project_ids),
                    "missing_ids": missing_project_ids[:10],  # Sample
                },
            )

            # Fetch missing projects from API
            from kafka_pipeline.claimx.handlers.transformers import ProjectTransformer

            project_rows = []
            api_errors = 0

            for project_id in missing_project_ids:
                try:
                    response = await self.api_client.get_project(int(project_id))

                    # Transform to project row
                    project_row = ProjectTransformer.to_project_row(
                        response,
                        source_event_id="pre-flight-check",
                    )

                    if project_row.get("project_id") is not None:
                        project_rows.append(project_row)

                except ClaimXApiError as e:
                    api_errors += 1
                    logger.warning(
                        "Pre-flight: Failed to fetch project",
                        extra={
                            "project_id": project_id,
                            "error": str(e)[:100],
                            "status_code": e.status_code,
                        },
                    )
                    # Continue with other projects
                except Exception as e:
                    api_errors += 1
                    logger.warning(
                        "Pre-flight: Unexpected error fetching project",
                        extra={
                            "project_id": project_id,
                            "error": str(e)[:100],
                        },
                    )
                    # Continue with other projects

            # Write fetched projects to Delta
            if project_rows:
                from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage

                entity_rows = EntityRowsMessage(projects=project_rows)
                counts = await self.entity_writer.write_all(entity_rows)

                logger.info(
                    "Pre-flight: Wrote missing projects to Delta",
                    extra={
                        "projects_written": counts.get("projects", 0),
                        "api_errors": api_errors,
                    },
                )

            elif api_errors > 0:
                logger.warning(
                    "Pre-flight: Could not fetch any missing projects",
                    extra={"api_errors": api_errors},
                )

        except Exception as e:
            # Pre-flight errors are non-fatal
            logger.error(
                "Pre-flight check failed",
                extra={
                    "error": str(e)[:200],
                    "project_count": len(project_ids),
                },
                exc_info=True,
            )

    async def _process_batch(self, tasks: List[ClaimXEnrichmentTask]) -> None:
        """
        Process a batch of enrichment tasks.

        Routes tasks to handlers, collects entity rows, writes to Delta,
        and produces download tasks.

        Args:
            tasks: List of enrichment tasks to process
        """
        if not tasks:
            return

        logger.info(
            "Processing enrichment batch",
            extra={"batch_size": len(tasks)},
        )

        start_time = datetime.now(timezone.utc)

        # Pre-flight check: Ensure all referenced projects exist in Delta table
        # This minimizes API calls (batch check vs per-event) and prevents
        # referential integrity issues when writing child entities
        project_ids = [task.project_id for task in tasks if task.project_id]
        if project_ids:
            await self._ensure_projects_exist(project_ids)

        # Convert tasks to events for handler processing
        events = [
            ClaimXEventMessage(
                event_id=task.event_id,
                event_type=task.event_type,
                project_id=task.project_id,
                media_id=task.media_id,
                task_assignment_id=task.task_assignment_id,
                video_collaboration_id=task.video_collaboration_id,
                master_file_name=task.master_file_name,
                event_date=task.created_at.isoformat(),
            )
            for task in tasks
        ]

        # Group events by handler type
        handler_groups = self.handler_registry.group_events_by_handler(events)

        if not handler_groups:
            logger.warning(
                "No handlers found for any events in batch",
                extra={"batch_size": len(events)},
            )
            return

        # Process each handler group
        all_entity_rows = EntityRowsMessage()
        all_download_tasks: List[ClaimXDownloadTask] = []
        total_api_calls = 0

        for handler_class, handler_events in handler_groups.items():
            logger.info(
                "Processing handler group",
                extra={
                    "handler": handler_class.__name__,
                    "event_count": len(handler_events),
                },
            )

            # Create handler instance
            handler = handler_class(self.api_client)

            # Process events with handler - wrap with error handling
            try:
                handler_result = await handler.process(handler_events)

                # Collect entity rows
                all_entity_rows.merge(handler_result.rows)
                total_api_calls += handler_result.api_calls

                # Generate download tasks from media rows
                if handler_result.rows.media:
                    download_tasks = self._create_download_tasks_from_media(
                        handler_result.rows.media
                    )
                    all_download_tasks.extend(download_tasks)

            except ClaimXApiError as e:
                # API error with category classification
                logger.error(
                    "Handler failed with API error",
                    extra={
                        "handler": handler_class.__name__,
                        "event_count": len(handler_events),
                        "error_category": e.category.value,
                        "error": str(e)[:200],
                    },
                    exc_info=True,
                )
                # Route each failed event to retry/DLQ
                for event in handler_events:
                    # Find original task for this event
                    task = self._find_task_for_event(event, tasks)
                    if task:
                        await self._handle_enrichment_failure(task, e, e.category)

            except Exception as e:
                # Unknown error - classify and route
                # Classify as UNKNOWN for conservative retry
                error_category = ErrorCategory.UNKNOWN
                logger.error(
                    "Handler failed with unexpected error",
                    extra={
                        "handler": handler_class.__name__,
                        "event_count": len(handler_events),
                        "error_type": type(e).__name__,
                        "error": str(e)[:200],
                    },
                    exc_info=True,
                )
                # Route each failed event to retry/DLQ
                for event in handler_events:
                    # Find original task for this event
                    task = self._find_task_for_event(event, tasks)
                    if task:
                        await self._handle_enrichment_failure(task, e, error_category)

        # Write entity rows to Delta tables (non-blocking)
        # Pass tasks list to enable retry routing on Delta failures
        if self.enable_delta_writes and not all_entity_rows.is_empty():
            self._create_tracked_task(
                self._write_entities_to_delta(all_entity_rows, tasks),
                task_name="delta_write",
                context={"row_count": all_entity_rows.row_count()},
            )

        # Produce download tasks
        if all_download_tasks:
            await self._produce_download_tasks(all_download_tasks)

        # Log batch summary
        elapsed_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000

        logger.info(
            "Enrichment batch complete",
            extra={
                "batch_size": len(tasks),
                "entity_rows": all_entity_rows.row_count(),
                "download_tasks": len(all_download_tasks),
                "api_calls": total_api_calls,
                "duration_ms": round(elapsed_ms, 2),
            },
        )

    def _create_download_tasks_from_media(
        self,
        media_rows: List[Dict[str, Any]],
    ) -> List[ClaimXDownloadTask]:
        """
        Create download tasks from media entity rows.

        Args:
            media_rows: List of media entity row dicts

        Returns:
            List of ClaimXDownloadTask instances
        """
        download_tasks = []

        for media_row in media_rows:
            download_url = media_row.get("full_download_link")
            if not download_url:
                logger.debug(
                    "Skipping media row without download URL",
                    extra={
                        "media_id": media_row.get("media_id"),
                        "project_id": media_row.get("project_id"),
                    },
                )
                continue

            # Create download task
            task = ClaimXDownloadTask(
                media_id=str(media_row.get("media_id", "")),
                project_id=str(media_row.get("project_id", "")),
                download_url=download_url,
                blob_path=self._generate_blob_path(media_row),
                file_type=media_row.get("file_type", ""),
                file_name=media_row.get("file_name", ""),
                source_event_id=media_row.get("source_event_id", ""),
                retry_count=0,
                expires_at=media_row.get("expires_at"),
                refresh_count=0,
            )
            download_tasks.append(task)

        logger.debug(
            "Created download tasks from media rows",
            extra={
                "media_rows": len(media_rows),
                "download_tasks": len(download_tasks),
            },
        )

        return download_tasks

    def _generate_blob_path(self, media_row: Dict[str, Any]) -> str:
        """
        Generate blob storage path for media file.

        Args:
            media_row: Media entity row dict

        Returns:
            Blob path string
        """
        project_id = media_row.get("project_id", "unknown")
        media_id = media_row.get("media_id", "unknown")
        file_name = media_row.get("file_name", f"media_{media_id}")

        # Format: claimx/{project_id}/media/{file_name}
        return f"claimx/{project_id}/media/{file_name}"

    async def _write_entities_to_delta(
        self,
        entity_rows: EntityRowsMessage,
        tasks: List[ClaimXEnrichmentTask],
    ) -> None:
        """
        Write entity rows to Delta Lake tables (background task).

        On failure, routes all tasks in the batch to retry/DLQ since entity rows
        are aggregated from multiple tasks.

        Args:
            entity_rows: EntityRowsMessage with rows for all tables
            tasks: Original enrichment tasks that produced these entity rows
        """
        try:
            counts = await self.entity_writer.write_all(entity_rows)

            # Record metrics for each table
            for table_name, row_count in counts.items():
                record_delta_write(
                    table=f"claimx_{table_name}",
                    event_count=row_count,
                    success=True,
                )

            logger.info(
                "Entity tables write complete",
                extra={
                    "tables_written": counts,
                    "total_rows": sum(counts.values()),
                },
            )

        except Exception as e:
            logger.error(
                "Error writing entities to Delta - routing batch to retry",
                extra={
                    "row_count": entity_rows.row_count(),
                    "task_count": len(tasks),
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

            # Record failure metrics
            record_delta_write(
                table="claimx_entities",
                event_count=entity_rows.row_count(),
                success=False,
            )

            # Delta write failures are typically transient (connection issues)
            # Route all tasks in batch to retry
            error_category = ErrorCategory.TRANSIENT
            for task in tasks:
                await self._handle_enrichment_failure(task, e, error_category)

    async def _produce_download_tasks(
        self,
        download_tasks: List[ClaimXDownloadTask],
    ) -> None:
        """
        Produce download tasks to Kafka topic.

        Args:
            download_tasks: List of ClaimXDownloadTask to produce
        """
        logger.info(
            "Producing download tasks",
            extra={"task_count": len(download_tasks)},
        )

        for task in download_tasks:
            try:
                metadata = await self.producer.send(
                    topic=self.download_topic,
                    key=task.media_id,
                    value=task,
                    headers={"event_id": task.source_event_id},
                )

                logger.debug(
                    "Produced download task",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to produce download task",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # Continue processing other tasks

    def _get_retry_topic(self, retry_level: int) -> str:
        """
        Get retry topic name for a specific retry level.

        Args:
            retry_level: Retry attempt number (0-indexed)

        Returns:
            Retry topic name (e.g., "claimx.enrichment.pending.retry.5m")

        Raises:
            ValueError: If retry_level exceeds configured retry delays
        """
        if retry_level >= len(self.consumer_config.retry_delays):
            raise ValueError(
                f"Retry level {retry_level} exceeds max retries "
                f"({len(self.consumer_config.retry_delays)})"
            )

        delay_seconds = self.consumer_config.retry_delays[retry_level]
        delay_minutes = delay_seconds // 60
        return f"{self.enrichment_topic}.retry.{delay_minutes}m"

    def _find_task_for_event(
        self,
        event: ClaimXEventMessage,
        tasks: List[ClaimXEnrichmentTask],
    ) -> Optional[ClaimXEnrichmentTask]:
        """
        Find original enrichment task for an event.

        Maps event back to its source task to preserve retry_count and metadata.

        Args:
            event: Event message processed by handler
            tasks: Original batch of enrichment tasks

        Returns:
            Original enrichment task, or None if not found
        """
        for task in tasks:
            if task.event_id == event.event_id:
                return task

        logger.error(
            "Could not find task for event",
            extra={"event_id": event.event_id, "event_type": event.event_type},
        )
        return None

    async def _handle_enrichment_failure(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: "ErrorCategory",
    ) -> None:
        """
        Handle failed enrichment task: route to retry topic or DLQ.

        Routes failures through retry handler which sends to:
        - Retry topic with exponential backoff (TRANSIENT, AUTH, CIRCUIT_OPEN, UNKNOWN)
        - DLQ immediately (PERMANENT errors)
        - DLQ after exhausting retries (max_retries reached)

        Args:
            task: Enrichment task that failed
            error: Exception that caused failure
            error_category: Classification of the error (TRANSIENT, PERMANENT, etc.)
        """
        assert self.retry_handler is not None, "Retry handler not initialized"

        logger.warning(
            "Enrichment task failed",
            extra={
                "event_id": task.event_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "error_category": error_category.value,
                "retry_count": task.retry_count,
                "error": str(error)[:200],
            },
        )

        # Route through retry handler
        await self.retry_handler.handle_failure(
            task=task,
            error=error,
            error_category=error_category,
        )


__all__ = ["ClaimXEnrichmentWorker"]
