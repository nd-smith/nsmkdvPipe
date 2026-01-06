"""
Entry point for running Kafka pipeline workers.

Usage:
    # Run all workers
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-event-ingester
    python -m kafka_pipeline --worker xact-local-ingester
    python -m kafka_pipeline --worker xact-delta-writer
    python -m kafka_pipeline --worker xact-delta-retry
    python -m kafka_pipeline --worker xact-download
    python -m kafka_pipeline --worker xact-upload
    python -m kafka_pipeline --worker xact-result-processor

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-poller
    python -m kafka_pipeline --worker claimx-ingester
    python -m kafka_pipeline --worker claimx-enricher
    python -m kafka_pipeline --worker claimx-downloader
    python -m kafka_pipeline --worker claimx-uploader
    python -m kafka_pipeline --worker claimx-result-processor

    # Run multiple worker instances (for horizontal scaling)
    python -m kafka_pipeline --worker xact-download --count 4
    python -m kafka_pipeline --worker xact-upload -c 3

    # Run with metrics server
    python -m kafka_pipeline --metrics-port 8000

    # Run in development mode (local Kafka only, no Event Hub/Eventhouse)
    python -m kafka_pipeline --dev

Event Source Configuration:
    Set EVENT_SOURCE environment variable:
    - eventhub (default): Use Azure Event Hub via Kafka protocol
    - eventhouse: Poll Microsoft Fabric Eventhouse

Architecture:
    xact Domain:
        Event Source → events.raw topic
            → xact-event-ingester → downloads.pending → download worker →
              downloads.cached → upload worker → downloads.results → result processor
            → xact-delta-writer → xact_events Delta table (parallel)
              ↓ (on failure)
            → delta-events.retry.* topics → xact-delta-retry → retry write or DLQ

    claimx Domain:
        Eventhouse → claimx-poller → claimx.events.raw → claimx-ingester →
        enrichment.pending → enrichment worker → entity tables + downloads.pending →
        download worker → downloads.cached → upload worker → downloads.results
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

from prometheus_client import start_http_server

from core.logging.context import set_log_context
from core.logging.setup import get_logger, setup_logging, setup_multi_worker_logging

# Worker stages for multi-worker logging
WORKER_STAGES = [
    "eventhouse-poller",
    "xact-event-ingester", "xact-local-ingester", "xact-delta-writer", "xact-delta-retry", "xact-download", "xact-upload", "xact-result-processor",
    "claimx-poller", "claimx-ingester", "claimx-enricher", "claimx-downloader", "claimx-uploader", "claimx-result-processor"
]

# Placeholder logger until setup_logging() is called in main()
# This allows module-level logging before full initialization
logger = logging.getLogger(__name__)

# Global shutdown event for graceful batch completion
# Set by signal handlers, checked by workers to finish current batch before exiting
_shutdown_event: Optional[asyncio.Event] = None


def get_shutdown_event() -> asyncio.Event:
    """Get or create the global shutdown event."""
    global _shutdown_event
    if _shutdown_event is None:
        _shutdown_event = asyncio.Event()
    return _shutdown_event


async def run_worker_pool(
    worker_fn: Callable[..., Coroutine[Any, Any, None]],
    count: int,
    worker_name: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Run multiple instances of a worker concurrently.

    Creates N asyncio tasks for the same worker function. Each instance
    joins the same Kafka consumer group, so partitions are automatically
    distributed across instances.

    Args:
        worker_fn: Async worker function to run (e.g., run_download_worker)
        count: Number of worker instances to launch
        worker_name: Base name for the worker (used in task naming)
        *args: Positional arguments to pass to worker_fn
        **kwargs: Keyword arguments to pass to worker_fn
    """
    logger.info(f"Starting {count} instances of {worker_name}...")

    tasks = []
    for i in range(count):
        task = asyncio.create_task(
            worker_fn(*args, **kwargs),
            name=f"{worker_name}-{i}",
        )
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info(f"Worker pool {worker_name} cancelled, shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Kafka pipeline workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all workers (Event Hub → Local Kafka pipeline)
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-download

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-enricher

    # Run in development mode (local Kafka only)
    python -m kafka_pipeline --dev

    # Run with custom metrics port
    python -m kafka_pipeline --metrics-port 9090
        """,
    )

    parser.add_argument(
        "--worker",
        choices=[
            "xact-event-ingester", "xact-local-ingester", "xact-delta-writer", "xact-delta-retry", "xact-download", "xact-upload", "xact-result-processor",
            "claimx-poller", "claimx-ingester", "claimx-enricher", "claimx-downloader", "claimx-uploader", "claimx-result-processor",
            "all"
        ],
        default="all",
        help="Which worker(s) to run (default: all)",
    )

    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8000,
        help="Port for Prometheus metrics server (default: 8000)",
    )

    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: use local Kafka only (no Event Hub/Eventhouse credentials required)",
    )

    parser.add_argument(
        "--no-delta",
        action="store_true",
        help="Disable Delta Lake writes (for testing)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help="Log directory path (default: from LOG_DIR env var or ./logs)",
    )

    parser.add_argument(
        "--count", "-c",
        type=int,
        default=1,
        help="Number of worker instances to run concurrently (default: 1). "
             "Multiple instances share the same consumer group for automatic partition distribution.",
    )

    return parser.parse_args()


async def run_event_ingester(
    eventhub_config,
    local_kafka_config,
    domain: str = "xact",
):
    """Run the Event Ingester worker.

    Reads events from Event Hub and produces download tasks to local Kafka.
    Uses separate configs: eventhub_config for consumer, local_kafka_config for producer.

    Note: Delta Lake writes are handled by a separate DeltaEventsWorker.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    set_log_context(stage="xact-event-ingester")
    logger.info("Starting xact Event Ingester worker...")

    worker = EventIngesterWorker(
        config=eventhub_config,
        domain=domain,
        producer_config=local_kafka_config,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping event ingester...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_eventhouse_poller(pipeline_config):
    """Run the Eventhouse Poller.

    Polls Microsoft Fabric Eventhouse for events and produces download tasks to local Kafka.

    Supports graceful shutdown: when shutdown event is set, the poller
    finishes its current poll cycle before exiting.

    Note: Deduplication handled by daily Fabric maintenance job.
    """
    from kafka_pipeline.common.eventhouse.kql_client import EventhouseConfig
    from kafka_pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig

    set_log_context(stage="eventhouse-poller")
    logger.info("Starting Eventhouse Poller...")

    eventhouse_source = pipeline_config.eventhouse
    if not eventhouse_source:
        raise ValueError("Eventhouse configuration required for EVENT_SOURCE=eventhouse")

    # Build Eventhouse config
    eventhouse_config = EventhouseConfig(
        cluster_url=eventhouse_source.cluster_url,
        database=eventhouse_source.database,
        query_timeout_seconds=eventhouse_source.query_timeout_seconds,
    )

    # Build poller config
    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=pipeline_config.local_kafka.to_kafka_config(),
        poll_interval_seconds=eventhouse_source.poll_interval_seconds,
        batch_size=eventhouse_source.batch_size,
        source_table=eventhouse_source.source_table,
        events_table_path=eventhouse_source.xact_events_table_path,
        backfill_start_stamp=eventhouse_source.backfill_start_stamp,
        backfill_stop_stamp=eventhouse_source.backfill_stop_stamp,
        bulk_backfill=eventhouse_source.bulk_backfill,
    )

    shutdown_event = get_shutdown_event()

    async def shutdown_watcher(poller: KQLEventPoller):
        """Wait for shutdown signal and stop poller gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping eventhouse poller...")
        await poller.stop()

    async with KQLEventPoller(poller_config) as poller:
        # Start shutdown watcher alongside poller
        watcher_task = asyncio.create_task(shutdown_watcher(poller))

        try:
            await poller.run()
        finally:
            # Guard against event loop being closed during shutdown
            try:
                watcher_task.cancel()
                await watcher_task
            except (asyncio.CancelledError, RuntimeError):
                # RuntimeError occurs if event loop is closed
                pass


async def run_delta_events_worker(kafka_config, events_table_path: str):
    """Run the Delta Events Worker.

    Consumes events from the events.raw topic and writes them to the
    xact_events Delta table. Runs independently of EventIngesterWorker
    with its own consumer group.

    Supports graceful shutdown: when shutdown event is set, the worker
    waits for pending Delta writes before exiting.
    """
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.xact.workers.delta_events_worker import DeltaEventsWorker

    set_log_context(stage="xact-delta-writer")
    logger.info("Starting xact Delta Events worker...")

    # Create producer for retry topic routing
    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="xact",
        worker_name="delta_events_writer",
    )
    await producer.start()

    worker = DeltaEventsWorker(
        config=kafka_config,
        producer=producer,
        events_table_path=events_table_path,
        domain="xact",
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping delta events worker...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()
        await producer.stop()


async def run_delta_retry_scheduler(kafka_config, events_table_path: str):
    """Run the Delta Batch Retry Scheduler.

    Consumes failed batches from retry topics and attempts to write them to
    the xact_events Delta table after the configured delay has elapsed.

    Routes permanently failed batches (retries exhausted) to DLQ.

    Supports graceful shutdown: when shutdown event is set, the scheduler
    stops consuming from retry topics.
    """
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.xact.retry.scheduler import DeltaBatchRetryScheduler

    set_log_context(stage="xact-delta-retry")
    logger.info("Starting xact Delta Retry Scheduler...")

    # Create producer for DLQ routing
    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="xact",
        worker_name="delta_retry_scheduler",
    )
    await producer.start()

    scheduler = DeltaBatchRetryScheduler(
        config=kafka_config,
        producer=producer,
        table_path=events_table_path,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop scheduler gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping delta retry scheduler...")
        await scheduler.stop()

    # Start shutdown watcher alongside scheduler
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await scheduler.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after scheduler exits
        await scheduler.stop()
        await producer.stop()


async def run_download_worker(kafka_config):
    """Run the Download Worker.

    Reads download tasks from local Kafka, downloads files to local cache,
    and produces CachedDownloadMessage for the upload worker.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.xact.workers.download_worker import DownloadWorker

    set_log_context(stage="xact-download")
    logger.info("Starting xact Download worker...")

    worker = DownloadWorker(config=kafka_config, domain="xact")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping download worker after current batch...")
        await worker.request_shutdown()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_upload_worker(kafka_config):
    """Run the Upload Worker.

    Reads cached downloads from local Kafka, uploads files to OneLake,
    and produces DownloadResultMessage.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.xact.workers.upload_worker import UploadWorker

    set_log_context(stage="xact-upload")
    logger.info("Starting xact Upload worker...")

    worker = UploadWorker(config=kafka_config, domain="xact")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping upload worker after current batch...")
        await worker.request_shutdown()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_result_processor(kafka_config, enable_delta_writes: bool = True):
    """Run the Result Processor worker.

    Reads download results from local Kafka and writes to Delta Lake tables:
    - xact_attachments: successful downloads
    - xact_attachments_failed: permanent failures (optional)

    Supports graceful shutdown: when shutdown event is set, the worker
    flushes pending batches before exiting.

    On Delta write failure, batches are routed to retry topics for reprocessing.
    """
    from kafka_pipeline.common.producer import BaseKafkaProducer
    from kafka_pipeline.xact.workers.result_processor import ResultProcessor

    set_log_context(stage="xact-result-processor")
    logger.info("Starting xact Result Processor worker...")

    # Get table paths from environment
    inventory_table_path = os.getenv("DELTA_INVENTORY_TABLE_PATH", "")
    failed_table_path = os.getenv("DELTA_FAILED_TABLE_PATH", "")

    # Create and start producer for retry topic routing
    producer = BaseKafkaProducer(
        config=kafka_config,
        domain="xact",
        worker_name="result_processor",
    )
    await producer.start()

    worker = ResultProcessor(
        config=kafka_config,
        producer=producer,
        inventory_table_path=inventory_table_path if enable_delta_writes else None,
        failed_table_path=failed_table_path if enable_delta_writes and failed_table_path else None,
        batch_size=100,
        batch_timeout_seconds=5.0,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping result processor...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()
        await producer.stop()


async def run_claimx_eventhouse_poller(pipeline_config):
    """Run the Eventhouse Poller for claimx domain.

    Polls Microsoft Fabric Eventhouse for claimx events and produces to claimx.events.raw topic.

    Supports graceful shutdown: when shutdown event is set, the poller
    finishes its current poll cycle before exiting.

    Note: Deduplication handled by daily Fabric maintenance job.
    """
    from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
    from kafka_pipeline.common.eventhouse.kql_client import EventhouseConfig
    from kafka_pipeline.common.eventhouse.poller import KQLEventPoller, PollerConfig

    set_log_context(stage="claimx-poller")
    logger.info("Starting ClaimX Eventhouse Poller...")

    claimx_eventhouse = pipeline_config.claimx_eventhouse
    if not claimx_eventhouse:
        raise ValueError(
            "ClaimX Eventhouse configuration required for claimx-poller worker. "
            "Set in config.yaml under 'claimx_eventhouse:' or via CLAIMX_EVENTHOUSE_* env vars."
        )

    # Build Eventhouse config from pipeline config
    eventhouse_config = EventhouseConfig(
        cluster_url=claimx_eventhouse.cluster_url,
        database=claimx_eventhouse.database,
        query_timeout_seconds=claimx_eventhouse.query_timeout_seconds,
    )

    # Create a modified kafka config with claimx events topic
    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()
    claimx_kafka_config = local_kafka_config
    # Update the claimx domain's events topic from eventhouse config
    if "claimx" not in claimx_kafka_config.claimx or not claimx_kafka_config.claimx:
        claimx_kafka_config.claimx = {"topics": {}}
    if "topics" not in claimx_kafka_config.claimx:
        claimx_kafka_config.claimx["topics"] = {}
    claimx_kafka_config.claimx["topics"]["events"] = claimx_eventhouse.events_topic

    # Build poller config with ClaimXEventMessage schema
    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=claimx_kafka_config,
        event_schema_class=ClaimXEventMessage,
        domain="claimx",
        poll_interval_seconds=claimx_eventhouse.poll_interval_seconds,
        batch_size=claimx_eventhouse.batch_size,
        source_table=claimx_eventhouse.source_table,
        events_table_path=claimx_eventhouse.claimx_events_table_path,
        backfill_start_stamp=claimx_eventhouse.backfill_start_stamp,
        backfill_stop_stamp=claimx_eventhouse.backfill_stop_stamp,
        bulk_backfill=claimx_eventhouse.bulk_backfill,
    )

    shutdown_event = get_shutdown_event()

    async def shutdown_watcher(poller: KQLEventPoller):
        """Wait for shutdown signal and stop poller gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx eventhouse poller...")
        await poller.stop()

    async with KQLEventPoller(poller_config) as poller:
        # Start shutdown watcher alongside poller
        watcher_task = asyncio.create_task(shutdown_watcher(poller))

        try:
            await poller.run()
        finally:
            # Guard against event loop being closed during shutdown
            try:
                watcher_task.cancel()
                await watcher_task
            except (asyncio.CancelledError, RuntimeError):
                # RuntimeError occurs if event loop is closed
                pass


async def run_local_event_ingester(
    local_kafka_config,
    domain: str = "xact",
):
    """Run EventIngester consuming from local Kafka events.raw topic.

    Used in Eventhouse mode where the poller publishes to events.raw
    and the ingester processes events to downloads.pending.

    Note: Delta Lake writes are handled by a separate DeltaEventsWorker.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    set_log_context(stage="xact-event-ingester")
    logger.info("Starting xact Event Ingester (local Kafka mode)...")

    worker = EventIngesterWorker(
        config=local_kafka_config,
        domain=domain,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping event ingester...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_claimx_event_ingester(
    kafka_config,
    enable_delta_writes: bool = True,
    events_table_path: str = "",
):
    """Run the ClaimX Event Ingester worker.

    Reads ClaimX events from events topic and produces enrichment tasks to local Kafka.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker

    set_log_context(stage="claimx-ingester")
    logger.info("Starting ClaimX Event Ingester worker...")

    worker = ClaimXEventIngesterWorker(
        config=kafka_config,
        domain="claimx",
        enable_delta_writes=enable_delta_writes,
        events_table_path=events_table_path,
    )
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx event ingester...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_claimx_enrichment_worker(kafka_config):
    """Run the ClaimX Enrichment Worker.

    Reads enrichment tasks from local Kafka, calls ClaimX API to fetch entity data,
    writes to Delta Lake entity tables, and produces download tasks for media files.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker

    set_log_context(stage="claimx-enricher")
    logger.info("Starting ClaimX Enrichment worker...")

    worker = ClaimXEnrichmentWorker(config=kafka_config, domain="claimx")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping enrichment worker...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_claimx_download_worker(kafka_config):
    """Run the ClaimX Download Worker.

    Reads download tasks from local Kafka, downloads files from presigned URLs
    to local cache, and produces CachedDownloadMessage for the upload worker.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.claimx.workers.download_worker import ClaimXDownloadWorker

    set_log_context(stage="claimx-downloader")
    logger.info("Starting ClaimX Download worker...")

    worker = ClaimXDownloadWorker(config=kafka_config, domain="claimx")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx download worker...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_claimx_upload_worker(kafka_config):
    """Run the ClaimX Upload Worker.

    Reads cached downloads from local Kafka, uploads files to OneLake,
    and produces UploadResultMessage.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

    set_log_context(stage="claimx-uploader")
    logger.info("Starting ClaimX Upload worker...")

    worker = ClaimXUploadWorker(config=kafka_config, domain="claimx")
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop worker gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx upload worker...")
        await worker.stop()

    # Start shutdown watcher alongside worker
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await worker.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_claimx_result_processor(kafka_config):
    """Run ClaimX result processor.

    Consumes upload result messages from local Kafka broker,
    logs outcomes, emits metrics, and tracks success/failure rates.

    The worker handles Kafka consumer lifecycle and graceful shutdown.
    """
    from kafka_pipeline.claimx.workers.result_processor import ClaimXResultProcessor

    set_log_context(stage="claimx-result-processor")
    logger.info("Starting ClaimX Result Processor...")

    processor = ClaimXResultProcessor(config=kafka_config)
    shutdown_event = get_shutdown_event()

    async def shutdown_watcher():
        """Wait for shutdown signal and stop processor gracefully."""
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping claimx result processor...")
        await processor.stop()

    # Start shutdown watcher alongside processor
    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await processor.start()
    finally:
        # Guard against event loop being closed during shutdown
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            # RuntimeError occurs if event loop is closed
            pass
        # Clean up resources after processor exits
        await processor.stop()


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently.

    Uses the configured event source (Event Hub or Eventhouse) for ingestion.
    Both modes use the same flow:
        events.raw → EventIngester → downloads.pending → DownloadWorker → ...
        events.raw → DeltaEventsWorker → Delta table (parallel)
    """
    from kafka_pipeline.pipeline_config import EventSourceType

    logger.info("Starting all pipeline workers...")

    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

    # Create tasks list
    tasks = []

    # Get events table path for delta writer
    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        events_table_path = (
            pipeline_config.eventhouse.xact_events_table_path
            or pipeline_config.events_table_path
        )
    else:
        events_table_path = pipeline_config.events_table_path

    # Create event source task based on configuration
    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        # Eventhouse mode: Poller → events.raw → EventIngester → downloads.pending
        tasks.append(
            asyncio.create_task(
                run_eventhouse_poller(pipeline_config),
                name="eventhouse-poller",
            )
        )
        tasks.append(
            asyncio.create_task(
                run_local_event_ingester(
                    local_kafka_config,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Eventhouse as event source")
    else:
        # EventHub mode: EventHub → events.raw → EventIngester → downloads.pending
        eventhub_config = pipeline_config.eventhub.to_kafka_config()
        tasks.append(
            asyncio.create_task(
                run_event_ingester(
                    eventhub_config,
                    local_kafka_config,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Event Hub as event source")

    # Add Delta events writer if enabled and path is configured
    if enable_delta_writes and events_table_path:
        tasks.append(
            asyncio.create_task(
                run_delta_events_worker(local_kafka_config, events_table_path),
                name="xact-delta-writer",
            )
        )
        logger.info("Delta events writer enabled")

        # Add Delta retry scheduler if Delta writes are enabled
        tasks.append(
            asyncio.create_task(
                run_delta_retry_scheduler(local_kafka_config, events_table_path),
                name="xact-delta-retry",
            )
        )
        logger.info("Delta retry scheduler enabled")

    # Add common workers
    tasks.extend([
        asyncio.create_task(
            run_download_worker(local_kafka_config),
            name="xact-download",
        ),
        asyncio.create_task(
            run_upload_worker(local_kafka_config),
            name="xact-upload",
        ),
        asyncio.create_task(
            run_result_processor(local_kafka_config, enable_delta_writes),
            name="xact-result-processor",
        ),
    ])

    # Wait for all tasks (they run indefinitely until stopped)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Workers cancelled, shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    """Set up signal handlers for graceful shutdown.

    Shutdown Behavior:
    - First CTRL+C (SIGINT/SIGTERM): Sets the global shutdown event.
      All workers receive this signal and initiate graceful shutdown:
      - Finish processing current batch/message
      - Flush pending data to Delta Lake
      - Commit Kafka offsets
      - Close connections gracefully
    - Second CTRL+C: Forces immediate shutdown by cancelling all tasks.
      Use only if graceful shutdown is stuck.

    Note: Signal handlers are not supported on Windows. On Windows,
    KeyboardInterrupt is used instead, which triggers asyncio.CancelledError.
    """

    def handle_signal(sig):
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
        # Set shutdown event to signal workers to finish current batch and exit
        shutdown_event = get_shutdown_event()
        if not shutdown_event.is_set():
            shutdown_event.set()
        else:
            # Second signal - force immediate shutdown
            logger.warning("Received second signal, forcing immediate shutdown...")
            for task in asyncio.all_tasks(loop):
                task.cancel()

    # Signal handlers are not supported on Windows
    if sys.platform == "win32":
        logger.debug("Signal handlers not supported on Windows, using KeyboardInterrupt")
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))


def main():
    """Main entry point."""
    global logger
    args = parse_args()

    # Determine log configuration
    log_level = getattr(logging, args.log_level)

    # JSON logs: controlled via JSON_LOGS env var (default: true)
    # Set JSON_LOGS=false for human-readable logs during local development
    json_logs = os.getenv("JSON_LOGS", "true").lower() in ("true", "1", "yes")

    # Log directory: CLI arg > env var > default ./logs
    log_dir_str = args.log_dir or os.getenv("LOG_DIR", "logs")
    log_dir = Path(log_dir_str)

    # Worker ID for log context
    worker_id = os.getenv("WORKER_ID", f"kafka-{args.worker}")

    # Determine domain from worker name for separate log directories
    # Extract domain prefix: xact-download -> xact, claimx-enricher -> claimx
    domain = "kafka"  # Default fallback
    if args.worker != "all" and "-" in args.worker:
        domain_prefix = args.worker.split("-")[0]
        if domain_prefix in ("xact", "claimx"):
            domain = domain_prefix

    # Initialize structured logging infrastructure
    if args.worker == "all":
        # Multi-worker mode: create per-worker log files
        # Note: For "all" mode, we use "kafka" as the combined domain
        # Individual workers will still log with domain context from set_log_context()
        setup_multi_worker_logging(
            workers=WORKER_STAGES,
            domain="kafka",
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
        )
    else:
        # Single worker mode: single log file with domain-specific directory
        setup_logging(
            name="kafka_pipeline",
            stage=args.worker,
            domain=domain,
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
            worker_id=worker_id,
        )

    # Re-get logger after setup to use new handlers
    logger = get_logger(__name__)

    # Start Prometheus metrics server
    logger.info(f"Starting metrics server on port {args.metrics_port}")
    start_http_server(args.metrics_port)

    # Load configuration
    # Dev mode bypasses Event Hub/Eventhouse requirements for local testing.
    # Production mode requires either Event Hub or Eventhouse credentials.
    # NOTE: Inline imports below are intentional - lazy loading for conditional code paths
    if args.dev:
        logger.info("Running in DEVELOPMENT mode (local Kafka only)")
        from kafka_pipeline.pipeline_config import (
            EventSourceType,
            LocalKafkaConfig,
            PipelineConfig,
        )

        local_config = LocalKafkaConfig.load_config()
        kafka_config = local_config.to_kafka_config()

        # In dev mode, create a minimal PipelineConfig
        pipeline_config = PipelineConfig(
            event_source=EventSourceType.EVENTHUB,
            local_kafka=local_config,
        )

        # For backwards compatibility with single-worker modes
        eventhub_config = kafka_config
        local_kafka_config = kafka_config
    else:
        # Production mode: Event Hub or Eventhouse + local Kafka
        from kafka_pipeline.pipeline_config import EventSourceType, get_pipeline_config

        try:
            pipeline_config = get_pipeline_config()
            local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                logger.info("Running in PRODUCTION mode (Eventhouse + local Kafka)")
                eventhub_config = None  # Not used for Eventhouse mode
            else:
                logger.info("Running in PRODUCTION mode (Event Hub + local Kafka)")
                eventhub_config = pipeline_config.eventhub.to_kafka_config()
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            logger.error("Use --dev flag for local development without Event Hub/Eventhouse")
            sys.exit(1)

    enable_delta_writes = not args.no_delta

    # Get or create event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Set up signal handlers
    setup_signal_handlers(loop)

    try:
        if args.worker == "xact-event-ingester":
            # Event ingester uses Event Hub or Eventhouse based on config
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                loop.run_until_complete(run_eventhouse_poller(pipeline_config))
            else:
                loop.run_until_complete(
                    run_event_ingester(
                        eventhub_config,
                        local_kafka_config,
                        domain=pipeline_config.domain,
                    )
                )
        elif args.worker == "xact-local-ingester":
            # Local event ingester - consumes events.raw and produces downloads.pending
            # Used after backfill to process events without running the full pipeline
            loop.run_until_complete(
                run_local_event_ingester(
                    local_kafka_config,
                    domain=pipeline_config.domain,
                )
            )
        elif args.worker == "xact-delta-writer":
            # Delta events writer - writes events to Delta table
            events_table_path = pipeline_config.events_table_path
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                events_table_path = (
                    pipeline_config.eventhouse.xact_events_table_path
                    or events_table_path
                )
            if not events_table_path:
                logger.error("DELTA_EVENTS_TABLE_PATH is required for xact-delta-writer")
                sys.exit(1)
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_delta_events_worker, args.count, "xact-delta-writer",
                        local_kafka_config, events_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_delta_events_worker(local_kafka_config, events_table_path)
                )
        elif args.worker == "xact-delta-retry":
            # Delta retry scheduler - retries failed Delta batch writes
            events_table_path = pipeline_config.events_table_path
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                events_table_path = (
                    pipeline_config.eventhouse.xact_events_table_path
                    or events_table_path
                )
            if not events_table_path:
                logger.error("DELTA_EVENTS_TABLE_PATH is required for xact-delta-retry")
                sys.exit(1)
            loop.run_until_complete(
                run_delta_retry_scheduler(local_kafka_config, events_table_path)
            )
        elif args.worker == "xact-download":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_download_worker, args.count, "xact-download",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_download_worker(local_kafka_config))
        elif args.worker == "xact-upload":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_upload_worker, args.count, "xact-upload",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_upload_worker(local_kafka_config))
        elif args.worker == "xact-result-processor":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_result_processor, args.count, "xact-result-processor",
                        local_kafka_config, enable_delta_writes,
                    )
                )
            else:
                loop.run_until_complete(
                    run_result_processor(local_kafka_config, enable_delta_writes)
                )
        elif args.worker == "claimx-poller":
            # ClaimX Eventhouse poller
            loop.run_until_complete(run_claimx_eventhouse_poller(pipeline_config))
        elif args.worker == "claimx-ingester":
            # ClaimX event ingester
            claimx_events_table_path = os.getenv("CLAIMX_EVENTS_TABLE_PATH", "")
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_event_ingester, args.count, "claimx-ingester",
                        local_kafka_config, enable_delta_writes,
                        events_table_path=claimx_events_table_path,
                    )
                )
            else:
                loop.run_until_complete(
                    run_claimx_event_ingester(
                        local_kafka_config,
                        enable_delta_writes,
                        events_table_path=claimx_events_table_path,
                    )
                )
        elif args.worker == "claimx-enricher":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_enrichment_worker, args.count, "claimx-enricher",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_enrichment_worker(local_kafka_config))
        elif args.worker == "claimx-downloader":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_download_worker, args.count, "claimx-downloader",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_download_worker(local_kafka_config))
        elif args.worker == "claimx-uploader":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_upload_worker, args.count, "claimx-uploader",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_upload_worker(local_kafka_config))
        elif args.worker == "claimx-result-processor":
            if args.count > 1:
                loop.run_until_complete(
                    run_worker_pool(
                        run_claimx_result_processor, args.count, "claimx-result-processor",
                        local_kafka_config,
                    )
                )
            else:
                loop.run_until_complete(run_claimx_result_processor(local_kafka_config))
        else:  # all
            loop.run_until_complete(
                run_all_workers(pipeline_config, enable_delta_writes)
            )
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Clean up
        loop.close()
        logger.info("Pipeline shutdown complete")


if __name__ == "__main__":
    main()
