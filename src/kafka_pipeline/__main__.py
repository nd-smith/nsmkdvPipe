"""
Entry point for running Kafka pipeline workers.

Usage:
    # Run all workers
    python -m kafka_pipeline

    # Run specific xact worker
    python -m kafka_pipeline --worker xact-event-ingester
    python -m kafka_pipeline --worker xact-download
    python -m kafka_pipeline --worker xact-upload
    python -m kafka_pipeline --worker xact-result-processor

    # Run specific claimx worker
    python -m kafka_pipeline --worker claimx-ingester
    python -m kafka_pipeline --worker claimx-enricher
    python -m kafka_pipeline --worker claimx-downloader
    python -m kafka_pipeline --worker claimx-uploader

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
        Event Source → xact events → downloads.pending → download worker →
        downloads.cached → upload worker → downloads.results → result processor

    claimx Domain:
        Event Source → claimx events → enrichment.pending → enrichment worker →
        entity tables + downloads.pending → download worker → downloads.cached →
        upload worker → downloads.results
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Optional

from prometheus_client import start_http_server

from core.logging.context import set_log_context
from core.logging.setup import get_logger, setup_logging, setup_multi_worker_logging

# Worker stages for multi-worker logging
WORKER_STAGES = [
    "eventhouse-poller",
    "xact-event-ingester", "xact-download", "xact-upload", "xact-result-processor",
    "claimx-ingester", "claimx-enricher", "claimx-downloader", "claimx-uploader"
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
            "xact-event-ingester", "xact-download", "xact-upload", "xact-result-processor",
            "claimx-ingester", "claimx-enricher", "claimx-downloader", "claimx-uploader",
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

    return parser.parse_args()


async def run_event_ingester(
    eventhub_config,
    local_kafka_config,
    enable_delta_writes: bool = True,
    events_table_path: str = "",
    domain: str = "xact",
):
    """Run the Event Ingester worker.

    Reads events from Event Hub and produces download tasks to local Kafka.
    Uses separate configs: eventhub_config for consumer, local_kafka_config for producer.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    set_log_context(stage="xact-event-ingester")
    logger.info("Starting xact Event Ingester worker...")

    worker = EventIngesterWorker(
        config=eventhub_config,
        enable_delta_writes=enable_delta_writes,
        events_table_path=events_table_path,
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_eventhouse_poller(pipeline_config):
    """Run the Eventhouse Poller.

    Polls Microsoft Fabric Eventhouse for events and produces download tasks to local Kafka.
    Uses KQL queries with deduplication against xact_events Delta table.

    Supports graceful shutdown: when shutdown event is set, the poller
    finishes its current poll cycle before exiting.
    """
    from kafka_pipeline.common.eventhouse.dedup import DedupConfig
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

    # Build deduplication config
    dedup_config = DedupConfig(
        xact_events_table_path=eventhouse_source.xact_events_table_path,
        xact_events_window_hours=eventhouse_source.xact_events_window_hours,
        eventhouse_query_window_hours=eventhouse_source.eventhouse_query_window_hours,
        overlap_minutes=eventhouse_source.overlap_minutes,
        kql_start_stamp=eventhouse_source.kql_start_stamp,
    )

    # Build poller config
    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=pipeline_config.local_kafka.to_kafka_config(),
        dedup=dedup_config,
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
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass


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

    worker = DownloadWorker(config=kafka_config)
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
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

    worker = UploadWorker(config=kafka_config)
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
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
    """
    from kafka_pipeline.xact.workers.result_processor import ResultProcessor

    set_log_context(stage="xact-result-processor")
    logger.info("Starting xact Result Processor worker...")

    # Get table paths from environment
    inventory_table_path = os.getenv("DELTA_INVENTORY_TABLE_PATH", "")
    failed_table_path = os.getenv("DELTA_FAILED_TABLE_PATH", "")

    worker = ResultProcessor(
        config=kafka_config,
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_local_event_ingester(
    local_kafka_config,
    enable_delta_writes: bool = True,
    events_table_path: str = "",
    domain: str = "xact",
):
    """Run EventIngester consuming from local Kafka events.raw topic.

    Used in Eventhouse mode where the poller publishes to events.raw
    and the ingester processes events to downloads.pending.

    Supports graceful shutdown: when shutdown event is set, the worker
    finishes its current batch before exiting.
    """
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker

    set_log_context(stage="xact-event-ingester")
    logger.info("Starting xact Event Ingester (local Kafka mode)...")

    worker = EventIngesterWorker(
        config=local_kafka_config,
        enable_delta_writes=enable_delta_writes,
        events_table_path=events_table_path,
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
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

    worker = ClaimXEnrichmentWorker(config=kafka_config)
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
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

    worker = ClaimXDownloadWorker(config=kafka_config)
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
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

    worker = ClaimXUploadWorker(config=kafka_config)
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
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        # Clean up resources after worker exits
        await worker.stop()


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently.

    Uses the configured event source (Event Hub or Eventhouse) for ingestion.
    Both modes use the same flow:
        events.raw → EventIngester → downloads.pending → DownloadWorker → ...
    """
    from kafka_pipeline.pipeline_config import EventSourceType

    logger.info("Starting all pipeline workers...")

    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

    # Create tasks list
    tasks = []

    # Create event source task based on configuration
    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        # Eventhouse mode: Poller → events.raw → EventIngester → downloads.pending
        # Use Eventhouse-specific path, fall back to generic path for consistency
        eventhouse_events_path = (
            pipeline_config.eventhouse.xact_events_table_path
            or pipeline_config.events_table_path
        )
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
                    enable_delta_writes=False,  # Poller already writes to Delta
                    events_table_path=eventhouse_events_path,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Eventhouse as event source (Delta writes handled by poller)")
    else:
        # EventHub mode: EventHub → events.raw → EventIngester → downloads.pending
        eventhub_config = pipeline_config.eventhub.to_kafka_config()
        tasks.append(
            asyncio.create_task(
                run_event_ingester(
                    eventhub_config,
                    local_kafka_config,
                    enable_delta_writes,
                    events_table_path=pipeline_config.events_table_path,
                    domain=pipeline_config.domain,
                ),
                name="xact-event-ingester",
            )
        )
        logger.info("Using Event Hub as event source")

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

    # Initialize structured logging infrastructure
    if args.worker == "all":
        # Multi-worker mode: create per-worker log files
        setup_multi_worker_logging(
            workers=WORKER_STAGES,
            domain="kafka",
            log_dir=log_dir,
            json_format=json_logs,
            console_level=log_level,
        )
    else:
        # Single worker mode: single log file
        setup_logging(
            name="kafka_pipeline",
            stage=args.worker,
            domain="kafka",
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
                        enable_delta_writes,
                        events_table_path=pipeline_config.events_table_path,
                        domain=pipeline_config.domain,
                    )
                )
        elif args.worker == "xact-download":
            loop.run_until_complete(run_download_worker(local_kafka_config))
        elif args.worker == "xact-upload":
            loop.run_until_complete(run_upload_worker(local_kafka_config))
        elif args.worker == "xact-result-processor":
            loop.run_until_complete(
                run_result_processor(local_kafka_config, enable_delta_writes)
            )
        elif args.worker == "claimx-ingester":
            # ClaimX event ingester
            claimx_events_table_path = os.getenv("CLAIMX_EVENTS_TABLE_PATH", "")
            loop.run_until_complete(
                run_claimx_event_ingester(
                    local_kafka_config,
                    enable_delta_writes,
                    events_table_path=claimx_events_table_path,
                )
            )
        elif args.worker == "claimx-enricher":
            loop.run_until_complete(run_claimx_enrichment_worker(local_kafka_config))
        elif args.worker == "claimx-downloader":
            loop.run_until_complete(run_claimx_download_worker(local_kafka_config))
        elif args.worker == "claimx-uploader":
            loop.run_until_complete(run_claimx_upload_worker(local_kafka_config))
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
