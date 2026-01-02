"""
Entry point for running Kafka pipeline workers.

Usage:
    # Run all workers
    python -m kafka_pipeline

    # Run specific worker
    python -m kafka_pipeline --worker event-ingester
    python -m kafka_pipeline --worker download
    python -m kafka_pipeline --worker upload
    python -m kafka_pipeline --worker result-processor

    # Run with metrics server
    python -m kafka_pipeline --metrics-port 8000

    # Run in development mode (local Kafka only, no Event Hub/Eventhouse)
    python -m kafka_pipeline --dev

Event Source Configuration:
    Set EVENT_SOURCE environment variable:
    - eventhub (default): Use Azure Event Hub via Kafka protocol
    - eventhouse: Poll Microsoft Fabric Eventhouse

Architecture:
    Download and Upload workers are decoupled for independent scaling:
    - Event Source: Event Hub or Eventhouse → downloads.pending
    - Download Worker: downloads.pending → local cache → downloads.cached
    - Upload Worker: downloads.cached → OneLake → downloads.results
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from typing import List, Optional

from prometheus_client import start_http_server

# Configure logging before imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Kafka pipeline workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all workers (Event Hub → Local Kafka pipeline)
    python -m kafka_pipeline

    # Run only the download worker
    python -m kafka_pipeline --worker download

    # Run in development mode (local Kafka only)
    python -m kafka_pipeline --dev

    # Run with custom metrics port
    python -m kafka_pipeline --metrics-port 9090
        """,
    )

    parser.add_argument(
        "--worker",
        choices=["event-ingester", "download", "upload", "result-processor", "all"],
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
        help="Development mode: use local Kafka for all connections (no Event Hub)",
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

    return parser.parse_args()


async def run_event_ingester(
    eventhub_config,
    local_kafka_config,
    enable_delta_writes: bool = True,
):
    """Run the Event Ingester worker.

    Reads events from Event Hub and produces download tasks to local Kafka.
    """
    from kafka_pipeline.workers.event_ingester import EventIngesterWorker

    logger.info("Starting Event Ingester worker...")

    # For event ingester, we need to read from Event Hub and write to local Kafka
    # This requires a modified worker that uses two different configs
    # For now, we'll use a single config approach with the Event Hub config

    # TODO: Refactor EventIngesterWorker to accept separate consumer/producer configs
    # For now, the worker uses the same config for both consumer and producer
    worker = EventIngesterWorker(
        config=eventhub_config,
        enable_delta_writes=enable_delta_writes,
    )

    await worker.start()


async def run_eventhouse_poller(pipeline_config):
    """Run the Eventhouse Poller.

    Polls Microsoft Fabric Eventhouse for events and produces download tasks to local Kafka.
    Uses KQL queries with deduplication against xact_events Delta table.
    """
    from kafka_pipeline.eventhouse.dedup import DedupConfig
    from kafka_pipeline.eventhouse.kql_client import EventhouseConfig
    from kafka_pipeline.eventhouse.poller import KQLEventPoller, PollerConfig

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
    )

    # Build poller config
    poller_config = PollerConfig(
        eventhouse=eventhouse_config,
        kafka=pipeline_config.local_kafka.to_kafka_config(),
        dedup=dedup_config,
        poll_interval_seconds=eventhouse_source.poll_interval_seconds,
        batch_size=eventhouse_source.batch_size,
        source_table=eventhouse_source.source_table,
    )

    async with KQLEventPoller(poller_config) as poller:
        await poller.run()


async def run_download_worker(kafka_config):
    """Run the Download Worker.

    Reads download tasks from local Kafka, downloads files to local cache,
    and produces CachedDownloadMessage for the upload worker.
    """
    from kafka_pipeline.workers.download_worker import DownloadWorker

    logger.info("Starting Download worker...")

    worker = DownloadWorker(config=kafka_config)
    await worker.start()


async def run_upload_worker(kafka_config):
    """Run the Upload Worker.

    Reads cached downloads from local Kafka, uploads files to OneLake,
    and produces DownloadResultMessage.
    """
    from kafka_pipeline.workers.upload_worker import UploadWorker

    logger.info("Starting Upload worker...")

    worker = UploadWorker(config=kafka_config)
    await worker.start()


async def run_result_processor(kafka_config, enable_delta_writes: bool = True):
    """Run the Result Processor worker.

    Reads download results from local Kafka and writes to Delta Lake tables:
    - xact_attachments: successful downloads
    - xact_attachments_failed: permanent failures (optional)
    """
    from kafka_pipeline.workers.result_processor import ResultProcessor

    logger.info("Starting Result Processor worker...")

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

    await worker.start()


async def run_all_workers(
    pipeline_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently.

    Uses the configured event source (Event Hub or Eventhouse) for ingestion.
    """
    from kafka_pipeline.pipeline_config import EventSourceType

    logger.info("Starting all pipeline workers...")

    local_kafka_config = pipeline_config.local_kafka.to_kafka_config()

    # Create event source task based on configuration
    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        event_source_task = asyncio.create_task(
            run_eventhouse_poller(pipeline_config),
            name="eventhouse-poller",
        )
        logger.info("Using Eventhouse as event source")
    else:
        eventhub_config = pipeline_config.eventhub.to_kafka_config()
        event_source_task = asyncio.create_task(
            run_event_ingester(eventhub_config, local_kafka_config, enable_delta_writes),
            name="event-ingester",
        )
        logger.info("Using Event Hub as event source")

    # Create tasks for all workers
    tasks = [
        event_source_task,
        asyncio.create_task(
            run_download_worker(local_kafka_config),
            name="download-worker",
        ),
        asyncio.create_task(
            run_upload_worker(local_kafka_config),
            name="upload-worker",
        ),
        asyncio.create_task(
            run_result_processor(local_kafka_config, enable_delta_writes),
            name="result-processor",
        ),
    ]

    # Wait for all tasks (they run indefinitely until stopped)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Workers cancelled, shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    """Set up signal handlers for graceful shutdown."""

    def handle_signal(sig):
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        # Cancel all tasks
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
    args = parse_args()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Start Prometheus metrics server
    logger.info(f"Starting metrics server on port {args.metrics_port}")
    start_http_server(args.metrics_port)

    # Load configuration
    if args.dev:
        # Development mode: use local Kafka for everything
        logger.info("Running in DEVELOPMENT mode (local Kafka only)")
        from kafka_pipeline.pipeline_config import (
            EventSourceType,
            LocalKafkaConfig,
            PipelineConfig,
        )

        local_config = LocalKafkaConfig.from_env()
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
        if args.worker == "event-ingester":
            # Event ingester uses Event Hub or Eventhouse based on config
            if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
                loop.run_until_complete(run_eventhouse_poller(pipeline_config))
            else:
                loop.run_until_complete(
                    run_event_ingester(eventhub_config, local_kafka_config, enable_delta_writes)
                )
        elif args.worker == "download":
            loop.run_until_complete(run_download_worker(local_kafka_config))
        elif args.worker == "upload":
            loop.run_until_complete(run_upload_worker(local_kafka_config))
        elif args.worker == "result-processor":
            loop.run_until_complete(
                run_result_processor(local_kafka_config, enable_delta_writes)
            )
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
