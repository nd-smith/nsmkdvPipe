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

    # Run in development mode (local Kafka only, no Event Hub)
    python -m kafka_pipeline --dev

Architecture:
    Download and Upload workers are decoupled for independent scaling:
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
    eventhub_config,
    local_kafka_config,
    enable_delta_writes: bool = True,
):
    """Run all pipeline workers concurrently."""
    logger.info("Starting all pipeline workers...")

    # Create tasks for all workers
    tasks = [
        asyncio.create_task(
            run_event_ingester(eventhub_config, local_kafka_config, enable_delta_writes),
            name="event-ingester",
        ),
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
        from kafka_pipeline.pipeline_config import LocalKafkaConfig

        local_config = LocalKafkaConfig.from_env()
        kafka_config = local_config.to_kafka_config()

        # In dev mode, event ingester reads from local Kafka too
        eventhub_config = kafka_config
        local_kafka_config = kafka_config
    else:
        # Production mode: Event Hub + local Kafka
        logger.info("Running in PRODUCTION mode (Event Hub + local Kafka)")
        from kafka_pipeline.pipeline_config import get_pipeline_config

        try:
            config = get_pipeline_config()
            eventhub_config = config.eventhub.to_kafka_config()
            local_kafka_config = config.local_kafka.to_kafka_config()
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            logger.error("Use --dev flag for local development without Event Hub")
            sys.exit(1)

    enable_delta_writes = not args.no_delta

    # Get or create event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Set up signal handlers
    setup_signal_handlers(loop)

    try:
        if args.worker == "event-ingester":
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
                run_all_workers(eventhub_config, local_kafka_config, enable_delta_writes)
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
