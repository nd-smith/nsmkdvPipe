"""
ClaimX Enrichment Worker

Standalone entry point for the ClaimX enrichment worker that consumes
enrichment tasks from Kafka, enriches them with API data, and produces
entity rows and download tasks.

Usage:
    python -m kafka_pipeline.workers.claimx_enrichment_worker

Configuration:
    Uses KafkaConfig which loads from environment variables and config files.

Environment Variables Required:
    - KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
    - CLAIMX_API_URL: ClaimX API base URL
    - CLAIMX_API_TOKEN: Bearer token for ClaimX API
"""

import asyncio
import logging
import signal
import sys

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Main entry point for ClaimX Enrichment Worker."""
    logger.info("=" * 70)
    logger.info("Starting ClaimX Enrichment Worker")
    logger.info("=" * 70)

    # Load Kafka configuration from environment
    try:
        config = KafkaConfig.from_env()
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    logger.info(f"Kafka bootstrap servers: {config.bootstrap_servers}")

    # Create worker
    worker = ClaimXEnrichmentWorker(config=config)

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()

    def signal_handler() -> None:
        logger.info("Received shutdown signal")
        asyncio.create_task(worker.request_shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    # Start worker (blocks until stopped)
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.exception(f"Worker failed with error: {e}")
        sys.exit(1)
    finally:
        await worker.stop()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
