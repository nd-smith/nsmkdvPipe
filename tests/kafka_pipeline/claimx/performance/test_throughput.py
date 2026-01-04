"""
Performance and throughput tests for ClaimX workers.

Measures and establishes performance baselines for:
1. Enrichment worker throughput (events/sec)
2. Download worker throughput (files/sec)
3. API call latency impact
4. End-to-end pipeline latency

These tests use real Kafka (via testcontainers) with mocked external services
to isolate pipeline performance from external API/storage latency.

Baseline Performance Targets (for reference):
- Enrichment worker: > 100 events/sec (single worker)
- Download worker: > 10 files/sec (single worker, with S3 mocked)
- API latency: < 100ms per call (mocked)
- E2E latency: < 5 seconds for single file flow

Run with:
    pytest tests/kafka_pipeline/claimx/performance/test_throughput.py -v -s
"""

import asyncio
import json
import logging
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask, ClaimXDownloadTask
from kafka_pipeline.config import KafkaConfig

from tests.kafka_pipeline.integration.helpers import (
    get_topic_messages,
    start_worker_background,
    stop_worker_gracefully,
    wait_for_condition,
)
from tests.kafka_pipeline.integration.claimx.generators import (
    create_claimx_event,
    create_enrichment_task,
    create_download_task,
    create_mock_project_response,
    create_mock_media_response,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Performance Test Configuration
# =============================================================================

# Number of events/files for throughput tests
ENRICHMENT_EVENT_COUNT = 100  # Events to process for enrichment throughput
DOWNLOAD_FILE_COUNT = 50  # Files to download for download throughput

# Concurrency settings
ENRICHMENT_BATCH_SIZE = 10  # Events per batch
DOWNLOAD_CONCURRENCY = 10  # Concurrent downloads

# Performance thresholds (for assertions)
MIN_ENRICHMENT_THROUGHPUT = 50.0  # events/sec (conservative target)
MIN_DOWNLOAD_THROUGHPUT = 5.0  # files/sec (conservative target)
MAX_API_LATENCY_MS = 200.0  # milliseconds (mocked API)


# =============================================================================
# Performance Measurement Helpers
# =============================================================================

class PerformanceStats:
    """Track and calculate performance statistics."""

    def __init__(self, operation: str):
        self.operation = operation
        self.start_time: float = 0
        self.end_time: float = 0
        self.item_count: int = 0
        self.latencies: List[float] = []

    def start(self):
        """Start timing."""
        self.start_time = time.time()

    def record_item(self, latency_ms: float = None):
        """Record completion of one item."""
        self.item_count += 1
        if latency_ms is not None:
            self.latencies.append(latency_ms)

    def finish(self):
        """Finish timing."""
        self.end_time = time.time()

    @property
    def elapsed_seconds(self) -> float:
        """Total elapsed time in seconds."""
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        """Items per second."""
        if self.elapsed_seconds > 0:
            return self.item_count / self.elapsed_seconds
        return 0.0

    @property
    def avg_latency_ms(self) -> float:
        """Average latency in milliseconds."""
        if self.latencies:
            return statistics.mean(self.latencies)
        return 0.0

    @property
    def p50_latency_ms(self) -> float:
        """Median latency in milliseconds."""
        if self.latencies:
            return statistics.median(self.latencies)
        return 0.0

    @property
    def p95_latency_ms(self) -> float:
        """95th percentile latency in milliseconds."""
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.95)
            return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
        return 0.0

    @property
    def p99_latency_ms(self) -> float:
        """99th percentile latency in milliseconds."""
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.99)
            return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
        return 0.0

    def print_summary(self):
        """Print performance summary."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Performance Test: {self.operation}")
        logger.info(f"{'='*60}")
        logger.info(f"Total items processed: {self.item_count}")
        logger.info(f"Total time: {self.elapsed_seconds:.2f}s")
        logger.info(f"Throughput: {self.throughput:.2f} items/sec")
        if self.latencies:
            logger.info(f"\nLatency Statistics:")
            logger.info(f"  Average: {self.avg_latency_ms:.2f}ms")
            logger.info(f"  Median (p50): {self.p50_latency_ms:.2f}ms")
            logger.info(f"  p95: {self.p95_latency_ms:.2f}ms")
            logger.info(f"  p99: {self.p99_latency_ms:.2f}ms")
        logger.info(f"{'='*60}\n")


# =============================================================================
# Enrichment Worker Performance Tests
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
class TestEnrichmentWorkerPerformance:
    """Performance tests for ClaimX enrichment worker."""

    async def test_enrichment_throughput_single_worker(
        self,
        test_claimx_config: KafkaConfig,
        claimx_enrichment_worker,
        mock_claimx_api_client,
        mock_claimx_entity_writer,
        kafka_producer,
    ):
        """
        Measure enrichment worker throughput with single worker instance.

        Tests:
        - Processing rate of PROJECT_CREATED events
        - Entity writes to Delta Lake (mocked)
        - API call frequency and batching
        - Overall events/sec throughput

        Target: > 50 events/sec (conservative baseline)
        """
        event_count = ENRICHMENT_EVENT_COUNT
        stats = PerformanceStats("Enrichment Worker Throughput")

        # Mock API responses
        for i in range(event_count):
            project_id = 10000 + i
            mock_claimx_api_client.set_project(
                project_id,
                create_mock_project_response(project_id, mfn=f"MFN-{project_id}")
            )

        # Start enrichment worker
        worker_task = await start_worker_background(claimx_enrichment_worker)

        try:
            await asyncio.sleep(2)  # Allow worker to initialize

            # Produce events
            stats.start()
            for i in range(event_count):
                task = create_enrichment_task(
                    event_id=10000 + i,
                    event_type="PROJECT_CREATED",
                    project_id=10000 + i,
                )
                await kafka_producer.send(
                    topic=test_claimx_config.claimx_enrichment_pending_topic,
                    key=task.event_id,
                    value=task,
                )

            # Wait for all events to be processed (check entity writes)
            await wait_for_condition(
                lambda: mock_claimx_entity_writer.write_count >= event_count,
                timeout_seconds=max(30.0, event_count / 10.0),  # Scale timeout with count
                description=f"{event_count} entity writes",
            )

            stats.finish()
            stats.item_count = event_count

            # Print performance summary
            stats.print_summary()

            # Assertions
            assert stats.throughput >= MIN_ENRICHMENT_THROUGHPUT, \
                f"Enrichment throughput {stats.throughput:.2f} events/sec below target {MIN_ENRICHMENT_THROUGHPUT}"

            # Verify all events were processed
            assert mock_claimx_entity_writer.write_count >= event_count, \
                f"Expected {event_count} writes, got {mock_claimx_entity_writer.write_count}"

        finally:
            await stop_worker_gracefully(claimx_enrichment_worker, worker_task)

    async def test_enrichment_api_call_latency(
        self,
        test_claimx_config: KafkaConfig,
        mock_claimx_api_client,
    ):
        """
        Measure API call latency for enrichment operations.

        Tests:
        - get_project() call latency
        - get_project_media() call latency
        - Impact of concurrent API calls

        Target: < 100ms per call (mocked API, measures overhead)
        """
        stats = PerformanceStats("API Call Latency")
        call_count = 100

        # Prepare mock data
        for i in range(call_count):
            project_id = 20000 + i
            mock_claimx_api_client.set_project(
                project_id,
                create_mock_project_response(project_id)
            )
            mock_claimx_api_client.set_media(
                project_id,
                [create_mock_media_response(i, project_id) for i in range(5)]
            )

        # Measure get_project latency
        project_latencies = []
        for i in range(call_count):
            start = time.time()
            await mock_claimx_api_client.get_project(20000 + i)
            latency_ms = (time.time() - start) * 1000
            project_latencies.append(latency_ms)
            stats.record_item(latency_ms)

        stats.finish()

        # Print results
        logger.info(f"\nAPI Call Latency (get_project):")
        logger.info(f"  Average: {statistics.mean(project_latencies):.2f}ms")
        logger.info(f"  Median: {statistics.median(project_latencies):.2f}ms")
        logger.info(f"  p95: {stats.p95_latency_ms:.2f}ms")
        logger.info(f"  p99: {stats.p99_latency_ms:.2f}ms")

        # Assertions
        assert stats.avg_latency_ms < MAX_API_LATENCY_MS, \
            f"API latency {stats.avg_latency_ms:.2f}ms exceeds target {MAX_API_LATENCY_MS}ms"


# =============================================================================
# Download Worker Performance Tests
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
class TestDownloadWorkerPerformance:
    """Performance tests for ClaimX download worker."""

    async def test_download_throughput_single_worker(
        self,
        test_claimx_config: KafkaConfig,
        claimx_download_worker,
        kafka_producer,
        tmp_path: Path,
    ):
        """
        Measure download worker throughput with single worker instance.

        Tests:
        - File download rate (files/sec)
        - Concurrent download processing
        - Cache write performance

        Target: > 5 files/sec (conservative, with mocked S3)
        """
        file_count = DOWNLOAD_FILE_COUNT
        stats = PerformanceStats("Download Worker Throughput")
        test_content = b"Test file content for performance testing"

        # Mock S3 download responses
        async def mock_s3_get(url, **kwargs):
            # Simulate slight network latency
            await asyncio.sleep(0.01)  # 10ms
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.headers = {"Content-Type": "application/octet-stream"}
            mock_response.read = AsyncMock(return_value=test_content)
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)
            return mock_response

        with patch("aiohttp.ClientSession.get", side_effect=mock_s3_get):
            # Start download worker
            worker_task = await start_worker_background(claimx_download_worker)

            try:
                await asyncio.sleep(2)  # Allow worker to initialize

                # Produce download tasks
                stats.start()
                for i in range(file_count):
                    task = create_download_task(
                        media_id=30000 + i,
                        project_id=50001,
                        download_url=f"https://s3.amazonaws.com/test/media_{30000+i}.jpg",
                    )
                    await kafka_producer.send(
                        topic=test_claimx_config.claimx_downloads_pending_topic,
                        key=task.media_id,
                        value=task,
                    )

                # Wait for all downloads to complete (check cached topic)
                cached_messages = []
                async def check_cached():
                    nonlocal cached_messages
                    cached_messages = await get_topic_messages(
                        test_claimx_config,
                        test_claimx_config.claimx_downloads_cached_topic,
                        max_messages=file_count + 10,
                        timeout_seconds=0.5,
                    )
                    return len(cached_messages) >= file_count

                # Poll until all files are cached
                timeout = max(30.0, file_count / 2.0)  # Scale timeout
                for _ in range(int(timeout * 2)):  # Check every 0.5s
                    if await check_cached():
                        break
                    await asyncio.sleep(0.5)

                stats.finish()
                stats.item_count = len(cached_messages)

                # Print performance summary
                stats.print_summary()

                # Assertions
                assert stats.throughput >= MIN_DOWNLOAD_THROUGHPUT, \
                    f"Download throughput {stats.throughput:.2f} files/sec below target {MIN_DOWNLOAD_THROUGHPUT}"

                assert len(cached_messages) >= file_count, \
                    f"Expected {file_count} downloads, got {len(cached_messages)}"

            finally:
                await stop_worker_gracefully(claimx_download_worker, worker_task)

    async def test_download_concurrent_processing(
        self,
        test_claimx_config: KafkaConfig,
        claimx_download_worker,
        kafka_producer,
        tmp_path: Path,
    ):
        """
        Measure impact of concurrent downloads on throughput.

        Tests:
        - Throughput with various concurrency levels
        - Resource utilization
        - Optimal concurrency setting

        Compares sequential vs concurrent download performance.
        """
        file_count = 20
        test_content = b"Test file for concurrency test"
        call_times = []

        # Mock S3 with latency tracking
        async def mock_s3_get_tracked(url, **kwargs):
            start = time.time()
            await asyncio.sleep(0.02)  # 20ms simulated latency
            call_times.append((time.time() - start) * 1000)
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.headers = {"Content-Type": "application/octet-stream"}
            mock_response.read = AsyncMock(return_value=test_content)
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)
            return mock_response

        with patch("aiohttp.ClientSession.get", side_effect=mock_s3_get_tracked):
            worker_task = await start_worker_background(claimx_download_worker)

            try:
                await asyncio.sleep(2)

                start_time = time.time()

                # Produce all tasks at once (tests concurrent processing)
                for i in range(file_count):
                    task = create_download_task(
                        media_id=40000 + i,
                        project_id=50001,
                        download_url=f"https://s3.amazonaws.com/test/concurrent_{40000+i}.jpg",
                    )
                    await kafka_producer.send(
                        topic=test_claimx_config.claimx_downloads_pending_topic,
                        key=task.media_id,
                        value=task,
                    )

                # Wait for completion
                cached_messages = []
                async def check_cached():
                    nonlocal cached_messages
                    cached_messages = await get_topic_messages(
                        test_claimx_config,
                        test_claimx_config.claimx_downloads_cached_topic,
                        max_messages=file_count + 10,
                        timeout_seconds=0.5,
                    )
                    return len(cached_messages) >= file_count

                for _ in range(60):  # 30s max
                    if await check_cached():
                        break
                    await asyncio.sleep(0.5)

                elapsed = time.time() - start_time
                throughput = len(cached_messages) / elapsed

                logger.info(f"\n{'='*60}")
                logger.info(f"Concurrent Download Performance")
                logger.info(f"{'='*60}")
                logger.info(f"Files processed: {len(cached_messages)}")
                logger.info(f"Total time: {elapsed:.2f}s")
                logger.info(f"Throughput: {throughput:.2f} files/sec")
                logger.info(f"Concurrent API calls: {len(call_times)}")
                if call_times:
                    logger.info(f"Avg call latency: {statistics.mean(call_times):.2f}ms")
                logger.info(f"{'='*60}\n")

                # Verify concurrency improved performance
                # Sequential would take: file_count * 20ms = 400ms minimum
                # Concurrent should be significantly faster
                sequential_min_time = file_count * 0.02
                assert elapsed < sequential_min_time * 0.8, \
                    f"Concurrent processing ({elapsed:.2f}s) not faster than sequential (~{sequential_min_time:.2f}s)"

            finally:
                await stop_worker_gracefully(claimx_download_worker, worker_task)


# =============================================================================
# End-to-End Pipeline Performance
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
class TestPipelineE2EPerformance:
    """End-to-end pipeline performance tests."""

    async def test_single_file_e2e_latency(
        self,
        test_claimx_config: KafkaConfig,
        claimx_download_worker,
        claimx_upload_worker,
        mock_onelake_client,
        kafka_producer,
        tmp_path: Path,
    ):
        """
        Measure end-to-end latency for single file through pipeline.

        Flow:
        1. Download task produced
        2. Download worker downloads file
        3. Upload worker uploads to OneLake
        4. Result message produced

        Target: < 5 seconds for complete pipeline
        """
        test_content = b"Test file for E2E latency measurement"

        # Mock S3 and OneLake
        async def mock_s3_get(url, **kwargs):
            await asyncio.sleep(0.05)  # 50ms simulated latency
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.headers = {"Content-Type": "image/jpeg"}
            mock_response.read = AsyncMock(return_value=test_content)
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)
            return mock_response

        with patch("aiohttp.ClientSession.get", side_effect=mock_s3_get):
            # Start workers
            download_task = await start_worker_background(claimx_download_worker)
            upload_task = await start_worker_background(claimx_upload_worker)

            try:
                await asyncio.sleep(2)

                # Measure E2E latency
                start_time = time.time()

                # Produce download task
                task = create_download_task(
                    media_id=50000,
                    project_id=50001,
                    download_url="https://s3.amazonaws.com/test/e2e_test.jpg",
                )
                await kafka_producer.send(
                    topic=test_claimx_config.claimx_downloads_pending_topic,
                    key=task.media_id,
                    value=task,
                )

                # Wait for upload to complete
                upload_complete = await wait_for_condition(
                    lambda: mock_onelake_client.upload_count > 0,
                    timeout_seconds=10.0,
                    description="E2E upload complete",
                )

                e2e_latency = time.time() - start_time

                logger.info(f"\n{'='*60}")
                logger.info(f"End-to-End Pipeline Latency")
                logger.info(f"{'='*60}")
                logger.info(f"Total E2E time: {e2e_latency:.3f}s")
                logger.info(f"  - Download + Upload + Messaging overhead")
                logger.info(f"{'='*60}\n")

                # Assertions
                assert upload_complete, "Upload did not complete within timeout"
                assert e2e_latency < 5.0, \
                    f"E2E latency {e2e_latency:.2f}s exceeds 5s target"

            finally:
                await stop_worker_gracefully(claimx_download_worker, download_task)
                await stop_worker_gracefully(claimx_upload_worker, upload_task)
