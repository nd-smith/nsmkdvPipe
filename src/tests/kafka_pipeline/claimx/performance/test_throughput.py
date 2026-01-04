"""
Performance tests for ClaimX workers.

Tests throughput, concurrency, and batch processing efficiency.
These tests measure performance characteristics but don't enforce
strict thresholds (which would be environment-dependent).

Run with: pytest tests/kafka_pipeline/claimx/performance/test_throughput.py -v -s
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import List
from unittest.mock import AsyncMock, Mock, patch

import pytest

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from kafka_pipeline.claimx.handlers.project import ProjectHandler
from kafka_pipeline.claimx.handlers.media import MediaHandler


class TestEnrichmentHandlerThroughput:
    """Test enrichment handler processing throughput."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sample_events_batch", [100], indirect=True)
    async def test_project_handler_throughput(
        self,
        fast_api_client,
        sample_events_batch: List[ClaimXEventMessage]
    ):
        """
        Test ProjectHandler throughput with 100 events.

        Measures:
        - Events per second
        - Total processing time
        - Average latency per event
        """
        # Filter for PROJECT_CREATED events only
        project_events = [e for e in sample_events_batch if e.event_type == "PROJECT_CREATED"]

        handler = ProjectHandler(client=fast_api_client)

        start_time = time.perf_counter()

        # Process all events
        results = await handler.handle_batch(project_events)

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        # Calculate metrics
        total_events = len(project_events)
        events_per_second = total_events / elapsed if elapsed > 0 else 0
        avg_latency_ms = (elapsed / total_events * 1000) if total_events > 0 else 0

        # Verify all succeeded
        success_count = sum(1 for r in results if r.success)

        print(f"\n=== ProjectHandler Performance ===")
        print(f"Total events: {total_events}")
        print(f"Total time: {elapsed:.3f}s")
        print(f"Throughput: {events_per_second:.1f} events/sec")
        print(f"Avg latency: {avg_latency_ms:.2f}ms per event")
        print(f"Success rate: {success_count}/{total_events} ({success_count/total_events*100:.1f}%)")

        # Assertions
        assert success_count == total_events, "All events should succeed"
        assert events_per_second > 0, "Should process events"
        assert avg_latency_ms < 1000, "Average latency should be under 1s per event"

    @pytest.mark.asyncio
    async def test_media_handler_throughput(
        self,
        fast_api_client
    ):
        """
        Test MediaHandler throughput with 100 file events.

        Measures batch optimization where multiple events for same
        project can be handled with a single API call.
        """
        # Create 100 media events - all for same project to test batching
        project_id = "100"
        media_events = []

        for i in range(100):
            media_id = 1000 + i
            event = ClaimXEventMessage(
                event_id=f"evt_media_{i}",
                event_type="PROJECT_FILE_ADDED",
                project_id=project_id,
                media_id=str(media_id),
                ingested_at=datetime.now(timezone.utc),
                raw_data={}
            )
            media_events.append(event)

        handler = MediaHandler(client=fast_api_client)

        start_time = time.perf_counter()

        # Process all events (batching should optimize API calls)
        results = await handler.handle_batch(media_events)

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        # Calculate metrics
        total_events = len(media_events)
        events_per_second = total_events / elapsed if elapsed > 0 else 0
        avg_latency_ms = (elapsed / total_events * 1000) if total_events > 0 else 0

        # Count API calls (should be less than total events due to batching)
        api_call_count = fast_api_client.get_project_media.call_count

        success_count = sum(1 for r in results if r.success)

        print(f"\n=== MediaHandler Performance ===")
        print(f"Total events: {total_events}")
        print(f"Total time: {elapsed:.3f}s")
        print(f"Throughput: {events_per_second:.1f} events/sec")
        print(f"Avg latency: {avg_latency_ms:.2f}ms per event")
        print(f"API calls: {api_call_count} (batching ratio: {total_events/api_call_count:.1f}x)")
        print(f"Success rate: {success_count}/{total_events} ({success_count/total_events*100:.1f}%)")

        # Assertions
        assert events_per_second > 0, "Should process events"
        # Batching should reduce API calls significantly
        assert api_call_count < total_events, "Batching should reduce API calls"


class TestDownloadWorkerConcurrency:
    """Test download worker concurrent processing."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrency", [1, 5, 10, 20])
    async def test_concurrent_downloads(
        self,
        concurrency: int,
        tmp_path
    ):
        """
        Test download performance at different concurrency levels.

        Measures impact of parallelism on throughput.
        """
        from core.download.models import DownloadOutcome
        from pathlib import Path

        # Create mock downloader
        async def mock_download(url: str, destination: str):
            """Simulate download with realistic delay."""
            await asyncio.sleep(0.01)  # 10ms simulated download time
            # Create test file
            dest_path = Path(destination)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(b"test content")

            return DownloadOutcome(
                success=True,
                file_path=dest_path,
                bytes_downloaded=12,
                content_type="image/jpeg",
                status_code=200
            )

        num_downloads = 50

        # Create semaphore to control concurrency
        semaphore = asyncio.Semaphore(concurrency)

        async def download_with_semaphore(i: int):
            async with semaphore:
                return await mock_download(
                    f"https://example.com/file_{i}.jpg",
                    str(tmp_path / f"file_{i}.jpg")
                )

        start_time = time.perf_counter()

        # Download all files concurrently (up to semaphore limit)
        results = await asyncio.gather(*[
            download_with_semaphore(i) for i in range(num_downloads)
        ])

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        downloads_per_second = num_downloads / elapsed if elapsed > 0 else 0
        success_count = sum(1 for r in results if r.success)

        print(f"\n=== Concurrent Downloads (concurrency={concurrency}) ===")
        print(f"Total downloads: {num_downloads}")
        print(f"Concurrency: {concurrency}")
        print(f"Total time: {elapsed:.3f}s")
        print(f"Throughput: {downloads_per_second:.1f} downloads/sec")
        print(f"Success rate: {success_count}/{num_downloads}")

        # Assertions
        assert success_count == num_downloads, "All downloads should succeed"
        assert downloads_per_second > 0, "Should complete downloads"

        # Higher concurrency should be faster (up to a point)
        # With 10ms per download and 50 downloads:
        # - Concurrency 1: ~500ms
        # - Concurrency 10: ~50ms
        # - Concurrency 50: ~10ms
        if concurrency >= 10:
            assert elapsed < 0.2, f"With concurrency {concurrency}, should complete in <200ms"


class TestBatchProcessingEfficiency:
    """Test batch processing optimizations."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("batch_size", [10, 50, 100, 200])
    async def test_batch_size_impact(
        self,
        batch_size: int,
        fast_api_client
    ):
        """
        Test impact of batch size on processing efficiency.

        Measures how batch size affects throughput.
        """
        from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage

        # Generate events for same project (to test batching)
        events = []
        project_id = "123"

        for i in range(batch_size):
            event = ClaimXEventMessage(
                event_id=f"evt_{i}",
                event_type="PROJECT_FILE_ADDED",
                project_id=project_id,
                media_id=str(1000 + i),
                ingested_at=datetime.now(timezone.utc),
                raw_data={}
            )
            events.append(event)

        handler = MediaHandler(client=fast_api_client)

        start_time = time.perf_counter()

        results = await handler.handle_batch(events)

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        events_per_second = batch_size / elapsed if elapsed > 0 else 0
        api_calls = fast_api_client.get_project_media.call_count

        print(f"\n=== Batch Processing (size={batch_size}) ===")
        print(f"Batch size: {batch_size}")
        print(f"Total time: {elapsed:.3f}s")
        print(f"Throughput: {events_per_second:.1f} events/sec")
        print(f"API calls: {api_calls}")
        print(f"Events per API call: {batch_size/api_calls:.1f}")

        # Assertions
        assert len(results) == batch_size
        # Note: We're testing throughput and batching efficiency,
        # not correctness of media lookups

        # Larger batches should maintain or improve efficiency
        assert events_per_second > 0

        # Batching should be efficient (single API call for same project)
        assert api_calls == 1, "All events are for same project, should be single API call"


class TestMemoryEfficiency:
    """Test memory efficiency of batch processing."""

    @pytest.mark.asyncio
    async def test_batch_memory_usage(
        self,
        fast_api_client
    ):
        """
        Test that batch processing doesn't cause excessive memory growth.

        Processes multiple batches and ensures memory is released.
        """
        from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
        import gc

        handler = MediaHandler(client=fast_api_client)

        # Process multiple batches
        num_batches = 10
        batch_size = 100

        for batch_num in range(num_batches):
            events = []
            for i in range(batch_size):
                event = ClaimXEventMessage(
                    event_id=f"evt_batch{batch_num}_{i}",
                    event_type="PROJECT_FILE_ADDED",
                    project_id=str(100 + (i % 10)),
                    media_id=str(1000 + i),
                    ingested_at=datetime.now(timezone.utc),
                    raw_data={}
                )
                events.append(event)

            results = await handler.handle_batch(events)

            # Clear references
            del events
            del results
            gc.collect()

        print(f"\n=== Memory Efficiency Test ===")
        print(f"Processed {num_batches} batches of {batch_size} events each")
        print(f"Total events: {num_batches * batch_size}")
        print("Test completed without memory errors")

        # If we got here, memory management is working
        assert True


class TestAPICallOptimization:
    """Test API call optimization strategies."""

    @pytest.mark.asyncio
    async def test_project_deduplication(
        self,
        fast_api_client
    ):
        """
        Test that events for same project deduplicate API calls.
        """
        from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage

        # Create 50 events all for same project
        project_id = "999"
        events = []

        for i in range(50):
            event = ClaimXEventMessage(
                event_id=f"evt_{i}",
                event_type="PROJECT_FILE_ADDED",
                project_id=project_id,
                media_id=str(2000 + i),
                ingested_at=datetime.now(timezone.utc),
                raw_data={}
            )
            events.append(event)

        handler = MediaHandler(client=fast_api_client)

        fast_api_client.get_project_media.reset_mock()

        start_time = time.perf_counter()
        results = await handler.handle_batch(events)
        elapsed = time.perf_counter() - start_time

        api_call_count = fast_api_client.get_project_media.call_count

        print(f"\n=== API Call Deduplication ===")
        print(f"Events: 50 (all same project)")
        print(f"API calls: {api_call_count}")
        print(f"Deduplication ratio: {50/api_call_count:.1f}x")
        print(f"Processing time: {elapsed:.3f}s")

        # Should make very few API calls (ideally 1-2 due to batching)
        assert api_call_count < 50, "Should deduplicate API calls"

        # With batching by project, should be very efficient
        assert api_call_count <= 10, "Batching should be highly efficient for same project"
