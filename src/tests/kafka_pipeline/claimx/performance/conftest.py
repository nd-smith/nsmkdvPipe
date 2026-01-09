"""Fixtures for ClaimX performance tests."""

import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.api_client import ClaimXApiClient


@pytest.fixture
def perf_test_config():
    """Create a KafkaConfig for performance testing."""
    config = Mock(spec=KafkaConfig)
    config.kafka_bootstrap_servers = "localhost:9092"
    config.consumer_group_prefix = "perf_test"
    config.claimx_events_topic = "claimx.events.raw"
    config.claimx_enrichment_pending_topic = "claimx.enrichment.pending"
    config.claimx_downloads_pending_topic = "claimx.downloads.pending"
    config.claimx_api_url = "https://api.claimx.test"
    config.claimx_api_token = "dGVzdDp0ZXN0"  # base64 encoded test:test
    config.claimx_api_timeout_seconds = 30
    config.claimx_api_max_retries = 3
    config.claimx_api_concurrency = 10
    config.retry_delays = [300, 600, 1200, 2400]
    config.max_retries = 4
    config.claimx_topic_prefix = "claimx"
    config.cache_dir = tempfile.mkdtemp()
    config.download_concurrency = 10
    config.download_batch_size = 20
    return config


@pytest.fixture
def fast_api_client():
    """Create a fast mock API client for performance testing."""
    client = AsyncMock(spec=ClaimXApiClient)

    # Fast project response
    async def mock_get_project(project_id: int) -> Dict[str, Any]:
        return {
            "data": {
                "project": {
                    "projectId": project_id,
                    "projectNumber": f"PRJ-{project_id}",
                    "mfn": f"MFN-{project_id}",
                    "status": "Active",
                    "createdDate": datetime.now(timezone.utc).isoformat(),
                    "customerInformation": {
                        "firstName": "John",
                        "lastName": "Doe",
                        "emails": [{"emailAddress": f"user{project_id}@example.com", "primary": True}],
                        "phones": [{"phoneNumber": "1234567890", "phoneCountryCode": 1}]
                    },
                    "address": {
                        "street1": "123 Main St",
                        "city": "Anytown",
                        "stateProvince": "CA",
                        "zipPostcode": "90210"
                    }
                },
                "teamMembers": []
            }
        }

    # Fast media response
    async def mock_get_project_media(project_id: int, media_ids: List[int] = None) -> Dict[str, Any]:
        media_list = []

        # If specific media IDs requested, return those
        if media_ids:
            ids_to_return = media_ids
        else:
            # Otherwise return a default set based on project_id
            ids_to_return = [project_id * 10 + i for i in range(10)]

        for media_id in ids_to_return:
            media_list.append({
                "mediaID": media_id,
                "taskAssignmentID": 1000 + media_id,
                "mediaType": "jpg",
                "mediaName": f"photo_{media_id}.jpg",
                "fullDownloadLink": f"https://s3.amazonaws.com/claimx/{project_id}/photo_{media_id}.jpg?sig=abc&expires=2026-01-05T00:00:00Z",
                "mediaDescription": "Test photo",
                "takenDate": datetime.now(timezone.utc).isoformat()
            })

        return {"data": media_list}

    client.get_project = AsyncMock(side_effect=mock_get_project)
    client.get_project_media = AsyncMock(side_effect=mock_get_project_media)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    return client


@pytest.fixture
def sample_events_batch(request):
    """Generate a batch of sample events for performance testing.

    Use with @pytest.mark.parametrize("sample_events_batch", [100], indirect=True)
    to specify the batch size.
    """
    batch_size = getattr(request, 'param', 100)

    from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage

    events = []
    for i in range(batch_size):
        event = ClaimXEventMessage(
            event_id=f"evt_perf_{i}",
            event_type="PROJECT_CREATED" if i % 2 == 0 else "PROJECT_FILE_ADDED",
            project_id=str(100 + (i % 50)),  # Distribute across 50 projects
            media_id=str(1000 + i) if i % 2 == 1 else None,
            ingested_at=datetime.now(timezone.utc),
            raw_data={}
        )
        events.append(event)

    return events


@pytest.fixture
def sample_download_tasks_batch(request):
    """Generate a batch of download tasks for performance testing."""
    batch_size = getattr(request, 'param', 100)

    from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask

    tasks = []
    for i in range(batch_size):
        task = ClaimXDownloadTask(
            media_id=str(1000 + i),
            project_id=str(100 + (i % 50)),
            download_url=f"https://s3.amazonaws.com/claimx/test_{i}.jpg?sig=abc&expires=2026-01-05T00:00:00Z",
            blob_path=f"claimx/{100 + (i % 50)}/media/test_{i}.jpg",
            file_type="jpg",
            file_name=f"test_{i}.jpg",
            source_event_id=f"evt_{i}",
            retry_count=0,
            created_at=datetime.now(timezone.utc),
            expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        )
        tasks.append(task)

    return tasks
