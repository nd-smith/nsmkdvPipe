"""Pytest fixtures for ClaimX integration tests."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.api_client import ClaimXApiClient
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.writers import ClaimXEntityWriter, ClaimXEventsDeltaWriter


@pytest.fixture
def mock_kafka_config():
    """Create a mock KafkaConfig for testing."""
    config = Mock(spec=KafkaConfig)
    config.kafka_bootstrap_servers = "localhost:9092"
    config.consumer_group_prefix = "test"
    config.claimx_events_topic = "claimx.events.raw"
    config.claimx_enrichment_pending_topic = "claimx.enrichment.pending"
    config.claimx_downloads_pending_topic = "claimx.downloads.pending"
    config.claimx_api_url = "https://api.claimx.test"
    config.claimx_api_token_env = "TEST_API_TOKEN"
    config.claimx_api_max_concurrent = 10
    config.claimx_api_timeout_seconds = 30
    config.retry_delays = [300, 600, 1200, 2400]
    config.max_retries = 4
    config.claimx_topic_prefix = "claimx"
    config.cache_dir = "/tmp/kafka_pipeline_cache"
    return config


@pytest.fixture
def mock_kafka_producer():
    """Create a mock Kafka producer for testing."""
    producer = AsyncMock()

    # Create a mock metadata object
    mock_metadata = Mock()
    mock_metadata.partition = 0
    mock_metadata.offset = 0

    producer.send = AsyncMock(return_value=mock_metadata)
    producer.send_and_wait = AsyncMock(return_value=mock_metadata)
    producer.start = AsyncMock(return_value=None)
    producer.stop = AsyncMock(return_value=None)
    producer.__aenter__ = AsyncMock(return_value=producer)
    producer.__aexit__ = AsyncMock(return_value=None)

    # Track messages sent
    producer.sent_messages = []

    async def track_send(topic: str = None, key: Any = None, value: Any = None, headers: Dict = None, **kwargs):
        """Track sent messages."""
        producer.sent_messages.append({
            "topic": topic,
            "key": key,
            "value": value,
            "headers": headers
        })
        return mock_metadata

    producer.send.side_effect = track_send
    producer.send_and_wait.side_effect = track_send

    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Create a mock Kafka consumer for testing."""
    consumer = AsyncMock()
    consumer.start = AsyncMock(return_value=None)
    consumer.stop = AsyncMock(return_value=None)
    consumer.commit = AsyncMock(return_value=None)
    consumer.__aenter__ = AsyncMock(return_value=consumer)
    consumer.__aexit__ = AsyncMock(return_value=None)

    # Track consumed messages
    consumer.messages = []
    consumer.current_index = 0

    async def mock_iter():
        """Async iterator for consumer messages."""
        while consumer.current_index < len(consumer.messages):
            message = consumer.messages[consumer.current_index]
            consumer.current_index += 1
            yield message

    consumer.__aiter__ = lambda self: mock_iter()

    return consumer


@pytest.fixture
def mock_api_client():
    """Create a mock ClaimXApiClient for testing."""
    client = AsyncMock(spec=ClaimXApiClient)

    # Mock project data - must match ClaimX API response format
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
                        "emails": [{"emailAddress": "john@example.com", "primary": True}],
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

    # Mock media data - must match ClaimX API response format
    async def mock_get_project_media(project_id: int, media_ids: List[int] = None) -> Dict[str, Any]:
        media_list = []
        # If specific media IDs requested, return those
        if media_ids:
            for media_id in media_ids:
                media_list.append({
                    "mediaID": media_id,
                    "taskAssignmentID": 1000 + media_id,
                    "mediaType": "jpg",
                    "mediaName": f"photo_{media_id}.jpg",
                    "fullDownloadLink": f"https://s3.amazonaws.com/claimx/{project_id}/photo_{media_id}.jpg?sig=abc123&expires=2026-01-05T00:00:00Z",
                    "mediaDescription": "Test photo",
                    "takenDate": datetime.now(timezone.utc).isoformat()
                })
        else:
            # Return a default media item
            media_list.append({
                "mediaID": project_id * 10,
                "taskAssignmentID": 1001,
                "mediaType": "jpg",
                "mediaName": "photo1.jpg",
                "fullDownloadLink": f"https://s3.amazonaws.com/claimx/{project_id}/photo1.jpg?sig=abc123&expires=2026-01-05T00:00:00Z",
                "mediaDescription": "Test photo"
            })

        return {"data": media_list}

    client.get_project = AsyncMock(side_effect=mock_get_project)
    client.get_project_media = AsyncMock(side_effect=mock_get_project_media)
    client.get_custom_task = AsyncMock(return_value={"data": {}})
    client.get_video_collaboration = AsyncMock(return_value={"data": {}})
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    return client


@pytest.fixture
def mock_delta_writer():
    """Create a mock ClaimXEntityWriter for testing."""
    writer = Mock(spec=ClaimXEntityWriter)

    # Track writes
    writer.written_entities = {
        "projects": [],
        "contacts": [],
        "media": [],
        "tasks": [],
        "task_templates": [],
        "external_links": [],
        "video_collab": []
    }

    def track_write(table_name: str, rows: List[Any]):
        """Track written rows."""
        if table_name in writer.written_entities:
            writer.written_entities[table_name].extend(rows)

    writer.write_projects = Mock(side_effect=lambda rows: track_write("projects", rows))
    writer.write_contacts = Mock(side_effect=lambda rows: track_write("contacts", rows))
    writer.write_media = Mock(side_effect=lambda rows: track_write("media", rows))
    writer.write_tasks = Mock(side_effect=lambda rows: track_write("tasks", rows))
    writer.write_task_templates = Mock(side_effect=lambda rows: track_write("task_templates", rows))
    writer.write_external_links = Mock(side_effect=lambda rows: track_write("external_links", rows))
    writer.write_video_collab = Mock(side_effect=lambda rows: track_write("video_collab", rows))

    return writer


@pytest.fixture
def mock_events_delta_writer():
    """Create a mock ClaimXEventsDeltaWriter for testing."""
    writer = AsyncMock(spec=ClaimXEventsDeltaWriter)

    # Track written events
    writer.written_events = []

    async def track_write_batch(events: List[Any]):
        """Track written events."""
        writer.written_events.extend(events)

    writer.write_batch = AsyncMock(side_effect=track_write_batch)

    return writer


@pytest.fixture
def sample_project_event():
    """Create a sample PROJECT_CREATED event."""
    return {
        "event_id": "evt_proj_001",
        "event_type": "PROJECT_CREATED",
        "project_id": "123",  # Use integer string as handlers expect int()
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_data": {
            "claim_number": "CLM-123",
            "policy_number": "POL-456"
        }
    }


@pytest.fixture
def sample_file_added_event():
    """Create a sample PROJECT_FILE_ADDED event."""
    return {
        "event_id": "evt_file_001",
        "event_type": "PROJECT_FILE_ADDED",
        "project_id": "123",  # Use integer string as handlers expect int()
        "media_id": "1001",  # Use integer string for media_id
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_data": {
            "file_name": "damage_photo.jpg",
            "file_size": 2048000
        }
    }
