"""
ClaimX event handlers for Kafka pipeline.

Provides handler classes for processing ClaimX events and enriching them with
data from the ClaimX API.

Handler Types:
    - ProjectHandler: Handles PROJECT_CREATED, PROJECT_MFN_ADDED events
    - MediaHandler: Handles PROJECT_FILE_ADDED events
    - TaskHandler: Handles CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED events
    - PolicyholderHandler: Handles POLICYHOLDER_INVITED, POLICYHOLDER_JOINED events
    - VideoCollabHandler: Handles VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED events

Base Classes:
    - EventHandler: Abstract base class for all handlers
    - NoOpHandler: Base for handlers that don't need API calls
    - HandlerRegistry: Registry for routing events to handlers

Usage:
    >>> from kafka_pipeline.claimx.api_client import ClaimXApiClient
    >>> from kafka_pipeline.claimx.handlers import get_handler_registry
    >>>
    >>> # Handlers are auto-registered via @register_handler decorator
    >>> registry = get_handler_registry()
    >>> handler = registry.get_handler("PROJECT_CREATED", api_client)
    >>> result = await handler.handle_event(event)
"""

# Import base classes
from kafka_pipeline.claimx.handlers.base import (
    EventHandler,
    NoOpHandler,
    EnrichmentResult,
    HandlerResult,
    HandlerRegistry,
    get_handler_registry,
    reset_registry,
    register_handler,
)

# Import specific handlers to trigger registration
from kafka_pipeline.claimx.handlers.project import ProjectHandler
from kafka_pipeline.claimx.handlers.media import MediaHandler
from kafka_pipeline.claimx.handlers.task import TaskHandler
from kafka_pipeline.claimx.handlers.contact import PolicyholderHandler
from kafka_pipeline.claimx.handlers.video import VideoCollabHandler

__all__ = [
    # Base classes
    "EventHandler",
    "NoOpHandler",
    "EnrichmentResult",
    "HandlerResult",
    "HandlerRegistry",
    "get_handler_registry",
    "reset_registry",
    "register_handler",
    # Specific handlers
    "ProjectHandler",
    "MediaHandler",
    "TaskHandler",
    "PolicyholderHandler",
    "VideoCollabHandler",
]
