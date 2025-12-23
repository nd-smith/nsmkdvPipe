"""
ClaimX service layer.

Provides focused services with single responsibilities:
- ProcessedEventCache: Processed event tracking
- EntityWriter: Entity and event log writing
- PathResolver: Domain-specific OneLake path generation

These services are extracted to improve maintainability and testability
through separation of concerns.
"""

from verisk_pipeline.claimx.services.event_cache import ProcessedEventCache
from verisk_pipeline.claimx.services.entity_writer import (
    EventLogWriter,
    EntityTableWriter,
    MERGE_KEYS,
    ENTITY_SCHEMAS,
)
from verisk_pipeline.claimx.services.path_resolver import (
    generate_blob_path,
    get_onelake_path_for_event,
)

__all__ = [
    # Event tracking
    "ProcessedEventCache",
    # Entity writing
    "EventLogWriter",
    "EntityTableWriter",
    "MERGE_KEYS",
    "ENTITY_SCHEMAS",
    # Path resolution
    "generate_blob_path",
    "get_onelake_path_for_event",
]
