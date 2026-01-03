"""ClaimX domain for processing ClaimX entity events and data.

This module contains all ClaimX-specific logic including:
- Event schemas and message definitions
- API client for ClaimX service
- Event handlers for different entity types
- Workers for event ingestion, enrichment, download, and upload
- Delta table writers for ClaimX entities
"""

from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError

__all__ = [
    "ClaimXApiClient",
    "ClaimXApiError",
]
