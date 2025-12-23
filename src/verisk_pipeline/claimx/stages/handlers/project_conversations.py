"""
Handler for PROJECT_CONVERSATION_UPDATED events.

Status: NOT IMPLEMENTED - returns success with empty rows.
TODO: Implement conversation enrichment when API endpoint available.
"""

import logging

from verisk_pipeline.claimx.claimx_models import ClaimXEvent, EntityRows
from verisk_pipeline.claimx.stages.handlers.base import NoOpHandler, register_handler
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.decorators import extract_log_context
from verisk_pipeline.common.logging.utilities import log_with_context

logger = get_logger(__name__)


@register_handler
class ConversationHandler(NoOpHandler):
    """
    Handler for PROJECT_CONVERSATION_UPDATED events.

    Status: NOT IMPLEMENTED - returns success with empty rows.
    TODO: Implement conversation enrichment when API endpoint available.
    """

    event_types = ["PROJECT_CONVERSATION_UPDATED"]

    def extract_rows(self, event: ClaimXEvent) -> EntityRows:
        """Intentional no-op until API endpoint is available."""
        log_with_context(
            logger,
            logging.DEBUG,
            "Handler skipped - API not available",
            handler_name="project_conversations",
            **extract_log_context(event),
        )
        return EntityRows()
