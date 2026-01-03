"""
Policyholder contact event handler.

Handles: POLICYHOLDER_INVITED, POLICYHOLDER_JOINED
"""

import logging
from datetime import datetime, timezone

from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.handlers.base import (
    NoOpHandler,
    register_handler,
)

from kafka_pipeline.common.logging import get_logger, log_with_context

logger = get_logger(__name__)


@register_handler
class PolicyholderHandler(NoOpHandler):
    """
    Handler for policyholder events.

    Updates projects table with invite/join timestamps.
    No contact record created (no reliable email in these events).
    """

    event_types = ["POLICYHOLDER_INVITED", "POLICYHOLDER_JOINED"]

    def extract_rows(self, event: ClaimXEventMessage) -> EntityRowsMessage:
        """Extract project update from policyholder event."""
        rows = EntityRowsMessage()
        now = datetime.now(timezone.utc).isoformat()

        project_row = {
            "project_id": str(event.project_id),
            "policyholder_invited_at": (
                now if event.event_type == "POLICYHOLDER_INVITED" else None
            ),
            "policyholder_joined_at": (
                now if event.event_type == "POLICYHOLDER_JOINED" else None
            ),
        }
        rows.projects.append(project_row)

        log_with_context(
            logger,
            logging.DEBUG,
            "Policyholder event processed",
            handler_name="contact",
            event_type=event.event_type,
            project_id=event.project_id,
            event_id=event.event_id,
        )

        return rows
