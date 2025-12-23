"""
Policyholder contact event handler.

Handles: POLICYHOLDER_INVITED, POLICYHOLDER_JOINED
"""

import logging
from datetime import datetime, timezone

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context
from verisk_pipeline.claimx.claimx_models import (
    ClaimXEvent,
    EntityRows,
)
from verisk_pipeline.claimx.stages.handlers.base import (
    NoOpHandler,
    register_handler,
)

logger = get_logger(__name__)


@register_handler
class PolicyholderHandler(NoOpHandler):
    """
    Handler for policyholder events.

    Updates projects table with invite/join timestamps.
    No contact record created (no reliable email in these events).
    """

    event_types = ["POLICYHOLDER_INVITED", "POLICYHOLDER_JOINED"]

    def extract_rows(self, event: ClaimXEvent) -> EntityRows:
        rows = EntityRows()
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
