"""
Handler for PERSONAL_PROPERTY_TASK_SHARED events.

Records contents_task_sent=True on the project when a personal property task is shared.
"""

import logging

from verisk_pipeline.claimx.claimx_models import ClaimXEvent, EntityRows
from verisk_pipeline.claimx.stages.handlers.base import NoOpHandler, register_handler
from verisk_pipeline.claimx.stages.handlers.utils import now_iso
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context

logger = get_logger(__name__)


@register_handler
class PersonalPropertyHandler(NoOpHandler):
    """Handler for PERSONAL_PROPERTY_TASK_SHARED events."""

    event_types = ["PERSONAL_PROPERTY_TASK_SHARED"]

    def extract_rows(self, event: ClaimXEvent) -> EntityRows:
        """Mark project with contents_task_sent=True."""
        rows = EntityRows()
        rows.projects.append(
            {
                "project_id": event.project_id,
                "contents_task_sent": True,
                "contents_task_at": (
                    str(event.ingested_at) if event.ingested_at else None
                ),
                "updated_at": now_iso(),
                "source_event_id": event.event_id,
            }
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "Marked contents_task_sent",
            handler_name="personal_property",
            project_id=event.project_id,
            event_id=event.event_id,
        )

        return rows
