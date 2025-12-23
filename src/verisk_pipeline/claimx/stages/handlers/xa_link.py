"""
Handler for PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL events.

Records xa_autolink_fail=True on the project. Project existence is ensured
by the enrich stage's _ensure_projects_exist() before this handler runs.
"""

import logging

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context
from verisk_pipeline.claimx.claimx_models import ClaimXEvent, EntityRows
from verisk_pipeline.claimx.stages.handlers.base import NoOpHandler, register_handler
from verisk_pipeline.claimx.stages.handlers.utils import now_iso

logger = get_logger(__name__)


@register_handler
class XaLinkFailHandler(NoOpHandler):
    """Handler for PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL events."""

    event_types = ["PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL"]

    def extract_rows(self, event: ClaimXEvent) -> EntityRows:
        """Mark project with xa_autolink_fail=True."""
        rows = EntityRows()
        rows.projects.append(
            {
                "project_id": event.project_id,
                "xa_autolink_fail": True,
                "xa_autolink_fail_at": (
                    str(event.ingested_at) if event.ingested_at else None
                ),
                "updated_at": now_iso(),
                "source_event_id": event.event_id,
            }
        )

        log_with_context(
            logger,
            logging.DEBUG,
            "Marked xa_autolink_fail",
            handler_name="xa_link",
            project_id=event.project_id,
            event_id=event.event_id,
        )

        return rows
