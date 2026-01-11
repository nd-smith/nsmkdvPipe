"""
iTel Cabinet Form Parser Enrichment Handler.

Parses ClaimX task responses for iTel Cabinet Repair Form and extracts
structured data for submissions and attachments.
"""

import logging
from typing import Any, Dict

from kafka_pipeline.plugins.enrichment import EnrichmentHandler, EnrichmentContext, EnrichmentResult
from kafka_pipeline.plugins.itel_cabinet_api.handlers.form_transformer import ItelFormTransformer


class ItelFormParser(EnrichmentHandler):
    """
    Enrichment handler that parses iTel Cabinet form responses.

    Extracts:
    - Submission data → stored in data['itel_submission']
    - Attachment data → stored in data['itel_attachments']

    These can then be written to Delta tables by DeltaTableWriter.

    Configuration:
        None required - uses task data from context
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize form parser.

        Args:
            config: Handler configuration (unused)
        """
        super().__init__(config)

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """
        Parse form response from enriched task data.

        Expected data structure:
            context.data should contain 'claimx_task_details' from ClaimX API lookup

        Args:
            context: Enrichment context with task data

        Returns:
            EnrichmentResult with parsed submission and attachments
        """
        try:
            # Get the full task data from ClaimX API lookup
            task_data = context.data.get("claimx_task_details")
            if not task_data:
                self._log(
                    logging.WARNING,
                    "No ClaimX task details found in context data",
                    event_id=context.data.get("event_id"),
                )
                return EnrichmentResult.failed("Missing claimx_task_details")

            event_id = context.data.get("event_id", "unknown")
            assignment_id = task_data.get("assignmentId")

            if not assignment_id:
                self._log(
                    logging.WARNING,
                    "No assignment_id found in task data",
                    event_id=event_id,
                )
                return EnrichmentResult.failed("Missing assignment_id")

            # Parse submission data
            submission_row = ItelFormTransformer.to_submission_row(
                task_data=task_data,
                event_id=event_id,
            )

            # Parse attachments
            attachment_rows = ItelFormTransformer.to_attachment_rows(
                task_data=task_data,
                assignment_id=assignment_id,
                event_id=event_id,
            )

            # Add parsed data to context
            context.data['itel_submission'] = submission_row
            context.data['itel_attachments'] = attachment_rows

            self._log(
                logging.INFO,
                "Form parsed successfully",
                event_id=event_id,
                assignment_id=assignment_id,
                attachment_count=len(attachment_rows),
            )

            return EnrichmentResult.ok(context.data)

        except Exception as e:
            self._log_exception(
                e,
                "Failed to parse iTel form",
                event_id=context.data.get("event_id"),
            )
            return EnrichmentResult.failed(f"Form parsing error: {str(e)}")
