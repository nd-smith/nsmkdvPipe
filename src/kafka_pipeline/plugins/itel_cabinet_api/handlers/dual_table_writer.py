"""
iTel Cabinet Dual Table Writer.

Writes iTel form submissions and attachments to separate Delta tables.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from kafka_pipeline.plugins.enrichment import EnrichmentHandler, EnrichmentContext, EnrichmentResult


class ItelDualTableWriter(EnrichmentHandler):
    """
    Writes iTel form data to two Delta tables:
    - claimx_itel_forms (submissions)
    - claimx_itel_attachments (media attachments)

    Configuration:
        submissions_table: Table name for submissions (default: claimx_itel_forms)
        attachments_table: Table name for attachments (default: claimx_itel_attachments)
        mode: Write mode (default: append)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize dual table writer.

        Args:
            config: Handler configuration
        """
        super().__init__(config)
        self.submissions_table = config.get("submissions_table", "claimx_itel_forms")
        self.attachments_table = config.get("attachments_table", "claimx_itel_attachments")
        self.mode = config.get("mode", "append")

        # Initialize Spark session (lazy)
        self._spark = None

    @property
    def spark(self):
        """Get or create Spark session."""
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """
        Write submission and attachments to Delta tables.

        Expected data structure:
            context.data['itel_submission'] - Submission row dict
            context.data['itel_attachments'] - List of attachment row dicts

        Args:
            context: Enrichment context with parsed form data

        Returns:
            EnrichmentResult indicating success/failure
        """
        try:
            submission_row = context.data.get("itel_submission")
            attachment_rows = context.data.get("itel_attachments", [])

            if not submission_row:
                self._log(
                    logging.WARNING,
                    "No submission data found in context",
                    event_id=context.data.get("event_id"),
                )
                return EnrichmentResult.failed("Missing itel_submission data")

            event_id = context.data.get("event_id", "unknown")
            assignment_id = submission_row.get("assignment_id")

            # Write submission
            submission_written = self._write_submission(submission_row, event_id)

            # Write attachments (if any)
            attachments_written = 0
            if attachment_rows:
                attachments_written = self._write_attachments(attachment_rows, event_id)

            self._log(
                logging.INFO,
                "Delta write complete",
                event_id=event_id,
                assignment_id=assignment_id,
                submissions_written=1 if submission_written else 0,
                attachments_written=attachments_written,
            )

            return EnrichmentResult.ok(context.data)

        except Exception as e:
            self._log_exception(
                e,
                "Failed to write to Delta tables",
                event_id=context.data.get("event_id"),
            )
            return EnrichmentResult.failed(f"Delta write error: {str(e)}")

    def _write_submission(self, submission_row: Dict[str, Any], event_id: str) -> bool:
        """
        Write submission row to Delta table.

        Args:
            submission_row: Submission data
            event_id: Event ID for logging

        Returns:
            True if successful
        """
        try:
            # Create DataFrame from single row
            df = self.spark.createDataFrame([submission_row])

            # Write to Delta table
            df.write.format("delta") \
                .mode(self.mode) \
                .saveAsTable(self.submissions_table)

            self._log(
                logging.DEBUG,
                f"Wrote submission to {self.submissions_table}",
                event_id=event_id,
                assignment_id=submission_row.get("assignment_id"),
            )

            return True

        except Exception as e:
            self._log_exception(
                e,
                f"Failed to write submission to {self.submissions_table}",
                event_id=event_id,
            )
            raise

    def _write_attachments(self, attachment_rows: List[Dict[str, Any]], event_id: str) -> int:
        """
        Write attachment rows to Delta table.

        Args:
            attachment_rows: List of attachment dicts
            event_id: Event ID for logging

        Returns:
            Number of rows written
        """
        try:
            if not attachment_rows:
                return 0

            # Create DataFrame from rows
            df = self.spark.createDataFrame(attachment_rows)

            # Write to Delta table
            df.write.format("delta") \
                .mode(self.mode) \
                .saveAsTable(self.attachments_table)

            self._log(
                logging.DEBUG,
                f"Wrote {len(attachment_rows)} attachments to {self.attachments_table}",
                event_id=event_id,
                attachment_count=len(attachment_rows),
            )

            return len(attachment_rows)

        except Exception as e:
            self._log_exception(
                e,
                f"Failed to write attachments to {self.attachments_table}",
                event_id=event_id,
            )
            raise
