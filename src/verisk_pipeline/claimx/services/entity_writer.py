"""
Entity and event log writing services.

Handles writes to all entity Delta tables and event log tracking.
"""

from __future__ import annotations

import gc
import logging
import os
from typing import Dict, List

import polars as pl
import psutil

from verisk_pipeline.common.config.claimx import ClaimXConfig
from verisk_pipeline.claimx.claimx_models import EntityRows
from verisk_pipeline.storage.delta import DeltaTableWriter
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_exception, log_with_context

logger = get_logger(__name__)


def _get_memory_mb():
    """Get current process memory usage in MB."""
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024


# Merge keys for each entity table
MERGE_KEYS: Dict[str, List[str]] = {
    "projects": ["project_id"],
    "contacts": ["project_id", "contact_email", "contact_type"],
    "media": ["media_id"],
    "tasks": ["assignment_id"],
    "task_templates": ["task_id"],
    "external_links": ["link_id"],
    "video_collab": ["video_collaboration_id"],
}

# Explicit schemas for entity tables to prevent Null type inference
ENTITY_SCHEMAS: Dict[str, Dict[str, pl.DataType]] = {
    "projects": {
        "project_id": pl.Utf8,
        "project_number": pl.Utf8,
        "master_file_name": pl.Utf8,
        "secondary_number": pl.Utf8,
        "created_date": pl.Utf8,
        "status": pl.Utf8,
        "date_of_loss": pl.Utf8,
        "type_of_loss": pl.Utf8,
        "cause_of_loss": pl.Utf8,
        "coverages": pl.Utf8,
        "loss_description": pl.Utf8,
        "customer_first_name": pl.Utf8,
        "customer_last_name": pl.Utf8,
        "custom_business_name": pl.Utf8,
        "business_line_type": pl.Utf8,
        "year_built": pl.Int64,
        "square_footage": pl.Int64,
        "street1": pl.Utf8,
        "street2": pl.Utf8,
        "city": pl.Utf8,
        "state_province": pl.Utf8,
        "zip_postcode": pl.Utf8,
        "county": pl.Utf8,
        "country": pl.Utf8,
        "primary_email": pl.Utf8,
        "primary_phone": pl.Utf8,
        "primary_phone_country_code": pl.Int64,
        "date_received": pl.Utf8,
        "date_contacted": pl.Utf8,
        "planned_inspection_date": pl.Utf8,
        "date_inspected": pl.Utf8,
        "appointment_date": pl.Utf8,
        "custom_attribute1": pl.Utf8,
        "custom_attribute2": pl.Utf8,
        "custom_attribute3": pl.Utf8,
        "custom_external_unique_id": pl.Utf8,
        "company_name": pl.Utf8,
        "source_event_id": pl.Utf8,
        "created_at": pl.Utf8,
        "updated_at": pl.Utf8,
        "xa_autolink_fail": pl.Boolean,
        "xa_autolink_fail_at": pl.Utf8,
        "contents_task_sent": pl.Boolean,
        "contents_task_at": pl.Utf8,
    },
    "contacts": {
        "project_id": pl.Utf8,
        "contact_email": pl.Utf8,
        "contact_type": pl.Utf8,
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "phone_number": pl.Utf8,
        "phone_country_code": pl.Int64,
        "is_primary_contact": pl.Boolean,
        "master_file_name": pl.Utf8,
        "source_event_id": pl.Utf8,
        "created_at": pl.Utf8,
        "updated_at": pl.Utf8,
        "created_date": pl.Utf8,
    },
    "tasks": {
        "assignment_id": pl.Int64,
        "task_id": pl.Int64,
        "project_id": pl.Utf8,
        "assignee_id": pl.Int64,
        "assignor_id": pl.Int64,
        "date_assigned": pl.Utf8,
        "date_completed": pl.Utf8,
        "cancelled_date": pl.Utf8,
        "cancelled_by_resource_id": pl.Int64,
        "status": pl.Utf8,
        "pdf_project_media_id": pl.Int64,
        "date_exported": pl.Utf8,
        "form_response_id": pl.Utf8,
        "stp_enabled": pl.Boolean,
        "stp_started_date": pl.Utf8,
        "mfn": pl.Utf8,
        "source_event_id": pl.Utf8,
        "created_at": pl.Utf8,
        "updated_at": pl.Utf8,
        "last_enriched_at": pl.Utf8,
    },
    "task_templates": {
        "task_id": pl.Int64,
        "comp_id": pl.Int64,
        "name": pl.Utf8,
        "description": pl.Utf8,
        "form_id": pl.Utf8,
        "form_name": pl.Utf8,
        "enabled": pl.Boolean,
        "is_default": pl.Boolean,
        "is_manual_delivery": pl.Boolean,
        "is_external_link_delivery": pl.Boolean,
        "provide_portal_access": pl.Boolean,
        "notify_assigned_send_recipient": pl.Boolean,
        "notify_assigned_send_recipient_sms": pl.Boolean,
        "notify_assigned_subject": pl.Utf8,
        "notify_task_completed": pl.Boolean,
        "notify_completed_subject": pl.Utf8,
        "allow_resubmit": pl.Boolean,
        "auto_generate_pdf": pl.Boolean,
        "modified_by": pl.Utf8,
        "modified_by_id": pl.Int64,
        "modified_date": pl.Utf8,
        "source_event_id": pl.Utf8,
        "created_at": pl.Utf8,
        "updated_at": pl.Utf8,
        "last_enriched_at": pl.Utf8,
    },
    "external_links": {
        "link_id": pl.Int64,
        "assignment_id": pl.Int64,
        "project_id": pl.Utf8,
        "link_code": pl.Utf8,
        "url": pl.Utf8,
        "notification_access_method": pl.Utf8,
        "country_id": pl.Int64,
        "state_id": pl.Int64,
        "created_date": pl.Utf8,
        "accessed_count": pl.Int64,
        "last_accessed": pl.Utf8,
        "source_event_id": pl.Utf8,
        "created_at": pl.Utf8,
        "updated_at": pl.Utf8,
    },
    "media": {
        "media_id": pl.Utf8,
        "project_id": pl.Utf8,
        "file_type": pl.Utf8,
        "file_name": pl.Utf8,
        "media_description": pl.Utf8,
        "media_comment": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "gps_source": pl.Utf8,
        "taken_date": pl.Utf8,
        "full_download_link": pl.Utf8,
        "source_event_id": pl.Utf8,
        "created_at": pl.Utf8,
        "updated_at": pl.Utf8,
        "last_enriched_at": pl.Utf8,
    },
}


class EventLogWriter(DeltaTableWriter):
    """
    Writer for claimx_event_log table.

    Tracks processing status for each event.
    """

    SCHEMA = {
        "event_id": pl.Utf8,
        "event_type": pl.Utf8,
        "project_id": pl.Utf8,
        "status": pl.Utf8,
        "error_message": pl.Utf8,
        "error_category": pl.Utf8,
        "api_calls": pl.Int32,
        "duration_ms": pl.Int32,
        "retry_count": pl.Int32,
        "processed_at": pl.Utf8,
        "processed_date": pl.Utf8,
    }

    def __init__(self, table_path: str):
        super().__init__(table_path, dedupe_column=None)

    def write_event_logs(self, rows: List[Dict]) -> int:
        """Write event log rows with schema enforcement."""
        if not rows:
            return 0
        result = self.write_rows(rows, schema=self.SCHEMA)
        return result if result is not None else 0


class EntityTableWriter:
    """
    Manages writes to all entity tables.

    Uses merge with merge keys for idempotency.
    """

    def __init__(self, config: ClaimXConfig):
        self.config = config
        lakehouse = config.lakehouse

        # Writers for each entity table (dedupe_column not needed - using merge)
        self._writers: Dict[str, DeltaTableWriter] = {
            "projects": DeltaTableWriter(
                lakehouse.table_path(lakehouse.projects_table),
                dedupe_column=None,
            ),
            "contacts": DeltaTableWriter(
                lakehouse.table_path(lakehouse.contacts_table),
                dedupe_column=None,
            ),
            "media": DeltaTableWriter(
                lakehouse.table_path(lakehouse.media_metadata_table),
                dedupe_column=None,
            ),
            "tasks": DeltaTableWriter(
                lakehouse.table_path(lakehouse.tasks_table),
                dedupe_column=None,
            ),
            "task_templates": DeltaTableWriter(
                lakehouse.table_path(lakehouse.task_templates_table),
                dedupe_column=None,
            ),
            "external_links": DeltaTableWriter(
                lakehouse.table_path(lakehouse.external_links_table),
                dedupe_column=None,
            ),
            "video_collab": DeltaTableWriter(
                lakehouse.table_path(lakehouse.video_collab_table),
                dedupe_column=None,
            ),
        }

        log_with_context(
            logger,
            logging.DEBUG,
            "EntityTableWriter initialized",
            tables=list(self._writers.keys()),
        )

    def write_all(self, rows: EntityRows) -> Dict[str, int]:
        """
        Write all entity rows to their respective tables.

        Args:
            rows: EntityRows with data for each table

        Returns:
            Dict mapping table name to rows written
        """

        counts: Dict[str, int] = {}

        if rows.projects:
            counts["projects"] = self._write_table("projects", rows.projects)
            rows.projects = []  # Free the source data
            gc.collect()

        if rows.contacts:
            counts["contacts"] = self._write_table("contacts", rows.contacts)
            rows.contacts = []
            gc.collect()

        if rows.media:
            counts["media"] = self._write_table("media", rows.media)
            rows.media = []
            gc.collect()

        if rows.tasks:
            counts["tasks"] = self._write_table("tasks", rows.tasks)
            rows.tasks = []
            gc.collect()

        if rows.task_templates:
            counts["task_templates"] = self._write_table(
                "task_templates", rows.task_templates
            )
            rows.task_templates = []
            gc.collect()

        if rows.external_links:
            counts["external_links"] = self._write_table(
                "external_links", rows.external_links
            )
            rows.external_links = []
            gc.collect()

        if rows.video_collab:
            counts["video_collab"] = self._write_table(
                "video_collab", rows.video_collab
            )
            rows.video_collab = []
            gc.collect()

        log_with_context(
            logger,
            logging.DEBUG,
            "Entity tables write summary",
            tables_written=counts,
            total_rows=sum(counts.values()),
        )

        return counts

    def _write_table(self, name: str, rows: List[Dict]) -> int:
        """Write rows to a table using merge or append."""
        if not rows:
            return 0

        mem_start = _get_memory_mb()
        log_with_context(
            logger,
            logging.DEBUG,
            "Writing entity table",
            table_name=name,
            rows_count=len(rows),
            memory_mb_start=round(mem_start, 1),
        )

        writer = self._writers.get(name)
        if not writer:
            log_with_context(
                logger, logging.WARNING, "No writer for table", table_name=name
            )
            return 0

        merge_keys = MERGE_KEYS.get(name)
        if not merge_keys:
            log_with_context(
                logger,
                logging.WARNING,
                "No merge keys defined for table",
                table_name=name,
            )
            return 0

        try:
            # Get columns actually present in the rows
            present_cols = set()
            for row in rows:
                present_cols.update(row.keys())

            # Only apply schema for columns that exist in the data
            schema = ENTITY_SCHEMAS.get(name)
            if schema:
                filtered_schema = {k: v for k, v in schema.items() if k in present_cols}
                df = pl.DataFrame(rows, schema=filtered_schema)
            else:
                df = pl.DataFrame(rows)

            mem_pre_write = _get_memory_mb()
            # Contacts: append-only (new contacts from new projects/events)
            if name == "contacts":
                result: int = writer.append(df, dedupe=False)  # type: ignore[assignment]
            elif name == "media":
                result: int = writer.append(df, dedupe=True)  # type: ignore[assignment]
            else:
                # Other tables: merge (upsert)
                result = writer.merge(df, merge_keys=merge_keys)  # type: ignore[assignment]

            mem_post_write = _get_memory_mb()
            log_with_context(
                logger,
                logging.DEBUG,
                "Entity table write complete",
                table_name=name,
                rows_written=result,
                memory_mb_delta=round(mem_post_write - mem_pre_write, 1),
            )
            # Free DataFrame immediately after write
            del df

            return result
        except Exception as e:
            log_exception(logger, e, "Error writing to table", table_name=name)
            return 0
