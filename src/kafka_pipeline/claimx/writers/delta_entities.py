"""
Delta Lake writer for ClaimX entity tables.

Writes ClaimX entity data to 7 separate Delta tables:
- claimx_projects: Project metadata
- claimx_contacts: Contact/policyholder information
- claimx_attachment_metadata: Attachment metadata
- claimx_tasks: Task information
- claimx_task_templates: Task template definitions
- claimx_external_links: External resource links
- claimx_video_collab: Video collaboration sessions

Uses merge (upsert) operations with appropriate primary keys for idempotency.
"""

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import polars as pl

from core.logging.setup import get_logger
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.common.writers.base import BaseDeltaWriter


# Schema definitions for contacts table to ensure proper type casting
# This prevents null type inference when all values in a column are None
CONTACTS_SCHEMA = {
    "project_id": pl.Utf8,
    "contact_email": pl.Utf8,
    "contact_type": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "phone_number": pl.Utf8,
    "phone_country_code": pl.Int64,
    "is_primary_contact": pl.Boolean,
    "master_file_name": pl.Utf8,
    "task_assignment_id": pl.Int32,
    "video_collaboration_id": pl.Utf8,
    "source_event_id": pl.Utf8,
    "created_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Utf8,
    "created_date": pl.Date,
    "last_enriched_at": pl.Datetime("us", "UTC"),
}


# Merge keys for each entity table (from verisk_pipeline)
MERGE_KEYS: Dict[str, List[str]] = {
    "projects": ["project_id"],
    "contacts": ["project_id", "contact_email", "contact_type"],
    "media": ["media_id"],
    "tasks": ["assignment_id"],
    "task_templates": ["task_id"],
    "external_links": ["link_id"],
    "video_collab": ["video_collaboration_id"],
}


class ClaimXEntityWriter:
    """
    Manages writes to all ClaimX entity Delta tables.

    Uses merge operations with merge keys for idempotency.
    Each entity type is written to its own Delta table with appropriate merge keys.

    Entity Tables:
        - projects → claimx_projects (merge key: project_id)
        - contacts → claimx_contacts (merge keys: project_id, contact_email, contact_type)
        - media → claimx_attachment_metadata (merge key: media_id)
        - tasks → claimx_tasks (merge key: assignment_id)
        - task_templates → claimx_task_templates (merge key: task_id)
        - external_links → claimx_external_links (merge key: link_id)
        - video_collab → claimx_video_collab (merge key: video_collaboration_id)

    Usage:
        >>> writer = ClaimXEntityWriter(
        ...     projects_table_path="abfss://.../claimx_projects",
        ...     contacts_table_path="abfss://.../claimx_contacts",
        ...     # ... other table paths
        ... )
        >>> entity_rows = EntityRowsMessage(projects=[...], contacts=[...])
        >>> await writer.write_all(entity_rows)
    """

    def __init__(
        self,
        projects_table_path: str,
        contacts_table_path: str,
        media_table_path: str,
        tasks_table_path: str,
        task_templates_table_path: str,
        external_links_table_path: str,
        video_collab_table_path: str,
    ):
        """
        Initialize ClaimX entity writer with table paths.

        Args:
            projects_table_path: Full abfss:// path to claimx_projects table
            contacts_table_path: Full abfss:// path to claimx_contacts table
            media_table_path: Full abfss:// path to claimx_attachment_metadata table
            tasks_table_path: Full abfss:// path to claimx_tasks table
            task_templates_table_path: Full abfss:// path to claimx_task_templates table
            external_links_table_path: Full abfss:// path to claimx_external_links table
            video_collab_table_path: Full abfss:// path to claimx_video_collab table
        """
        self.logger = get_logger(self.__class__.__name__)

        # Create individual writers for each entity table
        # Projects and Media are partitioned by project_id
        # Contacts and others get default partitioning (likely event_date or None depending on base)
        # Note: BaseDeltaWriter defaults to partition_column="event_date"
        self._writers: Dict[str, BaseDeltaWriter] = {
            "projects": BaseDeltaWriter(
                table_path=projects_table_path,
                partition_column="project_id",
            ),
            "contacts": BaseDeltaWriter(
                table_path=contacts_table_path,
                # Contacts not partitioned by project_id (user specified)
            ),
            "media": BaseDeltaWriter(
                table_path=media_table_path,
                partition_column="project_id",
            ),
            "tasks": BaseDeltaWriter(
                table_path=tasks_table_path,
            ),
            "task_templates": BaseDeltaWriter(
                table_path=task_templates_table_path,
            ),
            "external_links": BaseDeltaWriter(
                table_path=external_links_table_path,
            ),
            "video_collab": BaseDeltaWriter(
                table_path=video_collab_table_path,
            ),
        }

        self.logger.info(
            "Initialized ClaimXEntityWriter",
            extra={
                "tables": list(self._writers.keys()),
            },
        )

    async def write_all(self, entity_rows: EntityRowsMessage) -> Dict[str, int]:
        """
        Write all entity rows to their respective Delta tables.

        Uses merge (upsert) operations for all tables except contacts (append-only).

        Args:
            entity_rows: EntityRowsMessage with data for each table

        Returns:
            Dict mapping table name to rows written
        """
        counts: Dict[str, int] = {}

        # Process each entity type
        if entity_rows.projects:
            result = await self._write_table(
                "projects",
                entity_rows.projects,
            )
            if result is not None:
                counts["projects"] = result

        if entity_rows.contacts:
            result = await self._write_table(
                "contacts",
                entity_rows.contacts,
            )
            if result is not None:
                counts["contacts"] = result

        if entity_rows.media:
            result = await self._write_table(
                "media",
                entity_rows.media,
            )
            if result is not None:
                counts["media"] = result

        if entity_rows.tasks:
            result = await self._write_table(
                "tasks",
                entity_rows.tasks,
            )
            if result is not None:
                counts["tasks"] = result

        if entity_rows.task_templates:
            result = await self._write_table(
                "task_templates",
                entity_rows.task_templates,
            )
            if result is not None:
                counts["task_templates"] = result

        if entity_rows.external_links:
            result = await self._write_table(
                "external_links",
                entity_rows.external_links,
            )
            if result is not None:
                counts["external_links"] = result

        if entity_rows.video_collab:
            result = await self._write_table(
                "video_collab",
                entity_rows.video_collab,
            )
            if result is not None:
                counts["video_collab"] = result

        self.logger.info(
            f"Write cycle complete: {sum(counts.values())} total rows across {len(counts)} tables",
            extra={
                "tables_written": counts,
                "total_rows": sum(counts.values()),
                "table_count": len(counts),
            },
        )

        return counts

    async def _write_table(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> Optional[int]:
        """
        Write rows to a specific entity table using merge or append.

        Args:
            table_name: Name of the entity table
            rows: List of row dicts to write

        Returns:
            Number of rows affected, or None on error
        """
        if not rows:
            return 0

        self.logger.debug(
            f"Writing {len(rows)} rows to {table_name}",
            extra={
                "table_name": table_name,
                "row_count": len(rows),
            },
        )

        writer = self._writers.get(table_name)
        if not writer:
            self.logger.warning(
                f"No writer configured for table: {table_name}",
                extra={"table_name": table_name},
            )
            return None

        merge_keys = MERGE_KEYS.get(table_name)
        if not merge_keys:
            self.logger.warning(
                f"No merge keys defined for table: {table_name}",
                extra={"table_name": table_name},
            )
            return None

        try:
            # Create DataFrame from rows
            df = pl.DataFrame(rows)

            # Add created_at and updated_at timestamps if not present
            now = datetime.now(timezone.utc)
            if "created_at" not in df.columns:
                df = df.with_columns(pl.lit(now).alias("created_at"))
            if "updated_at" not in df.columns:
                df = df.with_columns(pl.lit(now).alias("updated_at"))

            # Apply schema casting for contacts table to avoid null type inference
            if table_name == "contacts":
                df = self._cast_contacts_schema(df, now)

            # Contacts: append-only (new contacts from new projects/events)
            # Media: append-only (new media from new events)
            # Other tables: merge (upsert)
            # Note: Deduplication handled by daily Fabric maintenance job
            if table_name == "contacts":
                # Contacts are append-only
                success = await writer._async_append(df)
                rows_affected = len(df) if success else 0
            elif table_name == "media":
                # Media is append-only
                success = await writer._async_append(df)
                rows_affected = len(df) if success else 0
            else:
                # Other tables use merge (upsert)
                # Preserve created_at on updates
                success = await writer._async_merge(
                    df,
                    merge_keys=merge_keys,
                    preserve_columns=["created_at"],
                )
                rows_affected = len(df) if success else 0

            if success:
                self.logger.info(
                    f"Wrote {rows_affected} rows to {table_name}",
                    extra={
                        "table_name": table_name,
                        "rows_written": rows_affected,
                    },
                )
                return rows_affected
            else:
                self.logger.error(
                    f"{table_name} table write failed",
                    extra={
                        "table_name": table_name,
                        "row_count": len(rows),
                    },
                )
                return None

        except Exception as e:
            self.logger.error(
                f"Error writing to {table_name} table",
                extra={
                    "table_name": table_name,
                    "row_count": len(rows),
                    "error": str(e),
                },
                exc_info=True,
            )
            return None

    def _cast_contacts_schema(
        self, df: pl.DataFrame, now: datetime
    ) -> pl.DataFrame:
        """
        Cast contacts DataFrame columns to match Delta table schema.

        Ensures proper type casting to avoid null type inference when
        columns contain only None values. Also adds missing required columns.

        Args:
            df: Input DataFrame with contact rows
            now: Current timestamp for last_enriched_at

        Returns:
            DataFrame with properly typed columns
        """
        # Add last_enriched_at if not present (required by table schema)
        if "last_enriched_at" not in df.columns:
            df = df.with_columns(pl.lit(now).alias("last_enriched_at"))

        # Convert updated_at to string if it's a datetime (table expects string)
        if "updated_at" in df.columns and df["updated_at"].dtype != pl.Utf8:
            df = df.with_columns(
                pl.col("updated_at").cast(pl.Utf8)
            )

        # Cast columns to their expected types to avoid null type inference
        cast_exprs = []
        for col_name, col_type in CONTACTS_SCHEMA.items():
            if col_name in df.columns:
                current_dtype = df[col_name].dtype
                # Only cast if needed (skip if already correct type or if it's a datetime)
                if current_dtype == pl.Null:
                    # Column is all nulls - cast to expected type
                    cast_exprs.append(pl.col(col_name).cast(col_type))
                elif current_dtype != col_type:
                    # Handle string -> datetime conversion (ISO format timestamps)
                    if col_type == pl.Datetime("us", "UTC") and current_dtype == pl.Utf8:
                        # Parse ISO format string to datetime
                        cast_exprs.append(
                            pl.col(col_name)
                            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", time_zone="UTC", strict=False)
                            .alias(col_name)
                        )
                    # Handle datetime -> timestamp conversion
                    elif col_type == pl.Datetime("us", "UTC") and current_dtype in (
                        pl.Datetime,
                        pl.Datetime("us"),
                        pl.Datetime("us", "UTC"),
                    ):
                        # Already a datetime, just ensure proper timezone
                        if current_dtype != pl.Datetime("us", "UTC"):
                            cast_exprs.append(
                                pl.col(col_name).dt.replace_time_zone("UTC").alias(col_name)
                            )
                    elif col_type == pl.Date and current_dtype == pl.Date:
                        # Already date type
                        pass
                    else:
                        cast_exprs.append(pl.col(col_name).cast(col_type))

        if cast_exprs:
            df = df.with_columns(cast_exprs)

        return df


__all__ = ["ClaimXEntityWriter", "MERGE_KEYS"]
