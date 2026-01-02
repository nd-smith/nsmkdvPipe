"""
Event transformation logic.

Flattens nested JSON event structures into tabular format for Delta tables.
"""

import json
import logging
from typing import Any, Dict, Optional, Set

import polars as pl

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_with_context

logger = get_logger(__name__)


def _safe_get(d: Optional[Dict], *keys: str, default: Any = None) -> Any:
    """
    Safely navigate nested dict keys.

    Args:
        d: Dictionary to navigate (can be None)
        *keys: Keys to traverse
        default: Default value if any key is missing

    Returns:
        Value at the nested key path, or default
    """
    if d is None:
        return default
    current = d
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _parse_data_column(data_json: Optional[str]) -> Optional[Dict]:
    """Parse JSON string to dict, handling None and errors."""
    if data_json is None:
        return None
    try:
        return json.loads(data_json)
    except (json.JSONDecodeError, TypeError):
        return None


def _extract_row_fields(data_dict: Optional[Dict]) -> Dict[str, Any]:
    """
    Extract all fields from a parsed data dict.

    Args:
        data_dict: Parsed JSON data dict (can be None)

    Returns:
        Dict with all extracted fields
    """
    if data_dict is None:
        return {
            "description": None,
            "assignment_id": None,
            "original_assignment_id": None,
            "xn_address": None,
            "carrier_id": None,
            "estimate_version": None,
            "note": None,
            "author": None,
            "sender_reviewer_name": None,
            "sender_reviewer_email": None,
            "carrier_reviewer_name": None,
            "carrier_reviewer_email": None,
            "event_datetime_mdt": None,
            "attachments": None,
            "claim_number": None,
            "contact_type": None,
            "contact_name": None,
            "contact_phone_type": None,
            "contact_phone_number": None,
            "contact_phone_extension": None,
            "contact_email_address": None,
        }

    # Extract attachments - join list to comma-separated string
    attachments_list = data_dict.get("attachments")
    attachments_str = None
    if attachments_list and isinstance(attachments_list, list):
        attachments_str = ",".join(str(a) for a in attachments_list if a)

    return {
        # Simple fields
        "description": data_dict.get("description"),
        "assignment_id": data_dict.get("assignmentId"),
        "original_assignment_id": data_dict.get("originalAssignmentId"),
        "xn_address": data_dict.get("xnAddress"),
        "carrier_id": data_dict.get("carrierId"),
        "estimate_version": data_dict.get("estimateVersion"),
        "note": data_dict.get("note"),
        "author": data_dict.get("author"),
        "sender_reviewer_name": data_dict.get("senderReviewerName"),
        "sender_reviewer_email": data_dict.get("senderReviewerEmail"),
        "carrier_reviewer_name": data_dict.get("carrierReviewerName"),
        "carrier_reviewer_email": data_dict.get("carrierReviewerEmail"),
        "event_datetime_mdt": data_dict.get("dateTime"),
        "attachments": attachments_str,
        # Nested fields
        "claim_number": _safe_get(data_dict, "adm", "coverageLoss", "claimNumber"),
        "contact_type": _safe_get(data_dict, "contact", "type"),
        "contact_name": _safe_get(data_dict, "contact", "name"),
        "contact_phone_type": _safe_get(
            data_dict, "contact", "contactMethods", "phone", "type"
        ),
        "contact_phone_number": _safe_get(
            data_dict, "contact", "contactMethods", "phone", "number"
        ),
        "contact_phone_extension": _safe_get(
            data_dict, "contact", "contactMethods", "phone", "extension"
        ),
        "contact_email_address": _safe_get(
            data_dict, "contact", "contactMethods", "email", "address"
        ),
    }


def flatten_events(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flatten events when data column is JSON string.

    This is the preferred path - handles heterogeneous schemas correctly.
    """
    # Build base columns
    base_df = df.select(
        [
            pl.col("type"),
            pl.col("type").str.split(".").list.last().alias("status_subtype"),
            pl.col("version"),
            pl.col("utcDateTime").cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            pl.col("utcDateTime")
            .cast(pl.Datetime("us", "UTC"))
            .dt.date()
            .alias("event_date"),  # ADD THIS
            pl.col("traceId").alias("trace_id"),
        ]
    )

    # Parse data column and extract fields row by row
    data_json_list = df["data"].to_list()

    # Parse all rows
    parsed_rows = [_extract_row_fields(_parse_data_column(d)) for d in data_json_list]

    # Build columns from parsed data
    extracted_columns = (
        {field: [row[field] for row in parsed_rows] for field in parsed_rows[0].keys()}
        if parsed_rows
        else {}
    )

    # Create DataFrame from extracted fields with explicit schema to prevent Null types
    # All extracted fields are strings - Delta Lake doesn't support Null type columns
    extracted_schema = {col: pl.Utf8 for col in extracted_columns.keys()}
    extracted_df = pl.DataFrame(extracted_columns, schema=extracted_schema)

    # Add raw_json column (the original data column)
    raw_json_col = df.select([pl.struct(pl.all()).cast(pl.Utf8).alias("raw_json")])

    # Combine all columns
    result = pl.concat([base_df, extracted_df, raw_json_col], how="horizontal")

    log_with_context(
        logger,
        logging.INFO,
        "Events flattened",
        rows=len(result),
        columns=len(result.columns),
    )
    return result


def _flatten_events_struct(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flatten events when data column is Struct type.

    Legacy path - kept for backwards compatibility but may have schema issues.
    """
    # Get available fields in data struct
    data_schema = df.schema.get("data")
    if data_schema is None:
        logger.warning("No 'data' column found in DataFrame")
        return df

    data_fields = (
        {f.name for f in data_schema.fields}
        if hasattr(data_schema, "fields")
        else set()
    )

    # Contact nested fields
    contact_fields = (
        _get_nested_fields(data_schema, "contact")
        if "contact" in data_fields
        else set()
    )
    phone_fields = (
        _get_nested_fields(data_schema, "contact", "contactMethods", "phone")
        if "contact" in data_fields
        else set()
    )
    email_fields = (
        _get_nested_fields(data_schema, "contact", "contactMethods", "email")
        if "contact" in data_fields
        else set()
    )

    # Build expressions list
    expressions = _build_base_expressions()
    expressions.extend(_build_data_field_expressions(data_fields))
    expressions.extend(
        _build_contact_expressions(
            data_fields, contact_fields, phone_fields, email_fields
        )
    )
    expressions.append(_build_raw_json_expression())

    df_flattened = df.select(expressions)
    logger.info(
        f"Flattened {len(df_flattened)} events with {len(df_flattened.columns)} columns"
    )

    return df_flattened


def _get_nested_fields(schema, *path) -> Set[str]:
    """
    Get field names at a nested path in a struct schema.

    Args:
        schema: Polars dtype (typically a Struct)
        *path: Path components to traverse

    Returns:
        Set of field names at the path, or empty set if path doesn't exist
    """
    current = schema
    for p in path:
        if not hasattr(current, "fields"):
            return set()
        found = None
        for f in current.fields:
            if f.name == p:
                found = f.dtype
                break
        if found is None:
            return set()
        current = found

    if hasattr(current, "fields"):
        return {f.name for f in current.fields}
    return set()


def _build_base_expressions() -> list:
    """Build expressions for base (non-nested) fields."""
    return [
        pl.col("type"),
        pl.col("type").str.split(".").list.last().alias("status_subtype"),
        pl.col("version"),
        pl.col("utcDateTime").cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        pl.col("utcDateTime")
        .cast(pl.Datetime("us", "UTC"))
        .dt.date()
        .alias("event_date"),  # ADD THIS
        pl.col("traceId").alias("trace_id"),
    ]


def _build_data_field_expressions(data_fields: Set[str]) -> list:
    """Build expressions for simple data.* fields."""
    expressions = []

    # Simple string field mappings: data field name -> output column name
    field_mappings = {
        "description": "description",
        "assignmentId": "assignment_id",
        "originalAssignmentId": "original_assignment_id",
        "xnAddress": "xn_address",
        "carrierId": "carrier_id",
        "estimateVersion": "estimate_version",
        "note": "note",
        "author": "author",
        "senderReviewerName": "sender_reviewer_name",
        "senderReviewerEmail": "sender_reviewer_email",
        "carrierReviewerName": "carrier_reviewer_name",
        "carrierReviewerEmail": "carrier_reviewer_email",
    }

    for field, alias in field_mappings.items():
        if field in data_fields:
            expressions.append(pl.col("data").struct.field(field).alias(alias))
        else:
            expressions.append(pl.lit(None).cast(pl.Utf8).alias(alias))

    # dateTime - optional timestamp
    if "dateTime" in data_fields:
        expressions.append(
            pl.col("data").struct.field("dateTime").alias("event_datetime_mdt")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("event_datetime_mdt"))

    # attachments - list of strings joined to CSV
    if "attachments" in data_fields:
        expressions.append(
            pl.col("data")
            .struct.field("attachments")
            .list.join(",")
            .alias("attachments")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("attachments"))

    # claim_number - nested in adm.coverageLoss
    if "adm" in data_fields:
        expressions.append(
            pl.col("data")
            .struct.field("adm")
            .struct.field("coverageLoss")
            .struct.field("claimNumber")
            .alias("claim_number")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("claim_number"))

    return expressions


def _build_contact_expressions(
    data_fields: Set[str],
    contact_fields: Set[str],
    phone_fields: Set[str],
    email_fields: Set[str],
) -> list:
    """Build expressions for contact nested fields."""
    expressions = []

    # contact.type
    if "contact" in data_fields and "type" in contact_fields:
        expressions.append(
            pl.col("data")
            .struct.field("contact")
            .struct.field("type")
            .alias("contact_type")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("contact_type"))

    # contact.name
    if "contact" in data_fields and "name" in contact_fields:
        expressions.append(
            pl.col("data")
            .struct.field("contact")
            .struct.field("name")
            .alias("contact_name")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("contact_name"))

    # contact.contactMethods.phone.type
    if "contact" in data_fields and "type" in phone_fields:
        expressions.append(
            pl.col("data")
            .struct.field("contact")
            .struct.field("contactMethods")
            .struct.field("phone")
            .struct.field("type")
            .alias("contact_phone_type")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("contact_phone_type"))

    # contact.contactMethods.phone.number
    if "contact" in data_fields and "number" in phone_fields:
        expressions.append(
            pl.col("data")
            .struct.field("contact")
            .struct.field("contactMethods")
            .struct.field("phone")
            .struct.field("number")
            .alias("contact_phone_number")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("contact_phone_number"))

    # contact.contactMethods.phone.extension
    if "contact" in data_fields and "extension" in phone_fields:
        expressions.append(
            pl.col("data")
            .struct.field("contact")
            .struct.field("contactMethods")
            .struct.field("phone")
            .struct.field("extension")
            .alias("contact_phone_extension")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("contact_phone_extension"))

    # contact.contactMethods.email.address
    if "contact" in data_fields and "address" in email_fields:
        expressions.append(
            pl.col("data")
            .struct.field("contact")
            .struct.field("contactMethods")
            .struct.field("email")
            .struct.field("address")
            .alias("contact_email_address")
        )
    else:
        expressions.append(pl.lit(None).cast(pl.Utf8).alias("contact_email_address"))

    return expressions


def _build_raw_json_expression() -> pl.Expr:
    """Build expression to preserve raw JSON."""
    return pl.struct(pl.all()).cast(pl.Utf8).alias("raw_json")


# Output schema for reference (all columns produced by flatten_events)
# Output schema for reference (all columns produced by flatten_events)
FLATTENED_SCHEMA = {
    "type": pl.Utf8,
    "status_subtype": pl.Utf8,
    "version": pl.Utf8,
    "ingested_at": pl.Datetime(time_zone="UTC"),
    "event_date": pl.Date,  # ADD THIS
    "trace_id": pl.Utf8,
    "description": pl.Utf8,
    "assignment_id": pl.Utf8,
    "original_assignment_id": pl.Utf8,
    "xn_address": pl.Utf8,
    "carrier_id": pl.Utf8,
    "estimate_version": pl.Utf8,
    "note": pl.Utf8,
    "author": pl.Utf8,
    "sender_reviewer_name": pl.Utf8,
    "sender_reviewer_email": pl.Utf8,
    "carrier_reviewer_name": pl.Utf8,
    "carrier_reviewer_email": pl.Utf8,
    "event_datetime_mdt": pl.Utf8,
    "attachments": pl.Utf8,
    "claim_number": pl.Utf8,
    "contact_type": pl.Utf8,
    "contact_name": pl.Utf8,
    "contact_phone_type": pl.Utf8,
    "contact_phone_number": pl.Utf8,
    "contact_phone_extension": pl.Utf8,
    "contact_email_address": pl.Utf8,
    "raw_json": pl.Utf8,
}


def get_expected_columns() -> list:
    """Get list of expected output column names."""
    return list(FLATTENED_SCHEMA.keys())
