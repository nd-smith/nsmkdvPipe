"""KQL query builder with optimization patterns for Azure Data Explorer."""

import logging
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


class KQLQueryBuilder:
    """Helper class for building optimized KQL queries.

    Enforces ADX best practices:
    - Always filter by time first (critical for performance)
    - Use column projection to reduce data transfer
    - Validate queries before execution
    """

    @staticmethod
    def incremental_query(
        table: str,
        time_column: str,
        watermark: datetime,
        columns: Optional[List[str]] = None,
        additional_filters: Optional[str] = None,
    ) -> str:
        """Build optimized incremental query with time filter.

        Args:
            table: Table name
            time_column: Column to filter by time
            watermark: Last processed timestamp (query for records after this)
            columns: Columns to project (if None, select all)
            additional_filters: Optional additional KQL filters

        Returns:
            Optimized KQL query string
        """
        # Format timestamp for KQL
        watermark_str = watermark.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Build query with time filter FIRST (critical for ADX performance)
        query_parts = [table, f"| where {time_column} > datetime({watermark_str})"]

        # Add additional filters if provided
        if additional_filters:
            query_parts.append(f"| where {additional_filters}")

        # Add column projection (reduces data transfer)
        if columns:
            columns_str = ", ".join(columns)
            query_parts.append(f"| project {columns_str}")

        query = "\n".join(query_parts)

        logger.debug(
            "Built incremental query",
            extra={
                "table": table,
                "time_column": time_column,
                "watermark": watermark_str,
                "columns_count": len(columns) if columns else "all",
            },
        )

        return query

    @staticmethod
    def time_window_query(
        table: str,
        time_column: str,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[List[str]] = None,
        aggregation: Optional[str] = None,
    ) -> str:
        """Build query for specific time window.

        Args:
            table: Table name
            time_column: Column to filter by time
            start_time: Window start (inclusive)
            end_time: Window end (exclusive)
            columns: Columns to project
            aggregation: Optional aggregation clause (e.g., "summarize count() by EventType")

        Returns:
            Optimized KQL query string
        """
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        query_parts = [
            table,
            f"| where {time_column} >= datetime({start_str}) and {time_column} < datetime({end_str})",
        ]

        if aggregation:
            query_parts.append(f"| {aggregation}")
        elif columns:
            columns_str = ", ".join(columns)
            query_parts.append(f"| project {columns_str}")

        return "\n".join(query_parts)

    @staticmethod
    def validate_query(query: str) -> tuple[bool, Optional[str]]:
        """Validate query follows best practices.

        Args:
            query: KQL query string to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for time filter (critical for ADX performance)
        has_time_filter = "where" in query.lower() and (
            "datetime(" in query.lower() or "ago(" in query.lower()
        )

        if not has_time_filter:
            return False, "Query missing time filter - required for ADX performance"

        # Check for table name
        if not query.strip():
            return False, "Empty query"

        # Additional validations can be added here

        return True, None

    @staticmethod
    def get_slow_query_threshold() -> int:
        """Get slow query threshold in seconds.

        Returns:
            Threshold in seconds (5s default)
        """
        return 5
