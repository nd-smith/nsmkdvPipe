"""
ClaimX-specific path resolution for OneLake storage.

Provides domain-specific path generation rules for different event types:
- documentsReceived
- firstNoticeOfLossReceived (FNOL)
- estimatePackageReceived

This logic is separated from storage layer to maintain separation of concerns.
"""

from typing import Optional, Tuple

from verisk_pipeline.common.security import extract_filename_from_url


def generate_blob_path(
    status_subtype: str,
    trace_id: str,
    assignment_id: str,
    download_url: str,
    estimate_version: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Generate blob path based on ClaimX status_subtype bucket rules.

    Args:
        status_subtype: Event status subtype (determines folder)
        trace_id: Unique trace ID
        assignment_id: Assignment identifier
        download_url: Source URL for filename extraction
        estimate_version: Optional estimate version for estimate packages

    Returns:
        Tuple of (blob_path, file_type)
        blob_path format: {status_subtype}/{trace_id}/{filename}

    Examples:
        >>> generate_blob_path(
        ...     "documentsReceived", "abc123", "claim-456",
        ...     "https://example.com/report.pdf"
        ... )
        ('documentsReceived/abc123/claim-456_report.pdf', 'PDF')

        >>> generate_blob_path(
        ...     "firstNoticeOfLossReceived", "xyz789", "claim-789",
        ...     "https://example.com/fnol.pdf"
        ... )
        ('firstNoticeOfLossReceived/xyz789/claim-789_FNOL_fnol.pdf', 'PDF')
    """
    url_filename, file_type = extract_filename_from_url(download_url)

    # Build filename based on bucket type
    if status_subtype == "documentsReceived":
        filename = f"{assignment_id}_{url_filename}"

    elif status_subtype == "firstNoticeOfLossReceived":
        filename = f"{assignment_id}_FNOL_{url_filename}"

    elif status_subtype == "estimatePackageReceived":
        version = estimate_version or "unknown"
        filename = f"{assignment_id}_{version}_{url_filename}"

    else:
        # Default pattern for unknown subtypes
        filename = f"{assignment_id}_{url_filename}"

    blob_path = f"{status_subtype}/{trace_id}/{filename}"
    return blob_path, file_type


def get_onelake_path_for_event(
    event_type: str,
    trace_id: str,
    assignment_id: str,
    download_url: str,
    base_path: str,
    estimate_version: Optional[str] = None,
) -> str:
    """
    Generate full OneLake path for a ClaimX event.

    Combines base path with domain-specific path rules.

    Args:
        event_type: ClaimX event type (status_subtype)
        trace_id: Unique trace ID
        assignment_id: Assignment identifier
        download_url: Source URL
        base_path: Base OneLake path (e.g., "abfss://workspace@onelake/lakehouse/Files")
        estimate_version: Optional estimate version

    Returns:
        Full OneLake path including base and domain-specific components

    Example:
        >>> get_onelake_path_for_event(
        ...     "documentsReceived", "abc123", "claim-456",
        ...     "https://example.com/report.pdf",
        ...     "abfss://ws@onelake/lakehouse/Files"
        ... )
        'abfss://ws@onelake/lakehouse/Files/documentsReceived/abc123/claim-456_report.pdf'
    """
    blob_path, _ = generate_blob_path(
        event_type, trace_id, assignment_id, download_url, estimate_version
    )

    # Combine base path with domain-specific path
    base = base_path.rstrip("/")
    return f"{base}/{blob_path}"
