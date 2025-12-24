"""
Xact-specific path resolution for OneLake storage.

Provides path generation rules for different event status subtypes:
- documentsReceived
- firstNoticeOfLossReceived (FNOL)
- estimatePackageReceived

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

    blob_path = f"{status_subtype}/{assignment_id}/{trace_id}/{filename}"
    return blob_path, file_type


def get_onelake_path_for_event(
    event_type: str,
    trace_id: str,
    assignment_id: str,
    download_url: str,
    base_path: str,
    estimate_version: Optional[str] = None,
) -> str:
    blob_path, _ = generate_blob_path(
        event_type, trace_id, assignment_id, download_url, estimate_version
    )

    # Combine base path with domain-specific path
    base = base_path.rstrip("/")
    return f"{base}/{blob_path}"
