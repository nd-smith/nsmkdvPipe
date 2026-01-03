"""
ClaimX upload result message schema for Kafka pipeline.

Contains Pydantic model for upload outcomes sent to results topic
after files are uploaded to OneLake.
"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


class ClaimXUploadResultMessage(BaseModel):
    """Schema for ClaimX upload outcomes (success or failure).

    Sent to claimx.downloads.results topic after upload worker processes
    a cached download and uploads it to OneLake.

    Attributes:
        media_id: Media file identifier from ClaimX
        project_id: ClaimX project ID this media belongs to
        download_url: Original S3 presigned URL the file was downloaded from
        blob_path: Target path in OneLake (relative to base path)
        file_type: File type/extension (e.g., "pdf", "jpg", "mp4")
        file_name: Original file name
        source_event_id: ID of the event that triggered this download
        status: Outcome status (completed, failed, failed_permanent)
        bytes_uploaded: Number of bytes uploaded (0 if failed)
        error_message: Error description if failed (truncated to 500 chars)
        created_at: Timestamp when result was created

    Example:
        >>> from datetime import datetime, timezone
        >>> result = ClaimXUploadResultMessage(
        ...     media_id="media_111",
        ...     project_id="proj_67890",
        ...     download_url="https://s3.amazonaws.com/claimx/presigned...",
        ...     blob_path="claimx/proj_67890/media/photo.jpg",
        ...     file_type="jpg",
        ...     file_name="photo.jpg",
        ...     source_event_id="evt_12345",
        ...     status="completed",
        ...     bytes_uploaded=2048576,
        ...     created_at=datetime.now(timezone.utc)
        ... )
    """

    media_id: str = Field(
        ...,
        description="Media file identifier from ClaimX",
        min_length=1
    )
    project_id: str = Field(
        ...,
        description="ClaimX project ID",
        min_length=1
    )
    download_url: str = Field(
        ...,
        description="Original S3 presigned URL the file was downloaded from",
        min_length=1
    )
    blob_path: str = Field(
        ...,
        description="Target path in OneLake (relative to base path)",
        min_length=1
    )
    file_type: str = Field(
        default="",
        description="File type/extension (e.g., 'pdf', 'jpg', 'mp4')"
    )
    file_name: str = Field(
        default="",
        description="Original file name"
    )
    source_event_id: str = Field(
        default="",
        description="ID of the event that triggered this download"
    )
    status: Literal["completed", "failed", "failed_permanent"] = Field(
        ...,
        description="Outcome status: completed, failed (transient), or failed_permanent"
    )
    bytes_uploaded: int = Field(
        default=0,
        description="Number of bytes uploaded (0 if failed)",
        ge=0
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error description if failed (truncated to 500 chars)"
    )
    created_at: datetime = Field(
        ...,
        description="Timestamp when result was created"
    )

    @field_validator('media_id', 'project_id', 'download_url', 'blob_path')
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        """Ensure required string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty or whitespace")
        return v.strip()

    @field_serializer('created_at')
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize datetime to ISO 8601 format."""
        return timestamp.isoformat()

    model_config = {
        'json_schema_extra': {
            'examples': [
                {
                    'media_id': 'media_111',
                    'project_id': 'proj_67890',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/photo.jpg?signature=...',
                    'blob_path': 'claimx/proj_67890/media/photo.jpg',
                    'file_type': 'jpg',
                    'file_name': 'photo.jpg',
                    'source_event_id': 'evt_12345',
                    'status': 'completed',
                    'bytes_uploaded': 2048576,
                    'created_at': '2024-12-25T10:30:10Z'
                },
                {
                    'media_id': 'media_222',
                    'project_id': 'proj_12345',
                    'download_url': 'https://s3.amazonaws.com/claimx-media/presigned/video.mp4?signature=...',
                    'blob_path': 'claimx/proj_12345/media/video.mp4',
                    'file_type': 'mp4',
                    'file_name': 'damage_video.mp4',
                    'source_event_id': 'evt_67890',
                    'status': 'failed_permanent',
                    'bytes_uploaded': 0,
                    'error_message': 'OneLake upload failed: Connection timeout',
                    'created_at': '2024-12-25T11:15:30Z'
                }
            ]
        }
    }
