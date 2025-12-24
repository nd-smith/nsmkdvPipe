"""Storage layer for verisk_pipeline."""

# Keep protocol exports - these are actually useful
from verisk_pipeline.storage.base import (
    StorageClientBase,
    StorageReader,
    StorageWriter,
    StorageClient,
)

from verisk_pipeline.storage.upload_service import (
    UploadService,
    UploadTask,
    UploadResult,
    UploadStatus,
    get_upload_service,
    init_upload_service,
    shutdown_upload_service,
)

__all__ = [
    "StorageClientBase",
    "StorageReader",
    "StorageWriter",
    "StorageClient",
    "UploadService",
    "UploadTask",
    "UploadResult",
    "UploadStatus",
    "get_upload_service",
    "init_upload_service",
    "shutdown_upload_service",
]
