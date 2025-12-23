"""Storage layer for verisk_pipeline."""

# Keep protocol exports - these are actually useful
from verisk_pipeline.storage.base import (
    StorageClientBase,
    StorageReader,
    StorageWriter,
    StorageClient,
)

__all__ = [
    "StorageClientBase",
    "StorageReader",
    "StorageWriter",
    "StorageClient",
]
