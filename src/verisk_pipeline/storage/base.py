"""
Abstract base class and protocols for storage clients.

Provides:
- StorageClientBase: Abstract base class with lifecycle management
- Storage protocols: Type-safe interfaces for readers, writers, and clients
- Common patterns for connection management and credential refresh
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Protocol, TypeVar, runtime_checkable

import polars as pl

# Use TypeVar for proper self-type annotation
T = TypeVar("T", bound="StorageClientBase")


@runtime_checkable
class StorageReader(Protocol):
    """
    Protocol for storage read operations.

    Defines interface for reading data from storage systems.
    Implementations may support different read patterns (full, filtered, lazy).
    """

    def read(self, columns: Optional[List[str]] = None, **kwargs: Any) -> pl.DataFrame:
        """
        Read data from storage.

        Args:
            columns: Optional list of columns to read
            **kwargs: Implementation-specific arguments

        Returns:
            Polars DataFrame containing read data
        """
        ...


@runtime_checkable
class StorageWriter(Protocol):
    """
    Protocol for storage write operations.

    Defines interface for writing data to storage systems.
    Implementations may support different write modes (overwrite, append, upsert).
    """

    def write(self, data: pl.DataFrame, **kwargs: Any) -> None:
        """
        Write data to storage.

        Args:
            data: Polars DataFrame to write
            **kwargs: Implementation-specific arguments (mode, partition_by, etc.)
        """
        ...


@runtime_checkable
class StorageClient(Protocol):
    """
    Protocol for storage clients with lifecycle management.

    Combines read/write capabilities with connection lifecycle.
    Clients implementing this protocol can be used interchangeably
    in contexts requiring storage access with proper cleanup.
    """

    def connect(self) -> None:
        """Establish connection to storage service."""
        ...

    def close(self) -> None:
        """Close connection and cleanup resources."""
        ...

    def is_connected(self) -> bool:
        """Check if client is currently connected."""
        ...

    def __enter__(self) -> "StorageClient":
        """Context manager entry."""
        ...

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        ...


class StorageClientBase(ABC):
    """
    Abstract base class for storage clients with lifecycle management.

    Provides:
    - Context manager protocol for automatic resource management
    - Abstract methods for connection and cleanup
    - Connection state tracking
    - Optional credential refresh support

    Subclasses must implement:
    - connect(): Establish connection/initialize client
    - close(): Close connection and cleanup resources

    Usage:
        class MyStorageClient(StorageClientBase):
            def connect(self):
                self._client = create_client()
                self._connected = True

            def close(self):
                if self._client:
                    self._client.close()
                self._client = None
                self._connected = False

        # Context manager usage (recommended)
        with MyStorageClient() as client:
            client.do_work()

        # Manual usage
        client = MyStorageClient()
        client.connect()
        try:
            client.do_work()
        finally:
            client.close()
    """

    def __init__(self) -> None:
        """Initialize base state tracking."""
        self._connected: bool = False

    def __enter__(self: T) -> T:
        """
        Context manager entry.

        Automatically establishes connection when entering context.

        Returns:
            Self for use in context
        """
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        """
        Context manager exit.

        Automatically closes connection when exiting context,
        regardless of whether an exception occurred.

        Args:
            *args: Exception info (exc_type, exc_val, exc_tb)
        """
        self.close()

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to storage service.

        Must be implemented by subclasses to:
        - Create/initialize client(s)
        - Authenticate with service
        - Set self._connected = True on success

        Raises:
            Implementation-specific exceptions on connection failure
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close connection and cleanup resources.

        Must be implemented by subclasses to:
        - Close client connection(s)
        - Release resources (network, file handles, etc.)
        - Set self._connected = False
        - Handle cleanup errors gracefully (log, don't raise)
        """
        pass

    def is_connected(self) -> bool:
        """
        Check if client is currently connected.

        Returns:
            True if connected, False otherwise
        """
        return self._connected

    def refresh(self) -> None:
        """
        Refresh credentials/connection.

        Default implementation is a no-op. Override in subclasses
        that support credential refresh (e.g., token rotation).

        Typical implementation:
        1. Clear cached credentials
        2. Close existing connection
        3. Re-establish connection with fresh credentials
        """
        pass
