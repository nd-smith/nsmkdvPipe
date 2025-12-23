"""
Project existence cache.

In-memory cache of project IDs that exist in the database.
Used to avoid redundant API calls when processing events.
"""

import logging
from datetime import datetime, timezone
from typing import Iterable, Optional, Set

logger = logging.getLogger(__name__)


class ProjectCache:
    """
    In-memory cache of existing project IDs.

    Loaded once at pipeline startup from Delta table.
    Updated as new projects are fetched during processing.
    """

    def __init__(self) -> None:
        self._ids: Set[str] = set()
        self._loaded_at: Optional[datetime] = None

    def bulk_load(self, ids: Iterable[str]) -> None:
        """
        Load project IDs from database.

        Args:
            ids: Iterable of project IDs
        """
        self._ids = set(ids)
        self._loaded_at = datetime.now(timezone.utc)
        logger.info(f"Loaded {len(self._ids)} project IDs into cache")

    def exists(self, project_id: str) -> bool:
        """
        Check if project exists in cache.

        Args:
            project_id: Project ID to check

        Returns:
            True if project exists
        """
        return project_id in self._ids

    def add(self, project_id: str) -> None:
        """
        Add project to cache.

        Args:
            project_id: Project ID to add
        """
        self._ids.add(project_id)

    def add_many(self, project_ids: Iterable[str]) -> None:
        """
        Add multiple projects to cache.

        Args:
            project_ids: Project IDs to add
        """
        self._ids.update(project_ids)

    def size(self) -> int:
        """Return number of cached project IDs."""
        return len(self._ids)

    def loaded_at(self) -> Optional[datetime]:
        """Return when cache was loaded."""
        return self._loaded_at

    def clear(self) -> None:
        """Clear the cache."""
        self._ids.clear()
        self._loaded_at = None
