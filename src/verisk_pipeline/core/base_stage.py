"""
Base stage abstract class for pipeline stages.

Provides template method pattern for consistent stage execution with:
- Lifecycle hooks for before/after execution
- Error handling and result creation
- Timing and metrics tracking
- Resource cleanup

All concrete stages should inherit from BaseStage and implement:
- _execute() - Core stage logic
- get_stage_name() - Stage identifier
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict

from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_exception, log_with_context

logger = get_logger(__name__)


class BaseStage(ABC):
    """
    Abstract base class for pipeline stages.

    Provides template method pattern for consistent stage execution:
    1. _before_execute() - Optional setup/validation hook
    2. _execute() - Core stage logic (abstract)
    3. _after_execute() - Optional cleanup/finalization hook
    4. _on_error() - Error handling and result creation

    Concrete stages must implement:
    - _execute() - Returns StageResult with execution details
    - get_stage_name() - Returns unique stage identifier

    Optional overrides:
    - _before_execute() - Pre-execution setup (default: no-op)
    - _after_execute() - Post-execution cleanup (default: no-op)
    - _on_error() - Custom error handling (default: creates FAILED result)
    - close() - Resource cleanup (default: no-op)
    - get_circuit_status() - Circuit breaker diagnostics (default: empty dict)
    """

    def __init__(self, config: Any):
        """
        Initialize stage with configuration.

        Args:
            config: Pipeline configuration object
        """
        self.config = config
        log_with_context(
            logger,
            logging.DEBUG,
            f"{self.__class__.__name__} initialized",
        )

    def run(self) -> Any:
        """
        Execute the stage with template method pattern.

        Template method that orchestrates stage execution:
        1. Record start time
        2. Call _before_execute() hook
        3. Execute core logic via _execute()
        4. Call _after_execute() hook
        5. Return result
        6. On error: call _on_error() to create failure result

        Returns:
            StageResult with execution status and metrics
        """
        start_time = datetime.now(timezone.utc)

        try:
            # Pre-execution hook (optional)
            self._before_execute()

            # Core stage logic (required)
            result = self._execute()

            # Post-execution hook (optional)
            self._after_execute(result)

            # Ensure timing is set if not already done by _execute
            if result.started_at is None:
                result.started_at = start_time
            if result.duration_seconds == 0.0:
                result.duration_seconds = (
                    datetime.now(timezone.utc) - start_time
                ).total_seconds()

            return result

        except Exception as e:
            # Error handling hook
            return self._on_error(e, start_time)

    @abstractmethod
    def _execute(self) -> Any:
        """
        Execute core stage logic.

        This is the main stage implementation that must be provided
        by concrete stage classes.

        Returns:
            StageResult with execution details

        Raises:
            Any exception will be caught by run() and passed to _on_error()
        """
        pass

    @abstractmethod
    def get_stage_name(self) -> str:
        """
        Get unique identifier for this stage.

        Used for logging, metrics, and result tracking.

        Returns:
            Stage name string (e.g., "ingest", "download", "retry")
        """
        pass

    def _before_execute(self) -> None:
        """
        Pre-execution hook.

        Called before _execute(). Use for:
        - Validation checks
        - Resource initialization
        - Pre-flight checks

        Default implementation is no-op. Override to add custom behavior.
        """
        pass

    def _after_execute(self, result: Any) -> None:
        """
        Post-execution hook.

        Called after _execute() completes successfully.
        Use for:
        - Result validation
        - Metrics recording
        - Cleanup operations

        Default implementation is no-op. Override to add custom behavior.

        Args:
            result: The StageResult returned by _execute()
        """
        pass

    def _on_error(self, error: Exception, start_time: datetime) -> Any:
        """
        Error handling hook.

        Called when _execute() raises an exception.
        Creates a FAILED StageResult with error details.

        Default implementation:
        - Logs the exception with context
        - Creates FAILED result with error message
        - Calculates duration from start_time

        Override to customize error handling behavior.

        Args:
            error: The exception that was raised
            start_time: When stage execution started

        Returns:
            StageResult with FAILED status and error details
        """
        from verisk_pipeline.xact.xact_models import StageResult, StageStatus

        stage_name = self.get_stage_name()

        log_exception(
            logger,
            error,
            f"Stage '{stage_name}' failed",
            stage=stage_name,
        )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        return StageResult(
            stage_name=stage_name,
            status=StageStatus.FAILED,
            duration_seconds=duration,
            started_at=start_time,
            error=str(error),
        )

    def close(self) -> None:
        """
        Clean up stage resources.

        Called to release resources like connections, file handles, etc.
        Default implementation is no-op. Override if resources need cleanup.
        """
        pass

    def get_circuit_status(self) -> Dict:
        """
        Get circuit breaker status for health checks.

        Returns diagnostics for any circuit breakers used by this stage.
        Default implementation returns empty dict.

        Override to provide circuit breaker diagnostics like:
        {
            "download": {"state": "closed", "failure_count": 0, ...},
            "upload": {"state": "closed", "failure_count": 0, ...}
        }

        Returns:
            Dictionary mapping circuit names to their diagnostics
        """
        return {}
