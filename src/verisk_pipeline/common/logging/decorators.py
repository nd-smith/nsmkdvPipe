"""Logging decorators and mixins."""

import functools
import logging
from typing import Any, Callable, Dict, List, Optional, TypeVar

from verisk_pipeline.common.logging.context_managers import (
    OperationContext,
    _extract_instance_context,
)
from verisk_pipeline.common.logging.setup import get_logger
from verisk_pipeline.common.logging.utilities import log_exception, log_with_context

F = TypeVar("F", bound=Callable[..., Any])


def _is_coroutine_function(func: Callable) -> bool:
    """Check if function is a coroutine function."""
    import asyncio

    return asyncio.iscoroutinefunction(func)


def logged_operation(
    level: int = logging.DEBUG,
    slow_threshold_ms: Optional[float] = 1000.0,
    log_start: bool = False,
    include_args: Optional[List[str]] = None,
    operation_name: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator for automatic operation logging on class methods.

    Args:
        level: Log level for completion message
        slow_threshold_ms: Promote to INFO if duration exceeds this (None to disable)
        log_start: Also log when operation starts
        include_args: Argument names to include in context
        operation_name: Override operation name (default: ClassName.method_name)

    Example:
        class DeltaTableReader(LoggedClass):
            @logged_operation(level=logging.DEBUG)
            def read(self, columns=None):
                ...
    """
    include_args = include_args or []

    def decorator(func: F) -> F:
        def _process_result(ctx: OperationContext, result: Any) -> None:
            """Extract row counts from results."""
            if result is None:
                return
            # DataFrame-like results: capture as rows_read
            if hasattr(result, "__len__") and hasattr(result, "columns"):
                ctx.add_context(rows_read=len(result))
            # Integer results: capture as rows_written (for write operations)
            elif isinstance(result, int):
                ctx.add_context(rows_written=result)

        if _is_coroutine_function(func):

            @functools.wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                _logger = getattr(self, "_logger", None) or get_logger(
                    self.__class__.__module__
                )
                op_name = operation_name or func.__name__
                full_op = f"{self.__class__.__name__}.{op_name}"
                context = _extract_instance_context(self)

                if include_args:
                    import inspect

                    sig = inspect.signature(func)
                    params = list(sig.parameters.keys())
                    for name in include_args:
                        if name in kwargs:
                            context[name] = kwargs[name]
                        elif name in params:
                            idx = params.index(name)
                            if idx > 0 and idx - 1 < len(args):
                                context[name] = args[idx - 1]

                with OperationContext(
                    _logger,
                    full_op,
                    level=level,
                    slow_threshold_ms=slow_threshold_ms,
                    log_start=log_start,
                    **context,
                ) as ctx:
                    result = await func(self, *args, **kwargs)
                    _process_result(ctx, result)
                    return result

            return async_wrapper  # type: ignore
        else:

            @functools.wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                _logger = getattr(self, "_logger", None) or get_logger(
                    self.__class__.__module__
                )
                op_name = operation_name or func.__name__
                full_op = f"{self.__class__.__name__}.{op_name}"
                context = _extract_instance_context(self)

                if include_args:
                    import inspect

                    sig = inspect.signature(func)
                    params = list(sig.parameters.keys())
                    for name in include_args:
                        if name in kwargs:
                            context[name] = kwargs[name]
                        elif name in params:
                            idx = params.index(name)
                            if idx > 0 and idx - 1 < len(args):
                                context[name] = args[idx - 1]

                with OperationContext(
                    _logger,
                    full_op,
                    level=level,
                    slow_threshold_ms=slow_threshold_ms,
                    log_start=log_start,
                    **context,
                ) as ctx:
                    result = func(self, *args, **kwargs)
                    _process_result(ctx, result)
                    return result

            return sync_wrapper  # type: ignore

    return decorator


class LoggedClass:
    """
    Mixin providing logging infrastructure for storage/client classes.

    Provides:
    - self._logger: Logger instance
    - self._log(): Log with auto-extracted context
    - self._log_exception(): Exception logging with context

    Auto-extracts context from instance attributes:
    - table_path -> "table" field
    - primary_keys -> "primary_keys" field

    Example:
        class DeltaTableReader(LoggedClass):
            def __init__(self, table_path: str):
                self.table_path = table_path
                super().__init__()

            def read(self):
                self._log(logging.DEBUG, "Reading", columns=columns)
                ...
    """

    log_component: Optional[str] = None  # Optional logger name suffix

    def __init__(self, *args, **kwargs):
        logger_name = self.__class__.__module__
        if self.log_component:
            logger_name = f"{logger_name}.{self.log_component}"
        self._logger = get_logger(logger_name)
        super().__init__(*args, **kwargs)

    def _log(self, level: int, msg: str, **extra: Any) -> None:
        """
        Log with automatic context extraction from instance.

        Args:
            level: Log level
            msg: Log message
            **extra: Additional context fields
        """
        context = _extract_instance_context(self)
        context.update(extra)
        log_with_context(self._logger, level, msg, **context)

    def _log_exception(
        self,
        exc: Exception,
        msg: str,
        level: int = logging.ERROR,
        **extra: Any,
    ) -> None:
        """
        Log exception with automatic context extraction from instance.

        Args:
            exc: Exception to log
            msg: Context message
            level: Log level (default: ERROR)
            **extra: Additional context fields
        """
        context = _extract_instance_context(self)
        context.update(extra)
        log_exception(self._logger, exc, msg, level=level, **context)


def extract_log_context(obj: Any) -> Dict[str, Any]:
    """
    Extract loggable context from any pipeline object.

    Handles:
    - Xact: Task, DownloadResult, EventRecord
    - ClaimX: ClaimXEvent, MediaTask, MediaDownloadResult, EnrichmentResult

    Unwraps result objects to get underlying task/event, then extracts
    curated identifier fields.

    Args:
        obj: Pipeline model object (Task, Event, Result, etc.)

    Returns:
        Dict with identifier fields suitable for logging

    Example:
        try:
            result = download(task)
        except Exception as e:
            log_exception(logger, e, "Download failed", **extract_log_context(task))
    """
    ctx: Dict[str, Any] = {}

    if obj is None:
        return ctx

    # Unwrap result objects to get underlying task/event
    inner = obj
    if hasattr(obj, "task") and obj.task is not None:
        inner = obj.task
        # Capture error info from result
        if hasattr(obj, "error_category") and obj.error_category:
            cat = obj.error_category
            ctx["error_category"] = cat.value if hasattr(cat, "value") else str(cat)
        if hasattr(obj, "http_status") and obj.http_status:
            ctx["http_status"] = obj.http_status
    elif hasattr(obj, "event") and obj.event is not None:
        inner = obj.event
        # Capture error info from result
        if hasattr(obj, "error_category") and obj.error_category:
            cat = obj.error_category
            ctx["error_category"] = cat.value if hasattr(cat, "value") else str(cat)
        if hasattr(obj, "api_calls") and obj.api_calls:
            ctx["api_calls"] = obj.api_calls

    # Xact identifiers
    if hasattr(inner, "trace_id") and inner.trace_id:
        ctx["trace_id"] = inner.trace_id
    if hasattr(inner, "assignment_id") and inner.assignment_id:
        ctx["assignment_id"] = inner.assignment_id
    if hasattr(inner, "status_subtype") and inner.status_subtype:
        ctx["status_subtype"] = inner.status_subtype

    # ClaimX identifiers
    if hasattr(inner, "event_id") and inner.event_id:
        ctx["event_id"] = inner.event_id
    if hasattr(inner, "event_type") and inner.event_type:
        ctx["event_type"] = inner.event_type
    if hasattr(inner, "project_id") and inner.project_id:
        ctx["project_id"] = inner.project_id
    if hasattr(inner, "media_id") and inner.media_id:
        ctx["media_id"] = inner.media_id

    return ctx
