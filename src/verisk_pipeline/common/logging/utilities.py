"""Logging utility functions."""

import logging
from typing import Any, Dict, Optional


def log_with_context(
    logger: logging.Logger,
    level: int,
    msg: str,
    **kwargs: Any,
) -> None:
    """
    Log with structured context fields.

    Args:
        logger: Logger instance
        level: Log level (logging.INFO, etc.)
        msg: Log message
        **kwargs: Additional context fields (trace_id, duration_ms, etc.)

    Example:
        log_with_context(
            logger, logging.INFO, "Download complete",
            trace_id=task.trace_id,
            duration_ms=elapsed,
            http_status=200,
        )
    """
    logger.log(level, msg, extra=kwargs)


def log_exception(
    logger: logging.Logger,
    exc: Exception,
    msg: str,
    level: int = logging.ERROR,
    include_traceback: bool = True,
    **kwargs: Any,
) -> None:
    """
    Log exception with context and optional traceback.

    Automatically extracts error_category from PipelineError subclasses.
    Sanitizes error messages to remove sensitive data.

    Args:
        logger: Logger instance
        exc: Exception to log
        msg: Context message
        level: Log level (default: ERROR)
        include_traceback: Include full traceback (default: True)
        **kwargs: Additional context fields

    Example:
        try:
            download_file(url)
        except Exception as e:
            log_exception(logger, e, "Download failed", trace_id=task.trace_id)
    """
    # Extract error category if available
    error_category = kwargs.get("error_category")
    if error_category is None and hasattr(exc, "category"):
        cat = exc.category
        error_category = cat.value if hasattr(cat, "value") else str(cat)
        kwargs["error_category"] = error_category

    # Sanitize error message
    error_msg = str(exc)
    if len(error_msg) > 500:
        error_msg = error_msg[:500] + "..."
    kwargs["error_message"] = error_msg

    if include_traceback:
        logger.log(level, msg, exc_info=exc, extra=kwargs)
    else:
        logger.log(level, msg, extra=kwargs)


def get_process_memory_mb() -> float:
    """
    Get current process memory usage in MB.

    Uses psutil for cross-platform support (Linux + Windows).

    Returns:
        Current RSS memory in megabytes, or 0.0 if psutil unavailable
    """
    try:
        import psutil

        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0


def log_memory_checkpoint(
    logger: logging.Logger,
    checkpoint: str,
    config: Optional[Any] = None,
    df: Optional[Any] = None,
    level: Optional[int] = None,
    **extra: Any,
) -> None:
    """
    Log memory usage at a checkpoint.

    No-op if config.memory_checkpoints_enabled is False.

    Args:
        logger: Logger instance
        checkpoint: Name of checkpoint (e.g., "after_read", "stage_end")
        config: ObservabilityConfig (checks memory_checkpoints_enabled)
        df: Optional polars DataFrame to include size info
        level: Override log level (default from config or DEBUG)
        **extra: Additional context fields

    Example:
        log_memory_checkpoint(
            logger, "after_read",
            config=self.config.observability,
            df=events_df
        )
    """
    # Check if enabled
    if config is not None:
        if (
            hasattr(config, "memory_checkpoints_enabled")
            and not config.memory_checkpoints_enabled
        ):
            return
        # Get level from config if not overridden
        if level is None and hasattr(config, "memory_checkpoint_level"):
            level_str = config.memory_checkpoint_level.upper()
            level = getattr(logging, level_str, logging.DEBUG)

    if level is None:
        level = logging.DEBUG

    # Build context
    context: Dict[str, Any] = {
        "checkpoint": checkpoint,
        "memory_mb": round(get_process_memory_mb(), 2),
    }
    context.update(extra)

    # Add DataFrame info if provided
    if df is not None:
        if hasattr(df, "__len__"):
            context["df_rows"] = len(df)
        if hasattr(df, "columns"):
            context["df_cols"] = len(df.columns)

    log_with_context(logger, level, f"Memory checkpoint: {checkpoint}", **context)
