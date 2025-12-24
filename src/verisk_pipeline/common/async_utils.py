"""
Async utilities with proper signal handling.

Provides signal-aware async execution that allows CTRL+C to properly
interrupt long-running async operations like batch downloads.
"""

import asyncio
import logging
import signal
import sys
import threading
from typing import Any, Coroutine, Optional, TypeVar

from verisk_pipeline.common.logging.setup import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


def run_async_with_shutdown(
    coro: Coroutine[Any, Any, T],
    shutdown_event: Optional[threading.Event] = None,
) -> T:
    """
    Run an async coroutine with proper SIGINT/SIGTERM handling.

    Unlike asyncio.run(), this function properly handles signals during async
    execution. When SIGINT (CTRL+C) or SIGTERM is received:
    1. All running async tasks are cancelled
    2. The shutdown_event is set (if provided)
    3. KeyboardInterrupt is raised to allow proper cleanup

    This fixes the issue where asyncio.run() blocks signal delivery until
    all async operations complete.

    Args:
        coro: The coroutine to run
        shutdown_event: Optional threading.Event to set on shutdown signal

    Returns:
        The result of the coroutine

    Raises:
        KeyboardInterrupt: When SIGINT or SIGTERM is received
        Any exception raised by the coroutine
    """

    async def run_with_signal_handling() -> T:
        """Inner async function that sets up signal handling."""
        loop = asyncio.get_running_loop()
        main_task = asyncio.current_task()
        shutdown_received = False

        def signal_handler() -> None:
            """Handle shutdown signals by cancelling the main task."""
            nonlocal shutdown_received
            shutdown_received = True

            logger.info("Shutdown signal received, cancelling async operations...")

            if shutdown_event is not None:
                shutdown_event.set()

            if main_task is not None and not main_task.done():
                main_task.cancel()

        # Set up signal handlers (Unix only, Windows uses different mechanism)
        signals_to_handle = []
        if sys.platform != "win32":
            signals_to_handle = [signal.SIGINT, signal.SIGTERM]
            for sig in signals_to_handle:
                try:
                    loop.add_signal_handler(sig, signal_handler)
                except (ValueError, RuntimeError):
                    # Signal handling not available in this context
                    pass

        try:
            return await coro
        except asyncio.CancelledError:
            if shutdown_received:
                # Signal-triggered cancellation - raise KeyboardInterrupt
                raise KeyboardInterrupt("Shutdown signal received during async operation")
            # Re-raise non-signal cancellations
            raise
        finally:
            # Remove signal handlers
            for sig in signals_to_handle:
                try:
                    loop.remove_signal_handler(sig)
                except (ValueError, RuntimeError):
                    pass

    return asyncio.run(run_with_signal_handling())
