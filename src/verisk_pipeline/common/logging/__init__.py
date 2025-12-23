"""
Logging module for Verisk Pipeline.

Import directly from sub-modules:
    from verisk_pipeline.common.logging.setup import get_logger, setup_logging
    from verisk_pipeline.common.logging.utilities import log_with_context
    from verisk_pipeline.common.logging.context_managers import StageLogContext
"""

import io
import sys

# Windows UTF-8 stdout/stderr fix - must run before any logging
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(
                sys.stdout.buffer, encoding="utf-8", errors="replace"
            )
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(
                sys.stderr.buffer, encoding="utf-8", errors="replace"
            )
    except Exception:
        pass  # Already wrapped or unavailable
