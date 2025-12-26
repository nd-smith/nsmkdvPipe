"""
pytest configuration for core library tests.

Adds src directory to Python path for imports and sets up test environment.
"""

import os
import sys
from pathlib import Path

# Set test environment variables BEFORE any imports
os.environ.setdefault("TEST_MODE", "true")
os.environ.setdefault("AUDIT_LOG_PATH", "/tmp/test_audit.log")

# Add src directory to Python path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

# Configure test environment for audit logging
# This needs to happen before any application modules are imported
def pytest_configure(config):
    """Configure pytest environment for testing."""
    # Set audit log path to writable location for tests
    from verisk_pipeline.common.config.xact import load_config_from_dict, set_config

    test_config = load_config_from_dict({
        "security": {
            "audit_logging_enabled": False,  # Disable audit logging in tests
            "audit_log_path": "/tmp/test_audit.log",
        }
    })
    set_config(test_config)
