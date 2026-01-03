"""Fixtures for ClaimX worker tests."""

import os

# Set environment variables for all worker tests
# These must be set before workers are instantiated

# OneLake base path for worker testing
if "ONELAKE_BASE_PATH" not in os.environ:
    os.environ["ONELAKE_BASE_PATH"] = "abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse"

# Test mode to prevent audit logger from creating /var/log/verisk_pipeline
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"

# Audit log path for testing
if "AUDIT_LOG_PATH" not in os.environ:
    os.environ["AUDIT_LOG_PATH"] = "/tmp/test_audit.log"

# ClaimX API test settings
if "CLAIMX_API_URL" not in os.environ:
    os.environ["CLAIMX_API_URL"] = "https://test.claimxperience.com/api"

if "CLAIMX_API_USERNAME" not in os.environ:
    os.environ["CLAIMX_API_USERNAME"] = "test_user"

if "CLAIMX_API_PASSWORD" not in os.environ:
    os.environ["CLAIMX_API_PASSWORD"] = "test_password"
