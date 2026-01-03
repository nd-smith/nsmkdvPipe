"""Fixtures for ClaimX writer tests."""

import os

# Set environment variables for writer tests
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"

# Prevent Delta Lake from creating actual tables in tests
if "DELTA_LAKE_TEST_MODE" not in os.environ:
    os.environ["DELTA_LAKE_TEST_MODE"] = "true"
