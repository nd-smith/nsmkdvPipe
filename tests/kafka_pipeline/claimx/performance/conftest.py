"""
Pytest configuration and fixtures for ClaimX performance tests.

Reuses integration test fixtures from parent conftest.py.
"""

import pytest

# Import fixtures from integration tests
pytest_plugins = [
    "tests.kafka_pipeline.integration.claimx.conftest",
    "tests.kafka_pipeline.integration.conftest",
]
