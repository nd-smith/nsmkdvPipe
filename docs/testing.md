# Testing Guide

This document describes how to run tests for the nsmkdvPipe project.

## Test Categories

The test suite is organized into three categories:

| Category | Count | Duration | Requirements |
|----------|-------|----------|--------------|
| Unit Tests | ~898 | ~40s | None |
| Integration Tests | ~87 | ~3-5 min | Docker |
| Performance Tests | ~22 | 10min - 4hrs | Docker |

## Prerequisites

### All Tests
- Python 3.13+
- Virtual environment activated: `source src/.venv/bin/activate`
- Dependencies installed: `pip install -r requirements.txt`

### Integration & Performance Tests
- **Docker** must be running
- Tests use [testcontainers](https://testcontainers.com/) to spin up Kafka containers automatically
- No manual Kafka setup required

## Running Tests

### Quick Test (Unit Tests Only)

Run unit tests only (~40 seconds):

```bash
cd /path/to/nsmkdvPipe
pytest tests/ --ignore=tests/kafka_pipeline/integration --ignore=tests/kafka_pipeline/performance
```

### Full Test Suite (Excluding Long-Running Performance Tests)

Run unit and integration tests (~5 minutes):

```bash
pytest tests/ --ignore=tests/kafka_pipeline/performance
```

### Integration Tests Only

```bash
pytest tests/kafka_pipeline/integration/ -v
```

### Performance Tests

Performance tests are designed for load testing and benchmarking. Some tests are long-running:

| Test | Approximate Duration |
|------|---------------------|
| `test_sustained_load_4_hours` | 4 hours |
| `test_consumer_lag_recovery` | up to 10 minutes |
| Other performance tests | 1-5 minutes each |

Run all performance tests (may take hours):

```bash
pytest tests/kafka_pipeline/performance/ -v
```

Run specific performance test:

```bash
pytest tests/kafka_pipeline/performance/test_throughput.py -v
```

### Using Markers

Tests are marked with `@pytest.mark.integration` and `@pytest.mark.performance`:

```bash
# Run only integration tests
pytest -m integration

# Run only performance tests
pytest -m performance

# Exclude both integration and performance tests
pytest -m "not integration and not performance"
```

## Test Configuration

### Environment Variables

Integration tests set these automatically via fixtures:

| Variable | Description | Default in Tests |
|----------|-------------|------------------|
| `TEST_MODE` | Enables test mode | `true` |
| `AUDIT_LOG_PATH` | Path for audit logs | `/tmp/test_audit.log` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | Set by testcontainer |
| `KAFKA_SECURITY_PROTOCOL` | Security protocol | `PLAINTEXT` |
| `ALLOWED_ATTACHMENT_DOMAINS` | Allowed download domains | `example.com,...` |
| `ONELAKE_BASE_PATH` | OneLake path | Test path |

### pytest.ini

Key configuration in `pytest.ini`:

```ini
[pytest]
pythonpath = src
asyncio_mode = auto
markers =
    integration: Integration tests requiring Docker and Kafka testcontainers
    performance: Performance/load tests (may be long-running)
```

## Test Architecture

### Unit Tests (`tests/core/`, `tests/kafka_pipeline/` excluding integration/performance)

- No external dependencies
- Mock all external services (Kafka, OneLake, Delta Lake)
- Fast execution

### Integration Tests (`tests/kafka_pipeline/integration/`)

- Use real Kafka via Docker testcontainers
- Mock storage (OneLake, Delta Lake) with in-memory implementations
- Test message flow through workers

Key fixtures:
- `kafka_container`: Session-scoped Kafka container
- `kafka_producer/kafka_consumer_factory`: Kafka clients
- `mock_onelake_client`: In-memory file storage
- `mock_delta_events_writer`: In-memory Delta Lake mock

### Performance Tests (`tests/kafka_pipeline/performance/`)

- Measure throughput, latency, resource usage
- Validate NFR (Non-Functional Requirements)
- Generate performance reports

Key fixtures:
- `performance_metrics`: Collects throughput/latency data
- `resource_monitor`: Tracks CPU/memory usage
- `load_generator`: Generates test events

## Troubleshooting

### Docker Not Running

```
Error: Cannot connect to Docker daemon
```

**Solution**: Start Docker service

```bash
sudo systemctl start docker
# or on macOS
open -a Docker
```

### Kafka Container Fails to Start

```
Error: Container startup failed
```

**Solution**: Check Docker resources and pull Kafka image

```bash
docker pull confluentinc/cp-kafka:latest
```

### Tests Timeout

Performance tests have built-in timeouts. If tests timeout unexpectedly:

1. Check Docker container health: `docker ps`
2. Check system resources: `htop`
3. Run with verbose logging: `pytest -v --log-cli-level=DEBUG`

### Topic Auto-Creation Warnings

```
WARNING: Topic X is not available during auto-create initialization
```

This is expected behavior when Kafka creates topics on first use. Tests handle this automatically.

## Current Test Status

As of the last run:

- **Unit Tests**: 898 passed
- **Integration Tests**: 84 passed, 3 failed (see Known Issues)
- **Performance Tests**: Require extended run time

### Known Issues

3 integration tests in `test_download_flow.py` are failing:

1. `test_transient_failure_routes_to_retry` - Topic name mismatch between test and code
2. `test_permanent_failure_routes_to_dlq` - Schema mismatch (`failure_reason` attribute missing)
3. `test_retry_exhaustion_routes_to_dlq` - DLQ routing issue

These require code fixes, not test setup changes.
