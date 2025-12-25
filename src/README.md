# Source Code Structure

This directory contains three packages representing different stages of the Kafka migration:

```
src/
├── verisk_pipeline/      # LEGACY - Existing polling-based pipeline
├── core/                 # SHARED - Reusable, infrastructure-agnostic components
└── kafka_pipeline/       # NEW - Kafka-based event-driven pipeline
```

## Package Overview

### `verisk_pipeline/` (Legacy)

The existing pipeline implementation using:
- Kusto/Eventhouse for event ingestion
- Delta Lake for storage and retry queue
- Polling-based orchestration (10-60s cycles)

**Status:** Running in production. Will be deprecated after Kafka migration.

### `core/` (Shared Library)

Reusable components extracted from the legacy pipeline, reviewed and refactored:

| Module | Purpose | Source |
|--------|---------|--------|
| `auth/` | Azure AD, SPN, Kafka OAUTHBEARER | `verisk_pipeline.common.auth` |
| `resilience/` | Circuit breaker, retry with backoff | `verisk_pipeline.common.circuit_breaker`, `retry` |
| `logging/` | Structured JSON logging | `verisk_pipeline.common.logging` |
| `errors/` | Error classification, exceptions | `verisk_pipeline.common.exceptions`, `storage.errors` |
| `security/` | URL validation, SSRF prevention | `verisk_pipeline.common.security` |
| `download/` | Async HTTP download logic | `verisk_pipeline.xact.stages.xact_download` |

**Design Principles:**
- No dependencies on Delta Lake, Kusto, or Kafka
- Independently testable
- Type hints throughout

### `kafka_pipeline/` (New Implementation)

Event-driven pipeline using Kafka:

| Module | Purpose |
|--------|---------|
| `kafka/` | Producers, consumers, retry handling |
| `workers/` | Event ingester, download worker, result processor |
| `schemas/` | Pydantic message schemas |

**Architecture:**
```
Kafka Topics:
  events.raw → downloads.pending → downloads.results
                     ↓ (failure)
              downloads.retry.* → (delayed redelivery)
                     ↓ (exhausted)
              downloads.dlq
```

## Migration Path

1. **Phase 1:** Extract and review components → `core/`
2. **Phase 2:** Build Kafka infrastructure → `kafka_pipeline/kafka/`
3. **Phase 3:** Implement workers → `kafka_pipeline/workers/`
4. **Phase 4:** Parallel run (both pipelines active)
5. **Phase 5:** Cutover and deprecate `verisk_pipeline/`

## Development Guidelines

### Adding to `core/`

When extracting from legacy:
1. Review the existing code for correctness
2. Remove infrastructure dependencies (Delta, Kusto)
3. Add comprehensive tests
4. Document any behavior changes

### Adding to `kafka_pipeline/`

1. Use `core/` for all reusable logic
2. Follow async-first patterns
3. Add Kafka-specific context to logs
4. Write integration tests with Testcontainers

## Import Examples

```python
# Using core components
from core.resilience import CircuitBreaker, with_retry
from core.security import validate_download_url
from core.logging import get_logger

# Using kafka_pipeline
from kafka_pipeline.workers import DownloadWorker
from kafka_pipeline.schemas import DownloadTaskMessage
```
