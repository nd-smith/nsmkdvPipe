# Source Code Structure

This directory contains the Kafka-based event-driven pipeline:

```
src/
├── core/                 # SHARED - Reusable, infrastructure-agnostic components
└── kafka_pipeline/       # Kafka-based event-driven pipeline
```

## Package Overview

### `core/` (Shared Library)

Reusable components for infrastructure-agnostic functionality:

| Module | Purpose |
|--------|---------|
| `auth/` | Azure AD, SPN, Kafka OAUTHBEARER |
| `resilience/` | Circuit breaker, retry with backoff |
| `logging/` | Structured JSON logging |
| `errors/` | Error classification, exceptions |
| `security/` | URL validation, SSRF prevention |
| `download/` | Async HTTP download logic |

**Design Principles:**
- No dependencies on Delta Lake, Kusto, or Kafka
- Independently testable
- Type hints throughout

### `kafka_pipeline/`

Event-driven pipeline using Kafka, organized by domain:

| Module | Purpose |
|--------|---------|
| `common/` | Shared infrastructure (consumer, producer, retry decorators, storage, writers) |
| `xact/` | Transaction domain (workers, schemas, writers, retry handler, DLQ handler) |
| `claimx/` | Claims domain (workers, handlers, API client, schemas, writers) |

**Domain-Based Architecture:**
```
xact Domain:
  events.raw → downloads.pending → downloads.cached → downloads.results
                     ↓ (failure)
              downloads.retry.* → (delayed redelivery)
                     ↓ (exhausted)
              downloads.dlq

claimx Domain:
  events.raw → enrichment.pending → entity tables + downloads.pending →
  downloads.cached → downloads.results
                     ↓ (failure)
              downloads.retry.* → (delayed redelivery)
                     ↓ (exhausted)
              downloads.dlq

All topics are namespaced by domain: {domain}.{workflow}.{stage}
```

## Implementation History

The Kafka pipeline was implemented through a phased reorganization:

1. **Phase 1:** Created `common/` infrastructure (consumers, producers, retry, DLQ, storage, writers)
2. **Phase 2:** Created `xact/` domain (schemas, workers, writers)
3. **Phase 3:** Created `claimx/` domain (schemas, workers, handlers, API client, writers)
4. **Phase 4:** Updated configuration and entry points for domain support
5. **Phase 5:** Migrated functionality from legacy pipeline and removed `verisk_pipeline/`

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
# Using core components (domain-agnostic)
from core.resilience import CircuitBreaker, with_retry
from core.security import validate_download_url
from core.logging import get_logger

# Using common kafka_pipeline infrastructure (generic utilities)
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.retry import with_retry, RetryConfig  # Generic retry decorators

# Using xact domain
from kafka_pipeline.xact.workers import DownloadWorker, EventIngesterWorker
from kafka_pipeline.xact.schemas import DownloadTaskMessage, EventMessage
from kafka_pipeline.xact.writers import DeltaEventsWriter
from kafka_pipeline.xact.retry import RetryHandler  # Download task retry routing
from kafka_pipeline.xact.dlq import DLQHandler  # DLQ message handling

# Using claimx domain
from kafka_pipeline.claimx.workers import ClaimXEnrichmentWorker
from kafka_pipeline.claimx.handlers import ProjectHandler, MediaHandler
from kafka_pipeline.claimx.schemas import ClaimXEventMessage, ClaimXEnrichmentTask
from kafka_pipeline.claimx.api_client import ClaimXApiClient
```
