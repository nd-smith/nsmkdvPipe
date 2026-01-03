# Kafka Pipeline Reorganization: Domain-Based Structure

## Summary

Reorganize `kafka_pipeline/` from a flat structure to a domain-based structure, aligning with `verisk_pipeline/` organization and enabling cleaner separation between xact and claimx processing.

## Current State

```
kafka_pipeline/
├── __init__.py
├── __main__.py
├── config.py
├── consumer.py
├── producer.py
├── metrics.py
├── monitoring.py
├── pipeline_config.py
├── dlq/
│   ├── cli.py
│   └── handler.py
├── eventhouse/
│   ├── dedup.py
│   ├── kql_client.py
│   └── poller.py
├── kafka/
│   └── __init__.py
├── retry/
│   ├── handler.py
│   └── scheduler.py
├── schemas/
│   ├── cached.py
│   ├── events.py      # xact-specific EventMessage
│   ├── results.py
│   └── tasks.py       # xact-specific DownloadTaskMessage
├── storage/
│   └── onelake_client.py
├── workers/
│   ├── download_worker.py
│   ├── event_ingester.py
│   ├── result_processor.py
│   └── upload_worker.py
└── writers/
    ├── delta_events.py
    └── delta_inventory.py
```

**Problems with current structure:**
1. All schemas/workers are implicitly xact-focused
2. Adding claimx would create naming confusion (`claimx_event_ingester.py` alongside `event_ingester.py`)
3. No clear pattern for adding future domains
4. Shared infrastructure mixed with domain-specific code

## Target State

```
kafka_pipeline/
├── __init__.py
├── __main__.py
├── config.py                    # Global config, loads domain configs
│
├── common/                      # Shared infrastructure
│   ├── __init__.py
│   ├── consumer.py              # Base Kafka consumer
│   ├── producer.py              # Base Kafka producer
│   ├── metrics.py               # Shared metrics
│   ├── monitoring.py            # Health checks
│   ├── retry/
│   │   ├── __init__.py
│   │   ├── handler.py           # Base retry handler
│   │   └── scheduler.py         # Retry scheduler
│   ├── dlq/
│   │   ├── __init__.py
│   │   ├── handler.py           # Base DLQ handler
│   │   └── cli.py               # DLQ CLI tools
│   ├── storage/
│   │   ├── __init__.py
│   │   └── onelake_client.py    # OneLake operations
│   ├── writers/
│   │   ├── __init__.py
│   │   ├── base.py              # Base Delta writer
│   │   └── delta_writer.py      # Generic Delta operations
│   └── eventhouse/
│       ├── __init__.py
│       ├── kql_client.py        # Kusto client
│       ├── dedup.py             # Dedup utilities
│       └── poller.py            # Event poller
│
├── xact/                        # xact domain
│   ├── __init__.py
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── events.py            # EventMessage
│   │   ├── tasks.py             # DownloadTaskMessage
│   │   ├── results.py           # ResultMessage
│   │   └── cached.py            # CachedDownloadMessage
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── event_ingester.py    # Xact event ingester
│   │   ├── download_worker.py   # Xact download worker
│   │   ├── upload_worker.py     # Xact upload worker
│   │   └── result_processor.py  # Xact result processor
│   └── writers/
│       ├── __init__.py
│       ├── delta_events.py      # Xact events table
│       └── delta_inventory.py   # Xact inventory table
│
└── claimx/                      # claimx domain (NEW)
    ├── __init__.py
    ├── schemas/
    │   ├── __init__.py
    │   ├── events.py            # ClaimXEventMessage
    │   ├── tasks.py             # ClaimXEnrichmentTask, ClaimXDownloadTask
    │   └── entities.py          # Entity row schemas
    ├── workers/
    │   ├── __init__.py
    │   ├── event_ingester.py    # ClaimX event ingester
    │   ├── enrichment_worker.py # ClaimX enrichment (NEW concept)
    │   ├── download_worker.py   # ClaimX download worker
    │   └── upload_worker.py     # ClaimX upload worker
    ├── handlers/
    │   ├── __init__.py
    │   ├── base.py              # EventHandler base
    │   ├── project.py           # ProjectHandler
    │   ├── media.py             # MediaHandler
    │   ├── task.py              # TaskHandler
    │   ├── contact.py           # PolicyholderHandler
    │   └── video.py             # VideoCollabHandler
    ├── writers/
    │   ├── __init__.py
    │   ├── delta_events.py      # ClaimX events table
    │   └── delta_entities.py    # ClaimX entity tables (7 tables)
    └── api_client.py            # ClaimX REST API client

```

## Decision Rationale

### Why Domain-Based Organization?

| Factor | Flat Structure | Domain-Based |
|--------|---------------|--------------|
| **Processing models** | xact and claimx have fundamentally different flows | Each domain owns its pipeline |
| **Enrichment** | Only claimx needs it - awkward to add | Natural fit in `claimx/` |
| **Entity tables** | Only claimx has 7 entity types | Contained in `claimx/writers/` |
| **API client** | Only claimx calls external API | Lives in `claimx/` |
| **Testing** | Mixed concerns, harder to isolate | Test each domain independently |
| **Deployment** | All workers deploy together | Can scale domains independently |
| **Future domains** | Gets messier each time | Clear pattern: add `domain_name/` |
| **Alignment** | Diverges from verisk_pipeline | Matches verisk_pipeline structure |

### Processing Model Differences

**xact pipeline:**
```
Event → Download → Upload → Result
```
- Events contain attachment URLs directly
- No API enrichment needed
- Single output table (inventory)

**claimx pipeline:**
```
Event → Enrich → Download → Upload → Result
           ↓
      Entity Tables (7)
```
- Events require API calls to get full data
- Enrichment worker is a new concept
- Multiple output tables (projects, contacts, media, tasks, etc.)

These are different enough that sharing workers would add complexity.

## What Gets Shared (common/)

| Component | Purpose | Used By |
|-----------|---------|---------|
| `consumer.py` | Base Kafka consumer with offset management | All workers |
| `producer.py` | Base Kafka producer with delivery callbacks | All workers |
| `metrics.py` | Prometheus metrics, counters | All workers |
| `monitoring.py` | Health check endpoints | All workers |
| `retry/handler.py` | Base retry logic, backoff calculation | Both domains |
| `retry/scheduler.py` | Retry topic scheduling | Both domains |
| `dlq/handler.py` | DLQ message handling | Both domains |
| `storage/onelake_client.py` | OneLake upload/download | Both domains |
| `writers/base.py` | Delta table base operations | Both domains |
| `eventhouse/kql_client.py` | Kusto queries | Both domains (if needed) |

## What Stays Domain-Specific

| Component | xact | claimx |
|-----------|------|--------|
| Event schema | `EventMessage` | `ClaimXEventMessage` |
| Task schema | `DownloadTaskMessage` | `ClaimXEnrichmentTask`, `ClaimXDownloadTask` |
| Event ingester | Produces download tasks | Produces enrichment tasks |
| Enrichment worker | N/A | Calls API, writes entities |
| Handlers | N/A | 5 handlers for event types |
| API client | N/A | ClaimXApiClient |
| Entity tables | N/A | 7 Delta tables |
| Download worker | URL from event | URL from API response |

## Migration Plan

### Phase 1: Create common/ structure
1. Create `common/` directory
2. Move shared code (consumer, producer, metrics, etc.)
3. Update imports throughout codebase
4. Ensure all tests pass

### Phase 2: Create xact/ domain
1. Create `xact/` directory structure
2. Move xact-specific schemas to `xact/schemas/`
3. Move xact workers to `xact/workers/`
4. Move xact writers to `xact/writers/`
5. Update imports and entry points
6. Ensure all tests pass

### Phase 3: Create claimx/ domain
1. Create `claimx/` directory structure
2. Implement claimx schemas
3. Implement claimx workers (including enrichment)
4. Port handlers from verisk_pipeline
5. Implement claimx writers
6. Add claimx entry points

### Phase 4: Update entry points
1. Update `__main__.py` with domain-prefixed commands:
   ```
   python -m kafka_pipeline xact-ingester
   python -m kafka_pipeline xact-downloader
   python -m kafka_pipeline claimx-ingester
   python -m kafka_pipeline claimx-enricher
   python -m kafka_pipeline claimx-downloader
   ```
2. Update config loading for domain-specific settings
3. Update deployment scripts/docs

## Import Examples

### Before (flat)
```python
from kafka_pipeline.schemas.events import EventMessage
from kafka_pipeline.workers.download_worker import DownloadWorker
from kafka_pipeline.storage.onelake_client import OneLakeClient
```

### After (domain-based)
```python
# xact domain
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.workers.download_worker import DownloadWorker

# claimx domain
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.workers.enrichment_worker import EnrichmentWorker

# shared
from kafka_pipeline.common.storage.onelake_client import OneLakeClient
from kafka_pipeline.common.consumer import BaseConsumer
```

## Configuration Updates

### config.py changes

```python
@dataclass
class KafkaConfig:
    # ... existing fields ...

    # Domain-specific topic prefixes
    xact_topic_prefix: str = "xact"
    claimx_topic_prefix: str = "claimx"

    # Domain-specific OneLake paths (already exists)
    onelake_domain_paths: Dict[str, str] = field(default_factory=dict)

    # ClaimX API settings (new)
    claimx_api_url: str = ""
    claimx_api_timeout_seconds: int = 30
    claimx_api_max_concurrent: int = 20

    def get_topic(self, domain: str, topic_type: str) -> str:
        """Get full topic name for domain.

        Example: get_topic("claimx", "enrichment.pending")
                 -> "claimx.enrichment.pending"
        """
        prefix = getattr(self, f"{domain}_topic_prefix", domain)
        return f"{prefix}.{topic_type}"
```

## Testing Strategy

### Unit Tests
- Each domain has its own test directory mirroring source structure
- `tests/kafka_pipeline/xact/`
- `tests/kafka_pipeline/claimx/`
- `tests/kafka_pipeline/common/`

### Integration Tests
- Domain-specific integration tests
- Cross-domain tests for shared infrastructure

### Test Isolation
- Can run `pytest tests/kafka_pipeline/xact/` independently
- Can run `pytest tests/kafka_pipeline/claimx/` independently

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking existing xact functionality | Phase 2 is pure refactor with no behavior changes; comprehensive tests |
| Import path changes break external code | Document migration; consider re-exports from old paths temporarily |
| Duplicate code between domains | Extract to `common/` aggressively; code review |
| Config complexity | Keep single config file with domain sections |

## Success Criteria

1. All existing xact tests pass after reorganization
2. Clear separation: no xact code imports from claimx or vice versa
3. Shared code lives only in `common/`
4. Adding a hypothetical third domain would be straightforward
5. Each domain can be deployed/scaled independently
