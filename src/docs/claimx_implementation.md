# ClaimX Domain Implementation for Kafka Pipeline

## Overview

This document outlines the implementation plan to add ClaimX domain support to the Kafka pipeline, achieving parity with the existing `verisk_pipeline/claimx` implementation.

## Current State Analysis

### Existing ClaimX Pipeline (`verisk_pipeline/claimx`)

The ClaimX pipeline follows a **batch processing model**:

```
Kusto Events -> Ingest Stage -> Enrich Stage -> Download Stage -> Retry Stage
                    |              |                 |
                    v              v                 v
             Delta Events    Entity Tables     OneLake Files
```

**Key Components:**

| Component | Purpose | Location |
|-----------|---------|----------|
| `ClaimXEvent` | Event dataclass from Kusto | `claimx_models.py:50-87` |
| `EntityRows` | Entity data container (7 types) | `claimx_models.py:90-128` |
| `EnrichmentResult` | API enrichment result | `claimx_models.py:131-177` |
| `MediaTask` | Download task | `claimx_models.py:227-275` |
| `IngestStage` | Read from Kusto, write to Delta | `stages/claimx_ingest.py` |
| `EnrichStage` | Call API, extract entities | `stages/enrich.py` |
| `DownloadStage` | Download media files | `stages/claimx_download.py` |
| `RetryStage` | Retry failed operations | `stages/claimx_retry_stage.py` |
| `ClaimXApiClient` | Async REST client | `api_client.py` |
| `EntityTableWriter` | Write to entity tables | `services/entity_writer.py` |

**Entity Types (written to separate Delta tables):**
1. `projects` -> `claimx_projects`
2. `contacts` -> `claimx_contacts`
3. `media` -> `claimx_attachment_metadata`
4. `tasks` -> `claimx_tasks`
5. `task_templates` -> `claimx_task_templates`
6. `external_links` -> `claimx_external_links`
7. `video_collab` -> `claimx_video_collab`

**Event Types Processed:**
- `PROJECT_CREATED`
- `PROJECT_FILE_ADDED`
- `PROJECT_MFN_ADDED`
- `CUSTOM_TASK_ASSIGNED`
- `CUSTOM_TASK_COMPLETED`
- `POLICYHOLDER_INVITED`
- `POLICYHOLDER_JOINED`
- `VIDEO_COLLABORATION_INVITE_SENT`
- `VIDEO_COLLABORATION_COMPLETED`

### Current Kafka Pipeline (`kafka_pipeline`)

The Kafka pipeline follows an **event-driven streaming model**:

```
Kafka Events -> EventIngester -> DownloadWorker -> UploadWorker -> ResultProcessor
                     |               |                  |
                     v               v                  v
              Delta Events     Local Cache        OneLake Files
```

**Key Components:**

| Component | Purpose | Location |
|-----------|---------|----------|
| `EventMessage` | Pydantic schema for events | `schemas/events.py` |
| `DownloadTaskMessage` | Download task schema | `schemas/tasks.py` |
| `EventIngesterWorker` | Consume events, produce tasks | `workers/event_ingester.py` |
| `DownloadWorker` | Download files | `workers/download_worker.py` |
| `UploadWorker` | Upload to OneLake | `workers/upload_worker.py` |
| `ResultProcessor` | Process results | `workers/result_processor.py` |

**Domain Support:**
- Config already has `onelake_domain_paths` with "xact" and "claimx" entries
- Topics are prefixed by domain (`xact.events.raw`, etc.)

## Implementation Architecture

### Target Architecture

```
                                    ┌─────────────────┐
                                    │   Kafka Topics  │
                                    └────────┬────────┘
                                             │
              ┌──────────────────────────────┼──────────────────────────────┐
              │                              │                              │
              ▼                              ▼                              ▼
    ┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
    │ claimx.events   │          │ claimx.downloads │          │ claimx.enrichment│
    │     .raw        │          │    .pending      │          │    .pending      │
    └────────┬────────┘          └────────┬────────┘          └────────┬────────┘
             │                            │                            │
             ▼                            ▼                            ▼
    ┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
    │   ClaimX Event  │          │   ClaimX        │          │   ClaimX        │
    │   Ingester      │          │   Download      │          │   Enrichment    │
    │   Worker        │          │   Worker        │          │   Worker        │
    └────────┬────────┘          └────────┬────────┘          └────────┬────────┘
             │                            │                            │
             ├────────────┐               │                ┌───────────┤
             ▼            ▼               ▼                ▼           ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────┐
    │ Delta Events │ │ Produce  │ │ OneLake      │ │ Entity       │ │ ClaimX  │
    │ Table        │ │ Enrich   │ │ Files        │ │ Tables       │ │ API     │
    └──────────────┘ │ Tasks    │ └──────────────┘ └──────────────┘ └─────────┘
                     └──────────┘
```

### Key Design Decisions

1. **Separate Topics per Domain**: `claimx.events.raw`, `claimx.downloads.pending`, etc.
2. **New Enrichment Worker**: Calls ClaimX API to fetch full entity data
3. **Shared Download/Upload Infrastructure**: Reuse existing download/upload workers with domain awareness
4. **Entity Table Writers**: New writers for 7 entity types

## Implementation Phases

### Phase 1: Schemas and Configuration

**Files to Create/Modify:**

#### 1.1 ClaimX Event Schema
`kafka_pipeline/schemas/claimx_events.py`

```python
class ClaimXEventMessage(BaseModel):
    """Schema for ClaimX events from Eventhouse/EventHub.

    Maps to verisk_pipeline.claimx.claimx_models.ClaimXEvent
    """
    event_id: str
    event_type: str  # e.g., "PROJECT_CREATED", "PROJECT_FILE_ADDED"
    project_id: str
    ingested_at: datetime

    # Optional fields depending on event type
    media_id: Optional[str] = None
    task_assignment_id: Optional[str] = None
    video_collaboration_id: Optional[str] = None
    master_file_name: Optional[str] = None

    # Raw payload for handler-specific parsing
    raw_data: Optional[Dict[str, Any]] = None
```

#### 1.2 ClaimX Task Schemas
`kafka_pipeline/schemas/claimx_tasks.py`

```python
class ClaimXEnrichmentTask(BaseModel):
    """Task for enriching a ClaimX event via API."""
    event_id: str
    event_type: str
    project_id: str
    retry_count: int = 0
    created_at: datetime

class ClaimXDownloadTask(BaseModel):
    """Task for downloading ClaimX media."""
    media_id: str
    project_id: str
    download_url: str
    blob_path: str
    file_type: str = ""
    file_name: str = ""
    source_event_id: str = ""
    retry_count: int = 0
    expires_at: Optional[str] = None
```

#### 1.3 Configuration Updates
`kafka_pipeline/config.py`

Add ClaimX-specific topics:
```python
# ClaimX topics (parallel to xact)
claimx_events_topic: str = "claimx.events.raw"
claimx_enrichment_pending_topic: str = "claimx.enrichment.pending"
claimx_downloads_pending_topic: str = "claimx.downloads.pending"
claimx_downloads_results_topic: str = "claimx.downloads.results"
claimx_dlq_topic: str = "claimx.downloads.dlq"

# ClaimX API settings
claimx_api_url: str = ""
claimx_api_token_env: str = "CLAIMX_API_TOKEN"
claimx_api_max_concurrent: int = 20
claimx_api_timeout_seconds: int = 30
```

### Phase 2: Event Ingester

**Files to Create:**

#### 2.1 ClaimX Event Ingester Worker
`kafka_pipeline/workers/claimx_event_ingester.py`

Responsibilities:
- Consume from `claimx.events.raw`
- Parse ClaimXEventMessage
- Write to Delta events table
- Produce enrichment tasks to `claimx.enrichment.pending`

Key differences from xact:
- Different event schema (ClaimXEventMessage vs EventMessage)
- Produces enrichment tasks instead of download tasks directly
- No attachment extraction (handled in enrichment)

### Phase 3: Enrichment Worker (New Component)

**Files to Create:**

#### 3.1 ClaimX Enrichment Worker
`kafka_pipeline/workers/claimx_enrichment_worker.py`

Responsibilities:
- Consume from `claimx.enrichment.pending`
- Call ClaimX API to get full entity data
- Route to appropriate handler based on event_type
- Write to entity Delta tables
- Produce download tasks for media
- Handle retries for API failures

#### 3.2 Handler Registry
`kafka_pipeline/claimx/handlers/`

Port handlers from `verisk_pipeline/claimx/stages/handlers/`:
- `base.py` - EventHandler base class, HandlerRegistry
- `project.py` - ProjectHandler
- `media.py` - MediaHandler
- `task.py` - TaskHandler
- `contact.py` - PolicyholderHandler
- `video.py` - VideoCollabHandler

#### 3.3 ClaimX API Client
`kafka_pipeline/claimx/api_client.py`

Port from `verisk_pipeline/claimx/api_client.py`:
- ClaimXApiClient with circuit breaker
- Methods: get_project, get_project_media, get_custom_task, get_video_collaboration

### Phase 4: Entity Writers

**Files to Create:**

#### 4.1 Entity Table Writer
`kafka_pipeline/writers/claimx_entities.py`

Write to Delta tables with merge semantics:
- `claimx_projects`
- `claimx_contacts`
- `claimx_attachment_metadata`
- `claimx_tasks`
- `claimx_task_templates`
- `claimx_external_links`
- `claimx_video_collab`

Merge keys (from `verisk_pipeline/claimx/services/entity_writer.py:MERGE_KEYS`):
```python
MERGE_KEYS = {
    "projects": ["project_id"],
    "contacts": ["contact_id", "project_id"],
    "media": ["media_id"],
    "tasks": ["task_id"],
    "task_templates": ["template_id", "project_id"],
    "external_links": ["link_id"],
    "video_collab": ["collaboration_id"],
}
```

### Phase 5: Download Worker Updates

**Files to Modify:**

#### 5.1 Download Worker Domain Support
`kafka_pipeline/workers/download_worker.py`

Updates needed:
- Add ClaimXDownloadTask parsing
- Handle ClaimX URL format (S3 presigned URLs)
- Domain-specific blob path resolution
- URL expiration handling (refresh via API)

#### 5.2 OneLake Client Updates
`kafka_pipeline/storage/onelake_client.py`

Updates needed:
- Domain-aware path resolution
- ClaimX file organization pattern

### Phase 6: Retry and DLQ

**Files to Create/Modify:**

#### 6.1 ClaimX Retry Handler
`kafka_pipeline/retry/claimx_handler.py`

Handle retries for:
- Enrichment failures (API errors)
- Download failures (network, S3 errors)
- URL expiration (refresh and retry)

### Phase 7: Orchestration

**Files to Modify:**

#### 7.1 Main Entry Point
`kafka_pipeline/__main__.py`

Add commands:
- `claimx-ingester` - Run ClaimX event ingester
- `claimx-enricher` - Run ClaimX enrichment worker
- `claimx-downloader` - Run ClaimX download worker

## File Structure Summary

```
kafka_pipeline/
├── schemas/
│   ├── claimx_events.py      # NEW: ClaimXEventMessage
│   └── claimx_tasks.py       # NEW: ClaimXEnrichmentTask, ClaimXDownloadTask
├── claimx/                   # NEW: ClaimX-specific components
│   ├── __init__.py
│   ├── api_client.py         # NEW: ClaimXApiClient
│   └── handlers/             # NEW: Event handlers
│       ├── __init__.py
│       ├── base.py
│       ├── project.py
│       ├── media.py
│       ├── task.py
│       ├── contact.py
│       └── video.py
├── workers/
│   ├── claimx_event_ingester.py   # NEW: ClaimX event ingester
│   └── claimx_enrichment_worker.py # NEW: ClaimX enrichment worker
├── writers/
│   └── claimx_entities.py    # NEW: Entity table writers
├── retry/
│   └── claimx_handler.py     # NEW: ClaimX retry logic
└── config.py                 # MODIFY: Add ClaimX config
```

## Kafka Topics

| Topic | Purpose | Consumer Group |
|-------|---------|----------------|
| `claimx.events.raw` | Raw events from EventHub | `claimx-event-ingester-worker` |
| `claimx.enrichment.pending` | All event types awaiting API enrichment (single queue) | `claimx-enrichment-worker` |
| `claimx.downloads.pending` | Media awaiting download | `claimx-download-worker` |
| `claimx.downloads.cached` | Downloaded files awaiting upload | `claimx-upload-worker` |
| `claimx.downloads.results` | Completed download results | `claimx-result-processor` |
| `claimx.downloads.dlq` | Failed downloads (exhausted retries) | Manual processing |
| `claimx.enrichment.dlq` | Failed enrichments (exhausted retries) | Manual processing |
| `claimx.enrichment.retry.*` | Enrichment retry buckets (5m, 10m, 20m, 40m) | Retry scheduler |
| `claimx.downloads.retry.*` | Download retry buckets | Retry scheduler |

## Delta Tables

| Table | Purpose | Merge Keys |
|-------|---------|------------|
| `claimx_events` | Raw events | `event_id` |
| `claimx_event_log` | Processing log | `event_id` |
| `claimx_projects` | Project entities | `project_id` |
| `claimx_contacts` | Contact entities | `contact_id`, `project_id` |
| `claimx_attachment_metadata` | Media metadata | `media_id` |
| `claimx_tasks` | Task entities | `task_id` |
| `claimx_task_templates` | Task templates | `template_id`, `project_id` |
| `claimx_external_links` | External links | `link_id` |
| `claimx_video_collab` | Video collaborations | `collaboration_id` |
| `claimx_attachments` | Downloaded files inventory | `media_id` |

## Configuration Reference

Environment variables needed:
```bash
# Kafka (existing)
KAFKA_BOOTSTRAP_SERVERS=...
ONELAKE_CLAIMX_PATH=abfss://...

# ClaimX API (new)
CLAIMX_API_URL=https://api.claimxperience.com
CLAIMX_API_TOKEN=<base64_auth_token>

# Optional
CLAIMX_API_MAX_CONCURRENT=20
CLAIMX_API_TIMEOUT_SECONDS=30
```

## Implementation Order

1. **Phase 1**: Schemas and Configuration (foundation)
2. **Phase 2**: Event Ingester (basic event flow)
3. **Phase 3**: Enrichment Worker + Handlers (core processing)
4. **Phase 4**: Entity Writers (data persistence)
5. **Phase 5**: Download Worker Updates (file handling)
6. **Phase 6**: Retry and DLQ (resilience)
7. **Phase 7**: Orchestration (deployment)

## Testing Strategy

1. **Unit Tests**
   - Schema validation
   - Handler logic
   - Entity transformations

2. **Integration Tests**
   - End-to-end event flow
   - API mock testing
   - Delta table operations

3. **Performance Tests**
   - Throughput under load
   - Concurrent API calls
   - Large batch processing

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| API rate limiting | Circuit breaker + exponential backoff |
| S3 URL expiration | Refresh URLs before retry |
| Large entity batches | Batch size limits + pagination |
| Schema evolution | Pydantic validation + graceful handling |

## Anti-Patterns to Avoid

The following patterns from `verisk_pipeline/claimx` should **NOT** be reproduced:

### 1. Environment Variables in `__post_init__`
The claimx config reads env vars in dataclass `__post_init__` methods:
```python
# BAD - Don't do this
def __post_init__(self):
    self.base_url = os.getenv("CLAIMX_API_URL", self.base_url)
```

**Why it's bad:** Makes testing hard, behavior depends on instantiation timing.

**What to do instead:** Keep env var loading in `load_config()` / `_apply_env_overrides()` as kafka_pipeline already does.

### 2. Global Singleton Handler Registry
```python
# BAD - Don't do this
_handler_registry: Optional["HandlerRegistry"] = None

def get_handler_registry() -> HandlerRegistry:
    global _handler_registry
    ...
```

**Why it's bad:** Global mutable state, testing nightmares, implicit dependencies.

**What to do instead:** Pass handler mapping as explicit dependency, or use a simple dict:
```python
CLAIMX_HANDLERS: Dict[str, Type[EventHandler]] = {
    "PROJECT_CREATED": ProjectHandler,
    "PROJECT_FILE_ADDED": MediaHandler,
    ...
}
```

### 3. In-Memory ProcessedEventCache
The claimx pipeline maintains an in-memory set of processed event IDs.

**Why it's bad:** Memory grows unbounded, lost on restart, doesn't work with multiple workers.

**What to do instead:** Rely on Kafka consumer offsets for progress tracking. Use Delta table dedup at write time (already implemented in kafka_pipeline).

### 4. Watermark-Based Progress Tracking
The batch pipeline uses watermarks stored in Delta tables.

**Why it's bad for Kafka:** Redundant - Kafka consumer groups already track offsets.

**What to do instead:** Use Kafka's native offset management.

### 5. Mixed Sync/Async Wrappers
The claimx pipeline uses `run_async_with_shutdown()` wrappers.

**What to do instead:** Keep kafka_pipeline consistently async throughout.

## Design Decisions

### Single Enrichment Queue with Consumer-Side Batching

**Decision:** Use a single `claimx.enrichment.pending` topic for all event types.

**Architecture:**
```
claimx.enrichment.pending → Enrichment Worker
                                   ↓
                           Pull batch of N events
                                   ↓
                           Group by handler type
                                   ↓
              ┌────────────────────┼────────────────────┐
              ↓                    ↓                    ↓
         MediaHandler         TaskHandler        ProjectHandler
         (batch by project)   (concurrent)       (concurrent)
```

**Rationale:**
- Simpler topology (one topic, one consumer group)
- Consumer-side grouping enables handler-specific optimizations
- MediaHandler can batch by project_id to reduce API calls (10 events = 1 API call)
- Scale horizontally by adding consumer instances
- No need for per-handler queues unless SLAs or volumes diverge significantly

**Handler routing:**
```python
CLAIMX_HANDLERS: Dict[str, Type[EventHandler]] = {
    "PROJECT_CREATED": ProjectHandler,
    "PROJECT_FILE_ADDED": MediaHandler,
    "PROJECT_MFN_ADDED": MediaHandler,
    "CUSTOM_TASK_ASSIGNED": TaskHandler,
    "CUSTOM_TASK_COMPLETED": TaskHandler,
    "POLICYHOLDER_INVITED": PolicyholderHandler,
    "POLICYHOLDER_JOINED": PolicyholderHandler,
    "VIDEO_COLLABORATION_INVITE_SENT": VideoCollabHandler,
    "VIDEO_COLLABORATION_COMPLETED": VideoCollabHandler,
}
```

### Kafka Partitioning: No Special Partitioning (Round-Robin)

**Decision:** Use default Kafka partitioning (round-robin) for enrichment tasks.

**Rationale:** Event ordering doesn't matter. What matters is **data dependency** - child entities (media, tasks, contacts) require the parent project row to exist first.

**Implementation:** The enrichment worker performs a pre-flight check before processing each batch:

```python
async def process_batch(events: List[ClaimXEnrichmentTask]):
    # 1. Collect all project_ids from this batch
    project_ids = {e.project_id for e in events}

    # 2. Ensure all projects exist (fetch from API if missing)
    for pid in project_ids:
        if not await project_exists_in_delta(pid):
            project_data = await claimx_api.get_project(pid)
            await write_project_to_delta(project_data)

    # 3. NOW process events (project rows guaranteed to exist)
    for event in events:
        await process_event(event)  # Safe to write child entities
```

This pattern:
- Handles out-of-order events gracefully
- Works with any number of consumer instances
- Avoids hot partitions from high-activity projects
- Already proven in `verisk_pipeline/claimx` via `EnrichStage._ensure_projects_exist`

## Open Questions

1. What's the expected event volume for ClaimX vs xact?
2. Are there ClaimX-specific security requirements (allowed domains, etc.)?
3. Should we support running both domains in a single worker process?
