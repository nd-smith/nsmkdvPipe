# ClaimX Implementation Backlog

> **Prerequisite:** Assumes kafka_pipeline reorganization is complete (common/, xact/, claimx/ structure).

## Work Package Structure

Each work package is designed to be completable in one session (~1-2 hours). Packages include:
- Clear deliverables
- Dependencies on prior packages
- Acceptance criteria
- Files to create/modify

---

## Epic 1: Foundation (Schemas & Config)

### WP-1.1: ClaimX Event Schema
**Priority:** P0 | **Dependencies:** None

Create the Pydantic schema for ClaimX events.

**Files:**
- `kafka_pipeline/claimx/schemas/__init__.py`
- `kafka_pipeline/claimx/schemas/events.py`

**Deliverables:**
- [ ] `ClaimXEventMessage` Pydantic model with fields:
  - event_id, event_type, project_id, ingested_at
  - Optional: media_id, task_assignment_id, video_collaboration_id, master_file_name
  - raw_data for handler-specific parsing
- [ ] Unit tests for schema validation
- [ ] Test cases for each event type (PROJECT_CREATED, PROJECT_FILE_ADDED, etc.)

**Acceptance Criteria:**
- Schema validates all 9 event types from ClaimX Kusto
- Proper datetime handling (timezone-aware)
- Serialization/deserialization round-trips correctly

---

### WP-1.2: ClaimX Task Schemas
**Priority:** P0 | **Dependencies:** WP-1.1

Create task schemas for enrichment and download stages.

**Files:**
- `kafka_pipeline/claimx/schemas/tasks.py`

**Deliverables:**
- [ ] `ClaimXEnrichmentTask` model:
  - event_id, event_type, project_id, retry_count, created_at
- [ ] `ClaimXDownloadTask` model:
  - media_id, project_id, download_url, blob_path
  - file_type, file_name, source_event_id, retry_count, expires_at
- [ ] Unit tests for both schemas

**Acceptance Criteria:**
- Schemas compatible with Kafka JSON serialization
- Default values for optional fields
- Retry count tracking works

---

### WP-1.3: ClaimX Entity Row Schemas
**Priority:** P0 | **Dependencies:** None

Create schemas for entity data written to Delta tables.

**Files:**
- `kafka_pipeline/claimx/schemas/entities.py`

**Deliverables:**
- [ ] `ProjectRow` schema
- [ ] `ContactRow` schema
- [ ] `MediaMetadataRow` schema
- [ ] `TaskRow` schema
- [ ] `TaskTemplateRow` schema
- [ ] `ExternalLinkRow` schema
- [ ] `VideoCollabRow` schema
- [ ] Unit tests

**Acceptance Criteria:**
- Schemas match existing Delta table structure in verisk_pipeline
- All fields typed correctly (str, int, datetime, Optional)
- Merge keys identified per entity type

---

### WP-1.4: Config Updates for ClaimX
**Priority:** P0 | **Dependencies:** WP-1.1, WP-1.2

Add ClaimX configuration to kafka_pipeline config.

**Files:**
- `kafka_pipeline/config.py`

**Deliverables:**
- [ ] ClaimX topic names:
  - `claimx_events_topic`
  - `claimx_enrichment_pending_topic`
  - `claimx_downloads_pending_topic`
  - `claimx_downloads_results_topic`
  - `claimx_dlq_topic`
- [ ] ClaimX API settings:
  - `claimx_api_url`
  - `claimx_api_token_env`
  - `claimx_api_max_concurrent`
  - `claimx_api_timeout_seconds`
- [ ] Environment variable overrides in `_apply_env_overrides()`
- [ ] Unit tests for config loading

**Acceptance Criteria:**
- Config loads with defaults when env vars not set
- Config loads from environment variables when set
- Existing xact config unchanged

---

## Epic 2: API Client

### WP-2.1: ClaimX API Client - Core
**Priority:** P0 | **Dependencies:** WP-1.4

Create async API client for ClaimX REST API.

**Files:**
- `kafka_pipeline/claimx/api_client.py`

**Deliverables:**
- [ ] `ClaimXApiClient` class with:
  - Constructor taking config (url, token, timeout, max_concurrent)
  - Semaphore for concurrency limiting
  - aiohttp session management (context manager)
- [ ] `get_project(project_id)` method
- [ ] Basic error handling (HTTP errors, timeouts)
- [ ] Unit tests with mocked responses

**Acceptance Criteria:**
- Client respects max_concurrent limit
- Proper async context management
- Returns structured data (dict or Pydantic model)

---

### WP-2.2: ClaimX API Client - Additional Methods
**Priority:** P0 | **Dependencies:** WP-2.1

Add remaining API methods.

**Files:**
- `kafka_pipeline/claimx/api_client.py`

**Deliverables:**
- [ ] `get_project_media(project_id)` - returns list of media with presigned URLs
- [ ] `get_custom_task(task_assignment_id)` - returns task details
- [ ] `get_video_collaboration(video_collab_id)` - returns video collab details
- [ ] Unit tests for each method

**Acceptance Criteria:**
- All methods handle 404 (not found) gracefully
- URL expiration metadata preserved for media
- Error responses captured with context

---

### WP-2.3: API Client - Circuit Breaker
**Priority:** P1 | **Dependencies:** WP-2.2

Add circuit breaker pattern for API resilience.

**Files:**
- `kafka_pipeline/claimx/api_client.py`

**Deliverables:**
- [ ] Circuit breaker with states: closed, open, half-open
- [ ] Configurable failure threshold and reset timeout
- [ ] Metrics/logging on circuit state changes
- [ ] Unit tests for circuit breaker behavior

**Acceptance Criteria:**
- Circuit opens after N consecutive failures
- Circuit half-opens after timeout
- Single success in half-open closes circuit

---

## Epic 3: Event Handlers

### WP-3.1: Handler Base Class
**Priority:** P0 | **Dependencies:** WP-1.3, WP-2.2

Create base handler class and handler mapping.

**Files:**
- `kafka_pipeline/claimx/handlers/__init__.py`
- `kafka_pipeline/claimx/handlers/base.py`

**Deliverables:**
- [ ] `EventHandler` abstract base class with:
  - `handle(event, api_client, writer)` method
  - `supports_batching` attribute
  - `batch_key` attribute (for batch grouping)
- [ ] `CLAIMX_HANDLERS` dict mapping event_type → handler class
- [ ] Unit tests

**Acceptance Criteria:**
- No global singleton registry (use explicit dict)
- Handler interface supports both single and batch processing
- Type hints throughout

---

### WP-3.2: ProjectHandler
**Priority:** P0 | **Dependencies:** WP-3.1

Implement handler for PROJECT_CREATED events.

**Files:**
- `kafka_pipeline/claimx/handlers/project.py`

**Deliverables:**
- [ ] `ProjectHandler` class
- [ ] Fetch project from API
- [ ] Extract ProjectRow
- [ ] Write to projects table
- [ ] Unit tests with mocked API

**Acceptance Criteria:**
- Creates project row on first event
- Handles already-existing project gracefully
- Logs processing outcomes

---

### WP-3.3: MediaHandler
**Priority:** P0 | **Dependencies:** WP-3.1

Implement handler for PROJECT_FILE_ADDED and PROJECT_MFN_ADDED events.

**Files:**
- `kafka_pipeline/claimx/handlers/media.py`

**Deliverables:**
- [ ] `MediaHandler` class
- [ ] Batch support (group by project_id)
- [ ] Fetch project media from API
- [ ] Extract MediaMetadataRow for each media item
- [ ] Produce download tasks for each media item
- [ ] Unit tests

**Acceptance Criteria:**
- Batches API calls by project (10 events for same project = 1 API call)
- Download tasks include presigned URL and expiration
- Handles projects with no media gracefully

---

### WP-3.4: TaskHandler
**Priority:** P1 | **Dependencies:** WP-3.1

Implement handler for CUSTOM_TASK_ASSIGNED and CUSTOM_TASK_COMPLETED events.

**Files:**
- `kafka_pipeline/claimx/handlers/task.py`

**Deliverables:**
- [ ] `TaskHandler` class
- [ ] Fetch task details from API
- [ ] Extract TaskRow and TaskTemplateRow
- [ ] Write to tasks and task_templates tables
- [ ] Unit tests

**Acceptance Criteria:**
- Updates task status on COMPLETED events
- Template written only if not exists
- Handles missing task (404) gracefully

---

### WP-3.5: PolicyholderHandler
**Priority:** P1 | **Dependencies:** WP-3.1

Implement handler for POLICYHOLDER_INVITED and POLICYHOLDER_JOINED events.

**Files:**
- `kafka_pipeline/claimx/handlers/contact.py`

**Deliverables:**
- [ ] `PolicyholderHandler` class
- [ ] Extract contact from event payload
- [ ] Create ContactRow
- [ ] Write to contacts table
- [ ] Unit tests

**Acceptance Criteria:**
- Updates invite_status on JOINED events
- Handles duplicate invites

---

### WP-3.6: VideoCollabHandler
**Priority:** P1 | **Dependencies:** WP-3.1

Implement handler for VIDEO_COLLABORATION_* events.

**Files:**
- `kafka_pipeline/claimx/handlers/video.py`

**Deliverables:**
- [ ] `VideoCollabHandler` class
- [ ] Fetch video collaboration from API
- [ ] Extract VideoCollabRow
- [ ] Write to video_collab table
- [ ] Unit tests

**Acceptance Criteria:**
- Updates status on COMPLETED events
- Includes recording URLs when available

---

## Epic 4: Entity Writers

### WP-4.1: Entity Writer Base
**Priority:** P0 | **Dependencies:** WP-1.3

Create base Delta writer for entity tables.

**Files:**
- `kafka_pipeline/claimx/writers/__init__.py`
- `kafka_pipeline/claimx/writers/entities.py`

**Deliverables:**
- [ ] `EntityTableWriter` class
- [ ] Generic `merge_rows(table_name, rows, merge_keys)` method
- [ ] Table path resolution using config
- [ ] Unit tests with mock Delta operations

**Acceptance Criteria:**
- MERGE semantics (upsert on merge keys)
- Handles empty row list gracefully
- Logging of rows written

---

### WP-4.2: Entity Writer - All Tables
**Priority:** P0 | **Dependencies:** WP-4.1

Add specific table configurations.

**Files:**
- `kafka_pipeline/claimx/writers/entities.py`

**Deliverables:**
- [ ] Merge keys for each table:
  - projects: [project_id]
  - contacts: [contact_id, project_id]
  - media: [media_id]
  - tasks: [task_id]
  - task_templates: [template_id, project_id]
  - external_links: [link_id]
  - video_collab: [collaboration_id]
- [ ] Convenience methods: `write_projects()`, `write_contacts()`, etc.
- [ ] Integration tests with real Delta (optional, mocked OK)

**Acceptance Criteria:**
- All 7 entity tables supported
- Schema evolution handled (new columns OK)

---

### WP-4.3: ClaimX Events Table Writer
**Priority:** P0 | **Dependencies:** None

Create Delta writer for claimx_events table.

**Files:**
- `kafka_pipeline/claimx/writers/delta_events.py`

**Deliverables:**
- [ ] `ClaimXEventsWriter` class (similar to existing xact events writer)
- [ ] Write raw events to `claimx_events` table
- [ ] Dedup on event_id
- [ ] Unit tests

**Acceptance Criteria:**
- Events written with ingestion timestamp
- Duplicate events ignored (idempotent)

---

## Epic 5: Workers

### WP-5.1: ClaimX Event Ingester Worker
**Priority:** P0 | **Dependencies:** WP-1.1, WP-1.2, WP-1.4, WP-4.3

Create event ingester worker.

**Files:**
- `kafka_pipeline/claimx/workers/__init__.py`
- `kafka_pipeline/claimx/workers/event_ingester.py`

**Deliverables:**
- [ ] `ClaimXEventIngesterWorker` class
- [ ] Consume from `claimx.events.raw`
- [ ] Parse `ClaimXEventMessage`
- [ ] Write to `claimx_events` Delta table
- [ ] Produce `ClaimXEnrichmentTask` to enrichment topic
- [ ] Unit tests with mock Kafka

**Acceptance Criteria:**
- Handles malformed events (log and skip)
- Commits offsets after successful write
- Produces one enrichment task per event

---

### WP-5.2: Enrichment Worker - Core Loop
**Priority:** P0 | **Dependencies:** WP-3.1, WP-5.1

Create enrichment worker main loop.

**Files:**
- `kafka_pipeline/claimx/workers/enrichment_worker.py`

**Deliverables:**
- [ ] `ClaimXEnrichmentWorker` class
- [ ] Consume from `claimx.enrichment.pending`
- [ ] Batch processing loop
- [ ] Handler routing by event_type
- [ ] Basic error handling (log and continue)
- [ ] Unit tests

**Acceptance Criteria:**
- Processes batches of events (configurable batch size)
- Routes to correct handler
- Graceful handling of unknown event types

---

### WP-5.3: Enrichment Worker - Pre-flight Project Check
**Priority:** P0 | **Dependencies:** WP-5.2, WP-3.2

Add pre-flight project existence check.

**Files:**
- `kafka_pipeline/claimx/workers/enrichment_worker.py`

**Deliverables:**
- [ ] `_ensure_projects_exist(project_ids)` method
- [ ] Check Delta table for existing projects
- [ ] Fetch missing projects from API
- [ ] Write missing projects before processing batch
- [ ] Unit tests

**Acceptance Criteria:**
- All project_ids in batch have rows before processing events
- Minimizes API calls (batch check, not per-event)
- Handles API failures for project fetch

---

### WP-5.4: Enrichment Worker - Batch Optimization
**Priority:** P1 | **Dependencies:** WP-5.3, WP-3.3

Add handler-level batching optimization.

**Files:**
- `kafka_pipeline/claimx/workers/enrichment_worker.py`

**Deliverables:**
- [ ] Group events by handler type
- [ ] For MediaHandler: group by project_id before calling
- [ ] Concurrent handler execution (where handlers are independent)
- [ ] Metrics on batch sizes
- [ ] Unit tests

**Acceptance Criteria:**
- MediaHandler receives project-grouped events
- Other handlers process concurrently
- Throughput improved vs naive sequential

---

### WP-5.5: Enrichment Worker - Download Task Production
**Priority:** P0 | **Dependencies:** WP-5.2, WP-3.3

Produce download tasks from enrichment.

**Files:**
- `kafka_pipeline/claimx/workers/enrichment_worker.py`

**Deliverables:**
- [ ] After MediaHandler, produce `ClaimXDownloadTask` for each media item
- [ ] Include presigned URL and expiration
- [ ] Produce to `claimx.downloads.pending`
- [ ] Unit tests

**Acceptance Criteria:**
- One download task per media item
- URL expiration captured
- Blob path follows claimx pattern

---

### WP-5.6: ClaimX Download Worker
**Priority:** P0 | **Dependencies:** WP-1.2, WP-1.4

Create or adapt download worker for ClaimX.

**Files:**
- `kafka_pipeline/claimx/workers/download_worker.py`

**Deliverables:**
- [ ] `ClaimXDownloadWorker` class (or adapt existing)
- [ ] Consume `ClaimXDownloadTask` from `claimx.downloads.pending`
- [ ] Download from S3 presigned URL
- [ ] Handle URL expiration (log for retry)
- [ ] Produce to upload queue or write directly
- [ ] Unit tests

**Acceptance Criteria:**
- Downloads files to local cache
- Handles expired URLs (sends to retry)
- Domain validation (only allowed S3 domains)

---

### WP-5.7: ClaimX Upload Worker
**Priority:** P0 | **Dependencies:** WP-5.6

Create upload worker for ClaimX files.

**Files:**
- `kafka_pipeline/claimx/workers/upload_worker.py`

**Deliverables:**
- [ ] `ClaimXUploadWorker` class
- [ ] Upload to OneLake (claimx path)
- [ ] Write to `claimx_attachments` inventory table
- [ ] Produce results to results topic
- [ ] Unit tests

**Acceptance Criteria:**
- Files uploaded to correct claimx lakehouse path
- Inventory record created on success
- Handles upload failures

---

### WP-5.8: ClaimX Result Processor
**Priority:** P1 | **Dependencies:** WP-5.7

Create result processor for ClaimX.

**Files:**
- `kafka_pipeline/claimx/workers/result_processor.py`

**Deliverables:**
- [ ] `ClaimXResultProcessor` class
- [ ] Consume from `claimx.downloads.results`
- [ ] Update inventory table with final status
- [ ] Metrics on success/failure rates
- [ ] Unit tests

**Acceptance Criteria:**
- Aggregates results for monitoring
- Updates attachment records
- Handles result processing failures

---

## Epic 6: Retry & DLQ

### WP-6.1: Enrichment Retry Handler
**Priority:** P1 | **Dependencies:** WP-5.2

Handle enrichment failures with retry.

**Files:**
- `kafka_pipeline/claimx/retry/enrichment_handler.py`

**Deliverables:**
- [ ] `EnrichmentRetryHandler` class
- [ ] Route failures to retry buckets (5m, 10m, 20m, 40m)
- [ ] Route exhausted retries to DLQ
- [ ] Metrics on retry counts
- [ ] Unit tests

**Acceptance Criteria:**
- Exponential backoff on retries
- Max retry limit enforced
- DLQ receives all exhausted messages

---

### WP-6.2: Download Retry with URL Refresh
**Priority:** P1 | **Dependencies:** WP-5.6, WP-2.2

Handle download failures with URL refresh.

**Files:**
- `kafka_pipeline/claimx/retry/download_handler.py`

**Deliverables:**
- [ ] `DownloadRetryHandler` class
- [ ] Detect expired URL failures
- [ ] Refresh URL from API before retry
- [ ] Route to retry or DLQ
- [ ] Unit tests

**Acceptance Criteria:**
- Expired URLs refreshed before retry attempt
- Permanent failures (404, forbidden) go to DLQ immediately
- Transient failures use backoff

---

### WP-6.3: DLQ CLI for ClaimX
**Priority:** P2 | **Dependencies:** WP-6.1, WP-6.2

CLI tools for managing claimx DLQs.

**Files:**
- `kafka_pipeline/claimx/dlq/cli.py`

**Deliverables:**
- [ ] `claimx-dlq list` - show DLQ message counts
- [ ] `claimx-dlq inspect` - show sample messages
- [ ] `claimx-dlq replay` - replay messages to main queue
- [ ] `claimx-dlq purge` - clear DLQ (with confirmation)
- [ ] Unit tests

**Acceptance Criteria:**
- Safe operations (confirmation for destructive)
- Works for both enrichment and download DLQs

---

## Epic 7: Entry Points & Orchestration

### WP-7.1: Main Entry Point Commands
**Priority:** P0 | **Dependencies:** WP-5.1, WP-5.2, WP-5.6, WP-5.7

Add claimx commands to __main__.py.

**Files:**
- `kafka_pipeline/__main__.py`

**Deliverables:**
- [ ] `claimx-ingester` command
- [ ] `claimx-enricher` command
- [ ] `claimx-downloader` command
- [ ] `claimx-uploader` command
- [ ] Help text for each command

**Acceptance Criteria:**
- Commands work: `python -m kafka_pipeline claimx-ingester`
- Proper argument parsing
- Graceful shutdown on SIGTERM

---

### WP-7.2: Health Checks for ClaimX Workers
**Priority:** P1 | **Dependencies:** WP-7.1

Add health check endpoints for claimx workers.

**Files:**
- `kafka_pipeline/claimx/monitoring.py`

**Deliverables:**
- [ ] Health check endpoints per worker
- [ ] Liveness: worker loop running
- [ ] Readiness: Kafka connection OK, API reachable
- [ ] Unit tests

**Acceptance Criteria:**
- Kubernetes-compatible health endpoints
- Reasonable timeouts

---

## Epic 8: Testing & Documentation

### WP-8.1: Integration Tests - Event Flow
**Priority:** P1 | **Dependencies:** WP-5.1, WP-5.5

End-to-end test for event ingestion.

**Files:**
- `tests/kafka_pipeline/claimx/integration/test_event_flow.py`

**Deliverables:**
- [ ] Test: event → ingester → enrichment queue
- [ ] Test: enrichment → handler → entity table
- [ ] Test: enrichment → download queue
- [ ] Mock Kafka and API

**Acceptance Criteria:**
- Full flow tested with mocks
- Assertions on data written

---

### WP-8.2: Integration Tests - Download Flow
**Priority:** P1 | **Dependencies:** WP-5.6, WP-5.7, WP-5.8

End-to-end test for download flow.

**Files:**
- `tests/kafka_pipeline/claimx/integration/test_download_flow.py`

**Deliverables:**
- [ ] Test: download task → download → upload → result
- [ ] Test: expired URL → retry → refresh → success
- [ ] Test: permanent failure → DLQ
- [ ] Mock S3 and OneLake

**Acceptance Criteria:**
- Retry scenarios tested
- DLQ behavior verified

---

### WP-8.3: Performance Tests
**Priority:** P2 | **Dependencies:** WP-8.1, WP-8.2

Throughput and latency tests.

**Files:**
- `tests/kafka_pipeline/claimx/performance/test_throughput.py`

**Deliverables:**
- [ ] Enrichment worker throughput (events/sec)
- [ ] Download worker throughput (files/sec)
- [ ] API call latency impact
- [ ] Baseline numbers documented

**Acceptance Criteria:**
- Performance baseline established
- No obvious regressions from xact numbers

---

### WP-8.4: Runbook Documentation
**Priority:** P2 | **Dependencies:** All workers

Operational documentation.

**Files:**
- `docs/claimx_runbook.md`

**Deliverables:**
- [ ] Worker deployment steps
- [ ] Configuration reference
- [ ] Troubleshooting guide
- [ ] DLQ management procedures
- [ ] Monitoring dashboards

**Acceptance Criteria:**
- New operator can deploy from doc
- Common issues documented

---

## Dependency Graph

```
WP-1.1 ─────┬─────────────────────────────────────────────────────────┐
            │                                                         │
WP-1.2 ─────┼────────────────────────────────────────┐               │
            │                                         │               │
WP-1.3 ─────┼─────────────────────┐                  │               │
            │                      │                  │               │
WP-1.4 ─────┴──────────────────────┼──────────────────┼───────────────┤
                                   │                  │               │
WP-2.1 ────────────────────────────┼──────────────────┤               │
            │                      │                  │               │
WP-2.2 ────┬┴──────────────────────┼──────────────────┤               │
           │                       │                  │               │
WP-2.3     │                       │                  │               │
           │                       │                  │               │
WP-3.1 ────┴───────────────────────┴──────────────────┤               │
   │                                                  │               │
WP-3.2 ────────────────────────────────────────┐      │               │
   │                                           │      │               │
WP-3.3 ────────────────────────────────────────┤      │               │
   │                                           │      │               │
WP-3.4, WP-3.5, WP-3.6                         │      │               │
                                               │      │               │
WP-4.1 ────────────────────────────────────────┤      │               │
   │                                           │      │               │
WP-4.2, WP-4.3                                 │      │               │
                                               │      │               │
WP-5.1 ────────────────────────────────────────┴──────┴───────────────┘
   │
WP-5.2 ─────────────────────────────────────┐
   │                                         │
WP-5.3 ──────────────────────────────────────┤
   │                                         │
WP-5.4, WP-5.5                               │
                                             │
WP-5.6 ──────────────────────────────────────┤
   │                                         │
WP-5.7 ──────────────────────────────────────┤
   │                                         │
WP-5.8                                       │
                                             │
WP-6.1, WP-6.2, WP-6.3                       │
                                             │
WP-7.1 ──────────────────────────────────────┘
   │
WP-7.2
   │
WP-8.1, WP-8.2, WP-8.3, WP-8.4
```

---

## Suggested Implementation Order

### Sprint 1: Foundation
1. WP-1.1: Event Schema
2. WP-1.2: Task Schemas
3. WP-1.3: Entity Row Schemas
4. WP-1.4: Config Updates

### Sprint 2: API Client & Handlers (Part 1)
5. WP-2.1: API Client Core
6. WP-2.2: API Client Methods
7. WP-3.1: Handler Base Class
8. WP-3.2: ProjectHandler

### Sprint 3: Handlers (Part 2) & Writers
9. WP-3.3: MediaHandler
10. WP-4.1: Entity Writer Base
11. WP-4.2: Entity Writer All Tables
12. WP-4.3: Events Table Writer

### Sprint 4: Core Workers
13. WP-5.1: Event Ingester
14. WP-5.2: Enrichment Worker Core
15. WP-5.3: Pre-flight Project Check
16. WP-5.5: Download Task Production

### Sprint 5: Download Pipeline
17. WP-5.6: Download Worker
18. WP-5.7: Upload Worker
19. WP-5.8: Result Processor

### Sprint 6: Resilience & Operations
20. WP-6.1: Enrichment Retry
21. WP-6.2: Download Retry
22. WP-7.1: Entry Point Commands

### Sprint 7: Remaining Items & Polish
23. WP-3.4: TaskHandler
24. WP-3.5: PolicyholderHandler
25. WP-3.6: VideoCollabHandler
26. WP-2.3: Circuit Breaker
27. WP-5.4: Batch Optimization
28. WP-7.2: Health Checks
29. WP-6.3: DLQ CLI

### Sprint 8: Testing & Documentation
30. WP-8.1: Integration Tests (Event Flow)
31. WP-8.2: Integration Tests (Download Flow)
32. WP-8.3: Performance Tests
33. WP-8.4: Runbook

---

## Notes

- **P0** = Must have for MVP
- **P1** = Important but can defer briefly
- **P2** = Nice to have

Each work package targets ~1-2 hours including tests. Adjust estimates based on actual velocity after first few packages.
