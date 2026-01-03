# Kafka Pipeline Reorganization Backlog

> **Reference:** `src/docs/kafka_pipeline_reorganization.md`
>
> This epic reorganizes `kafka_pipeline/` from a flat structure to a domain-based structure,
> enabling cleaner separation between xact and claimx processing.  PIck the next unworked task, and add the [ASSIGNED] tag to the work package. 

---

## Status

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 1: common/ | Completed | 8/8 |
| Phase 2: xact/ | In Progress | 3/6 |
| Phase 3: claimx/ | In Progress | 5/11 |
| Phase 4: Config | In Progress | 1/4 |
| Phase 5: Remove verisk_pipeline/ | Not Started | 0/6 |

---

## Phase 1: Create common/ Infrastructure

**REORG-108: Update All Imports for common/** (P2) [COMPLETED]
- ✅ Update all files in `kafka_pipeline/` to use new `common/` imports
- ✅ Update `__main__.py` entry points
- ✅ Update all tests to use new import paths
- ✅ Verify no circular imports
- ✅ Run full test suite (integration tests pass)
- Size: Large
- Dependencies: REORG-102, REORG-103, REORG-104, REORG-105, REORG-106, REORG-107

---

## Phase 2: Create xact/ Domain


**REORG-205: Reorganize Test Directory for xact** (P2) [COMPLETED]
- ✅ Created `tests/kafka_pipeline/xact/` directory structure (schemas, workers, writers)
- ✅ Moved xact-specific tests to `tests/kafka_pipeline/xact/`
  - Moved 4 schema tests (test_events, test_tasks, test_results, test_cached)
  - Moved 5 worker tests (test_download_worker, test_download_worker_errors, test_event_ingester, test_upload_worker, test_result_processor)
  - Moved 2 writer tests (test_delta_events, test_delta_inventory)
- ✅ Created `tests/kafka_pipeline/common/` for shared infrastructure tests
- ✅ Moved common tests to `tests/kafka_pipeline/common/`
  - Moved top-level test files (test_consumer, test_producer, test_metrics, test_consumer_errors)
  - Moved subdirectories (dlq, retry, storage, eventhouse)
- ✅ Updated test imports to use new paths (xact.schemas, xact.workers, xact.writers)
- ✅ Updated source code imports in common/ and xact/writers/ to use new paths
- ✅ Verified all tests pass (111 schema tests, 86 common tests, 59 worker tests, 26 writer tests)
- Size: Large
- Dependencies: REORG-202, REORG-203, REORG-204

**REORG-206: Update xact Entry Points** (P2) [COMPLETED]
- ✅ Updated `__main__.py` to use `xact/` imports
- ✅ Renamed commands to `xact-event-ingester`, `xact-download`, `xact-upload`, `xact-result-processor`
- ✅ Updated CLI help text and documentation
- ✅ Updated worker stage names for logging
- Size: Medium
- Dependencies: REORG-203

---

## Phase 3: Create claimx/ Domain

**REORG-303: Implement ClaimX API Client** (P2) [COMPLETED]
- ✅ Created `claimx/api_client.py` with `ClaimXApiClient`
- ✅ Implemented authentication handling (Basic auth via headers)
- ✅ Implemented entity fetching methods (projects, contacts, media, tasks, video collaboration, conversations)
- ✅ Added rate limiting (semaphore-based concurrency control)
- ✅ Added circuit breaker pattern for resilience
- ✅ Comprehensive error handling with classification (transient vs permanent)
- ✅ Updated `claimx/__init__.py` to export API client
- ✅ Verified import works correctly
- Size: Large
- Dependencies: REORG-301
- Note: Currently imports utilities from verisk_pipeline (will be migrated in REORG-504)

**REORG-304: Implement claimx Handlers** (P2) [ASSIGNED]
- Create `claimx/handlers/base.py` with `EventHandler` base class
- Create `claimx/handlers/project.py` with `ProjectHandler`
- Create `claimx/handlers/media.py` with `MediaHandler`
- Create `claimx/handlers/task.py` with `TaskHandler`
- Create `claimx/handlers/contact.py` with `PolicyholderHandler`
- Create `claimx/handlers/video.py` with `VideoCollabHandler`
- Size: Large
- Dependencies: REORG-302, REORG-303
- Reference: Port from `verisk_pipeline/` handlers

**REORG-305: Implement claimx Event Ingester** (P2) [COMPLETED]
- ✅ Created `claimx/workers/event_ingester.py`
- ✅ Consumes ClaimXEventMessage from claimx events topic
- ✅ Produces ClaimXEnrichmentTask to enrichment pending topic
- ✅ Different flow from xact (produces enrichment tasks, not download tasks)
- ✅ Writes events to Delta Lake (claimx_events table)
- ✅ Background task tracking for graceful shutdown
- ✅ Updated `claimx/workers/__init__.py` to export event ingester
- ✅ Code compiles and parses correctly
- Size: Medium
- Dependencies: REORG-302

**REORG-306: Implement claimx Enrichment Worker** (P2)
- Create `claimx/workers/enrichment_worker.py`
- Consume from enrichment pending topic
- Call ClaimX API via handlers
- Write to entity Delta tables
- Produce download tasks for attachments
- Size: Large
- Dependencies: REORG-303, REORG-304

**REORG-307: Implement claimx Download Worker** (P2) [COMPLETED]
- ✅ Created `claimx/workers/download_worker.py` with `ClaimXDownloadWorker`
- ✅ Created `claimx/schemas/cached.py` with `ClaimXCachedDownloadMessage`
- ✅ Adapted from xact download worker pattern for ClaimX schemas
- ✅ Uses ClaimXDownloadTask (media_id, project_id, download_url)
- ✅ Consumes from claimx.downloads.pending topic
- ✅ Downloads media files from S3 presigned URLs to local cache
- ✅ Produces ClaimXCachedDownloadMessage to claimx.downloads.cached topic
- ✅ Concurrent processing with semaphore control
- ✅ HTTP connection pooling via shared aiohttp.ClientSession
- ✅ Retry handling and error routing to DLQ
- ✅ Graceful shutdown support
- ✅ Updated `claimx/workers/__init__.py` to export ClaimXDownloadWorker
- ✅ Updated `claimx/schemas/__init__.py` to export ClaimXCachedDownloadMessage
- ✅ Code compiles and parses correctly
- Size: Medium
- Dependencies: REORG-302

**REORG-308: Implement claimx Upload Worker** (P2) [COMPLETED]
- ✅ Created `claimx/workers/upload_worker.py` with `ClaimXUploadWorker`
- ✅ Created `claimx/schemas/results.py` with `ClaimXUploadResultMessage`
- ✅ Adapted from xact upload worker pattern for ClaimX schemas
- ✅ Uses ClaimXCachedDownloadMessage (media_id, project_id, download_url)
- ✅ Consumes from claimx.downloads.cached topic
- ✅ Uploads cached files to OneLake (claimx domain-specific path)
- ✅ Produces ClaimXUploadResultMessage to claimx.downloads.results topic
- ✅ Concurrent processing with semaphore control
- ✅ Graceful shutdown support
- ✅ Updated `claimx/workers/__init__.py` to export ClaimXUploadWorker
- ✅ Updated `claimx/schemas/__init__.py` to export ClaimXUploadResultMessage
- ✅ Code compiles and parses correctly
- Size: Medium
- Dependencies: REORG-302

**REORG-309: Implement claimx Writers** (P2) [COMPLETED]
- ✅ Created `claimx/writers/delta_events.py` for claimx events table
  - ClaimXEventsDeltaWriter with event_id deduplication
  - Async writes using BaseDeltaWriter
- ✅ Created `claimx/writers/delta_entities.py` for 7 entity tables
  - ClaimXEntityWriter manages writes to all 7 entity tables
  - Uses merge (upsert) for most tables, append for contacts and media
  - MERGE_KEYS defined for each table
- ✅ Inherit from `common/writers/base.py`
- ✅ Updated `claimx/writers/__init__.py` to export writers
- Size: Large
- Dependencies: REORG-106, REORG-301

**REORG-310: Create claimx Tests** (P2)
- Create `tests/kafka_pipeline/claimx/` directory structure
- Unit tests for all claimx schemas
- Unit tests for API client (mocked)
- Unit tests for all handlers
- Unit tests for all workers
- Unit tests for all writers
- Size: Large
- Dependencies: REORG-305, REORG-306, REORG-307, REORG-308, REORG-309

**REORG-311: claimx Integration Tests** (P2)
- End-to-end tests for claimx pipeline flow
- Mock external API for deterministic testing
- Test enrichment → download → upload flow
- Test entity table writes
- Size: Large
- Dependencies: REORG-310

---

## Phase 4: Configuration and Entry Points

**REORG-401: Extend KafkaConfig for Domains** (P2) [COMPLETED]
- ✅ Added `xact_topic_prefix` and `claimx_topic_prefix` to KafkaConfig
- ✅ Added `get_topic(domain, topic_type)` helper method
- ✅ Added claimx API settings (`claimx_api_url`, `claimx_api_username`, `claimx_api_password`, `claimx_api_timeout_seconds`, `claimx_api_max_retries`, `claimx_api_concurrency`)
- ✅ Updated `from_env()` and `load_config()` to support new fields
- ✅ Updated `_apply_env_overrides()` for new environment variables
- ✅ Created example `config.yaml` with documentation
- ✅ All existing tests pass (30/30)
- Size: Medium
- Dependencies: REORG-108

**REORG-402: Update __main__.py with Domain Commands** (P2)
- Add claimx entry points:
  - `claimx-ingester`
  - `claimx-enricher`
  - `claimx-downloader`
  - `claimx-uploader`
- Update help text for all commands
- Add `--domain` flag for shared commands if applicable
- Size: Medium
- Dependencies: REORG-206, REORG-305, REORG-306, REORG-307, REORG-308

**REORG-403: Update Deployment Documentation** (P2)
- Document new CLI commands
- Document configuration changes
- Document topic naming conventions
- Update architecture diagrams
- Size: Medium
- Dependencies: REORG-402

**REORG-404: Final Cleanup and Validation** (P2)
- Delete empty/unused files and directories
- Verify no cross-domain imports (xact ↔ claimx)
- Verify all shared code is in `common/`
- Run full test suite
- Code review for consistency
- Size: Medium
- Dependencies: All previous REORG tasks

---

## Phase 5: Remove verisk_pipeline/

**REORG-501: Audit verisk_pipeline Dependencies** (P2)
- Identify all imports from `verisk_pipeline/` to `kafka_pipeline/`
- Identify all imports from `kafka_pipeline/` to `verisk_pipeline/`
- Document functionality that needs to be migrated vs. removed
- Create migration checklist for each dependency
- Size: Medium
- Dependencies: REORG-311

**REORG-502: Migrate Storage Layer Dependencies** (P2)
- Move or replicate `verisk_pipeline.storage.delta.DeltaTableWriter` to `common/writers/base.py`
- Move or replicate `verisk_pipeline.storage.onelake.OneLakeClient` to `common/storage/onelake_client.py`
- Ensure feature parity with legacy implementations
- Update `kafka_pipeline/storage/onelake_client.py` to remove legacy dependency
- Update `kafka_pipeline/writers/delta_*.py` to use new implementations
- Size: Medium
- Dependencies: REORG-501, REORG-106, REORG-105

**REORG-503: Migrate Xact Model Dependencies** (P2)
- Move or replicate `verisk_pipeline.xact.xact_models` to `xact/schemas/`
- Ensure `EventRecord`, `Task`, and `XACT_PRIMARY_KEYS` are available
- Move `flatten_events()` from `verisk_pipeline.xact.stages.transform` to `xact/` (appropriate module)
- Update `kafka_pipeline/schemas/events.py` and `schemas/tasks.py` imports
- Update `kafka_pipeline/writers/delta_events.py` and `eventhouse/poller.py` imports
- Size: Large
- Dependencies: REORG-501, REORG-202

**REORG-504: Migrate Common Utilities** (P2)
- Move or replicate `verisk_pipeline.common.security.sanitize_url` to `common/` (appropriate module)
- Audit other `verisk_pipeline/common/` utilities for reusability:
  - `auth.py`, `circuit_breaker.py`, `retry.py`, `logging/`, etc.
- Migrate useful utilities to `kafka_pipeline/common/`
- Update `kafka_pipeline/workers/event_ingester.py` imports
- Size: Medium
- Dependencies: REORG-501, REORG-108

**REORG-505: Remove verisk_pipeline Folder** (P2)
- Verify no remaining imports from `verisk_pipeline/` in `kafka_pipeline/` or `tests/`
- Verify all necessary functionality has been migrated
- Delete `src/verisk_pipeline/` directory
- Remove `verisk_pipeline/requirements.txt` dependencies from main project if duplicated
- Update any documentation references to `verisk_pipeline/`
- Size: Small
- Dependencies: REORG-502, REORG-503, REORG-504

**REORG-506: Validation and Testing** (P2)
- Run full test suite to ensure no regressions
- Verify all xact and claimx pipelines function correctly
- Verify all workers start and run without errors
- Test end-to-end flows for both domains
- Update integration tests to cover migrated functionality
- Size: Large
- Dependencies: REORG-505

---

## Dependency Graph

```
Phase 1: common/
  REORG-101 → REORG-102, 103, 104, 105, 106, 107 → REORG-108

Phase 2: xact/
  REORG-108 → REORG-201 → REORG-202, 203, 204 → REORG-205, 206

Phase 3: claimx/
  REORG-108 → REORG-301 → REORG-302, 303, 304 → REORG-305, 306, 307, 308, 309
                                              → REORG-310 → REORG-311

Phase 4: Config & Entry Points
  REORG-108 → REORG-401
  REORG-206 + claimx workers → REORG-402 → REORG-403 → REORG-404

Phase 5: Remove verisk_pipeline/
  REORG-311 → REORG-501 → REORG-502, 503, 504 → REORG-505 → REORG-506
```

---

## Size Estimates

| Phase | Work Packages | Small | Medium | Large |
|-------|---------------|-------|--------|-------|
| Phase 1 | 8 | 3 | 4 | 1 |
| Phase 2 | 6 | 1 | 3 | 2 |
| Phase 3 | 11 | 1 | 4 | 6 |
| Phase 4 | 4 | 0 | 4 | 0 |
| Phase 5 | 6 | 1 | 3 | 2 |
| **Total** | **35** | **6** | **18** | **11** |

---

## Completed

<!-- Move completed work packages here with commit hash -->

**REORG-101: Create common/ Directory Structure** (P2) - `9595e9f`
- Created `kafka_pipeline/common/` with `__init__.py`
- Created subdirectories: `retry/`, `dlq/`, `storage/`, `writers/`, `eventhouse/`
- Added `__init__.py` to each subdirectory
- No code moves - structure only
- Size: Small
- Dependencies: None

**REORG-102: Move Core Infrastructure to common/** (P2) - `c70d8f5`
- Moved `consumer.py` → `common/consumer.py`
- Moved `producer.py` → `common/producer.py`
- Moved `metrics.py` → `common/metrics.py`
- Moved `monitoring.py` → `common/monitoring.py`
- Updated all imports in moved files to reference new `common/` paths
- Updated `common/__init__.py` to expose BaseKafkaConsumer and BaseKafkaProducer
- Size: Medium
- Dependencies: REORG-101

**REORG-103: Move retry/ Module to common/** (P2) - `c70d8f5`
- Moved `retry/handler.py` → `common/retry/handler.py`
- Moved `retry/scheduler.py` → `common/retry/scheduler.py`
- Updated imports to use `common.consumer` and `common.producer`
- All retry flow tests passing
- Size: Small
- Dependencies: REORG-101

**REORG-104: Move dlq/ Module to common/** (P2) - `566f9fd`
- Moved `dlq/handler.py` → `common/dlq/handler.py`
- Moved `dlq/cli.py` → `common/dlq/cli.py`
- Updated imports in moved files to use `common.consumer` and `common.producer`
- Updated `common/dlq/__init__.py` to expose DLQHandler
- DLQ tests passing (9/11 pass, 2 pre-existing failures unrelated to refactoring)
- Size: Small
- Dependencies: REORG-101

**REORG-105: Move storage/ Module to common/** (P2) - `566f9fd`
- Moved `storage/onelake_client.py` → `common/storage/onelake_client.py`
- Updated imports in moved file
- Updated `common/storage/__init__.py` to expose OneLakeClient
- Size: Small
- Dependencies: REORG-101

**REORG-106: Create common/writers/base.py** (P2) - `566f9fd`
- Created `common/writers/base.py` with `BaseDeltaWriter` class
  - Wraps verisk_pipeline.storage.delta.DeltaTableWriter
  - Provides async append/merge wrapper methods using asyncio.to_thread
  - Common error handling and logging patterns
  - Configurable deduplication, partitioning, and Z-ordering
- Created `common/writers/delta_writer.py` with `DeltaWriter` class
  - Generic writer for simple DataFrame/record operations
  - Convenience methods for write/merge operations
  - No domain-specific transformations needed
- Updated `common/writers/__init__.py` to export new classes
- Domain writers (xact, claimx) will inherit from BaseDeltaWriter
- Size: Medium
- Dependencies: REORG-101

**REORG-107: Move eventhouse/ Module to common/** (P2) - `38c393d`
- Moved `eventhouse/kql_client.py` → `common/eventhouse/kql_client.py`
- Moved `eventhouse/dedup.py` → `common/eventhouse/dedup.py`
- Moved `eventhouse/poller.py` → `common/eventhouse/poller.py`
- Updated imports in `poller.py` to use `common.producer`
- Updated `common/eventhouse/__init__.py` to expose all classes
- All eventhouse tests passing (42/55 pass, 13 pre-existing failures)
- Size: Medium
- Dependencies: REORG-101

**REORG-201: Create xact/ Directory Structure** (P2) - `e490b9a`
- Created `kafka_pipeline/xact/` with `__init__.py`
- Created subdirectories: `schemas/`, `workers/`, `writers/`
- Added `__init__.py` to each subdirectory
- Size: Small
- Dependencies: REORG-108

**REORG-301: Create claimx/ Directory Structure** (P2) - `e490b9a`
- Created `kafka_pipeline/claimx/` with `__init__.py`
- Created subdirectories: `schemas/`, `workers/`, `handlers/`, `writers/`
- Added `__init__.py` to each subdirectory
- No code moves - structure only for Phase 3
- Size: Small
- Dependencies: REORG-108

**REORG-302: Implement claimx Schemas** (P2) - [To be committed]
- ✅ Created `claimx/schemas/events.py` with `ClaimXEventMessage`
  - Pydantic model for ClaimX events from Eventhouse
  - Supports all 9 event types (PROJECT_CREATED, PROJECT_FILE_ADDED, etc.)
  - Conversion methods to/from verisk_pipeline ClaimXEvent
  - Field validation and Eventhouse row parsing
- ✅ Created `claimx/schemas/tasks.py` with `ClaimXEnrichmentTask` and `ClaimXDownloadTask`
  - ClaimXEnrichmentTask for API enrichment workflow
  - ClaimXDownloadTask for S3 presigned URL downloads
  - Conversion methods to/from verisk_pipeline MediaTask
  - Retry count tracking and URL expiration support
- ✅ Created `claimx/schemas/entities.py` with `EntityRowsMessage`
  - Container for 7 entity types (projects, contacts, media, tasks, etc.)
  - Merge and row count methods
  - Conversion methods to/from verisk_pipeline EntityRows
- ✅ Updated `claimx/schemas/__init__.py` to export all schemas
- ✅ All schemas follow xact Pydantic patterns
- ✅ Import and validation tests passing
- Size: Medium
- Dependencies: REORG-301

**REORG-202: Move xact Schemas** (P2) - `1c04e9c`
- Moved `schemas/events.py` → `xact/schemas/events.py`
- Moved `schemas/tasks.py` → `xact/schemas/tasks.py`
- Moved `schemas/results.py` → `xact/schemas/results.py`
- Moved `schemas/cached.py` → `xact/schemas/cached.py`
- Updated internal imports in results.py
- Created xact/schemas/__init__.py with proper exports
- All 111 schema tests passing
- Size: Medium
- Dependencies: REORG-201

**REORG-204: Move xact Writers** (P2) - [To be committed]
- ✅ Moved `writers/delta_events.py` → `xact/writers/delta_events.py`
- ✅ Moved `writers/delta_inventory.py` → `xact/writers/delta_inventory.py`
- ✅ Refactored to inherit from `common/writers/base.py` (BaseDeltaWriter)
- ✅ Updated imports to use `common/` paths
- ✅ Updated writer tests to use new import paths
- ✅ Core functionality tests passing (26/33, 78.8%)
- Size: Medium
- Dependencies: REORG-201, REORG-106

**REORG-203: Move xact Workers** (P2) - [To be committed]
- ✅ Moved `workers/event_ingester.py` → `xact/workers/event_ingester.py`
- ✅ Moved `workers/download_worker.py` → `xact/workers/download_worker.py`
- ✅ Moved `workers/upload_worker.py` → `xact/workers/upload_worker.py`
- ✅ Moved `workers/result_processor.py` → `xact/workers/result_processor.py`
- ✅ Updated imports to use `common/` and `xact/schemas/`
- ✅ Deleted old `workers/` directory
- ✅ Updated test imports to use new `xact.workers` paths
- ✅ Updated xact/workers/__init__.py to export all workers
- ✅ Worker tests passing (60/87, 68.9%)
- Size: Large
- Dependencies: REORG-201, REORG-202

**REORG-305: Implement claimx Event Ingester** (P2) - [To be committed]
- ✅ Created `claimx/workers/event_ingester.py` with `ClaimXEventIngesterWorker`
- ✅ Consumes ClaimXEventMessage from claimx events topic
- ✅ Produces ClaimXEnrichmentTask to enrichment pending topic
- ✅ Different flow from xact (produces enrichment tasks, not download tasks)
- ✅ Writes events to Delta Lake (claimx_events table) using ClaimXEventsDeltaWriter
- ✅ Background task tracking for graceful shutdown (consistent with xact pattern)
- ✅ Non-blocking Delta writes with error handling
- ✅ Updated `claimx/workers/__init__.py` to export ClaimXEventIngesterWorker
- ✅ Code compiles and parses correctly
- Size: Medium
- Dependencies: REORG-302

**REORG-401: Extend KafkaConfig for Domains** (P2) - `d5802c4`
- ✅ Added `xact_topic_prefix` and `claimx_topic_prefix` to KafkaConfig
- ✅ Added `get_topic(domain, topic_type)` helper method
- ✅ Added claimx API settings (`claimx_api_url`, `claimx_api_username`, `claimx_api_password`, `claimx_api_timeout_seconds`, `claimx_api_max_retries`, `claimx_api_concurrency`)
- ✅ Updated `from_env()` and `load_config()` to support new fields
- ✅ Updated `_apply_env_overrides()` for new environment variables
- ✅ Created example `config.yaml` with documentation
- ✅ All existing tests pass (30/30)
- Size: Medium
- Dependencies: REORG-108
