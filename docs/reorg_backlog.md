# Kafka Pipeline Reorganization Backlog

> **Reference:** `src/docs/kafka_pipeline_reorganization.md`
>
> This epic reorganizes `kafka_pipeline/` from a flat structure to a domain-based structure,
> enabling cleaner separation between xact and claimx processing.  PIck the next unworked task, and add the [ASSIGNED] tag to the work package. 

---

## Status

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 1: common/ | In Progress | 1/9 |
| Phase 2: xact/ | Not Started | 0/7 |
| Phase 3: claimx/ | Not Started | 0/11 |
| Phase 4: Config | Not Started | 0/4 |

---

## Phase 1: Create common/ Infrastructure

**REORG-101: Create common/ Directory Structure [ASSIGNED]** (P2)
- Create `kafka_pipeline/common/` with `__init__.py`
- Create subdirectories: `retry/`, `dlq/`, `storage/`, `writers/`, `eventhouse/`
- Add `__init__.py` to each subdirectory
- No code moves yet - structure only
- Size: Small
- Dependencies: None

**REORG-102: Move Core Infrastructure to common/** (P2)
- Move `consumer.py` → `common/consumer.py`
- Move `producer.py` → `common/producer.py`
- Move `metrics.py` → `common/metrics.py`
- Move `monitoring.py` → `common/monitoring.py`
- Update all imports in moved files
- Add re-exports from old locations for backward compatibility (temporary)
- Size: Medium
- Dependencies: REORG-101

**REORG-103: Move retry/ Module to common/** (P2)
- Move `retry/handler.py` → `common/retry/handler.py`
- Move `retry/scheduler.py` → `common/retry/scheduler.py`
- Update imports
- Add re-exports from old location
- Size: Small
- Dependencies: REORG-101

**REORG-104: Move dlq/ Module to common/** (P2)
- Move `dlq/handler.py` → `common/dlq/handler.py`
- Move `dlq/cli.py` → `common/dlq/cli.py`
- Update imports
- Add re-exports from old location
- Size: Small
- Dependencies: REORG-101

**REORG-105: Move storage/ Module to common/** (P2)
- Move `storage/onelake_client.py` → `common/storage/onelake_client.py`
- Update imports
- Add re-exports from old location
- Size: Small
- Dependencies: REORG-101

**REORG-106: Create common/writers/base.py** (P2)
- Extract base Delta writer functionality from existing writers
- Create `common/writers/base.py` with `BaseDeltaWriter` class
- Create `common/writers/delta_writer.py` for generic operations
- Domain writers will inherit from this base
- Size: Medium
- Dependencies: REORG-101

**REORG-107: Move eventhouse/ Module to common/** (P2)
- Move `eventhouse/kql_client.py` → `common/eventhouse/kql_client.py`
- Move `eventhouse/dedup.py` → `common/eventhouse/dedup.py`
- Move `eventhouse/poller.py` → `common/eventhouse/poller.py`
- Update imports
- Add re-exports from old location
- Size: Medium
- Dependencies: REORG-101

**REORG-108: Update All Imports for common/** (P2)
- Update all files in `kafka_pipeline/` to use new `common/` imports
- Update `__main__.py` entry points
- Update all tests to use new import paths
- Verify no circular imports
- Run full test suite
- Size: Large
- Dependencies: REORG-102, REORG-103, REORG-104, REORG-105, REORG-106, REORG-107

**REORG-109: Remove Backward Compatibility Re-exports** (P3)
- Remove temporary re-exports from old locations
- Delete empty old module files
- Clean up `kafka/` directory (currently just `__init__.py`)
- Final import cleanup
- Size: Small
- Dependencies: REORG-108, all external consumers updated

---

## Phase 2: Create xact/ Domain

**REORG-201: Create xact/ Directory Structure** (P2)
- Create `kafka_pipeline/xact/` with `__init__.py`
- Create subdirectories: `schemas/`, `workers/`, `writers/`
- Add `__init__.py` to each subdirectory
- Size: Small
- Dependencies: REORG-108

**REORG-202: Move xact Schemas** (P2)
- Move `schemas/events.py` → `xact/schemas/events.py`
- Move `schemas/tasks.py` → `xact/schemas/tasks.py`
- Move `schemas/results.py` → `xact/schemas/results.py`
- Move `schemas/cached.py` → `xact/schemas/cached.py`
- Update imports
- Add re-exports from old `schemas/` for backward compatibility
- Size: Medium
- Dependencies: REORG-201

**REORG-203: Move xact Workers** (P2)
- Move `workers/event_ingester.py` → `xact/workers/event_ingester.py`
- Move `workers/download_worker.py` → `xact/workers/download_worker.py`
- Move `workers/upload_worker.py` → `xact/workers/upload_worker.py`
- Move `workers/result_processor.py` → `xact/workers/result_processor.py`
- Update imports to use `common/` and `xact/schemas/`
- Add re-exports from old `workers/` for backward compatibility
- Size: Large
- Dependencies: REORG-201, REORG-202

**REORG-204: Move xact Writers** (P2)
- Move `writers/delta_events.py` → `xact/writers/delta_events.py`
- Move `writers/delta_inventory.py` → `xact/writers/delta_inventory.py`
- Update to inherit from `common/writers/base.py`
- Update imports
- Add re-exports from old `writers/` for backward compatibility
- Size: Medium
- Dependencies: REORG-201, REORG-106

**REORG-205: Reorganize Test Directory for xact** (P2)
- Create `tests/kafka_pipeline/xact/` directory structure
- Move xact-specific tests to `tests/kafka_pipeline/xact/`
- Create `tests/kafka_pipeline/common/` for shared infrastructure tests
- Update test imports
- Verify all tests pass
- Size: Large
- Dependencies: REORG-202, REORG-203, REORG-204

**REORG-206: Update xact Entry Points** (P2)
- Update `__main__.py` to use `xact/` imports
- Rename commands to `xact-ingester`, `xact-downloader`, etc.
- Keep old command names as aliases (temporary backward compatibility)
- Update CLI help text
- Size: Medium
- Dependencies: REORG-203

**REORG-207: Remove xact Backward Compatibility** (P3)
- Remove temporary re-exports from old `schemas/`, `workers/`, `writers/`
- Remove old command name aliases from CLI
- Delete empty old directories
- Update documentation
- Size: Small
- Dependencies: REORG-206, deployment scripts updated

---

## Phase 3: Create claimx/ Domain

**REORG-301: Create claimx/ Directory Structure** (P2)
- Create `kafka_pipeline/claimx/` with `__init__.py`
- Create subdirectories: `schemas/`, `workers/`, `handlers/`, `writers/`
- Add `__init__.py` to each subdirectory
- Size: Small
- Dependencies: REORG-108

**REORG-302: Implement claimx Schemas** (P2)
- Create `claimx/schemas/events.py` with `ClaimXEventMessage`
- Create `claimx/schemas/tasks.py` with `ClaimXEnrichmentTask`, `ClaimXDownloadTask`
- Create `claimx/schemas/entities.py` with entity row schemas
- Use Pydantic models consistent with xact patterns
- Size: Medium
- Dependencies: REORG-301

**REORG-303: Implement ClaimX API Client** (P2)
- Create `claimx/api_client.py` with `ClaimXApiClient`
- Implement authentication handling
- Implement entity fetching methods (projects, contacts, media, tasks, etc.)
- Add rate limiting and retry logic
- Add comprehensive error handling
- Size: Large
- Dependencies: REORG-301
- Reference: Port patterns from `verisk_pipeline/` if applicable

**REORG-304: Implement claimx Handlers** (P2)
- Create `claimx/handlers/base.py` with `EventHandler` base class
- Create `claimx/handlers/project.py` with `ProjectHandler`
- Create `claimx/handlers/media.py` with `MediaHandler`
- Create `claimx/handlers/task.py` with `TaskHandler`
- Create `claimx/handlers/contact.py` with `PolicyholderHandler`
- Create `claimx/handlers/video.py` with `VideoCollabHandler`
- Size: Large
- Dependencies: REORG-302, REORG-303
- Reference: Port from `verisk_pipeline/` handlers

**REORG-305: Implement claimx Event Ingester** (P2)
- Create `claimx/workers/event_ingester.py`
- Consume from claimx events topic
- Produce to claimx enrichment pending topic
- Different flow from xact (produces enrichment tasks, not download tasks)
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

**REORG-307: Implement claimx Download Worker** (P2)
- Create `claimx/workers/download_worker.py`
- Similar to xact download worker but uses enriched URLs
- Consume from claimx download pending topic
- Download attachments to cache
- Produce to upload pending topic
- Size: Medium
- Dependencies: REORG-302

**REORG-308: Implement claimx Upload Worker** (P2)
- Create `claimx/workers/upload_worker.py`
- Similar to xact upload worker
- Upload from cache to OneLake
- Produce to results topic
- Size: Medium
- Dependencies: REORG-302

**REORG-309: Implement claimx Writers** (P2)
- Create `claimx/writers/delta_events.py` for claimx events table
- Create `claimx/writers/delta_entities.py` for 7 entity tables
- Inherit from `common/writers/base.py`
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

**REORG-401: Extend KafkaConfig for Domains** (P2)
- Add `xact_topic_prefix` and `claimx_topic_prefix` to config
- Add `get_topic(domain, topic_type)` helper method
- Add claimx API settings (`claimx_api_url`, `claimx_api_timeout_seconds`, etc.)
- Update `config.yaml` schema
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
- Remove all temporary backward compatibility code
- Delete empty/unused files and directories
- Verify no cross-domain imports (xact ↔ claimx)
- Verify all shared code is in `common/`
- Run full test suite
- Code review for consistency
- Size: Medium
- Dependencies: All previous REORG tasks

---

## Dependency Graph

```
Phase 1: common/
  REORG-101 → REORG-102, 103, 104, 105, 106, 107 → REORG-108 → REORG-109

Phase 2: xact/
  REORG-108 → REORG-201 → REORG-202, 203, 204 → REORG-205, 206 → REORG-207

Phase 3: claimx/
  REORG-108 → REORG-301 → REORG-302, 303, 304 → REORG-305, 306, 307, 308, 309
                                              → REORG-310 → REORG-311

Phase 4: Config & Entry Points
  REORG-108 → REORG-401
  REORG-206 + claimx workers → REORG-402 → REORG-403 → REORG-404
```

---

## Size Estimates

| Phase | Work Packages | Small | Medium | Large |
|-------|---------------|-------|--------|-------|
| Phase 1 | 9 | 4 | 4 | 1 |
| Phase 2 | 7 | 2 | 3 | 2 |
| Phase 3 | 11 | 1 | 4 | 6 |
| Phase 4 | 4 | 0 | 4 | 0 |
| **Total** | **31** | **7** | **15** | **9** |

---

## Completed

<!-- Move completed work packages here with commit hash -->

(none)
