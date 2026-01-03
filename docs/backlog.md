# Kafka Pipeline Backlog

> **Usage:**
> 1. Read this file at session start to understand pending work
> 2. Pick a task from "Ready" based on priority
> 3. Move to "In Progress" when starting
> 4. Move to "Completed" with commit hash when done
> 5. Commit backlog updates alongside code changes

---

## Priority Definitions

| Priority | Description | SLA |
|----------|-------------|-----|
| **P0** | Critical - Blocks production or causes data loss | Immediate |
| **P1** | High - Significant functionality gap or bug | This sprint |
| **P2** | Medium - Important improvement or minor bug | Next sprint |
| **P3** | Low - Nice-to-have, tech debt, cleanup | Backlog |

---

## In Progress

<!-- Move tasks here when starting work -->

(none)

---

## Ready

### P2 - Medium Priority

**TECH-003: Consolidate Configuration Loading (config.yaml Priority)**
- Ensure all modules load config consistently: config.yaml first, env vars as overrides
- Audit all config loading paths across workers and components
- Remove or deprecate `from_env()` methods in favor of unified `load_config()`
- Document configuration precedence clearly
- Size: Medium
- See: [Research Notes](#tech-003-research-notes)

**WP-506: Eventhouse Integration Testing**
- Integration tests for Eventhouse polling flow
- Mock Eventhouse for unit tests, optional real integration
- Size: Medium

**WP-507: Eventhouse Observability**
- Add metrics, alerts, and dashboard for Eventhouse polling
- New Grafana dashboard and Prometheus alert rules
- Size: Medium

### P3 - Low Priority

**WP-505: Multi-Pipeline Support**
- Support multiple pipelines (xact, claimx) with independent configs
- CLI flag for pipeline selection, separate source tables
- Size: Medium

**TECH-001: Refactor EventIngesterWorker for Separate Consumer/Producer Configs**
- Location: `kafka_pipeline/__main__.py:157`
- Currently `EventIngesterWorker` accepts single `KafkaConfig` for both consumer and producer
- In Event Hub mode, should read from Event Hub (consumer) but write to local Kafka (producer)
- `run_event_ingester()` receives both configs but only passes `eventhub_config`
- **Note:** Only affects `EVENT_SOURCE=eventhub` mode; Eventhouse mode works correctly
- Size: Small
- See: [Research Notes](#tech-001-research-notes)

**TECH-002: Move Inline Imports to Top of Files**
- Move all inline/function-level imports to top of files per PEP 8 convention
- Review each inline import for valid exceptions (circular import avoidance, optional dependencies)
- Document any intentional exceptions with comments
- Size: Medium (codebase-wide)

**TECH-005: Review Dev Mode Necessity**
- Location: `kafka_pipeline/__main__.py:512`
- TODO asks: "Do we need dev mode?"
- Evaluate if `--dev` flag is still useful or if config.yaml approach replaces it
- If redundant, remove; if needed, document the use case
- Size: Small

**TECH-007: Confirm Consistent CTRL+C / Graceful Shutdown Behavior**
- Verify all workers handle KeyboardInterrupt consistently
- CTRL+C should initiate graceful shutdown: finish current batch, then exit
- Test: event-ingester, download-worker, upload-worker, result-processor, poller
- Document expected shutdown behavior
- Size: Medium

**TECH-008: Review main.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/__main__.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Consider refactoring large functions into smaller units
- Increase readability
- Size: Medium

**TECH-009: Review poller.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/eventhouse/poller.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Consider refactoring large functions into smaller units
- Increase readability
- Size: Medium

**TECH-010: Review download_worker.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/workers/download_worker.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Consider refactoring large functions into smaller units
- Increase readability
- Size: Medium

**TECH-011: Review pipeline_config.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/pipeline_config.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-012: Review kql_client.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/eventhouse/kql_client.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-013: Review dedup.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/eventhouse/dedup.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-014: Review event_ingester.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/workers/event_ingester.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-015: Review config.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/config.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-016: Review producer.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/producer.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-017: Review delta_events.py Structure and Remove Dead Code**
- Location: `kafka_pipeline/writers/delta_events.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-018: Review core/logging/setup.py Structure and Remove Dead Code**
- Location: `core/logging/setup.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-019: Review core/logging/context.py Structure and Remove Dead Code**
- Location: `core/logging/context.py`
- Review overall code structure and organization
- Remove dead/unreachable code paths
- Apply best practices (single responsibility, reduce complexity)
- Increase readability
- Size: Medium

**TECH-020: Ensure File Names are Logical and Descriptive**
- Audit all file names across the project
- Rename files that are unclear or don't reflect their contents
- Ensure consistency in naming conventions (snake_case, etc.)
- Update imports after any renames
- Size: Medium (codebase-wide)

---

## Blocked

<!-- Tasks waiting on dependencies or decisions -->

(none)

---

## Completed

<!-- Done tasks with commit references -->

**TECH-006: Verify Logger Usage in run_eventhouse_poller** ✓
- Verified logger scoping: module-level `logger` is correct (context attached at format time via `ContextVar`)
- Found: `stage="event-ingester"` was inappropriate for the poller (same name as `run_local_event_ingester`)
- Fixed: Changed to `stage="eventhouse-poller"` to distinguish poller logs from event ingester logs
- All 61 logging tests pass
- Size: Small

**TECH-004: Review JSON_LOGS / Logging Configuration Redundancy** ✓
- Reviewed `--json-logs` CLI arg and `JSON_LOGS` env var usage
- Found: `setup_logging()` and `setup_multi_worker_logging()` already default to `json_format=True`
- Found: The `--json-logs` CLI arg was useless (could only enable, never disable)
- Removed the redundant `--json-logs` CLI argument
- Kept `JSON_LOGS` env var for debugging flexibility (`JSON_LOGS=false` for human-readable logs)
- Added documentation comment explaining the env var purpose
- Size: Small

**WP-315: Download Worker - Cache Behavior Tests** ✓
- Updated Download Worker tests for new caching behavior (decoupled download/upload)
- Added `temp_cache_dir` fixture and `downloads_cached_topic` to test configuration
- Verified cache file creation with proper directory structure (cache_dir/trace_id/filename)
- Verified `CachedDownloadMessage` production with correct fields
- Updated batch tests to verify cache behavior for concurrent processing
- Size: Small

(see docs/archive/backlog.md for historical completions)

---

## Research Notes

### TECH-001 Research Notes

**Context:**
The TODO at `kafka_pipeline/__main__.py:157` identifies an architectural issue in `EventIngesterWorker`.

**Current Behavior:**
```python
# run_event_ingester() receives BOTH configs:
async def run_event_ingester(
    eventhub_config,      # For reading from Event Hub
    local_kafka_config,   # For writing to local Kafka (UNUSED!)
    ...
):
    # But only passes eventhub_config:
    worker = EventIngesterWorker(
        config=eventhub_config,  # Used for both consumer AND producer
        ...
    )
```

**Impact Analysis:**
- **Eventhouse mode (`EVENT_SOURCE=eventhouse`)**: NOT AFFECTED
  - Uses `run_local_event_ingester()` which correctly uses single local Kafka config
  - KQLEventPoller writes to `events.raw`, EventIngester reads from same local Kafka
- **Event Hub mode (`EVENT_SOURCE=eventhub`)**: AFFECTED
  - Producer incorrectly configured with Event Hub settings instead of local Kafka
  - This mode is the legacy/fallback path, not primary production

**Fix Required:**
1. Modify `EventIngesterWorker.__init__` to accept separate `consumer_config` and `producer_config`
2. Update `start()` to use appropriate config for each component
3. Update `run_event_ingester()` to pass both configs

**Priority Justification (P3):**
- Eventhouse is the primary production path and works correctly
- Event Hub mode is legacy/rarely used
- No known production impact currently

### TECH-003 Research Notes

**Goal:**
Establish consistent configuration loading across the codebase with clear precedence:
1. config.yaml (primary source)
2. Environment variables (overrides for deployment flexibility)
3. Dataclass defaults (fallbacks)

**Current State (needs audit):**
- `KafkaConfig` has both `from_env()` and `load_config()` methods
- `PollerConfig` has both `from_env()` and `load_config()` methods
- `EventhouseConfig` likely similar pattern
- Some modules may call `from_env()` directly, bypassing YAML

**Scope:**
- `kafka_pipeline/config.py` - KafkaConfig
- `kafka_pipeline/eventhouse/poller.py` - PollerConfig
- `kafka_pipeline/eventhouse/kql_client.py` - EventhouseConfig
- `kafka_pipeline/__main__.py` - entry point config loading
- `kafka_pipeline/pipeline_config.py` - PipelineConfig

**Deliverables:**
1. Audit all config classes for loading consistency
2. Standardize on `load_config(path)` as primary entry point
3. Mark `from_env()` as deprecated or remove
4. Update `__main__.py` to use unified loading
5. Add configuration documentation (precedence, all options)

---

## Future Work

### Kafka Pipeline Reorganization
See **[reorg_backlog.md](reorg_backlog.md)** for the full epic (31 work packages across 4 phases).

### Phase 6: Migration (Not Yet Scoped)
- WP-601: Parallel run validation (Event Hub vs Eventhouse)
- WP-602: Cutover procedure
- WP-603: Decommission Event Hub consumer (optional)

