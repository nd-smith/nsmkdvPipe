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

**TECH-007: Confirm Consistent CTRL+C / Graceful Shutdown Behavior**
- Verify all workers handle KeyboardInterrupt consistently
- CTRL+C should initiate graceful shutdown: finish current batch, then exit
- Review: event-ingester, download-worker, upload-worker, result-processor, poller
- Document expected shutdown behavior
- Size: Medium

---

## Ready

### P2 - Medium Priority

**WP-506: Eventhouse Integration Testing**
- Integration tests for Eventhouse polling flow
- Mock Eventhouse for unit tests, optional real integration
- Size: Medium

**WP-507: Eventhouse Observability**
- Add metrics, alerts, and dashboard for Eventhouse polling
- New Grafana dashboard and Prometheus alert rules
- Size: Medium

### P3 - Low Priority

---

## Blocked

<!-- Tasks waiting on dependencies or decisions -->

(none)

---

## Completed

<!-- Done tasks with commit references -->

**TECH-001: Refactor EventIngesterWorker for Separate Consumer/Producer Configs** ✓
- Modified `EventIngesterWorker.__init__` to accept optional `producer_config` parameter
- Added `consumer_config` and `producer_config` attributes (consumer_config is primary, producer_config defaults to consumer_config)
- Updated `start()` to use `producer_config` for producer, `consumer_config` for consumer
- Updated `run_event_ingester()` to pass `local_kafka_config` as `producer_config`
- Added backward-compatible `config` property returning `consumer_config`
- **Impact**: Event Hub mode now correctly writes to local Kafka instead of Event Hub
- Size: Small

**TECH-002: Move Inline Imports to Top of Files** ✓
- Audited all inline imports in `kafka_pipeline/` directory
- **Finding**: 1 inline import should be moved, 5 are valid exceptions
- **Fixed**: Moved `import json` to top of `poller.py` (was inside `_parse_event_row` method)
- **Valid exceptions documented with comments**:
  - `__main__.py:507,527`: Lazy loading for conditional dev/prod code paths
  - `schemas/tasks.py:149`: Avoids circular dependency with verisk_pipeline
  - `schemas/events.py:174`: Avoids circular dependency with verisk_pipeline
  - `dlq/cli.py:273`: Lazy loading for CLI commands (only import what's needed)
- Size: Small (1 file changed, 4 files with added comments)

**TECH-017: Review delta_events.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/writers/delta_events.py` (155 lines, 1 class with 2 methods)
- **Finding**: No dead code found - file is small, clean, and well-structured
- All 7 imports verified in use:
  - `asyncio` - used for `asyncio.to_thread` non-blocking writes
  - `datetime, timezone` - used for timestamp generation
  - `Any, Dict, List` - used in type hints
  - `polars as pl` - used for DataFrame operations
  - `get_logger`, `DeltaTableWriter`, `flatten_events` - all in use
- `DeltaEventsWriter` class actively used in:
  - `poller.py` (KQLEventPoller)
  - `event_ingester.py` (EventIngesterWorker)
  - Exported via `writers/__init__.py`
  - Tested in `test_delta_events.py`
- Code quality is good: comprehensive docstrings, proper error handling, async non-blocking pattern
- No changes required
- Size: Small (review only)

**TECH-009: Review poller.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/eventhouse/poller.py` (973 → 836 lines, reduced by 137 lines)
- **Finding**: Dead code found - 4 unused imports + 2 legacy methods
- **Fixed**: Removed unused imports:
  - `timedelta` from `datetime` (never used)
  - `timezone` from `datetime` (never used)
  - `Callable` from `typing` (never used)
  - `WeakSet` from `weakref` (never used)
- **Fixed**: Removed legacy methods and their imports:
  - `_process_event_attachments()` - marked as legacy, only called from tests
  - `_process_single_attachment()` - marked as legacy, only called by `_process_event_attachments`
  - Removed 4 imports only used by legacy methods: `DownloadTaskMessage`, `generate_blob_path`, `validate_download_url`, `sanitize_url`
- Updated docstring workflow to reflect current architecture (events go to events.raw topic, EventIngester handles attachments)
- Removed obsolete patches and tests for legacy methods
- All 14 poller tests pass
- Size: Small (mostly cleanup)

**TECH-008: Review main.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/__main__.py` (593 lines, 14 top-level symbols)
- **Finding**: No dead code found - file is already well-structured
- All functions are used:
  - `get_shutdown_event()` - used by download/upload workers and signal handlers
  - `parse_args()` - used by main()
  - Worker run functions (`run_event_ingester`, `run_eventhouse_poller`, `run_download_worker`, `run_upload_worker`, `run_result_processor`, `run_local_event_ingester`) - all used in main() and/or run_all_workers()
  - `run_all_workers()` - used by main()
  - `setup_signal_handlers()` - used by main()
  - `main()` - entry point
- All constants used:
  - `WORKER_STAGES` - used in multi-worker logging setup
  - `logger` - module-level logger, properly initialized
  - `_shutdown_event` - global for graceful shutdown coordination
- TECH-001 TODO comment removed (issue fixed in separate PR)
- Inline imports are intentional for lazy loading and circular import avoidance
- Code quality is good: clear docstrings, well-organized sections, proper signal handling
- No changes required
- Size: Small (review only)

**TECH-013: Review dedup.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/eventhouse/dedup.py` (445 lines, 1 dataclass + 1 class with 10 methods + 1 standalone function)
- **Finding**: No dead code found - file is clean and well-structured
- All public symbols in use:
  - `DedupConfig` - used in `__main__.py`, `poller.py`, tests, exported via `__init__.py`
  - `EventhouseDeduplicator` - used in `poller.py`, tests, exported via `__init__.py`
  - `get_recent_trace_ids_sync` - standalone utility, exported via `__init__.py`, used in tests
- All methods of `EventhouseDeduplicator` in use:
  - `get_recent_trace_ids()`, `add_to_cache()`, `build_deduped_query()`, `get_poll_window()` - used in `poller.py`
  - `get_cache_size()` - used in `poller.py` for logging
  - `is_cache_initialized()` - accessor for internal state, tested
  - `build_kql_anti_join_filter()` - used internally by `build_deduped_query()`
  - Private methods: `_get_storage_options()`, `_load_cache_from_delta()` - used internally
- All 9 imports verified in use (logging, time, dataclass, datetime/timedelta/timezone, Optional, polars, get_storage_options)
- Code quality is good: clear docstrings, type hints, proper error handling, partition pruning optimization
- No changes required
- Size: Small (review only)

**TECH-012: Review kql_client.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/eventhouse/kql_client.py` (677 lines, 2 classes + 2 dataclasses)
- **Finding**: Minimal dead code - 2 unused imports
- **Fixed**: Removed unused imports:
  - `dataframe_from_result_table` from `azure.kusto.data.helpers` (never used)
  - `get_default_provider` from `core.auth.credentials` (never used)
- All public symbols in use:
  - `EventhouseConfig` - used in `__main__.py`, `poller.py`, tests, exported via `__init__.py`
  - `KQLClient` - used in `poller.py`, tests, exported via `__init__.py`
  - `KQLQueryResult` - used in `poller.py`, tests, exported via `__init__.py`
- Internal symbols in use:
  - `FileBackedKustoCredential` - used internally for token file authentication
  - `KUSTO_RESOURCE` - used by `FileBackedKustoCredential`
  - `DEFAULT_CONFIG_PATH` - used by `EventhouseConfig.load_config()` and imported by `poller.py`
- `from_env()` marked as deprecated, consistent with TECH-003 pattern
- Code quality is good: clear docstrings, type hints, proper retry logic, error handling
- Size: Small (just import cleanup)

**TECH-014: Review event_ingester.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/workers/event_ingester.py` (557 lines, 1 class with 8 methods)
- **Finding**: Minimal dead code - 1 unused import
- **Fixed**: Removed unused import:
  - `List` from `typing` (not used in type hints)
- All methods are in use:
  - Public: `start()`, `stop()` - used in production code (`__main__.py`)
  - Private: `_create_tracked_task()`, `_wait_for_pending_tasks()`, `_handle_event_message()`, `_process_attachment()`, `_write_event_to_delta()` - all used internally
- `EventIngesterWorker` is heavily used in `__main__.py`, `workers/__init__.py`, and test files
- Code quality is good: clear docstrings, type hints, proper error handling, graceful shutdown
- Size: Small (just import cleanup)

**TECH-003: Consolidate Configuration Loading (config.yaml Priority)** ✓
- Audited all config classes across the codebase
- **Finding**: `pipeline_config.py` had misnamed `from_env()` methods that actually loaded from config.yaml
- **Fixed**: Renamed to `load_config()` for consistency with other config classes:
  - `LocalKafkaConfig.from_env()` → `load_config()`
  - `EventhouseSourceConfig.from_env()` → `load_config()`
  - `PipelineConfig.from_env()` → `load_config()`
- Updated all callers in `__main__.py` and tests
- Configuration precedence is now consistent across all classes:
  1. Environment variables (highest priority)
  2. config.yaml file
  3. Dataclass defaults (lowest priority)
- Size: Medium

**TECH-015: Review config.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/config.py` (423 lines)
- **Finding**: No dead code found - file is well-structured
- All public symbols in use:
  - `KafkaConfig` - heavily used across the codebase (workers, DLQ, tests)
  - `load_config()` - used in poller.py and internally by `get_config()`
  - `get_config()`, `set_config()`, `reset_config()` - singleton pattern, used in tests
  - `from_env()` - deprecated but still actively used (DLQ CLI, poller, conftest)
  - `get_retry_topic()` - used in RetryHandler, DelayedRedeliveryScheduler, DownloadWorker
  - `get_consumer_group()` - used in DLQ CLI, DLQHandler, DelayedRedeliveryScheduler
- Private helpers (`_get_default_cache_dir`, `_deep_merge`, `_apply_env_overrides`) all in use
- Code quality is good: clear docstrings, type hints, proper configuration precedence
- No changes required
- Size: Small (review only)

**TECH-010: Review download_worker.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/workers/download_worker.py` (963 lines, 1 dataclass + 1 class with 16 methods)
- **Finding**: Minimal dead code - 3 unused imports
- **Fixed**: Removed unused imports:
  - `Dict` from `typing` (not used in type hints)
  - `update_consumer_lag` from metrics (never called)
  - `update_consumer_offset` from metrics (never called)
- All methods are in use:
  - Public: `start()`, `stop()`, `request_shutdown()`, `is_running`, `in_flight_count` - used in production and tests
  - Private: `_create_consumer()`, `_consume_batch_loop()`, `_process_batch()`, etc. - all used internally
- `TaskResult` dataclass used internally and in tests
- Code quality is good: clear docstrings, proper error handling, concurrent batch processing
- Size: Small (just import cleanup)

**TECH-011: Review pipeline_config.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/pipeline_config.py` (471 lines, 4 dataclasses + 2 helper functions)
- **Finding**: No dead code found - file is well-structured
- All public symbols in use:
  - `EventSourceType` - used in `__main__.py` (6 locations) for source type checks
  - `EventHubConfig` - used by `PipelineConfig.load_config()` and tests
  - `LocalKafkaConfig` - used in `__main__.py` and `PipelineConfig.load_config()`
  - `EventhouseSourceConfig` - used by `PipelineConfig.load_config()` and tests
  - `PipelineConfig` - primary config class, used in `__main__.py`
  - `get_pipeline_config()` - main entry point, used in `__main__.py`
  - `get_event_source_type()` - lightweight check (tests only, but useful API)
- All `to_kafka_config()` methods actively used for worker configuration
- All `load_config()` methods follow consistent precedence (env vars > yaml > defaults)
- `is_eventhub_source`/`is_eventhouse_source` properties used in tests (convenience API)
- All imports verified in use
- No changes required
- Size: Small (review only)

**TECH-020: Ensure File Names are Logical and Descriptive** ✓
- Audited all file names across `kafka_pipeline/` and `core/` packages
- **Finding**: All file names are logical, descriptive, and use consistent snake_case
- Removed dead `kafka_pipeline/kafka/` package (only had stale documentation `__init__.py`)
- No renames needed - naming conventions are consistent
- Size: Small (just cleanup)

**TECH-019: Review core/logging/context.py Structure and Remove Dead Code** ✓
- Reviewed `core/logging/context.py` (59 lines, 4 ContextVars + 3 functions)
- **Finding**: No dead code found - file is small and well-structured
- All functions in use:
  - `set_log_context()` - used in `__main__.py`, `setup.py`, context managers
  - `get_log_context()` - used in formatters and filters
  - `clear_log_context()` - used in test fixtures, exported API
- No changes required
- Size: Small (review only)

**TECH-018: Review core/logging/setup.py Structure and Remove Dead Code** ✓
- Reviewed `core/logging/setup.py` (313 lines)
- **Finding**: No dead code found - file is already well-structured
- All functions in use:
  - `setup_logging()` - used in `__main__.py` for single-worker mode
  - `setup_multi_worker_logging()` - used in `__main__.py` for multi-worker mode
  - `get_logger()` - widely used across consumer, producer, workers, writers
  - `get_log_file_path()` - internal helper used by both setup functions
  - `generate_cycle_id()` - used by verisk_pipeline modules (shared core library)
- Code quality is good: clear docstrings, type hints, Windows compatibility, rotating file handlers
- No changes required
- Size: Small (review only)

**TECH-016: Review producer.py Structure and Remove Dead Code** ✓
- Reviewed `kafka_pipeline/producer.py` (416 lines, single class `BaseKafkaProducer`)
- **Finding**: No dead code found - file is already well-structured
- All methods in use:
  - `start()`, `stop()`, `send()`, `is_started` - used in production code
  - `send_batch()`, `flush()` - tested API, used in performance/integration tests
- Code quality is good: clear docstrings, type hints, circuit breaker pattern, metrics
- No changes required
- Size: Small (review only)

**TECH-005: Review Dev Mode Necessity** ✓
- Evaluated `--dev` flag usage in `kafka_pipeline/__main__.py`
- **Finding**: The `--dev` flag IS needed and should be kept
- **Rationale**:
  - Production mode requires Event Hub credentials (`EVENTHUB_BOOTSTRAP_SERVERS`, `EVENTHUB_CONNECTION_STRING`)
    or Eventhouse credentials (`cluster_url`, `database`)
  - Dev mode bypasses these requirements, allowing local-only Kafka testing
  - Adding a `event_source: local` option to config.yaml would require more invasive changes
- Updated help text and added clarifying comments in code
- Size: Small

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

