# Kafka Pipeline Backlog

> **How to use this file:**
> 1. At session start, read this file to understand current state
> 2. Pick a task from "Ready" and move to "In Progress"
> 3. Complete the work package (code + tests)
> 4. Move to "Completed" with commit hash
> 5. Commit backlog update alongside code changes
> 6. RULE: Do not include claude code, claude, anthropic references in the codebase or commit messages. 

---

## Current Sprint: Phase 4 - Integration

### In Progress
<!-- Move tasks here when starting work -->

(none)

### Ready
<!-- Tasks ready to be picked up -->

Phase 4 work packages are now available (WP-401 to WP-410). Recommended implementation order:

**Testing Track** (can run in parallel):
1. ~~WP-401: E2E Test Infrastructure Setup~~ ✅ Complete
2. ~~WP-402: E2E Happy Path Test~~ ✅ Complete
3. ~~WP-403: E2E Retry Flow Test~~ ✅ Complete
4. ~~WP-404: E2E DLQ Flow Test~~ ✅ Complete
5. ~~WP-405: E2E Failure Scenarios~~ ✅ Complete

**Performance Track** (depends on WP-402):
6. ~~WP-406: Performance Benchmarking~~ ✅ Complete
7. ~~WP-407: Load Testing and Scaling Validation~~ ✅ Complete

**Observability Track** (can start early, depends on metrics from Phase 2):
8. ~~WP-408: Grafana Dashboards~~ ✅ Complete
9. ~~WP-409: Alerting Configuration~~ ✅ Complete
10. ~~WP-410: Runbook Documentation~~ ✅ Complete

### Blocked
<!-- Tasks waiting on dependencies or decisions -->

(none)

### Completed
<!-- Done tasks with commit references -->

- [x] **WP-001**: Create implementation plan (commit: f468314)
- [x] **WP-002**: Set up folder structure (commit: 92e4e9e)
- [x] **WP-003**: Add Claude Code constraints to plan (commit: d4d5509)
- [x] **WP-004**: Incorporate user feedback into plan (commit: b48068e)
- [x] **WP-101**: Create core package structure and base types (commit: 5f38763)
- [x] **WP-102**: Extract and review auth module - token cache (commit: c510e71)
- [x] **WP-103**: Extract and review auth module - Azure credentials (commit: 9d74b72)
- [x] **WP-104**: Add Kafka OAUTHBEARER support to auth (commit: b054206)
- [x] **WP-105**: Extract circuit breaker to core/resilience (commit: 7c7f064)
- [x] **WP-106**: Extract retry decorator to core/resilience (commit: 569642c)
- [x] **WP-107**: Extract logging - base logger and formatters (commit: c908f90)
- [x] **WP-108**: Extract logging - context managers (commit: 4df0b49)
- [x] **WP-109**: Add Kafka context to logging (commit: 0d7bd86)
- [x] **WP-110**: Extract error classification - exception hierarchy (commit: 323e765)
- [x] **WP-111**: Extract error classification - Azure error classifier (commit: d098cf8)
- [x] **WP-112**: Add Kafka error classifier (commit: ec21f42)
- [x] **WP-113**: Extract security - URL validation (commit: 9088292)
- [x] **WP-114**: Extract security - file type validation (commit: 261fc27)
- [x] **WP-115**: Extract security - presigned URL handling (commit: 5274dd8)
- [x] **WP-116**: Extract path resolution (commit: 4e09b6a)
- [x] **WP-117**: Extract async download - core HTTP logic (commit: b546028)
- [x] **WP-118**: Extract async download - streaming for large files (commit: a1f2acd)
- [x] **WP-119**: Create AttachmentDownloader class with clean interface (commit: 07e7cda)
- [x] **WP-201**: Kafka configuration and connection (commit: 5324f55)
- [x] **WP-202**: Implement EventMessage schema (commit: eb795f9)
- [x] **WP-203**: Implement DownloadTaskMessage schema (commit: b1dbc73)
- [x] **WP-204**: Implement DownloadResultMessage and FailedDownloadMessage schemas (commit: 2f374f2)
- [x] **WP-205**: Implement BaseKafkaProducer (commit: e7fbf6d)
- [x] **WP-206**: Implement BaseKafkaConsumer - core message loop (commit: 32bbd00)
- [x] **WP-207**: Implement BaseKafkaConsumer - error handling (commit: b8af83a)
- [x] **WP-208**: Add Kafka metrics (Prometheus) (commit: ebaa751)
- [x] **WP-209**: Implement RetryHandler (commit: 7413495)
- [x] **WP-210**: Implement DelayedRedeliveryScheduler (commit: 9f9c627)
- [x] **WP-211**: DLQ Handler skeleton (commit: 1cfde74)
- [x] **WP-212**: Integration tests with Testcontainers (commit: c510372)
- [x] **WP-301**: Event Ingester Worker - Core Implementation (commit: 178ae86)
- [x] **WP-302**: Event Ingester - Delta Analytics Integration (commit: 696705b)
- [x] **WP-303**: Event Ingester - Testing (commit: e6a915a)
- [x] **WP-304**: Download Worker - Core Implementation (commit: 53a0205)
- [x] **WP-305**: Download Worker - Upload Integration (commit: 0c0ccf0)
- [x] **WP-306**: Download Worker - Error Handling (commit: ba5793f)
- [x] **WP-307**: Download Worker - Testing (commit: 6eb906f)
- [x] **WP-308**: Result Processor - Core Implementation (commit: aaf283d)
- [x] **WP-309**: Result Processor - Delta Inventory Integration (commit: 0704b11)
- [x] **WP-310**: Result Processor - Testing (commit: 6a360c8)
- [x] **WP-311**: DLQ Handler - Implementation (commit: 07d3712)
- [x] **WP-312**: DLQ Handler - Testing (commit: 8479138)
- [x] **WP-401**: E2E Test Infrastructure Setup (commit: a281078)
- [x] **WP-402**: E2E Happy Path Test (commit: 928d09c)
- [x] **WP-403**: E2E Retry Flow Test (commit: f7c149f)
- [x] **WP-404**: E2E DLQ Flow Test (commit: ee5dcce)
- [x] **WP-405**: E2E Failure Scenarios (commit: 99ed4a2)
- [x] **WP-406**: Performance Benchmarking (commit: 1cf4c78)
- [x] **WP-407**: Load Testing and Scaling Validation (commit: 3e0badd)
- [x] **WP-408**: Grafana Dashboards (commit: 7b4152b)
- [x] **WP-409**: Alerting Configuration (commit: a32e8f7)

---

## Work Package Details

### Verified Work Packages

<details>
<summary><strong>Phase 1: Foundation (WP-101 to WP-119) ✅ Complete & Verified</strong></summary>

### WP-101: Create core package structure and base types

**Objective**: Set up core package with base types and protocols used across modules
**Size**: Small
**Files to read**:
- `src/core/__init__.py` (existing skeleton)
**Files to create/modify**:
- `src/core/__init__.py` (update exports)
- `src/core/types.py` (ErrorCategory enum, base protocols)
- `src/core/py.typed` (PEP 561 marker)
- `pyproject.toml` or `setup.py` (if needed for package)
**Dependencies**: None
**Deliverable**: Importable `core` package with `ErrorCategory` enum

---

### WP-102: Extract and review auth module - token cache

**Objective**: Extract TokenCache class with proper thread safety review
**Size**: Medium
**Files to read**:
- `src/verisk_pipeline/common/auth.py` (lines 1-200 approx)
**Files to create/modify**:
- `src/core/auth/token_cache.py`
- `tests/core/auth/test_token_cache.py`
**Dependencies**: WP-101
**Deliverable**: Working TokenCache with tests, thread-safe token refresh
**Review checklist**:
- [ ] Token expiry buffer (currently 50min) - is this correct?
- [ ] Thread safety of cache access
- [ ] Proper timezone handling (UTC)

---

### WP-103: Extract and review auth module - Azure credentials

**Objective**: Extract Azure credential abstraction (CLI, SPN, managed identity)
**Size**: Medium
**Files to read**:
- `src/verisk_pipeline/common/auth.py` (credential methods)
**Files to create/modify**:
- `src/core/auth/credentials.py`
- `tests/core/auth/test_credentials.py`
**Dependencies**: WP-102
**Deliverable**: Unified credential provider supporting CLI, SPN, DefaultAzureCredential
**Review checklist**:
- [ ] All auth methods work correctly
- [ ] Error messages are actionable
- [ ] No secrets logged

---

### WP-104: Add Kafka OAUTHBEARER support to auth

**Objective**: Add Kafka SASL/OAUTHBEARER callback for Azure AD authentication
**Size**: Small
**Files to read**:
- `src/core/auth/credentials.py` (from WP-103)
- aiokafka OAUTHBEARER docs
**Files to create/modify**:
- `src/core/auth/kafka_oauth.py`
- `tests/core/auth/test_kafka_oauth.py`
**Dependencies**: WP-103
**Deliverable**: `get_kafka_oauth_callback()` function for aiokafka
**Note**: SPN is used to obtain OAuth token - this is the mechanism

---

### WP-105: Extract circuit breaker to core/resilience

**Objective**: Extract CircuitBreaker with state machine review
**Size**: Medium
**Files to read**:
- `src/verisk_pipeline/common/circuit_breaker.py`
**Files to create/modify**:
- `src/core/resilience/circuit_breaker.py`
- `tests/core/resilience/test_circuit_breaker.py`
**Dependencies**: WP-101
**Deliverable**: CircuitBreaker class with CLOSED/OPEN/HALF_OPEN states
**Review checklist**:
- [ ] State transitions are correct
- [ ] Half-open properly tests recovery
- [ ] Thread safety for shared instances
- [ ] Async support (`call_async`)

---

### WP-106: Extract retry decorator to core/resilience

**Objective**: Extract retry logic with exponential backoff and jitter
**Size**: Small
**Files to read**:
- `src/verisk_pipeline/common/retry.py`
**Files to create/modify**:
- `src/core/resilience/retry.py`
- `tests/core/resilience/test_retry.py`
**Dependencies**: WP-101
**Deliverable**: `@with_retry` decorator and `RetryConfig` dataclass
**Review checklist**:
- [ ] Jitter prevents thundering herd
- [ ] Works with async functions
- [ ] Respects Retry-After headers

---

### WP-107: Extract logging - base logger and formatters

**Objective**: Extract structured JSON logging foundation
**Size**: Medium
**Files to read**:
- `src/verisk_pipeline/common/logging/setup.py`
- `src/verisk_pipeline/common/logging/formatters.py`
**Files to create/modify**:
- `src/core/logging/setup.py`
- `src/core/logging/formatters.py`
- `tests/core/logging/test_formatters.py`
**Dependencies**: WP-101
**Deliverable**: JSON logger with correlation ID support
**Review checklist**:
- [ ] JSON output is valid
- [ ] No PII in logs
- [ ] Performance is acceptable

---

### WP-108: Extract logging - context managers

**Objective**: Extract log context managers for phases and operations
**Size**: Small
**Files to read**:
- `src/verisk_pipeline/common/logging/context_managers.py`
**Files to create/modify**:
- `src/core/logging/context.py`
- `tests/core/logging/test_context.py`
**Dependencies**: WP-107
**Deliverable**: `log_phase`, `StageLogContext` context managers

---

### WP-109: Add Kafka context to logging

**Objective**: Add Kafka-specific context fields (topic, partition, offset)
**Size**: Small
**Files to read**:
- `src/core/logging/context.py` (from WP-108)
**Files to create/modify**:
- `src/core/logging/kafka_context.py`
- `tests/core/logging/test_kafka_context.py`
**Dependencies**: WP-108
**Deliverable**: `KafkaLogContext` for consumer/producer logging

---

### WP-110: Extract error classification - exception hierarchy

**Objective**: Extract PipelineError hierarchy and ErrorCategory
**Size**: Small
**Files to read**:
- `src/verisk_pipeline/common/exceptions.py`
**Files to create/modify**:
- `src/core/errors/exceptions.py`
- `tests/core/errors/test_exceptions.py`
**Dependencies**: WP-101
**Deliverable**: Clean exception hierarchy (AuthError, TransientError, PermanentError, etc.)

---

### WP-111: Extract error classification - Azure error classifier

**Objective**: Extract Azure/storage error code classification
**Size**: Medium
**Files to read**:
- `src/verisk_pipeline/storage/errors.py`
**Files to create/modify**:
- `src/core/errors/classifiers.py`
- `tests/core/errors/test_classifiers.py`
**Dependencies**: WP-110
**Deliverable**: `StorageErrorClassifier` with Azure error code mappings
**Review checklist**:
- [ ] All error codes correctly classified
- [ ] Transient vs permanent is accurate

---

### WP-112: Add Kafka error classifier

**Objective**: Add Kafka-specific error classification
**Size**: Small
**Files to read**:
- `src/core/errors/classifiers.py` (from WP-111)
- aiokafka exception types
**Files to create/modify**:
- `src/core/errors/kafka_classifier.py`
- `tests/core/errors/test_kafka_classifier.py`
**Dependencies**: WP-111
**Deliverable**: `KafkaErrorClassifier` for consumer/producer errors

---

### WP-113: Extract security - URL validation

**Objective**: Extract SSRF prevention and URL validation
**Size**: Small
**Files to read**:
- `src/verisk_pipeline/common/security.py`
**Files to create/modify**:
- `src/core/security/url_validation.py`
- `tests/core/security/test_url_validation.py`
**Dependencies**: WP-101
**Deliverable**: `validate_download_url()` with domain allowlist
**Review checklist**:
- [ ] No bypass vectors
- [ ] Unicode/encoding edge cases handled

---

### WP-114: Extract security - file type validation (new)

**Objective**: Implement file type allowlist validation per FR-2.2.1
**Size**: Small
**Files to read**:
- `src/core/security/url_validation.py` (from WP-113)
**Files to create/modify**:
- `src/core/security/file_validation.py`
- `tests/core/security/test_file_validation.py`
**Dependencies**: WP-113
**Deliverable**: `validate_file_type()` checking extension and content-type

---

### WP-115: Extract security - presigned URL handling

**Objective**: Extract presigned URL detection and expiration checking
**Size**: Small
**Files to read**:
- `src/verisk_pipeline/common/url_expiration.py`
**Files to create/modify**:
- `src/core/security/presigned_urls.py`
- `tests/core/security/test_presigned_urls.py`
**Dependencies**: WP-113
**Deliverable**: `check_presigned_url()` for S3 and ClaimX URLs

---

### WP-116: Extract path resolution

**Objective**: Extract blob path generation logic
**Size**: Small
**Files to read**:
- `src/verisk_pipeline/xact/services/path_resolver.py`
**Files to create/modify**:
- `src/core/paths/resolver.py`
- `tests/core/paths/test_resolver.py`
**Dependencies**: WP-101
**Deliverable**: `generate_blob_path()` function (unchanged logic)

---

### WP-117: Extract async download - core HTTP logic

**Objective**: Extract aiohttp download logic without Delta coupling
**Size**: Medium
**Files to read**:
- `src/verisk_pipeline/xact/stages/xact_download.py` (lines 200-350)
**Files to create/modify**:
- `src/core/download/http_client.py`
- `tests/core/download/test_http_client.py`
**Dependencies**: WP-105 (circuit breaker), WP-113 (URL validation)
**Deliverable**: Basic async HTTP download with timeout and retry
**Review checklist**:
- [ ] Timeouts properly configured
- [ ] Connection pooling efficient
- [ ] SSL verification enabled

---

### WP-118: Extract async download - streaming for large files

**Objective**: Add streaming support for files >50MB
**Size**: Small
**Files to read**:
- `src/core/download/http_client.py` (from WP-117)
- `src/verisk_pipeline/xact/stages/xact_download.py` (streaming code)
**Files to create/modify**:
- `src/core/download/streaming.py`
- `tests/core/download/test_streaming.py`
**Dependencies**: WP-117
**Deliverable**: Chunked streaming download with memory bounds

---

### WP-119: Create AttachmentDownloader class with clean interface

**Objective**: Create unified downloader with DownloadTask/DownloadOutcome interface
**Size**: Medium
**Files to read**:
- `src/core/download/http_client.py` (from WP-117)
- `src/core/download/streaming.py` (from WP-118)
**Files to create/modify**:
- `src/core/download/downloader.py`
- `src/core/download/models.py`
- `tests/core/download/test_downloader.py`
**Dependencies**: WP-117, WP-118
**Deliverable**: `AttachmentDownloader.download(task) -> DownloadOutcome`

</details>

<details open>
<summary><strong>Phase 2: Kafka Infrastructure (WP-201 to WP-212)</strong></summary>

### WP-201: Create kafka_pipeline package structure and KafkaConfig

**Objective**: Set up kafka_pipeline package with configuration dataclass
**Size**: Small
**Files to read**:
- `src/kafka_pipeline/__init__.py` (existing skeleton)
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.1, 7.1)
**Files to create/modify**:
- `src/kafka_pipeline/__init__.py` (update)
- `src/kafka_pipeline/config.py` (KafkaConfig dataclass)
- `src/kafka_pipeline/py.typed` (PEP 561 marker)
- `tests/kafka_pipeline/test_config.py`
**Dependencies**: None
**Deliverable**: `KafkaConfig.from_env()` loading from environment variables
**Review checklist**:
- [ ] All env vars from Section 7.1 supported
- [ ] Sensible defaults for optional settings
- [ ] Validation for required fields

---

### WP-202: Implement EventMessage schema

**Objective**: Create Pydantic model for raw event messages from source
**Size**: Small
**Files to read**:
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.2)
- Existing event schemas if any
**Files to create/modify**:
- `src/kafka_pipeline/schemas/__init__.py`
- `src/kafka_pipeline/schemas/events.py`
- `tests/kafka_pipeline/schemas/test_events.py`
**Dependencies**: WP-201
**Deliverable**: `EventMessage` with JSON serialization tests
**Review checklist**:
- [ ] datetime serialization to ISO format
- [ ] Optional fields handled correctly
- [ ] Validation for required fields (trace_id, event_type)

---

### WP-203: Implement DownloadTaskMessage schema

**Objective**: Create Pydantic model for download work items
**Size**: Small
**Files to read**:
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.2)
- `src/kafka_pipeline/schemas/events.py` (from WP-202)
**Files to create/modify**:
- `src/kafka_pipeline/schemas/tasks.py`
- `tests/kafka_pipeline/schemas/test_tasks.py`
**Dependencies**: WP-202
**Deliverable**: `DownloadTaskMessage` with retry_count tracking
**Review checklist**:
- [ ] Metadata dict for extensibility
- [ ] retry_count defaults to 0
- [ ] original_timestamp preserved through retries

---

### WP-204: Implement DownloadResultMessage and FailedDownloadMessage schemas

**Objective**: Create Pydantic models for download outcomes and DLQ messages
**Size**: Small
**Files to read**:
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.2, 4.2.5)
- `src/kafka_pipeline/schemas/tasks.py` (from WP-203)
**Files to create/modify**:
- `src/kafka_pipeline/schemas/results.py`
- `tests/kafka_pipeline/schemas/test_results.py`
**Dependencies**: WP-203
**Deliverable**: `DownloadResultMessage`, `FailedDownloadMessage` schemas
**Review checklist**:
- [ ] Status enum: success, failed_transient, failed_permanent
- [ ] FailedDownloadMessage includes original_task reference
- [ ] Error context preserved (truncated to avoid huge messages)

---

### WP-205: Implement BaseKafkaProducer

**Objective**: Create async Kafka producer with circuit breaker integration
**Size**: Medium
**Files to read**:
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.4)
- `src/kafka_pipeline/config.py` (from WP-201)
- `src/core/resilience/circuit_breaker.py` (from WP-105)
**Files to create/modify**:
- `src/kafka_pipeline/producer.py`
- `tests/kafka_pipeline/test_producer.py`
**Dependencies**: WP-201, WP-105 (circuit breaker)
**Deliverable**: `BaseKafkaProducer` with `send()` and `send_batch()` methods
**Review checklist**:
- [ ] OAUTHBEARER callback integration
- [ ] Circuit breaker wraps send operations
- [ ] Proper cleanup in `stop()`
- [ ] Headers support for routing metadata

---

### WP-206: Implement BaseKafkaConsumer - core message loop

**Objective**: Create async Kafka consumer with manual commit
**Size**: Medium
**Files to read**:
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.3)
- `src/kafka_pipeline/config.py` (from WP-201)
**Files to create/modify**:
- `src/kafka_pipeline/consumer.py`
- `tests/kafka_pipeline/test_consumer.py`
**Dependencies**: WP-201, WP-105 (circuit breaker)
**Deliverable**: `BaseKafkaConsumer` with `start()`, `stop()`, message iteration
**Review checklist**:
- [ ] Manual offset commit after successful processing
- [ ] Graceful shutdown handling
- [ ] OAUTHBEARER callback integration
- [ ] Multiple topics subscription

---

### WP-207: Implement BaseKafkaConsumer - error handling

**Objective**: Add error classification and retry/DLQ routing to consumer
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/consumer.py` (from WP-206)
- `src/core/errors/` (from WP-110, WP-112)
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.3 error handling)
**Files to create/modify**:
- `src/kafka_pipeline/consumer.py` (extend)
- `tests/kafka_pipeline/test_consumer_errors.py`
**Dependencies**: WP-206, WP-110, WP-112
**Deliverable**: Error routing: transient→retry, permanent→DLQ, auth→reprocess
**Review checklist**:
- [ ] ErrorCategory correctly routes messages
- [ ] Circuit open errors don't commit (will reprocess)
- [ ] Logging includes error context

---

### WP-208: Add Kafka metrics (Prometheus)

**Objective**: Instrument producer and consumer with Prometheus metrics
**Size**: Small
**Files to read**:
- `src/kafka_pipeline/producer.py` (from WP-205)
- `src/kafka_pipeline/consumer.py` (from WP-206)
- prometheus_client docs
**Files to create/modify**:
- `src/kafka_pipeline/metrics.py`
- `src/kafka_pipeline/producer.py` (add metrics)
- `src/kafka_pipeline/consumer.py` (add metrics)
- `tests/kafka_pipeline/test_metrics.py`
**Dependencies**: WP-205, WP-206
**Deliverable**: Metrics: messages_produced, messages_consumed, consumer_lag, errors
**Review checklist**:
- [ ] Labels: topic, partition, consumer_group
- [ ] Histogram for processing time
- [ ] Counter for errors by category

---

### WP-209: Implement RetryHandler

**Objective**: Create retry routing logic with exponential backoff topics
**Size**: Medium
**Files to read**:
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.5)
- `src/kafka_pipeline/producer.py` (from WP-205)
- `src/kafka_pipeline/schemas/tasks.py` (from WP-203)
**Files to create/modify**:
- `src/kafka_pipeline/retry/__init__.py`
- `src/kafka_pipeline/retry/handler.py`
- `tests/kafka_pipeline/retry/test_handler.py`
**Dependencies**: WP-205, WP-203, WP-204
**Deliverable**: `RetryHandler.handle_failure()` routing to retry topics or DLQ
**Review checklist**:
- [ ] Retry count incremented correctly
- [ ] Error context preserved in metadata
- [ ] DLQ routing after max retries exhausted
- [ ] Configurable retry delays

---

### WP-210: Implement DelayedRedeliveryScheduler

**Objective**: Create scheduler for moving messages from retry topics to pending
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/retry/handler.py` (from WP-209)
- `src/kafka_pipeline/consumer.py` (from WP-206)
**Files to create/modify**:
- `src/kafka_pipeline/retry/scheduler.py`
- `tests/kafka_pipeline/retry/test_scheduler.py`
**Dependencies**: WP-209, WP-206
**Deliverable**: Scheduler that consumes retry topics and redelivers after delay
**Review checklist**:
- [ ] Respects retry_at timestamp in metadata
- [ ] Pauses if pending topic has high lag
- [ ] Graceful shutdown

---

### WP-211: DLQ Handler skeleton

**Objective**: Create basic DLQ consumer for manual review/replay
**Size**: Small
**Files to read**:
- `src/kafka_pipeline/consumer.py` (from WP-206)
- `src/kafka_pipeline/schemas/results.py` (from WP-204)
**Files to create/modify**:
- `src/kafka_pipeline/dlq/__init__.py`
- `src/kafka_pipeline/dlq/handler.py`
- `tests/kafka_pipeline/dlq/test_handler.py`
**Dependencies**: WP-206, WP-204
**Deliverable**: `DLQHandler` with manual ack and replay capability
**Review checklist**:
- [ ] Manual ack (no auto-commit)
- [ ] Replay sends to original pending topic
- [ ] Logging for audit trail

---

### WP-212: Integration tests with Testcontainers

**Objective**: Set up Docker-based Kafka for integration testing
**Size**: Medium
**Files to read**:
- All kafka_pipeline modules
- testcontainers-python docs
**Files to create/modify**:
- `tests/kafka_pipeline/conftest.py` (Kafka fixtures)
- `tests/kafka_pipeline/integration/__init__.py`
- `tests/kafka_pipeline/integration/test_produce_consume.py`
- `tests/kafka_pipeline/integration/test_retry_flow.py`
**Dependencies**: WP-205, WP-206, WP-209
**Deliverable**: Working integration tests with real Kafka container
**Review checklist**:
- [ ] Container starts/stops reliably
- [ ] Tests are isolated (unique topics per test)
- [ ] CI-friendly (works in GitHub Actions)

</details>

---

<details>
<summary><strong>Phase 3: Workers (WP-301 to WP-312)</strong></summary>

## Phase 3 Overview

**Goal**: Build the worker components that process events, download attachments, and write results.

**Structure**: Phase 3 implements the three main workers plus DLQ handler:
1. **Event Ingester** (WP-301 to WP-303): Consumes events, produces download tasks
2. **Download Worker** (WP-304 to WP-307): Downloads attachments, uploads to OneLake
3. **Result Processor** (WP-308 to WP-310): Batches results, writes to Delta inventory
4. **DLQ Handler** (WP-311 to WP-312): Manual review and replay for failed tasks

**Recommended Implementation Order**:
- Start with Event Ingester (WP-301→302→303) - establishes the entry point
- Then Download Worker (WP-304→305→306→307) - core business logic
- Then Result Processor (WP-308→309→310) - completes the pipeline
- Finally DLQ Handler (WP-311→312) - failure handling

**Context Management Best Practices**:
- Each work package is sized to fit within Claude Code context limits
- Testing work packages are separate to reduce context load
- Integration work packages extend base implementations rather than creating from scratch
- Review checklists ensure completeness without requiring re-reading implementations

---

### WP-301: Event Ingester Worker - Core Implementation

**Objective**: Create worker to consume events and produce download tasks
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/consumer.py` (from WP-206)
- `src/kafka_pipeline/producer.py` (from WP-205)
- `src/kafka_pipeline/schemas/events.py` (from WP-202)
- `src/kafka_pipeline/schemas/tasks.py` (from WP-203)
- `src/core/security/url_validation.py` (from WP-113)
- `src/core/paths/resolver.py` (from WP-116)
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.6)
**Files to create/modify**:
- `src/kafka_pipeline/workers/__init__.py`
- `src/kafka_pipeline/workers/event_ingester.py`
- `tests/kafka_pipeline/workers/test_event_ingester.py`
**Dependencies**: WP-206, WP-205, WP-202, WP-203, WP-113, WP-116
**Deliverable**: `EventIngesterWorker` consuming events.raw, producing to downloads.pending
**Review checklist**:
- [ ] Consumes from events.raw topic with correct consumer group
- [ ] Validates URLs before creating download tasks
- [ ] Generates correct blob paths for each attachment
- [ ] Produces well-formed DownloadTaskMessage to pending topic
- [ ] Skips events with no attachments
- [ ] Logs validation failures with sanitized URLs

---

### WP-302: Event Ingester - Delta Analytics Integration

**Objective**: Add Delta Lake write for event analytics and deduplication
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/workers/event_ingester.py` (from WP-301)
- Existing Delta write patterns from legacy codebase
**Files to create/modify**:
- `src/kafka_pipeline/workers/event_ingester.py` (extend)
- `src/kafka_pipeline/writers/__init__.py`
- `src/kafka_pipeline/writers/delta_events.py`
- `tests/kafka_pipeline/writers/test_delta_events.py`
**Dependencies**: WP-301
**Deliverable**: Event ingester writing to Delta `xact_events` table with deduplication
**Review checklist**:
- [ ] Delta writes are non-blocking (asyncio.create_task)
- [ ] Deduplication by trace_id within configurable window
- [ ] Schema matches existing `xact_events` table
- [ ] Write failures don't block Kafka processing
- [ ] Metrics for Delta write success/failure

---

### WP-303: Event Ingester - Testing

**Objective**: Comprehensive testing for event ingestion flow
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/workers/event_ingester.py` (from WP-302)
- `tests/kafka_pipeline/conftest.py` (from WP-212)
**Files to create/modify**:
- `tests/kafka_pipeline/workers/test_event_ingester.py` (extend)
- `tests/kafka_pipeline/integration/test_event_ingestion.py`
**Dependencies**: WP-302, WP-212
**Deliverable**: Unit and integration tests with >90% coverage
**Review checklist**:
- [ ] Unit tests: URL validation, path generation, message production
- [ ] Integration test: event → download task flow
- [ ] Integration test: deduplication behavior
- [ ] Integration test: invalid URL handling
- [ ] Integration test: Delta write success/failure
- [ ] Tests use Testcontainers for real Kafka

---

### WP-304: Download Worker - Core Implementation

**Objective**: Create worker to process download tasks
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/consumer.py` (from WP-206)
- `src/kafka_pipeline/producer.py` (from WP-205)
- `src/kafka_pipeline/schemas/tasks.py` (from WP-203)
- `src/core/download/downloader.py` (from WP-119)
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.7)
**Files to create/modify**:
- `src/kafka_pipeline/workers/download_worker.py`
- `tests/kafka_pipeline/workers/test_download_worker.py`
**Dependencies**: WP-206, WP-205, WP-203, WP-119
**Deliverable**: `DownloadWorker` consuming from pending + retry topics
**Review checklist**:
- [ ] Subscribes to all required topics (pending + retry.*)
- [ ] Integrates with AttachmentDownloader
- [ ] Handles DownloadTask → DownloadOutcome conversion
- [ ] Measures processing time per task
- [ ] Consumer group configured correctly

---

### WP-305: Download Worker - Upload Integration

**Objective**: Add OneLake upload and result production
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/workers/download_worker.py` (from WP-304)
- `src/kafka_pipeline/schemas/results.py` (from WP-204)
- Existing OneLake client patterns
**Files to create/modify**:
- `src/kafka_pipeline/workers/download_worker.py` (extend)
- `src/kafka_pipeline/storage/__init__.py`
- `src/kafka_pipeline/storage/onelake_client.py`
- `tests/kafka_pipeline/storage/test_onelake_client.py`
**Dependencies**: WP-304, WP-204
**Deliverable**: Download worker uploading to OneLake and producing results
**Review checklist**:
- [ ] Successful downloads uploaded to correct blob path
- [ ] DownloadResultMessage produced with all fields
- [ ] Upload failures handled gracefully
- [ ] Temporary files cleaned up after upload
- [ ] OneLake client follows Microsoft best practices (NFR-2.10, FR-2.11)

---

### WP-306: Download Worker - Error Handling

**Objective**: Add error classification and retry/DLQ routing
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/workers/download_worker.py` (from WP-305)
- `src/kafka_pipeline/retry/handler.py` (from WP-209)
- `src/core/errors/classifiers.py` (from WP-111, WP-112)
**Files to create/modify**:
- `src/kafka_pipeline/workers/download_worker.py` (extend)
- `tests/kafka_pipeline/workers/test_download_worker_errors.py`
**Dependencies**: WP-305, WP-209, WP-111, WP-112
**Deliverable**: Complete error handling with retry/DLQ routing
**Review checklist**:
- [ ] Transient errors routed to retry topics
- [ ] Permanent errors routed to DLQ
- [ ] Retry count incremented correctly
- [ ] Error context preserved in metadata
- [ ] Circuit breaker errors don't commit offset
- [ ] Auth errors trigger reprocessing

---

### WP-307: Download Worker - Testing

**Objective**: Comprehensive testing for download worker
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/workers/download_worker.py` (from WP-306)
- `tests/kafka_pipeline/conftest.py` (from WP-212)
**Files to create/modify**:
- `tests/kafka_pipeline/workers/test_download_worker.py` (extend)
- `tests/kafka_pipeline/integration/test_download_flow.py`
**Dependencies**: WP-306, WP-212
**Deliverable**: Unit and integration tests with >90% coverage
**Review checklist**:
- [ ] Unit tests: download success, transient failure, permanent failure
- [ ] Integration test: pending → download → upload → result
- [ ] Integration test: transient failure → retry topic
- [ ] Integration test: permanent failure → DLQ
- [ ] Integration test: retry exhaustion → DLQ
- [ ] Mock OneLake for unit tests, real for integration

---

### WP-308: Result Processor - Core Implementation

**Objective**: Create worker to consume results with batching logic
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/consumer.py` (from WP-206)
- `src/kafka_pipeline/schemas/results.py` (from WP-204)
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.8)
**Files to create/modify**:
- `src/kafka_pipeline/workers/result_processor.py`
- `tests/kafka_pipeline/workers/test_result_processor.py`
**Dependencies**: WP-206, WP-204
**Deliverable**: `ResultProcessor` with size and timeout-based batching
**Review checklist**:
- [ ] Consumes from downloads.results topic
- [ ] Batches messages by size (default: 100)
- [ ] Batches messages by timeout (default: 5s)
- [ ] Thread-safe batch accumulation
- [ ] Filters for successful downloads only
- [ ] Graceful shutdown flushes pending batch

---

### WP-309: Result Processor - Delta Inventory Integration

**Objective**: Add Delta inventory table writes with idempotency
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/workers/result_processor.py` (from WP-308)
- Existing Delta inventory patterns
**Files to create/modify**:
- `src/kafka_pipeline/workers/result_processor.py` (extend)
- `src/kafka_pipeline/writers/delta_inventory.py`
- `tests/kafka_pipeline/writers/test_delta_inventory.py`
**Dependencies**: WP-308
**Deliverable**: Result processor writing to Delta `xact_attachments` table
**Review checklist**:
- [ ] Schema matches existing `xact_attachments` table
- [ ] Merge on (trace_id, attachment_url) for idempotency
- [ ] Batch writes use asyncio.to_thread for blocking I/O
- [ ] Write failures logged with batch details
- [ ] Metrics for batch size and write latency

---

### WP-310: Result Processor - Testing

**Objective**: Comprehensive testing for result processing
**Size**: Small
**Files to read**:
- `src/kafka_pipeline/workers/result_processor.py` (from WP-309)
- `tests/kafka_pipeline/conftest.py` (from WP-212)
**Files to create/modify**:
- `tests/kafka_pipeline/workers/test_result_processor.py` (extend)
- `tests/kafka_pipeline/integration/test_result_processing.py`
**Dependencies**: WP-309, WP-212
**Deliverable**: Unit and integration tests with >90% coverage
**Review checklist**:
- [ ] Unit tests: batch accumulation, size flush, timeout flush
- [ ] Unit tests: successful download filtering
- [ ] Integration test: result → batch → Delta write
- [ ] Integration test: idempotency (duplicate results)
- [ ] Integration test: graceful shutdown with pending batch
- [ ] Mock Delta for unit tests, real for integration

---

### WP-311: DLQ Handler - Implementation

**Objective**: Create DLQ consumer with manual review and replay
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/consumer.py` (from WP-206)
- `src/kafka_pipeline/producer.py` (from WP-205)
- `src/kafka_pipeline/schemas/results.py` (from WP-204)
- `src/kafka_pipeline/dlq/handler.py` (from WP-211)
- `docs/kafka-greenfield-implementation-plan.md` (Section 4.2.6)
**Files to create/modify**:
- `src/kafka_pipeline/dlq/handler.py` (extend from WP-211 skeleton)
- `src/kafka_pipeline/dlq/cli.py`
- `tests/kafka_pipeline/dlq/test_handler.py` (extend)
**Dependencies**: WP-206, WP-205, WP-204, WP-211
**Deliverable**: DLQ handler with CLI for manual review and replay
**Review checklist**:
- [ ] Consumes from DLQ topic with manual commit
- [ ] CLI tool to list DLQ messages
- [ ] CLI tool to view message details
- [ ] CLI tool to replay message to pending topic
- [ ] CLI tool to mark message as resolved (commit offset)
- [ ] Audit logging for all DLQ operations

---

### WP-312: DLQ Handler - Testing

**Objective**: Testing for DLQ handler and CLI
**Size**: Small
**Files to read**:
- `src/kafka_pipeline/dlq/handler.py` (from WP-311)
- `src/kafka_pipeline/dlq/cli.py` (from WP-311)
- `tests/kafka_pipeline/conftest.py` (from WP-212)
**Files to create/modify**:
- `tests/kafka_pipeline/dlq/test_handler.py` (extend)
- `tests/kafka_pipeline/dlq/test_cli.py`
- `tests/kafka_pipeline/integration/test_dlq_flow.py`
**Dependencies**: WP-311, WP-212
**Deliverable**: Unit and integration tests for DLQ operations
**Review checklist**:
- [ ] Unit tests: message listing, viewing, replay, resolve
- [ ] Integration test: permanent failure → DLQ
- [ ] Integration test: exhausted retry → DLQ
- [ ] Integration test: replay from DLQ → pending topic
- [ ] Integration test: manual ack after resolve
- [ ] CLI tests with mock consumer/producer

</details>

---

<details open>
<summary><strong>Phase 4: Integration (WP-401 to WP-410)</strong></summary>

## Phase 4 Overview

**Goal**: Validate the complete system through end-to-end testing, establish production observability, and verify performance targets.

**Structure**: Phase 4 implements three critical validation domains:
1. **End-to-End Testing** (WP-401 to WP-405): Complete flow validation from event ingestion to inventory
2. **Performance Validation** (WP-406 to WP-407): Benchmarking and load testing
3. **Production Readiness** (WP-408 to WP-410): Observability, alerting, and operational documentation

**Success Criteria**:
- All end-to-end flows complete successfully
- Performance targets met (Section 9 of implementation plan)
- Observability dashboards operational
- Runbooks complete for all operational scenarios

**Context Management Best Practices**:
- Each work package focuses on specific test scenarios or observability components
- Integration tests build incrementally on existing test infrastructure
- Performance tests isolated from functional tests to reduce context
- Observability work packages separated by concern (dashboards, alerts, documentation)

---

### WP-401: E2E Test Infrastructure Setup

**Objective**: Set up complete integration test environment with all workers running
**Size**: Medium
**Files to read**:
- `tests/kafka_pipeline/conftest.py` (existing Kafka fixtures)
- `tests/kafka_pipeline/integration/` (existing integration tests)
- `src/kafka_pipeline/workers/` (all workers from Phase 3)
- `docs/kafka-greenfield-implementation-plan.md` (Section 6.2 integration tests)
**Files to create/modify**:
- `tests/kafka_pipeline/integration/conftest.py` (worker fixtures)
- `tests/kafka_pipeline/integration/fixtures/` (test data generators)
- `tests/kafka_pipeline/integration/helpers.py` (test utilities)
- `tests/kafka_pipeline/integration/test_environment.py` (environment validation)
**Dependencies**: Phase 3 complete (WP-301 to WP-312)
**Deliverable**: Working integration test environment with all workers, test data generators, and utilities
**Review checklist**:
- [ ] All workers can be started in test environment
- [ ] Test data generators for EventMessage, DownloadTaskMessage
- [ ] Mock OneLake client for testing without external dependencies
- [ ] Mock Delta Lake writers with in-memory verification
- [ ] Utility functions for Kafka topic inspection
- [ ] Utility functions for waiting on async operations
- [ ] Environment health checks (all workers connected, topics created)

---

### WP-402: E2E Happy Path Test

**Objective**: Complete end-to-end test from event ingestion through download to inventory write
**Size**: Medium
**Files to read**:
- `tests/kafka_pipeline/integration/conftest.py` (from WP-401)
- `tests/kafka_pipeline/integration/helpers.py` (from WP-401)
- `src/kafka_pipeline/workers/event_ingester.py`
- `src/kafka_pipeline/workers/download_worker.py`
- `src/kafka_pipeline/workers/result_processor.py`
**Files to create/modify**:
- `tests/kafka_pipeline/integration/test_e2e_happy_path.py`
**Dependencies**: WP-401
**Deliverable**: Passing E2E test validating complete happy path flow
**Review checklist**:
- [ ] Test produces EventMessage to events.raw topic
- [ ] Validates DownloadTaskMessage created in downloads.pending
- [ ] Mocks successful download (with configurable file size)
- [ ] Validates OneLake upload called with correct path
- [ ] Validates DownloadResultMessage produced to downloads.results
- [ ] Validates Delta inventory write with correct data
- [ ] End-to-end latency measured and logged
- [ ] Test includes multiple events with multiple attachments each
- [ ] Validates deduplication by trace_id

---

### WP-403: E2E Retry Flow Test

**Objective**: Test transient failure → retry topic → successful redelivery flow
**Size**: Medium
**Files to read**:
- `tests/kafka_pipeline/integration/conftest.py` (from WP-401)
- `src/kafka_pipeline/workers/download_worker.py`
- `src/kafka_pipeline/retry/handler.py`
- `src/kafka_pipeline/retry/scheduler.py`
**Files to create/modify**:
- `tests/kafka_pipeline/integration/test_e2e_retry_flow.py`
**Dependencies**: WP-401
**Deliverable**: Passing E2E test validating retry flow with exponential backoff
**Review checklist**:
- [ ] Test simulates transient download failure (timeout, 503 error)
- [ ] Validates message routed to first retry topic (5m delay)
- [ ] Validates retry_count incremented
- [ ] Validates error metadata preserved
- [ ] Validates DelayedRedeliveryScheduler redelivers after delay
- [ ] Validates successful download on retry attempt
- [ ] Test simulates multiple retry attempts (1st fails, 2nd succeeds)
- [ ] Validates retry exhaustion → DLQ routing
- [ ] Validates metrics: retry_count, error_category labels

---

### WP-404: E2E DLQ Flow Test

**Status**: ✅ Complete (commit: ee5dcce)

**Objective**: Test permanent failure → DLQ → manual replay flow
**Size**: Medium
**Files to read**:
- `tests/kafka_pipeline/integration/conftest.py` (from WP-401)
- `src/kafka_pipeline/workers/download_worker.py`
- `src/kafka_pipeline/retry/handler.py`
- `src/kafka_pipeline/dlq/handler.py`
- `src/kafka_pipeline/dlq/cli.py`
**Files to create/modify**:
- `tests/kafka_pipeline/integration/test_e2e_dlq_flow.py`
**Dependencies**: WP-401
**Deliverable**: Passing E2E test validating DLQ routing and replay
**Review checklist**:
- [x] Test simulates permanent failure (404, invalid URL, file type validation)
- [x] Validates message routed directly to DLQ (no retry)
- [x] Validates FailedDownloadMessage contains original_task
- [x] Validates error context preserved (final_error, error_category)
- [x] Test simulates retry exhaustion scenario
- [x] Validates DLQ CLI can list messages
- [x] Validates DLQ CLI can view message details
- [x] Validates DLQ CLI replay sends message to pending topic
- [x] Validates replayed message has retry_count reset to 0
- [x] Validates audit log entries for DLQ operations

---

### WP-405: E2E Failure Scenarios

**Objective**: Test error handling for circuit breaker, auth failures, and edge cases
**Size**: Medium
**Files to read**:
- `tests/kafka_pipeline/integration/conftest.py` (from WP-401)
- `src/kafka_pipeline/consumer.py` (error handling)
- `src/core/resilience/circuit_breaker.py`
- `src/core/errors/` (error classifiers)
**Files to create/modify**:
- `tests/kafka_pipeline/integration/test_e2e_failure_scenarios.py`
**Dependencies**: WP-401
**Deliverable**: Passing tests for all failure modes and edge cases
**Review checklist**:
- [ ] Circuit breaker open: no offset commit, reprocessing on recovery
- [ ] Auth failure: no offset commit, reprocessing on token refresh
- [ ] Kafka broker unavailability: graceful degradation
- [ ] OneLake unavailability: retry with backoff
- [ ] Delta Lake write failure: logged, doesn't block Kafka processing
- [ ] Invalid event schema: logged, message skipped
- [ ] Missing required fields in event: validation error handling
- [ ] Expired presigned URL: refresh logic (if applicable to domain)
- [ ] Large file streaming (>50MB): validates memory bounds
- [ ] Consumer lag buildup: scheduler pauses retry delivery

---

### WP-406: Performance Benchmarking

**Objective**: Measure throughput, latency, and resource usage under various loads
**Size**: Medium
**Files to read**:
- `tests/kafka_pipeline/integration/conftest.py` (from WP-401)
- `docs/kafka-greenfield-implementation-plan.md` (Section 2.1 performance targets)
**Files to create/modify**:
- `tests/kafka_pipeline/performance/` (new package)
- `tests/kafka_pipeline/performance/conftest.py` (performance fixtures)
- `tests/kafka_pipeline/performance/test_throughput.py`
- `tests/kafka_pipeline/performance/test_latency.py`
- `tests/kafka_pipeline/performance/test_resource_usage.py`
- `tests/kafka_pipeline/performance/benchmarks.md` (results documentation)
**Dependencies**: WP-402 (happy path must work first)
**Deliverable**: Performance benchmark suite with baseline measurements
**Review checklist**:
- [ ] Throughput test: 1,000 events/second sustained (NFR-1.2)
- [ ] Latency test: p50, p95, p99 measurements (NFR-1.1 target: <5s p95)
- [ ] Memory usage per worker: measured under load (target: <512MB per worker)
- [ ] CPU usage per worker: measured under load
- [ ] Consumer lag recovery: 100k message backlog cleared in <10min (NFR-1.4)
- [ ] Download concurrency: 50 parallel downloads (NFR-1.3)
- [ ] Delta batch write performance: measured
- [ ] Results documented in benchmarks.md with graphs/tables
- [ ] Performance regression detection (compare to baselines)

---

### WP-407: Load Testing and Scaling Validation

**Objective**: Validate horizontal scaling and system behavior under peak load
**Size**: Large
**Files to read**:
- `tests/kafka_pipeline/performance/` (from WP-406)
- `docs/kafka-greenfield-implementation-plan.md` (Section 2.3 scalability)
**Files to create/modify**:
- `tests/kafka_pipeline/performance/test_load_scaling.py`
- `tests/kafka_pipeline/performance/test_consumer_groups.py`
- `tests/kafka_pipeline/performance/test_peak_load.py`
- `tests/kafka_pipeline/performance/load_test_report.md`
**Dependencies**: WP-406
**Deliverable**: Load test suite validating scaling targets
**Review checklist**:
- [ ] Horizontal scaling: add consumers without code changes (NFR-3.1)
- [ ] Consumer instances: test with 1, 3, 6, 12 instances (target: 1-20, NFR-3.2)
- [ ] Partition distribution: validate even load distribution
- [ ] Peak load: 2x normal load (2,000 events/sec)
- [ ] Sustained load: 4 hour test at target throughput
- [ ] Failure recovery: kill worker, validate rebalancing
- [ ] Backpressure: validate scheduler pauses on high lag
- [ ] Resource limits: memory, CPU under sustained load
- [ ] Load test results documented in load_test_report.md

---

### WP-408: Grafana Dashboards

**Objective**: Create Grafana dashboards for monitoring pipeline health and performance
**Size**: Medium
**Files to read**:
- `src/kafka_pipeline/metrics.py` (all metrics defined)
- `docs/kafka-greenfield-implementation-plan.md` (Section 2.4 observability)
**Files to create/modify**:
- `observability/grafana/` (new directory)
- `observability/grafana/dashboards/kafka-pipeline-overview.json`
- `observability/grafana/dashboards/consumer-health.json`
- `observability/grafana/dashboards/download-performance.json`
- `observability/grafana/dashboards/dlq-monitoring.json`
- `observability/grafana/README.md` (dashboard documentation)
**Dependencies**: WP-208 (metrics implemented)
**Deliverable**: Production-ready Grafana dashboards for pipeline monitoring
**Review checklist**:
- [ ] **Pipeline Overview Dashboard**:
  - Total throughput (events/sec, downloads/sec)
  - End-to-end latency (p50, p95, p99)
  - Error rates by category
  - Circuit breaker states
  - Consumer lag by topic/partition
- [ ] **Consumer Health Dashboard**:
  - Consumer group lag (per topic/partition)
  - Partition assignments
  - Offset progress
  - Rebalance events
  - Connection status
- [ ] **Download Performance Dashboard**:
  - Download success/failure rates
  - Download latency distribution
  - Concurrent downloads gauge
  - OneLake upload performance
  - File size distribution
- [ ] **DLQ Monitoring Dashboard**:
  - DLQ message count (gauge)
  - DLQ message rate (counter)
  - Error distribution by category
  - Replay operations
  - DLQ age (oldest message timestamp)
- [ ] All dashboards use consistent time range selectors
- [ ] Variable filters: environment, consumer_group, topic
- [ ] Panel descriptions and documentation
- [ ] Export dashboards as JSON for version control

---

### WP-409: Alerting Configuration

**Objective**: Configure Prometheus alerting rules for critical metrics
**Size**: Small
**Files to read**:
- `src/kafka_pipeline/metrics.py` (metrics to alert on)
- `observability/grafana/dashboards/` (from WP-408)
- `docs/kafka-greenfield-implementation-plan.md` (Section 2.4.5 alerting requirements)
**Files to create/modify**:
- `observability/prometheus/` (new directory)
- `observability/prometheus/alerts/kafka-pipeline.yml` (Prometheus alert rules)
- `observability/prometheus/alerts/README.md` (alert documentation)
**Dependencies**: WP-408
**Deliverable**: Prometheus alert rules for critical pipeline conditions
**Review checklist**:
- [ ] **Consumer Lag Alert**: lag > 10,000 messages for 5 minutes
- [ ] **DLQ Growth Alert**: DLQ messages increasing rapidly (rate > 10/min)
- [ ] **Error Rate Alert**: error rate > 1% for 5 minutes
- [ ] **Circuit Breaker Open Alert**: circuit breaker open for > 2 minutes
- [ ] **Consumer Disconnected Alert**: consumer connection down
- [ ] **Download Failure Rate Alert**: download failures > 5% for 5 minutes
- [ ] **Delta Write Failure Alert**: Delta writes failing
- [ ] **High Latency Alert**: p95 latency > 10 seconds for 5 minutes
- [ ] Alert severity levels (critical, warning, info)
- [ ] Alert descriptions with context and remediation hints
- [ ] Runbook links in alert annotations
- [ ] Alert routing configuration (placeholder for Teams integration)

---

### WP-410: Runbook Documentation

**Objective**: Create operational runbooks for common scenarios and troubleshooting
**Size**: Medium
**Files to read**:
- `observability/prometheus/alerts/kafka-pipeline.yml` (from WP-409)
- `src/kafka_pipeline/dlq/cli.py` (DLQ operations)
- `docs/kafka-greenfield-implementation-plan.md` (architecture and design decisions)
**Files to create/modify**:
- `docs/runbooks/` (new directory)
- `docs/runbooks/README.md` (runbook index)
- `docs/runbooks/consumer-lag.md`
- `docs/runbooks/dlq-management.md`
- `docs/runbooks/circuit-breaker-open.md`
- `docs/runbooks/deployment-procedures.md`
- `docs/runbooks/incident-response.md`
- `docs/runbooks/scaling-operations.md`
**Dependencies**: WP-409
**Deliverable**: Complete operational runbooks for production support
**Review checklist**:
- [ ] **Consumer Lag Runbook**:
  - Symptoms and detection
  - Root cause analysis steps
  - Remediation: scale consumers, check for slow processing
  - Prevention: capacity planning
- [ ] **DLQ Management Runbook**:
  - DLQ message inspection workflow
  - Replay procedures with CLI commands
  - Common failure patterns and resolutions
  - Escalation criteria
- [ ] **Circuit Breaker Open Runbook**:
  - Symptoms and impacts
  - Diagnostic steps (check broker health, network, auth)
  - Recovery procedures
  - Prevention: proper timeout tuning
- [ ] **Deployment Procedures**:
  - Pre-deployment checklist
  - Rolling deployment steps
  - Rollback procedures
  - Post-deployment validation
- [ ] **Incident Response**:
  - Severity classification
  - Escalation paths
  - Communication templates
  - Post-incident review process
- [ ] **Scaling Operations**:
  - When to scale (metrics thresholds)
  - How to add/remove consumers
  - Partition rebalancing
  - Capacity planning guidelines
- [ ] All runbooks include:
  - Clear step-by-step procedures
  - CLI commands with examples
  - Expected outputs
  - Troubleshooting decision trees
  - Links to relevant dashboards and alerts

</details>

---

## Future Phases (Not Yet Detailed)

### Phase 5: Migration
- WP-501: Retry queue migration job
- WP-502: Parallel run validation
- WP-503: Cutover procedure

---

## Session Log

| Date | Session | Work Packages | Commits |
|------|---------|---------------|---------|
| 2024-12-25 | Initial planning | WP-001 to WP-004 | f468314, 92e4e9e, d4d5509, b48068e |
