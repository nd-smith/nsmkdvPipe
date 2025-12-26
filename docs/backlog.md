# Kafka Pipeline Backlog

> **How to use this file:**
> 1. At session start, read this file to understand current state
> 2. Pick a task from "Ready" and move to "In Progress"
> 3. Complete the work package (code + tests)
> 4. Move to "Completed" with commit hash
> 5. Commit backlog update alongside code changes
> 6. RULE: Do not include claude code, claude, anthropic references in the codebase or commit messages. 

---

## Current Sprint: Phase 1 - Foundation

### In Progress
<!-- Move tasks here when starting work -->

- [ ] **WP-206**: Implement BaseKafkaConsumer - core message loop

### Ready
<!-- Tasks ready to be picked up -->

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

## Future Phases (Not Yet Detailed)

### Phase 3: Workers
- WP-301: Event ingester worker
- WP-302: Download worker
- WP-303: Result processor
- WP-304: DLQ handler

### Phase 4: Integration
- WP-401: End-to-end tests
- WP-402: Observability setup
- WP-403: Performance testing

### Phase 5: Migration
- WP-501: Retry queue migration job
- WP-502: Parallel run validation
- WP-503: Cutover procedure

---

## Session Log

| Date | Session | Work Packages | Commits |
|------|---------|---------------|---------|
| 2024-12-25 | Initial planning | WP-001 to WP-004 | f468314, 92e4e9e, d4d5509, b48068e |
