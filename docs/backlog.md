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

- [ ] **WP-104**: Add Kafka OAUTHBEARER support to auth

### Ready
<!-- Tasks ready to be picked up -->
- [ ] **WP-105**: Extract circuit breaker to core/resilience
- [ ] **WP-106**: Extract retry decorator to core/resilience
- [ ] **WP-107**: Extract logging - base logger and formatters
- [ ] **WP-108**: Extract logging - context managers
- [ ] **WP-109**: Add Kafka context to logging
- [ ] **WP-110**: Extract error classification - exception hierarchy
- [ ] **WP-111**: Extract error classification - Azure error classifier
- [ ] **WP-112**: Add Kafka error classifier
- [ ] **WP-113**: Extract security - URL validation
- [ ] **WP-114**: Extract security - file type validation (new)
- [ ] **WP-115**: Extract security - presigned URL handling
- [ ] **WP-116**: Extract path resolution
- [ ] **WP-117**: Extract async download - core HTTP logic
- [ ] **WP-118**: Extract async download - streaming for large files
- [ ] **WP-119**: Create AttachmentDownloader class with clean interface

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

---

## Work Package Details

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

---

## Future Phases (Not Yet Detailed)

### Phase 2: Kafka Infrastructure
- WP-201: Kafka configuration and connection
- WP-202: Message schemas (Pydantic)
- WP-203: Base producer
- WP-204: Base consumer
- WP-205: Retry handler
- WP-206: Integration tests with Testcontainers

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
