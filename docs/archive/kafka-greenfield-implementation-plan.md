# Kafka Pipeline: Greenfield Implementation Plan

## Executive Summary

This document outlines the requirements and implementation plan for rebuilding the Verisk data pipeline as a Kafka-centric, event-driven system. The approach preserves ~5,500 lines of battle-tested business logic while replacing ~10,000 lines of polling-specific infrastructure with a modern streaming architecture.

**Project Goals:**
- Replace polling-based batch processing with real-time event streaming
- Enable horizontal scaling via Kafka consumer groups
- Reduce end-to-end latency from 60+ seconds to sub-second
- Simplify retry logic via Kafka's native patterns
- Maintain all existing business logic and security controls

**Timeline:** 8 weeks
**Team Size:** 1 Engineer, 1 Claude-Code 
**Risk Level:** Low - this application is pre-production.  Backwards compatability is not important.  While the current application is pre-prod, we have been running it and have built up sizeable databases.  Being able to preserve and re-use that data is the only thing I really care about. 

---

## 0. Development Constraints: Claude Code

> **Golden Rule:** This project is developed 100% with Claude Code. All work packages must be designed around context window constraints.

### 0.1 Context Window Limitations

Claude Code operates within a finite context window. Large files, complex refactors, or tasks requiring awareness of many files simultaneously can exceed these limits. Work packages must be:

- **Self-contained**: Each task completable without needing to hold the entire codebase in context
- **Focused**: One module, one concern, one deliverable per session
- **Well-documented**: Clear interfaces so dependent work doesn't require re-reading implementations

### 0.2 Work Package Sizing Guidelines

| Size | Lines of Code | Files | Context Required | Example |
|------|---------------|-------|------------------|---------|
| **Small** | < 200 | 1-2 | Minimal | Single class, utility function |
| **Medium** | 200-500 | 2-4 | Moderate | Module with tests |
| **Large** | 500-800 | 4-6 | Significant | Component with integration |
| **Too Large** | > 800 | > 6 | Exceeds limits | **Split into smaller tasks** |

### 0.3 Task Design Principles

1. **Atomic commits**: Each work package produces a working, tested increment
2. **Clear inputs/outputs**: Define interfaces before implementation
3. **Minimal cross-file edits**: Prefer new files over modifying many existing ones
4. **Tests alongside code**: Write tests in the same session as implementation
5. **Documentation as you go**: Docstrings and type hints reduce future context needs

### 0.4 Session Boundaries

Each Claude Code session should:
- Start with a clear, specific task from the backlog
- Read only the files necessary for that task
- Produce a commit with passing tests
- Update the task tracker before ending

### 0.5 Work Package Template

```markdown
## Task: [NAME]

**Objective**: [One sentence]
**Files to read**: [List - keep minimal]
**Files to create/modify**: [List]
**Dependencies**: [What must exist first]
**Deliverable**: [What "done" looks like]
**Estimated size**: [Small/Medium/Large]
```

### 0.6 Anti-Patterns to Avoid

| Anti-Pattern | Problem | Solution |
|--------------|---------|----------|
| "Refactor everything" | Exceeds context | Break into focused refactors |
| "Read the whole codebase" | Wastes context | Read only what's needed |
| "Fix all tests at once" | Too many files | Fix tests per-module |
| "Large file extraction" | Can't hold source + target | Extract in stages |
| "Simultaneous multi-file edit" | Context fragmentation | Sequential file edits |

---

## 1. Functional Requirements

### 1.1 Event Ingestion

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-1.1 | System SHALL consume events from source Kafka topic | P0 | New |
| ~~FR-1.2~~ | ~~System SHALL support consuming from Kusto as a fallback/bridge~~ | ~~P1~~ | **REMOVED** - Kafka is source of truth; fallback adds complexity without value |
| FR-1.3 | System SHALL deduplicate events by `trace_id` within a configurable window | P0 | Existing |
| FR-1.4 | System SHALL write ingested events to Delta Lake for analytics | P1 | Existing |
| FR-1.5 | System SHALL track consumer offsets for exactly-once processing | P0 | New |

### 1.2 Attachment Download

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-2.1 | System SHALL download attachments from presigned URLs | P0 | Existing |
| FR-2.2 | System SHALL validate URLs against domain allowlist (SSRF prevention) | P0 | Existing |
| FR-2.2.1 | System SHALL validate files against allowed file type list | P0 | New |
| FR-2.3 | System SHALL detect and handle expired presigned URLs via domain strategy | P0 | Existing |
| FR-2.4 | System SHALL stream large files (>50MB) to avoid memory exhaustion | P0 | Existing |
| FR-2.5 | System SHALL upload files to OneLake with deterministic paths | P0 | Existing |
| FR-2.6 | System SHALL process downloads concurrently (configurable parallelism) | P0 | Existing |
| FR-2.7 | System SHALL classify download failures as transient/permanent via domain strategy | P0 | Existing |
| FR-2.8 | System SHALL handle URL redirects per domain configuration | P0 | New |
| FR-2.9 | System SHALL have a decoupled file uploader service separate from data ops | P0 | New |
| FR-2.10 | System SHALL support file attachment encryption at rest | P0 | New |
| FR-2.11 | System SHALL interact with OneLake using Microsoft best practices | P0 | New |

### 1.3 Retry Handling

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-3.1 | System SHALL retry transient failures with exponential backoff | P0 | Existing |
| FR-3.2 | System SHALL support configurable retry delays (default: 5m, 10m, 20m, 40m) | P0 | Existing |
| FR-3.3 | System SHALL route exhausted retries to dead-letter queue | P0 | New |
| FR-3.4 | System SHALL preserve original error context through retry chain | P1 | New |
| FR-3.5 | System SHALL support manual replay from DLQ | P2 | New |

### 1.4 Inventory Tracking

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-4.1 | System SHALL record successful downloads in inventory table | P0 | Existing |
| FR-4.2 | System SHALL support idempotent writes (same event processed multiple times = same result) | P0 | Existing |
| FR-4.3 | System SHALL batch inventory writes for efficiency | P1 | New |

### 1.5 Multi-Domain Support

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-5.1 | System SHALL support XACT domain events | P0 | Existing |
| FR-5.2 | System SHALL support ClaimX domain events | P0 | Existing |
| FR-5.3 | System SHALL support the addition of new domains | P1 | New |
| FR-5.4 | System SHALL route events to domain-specific handlers based on message headers | P0 | New |

### 1.6 Domain Strategy Pattern

> Several requirements above reference "domain strategy" - this section defines the pattern.

Each domain (XACT, ClaimX, future domains) may have different behaviors. These are encapsulated in a **DomainStrategy** interface:

```python
class DomainStrategy(Protocol):
    """Domain-specific behavior configuration."""

    domain_name: str

    # URL handling
    def can_refresh_expired_url(self) -> bool: ...
    def refresh_url(self, expired_url: str, context: dict) -> Optional[str]: ...

    # Error classification
    def classify_error(self, error: Exception) -> ErrorCategory: ...
    def is_transient(self, status_code: int) -> bool: ...

    # Redirect handling
    def should_follow_redirect(self, original_url: str, redirect_url: str) -> bool: ...

    # File validation
    def allowed_file_types(self) -> List[str]: ...
    def validate_file(self, filename: str, content_type: str) -> bool: ...
```

**Current Domain Behaviors:**

| Behavior | XACT | ClaimX |
|----------|------|--------|
| Refresh expired URLs | No | Yes (via API) |
| Follow redirects | Configurable | Yes |
| Transient HTTP codes | 429, 500, 502, 503, 504 | 429, 500, 502, 503, 504, 401 |
| Allowed file types | PDF, XML, images | All |

---

## 2. Non-Functional Requirements

### 2.1 Performance

| ID | Requirement | Target | Current |
|----|-------------|--------|---------|
| NFR-1.1 | End-to-end latency (event → file available) | < 5 seconds (p95) | 60-120 seconds |
| NFR-1.2 | Throughput | 1,000 events/second | 100 events/cycle |
| NFR-1.3 | Download concurrency | 50 parallel downloads | 10 parallel |
| NFR-1.4 | Consumer lag recovery | < 10 minutes for 100k backlog | N/A |

### 2.2 Reliability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-2.1 | Message delivery guarantee | At-least-once (with idempotent processing) |
| NFR-2.2 | Data loss tolerance | Zero (DLQ captures all failures) |
| NFR-2.3 | Availability | 99.9% (excluding planned maintenance) |
| NFR-2.4 | Recovery time objective (RTO) | < 5 minutes |
| NFR-2.5 | Recovery point objective (RPO) | Zero (Kafka retains messages) |

### 2.25 Readability, coding style, best practices 
| NFR-2.25.1 | Limited comments in the codebase - reserved for high impact, succinct messages |
| NFR-2.25.2 | DRY violations is an acceptable sacrifice for more readable code |
| NFR-2.25.3 | Codebase should be designed for maximum readability |
| NFR-2.25.4 | Readability over clever or highly technical  | EX: one-liner" versus the explicit loop, Bitwise tricks, Nested ternaries, Regex golf, Chained boolean short-circuits, etc | Developer of average level skill should be able to read and maintain this application |




### 2.3 Scalability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-3.1 | Horizontal scaling | Add consumers without code changes |
| NFR-3.2 | Consumer instances | 1-20 per consumer group |
| NFR-3.3 | Partition count | 12 partitions (expandable) |
| NFR-3.4 | Message retention | 7 days (configurable) |

### 2.4 Observability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-4.1 | Structured logging | JSON with correlation IDs |
<note-NFR-4.1> It is imperative we get logging right. Logs need to contain all details needed to identify a problem.  Person reviewing/capturing logs may be non-technical.  Logs should be easy to parse and clearly tell the 'story'.  Research best practices. </note-NFR-4.1>
| NFR-4.2 | Metrics | Prometheus-compatible |
| NFR-4.3 | Consumer lag monitoring | Real-time dashboard |
| NFR-4.4 | Distributed tracing | OpenTelemetry compatible |
| NFR-4.5 | Alerting | PagerDuty/Slack integration | <note-NFR-4.5>This is deferred, and when implemented it will be for microsoft teams
</note-NFR-4.5>
| NFR-4.6 | WebUI for remote management |



### 2.5 Security

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-5.1 | Kafka authentication | SASL/OAUTHBEARER (Azure AD) | <note-NFR-5.1>What about SPN?</note-NFR-5.1>
| NFR-5.2 | Encryption in transit | TLS 1.2+ |
| NFR-5.3 | Secret management | Azure Key Vault |
| NFR-5.4 | URL validation | Domain allowlist (SSRF prevention) |
| NFR-5.4.5 | File type validation |
| NFR-5.5 | Sensitive data handling | No PII in logs |
| NFR-5.6 | No issues identified in the OWASP list may be present in codebase |

---

## 3. Architecture Overview

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KAFKA CLUSTER                                  │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌─────────────────┐  │
│  │ events.raw    │ │ downloads.    │ │ downloads.    │ │ downloads.dlq   │  │
│  │ (12 parts)    │ │ pending (12)  │ │ results (6)   │ │ (3 parts)       │  │
│  └───────┬───────┘ └───────┬───────┘ └───────┬───────┘ └────────┬────────┘  │
│          │                 │                 │                   │          │
└──────────┼─────────────────┼─────────────────┼───────────────────┼──────────┘
           │                 │                 │                   │
           ▼                 ▼                 ▼                   ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌─────────────┐
│  EVENT INGESTER  │ │ DOWNLOAD WORKER  │ │ RESULT PROCESSOR │ │ DLQ HANDLER │
│  (3 instances)   │ │ (6 instances)    │ │ (2 instances)    │ │ (1 instance)│
│                  │ │                  │ │                  │ │             │
│ ┌──────────────┐ │ │ ┌──────────────┐ │ │ ┌──────────────┐ │ │ Manual      │
│ │ Consume      │ │ │ │ Consume      │ │ │ │ Consume      │ │ │ review +    │
│ │ events.raw   │ │ │ │ downloads.   │ │ │ │ downloads.   │ │ │ replay      │
│ └──────┬───────┘ │ │ │ pending      │ │ │ │ results      │ │ │             │
│        │         │ │ └──────┬───────┘ │ │ └──────┬───────┘ │ └─────────────┘
│        ▼         │ │        │         │ │        │         │
│ ┌──────────────┐ │ │        ▼         │ │        ▼         │
│ │ Produce to   │ │ │ ┌──────────────┐ │ │ ┌──────────────┐ │
│ │ downloads.   │ │ │ │ Download     │ │ │ │ Batch write  │ │
│ │ pending      │ │ │ │ attachment   │ │ │ │ to Delta     │ │
│ └──────┬───────┘ │ │ └──────┬───────┘ │ │ │ (inventory)  │ │
│        │         │ │        │         │ │ └──────────────┘ │
│        ▼         │ │        ▼         │ │                  │
│ ┌──────────────┐ │ │ ┌──────────────┐ │ └──────────────────┘
│ │ Write to     │ │ │ │ Upload to    │ │
│ │ Delta        │ │ │ │ OneLake      │ │
│ │ (events)     │ │ │ └──────┬───────┘ │
│ └──────────────┘ │ │        │         │
└──────────────────┘ │        ▼         │
                     │ ┌──────────────┐ │
                     │ │ Produce to   │ │
                     │ │ downloads.   │ │
                     │ │ results      │ │
                     │ └──────────────┘ │
                     └──────────────────┘

                     RETRY FLOW (on failure):
                     ┌──────────────────┐
                     │ Produce to       │
                     │ downloads.retry  │──────▶ Delayed redelivery
                     │ (with headers)   │        to downloads.pending
                     └──────────────────┘
```

### 3.2 Topic Design

| Topic | Purpose | Partitions | Retention | Key | Value Schema |
|-------|---------|------------|-----------|-----|--------------|
| `xact.events.raw` | Raw events from source | 12 | 7 days | `trace_id` | `EventMessage` |
| `xact.downloads.pending` | Work queue for downloaders | 12 | 24 hours | `trace_id` | `DownloadTask` |
| `xact.downloads.results` | Download outcomes | 6 | 7 days | `trace_id` | `DownloadResult` |
| `xact.downloads.retry.5m` | Retry after 5 minutes | 6 | 24 hours | `trace_id` | `DownloadTask` |
| `xact.downloads.retry.10m` | Retry after 10 minutes | 6 | 24 hours | `trace_id` | `DownloadTask` |
| `xact.downloads.retry.20m` | Retry after 20 minutes | 6 | 24 hours | `trace_id` | `DownloadTask` |
| `xact.downloads.dlq` | Dead-letter queue | 3 | 30 days | `trace_id` | `FailedDownload` |

### 3.3 Consumer Groups

| Consumer Group | Topics | Instances | Processing |
|----------------|--------|-----------|------------|
| `xact-event-ingester` | `events.raw` | 3 | At-least-once |
| `xact-download-worker` | `downloads.pending`, `retry.*` | 6 | At-least-once |
| `xact-result-processor` | `downloads.results` | 2 | Exactly-once (txn) |
| `xact-dlq-handler` | `downloads.dlq` | 1 | Manual ack |

### 3.4 Data Compatibility & Migration

> **Constraint**: Preserve existing data. No unnecessary file I/O for restructuring.

#### 3.4.1 Existing Delta Tables (Reuse As-Is)

| Table | Current Schema | Kafka Pipeline Usage | Migration |
|-------|----------------|---------------------|-----------|
| `xact_events` | trace_id, event_type, timestamp, payload, ... | Continue writing ingested events | **None** - schema compatible |
| `xact_attachments` | trace_id, attachment_url, blob_path, ... | Continue writing inventory | **None** - schema compatible |
| `xact_retry` | trace_id, retry_count, next_retry_at, ... | **Deprecated** - replaced by Kafka retry topics | Read-only for migration |

#### 3.4.2 OneLake File Structure (Preserve)

```
Files/
├── xact/
│   ├── documentsReceived/{assignment_id}/{trace_id}/...
│   ├── estimatePackageReceived/{assignment_id}/{trace_id}/...
│   └── FNOL/{assignment_id}/{trace_id}/...
└── claimx/
    └── {claim_id}/{document_type}/...
```

**No changes to file paths.** The `generate_blob_path()` function remains unchanged.

#### 3.4.3 Migration Strategy

| Phase | Action | Risk |
|-------|--------|------|
| **Pre-cutover** | New pipeline writes to same tables | None - additive |
| **Cutover** | Stop legacy pipeline, Kafka takes over | Low - same schemas |
| **Post-cutover** | Drain `xact_retry` table via one-time job | Low - read-only |

#### 3.4.4 Retry Queue Migration

The legacy Delta-based retry queue (`xact_retry`) will be migrated to Kafka:

```python
# One-time migration job (run once at cutover)
async def migrate_retry_queue():
    pending = retry_queue_reader.get_pending_retries(max_retries=10)
    for row in pending.iter_rows(named=True):
        task = DownloadTaskMessage(
            trace_id=row["trace_id"],
            attachment_url=row["attachment_url"],
            retry_count=row["retry_count"],
            # ... map remaining fields
        )
        await producer.send("xact.downloads.pending", task.trace_id, task)
    # After validation, truncate xact_retry table
```

---

## 4. Component Specifications

### 4.1 Reusable Components (Extract from Existing)

These components will be extracted with minimal modification:

#### 4.1.1 Authentication (`common/auth.py`)
```
Extract: Full module (899 lines)
Changes: Add Kafka OAUTHBEARER callback
Tests: Existing tests remain valid
```

**Interface:**
```python
class AuthProvider:
    def get_kafka_oauth_token(self) -> str: ...
    def get_storage_options(self) -> Dict[str, str]: ...
    def get_azure_credential(self) -> TokenCredential: ...
```

#### 4.1.2 Circuit Breaker (`common/circuit_breaker.py`)
```
Extract: Full module (602 lines)
Changes: Add Kafka-specific presets
Tests: Existing tests remain valid
```

**New Presets:**
```python
KAFKA_PRODUCER_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=10,
    timeout_seconds=60.0,
)

KAFKA_CONSUMER_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    timeout_seconds=30.0,
)
```

#### 4.1.3 Retry Logic (`common/retry.py`)
```
Extract: Full module (~200 lines)
Changes: None
Tests: Existing tests remain valid
```

#### 4.1.4 Logging Infrastructure (`common/logging/`)
```
Extract: Full package (8 files, ~1,500 lines)
Changes: Add Kafka offset context fields
Tests: Existing tests remain valid
```

**New Context Fields:**
```python
@dataclass
class KafkaLogContext:
    topic: str
    partition: int
    offset: int
    consumer_group: str
```

#### 4.1.5 Error Classification (`common/exceptions.py`, `storage/errors.py`)
```
Extract: Both modules (~950 lines)
Changes: Add Kafka error classification
Tests: Extend with Kafka error cases
```

**New Classifications:**
```python
class KafkaErrorClassifier:
    @staticmethod
    def classify(error: Exception) -> ErrorCategory:
        if isinstance(error, KafkaTimeoutError):
            return ErrorCategory.TRANSIENT
        if isinstance(error, AuthenticationError):
            return ErrorCategory.AUTH
        # ...
```

#### 4.1.6 Security Validation (`common/security.py`, `common/url_expiration.py`)
```
Extract: Both modules (~434 lines)
Changes: None
Tests: Existing tests remain valid
```

#### 4.1.7 Path Resolution (`xact/services/path_resolver.py`)
```
Extract: Full module (~60 lines)
Changes: None
Tests: Existing tests remain valid
```

**Interface (unchanged):**
```python
def generate_blob_path(
    status_subtype: str,
    assignment_id: str,
    trace_id: str,
    attachment_url: str,
    file_type: Optional[str] = None,
) -> str: ...
```

#### 4.1.8 Async Download Core
```
Extract: From xact/stages/xact_download.py (lines 200-400)
Changes: Decouple from Delta reads, accept task as parameter
Tests: New unit tests for decoupled version
```

**New Interface:**
```python
@dataclass
class DownloadTask:
    trace_id: str
    attachment_url: str
    destination_path: str
    metadata: Dict[str, Any]

@dataclass
class DownloadOutcome:
    task: DownloadTask
    status: Literal["success", "failed_transient", "failed_permanent"]
    bytes_downloaded: Optional[int]
    error: Optional[str]
    error_category: Optional[ErrorCategory]

class AttachmentDownloader:
    async def download(self, task: DownloadTask) -> DownloadOutcome: ...
    async def download_batch(self, tasks: List[DownloadTask]) -> List[DownloadOutcome]: ...
```

### 4.2 New Components (Build Fresh)

#### 4.2.1 Kafka Configuration
```python
# kafka/config.py

@dataclass
class KafkaConfig:
    """Kafka connection and behavior configuration."""

    # Connection
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "OAUTHBEARER"

    # Consumer defaults
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 100
    max_poll_interval_ms: int = 300000  # 5 minutes
    session_timeout_ms: int = 30000

    # Producer defaults
    acks: str = "all"
    retries: int = 3
    retry_backoff_ms: int = 1000

    # Topics
    events_topic: str = "xact.events.raw"
    downloads_pending_topic: str = "xact.downloads.pending"
    downloads_results_topic: str = "xact.downloads.results"
    dlq_topic: str = "xact.downloads.dlq"

    # Retry configuration
    retry_delays: List[int] = field(default_factory=lambda: [300, 600, 1200, 2400])
    max_retries: int = 4

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Load configuration from environment variables."""
        ...
```

#### 4.2.2 Message Schemas
```python
# kafka/schemas/events.py

from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Dict, Any

class EventMessage(BaseModel):
    """Schema for raw event messages."""
    trace_id: str
    event_type: str
    event_subtype: str
    timestamp: datetime
    source_system: str
    payload: Dict[str, Any]
    attachments: Optional[List[str]] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class DownloadTaskMessage(BaseModel):
    """Schema for download work items."""
    trace_id: str
    attachment_url: str
    destination_path: str
    event_type: str
    event_subtype: str
    retry_count: int = 0
    original_timestamp: datetime
    metadata: Dict[str, Any] = {}

class DownloadResultMessage(BaseModel):
    """Schema for download outcomes."""
    trace_id: str
    attachment_url: str
    status: Literal["success", "failed_transient", "failed_permanent"]
    destination_path: Optional[str]
    bytes_downloaded: Optional[int]
    error_message: Optional[str]
    error_category: Optional[str]
    processing_time_ms: int
    completed_at: datetime
```

#### 4.2.3 Base Consumer
```python
# kafka/consumer.py

class BaseKafkaConsumer:
    """Base consumer with standard error handling and observability."""

    def __init__(
        self,
        config: KafkaConfig,
        topics: List[str],
        group_id: str,
        message_handler: Callable[[ConsumerRecord], Awaitable[None]],
    ):
        self.config = config
        self.topics = topics
        self.group_id = group_id
        self.message_handler = message_handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # Reused components
        self._circuit_breaker = get_circuit_breaker(
            f"consumer_{group_id}",
            KAFKA_CONSUMER_CIRCUIT_CONFIG
        )
        self._metrics = ConsumerMetrics(group_id)

    async def start(self) -> None:
        """Start consuming messages."""
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.config.auto_offset_reset,
            # ... security config
        )
        await self._consumer.start()
        self._running = True

        try:
            async for message in self._consumer:
                await self._process_message(message)
        finally:
            await self._consumer.stop()

    async def _process_message(self, message: ConsumerRecord) -> None:
        """Process single message with error handling."""
        with log_context(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
        ):
            try:
                await self._circuit_breaker.call_async(
                    lambda: self.message_handler(message)
                )
                await self._consumer.commit({
                    TopicPartition(message.topic, message.partition):
                    OffsetAndMetadata(message.offset + 1, None)
                })
                self._metrics.record_success()

            except Exception as e:
                self._metrics.record_failure()
                await self._handle_error(message, e)

    async def _handle_error(self, message: ConsumerRecord, error: Exception) -> None:
        """Route errors to appropriate handler."""
        category = classify_error(error)

        if category == ErrorCategory.TRANSIENT:
            await self._send_to_retry(message)
        elif category == ErrorCategory.PERMANENT:
            await self._send_to_dlq(message, error)
        else:
            # Auth errors, circuit open - don't commit, will reprocess
            raise
```

#### 4.2.4 Base Producer
```python
# kafka/producer.py

class BaseKafkaProducer:
    """Base producer with batching and error handling."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self._producer: Optional[AIOKafkaProducer] = None
        self._circuit_breaker = get_circuit_breaker(
            "producer",
            KAFKA_PRODUCER_CIRCUIT_CONFIG
        )

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            acks=self.config.acks,
            # ... security config
        )
        await self._producer.start()

    async def send(
        self,
        topic: str,
        key: str,
        value: BaseModel,
        headers: Optional[Dict[str, str]] = None,
    ) -> RecordMetadata:
        """Send single message."""
        return await self._circuit_breaker.call_async(
            lambda: self._producer.send_and_wait(
                topic,
                key=key.encode(),
                value=value.json().encode(),
                headers=[(k, v.encode()) for k, v in (headers or {}).items()],
            )
        )

    async def send_batch(
        self,
        topic: str,
        messages: List[Tuple[str, BaseModel]],
    ) -> List[RecordMetadata]:
        """Send batch of messages."""
        futures = []
        for key, value in messages:
            future = await self._producer.send(
                topic,
                key=key.encode(),
                value=value.json().encode(),
            )
            futures.append(future)

        await self._producer.flush()
        return [await f for f in futures]
```

#### 4.2.5 Retry Handler
```python
# kafka/retry/handler.py

class RetryHandler:
    """Handles retry logic via Kafka topics."""

    RETRY_TOPICS = [
        ("xact.downloads.retry.5m", 300),
        ("xact.downloads.retry.10m", 600),
        ("xact.downloads.retry.20m", 1200),
        ("xact.downloads.retry.40m", 2400),
    ]

    def __init__(self, producer: BaseKafkaProducer, dlq_topic: str):
        self.producer = producer
        self.dlq_topic = dlq_topic

    async def handle_failure(
        self,
        task: DownloadTaskMessage,
        error: Exception,
    ) -> None:
        """Route failed task to appropriate retry topic or DLQ."""
        retry_count = task.retry_count

        if retry_count >= len(self.RETRY_TOPICS):
            # Exhausted retries - send to DLQ
            await self._send_to_dlq(task, error)
            return

        # Send to next retry topic
        retry_topic, delay_seconds = self.RETRY_TOPICS[retry_count]

        task.retry_count += 1
        task.metadata["last_error"] = str(error)[:500]
        task.metadata["retry_at"] = (
            datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        ).isoformat()

        await self.producer.send(
            retry_topic,
            key=task.trace_id,
            value=task,
            headers={"x-retry-count": str(task.retry_count)},
        )

        logger.info(
            "Task scheduled for retry",
            trace_id=task.trace_id,
            retry_count=task.retry_count,
            retry_topic=retry_topic,
            delay_seconds=delay_seconds,
        )

    async def _send_to_dlq(
        self,
        task: DownloadTaskMessage,
        error: Exception,
    ) -> None:
        """Send exhausted task to dead-letter queue."""
        dlq_message = FailedDownloadMessage(
            trace_id=task.trace_id,
            attachment_url=task.attachment_url,
            original_task=task,
            final_error=str(error),
            error_category=classify_error(error).value,
            retry_count=task.retry_count,
            failed_at=datetime.now(timezone.utc),
        )

        await self.producer.send(
            self.dlq_topic,
            key=task.trace_id,
            value=dlq_message,
        )

        logger.error(
            "Task sent to DLQ after exhausting retries",
            trace_id=task.trace_id,
            retry_count=task.retry_count,
            error=str(error)[:200],
        )
```

#### 4.2.6 Event Ingester Worker
```python
# workers/event_ingester.py

class EventIngesterWorker:
    """Consumes raw events and produces download tasks."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer = BaseKafkaConsumer(
            config=config,
            topics=[config.events_topic],
            group_id="xact-event-ingester",
            message_handler=self._handle_event,
        )
        self.producer = BaseKafkaProducer(config)
        self.delta_writer = DeltaTableWriter(...)  # For analytics

        # Reused components
        self.path_resolver = generate_blob_path  # From existing
        self.url_validator = validate_download_url  # From existing

    async def _handle_event(self, message: ConsumerRecord) -> None:
        """Process single event message."""
        event = EventMessage.parse_raw(message.value)

        # Write to Delta for analytics (non-blocking)
        asyncio.create_task(self._write_to_delta(event))

        # Extract attachments and create download tasks
        if not event.attachments:
            return

        for attachment_url in event.attachments:
            # Validate URL (reused security logic)
            if not self.url_validator(attachment_url):
                logger.warning(
                    "Skipping invalid URL",
                    trace_id=event.trace_id,
                    url=sanitize_url(attachment_url),
                )
                continue

            # Generate destination path (reused logic)
            dest_path = self.path_resolver(
                status_subtype=event.event_subtype,
                assignment_id=event.payload.get("assignmentId", "unknown"),
                trace_id=event.trace_id,
                attachment_url=attachment_url,
            )

            # Create download task
            task = DownloadTaskMessage(
                trace_id=event.trace_id,
                attachment_url=attachment_url,
                destination_path=dest_path,
                event_type=event.event_type,
                event_subtype=event.event_subtype,
                original_timestamp=event.timestamp,
                metadata={"source_partition": message.partition},
            )

            await self.producer.send(
                self.config.downloads_pending_topic,
                key=event.trace_id,
                value=task,
            )
```

#### 4.2.7 Download Worker
```python
# workers/download_worker.py

class DownloadWorker:
    """Consumes download tasks and uploads to OneLake."""

    def __init__(self, config: KafkaConfig):
        self.config = config

        # Consumer for pending + all retry topics
        topics = [
            config.downloads_pending_topic,
            "xact.downloads.retry.5m",
            "xact.downloads.retry.10m",
            "xact.downloads.retry.20m",
            "xact.downloads.retry.40m",
        ]

        self.consumer = BaseKafkaConsumer(
            config=config,
            topics=topics,
            group_id="xact-download-worker",
            message_handler=self._handle_task,
        )

        self.result_producer = BaseKafkaProducer(config)
        self.retry_handler = RetryHandler(
            producer=self.result_producer,
            dlq_topic=config.dlq_topic,
        )

        # Reused components
        self.downloader = AttachmentDownloader(
            circuit_breaker=get_circuit_breaker("download", EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG),
            url_validator=validate_download_url,
            url_expiration_checker=check_presigned_url,
        )
        self.uploader = OneLakeClient(...)

    async def _handle_task(self, message: ConsumerRecord) -> None:
        """Process single download task."""
        task = DownloadTaskMessage.parse_raw(message.value)
        start_time = time.monotonic()

        try:
            # Download attachment (reused async logic)
            outcome = await self.downloader.download(
                DownloadTask(
                    trace_id=task.trace_id,
                    attachment_url=task.attachment_url,
                    destination_path=task.destination_path,
                    metadata=task.metadata,
                )
            )

            if outcome.status == "success":
                # Upload to OneLake
                await self.uploader.upload(
                    source=outcome.local_path,
                    destination=task.destination_path,
                )

                # Produce success result
                await self._send_result(task, outcome, start_time)
            else:
                # Handle failure
                await self._handle_failure(task, outcome)

        except Exception as e:
            await self.retry_handler.handle_failure(task, e)

    async def _send_result(
        self,
        task: DownloadTaskMessage,
        outcome: DownloadOutcome,
        start_time: float,
    ) -> None:
        """Send successful result to results topic."""
        result = DownloadResultMessage(
            trace_id=task.trace_id,
            attachment_url=task.attachment_url,
            status="success",
            destination_path=task.destination_path,
            bytes_downloaded=outcome.bytes_downloaded,
            processing_time_ms=int((time.monotonic() - start_time) * 1000),
            completed_at=datetime.now(timezone.utc),
        )

        await self.result_producer.send(
            self.config.downloads_results_topic,
            key=task.trace_id,
            value=result,
        )
```

#### 4.2.8 Result Processor
```python
# workers/result_processor.py

class ResultProcessor:
    """Consumes download results and writes to Delta inventory."""

    BATCH_SIZE = 100
    BATCH_TIMEOUT_SECONDS = 5

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer = BaseKafkaConsumer(
            config=config,
            topics=[config.downloads_results_topic],
            group_id="xact-result-processor",
            message_handler=self._handle_result,
        )

        # Batching for efficient Delta writes
        self._batch: List[DownloadResultMessage] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()

        # Delta writer (minimal coupling)
        self.inventory_writer = InventoryTableWriter(...)

    async def _handle_result(self, message: ConsumerRecord) -> None:
        """Add result to batch, flush if needed."""
        result = DownloadResultMessage.parse_raw(message.value)

        if result.status != "success":
            return  # Only write successful downloads to inventory

        async with self._batch_lock:
            self._batch.append(result)

            should_flush = (
                len(self._batch) >= self.BATCH_SIZE or
                time.monotonic() - self._last_flush > self.BATCH_TIMEOUT_SECONDS
            )

            if should_flush:
                await self._flush_batch()

    async def _flush_batch(self) -> None:
        """Write batch to Delta inventory table."""
        if not self._batch:
            return

        batch = self._batch
        self._batch = []
        self._last_flush = time.monotonic()

        # Convert to inventory records
        records = [
            {
                "trace_id": r.trace_id,
                "attachment_url": r.attachment_url,
                "blob_path": r.destination_path,
                "bytes_downloaded": r.bytes_downloaded,
                "downloaded_at": r.completed_at,
                "processing_time_ms": r.processing_time_ms,
            }
            for r in batch
        ]

        # Batch write with idempotency
        await asyncio.to_thread(
            self.inventory_writer.merge,
            pl.DataFrame(records),
            merge_keys=["trace_id", "attachment_url"],
        )

        logger.info(
            "Flushed inventory batch",
            batch_size=len(records),
        )
```

---

## 5. Implementation Phases

### Phase 1: Foundation (Week 1-2)

#### 1.1 Extract Reusable Components
| Task | Files | Effort |
|------|-------|--------|
| Create `verisk_pipeline.core` package | New | 2h |
| Extract auth module | `common/auth.py` → `core/auth.py` | 4h |
| Extract circuit breaker | `common/circuit_breaker.py` → `core/resilience.py` | 2h |
| Extract retry logic | `common/retry.py` → `core/retry.py` | 1h |
| Extract logging | `common/logging/` → `core/logging/` | 4h |
| Extract error classification | `common/exceptions.py`, `storage/errors.py` → `core/errors.py` | 4h |
| Extract security | `common/security.py`, `common/url_expiration.py` → `core/security.py` | 2h |
| Extract path resolution | `xact/services/path_resolver.py` → `core/paths.py` | 1h |
| Write tests for extracted components | New | 8h |

**Deliverable:** `verisk_pipeline.core` package with 100% test coverage

#### 1.2 Extract Download Logic
| Task | Files | Effort |
|------|-------|--------|
| Create `AttachmentDownloader` class | New | 8h |
| Decouple from Delta reads | Refactor from `xact_download.py` | 4h |
| Add async batch download | New | 4h |
| Write unit tests | New | 4h |

**Deliverable:** Standalone `AttachmentDownloader` with no infrastructure dependencies

### Phase 2: Kafka Infrastructure (Week 2-3)

#### 2.1 Kafka Package Setup
| Task | Effort |
|------|--------|
| Create `kafka/` package structure | 2h |
| Implement `KafkaConfig` with env loading | 4h |
| Implement message schemas (Pydantic) | 8h |
| Implement `BaseKafkaProducer` | 8h |
| Implement `BaseKafkaConsumer` | 12h |
| Add Kafka metrics (Prometheus) | 4h |
| Integration tests with Testcontainers | 8h |

**Deliverable:** Working Kafka producer/consumer with tests

#### 2.2 Retry Infrastructure
| Task | Effort |
|------|--------|
| Implement `RetryHandler` | 8h |
| Set up retry topics | 2h |
| Implement delayed redelivery (scheduler) | 8h |
| DLQ handler skeleton | 4h |
| Integration tests | 4h |

**Deliverable:** Working retry flow with DLQ

### Phase 3: Workers (Week 4-5)

#### 3.1 Event Ingester
| Task | Effort |
|------|--------|
| Implement `EventIngesterWorker` | 12h |
| Add Delta write for analytics | 4h |
| Deduplication logic | 4h |
| Unit tests | 4h |
| Integration tests | 4h |

**Deliverable:** Event ingester consuming from Kafka, producing download tasks

#### 3.2 Download Worker
| Task | Effort |
|------|--------|
| Implement `DownloadWorker` | 16h |
| Integrate reusable `AttachmentDownloader` | 4h |
| Add OneLake upload | 4h |
| Result production | 4h |
| Error handling + retry routing | 4h |
| Unit tests | 8h |
| Integration tests | 8h |

**Deliverable:** Download worker processing tasks end-to-end

#### 3.3 Result Processor
| Task | Effort |
|------|--------|
| Implement `ResultProcessor` | 8h |
| Batching logic | 4h |
| Delta inventory writes | 4h |
| Unit tests | 4h |
| Integration tests | 4h |

**Deliverable:** Result processor with batched Delta writes

### Phase 4: Integration (Week 6)

#### 4.1 End-to-End Testing
| Task | Effort |
|------|--------|
| Set up integration test environment | 8h |
| E2E test: event → download → inventory | 8h |
| E2E test: retry flow | 4h |
| E2E test: DLQ flow | 4h |
| Performance benchmarking | 8h |
| Load testing | 8h |

**Deliverable:** Validated end-to-end flow

#### 4.2 Observability
| Task | Effort |
|------|--------|
| Grafana dashboards | 8h |
| Consumer lag alerting | 4h |
| DLQ growth alerting | 2h |
| Error rate alerting | 2h |
| Runbook documentation | 4h |

**Deliverable:** Production-ready observability

### Phase 5: Migration (Week 7-8)

#### 5.1 Bridge Setup
| Task | Effort |
|------|--------|
| Kusto → Kafka bridge producer | 8h |
| Dual-write validation | 8h |
| Shadow mode testing | 16h |

**Deliverable:** Bridge publishing to Kafka alongside existing pipeline

#### 5.2 Cutover
| Task | Effort |
|------|--------|
| Canary deployment (10% traffic) | 4h |
| Monitor and validate | 8h |
| Gradual traffic increase | 8h |
| Full cutover | 4h |
| Legacy pipeline deprecation | 4h |

**Deliverable:** Production traffic on Kafka pipeline

---

## 6. Testing Strategy

### 6.1 Unit Tests

| Component | Test Focus | Coverage Target |
|-----------|------------|-----------------|
| Message schemas | Serialization/deserialization | 100% |
| Retry handler | Backoff calculation, routing | 100% |
| Error classification | All error types | 100% |
| Path resolution | All status subtypes | 100% |
| URL validation | Allow/deny cases | 100% |

### 6.2 Integration Tests

| Test Case | Components | Validation |
|-----------|------------|------------|
| Happy path | Ingester → Worker → Processor | Message flows through |
| Transient failure | Worker → Retry → Worker | Retry succeeds |
| Permanent failure | Worker → DLQ | DLQ contains message |
| Consumer restart | Consumer | Resumes from offset |
| Batch flush | Processor | Delta write on batch size |
| Timeout flush | Processor | Delta write on timeout |

### 6.3 Performance Tests

| Metric | Target | Test Method |
|--------|--------|-------------|
| Throughput | 1,000 msg/sec | Sustained load test |
| Latency (p50) | < 500ms | End-to-end timing |
| Latency (p99) | < 2s | End-to-end timing |
| Consumer lag recovery | 10k msg/min | Backlog test |
| Memory usage | < 512MB per worker | Load test monitoring |

---

## 7. Configuration Reference

### 7.1 Environment Variables

```bash
# Kafka Connection
KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9093
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=OAUTHBEARER

# Azure Authentication
AZURE_TENANT_ID=xxx
AZURE_CLIENT_ID=xxx
AZURE_CLIENT_SECRET=xxx  # Or use managed identity

# Topic Names
KAFKA_EVENTS_TOPIC=xact.events.raw
KAFKA_DOWNLOADS_PENDING_TOPIC=xact.downloads.pending
KAFKA_DOWNLOADS_RESULTS_TOPIC=xact.downloads.results
KAFKA_DLQ_TOPIC=xact.downloads.dlq

# Consumer Configuration
KAFKA_CONSUMER_GROUP_PREFIX=xact
KAFKA_MAX_POLL_RECORDS=100
KAFKA_SESSION_TIMEOUT_MS=30000

# Worker Configuration
DOWNLOAD_CONCURRENCY=20
DOWNLOAD_TIMEOUT_SECONDS=300
BATCH_SIZE=100
BATCH_TIMEOUT_SECONDS=5

# Storage
ONELAKE_FILES_PATH=abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files
DELTA_EVENTS_TABLE=abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_events
DELTA_INVENTORY_TABLE=abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_attachments

# Retry Configuration
RETRY_DELAYS=300,600,1200,2400
MAX_RETRIES=4

# Observability
LOG_LEVEL=INFO
LOG_FORMAT=json
METRICS_PORT=9090
```

### 7.2 Kubernetes Deployment

```yaml
# download-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: xact-download-worker
spec:
  replicas: 6
  selector:
    matchLabels:
      app: xact-download-worker
  template:
    metadata:
      labels:
        app: xact-download-worker
    spec:
      containers:
      - name: worker
        image: verisk-pipeline:kafka-v1
        command: ["python", "-m", "verisk_pipeline.workers.download_worker"]
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "1000m"
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            secretKeyRef:
              name: kafka-secrets
              key: bootstrap-servers
        # ... other env vars
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

---

## 8. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Kafka cluster unavailability | Low | High | Multi-AZ deployment, circuit breaker fallback |
| Consumer lag growth | Medium | Medium | Auto-scaling, alerting, capacity planning |
| Schema evolution issues | Medium | Medium | Schema registry, backward compatibility tests |
| Retry storm | Low | High | Rate limiting, backoff jitter |
| Data loss during migration | Low | Critical | Dual-write validation, rollback capability |
| Performance regression | Medium | Medium | Benchmarking, gradual traffic shift |

---

## 9. Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| End-to-end latency | < 5s (p95) | Datadog APM |
| Throughput | 1,000 events/sec sustained | Kafka metrics |
| Error rate | < 0.1% | DLQ message rate |
| Consumer lag | < 1,000 messages | Kafka consumer lag |
| Availability | 99.9% | Uptime monitoring |
| Test coverage | > 80% | pytest-cov |

---

## 10. Appendix

### 10.1 Glossary

| Term | Definition |
|------|------------|
| DLQ | Dead-letter queue - topic for messages that exhaust retries |
| Consumer lag | Difference between latest offset and committed offset |
| At-least-once | Message may be processed multiple times, never lost |
| Exactly-once | Message processed exactly once (requires transactions) |
| Backoff | Increasing delay between retry attempts |

### 10.2 References

- Existing codebase analysis: `docs/kafka-refactor-plan.md`
- Kafka documentation: https://kafka.apache.org/documentation/
- aiokafka: https://aiokafka.readthedocs.io/
- Delta Lake: https://delta.io/

---

