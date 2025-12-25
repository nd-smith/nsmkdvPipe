# Kafka Refactor Plan for Verisk Pipeline

## Executive Summary

This document outlines a plan to refactor the current polling-based Delta/Kusto architecture to a Kafka-centric event-driven system. The refactor aims to improve real-time processing capabilities, reduce latency, and establish a more scalable foundation for future growth.

---

## 1. Current Architecture Analysis

### 1.1 Data Sources and Sinks

| Component | Type | Current Technology | Purpose |
|-----------|------|-------------------|---------|
| Event Source | Source | Azure Kusto/Eventhouse | Raw event ingestion via KQL queries |
| Events Table | Storage | Delta Lake (`xact_events`) | Staged events for processing |
| Inventory Table | Storage | Delta Lake (`xact_attachments`) | Successfully processed attachments |
| Retry Queue | Storage | Delta Lake (`xact_retry`) | Failed items with exponential backoff |
| File Storage | Storage | OneLake (ADLS Gen2) | Binary attachment files |

### 1.2 Current Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CURRENT ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────┘

                    POLLING INTERVAL (10-60s)
                           │
    ┌──────────────────────┼──────────────────────┐
    │                      ▼                      │
    │    ┌────────────────────────────────┐       │
    │    │       INGEST STAGE             │       │
    │    │  ┌──────────┐   ┌───────────┐  │       │
    │    │  │  Kusto   │──▶│  Delta    │  │       │
    │    │  │ (Source) │   │(xact_events) │       │
    │    │  └──────────┘   └───────────┘  │       │
    │    └────────────────────────────────┘       │
    │                      │                      │
    │                      ▼                      │
    │    ┌────────────────────────────────┐       │
    │    │       DOWNLOAD STAGE           │       │
    │    │  ┌───────────┐   ┌──────────┐  │       │
    │    │  │  Delta    │──▶│ OneLake  │  │       │
    │    │  │(xact_events)  │ (Files)  │  │       │
    │    │  └───────────┘   └──────────┘  │       │
    │    │         │              │       │       │
    │    │         ▼              ▼       │       │
    │    │  ┌───────────┐  ┌───────────┐  │       │
    │    │  │  Retry    │  │ Inventory │  │       │
    │    │  │  Queue    │  │  Table    │  │       │
    │    │  └───────────┘  └───────────┘  │       │
    │    └────────────────────────────────┘       │
    │                      │                      │
    │                      ▼                      │
    │    ┌────────────────────────────────┐       │
    │    │       RETRY STAGE              │       │
    │    │  ┌───────────┐   ┌──────────┐  │       │
    │    │  │  Retry    │──▶│ OneLake  │  │       │
    │    │  │  Queue    │   │ (Files)  │  │       │
    │    │  └───────────┘   └──────────┘  │       │
    │    └────────────────────────────────┘       │
    │                      │                      │
    └──────────────────────┴──────────────────────┘
                     NEXT CYCLE
```

### 1.3 Key Components Analysis

#### Kusto Reader (`storage/kusto.py`)
- **Line 49-659**: `KustoReader` class with circuit breaker, retry, and caching
- **Authentication**: Token file, CLI, SPN, DefaultAzureCredential
- **Query execution**: KQL with configurable timeouts (5 min default)
- **Resilience**: Circuit breaker (5 failures, 30s timeout), exponential backoff

#### Delta Lake (`storage/delta.py`)
- **Line 60-268**: `DeltaTableReader` with Polars integration
- **Line 270-1081**: `DeltaTableWriter` with merge, append, idempotency
- **Line 1083-1257**: `EventsTableReader` with partition pruning
- **Features**: Z-ordering, deduplication, batched merges

#### Retry Queue (`storage/retry_queue_writer.py`)
- **Line 62-612**: `RetryQueueWriter` with MERGE/DELETE lifecycle
- **Backoff**: Exponential with configurable base (300s) and multiplier (2.0)
- **Retention**: Automatic cleanup after configurable days

#### Pipeline Orchestration (`xact/xact_pipeline.py`)
- **Line 50-650**: Main `Pipeline` class with stage coordination
- **Scheduling**: Configurable interval (default 10-60s)
- **Health**: REST endpoint with circuit breaker status

### 1.4 Current Limitations

| Limitation | Description | Impact |
|------------|-------------|--------|
| **Polling latency** | 10-60s interval delays | Not suitable for real-time use cases |
| **Batch coupling** | Stages tightly coupled via Delta tables | Scaling requires larger instances |
| **Retry inefficiency** | Delta-based retry queue adds write latency | Delays before retry attempts |
| **No event replay** | Kusto has limited retention | Lost events if pipeline down |
| **Single consumer** | One pipeline instance per stage | Limited horizontal scaling |

---

## 2. Proposed Kafka-Centric Architecture

### 2.1 Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    KAFKA-CENTRIC ARCHITECTURE                            │
└─────────────────────────────────────────────────────────────────────────┘

    ┌───────────────────────────────────────────────────────────────────┐
    │                        KAFKA CLUSTER                               │
    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────┐ │
    │  │ xact.events  │ │xact.downloads│ │ xact.results │ │xact.dlq   │ │
    │  │ (raw events) │ │ (work items) │ │  (outcomes)  │ │(failures) │ │
    │  └──────────────┘ └──────────────┘ └──────────────┘ └───────────┘ │
    └───────────────────────────────────────────────────────────────────┘
           │                    │                 │              │
           ▼                    ▼                 ▼              ▼
    ┌─────────────┐      ┌─────────────┐   ┌─────────────┐ ┌───────────┐
    │   EVENT     │      │  DOWNLOAD   │   │   RESULT    │ │   DLQ     │
    │  INGESTER   │──────│   WORKER    │───│  PROCESSOR  │ │  HANDLER  │
    │  (Consumer) │      │  (Consumer) │   │  (Consumer) │ │ (Consumer)│
    └─────────────┘      └─────────────┘   └─────────────┘ └───────────┘
           │                    │                 │              │
           ▼                    ▼                 ▼              ▼
    ┌─────────────┐      ┌─────────────┐   ┌─────────────┐ ┌───────────┐
    │   Delta     │      │   OneLake   │   │   Delta     │ │  Manual   │
    │ (xact_events)      │  (Files)    │   │ (inventory) │ │  Review   │
    └─────────────┘      └─────────────┘   └─────────────┘ └───────────┘

    ┌───────────────────────────────────────────────────────────────────┐
    │                    EXTERNAL INTEGRATIONS                           │
    │  ┌──────────────┐                           ┌──────────────────┐  │
    │  │ Kusto/Event  │──(Kafka Connect)────────▶│ xact.events      │  │
    │  │ Hub Source   │                           │ (Kafka Topic)    │  │
    │  └──────────────┘                           └──────────────────┘  │
    └───────────────────────────────────────────────────────────────────┘
```

### 2.2 Kafka Topics Design

| Topic | Purpose | Partitions | Retention | Key |
|-------|---------|------------|-----------|-----|
| `xact.events.raw` | Raw events from source | 6 | 7 days | `trace_id` |
| `xact.downloads.pending` | Work items for download workers | 12 | 24 hours | `trace_id` |
| `xact.downloads.results` | Download outcomes (success/failure) | 6 | 7 days | `trace_id` |
| `xact.downloads.dlq` | Dead-letter queue for exhausted retries | 3 | 30 days | `trace_id` |
| `xact.downloads.retry` | Retry topic with delay headers | 6 | 7 days | `trace_id` |

### 2.3 Consumer Group Strategy

```python
# Proposed consumer group configuration
CONSUMER_GROUPS = {
    "xact-event-ingester": {
        "topics": ["xact.events.raw"],
        "instances": 3,
        "processing": "at-least-once",
    },
    "xact-download-worker": {
        "topics": ["xact.downloads.pending", "xact.downloads.retry"],
        "instances": 6,  # Scale based on download throughput
        "processing": "at-least-once",
    },
    "xact-result-processor": {
        "topics": ["xact.downloads.results"],
        "instances": 2,
        "processing": "exactly-once",  # Idempotent writes to Delta
    },
    "xact-dlq-handler": {
        "topics": ["xact.downloads.dlq"],
        "instances": 1,
        "processing": "manual-ack",
    },
}
```

---

## 3. Migration Strategy

### 3.1 Phase 1: Kafka Infrastructure Setup

**Goal**: Establish Kafka cluster and topic infrastructure without disrupting current pipeline.

**Tasks**:
1. Provision Kafka cluster (Azure Event Hubs with Kafka protocol or Confluent Cloud)
2. Create topics with appropriate partitioning and retention
3. Set up schema registry for message schemas (Avro/Protobuf)
4. Configure authentication (SASL/OAuth)
5. Establish monitoring (Kafka lag, throughput metrics)

**Files to Create**:
```
src/verisk_pipeline/kafka/
├── __init__.py
├── config.py           # Kafka connection configuration
├── schemas/
│   ├── __init__.py
│   ├── event_schema.py # Avro/Pydantic schemas
│   └── result_schema.py
├── producer.py         # Generic Kafka producer
├── consumer.py         # Generic Kafka consumer with retry
└── serialization.py    # Avro/JSON serialization
```

### 3.2 Phase 2: Parallel Event Ingestion

**Goal**: Run Kafka ingestion alongside existing Kusto polling.

**Changes**:

| Current File | Changes |
|--------------|---------|
| `xact/stages/xact_ingest.py` | Add Kafka producer after Delta write |
| `storage/kusto.py` | No changes (still reads from Kusto) |

**New File**: `kafka/producers/event_producer.py`
```python
# Pseudocode for event producer
class EventProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.producer = KafkaProducer(...)

    def publish_events(self, events_df: pl.DataFrame) -> int:
        """Publish events to Kafka topic after Delta write."""
        for row in events_df.iter_rows(named=True):
            key = row["trace_id"].encode()
            value = serialize_event(row)
            self.producer.send(self.topic, key=key, value=value)
        self.producer.flush()
        return len(events_df)
```

**Validation**: Compare Kafka message count with Delta row count.

### 3.3 Phase 3: Download Worker Kafka Consumer

**Goal**: Replace Delta polling in download stage with Kafka consumer.

**Current Flow** (`xact/stages/xact_download.py:140-200`):
```python
# Current: Poll Delta table
items_df = self._query_events(wm_session.start_watermark)
tasks = self._build_tasks(items_df, wm_session)
results = await self._download_batch(tasks)
```

**New Flow**:
```python
# Proposed: Consume from Kafka
class DownloadWorker:
    def __init__(self, config: KafkaConfig):
        self.consumer = KafkaConsumer(
            "xact.downloads.pending",
            group_id="xact-download-worker",
            ...
        )
        self.result_producer = KafkaProducer(...)

    async def process_message(self, message: ConsumerRecord) -> None:
        event = deserialize_event(message.value)
        result = await self._download_attachment(event)

        # Publish result to results topic
        self.result_producer.send(
            "xact.downloads.results",
            key=message.key,
            value=serialize_result(result),
        )
```

### 3.4 Phase 4: Kafka-Based Retry with Delayed Redelivery

**Goal**: Replace Delta-based retry queue with Kafka retry topics.

**Current Flow** (`storage/retry_queue_writer.py`):
- Delta MERGE to add failed items
- Polling with `next_retry_at` filter
- Exponential backoff via timestamp calculation

**Proposed Kafka Retry Pattern**:

```python
# Option 1: Kafka Headers with Delay (requires Kafka Streams or external scheduler)
class RetryHandler:
    RETRY_DELAYS = [300, 600, 1200]  # 5min, 10min, 20min

    def handle_failure(self, message: ConsumerRecord, error: Exception):
        retry_count = int(message.headers.get("x-retry-count", 0))

        if retry_count >= len(self.RETRY_DELAYS):
            # Send to DLQ
            self.dlq_producer.send("xact.downloads.dlq", ...)
            return

        # Send to retry topic with delay header
        delay_ms = self.RETRY_DELAYS[retry_count] * 1000
        headers = {
            "x-retry-count": str(retry_count + 1),
            "x-retry-delay-ms": str(delay_ms),
            "x-original-error": error.message[:500],
        }
        self.retry_producer.send(
            "xact.downloads.retry",
            key=message.key,
            value=message.value,
            headers=headers,
        )
```

**Option 2: Time-Partitioned Retry Topics**
```
xact.downloads.retry.5min   # Consumed after 5min
xact.downloads.retry.10min  # Consumed after 10min
xact.downloads.retry.20min  # Consumed after 20min
```

### 3.5 Phase 5: Result Processing and Delta Sink

**Goal**: Consolidate Delta writes to a dedicated result processor.

**Benefits**:
- Batched writes for better Delta performance
- Exactly-once semantics via Kafka transactions
- Clear separation of concerns

**New Component**: `kafka/consumers/result_processor.py`
```python
class ResultProcessor:
    def __init__(self, config: KafkaConfig):
        self.consumer = KafkaConsumer(
            "xact.downloads.results",
            group_id="xact-result-processor",
            enable_auto_commit=False,
        )
        self.inventory_writer = InventoryTableWriter(...)

    async def process_batch(self) -> None:
        messages = self.consumer.poll(timeout_ms=1000, max_records=100)

        successes = []
        for msg in messages:
            result = deserialize_result(msg.value)
            if result.status == "success":
                successes.append(result.to_inventory_row())

        if successes:
            # Batch write to Delta
            self.inventory_writer.merge(pl.DataFrame(successes), ...)

        # Commit offsets after successful write
        self.consumer.commit()
```

---

## 4. Component Mapping

### 4.1 Current to Kafka Component Mapping

| Current Component | Current Location | Kafka Replacement |
|-------------------|------------------|-------------------|
| `KustoReader` | `storage/kusto.py` | Keep for initial ingestion OR replace with Kafka Connect |
| `EventsTableReader` | `storage/delta.py:1083` | Kafka Consumer (`xact.events.raw`) |
| `DeltaTableWriter.append()` | `storage/delta.py:306` | Kafka Producer → Result Processor |
| `RetryQueueWriter` | `storage/retry_queue_writer.py` | Kafka Retry Topic + DLQ |
| `InventoryTableWriter` | `storage/inventory_writer.py` | Result Processor (batched writes) |
| `WatermarkManager` | `storage/watermark.py` | Kafka Consumer Offsets |
| `UploadService` | `storage/upload_service.py` | Keep (async upload unchanged) |
| `Pipeline.run_cycle()` | `xact/xact_pipeline.py:289` | Kafka Consumer Event Loop |

### 4.2 Preserved Components

These components remain largely unchanged:

| Component | Reason |
|-----------|--------|
| `OneLakeClient` | File upload logic unchanged |
| `UploadService` | Async upload pattern still valid |
| `CircuitBreaker` | Resilience pattern applies to external calls |
| `DeltaTableWriter.merge()` | Used by result processor for batched writes |
| Error classification | `storage/errors.py` still useful |

---

## 5. New File Structure

```
src/verisk_pipeline/
├── kafka/
│   ├── __init__.py
│   ├── config.py                    # KafkaConfig dataclass
│   ├── connection.py                # Connection factory with retries
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── event.py                 # Event message schema
│   │   ├── download_task.py         # Download work item schema
│   │   ├── download_result.py       # Download outcome schema
│   │   └── registry.py              # Schema registry client
│   │
│   ├── producers/
│   │   ├── __init__.py
│   │   ├── base.py                  # BaseProducer with retry
│   │   ├── event_producer.py        # Publishes to xact.events.raw
│   │   └── result_producer.py       # Publishes download results
│   │
│   ├── consumers/
│   │   ├── __init__.py
│   │   ├── base.py                  # BaseConsumer with error handling
│   │   ├── event_consumer.py        # Consumes xact.events.raw
│   │   ├── download_consumer.py     # Consumes pending downloads
│   │   ├── result_consumer.py       # Consumes results for Delta write
│   │   └── dlq_consumer.py          # Dead-letter queue handler
│   │
│   ├── retry/
│   │   ├── __init__.py
│   │   ├── handler.py               # Retry logic with backoff
│   │   └── dlq.py                   # Dead-letter queue processing
│   │
│   └── workers/
│       ├── __init__.py
│       ├── event_ingester.py        # Kusto → Kafka bridge
│       ├── download_worker.py       # Main download worker
│       └── result_processor.py      # Kafka → Delta sink
│
├── xact/
│   ├── kafka_pipeline.py            # New Kafka-based orchestration
│   └── stages/
│       ├── xact_ingest.py           # Modified: add Kafka publish
│       ├── xact_download.py         # Deprecated: replaced by worker
│       └── xact_retry_stage.py      # Deprecated: replaced by retry handler
```

---

## 6. Configuration Changes

### 6.1 New Kafka Configuration

**File**: `common/config/kafka.py`

```python
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class KafkaConfig:
    """Kafka connection and topic configuration."""

    # Connection
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "OAUTHBEARER"

    # Topics
    events_topic: str = "xact.events.raw"
    downloads_topic: str = "xact.downloads.pending"
    results_topic: str = "xact.downloads.results"
    retry_topic: str = "xact.downloads.retry"
    dlq_topic: str = "xact.downloads.dlq"

    # Consumer settings
    consumer_group_prefix: str = "xact"
    auto_offset_reset: str = "earliest"
    max_poll_records: int = 100
    session_timeout_ms: int = 30000

    # Producer settings
    acks: str = "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 100

    # Retry configuration
    retry_delays_seconds: List[int] = field(
        default_factory=lambda: [300, 600, 1200, 2400]
    )
    max_retries: int = 4
```

### 6.2 Environment Variables

```bash
# Kafka Connection
KAFKA_BOOTSTRAP_SERVERS=your-kafka-cluster:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=OAUTHBEARER

# Or for Azure Event Hubs
EVENTHUBS_CONNECTION_STRING=Endpoint=sb://...
EVENTHUBS_NAMESPACE=your-namespace.servicebus.windows.net

# Schema Registry (if using Avro)
SCHEMA_REGISTRY_URL=https://your-registry
SCHEMA_REGISTRY_AUTH=...

# Topic Names (optional overrides)
KAFKA_EVENTS_TOPIC=xact.events.raw
KAFKA_DOWNLOADS_TOPIC=xact.downloads.pending
```

---

## 7. Rollback Strategy

### 7.1 Feature Flags

```python
@dataclass
class FeatureFlags:
    """Feature flags for gradual Kafka rollout."""

    # Phase 1: Dual-write to Kafka
    kafka_publish_events: bool = False

    # Phase 2: Kafka consumer for downloads
    kafka_download_consumer: bool = False

    # Phase 3: Kafka retry instead of Delta
    kafka_retry_enabled: bool = False

    # Phase 4: Full Kafka mode (disable Delta polling)
    kafka_only_mode: bool = False
```

### 7.2 Rollback Procedures

| Scenario | Rollback Action |
|----------|-----------------|
| Kafka producer failures | Disable `kafka_publish_events`, pipeline continues with Delta only |
| Consumer lag too high | Scale consumer instances, or fallback to Delta polling |
| Retry topic issues | Enable `delta_retry_fallback`, use Delta retry queue |
| Full rollback | Set `kafka_only_mode=False`, all flags to False |

---

## 8. Monitoring and Observability

### 8.1 New Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_messages_produced_total` | Counter | Messages published to Kafka |
| `kafka_messages_consumed_total` | Counter | Messages consumed from Kafka |
| `kafka_consumer_lag` | Gauge | Consumer lag per partition |
| `kafka_producer_latency_ms` | Histogram | Time to publish message |
| `kafka_consumer_processing_time_ms` | Histogram | Time to process message |
| `kafka_dlq_messages_total` | Counter | Messages sent to DLQ |
| `kafka_retry_messages_total` | Counter | Messages sent to retry topic |

### 8.2 Alerting Rules

```yaml
# Prometheus alerting rules
groups:
  - name: kafka-alerts
    rules:
      - alert: KafkaConsumerLagHigh
        expr: kafka_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka consumer lag is high"

      - alert: KafkaDLQGrowing
        expr: rate(kafka_dlq_messages_total[5m]) > 1
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Messages accumulating in DLQ"
```

---

## 9. Testing Strategy

### 9.1 Unit Tests

- Mock Kafka producers/consumers
- Test serialization/deserialization
- Test retry logic with various failure scenarios
- Test offset commit behavior

### 9.2 Integration Tests

- Use Testcontainers for local Kafka
- End-to-end flow: Kusto → Kafka → Download → Delta
- Verify exactly-once semantics
- Test consumer rebalancing

### 9.3 Performance Tests

- Measure throughput under load
- Verify latency improvements over polling
- Test with realistic message sizes
- Validate partition scaling

---

## 10. Implementation Timeline

### Phase 1: Infrastructure (Weeks 1-2)
- [ ] Provision Kafka cluster
- [ ] Create topics and schemas
- [ ] Implement base producer/consumer
- [ ] Set up monitoring

### Phase 2: Dual-Write (Weeks 3-4)
- [ ] Add Kafka producer to ingest stage
- [ ] Validate message parity
- [ ] Monitor producer performance

### Phase 3: Download Consumer (Weeks 5-7)
- [ ] Implement download worker
- [ ] Parallel testing with Delta polling
- [ ] Gradual traffic shift

### Phase 4: Kafka Retry (Weeks 8-9)
- [ ] Implement retry topic pattern
- [ ] Migrate from Delta retry queue
- [ ] Validate retry behavior

### Phase 5: Full Migration (Week 10+)
- [ ] Disable Delta polling
- [ ] Monitor and stabilize
- [ ] Clean up deprecated code

---

## 11. Open Questions

1. **Kafka Provider**: Azure Event Hubs (Kafka-compatible) vs. Confluent Cloud vs. self-managed?
2. **Schema Registry**: Required for Avro? Or use JSON with Pydantic validation?
3. **Exactly-Once**: Required for result processing? Or acceptable with idempotent Delta writes?
4. **Retry Delay Implementation**: Kafka Streams, external scheduler, or time-partitioned topics?
5. **Multi-tenant**: Should topics be per-tenant or use message headers for routing?

---

## 12. References

- Current Kusto reader: `src/verisk_pipeline/storage/kusto.py:49-659`
- Current Delta writer: `src/verisk_pipeline/storage/delta.py:270-1081`
- Current retry queue: `src/verisk_pipeline/storage/retry_queue_writer.py:62-612`
- Current pipeline: `src/verisk_pipeline/xact/xact_pipeline.py:50-650`
- Download stage: `src/verisk_pipeline/xact/stages/xact_download.py:64-200`
