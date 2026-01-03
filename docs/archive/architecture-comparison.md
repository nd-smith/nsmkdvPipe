# Pipeline Architecture Comparison

## Old Architecture (Polling-Based)

```mermaid
flowchart TB
    subgraph Source["Data Source"]
        Kusto[(Kusto/ADX)]
    end

    subgraph Pipeline["Pipeline - Single Process"]
        Poll[/"Poll every 60s"/]
        Download["Download Stage"]
        RetryTable[(Delta Retry Table)]
        Inventory[(Delta Inventory)]
    end

    subgraph Storage["Storage"]
        OneLake[(OneLake Files)]
    end

    Kusto -->|"Batch Query"| Poll
    Poll -->|"Process Events"| Download
    Download -->|"On Failure"| RetryTable
    RetryTable -->|"Re-poll"| Poll
    Download -->|"Upload"| OneLake
    Download -->|"Record"| Inventory
```

**Characteristics:**
- Polling interval: 60 seconds
- Single process architecture
- Delta Lake table for retry queue
- End-to-end latency: 60-120 seconds
- Max 10 parallel downloads

---

## New Architecture (Kafka-Based, Decoupled Download/Upload)

```mermaid
flowchart TB
    subgraph Azure["Azure Cloud"]
        EventHub[("Event Hub\nevents.raw")]
        OneLake[(OneLake Files)]
        DeltaEvents[(Delta Events)]
        DeltaInventory[(Delta Inventory)]
    end

    subgraph Local["Local Kafka - Docker"]
        Pending[["downloads.pending"]]
        Cached[["downloads.cached"]]
        Results[["downloads.results"]]
        Retry5[["retry.5m"]]
        Retry10[["retry.10m"]]
        Retry20[["retry.20m"]]
        Retry40[["retry.40m"]]
        DLQ[["downloads.dlq"]]
    end

    subgraph LocalStorage["Local Storage"]
        Cache[("Local Cache\n/tmp/cache")]
    end

    subgraph Workers["Workers - Horizontally Scalable"]
        Ingester["Event Ingester\nx3 instances"]
        DownloadWorker["Download Worker\nx6 instances"]
        UploadWorker["Upload Worker\nx4 instances"]
        Processor["Result Processor\nx2 instances"]
        DLQHandler["DLQ Handler\nx1 manual"]
    end

    EventHub -->|"Consume"| Ingester
    Ingester -->|"Produce"| Pending
    Ingester -.->|"Analytics"| DeltaEvents

    Pending --> DownloadWorker
    Retry5 --> DownloadWorker
    Retry10 --> DownloadWorker
    Retry20 --> DownloadWorker
    Retry40 --> DownloadWorker

    DownloadWorker -->|"Save"| Cache
    DownloadWorker -->|"Success"| Cached
    DownloadWorker -->|"Transient"| Retry5
    DownloadWorker -->|"Permanent"| DLQ

    Cached --> UploadWorker
    Cache -->|"Read"| UploadWorker
    UploadWorker -->|"Upload"| OneLake
    UploadWorker -->|"Result"| Results
    UploadWorker -->|"Cleanup"| Cache

    Results --> Processor
    Processor -->|"Batch Write"| DeltaInventory

    DLQ --> DLQHandler
    DLQHandler -.->|"Replay"| Pending
```

**Characteristics:**
- Real-time event streaming
- Horizontally scalable workers
- **Decoupled download/upload** for independent scaling
- Local cache buffer protects against OneLake issues
- Kafka topics for retry with exponential backoff
- End-to-end latency: <5 seconds (p95)
- Max 50 parallel downloads, 40 parallel uploads

---

## Retry Flow Detail

```mermaid
flowchart LR
    Task[Download Task] --> Worker[Download Worker]

    Worker -->|Success| Results[results topic]
    Worker -->|"Transient Fail\n(retry 1)"| R1[retry.5m]

    R1 -->|"After 5min"| Worker
    Worker -->|"Transient Fail\n(retry 2)"| R2[retry.10m]

    R2 -->|"After 10min"| Worker
    Worker -->|"Transient Fail\n(retry 3)"| R3[retry.20m]

    R3 -->|"After 20min"| Worker
    Worker -->|"Transient Fail\n(retry 4)"| R4[retry.40m]

    R4 -->|"After 40min"| Worker
    Worker -->|"Exhausted"| DLQ[DLQ]

    Worker -->|"Permanent Fail"| DLQ
```

---

## Key Differences

| Aspect | Old | New |
|--------|-----|-----|
| **Event Delivery** | Polling (60s) | Real-time streaming |
| **Scaling** | Single process | Horizontal (1-20 workers) |
| **Architecture** | Coupled download/upload | Decoupled (cache buffer) |
| **Retry Mechanism** | Delta table polling | Kafka topics with delays |
| **Latency (p95)** | 60-120 seconds | <5 seconds |
| **Parallelism** | 10 downloads | 50 downloads + 40 uploads |
| **Failure Handling** | Re-poll on next cycle | Exponential backoff + DLQ |
| **OneLake Resilience** | Blocking on failures | Cache buffer, independent upload |
| **Observability** | Basic logging | Prometheus metrics + Grafana |
