::: mermaid
    flowchart TB
      subgraph OLD["OLD ARCHITECTURE (Polling-Based)"]
          direction TB

          subgraph OldSource["Data Source"]
              Kusto[(Kusto/ADX)]
          end

          subgraph OldPipeline["Pipeline (Single Process)"]
              Poll[/"Poll every 60s"/]
              OldDownload["Download Stage"]
              OldRetry[(Delta Retry Table)]
              OldInventory[(Delta Inventory)]
          end

          subgraph OldStorage["Storage"]
              OldOneLake[(OneLake Files)]
          end

          Kusto -->|"Batch Query"| Poll
          Poll -->|"Process Events"| OldDownload
          OldDownload -->|"On Failure"| OldRetry
          OldRetry -->|"Re-poll"| Poll
          OldDownload -->|"Upload"| OldOneLake
          OldDownload -->|"Record"| OldInventory
      end

      subgraph NEW["NEW ARCHITECTURE (Kafka-Based)"]
          direction TB

          subgraph NewSource["Azure Event Hub"]
              EventHub[("xact.events.raw\n(Real-time)")]
          end

          subgraph LocalKafka["Local Kafka (Docker)"]
              Pending[["xact.downloads.pending"]]
              Cached[["xact.downloads.cached"]]
              Results[["xact.downloads.results"]]
              Retry5[["retry.5m"]]
              Retry10[["retry.10m"]]
              Retry20[["retry.20m"]]
              Retry40[["retry.40m"]]
              DLQ[["xact.downloads.dlq"]]
          end

          subgraph LocalCache["Local Cache"]
              Cache[("File Cache\n/tmp/kafka_pipeline_cache")]
          end

          subgraph Workers["Workers (Parallel)"]
              Ingester["Event Ingester\n(3 instances)"]
              Download["Download Worker\n(6 instances)"]
              Upload["Upload Worker\n(6 instances)"]
              Processor["Result Processor\n(2 instances)"]
              DLQHandler["DLQ Handler\n(Manual)"]
          end

          subgraph NewStorage["Azure Storage"]
              NewOneLake[(OneLake Files)]
              DeltaEvents[(Delta Events)]
              DeltaInventory[(Delta Inventory)]
          end

          EventHub -->|"Consume"| Ingester
          Ingester -->|"Produce Tasks"| Pending
          Ingester -.->|"Analytics"| DeltaEvents

          Pending -->|"Consume"| Download
          Retry5 -->|"Consume"| Download
          Retry10 -->|"Consume"| Download
          Retry20 -->|"Consume"| Download
          Retry40 -->|"Consume"| Download

          Download -->|"Write"| Cache
          Download -->|"Success"| Cached
          Download -->|"Transient Fail"| Retry5
          Retry5 -.->|"Still Failing"| Retry10
          Retry10 -.->|"Still Failing"| Retry20
          Retry20 -.->|"Still Failing"| Retry40
          Retry40 -.->|"Exhausted"| DLQ
          Download -->|"Permanent Fail"| DLQ

          Cached -->|"Consume"| Upload
          Cache -->|"Read"| Upload
          Upload -->|"Upload"| NewOneLake
          Upload -->|"Success"| Results
          Upload -->|"Transient Fail"| Retry5
          Upload -->|"Permanent Fail"| DLQ

          Results -->|"Consume"| Processor
          Processor -->|"Batch Write"| DeltaInventory

          DLQ -->|"Manual Review"| DLQHandler
          DLQHandler -.->|"Replay"| Pending
      end

      style OLD fill:#ffcccc,stroke:#990000
      style NEW fill:#ccffcc,stroke:#009900
      style LocalKafka fill:#ffffcc,stroke:#999900
      style LocalCache fill:#e6f3ff,stroke:#0066cc
      style EventHub fill:#cce5ff,stroke:#004085
:::
