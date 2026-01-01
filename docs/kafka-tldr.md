# Kafka TLDR

Quick reference for Kafka concepts as they apply to this pipeline.

## Core Concepts

### What is Kafka?
A distributed message queue. Think of it as a persistent, ordered log of messages that multiple services can write to and read from independently.

### The Mental Model
```
Producer --> [Topic] --> Consumer Group --> Processing
                |
                +--> Another Consumer Group --> Different Processing
```

## Key Terminology

| Term | What It Means |
|------|---------------|
| **Topic** | A named channel for messages (like `xact.downloads.pending`) |
| **Partition** | A topic is split into partitions for parallelism. Messages with the same key go to the same partition |
| **Offset** | Position of a message within a partition (like an index). Consumers track "how far along" they are |
| **Consumer Group** | A named group of consumers that share work. Each partition is read by only ONE consumer in the group |
| **Producer** | Sends messages to topics |
| **Consumer** | Reads messages from topics |
| **Broker** | A Kafka server. Multiple brokers form a cluster |

## How Consumer Groups Work

This is the key concept for parallelism:

```
Topic: xact.downloads.pending (6 partitions)
       P0  P1  P2  P3  P4  P5
       |   |   |   |   |   |
       +---+---+   +---+---+
           |           |
      Consumer-1  Consumer-2   (same group = split work)
```

- **Same group**: Partitions are divided among consumers (parallel processing)
- **Different groups**: Each group gets ALL messages (fan-out)

If you have 6 partitions and 3 consumers in a group, each consumer handles 2 partitions.

## Topics in This Project

| Topic | Purpose |
|-------|---------|
| `xact.downloads.pending` | Main work queue - files that need downloading |
| `xact.downloads.cached` | Files downloaded locally, ready for upload |
| `xact.downloads.results` | Download outcomes (success/failure metadata) |
| `xact.downloads.dlq` | Dead Letter Queue - permanently failed messages |
| `xact.downloads.pending.retry.*` | Retry queues with delays (5m, 10m, 20m, 40m) |
| `xact.events.raw` | Raw events (dev mode) |

## Message Flow

```
1. Event arrives
       |
       v
2. xact.downloads.pending  <-- DownloadWorker consumes
       |
       v
3. Download file to local cache
       |
       v
4. xact.downloads.cached  <-- UploadWorker consumes
       |
       v
5. Upload to OneLake
       |
       v
6. xact.downloads.results  <-- ResultProcessor records outcome
```

## Error Handling

### Retry Flow
```
Processing fails (transient error)
       |
       v
xact.downloads.pending.retry.5m  (wait 5 min)
       |
       v
Back to xact.downloads.pending  (try again)
       |
       v
Still failing? -> retry.10m -> retry.20m -> retry.40m
       |
       v
Max retries exceeded? -> DLQ
```

### Dead Letter Queue (DLQ)
Messages that can't be processed after all retries go here. They sit in `xact.downloads.dlq` for manual inspection/replay.

## Key Settings

| Setting | What It Does |
|---------|--------------|
| `enable_auto_commit: False` | We manually commit after processing (at-least-once) |
| `auto_offset_reset: earliest` | New consumers start from beginning of topic |
| `max_poll_records` | How many messages to fetch per poll |
| `session_timeout_ms` | How long before Kafka considers consumer dead |
| `max_poll_interval_ms` | Max time between polls before rebalance |

## At-Least-Once Semantics

This project uses **at-least-once** delivery:
1. Fetch message
2. Process it
3. **Then** commit offset

If we crash after processing but before committing, the message gets reprocessed. Your handlers should be **idempotent** (safe to run multiple times).

## Local Development

```bash
# Start local Kafka
docker-compose -f docker-compose.kafka.yml up -d

# With UI (localhost:8080)
docker-compose -f docker-compose.kafka.yml --profile ui up -d

# Check topics
docker exec kafka-pipeline-local kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Circuit Breaker

The consumer has a circuit breaker that opens when Kafka/broker failures pile up. When open:
- Messages aren't fetched
- After cooldown, circuit closes and processing resumes

This prevents hammering a failing broker.

## Quick Reference: Consumer Lifecycle

```python
# Consumer setup (simplified)
consumer = BaseKafkaConsumer(
    config=config,
    topics=["xact.downloads.pending"],
    group_id="download-workers",
    message_handler=process_download_task
)

await consumer.start()  # Blocks, processing messages
await consumer.stop()   # Graceful shutdown
```

## What You Need to Remember

1. **Partitions = parallelism** - more partitions = more consumers can work in parallel
2. **Consumer groups share work** - each partition goes to one consumer in the group
3. **Offsets track progress** - consumers remember where they left off
4. **Manual commit = at-least-once** - design handlers to be idempotent
5. **Retry topics = exponential backoff** - transient failures get delayed retries
6. **DLQ = investigation needed** - permanent failures need human attention
