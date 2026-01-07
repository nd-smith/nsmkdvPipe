# Configuration Refactor Plan
## Individual Consumer/Producer Configuration

**Status:** Planning
**Date:** 2026-01-05
**Breaking Change:** Yes - No backward compatibility required

---

## Executive Summary

Refactor `config.yaml` to support individual configuration for each consumer and producer worker, organized by domain (xact/claimx) and worker type. Focus on batch sizes, time windows, polling intervals, and throughput settings.

---

## Part 1: Configuration Structure Redesign

### Current Structure (Flat)
```yaml
kafka:
  max_poll_records: 100
  session_timeout_ms: 30000
  acks: "all"
  download_concurrency: 10
  # ... all settings apply uniformly
```

### New Structure (Hierarchical by Domain & Worker)

```yaml
kafka:
  # ========================================================================
  # SHARED CONNECTION SETTINGS
  # Applied to all consumers and producers across all domains
  # ========================================================================
  connection:
    bootstrap_servers: "localhost:9092"        # Comma-separated broker addresses
    security_protocol: "PLAINTEXT"             # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    sasl_mechanism: "OAUTHBEARER"              # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
    sasl_plain_username: ""                    # Username for SASL/PLAIN or Event Hubs
    sasl_plain_password: ""                    # Password for SASL/PLAIN or Event Hubs

    # Connection timeouts to prevent long operations from hanging
    request_timeout_ms: 120000                 # 2 minutes - max time for any request
    metadata_max_age_ms: 300000                # 5 minutes - how often to refresh metadata
    connections_max_idle_ms: 540000            # 9 minutes - close idle connections after this

  # ========================================================================
  # DEFAULT CONSUMER SETTINGS
  # Applied to all consumers unless overridden in worker-specific config
  # ========================================================================
  consumer_defaults:
    # Offset management
    auto_offset_reset: "earliest"              # Where to start if no offset: earliest, latest, none
    enable_auto_commit: false                  # Auto-commit offsets (false = manual commit for exactly-once)

    # Polling behavior - controls batch size and timing
    max_poll_records: 100                      # Max records returned per poll() call
    max_poll_interval_ms: 300000               # 5 min - max time between polls before rebalance
    fetch_min_bytes: 1                         # Min bytes to accumulate before returning from fetch
    fetch_max_wait_ms: 500                     # Max time to wait for fetch_min_bytes to accumulate

    # Session management - controls when consumer is considered dead
    session_timeout_ms: 30000                  # 30 sec - max time without heartbeat before rebalance
    heartbeat_interval_ms: 3000                # 3 sec - how often to send heartbeats (< session_timeout_ms/3)

    # Partition assignment
    partition_assignment_strategy: "RoundRobin"  # RoundRobin, Range, Sticky

  # ========================================================================
  # DEFAULT PRODUCER SETTINGS
  # Applied to all producers unless overridden in worker-specific config
  # ========================================================================
  producer_defaults:
    # Durability guarantees
    acks: "all"                                # 0 (none), 1 (leader), all (all replicas) - durability vs speed

    # Retry behavior
    retries: 3                                 # Max retry attempts for failed sends
    retry_backoff_ms: 1000                     # 1 sec - delay between retries

    # Batching for throughput - larger batches = better throughput, higher latency
    batch_size: 16384                          # 16 KB - max batch size in bytes
    linger_ms: 0                               # How long to wait for batch to fill (0 = send immediately)

    # Compression - reduces network usage, increases CPU
    compression_type: "none"                   # none, gzip, snappy, lz4, zstd

    # Concurrency control
    max_in_flight_requests_per_connection: 5   # Max unacknowledged requests per connection

    # Memory management
    buffer_memory: 33554432                    # 32 MB - total buffer memory for producer

  # ========================================================================
  # XACT DOMAIN CONFIGURATION
  # ========================================================================
  xact:
    # Topic configuration (shared across workers)
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
      downloads_cached: "xact.downloads.cached"
      downloads_results: "xact.downloads.results"
      dlq: "xact.downloads.dlq"

    consumer_group_prefix: "xact"              # Prefix for all consumer groups in this domain

    # Retry configuration for download retry topics
    retry_delays: [300, 600, 1200, 2400]       # Seconds: 5m, 10m, 20m, 40m
    max_retries: 4

    # ----------------------------------------------------------------------
    # Event Ingester Worker
    # Consumes: xact.events.raw
    # Produces to: xact.downloads.pending
    # ----------------------------------------------------------------------
    event_ingester:
      consumer:
        group_id: "xact-event-ingester"        # Consumer group name (overrides auto-generated)
        max_poll_records: 500                  # Higher batch size for high-volume ingestion
        max_poll_interval_ms: 600000           # 10 min - longer processing window
        session_timeout_ms: 45000              # 45 sec - more tolerance for processing
        fetch_min_bytes: 10240                 # 10 KB - wait for more data before returning
        fetch_max_wait_ms: 1000                # 1 sec - max wait for batch to accumulate

      producer:
        acks: "1"                              # Only wait for leader (faster, less durable)
        batch_size: 32768                      # 32 KB - larger batches for throughput
        linger_ms: 100                         # Wait 100ms for batch to fill
        compression_type: "lz4"                # Fast compression for high throughput

      processing:
        batch_size: 100                        # Application-level batch size for processing
        max_batches:                           # Optional limit for testing (null = unlimited)

    # ----------------------------------------------------------------------
    # Download Worker
    # Consumes: xact.downloads.pending + retry topics
    # Produces to: xact.downloads.cached
    # ----------------------------------------------------------------------
    download_worker:
      consumer:
        group_id: "xact-download-worker"
        max_poll_records: 50                   # Smaller batches - downloads are slow
        max_poll_interval_ms: 900000           # 15 min - downloads can take time
        session_timeout_ms: 60000              # 60 sec - longer timeout for slow downloads
        fetch_max_wait_ms: 2000                # 2 sec - wait longer for batch to accumulate

      producer:
        acks: "all"                            # Ensure cached downloads are durable
        compression_type: "none"               # Don't compress metadata messages

      processing:
        concurrency: 10                        # Max concurrent downloads (1-50)
        batch_size: 20                         # Messages to fetch per batch
        timeout_seconds: 60                    # Download timeout per file

    # ----------------------------------------------------------------------
    # Upload Worker
    # Consumes: xact.downloads.cached
    # Produces to: xact.downloads.results
    # ----------------------------------------------------------------------
    upload_worker:
      consumer:
        group_id: "xact-upload-worker"
        max_poll_records: 50                   # Smaller batches - uploads are slow
        max_poll_interval_ms: 900000           # 15 min - uploads can take time
        session_timeout_ms: 60000              # 60 sec - longer timeout for slow uploads

      producer:
        acks: "all"                            # Ensure results are durable
        compression_type: "none"

      processing:
        concurrency: 10                        # Max concurrent uploads (1-50)
        batch_size: 20                         # Messages to fetch per batch

    # ----------------------------------------------------------------------
    # Result Processor Worker
    # Consumes: xact.downloads.results
    # Produces to: (none - writes to Delta)
    # ----------------------------------------------------------------------
    result_processor:
      consumer:
        group_id: "xact-result-processor"
        max_poll_records: 100
        max_poll_interval_ms: 300000           # 5 min

      processing:
        batch_size: 100

    # ----------------------------------------------------------------------
    # Delta Events Writer Worker
    # Consumes: xact.downloads.results
    # Produces to: delta-events.retry.*, delta-events.dlq
    # ----------------------------------------------------------------------
    delta_events_writer:
      consumer:
        group_id: "xact-delta-events-writer"
        max_poll_records: 1000                 # Large batches for Delta writes
        max_poll_interval_ms: 600000           # 10 min - batch writing takes time
        session_timeout_ms: 45000              # 45 sec

      producer:
        acks: "all"
        compression_type: "snappy"             # Good compression for retry/DLQ

      processing:
        batch_size: 1000                       # Events per Delta batch
        max_batches:                           # Optional limit for testing
        retry_delays: [300, 600, 1200, 2400]   # 5m, 10m, 20m, 40m
        max_retries: 4
        retry_topic_prefix: "delta-events.retry"
        dlq_topic: "delta-events.dlq"

  # ========================================================================
  # CLAIMX DOMAIN CONFIGURATION
  # ========================================================================
  claimx:
    # Topic configuration (shared across workers)
    topics:
      events: "claimx.events.raw"
      enrichment_pending: "claimx.enrichment.pending"
      downloads_pending: "claimx.downloads.pending"
      downloads_cached: "claimx.downloads.cached"
      downloads_results: "claimx.downloads.results"
      dlq: "claimx.downloads.dlq"

    consumer_group_prefix: "claimx"

    retry_delays: [300, 600, 1200, 2400]
    max_retries: 4

    # ----------------------------------------------------------------------
    # Event Ingester Worker
    # Consumes: claimx.events.raw
    # Produces to: claimx.enrichment.pending
    # ----------------------------------------------------------------------
    event_ingester:
      consumer:
        group_id: "claimx-event-ingester"
        max_poll_records: 200
        max_poll_interval_ms: 300000

      producer:
        acks: "1"
        batch_size: 32768
        linger_ms: 100
        compression_type: "lz4"

      processing:
        batch_size: 100

    # ----------------------------------------------------------------------
    # Enrichment Worker
    # Consumes: claimx.enrichment.pending
    # Produces to: claimx.downloads.pending
    # ----------------------------------------------------------------------
    enrichment_worker:
      consumer:
        group_id: "claimx-enrichment-worker"
        max_poll_records: 100
        max_poll_interval_ms: 600000           # 10 min - API calls can be slow
        session_timeout_ms: 45000

      producer:
        acks: "1"
        compression_type: "lz4"

      processing:
        api_concurrency: 20                    # Max concurrent API requests
        api_timeout_seconds: 30
        batch_size: 50

    # ----------------------------------------------------------------------
    # Download Worker
    # Consumes: claimx.downloads.pending + retry topics
    # Produces to: claimx.downloads.cached
    # ----------------------------------------------------------------------
    download_worker:
      consumer:
        group_id: "claimx-download-worker"
        max_poll_records: 50
        max_poll_interval_ms: 900000           # 15 min
        session_timeout_ms: 60000

      producer:
        acks: "all"

      processing:
        concurrency: 15                        # ClaimX may need more concurrency
        batch_size: 30
        timeout_seconds: 90                    # ClaimX files may be larger

    # ----------------------------------------------------------------------
    # Upload Worker
    # Consumes: claimx.downloads.cached
    # Produces to: claimx.downloads.results
    # ----------------------------------------------------------------------
    upload_worker:
      consumer:
        group_id: "claimx-upload-worker"
        max_poll_records: 50
        max_poll_interval_ms: 900000
        session_timeout_ms: 60000

      producer:
        acks: "all"

      processing:
        concurrency: 15
        batch_size: 30

    # ----------------------------------------------------------------------
    # Delta Events Writer Worker
    # Consumes: claimx.downloads.results
    # Produces to: delta-events.retry.*, delta-events.dlq
    # ----------------------------------------------------------------------
    delta_events_writer:
      consumer:
        group_id: "claimx-delta-events-writer"
        max_poll_records: 1000
        max_poll_interval_ms: 600000

      producer:
        acks: "all"
        compression_type: "snappy"

      processing:
        batch_size: 1000
        retry_delays: [300, 600, 1200, 2400]
        max_retries: 4

  # ========================================================================
  # SHARED STORAGE CONFIGURATION
  # ========================================================================
  storage:
    # OneLake paths by domain
    onelake_domain_paths:
      xact: ""                                 # abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/xact
      claimx: ""                               # abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/claimx

    onelake_base_path: ""                      # Fallback if domain not in onelake_domain_paths

    cache_dir: "/tmp/kafka_pipeline_cache"     # Local cache for downloads awaiting upload
```

---

## Part 2: Config Cleanup - Remove Legacy Settings

### Settings to REMOVE from config.yaml.example

These are defined in the old XACT/ClaimX sections but NOT used by `KafkaConfig`:

**From `xact:` section:**
- `xact.kusto.*` - Old Kusto connection (replaced by Eventhouse)
- `xact.lakehouse.max_batch_size_*` - Not in KafkaConfig
- `xact.lakehouse.upload_max_concurrency` - Not in KafkaConfig
- `xact.lakehouse.upload_block_size_mb` - Not in KafkaConfig
- `xact.lakehouse.upload_max_single_put_mb` - Not in KafkaConfig
- `xact.lakehouse.max_cache_size_completed_ids` - Not in KafkaConfig
- `xact.processing.*` - Not in KafkaConfig
- `xact.download.*` - Not in KafkaConfig (replaced by worker-specific settings)
- `xact.retry.*` - Not in KafkaConfig (different from Kafka retry)
- `xact.schedule.*` - Not in KafkaConfig
- `xact.security.*` - Not in KafkaConfig
- `xact.timeouts.*` - Not in KafkaConfig

**From `claimx:` section:**
- `claimx.kusto.*` - Old Kusto connection (replaced by Eventhouse)
- `claimx.api.*` - Now moved to `kafka.claimx_api_*` in KafkaConfig
- `claimx.lakehouse.*` - Not in KafkaConfig
- `claimx.processing.*` - Not in KafkaConfig
- `claimx.download.*` - Not in KafkaConfig
- `claimx.retry.*` - Not in KafkaConfig
- `claimx.schedule.*` - Not in KafkaConfig
- `claimx.security.*` - Not in KafkaConfig

**Keep these sections** (not managed by KafkaConfig but still used):
- `eventhouse:` - Used by Eventhouse poller
- `health:` - Used by health check server
- `logging:` - Used by logging setup
- `observability:` - Used by observability framework
- `event_source:` - Used by __main__.py
- `worker_id:` - Used by workers
- `test_mode:` - Used for testing

---

## Part 3: Worker Refactor Details

### Current State: Inconsistent Consumer Patterns

**Type 1: Uses BaseKafkaConsumer (abstraction)**
- Event Ingester Worker

**Type 2: Uses AIOKafkaConsumer directly (manual)**
- Download Worker
- Upload Worker

### Problem with Type 2 Workers

Download and Upload workers create `AIOKafkaConsumer` directly in their `start()` methods:

```python
# download_worker.py line ~170
self._consumer = AIOKafkaConsumer(
    *self.topics,
    bootstrap_servers=self.config.bootstrap_servers,
    group_id=self.CONSUMER_GROUP,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    max_poll_records=self.config.download_batch_size,  # Uses batch_size as poll size!
    # ... hardcoded settings
)
```

Issues:
1. Hardcoded consumer group (class constant `CONSUMER_GROUP`)
2. Hardcoded polling/session settings (not configurable)
3. `max_poll_records` incorrectly uses `download_batch_size` (application concern, not Kafka concern)
4. Bypasses `BaseKafkaConsumer` abstraction
5. Manual OAuth setup, manual metrics, manual error handling

### Refactor Options

**Option A: Standardize on BaseKafkaConsumer (Recommended)**

Pros:
- Consistent abstraction across all workers
- Reuses circuit breaker, metrics, OAuth, error handling
- Easier to test and maintain
- Single place to update consumer behavior

Cons:
- Requires refactoring Download/Upload workers
- May need to enhance BaseKafkaConsumer for batch processing patterns

**Option B: Keep Manual AIOKafkaConsumer, Add Config Support**

Pros:
- Minimal changes to working code
- Workers keep full control

Cons:
- Duplicate logic across workers
- Harder to maintain consistency
- Still need to parameterize consumer creation

### Recommended Approach: Option A

**Step 1: Enhance BaseKafkaConsumer**

Add support for worker-specific config:

```python
class BaseKafkaConsumer:
    def __init__(
        self,
        config: KafkaConfig,
        worker_name: str,              # NEW: "download_worker", "upload_worker", etc.
        domain: str,                   # NEW: "xact", "claimx"
        topics: List[str],
        message_handler: Callable[[ConsumerRecord], Awaitable[None]],
        circuit_breaker: Optional[CircuitBreaker] = None,
        max_batches: Optional[int] = None,
        enable_message_commit: bool = True,
    ):
        # Resolve worker-specific config
        worker_config = config.get_worker_config(domain, worker_name, "consumer")

        # Extract settings with fallback to defaults
        self.group_id = worker_config.get("group_id") or config.get_consumer_group(worker_name)
        self.max_poll_records = worker_config.get("max_poll_records", 100)
        self.session_timeout_ms = worker_config.get("session_timeout_ms", 30000)
        # ... etc

        # Create AIOKafkaConsumer with resolved settings
        # ...
```

**Step 2: Add Config Resolution to KafkaConfig**

```python
@dataclass
class KafkaConfig:
    # ... existing fields ...

    # NEW: Worker-specific configurations
    xact_workers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    claimx_workers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    consumer_defaults: Dict[str, Any] = field(default_factory=dict)
    producer_defaults: Dict[str, Any] = field(default_factory=dict)

    def get_worker_config(
        self,
        domain: str,
        worker_name: str,
        component: str  # "consumer" or "producer"
    ) -> Dict[str, Any]:
        """Get merged config for a specific worker's consumer or producer.

        Merge priority (highest to lowest):
        1. Worker-specific config (e.g., xact.download_worker.consumer)
        2. Default config (consumer_defaults or producer_defaults)

        Args:
            domain: "xact" or "claimx"
            worker_name: "download_worker", "upload_worker", etc.
            component: "consumer" or "producer"

        Returns:
            Merged configuration dict
        """
        # Get defaults
        defaults = self.consumer_defaults if component == "consumer" else self.producer_defaults
        result = defaults.copy()

        # Get worker-specific overrides
        workers = self.xact_workers if domain == "xact" else self.claimx_workers
        worker_config = workers.get(worker_name, {})
        component_config = worker_config.get(component, {})

        # Merge (worker-specific overrides defaults)
        result.update(component_config)

        return result
```

**Step 3: Refactor Download Worker**

Before:
```python
class DownloadWorker:
    CONSUMER_GROUP = "xact-download-worker"  # Hardcoded

    def __init__(self, config: KafkaConfig, temp_dir: Optional[Path] = None):
        self.config = config
        # ...

    async def start(self):
        # Manually create consumer with hardcoded settings
        self._consumer = AIOKafkaConsumer(...)
```

After:
```python
class DownloadWorker:
    WORKER_NAME = "download_worker"

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "xact",  # NEW
        temp_dir: Optional[Path] = None
    ):
        self.config = config
        self.domain = domain

        # Use BaseKafkaConsumer
        self.consumer = BaseKafkaConsumer(
            config=config,
            worker_name=self.WORKER_NAME,
            domain=domain,
            topics=self._get_topics(),  # pending + retry topics
            message_handler=self._handle_message,
            enable_message_commit=False,  # Batch commits
        )

        # Get worker-specific processing config
        worker_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = worker_config.get("concurrency", 10)
        self.batch_size = worker_config.get("batch_size", 20)
        self.timeout = worker_config.get("timeout_seconds", 60)

        # Create producer
        self.producer = BaseKafkaProducer(
            config=config,
            worker_name=self.WORKER_NAME,
            domain=domain,
        )

    def _get_topics(self) -> List[str]:
        """Build list of topics to consume from."""
        topics_config = (
            self.config.xact_workers.get("topics", {})
            if self.domain == "xact"
            else self.config.claimx_workers.get("topics", {})
        )

        pending_topic = topics_config.get("downloads_pending")
        retry_delays = self.config.retry_delays
        retry_topics = [
            self.config.get_retry_topic(i) for i in range(len(retry_delays))
        ]

        return [pending_topic] + retry_topics
```

**Step 4: Refactor Upload Worker (similar pattern)**

---

## Part 4: Implementation Tasks

### Task 1: Config Schema Changes
- [ ] Design new YAML schema (shown above)
- [ ] Update `config.yaml.example` with new structure
- [ ] Remove all legacy/unused settings
- [ ] Add comprehensive comments to every setting

### Task 2: KafkaConfig Dataclass Changes
- [ ] Add nested dataclasses: `ConsumerDefaults`, `ProducerDefaults`
- [ ] Add `xact_workers: Dict[str, Dict]`, `claimx_workers: Dict[str, Dict]`
- [ ] Add `consumer_defaults: Dict`, `producer_defaults: Dict`
- [ ] Add `get_worker_config(domain, worker_name, component)` method
- [ ] Update `load_config()` to parse new YAML structure
- [ ] Update `_apply_env_overrides()` for new structure (if needed)

### Task 3: BaseKafkaConsumer Enhancement
- [ ] Add `worker_name: str` parameter to `__init__`
- [ ] Add `domain: str` parameter to `__init__`
- [ ] Modify to call `config.get_worker_config()` for settings
- [ ] Apply worker-specific settings to `AIOKafkaConsumer` creation
- [ ] Support optional `group_id` override

### Task 4: BaseKafkaProducer Enhancement
- [ ] Add `worker_name: str` parameter to `__init__`
- [ ] Add `domain: str` parameter to `__init__`
- [ ] Modify to call `config.get_worker_config()` for settings
- [ ] Apply worker-specific settings to `AIOKafkaProducer` creation

### Task 5: Worker Refactors

**Download Worker (xact & claimx):**
- [ ] Add `domain: str` parameter to `__init__`
- [ ] Replace manual `AIOKafkaConsumer` with `BaseKafkaConsumer`
- [ ] Remove `CONSUMER_GROUP` constant (use config)
- [ ] Extract processing settings from `config.get_worker_config(domain, "download_worker", "processing")`
- [ ] Update `start()` method to use `BaseKafkaConsumer`
- [ ] Update tests

**Upload Worker (xact & claimx):**
- [ ] Add `domain: str` parameter to `__init__`
- [ ] Replace manual `AIOKafkaConsumer` with `BaseKafkaConsumer`
- [ ] Remove `CONSUMER_GROUP` constant (use config)
- [ ] Extract processing settings from `config.get_worker_config(domain, "upload_worker", "processing")`
- [ ] Update `start()` method to use `BaseKafkaConsumer`
- [ ] Update tests

**Event Ingester (already uses BaseKafkaConsumer):**
- [ ] Add `domain: str` parameter
- [ ] Update `BaseKafkaConsumer` instantiation to pass `worker_name` and `domain`
- [ ] Update tests

**Delta Events Writer:**
- [ ] Add `domain: str` parameter
- [ ] Update `BaseKafkaConsumer` instantiation to pass `worker_name` and `domain`
- [ ] Extract processing settings from worker config
- [ ] Update tests

### Task 6: Remove Environment Variable Support
- [ ] Remove all env var parsing from `KafkaConfig.from_env()`
- [ ] Remove `_apply_env_overrides()` function
- [ ] Remove env var mapping dict from `load_config()`
- [ ] Update docstrings to remove env var references
- [ ] Configuration now ONLY loads from config.yaml (no env var overrides)

### Task 7: Add Config Validation
- [ ] Add `validate()` method to `KafkaConfig`
- [ ] Validate: `heartbeat_interval_ms < session_timeout_ms / 3`
- [ ] Validate: `session_timeout_ms < max_poll_interval_ms`
- [ ] Validate: `concurrency` in range 1-50
- [ ] Validate: Required fields (bootstrap_servers, topics, etc.)
- [ ] Validate: Compression types are valid
- [ ] Validate: Acks values are valid (0, 1, "all")
- [ ] Call `validate()` in `load_config()` before returning

### Task 8: Testing
- [ ] Unit tests for `get_worker_config()` merging logic
- [ ] Unit tests for each worker with custom configs
- [ ] Integration tests with different worker configurations
- [ ] Test that defaults work when no worker config specified
- [ ] Test that worker-specific settings override defaults
- [ ] Test config validation catches invalid configurations

### Task 9: Documentation
- [ ] Update README with new config structure
- [ ] Document all available settings and their impacts
- [ ] Provide example configs for common scenarios
- [ ] Migration guide from old to new config format
- [ ] Document that env vars are NO LONGER SUPPORTED

---

## Part 5: Settings Reference

### Consumer Settings (Batch Sizes, Time Windows)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `max_poll_records` | int | 100 | **Batch size** - Max records per poll(). Higher = more throughput, more memory |
| `max_poll_interval_ms` | int | 300000 | **Processing window** - Max time between polls (5 min). Increase for slow processing |
| `session_timeout_ms` | int | 30000 | **Liveness timeout** - Max time without heartbeat (30 sec). Increase for slow processing |
| `heartbeat_interval_ms` | int | 3000 | **Heartbeat frequency** - Must be < session_timeout_ms/3 |
| `fetch_min_bytes` | int | 1 | **Batch accumulation** - Min bytes before returning from fetch. Higher = wait for more data |
| `fetch_max_wait_ms` | int | 500 | **Wait window** - Max time to wait for fetch_min_bytes. Higher = more batching, higher latency |

### Producer Settings (Batch Sizes, Time Windows)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `acks` | str | "all" | **Durability** - 0 (none), 1 (leader), all (all replicas). all = slowest, most durable |
| `batch_size` | int | 16384 | **Batch size** - Max batch in bytes (16 KB). Higher = better throughput |
| `linger_ms` | int | 0 | **Batching window** - Wait time for batch to fill. 0 = send immediately, >0 = wait and batch |
| `retries` | int | 3 | **Retry attempts** - Max retries for failed sends |
| `retry_backoff_ms` | int | 1000 | **Retry delay** - Delay between retries (1 sec) |
| `compression_type` | str | "none" | **Compression** - none, gzip, snappy, lz4, zstd. Reduces network, increases CPU |

### Processing Settings (Worker-Specific)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `concurrency` | int | 10 | **Parallelism** - Max concurrent operations (downloads/uploads). Range: 1-50 |
| `batch_size` | int | 20 | **Application batch** - Messages to process per batch (different from max_poll_records) |
| `timeout_seconds` | int | 60 | **Operation timeout** - Timeout per download/upload/API call |

---

## Part 6: Example Use Cases

### High-Throughput Event Ingestion
```yaml
xact:
  event_ingester:
    consumer:
      max_poll_records: 1000           # Large batches
      fetch_min_bytes: 102400          # Wait for 100 KB
      fetch_max_wait_ms: 2000          # Wait up to 2 sec
    producer:
      batch_size: 65536                # 64 KB batches
      linger_ms: 200                   # Wait 200ms to fill batch
      compression_type: "lz4"          # Fast compression
```

### Low-Latency Processing
```yaml
xact:
  event_ingester:
    consumer:
      max_poll_records: 10             # Small batches
      fetch_max_wait_ms: 100           # Don't wait long
    producer:
      linger_ms: 0                     # Send immediately
      compression_type: "none"         # No compression delay
```

### Slow Download Workers
```yaml
xact:
  download_worker:
    consumer:
      max_poll_records: 20             # Small batches
      max_poll_interval_ms: 1800000    # 30 min for very slow downloads
      session_timeout_ms: 120000       # 2 min heartbeat tolerance
    processing:
      concurrency: 5                   # Fewer concurrent downloads
      timeout_seconds: 300             # 5 min per download
```

---

## Part 7: Migration Impact

### Breaking Changes
1. **Config file structure completely changed** - Old config.yaml files will NOT work
2. **Environment variables NO LONGER SUPPORTED** - All configuration must be in config.yaml
3. **Worker constructors require `domain` parameter** - All worker instantiation must be updated
4. **Removed legacy settings** - Old xact/claimx sections removed from config
5. **Consumer group naming may change** - If using custom group_id overrides
6. **Config validation enforced** - Invalid configs will fail on startup

### Migration Steps
1. Create new config.yaml based on new structure
2. **Migrate all environment variables to config.yaml** - No env var fallbacks
3. Update worker instantiation in `__main__.py` to pass `domain` parameter
4. Test each worker independently with new config
5. Verify config validation passes
6. Remove old config.yaml after migration complete

### No Migration Needed For
- Topic names (unchanged, just reorganized)
- OneLake paths (moved but same structure)
- Producer/consumer connection logic (abstracted by config)

---

## Design Decisions

### 1. Processing Settings Placement ✅
**Decision:** Processing settings (concurrency, batch_size, timeout_seconds) are part of `KafkaConfig`.

**Rationale:** Single configuration dataclass simplifies loading, validation, and testing. All worker configuration in one place.

### 2. Environment Variables ✅
**Decision:** REMOVE all environment variable support. Configuration ONLY from config.yaml.

**Rationale:**
- Simplifies configuration system (one source of truth)
- Eliminates env var / YAML merge complexity
- Forces explicit, version-controlled configuration
- Easier to understand and debug
- No risk of production env vars overriding intended config

**Impact:** Breaking change - users must migrate all env vars to config.yaml

### 3. Default Inheritance ✅
**Decision:** Workers inherit ONLY from global defaults (consumer_defaults, producer_defaults). NO domain-level defaults.

**Rationale:**
- Simpler inheritance model (2 levels instead of 3)
- Clearer configuration: global defaults → worker overrides
- Less confusion about precedence
- Domain-specific differences handled in worker configs

**Inheritance Chain:**
```
consumer_defaults
    ↓
xact.download_worker.consumer  (overrides defaults)
```

NOT:
```
consumer_defaults → xact.consumer_defaults → xact.download_worker.consumer
```

### 4. Config Validation ✅
**Decision:** YES - Implement comprehensive config validation on load.

**Validations:**
- `heartbeat_interval_ms < session_timeout_ms / 3` (Kafka requirement)
- `session_timeout_ms < max_poll_interval_ms` (logical requirement)
- `concurrency` in range 1-50 (operational constraint)
- Required fields present (bootstrap_servers, topics, etc.)
- Enum values valid (compression_type, acks, etc.)
- Numeric values in acceptable ranges

**Behavior:** Fail fast on startup with clear error messages.

### 5. Hot Reload ✅
**Decision:** NO hot reload. Config changes require restart.

**Rationale:**
- Simpler implementation (no watch threads, no state sync)
- Predictable behavior (config stable during runtime)
- Kafka consumers/producers need restart for most changes anyway
- Restarts are acceptable in containerized environments
- Avoids complex edge cases during reload

---

## Implementation Priority

### Phase 1: Core Infrastructure (Week 1)
1. Design and document new config.yaml structure
2. Update `KafkaConfig` dataclass with new structure
3. Implement `get_worker_config()` merging logic
4. Add config validation
5. Remove all env var support
6. Write unit tests for config loading and validation

### Phase 2: Worker Refactors (Week 2)
1. Enhance `BaseKafkaConsumer` and `BaseKafkaProducer`
2. Refactor Download Workers (xact + claimx)
3. Refactor Upload Workers (xact + claimx)
4. Update Event Ingester Workers
5. Update Delta Events Writer Workers
6. Write integration tests

### Phase 3: Migration (Week 3)
1. Update `__main__.py` for new worker initialization
2. Create new config.yaml.example
3. Test all workers with new config
4. Update documentation
5. Migration guide

---

## Success Criteria

- ✅ All workers use `BaseKafkaConsumer` (no manual AIOKafkaConsumer)
- ✅ Each worker's consumer/producer individually configurable
- ✅ Config organized by domain (xact/claimx) and worker
- ✅ Every setting has clear, helpful comment
- ✅ All legacy/unused settings removed from config.yaml
- ✅ Config validation prevents invalid configurations
- ✅ No environment variable support (YAML only)
- ✅ All tests passing with new config system
- ✅ Documentation complete with examples
