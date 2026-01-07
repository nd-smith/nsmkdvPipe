# Configuration Refactor Migration Guide

## Overview

The Kafka pipeline configuration has been refactored to use a hierarchical YAML-based structure that organizes settings by domain (xact/claimx) and worker. This replaces the previous environment variable-based configuration.

**Migration Status**: Breaking changes - requires config.yaml update and removal of old environment variables.

## Key Changes

### What's New ✅

1. **Hierarchical Configuration Structure**
   - Settings organized by domain → worker → component (consumer/producer/processing)
   - Worker-specific overrides with global defaults
   - Explicit configuration merge logic

2. **Improved Config Organization**
   - All Kafka settings in `config.yaml` (no more scattered environment variables)
   - Clear separation of concerns: consumer settings, producer settings, processing settings
   - Domain-specific topic naming with fallback to global defaults

3. **Better Validation**
   - Comprehensive config validation at startup
   - Clear error messages for missing or invalid settings
   - Kafka-specific constraint validation (e.g., heartbeat < session_timeout/3)

4. **Enhanced Worker Initialization**
   - All workers now accept `domain` parameter (e.g., "xact", "claimx")
   - Dynamic topic/consumer group resolution via helper methods
   - Consistent patterns across all workers

### What's Removed ❌

1. **Environment Variables** - Completely removed:
   ```bash
   # These no longer work:
   KAFKA_BOOTSTRAP_SERVERS
   KAFKA_CONSUMER_GROUP_PREFIX
   DOWNLOAD_CONCURRENCY
   DOWNLOAD_BATCH_SIZE
   UPLOAD_CONCURRENCY
   UPLOAD_BATCH_SIZE
   # ... and all other Kafka-related env vars
   ```

2. **Old Config Structure**
   - Flat config with hardcoded settings
   - Worker-specific config dataclasses (e.g., `DownloadWorkerConfig`)
   - `from_env()` class methods

3. **Hardcoded Consumer Groups and Topics**
   - Workers no longer have hardcoded `CONSUMER_GROUP` class variables
   - Topics dynamically resolved from config

## New Configuration Structure

### config.yaml Format

```yaml
kafka:
  # Global Kafka connection settings (required)
  bootstrap_servers: "localhost:9092"
  security_protocol: "PLAINTEXT"  # or SASL_SSL, SASL_PLAINTEXT
  sasl_mechanism: "PLAIN"  # if using SASL

  # Global topic naming (optional - can override per domain)
  topic_prefix: "dev"
  consumer_group_prefix: "dev"

  # Global defaults for all consumers (optional)
  defaults:
    consumer:
      auto_offset_reset: "earliest"
      max_poll_records: 500
      session_timeout_ms: 60000
      max_poll_interval_ms: 300000
      heartbeat_interval_ms: 3000
      fetch_min_bytes: 1
      fetch_max_wait_ms: 500

    producer:
      acks: "all"
      retries: 3
      compression_type: "lz4"
      linger_ms: 10
      batch_size: 16384

  # Domain-specific configuration
  xact:
    # xact topic overrides (optional)
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
      downloads_cached: "xact.downloads.cached"
      downloads_results: "xact.downloads.results"

    # Worker-specific configuration
    event_ingester:
      processing:
        health_port: 8080
      # Can override consumer/producer settings here

    download_worker:
      processing:
        concurrency: 10
        batch_size: 20
        timeout_seconds: 60
      consumer:
        max_poll_records: 20  # Override default
        max_poll_interval_ms: 300000

    upload_worker:
      processing:
        concurrency: 10
        batch_size: 20
      consumer:
        max_poll_records: 20

    delta_events_worker:
      processing:
        batch_size: 100
        max_batches: null  # null = unlimited

  # ClaimX domain configuration
  claimx:
    topics:
      events: "claimx.events.raw"
      enrichment_pending: "claimx.enrichment.pending"
      downloads_pending: "claimx.downloads.pending"
      downloads_cached: "claimx.downloads.cached"
      downloads_results: "claimx.downloads.results"

    event_ingester:
      processing:
        health_port: 8084

    enrichment_worker:
      processing:
        batch_size: 100
        batch_timeout_seconds: 5.0
        api_concurrency: 20
        health_port: 8081

    download_worker:
      processing:
        concurrency: 10
        batch_size: 20
        health_port: 8082

    upload_worker:
      processing:
        concurrency: 10
        batch_size: 20
        health_port: 8083

# OneLake paths (still supported via config.yaml or env vars)
onelake:
  domain_paths:
    xact: "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/Files/xact"
    claimx: "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/Files/claimx"
  # Or use base_path as fallback:
  # base_path: "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/Files"

# Cache directory for downloads
cache_dir: "/tmp/kafka_pipeline_cache"

# Retry configuration
retry:
  delays: [60, 300, 900, 3600]  # 1min, 5min, 15min, 1hour
```

## Migration Steps

### Step 1: Create config.yaml

1. Copy the example configuration:
   ```bash
   cp config.yaml.example config.yaml
   ```

2. Update connection settings:
   ```yaml
   kafka:
     bootstrap_servers: "your-kafka-broker:9092"
     security_protocol: "SASL_SSL"  # if using Azure Event Hub
     sasl_mechanism: "OAUTHBEARER"  # for Event Hub
   ```

3. Configure domain-specific settings:
   ```yaml
   kafka:
     xact:
       download_worker:
         processing:
           concurrency: 20  # Adjust based on your needs
           batch_size: 50
   ```

### Step 2: Remove Environment Variables

Remove all Kafka-related environment variables from your deployment:

```bash
# OLD - Remove these:
unset KAFKA_BOOTSTRAP_SERVERS
unset KAFKA_CONSUMER_GROUP_PREFIX
unset DOWNLOAD_CONCURRENCY
unset DOWNLOAD_BATCH_SIZE
unset UPLOAD_CONCURRENCY
unset UPLOAD_BATCH_SIZE
unset KAFKA_AUTO_OFFSET_RESET
unset KAFKA_MAX_POLL_RECORDS
# ... etc
```

**Important**: OneLake paths can still be configured via environment variables as a fallback:
```bash
# These still work (but config.yaml is preferred):
export ONELAKE_XACT_PATH="abfss://..."
export ONELAKE_CLAIMX_PATH="abfss://..."
```

### Step 3: Update Worker Initialization (if custom)

If you have custom scripts that initialize workers, update them to pass the `domain` parameter:

```python
# OLD:
worker = DownloadWorker(config=kafka_config)

# NEW:
worker = DownloadWorker(config=kafka_config, domain="xact")
```

All workers now accept `domain` parameter:
- `EventIngesterWorker(config, domain="xact", producer_config=None)`
- `DeltaEventsWorker(config, producer, events_table_path, domain="xact")`
- `DownloadWorker(config, domain="xact", temp_dir=None)`
- `UploadWorker(config, domain="xact")`
- `ClaimXEventIngesterWorker(config, domain="claimx", ...)`
- `ClaimXEnrichmentWorker(config, domain="claimx", ...)`
- `ClaimXDownloadWorker(config, domain="claimx", ...)`
- `ClaimXUploadWorker(config, domain="claimx")`

### Step 4: Test Configuration

1. **Validate config.yaml**:
   ```python
   from kafka_pipeline.config import KafkaConfig

   config = KafkaConfig.load_config("config.yaml")
   print("Config loaded successfully!")
   ```

2. **Run a single worker** to verify:
   ```bash
   python -m kafka_pipeline --worker xact-download
   ```

3. **Check logs** for configuration:
   ```
   INFO Initialized DownloadWorker with concurrent processing
     domain=xact
     worker_name=download_worker
     consumer_group=dev-xact-download-worker
     download_concurrency=10
     download_batch_size=20
   ```

### Step 5: Deploy

1. Update your deployment configuration files (Docker, Kubernetes, etc.)
2. Mount `config.yaml` into containers
3. Remove old environment variable declarations
4. Deploy and monitor

## Breaking Changes

### 1. Environment Variables No Longer Supported

**Impact**: HIGH
**Action Required**: YES

All `KAFKA_*` environment variables must be migrated to `config.yaml`.

### 2. Worker Initialization Changes

**Impact**: MEDIUM (only if you have custom scripts)
**Action Required**: Only if initializing workers directly in code

Workers now require `domain` parameter:
```python
# This will still work (uses default):
worker = DownloadWorker(config=kafka_config)

# But this is now recommended:
worker = DownloadWorker(config=kafka_config, domain="xact")
```

### 3. Consumer Group Naming

**Impact**: LOW (automatic migration)
**Action Required**: NO

Consumer groups are now dynamically generated as:
```
{prefix}-{domain}-{worker_name}
```

Example: `dev-xact-download-worker` (previously `dev-download-worker`)

This means existing consumer group offsets won't carry over. Workers will start from `auto_offset_reset` (usually "earliest").

**If you need to preserve offsets**, use the `kafka-consumer-groups` tool to rename groups before migration.

### 4. Config Loading

**Impact**: MEDIUM
**Action Required**: Only if loading config programmatically

```python
# OLD:
config = KafkaConfig.from_env()

# NEW:
config = KafkaConfig.load_config("config.yaml")
```

## Helper Methods Reference

### Topic Resolution

```python
config.get_topic(domain: str, topic_key: str) -> str
```

Examples:
```python
config.get_topic("xact", "events")
# Returns: "xact.events.raw" (or custom from config)

config.get_topic("claimx", "downloads_pending")
# Returns: "claimx.downloads.pending"
```

### Consumer Group Resolution

```python
config.get_consumer_group(domain: str, worker_name: str) -> str
```

Examples:
```python
config.get_consumer_group("xact", "download_worker")
# Returns: "dev-xact-download-worker"

config.get_consumer_group("claimx", "enrichment_worker")
# Returns: "dev-claimx-enrichment-worker"
```

### Worker Config Resolution

```python
config.get_worker_config(domain: str, worker_name: str, component: str) -> Dict[str, Any]
```

Examples:
```python
# Get merged consumer config for xact download worker
consumer_config = config.get_worker_config("xact", "download_worker", "consumer")
# Returns: {"auto_offset_reset": "earliest", "max_poll_records": 20, ...}

# Get processing config for claimx enrichment worker
processing_config = config.get_worker_config("claimx", "enrichment_worker", "processing")
# Returns: {"concurrency": 20, "batch_size": 100, ...}
```

Merge priority: worker-specific → domain defaults → global defaults

### Retry Topic Resolution

```python
config.get_retry_topic(domain: str, retry_level: int) -> str
```

Examples:
```python
config.get_retry_topic("xact", 0)
# Returns: "xact.downloads.retry.60s" (first retry = 60s)

config.get_retry_topic("claimx", 2)
# Returns: "claimx.downloads.retry.900s" (third retry = 15min)
```

## Common Issues and Solutions

### Issue: "Config file not found"

**Error**:
```
FileNotFoundError: Config file not found: config.yaml
```

**Solution**: Ensure `config.yaml` exists in the working directory or provide full path:
```python
config = KafkaConfig.load_config("/path/to/config.yaml")
```

### Issue: "Missing required configuration"

**Error**:
```
ValueError: Missing required configuration: kafka.bootstrap_servers
```

**Solution**: Add required settings to `config.yaml`:
```yaml
kafka:
  bootstrap_servers: "localhost:9092"
```

### Issue: "Kafka constraint validation failed"

**Error**:
```
ValueError: heartbeat_interval_ms (20000) must be less than session_timeout_ms/3 (20000.0)
```

**Solution**: Fix Kafka timing constraints in config:
```yaml
kafka:
  defaults:
    consumer:
      session_timeout_ms: 60000  # Must be 3x heartbeat
      heartbeat_interval_ms: 3000  # Must be < session_timeout/3
```

### Issue: "Worker starts from beginning after migration"

**Symptom**: Worker reprocesses old messages after migration

**Cause**: Consumer group name changed (e.g., `dev-download-worker` → `dev-xact-download-worker`)

**Solution**: Either:
1. Accept reprocessing (if safe)
2. Rename consumer group offsets before migration using `kafka-consumer-groups` tool

## Rollback Procedure

If you need to rollback to the old environment variable-based configuration:

1. Checkout previous git commit:
   ```bash
   git checkout <commit-before-refactor>
   ```

2. Restore environment variables in deployment

3. Redeploy

**Note**: Consumer group offsets will need manual adjustment if the new naming was used.

## Additional Resources

- **Config Example**: `config.yaml.example`
- **Implementation Plan**: `docs/config-refactor-plan.md`
- **Code Changes**: See git commits on `config-refactor` branch
- **Validation Logic**: `kafka_pipeline/config.py:validate_config()`

## Questions?

For issues or questions:
1. Check the logs for detailed error messages
2. Review the example configuration: `config.yaml.example`
3. Validate your config programmatically before deployment
4. Open an issue with the config-refactor label
