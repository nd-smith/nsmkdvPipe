# Config Merge Validator Specification

## Purpose
Validate that merged configuration from split YAML files produces the same effective configuration as the original monolithic `config.yaml`.

## What "Correct Merge" Means

A correct merge satisfies these properties:

1. **Shared Settings Present**: All shared/default settings (connection, consumer_defaults, producer_defaults) are present in merged config
2. **Domain Completeness**: Each domain (xact, claimx) has complete configuration with all required keys
3. **Override Precedence**: Files loaded later override earlier files (e.g., `03_domain_xact.yaml` overrides `01_shared.yaml`)
4. **Deep Merge for Dicts**: Nested dictionaries merge deeply (merge keys, don't replace entire dict)
5. **Shallow Merge for Arrays**: Arrays/lists replace entirely (don't concatenate)
6. **Structural Equivalence**: Final merged config has same structure as original config

## Test Cases

### 1. Shared Settings Present
**Input**: Merged config from split files
**Expected**:
- `kafka.connection.bootstrap_servers` exists
- `kafka.consumer_defaults` exists with all default keys
- `kafka.producer_defaults` exists with all default keys

### 2. XACT Domain Complete
**Input**: Merged config from split files
**Expected**:
- `kafka.xact.topics` contains all required topic names (events, downloads_pending, downloads_cached, downloads_results, dlq, events_ingested)
- `kafka.xact.event_ingester` exists with consumer/producer/processing sections
- `kafka.xact.download_worker` exists with consumer/producer/processing sections
- `kafka.xact.upload_worker` exists with consumer/producer/processing sections
- `kafka.xact.delta_events_writer` exists with consumer/producer/processing sections
- `kafka.xact.consumer_group_prefix` exists
- `kafka.xact.retry_delays` exists as array
- `kafka.xact.max_retries` exists

### 3. ClaimX Domain Complete
**Input**: Merged config from split files
**Expected**:
- `kafka.claimx.topics` contains all required topic names
- `kafka.claimx.event_ingester` exists
- `kafka.claimx.enrichment_worker` exists
- `kafka.claimx.download_worker` exists
- `kafka.claimx.upload_worker` exists
- `kafka.claimx.delta_events_writer` exists
- `kafka.claimx.consumer_group_prefix` exists
- `kafka.claimx.retry_delays` exists

### 4. Override Precedence
**Scenario**: Domain-specific file overrides shared setting
**Input**:
```yaml
# 01_shared.yaml
kafka:
  consumer_defaults:
    max_poll_records: 1000
    session_timeout_ms: 60000

# 03_domain_xact.yaml
kafka:
  xact:
    event_ingester:
      consumer:
        max_poll_records: 2000  # Override shared default
```
**Expected**: `kafka.xact.event_ingester.consumer.max_poll_records == 2000`
**Validation**: Later file's value takes precedence

### 5. Deep Merge for Nested Dicts
**Scenario**: Nested dict merging preserves both old and new keys
**Input**:
```yaml
# 01_shared.yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"

# 02_connection_override.yaml (if exists)
kafka:
  connection:
    sasl_mechanism: "OAUTHBEARER"
```
**Expected**: All three keys present:
- `kafka.connection.bootstrap_servers == "localhost:9094"`
- `kafka.connection.security_protocol == "PLAINTEXT"`
- `kafka.connection.sasl_mechanism == "OAUTHBEARER"`
**Validation**: Keys from both files are preserved

### 6. Arrays Replace (Don't Merge)
**Scenario**: Array in later file replaces array in earlier file
**Input**:
```yaml
# 01_shared.yaml
kafka:
  xact:
    retry_delays: [300, 600, 1200, 2400]

# 03_domain_xact.yaml
kafka:
  xact:
    retry_delays: [300, 600]  # Override with shorter list
```
**Expected**: `kafka.xact.retry_delays == [300, 600]` (NOT `[300, 600, 1200, 2400]`)
**Validation**: Array completely replaced, not concatenated

### 7. Missing Keys Detected
**Input**: Merged config missing required key
**Expected**: Validator returns error like `["Missing required key: kafka.xact.topics"]`

### 8. Type Mismatches Detected
**Input**: Key has wrong type (e.g., string instead of dict)
**Expected**: Validator returns error like `["Type mismatch at kafka.connection: expected dict, got str"]`

### 9. Extra Keys Allowed
**Input**: Merged config has extra keys not in original
**Expected**: No error (extra keys are allowed for extensibility)

### 10. Structural Equivalence
**Input**: Original monolithic config, Merged split config
**Expected**: After normalizing both (sorting keys, etc.), structures match exactly

## Function Signature

```python
def validate_merged_config(
    merged: Dict[str, Any],
    original: Dict[str, Any]
) -> List[str]:
    """Validate that merged config matches original config.

    Args:
        merged: Configuration from merging split YAML files
        original: Original monolithic configuration from config.yaml

    Returns:
        List of error messages. Empty list means validation passed.

    Examples:
        >>> errors = validate_merged_config(merged_cfg, original_cfg)
        >>> if errors:
        ...     print("Validation failed:")
        ...     for error in errors:
        ...         print(f"  - {error}")
        ... else:
        ...     print("Validation passed!")
    """
```

## CLI Interface

```bash
# Validate merged config matches original
python -m kafka_pipeline.config --validate

# Show merged configuration (for debugging)
python -m kafka_pipeline.config --show-merged

# Validate and show merged (combined)
python -m kafka_pipeline.config --validate --show-merged

# Load from custom directory
python -m kafka_pipeline.config --validate --config-dir /path/to/config/

# JSON output for automation
python -m kafka_pipeline.config --validate --json
```

### CLI Output Examples

**Success**:
```
✓ Configuration validation passed
  - Shared settings: OK
  - XACT domain: OK
  - ClaimX domain: OK
  - Merge integrity: OK
```

**Failure**:
```
✗ Configuration validation failed (3 errors):
  - Missing required key: kafka.xact.topics.events
  - Type mismatch at kafka.connection: expected dict, got str
  - Override not applied: kafka.xact.event_ingester.consumer.max_poll_records
```

**Show Merged**:
```
Merged configuration (from config/ directory):
---
kafka:
  connection:
    bootstrap_servers: localhost:9094
    security_protocol: PLAINTEXT
    ...
  xact:
    topics:
      events: xact.events.raw
      ...
```

## Implementation Notes

1. **Deep Comparison**: Use recursive comparison for nested structures
2. **Type Checking**: Validate types match (dict vs list vs scalar)
3. **Path Reporting**: Error messages include full path (e.g., `kafka.xact.topics.events`)
4. **Normalization**: Before comparison, normalize configs (sort keys, handle null vs missing, etc.)
5. **Backwards Compatibility**: Validator should work with both old (monolithic) and new (split) config loading

## Example Merge Scenario

### Input Files

**01_shared.yaml**:
```yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"
    request_timeout_ms: 120000

  consumer_defaults:
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    max_poll_records: 1000

  producer_defaults:
    acks: "all"
    retries: 3
```

**02_storage.yaml**:
```yaml
kafka:
  storage:
    onelake_base_path: "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files"
    cache_dir: "/tmp/kafka_pipeline_cache"
```

**03_domain_xact.yaml**:
```yaml
kafka:
  xact:
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
      downloads_cached: "xact.downloads.cached"
      downloads_results: "xact.downloads.results"
      dlq: "xact.downloads.dlq"
      events_ingested: "xact.events.ingested"

    consumer_group_prefix: "xact"
    retry_delays: [300, 600]
    max_retries: 4

    event_ingester:
      consumer:
        group_id: "xact-event-ingester"
        max_poll_records: 1000  # Uses shared default
        max_poll_interval_ms: 3000000
        session_timeout_ms: 45000

      producer:
        acks: "1"  # Override shared default
        batch_size: 32768
        linger_ms: 100
        compression_type: "lz4"

      processing:
        batch_size: 1000
        max_batches: null
        health_port: 8080
```

### Expected Merged Output

```yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"
    request_timeout_ms: 120000

  consumer_defaults:
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    max_poll_records: 1000

  producer_defaults:
    acks: "all"
    retries: 3

  storage:
    onelake_base_path: "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files"
    cache_dir: "/tmp/kafka_pipeline_cache"

  xact:
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
      downloads_cached: "xact.downloads.cached"
      downloads_results: "xact.downloads.results"
      dlq: "xact.downloads.dlq"
      events_ingested: "xact.events.ingested"

    consumer_group_prefix: "xact"
    retry_delays: [300, 600]
    max_retries: 4

    event_ingester:
      consumer:
        # Merged: shared defaults + worker-specific overrides
        auto_offset_reset: "earliest"  # From consumer_defaults
        enable_auto_commit: false      # From consumer_defaults
        max_poll_records: 1000          # From consumer_defaults (not overridden)
        group_id: "xact-event-ingester" # Worker-specific
        max_poll_interval_ms: 3000000   # Worker-specific
        session_timeout_ms: 45000       # Worker-specific

      producer:
        # Merged: shared defaults + worker-specific overrides
        acks: "1"                  # OVERRIDE from worker (was "all" in defaults)
        retries: 3                 # From producer_defaults
        batch_size: 32768          # Worker-specific
        linger_ms: 100             # Worker-specific
        compression_type: "lz4"    # Worker-specific

      processing:
        batch_size: 1000
        max_batches: null
        health_port: 8080
```

### Validation Checks

1. **Shared present**: ✓ connection, consumer_defaults, producer_defaults all exist
2. **XACT complete**: ✓ All required topics, workers, and config present
3. **Override precedence**: ✓ `event_ingester.producer.acks == "1"` (overrode default "all")
4. **Deep merge**: ✓ `event_ingester.consumer` has both shared and worker-specific keys
5. **Array replace**: ✓ `retry_delays == [300, 600]` (not concatenated with anything)

## Edge Cases

### Empty Arrays
**Input**: `retry_delays: []`
**Expected**: Treated as "no retries configured" (valid)

### Null Values
**Input**: `max_batches: null`
**Expected**: Treated as "explicitly set to null" (different from missing key)

### Mixed Types in Array
**Input**: `retry_delays: [300, "5m", 600]`
**Expected**: Error - array elements have inconsistent types

### Deeply Nested Overrides (5+ levels)
**Input**: Nested dict 5+ levels deep
**Expected**: Deep merge applies at all levels

### Circular References
**Input**: Config with YAML aliases creating circular refs
**Expected**: Error or detection of cycle

## Success Criteria

The validator passes if:
1. All test cases produce expected results
2. CLI returns exit code 0 on success, 1 on failure
3. Error messages are clear and actionable
4. Performance: validation completes in < 100ms for typical configs
5. No false positives (valid configs never fail)
6. No false negatives (invalid configs always detected)
