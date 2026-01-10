# Configuration Loading and Merge Precedence

**Status:** Active (Config Refactor 2026-01)
**Last Updated:** 2026-01-10

---

## Overview

The Kafka pipeline uses a multi-file configuration system that organizes configuration by concern (shared settings, domain-specific settings, plugins). The system uses predictable merge precedence to combine settings from multiple sources.

### Key Features

- **Multi-file structure**: Split configuration across multiple YAML files
- **Deep merging**: Nested dictionaries are intelligently merged
- **Override hierarchy**: Later files override earlier files
- **Environment variable support**: Environment variables have highest precedence
- **Plugin support**: Dynamic plugin configuration via `plugins/*.yaml`

---

## Configuration File Structure

### Directory Layout

```
/home/nick/projects/nsmkdvPipe/
├── config/
│   ├── shared.yaml              # Shared Kafka connection settings
│   ├── xact_config.yaml         # XACT domain configuration
│   ├── claimx_config.yaml       # ClaimX domain configuration
│   └── plugins/
│       ├── task_triggers.yaml   # Task trigger plugin config
│       ├── monitoring.yaml      # Monitoring plugin config (optional)
│       └── *.yaml               # Additional plugin configs
└── src/
    └── config.yaml.old          # Archived legacy config (for reference only)
```

### Loading Order and Precedence

Configuration files are loaded in the following order, with **later files overriding earlier files**:

```
┌─────────────────────────────────────────────────────────────┐
│                    CONFIGURATION PRECEDENCE                  │
│                   (Lowest to Highest Priority)               │
└─────────────────────────────────────────────────────────────┘

   LOWEST PRIORITY
        │
        ▼
┌─────────────────┐
│  1. shared.yaml │  ◄─ Base Kafka connection & defaults
└─────────────────┘
        │
        ▼
┌──────────────────────┐
│  2. xact_config.yaml │  ◄─ XACT domain settings
└──────────────────────┘
        │
        ▼
┌────────────────────────┐
│ 3. claimx_config.yaml  │  ◄─ ClaimX domain settings
└────────────────────────┘
        │
        ▼
┌────────────────────────┐
│  4. plugins/*.yaml     │  ◄─ Plugin configurations (alphabetical)
└────────────────────────┘
        │
        ▼
┌────────────────────────┐
│ 5. Environment Vars    │  ◄─ Runtime overrides (highest priority)
└────────────────────────┘
        │
        ▼
   HIGHEST PRIORITY
```

---

## Merge Rules

The configuration system uses **deep merging** with specific rules for different data types:

### 1. Nested Dictionaries: Deep Merge

Nested dictionaries are **recursively merged**, allowing fine-grained overrides.

**Example:**

```yaml
# shared.yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9092"
    security_protocol: "PLAINTEXT"
    request_timeout_ms: 120000
  consumer_defaults:
    max_poll_records: 100
    session_timeout_ms: 30000

# xact_config.yaml
kafka:
  connection:
    bootstrap_servers: "prod-broker:9092"  # Override just this field
  consumer_defaults:
    max_poll_records: 50                    # Override just this field
```

**Merged Result:**

```yaml
kafka:
  connection:
    bootstrap_servers: "prod-broker:9092"   # ✓ Overridden from xact_config.yaml
    security_protocol: "PLAINTEXT"          # ✓ Kept from shared.yaml
    request_timeout_ms: 120000              # ✓ Kept from shared.yaml
  consumer_defaults:
    max_poll_records: 50                    # ✓ Overridden from xact_config.yaml
    session_timeout_ms: 30000               # ✓ Kept from shared.yaml
```

### 2. Arrays/Lists: Complete Replacement

Arrays are **replaced entirely**, not merged.

**Example:**

```yaml
# shared.yaml
kafka:
  xact:
    retry_delays: [300, 600, 1200, 2400]

# xact_config.yaml
kafka:
  xact:
    retry_delays: [60, 300]  # Replaces entire array
```

**Merged Result:**

```yaml
kafka:
  xact:
    retry_delays: [60, 300]  # ✓ Complete replacement (not [60, 300, 1200, 2400])
```

### 3. Primitive Values: Direct Replacement

Strings, numbers, booleans are directly replaced.

```yaml
# shared.yaml
claimx_api_timeout_seconds: 30

# claimx_config.yaml
claimx_api_timeout_seconds: 60  # Replaces 30
```

### 4. Environment Variables: Highest Priority

Environment variables override **all YAML settings** and are applied after file merging.

**Example:**

```bash
export CLAIMX_API_TOKEN="secret-token-123"
export BOOTSTRAP_SERVERS="prod-kafka:9092"
```

These take precedence over any YAML configuration.

---

## Visual Merge Example

### Scenario: Three-File Configuration

**shared.yaml** (Base settings):
```yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9092"
    security_protocol: "PLAINTEXT"
    request_timeout_ms: 120000
  consumer_defaults:
    max_poll_records: 100
    session_timeout_ms: 30000
    auto_offset_reset: "earliest"
  producer_defaults:
    acks: "all"
    retries: 3
```

**xact_config.yaml** (XACT domain):
```yaml
kafka:
  connection:
    bootstrap_servers: "xact-broker:9092"
  xact:
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
    download_worker:
      consumer:
        max_poll_records: 50  # Override default
      processing:
        concurrency: 10
```

**claimx_config.yaml** (ClaimX domain):
```yaml
kafka:
  connection:
    security_protocol: "SASL_SSL"  # Override for ClaimX
  claimx:
    topics:
      events: "claimx.events.raw"
      enrichment_pending: "claimx.enrichment.pending"
claimx:
  api:
    base_url: "https://api.claimx.example.com"
    timeout_seconds: 60
```

**plugins/task_triggers.yaml** (Plugin config):
```yaml
task_triggers:
  - name: "photo_trigger"
    enabled: true
    triggers:
      - task_id: 456
        on_completed:
          publish_to_topic: "claimx-task-photo-completed"
```

### Final Merged Configuration

```yaml
kafka:
  connection:
    bootstrap_servers: "xact-broker:9092"      # From xact_config.yaml
    security_protocol: "SASL_SSL"              # From claimx_config.yaml (last wins)
    request_timeout_ms: 120000                 # From shared.yaml (not overridden)
  consumer_defaults:
    max_poll_records: 100                      # From shared.yaml
    session_timeout_ms: 30000                  # From shared.yaml
    auto_offset_reset: "earliest"              # From shared.yaml
  producer_defaults:
    acks: "all"                                # From shared.yaml
    retries: 3                                 # From shared.yaml
  xact:
    topics:
      events: "xact.events.raw"
      downloads_pending: "xact.downloads.pending"
    download_worker:
      consumer:
        max_poll_records: 50                   # Overridden in xact_config.yaml
      processing:
        concurrency: 10
  claimx:
    topics:
      events: "claimx.events.raw"
      enrichment_pending: "claimx.enrichment.pending"

claimx:
  api:
    base_url: "https://api.claimx.example.com"
    timeout_seconds: 60

task_triggers:
  - name: "photo_trigger"
    enabled: true
    triggers:
      - task_id: 456
        on_completed:
          publish_to_topic: "claimx-task-photo-completed"
```

---

## Common Configuration Scenarios

### Scenario 1: Separate Development and Production Settings

**Strategy**: Use environment-specific shared.yaml files

```yaml
# config/shared.dev.yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9092"
    security_protocol: "PLAINTEXT"

# config/shared.prod.yaml
kafka:
  connection:
    bootstrap_servers: "prod-kafka.example.com:9093"
    security_protocol: "SASL_SSL"
    sasl_mechanism: "SCRAM-SHA-512"
```

**Usage:**
```bash
# Development
ln -s shared.dev.yaml config/shared.yaml

# Production
ln -s shared.prod.yaml config/shared.yaml
```

### Scenario 2: Local Developer Overrides

**Strategy**: Use `config/local.yaml` (gitignored) for personal overrides

```yaml
# config/local.yaml (not committed to git)
kafka:
  connection:
    bootstrap_servers: "localhost:19092"  # Custom local port
  consumer_defaults:
    max_poll_records: 10  # Smaller batches for debugging
```

Load with:
```python
config = load_config(local_overrides="config/local.yaml")
```

### Scenario 3: Domain-Specific Connection Settings

**Problem**: XACT and ClaimX use different Kafka clusters

**Solution**: Override connection in domain config files

```yaml
# xact_config.yaml
kafka:
  connection:
    bootstrap_servers: "xact-kafka:9092"
  xact:
    topics:
      events: "xact.events.raw"

# claimx_config.yaml
kafka:
  connection:
    bootstrap_servers: "claimx-kafka:9092"  # Different cluster!
  claimx:
    topics:
      events: "claimx.events.raw"
```

**Result**: Last loaded config (claimx_config.yaml) determines the connection. If you need separate connections per domain, this architecture needs extension.

### Scenario 4: Plugin Configuration Without Touching Core Config

**Strategy**: Drop plugin YAML files into `config/plugins/`

```yaml
# config/plugins/new_feature.yaml
new_feature:
  enabled: true
  settings:
    threshold: 100
    timeout: 30
```

Plugin configs are automatically loaded and merged alphabetically.

---

## Configuration Validation

### Automatic Validation

The `load_config()` function automatically validates:

- Required fields present (e.g., `bootstrap_servers`)
- Kafka constraint compliance (e.g., `heartbeat_interval_ms < session_timeout_ms/3`)
- Numeric ranges (e.g., `concurrency` between 1-50)
- Enum values (e.g., `auto_offset_reset` in `["earliest", "latest", "none"]`)

### Manual Validation

Validate your configuration before deployment:

```bash
# Validate merged config
python -m kafka_pipeline.config --validate

# Show merged config (useful for debugging)
python -m kafka_pipeline.config --show-merged
```

Example output:
```
✓ Configuration valid
✓ Loaded 4 files: shared.yaml, xact_config.yaml, claimx_config.yaml, plugins/task_triggers.yaml
✓ All Kafka constraints satisfied
✓ Consumer timeout constraints valid
✓ Producer settings valid
```

---

## Debugging Configuration Issues

### Problem: "Configuration file not found"

**Cause**: `load_config()` couldn't find config files

**Solution**: Check file paths and loading order

```bash
# Show which files are being loaded
python -m kafka_pipeline.config --show-merged --verbose

# Output:
# Loading: /home/nick/projects/nsmkdvPipe/config/shared.yaml ✓
# Loading: /home/nick/projects/nsmkdvPipe/config/xact_config.yaml ✓
# Loading: /home/nick/projects/nsmkdvPipe/config/claimx_config.yaml ✗ (not found)
```

### Problem: "My setting is being overridden"

**Cause**: Later file in load order is overriding your setting

**Solution**: Check merge precedence

```bash
# Show final merged config with source annotations
python -m kafka_pipeline.config --show-merged --trace-sources

# Output shows which file each setting came from:
# kafka.connection.bootstrap_servers: "prod-kafka:9092" (from: xact_config.yaml)
# kafka.consumer_defaults.max_poll_records: 100 (from: shared.yaml)
```

### Problem: "Array not merging as expected"

**Cause**: Arrays are replaced, not merged

**Solution**: Define complete array in the file with highest precedence

```yaml
# ✗ WRONG - Expecting arrays to merge
# shared.yaml
retry_delays: [300, 600, 1200]

# xact_config.yaml
retry_delays: [60]  # You expect [60, 300, 600, 1200] but get [60]

# ✓ CORRECT - Define complete array
# xact_config.yaml
retry_delays: [60, 300, 600, 1200]  # Full array
```

### Problem: "Environment variable not overriding YAML"

**Cause**: Environment variable not set or misspelled

**Solution**: Check environment variable names

```bash
# List current environment variables
env | grep -E "KAFKA|CLAIMX"

# Verify variable is set
echo $CLAIMX_API_TOKEN

# Set if missing
export CLAIMX_API_TOKEN="your-token"
```

---

## Best Practices

### 1. Organize by Concern

- **shared.yaml**: Connection settings, defaults common to all domains
- **domain_config.yaml**: Domain-specific topics, workers, retry policies
- **plugins/*.yaml**: Plugin-specific configuration

### 2. Use Comments Generously

```yaml
kafka:
  consumer_defaults:
    # Kafka requirement: heartbeat_interval_ms < session_timeout_ms/3
    # Recommended: session_timeout_ms = 30000, heartbeat_interval_ms = 3000
    session_timeout_ms: 30000
    heartbeat_interval_ms: 3000
```

### 3. Validate Before Deployment

```bash
# Always validate before deploying
python -m kafka_pipeline.config --validate

# Check merged config matches expectations
python -m kafka_pipeline.config --show-merged | grep -A5 "bootstrap_servers"
```

### 4. Document Override Rationale

When overriding defaults, document why:

```yaml
kafka:
  xact:
    download_worker:
      consumer:
        # Override: XACT downloads are large (100MB+), process fewer per batch
        max_poll_records: 10  # Default is 100
```

### 5. Use Version Control for Config

- Commit all config files to git
- Use `.gitignore` for secrets and local overrides
- Document config changes in commit messages

```gitignore
# .gitignore
config/local.yaml
config/secrets.yaml
config/*.env
```

### 6. Keep Secrets Out of YAML

Use environment variables for sensitive data:

```yaml
# ✗ BAD - Secret in YAML
claimx:
  api:
    token: "secret-token-123"

# ✓ GOOD - Secret in environment
claimx:
  api:
    token: "${CLAIMX_API_TOKEN}"  # Loaded from env var
```

---

## Configuration Schema Reference

See the inline documentation in `kafka_pipeline/config.py` for the complete schema reference, including:

- All available configuration keys
- Data types and constraints
- Default values
- Validation rules

---

## Troubleshooting

### Issue: Config validation fails with timeout constraint error

**Error:**
```
ValueError: consumer_defaults: heartbeat_interval_ms (10000) must be < session_timeout_ms/3 (10000.0)
```

**Solution:** Ensure `heartbeat_interval_ms < session_timeout_ms / 3`

```yaml
# Fix:
consumer_defaults:
  session_timeout_ms: 30000
  heartbeat_interval_ms: 3000  # 3000 < 30000/3 = 10000 ✓
```

### Issue: Plugin config not loading

**Cause:** Plugin YAML file not in `config/plugins/` directory

**Solution:** Move plugin config to correct location

```bash
# Move plugin config
mv my_plugin.yaml config/plugins/my_plugin.yaml

# Verify it loads
python -m kafka_pipeline.config --show-merged | grep -A10 "my_plugin"
```

---

## References

- `kafka_pipeline/config.py` - Configuration loading implementation
- `config/` - Multi-file configuration directory
- `docs/archive/config-refactor-plan.md` - Original refactor planning doc
- [Apache Kafka Configuration Documentation](https://kafka.apache.org/documentation/#configuration)

---

## Changelog

### 2026-01-10
- Removed backwards compatibility with single-file config.yaml
- Updated documentation to reflect multi-file only support
- Archived config.yaml to config.yaml.old

### 2026-01-10 (Initial)
- Initial documentation for multi-file config system
- Added merge precedence rules and visual diagrams
- Added common scenarios and troubleshooting guide
