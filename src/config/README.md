# Config Directory - Multi-File Configuration

This directory contains the configuration files for the Kafka pipeline.
The configuration is organized into multiple focused files for better maintainability
and separation of concerns.

## Directory Structure

```
config/
├── README.md                    # This file
├── VALIDATOR_SPEC.md            # Spec for merge validator (Agent 3 - Task C)
├── TESTING_PROGRESS.md          # Testing status and progress (Agent 3 - Task G)
├── shared.yaml                  # Shared settings (connection, defaults, storage)
├── xact_config.yaml             # XACT domain configuration
├── claimx_config.yaml           # ClaimX domain configuration
└── plugins/                     # Plugin-specific configurations
    └── *.yaml                   # Plugin config files (optional)
```

## File Loading Order

Configuration files are loaded in a **specific order**:

1. `shared.yaml` - Loaded first (lowest precedence)
2. `xact_config.yaml`
3. `claimx_config.yaml`
4. `plugins/*.yaml` - Loaded last (highest precedence)

**Later files override earlier files** for conflicting keys.

## Merge Behavior

### Deep Merge for Nested Dicts
Nested dictionaries merge deeply - keys from all files are combined:

```yaml
# shared.yaml
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"

# xact_config.yaml
kafka:
  connection:
    sasl_mechanism: "OAUTHBEARER"  # Adds new key

# Result: All three keys present
kafka:
  connection:
    bootstrap_servers: "localhost:9094"
    security_protocol: "PLAINTEXT"
    sasl_mechanism: "OAUTHBEARER"
```

### Shallow Merge for Arrays
Arrays/lists are **replaced entirely** (not concatenated):

```yaml
# shared.yaml
kafka:
  xact:
    retry_delays: [300, 600, 1200, 2400]

# xact_config.yaml
kafka:
  xact:
    retry_delays: [300, 600]  # Replaces entire array

# Result: [300, 600] (NOT [300, 600, 1200, 2400])
```

## Configuration Structure

The configuration system only supports the multi-file directory structure.
Single-file `config.yaml` is no longer supported.

## Validation

See `VALIDATOR_SPEC.md` for complete validation specification.

### Validate Configuration

```bash
# Validate merged config matches original
python -m kafka_pipeline.config --validate

# Show merged configuration
python -m kafka_pipeline.config --show-merged

# Both validate and show
python -m kafka_pipeline.config --validate --show-merged

# JSON output (for CI/CD)
python -m kafka_pipeline.config --validate --json
```

## Testing

All configuration loading is tested in:
- **Tests**: `/home/nick/projects/nsmkdvPipe/tests/kafka_pipeline/test_config_loading.py`
- **Unit tests** covering:
  - Multi-file config loading from directory
  - Merge precedence and deep merge behavior
  - Environment variable overrides
  - Plugin config loading
  - Validation and error handling

Run tests:
```bash
pytest tests/kafka_pipeline/test_config_loading.py -v
```

## Configuration File Contents

### shared.yaml
- Kafka connection settings (bootstrap_servers, security, etc.)
- Consumer defaults (shared across all consumers)
- Producer defaults (shared across all producers)
- OneLake paths (base path and domain-specific paths)
- Local cache directory
- Event source type (eventhouse/eventhub)
- Global Delta settings (enable_writes)

### xact_config.yaml
- XACT domain topics
- XACT worker configurations (event_ingester, download_worker, upload_worker, delta_events_writer)
- XACT retry configuration
- XACT Delta table paths
- XACT Eventhouse configuration

### claimx_config.yaml
- ClaimX domain topics
- ClaimX API configuration
- ClaimX worker configurations (event_ingester, enrichment_worker, download_worker, upload_worker, delta_events_writer)
- ClaimX retry configuration
- ClaimX Delta table paths (projects, contacts, media, etc.)
- ClaimX Eventhouse configuration

## Plugin Configuration

Plugin-specific configuration files go in `config/plugins/`:

```
config/plugins/
├── my_plugin.yaml
└── another_plugin.yaml
```

Plugins are loaded separately and don't affect main configuration.

## Implementation Status

- ✅ **Task C**: Validator specification complete (`VALIDATOR_SPEC.md`)
- ✅ **Task G**: Unit tests complete (`test_config_loading.py`)
- ✅ **Task E**: Multi-file config loader implemented (`config.py`)
- ⏳ **Task H**: Validator implementation (Agent 4)

### Implementation Details

The multi-file configuration loader has been implemented in `kafka_pipeline/config.py`:

- **Function**: `load_config()` now supports both single-file and multi-file modes
- **Helper**: `_load_multi_file_config()` handles directory-based loading
- **Utility**: `load_yaml()` safely loads individual YAML files
- **Constant**: `DEFAULT_CONFIG_DIR` points to the config directory
- **Backwards Compatibility**: Automatically falls back to `config.yaml` if `config/` doesn't exist

See `TESTING_PROGRESS.md` for detailed status.

## Documentation

- **Validator Spec**: `VALIDATOR_SPEC.md` (374 lines)
- **Testing Progress**: `TESTING_PROGRESS.md` (177 lines)
- **Unit Tests**: `test_config_loading.py` (841 lines)
- **This README**: `README.md`

Total: **1,392 lines of specification, tests, and documentation**

---

**For questions or issues, see the main project documentation or contact the development team.**
