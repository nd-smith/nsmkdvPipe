# Plugin Configuration Directory

This directory contains individual plugin configurations. Each plugin has its own subdirectory with a `config.yaml` file.

## Directory Structure

```
plugins/config/
├── README.md (this file)
├── task_trigger/
│   ├── config.yaml       # Plugin configuration
│   └── README.md         # Plugin documentation
├── event_filter/         # (example - create when needed)
│   ├── config.yaml
│   └── README.md
└── custom_metrics/       # (example - create when needed)
    ├── config.yaml
    └── README.md
```

## How Plugins Are Loaded

The enrichment worker automatically scans this directory on startup:
1. Finds all subdirectories in `plugins/config/`
2. Looks for `config.yaml` in each subdirectory
3. Loads and registers each plugin found
4. Plugins are executed based on their configured stages and priorities

## Creating a New Plugin

### Option 1: Use an Existing Plugin Class

1. **Create plugin directory:**
   ```bash
   mkdir plugins/config/my_plugin
   ```

2. **Create `config.yaml`:**
   ```yaml
   name: "my_plugin"
   module: "kafka_pipeline.plugins.task_trigger"  # Use existing plugin
   class: "TaskTriggerPlugin"
   enabled: true
   priority: 100
   config:
     # Plugin-specific configuration
   ```

3. **Restart the enrichment worker** to load the new plugin.

### Option 2: Implement a Custom Plugin Class

1. **Create plugin class in code:**
   ```python
   # kafka_pipeline/plugins/my_custom_plugin.py
   from kafka_pipeline.plugins.base import Plugin, PluginContext, PluginResult

   class MyCustomPlugin(Plugin):
       name = "my_custom_plugin"
       # ... implement execute() method
   ```

2. **Create plugin directory:**
   ```bash
   mkdir plugins/config/my_custom_plugin
   ```

3. **Create `config.yaml`:**
   ```yaml
   name: "my_custom_plugin"
   module: "kafka_pipeline.plugins.my_custom_plugin"
   class: "MyCustomPlugin"
   enabled: true
   priority: 100
   config:
     # Your configuration here
   ```

4. **Restart the enrichment worker.**

## Plugin Configuration Format

Each `config.yaml` must include:

```yaml
# Required fields
name: "plugin_name"           # Unique plugin identifier
module: "python.module.path"  # Python module containing the plugin class
class: "PluginClassName"      # Plugin class name
enabled: true                 # Enable/disable the plugin

# Optional fields
priority: 50                  # Execution order (lower = runs first)

# Plugin-specific configuration
config:
  # This section is passed to the plugin's __init__(config=...)
  # Structure depends on the plugin class
```

## Available Plugins

### task_trigger
**Purpose:** Trigger actions when specific ClaimX task types are assigned or completed
**Location:** `plugins/config/task_trigger/`
**Documentation:** See `plugins/config/task_trigger/README.md`

**Use cases:**
- Publish to Kafka topics when tasks complete
- Call webhooks to notify external systems
- Log audit trails for specific task types

## Disabling Plugins

### Disable a specific plugin:
Edit the plugin's `config.yaml`:
```yaml
enabled: false
```

### Disable all plugins:
Rename or remove the `plugins/config` directory.

### Temporarily disable plugins:
Configure the enrichment worker:
```yaml
kafka:
  claimx:
    enrichment_worker:
      processing:
        plugins_dir: ""  # Empty string disables plugin loading
```

## Plugin Execution Flow

```
Event Received → Handler (API Call) → Enriched Entities
                                            ↓
                                  Plugin Orchestrator
                                            ↓
                        ┌───────────────────┴───────────────────┐
                        ↓                                       ↓
                  Plugin Priority 10                     Plugin Priority 100
                  (runs first)                           (runs last)
                        ↓                                       ↓
                  PluginResult                           PluginResult
                        ↓                                       ↓
                    Actions Executed                      Actions Executed
                        └───────────────────┬───────────────────┘
                                            ↓
                                  If not terminated:
                                    Continue Pipeline
                                            ↓
                                  Entity Write → Delta Tables
```

## Best Practices

1. **One plugin per directory** - Keeps configuration organized
2. **Include README.md** - Document plugin purpose and configuration
3. **Use descriptive names** - Both directory and plugin name should be clear
4. **Set appropriate priorities** - Lower numbers run first
5. **Test with enabled: false** - Verify plugin doesn't break if disabled
6. **Version control** - Commit plugin configs to track changes
7. **Document triggers** - Comment complex trigger logic in config

## Troubleshooting

### Plugin not loading
```
WARNING: No config file found in plugin directory
```
- Ensure `config.yaml` exists (not `config.yml`)
- Check file permissions

### Plugin import error
```
ERROR: Failed to import plugin class | module=kafka_pipeline.plugins.bad_plugin
```
- Verify module and class names are correct
- Ensure plugin class exists and is properly imported

### Plugin not executing
```
DEBUG: Plugin loaded | plugin_name=my_plugin
# But no execution logs
```
- Check plugin's `domains`, `stages`, and `event_types` filters
- Verify events matching those filters are being processed
- Enable DEBUG logging to see why plugin.should_run() returns False

## Configuration Override

Override plugin directory in worker config:

```yaml
# config/claimx_config.yaml
kafka:
  claimx:
    enrichment_worker:
      processing:
        plugins_dir: "/custom/path/to/plugins"
```

Default: `plugins/config` (relative to working directory)

## Examples

See individual plugin directories for specific examples:
- `task_trigger/README.md` - Task-based triggers with webhooks and Kafka
- (Add more as plugins are created)
