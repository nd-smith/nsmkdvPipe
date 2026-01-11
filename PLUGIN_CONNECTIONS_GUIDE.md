# Plugin Connection Infrastructure Guide

This guide explains the connection management infrastructure for the plugin system, including HTTP connections, enrichment pipelines, plugin workers, and built-in logging.

**✨ Updated for commit 6066082** - Now includes built-in logging infrastructure for all plugins.

## Table of Contents

- [Overview](#overview)
- [Built-in Logging Infrastructure](#built-in-logging-infrastructure) ⭐ NEW
- [Connection Manager](#connection-manager)
- [Enrichment Framework](#enrichment-framework)
- [Plugin Workers](#plugin-workers)
- [Configuration](#configuration)
- [Examples](#examples)
- [Integration with Existing Plugins](#integration-with-existing-plugins)

---

## Overview

The connection infrastructure provides three key capabilities for plugins:

1. **Connection Manager**: Centralized HTTP client pool with named connections, authentication, and retry logic
2. **Enrichment Framework**: Pipeline-based data transformation, validation, lookup, and batching
3. **Plugin Workers**: Generic workers that consume plugin-triggered topics and send to external APIs

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Plugin System                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │   Plugin         │         │ Action Executor  │              │
│  │   Execution      │────────▶│ - Kafka publish  │              │
│  │                  │         │ - HTTP webhooks  │              │
│  └──────────────────┘         └────────┬─────────┘              │
│                                        │                          │
│                                        ▼                          │
│                              ┌──────────────────┐                │
│                              │   Connection     │                │
│                              │   Manager        │                │
│                              │ - Named conns    │                │
│                              │ - Auth           │                │
│                              │ - Retry          │                │
│                              │ - Pooling        │                │
│                              └────────┬─────────┘                │
│                                       │                           │
│                                       │                           │
│        Topic: plugin.photo_tasks      │                           │
│               │                       │                           │
│               ▼                       │                           │
│  ┌────────────────────────┐          │                           │
│  │  Plugin Action Worker  │          │                           │
│  │  - Kafka consumer      │          │                           │
│  │  - Enrichment pipeline ├──────────┘                           │
│  │  - API sender          │                                      │
│  └────────────────────────┘                                      │
│               │                                                   │
│               ▼                                                   │
│  ┌─────────────────────────────────────┐                         │
│  │     Enrichment Pipeline             │                         │
│  │  ┌──────────────────────────────┐   │                         │
│  │  │ 1. Transform Handler         │   │                         │
│  │  │    - Field mapping           │   │                         │
│  │  │    - Defaults                │   │                         │
│  │  └──────────────────────────────┘   │                         │
│  │  ┌──────────────────────────────┐   │                         │
│  │  │ 2. Validation Handler        │   │                         │
│  │  │    - Required fields         │   │                         │
│  │  │    - Value constraints       │   │                         │
│  │  │    - Conditional skip        │   │                         │
│  │  └──────────────────────────────┘   │                         │
│  │  ┌──────────────────────────────┐   │                         │
│  │  │ 3. Lookup Handler            │   │                         │
│  │  │    - External API calls      │   │                         │
│  │  │    - Data enrichment         │   │                         │
│  │  │    - Caching                 │   │                         │
│  │  └──────────────────────────────┘   │                         │
│  │  ┌──────────────────────────────┐   │                         │
│  │  │ 4. Batching Handler          │   │                         │
│  │  │    - Message accumulation    │   │                         │
│  │  │    - Batch flush             │   │                         │
│  │  └──────────────────────────────┘   │                         │
│  └─────────────────────────────────────┘                         │
│               │                                                   │
│               ▼                                                   │
│         External API                                              │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

---

## Built-in Logging Infrastructure

**New in commit 6066082:** All plugins now inherit from `LoggedClass`, providing automatic structured logging without manual setup.

### Features

- **Pre-configured Logger**: Every plugin has `self._logger` ready to use
- **Structured Logging**: `self._log(level, msg, **context)` with automatic context extraction
- **Exception Logging**: `self._log_exception(exc, msg, **context)` with full traceback
- **Zero Configuration**: Works out of the box for all plugins

### Basic Usage

```python
from kafka_pipeline.plugins.base import Plugin, PluginContext, PluginResult
import logging

class MyCustomPlugin(Plugin):
    name = "my_plugin"

    def execute(self, context: PluginContext) -> PluginResult:
        # Structured logging with context
        self._log(
            logging.INFO,
            "Processing event",
            event_id=context.event_id,
            event_type=context.event_type
        )

        try:
            # Your plugin logic here
            result = self._process_event(context)

            # Log success with metrics
            self._log(
                logging.DEBUG,
                "Event processed successfully",
                event_id=context.event_id,
                records_processed=result.count
            )

            return PluginResult.ok()

        except Exception as e:
            # Automatic exception logging with context
            self._log_exception(
                e,
                "Failed to process event",
                event_id=context.event_id
            )
            return PluginResult.failed(str(e))
```

### Available Methods

| Method | Description | Example |
|--------|-------------|---------|
| `self._logger` | Pre-configured logger instance | `self._logger.info("message")` |
| `self._log(level, msg, **extra)` | Structured logging with context | `self._log(logging.INFO, "Processing", event_id=id)` |
| `self._log_exception(exc, msg, **extra)` | Exception logging with traceback | `self._log_exception(e, "Failed", event_id=id)` |

### Log Levels

Use appropriate log levels for different scenarios:

```python
# DEBUG - Detailed execution flow
self._log(logging.DEBUG, "Validating field", field_name="email")

# INFO - Important state changes
self._log(logging.INFO, "Task completed", task_id=123, duration_ms=250)

# WARNING - Issues that don't prevent execution
self._log(logging.WARNING, "Data quality issue", field="age", value=-1)

# ERROR - Failures that prevent execution
self._log(logging.ERROR, "Validation failed", errors=error_list)
```

### Best Practices

1. **Always include event_id** for correlation:
   ```python
   self._log(logging.INFO, "Message", event_id=context.event_id)
   ```

2. **Use structured context** (not string formatting):
   ```python
   # ✅ Good - structured context
   self._log(logging.INFO, "Task completed", task_id=123, duration=250)

   # ❌ Bad - string formatting
   logger.info(f"Task {task_id} completed in {duration}ms")
   ```

3. **Use _log_exception for all exceptions**:
   ```python
   # ✅ Good - automatic traceback and context
   except Exception as e:
       self._log_exception(e, "Processing failed", event_id=context.event_id)

   # ❌ Bad - manual exception logging
   except Exception as e:
       logger.error(f"Error: {e}")
   ```

4. **Log operation results**:
   ```python
   self._log(
       logging.INFO,
       "Enrichment completed",
       event_id=context.event_id,
       fields_added=5,
       api_calls=2,
       duration_ms=150
   )
   ```

### Example: Complete Custom Plugin

See `plugins/docs/examples/custom_plugin_example.py` for complete examples including:
- Data quality validation plugin
- Enrichment metrics tracking plugin
- Error recovery plugin
- Best practices and patterns

---

## Connection Manager

The `ConnectionManager` provides a shared HTTP client pool for making requests to external APIs.

### Features

- **Named Connections**: Define connections by name with full configuration
- **Authentication**: Built-in support for Bearer tokens, API keys, Basic auth
- **Retry Logic**: Automatic exponential backoff for failed requests
- **Connection Pooling**: Efficient reuse of HTTP connections
- **Environment Variables**: Reference secrets from environment

### Basic Usage

```python
from kafka_pipeline.plugins.connections import ConnectionManager, ConnectionConfig, AuthType

# Create manager
manager = ConnectionManager()

# Add a connection
manager.add_connection(ConnectionConfig(
    name="external_api",
    base_url="https://api.example.com",
    auth_type=AuthType.BEARER,
    auth_token=os.getenv("EXTERNAL_API_TOKEN"),
    timeout_seconds=30,
    max_retries=3,
))

# Start manager (creates HTTP session)
await manager.start()

# Make requests
response = await manager.request(
    connection_name="external_api",
    method="POST",
    path="/v1/events",
    json={"event": "data"},
)

# Cleanup
await manager.close()
```

### Configuration Schema

```yaml
connections:
  connection_name:
    name: connection_name           # Unique identifier
    base_url: https://api.example.com  # Base URL (no trailing slash)
    auth_type: bearer               # none | bearer | api_key | basic
    auth_token: ${ENV_VAR}          # Token/key (supports env vars)
    auth_header: Authorization      # Header name (auto-detected by default)
    timeout_seconds: 30             # Request timeout
    max_retries: 3                  # Max retry attempts
    retry_backoff_base: 2           # Backoff base (exponential)
    retry_backoff_max: 60           # Max backoff between retries
    headers:                        # Additional headers
      User-Agent: MyApp/1.0
      X-Custom: value
```

### Authentication Types

#### Bearer Token
```yaml
auth_type: bearer
auth_token: ${API_TOKEN}
# Results in: Authorization: Bearer <token>
```

#### API Key
```yaml
auth_type: api_key
auth_token: ${API_KEY}
auth_header: X-API-Key  # Optional, defaults to X-API-Key
# Results in: X-API-Key: <key>
```

#### Basic Auth
```yaml
auth_type: basic
auth_token: ${BASIC_CREDS}  # Base64 encoded "user:pass"
# Results in: Authorization: Basic <creds>
```

#### No Auth
```yaml
auth_type: none
```

---

## Enrichment Framework

The enrichment framework provides a pipeline-based approach to transforming data before sending to external APIs.

### Built-in Handlers

#### 1. Transform Handler

Extract, rename, and set default values for fields.

```yaml
type: transform
config:
  mappings:
    # output_field: input_field
    event_id: event_id
    project_name: task.project.name  # Nested access with dot notation
    status: task_status
  defaults:
    priority: normal  # Set if not present
    source: claimx
```

#### 2. Validation Handler

Validate required fields and apply business rules.

```yaml
type: validation
config:
  required_fields:
    - event_id
    - project_id
  field_rules:
    status:
      allowed_values: [completed, verified, cancelled]
    priority:
      min: 1
      max: 10
  skip_if:
    field: status
    equals: cancelled  # Skip message if status is cancelled
```

#### 3. Lookup Handler

Fetch additional data from external APIs.

```yaml
type: lookup
config:
  connection: internal_service      # Named connection to use
  endpoint: /api/projects/{project_id}  # Supports path params
  path_params:
    project_id: project_id          # Field to use for param
  query_params:
    include: details                # Static query param
    status: task_status             # Field reference
  result_field: project_details     # Where to store result
  cache_ttl: 300                    # Cache for 5 minutes
```

#### 4. Batching Handler

Accumulate messages and send in batches.

```yaml
type: batching
config:
  batch_size: 10                    # Batch size
  batch_timeout_seconds: 5.0        # Or timeout (whichever comes first)
  batch_field: items                # Field name for batch array
```

### Custom Handlers

Create custom handlers by extending `EnrichmentHandler`:

```python
from kafka_pipeline.plugins.enrichment import EnrichmentHandler, EnrichmentContext, EnrichmentResult

class CustomHandler(EnrichmentHandler):
    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        # Your enrichment logic
        context.data["custom_field"] = "value"
        return EnrichmentResult.ok(context.data)
```

Register in config:
```yaml
type: my_module.handlers:CustomHandler
config:
  custom_param: value
```

---

## Plugin Workers

Plugin workers consume messages from plugin-triggered Kafka topics, enrich them, and send to external APIs.

### Features

- **Declarative Configuration**: No code required
- **Enrichment Pipeline**: Apply multiple handlers in sequence
- **Error Handling**: Optional error topic for failed messages
- **Success Tracking**: Optional success topic for audit trail
- **Graceful Shutdown**: Flushes batches before stopping
- **Metrics**: Built-in processing metrics

### Basic Usage

```python
from kafka_pipeline.plugins.workers import PluginActionWorker, WorkerConfig
from kafka_pipeline.plugins.connections import ConnectionManager

# Create worker config
config = WorkerConfig(
    name="my_worker",
    input_topic="plugin.events",
    consumer_group="my_worker_group",
    destination_connection="external_api",
    destination_path="/v1/events",
    enrichment_handlers=[
        {"type": "transform", "config": {...}},
        {"type": "validation", "config": {...}},
    ],
)

# Create worker
worker = PluginActionWorker(
    config=config,
    kafka_config={"bootstrap_servers": "localhost:9092"},
    connection_manager=connection_manager,
    producer=kafka_producer,  # Optional, for success/error topics
)

# Run worker
await worker.start()
await worker.run()
```

### Configuration Schema

```yaml
workers:
  worker_name:
    name: worker_name

    # Kafka config
    input_topic: plugin.topic
    consumer_group: worker_group
    batch_size: 100
    enable_auto_commit: false

    # Destination
    destination_connection: connection_name
    destination_path: /api/endpoint
    destination_method: POST  # GET, PUT, DELETE, etc.
    max_retries: 3

    # Optional result topics
    success_topic: plugin.topic.success
    error_topic: plugin.topic.errors

    # Enrichment pipeline
    enrichment_handlers:
      - type: transform
        config: {...}
      - type: validation
        config: {...}
      - type: lookup
        config: {...}
```

---

## Configuration

### File Structure

```
plugins/config/
├── connections.yaml              # Connection definitions
├── workers.yaml                  # Worker configurations
└── task_trigger/
    └── config.yaml               # Plugin using connections
```

### Loading Configuration

```python
from kafka_pipeline.plugins.connections import ConnectionManager, ConnectionConfig
import yaml

# Load connections
with open("connections.yaml") as f:
    config = yaml.safe_load(f)

manager = ConnectionManager()
for conn_config in config["connections"].values():
    manager.add_connection(ConnectionConfig(**conn_config))

await manager.start()

# Load workers
with open("workers.yaml") as f:
    worker_configs = yaml.safe_load(f)

for worker_config in worker_configs["workers"].values():
    worker = PluginActionWorker(
        config=WorkerConfig(**worker_config),
        kafka_config=kafka_config,
        connection_manager=manager,
    )
    # Start worker...
```

---

## Examples

### Example 1: Simple Plugin Webhook with Named Connection

**Plugin Config:**
```yaml
name: task_notifier
module: kafka_pipeline.plugins.task_trigger
class: TaskTriggerPlugin

config:
  triggers:
    456:
      name: Photo Task
      on_completed:
        webhook:
          connection: external_api  # Named connection
          path: /v1/tasks/completed
          method: POST
        log: "Photo task completed"
```

**Connection Config:**
```yaml
connections:
  external_api:
    name: external_api
    base_url: https://api.example.com
    auth_type: bearer
    auth_token: ${EXTERNAL_API_TOKEN}
    timeout_seconds: 30
    max_retries: 3
```

### Example 2: Complete POC Flow

**Step 1: Plugin publishes to topic**
```yaml
# Plugin config
name: task_trigger
config:
  triggers:
    456:
      name: Photo Task
      on_completed:
        publish_to_topic: plugin.photo_tasks
        log: "Published to worker topic"
```

**Step 2: Worker consumes and enriches**
```yaml
# Worker config
workers:
  photo_worker:
    input_topic: plugin.photo_tasks
    consumer_group: photo_worker_group

    destination_connection: external_api
    destination_path: /v1/photo_tasks

    enrichment_handlers:
      # Extract fields
      - type: transform
        config:
          mappings:
            task_id: task_id
            project_id: project_id
            completed_at: timestamp

      # Validate
      - type: validation
        config:
          required_fields: [task_id, project_id]

      # Enrich with project data
      - type: lookup
        config:
          connection: internal_service
          endpoint: /api/projects/{project_id}
          path_params:
            project_id: project_id
          result_field: project
          cache_ttl: 300
```

**Step 3: Worker sends to external API**
```yaml
# Connection config
connections:
  external_api:
    name: external_api
    base_url: https://api.external.com
    auth_type: bearer
    auth_token: ${EXTERNAL_TOKEN}
```

**Result:** Task data is enriched with project details and sent to external API with automatic retries and authentication.

### Example 3: Batching for Efficiency

```yaml
workers:
  batch_event_worker:
    input_topic: plugin.events
    consumer_group: batch_worker

    destination_connection: external_api
    destination_path: /v1/events/batch
    destination_method: POST

    enrichment_handlers:
      # Transform individual events
      - type: transform
        config:
          mappings:
            id: event_id
            type: event_type
            data: payload

      # Batch them
      - type: batching
        config:
          batch_size: 10              # Send 10 at a time
          batch_timeout_seconds: 5.0  # Or after 5 seconds
          batch_field: events         # {"events": [...]}
```

**Result:** Events are collected and sent in batches of 10 (or after 5 seconds), reducing API calls by 10x.

---

## Integration with Existing Plugins

### Updating ActionExecutor

The `ActionExecutor` now supports both legacy and named connection modes:

**Legacy (still supported):**
```yaml
webhook:
  url: https://api.example.com/webhook
  method: POST
  # Requires http_client in ActionExecutor
```

**New (recommended):**
```yaml
webhook:
  connection: external_api
  path: /webhook
  method: POST
  # Uses ConnectionManager
```

### Initializing with ConnectionManager

```python
from kafka_pipeline.plugins.registry import PluginOrchestrator, ActionExecutor
from kafka_pipeline.plugins.connections import get_connection_manager

# Get or create connection manager
connection_manager = get_connection_manager()

# Add connections (from config)
# ...

await connection_manager.start()

# Create action executor with connection manager
action_executor = ActionExecutor(
    producer=kafka_producer,
    connection_manager=connection_manager,
)

# Create orchestrator
orchestrator = PluginOrchestrator(
    registry=plugin_registry,
    action_executor=action_executor,
)
```

### Worker Lifecycle

```python
# Startup
await connection_manager.start()
await worker.start()

# Run
await worker.run()  # Blocks until shutdown signal

# Shutdown
# Worker automatically:
# 1. Flushes batching handlers
# 2. Commits final offsets
# 3. Closes consumer
# 4. Cleans up enrichment handlers

await connection_manager.close()
```

---

## Best Practices

### Connection Management

1. **Use Named Connections**: Always prefer named connections over direct URLs
2. **Environment Variables**: Store secrets in env vars, reference with `${VAR}`
3. **Timeouts**: Set appropriate timeouts for external services
4. **Retries**: Configure retries based on API characteristics
5. **Connection Pooling**: Reuse ConnectionManager across workers

### Enrichment Pipelines

1. **Transform First**: Extract/transform fields early in pipeline
2. **Validate Early**: Validate and filter before expensive lookups
3. **Cache Lookups**: Use `cache_ttl` for frequently accessed data
4. **Batch When Possible**: Use batching for high-volume streams
5. **Error Handling**: Use error topics to capture failures

### Worker Configuration

1. **Consumer Groups**: Use unique group per worker
2. **Manual Commits**: Disable auto-commit for exactly-once semantics
3. **Batch Sizes**: Tune based on message rate and API capacity
4. **Success/Error Topics**: Enable for audit trail and debugging
5. **Monitoring**: Track worker metrics (processed, succeeded, failed)

### Security

1. **Never Commit Secrets**: Use environment variables
2. **Least Privilege**: API tokens should have minimum required permissions
3. **HTTPS Only**: Always use HTTPS for external APIs
4. **Token Rotation**: Support token rotation via env var updates

---

## Troubleshooting

### Connection Issues

**Problem**: `Connection 'xyz' not found`
**Solution**: Ensure connection is defined in `connections.yaml` and loaded at startup

**Problem**: `ConnectionManager not started`
**Solution**: Call `await manager.start()` before using

**Problem**: `Authentication failed (401)`
**Solution**: Check auth_token env var is set and valid

### Worker Issues

**Problem**: Worker not consuming messages
**Solution**: Check topic name, consumer group, and Kafka connectivity

**Problem**: Messages skipped without processing
**Solution**: Check validation/skip conditions in enrichment handlers

**Problem**: API requests failing
**Solution**: Check destination_connection exists and connection is valid

### Enrichment Issues

**Problem**: `Required field 'X' is missing`
**Solution**: Check field mapping in transform handler or message structure

**Problem**: Lookup handler returns 404
**Solution**: Verify endpoint path and path_params are correct

**Problem**: Batch never flushed
**Solution**: Check batch_size and batch_timeout_seconds configuration

---

## Performance Tuning

### Connection Pool

```python
# Increase pool size for high-throughput
manager = ConnectionManager(
    connector_limit=200,        # Total connections
    connector_limit_per_host=50 # Per host
)
```

### Worker Throughput

```yaml
# Increase batch size for higher throughput
batch_size: 500

# Adjust enrichment for speed
enrichment_handlers:
  - type: lookup
    config:
      cache_ttl: 600  # Cache longer to reduce API calls
```

### Batching Strategy

```yaml
# High volume + API supports batching
type: batching
config:
  batch_size: 50              # Large batches
  batch_timeout_seconds: 1.0  # Short timeout

# Low volume + reduce latency
type: batching
config:
  batch_size: 5               # Small batches
  batch_timeout_seconds: 10.0 # Long timeout
```

---

## API Reference

### ConnectionManager

- `add_connection(config)` - Register a connection
- `get_connection(name)` - Get connection config
- `start()` - Initialize HTTP session
- `close()` - Cleanup resources
- `request(connection_name, method, path, ...)` - Make HTTP request
- `request_json(...)` - Request and parse JSON response

### EnrichmentHandler

- `enrich(context)` - Transform/enrich data
- `initialize()` - One-time setup
- `cleanup()` - Resource cleanup

### PluginActionWorker

- `start()` - Initialize worker
- `run()` - Run processing loop (blocks)
- `stop()` - Graceful shutdown

---

## Migration Guide

### From Direct URLs to Named Connections

**Before:**
```yaml
webhook:
  url: https://api.example.com/v1/webhook
  method: POST
```

**After:**

1. Add connection definition:
```yaml
# connections.yaml
connections:
  my_api:
    name: my_api
    base_url: https://api.example.com
    auth_type: bearer
    auth_token: ${API_TOKEN}
```

2. Update plugin config:
```yaml
webhook:
  connection: my_api
  path: /v1/webhook
  method: POST
```

3. Initialize ActionExecutor with ConnectionManager:
```python
action_executor = ActionExecutor(
    producer=producer,
    connection_manager=connection_manager,
)
```

**Benefits**: Centralized auth, automatic retries, connection pooling

---

## Support

For questions or issues:
1. Check example configs in `plugins/config/*.example.yaml`
2. Review source code documentation in `kafka_pipeline/plugins/`
3. File issues in project repository

---

**Last Updated**: 2026-01-10
**Version**: 1.0.0
