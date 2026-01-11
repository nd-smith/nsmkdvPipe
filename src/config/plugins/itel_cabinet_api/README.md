# iTel Cabinet Task Tracking Plugin

## Overview

This plugin automatically tracks the complete lifecycle of iTel cabinet-related tasks in ClaimX (task ID 32513). Every status change is captured, enriched with full task details from the ClaimX API, and stored in a Delta table for analysis and monitoring.

## What It Does

1. **Monitors** ClaimX task 32513 for any status changes (creation, assignment, progress, completion)
2. **Publishes** events to a Kafka topic for asynchronous processing
3. **Two Independent Workers** process each event:
   - **Tracking Worker**: Enriches with ClaimX API data and writes to Delta table
   - **API Worker**: Enriches with ClaimX API data and sends to iTel Cabinet API
4. **Dual Architecture Benefits**:
   - If one worker fails, the other continues
   - Separate logs and error handling
   - Independent scaling and monitoring

## Quick Start

### Prerequisites

- ClaimX API access credentials
- Kafka cluster running
- Delta Lake storage configured
- Python environment with required dependencies

### Environment Variables

Set these environment variables before starting the workers:

```bash
# Required for both workers
export CLAIMX_API_BASE_URL="https://api.claimx.com"
export CLAIMX_API_TOKEN="your-bearer-token-here"

# Required only for API worker
export ITEL_CABINET_API_BASE_URL="https://api.itelcabinet.com"  # TODO: Update
export ITEL_CABINET_API_TOKEN="your-itel-token-here"            # TODO: Update
```

### Enable the Plugin

The plugin is already configured in `config.yaml`. To enable it:

```yaml
enabled: true  # Set to false to disable
```

### Start the Workers

The plugin requires three components to be running:

1. **ClaimX Enrichment Worker** (runs the plugin)
2. **iTel Cabinet Tracking Worker** (writes to Delta table)
3. **iTel Cabinet API Worker** (sends to iTel API)

```bash
# Start enrichment worker (includes plugin)
python -m kafka_pipeline.workers.claimx_enrichment_worker

# Start tracking worker (in separate terminal)
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker

# Start API worker (in separate terminal)
python -m kafka_pipeline.workers.itel_cabinet_api_worker
```

**Note:** The two iTel workers are independent - if one fails, the other continues.

## Configuration

### Task Tracking

The plugin tracks task ID **32513** for all status changes.

To track additional tasks, edit `config.yaml`:

```yaml
triggers:
  32513:
    name: iTel Cabinet Task Tracking
    on_any:  # Tracks ALL status changes
      publish_to_topic: itel.cabinet.task.tracking
```

### Worker Configuration

Worker settings are in `plugins/config/workers.yaml`:

```yaml
itel_cabinet_tracking_worker:
  input_topic: itel.cabinet.task.tracking
  consumer_group: itel_cabinet_tracking_worker_group
  # ... enrichment pipeline configuration
```

## Data Flow

```
ClaimX Event → Plugin Detects → Kafka Topic
   (Task        (task_id          (itel.cabinet.task.tracking)
    32513        = 32513)                    ↓
    status                          ┌────────┴────────┐
    change)                         ↓                 ↓
                            Tracking Worker    API Worker
                            (ClaimX API        (ClaimX API
                             lookup)            lookup)
                                    ↓                 ↓
                            Delta Table        iTel API
                            (historical)       (integration)
```

See [DUAL_WORKER_ARCHITECTURE.md](./DUAL_WORKER_ARCHITECTURE.md) for detailed architecture documentation.

## Querying Tracked Data

### View Task History

```sql
SELECT
  event_timestamp,
  task_status,
  task_name,
  assigned_to_user_id
FROM itel_cabinet_task_tracking
WHERE task_id = 32513
ORDER BY event_timestamp DESC;
```

### Analyze Status Transitions

```sql
SELECT
  task_id,
  task_status,
  COUNT(*) as status_count,
  MIN(event_timestamp) as first_seen,
  MAX(event_timestamp) as last_seen
FROM itel_cabinet_task_tracking
WHERE year = 2026 AND month = 1
GROUP BY task_id, task_status;
```

### Track Task Completion Time

```sql
SELECT
  task_id,
  assignment_id,
  task_created_at,
  task_completed_at,
  DATEDIFF(task_completed_at, task_created_at) as days_to_complete
FROM itel_cabinet_task_tracking
WHERE task_completed_at IS NOT NULL
ORDER BY days_to_complete DESC;
```

## Monitoring

### Check Plugin Status

```bash
# View plugin logs
tail -f logs/claimx/enrichment_worker.log | grep "iTel Cabinet"

# Expected output:
# INFO: iTel Cabinet task event tracked | event_id=abc123 | task_id=32513 | status=assigned
```

### Check Worker Status

```bash
# View worker logs
tail -f logs/itel/tracking_worker.log

# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group itel_cabinet_tracking_worker_group
```

### Metrics

Monitor these metrics in your observability platform:

- `itel_plugin_events_triggered_total` - Events detected by plugin
- `itel_worker_messages_processed_total` - Events processed by worker
- `itel_worker_delta_writes_total` - Records written to Delta table
- `itel_worker_claimx_api_calls_total` - API enrichment calls made

## Troubleshooting

### Plugin Not Triggering

**Problem:** No events published to `itel.cabinet.task.tracking` topic

**Solutions:**
1. Check plugin is enabled: `enabled: true` in `config.yaml`
2. Verify task_id in ClaimX events matches 32513
3. Check enrichment worker is running and processing events
4. Enable DEBUG logging to see plugin execution

### Worker Not Processing

**Problem:** Messages stuck in topic, not written to Delta table

**Solutions:**
1. Check worker is running: `ps aux | grep itel_cabinet_tracking_worker`
2. Verify Kafka connectivity: Can consumer reach topic?
3. Check ClaimX API credentials: `CLAIMX_API_TOKEN` set correctly?
4. Review worker logs for errors

### ClaimX API Failures

**Problem:** `itel_worker_claimx_api_errors_total` increasing

**Solutions:**
1. Verify API credentials are valid and not expired
2. Check ClaimX API health and rate limits
3. Review API response errors in worker logs
4. Increase cache TTL to reduce API calls

### Delta Table Write Errors

**Problem:** `itel_worker_delta_write_errors_total` increasing

**Solutions:**
1. Check Delta table exists and is accessible
2. Verify write permissions to Delta table location
3. Check for schema conflicts (enable schema evolution if needed)
4. Review Delta table logs for specific errors

## Advanced Configuration

### Adjust API Cache Duration

Reduce ClaimX API calls by increasing cache time:

```yaml
enrichment_handlers:
  - type: lookup
    config:
      cache_ttl: 300  # Cache for 5 minutes (default: 60s)
```

### Custom Field Mapping

Add additional fields to track:

```yaml
enrichment_handlers:
  - type: transform
    config:
      mappings:
        # Add custom fields here
        custom_field: task.metadata.custom_field
```

### Error Handling

Configure what happens when enrichment fails:

```yaml
enrichment_handlers:
  - type: lookup
    config:
      on_error: log_and_continue  # or "fail" to stop processing
```

## Kafka Topics

| Topic | Purpose | Retention |
|-------|---------|-----------|
| `itel.cabinet.task.tracking` | Plugin → Worker messages | 7 days |
| `itel.cabinet.task.tracking.success` | Successfully processed events | 3 days |
| `itel.cabinet.task.tracking.errors` | Failed events (DLQ) | 30 days |

## Delta Table

**Table:** `itel_cabinet_task_tracking`

**Location:** `s3://your-bucket/delta/itel_cabinet_task_tracking`

**Schema:** See [SPEC.md](./SPEC.md#delta-table-schema) for full schema definition

**Partitioning:** By date (year, month, day) for query performance

## Architecture

For detailed architecture, data flow, and technical specifications, see [SPEC.md](./SPEC.md).

## Extending the Plugin

The iTel Cabinet plugin uses the TaskTriggerPlugin base class, which inherits from LoggedClass. If you need to create a custom plugin, you automatically get:

**Built-in Logging:**
```python
from kafka_pipeline.plugins.base import Plugin

class CustomPlugin(Plugin):
    def execute(self, context):
        # Use structured logging
        self._log(logging.INFO, "Processing event", event_id=context.event_id)

        try:
            # Your logic here
            result = process_data()
        except Exception as e:
            # Exception logging with context
            self._log_exception(e, "Failed to process", event_id=context.event_id)
            raise

        return PluginResult.ok()
```

**Available logging methods:**
- `self._logger` - Pre-configured logger instance
- `self._log(level, msg, **extra)` - Structured logging with automatic context
- `self._log_exception(exc, msg, **extra)` - Exception logging with full traceback

See `plugins/docs/examples/custom_plugin_example.py` for complete examples.

## Support

### Getting Help

1. Check this README for common issues
2. Review [SPEC.md](./SPEC.md) for detailed specifications
3. Check worker logs for specific errors
4. Review [Plugin Connections Guide](../../../PLUGIN_CONNECTIONS_GUIDE.md)

### Reporting Issues

Include the following in your issue report:

- Plugin configuration (`config.yaml`)
- Worker logs (last 50 lines)
- Kafka consumer lag
- ClaimX API response (if applicable)
- Delta table query results (if applicable)

## Related Documentation

- [SPEC.md](./SPEC.md) - Technical specification
- [Plugin Connections Guide](../../../PLUGIN_CONNECTIONS_GUIDE.md) - Plugin system overview
- [Task Trigger Plugin](../../../plugins/config/task_trigger/README.md) - Base plugin documentation

---

**Last Updated:** 2026-01-10
**Version:** 1.0.0
**Status:** Ready for Implementation
