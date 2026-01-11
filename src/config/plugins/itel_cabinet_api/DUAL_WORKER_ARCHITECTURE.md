# iTel Cabinet Plugin - Dual Worker Architecture

**Created:** 2026-01-10
**Status:** Ready for Implementation

---

## Overview

The iTel Cabinet plugin uses a **dual-worker architecture** where two independent workers process the same events for different purposes:

1. **Tracking Worker** - Writes to Delta table for historical tracking
2. **API Worker** - Sends data to iTel Cabinet API for integration

Both workers operate independently with separate logs, error handling, and scaling.

---

## Architecture Diagram

```
ClaimX Task Event (task_id=32513)
         ↓
   Plugin Triggers
         ↓
   Publishes to Kafka Topic:
   itel.cabinet.task.tracking
         ↓
   ┌─────┴─────────┐
   ↓               ↓
Worker 1          Worker 2
(Tracking)        (API)
         ↓               ↓
   ┌─────┴─────┐   ┌─────┴─────┐
   │Transform  │   │Transform  │
   │Validate   │   │Validate   │
   │ClaimX API │   │ClaimX API │
   │Lookup     │   │Lookup     │
   └─────┬─────┘   └─────┬─────┘
         ↓               ↓
   Delta Write     iTel API Send
         ↓               ↓
   Delta Table     iTel Cabinet
   (tracking)      (integration)
```

---

## Why Dual Workers?

### Independent Processing
- **Failure Isolation**: If one worker fails, the other continues
- **Independent Scaling**: Scale workers based on their specific needs
- **Separate Logs**: Each worker has its own log file for debugging
- **Different Error Handling**: Delta failures don't affect API calls (and vice versa)

### API First, Delta Second
- **API Priority**: iTel integration is the primary goal
- **Delta Tracking**: Historical data is secondary
- **No Dependencies**: Neither worker waits for the other

### Operational Benefits
- **Monitor Separately**: Different metrics and alerts per worker
- **Deploy Independently**: Update one worker without affecting the other
- **Debug Easier**: Logs are separated by concern

---

## Workers Comparison

| Feature | Tracking Worker | API Worker |
|---------|----------------|------------|
| **Purpose** | Historical tracking | iTel integration |
| **Consumer Group** | `itel_cabinet_tracking_worker_group` | `itel_cabinet_api_worker_group` |
| **Input Topic** | `itel.cabinet.task.tracking` | `itel.cabinet.task.tracking` (same) |
| **Success Topic** | `itel.cabinet.task.tracking.success` | `itel.cabinet.api.success` |
| **Error Topic** | `itel.cabinet.task.tracking.errors` | `itel.cabinet.api.errors` |
| **Batch Size** | 100 | 50 |
| **Final Handler** | DeltaTableWriter | ItelApiSender |
| **Logs** | `logs/itel/tracking_worker.log` | `logs/itel/api_worker.log` |
| **Priority** | Secondary | Primary |

---

## Enrichment Pipelines

### Both Workers Share First 3 Stages

**Stage 1: Transform**
- Extract fields from event
- Add metadata

**Stage 2: Validate**
- Check required fields
- Verify task_id = 32513

**Stage 3: ClaimX API Lookup**
- Fetch full task details
- Cache for 60 seconds
- Shared cache between workers (same connection)

### Workers Diverge at Stage 4

**Tracking Worker Stage 4:**
```yaml
- type: kafka_pipeline.plugins.handlers.delta_writer:DeltaTableWriter
  config:
    table_name: itel_cabinet_task_tracking
    mode: append
    column_mapping: { ... }
```

**API Worker Stage 4:**
```yaml
- type: kafka_pipeline.plugins.itel_cabinet_api.handlers.itel_api_sender:ItelApiSender
  config:
    connection: itel_cabinet_api
    endpoint: /api/v1/tasks
    method: POST
```

---

## Data Flow

### Same Topic, Different Consumers

```
Plugin Publishes:
  Topic: itel.cabinet.task.tracking
  Payload: { event_id, task_id, project_id, ... }
         ↓
   Kafka Broker
         ↓
   ┌─────┴──────┐
   ↓            ↓
Worker 1       Worker 2
Group A        Group B
         ↓            ↓
   Read         Read
   (offset 0)   (offset 0)
         ↓            ↓
   Process      Process
   Independently
```

**Key Point:** Both workers get ALL messages because they use different consumer groups.

### Payload Transformation

**Event from Plugin:**
```json
{
  "trigger_name": "iTel Cabinet Repair Form Task",
  "event_id": "abc123",
  "event_type": "CUSTOM_TASK_COMPLETED",
  "task_id": 32513,
  "assignment_id": 12345,
  "project_id": "PRJ-001",
  "task_status": "completed",
  "task": { /* full task data */ },
  "project": { /* project data */ }
}
```

**After ClaimX API Enrichment (both workers):**
```json
{
  /* All fields above, plus: */
  "claimx_task_details": {
    /* Full task from ClaimX REST API */
    "custom_fields": { "cabinet_id": "CAB-123", ... },
    "attachments": [ ... ],
    "notes": "Repair completed"
  }
}
```

**Tracking Worker Output (Delta Table):**
```json
{
  "event_id": "abc123",
  "task_id": 32513,
  "task_status": "completed",
  "claimx_task_full": "{ JSON string }",
  /* Partitioned by year/month/day */
}
```

**API Worker Output (iTel API):**
```json
{
  "task_id": 32513,
  "assignment_id": 12345,
  "project_id": "PRJ-001",
  "task_status": "completed",
  "repair_details": {
    "cabinet_id": "CAB-123",
    "technician_notes": "Repair completed"
  },
  "source_system": "claimx"
}
```

---

## Configuration Files

### Connections

**ClaimX API** (`config/plugins/shared/connections/claimx.yaml`)
- Used by BOTH workers for task enrichment
- Shared cache improves performance

**iTel API** (`config/plugins/shared/connections/app.itel.yaml`)
- Used only by API worker
- PLACEHOLDER - update with actual iTel API details

### Workers (`config/plugins/itel_cabinet_api/workers.yaml`)

Single file defines both workers:
```yaml
workers:
  itel_cabinet_tracking_worker:
    # Delta table tracking config
    ...

  itel_cabinet_api_worker:
    # iTel API integration config
    ...
```

---

## Deployment

### Environment Variables

**Both Workers Need:**
```bash
export CLAIMX_API_BASE_URL="https://api.claimx.com"
export CLAIMX_API_TOKEN="your-claimx-token"
```

**API Worker Also Needs:**
```bash
export ITEL_CABINET_API_BASE_URL="https://api.itelcabinet.com"
export ITEL_CABINET_API_TOKEN="your-itel-token"
```

### Starting Workers

**Option 1: Separate Terminals (Development)**
```bash
# Terminal 1 - Tracking Worker
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker

# Terminal 2 - API Worker
python -m kafka_pipeline.workers.itel_cabinet_api_worker
```

**Option 2: Background Processes (Production)**
```bash
# Start tracking worker
nohup python -m kafka_pipeline.workers.itel_cabinet_tracking_worker \
  > logs/itel/tracking_worker.log 2>&1 &

# Start API worker
nohup python -m kafka_pipeline.workers.itel_cabinet_api_worker \
  > logs/itel/api_worker.log 2>&1 &
```

**Option 3: Systemd Services (Production)**
```bash
sudo systemctl start itel-tracking-worker
sudo systemctl start itel-api-worker
```

---

## Monitoring

### Consumer Lag

Check each worker independently:

```bash
# Tracking worker lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group itel_cabinet_tracking_worker_group

# API worker lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group itel_cabinet_api_worker_group
```

### Logs

```bash
# Tracking worker logs
tail -f logs/itel/tracking_worker.log

# API worker logs
tail -f logs/itel/api_worker.log

# Both workers
tail -f logs/itel/*.log
```

### Metrics

**Tracking Worker:**
- `itel_tracking_worker_messages_processed_total`
- `itel_tracking_worker_delta_writes_total`
- `itel_tracking_worker_delta_write_errors_total`

**API Worker:**
- `itel_api_worker_messages_processed_total`
- `itel_api_worker_api_calls_total`
- `itel_api_worker_api_errors_total`
- `itel_api_worker_api_latency_seconds`

---

## Error Handling

### Independent Error Topics

**Tracking Worker Errors** → `itel.cabinet.task.tracking.errors`
- Delta write failures
- Schema conflicts
- Permission errors

**API Worker Errors** → `itel.cabinet.api.errors`
- iTel API failures
- Authentication errors
- Rate limit errors

### Failure Scenarios

| Scenario | Tracking Worker | API Worker | Impact |
|----------|----------------|------------|---------|
| ClaimX API down | Logs warning, continues | Logs warning, continues | Both workers skip enrichment |
| Delta table unavailable | Fails, sends to error topic | Unaffected | API integration continues |
| iTel API down | Unaffected | Fails, sends to error topic | Tracking continues |
| Kafka down | Both stop | Both stop | Total failure |
| Network issue | Retries with backoff | Retries with backoff | Both retry independently |

---

## Scaling

### Scale Each Worker Independently

**Tracking Worker** - Scale for write throughput:
```bash
# Run 3 instances in same consumer group
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker &  # Instance 1
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker &  # Instance 2
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker &  # Instance 3
```

**API Worker** - Scale for API call volume:
```bash
# Run 2 instances in same consumer group
python -m kafka_pipeline.workers.itel_cabinet_api_worker &  # Instance 1
python -m kafka_pipeline.workers.itel_cabinet_api_worker &  # Instance 2
```

Messages are distributed across instances within same consumer group.

---

## Troubleshooting

### Both Workers Not Consuming

**Check:**
1. Topic exists: `kafka-topics --list | grep itel.cabinet.task.tracking`
2. Plugin is publishing: Check enrichment worker logs
3. Consumer groups exist: `kafka-consumer-groups --list`

### One Worker Working, Other Not

**Check:**
1. Worker-specific logs
2. Worker-specific environment variables
3. Worker-specific error topics
4. Consumer lag for each group

### Messages Processed Twice

**This is normal!** Both workers process every message because they use different consumer groups. This is by design.

---

## Best Practices

1. **Monitor Both Workers** - Set up separate alerts for each
2. **Different SLAs** - API worker is critical, tracking worker is best-effort
3. **Scale Independently** - More API workers for high volume, fewer tracking workers
4. **Separate Logs** - Keep logs separate for easier debugging
5. **Test Independently** - Test each worker's error handling separately

---

## Future Enhancements

### Phase 2
- [ ] Add batch API sender for high volume
- [ ] Implement API response tracking in Delta
- [ ] Add retry logic for failed API calls
- [ ] Circuit breaker for iTel API

### Phase 3
- [ ] Dead letter queue processing
- [ ] Automatic payload customization based on task data
- [ ] Real-time alerting on API failures
- [ ] Performance metrics dashboard

---

**Status:** Architecture Complete - Ready for Testing
**Next Steps:** Update iTel API connection with real endpoints and test end-to-end
