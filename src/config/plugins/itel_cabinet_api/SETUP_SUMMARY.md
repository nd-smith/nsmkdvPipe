# iTel Cabinet Plugin - Setup Summary

**Created:** 2026-01-10
**Status:** ✅ Ready for Testing

---

## What We Built

A complete plugin system to track iTel Cabinet Repair Form tasks (task_id 32513) through their entire lifecycle, from creation to completion, with enrichment and historical storage.

### Architecture

```
ClaimX Task Event (32513)
         ↓
   Plugin Triggers
         ↓
   Kafka Topic: itel.cabinet.task.tracking
         ↓
   Worker Consumes
         ↓
   Enrichment Pipeline:
     1. Transform
     2. Validate
     3. ClaimX API Lookup
     4. Delta Table Write
         ↓
   Delta Table: itel_cabinet_task_tracking
```

---

## Files Created/Modified

### 1. Plugin Configuration
**Location:** `config/plugins/itel_cabinet_api/config.yaml`
- ✅ Updated with task_id 32513
- ✅ Uses `on_any` trigger for complete lifecycle tracking
- ✅ Publishes to `itel.cabinet.task.tracking` topic

### 2. Shared Connections
**Location:** `config/plugins/shared/connections/claimx.yaml`
- ✅ ClaimX API connection with Bearer auth
- ✅ Retry logic and timeouts configured
- ✅ Environment variable support for secrets

### 3. Worker Configuration
**Location:** `config/plugins/itel_cabinet_api/workers.yaml`
- ✅ Complete enrichment pipeline defined
- ✅ Transform, validation, lookup, and Delta write handlers
- ✅ Success/error topics configured

### 4. Delta Table Writer Handler
**Location:** `kafka_pipeline/plugins/handlers/delta_writer.py`
- ✅ Custom enrichment handler implementation
- ✅ Supports schema evolution
- ✅ Partitioning support (year/month/day)
- ✅ Batch variant available for high-volume scenarios

### 5. Table Initialization Script
**Location:** `scripts/delta_tables/create_itel_cabinet_tracking_table.py`
- ✅ Creates Delta table with proper schema
- ✅ Configures partitioning and optimization
- ✅ Supports dry-run and drop-existing options

### 6. Documentation
- ✅ `SPEC.md` - Technical specification
- ✅ `README.md` - User documentation
- ✅ `scripts/delta_tables/README.md` - Table setup guide
- ✅ `scripts/delta_tables/itel_cabinet_queries.sql` - Sample queries

### 7. Example Files (Moved)
**Location:** `plugins/docs/examples/`
- ✅ connections.example.yaml
- ✅ workers.example.yaml
- ✅ task_trigger_with_connections.example.yaml

---

## Directory Structure

```
src/
├── config/
│   └── plugins/
│       ├── shared/
│       │   └── connections/
│       │       └── claimx.yaml                      # Shared ClaimX API connection
│       └── itel_cabinet_api/
│           ├── config.yaml                          # Plugin trigger config
│           ├── workers.yaml                         # Worker config
│           ├── SPEC.md                              # Technical spec
│           ├── README.md                            # User docs
│           └── SETUP_SUMMARY.md                     # This file
│
├── kafka_pipeline/
│   └── plugins/
│       └── handlers/
│           ├── __init__.py
│           └── delta_writer.py                      # Custom Delta writer handler
│
├── scripts/
│   └── delta_tables/
│       ├── create_itel_cabinet_tracking_table.py    # Table creation script
│       ├── itel_cabinet_queries.sql                 # Sample queries
│       └── README.md                                # Setup documentation
│
└── plugins/
    └── docs/
        └── examples/
            ├── connections.example.yaml
            ├── workers.example.yaml
            └── task_trigger_with_connections.example.yaml
```

---

## Setup Steps

### 1. Set Environment Variables

```bash
export CLAIMX_API_BASE_URL="https://api.claimx.com"
export CLAIMX_API_TOKEN="your-bearer-token-here"
```

### 2. Create Delta Table

```bash
# Dry run first to verify
python scripts/delta_tables/create_itel_cabinet_tracking_table.py --dry-run

# Create the table
python scripts/delta_tables/create_itel_cabinet_tracking_table.py
```

### 3. Create Kafka Topics

```bash
# Create main tracking topic
kafka-topics --create \
  --topic itel.cabinet.task.tracking \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=604800000  # 7 days

# Create success topic
kafka-topics --create \
  --topic itel.cabinet.task.tracking.success \
  --partitions 1 \
  --replication-factor 3 \
  --config retention.ms=259200000  # 3 days

# Create error/DLQ topic
kafka-topics --create \
  --topic itel.cabinet.task.tracking.errors \
  --partitions 1 \
  --replication-factor 3 \
  --config retention.ms=2592000000  # 30 days
```

### 4. Start Workers

```bash
# Start ClaimX enrichment worker (includes plugin)
python -m kafka_pipeline.workers.claimx_enrichment_worker

# Start iTel tracking worker (in separate terminal)
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker
```

---

## Testing

### 1. Verify Plugin Triggers

```bash
# Watch plugin logs
tail -f logs/claimx/enrichment_worker.log | grep "iTel Cabinet"

# Expected output when task 32513 events occur:
# INFO: iTel Cabinet task event tracked | event_id=... | task_id=32513 | status=assigned
```

### 2. Verify Topic Messages

```bash
# Monitor the tracking topic
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic itel.cabinet.task.tracking \
  --from-beginning
```

### 3. Query Delta Table

```sql
-- Check table has data
SELECT COUNT(*) FROM itel_cabinet_task_tracking;

-- View recent events
SELECT event_timestamp, task_status, assignment_id
FROM itel_cabinet_task_tracking
ORDER BY event_timestamp DESC
LIMIT 10;
```

### 4. Test End-to-End

1. Create or update a task with task_id 32513 in ClaimX
2. Verify plugin triggers (check logs)
3. Verify message published to Kafka (check topic)
4. Verify worker processes message (check worker logs)
5. Verify record in Delta table (run query)

---

## Monitoring

### Key Metrics

**Plugin:**
- `itel_plugin_events_triggered_total` - Events detected
- `itel_plugin_events_published_total` - Events published

**Worker:**
- `itel_worker_messages_consumed_total` - Messages consumed
- `itel_worker_messages_processed_total` - Successfully processed
- `itel_worker_claimx_api_calls_total` - API enrichment calls
- `itel_worker_delta_writes_total` - Delta table writes

**Consumer Lag:**
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group itel_cabinet_tracking_worker_group
```

### Dashboards

Monitor these in your observability platform:
1. Events tracked over time
2. Processing latency
3. ClaimX API latency
4. Delta write rate
5. Consumer lag

---

## Troubleshooting

### Plugin Not Triggering
- Check plugin enabled: `config.yaml` has `enabled: true`
- Verify task_id matches exactly: 32513
- Check enrichment worker running
- Enable DEBUG logging

### Worker Not Processing
- Verify Kafka connectivity
- Check ClaimX API credentials valid
- Review worker logs for errors
- Check consumer group lag

### Delta Write Errors
- Verify table exists: Run create script
- Check Spark connectivity
- Verify write permissions
- Review Delta table logs

### ClaimX API Failures
- Check token not expired
- Verify API rate limits
- Check API health status
- Consider increasing cache_ttl

---

## Next Steps

### Phase 1: Current Implementation ✅
- [x] Plugin configuration
- [x] Worker pipeline
- [x] Delta table storage
- [x] Documentation

### Phase 2: Enhancements (Future)
- [ ] Direct iTel Cabinet API integration
- [ ] Photo/attachment processing
- [ ] Real-time alerting on status changes
- [ ] SLA tracking and metrics
- [ ] Automated data quality checks

### Phase 3: Operations (Future)
- [ ] Monitoring dashboards in Grafana
- [ ] Alert rules in PagerDuty
- [ ] Automated table optimization
- [ ] Data retention policies
- [ ] Backup and recovery procedures

---

## Support

### Documentation
- [Technical Specification](./SPEC.md) - Detailed architecture and implementation
- [User Guide](./README.md) - How to use and operate the plugin
- [Plugin Connections Guide](../../../PLUGIN_CONNECTIONS_GUIDE.md) - General plugin system docs

### Common Commands

```bash
# Check plugin configuration
cat config/plugins/itel_cabinet_api/config.yaml

# View worker configuration
cat config/plugins/itel_cabinet_api/workers.yaml

# Test ClaimX connection
python -c "import os; print(os.getenv('CLAIMX_API_TOKEN', 'NOT SET'))"

# View table schema
spark-sql -e "DESCRIBE itel_cabinet_task_tracking;"

# Check table record count
spark-sql -e "SELECT COUNT(*) FROM itel_cabinet_task_tracking;"
```

---

**Setup Status:** ✅ Complete and Ready for Testing
**Next Action:** Set environment variables and create Delta table
**Owner:** Data Engineering Team
**Created By:** Claude Code Assistant
**Date:** 2026-01-10
