# Production Rollout Guide

This guide covers deploying the Kafka Pipeline to production, including local workstation testing before rollout.

## Table of Contents

1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Local Workstation Testing](#local-workstation-testing)
4. [Production Deployment](#production-deployment)
5. [Validation Procedures](#validation-procedures)
6. [Rollback Procedures](#rollback-procedures)

---

## System Overview

The Kafka Pipeline is a real-time event processing system that:

- Consumes events from Azure Event Hub (source of truth)
- Downloads attachments from presigned URLs
- Uploads files to OneLake
- Writes metadata to Delta Lake tables

### Architecture

```
Azure Event Hub          Local Kafka Cluster
(xact.events.raw)        (internal communication)
       │                         │
       ▼                         ▼
┌─────────────────┐    ┌─────────────────────────────────┐
│ Event Ingester  │───>│ downloads.pending               │
└─────────────────┘    │ downloads.cached    (new)       │
                       │ downloads.results               │
                       │ downloads.dlq                   │
                       │ downloads.pending.retry.*       │
                       └─────────────────────────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────────────┐
                       │ Download Worker → Local Cache   │
                       │      ↓                          │
                       │ Upload Worker → OneLake         │
                       │      ↓                          │
                       │ Result Processor → Delta Lake   │
                       └─────────────────────────────────┘
```

**Decoupled Download/Upload Architecture:**
- Download Worker: downloads files to local cache, produces to `downloads.cached`
- Upload Worker: uploads cached files to OneLake, produces to `downloads.results`
- This allows independent scaling of download vs upload workers
- Cache acts as buffer if OneLake has temporary issues

### Completed Implementation

All phases are complete (WP-001 through WP-410, plus WP-313):

- **Phase 1**: Foundation (core modules, auth, resilience, logging, download)
- **Phase 2**: Kafka Infrastructure (producer, consumer, retry, DLQ)
- **Phase 3**: Workers (Event Ingester, Download Worker, Result Processor, DLQ Handler)
- **Phase 4**: Integration (E2E tests, performance, observability, runbooks)

---

## Prerequisites

### Software Requirements

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11+ | Runtime |
| Docker | 20.10+ | Local Kafka |
| Docker Compose | 2.0+ | Container orchestration |
| Azure CLI | 2.50+ | Azure authentication |

### Azure Resources

- **Azure Event Hub** namespace with Kafka endpoint
- **OneLake** workspace and lakehouse
- **Azure AD** service principal (for production) or Azure CLI login (for local testing)

### Network Access

- Event Hub endpoint: `*.servicebus.windows.net:9093`
- OneLake endpoint: `*.dfs.fabric.microsoft.com:443`

---

## Local Workstation Testing

This section covers running the full pipeline locally before production deployment.

### Step 1: Clone and Set Up Environment

```bash
# Navigate to project directory
cd /home/nick/Documents/nsmkdvPipe

# Create and activate virtual environment
cd src
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .
pip install -e ".[dev]"  # Include test dependencies
```

### Step 2: Start Local Kafka

The project includes a Docker Compose file for local Kafka:

```bash
# Start Kafka with topic initialization
docker-compose -f docker-compose.kafka.yml up -d

# Wait for Kafka to be ready (about 30 seconds)
docker-compose -f docker-compose.kafka.yml logs -f kafka-init

# Expected output:
# Creating Kafka topics...
# Topics created successfully:
# xact.downloads.pending
# xact.downloads.cached
# xact.downloads.results
# xact.downloads.dlq
# xact.downloads.pending.retry.5m
# xact.downloads.pending.retry.10m
# xact.downloads.pending.retry.20m
# xact.downloads.pending.retry.40m
# xact.events.raw

# Verify Kafka is healthy
docker exec kafka-pipeline-local kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Optional: Start Kafka UI for debugging**

```bash
# Start with UI profile
docker-compose -f docker-compose.kafka.yml --profile ui up -d

# Access UI at http://localhost:8080
```

### Step 3: Configure Environment

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your settings
```

**Minimum configuration for local testing:**

```bash
# .env file

# Local Kafka (required for local testing)
LOCAL_KAFKA_BOOTSTRAP_SERVERS=localhost:9094
LOCAL_KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Azure Event Hub (optional for local testing - use --dev flag to skip)
# EVENTHUB_BOOTSTRAP_SERVERS=your-namespace.servicebus.windows.net:9093
# EVENTHUB_CONNECTION_STRING=Endpoint=sb://...

# Azure Authentication (for OneLake access)
# Option 1: Use Azure CLI (recommended for local dev)
# Run: az login

# Option 2: Service Principal
# AZURE_TENANT_ID=your-tenant-id
# AZURE_CLIENT_ID=your-client-id
# AZURE_CLIENT_SECRET=your-client-secret

# OneLake paths (update with your workspace)
ONELAKE_BASE_PATH=abfss://your-workspace@onelake.dfs.fabric.microsoft.com/your-lakehouse/Files

# Delta Lake tables
DELTA_EVENTS_TABLE_PATH=abfss://your-workspace@onelake.dfs.fabric.microsoft.com/your-lakehouse/Tables/xact_events
DELTA_INVENTORY_TABLE_PATH=abfss://your-workspace@onelake.dfs.fabric.microsoft.com/your-lakehouse/Tables/xact_attachments

# Observability
LOG_LEVEL=DEBUG
METRICS_PORT=8000
```

### Step 4: Authenticate with Azure

```bash
# Login to Azure CLI (for local development)
az login

# Verify authentication
az account show

# Test OneLake access
az storage fs list --account-name onelake --auth-mode login
```

### Step 5: Run the Pipeline in Dev Mode

**Dev mode** uses local Kafka for both events and internal communication (no Event Hub required):

```bash
# Run in development mode
python -m kafka_pipeline --dev

# This starts:
# - Event Ingester (consuming from local Kafka)
# - Download Worker (with concurrent processing)
# - Result Processor
# - Retry Scheduler
```

**Monitor pipeline logs:**

```bash
# In a separate terminal
docker-compose -f docker-compose.kafka.yml logs -f kafka
```

### Step 6: Produce Test Events

```bash
# Produce a test event to local Kafka
docker exec kafka-pipeline-local kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.events.raw << 'EOF'
{
  "trace_id": "test-001",
  "event_type": "documentsReceived",
  "event_subtype": "xact",
  "timestamp": "2024-12-31T12:00:00Z",
  "source_system": "test",
  "payload": {"assignmentId": "A123"},
  "attachments": ["https://example.com/test.pdf"]
}
EOF
```

### Step 7: Verify Pipeline Processing

```bash
# Check download tasks were created
docker exec kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.pending \
  --from-beginning \
  --max-messages 1

# Check files were cached (download complete, awaiting upload)
docker exec kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.cached \
  --from-beginning \
  --max-messages 1

# Check results were produced (upload complete)
docker exec kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.results \
  --from-beginning \
  --max-messages 1

# Check metrics
curl -s http://localhost:8000/metrics | grep kafka_messages
```

### Step 8: Run Integration Tests

```bash
# Activate virtual environment
source .venv/bin/activate

# Run unit tests
python -m pytest tests/kafka_pipeline/ -v

# Run integration tests (requires Docker)
python -m pytest tests/kafka_pipeline/integration/ -v --tb=short

# Run E2E tests
python -m pytest tests/kafka_pipeline/integration/test_e2e_*.py -v
```

### Step 9: Clean Up Local Environment

```bash
# Stop pipeline
# Press Ctrl+C in the terminal running the pipeline

# Stop Kafka
docker-compose -f docker-compose.kafka.yml down

# Remove volumes (optional - clears all data)
docker-compose -f docker-compose.kafka.yml down -v
```

---

## Production Deployment

### Pre-Deployment Checklist

- [ ] All tests passing locally
- [ ] E2E tests passing with local Kafka
- [ ] Azure CLI authentication verified
- [ ] OneLake access verified
- [ ] Event Hub connection string available
- [ ] Service principal credentials configured (or Azure CLI login)
- [ ] Monitoring dashboards ready

### Step 1: Configure Production Environment

Create production `.env` file:

```bash
# Production Configuration

# Azure Event Hub (Source)
EVENTHUB_BOOTSTRAP_SERVERS=your-namespace.servicebus.windows.net:9093
EVENTHUB_CONNECTION_STRING=Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>;EntityPath=xact.events.raw
EVENTHUB_EVENTS_TOPIC=xact.events.raw
EVENTHUB_CONSUMER_GROUP=xact-event-ingester
EVENTHUB_AUTO_OFFSET_RESET=earliest

# Local Kafka (Internal Communication)
# Use localhost:9094 if running Kafka via docker-compose on the same machine
LOCAL_KAFKA_BOOTSTRAP_SERVERS=localhost:9094
LOCAL_KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Topic Names
KAFKA_DOWNLOADS_PENDING_TOPIC=xact.downloads.pending
KAFKA_DOWNLOADS_CACHED_TOPIC=xact.downloads.cached
KAFKA_DOWNLOADS_RESULTS_TOPIC=xact.downloads.results
KAFKA_DLQ_TOPIC=xact.downloads.dlq

# Retry Configuration
RETRY_DELAYS=300,600,1200,2400
MAX_RETRIES=4

# Azure Storage
ONELAKE_BASE_PATH=abfss://prod-workspace@onelake.dfs.fabric.microsoft.com/prod-lakehouse/Files

# Delta Lake
ENABLE_DELTA_WRITES=true
DELTA_EVENTS_TABLE_PATH=abfss://prod-workspace@onelake.dfs.fabric.microsoft.com/prod-lakehouse/Tables/xact_events
DELTA_INVENTORY_TABLE_PATH=abfss://prod-workspace@onelake.dfs.fabric.microsoft.com/prod-lakehouse/Tables/xact_attachments

# Azure Authentication
# Option 1: Service Principal (recommended for unattended operation)
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret

# Option 2: Azure CLI (for local workstation)
# Just run: az login

# Worker Configuration
DOWNLOAD_CONCURRENCY=10
DOWNLOAD_BATCH_SIZE=20
UPLOAD_CONCURRENCY=10
UPLOAD_BATCH_SIZE=20

# Local Cache (for downloaded files awaiting upload)
CACHE_DIR=/tmp/kafka_pipeline_cache

# Observability
LOG_LEVEL=INFO
METRICS_PORT=8000
```

### Step 2: Start Local Kafka

Use Docker Compose to run the internal Kafka cluster:

```bash
# Start Kafka with all topics pre-created
docker-compose -f docker-compose.kafka.yml up -d

# Wait for initialization (topics are created automatically)
docker-compose -f docker-compose.kafka.yml logs -f kafka-init

# Verify Kafka is running and topics exist
docker exec kafka-pipeline-local kafka-topics.sh --list --bootstrap-server localhost:9092

# Expected topics:
# xact.downloads.pending
# xact.downloads.cached
# xact.downloads.results
# xact.downloads.dlq
# xact.downloads.pending.retry.5m
# xact.downloads.pending.retry.10m
# xact.downloads.pending.retry.20m
# xact.downloads.pending.retry.40m
# xact.events.raw
```

**Optional: Start Kafka UI for monitoring**

```bash
docker-compose -f docker-compose.kafka.yml --profile ui up -d
# Access at http://localhost:8080
```

### Step 3: Run the Pipeline

**Option A: Run all workers in one process (simplest)**

```bash
# Activate virtual environment
cd /home/nick/Documents/nsmkdvPipe/src
source .venv/bin/activate

# Run all workers (connects to Event Hub + local Kafka)
python -m kafka_pipeline

# Or run in background with nohup
nohup python -m kafka_pipeline > pipeline.log 2>&1 &
```

**Option B: Run workers separately (for scaling/debugging)**

```bash
# Terminal 1: Event Ingester
python -m kafka_pipeline --worker event-ingester --metrics-port 8001

# Terminal 2: Download Worker (downloads to local cache)
python -m kafka_pipeline --worker download --metrics-port 8002

# Terminal 3: Upload Worker (uploads cache to OneLake)
python -m kafka_pipeline --worker upload --metrics-port 8003

# Terminal 4: Result Processor
python -m kafka_pipeline --worker result-processor --metrics-port 8004
```

**Option C: Run as systemd services (for persistent operation)**

Create service files:

```bash
# /etc/systemd/system/kafka-pipeline.service
[Unit]
Description=Kafka Pipeline Workers
After=network.target docker.service

[Service]
Type=simple
User=nick
WorkingDirectory=/home/nick/Documents/nsmkdvPipe/src
Environment="PATH=/home/nick/Documents/nsmkdvPipe/src/.venv/bin"
EnvironmentFile=/home/nick/Documents/nsmkdvPipe/.env
ExecStart=/home/nick/Documents/nsmkdvPipe/src/.venv/bin/python -m kafka_pipeline
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka-pipeline
sudo systemctl start kafka-pipeline
sudo systemctl status kafka-pipeline
```

### Step 4: Validate Deployment

```bash
# Check pipeline is running
ps aux | grep kafka_pipeline

# Check logs (if running with nohup)
tail -f pipeline.log

# Check logs (if running as systemd service)
journalctl -u kafka-pipeline -f

# Check metrics endpoint
curl -s http://localhost:8000/metrics | grep kafka_messages

# Check consumer groups are connected
docker exec kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# Check consumer lag (download worker)
docker exec kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group xact-download-worker --describe

# Check consumer lag (upload worker)
docker exec kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group xact-upload-worker --describe
```

---

## Validation Procedures

### Smoke Tests

Run these tests immediately after deployment:

**1. Health Check**

```bash
# All pods should be Running
kubectl get pods -n kafka-pipeline

# Expected: All pods STATUS=Running, READY=1/1
```

**2. Consumer Group Check**

```bash
# Verify consumers are connected
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --list

# Expected:
# xact-event-ingester-worker
# xact-download-worker
# xact-upload-worker
# xact-result-processor-worker
```

**3. Metrics Check**

```bash
# Verify metrics are being collected
curl -s http://localhost:8000/metrics | grep kafka_messages_consumed_total

# Expected: Counters > 0 if messages are flowing
```

### Integration Validation

**1. End-to-End Flow Test**

```bash
# Produce a test event (use a trace_id that won't conflict with production data)
kafka-console-producer --bootstrap-server $EVENTHUB_BROKER --topic xact.events.raw << 'EOF'
{
  "trace_id": "validation-test-$(date +%s)",
  "event_type": "documentsReceived",
  "event_subtype": "xact",
  "timestamp": "$(date -Iseconds)",
  "source_system": "validation",
  "payload": {"assignmentId": "VALIDATE-001"},
  "attachments": ["https://httpbin.org/bytes/1024"]
}
EOF

# Wait 30 seconds for processing

# Check result appeared
kafka-console-consumer --bootstrap-server $LOCAL_KAFKA_BROKER \
  --topic xact.downloads.results \
  --from-beginning \
  --max-messages 1 \
  | grep "validation-test"
```

**2. Delta Lake Write Verification**

```python
# Using Python/PySpark
import polars as pl

# Check events table
events = pl.read_delta("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_events")
print(events.filter(pl.col("trace_id").str.starts_with("validation-test")).head())

# Check inventory table
inventory = pl.read_delta("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_attachments")
print(inventory.filter(pl.col("trace_id").str.starts_with("validation-test")).head())
```

### Performance Validation

Monitor these metrics for 15 minutes after deployment:

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Download Worker Lag | < 1,000 | > 5,000 |
| Upload Worker Lag | < 1,000 | > 5,000 |
| Error Rate | < 0.1% | > 1% |
| Processing Latency p95 | < 5s | > 10s |
| Download Success Rate | > 99% | < 95% |
| Upload Success Rate | > 99% | < 95% |
| Cache Disk Usage | < 80% | > 90% |

---

## Rollback Procedures

### Quick Rollback (< 5 minutes)

```bash
# 1. Stop the pipeline
# If running in foreground: Ctrl+C
# If running with nohup:
pkill -f "python -m kafka_pipeline"

# If running as systemd service:
sudo systemctl stop kafka-pipeline

# 2. Checkout previous working version
cd /home/nick/Documents/nsmkdvPipe
git stash  # Save any local changes
git checkout v<previous-version>  # Or specific commit hash

# 3. Restart the pipeline
source src/.venv/bin/activate
python -m kafka_pipeline

# Or restart systemd service:
sudo systemctl start kafka-pipeline
```

### Full Rollback

If issues persist after quick rollback:

```bash
# 1. Stop the pipeline completely
pkill -f "python -m kafka_pipeline"
# Or: sudo systemctl stop kafka-pipeline

# 2. Check for data integrity issues
# Review DLQ for failed messages
docker exec kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.dlq \
  --from-beginning

# 3. Check consumer group offsets (to understand processing state)
docker exec kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group xact-download-worker --describe

# 4. Restore previous version from git
cd /home/nick/Documents/nsmkdvPipe
git log --oneline -10  # Find last known good commit
git checkout <commit-hash>

# 5. Reinstall dependencies if needed
cd src
source .venv/bin/activate
pip install -e .

# 6. Restart pipeline
python -m kafka_pipeline
```

### Reset Consumer Offsets (if needed)

If you need to reprocess messages:

```bash
# Reset to earliest (reprocess all messages)
docker exec kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group xact-download-worker \
  --topic xact.downloads.pending \
  --reset-offsets --to-earliest --execute

# Reset to specific offset
docker exec kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group xact-download-worker \
  --topic xact.downloads.pending \
  --reset-offsets --to-offset 1000 --execute
```

---

## Troubleshooting

### Common Issues

**1. Kafka Connection Failed**

```
Error: KafkaConnectionError: Unable to connect to broker
```

**Solution:**
- Verify `LOCAL_KAFKA_BOOTSTRAP_SERVERS` is correct (use `localhost:9094` for docker-compose)
- Check Docker container is running: `docker ps | grep kafka`
- Check Kafka logs: `docker-compose -f docker-compose.kafka.yml logs kafka`

**2. Event Hub Authentication Failed**

```
Error: SASL authentication failed
```

**Solution:**
- Verify `EVENTHUB_CONNECTION_STRING` is correct
- Check connection string includes `EntityPath`
- Verify SAS key has not expired

**3. OneLake Upload Failed**

```
Error: AuthorizationPermissionMismatch
```

**Solution:**
- Verify service principal has Storage Blob Data Contributor role
- Check OneLake workspace permissions
- Verify `ONELAKE_BASE_PATH` is correct

**4. High Consumer Lag (Download Worker)**

**Solution:**
- Increase `DOWNLOAD_CONCURRENCY` in `.env` (default: 10, max: 50)
- Run multiple download worker processes (use `--worker download` flag)
- Check source URLs are responding quickly
- Monitor with: `docker exec kafka-pipeline-local kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group xact-download-worker --describe`

**5. High Consumer Lag (Upload Worker)**

**Solution:**
- Increase `UPLOAD_CONCURRENCY` in `.env` (default: 10, max: 50)
- Run multiple upload worker processes (use `--worker upload` flag)
- Check OneLake connectivity and throttling
- Monitor with: `docker exec kafka-pipeline-local kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group xact-upload-worker --describe`

**6. Cache Directory Full or Inaccessible**

```
Error: OSError: No space left on device
Error: PermissionError: [Errno 13] Permission denied
```

**Solution:**
- Check disk space: `df -h /tmp/kafka_pipeline_cache`
- Verify directory permissions: `ls -la /tmp/kafka_pipeline_cache`
- Clear stale cache files: `find /tmp/kafka_pipeline_cache -mtime +1 -delete`
- Consider changing `CACHE_DIR` to a path with more space

---

## Related Documentation

- [Runbook Index](./runbooks/README.md)
- [Consumer Lag Runbook](./runbooks/consumer-lag.md)
- [DLQ Management Runbook](./runbooks/dlq-management.md)
- [Deployment Procedures](./runbooks/deployment-procedures.md)
- [Incident Response](./runbooks/incident-response.md)

---

**Last Updated**: 2026-01-01
**Owner**: Platform Engineering
