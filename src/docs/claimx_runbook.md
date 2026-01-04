# ClaimX Pipeline Runbook

> **Version:** 1.0
> **Last Updated:** 2026-01-04
> **Audience:** Platform operators, SREs, DevOps engineers

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Configuration](#configuration)
5. [Deployment](#deployment)
6. [Worker Operations](#worker-operations)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)
9. [DLQ Management](#dlq-management)
10. [Operational Procedures](#operational-procedures)
11. [Performance Tuning](#performance-tuning)

---

## Overview

The ClaimX pipeline is an event-driven data ingestion system that:

- **Consumes** ClaimX events from Kafka topics
- **Enriches** events by fetching additional data from the ClaimX API
- **Downloads** media files (photos, documents) from S3 presigned URLs
- **Uploads** files to OneLake for long-term storage
- **Writes** structured entity data to Delta Lake tables

### Key Features

- **High Throughput**: 25-54k events/sec enrichment, 1500+ downloads/sec @ 20x concurrency
- **Resilient**: Automatic retry with exponential backoff, URL refresh for expired presigned URLs
- **Observable**: Health checks, metrics, comprehensive logging
- **Recoverable**: DLQ for failed messages with CLI tools for inspection and replay

---

## Architecture

### Components

```
┌─────────────────┐
│  ClaimX Events  │ (Kusto/EventHub)
│  (Raw JSON)     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Event Ingester Worker                                       │
│ - Parses ClaimXEventMessage                                 │
│ - Writes to claimx_events Delta table                       │
│ - Produces ClaimXEnrichmentTask                             │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Enrichment Worker                                           │
│ - Routes events to handlers (Project, Media, Task, etc.)    │
│ - Pre-flight project check (ensures projects exist)         │
│ - Batches API calls by project_id                           │
│ - Writes entity data to Delta tables                        │
│ - Produces ClaimXDownloadTask for media files               │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Download Worker                                             │
│ - Downloads from S3 presigned URLs                          │
│ - Handles URL expiration (refreshes from API)               │
│ - Produces ClaimXCachedDownloadMessage                      │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Upload Worker                                               │
│ - Uploads files to OneLake                                  │
│ - Writes to claimx_attachments inventory table              │
│ - Produces ClaimXUploadResultMessage                        │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Result Processor                                            │
│ - Aggregates upload success/failure statistics              │
│ - Logs detailed outcome metrics                             │
│ - Emits monitoring metrics                                  │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Raw Events** → `claimx.events.raw` topic
2. **Enrichment Tasks** → `claimx.enrichment.pending` topic
3. **Download Tasks** → `claimx.downloads.pending` topic
4. **Cached Downloads** → `claimx.downloads.cached` topic
5. **Upload Results** → `claimx.downloads.results` topic

### Retry Topics

**Enrichment Retries:**
- `claimx.enrichment.pending.retry.5m`
- `claimx.enrichment.pending.retry.10m`
- `claimx.enrichment.pending.retry.20m`
- `claimx.enrichment.pending.retry.40m`

**Download Retries:**
- `claimx.downloads.pending.retry.300s` (5 minutes)
- `claimx.downloads.pending.retry.600s` (10 minutes)
- `claimx.downloads.pending.retry.1200s` (20 minutes)
- `claimx.downloads.pending.retry.2400s` (40 minutes)

### DLQ Topics

- `claimx.enrichment.dlq` - Failed enrichment tasks (exhausted retries or permanent errors)
- `claimx.downloads.dlq` - Failed downloads (404, forbidden, exhausted retries)

### Delta Tables

**Entity Tables:**
- `claimx_projects` - Project metadata
- `claimx_contacts` - Customer contact information
- `claimx_media` - Media file metadata
- `claimx_tasks` - Custom task assignments
- `claimx_task_templates` - Task templates
- `claimx_external_links` - External links and references
- `claimx_video_collab` - Video collaboration sessions

**Operational Tables:**
- `claimx_events` - Raw event log (deduped by event_id)
- `claimx_attachments` - File inventory (OneLake paths, sizes, checksums)

---

## Prerequisites

### Infrastructure

- **Kafka Cluster**: Accessible with bootstrap servers configured
- **OneLake**: Workspace and lakehouse with write permissions
- **ClaimX API**: API credentials (username/password or token)
- **Delta Lake**: Storage for Delta tables

### Runtime Requirements

- **Python**: 3.11 or higher
- **Virtual Environment**: `venv` with all dependencies installed
- **Network**: Outbound access to:
  - Kafka brokers
  - ClaimX API endpoints
  - S3 presigned URLs (amazonaws.com)
  - OneLake endpoints

### Permissions

- **Kafka**: Read from all topics, write to enrichment/download/result topics
- **OneLake**: Write to `claimx/*` paths
- **Delta Lake**: Read/write access to all claimx_* tables
- **ClaimX API**: Valid API credentials with read access to projects, media, tasks

---

## Configuration

### Environment Variables

All configuration can be set via environment variables. See `kafka_pipeline/config.py` for full reference.

#### Kafka Configuration

```bash
# Required
export KAFKA_BOOTSTRAP_SERVERS="kafka-broker-1:9092,kafka-broker-2:9092"

# Optional (defaults shown)
export KAFKA_CONSUMER_GROUP_PREFIX="claimx"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_SASL_MECHANISM="PLAIN"
export KAFKA_SASL_USERNAME="your-username"
export KAFKA_SASL_PASSWORD="your-password"
```

#### ClaimX API Configuration

```bash
# Required
export CLAIMX_API_URL="https://api.claimx.com"
export CLAIMX_API_USERNAME="api-user"
export CLAIMX_API_PASSWORD="api-password"

# Optional (defaults shown)
export CLAIMX_API_TIMEOUT_SECONDS="30"
export CLAIMX_API_MAX_RETRIES="3"
export CLAIMX_API_CONCURRENCY="10"
```

#### Topic Configuration

```bash
# Optional (defaults shown)
export CLAIMX_EVENTS_TOPIC="claimx.events.raw"
export CLAIMX_ENRICHMENT_PENDING_TOPIC="claimx.enrichment.pending"
export CLAIMX_DOWNLOADS_PENDING_TOPIC="claimx.downloads.pending"
export CLAIMX_DOWNLOADS_RESULTS_TOPIC="claimx.downloads.results"
export CLAIMX_DLQ_TOPIC="claimx.dlq"
```

#### OneLake Configuration

```bash
export ONELAKE_WORKSPACE_ID="your-workspace-id"
export ONELAKE_LAKEHOUSE_ID="your-lakehouse-id"
export ONELAKE_ENDPOINT="https://onelake.dfs.fabric.microsoft.com"
```

#### Retry Configuration

```bash
# Retry delays in seconds (defaults shown)
export RETRY_DELAYS="300,600,1200,2400"  # 5m, 10m, 20m, 40m
export MAX_RETRIES="4"
```

#### Worker-Specific Configuration

```bash
# Download worker
export DOWNLOAD_CONCURRENCY="10"
export DOWNLOAD_BATCH_SIZE="20"
export CACHE_DIR="/tmp/kafka_pipeline_cache"

# Enrichment worker
export ENRICHMENT_BATCH_SIZE="100"
export ENABLE_DELTA_WRITES="true"
```

### Configuration File

Alternatively, create a `config.yaml` file:

```yaml
kafka:
  bootstrap_servers: "kafka-broker-1:9092,kafka-broker-2:9092"
  consumer_group_prefix: "claimx"
  security_protocol: "SASL_SSL"

claimx:
  api_url: "https://api.claimx.com"
  api_username: "api-user"
  api_password: "api-password"
  api_timeout_seconds: 30
  api_concurrency: 10

topics:
  events: "claimx.events.raw"
  enrichment_pending: "claimx.enrichment.pending"
  downloads_pending: "claimx.downloads.pending"
  downloads_results: "claimx.downloads.results"

retry:
  delays: [300, 600, 1200, 2400]
  max_retries: 4
```

---

## Deployment

### Step 1: Install Dependencies

```bash
# Clone repository
git clone <repository-url>
cd kafka_pipeline

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your configuration
vim .env

# Source environment variables
source .env
```

### Step 3: Verify Configuration

```bash
# Test Kafka connection
python -m kafka_pipeline --worker claimx-ingester --dry-run

# Test ClaimX API connection
python -c "from kafka_pipeline.claimx.api_client import ClaimXApiClient; import asyncio; asyncio.run(ClaimXApiClient.test_connection())"
```

### Step 4: Start Workers

Start each worker in a separate process/container:

```bash
# Event Ingester
python -m kafka_pipeline --worker claimx-ingester

# Enrichment Worker
python -m kafka_pipeline --worker claimx-enricher

# Download Worker
python -m kafka_pipeline --worker claimx-downloader

# Upload Worker
python -m kafka_pipeline --worker claimx-uploader

# Result Processor
python -m kafka_pipeline --worker claimx-result-processor
```

### Kubernetes Deployment

Example deployment manifests:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: claimx-enricher
spec:
  replicas: 3
  selector:
    matchLabels:
      app: claimx-enricher
  template:
    metadata:
      labels:
        app: claimx-enricher
    spec:
      containers:
      - name: enricher
        image: kafka-pipeline:latest
        command: ["python", "-m", "kafka_pipeline", "--worker", "claimx-enricher"]
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-config
              key: bootstrap_servers
        - name: CLAIMX_API_URL
          valueFrom:
            configMapKeyRef:
              name: claimx-config
              key: api_url
        - name: CLAIMX_API_USERNAME
          valueFrom:
            secretKeyRef:
              name: claimx-api
              key: username
        - name: CLAIMX_API_PASSWORD
          valueFrom:
            secretKeyRef:
              name: claimx-api
              key: password
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
```

---

## Worker Operations

### Event Ingester

**Purpose:** Ingest raw ClaimX events and produce enrichment tasks

**Command:**
```bash
python -m kafka_pipeline --worker claimx-ingester
```

**Health Check Port:** 8084

**Scaling:** 1-3 replicas (limited by topic partitions)

**Key Metrics:**
- Events ingested per second
- Enrichment tasks produced
- Parse errors (malformed events)

### Enrichment Worker

**Purpose:** Fetch data from ClaimX API and write to Delta tables

**Command:**
```bash
python -m kafka_pipeline --worker claimx-enricher
```

**Health Check Port:** 8081

**Scaling:** 3-10 replicas (high throughput, API-bound)

**Key Metrics:**
- Events enriched per second
- API call latency (p50, p95, p99)
- Batch sizes (should see batching by project_id)
- Delta write throughput

**Batch Optimization:**
- MediaHandler batches API calls by project_id
- Typical batching ratio: 50-100x for same-project events

### Download Worker

**Purpose:** Download media files from S3 presigned URLs

**Command:**
```bash
python -m kafka_pipeline --worker claimx-downloader
```

**Health Check Port:** 8082

**Scaling:** 5-20 replicas (I/O-bound, benefits from high concurrency)

**Key Metrics:**
- Downloads per second
- Download latency
- Bytes downloaded
- Expired URL rate (triggers retry with refresh)
- 404 rate (permanent failures)

**Performance:**
- Baseline: 98 downloads/sec @ 1x concurrency
- Scales linearly: 1565 downloads/sec @ 20x concurrency

### Upload Worker

**Purpose:** Upload files to OneLake

**Command:**
```bash
python -m kafka_pipeline --worker claimx-uploader
```

**Health Check Port:** 8083

**Scaling:** 5-15 replicas (I/O-bound)

**Key Metrics:**
- Uploads per second
- Upload latency
- Bytes uploaded
- Upload failure rate

### Result Processor

**Purpose:** Aggregate upload statistics and emit metrics

**Command:**
```bash
python -m kafka_pipeline --worker claimx-result-processor
```

**Health Check Port:** N/A (no health endpoint, lightweight)

**Scaling:** 1 replica (stateful aggregation)

**Key Metrics:**
- Success rate percentage
- Failure rate percentage
- Messages processed per second
- Total bytes uploaded

---

## Monitoring

### Health Checks

Each worker exposes health check endpoints for Kubernetes liveness and readiness probes:

**Liveness Probe:** `GET /health/live`
- Returns 200 OK if worker process is running
- Use for restart decisions

**Readiness Probe:** `GET /health/ready`
- Returns 200 OK if worker is ready to process messages
- Checks: Kafka connected, API reachable, circuit breaker closed
- Use for load balancer inclusion

**Worker Ports:**
- Enrichment Worker: 8081
- Download Worker: 8082
- Upload Worker: 8083
- Event Ingester: 8084

### Key Metrics to Monitor

#### Throughput Metrics

- `claimx.enricher.events_per_second` - Should sustain 25k+
- `claimx.downloader.downloads_per_second` - Should sustain 500+
- `claimx.uploader.uploads_per_second` - Should sustain 300+

#### Latency Metrics

- `claimx.api.latency_ms` - p50, p95, p99
- `claimx.download.latency_ms` - p50, p95, p99
- `claimx.upload.latency_ms` - p50, p95, p99

#### Error Metrics

- `claimx.enricher.api_errors` - Should be <1%
- `claimx.downloader.expired_urls` - Triggers retry with refresh
- `claimx.downloader.404_errors` - Permanent failures → DLQ
- `claimx.uploader.failures` - Should be <1%

#### Queue Metrics

- `claimx.enrichment.pending.lag` - Consumer lag on enrichment topic
- `claimx.downloads.pending.lag` - Consumer lag on downloads topic
- `claimx.dlq.enrichment.count` - Messages in enrichment DLQ
- `claimx.dlq.downloads.count` - Messages in download DLQ

#### Retry Metrics

- `claimx.enrichment.retry_rate` - Percentage of events retried
- `claimx.downloads.retry_rate` - Percentage of downloads retried
- `claimx.downloads.url_refresh_rate` - Expired URLs refreshed

### Alerting Thresholds

**Critical Alerts:**

```yaml
- alert: ClaimXEnrichmentLagHigh
  expr: claimx_enrichment_pending_lag > 10000
  for: 5m
  severity: critical
  description: Enrichment queue lag is high, may cause delayed processing

- alert: ClaimXAPIErrorRateHigh
  expr: rate(claimx_api_errors[5m]) > 0.05
  for: 2m
  severity: critical
  description: ClaimX API error rate >5%, check API health

- alert: ClaimXDLQGrowing
  expr: increase(claimx_dlq_count[1h]) > 100
  for: 5m
  severity: critical
  description: DLQ growing rapidly, investigate failures
```

**Warning Alerts:**

```yaml
- alert: ClaimXDownloadLatencyHigh
  expr: claimx_download_latency_p95 > 5000
  for: 10m
  severity: warning
  description: Download latency p95 >5s, may indicate S3 issues

- alert: ClaimXUploadFailureRate
  expr: rate(claimx_upload_failures[10m]) > 0.01
  for: 5m
  severity: warning
  description: Upload failure rate >1%, check OneLake connectivity
```

### Grafana Dashboard

Example dashboard panels:

**Row 1: Throughput**
- Events ingested/sec (line chart)
- Events enriched/sec (line chart)
- Downloads/sec (line chart)
- Uploads/sec (line chart)

**Row 2: Latency**
- API call latency (p50, p95, p99 line charts)
- Download latency (histogram)
- Upload latency (histogram)

**Row 3: Errors & Retries**
- Error rate by type (stacked area)
- Retry rate (line chart)
- DLQ message count (line chart)

**Row 4: Queue Health**
- Consumer lag by topic (line chart)
- Partition assignment (table)
- Commit rate (line chart)

---

## Troubleshooting

### Common Issues

#### 1. High Consumer Lag

**Symptoms:**
- `claimx.enrichment.pending.lag` increasing
- Events processed slowly

**Diagnosis:**
```bash
# Check consumer group status
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --group claimx-enricher --describe

# Check worker logs
kubectl logs -l app=claimx-enricher --tail=100
```

**Solutions:**
- **Scale up:** Increase worker replicas
- **Batch size:** Increase `ENRICHMENT_BATCH_SIZE` (default 100)
- **API concurrency:** Increase `CLAIMX_API_CONCURRENCY` (default 10)
- **Check API health:** Slow API responses will bottleneck enrichment

#### 2. API Errors

**Symptoms:**
- `claimx.api.errors` spiking
- Enrichment tasks going to DLQ

**Diagnosis:**
```bash
# Check API health
curl -i $CLAIMX_API_URL/health

# Check worker logs for error details
kubectl logs -l app=claimx-enricher | grep "API error"
```

**Solutions:**
- **Rate limiting:** ClaimX API may be rate limiting, reduce concurrency
- **Circuit breaker:** Check if circuit breaker is open (API unavailable)
- **Credentials:** Verify API credentials are valid
- **Network:** Check network connectivity to ClaimX API

#### 3. Expired URL Failures

**Symptoms:**
- Downloads failing with 403 Forbidden
- High rate of URL refresh attempts

**Diagnosis:**
```bash
# Check download worker logs
kubectl logs -l app=claimx-downloader | grep "expired"

# Inspect DLQ for URL refresh failures
python -m kafka_pipeline.claimx.dlq.cli inspect download --limit 10
```

**Solutions:**
- **Expected behavior:** URLs expire after 24 hours, retry handler refreshes automatically
- **If refresh fails:** API may not be returning fresh URLs, check API health
- **Manual refresh:** Use DLQ CLI to replay with fresh URLs

#### 4. OneLake Upload Failures

**Symptoms:**
- Uploads failing with timeout or authentication errors
- `claimx.uploader.failures` increasing

**Diagnosis:**
```bash
# Check upload worker logs
kubectl logs -l app=claimx-uploader | grep "upload failed"

# Check OneLake connectivity
curl -i $ONELAKE_ENDPOINT
```

**Solutions:**
- **Authentication:** Verify OneLake credentials and permissions
- **Network:** Check connectivity to OneLake endpoints
- **Quota:** Check OneLake storage quota
- **File size:** Very large files (>1GB) may timeout, increase timeout

#### 5. DLQ Growing

**Symptoms:**
- DLQ message count increasing steadily
- Permanent failures accumulating

**Diagnosis:**
```bash
# List DLQ counts
python -m kafka_pipeline.claimx.dlq.cli list

# Inspect sample messages
python -m kafka_pipeline.claimx.dlq.cli inspect enrichment --limit 10
python -m kafka_pipeline.claimx.dlq.cli inspect download --limit 10
```

**Solutions:**
- **Analyze errors:** Look for patterns in error messages
- **Fix root cause:** Address underlying issue (API, network, data quality)
- **Replay:** After fix, replay DLQ messages to retry

#### 6. Worker Crashes

**Symptoms:**
- Worker pods restarting frequently
- Liveness probe failures

**Diagnosis:**
```bash
# Check pod status
kubectl get pods -l app=claimx-enricher

# Check recent logs
kubectl logs -l app=claimx-enricher --previous

# Check events
kubectl describe pod <pod-name>
```

**Solutions:**
- **OOM:** Increase memory limits if seeing OOM kills
- **Unhandled exceptions:** Check logs for stack traces, may need code fix
- **Kafka connection loss:** Check Kafka broker health
- **Graceful shutdown:** Ensure SIGTERM handling is working

---

## DLQ Management

The ClaimX pipeline includes a CLI tool for managing Dead Letter Queues (DLQs).

### DLQ CLI Usage

```bash
python -m kafka_pipeline.claimx.dlq.cli <command> [options]
```

### Commands

#### 1. List DLQ Counts

Show message counts for both enrichment and download DLQs:

```bash
python -m kafka_pipeline.claimx.dlq.cli list
```

**Output:**
```
Enrichment DLQ: 142 messages
Download DLQ: 37 messages
```

#### 2. Inspect Messages

View sample messages from a DLQ with full metadata:

```bash
# Inspect enrichment DLQ (default: 10 messages)
python -m kafka_pipeline.claimx.dlq.cli inspect enrichment

# Inspect download DLQ with custom limit
python -m kafka_pipeline.claimx.dlq.cli inspect download --limit 20
```

**Output:**
```
=== Message 1 ===
Offset: 1234
Timestamp: 2026-01-04 12:34:56
Event ID: evt_001
Event Type: PROJECT_CREATED
Project ID: 123
Retry Count: 4
Error: API call failed: 500 Internal Server Error
Error Category: TRANSIENT
---
```

#### 3. Replay Messages

Replay messages from DLQ back to the pending topic with retry count reset:

```bash
# Replay all enrichment DLQ messages
python -m kafka_pipeline.claimx.dlq.cli replay enrichment

# Replay specific event IDs
python -m kafka_pipeline.claimx.dlq.cli replay enrichment --event-ids evt_001,evt_002

# Replay all download DLQ messages
python -m kafka_pipeline.claimx.dlq.cli replay download

# Replay specific media IDs
python -m kafka_pipeline.claimx.dlq.cli replay download --media-ids 1001,1002
```

**Behavior:**
- Reads messages from DLQ
- Resets `retry_count` to 0
- Produces to pending topic (enrichment or download)
- Messages will be reprocessed by workers

**Use cases:**
- After fixing API issues
- After fixing configuration
- For transient failures that may succeed on retry

#### 4. Purge DLQ

Clear all messages from a DLQ (destructive operation):

```bash
# Purge enrichment DLQ (requires confirmation)
python -m kafka_pipeline.claimx.dlq.cli purge enrichment

# Purge download DLQ
python -m kafka_pipeline.claimx.dlq.cli purge download
```

**Safety:**
- Prompts for confirmation before purging
- Permanently deletes messages (cannot be recovered)

**Use cases:**
- After analyzing and deciding messages are not recoverable
- Cleaning up test/development DLQs
- After manual processing of failed messages

### DLQ Best Practices

1. **Monitor DLQ Growth:** Alert when DLQ count increases rapidly
2. **Investigate Patterns:** Use `inspect` to identify common failure causes
3. **Fix Root Cause First:** Don't replay until underlying issue is resolved
4. **Selective Replay:** Use ID filters to replay specific messages
5. **Document Decisions:** Log why messages were purged vs. replayed

---

## Operational Procedures

### Routine Operations

#### Daily Checks

```bash
# Check worker health
for port in 8081 8082 8083 8084; do
  curl -s http://localhost:$port/health/ready && echo "Port $port: OK" || echo "Port $port: FAIL"
done

# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --group claimx-enricher --describe | grep LAG

# Check DLQ counts
python -m kafka_pipeline.claimx.dlq.cli list
```

#### Weekly Reviews

- Review DLQ messages and identify patterns
- Check performance metrics trends
- Review error logs for recurring issues
- Validate Delta table row counts

#### Monthly Tasks

- Review and optimize worker scaling
- Analyze cost metrics (API calls, storage)
- Update runbook with new issues/solutions
- Review and update alert thresholds

### Backfill Procedure

To backfill historical ClaimX events:

**Step 1: Prepare Backfill Data**

```bash
# Export events from Kusto to Kafka topic
# (Use your Kusto export tool)
```

**Step 2: Create Backfill Topic**

```bash
# Create temporary backfill topic
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --create --topic claimx.events.backfill \
  --partitions 10 --replication-factor 3
```

**Step 3: Configure Ingester for Backfill**

```bash
# Set environment to consume from backfill topic
export CLAIMX_EVENTS_TOPIC="claimx.events.backfill"

# Start ingester in backfill mode
python -m kafka_pipeline --worker claimx-ingester
```

**Step 4: Monitor Progress**

```bash
# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --group claimx-ingester --describe
```

**Step 5: Cleanup**

```bash
# After backfill completes, delete temporary topic
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --delete --topic claimx.events.backfill
```

### Disaster Recovery

#### Scenario 1: Kafka Cluster Failure

**Recovery Steps:**

1. **Wait for Kafka Recovery:** Workers will retry connection automatically
2. **Check Consumer Groups:** After recovery, verify consumer group offsets
3. **Manual Reset (if needed):** Reset to earliest if offsets lost

```bash
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --group claimx-enricher --reset-offsets --to-earliest \
  --topic claimx.enrichment.pending --execute
```

#### Scenario 2: Delta Table Corruption

**Recovery Steps:**

1. **Stop Workers:** Prevent further writes
2. **Restore from Backup:** Use Delta Lake time travel

```python
from deltalake import DeltaTable

# Restore table to previous version
dt = DeltaTable("path/to/claimx_projects")
dt.restore(version=123)  # or datetime
```

3. **Restart Workers:** Resume processing

#### Scenario 3: ClaimX API Outage

**Expected Behavior:**
- Circuit breaker opens after consecutive failures
- Workers stop making API calls
- Messages retry with backoff
- DLQ receives exhausted retries

**Recovery:**
- Wait for API recovery
- Circuit breaker will auto-close after successful request
- Replay DLQ if needed

---

## Performance Tuning

### Baseline Performance Metrics

From WP-8.3 performance tests:

- **ProjectHandler:** 25,000 events/sec
- **MediaHandler:** 54,000 events/sec (with 100x batching)
- **Downloads:** 98/sec @ 1x → 1,565/sec @ 20x concurrency
- **Batch Processing:** 30-56k events/sec (batch sizes 10-200)
- **API Call Optimization:** 50x deduplication for same-project events

### Tuning Parameters

#### Enrichment Worker

**Batch Size** (`ENRICHMENT_BATCH_SIZE`)
- Default: 100
- Range: 50-500
- Higher = better throughput, higher latency
- Lower = lower latency, lower throughput

**API Concurrency** (`CLAIMX_API_CONCURRENCY`)
- Default: 10
- Range: 5-50
- Higher = more parallel API calls
- Watch for rate limiting

**Consumer Count**
- Default: 1 per worker
- Scale workers up to match topic partitions
- More workers = higher throughput

#### Download Worker

**Concurrency** (`DOWNLOAD_CONCURRENCY`)
- Default: 10
- Range: 5-50
- Linear scaling observed up to 20x
- Higher = more parallel downloads

**Batch Size** (`DOWNLOAD_BATCH_SIZE`)
- Default: 20
- Range: 10-100
- Number of download tasks fetched per batch

#### Upload Worker

**Concurrency** (same as download)
- Default: 10
- Range: 5-30
- I/O bound, benefits from parallelism

**Upload Timeout**
- Default: 60s
- Increase for large files (>500MB)

### Optimization Strategies

#### 1. Maximize Batching

MediaHandler automatically batches by project_id. To maximize batching:

- Increase `ENRICHMENT_BATCH_SIZE` to accumulate more events
- Order events by project_id before producing (if possible)
- Monitor batching ratio in metrics

#### 2. Scale Horizontally

- Add worker replicas up to topic partition count
- Use Kubernetes HorizontalPodAutoscaler based on consumer lag:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: claimx-enricher-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: claimx-enricher
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: External
    external:
      metric:
        name: kafka_consumer_lag
        selector:
          matchLabels:
            topic: claimx.enrichment.pending
      target:
        type: AverageValue
        averageValue: "1000"
```

#### 3. Optimize Delta Writes

- Use partitioning on Delta tables by project_id or date
- Enable Z-ordering for frequently queried columns
- Compact small files regularly

```python
from deltalake import DeltaTable

# Optimize table
dt = DeltaTable("path/to/claimx_projects")
dt.optimize()
dt.vacuum()  # Remove old files
```

#### 4. Monitor and Adjust

- Start with defaults
- Monitor throughput and latency
- Increase concurrency/batch size gradually
- Watch for degradation (API rate limits, memory pressure)

### Resource Sizing

**Small Deployment** (< 1M events/day):
- Enricher: 2 replicas, 512Mi RAM, 500m CPU
- Downloader: 2 replicas, 256Mi RAM, 250m CPU
- Uploader: 2 replicas, 256Mi RAM, 250m CPU

**Medium Deployment** (1-10M events/day):
- Enricher: 5 replicas, 1Gi RAM, 1000m CPU
- Downloader: 10 replicas, 512Mi RAM, 500m CPU
- Uploader: 5 replicas, 512Mi RAM, 500m CPU

**Large Deployment** (> 10M events/day):
- Enricher: 10 replicas, 2Gi RAM, 2000m CPU
- Downloader: 20 replicas, 1Gi RAM, 1000m CPU
- Uploader: 10 replicas, 1Gi RAM, 1000m CPU

---

## Appendix

### Topic Retention Policies

```bash
# Main topics: 7 days retention
kafka-configs.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --alter --entity-type topics --entity-name claimx.events.raw \
  --add-config retention.ms=604800000

# Retry topics: Match longest retry delay (40m + buffer)
kafka-configs.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --alter --entity-type topics --entity-name claimx.enrichment.pending.retry.40m \
  --add-config retention.ms=3600000

# DLQ topics: 30 days retention (for investigation)
kafka-configs.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --alter --entity-type topics --entity-name claimx.enrichment.dlq \
  --add-config retention.ms=2592000000
```

### Useful Commands Reference

```bash
# List all ClaimX topics
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --list | grep claimx

# Check topic lag
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --describe --group claimx-enricher

# Tail events topic
kafka-console-consumer.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic claimx.events.raw --from-beginning --max-messages 10

# Count messages in topic
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list $KAFKA_BOOTSTRAP_SERVERS \
  --topic claimx.enrichment.pending --time -1
```

### Contact & Support

- **Team:** Data Platform Team
- **Slack:** #claimx-pipeline
- **On-call:** PagerDuty rotation
- **Runbook Issues:** File PR to update this document

---

**End of Runbook**
