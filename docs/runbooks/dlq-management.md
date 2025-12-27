# DLQ Management Runbook

## Overview

The Dead-Letter Queue (DLQ) captures messages that cannot be processed successfully after exhausting all retry attempts or experiencing permanent failures. This runbook covers inspection, analysis, and replay of DLQ messages.

## Symptoms

- **Alert**: `KafkaDLQGrowthRapid`, `KafkaDLQGrowthWarning`, or `KafkaDLQSizeHigh`
- **Dashboard**: DLQ Monitoring dashboard shows message accumulation
- **User Impact**: Files not downloaded, data incomplete in analytics
- **Metrics**: Messages accumulating in `xact.downloads.dlq` topic

## Severity Classification

| DLQ Rate/Size | Severity | Action Required | Expected Response Time |
|---------------|----------|-----------------|------------------------|
| < 5 messages/10min | Normal | Monitor | Daily review |
| 5-10 messages/10min | Warning | Investigate within 4 hours | Same day |
| > 10 messages/min | Critical | Immediate investigation | < 1 hour |
| > 1,000 total messages | Warning | Manual review required | < 4 hours |

## DLQ Message Lifecycle

```
[Failed Download] → [Retry 1-4x] → [Retry Exhausted OR Permanent Error] → [DLQ]
                                                                              ↓
                                    [Manual Review] → [Replay] OR [Resolve]
                                                         ↓              ↓
                                                [Pending Topic]    [Committed/Archived]
```

## Initial Triage (10 minutes)

### Step 1: Check DLQ Message Count

```bash
# Get current DLQ size
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list $KAFKA_BROKER \
  --topic xact.downloads.dlq | awk -F':' '{sum += $3} END {print sum}'

# Check DLQ growth rate (from metrics)
curl -s http://localhost:8000/metrics | \
  grep 'kafka_messages_produced_total.*dlq'
```

### Step 2: List DLQ Messages

```bash
# List recent DLQ messages (limit 50)
python -m kafka_pipeline.dlq.cli list --limit 50

# List more messages if needed
python -m kafka_pipeline.dlq.cli list --limit 200 --timeout 10000
```

**Output Format**:
```
================================================================================
DLQ Messages (50 total)
================================================================================
Offset   Trace ID             Retry Count  Error Category  URL
--------------------------------------------------------------------------------
12345    evt-abc-123         4            permanent       https://example.com/file1.pdf...
12346    evt-abc-124         0            permanent       https://example.com/invalid...
12347    evt-abc-125         4            transient       https://timeout.com/slow.pdf...
```

### Step 3: Identify Common Patterns

Analyze DLQ messages for:

1. **Error Categories**:
   - `permanent`: Non-retryable (404, invalid URL, file type validation)
   - `transient`: Retryable but exhausted (timeout, 503, network)

2. **Common URLs/Domains**: Same domain failing repeatedly?

3. **Retry Counts**:
   - `0`: Direct to DLQ (permanent error)
   - `4`: Retry exhausted (transient error, but persisted)

4. **Time Patterns**: All failures recent or spread over time?

## Common Failure Patterns

### Pattern 1: Invalid/Expired URLs (404, 410)

**Symptoms**:
- Error category: `permanent`
- Retry count: 0 (direct to DLQ)
- HTTP status: 404 Not Found or 410 Gone

**Example DLQ Message**:
```bash
python -m kafka_pipeline.dlq.cli view evt-abc-123
```

**Output**:
```
Error Information:
  HTTP 404: Not Found - URL no longer available
```

**Root Cause**: Presigned URLs expired or file deleted from source

**Resolution**:
1. **Verify URL status**: Manually check URL in browser or curl
2. **Check presigned URL expiration**: Most presigned URLs expire after 1-7 days
3. **Contact source system**: Request new presigned URL or file re-upload
4. **Replay with new URL**: If new URL available, update and replay

**Prevention**:
- Reduce event-to-download latency (see [Scaling Operations](./scaling-operations.md))
- Negotiate longer presigned URL expiration with source system
- Implement presigned URL refresh mechanism for supported domains

### Pattern 2: File Type Validation Failures

**Symptoms**:
- Error category: `permanent`
- Retry count: 0
- Error: "File type not allowed"

**Example Error**:
```
Error Information:
  File type validation failed: .exe not in allowed types [.pdf, .xml, .jpg, .png]
```

**Root Cause**: File type not in domain allowlist

**Resolution**:
1. **Verify file type is valid**: Is this a legitimate business file?
2. **If legitimate**: Update allowed file types in domain configuration
3. **If malicious/spam**: Confirm rejection is correct, resolve without replay

**Actions**:

```bash
# View message details
python -m kafka_pipeline.dlq.cli view evt-abc-456

# If file type should be allowed, update configuration
# (Requires code change to domain strategy)

# If rejection is correct, mark as resolved
python -m kafka_pipeline.dlq.cli resolve evt-abc-456
```

**Prevention**:
- Review and validate allowed file types with business stakeholders
- Add monitoring for common legitimate file types being rejected

### Pattern 3: Network/Timeout Failures (Retry Exhausted)

**Symptoms**:
- Error category: `transient`
- Retry count: 4 (exhausted all retries)
- Error: Connection timeout, 503 Service Unavailable

**Example Error**:
```
Error Information:
  Connection timeout after 30s - retries exhausted (4 attempts)
```

**Root Cause**: Persistent network issues or slow source servers

**Resolution**:
1. **Check if issue is resolved**: Transient issues may have cleared
2. **Verify source server health**: Contact source system team
3. **Replay message**: If source is healthy, replay to retry download

**Actions**:

```bash
# View message details
python -m kafka_pipeline.dlq.cli view evt-timeout-789

# If source is healthy now, replay
python -m kafka_pipeline.dlq.cli replay evt-timeout-789

# Monitor replay success
tail -f /var/log/kafka-pipeline/download-worker.log | grep evt-timeout-789
```

**Prevention**:
- Increase download timeout for large files
- Implement adaptive timeout based on file size
- Add retry backoff jitter to avoid thundering herd

### Pattern 4: OneLake Upload Failures

**Symptoms**:
- Error category: `transient` (but may become permanent if persistent)
- Download succeeded, upload failed
- Error: Azure authentication, storage quota, network issues

**Example Error**:
```
Error Information:
  OneLake upload failed: AuthenticationError - Token expired
```

**Root Cause**: Azure AD token expiration, OneLake service issues, storage quota

**Resolution**:

1. **Check OneLake service health**:
   ```bash
   # Check Azure status
   curl -s https://status.azure.com/status/ | grep "Storage"
   ```

2. **Verify authentication**:
   ```bash
   # Test OneLake connectivity
   az storage blob list --account-name <account> --container-name <container>
   ```

3. **Check storage quota**:
   - Review OneLake capacity metrics
   - Verify no quota limits reached

4. **Replay message** if issues resolved:
   ```bash
   python -m kafka_pipeline.dlq.cli replay evt-upload-fail-123
   ```

**Prevention**:
- Monitor Azure AD token expiration and refresh proactively
- Set up alerts for OneLake storage quota (< 90% capacity)
- Implement exponential backoff for OneLake transient errors

### Pattern 5: Systematic Failures (Multiple Related)

**Symptoms**:
- Many DLQ messages with same error pattern
- All from same domain or event type
- Rapid DLQ growth (> 10 messages/min)

**Example**:
```bash
# Check DLQ messages
python -m kafka_pipeline.dlq.cli list | grep "example.com"
```

**All showing**:
```
Error Category: permanent
Error: SSL certificate verification failed
```

**Root Cause**: Systemic issue (SSL cert expired, service outage, config change)

**Resolution**:

1. **Stop further failures**:
   ```bash
   # Pause affected worker or topic consumption temporarily
   kubectl scale deployment download-worker --replicas=0 -n kafka-pipeline
   ```

2. **Investigate root cause**:
   - Check source system status
   - Review recent configuration changes
   - Verify SSL certificates are valid

3. **Fix root cause**: Update config, SSL certs, or wait for service recovery

4. **Bulk replay**:
   ```bash
   # After fix, replay all affected messages
   python -m kafka_pipeline.dlq.cli list | grep "example.com" | \
     awk '{print $2}' | xargs -I {} python -m kafka_pipeline.dlq.cli replay {}
   ```

5. **Resume workers**:
   ```bash
   kubectl scale deployment download-worker --replicas=<original-count> -n kafka-pipeline
   ```

**Prevention**:
- Monitor SSL certificate expiration (alert at 30 days before expiry)
- Implement circuit breaker for systematic failures
- Add source system health checks before processing events

## DLQ CLI Commands Reference

### List Messages

```bash
# List DLQ messages
python -m kafka_pipeline.dlq.cli list [OPTIONS]

Options:
  --limit INT     Maximum messages to fetch (default: 100)
  --timeout INT   Timeout in milliseconds (default: 5000)

Examples:
  # List first 50 messages
  python -m kafka_pipeline.dlq.cli list --limit 50

  # List with longer timeout for large DLQ
  python -m kafka_pipeline.dlq.cli list --limit 200 --timeout 10000
```

### View Message Details

```bash
# View specific message
python -m kafka_pipeline.dlq.cli view <trace_id>

Examples:
  # View message by trace ID
  python -m kafka_pipeline.dlq.cli view evt-abc-123

Output includes:
  - Kafka metadata (topic, partition, offset)
  - Message details (trace ID, URL, retry count)
  - Error information (category, final error)
  - Original task details (event type, destination path)
  - Metadata (timestamps, domain info)
```

### Replay Message

```bash
# Replay message to pending topic
python -m kafka_pipeline.dlq.cli replay <trace_id>

Examples:
  # Replay single message
  python -m kafka_pipeline.dlq.cli replay evt-abc-123

What happens:
  1. Message sent back to downloads.pending topic
  2. Retry count reset to 0
  3. Download worker will process again
  4. DLQ offset NOT committed (message remains in DLQ)
```

**Important**: Replay does NOT remove message from DLQ. After successful reprocessing, use `resolve` to commit offset.

### Resolve Message

```bash
# Mark message as resolved (commit offset)
python -m kafka_pipeline.dlq.cli resolve <trace_id>

Examples:
  # Resolve after successful replay
  python -m kafka_pipeline.dlq.cli resolve evt-abc-123

  # Resolve without replay (permanent failure accepted)
  python -m kafka_pipeline.dlq.cli resolve evt-invalid-file

What happens:
  1. Consumer offset committed for this message
  2. Message will not appear in future list commands
  3. Audit log entry created
  4. Message remains in Kafka (retention policy applies)
```

## DLQ Review Workflow

### Daily DLQ Review (15 minutes)

Perform during business hours as part of daily operations:

1. **Check DLQ dashboard**:
   - Open Grafana DLQ Monitoring dashboard
   - Review message count and growth trends
   - Identify any spikes or anomalies

2. **List recent DLQ messages**:
   ```bash
   python -m kafka_pipeline.dlq.cli list --limit 100
   ```

3. **Categorize failures**:
   - Group by error category
   - Identify systematic vs isolated failures
   - Note any new error patterns

4. **Take action**:
   - **Permanent failures**: Verify rejection is correct, resolve
   - **Transient failures**: Replay if issue likely resolved
   - **Systematic failures**: Escalate for investigation

5. **Document trends**:
   - Track common failure patterns in team wiki
   - Share insights with source system teams
   - Update runbooks with new patterns

### Weekly DLQ Cleanup (30 minutes)

Perform weekly to prevent DLQ backlog:

1. **Review old messages** (> 7 days in DLQ):
   ```bash
   # Get message age from timestamp
   python -m kafka_pipeline.dlq.cli list --limit 500
   ```

2. **Bulk replay or resolve**:
   - Messages > 7 days likely need manual intervention
   - Coordinate with data team for impact assessment
   - Bulk replay if issues resolved, bulk resolve if permanent

3. **Generate DLQ report**:
   - Total messages processed
   - Error distribution (permanent vs transient)
   - Top failure domains/URLs
   - Actions taken (replayed vs resolved)

## Monitoring and Alerts

### Key Metrics

```bash
# DLQ message count
curl -s http://localhost:8000/metrics | \
  grep 'kafka_consumer_lag.*dlq'

# DLQ production rate
curl -s http://localhost:8000/metrics | \
  grep 'kafka_messages_produced_total.*dlq'

# Error distribution by category
curl -s http://localhost:8000/metrics | \
  grep 'kafka_processing_errors_total'
```

### Alert Thresholds

| Alert | Condition | Action |
|-------|-----------|--------|
| `KafkaDLQGrowthRapid` | > 10 messages/min for 5 min | Investigate immediately |
| `KafkaDLQGrowthWarning` | > 5 messages/min for 10 min | Review within 4 hours |
| `KafkaDLQSizeHigh` | > 1,000 total messages for 30 min | Schedule cleanup |

## Escalation Criteria

Escalate to L3 Engineering if:

- [ ] DLQ growth > 50 messages/min (systematic failure)
- [ ] Same error affecting > 100 messages
- [ ] Unknown error category or pattern
- [ ] Replay failures (messages return to DLQ immediately)
- [ ] Business critical files stuck in DLQ

**Escalation Information to Provide**:
1. DLQ message count and growth rate
2. Common error patterns (output of `list` command)
3. Sample message details (output of `view` command)
4. Recent configuration or deployment changes
5. Grafana DLQ dashboard screenshots
6. Actions already attempted (replays, resolves)

## Audit Trail

All DLQ operations are logged for compliance and troubleshooting:

```bash
# View DLQ audit log
tail -f /var/log/kafka-pipeline/dlq-audit.log

# Search for specific trace ID
grep "evt-abc-123" /var/log/kafka-pipeline/dlq-audit.log
```

**Audit Log Fields**:
- Timestamp
- Action (list, view, replay, resolve)
- Trace ID
- Kafka offset and partition
- User/operator
- Result (success/failure)

## Related Runbooks

- [Circuit Breaker Open](./circuit-breaker-open.md) - If DLQ messages due to service failures
- [Incident Response](./incident-response.md) - If DLQ growth indicates systematic issue
- [Consumer Lag](./consumer-lag.md) - If lag causes messages to reach DLQ due to timeout

---

**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team
**Reviewers**: Data Engineering Team, SRE Team
