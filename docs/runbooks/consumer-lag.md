# Consumer Lag Runbook

## Overview

Consumer lag occurs when a Kafka consumer cannot process messages as fast as they are being produced. This runbook covers detection, diagnosis, and remediation of consumer lag issues.

## Symptoms

- **Alert**: `KafkaConsumerLagHigh` or `KafkaConsumerLagWarning`
- **Dashboard**: Consumer Health dashboard shows increasing lag
- **User Impact**: Delayed data processing, files not appearing in OneLake promptly
- **Metrics**: `kafka_consumer_lag` > threshold (5,000 warning, 10,000 critical)

## Severity Classification

| Lag Level | Severity | Action Required | Expected Resolution Time |
|-----------|----------|-----------------|-------------------------|
| < 5,000 messages | Normal | Monitor | N/A |
| 5,000-10,000 messages | Warning | Investigate within 30 minutes | < 2 hours |
| > 10,000 messages | Critical | Immediate investigation | < 1 hour |
| > 100,000 messages | Critical | Escalate to L3 | < 30 minutes |

## Initial Triage (5 minutes)

### Step 1: Identify Affected Consumers

```bash
# List all consumer groups and their lag
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --list | xargs -I {} kafka-consumer-groups \
  --bootstrap-server $KAFKA_BROKER --group {} --describe

# Check specific consumer group
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-event-ingester --describe
```

**Look for**:
- `LAG` column values > 10,000
- `CURRENT-OFFSET` not increasing
- Consumer `STATE`: should be `Stable`, not `Rebalancing` or `Dead`

### Step 2: Check Consumer Health

```bash
# Check if workers are running (Kubernetes example)
kubectl get pods -n kafka-pipeline -l app=download-worker

# Check recent logs for errors
kubectl logs -n kafka-pipeline <pod-name> --tail=100 | grep -i error

# Check resource usage
kubectl top pods -n kafka-pipeline
```

**Look for**:
- Pods in `CrashLoopBackOff` or `Error` state
- Out-of-memory (OOM) errors in logs
- High CPU/memory usage near limits

### Step 3: Check Grafana Dashboards

Open **Consumer Health** dashboard and check:

- **Consumer Lag by Topic**: Which topics are lagging?
- **Partition Assignments**: Are all partitions assigned?
- **Offset Progress**: Is offset increasing or stuck?
- **Error Rate**: Are consumers experiencing errors?
- **Rebalance Events**: Frequent rebalances indicate instability

## Root Cause Analysis

### Scenario 1: Slow Message Processing

**Symptoms**:
- Offset increases slowly
- High processing latency (p95 > 5 seconds)
- No errors in logs

**Common Causes**:
1. Slow downstream operations (OneLake uploads, Delta writes)
2. Network latency to external services
3. Large file downloads taking too long
4. Inefficient message processing code

**Diagnostic Commands**:

```bash
# Check processing latency distribution
curl -s http://localhost:8000/metrics | grep kafka_message_processing_duration

# Check download latency
curl -s http://localhost:8000/metrics | grep download_duration

# Check OneLake upload latency
curl -s http://localhost:8000/metrics | grep onelake_upload_duration
```

**Resolution**:
- **Short-term**: Scale consumer instances horizontally (see [Scaling Operations](./scaling-operations.md))
- **Long-term**: Optimize slow operations, increase timeouts if needed

### Scenario 2: Consumer Stuck/Not Processing

**Symptoms**:
- Offset not increasing at all
- No logs being written
- Consumer appears healthy but inactive

**Common Causes**:
1. Consumer in infinite loop or deadlock
2. Waiting on blocked resource (database connection, file handle)
3. Circuit breaker stuck open
4. Thread starvation

**Diagnostic Commands**:

```bash
# Check if consumer is alive
kubectl exec -n kafka-pipeline <pod-name> -- ps aux | grep python

# Check thread dumps (if available)
kubectl exec -n kafka-pipeline <pod-name> -- kill -SIGUSR1 <pid>
kubectl logs -n kafka-pipeline <pod-name> | grep "Thread dump"

# Check circuit breaker state
curl -s http://localhost:8000/metrics | grep kafka_circuit_breaker_state
```

**Resolution**:
1. Restart affected consumer instances
2. Check circuit breaker status (see [Circuit Breaker Runbook](./circuit-breaker-open.md))
3. Review application logs for deadlock or blocking operations

### Scenario 3: Partition Imbalance

**Symptoms**:
- Some consumers have no lag, others have high lag
- Uneven partition distribution
- Frequent rebalancing

**Common Causes**:
1. Too few consumer instances for partition count
2. Consumer group rebalancing issues
3. Sticky partition assignment not working

**Diagnostic Commands**:

```bash
# Check partition assignments
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe

# Check consumer instance count vs partition count
kubectl get pods -n kafka-pipeline -l app=download-worker | wc -l
```

**Partition to Consumer Ratio**:
- **Ideal**: 2-4 partitions per consumer instance
- **Acceptable**: 1-6 partitions per consumer instance
- **Poor**: 7+ partitions per consumer instance (scale up consumers)

**Resolution**:
1. Scale consumer instances to match partition count (see [Scaling Operations](./scaling-operations.md))
2. If partition count is too low for scale needs, increase partitions (requires Kafka admin)
3. Verify consumer group configuration uses `range` or `sticky` assignment strategy

### Scenario 4: Sudden Spike in Event Volume

**Symptoms**:
- Lag increases rapidly
- No errors, consumers processing normally
- Event production rate unusually high

**Common Causes**:
1. Batch upload from source system
2. Replay of historical events
3. Duplicate event production

**Diagnostic Commands**:

```bash
# Check message production rate
curl -s http://localhost:8000/metrics | grep kafka_messages_produced_total

# Check topic message count
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list $KAFKA_BROKER \
  --topic xact.events.raw
```

**Resolution**:
- **Normal spike**: Scale consumers temporarily, lag will clear within acceptable time
- **Abnormal spike**: Investigate event source, consider rate limiting at ingestion
- **Replay scenario**: Coordinate with source system team, may need temporary capacity increase

### Scenario 5: No Partitions Assigned

**Symptoms**:
- Alert: `KafkaConsumerNoPartitions`
- Consumer connected but lag not decreasing
- No error logs

**Common Causes**:
1. More consumer instances than partitions
2. Consumer group misconfiguration
3. Kafka broker issues

**Diagnostic Commands**:

```bash
# Check partition assignment
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe

# Check topic partition count
kafka-topics --bootstrap-server $KAFKA_BROKER \
  --topic xact.downloads.pending --describe
```

**Resolution**:
- If consumers > partitions: This is normal for over-provisioning, extra consumers are standby
- If consumers < partitions but no assignments: Check Kafka broker logs, may need broker restart
- If all consumers have no partitions: Verify consumer group ID is correct, check topic exists

## Remediation Procedures

### Option 1: Scale Consumer Instances (Recommended)

**When to use**: Lag is consistently high but consumers are healthy

```bash
# Kubernetes: Scale deployment
kubectl scale deployment download-worker \
  --replicas=<new-count> -n kafka-pipeline

# Verify new pods are running and assigned partitions
kubectl get pods -n kafka-pipeline -l app=download-worker
```

**Best Practices**:
- Scale gradually (2x at a time)
- Wait 2-3 minutes between scale operations for rebalancing
- Monitor lag decrease after scaling
- Max recommended: number of partitions (extras will be idle)

**Expected Impact**:
- Lag should decrease proportionally to scale increase
- 100k lag with 3 consumers → add 3 more consumers = lag clears in ~5 minutes

### Option 2: Restart Consumer Instances

**When to use**: Consumer appears stuck or degraded

```bash
# Kubernetes: Rolling restart
kubectl rollout restart deployment download-worker -n kafka-pipeline

# Monitor rollout status
kubectl rollout status deployment download-worker -n kafka-pipeline

# Verify consumers reconnected
kubectl logs -n kafka-pipeline -l app=download-worker --tail=20
```

**Caution**: Restarting causes temporary rebalancing, may briefly increase lag

### Option 3: Pause Event Ingestion (Emergency Only)

**When to use**: Critical lag (>1M messages), system overwhelmed, need time to investigate

```bash
# Scale event ingester to zero
kubectl scale deployment event-ingester --replicas=0 -n kafka-pipeline

# Verify ingestion stopped
curl -s http://localhost:8000/metrics | grep kafka_messages_produced_total
```

**Warning**: Stops new events from entering pipeline. Only use in emergency situations.

**Recovery**:

```bash
# Resume ingestion
kubectl scale deployment event-ingester --replicas=<original-count> -n kafka-pipeline
```

## Monitoring Recovery

After applying remediation, monitor these metrics:

1. **Consumer Lag** (target: < 1,000 messages)
   ```bash
   watch -n 10 "kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
     --group xact-download-worker --describe | grep LAG"
   ```

2. **Processing Rate** (target: > 100 messages/second)
   ```bash
   curl -s http://localhost:8000/metrics | grep kafka_messages_consumed_total
   ```

3. **Error Rate** (target: < 1%)
   ```bash
   curl -s http://localhost:8000/metrics | grep kafka_processing_errors_total
   ```

**Recovery Timeline**:
- **10k lag**: Should clear in < 10 minutes after remediation
- **100k lag**: Should clear in < 30 minutes after scaling
- **1M+ lag**: May require 1-2 hours, consider further scaling

## Prevention

### Capacity Planning

- **Review lag metrics weekly**: Identify trends before they become incidents
- **Set autoscaling policies**: Automatic scale-up when lag > 5,000
- **Regular load testing**: Validate system can handle peak loads

### Proactive Monitoring

- **Alert thresholds**: Warning at 5k, critical at 10k
- **Lag growth rate**: Alert if lag increasing > 1,000 messages/minute
- **Dashboard review**: Include lag metrics in daily operations dashboard

### Code Optimization

- **Profile slow operations**: Identify bottlenecks in download/upload code
- **Optimize Delta writes**: Batch efficiently, use async operations
- **Connection pooling**: Reuse HTTP connections, reduce overhead

## Escalation Criteria

Escalate to L3 Engineering if:

- [ ] Lag > 100,000 messages for > 1 hour
- [ ] Lag continues increasing after scaling consumers
- [ ] Consumer crashes repeatedly (> 3 times in 30 minutes)
- [ ] Root cause is not identified within 2 hours
- [ ] Business impact is severe (SLA breach imminent)

**Escalation Information to Provide**:
1. Current lag level and trend (increasing/stable/decreasing)
2. Consumer group name and topic
3. Recent scaling or configuration changes
4. Error logs from affected consumers
5. Grafana dashboard screenshots
6. Actions already attempted

## Related Runbooks

- [Scaling Operations](./scaling-operations.md) - Detailed scaling procedures
- [Circuit Breaker Open](./circuit-breaker-open.md) - If consumers are blocked by circuit breaker
- [Incident Response](./incident-response.md) - If lag causes business impact

---

**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team
**Reviewers**: SRE Team, Data Engineering Team
