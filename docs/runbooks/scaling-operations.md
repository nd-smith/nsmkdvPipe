# Scaling Operations Runbook

## Overview

This runbook covers horizontal scaling of Kafka pipeline workers, capacity planning, and performance optimization. The pipeline is designed for elastic scaling to handle varying workloads.

## When to Scale

### Scale Up Triggers

| Metric | Threshold | Action | Urgency |
|--------|-----------|--------|---------|
| Consumer Lag | > 10,000 messages | Scale up immediately | Critical |
| Processing Latency p95 | > 5 seconds | Scale up within 1 hour | Warning |
| CPU Usage | > 80% sustained | Scale up within 1 hour | Warning |
| Memory Usage | > 85% sustained | Scale up within 30 minutes | Critical |
| Error Rate | > 1% due to timeouts | Scale up within 1 hour | Warning |
| Throughput | < target (100 msg/s per worker) | Scale up within 4 hours | Info |

### Scale Down Triggers

| Metric | Threshold | Action | Wait Period |
|--------|-----------|--------|-------------|
| Consumer Lag | < 1,000 messages sustained | Consider scale down | 1 hour |
| CPU Usage | < 30% sustained | Scale down | 4 hours |
| Memory Usage | < 40% sustained | Scale down | 4 hours |
| Over-provisioned | More workers than partitions | Scale down | Immediate |

**Important**: Always scale down gradually and monitor. Never scale below minimum required capacity.

## Scaling Architecture

### Component Scaling Characteristics

| Component | Scalability | Max Instances | Limited By |
|-----------|-------------|---------------|------------|
| **Event Ingester** | Horizontal | Number of partitions | Topic: events.raw partitions |
| **Download Worker** | Horizontal | Number of partitions | Topic: downloads.pending partitions |
| **Result Processor** | Horizontal | Number of partitions | Topic: downloads.results partitions |
| **DLQ Handler** | Single instance | 1 | Manual processing workflow |
| **Retry Scheduler** | Single instance | 1 | Needs global view of retry topics |

### Partition Configuration

Current partition counts (example - verify actual configuration):

| Topic | Partitions | Recommended Workers |
|-------|-----------|---------------------|
| `xact.events.raw` | 12 | 3-6 event ingesters |
| `xact.downloads.pending` | 24 | 6-12 download workers |
| `xact.downloads.results` | 6 | 2-3 result processors |
| `xact.downloads.dlq` | 3 | 1 DLQ handler |
| `xact.downloads.retry.*` | 3 each | 1 retry scheduler |

**Golden Rule**: Workers should be ≤ partition count. Extras will be idle.

## Horizontal Scaling Procedures

### Scale Up Download Workers (Most Common)

**When**: Consumer lag > 10k or latency > 5s

```bash
# 1. Check current scale
kubectl get deployment download-worker -n kafka-pipeline

# Current: READY 6/6

# 2. Determine target scale
# Rule of thumb: 2-4 partitions per worker
# If 24 partitions and lag is high: scale to 8-12 workers

# 3. Scale deployment
kubectl scale deployment download-worker --replicas=9 -n kafka-pipeline

# 4. Monitor rollout
kubectl rollout status deployment download-worker -n kafka-pipeline

# Expected: New pods starting, should be Running within 30-60s
```

**Verification**:

```bash
# 1. Verify all pods running
kubectl get pods -n kafka-pipeline -l app=download-worker

# Expected: 9/9 pods Running, READY 1/1

# 2. Check partition assignment
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe

# Expected: Partitions evenly distributed (24 partitions / 9 workers ≈ 2-3 each)

# 3. Monitor lag decrease
watch -n 10 "kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe | grep LAG"

# Expected: Lag decreasing steadily

# 4. Check resource usage
kubectl top pods -n kafka-pipeline -l app=download-worker

# Expected: CPU and memory within limits
```

**Timeline**:
- Pods start: 30-60 seconds
- Consumer group rebalance: 10-30 seconds
- Lag starts decreasing: Immediate after rebalance
- Full lag recovery: Depends on lag size (10k lag ≈ 5-10 minutes with proper scale)

### Scale Up Event Ingesters

**When**: Events topic lag high or ingestion rate below expected

```bash
# 1. Check current scale
kubectl get deployment event-ingester -n kafka-pipeline

# 2. Scale up (max = partition count)
kubectl scale deployment event-ingester --replicas=6 -n kafka-pipeline

# 3. Monitor ingestion rate increase
curl -s http://localhost:8000/metrics | grep kafka_messages_consumed_total

# Expected: Message consumption rate increases
```

### Scale Up Result Processors

**When**: Results topic lag high or batch processing slow

```bash
# 1. Check current scale
kubectl get deployment result-processor -n kafka-pipeline

# 2. Scale up
kubectl scale deployment result-processor --replicas=3 -n kafka-pipeline

# 3. Verify Delta write throughput increases
curl -s http://localhost:8000/metrics | grep delta_writes_total

# Expected: Write rate increases
```

### Scale Down Workers (Gradual)

**When**: System over-provisioned, lag consistently low, low resource usage

```bash
# IMPORTANT: Scale down gradually (1-2 instances at a time)
# Monitor between each scale-down operation

# 1. Verify lag is low and sustained
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe

# Lag should be < 1,000 for at least 1 hour

# 2. Scale down incrementally
kubectl scale deployment download-worker --replicas=7 -n kafka-pipeline

# 3. Monitor for 15 minutes
watch -n 30 "kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe"

# 4. If lag remains low, continue scaling down
kubectl scale deployment download-worker --replicas=6 -n kafka-pipeline

# 5. Stop if lag starts increasing
# Do not scale below minimum required capacity
```

**Minimum Recommended Scale**:
- Event Ingester: 2 instances (for availability)
- Download Worker: 4 instances (2-3 partitions each)
- Result Processor: 2 instances (for availability)

## Auto-Scaling Configuration

### Horizontal Pod Autoscaler (HPA)

Enable automatic scaling based on CPU/memory:

```yaml
# download-worker-hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: download-worker-hpa
  namespace: kafka-pipeline
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: download-worker
  minReplicas: 4
  maxReplicas: 12  # Should not exceed partition count
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
      - type: Pods
        value: 2
        periodSeconds: 60
      selectPolicy: Max
    scaleDown:
      stabilizationWindowSeconds: 300  # 5 minute cooldown
      policies:
      - type: Percent
        value: 25
        periodSeconds: 60
      selectPolicy: Min
```

**Apply HPA**:

```bash
kubectl apply -f download-worker-hpa.yaml -n kafka-pipeline

# Verify HPA status
kubectl get hpa -n kafka-pipeline

# Monitor HPA decisions
kubectl describe hpa download-worker-hpa -n kafka-pipeline
```

### Custom Metrics Autoscaling (KEDA)

For more advanced autoscaling based on Kafka lag:

```yaml
# download-worker-scaledobject.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: download-worker-scaler
  namespace: kafka-pipeline
spec:
  scaleTargetRef:
    name: download-worker
  minReplicaCount: 4
  maxReplicaCount: 12
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: $KAFKA_BROKER
      consumerGroup: xact-download-worker
      topic: xact.downloads.pending
      lagThreshold: "1000"  # Scale up if lag > 1000 per partition
      offsetResetPolicy: latest
```

**Benefits of KEDA**:
- Scales based on actual consumer lag, not just CPU/memory
- More responsive to workload changes
- Can scale to zero during idle periods (if configured)

## Performance Optimization

### Vertical Scaling (Resource Limits)

If horizontal scaling doesn't help, consider increasing per-pod resources:

```yaml
# In deployment spec
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "1Gi"    # Increased from 512Mi
    cpu: "1000m"     # Increased from 500m
```

**When to increase resources**:
- Workers consistently hitting memory limits (OOMKilled)
- CPU usage at 100% sustained
- Download/upload operations slow due to resource constraints

**Caution**: Increasing limits reduces total worker capacity on nodes. May need to add cluster nodes.

### Download Concurrency Tuning

Adjust concurrent downloads per worker:

```yaml
# Environment variable in deployment
env:
  - name: DOWNLOAD_CONCURRENCY
    value: "10"  # Increase from default 5
```

**Tuning Guidelines**:
| Worker Resources | Recommended Concurrency |
|------------------|-------------------------|
| 512Mi RAM, 500m CPU | 5 parallel downloads |
| 1Gi RAM, 1000m CPU | 10 parallel downloads |
| 2Gi RAM, 2000m CPU | 20 parallel downloads |

**Trade-offs**:
- Higher concurrency = better throughput but more memory usage
- Too high = OOM or network saturation
- Monitor memory usage and adjust accordingly

### Batch Size Tuning (Result Processor)

Adjust Delta write batch size:

```yaml
env:
  - name: BATCH_SIZE
    value: "200"  # Increase from default 100
  - name: BATCH_TIMEOUT_SECONDS
    value: "10"   # Increase from default 5
```

**Tuning Guidelines**:
- Larger batches = more efficient Delta writes but higher latency
- Smaller batches = lower latency but more write operations
- Recommended: 100-500 messages per batch

### Kafka Consumer Tuning

Optimize Kafka consumer performance:

```yaml
env:
  - name: KAFKA_MAX_POLL_RECORDS
    value: "100"  # Messages fetched per poll
  - name: KAFKA_FETCH_MIN_BYTES
    value: "1024"  # Minimum bytes to fetch
  - name: KAFKA_FETCH_MAX_WAIT_MS
    value: "500"   # Max wait for min bytes
```

**Tuning for Throughput**:
- Increase `max_poll_records` to fetch more messages (100-500)
- Increase `fetch_min_bytes` to reduce network calls (1KB-100KB)
- Reduce `fetch_max_wait_ms` for lower latency (100-500ms)

**Tuning for Latency**:
- Decrease `max_poll_records` to process faster (10-50)
- Decrease `fetch_min_bytes` to fetch immediately (1 byte)
- Decrease `fetch_max_wait_ms` for immediate processing (50-100ms)

## Capacity Planning

### Calculating Required Capacity

**Formula**:
```
Required Workers = (Event Rate × Processing Time) / Target Throughput

Where:
- Event Rate: Messages per second produced
- Processing Time: Seconds per message (p95 latency)
- Target Throughput: Messages per second per worker
```

**Example**:
```
Event Rate: 1,000 messages/second
Processing Time: 3 seconds (p95 latency)
Target Throughput: 100 messages/second per worker (desired)

Required Workers = (1000 × 3) / 100 = 30 workers

But we only have 24 partitions, so max effective workers = 24
To handle this load, we need to either:
1. Increase partitions to 30+
2. Optimize processing time to < 2.4s per message
```

### Partition Scaling

To increase maximum worker capacity, increase partition count:

**Warning**: Increasing partitions cannot be reversed easily. Plan carefully.

```bash
# Increase partitions (Kafka admin required)
kafka-topics --bootstrap-server $KAFKA_BROKER \
  --alter --topic xact.downloads.pending \
  --partitions 36

# Verify new partition count
kafka-topics --bootstrap-server $KAFKA_BROKER \
  --describe --topic xact.downloads.pending

# After partition increase, scale workers
kubectl scale deployment download-worker --replicas=12 -n kafka-pipeline
```

**Best Practices**:
- Plan for 2-3x current peak load
- Partition count should be multiple of expected max worker count
- Consider future growth (partitions can't easily be reduced)
- Recommended: 24-48 partitions for download workers

### Resource Capacity Planning

**Per-Worker Resource Usage** (baseline):

| Component | CPU (avg) | CPU (peak) | Memory (avg) | Memory (peak) |
|-----------|-----------|------------|--------------|---------------|
| Event Ingester | 200m | 400m | 256Mi | 384Mi |
| Download Worker | 400m | 800m | 384Mi | 768Mi |
| Result Processor | 300m | 600m | 256Mi | 512Mi |

**Cluster Capacity Calculation**:

```
Download Workers: 12 instances × 800m CPU × 1.2 buffer = 11.5 cores
Download Workers: 12 instances × 768Mi RAM × 1.2 buffer = 11GB RAM

Total Pipeline (all components):
- CPU: ~15 cores (with buffer)
- Memory: ~15GB (with buffer)
- Storage: Minimal (logs only, stateless workers)
```

**Recommendation**: Provision 20-30% above calculated needs for headroom.

## Monitoring Scaling Operations

### Pre-Scale Checklist

Before scaling operation:

- [ ] Current lag level and trend (increasing/stable/decreasing)
- [ ] Current resource usage (CPU, memory)
- [ ] Recent error rate and types
- [ ] Partition count and distribution
- [ ] Recent deployment or configuration changes

### During Scale Monitoring

Watch these metrics during scaling:

```bash
# 1. Watch consumer group rebalancing
kubectl logs -n kafka-pipeline -l app=download-worker --tail=50 -f | \
  grep -i "rebalance\|partition"

# 2. Monitor lag in real-time
watch -n 5 "kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe | grep LAG"

# 3. Watch resource usage
watch -n 10 "kubectl top pods -n kafka-pipeline -l app=download-worker"

# 4. Monitor error rate
watch -n 10 "curl -s http://localhost:8000/metrics | \
  grep kafka_processing_errors_total | tail -5"
```

### Post-Scale Validation (15-30 minutes)

After scaling operation, verify:

**Immediate** (5 minutes):
- [ ] All new pods running (kubectl get pods)
- [ ] Partition rebalance completed (consumer group describe)
- [ ] No error spike (error rate metrics)
- [ ] Lag trend improving (lag decreasing)

**Short-term** (15 minutes):
- [ ] Lag reduced to acceptable level (< 5,000)
- [ ] Resource usage stable (not hitting limits)
- [ ] Processing latency improved (p95 metric)
- [ ] No circuit breaker opens (circuit breaker metrics)

**Medium-term** (1 hour):
- [ ] Lag remains low and stable
- [ ] Error rate normal (< 1%)
- [ ] No pod restarts or failures
- [ ] Throughput meets expectations

## Troubleshooting Scaling Issues

### Scaling Doesn't Reduce Lag

**Symptoms**: Scaled workers but lag still increasing

**Possible Causes**:

1. **Bottleneck elsewhere**:
   ```bash
   # Check if download/upload is the bottleneck
   curl -s http://localhost:8000/metrics | grep download_duration

   # If download latency is high, scaling workers won't help
   # Need to optimize download performance or external service
   ```

2. **Partition count limiting**:
   ```bash
   # Check if workers > partitions
   kubectl get pods -n kafka-pipeline -l app=download-worker | wc -l
   kafka-topics --describe --topic xact.downloads.pending | grep PartitionCount

   # If workers > partitions, extra workers are idle
   # Need to increase partitions or reduce workers
   ```

3. **Resource limits hit**:
   ```bash
   # Check if pods are resource-constrained
   kubectl top pods -n kafka-pipeline -l app=download-worker
   kubectl describe pods -n kafka-pipeline -l app=download-worker | \
     grep -A 5 "Limits"

   # If CPU/memory at limits, increase resource limits
   ```

### Rebalancing Takes Too Long

**Symptoms**: Consumer group rebalancing for > 2 minutes

**Resolution**:

```bash
# Check rebalance settings
kubectl logs -n kafka-pipeline -l app=download-worker | \
  grep -i "rebalance timeout"

# If timeout is too low, increase:
env:
  - name: KAFKA_MAX_POLL_INTERVAL_MS
    value: "300000"  # 5 minutes (increase from default)
  - name: KAFKA_SESSION_TIMEOUT_MS
    value: "30000"   # 30 seconds
```

### Pods Not Starting After Scale

**Symptoms**: New pods stuck in Pending or ContainerCreating

**Diagnostic**:

```bash
# Check pod events
kubectl describe pod <pod-name> -n kafka-pipeline

# Common issues:
# - Insufficient cluster resources (add nodes)
# - Image pull failures (check registry)
# - Resource quota exceeded (increase quota)
```

## Related Runbooks

- [Consumer Lag](./consumer-lag.md) - Detailed lag investigation
- [Deployment Procedures](./deployment-procedures.md) - For config changes
- [Incident Response](./incident-response.md) - If scaling doesn't resolve issue

---

**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team
**Reviewers**: SRE Team, Capacity Planning Team
