# Circuit Breaker Open Runbook

## Overview

The circuit breaker pattern protects the system from cascading failures when downstream services become unhealthy. When a circuit opens, requests to the failing service are blocked to prevent resource exhaustion and allow the service time to recover.

## Symptoms

- **Alert**: `KafkaCircuitBreakerOpen`, `KafkaConsumerDisconnected`, `KafkaProducerDisconnected`
- **Dashboard**: Circuit breaker state shows `OPEN` (1) instead of `CLOSED` (0)
- **User Impact**: Messages not being consumed or produced, processing stopped
- **Logs**: "Circuit breaker open, rejecting call" errors

## Circuit Breaker States

| State | Value | Meaning | Behavior |
|-------|-------|---------|----------|
| **CLOSED** | 0 | Normal operation | All requests pass through |
| **OPEN** | 1 | Service unhealthy | All requests rejected immediately |
| **HALF_OPEN** | 2 | Testing recovery | Limited requests allowed (probe) |

## State Transitions

```
                    Too many failures
          CLOSED ----------------------> OPEN
             ↑                             |
             |                             |
    Probe succeeds                  Timeout expires
             |                             |
             |                             ↓
          HALF_OPEN <------------------- (Wait)
                |
                |  Probe fails
                └-----------------> OPEN
```

## Severity Classification

| Duration | Severity | Action Required | Expected Resolution |
|----------|----------|-----------------|---------------------|
| < 2 minutes | Info | Monitor | Auto-recovery likely |
| 2-5 minutes | Warning | Investigate | < 15 minutes |
| 5-15 minutes | Critical | Active troubleshooting | < 30 minutes |
| > 15 minutes | Critical | Escalate to L3 | Immediate |

## Initial Triage (5 minutes)

### Step 1: Identify Which Circuit Breaker

```bash
# Check circuit breaker states
curl -s http://localhost:8000/metrics | grep kafka_circuit_breaker_state

# Expected output shows component and state:
# kafka_circuit_breaker_state{component="kafka_consumer"} 1
# kafka_circuit_breaker_state{component="kafka_producer"} 0
# kafka_circuit_breaker_state{component="onelake_client"} 1
```

**Circuit Breakers in System**:
- `kafka_consumer`: Kafka broker connectivity for consumers
- `kafka_producer`: Kafka broker connectivity for producers
- `onelake_client`: Azure OneLake storage operations
- `delta_writer`: Delta Lake write operations

### Step 2: Check Recent Errors

```bash
# View recent circuit breaker errors
kubectl logs -n kafka-pipeline <pod-name> --tail=100 | \
  grep -i "circuit breaker"

# Check failure count
curl -s http://localhost:8000/metrics | grep kafka_circuit_breaker_failures_total
```

**Look for**:
- Timestamp of first failure
- Error messages indicating root cause
- Frequency of failures before circuit opened

### Step 3: Check Underlying Service Health

Based on which circuit breaker is open:

**Kafka Circuit Breaker**:
```bash
# Test Kafka broker connectivity
kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER

# Check if brokers are reachable
ping <kafka-broker-hostname>

# Verify DNS resolution
nslookup <kafka-broker-hostname>
```

**OneLake Circuit Breaker**:
```bash
# Test OneLake connectivity
az storage blob list --account-name <account> --container-name <container>

# Check Azure AD authentication
az account show

# Verify network connectivity to Azure
ping <storage-account>.blob.core.windows.net
```

## Root Cause Analysis

### Scenario 1: Kafka Broker Unreachable

**Symptoms**:
- Circuit breaker: `kafka_consumer` or `kafka_producer`
- State: OPEN
- Error logs: "Connection refused", "Broker not available"

**Common Causes**:
1. Kafka broker down or restarting
2. Network connectivity issues
3. DNS resolution failure
4. Firewall/security group blocking traffic

**Diagnostic Commands**:

```bash
# Test connectivity to Kafka broker
telnet <kafka-broker-hostname> 9093

# Check network route
traceroute <kafka-broker-hostname>

# Verify DNS resolution
dig <kafka-broker-hostname>

# Check Kafka broker status (if accessible)
kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER
```

**Resolution**:

1. **If Kafka broker is down**:
   - Contact Kafka infrastructure team
   - Check Kafka broker logs for errors
   - May need broker restart or failover

2. **If network issue**:
   - Verify network security groups allow traffic on port 9093
   - Check VPN or network connectivity
   - Review recent network changes

3. **Circuit breaker will auto-recover** once broker is reachable (typically 30-60 seconds)

**Expected Recovery**:
- Circuit transitions to HALF_OPEN after timeout
- Test request succeeds
- Circuit transitions to CLOSED
- Normal processing resumes

### Scenario 2: Authentication Failures

**Symptoms**:
- Circuit breaker: `kafka_consumer` or `kafka_producer`
- State: OPEN
- Error logs: "Authentication failed", "Invalid credentials"

**Common Causes**:
1. Azure AD token expired
2. Service principal credentials invalid
3. RBAC permissions changed
4. Token refresh mechanism failing

**Diagnostic Commands**:

```bash
# Check Azure AD authentication
az account show

# Verify service principal has valid credentials
az ad sp list --display-name <service-principal-name>

# Check token expiration
az account get-access-token | jq '.expiresOn'

# Test Kafka authentication manually
kafka-console-consumer --bootstrap-server $KAFKA_BROKER \
  --topic test-topic --consumer.config <auth-config>
```

**Resolution**:

1. **Refresh Azure AD token**:
   ```bash
   # If using CLI credentials, re-login
   az login

   # If using managed identity, verify VM/container identity is attached
   az vm identity show --resource-group <rg> --name <vm-name>
   ```

2. **Verify service principal credentials**:
   - Check Azure Key Vault for client secret (if using SPN)
   - Verify secret has not expired
   - Regenerate credentials if needed

3. **Check RBAC permissions**:
   - Verify service principal has required Kafka roles
   - Check Azure RBAC assignments: `az role assignment list`

4. **Restart application** (circuit breaker will reset):
   ```bash
   kubectl rollout restart deployment <deployment-name> -n kafka-pipeline
   ```

**Expected Recovery**: Immediate after authentication fix + circuit breaker timeout

### Scenario 3: OneLake Upload Failures

**Symptoms**:
- Circuit breaker: `onelake_client`
- State: OPEN
- Error logs: "Upload failed", "Azure storage error"

**Common Causes**:
1. OneLake service degradation
2. Storage quota exceeded
3. Network issues to Azure
4. Invalid storage account credentials

**Diagnostic Commands**:

```bash
# Check Azure Storage service health
curl -s https://status.azure.com/status/ | grep -i storage

# Test OneLake connectivity
az storage blob exists --account-name <account> \
  --container-name <container> --name test.txt

# Check storage account quota
az storage account show --name <account> | jq '.usage'

# Verify network connectivity to Azure
ping <storage-account>.blob.core.windows.net
```

**Resolution**:

1. **If Azure service issue**:
   - Monitor Azure status page
   - Wait for service recovery
   - Circuit breaker will auto-recover when service is healthy

2. **If quota exceeded**:
   - Clean up old files or increase storage quota
   - Coordinate with Azure admin team

3. **If authentication issue**:
   - Verify storage account key or SAS token
   - Check Azure AD permissions for OneLake access
   - Refresh credentials if needed

**Expected Recovery**: Depends on root cause resolution + circuit breaker timeout (30-60s)

### Scenario 4: Delta Lake Write Failures

**Symptoms**:
- Circuit breaker: `delta_writer`
- State: OPEN
- Error logs: "Delta write failed", "Schema mismatch", "Table not found"

**Common Causes**:
1. Delta table schema change
2. OneLake workspace unavailable
3. Permissions issue on Delta table
4. Table corruption

**Diagnostic Commands**:

```bash
# Check Delta table accessibility
# (Requires Delta Lake Python client)
python -c "from deltalake import DeltaTable; \
  print(DeltaTable('<table-path>').schema())"

# Check OneLake workspace health
az rest --method GET --uri "<onelake-workspace-api-endpoint>"

# Review Delta write error logs
kubectl logs -n kafka-pipeline <result-processor-pod> | \
  grep -i "delta.*error"
```

**Resolution**:

1. **If schema mismatch**:
   - Review schema change history
   - Update writer code to match new schema
   - Deploy schema-compatible version

2. **If permissions issue**:
   - Verify service principal has write access to Delta table
   - Check OneLake workspace permissions

3. **If table corruption**:
   - Contact data engineering team
   - May need to rebuild Delta table from checkpoint

**Expected Recovery**: After code/config fix + deployment + circuit breaker timeout

## Circuit Breaker Configuration

Circuit breaker behavior is controlled by configuration:

```python
# Circuit breaker settings (example from code)
CircuitBreakerConfig(
    failure_threshold=5,      # Open after 5 consecutive failures
    recovery_timeout=30,      # Wait 30s before testing recovery (HALF_OPEN)
    expected_exception=Exception,  # Types of exceptions to count
)
```

**Tuning Guidance**:
- **failure_threshold**: Increase for tolerance to transient errors, decrease for faster failure detection
- **recovery_timeout**: Increase for services that need longer recovery, decrease for faster retry
- **expected_exception**: Only count specific exceptions (e.g., ConnectionError) to avoid false opens

## Manual Recovery Procedures

### Force Circuit Breaker Reset (Last Resort)

If circuit breaker stuck open due to misconfiguration or stale state:

```bash
# Restart application to reset circuit breaker state
kubectl rollout restart deployment <deployment-name> -n kafka-pipeline

# Verify circuit breaker closed after restart
curl -s http://localhost:8000/metrics | grep kafka_circuit_breaker_state
```

**Caution**: This does not fix the underlying issue. Use only if:
- Root cause is identified and resolved
- Circuit breaker state is stale/incorrect
- Testing shows underlying service is healthy

### Bypass Circuit Breaker (Emergency Only)

For critical scenarios where circuit breaker is blocking valid operations:

1. **Update configuration** to disable circuit breaker temporarily:
   ```yaml
   # In deployment config
   env:
     - name: CIRCUIT_BREAKER_ENABLED
       value: "false"
   ```

2. **Redeploy** with circuit breaker disabled

3. **Monitor closely** for cascading failures

4. **Re-enable** as soon as underlying issue is resolved

**Warning**: Disabling circuit breaker removes protection from cascading failures. Only use in emergency situations with close monitoring.

## Monitoring Recovery

After resolving the underlying issue, monitor circuit breaker recovery:

### Step 1: Watch Circuit Breaker State

```bash
# Monitor circuit breaker state changes
watch -n 5 "curl -s http://localhost:8000/metrics | \
  grep kafka_circuit_breaker_state"

# Expected progression:
# OPEN (1) → wait for recovery_timeout → HALF_OPEN (2) → probe → CLOSED (0)
```

### Step 2: Verify Normal Operation

```bash
# Check message consumption resumed
curl -s http://localhost:8000/metrics | \
  grep kafka_messages_consumed_total

# Check error rate decreased
curl -s http://localhost:8000/metrics | \
  grep kafka_processing_errors_total
```

### Step 3: Review Grafana Dashboards

- **Pipeline Overview**: Verify throughput returning to normal
- **Consumer Health**: Check lag not increasing
- **Error Rate**: Should decrease to < 1%

**Recovery Timeline**:
- Circuit breaker timeout: 30-60 seconds (configurable)
- Half-open probe: 1-5 seconds
- Full recovery: < 2 minutes after root cause fixed

## Prevention

### Proactive Monitoring

1. **Alert on frequent failures** (before circuit opens):
   - Alert: `KafkaCircuitBreakerFailuresHigh`
   - Threshold: > 5 failures in 10 minutes
   - Action: Investigate before circuit opens

2. **Monitor half-open states**:
   - Frequent half-open states indicate intermittent issues
   - Review logs for recurring error patterns

3. **Track circuit breaker metrics**:
   - Failure rate by component
   - Time spent in OPEN state
   - Recovery success rate

### Service Health Checks

1. **Kafka broker health**:
   - Monitor broker availability and connectivity
   - Alert on broker rebalances or failures

2. **Azure service health**:
   - Subscribe to Azure status notifications
   - Monitor OneLake and storage account health

3. **Authentication token expiration**:
   - Proactively refresh tokens before expiration
   - Alert on token expiration within 1 hour

### Configuration Best Practices

1. **Tune failure thresholds** based on service SLAs:
   - Stable services: Higher threshold (10 failures)
   - Unreliable services: Lower threshold (3 failures)

2. **Set appropriate timeouts**:
   - Fast-recovering services: 10-30s timeout
   - Slow services: 60-120s timeout

3. **Test circuit breaker behavior**:
   - Regularly test recovery procedures in staging
   - Validate circuit breaker opens/closes correctly

## Escalation Criteria

Escalate to L3 Engineering if:

- [ ] Circuit breaker open > 15 minutes
- [ ] Root cause not identified within 30 minutes
- [ ] Circuit repeatedly opens (> 3 times in 1 hour)
- [ ] Multiple circuit breakers open simultaneously
- [ ] Manual recovery procedures ineffective

**Escalation Information to Provide**:
1. Which circuit breaker(s) are open
2. How long circuit has been open
3. Recent error logs showing failure pattern
4. Underlying service health check results
5. Recent configuration or deployment changes
6. Actions already attempted

## Related Runbooks

- [Consumer Lag](./consumer-lag.md) - Circuit breaker prevents consumption, causing lag
- [DLQ Management](./dlq-management.md) - Circuit open may cause messages to DLQ
- [Incident Response](./incident-response.md) - If circuit breaker indicates system-wide issue

---

**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team
**Reviewers**: SRE Team, Infrastructure Team
