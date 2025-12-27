# Incident Response Runbook

## Overview

This runbook provides structured procedures for responding to production incidents affecting the Kafka pipeline. It covers incident classification, triage, resolution, and post-incident activities.

## Incident Severity Levels

| Severity | Definition | Response Time | Examples |
|----------|------------|---------------|----------|
| **SEV-1** | Complete service outage, data loss | < 15 minutes | All workers down, Kafka broker unavailable, data corruption |
| **SEV-2** | Significant degradation, partial outage | < 1 hour | High error rate (>10%), massive lag (>100k), DLQ flooding |
| **SEV-3** | Minor degradation, limited user impact | < 4 hours | Elevated lag (10k-50k), error rate 5-10%, single worker down |
| **SEV-4** | No immediate user impact, monitoring alert | < 24 hours | Warning alerts, minor performance degradation |

## Incident Response Workflow

```
[Alert Fires] → [Acknowledge] → [Triage] → [Incident Commander Assigned]
                                               ↓
    [Resolved] ← [Verify] ← [Implement Fix] ← [Diagnose Root Cause]
         ↓
    [Post-Mortem]
```

## Initial Response (Within 15 minutes)

### Step 1: Acknowledge Alert

```bash
# Acknowledge in monitoring system (PagerDuty, etc.)
# This stops additional pages and starts incident timer

# Join incident response channel
# Teams channel: #kafka-pipeline-incidents
# Create incident ticket: JIRA/ServiceNow
```

### Step 2: Assess Severity

Review these factors to classify severity:

**Impact Assessment**:
- [ ] How many users/systems affected? (All / Most / Some / Few)
- [ ] Is data being lost? (Yes = SEV-1, No = lower)
- [ ] Is data being delayed? (By how much: minutes / hours / days)
- [ ] Are downstream systems impacted? (Analytics, reporting, etc.)

**System Health Quick Check**:

```bash
# 1. Check all workers running
kubectl get pods -n kafka-pipeline

# All CrashLoopBackOff or Error → SEV-1
# Some degraded → SEV-2 or SEV-3

# 2. Check consumer lag
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe

# Lag > 1M messages → SEV-1
# Lag 100k-1M → SEV-2
# Lag 10k-100k → SEV-3

# 3. Check error rate
curl -s http://localhost:8000/metrics | grep kafka_processing_errors_total

# Error rate > 20% → SEV-1
# Error rate 10-20% → SEV-2
# Error rate 1-10% → SEV-3

# 4. Check DLQ growth
curl -s http://localhost:8000/metrics | grep 'kafka_messages_produced_total.*dlq'

# > 100 messages/min → SEV-1 or SEV-2
# 10-100 messages/min → SEV-3
```

### Step 3: Notify Stakeholders

Based on severity, notify:

**SEV-1 (Immediate)**:
- On-call engineer (already paged)
- Engineering manager
- Product owner
- Customer support team (for external communication)

**SEV-2 (Within 1 hour)**:
- On-call engineer (paged)
- Team lead
- Engineering manager (notified)

**SEV-3 (Within 4 hours)**:
- On-call engineer
- Team lead (next business day if after hours)

**Communication Template**:
```
INCIDENT: [SEV-X] Kafka Pipeline - [Brief Description]

Impact:
- User-facing: [Yes/No - describe impact]
- Data delay: [X minutes/hours]
- Affected systems: [List]

Status: Investigating

ETA for update: [15 minutes for SEV-1, 1 hour for SEV-2]

Incident Commander: [Name]
Incident Channel: #kafka-pipeline-incidents
Incident Ticket: [JIRA-123]
```

## Triage and Diagnosis (Within 30 minutes)

### Assign Incident Commander

For SEV-1 and SEV-2 incidents:

**Incident Commander Responsibilities**:
1. Lead incident response coordination
2. Communicate with stakeholders
3. Make decisions on remediation actions
4. Delegate tasks to responders
5. Track timeline and actions taken
6. Call post-mortem after resolution

**Typically**:
- SEV-1: Engineering Manager or Senior Engineer
- SEV-2: Team Lead or On-call Engineer
- SEV-3: On-call Engineer

### Gather Context

Collect information systematically:

```bash
# 1. Recent changes
# Check deployments in last 24 hours
kubectl rollout history deployment/download-worker -n kafka-pipeline

# Check recent configuration changes
git log --since="24 hours ago" --oneline -- config/

# Check recent database migrations
alembic history -r current:head

# 2. System snapshot
# Capture current state for analysis
kubectl get pods -n kafka-pipeline > incident-pods-$(date +%s).txt
kubectl get events -n kafka-pipeline --sort-by='.lastTimestamp' > incident-events-$(date +%s).txt
curl -s http://localhost:8000/metrics > incident-metrics-$(date +%s).txt

# 3. Error logs
# Collect logs from all components
kubectl logs -n kafka-pipeline -l app=event-ingester --tail=500 > incident-logs-ingester.txt
kubectl logs -n kafka-pipeline -l app=download-worker --tail=500 > incident-logs-download.txt
kubectl logs -n kafka-pipeline -l app=result-processor --tail=500 > incident-logs-processor.txt
```

### Common Incident Scenarios

## Scenario 1: Complete System Outage (SEV-1)

**Symptoms**:
- All workers in CrashLoopBackOff
- No message consumption
- Kafka connection errors

**Diagnostic Tree**:

```
Complete Outage?
├─ Kafka broker issue?
│  ├─ Yes → Contact Kafka infrastructure team
│  │        Check broker status, restart if needed
│  └─ No → Continue
├─ Authentication failure?
│  ├─ Yes → Refresh Azure AD tokens
│  │        Verify service principal credentials
│  └─ No → Continue
├─ Resource exhaustion?
│  ├─ Yes → Scale up resources or reduce load
│  └─ No → Continue
└─ Recent deployment?
   ├─ Yes → Rollback immediately
   └─ No → Escalate to engineering
```

**Immediate Actions**:

```bash
# 1. Check Kafka broker connectivity
kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER

# If Kafka is down:
# → Contact infrastructure team immediately
# → This is outside our control, track external incident

# 2. If Kafka is up, check authentication
az account show

# If authentication failed:
# → Re-authenticate
az login
# → Restart workers
kubectl rollout restart deployment/download-worker -n kafka-pipeline

# 3. If recent deployment, rollback
kubectl rollout undo deployment/download-worker -n kafka-pipeline

# 4. Monitor recovery
watch -n 5 "kubectl get pods -n kafka-pipeline"
```

## Scenario 2: High Error Rate (SEV-2)

**Symptoms**:
- Alert: `KafkaErrorRateHigh` or `KafkaDownloadFailureRateHigh`
- Error rate > 10%
- Messages flowing but many failing

**Diagnostic Questions**:

1. **What type of errors**?
   ```bash
   # Check error distribution by category
   curl -s http://localhost:8000/metrics | grep kafka_processing_errors_total

   # Check logs for error patterns
   kubectl logs -n kafka-pipeline -l app=download-worker --tail=100 | grep ERROR
   ```

2. **All events or specific domain?**
   ```bash
   # Check if errors are domain-specific
   kubectl logs -n kafka-pipeline -l app=download-worker | \
     grep ERROR | awk '{print $NF}' | sort | uniq -c

   # If specific domain: investigate domain-specific issue
   # If all domains: system-wide issue
   ```

3. **Recent spike or gradual increase?**
   - Sudden spike → Likely external issue (source system, Azure)
   - Gradual increase → Capacity or degradation issue

**Resolution Paths**:

**If external service degraded** (OneLake, source URLs):
```bash
# 1. Check Azure service health
curl -s https://status.azure.com/status/ | grep -i storage

# 2. Check source system health
# Contact source system team

# 3. If transient, increase retry tolerance
# Update circuit breaker thresholds temporarily

# 4. Monitor for recovery
# Errors should decrease as service recovers
```

**If capacity issue**:
```bash
# 1. Scale workers horizontally
kubectl scale deployment download-worker --replicas=12 -n kafka-pipeline

# 2. Monitor error rate decrease
watch -n 10 "curl -s http://localhost:8000/metrics | grep kafka_processing_errors_total"

# 3. Review capacity planning (see Scaling Operations runbook)
```

## Scenario 3: Data Integrity Issue (SEV-1)

**Symptoms**:
- Data corruption reported
- Duplicate or missing files in OneLake
- Delta Lake write failures

**Immediate Containment**:

```bash
# 1. STOP processing to prevent further corruption
kubectl scale deployment event-ingester --replicas=0 -n kafka-pipeline
kubectl scale deployment download-worker --replicas=0 -n kafka-pipeline
kubectl scale deployment result-processor --replicas=0 -n kafka-pipeline

# 2. Verify processing stopped
curl -s http://localhost:8000/metrics | grep kafka_messages_consumed_total
# Should show no recent increase

# 3. Document last known good state
# Record Kafka offsets for all consumer groups
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --all-groups --describe > offsets-snapshot.txt
```

**Investigation**:

```bash
# 1. Identify scope of corruption
# Check Delta Lake for inconsistencies
python scripts/validate_data_integrity.py --since "2024-01-01"

# 2. Identify root cause
# Review recent code changes
# Check for schema mismatches
# Verify upload logic correctness

# 3. Determine recovery strategy
# Can we replay from Kafka? (if offsets still available)
# Do we need to restore from backup?
# Can we fix data in place?
```

**Recovery**:

```bash
# Option A: Replay from Kafka (if offsets available)
# 1. Reset consumer group offsets to known good point
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker \
  --topic xact.downloads.pending \
  --reset-offsets --to-datetime 2024-01-01T00:00:00.000 \
  --execute

# 2. Fix code/config issue
# 3. Resume processing
kubectl scale deployment download-worker --replicas=6 -n kafka-pipeline

# Option B: Restore from backup
# 1. Restore OneLake files from backup
# 2. Restore Delta tables from checkpoint
# 3. Resume processing from current Kafka offset
```

## Scenario 4: Delta Lake Write Failures (SEV-2)

**Symptoms**:
- Alert: `KafkaDeltaWriteFailures`
- Analytics data not updating
- Delta write error logs

**Quick Checks**:

```bash
# 1. Verify OneLake workspace accessible
az rest --method GET --uri "<onelake-workspace-api>"

# 2. Check Delta table health
python -c "from deltalake import DeltaTable; print(DeltaTable('<table-path>').schema())"

# 3. Check for schema mismatches
kubectl logs -n kafka-pipeline -l app=result-processor | grep -i "schema\|mismatch"

# 4. Verify permissions
# Service principal should have write access to Delta tables
```

**Resolution**:

```bash
# If OneLake issue:
# → Wait for Azure service recovery or contact support

# If schema mismatch:
# → Update Delta table schema or fix writer code
# → Deploy fix
# → Writes should resume automatically

# If permissions issue:
# → Grant required permissions to service principal
# → Restart result processor
kubectl rollout restart deployment result-processor -n kafka-pipeline
```

**Important**: Delta writes are non-blocking. Kafka processing continues even if Delta writes fail. Files are still uploaded to OneLake.

## Scenario 5: Runaway DLQ Growth (SEV-2)

**Symptoms**:
- Alert: `KafkaDLQGrowthRapid`
- > 100 messages/min entering DLQ
- Systematic failure pattern

**Investigation**:

```bash
# 1. Check DLQ for common error pattern
python -m kafka_pipeline.dlq.cli list --limit 50

# 2. Identify systematic issue
# Are all errors the same type?
# Are all from the same domain/source?

# 3. Check if issue is ongoing
# Are new messages still entering DLQ?
curl -s http://localhost:8000/metrics | grep 'kafka_messages_produced_total.*dlq'
```

**Resolution**:

```bash
# If source system issue (e.g., invalid URLs from source):
# 1. Contact source system team
# 2. Pause event ingestion if necessary to prevent further DLQ growth
kubectl scale deployment event-ingester --replicas=0 -n kafka-pipeline

# If pipeline configuration issue (e.g., wrong domain allowlist):
# 1. Fix configuration
# 2. Deploy fix
# 3. Bulk replay DLQ messages
python -m kafka_pipeline.dlq.cli list | awk '{print $2}' | \
  xargs -I {} python -m kafka_pipeline.dlq.cli replay {}
```

## Mitigation Strategies

### Short-term Containment

| Issue | Mitigation | Command |
|-------|------------|---------|
| Resource exhaustion | Scale up workers | `kubectl scale deployment <name> --replicas=<N>` |
| Cascading failures | Open circuit breaker manually | Update config, restart |
| Data corruption | Stop processing | `kubectl scale deployment <name> --replicas=0` |
| Downstream impact | Pause event ingestion | Scale event-ingester to 0 |
| Kafka connectivity | Restart workers | `kubectl rollout restart deployment <name>` |

### Long-term Remediation

After immediate mitigation:

1. **Identify root cause**: Code bug, config error, capacity issue, external dependency
2. **Implement fix**: Code change, config update, scaling, external coordination
3. **Test fix**: Deploy to staging, validate behavior
4. **Deploy to production**: Follow deployment procedures (see [Deployment Procedures](./deployment-procedures.md))
5. **Monitor recovery**: Verify metrics return to normal

## Communication Guidelines

### Status Update Frequency

| Severity | Update Frequency | Channels |
|----------|------------------|----------|
| SEV-1 | Every 15 minutes | Incident channel, email, status page |
| SEV-2 | Every 1 hour | Incident channel, email |
| SEV-3 | Every 4 hours or at milestones | Incident channel |

### Status Update Template

```
INCIDENT UPDATE - [SEV-X] [Timestamp]

Current Status: [Investigating / Identified / Resolving / Monitoring / Resolved]

What we know:
- [Key findings from investigation]

Impact:
- [Current user impact]
- [Affected systems]

Actions taken:
- [What we've done so far]

Next steps:
- [What we're doing next]
- [ETA for next update]

Incident Commander: [Name]
```

### Resolution Communication

```
INCIDENT RESOLVED - [SEV-X] [Timestamp]

Summary:
[Brief description of what happened]

Impact:
- Duration: [X hours Y minutes]
- Users affected: [Number or percentage]
- Data impact: [None / Delayed / Lost - with specifics]

Root cause:
[What caused the incident]

Resolution:
[How it was fixed]

Next steps:
- Post-mortem scheduled: [Date/time]
- Preventive measures: [Brief list]

Thank you to all responders and affected users for patience.

Incident Commander: [Name]
```

## Post-Incident Activities

### Immediate (Within 24 hours)

1. **Update incident ticket** with complete timeline and actions
2. **Verify recovery** is stable (no regression)
3. **Schedule post-mortem** meeting (within 3-5 days)
4. **Document lessons learned** in incident database

### Post-Mortem Meeting

**Agenda**:
1. Timeline review (what happened, when)
2. Root cause analysis (why it happened)
3. Response evaluation (what went well, what didn't)
4. Action items (how to prevent recurrence)

**Attendees**:
- Incident Commander
- All responders
- Team lead
- Engineering manager
- Affected stakeholders

**Post-Mortem Template**:

```markdown
# Post-Mortem: [Incident Title]

**Date**: [Incident date]
**Severity**: SEV-X
**Duration**: X hours Y minutes
**Incident Commander**: [Name]

## Summary
[2-3 sentence summary of incident]

## Timeline
| Time (UTC) | Event |
|------------|-------|
| 14:00 | Initial alert fired |
| 14:05 | On-call engineer acknowledged |
| 14:15 | Identified as Kafka connectivity issue |
| 14:30 | Implemented mitigation (restarted workers) |
| 14:45 | Verified recovery |
| 15:00 | Incident resolved |

## Impact
- **Users affected**: X% of downloads delayed
- **Duration**: 1 hour
- **Data impact**: No data loss, 30-minute delay in processing
- **Downstream systems**: Analytics reporting delayed by 2 hours

## Root Cause
[Detailed technical explanation of what caused the incident]

## Resolution
[Detailed explanation of how it was fixed]

## What Went Well
- Fast acknowledgement (5 minutes)
- Correct diagnosis (15 minutes)
- Clear communication with stakeholders

## What Could Be Improved
- Earlier detection (alert threshold too high)
- Faster mitigation (manual steps were slow)
- Documentation was incomplete

## Action Items
| Action | Owner | Due Date | Priority |
|--------|-------|----------|----------|
| Lower alert threshold for Kafka connectivity | SRE Team | 2024-01-15 | High |
| Automate worker restart on connectivity failure | Engineering | 2024-01-22 | Medium |
| Update runbook with new diagnostic steps | On-call | 2024-01-10 | High |

## References
- Incident ticket: JIRA-123
- Grafana snapshots: [link]
- Communication thread: [link]
```

## Escalation Paths

### When to Escalate

- Root cause not identified within response time SLA
- Mitigation attempts unsuccessful
- Scope or severity increasing
- External dependencies required (vendor support, infrastructure team)
- Decision needed beyond on-call authority

### Escalation Contacts

| Level | Role | Contact | Availability |
|-------|------|---------|--------------|
| L1 | On-call Engineer | PagerDuty | 24/7 |
| L2 | Team Lead | Phone/Teams | Business hours + on-call |
| L3 | Engineering Manager | Phone/Teams | Business hours + escalation |
| L4 | Director of Engineering | Phone | Escalation only |

**External Escalations**:
- **Kafka Infrastructure**: [Contact info]
- **Azure Support**: [Support ticket portal]
- **Source System Teams**: [Contact info]

## Related Runbooks

- [Consumer Lag](./consumer-lag.md) - For lag-related incidents
- [DLQ Management](./dlq-management.md) - For DLQ flooding incidents
- [Circuit Breaker Open](./circuit-breaker-open.md) - For connectivity incidents
- [Deployment Procedures](./deployment-procedures.md) - For deployment-related incidents

---

**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team
**Reviewers**: SRE Team, Engineering Management
