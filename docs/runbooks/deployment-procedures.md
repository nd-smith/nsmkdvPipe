# Deployment Procedures Runbook

## Overview

This runbook covers deployment procedures for the Kafka pipeline, including rolling updates, configuration changes, rollbacks, and validation procedures.

## Deployment Types

| Type | Scope | Risk | Downtime | Approval Required |
|------|-------|------|----------|-------------------|
| **Configuration** | Environment variables, settings | Low | None | Team Lead |
| **Patch** | Bug fixes, minor updates | Low | None | Team Lead |
| **Minor** | New features, enhancements | Medium | None | Product Owner |
| **Major** | Breaking changes, architecture updates | High | Minimal | Engineering Manager |
| **Emergency Hotfix** | Critical production fixes | Medium | None | On-call Engineer |

## Pre-Deployment Checklist

### Required Before ANY Deployment

- [ ] **Code Review**: PR approved by at least one team member
- [ ] **Tests Passing**: All unit, integration, and E2E tests green
- [ ] **Documentation Updated**: README, runbooks, changelog updated
- [ ] **Configuration Reviewed**: Environment variables verified
- [ ] **Database Migrations**: Schema changes validated (if applicable)
- [ ] **Monitoring Ready**: Alerts and dashboards configured for new features

### Additional for Minor/Major Deployments

- [ ] **Staging Deployment**: Successfully deployed and tested in staging
- [ ] **Load Testing**: Performance validated under expected load
- [ ] **Rollback Plan**: Documented rollback procedure tested
- [ ] **Stakeholder Notification**: Teams informed of deployment window
- [ ] **Runbook Updates**: New operational procedures documented

### Additional for Major Deployments

- [ ] **Architecture Review**: Design reviewed by principal engineers
- [ ] **Capacity Planning**: Resource requirements validated
- [ ] **Feature Flags**: New features behind flags for gradual rollout
- [ ] **Canary Strategy**: Plan for gradual traffic shift
- [ ] **Communication Plan**: User-facing changes communicated

## Deployment Environments

| Environment | Purpose | Update Frequency | Approval |
|-------------|---------|------------------|----------|
| **Development** | Feature development, testing | Continuous | Self-service |
| **Staging** | Pre-production validation | Daily | Automated |
| **Production** | Live user traffic | Weekly (scheduled) | Manager |

## Standard Deployment Procedure (Rolling Update)

### Step 1: Pre-Deployment Validation (15 minutes)

```bash
# 1. Verify staging deployment successful
kubectl get pods -n kafka-pipeline-staging
kubectl logs -n kafka-pipeline-staging <pod-name> --tail=50

# 2. Check production system health
kubectl get pods -n kafka-pipeline
curl -s http://localhost:8000/health | jq .

# 3. Review recent metrics for baseline
# - Consumer lag < 5,000 messages
# - Error rate < 1%
# - Processing latency p95 < 5s

# 4. Verify no active incidents
# Check incident management system for open tickets

# 5. Confirm deployment window
# Preferred: Business hours for monitoring
# Avoid: Friday afternoon, day before holiday
```

### Step 2: Create Deployment Ticket

Document deployment in tracking system:

```markdown
## Deployment Ticket

**Version**: v1.2.3
**Type**: Minor Release
**Scope**: Download worker optimization, retry logic update

**Changes**:
- [WP-123] Optimize OneLake upload performance
- [WP-124] Update retry backoff strategy
- [WP-125] Add file size validation

**Risk Assessment**: Low
- Only affects download worker component
- No schema changes
- Backward compatible with existing messages

**Rollback Plan**: Revert to v1.2.2 using git tag

**Monitoring Focus**:
- Download success rate
- OneLake upload latency
- Retry rate and DLQ growth

**Deployed By**: [Engineer Name]
**Approved By**: [Team Lead Name]
```

### Step 3: Execute Rolling Deployment (10 minutes)

```bash
# 1. Set new image version
kubectl set image deployment/download-worker \
  download-worker=<registry>/download-worker:v1.2.3 \
  -n kafka-pipeline

# Alternative: Apply updated manifest
kubectl apply -f deployments/download-worker.yaml -n kafka-pipeline

# 2. Monitor rollout progress
kubectl rollout status deployment/download-worker -n kafka-pipeline

# Expected output:
# Waiting for deployment "download-worker" rollout to finish: 2 out of 6 new replicas have been updated...
# Waiting for deployment "download-worker" rollout to finish: 4 out of 6 new replicas have been updated...
# deployment "download-worker" successfully rolled out

# 3. Watch pod status during rollout
watch -n 2 "kubectl get pods -n kafka-pipeline -l app=download-worker"

# Expected progression:
# Old pods: Running → Terminating → Terminated
# New pods: ContainerCreating → Running
```

**Rollout Timing**:
- Default: One pod at a time (safe but slow)
- Configured: 25% max surge, 25% max unavailable (faster)
- 6 pod deployment: ~2-3 minutes total

### Step 4: Post-Deployment Validation (15 minutes)

```bash
# 1. Verify all pods running new version
kubectl get pods -n kafka-pipeline -l app=download-worker \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}'

# Expected: All pods show v1.2.3

# 2. Check pod health
kubectl get pods -n kafka-pipeline -l app=download-worker

# Expected: All pods STATUS=Running, READY=1/1

# 3. Review startup logs for errors
kubectl logs -n kafka-pipeline -l app=download-worker --tail=50 | grep -i error

# Expected: No critical errors

# 4. Verify metrics endpoint responding
curl -s http://localhost:8000/metrics | grep kafka_messages_consumed_total

# 5. Check consumer lag not increasing
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-download-worker --describe

# Expected: LAG stable or decreasing
```

### Step 5: Monitor for 1 Hour

Watch these dashboards/metrics:

**Critical Metrics** (monitor for 15 minutes):
- [ ] Consumer lag: Should remain < 5,000 messages
- [ ] Error rate: Should stay < 1%
- [ ] Processing latency p95: Should be < 5 seconds
- [ ] Pod restart count: Should be 0 (no crash loops)

**Important Metrics** (monitor for 1 hour):
- [ ] Download success rate: Should match baseline (>95%)
- [ ] OneLake upload latency: Compare to pre-deployment baseline
- [ ] DLQ message rate: Should not spike
- [ ] Circuit breaker state: Should remain CLOSED

**Grafana Dashboards**:
1. Open Pipeline Overview dashboard
2. Set time range to "Last 2 hours" to see pre/post deployment
3. Compare current metrics to baseline
4. Watch for anomalies or degradation

### Step 6: Update Deployment Log

```bash
# Record successful deployment
echo "$(date): Deployed v1.2.3 download-worker successfully" >> deployment-log.txt

# Tag Git commit
git tag -a v1.2.3-prod -m "Production deployment v1.2.3"
git push origin v1.2.3-prod
```

## Configuration Change Procedure

For environment variable or config map changes only (no code changes):

```bash
# 1. Update config map
kubectl edit configmap kafka-pipeline-config -n kafka-pipeline

# Or apply updated config file
kubectl apply -f config/kafka-pipeline-config.yaml -n kafka-pipeline

# 2. Restart deployments to pick up new config
kubectl rollout restart deployment/event-ingester -n kafka-pipeline
kubectl rollout restart deployment/download-worker -n kafka-pipeline
kubectl rollout restart deployment/result-processor -n kafka-pipeline

# 3. Verify new config loaded
kubectl exec -n kafka-pipeline <pod-name> -- env | grep KAFKA_

# 4. Monitor for config-related errors
kubectl logs -n kafka-pipeline <pod-name> --tail=100 | grep -i "config\|error"
```

**Common Configuration Changes**:
- Kafka broker endpoints
- Consumer group IDs
- Retry delays and thresholds
- Download concurrency limits
- Circuit breaker settings

## Rollback Procedure

### When to Rollback

- New version has critical bugs affecting users
- Error rate exceeds 5% for > 5 minutes
- Consumer lag growing > 10,000 messages
- Circuit breakers opening unexpectedly
- Data corruption or loss detected

### Standard Rollback (5 minutes)

```bash
# 1. Rollback to previous version
kubectl rollout undo deployment/download-worker -n kafka-pipeline

# Alternative: Rollback to specific revision
kubectl rollout history deployment/download-worker -n kafka-pipeline
kubectl rollout undo deployment/download-worker --to-revision=<N> -n kafka-pipeline

# 2. Monitor rollback progress
kubectl rollout status deployment/download-worker -n kafka-pipeline

# 3. Verify old version running
kubectl get pods -n kafka-pipeline -l app=download-worker \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}'

# 4. Check system health
kubectl get pods -n kafka-pipeline
curl -s http://localhost:8000/health | jq .

# 5. Monitor recovery metrics
watch -n 5 "curl -s http://localhost:8000/metrics | grep kafka_processing_errors_total"
```

### Emergency Rollback (Database Issues)

If deployment included database migrations:

```bash
# 1. Rollback application code first
kubectl rollout undo deployment/<deployment-name> -n kafka-pipeline

# 2. Assess database state
# Check if new schema is backward compatible
# Verify data integrity

# 3. If needed, rollback database migration
# (Depends on migration tool used, e.g., Alembic, Flyway)
alembic downgrade -1

# 4. Verify application connects to database
kubectl logs -n kafka-pipeline <pod-name> | grep -i "database\|connection"

# 5. Validate data integrity
# Run data validation queries
# Check for missing or corrupted records
```

## Canary Deployment (Gradual Rollout)

For high-risk deployments, use canary strategy:

### Step 1: Deploy Canary (10% traffic)

```bash
# 1. Create canary deployment
kubectl apply -f deployments/download-worker-canary.yaml -n kafka-pipeline

# Canary spec includes:
# - replicas: 1 (out of 10 total = 10%)
# - image: new version
# - labels: app=download-worker, version=canary

# 2. Verify canary pod running
kubectl get pods -n kafka-pipeline -l version=canary

# 3. Monitor canary metrics separately
curl -s http://localhost:8000/metrics | grep 'version="canary"'
```

### Step 2: Validate Canary (30 minutes)

Compare canary metrics to stable version:

| Metric | Canary | Stable | Delta | Acceptable? |
|--------|--------|--------|-------|-------------|
| Error Rate | 0.5% | 0.4% | +0.1% | ✓ (< 1% delta) |
| Latency p95 | 4.2s | 4.0s | +0.2s | ✓ (< 1s delta) |
| Throughput | 120 msg/s | 115 msg/s | +5 msg/s | ✓ (similar) |
| DLQ Rate | 0 msg/min | 0 msg/min | 0 | ✓ |

**Proceed if**:
- Error rate delta < 1%
- Latency delta < 1 second or 10%
- No unexpected errors or circuit breaker opens
- DLQ rate not elevated

### Step 3: Increase Canary to 50%

```bash
# Scale canary and stable deployments
kubectl scale deployment download-worker-canary --replicas=5 -n kafka-pipeline
kubectl scale deployment download-worker --replicas=5 -n kafka-pipeline

# Monitor for 15 minutes
```

### Step 4: Complete Rollout to 100%

```bash
# Replace stable deployment with canary version
kubectl set image deployment/download-worker \
  download-worker=<registry>/download-worker:v1.2.3 \
  -n kafka-pipeline

# Remove canary deployment
kubectl delete deployment download-worker-canary -n kafka-pipeline

# Verify all pods running new version
kubectl get pods -n kafka-pipeline -l app=download-worker
```

## Database Migration Deployment

For deployments with schema changes:

### Before Deployment

```bash
# 1. Backup database
pg_dump -h <host> -U <user> -d <database> > backup_$(date +%Y%m%d_%H%M%S).sql

# 2. Test migration in staging
# Apply migration to staging database
# Verify application works with new schema
# Test rollback migration

# 3. Check migration is backward compatible
# Old code should work with new schema during rollout
```

### During Deployment

```bash
# 1. Deploy code that works with BOTH old and new schema
# (Backward compatibility required for rolling deployment)
kubectl apply -f deployments/<deployment>.yaml -n kafka-pipeline

# 2. Wait for all pods to be running new code
kubectl rollout status deployment/<deployment> -n kafka-pipeline

# 3. Apply database migration
# (Now safe since all code is compatible)
alembic upgrade head

# 4. Verify migration succeeded
alembic current
psql -h <host> -U <user> -d <database> -c "\d <table>"

# 5. Deploy code optimized for new schema (if needed)
kubectl apply -f deployments/<deployment>-final.yaml -n kafka-pipeline
```

## Post-Deployment Procedures

### Immediate (Within 1 hour)

- [ ] Update deployment tracker with completion status
- [ ] Notify stakeholders of successful deployment
- [ ] Document any issues encountered and resolutions
- [ ] Update version numbers in documentation

### Within 24 Hours

- [ ] Review monitoring dashboards for trends
- [ ] Check for any delayed errors or issues
- [ ] Validate data quality in analytics tables
- [ ] Collect user feedback on new features

### Within 1 Week

- [ ] Conduct post-deployment retrospective
- [ ] Document lessons learned
- [ ] Update runbooks based on deployment experience
- [ ] Plan improvements for next deployment

## Emergency Hotfix Procedure

For critical production bugs requiring immediate fix:

### 1. Create Hotfix Branch

```bash
# Branch from production tag
git checkout -b hotfix/critical-bug v1.2.3-prod

# Make minimal fix
git commit -m "Hotfix: Fix critical bug in download retry logic"

# Tag hotfix version
git tag -a v1.2.4-hotfix -m "Emergency hotfix for retry bug"
```

### 2. Fast-Track Testing

```bash
# Run critical tests only
pytest tests/kafka_pipeline/workers/test_download_worker.py

# Deploy to staging for quick validation
kubectl set image deployment/download-worker \
  download-worker=<registry>/download-worker:v1.2.4-hotfix \
  -n kafka-pipeline-staging

# Validate fix in staging (15 minute soak)
```

### 3. Production Deployment

```bash
# Deploy hotfix to production
kubectl set image deployment/download-worker \
  download-worker=<registry>/download-worker:v1.2.4-hotfix \
  -n kafka-pipeline

# Monitor closely for 30 minutes
```

### 4. Post-Hotfix

- Notify team of emergency deployment
- Create post-mortem for root cause and prevention
- Backport fix to main branch
- Schedule proper release with full testing

## Troubleshooting Common Deployment Issues

### Pods Not Starting (ImagePullBackOff)

```bash
# Check image exists in registry
docker pull <registry>/download-worker:v1.2.3

# Verify image pull secret configured
kubectl get secrets -n kafka-pipeline | grep regcred

# Check pod events for details
kubectl describe pod <pod-name> -n kafka-pipeline
```

### Pods Crashing (CrashLoopBackOff)

```bash
# Check pod logs for startup errors
kubectl logs -n kafka-pipeline <pod-name>

# Common issues:
# - Missing environment variables
# - Database connection failures
# - Configuration errors
# - Out of memory

# Check resource limits
kubectl describe pod <pod-name> -n kafka-pipeline | grep -A 5 "Limits"
```

### Rollout Stuck (Progressing Deadline Exceeded)

```bash
# Check rollout status
kubectl rollout status deployment/<deployment> -n kafka-pipeline

# View events
kubectl get events -n kafka-pipeline --sort-by='.lastTimestamp'

# Common causes:
# - Readiness probe failing
# - Resource quota exceeded
# - Insufficient cluster capacity

# Rollback if stuck
kubectl rollout undo deployment/<deployment> -n kafka-pipeline
```

## Related Runbooks

- [Incident Response](./incident-response.md) - If deployment causes incident
- [Scaling Operations](./scaling-operations.md) - Scaling during deployment
- [Rollback Procedures](#rollback-procedure) - Detailed rollback steps

---

**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team
**Reviewers**: SRE Team, Release Engineering
