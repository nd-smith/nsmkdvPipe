# Kafka Pipeline Operational Runbooks

This directory contains operational runbooks for the Kafka-based data pipeline. These runbooks provide step-by-step procedures for common scenarios, troubleshooting, and incident response.

## Quick Reference

| Runbook | When to Use | Severity |
|---------|-------------|----------|
| [Consumer Lag](./consumer-lag.md) | High consumer lag alerts, slow processing | Critical/Warning |
| [DLQ Management](./dlq-management.md) | Failed messages accumulating in DLQ | Warning/Info |
| [Circuit Breaker Open](./circuit-breaker-open.md) | Circuit breaker alerts, connection failures | Critical |
| [Deployment Procedures](./deployment-procedures.md) | Rolling out new versions, configuration changes | - |
| [Incident Response](./incident-response.md) | Production incidents, system degradation | Critical |
| [Scaling Operations](./scaling-operations.md) | Performance degradation, capacity planning | Warning |

## Alert → Runbook Mapping

This table maps Prometheus alerts to their corresponding runbooks:

| Alert Name | Severity | Runbook |
|------------|----------|---------|
| `KafkaConsumerLagHigh` | Critical | [Consumer Lag](./consumer-lag.md) |
| `KafkaConsumerLagWarning` | Warning | [Consumer Lag](./consumer-lag.md) |
| `KafkaConsumerNoPartitions` | Warning | [Consumer Lag](./consumer-lag.md) |
| `KafkaDLQGrowthRapid` | Critical | [DLQ Management](./dlq-management.md) |
| `KafkaDLQGrowthWarning` | Warning | [DLQ Management](./dlq-management.md) |
| `KafkaDLQSizeHigh` | Warning | [DLQ Management](./dlq-management.md) |
| `KafkaCircuitBreakerOpen` | Critical | [Circuit Breaker Open](./circuit-breaker-open.md) |
| `KafkaCircuitBreakerHalfOpen` | Info | [Circuit Breaker Open](./circuit-breaker-open.md) |
| `KafkaConsumerDisconnected` | Critical | [Circuit Breaker Open](./circuit-breaker-open.md) |
| `KafkaProducerDisconnected` | Critical | [Circuit Breaker Open](./circuit-breaker-open.md) |
| `KafkaErrorRateHigh` | Critical | [Incident Response](./incident-response.md) |
| `KafkaDownloadFailureRateHigh` | Critical | [Incident Response](./incident-response.md) |
| `KafkaDeltaWriteFailures` | Critical | [Incident Response](./incident-response.md) |
| `KafkaWorkerDead` | Critical | [Incident Response](./incident-response.md) |
| `KafkaProcessingLatencyHigh` | Critical | [Scaling Operations](./scaling-operations.md) |
| `KafkaProcessingLatencyWarning` | Warning | [Scaling Operations](./scaling-operations.md) |
| `KafkaThroughputLow` | Warning | [Scaling Operations](./scaling-operations.md) |

## Dashboard Links

- [Pipeline Overview](../observability/grafana/dashboards/kafka-pipeline-overview.json) - High-level system health
- [Consumer Health](../observability/grafana/dashboards/consumer-health.json) - Consumer lag and partition assignments
- [Download Performance](../observability/grafana/dashboards/download-performance.json) - Download metrics and OneLake performance
- [DLQ Monitoring](../observability/grafana/dashboards/dlq-monitoring.json) - Dead-letter queue analysis

## Escalation Paths

### Business Hours (9am-5pm EST)
1. **L1 Support** - Initial triage and basic troubleshooting
2. **L2 Support** - Platform team, follows runbooks
3. **L3 Support** - Engineering team, code-level debugging

### After Hours
1. **On-call Engineer** - Paged for critical alerts
2. **Team Lead** - Escalation for architectural decisions
3. **Management** - Business impact communication

## Common CLI Commands

```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --group xact-event-ingester --describe

# List DLQ messages
python -m kafka_pipeline.dlq.cli list --limit 50

# View specific DLQ message
python -m kafka_pipeline.dlq.cli view <trace_id>

# Replay DLQ message
python -m kafka_pipeline.dlq.cli replay <trace_id>

# Check worker health
kubectl get pods -n kafka-pipeline
kubectl logs -n kafka-pipeline <pod-name> --tail=100

# View metrics
curl -s http://localhost:8000/metrics | grep kafka_
```

## Emergency Contacts

| Role | Contact | Availability |
|------|---------|--------------|
| On-call Engineer | PagerDuty | 24/7 |
| Platform Team Lead | Teams/Email | Business hours |
| Infrastructure Team | ServiceNow | 24/7 |
| Kafka Support | Vendor portal | 24/7 |

## Documentation

- [Implementation Plan](../kafka-greenfield-implementation-plan.md) - System architecture and design
- [Testing Guide](../TESTING.md) - Testing procedures and environments
- [Configuration Reference](../../src/kafka_pipeline/config.py) - Environment variables and settings

## Runbook Maintenance

**Review Schedule**: Quarterly
**Last Updated**: 2024-12-27
**Owner**: Platform Engineering Team

When updating runbooks:
1. Test all commands in staging environment
2. Update last modified date at bottom of runbook
3. Request peer review from team member
4. Commit changes with descriptive message
