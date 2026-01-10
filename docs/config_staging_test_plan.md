# Config Refactor Staging Test Plan

**Date:** 2026-01-10
**Purpose:** Validate multi-file config system in staging before production deployment
**Duration:** 2-3 days minimum

---

## Overview

This test plan ensures the multi-file config system works correctly in a production-like environment before deploying to production. All tests must pass before proceeding to production deployment.

---

## Test Environment Setup

### Staging Environment Requirements
- [ ] Staging Kafka cluster available (mirrors production)
- [ ] Staging OneLake/storage available
- [ ] Staging ClaimX API available (or mock)
- [ ] Application deployed in staging
- [ ] Monitoring/logging infrastructure active
- [ ] Test data available

### Configuration Files Deployed
- [ ] `config/shared.yaml` - with staging Kafka connection
- [ ] `config/xact_config.yaml` - with XACT topics and workers
- [ ] `config/claimx_config.yaml` - with ClaimX topics and workers
- [ ] `config/plugins/*.yaml` - any plugin configs
- [ ] Environment variables set correctly

---

## Test Suite

### Phase 1: Configuration Loading Tests (Day 1)

#### Test 1.1: Config Validation
**Objective:** Verify config files are valid and complete

**Steps:**
```bash
# Run validation
python -m kafka_pipeline.config --validate

# Expected output:
# ✓ Configuration validation passed
#   - Shared settings: OK
#   - XACT domain: OK
#   - ClaimX domain: OK
#   - Merge integrity: OK
```

**Pass Criteria:**
- [ ] Validation passes without errors
- [ ] No missing required keys
- [ ] All domains configured
- [ ] Kafka constraints satisfied

**On Failure:**
- Document error messages
- Fix config files
- Re-run validation
- Do not proceed until passing

---

#### Test 1.2: Merged Config Display
**Objective:** Verify merged config matches expectations

**Steps:**
```bash
# Show merged config
python -m kafka_pipeline.config --show-merged > merged_config_staging.yaml

# Compare with expected structure
diff merged_config_staging.yaml expected_merged_config.yaml
```

**Pass Criteria:**
- [ ] Merged config contains all expected sections
- [ ] Bootstrap servers point to staging Kafka
- [ ] Topic names match staging conventions
- [ ] Worker settings correct
- [ ] No unexpected keys or values

**On Failure:**
- Identify which file caused incorrect merge
- Check merge precedence rules
- Fix config files
- Re-run test

---

#### Test 1.3: Debug Logging
**Objective:** Verify debug logging shows correct config loading sequence

**Steps:**
```bash
# Run with verbose logging
python -m kafka_pipeline.config --validate --verbose 2>&1 | tee config_load_debug.log

# Check log file for expected entries
grep "Loading configuration from" config_load_debug.log
grep "Configuration merge complete" config_load_debug.log
grep "Configuration validation passed" config_load_debug.log
```

**Pass Criteria:**
- [ ] Log shows files being loaded
- [ ] Log shows merge operations
- [ ] Log shows final merged keys
- [ ] Log shows validation steps
- [ ] No errors or warnings

**On Failure:**
- Review log for error messages
- Check file paths
- Verify file permissions
- Fix issues and re-test

---

### Phase 2: Service Startup Tests (Day 1)

#### Test 2.1: XACT Workers Startup
**Objective:** Verify all XACT workers start successfully with new config

**Steps:**
```bash
# Start XACT workers
sudo systemctl start kafka-pipeline-xact-*

# Check status
sudo systemctl status kafka-pipeline-xact-event-ingester
sudo systemctl status kafka-pipeline-xact-download
sudo systemctl status kafka-pipeline-xact-upload
sudo systemctl status kafka-pipeline-xact-delta-writer

# Check logs
tail -100 /var/log/kafka-pipeline/xact-*.log | grep -E "config|Config|CONFIG"
```

**Pass Criteria:**
- [ ] All workers start without errors
- [ ] Workers connect to Kafka successfully
- [ ] Workers load config correctly
- [ ] No config-related errors in logs
- [ ] Health checks pass

**On Failure:**
- Check service logs for detailed errors
- Verify Kafka connectivity
- Verify topic existence
- Fix config and restart

---

#### Test 2.2: ClaimX Workers Startup
**Objective:** Verify all ClaimX workers start successfully with new config

**Steps:**
```bash
# Start ClaimX workers
sudo systemctl start kafka-pipeline-claimx-*

# Check status
sudo systemctl status kafka-pipeline-claimx-ingester
sudo systemctl status kafka-pipeline-claimx-enricher
sudo systemctl status kafka-pipeline-claimx-download
sudo systemctl status kafka-pipeline-claimx-upload
sudo systemctl status kafka-pipeline-claimx-delta-writer

# Check logs
tail -100 /var/log/kafka-pipeline/claimx-*.log | grep -E "config|Config|CONFIG"
```

**Pass Criteria:**
- [ ] All workers start without errors
- [ ] Workers connect to Kafka successfully
- [ ] ClaimX API connection successful
- [ ] Workers load config correctly
- [ ] No config-related errors in logs
- [ ] Health checks pass

**On Failure:**
- Check service logs
- Verify ClaimX API token
- Verify topics exist
- Fix and restart

---

### Phase 3: Functional Tests (Day 1-2)

#### Test 3.1: XACT Event Ingestion
**Objective:** Verify XACT event ingestion works with new config

**Test Data:**
```json
{
  "event_id": "test-001",
  "timestamp": "2026-01-10T12:00:00Z",
  "event_type": "download_request",
  "payload": {"file_id": "test-file-001"}
}
```

**Steps:**
```bash
# Publish test event to XACT events topic
kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER \
  --topic xact.events.raw \
  --property "parse.key=true" \
  --property "key.separator=:"

# Input: test-001:{...event JSON...}

# Monitor processing
tail -f /var/log/kafka-pipeline/xact-event-ingester.log

# Verify event reaches downloads_pending topic
kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER \
  --topic xact.downloads.pending \
  --from-beginning \
  --max-messages 1
```

**Pass Criteria:**
- [ ] Event ingester receives event
- [ ] Event processed correctly
- [ ] Event published to downloads_pending topic
- [ ] No errors in logs
- [ ] Processing time within expected range

**On Failure:**
- Check consumer group assignment
- Verify topic names
- Check serialization settings
- Review error logs

---

#### Test 3.2: ClaimX Enrichment Flow
**Objective:** Verify ClaimX enrichment pipeline works with new config

**Steps:**
```bash
# Publish test event
kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER \
  --topic claimx.events.raw

# Monitor enrichment worker
tail -f /var/log/kafka-pipeline/claimx-enricher.log

# Check enrichment results topic
kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER \
  --topic claimx.enrichment.results \
  --from-beginning \
  --max-messages 1
```

**Pass Criteria:**
- [ ] Event received by enrichment worker
- [ ] ClaimX API called successfully
- [ ] Enriched data published to results topic
- [ ] No errors in logs
- [ ] API timeout settings working

**On Failure:**
- Verify ClaimX API token
- Check API URL configuration
- Review timeout settings
- Check error handling

---

#### Test 3.3: Download Worker with Retry
**Objective:** Verify retry delays configured correctly

**Steps:**
```bash
# Publish event with invalid URL (to trigger retry)
kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER \
  --topic xact.downloads.pending
# Input: {invalid download event}

# Monitor retry topic
kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER \
  --topic xact.downloads.pending.retry.5m

# Check retry delay is correct (should be 300 seconds from config)
# Compare event timestamps
```

**Pass Criteria:**
- [ ] Failed download triggers retry
- [ ] Event published to correct retry topic
- [ ] Retry delay matches config (300s, 600s, etc.)
- [ ] Max retries respected
- [ ] DLQ reached after max retries

**On Failure:**
- Check retry_delays configuration
- Verify retry topic names
- Review retry logic
- Check DLQ configuration

---

### Phase 4: Performance Tests (Day 2)

#### Test 4.1: Throughput Test
**Objective:** Verify consumer/producer settings provide expected throughput

**Steps:**
```bash
# Generate load using kafka-producer-perf-test
kafka-producer-perf-test.sh \
  --topic xact.events.raw \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=$KAFKA_BROKER

# Monitor consumer lag
kafka-consumer-groups.sh --bootstrap-server $KAFKA_BROKER \
  --group xact-event-ingester \
  --describe

# Check processing metrics
curl http://localhost:8000/metrics | grep kafka_
```

**Pass Criteria:**
- [ ] Consumer processes at expected rate (based on max_poll_records)
- [ ] Consumer lag remains under 1000 messages
- [ ] No backpressure or blocking
- [ ] Batch sizes match configuration
- [ ] Processing latency acceptable (< 1s p99)

**On Failure:**
- Review max_poll_records settings
- Check session_timeout_ms vs max_poll_interval_ms
- Adjust batch sizes if needed
- Review processing concurrency

---

#### Test 4.2: Concurrency Test
**Objective:** Verify processing concurrency settings work correctly

**Steps:**
```bash
# Check worker concurrency from logs
grep "concurrency" /var/log/kafka-pipeline/*.log

# Monitor thread/process count
ps aux | grep kafka_pipeline | wc -l

# Send burst of messages
# (Use kafka-producer-perf-test from Test 4.1)

# Check processing distribution
# (All concurrent workers should be busy)
```

**Pass Criteria:**
- [ ] Worker uses configured concurrency (e.g., 10 concurrent tasks)
- [ ] All workers processing in parallel
- [ ] No worker starvation
- [ ] CPU utilization matches expectations

**On Failure:**
- Review processing.concurrency settings
- Check for bottlenecks
- Monitor resource usage
- Adjust concurrency if needed

---

### Phase 5: Failure Scenarios (Day 2-3)

#### Test 5.1: Kafka Connection Failure
**Objective:** Verify graceful handling of Kafka connection issues

**Steps:**
```bash
# Block Kafka connection
sudo iptables -A OUTPUT -p tcp --dport 9092 -j DROP

# Monitor worker behavior
tail -f /var/log/kafka-pipeline/*.log

# Wait 5 minutes

# Restore connection
sudo iptables -D OUTPUT -p tcp --dport 9092 -j DROP

# Verify recovery
```

**Pass Criteria:**
- [ ] Workers detect connection failure
- [ ] Workers retry connection (based on retry config)
- [ ] Workers recover when connection restored
- [ ] No data loss
- [ ] Processing resumes normally

**On Failure:**
- Review connection timeout settings
- Check retry/backoff configuration
- Verify error handling

---

#### Test 5.2: Invalid Config Scenario
**Objective:** Verify validation catches config errors

**Steps:**
```bash
# Introduce error in config (e.g., missing required key)
# Remove xact.topics.events from xact_config.yaml

# Run validation
python -m kafka_pipeline.config --validate

# Expected: Validation should FAIL with clear error
```

**Pass Criteria:**
- [ ] Validation fails with clear error message
- [ ] Error identifies missing key
- [ ] Exit code non-zero
- [ ] Services refuse to start with invalid config

**On Failure:**
- Enhance validation rules
- Add missing checks
- Improve error messages

---

#### Test 5.3: Environment Variable Override
**Objective:** Verify environment variables correctly override config

**Steps:**
```bash
# Set environment variable
export BOOTSTRAP_SERVERS="override-kafka:9092"

# Start worker
# (Should use override-kafka:9092, not config value)

# Check logs for connection attempt
tail -100 /var/log/kafka-pipeline/*.log | grep "override-kafka"

# Unset variable
unset BOOTSTRAP_SERVERS
```

**Pass Criteria:**
- [ ] Environment variable overrides config file value
- [ ] Worker connects to override bootstrap server
- [ ] Override logged in debug output

**On Failure:**
- Review environment variable precedence
- Check config loading order
- Verify override logic

---

### Phase 6: Backwards Compatibility (Day 3)

#### Test 6.1: Legacy Config Still Works
**Objective:** Verify old single-file config.yaml still works

**Steps:**
```bash
# Disable multi-file config
mv config config.disabled

# Verify old config exists
ls -la src/config.yaml

# Start workers
sudo systemctl restart kafka-pipeline-*

# Verify they start successfully
sudo systemctl status kafka-pipeline-*
```

**Pass Criteria:**
- [ ] Workers start with old config.yaml
- [ ] No errors in logs
- [ ] Processing works normally
- [ ] Backwards compatibility maintained

**On Failure:**
- Fix backwards compatibility issues
- Ensure both config methods supported
- Document any breaking changes

---

#### Test 6.2: Migration Verification
**Objective:** Verify migrated config produces same behavior as old config

**Steps:**
```bash
# Load old config
python -m kafka_pipeline.config --config src/config.yaml --show-merged > old_config.yaml

# Enable new config
mv config.disabled config

# Load new config
python -m kafka_pipeline.config --show-merged > new_config.yaml

# Compare
diff old_config.yaml new_config.yaml
```

**Pass Criteria:**
- [ ] Merged configs functionally equivalent
- [ ] Only differences are intentional (e.g., environment-specific)
- [ ] No unexpected changes

**On Failure:**
- Review merge logic
- Check for config drift
- Fix migration issues

---

## Test Results Summary

| Test ID | Test Name | Status | Date Tested | Notes |
|---------|-----------|--------|-------------|-------|
| 1.1 | Config Validation | ☐ PASS ☐ FAIL | | |
| 1.2 | Merged Config Display | ☐ PASS ☐ FAIL | | |
| 1.3 | Debug Logging | ☐ PASS ☐ FAIL | | |
| 2.1 | XACT Workers Startup | ☐ PASS ☐ FAIL | | |
| 2.2 | ClaimX Workers Startup | ☐ PASS ☐ FAIL | | |
| 3.1 | XACT Event Ingestion | ☐ PASS ☐ FAIL | | |
| 3.2 | ClaimX Enrichment Flow | ☐ PASS ☐ FAIL | | |
| 3.3 | Download Worker Retry | ☐ PASS ☐ FAIL | | |
| 4.1 | Throughput Test | ☐ PASS ☐ FAIL | | |
| 4.2 | Concurrency Test | ☐ PASS ☐ FAIL | | |
| 5.1 | Kafka Connection Failure | ☐ PASS ☐ FAIL | | |
| 5.2 | Invalid Config Scenario | ☐ PASS ☐ FAIL | | |
| 5.3 | Environment Variable Override | ☐ PASS ☐ FAIL | | |
| 6.1 | Legacy Config Still Works | ☐ PASS ☐ FAIL | | |
| 6.2 | Migration Verification | ☐ PASS ☐ FAIL | | |

---

## Sign-Off

### Testing Sign-Off
- [ ] All tests passed
- [ ] No critical issues found
- [ ] Performance meets expectations
- [ ] Backwards compatibility verified
- [ ] Ready for production deployment

**Tested By:** _____________________ **Date:** _____________________

**Reviewed By:** _____________________ **Date:** _____________________

---

## Issues Found During Testing

| Issue # | Severity | Description | Resolution | Status |
|---------|----------|-------------|------------|--------|
| | | | | |
| | | | | |

---

## Staging Environment Details

**Kafka Cluster:** _____________________
**Bootstrap Servers:** _____________________
**OneLake Path:** _____________________
**ClaimX API URL:** _____________________
**Monitoring Dashboard:** _____________________

---

## Next Steps

After all tests pass:
1. [ ] Document test results
2. [ ] Get sign-off from stakeholders
3. [ ] Schedule production deployment
4. [ ] Review rollback plan
5. [ ] Prepare deployment team
6. [ ] Execute production deployment (use deployment checklist)

---

## References

- Deployment Checklist: `docs/config_deployment_checklist.md`
- Config Loading Docs: `docs/config_loading.md`
- Validator Spec: `src/config/VALIDATOR_SPEC.md`
- Validation Tool: `python -m kafka_pipeline.config --validate`
