# iTel Cabinet Task Tracking Plugin - Specification

**Version:** 1.0.0
**Created:** 2026-01-10
**Status:** Draft

---

## Executive Summary

This plugin tracks the complete lifecycle of ClaimX tasks related to iTel cabinet operations (task template ID: 32513). It monitors all status changes from task creation through completion, enriches the data with full task details from the ClaimX REST API, and writes timestamped tracking records to a dedicated Delta table for analysis and auditing.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Component Specifications](#component-specifications)
4. [Data Flow](#data-flow)
5. [Configuration](#configuration)
6. [Delta Table Schema](#delta-table-schema)
7. [Implementation Plan](#implementation-plan)
8. [Testing Strategy](#testing-strategy)
9. [Monitoring & Metrics](#monitoring--metrics)
10. [Future Enhancements](#future-enhancements)

---

## Overview

### Purpose

Track the complete lifecycle of iTel cabinet-related tasks (task_id: 32513) in ClaimX, capturing every status change and enriching with full task details for operational visibility and historical analysis.

### Business Requirements

- **BR-1**: Automatically detect when task 32513 is created, modified, or completed
- **BR-2**: Capture all status transitions for complete audit trail
- **BR-3**: Enrich events with comprehensive task details from ClaimX API
- **BR-4**: Store historical tracking data in queryable Delta table
- **BR-5**: Enable near-real-time monitoring of iTel cabinet task progress

### Scope

**In Scope:**
- Task creation detection
- All status change tracking (assigned → in-progress → completed, etc.)
- ClaimX API enrichment for full task details
- Delta table storage for historical tracking
- Error handling and retry logic
- Metrics and monitoring

**Out of Scope:**
- Direct integration with iTel Cabinet API (future enhancement)
- Photo/attachment storage or processing
- Real-time alerting (use downstream consumers)
- Data archival/purging (handled by Delta table retention)

---

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ClaimX Event Stream                          │
│                  (CUSTOM_TASK_* events)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              ClaimX Enrichment Worker                            │
│              (plugins enabled)                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │  iTel Cabinet Plugin   │
            │  (TaskTriggerPlugin)   │
            │                        │
            │  Filter: task_id=32513 │
            │  Events: ALL statuses  │
            └────────────┬───────────┘
                         │
                         ▼
            Publish to Kafka Topic:
            itel.cabinet.task.tracking
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│            iTel Cabinet Tracking Worker                         │
│            (PluginActionWorker)                                 │
│                                                                  │
│  ┌──────────────────────────────────────────────────┐          │
│  │         Enrichment Pipeline                       │          │
│  │                                                    │          │
│  │  1. Transform Handler                             │          │
│  │     - Extract key fields                          │          │
│  │     - Add tracking metadata                       │          │
│  │                                                    │          │
│  │  2. Validation Handler                            │          │
│  │     - Validate required fields                    │          │
│  │     - Skip invalid messages                       │          │
│  │                                                    │          │
│  │  3. ClaimX API Lookup Handler                     │          │
│  │     - Fetch full task details                     │          │
│  │     - GET /api/v1/tasks/{task_id}                 │          │
│  │     - Cache for 60s                               │          │
│  │                                                    │          │
│  │  4. Delta Table Writer                            │          │
│  │     - Write to itel_cabinet_task_tracking         │          │
│  │     - Append mode                                 │          │
│  │                                                    │          │
│  └──────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │  Delta Table:          │
            │  itel_cabinet_task_    │
            │  tracking              │
            │                        │
            │  - Event history       │
            │  - Enriched task data  │
            │  - Status transitions  │
            └────────────────────────┘
```

### Component Overview

| Component | Type | Purpose |
|-----------|------|---------|
| iTel Cabinet Plugin | TaskTriggerPlugin | Detects task 32513 events and publishes to topic |
| Tracking Worker | PluginActionWorker | Enriches and writes to Delta table |
| ClaimX API Connection | Named Connection | Authenticated API client for enrichment |
| Delta Table | Data Storage | Historical tracking and analysis |

---

## Component Specifications

### 1. iTel Cabinet Plugin

**Type:** TaskTriggerPlugin
**Module:** `kafka_pipeline.plugins.task_trigger`
**Config Location:** `config/plugins/itel_cabinet_api/config.yaml`

**Configuration:**
```yaml
name: itel_cabinet_api
module: kafka_pipeline.plugins.task_trigger
class: TaskTriggerPlugin
enabled: true
priority: 50

config:
  include_task_data: true
  include_project_data: true

  triggers:
    32513:
      name: iTel Cabinet Task Tracking
      description: Track all status changes for iTel cabinet tasks

      # Track ALL events for this task type
      on_any:
        publish_to_topic: itel.cabinet.task.tracking

        log:
          level: info
          message: "iTel Cabinet task event tracked | event_id={event_id} | task_id={task_id} | status={task_status}"
```

**Behavior:**
- Filters for task_id = 32513
- Triggers on ANY status change (creation, assignment, completion, etc.)
- Publishes full event payload to `itel.cabinet.task.tracking` topic
- Logs every tracked event for observability

**Payload Structure:**
```json
{
  "trigger_name": "iTel Cabinet Task Tracking",
  "event_id": "abc123...",
  "event_type": "CUSTOM_TASK_ASSIGNED|CUSTOM_TASK_COMPLETED|etc",
  "project_id": "12345",
  "task_id": 32513,
  "assignment_id": 789,
  "task_name": "iTel Cabinet Documentation",
  "task_status": "assigned|in_progress|completed|etc",
  "timestamp": "2026-01-10T12:34:56.789Z",
  "task": { /* full task row from event */ },
  "project": { /* full project row from event */ }
}
```

---

### 2. ClaimX API Connection

**Type:** Named Connection
**Config Location:** `plugins/config/connections.yaml`

**Configuration:**
```yaml
connections:
  claimx_api:
    name: claimx_api
    base_url: ${CLAIMX_API_BASE_URL}  # e.g., https://api.claimx.com
    auth_type: bearer
    auth_token: ${CLAIMX_API_TOKEN}
    timeout_seconds: 30
    max_retries: 3
    retry_backoff_base: 2
    retry_backoff_max: 60
    headers:
      User-Agent: ClaimXPipeline-iTel/1.0
      Accept: application/json
```

**Environment Variables Required:**
- `CLAIMX_API_BASE_URL`: ClaimX API base URL
- `CLAIMX_API_TOKEN`: API authentication token (Bearer)

**API Endpoints Used:**
- `GET /api/v1/tasks/{task_id}` - Fetch full task details
- `GET /api/v1/projects/{project_id}` - (Optional) Fetch additional project data

---

### 3. iTel Cabinet Tracking Worker

**Type:** PluginActionWorker
**Config Location:** `plugins/config/workers.yaml`

**Configuration:**
```yaml
workers:
  itel_cabinet_tracking_worker:
    name: itel_cabinet_tracking_worker

    # Kafka configuration
    input_topic: itel.cabinet.task.tracking
    consumer_group: itel_cabinet_tracking_worker_group
    batch_size: 100
    enable_auto_commit: false

    # No destination_connection - we're writing to Delta table
    # destination_connection: null
    # destination_path: null

    # Result topics
    success_topic: itel.cabinet.task.tracking.success
    error_topic: itel.cabinet.task.tracking.errors

    # Enrichment pipeline
    enrichment_handlers:
      # Step 1: Transform - Extract and structure fields
      - type: transform
        config:
          mappings:
            # Event metadata
            event_id: event_id
            event_type: event_type
            event_timestamp: timestamp

            # Task identifiers
            task_id: task_id
            assignment_id: assignment_id
            project_id: project_id

            # Task status tracking
            task_name: task_name
            task_status: task_status
            previous_status: task.previous_status  # If available

            # Basic task data from event
            assigned_to_user_id: task.assigned_to_user_id
            assigned_by_user_id: task.assigned_by_user_id
            completed_at: task.completed_at
            created_at: task.created_at

          defaults:
            tracking_version: "1.0"
            source: "claimx_itel_plugin"
            processed_at: "${CURRENT_TIMESTAMP}"  # Worker processing time

      # Step 2: Validation - Ensure data integrity
      - type: validation
        config:
          required_fields:
            - event_id
            - task_id
            - event_type
            - task_status
            - project_id

          field_rules:
            task_id:
              equals: 32513  # Extra validation

          # Don't skip any messages - track all statuses
          # skip_if: null

      # Step 3: ClaimX API Lookup - Enrich with full task details
      - type: lookup
        config:
          connection: claimx_api
          endpoint: /api/v1/tasks/{task_id}
          path_params:
            task_id: task_id
          result_field: claimx_task_details
          cache_ttl: 60  # Cache for 1 minute (tasks change frequently)

          # Error handling
          on_error: log_and_continue  # Don't fail entire message if API is down

      # Step 4: Custom handler - Write to Delta table
      # NOTE: This requires a custom enrichment handler
      - type: kafka_pipeline.plugins.handlers.delta_writer:DeltaTableWriter
        config:
          table_name: itel_cabinet_task_tracking
          mode: append
          schema_evolution: true

          # Field mapping for Delta table
          column_mapping:
            # Primary tracking fields
            event_id: event_id
            event_type: event_type
            event_timestamp: event_timestamp
            processed_timestamp: processed_at

            # Task identification
            task_id: task_id
            assignment_id: assignment_id
            project_id: project_id

            # Status tracking
            task_status: task_status
            task_name: task_name

            # User tracking
            assigned_to_user_id: assigned_to_user_id
            assigned_by_user_id: assigned_by_user_id

            # Timestamps
            task_created_at: created_at
            task_completed_at: completed_at

            # Enriched data (JSON columns)
            claimx_task_full: claimx_task_details
            original_event: task
            project_data: project
```

**Behavior:**
1. Consumes from `itel.cabinet.task.tracking` topic
2. Transforms and validates event data
3. Enriches with full task details from ClaimX API
4. Writes enriched record to Delta table
5. Publishes success/error events for monitoring

---

## Data Flow

### Event Lifecycle

```
1. ClaimX Event Occurs
   - Task 32513 is created/assigned/updated/completed
   - Event published to claimx.events topic
   ↓

2. Enrichment Worker Processes Event
   - Enriches with ClaimX entity data
   - Runs plugin orchestrator
   ↓

3. iTel Cabinet Plugin Triggered
   - Filters task_id = 32513
   - Publishes to itel.cabinet.task.tracking topic
   - Logs event
   ↓

4. Tracking Worker Consumes Event
   - Kafka consumer reads from itel.cabinet.task.tracking
   ↓

5. Enrichment Pipeline Executes
   Step 1: Transform
     - Extract fields
     - Add metadata
   ↓
   Step 2: Validate
     - Check required fields
     - Verify task_id = 32513
   ↓
   Step 3: ClaimX API Lookup
     - GET /api/v1/tasks/{task_id}
     - Cache response for 60s
   ↓
   Step 4: Delta Table Write
     - Append record to itel_cabinet_task_tracking
   ↓

6. Success/Error Tracking
   - Publish to success or error topic
   - Worker commits Kafka offset
```

### Data Transformations

| Stage | Input | Output |
|-------|-------|--------|
| Plugin | ClaimX enriched event | Topic message with trigger metadata |
| Transform | Topic message | Structured tracking record |
| Validation | Structured record | Validated record |
| Lookup | Validated record | Record + full ClaimX task details |
| Delta Write | Enriched record | Delta table row |

---

## Configuration

### Directory Structure

```
config/plugins/itel_cabinet_api/
├── config.yaml          # Plugin configuration
├── SPEC.md              # This specification document
└── README.md            # User-facing documentation

plugins/config/
├── connections.yaml     # Add claimx_api connection
└── workers.yaml         # Add itel_cabinet_tracking_worker
```

### Environment Variables

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `CLAIMX_API_BASE_URL` | ClaimX API base URL | `https://api.claimx.com` | Yes |
| `CLAIMX_API_TOKEN` | ClaimX API Bearer token | `eyJ0eXAiOiJKV1QiLCJh...` | Yes |

### Kafka Topics

| Topic | Purpose | Retention | Partitions |
|-------|---------|-----------|------------|
| `itel.cabinet.task.tracking` | Plugin → Worker communication | 7 days | 3 |
| `itel.cabinet.task.tracking.success` | Successful writes | 3 days | 1 |
| `itel.cabinet.task.tracking.errors` | Failed writes (DLQ) | 30 days | 1 |

---

## Delta Table Schema

### Table: `itel_cabinet_task_tracking`

**Purpose:** Store complete history of iTel cabinet task status changes with enriched data.

**Schema:**

```sql
CREATE TABLE itel_cabinet_task_tracking (
  -- Event identification
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  processed_timestamp TIMESTAMP NOT NULL,

  -- Task identification
  task_id INTEGER NOT NULL,
  assignment_id INTEGER,
  project_id STRING NOT NULL,

  -- Status tracking
  task_status STRING NOT NULL,
  task_name STRING,

  -- User tracking
  assigned_to_user_id INTEGER,
  assigned_by_user_id INTEGER,

  -- Timestamps
  task_created_at TIMESTAMP,
  task_completed_at TIMESTAMP,

  -- Enriched data (complex types)
  claimx_task_full STRUCT<...>,  -- Full task details from ClaimX API
  original_event STRUCT<...>,     -- Original event task data
  project_data STRUCT<...>,       -- Project data from event

  -- Metadata
  tracking_version STRING,
  source STRING,

  -- Partitioning (optional)
  -- year INTEGER,
  -- month INTEGER,
  -- day INTEGER
)
USING delta
PARTITIONED BY (year, month, day)  -- Partition by event_timestamp
LOCATION 's3://your-bucket/delta/itel_cabinet_task_tracking'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

**Indexes/Optimization:**
- Partition by date (year, month, day) from `event_timestamp`
- Z-ORDER by `task_id`, `task_status` for query performance
- Enable Change Data Feed for downstream consumers

**Sample Query - Task Lifecycle:**
```sql
SELECT
  task_id,
  assignment_id,
  task_status,
  event_timestamp,
  task_created_at,
  task_completed_at,
  DATEDIFF(task_completed_at, task_created_at) as days_to_complete
FROM itel_cabinet_task_tracking
WHERE task_id = 32513
ORDER BY event_timestamp ASC;
```

**Sample Query - Status Transitions:**
```sql
SELECT
  task_id,
  task_status,
  COUNT(*) as status_count,
  MIN(event_timestamp) as first_seen,
  MAX(event_timestamp) as last_seen
FROM itel_cabinet_task_tracking
WHERE year = 2026 AND month = 1
GROUP BY task_id, task_status
ORDER BY task_id, first_seen;
```

---

## Implementation Plan

### Phase 1: Core Infrastructure (Estimated: 2-3 days)

**Tasks:**
1. ✅ Review plugin documentation (DONE)
2. ✅ Create specification document (DONE)
3. ⬜ Update plugin configuration with actual task_id 32513
4. ⬜ Add `claimx_api` connection to `connections.yaml`
5. ⬜ Create Delta table schema and table
6. ⬜ Implement custom `DeltaTableWriter` enrichment handler
7. ⬜ Add worker configuration to `workers.yaml`

**Deliverables:**
- Updated plugin config
- Connection configuration
- Delta table created
- Custom handler implemented
- Worker configuration

### Phase 2: Testing & Validation (Estimated: 1-2 days)

**Tasks:**
1. ⬜ Unit tests for DeltaTableWriter handler
2. ⬜ Integration test with mock ClaimX events
3. ⬜ End-to-end test with real task 32513 events
4. ⬜ Validate Delta table writes
5. ⬜ Test error handling and retry logic
6. ⬜ Load testing (handle burst of events)

**Deliverables:**
- Test suite
- Test documentation
- Performance benchmarks

### Phase 3: Deployment & Monitoring (Estimated: 1 day)

**Tasks:**
1. ⬜ Deploy to staging environment
2. ⬜ Set up monitoring dashboards
3. ⬜ Configure alerts
4. ⬜ Deploy to production
5. ⬜ Monitor initial production traffic
6. ⬜ Document runbooks

**Deliverables:**
- Production deployment
- Monitoring dashboards
- Alert configuration
- Runbook documentation

---

## Testing Strategy

### Unit Tests

**DeltaTableWriter Handler:**
```python
def test_delta_writer_writes_record():
    """Test that DeltaTableWriter correctly writes to Delta table"""

def test_delta_writer_handles_schema_evolution():
    """Test schema evolution when new fields are added"""

def test_delta_writer_handles_write_errors():
    """Test error handling when Delta write fails"""
```

### Integration Tests

**Plugin Triggering:**
```python
def test_plugin_triggers_on_task_32513():
    """Test that plugin only triggers for task_id 32513"""

def test_plugin_publishes_to_correct_topic():
    """Test that plugin publishes to itel.cabinet.task.tracking"""

def test_plugin_includes_full_task_data():
    """Test that published message includes task and project data"""
```

**Worker Processing:**
```python
def test_worker_enriches_with_claimx_api():
    """Test that worker calls ClaimX API and enriches data"""

def test_worker_writes_to_delta_table():
    """Test end-to-end worker processing to Delta table"""

def test_worker_publishes_success_events():
    """Test that successful writes publish to success topic"""
```

### End-to-End Tests

**Scenario 1: Task Creation**
1. Simulate task 32513 creation event
2. Verify plugin triggers and publishes
3. Verify worker enriches and writes to Delta
4. Query Delta table and validate record

**Scenario 2: Status Transitions**
1. Simulate multiple status changes for same task
2. Verify all transitions are tracked
3. Verify Delta table contains all status records
4. Validate timeline is correct

**Scenario 3: Error Handling**
1. Simulate ClaimX API failure
2. Verify worker logs error and continues
3. Verify error published to DLQ topic
4. Verify worker doesn't crash

---

## Monitoring & Metrics

### Key Metrics

**Plugin Metrics:**
- `itel_plugin_events_triggered_total` - Total events triggered
- `itel_plugin_events_published_total` - Total events published to topic
- `itel_plugin_publish_errors_total` - Publish failures

**Worker Metrics:**
- `itel_worker_messages_consumed_total` - Total messages consumed
- `itel_worker_messages_processed_total` - Successfully processed
- `itel_worker_messages_failed_total` - Failed processing
- `itel_worker_claimx_api_calls_total` - API calls made
- `itel_worker_claimx_api_errors_total` - API call failures
- `itel_worker_claimx_api_latency_seconds` - API latency histogram
- `itel_worker_delta_writes_total` - Delta table writes
- `itel_worker_delta_write_errors_total` - Delta write failures
- `itel_worker_processing_latency_seconds` - End-to-end processing time

### Alerts

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| Plugin Publish Failures | `itel_plugin_publish_errors_total` > 10 in 5m | Warning | Check Kafka connectivity |
| Worker Consumer Lag | Consumer lag > 1000 messages | Warning | Scale worker or investigate |
| ClaimX API Errors | `itel_worker_claimx_api_errors_total` > 20 in 5m | Critical | Check ClaimX API health |
| Delta Write Failures | `itel_worker_delta_write_errors_total` > 5 in 5m | Critical | Check Delta table and permissions |
| Worker Stopped | No messages consumed in 10m | Critical | Restart worker |

### Dashboards

**Dashboard 1: iTel Cabinet Tracking Overview**
- Events tracked over time (line chart)
- Status distribution (pie chart)
- Processing latency (histogram)
- Error rate (line chart)

**Dashboard 2: Worker Health**
- Consumer lag (gauge)
- Messages processed/sec (line chart)
- ClaimX API latency (histogram)
- Delta write rate (line chart)

**Dashboard 3: Data Quality**
- Events by status (bar chart)
- Tasks tracked (counter)
- Enrichment success rate (gauge)
- Schema evolution events (counter)

---

## Future Enhancements

### Phase 2 Features

1. **Direct iTel Cabinet API Integration**
   - Add connection to iTel Cabinet API
   - Send enriched data to iTel endpoints
   - Handle iTel API responses and errors

2. **Photo/Attachment Processing**
   - Fetch photo URLs from ClaimX
   - Upload to iTel Cabinet storage
   - Track photo processing status

3. **Bidirectional Sync**
   - Consume updates from iTel Cabinet
   - Update ClaimX tasks based on iTel status
   - Conflict resolution strategy

4. **Advanced Tracking**
   - Track time in each status
   - Calculate SLAs and performance metrics
   - Anomaly detection for stuck tasks

5. **Batch Processing**
   - Batch multiple status updates
   - Reduce Delta table write frequency
   - Optimize for high-volume scenarios

### Technical Improvements

1. **Performance Optimization**
   - Implement caching layer for ClaimX API
   - Optimize Delta table partitioning
   - Add connection pooling tuning

2. **Operational Excellence**
   - Automated testing in CI/CD
   - Blue/green deployment support
   - Disaster recovery procedures

3. **Data Governance**
   - PII detection and masking
   - Data retention policies
   - Audit logging enhancements

---

## Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| Plugin | Reactive component that triggers on specific ClaimX events |
| Worker | Background process that consumes Kafka topics and processes messages |
| Enrichment | Process of adding additional data from external sources |
| Delta Table | Lakehouse table format with ACID transactions |
| Named Connection | Reusable HTTP client configuration with auth and retry logic |

### B. References

- [Plugin Connections Guide](../../../PLUGIN_CONNECTIONS_GUIDE.md)
- [Task Trigger Plugin README](../../../plugins/config/task_trigger/README.md)
- [ClaimX API Documentation](link-to-claimx-docs)
- [Delta Lake Documentation](https://docs.delta.io/)

### C. Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | 2026-01-10 | System | Initial specification created |

---

## Approval

**Specification Status:** Draft - Pending Review

**Reviewers:**
- [ ] Technical Lead
- [ ] Product Owner
- [ ] DevOps Lead

**Approvals:**
- [ ] Approved for Implementation
- [ ] Architecture Review Complete
- [ ] Security Review Complete

---

**Next Steps:**
1. Review this specification document
2. Gather feedback and requirements
3. Update configuration with actual ClaimX API details
4. Implement custom DeltaTableWriter handler
5. Begin Phase 1 implementation
