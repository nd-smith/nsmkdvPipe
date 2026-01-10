# Task Trigger Plugin

## Overview
Triggers custom actions when specific ClaimX task types are assigned or completed.

## Configuration
Edit `config.yaml` in this directory to configure task triggers.

## Usage

### 1. Find Your Task Template IDs
Query your ClaimX data to find task template IDs (task_id):
```sql
SELECT DISTINCT task_id, task_name
FROM claimx_task_templates
ORDER BY task_id;
```

### 2. Configure Triggers
Edit `config.yaml` and add entries to the `triggers` section:

```yaml
config:
  triggers:
    456:  # Your task_id
      name: "Photo Documentation"
      on_completed:
        publish_to_topic: "claimx.task-photo-completed"
        webhook: "https://api.example.com/notify"
```

### 3. Enable the Plugin
Set `enabled: true` in `config.yaml` (default).

### 4. Test
Start the enrichment worker and process events with your configured task types.

## Available Actions

### Publish to Kafka Topic
```yaml
on_completed:
  publish_to_topic: "claimx.task-completed"
```

### Call HTTP Webhook
```yaml
on_completed:
  webhook:
    url: "https://api.example.com/webhook"
    method: "POST"
    headers:
      Authorization: "Bearer ${WEBHOOK_TOKEN}"
```

### Log Message
```yaml
on_completed:
  log:
    level: "info"
    message: "Task completed successfully"
```

### Combine Multiple Actions
```yaml
on_completed:
  publish_to_topic: "claimx.task-completed"
  webhook: "https://api.example.com/notify"
  log: "Task completed and webhook sent"
```

## Event Types

- `CUSTOM_TASK_ASSIGNED` - Task was assigned to someone
- `CUSTOM_TASK_COMPLETED` - Task was marked as completed

Configure triggers with:
- `on_assigned` - Only fires on assignment
- `on_completed` - Only fires on completion
- `on_any` - Fires on both events

## Payload Structure

Messages sent to Kafka topics or webhooks:

```json
{
  "trigger_name": "Photo Documentation",
  "event_id": "abc123...",
  "event_type": "CUSTOM_TASK_COMPLETED",
  "project_id": "12345",
  "task_id": 456,
  "assignment_id": 789,
  "task_name": "Photo Documentation",
  "task_status": "completed",
  "timestamp": "2026-01-10T12:34:56.789Z",
  "task": { /* full task data */ }
}
```

## Troubleshooting

### Plugin Not Triggering
- Verify `enabled: true` in config.yaml
- Check task_id matches ClaimX data
- Ensure event_type is CUSTOM_TASK_ASSIGNED or CUSTOM_TASK_COMPLETED
- Enable DEBUG logging to see plugin execution

### Webhook Failures
- Check webhook URL is reachable
- Verify authentication headers
- Check webhook server logs for errors

## Examples

### Notify External System on Task Completion
```yaml
triggers:
  456:
    name: "Final Report Submitted"
    on_completed:
      webhook: "https://reporting.example.com/api/report-complete"
      log: "Final report webhook sent"
```

### Multi-Channel Notification
```yaml
triggers:
  789:
    name: "Urgent Inspection Required"
    on_assigned:
      publish_to_topic: "notifications.urgent"
      webhook: "https://sms.example.com/api/send"
      log:
        level: "warning"
        message: "Urgent inspection task assigned - notifications sent"
```

### Audit Trail for Compliance
```yaml
triggers:
  999:
    name: "Compliance Checklist Complete"
    on_completed:
      publish_to_topic: "audit.compliance"
      webhook:
        url: "https://audit.example.com/api/log"
        headers:
          X-Audit-Source: "claimx-pipeline"
      log:
        level: "warning"
        message: "AUDIT: Compliance checklist completed"
```
