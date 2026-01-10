# Plugin System Integration Plan

## Overview
Integration plan for wiring the domain-agnostic plugin system into the existing XACT and ClaimX pipeline infrastructure.

**Status:** ✅ Plugin framework complete, 🔧 Worker integration needed

---

## Architecture Summary

### Current Plugin System Components
- ✅ **base.py** - Plugin base classes, PluginContext, PluginResult, Domain/Stage enums
- ✅ **registry.py** - PluginRegistry, PluginOrchestrator, ActionExecutor
- ✅ **task_trigger.py** - POC plugin for ClaimX task triggers
- ✅ **loader.py** - YAML-based plugin configuration loading

### Integration Approach
**Pattern:** Add plugin orchestration at key processing points in each worker

```python
# Pseudocode for each integration point:
context = build_plugin_context(domain, stage, message, data)
result = await plugin_orchestrator.execute(context)
if result.terminated:
    return  # Plugin filtered out message
# Continue normal processing
```

---

## Integration Points by Worker

### XACT Pipeline (6 integration points)

| Worker | Stage | Location | Priority |
|--------|-------|----------|----------|
| EventIngesterWorker | EVENT_INGEST | After event parsing | P1 |
| DownloadWorker | DOWNLOAD_QUEUED | Before download starts | P1 |
| DownloadWorker | DOWNLOAD_COMPLETE | After download success | P1 |
| DownloadWorker | ERROR | On download failure | P2 |
| UploadWorker | UPLOAD_COMPLETE | After upload success | P1 |
| DeltaEventsWorker | EVENT_INGEST | Before Delta write | P2 |

### ClaimX Pipeline (8 integration points)

| Worker | Stage | Location | Priority |
|--------|-------|----------|----------|
| ClaimXEventIngesterWorker | EVENT_INGEST | After event parsing | P1 |
| ClaimXEnrichmentWorker | ENRICHMENT_QUEUED | Before handler execution | P1 |
| ClaimXEnrichmentWorker | ENRICHMENT_COMPLETE | After handler success | P0 (POC exists) |
| ClaimXEnrichmentWorker | ENTITY_WRITE | Before entity rows produced | P2 |
| ClaimXEnrichmentWorker | ERROR | On enrichment failure | P2 |
| ClaimXDownloadWorker | DOWNLOAD_QUEUED | Before download starts | P1 |
| ClaimXDownloadWorker | DOWNLOAD_COMPLETE | After download success | P1 |
| ClaimXUploadWorker | UPLOAD_COMPLETE | After upload success | P1 |

---

## Implementation Phases

### Phase 1: Core Infrastructure (Priority P0-P1)
**Goal:** Enable plugin execution in all critical workers

#### 1.1 Add Plugin Orchestrator to Workers
**Files to modify:**
- `kafka_pipeline/xact/workers/event_ingester.py`
- `kafka_pipeline/xact/workers/download_worker.py`
- `kafka_pipeline/xact/workers/upload_worker.py`
- `kafka_pipeline/claimx/workers/event_ingester.py`
- `kafka_pipeline/claimx/workers/enrichment_worker.py`
- `kafka_pipeline/claimx/workers/download_worker.py`
- `kafka_pipeline/claimx/workers/upload_worker.py`

**Changes per worker:**
```python
from kafka_pipeline.plugins.registry import (
    PluginOrchestrator,
    get_plugin_registry,
)
from kafka_pipeline.plugins.base import PluginContext, Domain, PipelineStage

class Worker:
    def __init__(self, config):
        # ... existing init ...
        self.plugin_registry = get_plugin_registry()
        self.plugin_orchestrator = PluginOrchestrator(
            registry=self.plugin_registry,
            action_executor=None,  # Will create default executor
        )
```

#### 1.2 Build PluginContext at Each Stage
**Example: XACT EventIngester (EVENT_INGEST stage)**
```python
# In _handle_event_message() after parsing EventMessage:

context = PluginContext(
    domain=Domain.XACT,
    stage=PipelineStage.EVENT_INGEST,
    message=event_message,
    event_id=event_message.trace_id,
    event_type=event_message.type,
    project_id=None,  # XACT doesn't have project_id
    data={
        "attachments": event_message.attachments,
        "assignment_id": event_message.assignment_id,
    },
    headers={},  # Populate from Kafka headers if available
)

orchestrator_result = await self.plugin_orchestrator.execute(context)

if orchestrator_result.terminated:
    log_with_context(
        logger, logging.INFO,
        "Event filtered by plugin",
        event_id=event_message.trace_id,
        reason=orchestrator_result.termination_reason,
    )
    return  # Skip producing download tasks
```

**Example: ClaimX EnrichmentWorker (ENRICHMENT_COMPLETE stage)**
```python
# In _process_single_task() after handler.process():

context = PluginContext(
    domain=Domain.CLAIMX,
    stage=PipelineStage.ENRICHMENT_COMPLETE,
    message=task,  # ClaimXEnrichmentTask
    event_id=task.event_id,
    event_type=task.event_type,
    project_id=task.project_id,
    data={
        "entities": entity_rows,  # EntityRowsMessage
        "handler_result": handler_result,
    },
    headers={},
)

orchestrator_result = await self.plugin_orchestrator.execute(context)

if orchestrator_result.terminated:
    # Plugin filtered this event, skip entity write
    return
```

#### 1.3 Handle Plugin Results
**Termination Handling:**
- If `orchestrator_result.terminated == True`, skip subsequent pipeline processing
- Log termination reason for observability
- Consider metrics for filtered events

**Action Execution:**
- ActionExecutor already handles common actions (publish, webhook, log)
- Need to pass Kafka producer to ActionExecutor for publish actions
- Update worker initialization to provide producer

**Example:**
```python
# In worker __init__:
from kafka_pipeline.plugins.registry import ActionExecutor

self.action_executor = ActionExecutor(
    producer=self.producer,  # Pass Kafka producer
    http_client=None,  # Optional: pass aiohttp client for webhooks
)

self.plugin_orchestrator = PluginOrchestrator(
    registry=self.plugin_registry,
    action_executor=self.action_executor,
)
```

#### 1.4 Plugin Configuration Loading
**Create plugin configuration files:**
- `config/plugins.yaml` - Global plugin config
- `config/xact_plugins.yaml` - XACT-specific plugins
- `config/claimx_plugins.yaml` - ClaimX-specific plugins

**Example `config/claimx_plugins.yaml`:**
```yaml
plugins:
  - name: task_trigger
    enabled: true
    config:
      triggers:
        456:  # task_id for photo documentation
          name: "Photo Documentation Task"
          on_completed:
            publish_to_topic: "claimx.task-456-completed"
            webhook: "https://api.example.com/task-complete"
        789:
          name: "Damage Assessment Task"
          on_completed:
            publish_to_topic: "claimx.damage-assessment-complete"
```

**Load plugins on worker startup:**
```python
# In worker main() or __init__:
from kafka_pipeline.plugins.loader import load_plugins_from_yaml

plugin_config_path = config.get("plugins_config_path")
if plugin_config_path:
    loaded_plugins = load_plugins_from_yaml(plugin_config_path)
    logger.info(f"Loaded {len(loaded_plugins)} plugins from {plugin_config_path}")
```

---

### Phase 2: Extended Integration (Priority P2)

#### 2.1 Error/Retry Stage Plugins
**Workers:** All download/upload/enrichment workers

**Integration point:** In error handling blocks
```python
# In _handle_failure() or exception handlers:
context = PluginContext(
    domain=Domain.XACT,  # or CLAIMX
    stage=PipelineStage.ERROR,
    message=task_message,
    event_id=task_message.trace_id,
    data={
        "error_category": error_category,
        "retry_count": task_message.retry_count,
    },
    error=exception,
    retry_count=task_message.retry_count,
)

await self.plugin_orchestrator.execute(context)
# Continue with normal retry/DLQ handling
```

**Use cases:**
- Custom alerting for permanent failures
- Webhook notifications on circuit breaker open
- Log aggregation for specific error patterns
- Custom retry policies

#### 2.2 DLQ Stage Plugins
**Workers:** Download/upload/enrichment workers

**Integration point:** Before sending to DLQ
```python
context = PluginContext(
    domain=Domain.XACT,
    stage=PipelineStage.DLQ,
    message=task_message,
    event_id=task_message.trace_id,
    data={"reason": "max_retries_exceeded"},
    error=last_exception,
    retry_count=task_message.retry_count,
)

await self.plugin_orchestrator.execute(context)
```

**Use cases:**
- External alerting for DLQ messages
- Archive DLQ messages to external storage
- Trigger manual review workflows

#### 2.3 Delta Writer Integration
**Workers:** DeltaEventsWorker (xact and claimx)

**Integration point:** Before batch write
```python
context = PluginContext(
    domain=Domain.XACT,
    stage=PipelineStage.EVENT_INGEST,  # or custom DELTA_WRITE stage
    message=batch_events[0],  # First event in batch
    event_id=batch_events[0].get("trace_id"),
    data={
        "batch_size": len(batch_events),
        "table_path": self.events_table_path,
    },
)

await self.plugin_orchestrator.execute(context)
```

---

### Phase 3: Advanced Features

#### 3.1 Plugin Lifecycle Management
**Add lifecycle hooks to workers:**
```python
class Worker:
    async def start(self):
        # ... existing startup ...

        # Call on_load for all plugins
        for plugin in self.plugin_registry.list_plugins():
            await plugin.on_load()

    async def stop(self):
        # Call on_unload for all plugins
        for plugin in self.plugin_registry.list_plugins():
            await plugin.on_unload()

        # ... existing shutdown ...
```

**Use cases:**
- Plugins can initialize resources (DB connections, API clients)
- Plugins can clean up resources on shutdown
- Plugins can validate configuration on load

#### 3.2 Plugin Metrics & Observability
**Add metrics for plugin execution:**
```python
# In PluginOrchestrator.execute():
from kafka_pipeline.common.metrics import get_metrics_client

metrics = get_metrics_client()

start_time = time.time()
result = await plugin.execute(context)
duration = time.time() - start_time

metrics.histogram(
    "plugin.execution.duration",
    duration,
    tags={
        "plugin": plugin.name,
        "stage": context.stage.value,
        "domain": context.domain.value,
        "success": result.success,
    }
)
```

**Metrics to track:**
- Plugin execution duration
- Plugin success/failure rates
- Actions executed per plugin
- Messages filtered by plugins
- Error rates per plugin

#### 3.3 Dynamic Plugin Reloading
**Add hot-reload capability:**
```python
# Watch plugin config file for changes
# Reload plugins without restarting workers
# Useful for development and testing
```

#### 3.4 Plugin Testing Framework
**Create test utilities:**
```python
# tests/kafka_pipeline/plugins/test_helpers.py

def create_test_context(
    domain: Domain,
    stage: PipelineStage,
    **kwargs
) -> PluginContext:
    """Helper to create PluginContext for testing."""
    pass

def assert_plugin_result(
    result: PluginResult,
    success: bool = True,
    actions: int = 0,
    terminated: bool = False,
):
    """Assert plugin result expectations."""
    pass
```

---

## Testing Strategy

### Unit Tests
**Per worker integration:**
- Test PluginContext building with mock messages
- Test orchestrator call with mock plugins
- Test termination handling
- Test action execution

**Example:**
```python
# tests/kafka_pipeline/xact/workers/test_event_ingester_plugins.py

async def test_event_ingester_plugin_execution():
    """Test plugin execution in event ingester."""
    # Create mock plugin that filters events
    # Verify event is not produced to downloads.pending
    pass

async def test_event_ingester_plugin_publish_action():
    """Test plugin publish action."""
    # Create mock plugin that publishes to custom topic
    # Verify message published
    pass
```

### Integration Tests
**End-to-end flows:**
- Test TaskTriggerPlugin with real ClaimX enrichment
- Test filtering plugin that skips certain events
- Test webhook plugin with mock HTTP server
- Test publish plugin with test Kafka topics

### Performance Tests
**Plugin overhead:**
- Measure baseline worker throughput
- Measure throughput with 1, 5, 10 plugins
- Identify bottlenecks in orchestration
- Optimize hot paths

---

## Configuration Schema

### Worker Configuration Addition
**Add to `kafka.xact` and `kafka.claimx` sections:**
```yaml
kafka:
  xact:
    # ... existing config ...

    # Plugin configuration
    plugins:
      enabled: true
      config_path: "config/xact_plugins.yaml"

  claimx:
    # ... existing config ...

    plugins:
      enabled: true
      config_path: "config/claimx_plugins.yaml"
```

### Plugin Configuration Schema
**`config/xact_plugins.yaml`:**
```yaml
plugins:
  - name: "custom_filter"
    module: "kafka_pipeline.plugins.custom_filter"
    class: "CustomFilterPlugin"
    enabled: true
    priority: 10  # Run early
    config:
      filter_pattern: ".*test.*"

  - name: "audit_logger"
    module: "kafka_pipeline.plugins.audit"
    class: "AuditLoggerPlugin"
    enabled: true
    priority: 1000  # Run late
    config:
      log_level: "INFO"
      include_payload: false
```

**`config/claimx_plugins.yaml`:**
```yaml
plugins:
  - name: "task_trigger"
    module: "kafka_pipeline.plugins.task_trigger"
    class: "TaskTriggerPlugin"
    enabled: true
    config:
      triggers:
        456:
          name: "Photo Documentation"
          on_completed:
            publish_to_topic: "claimx.task-456-completed"
            webhook:
              url: "https://api.example.com/task-complete"
              method: "POST"
              headers:
                Authorization: "Bearer ${WEBHOOK_TOKEN}"
```

---

## Rollout Plan

### Step 1: Foundation (Week 1)
- [ ] Add PluginOrchestrator to all P0/P1 workers
- [ ] Build PluginContext at each integration point
- [ ] Test with no plugins registered (should be no-op)
- [ ] Deploy to dev environment

### Step 2: POC Plugin (Week 1-2)
- [ ] Enhance TaskTriggerPlugin configuration
- [ ] Test TaskTriggerPlugin in ClaimX enrichment worker
- [ ] Verify publish and webhook actions work
- [ ] Document plugin development guide

### Step 3: Expanded Integration (Week 2-3)
- [ ] Add error/retry stage integration
- [ ] Add DLQ stage integration
- [ ] Create 2-3 additional example plugins:
  - Event filter plugin (skip events matching criteria)
  - Audit logger plugin (log all events to external system)
  - Custom metrics plugin (emit domain-specific metrics)
- [ ] Deploy to staging environment

### Step 4: Production Readiness (Week 3-4)
- [ ] Add comprehensive unit tests
- [ ] Add integration tests
- [ ] Performance testing (measure overhead)
- [ ] Documentation (user guide, API reference)
- [ ] Metrics and monitoring dashboards
- [ ] Production deployment plan

### Step 5: Production Deployment (Week 4+)
- [ ] Deploy with plugins disabled (validate no regression)
- [ ] Enable TaskTriggerPlugin for subset of tasks
- [ ] Monitor performance and errors
- [ ] Gradual rollout to all tasks
- [ ] Enable additional plugins as needed

---

## Success Criteria

### Functional Requirements
- ✅ Plugins execute at all defined stages in both XACT and ClaimX pipelines
- ✅ Plugin filtering (terminate_pipeline) works correctly
- ✅ Plugin actions (publish, webhook, log) execute successfully
- ✅ Plugin configuration loads from YAML
- ✅ Plugin lifecycle hooks (on_load, on_unload) are called
- ✅ Plugins can access domain-specific context data

### Performance Requirements
- ✅ Plugin orchestration adds <10ms latency per message
- ✅ No throughput degradation with ≤5 plugins per stage
- ✅ No memory leaks from plugin execution
- ✅ Action execution is non-blocking for async actions

### Operational Requirements
- ✅ Plugin errors don't crash workers
- ✅ Plugin execution is logged for debugging
- ✅ Metrics available for plugin performance monitoring
- ✅ Configuration can be updated without code changes
- ✅ Documentation covers common plugin use cases

---

## Risk Mitigation

### Risk: Plugin Performance Impact
**Mitigation:**
- Benchmark plugin orchestration overhead
- Set timeout limits on plugin execution
- Make plugin execution optional via config
- Monitor latency metrics per stage

### Risk: Plugin Errors Breaking Pipeline
**Mitigation:**
- Wrap plugin execution in try/except
- Continue pipeline processing on plugin failure
- Log plugin errors for debugging
- Add circuit breaker for failing plugins

### Risk: Complex Plugin Configuration
**Mitigation:**
- Provide clear examples in documentation
- Validate plugin config on load
- Provide helpful error messages
- Create plugin templates for common patterns

### Risk: Plugin Action Failures
**Mitigation:**
- Make action execution resilient (retry, fallback)
- Don't block pipeline on action failures
- Log action failures for debugging
- Provide action execution metrics

---

## Documentation Needs

### Developer Documentation
- [ ] Plugin development guide (how to create custom plugins)
- [ ] Plugin API reference (base classes, context, results)
- [ ] Integration guide (how to add plugin support to new workers)
- [ ] Testing guide (how to test plugins)

### User Documentation
- [ ] Plugin configuration guide (YAML schema, examples)
- [ ] Built-in plugins reference (TaskTriggerPlugin, etc.)
- [ ] Common use cases and recipes
- [ ] Troubleshooting guide

### Operational Documentation
- [ ] Deployment guide (enabling/disabling plugins)
- [ ] Monitoring guide (metrics, logs, dashboards)
- [ ] Performance tuning guide
- [ ] Security considerations (webhook URLs, credentials)

---

## Future Enhancements

### Plugin Marketplace
- Create repository of reusable plugins
- Plugin versioning and compatibility
- Plugin discovery mechanism

### Advanced Plugin Features
- Multi-stage plugins (execute at multiple stages)
- Conditional execution (beyond domain/stage/event_type)
- Plugin dependencies (ensure plugin A runs before B)
- Plugin state management (share state between executions)

### Visual Plugin Designer
- UI for building plugins without code
- Visual workflow builder
- No-code plugin configuration

### Plugin Isolation
- Run plugins in separate processes/containers
- Resource limits per plugin
- Fault isolation (plugin crash doesn't affect others)

---

## Appendix: Code Examples

### Example 1: Simple Filter Plugin
```python
from kafka_pipeline.plugins.base import (
    Plugin, PluginContext, PluginResult, Domain, PipelineStage
)

class EventFilterPlugin(Plugin):
    """Filter events based on regex pattern."""

    name = "event_filter"
    description = "Filter events matching a pattern"
    version = "1.0.0"

    domains = [Domain.XACT, Domain.CLAIMX]  # Both domains
    stages = [PipelineStage.EVENT_INGEST]

    default_config = {
        "pattern": ".*",  # Regex pattern
        "invert": False,  # If true, filter events NOT matching
    }

    async def execute(self, context: PluginContext) -> PluginResult:
        import re

        pattern = self.config.get("pattern")
        invert = self.config.get("invert", False)

        event_type = context.event_type or ""
        matches = bool(re.match(pattern, event_type))

        should_filter = matches if not invert else not matches

        if should_filter:
            return PluginResult.filter_out(
                f"Event type '{event_type}' matched filter pattern '{pattern}'"
            )

        return PluginResult.success()
```

### Example 2: Webhook Notification Plugin
```python
class WebhookNotificationPlugin(Plugin):
    """Send webhook notification for specific events."""

    name = "webhook_notification"
    description = "Send webhook on event processing"
    version = "1.0.0"

    stages = [PipelineStage.ENRICHMENT_COMPLETE]

    default_config = {
        "webhook_url": "",
        "include_payload": True,
    }

    async def execute(self, context: PluginContext) -> PluginResult:
        webhook_url = self.config.get("webhook_url")
        if not webhook_url:
            return PluginResult.skip("No webhook URL configured")

        payload = {
            "event_id": context.event_id,
            "event_type": context.event_type,
            "project_id": context.project_id,
            "timestamp": context.timestamp.isoformat(),
        }

        if self.config.get("include_payload"):
            # Add domain-specific data
            if context.domain == Domain.CLAIMX:
                entities = context.get_claimx_entities()
                if entities:
                    payload["tasks"] = entities.tasks

        return PluginResult.webhook(
            url=webhook_url,
            method="POST",
            body=payload,
            headers={"Content-Type": "application/json"}
        )
```

### Example 3: Metrics Plugin
```python
class MetricsPlugin(Plugin):
    """Emit custom metrics for events."""

    name = "custom_metrics"
    description = "Emit domain-specific metrics"
    version = "1.0.0"

    async def execute(self, context: PluginContext) -> PluginResult:
        actions = []

        # Emit event count metric
        actions.append(PluginAction(
            action_type=ActionType.METRIC,
            params={
                "name": "pipeline.event.processed",
                "labels": {
                    "domain": context.domain.value,
                    "stage": context.stage.value,
                    "event_type": context.event_type or "unknown",
                }
            }
        ))

        # Domain-specific metrics
        if context.domain == Domain.CLAIMX:
            tasks = context.get_tasks()
            if tasks:
                actions.append(PluginAction(
                    action_type=ActionType.METRIC,
                    params={
                        "name": "claimx.tasks.count",
                        "labels": {
                            "project_id": context.project_id,
                            "event_type": context.event_type,
                        }
                    }
                ))

        return PluginResult(success=True, actions=actions)
```

---

## Contact & Support

**Plugin System Lead:** [Add contact info]
**Documentation:** [Link to docs]
**Issues/Questions:** [Link to issue tracker]
