# WIP: Plugin-Based Routing Architecture

> **Status**: Research / Design Phase
> **Created**: 2026-01-02
> **Goal**: Introduce business logic into results processing via a plugin system that scales across domains (XACT, ClaimX, future integrations)

---

## Problem Statement

We need a way to route results to multiple queues based on business rules. For example:
- A certain KQL status triggers routing to more than one queue
- ESX files get special processing
- High-retry failures trigger alerts

The solution must scale to future domains (ClaimX, etc.) without major rework.

---

## Current Architecture

### Results Processing Flow
```
Kafka Topic: xact.downloads.results
    │
    ▼
ResultProcessor._handle_result()
    │
    ▼
Status-based routing (hardcoded):
    ├─→ "completed" → Success batch → Delta table (xact_attachments)
    ├─→ "failed_permanent" → Failed batch → Delta table (xact_attachments_failed)
    └─→ "failed" (transient) → Skip (still retrying)
```

### Key Files
| File | Purpose |
|------|---------|
| `kafka_pipeline/workers/result_processor.py` | Main processor, routing logic (lines 218-288) |
| `kafka_pipeline/schemas/results.py` | `DownloadResultMessage` schema |
| `kafka_pipeline/writers/delta_inventory.py` | Delta table writers |

### Current Limitations
- Routing logic is **hardcoded** in `_handle_result()`
- No hooks for custom routing based on arbitrary fields
- No support for sending to multiple destinations
- No middleware/interceptor pattern

---

## Proposed Design: Plugin Architecture

### Directory Structure
```
src/kafka_pipeline/plugins/
├── __init__.py
├── base.py              # Generic RoutingPlugin[T], lifecycle hooks
├── protocols.py         # Routable, DownloadRoutable, EventRoutable
├── registry.py          # Generic PluginRegistry[T]
│
├── xact/                # XACT-specific plugins
│   ├── __init__.py      # XactRoutingPlugin base
│   ├── esx_processor.py
│   ├── failure_alerter.py
│   └── status_router.py
│
├── claimx/              # ClaimX-specific plugins (future)
│   ├── __init__.py      # ClaimXRoutingPlugin base
│   ├── media_processor.py
│   └── project_notifier.py
│
└── shared/              # Cross-domain plugins (use protocols)
    └── generic_alerter.py
```

### Core Components

#### 1. Generic Plugin Base Class
```python
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Any
from aiokafka import AIOKafkaProducer

T = TypeVar("T")

class RoutingPlugin(ABC, Generic[T]):
    """
    Base class for routing plugins.

    Type parameter T is the message type this plugin handles.
    """

    name: str
    domain: str  # "xact", "claimx", etc.

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._producer: AIOKafkaProducer | None = None

    def set_producer(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def start(self) -> None:
        """Lifecycle hook: called on startup."""
        pass

    async def stop(self) -> None:
        """Lifecycle hook: called on shutdown."""
        pass

    @abstractmethod
    def should_route(self, message: T) -> bool:
        """Determine if this plugin handles the message."""
        ...

    @abstractmethod
    async def route(self, message: T) -> None:
        """Route the message to additional destination(s)."""
        ...

    @abstractmethod
    def get_trace_id(self, message: T) -> str:
        """Extract trace/correlation ID for logging."""
        ...

    async def send_to_topic(
        self,
        topic: str,
        payload: dict[str, Any],
        key: str,
    ) -> None:
        """Send payload to Kafka topic."""
        if not self._producer:
            raise RuntimeError(f"Plugin {self.name}: No producer configured")
        await self._producer.send_and_wait(
            topic,
            value=json.dumps(payload, default=str).encode(),
            key=key.encode(),
        )
```

#### 2. Generic Plugin Registry
```python
from typing import TypeVar, Generic

T = TypeVar("T")

class PluginRegistry(Generic[T]):
    """
    Type-safe plugin registry.

    Usage:
        xact_registry: PluginRegistry[DownloadResultMessage] = PluginRegistry("xact")
        xact_registry.register(EsxProcessorPlugin())
    """

    def __init__(self, domain: str = "default"):
        self.domain = domain
        self._plugins: dict[str, RoutingPlugin[T]] = {}
        self._started = False

    def register(self, plugin: RoutingPlugin[T]) -> "PluginRegistry[T]":
        if plugin.name in self._plugins:
            raise ValueError(f"Plugin '{plugin.name}' already registered")
        self._plugins[plugin.name] = plugin
        return self

    async def start(self, producer: AIOKafkaProducer) -> None:
        for plugin in self._plugins.values():
            plugin.set_producer(producer)
            await plugin.start()
        self._started = True

    async def stop(self) -> None:
        for plugin in self._plugins.values():
            await plugin.stop()
        self._started = False

    async def route(self, message: T) -> list[str]:
        """Route message through all matching plugins."""
        handled_by = []
        for plugin in self._plugins.values():
            try:
                if plugin.should_route(message):
                    await plugin.route(message)
                    handled_by.append(plugin.name)
            except Exception as e:
                # Log error but continue with other plugins
                logger.error(f"Plugin '{plugin.name}' failed", exc_info=True)
        return handled_by
```

#### 3. Domain-Specific Base Classes

**XACT:**
```python
from kafka_pipeline.plugins.base import RoutingPlugin
from kafka_pipeline.schemas.results import DownloadResultMessage

class XactRoutingPlugin(RoutingPlugin[DownloadResultMessage]):
    """Base class for XACT download result routing plugins."""

    domain = "xact"

    def get_trace_id(self, message: DownloadResultMessage) -> str:
        return message.trace_id
```

**ClaimX (future):**
```python
from kafka_pipeline.plugins.base import RoutingPlugin
from verisk_pipeline.claimx.claimx_models import EnrichmentResult

class ClaimXEnrichmentPlugin(RoutingPlugin[EnrichmentResult]):
    """Base class for ClaimX enrichment result routing plugins."""

    domain = "claimx"

    def get_trace_id(self, message: EnrichmentResult) -> str:
        return message.event.event_id
```

#### 4. Example Plugins

**ESX Processor:**
```python
class EsxProcessorPlugin(XactRoutingPlugin):
    """Route completed ESX files to specialized processing queue."""

    name = "esx-processor"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.target_topic = self.config.get("topic", "xact.esx.processing")

    def should_route(self, result: DownloadResultMessage) -> bool:
        return result.file_type == "esx" and result.status == "completed"

    async def route(self, result: DownloadResultMessage) -> None:
        await self.send_to_topic(
            self.target_topic,
            result.model_dump(),
            result.trace_id
        )
```

**Failure Alerter:**
```python
class FailureAlerterPlugin(XactRoutingPlugin):
    """Route high-retry permanent failures to alerting system."""

    name = "failure-alerter"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.target_topic = self.config.get("topic", "xact.alerts.failures")
        self.min_retries = self.config.get("min_retries", 3)

    def should_route(self, result: DownloadResultMessage) -> bool:
        return (
            result.status == "failed_permanent"
            and result.retry_count >= self.min_retries
        )

    async def route(self, result: DownloadResultMessage) -> None:
        alert_payload = {
            "alert_type": "download_failure",
            "severity": "high" if result.retry_count >= 5 else "medium",
            "trace_id": result.trace_id,
            "assignment_id": result.assignment_id,
            "error_message": result.error_message,
            "retry_count": result.retry_count,
            "http_status": result.http_status,
        }
        await self.send_to_topic(self.target_topic, alert_payload, result.trace_id)
```

**Status Subtype Router:**
```python
class StatusSubtypeRouterPlugin(XactRoutingPlugin):
    """Route results to topics based on status_subtype."""

    name = "status-subtype-router"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.routes: dict[str, str] = self.config.get("routes", {
            "estimateCreated": "xact.estimates.received",
            "documentsReceived": "xact.documents.received",
        })

    def should_route(self, result: DownloadResultMessage) -> bool:
        return (
            result.status == "completed"
            and result.status_subtype in self.routes
        )

    async def route(self, result: DownloadResultMessage) -> None:
        topic = self.routes[result.status_subtype]
        await self.send_to_topic(topic, result.model_dump(), result.trace_id)
```

#### 5. Integration with ResultProcessor

```python
class ResultProcessor:
    def __init__(
        self,
        *args,
        plugin_registry: PluginRegistry[DownloadResultMessage] | None = None,
        **kwargs
    ):
        # ... existing init ...
        self._plugin_registry = plugin_registry
        self._routing_producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        # ... existing start logic ...

        if self._plugin_registry:
            self._routing_producer = AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers,
            )
            await self._routing_producer.start()
            await self._plugin_registry.start(self._routing_producer)

    async def _handle_result(self, message: ConsumerRecord) -> None:
        result = DownloadResultMessage.model_validate_json(message.value)

        # Plugin routing (before standard processing)
        if self._plugin_registry:
            handled_by = await self._plugin_registry.route(result)
            if handled_by:
                logger.debug(
                    "Result routed by plugins",
                    extra={"trace_id": result.trace_id, "plugins": handled_by}
                )

        # Continue with standard processing (Delta writes)
        # ... existing logic ...
```

---

## Available Routing Fields

### XACT DownloadResultMessage
| Field | Type | Example Condition |
|-------|------|-------------------|
| `status` | `"completed" \| "failed" \| "failed_permanent"` | `r.status == "completed"` |
| `status_subtype` | `str` | `r.status_subtype == "estimateCreated"` |
| `file_type` | `str` | `r.file_type in ["esx", "pdf"]` |
| `http_status` | `int \| None` | `r.http_status == 403` |
| `retry_count` | `int` | `r.retry_count >= 3` |
| `bytes_downloaded` | `int` | `r.bytes_downloaded > 10_000_000` |
| `error_message` | `str \| None` | `"timeout" in (r.error_message or "")` |
| `assignment_id` | `str` | `r.assignment_id.startswith("A")` |

### ClaimX EnrichmentResult (future)
| Field | Type | Example Condition |
|-------|------|-------------------|
| `event.event_type` | `str` | `r.event.event_type == "media.created"` |
| `event.project_id` | `str` | Project-based routing |
| `success` | `bool` | `not r.success` |
| `error_category` | `ErrorCategory` | `r.error_category == ErrorCategory.AUTH` |
| `is_retryable` | `bool` | `not r.is_retryable` |

---

## Cross-Domain Support (Protocols)

For plugins that work across domains:

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Routable(Protocol):
    """Any message that can be routed."""

    @property
    def trace_id(self) -> str: ...

    @property
    def status(self) -> str: ...
```

---

## Future Enhancements

1. **Config-driven registration** - Load plugins from YAML/JSON config
2. **Plugin priority/ordering** - Control execution order
3. **Auto-discovery** - Scan `plugins/` directory for plugin classes
4. **Per-plugin metrics** - Track routing counts, latencies, errors
5. **Circuit breaker per plugin** - Disable failing plugins temporarily
6. **Hot reload** - Update plugin config without restart

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Generics over inheritance | Type safety, IDE support, cleaner APIs |
| Domain-specific base classes | Encapsulate domain knowledge, simplify plugin authoring |
| Registry per domain | Isolation, separate lifecycle management |
| Plugins run before Delta writes | Routing shouldn't block core persistence |
| Error isolation | One plugin failure doesn't stop others |

---

## Open Questions

- [ ] Should plugins be able to modify/transform messages before Delta write?
- [ ] How to handle plugin ordering when order matters?
- [ ] Config file format for plugin registration (YAML? Python?)
- [ ] Metrics/observability strategy per plugin
- [ ] Testing strategy for plugin integration tests

---

## References

- `src/kafka_pipeline/workers/result_processor.py` - Current implementation
- `src/kafka_pipeline/schemas/results.py` - DownloadResultMessage schema
- `src/verisk_pipeline/claimx/claimx_models.py` - ClaimX domain models
