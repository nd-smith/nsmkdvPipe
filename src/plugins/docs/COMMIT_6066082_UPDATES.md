# Updates for Commit 6066082 - Logging Infrastructure

**Commit:** 6066082 - Add logging infrastructure to Plugin base class
**Date:** 2026-01-10
**Status:** ✅ Documentation and Examples Updated

---

## What Changed in Commit 6066082

### Core Changes

1. **Plugin Base Class** (`kafka_pipeline/plugins/base.py`)
   - Now inherits from `LoggedClass`
   - Provides automatic logging infrastructure to all plugins

2. **Logging Methods Available**
   - `self._logger` - Pre-configured logger instance
   - `self._log(level, msg, **extra)` - Structured logging with context
   - `self._log_exception(exc, msg, **extra)` - Exception logging with traceback

3. **ActionExecutor Updates** (`kafka_pipeline/plugins/registry.py`)
   - Now supports `connection_manager` parameter
   - Webhooks can use named connections (recommended)
   - Legacy direct URL support maintained

4. **Code Cleanup**
   - Removed duplicate `_log_with_context` functions
   - Consolidated to `core.logging.log_with_context`
   - TaskTriggerPlugin updated to use `self._log()`

---

## Files Updated in Response

### 1. iTel Cabinet Plugin Documentation ✅

**File:** `config/plugins/itel_cabinet_api/README.md`

**Changes:**
- Added "Extending the Plugin" section
- Documented LoggedClass capabilities
- Provided code example using `self._log()` and `self._log_exception()`
- Referenced custom plugin example file

**Impact:** Plugin developers now know how to use built-in logging

---

### 2. Custom Plugin Example ✅

**File:** `plugins/docs/examples/custom_plugin_example.py`

**Created:** Complete example file demonstrating:
- **DataQualityPlugin** - Using structured logging in validation
- **EnrichmentMetricsPlugin** - Logging with metrics and thresholds
- **ErrorRecoveryPlugin** - Exception handling with `_log_exception()`

**Features Demonstrated:**
- Structured logging with context (`self._log(level, msg, **extra)`)
- Exception logging (`self._log_exception(exc, msg, **extra)`)
- Appropriate log level usage (DEBUG, INFO, WARNING, ERROR)
- Best practices documentation

**Impact:** Developers have working examples to copy/adapt

---

### 3. Example YAML Files ✅

**File:** `plugins/docs/examples/task_trigger_with_connections.example.yaml`

**Changes:**
- Added header comment documenting commit 6066082 changes
- Highlighted new ActionExecutor connection_manager support
- Listed benefits of named connections

**Impact:** Users understand the improved webhook capabilities

---

### 4. Plugin Connections Guide ✅

**File:** `PLUGIN_CONNECTIONS_GUIDE.md`

**Changes:**
- Added "Built-in Logging Infrastructure" section (new TOC entry)
- Comprehensive logging documentation with examples
- Table of available logging methods
- Best practices guide
- Log level usage examples
- Reference to custom plugin examples

**Sections Added:**
- Features overview
- Basic usage example
- Available methods table
- Log levels guide
- Best practices (4 key practices)
- Complete custom plugin example reference

**Impact:** Complete reference documentation for logging infrastructure

---

## Quick Reference: Using the New Logging

### In Custom Plugins

```python
from kafka_pipeline.plugins.base import Plugin, PluginContext, PluginResult
import logging

class MyPlugin(Plugin):
    name = "my_plugin"

    def execute(self, context: PluginContext) -> PluginResult:
        # Use structured logging
        self._log(
            logging.INFO,
            "Processing event",
            event_id=context.event_id,
            custom_field=value
        )

        try:
            # Your logic
            result = do_work()

            self._log(
                logging.DEBUG,
                "Work completed",
                event_id=context.event_id,
                records=result.count
            )

            return PluginResult.ok()

        except Exception as e:
            # Exception logging with context
            self._log_exception(
                e,
                "Work failed",
                event_id=context.event_id
            )
            return PluginResult.failed(str(e))
```

### Best Practices

1. ✅ **Always include event_id** for correlation
2. ✅ **Use structured context** (not f-strings)
3. ✅ **Use `_log_exception`** for all exceptions
4. ✅ **Log operation results** with metrics

### Before vs After

**Before commit 6066082:**
```python
import logging
logger = logging.getLogger(__name__)

def _log_with_context(logger, level, msg, **kwargs):
    # Custom implementation...
    pass

class MyPlugin(Plugin):
    def execute(self, context):
        logger.info(f"Processing {context.event_id}")  # ❌ String formatting
        # Manual logging setup required
```

**After commit 6066082:**
```python
# No imports needed - LoggedClass provides everything

class MyPlugin(Plugin):
    def execute(self, context):
        self._log(logging.INFO, "Processing", event_id=context.event_id)  # ✅ Structured
        # Logging automatically available
```

---

## Migration Guide

### For Existing Custom Plugins

**If you have custom plugins using manual logging:**

1. Remove manual logger setup:
   ```python
   # Remove this:
   logger = logging.getLogger(__name__)
   ```

2. Replace `logger.info/error/etc` with `self._log`:
   ```python
   # Before:
   logger.info(f"Processing event {event_id}")

   # After:
   self._log(logging.INFO, "Processing event", event_id=event_id)
   ```

3. Replace exception logging with `self._log_exception`:
   ```python
   # Before:
   except Exception as e:
       logger.exception(f"Failed: {e}")

   # After:
   except Exception as e:
       self._log_exception(e, "Failed", event_id=context.event_id)
   ```

**No changes needed for:**
- TaskTriggerPlugin configurations (already updated)
- Plugin YAML config files
- Webhook configurations

---

## Testing the Updates

### 1. Verify Custom Plugin Logging

```python
# Create test plugin with logging
class TestPlugin(Plugin):
    name = "test"
    def execute(self, context):
        self._log(logging.INFO, "Test log", event_id=context.event_id)
        return PluginResult.ok()
```

Expected log output:
```
INFO: Test log | event_id=abc123
```

### 2. Verify Exception Logging

```python
try:
    raise ValueError("Test error")
except Exception as e:
    self._log_exception(e, "Test exception", event_id="test123")
```

Expected log output:
```
ERROR: Test exception | event_id=test123
Traceback (most recent call last):
  ...
ValueError: Test error
```

### 3. Verify Named Connection Webhooks

With ActionExecutor configured with connection_manager, webhooks using named connections should work:

```yaml
webhook:
  connection: claimx_api  # Named connection
  path: /api/test
  method: POST
```

---

## Documentation Index

| File | Purpose | Status |
|------|---------|--------|
| `config/plugins/itel_cabinet_api/README.md` | iTel plugin docs with logging section | ✅ Updated |
| `plugins/docs/examples/custom_plugin_example.py` | Complete working examples | ✅ Created |
| `plugins/docs/examples/task_trigger_with_connections.example.yaml` | Named connection examples | ✅ Updated |
| `PLUGIN_CONNECTIONS_GUIDE.md` | Complete reference guide | ✅ Updated |
| `plugins/docs/COMMIT_6066082_UPDATES.md` | This summary | ✅ Created |

---

## Questions & Support

### Where to find examples?
- **Basic usage:** `PLUGIN_CONNECTIONS_GUIDE.md` - Built-in Logging Infrastructure section
- **Complete plugins:** `plugins/docs/examples/custom_plugin_example.py`
- **Best practices:** This file, section above

### How do I use this in my plugin?
Just extend `Plugin` base class - logging is automatic:
```python
class MyPlugin(Plugin):
    def execute(self, context):
        self._log(logging.INFO, "Message", event_id=context.event_id)
```

### Do I need to change existing plugins?
**No** - existing plugins continue to work. The logging infrastructure is available when you need it.

### What about TaskTriggerPlugin?
Already updated in commit 6066082 - uses `self._log()` internally.

---

## Summary

✅ **All documentation updated**
✅ **Example files created**
✅ **Best practices documented**
✅ **Migration guide provided**
✅ **Testing guidance included**

**Impact:** Plugin developers can now use consistent, structured logging across all plugins with zero configuration.

**Breaking Changes:** None - fully backwards compatible

**Recommended Action:** Update custom plugins to use new logging infrastructure for better observability and consistency.

---

**Last Updated:** 2026-01-10
**Reviewed By:** Development Team
**Status:** Ready for Use
