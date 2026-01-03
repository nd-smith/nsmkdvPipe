# REORG-501: verisk_pipeline Dependency Audit

**Date:** 2026-01-03
**Status:** Complete
**Dependencies Found:** 58 import statements across 16 files

---

## Executive Summary

This audit identifies all dependencies between `kafka_pipeline/` and `verisk_pipeline/` to plan the migration work for Phase 5 of the reorganization.

**Key Findings:**
- ✅ **One-way dependency:** kafka_pipeline → verisk_pipeline (no reverse imports)
- 📦 **16 files** in kafka_pipeline depend on verisk_pipeline
- 🔧 **5 major categories** of functionality to migrate
- 🎯 **All dependencies** are already flagged with TODO comments

**Dependency Direction:**
```
kafka_pipeline/ ──────→ verisk_pipeline/
                (58 imports)

verisk_pipeline/ ────X→ kafka_pipeline/
                (0 imports)
```

---

## Detailed Dependency Analysis

### Category 1: Storage Layer (REORG-502)

**Files Affected:** 2

#### 1.1 DeltaTableWriter
**Import:** `from verisk_pipeline.storage.delta import DeltaTableWriter`
- **Used In:**
  - `kafka_pipeline/common/writers/base.py:19`
- **Usage:** BaseDeltaWriter wraps DeltaTableWriter for async operations
- **Migration:** Move or replicate to `common/writers/base.py`
- **Complexity:** Medium (core storage abstraction)

#### 1.2 OneLakeClient
**Import:** `from verisk_pipeline.storage.onelake import OneLakeClient as LegacyOneLakeClient`
- **Used In:**
  - `kafka_pipeline/common/storage/onelake_client.py:14`
- **Usage:** Currently wraps legacy implementation
- **Migration:** Already has wrapper, needs full implementation
- **Complexity:** Medium (requires feature parity)

---

### Category 2: Xact Model Dependencies (REORG-503)

**Files Affected:** 4

#### 2.1 XACT_PRIMARY_KEYS
**Import:** `from verisk_pipeline.xact.xact_models import XACT_PRIMARY_KEYS`
- **Used In:**
  - `kafka_pipeline/xact/writers/delta_inventory.py:20`
- **Usage:** Defines merge keys for Delta table operations
- **Migration:** Move constant to `xact/schemas/` or `xact/writers/`
- **Complexity:** Low (simple constant)

#### 2.2 flatten_events()
**Import:** `from verisk_pipeline.xact.stages.transform import flatten_events`
- **Used In:**
  - `kafka_pipeline/xact/writers/delta_events.py:19`
  - References in documentation/comments:
    - `kafka_pipeline/xact/writers/delta_events.py:10, 26, 80, 101`
    - `kafka_pipeline/common/eventhouse/poller.py:772`
    - `kafka_pipeline/xact/workers/event_ingester.py:377, 524`
- **Usage:** Transforms Eventhouse rows to flattened Delta schema
- **Migration:** Move to `xact/schemas/` or `xact/utils/transform.py`
- **Complexity:** Medium (transformation logic, needs testing)

#### 2.3 EventRecord (conversion)
**Import:** `from verisk_pipeline.xact.xact_models import EventRecord`
- **Used In:**
  - `kafka_pipeline/xact/schemas/events.py:175` (in conversion methods)
- **Usage:** Type conversion for legacy compatibility
- **Migration Decision:**
  - **Option A:** Keep for backward compatibility layer
  - **Option B:** Remove conversion methods (breaking change)
- **Complexity:** Low-Medium (depends on decision)

#### 2.4 Task (conversion)
**Import:** `from verisk_pipeline.xact.xact_models import Task`
- **Used In:**
  - `kafka_pipeline/xact/schemas/tasks.py:150, 165` (in conversion methods)
- **Usage:** Type conversion for legacy compatibility
- **Migration Decision:**
  - **Option A:** Keep for backward compatibility layer
  - **Option B:** Remove conversion methods (breaking change)
- **Complexity:** Low-Medium (depends on decision)

---

### Category 3: ClaimX Model Dependencies

**Files Affected:** 3

#### 3.1 ClaimXEvent (conversion)
**Import:** `from verisk_pipeline.claimx.claimx_models import ClaimXEvent`
- **Used In:**
  - `kafka_pipeline/claimx/schemas/events.py:122, 138`
- **Usage:** Type conversion for legacy compatibility
- **Migration Decision:**
  - **Option A:** Keep for backward compatibility layer
  - **Option B:** Remove conversion methods (breaking change)
- **Complexity:** Low

#### 3.2 MediaTask (conversion)
**Import:** `from verisk_pipeline.claimx.claimx_models import MediaTask`
- **Used In:**
  - `kafka_pipeline/claimx/schemas/tasks.py:234, 251`
- **Usage:** Type conversion for legacy compatibility
- **Migration Decision:**
  - **Option A:** Keep for backward compatibility layer
  - **Option B:** Remove conversion methods (breaking change)
- **Complexity:** Low

#### 3.3 EntityRows (conversion)
**Import:** `from verisk_pipeline.claimx.claimx_models import EntityRows`
- **Used In:**
  - `kafka_pipeline/claimx/schemas/entities.py:150, 164`
- **Usage:** Type conversion for legacy compatibility
- **Migration Decision:**
  - **Option A:** Keep for backward compatibility layer
  - **Option B:** Remove conversion methods (breaking change)
- **Complexity:** Low

---

### Category 4: Common Utilities (REORG-504)

**Files Affected:** 8

#### 4.1 Circuit Breaker
**Import:** `from verisk_pipeline.common.circuit_breaker import CircuitBreaker, CircuitBreakerState`
- **Used In:**
  - `kafka_pipeline/claimx/api_client.py:16-19`
- **Usage:** Circuit breaker pattern for API resilience
- **Migration:** Move to `common/resilience/circuit_breaker.py`
- **Complexity:** Medium (stateful pattern, needs testing)
- **Note:** Already has TODO comment in code

#### 4.2 ErrorCategory
**Import:** `from verisk_pipeline.common.exceptions import ErrorCategory`
- **Used In:**
  - `kafka_pipeline/claimx/api_client.py:20`
  - `kafka_pipeline/claimx/handlers/base.py:19`
  - `kafka_pipeline/claimx/handlers/project.py:29`
  - `kafka_pipeline/claimx/handlers/task.py:30`
  - `kafka_pipeline/claimx/handlers/media.py:28`
- **Usage:** Error classification (transient vs permanent)
- **Migration:** Move to `common/exceptions.py`
- **Complexity:** Low (enum/constant)

#### 4.3 Logging Infrastructure
**Imports:**
- `from verisk_pipeline.common.logging.setup import get_logger`
- `from verisk_pipeline.common.logging.decorators import logged_operation, LoggedClass, extract_log_context`
- `from verisk_pipeline.common.logging.utilities import log_exception, log_with_context`

**Used In:**
- `kafka_pipeline/claimx/api_client.py:21-22`
- `kafka_pipeline/claimx/handlers/base.py:20-22`
- `kafka_pipeline/claimx/handlers/project.py:30-32`
- `kafka_pipeline/claimx/handlers/video.py:29-30`
- `kafka_pipeline/claimx/handlers/contact.py:18-19`
- `kafka_pipeline/claimx/handlers/task.py:31-32`
- `kafka_pipeline/claimx/handlers/media.py:29-31`

**Usage:** Structured logging with decorators and context
**Migration:** Move to `common/logging/`
**Complexity:** Medium (multiple modules, decorators, context management)
**Note:** All have TODO comments in code

#### 4.4 Security Utilities
**Import:** `from verisk_pipeline.common.security import sanitize_url`
- **Used In:**
  - `kafka_pipeline/xact/workers/event_ingester.py:40`
- **Usage:** URL sanitization for logging
- **Migration:** Move to `common/security.py` or `common/utils.py`
- **Complexity:** Low (simple utility function)

#### 4.5 URL Expiration Utilities
**Import:** `from verisk_pipeline.common.url_expiration import extract_expires_at_iso`
- **Used In:**
  - `kafka_pipeline/claimx/handlers/media.py:32`
- **Usage:** Extract expiration time from presigned URLs
- **Migration:** Move to `common/utils.py` or `claimx/utils.py`
- **Complexity:** Low (simple utility function)

---

## Migration Checklist

### Phase 5.1: Storage Layer (REORG-502)

- [ ] **DeltaTableWriter Migration**
  - [ ] Review verisk_pipeline.storage.delta.DeltaTableWriter implementation
  - [ ] Identify all features used by BaseDeltaWriter
  - [ ] Move/replicate to common/writers/base.py (or create delta_writer.py)
  - [ ] Update common/writers/base.py imports
  - [ ] Add unit tests for migrated functionality
  - [ ] Verify all writer tests pass

- [ ] **OneLakeClient Migration**
  - [ ] Review verisk_pipeline.storage.onelake.OneLakeClient implementation
  - [ ] Identify all features used by kafka_pipeline
  - [ ] Implement full OneLakeClient in common/storage/onelake_client.py
  - [ ] Remove LegacyOneLakeClient wrapper
  - [ ] Add unit tests for migrated functionality
  - [ ] Verify all storage tests pass

**Estimated Effort:** Medium (2-3 days)

---

### Phase 5.2: Xact Model Dependencies (REORG-503)

- [ ] **XACT_PRIMARY_KEYS Migration**
  - [ ] Move XACT_PRIMARY_KEYS to xact/schemas/constants.py
  - [ ] Update xact/writers/delta_inventory.py import
  - [ ] Verify writer tests pass

- [ ] **flatten_events() Migration**
  - [ ] Review verisk_pipeline.xact.stages.transform.flatten_events implementation
  - [ ] Move to xact/utils/transform.py or xact/schemas/transform.py
  - [ ] Update imports in:
    - xact/writers/delta_events.py
    - (common/eventhouse/poller.py - if xact-specific, see note below)
  - [ ] Update documentation references
  - [ ] Add unit tests for flatten_events
  - [ ] Verify all delta_events tests pass

- [ ] **Legacy Type Conversions Decision**
  - [ ] **DECISION NEEDED:** Keep or remove conversion methods?
  - [ ] If keeping:
    - [ ] Move EventRecord, Task to xact/schemas/legacy.py
    - [ ] Keep to_legacy() / from_legacy() methods
    - [ ] Document as deprecated
  - [ ] If removing:
    - [ ] Remove all to_legacy() / from_legacy() methods from:
      - xact/schemas/events.py
      - xact/schemas/tasks.py
    - [ ] Update any code that depends on conversions
    - [ ] Add migration guide for external users

**Estimated Effort:** Large (3-5 days)

**IMPORTANT NOTE - Architectural Issue:**
The following modules in common/ import from xact/, violating domain-agnostic principle:
- `common/eventhouse/poller.py` - imports xact.schemas.events, xact.writers.delta_events
- `common/dlq/` - may import xact schemas
- `common/retry/` - may import xact schemas

**Resolution Options:**
1. **Move to xact/** - If these modules are xact-specific, move them to xact/
2. **Make Generic** - Refactor with type parameters to support multiple domains
3. **Duplicate** - Create domain-specific versions (xact/eventhouse/poller.py, claimx/eventhouse/poller.py)

This should be addressed as part of REORG-503 or as a separate work package.

---

### Phase 5.3: ClaimX Model Dependencies

- [ ] **Legacy Type Conversions Decision**
  - [ ] **DECISION NEEDED:** Keep or remove conversion methods?
  - [ ] If keeping:
    - [ ] Move ClaimXEvent, MediaTask, EntityRows to claimx/schemas/legacy.py
    - [ ] Keep to_legacy() / from_legacy() methods
    - [ ] Document as deprecated
  - [ ] If removing:
    - [ ] Remove all to_legacy() / from_legacy() methods from:
      - claimx/schemas/events.py
      - claimx/schemas/tasks.py
      - claimx/schemas/entities.py
    - [ ] Update any code that depends on conversions
    - [ ] Add migration guide for external users

**Estimated Effort:** Small (1 day)

---

### Phase 5.4: Common Utilities (REORG-504)

- [ ] **Circuit Breaker Migration**
  - [ ] Review verisk_pipeline.common.circuit_breaker implementation
  - [ ] Move to common/resilience/circuit_breaker.py (create directory)
  - [ ] Update claimx/api_client.py import
  - [ ] Add unit tests for circuit breaker
  - [ ] Verify API client tests pass

- [ ] **ErrorCategory Migration**
  - [ ] Review verisk_pipeline.common.exceptions.ErrorCategory
  - [ ] Move to common/exceptions.py
  - [ ] Update imports in:
    - claimx/api_client.py
    - claimx/handlers/base.py
    - claimx/handlers/project.py
    - claimx/handlers/task.py
    - claimx/handlers/media.py
  - [ ] Verify all handler tests pass

- [ ] **Logging Infrastructure Migration**
  - [ ] Review verisk_pipeline.common.logging modules
  - [ ] Create common/logging/ directory structure
  - [ ] Move setup.py (get_logger)
  - [ ] Move decorators.py (logged_operation, LoggedClass, extract_log_context)
  - [ ] Move utilities.py (log_exception, log_with_context)
  - [ ] Update imports in:
    - claimx/api_client.py
    - claimx/handlers/*.py (6 files)
  - [ ] Add unit tests for logging utilities
  - [ ] Verify all tests pass

- [ ] **Security Utilities Migration**
  - [ ] Review verisk_pipeline.common.security.sanitize_url
  - [ ] Move to common/security.py or common/utils.py
  - [ ] Update xact/workers/event_ingester.py import
  - [ ] Add unit test for sanitize_url
  - [ ] Verify worker tests pass

- [ ] **URL Utilities Migration**
  - [ ] Review verisk_pipeline.common.url_expiration.extract_expires_at_iso
  - [ ] Move to common/utils.py or claimx/utils.py
  - [ ] Update claimx/handlers/media.py import
  - [ ] Add unit test for extract_expires_at_iso
  - [ ] Verify handler tests pass

**Estimated Effort:** Medium (2-4 days)

---

### Phase 5.5: Final Cleanup (REORG-505)

- [ ] **Verify No Remaining Dependencies**
  - [ ] Run: `grep -r "from verisk_pipeline" kafka_pipeline/`
  - [ ] Run: `grep -r "import verisk_pipeline" kafka_pipeline/`
  - [ ] Verify zero results

- [ ] **Remove verisk_pipeline**
  - [ ] Delete src/verisk_pipeline/ directory
  - [ ] Remove verisk_pipeline references from:
    - requirements.txt (if duplicated)
    - setup.py / pyproject.toml
    - Documentation files

- [ ] **Update Documentation**
  - [ ] Remove verisk_pipeline references from all docs
  - [ ] Update architecture diagrams
  - [ ] Update migration guides

**Estimated Effort:** Small (1 day)

---

### Phase 5.6: Validation (REORG-506)

- [ ] **Test Suite Validation**
  - [ ] Run full unit test suite (aim for >95% pass rate)
  - [ ] Run integration tests
  - [ ] Run performance tests
  - [ ] Fix any test failures

- [ ] **Manual Testing**
  - [ ] Test xact pipeline end-to-end
  - [ ] Test claimx pipeline end-to-end
  - [ ] Test all worker commands start successfully
  - [ ] Test DLQ and retry flows

- [ ] **Code Review**
  - [ ] Review all migrated code
  - [ ] Verify no code duplication
  - [ ] Verify consistent patterns
  - [ ] Document any breaking changes

**Estimated Effort:** Large (3-5 days)

---

## Summary of Effort Estimates

| Work Package | Size | Estimated Effort |
|--------------|------|------------------|
| REORG-502: Storage Layer | Medium | 2-3 days |
| REORG-503: Xact Models | Large | 3-5 days |
| REORG-503.1: ClaimX Models | Small | 1 day |
| REORG-504: Common Utilities | Medium | 2-4 days |
| REORG-505: Final Cleanup | Small | 1 day |
| REORG-506: Validation | Large | 3-5 days |
| **Total** | **Large** | **12-19 days** |

---

## Recommendations

### 1. Migration Order
Follow the dependency order:
1. **REORG-504 first** - Common utilities (logging, exceptions) have no dependencies
2. **REORG-502 next** - Storage layer needed by writers
3. **REORG-503 next** - Xact models (depends on storage)
4. **ClaimX models** - Can be done in parallel with xact
5. **REORG-505, REORG-506** - Cleanup and validation

### 2. Legacy Conversion Decision
**Recommendation:** Remove conversion methods (Option B)
- **Rationale:**
  - Simpler codebase
  - kafka_pipeline is new pipeline, no legacy compatibility needed
  - verisk_pipeline will be deleted anyway
  - Keeps domains fully independent
- **Impact:** Breaking change if any code depends on conversions (audit first)

### 3. Architectural Issue Resolution
**Recommendation:** Move poller, dlq, retry to xact/ domain
- **Rationale:**
  - These modules import xact schemas, so they're xact-specific
  - common/ should be truly domain-agnostic
  - ClaimX can implement its own versions if needed
- **Impact:** Large refactor, but cleaner architecture

### 4. Testing Strategy
- Migrate one category at a time
- Run tests after each migration
- Keep verisk_pipeline until all migrations complete
- Use feature flags if needed for gradual rollout

---

## Files Requiring Changes

### Direct Import Changes (16 files)
1. `kafka_pipeline/common/writers/base.py`
2. `kafka_pipeline/common/storage/onelake_client.py`
3. `kafka_pipeline/xact/writers/delta_inventory.py`
4. `kafka_pipeline/xact/writers/delta_events.py`
5. `kafka_pipeline/xact/workers/event_ingester.py`
6. `kafka_pipeline/xact/schemas/events.py`
7. `kafka_pipeline/xact/schemas/tasks.py`
8. `kafka_pipeline/claimx/api_client.py`
9. `kafka_pipeline/claimx/schemas/events.py`
10. `kafka_pipeline/claimx/schemas/tasks.py`
11. `kafka_pipeline/claimx/schemas/entities.py`
12. `kafka_pipeline/claimx/handlers/base.py`
13. `kafka_pipeline/claimx/handlers/project.py`
14. `kafka_pipeline/claimx/handlers/video.py`
15. `kafka_pipeline/claimx/handlers/contact.py`
16. `kafka_pipeline/claimx/handlers/task.py`
17. `kafka_pipeline/claimx/handlers/media.py`

### Indirect References (documentation/comments)
- `kafka_pipeline/common/eventhouse/poller.py`
- `kafka_pipeline/claimx/writers/delta_entities.py` (comment only)
- `kafka_pipeline/claimx/schemas/__init__.py` (docstring only)

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Breaking existing integrations | Medium | High | Thorough testing, feature flags |
| Missing dependencies | Low | High | Complete audit (this document) |
| Test failures during migration | High | Medium | Incremental migration, continuous testing |
| Performance regression | Low | Medium | Performance test suite |
| Architectural issues in common/ | High | High | Address in REORG-503 (see recommendations) |

---

## Next Steps

1. ✅ **REORG-501 Complete** - This audit document
2. ⏭️  **Decision Point** - Review recommendations with team
   - Decide on legacy conversion methods (keep vs remove)
   - Decide on common/ architectural issue (move to xact/ vs generic vs duplicate)
3. ⏭️  **REORG-504** - Start with common utilities (no dependencies)
4. ⏭️  **Continue Phase 5** - Follow migration checklist above

---

**Audit Completed:** 2026-01-03
**Next Review:** After each work package completion
