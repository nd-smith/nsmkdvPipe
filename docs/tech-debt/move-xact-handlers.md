# Session: Move xact-specific handlers from common/ to xact/ domain

## Problem

The `kafka_pipeline/common/` package contains xact-specific code that violates domain boundaries:

- `common/dlq/handler.py` - DLQHandler imports xact schemas (FailedDownloadMessage, DownloadTaskMessage)
- `common/retry/handler.py` - RetryHandler imports xact schemas (FailedDownloadMessage, DownloadTaskMessage)

These handlers are only used by the xact domain but live in common/, creating:

1. Import coupling between common and xact packages
2. Confusion about what's truly shared vs domain-specific
3. Potential for claimx domain to accidentally depend on xact-specific code

## Task

Move the xact-specific handlers to their proper domain location:

### 1. Move DLQ Handler

- **FROM:** `src/kafka_pipeline/common/dlq/handler.py` (DLQHandler class)
- **TO:** `src/kafka_pipeline/xact/dlq/handler.py`

Update imports in:
- `src/kafka_pipeline/xact/workers/download_worker.py`
- `tests/kafka_pipeline/common/dlq/test_handler.py` → move to `tests/kafka_pipeline/xact/dlq/`
- `tests/kafka_pipeline/common/dlq/test_cli.py` → move to `tests/kafka_pipeline/xact/dlq/`
- `tests/kafka_pipeline/integration/test_dlq_flow.py`
- `tests/kafka_pipeline/integration/test_e2e_dlq_flow.py`

### 2. Move Retry Handler

- **FROM:** `src/kafka_pipeline/common/retry/handler.py` (RetryHandler class)
- **TO:** `src/kafka_pipeline/xact/retry/download_retry.py` (rename to avoid conflict with existing DeltaRetryHandler)

Update imports in:
- `src/kafka_pipeline/xact/workers/download_worker.py`
- `tests/kafka_pipeline/common/retry/test_handler.py` → move to `tests/kafka_pipeline/xact/retry/`
- `tests/kafka_pipeline/integration/test_retry_flow.py`
- `tests/kafka_pipeline/integration/test_dlq_flow.py`

### 3. Update common/ package

After moving, `common/dlq/` and `common/retry/` should only contain:

- Generic retry decorators (`common/retry/decorators.py`) - keep in common
- Delayed redelivery scheduler (`common/retry/scheduler.py`) - keep in common
- CLI tools may need adjustment or moving

### 4. Update documentation

- Update `src/README.md` which references `from kafka_pipeline.common.retry import RetryHandler`

## Verification

1. Run all tests: `pytest tests/kafka_pipeline/`
2. Verify no remaining imports of moved classes from common/
3. Ensure xact workers still function correctly

## Related Files

The tech debt warnings are documented inline in these files:
- `src/kafka_pipeline/common/dlq/handler.py` (lines 10-20)
- `src/kafka_pipeline/common/retry/handler.py` (lines 7-17)
