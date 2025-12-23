"""
This module contains xact-specific pipeline logic including:
"""

import os

os.environ["RUST_LOG"] = "error"
os.environ["POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY"] = "1"

# Models can be imported eagerly (no circular deps)
from verisk_pipeline.xact.xact_models import (
    Task,
    TaskStatus,
    StageStatus,
    StageResult,
    CycleResult,
    HealthStatus,
    CycleSummary,
    BatchResult,
    DownloadResult,
    EventRecord,
)


# Pipeline imported lazily to avoid circular import with api.health
def __getattr__(name):
    if name in ("Pipeline", "run_pipeline"):
        from verisk_pipeline.xact.xact_pipeline import Pipeline, run_pipeline

        return Pipeline if name == "Pipeline" else run_pipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Task",
    "TaskStatus",
    "StageStatus",
    "StageResult",
    "CycleResult",
    "HealthStatus",
    "CycleSummary",
    "BatchResult",
    "DownloadResult",
    "EventRecord",
]
