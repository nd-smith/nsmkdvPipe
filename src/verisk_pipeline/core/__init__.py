"""
Core pipeline components.

Provides shared base classes and abstractions for pipelines and stages.
"""

from verisk_pipeline.core.base_pipeline import BasePipeline
from verisk_pipeline.core.base_stage import BaseStage

__all__ = [
    "BasePipeline",
    "BaseStage",
]
