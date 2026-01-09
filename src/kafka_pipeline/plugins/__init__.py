"""
Plugin framework for injecting business logic into the pipeline.

Provides a lightweight mechanism for reacting to pipeline events
based on configurable conditions and executing actions.
"""

from kafka_pipeline.plugins.base import (
    Plugin,
    PluginContext,
    PluginResult,
    PluginAction,
    ActionType,
    Domain,
    PipelineStage,
)
from kafka_pipeline.plugins.registry import (
    PluginRegistry,
    get_plugin_registry,
    register_plugin,
)

__all__ = [
    "Plugin",
    "PluginContext",
    "PluginResult",
    "PluginAction",
    "ActionType",
    "Domain",
    "PipelineStage",
    "PluginRegistry",
    "get_plugin_registry",
    "register_plugin",
]
