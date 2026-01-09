"""
Plugin configuration loader.

Loads plugin configurations from YAML files and registers them.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from kafka_pipeline.plugins.base import Plugin, Domain, PipelineStage
from kafka_pipeline.plugins.registry import PluginRegistry, get_plugin_registry
from kafka_pipeline.plugins.task_trigger import TaskTriggerPlugin

logger = logging.getLogger(__name__)


def _log_with_context(
    logger: logging.Logger,
    level: int,
    msg: str,
    **kwargs,
) -> None:
    """Log with structured context."""
    if kwargs:
        extra_str = " ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
        logger.log(level, f"{msg} | {extra_str}")
    else:
        logger.log(level, msg)


log_with_context = _log_with_context


def load_plugins_from_yaml(
    config_path: str,
    registry: Optional[PluginRegistry] = None,
) -> List[Plugin]:
    """
    Load plugin configurations from YAML file.

    Args:
        config_path: Path to YAML configuration file
        registry: Registry to register plugins to (uses global if not provided)

    Returns:
        List of loaded plugins
    """
    registry = registry or get_plugin_registry()
    path = Path(config_path)

    if not path.exists():
        log_with_context(
            logger,
            logging.WARNING,
            "Plugin config file not found",
            config_path=config_path,
        )
        return []

    with open(path) as f:
        config = yaml.safe_load(f)

    plugins_loaded = []

    # Load task triggers
    task_triggers = config.get("task_triggers", [])
    for trigger_config in task_triggers:
        plugin = _create_task_trigger_from_config(trigger_config)
        if plugin:
            registry.register(plugin)
            plugins_loaded.append(plugin)

    log_with_context(
        logger,
        logging.INFO,
        "Loaded plugins from config",
        config_path=config_path,
        plugins_loaded=len(plugins_loaded),
        plugin_names=[p.name for p in plugins_loaded],
    )

    return plugins_loaded


def _create_task_trigger_from_config(config: Dict[str, Any]) -> Optional[TaskTriggerPlugin]:
    """
    Create TaskTriggerPlugin from YAML config.

    Expected format:
        name: my_task_trigger
        description: "Triggers on specific tasks"
        enabled: true
        triggers:
          - task_id: 456
            name: "Photo Documentation"
            on_assigned:
              publish_to_topic: task-456-assigned
            on_completed:
              publish_to_topic: task-456-completed
              webhook: https://api.example.com/notify
    """
    if not config.get("enabled", True):
        return None

    name = config.get("name", "task_trigger")
    description = config.get("description", "")

    # Convert triggers list to dict keyed by task_id
    triggers_dict = {}
    for trigger in config.get("triggers", []):
        task_id = trigger.get("task_id")
        if task_id is not None:
            triggers_dict[task_id] = {
                "name": trigger.get("name", f"Task {task_id}"),
                "on_assigned": trigger.get("on_assigned"),
                "on_completed": trigger.get("on_completed"),
                "on_any": trigger.get("on_any"),
            }

    plugin = TaskTriggerPlugin(config={
        "triggers": triggers_dict,
        "include_task_data": config.get("include_task_data", True),
        "include_project_data": config.get("include_project_data", False),
    })

    # Override metadata
    plugin.name = name
    plugin.description = description

    return plugin


def load_plugins_from_config(
    main_config: Dict[str, Any],
    registry: Optional[PluginRegistry] = None,
) -> List[Plugin]:
    """
    Load plugins from main application config dict.

    Looks for 'plugins' section in the config.

    Args:
        main_config: Main application configuration dict
        registry: Registry to use (global if not provided)

    Returns:
        List of loaded plugins
    """
    registry = registry or get_plugin_registry()
    plugins_config = main_config.get("plugins", {})

    if not plugins_config.get("enabled", True):
        log_with_context(
            logger,
            logging.INFO,
            "Plugins disabled in config",
        )
        return []

    plugins_loaded = []

    # Load from external YAML if specified
    yaml_path = plugins_config.get("config_path")
    if yaml_path:
        plugins_loaded.extend(load_plugins_from_yaml(yaml_path, registry))

    # Load inline task triggers
    task_triggers = plugins_config.get("task_triggers", [])
    for trigger_config in task_triggers:
        plugin = _create_task_trigger_from_config(trigger_config)
        if plugin:
            registry.register(plugin)
            plugins_loaded.append(plugin)

    return plugins_loaded
