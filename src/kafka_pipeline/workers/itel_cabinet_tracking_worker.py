"""
iTel Cabinet Tracking Worker

Standalone worker that consumes iTel cabinet task events from Kafka,
enriches them with ClaimX API data, and writes to Delta tables.

Usage:
    python -m kafka_pipeline.workers.itel_cabinet_tracking_worker

Configuration:
    - config/plugins/itel_cabinet_api/workers.yaml (worker settings)
    - config/plugins/shared/connections/claimx.yaml (API connections)

Environment Variables Required:
    - CLAIMX_API_BASE_URL: ClaimX API base URL
    - CLAIMX_API_TOKEN: Bearer token for ClaimX API
    - KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
"""

import asyncio
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from kafka_pipeline.plugins.connections import (
    AuthType,
    ConnectionConfig,
    ConnectionManager,
)
from kafka_pipeline.plugins.workers.plugin_action_worker import (
    PluginActionWorker,
    WorkerConfig,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default config paths relative to src/
DEFAULT_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
WORKERS_CONFIG_PATH = DEFAULT_CONFIG_DIR / "plugins" / "itel_cabinet_api" / "workers.yaml"
CONNECTIONS_CONFIG_PATH = DEFAULT_CONFIG_DIR / "plugins" / "shared" / "connections" / "claimx.yaml"


def expand_env_vars(value: str) -> str:
    """Expand ${VAR_NAME} environment variable references in a string.

    Args:
        value: String potentially containing ${VAR_NAME} references

    Returns:
        String with environment variables expanded
    """
    if not isinstance(value, str):
        return value

    pattern = r"\$\{([^}]+)\}"

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name, "")
        if not env_value:
            logger.warning(f"Environment variable {var_name} is not set")
        return env_value

    return re.sub(pattern, replacer, value)


def expand_env_vars_recursive(obj: Any) -> Any:
    """Recursively expand environment variables in a data structure.

    Args:
        obj: Dict, list, or scalar value

    Returns:
        Same structure with environment variables expanded
    """
    if isinstance(obj, dict):
        return {k: expand_env_vars_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [expand_env_vars_recursive(item) for item in obj]
    elif isinstance(obj, str):
        return expand_env_vars(obj)
    return obj


def load_yaml_config(path: Path) -> dict[str, Any]:
    """Load YAML configuration file.

    Args:
        path: Path to YAML file

    Returns:
        Parsed YAML as dict

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    logger.info(f"Loading configuration from {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_worker_config(worker_name: str = "itel_cabinet_tracking_worker") -> WorkerConfig:
    """Load worker configuration from YAML.

    Args:
        worker_name: Name of worker to load from workers.yaml

    Returns:
        WorkerConfig instance
    """
    config_data = load_yaml_config(WORKERS_CONFIG_PATH)
    workers = config_data.get("workers", {})

    if worker_name not in workers:
        raise ValueError(
            f"Worker '{worker_name}' not found in {WORKERS_CONFIG_PATH}. "
            f"Available workers: {list(workers.keys())}"
        )

    worker_data = workers[worker_name]
    logger.info(f"Loaded worker config: {worker_name}")

    return WorkerConfig(
        name=worker_data.get("name", worker_name),
        input_topic=worker_data["input_topic"],
        consumer_group=worker_data["consumer_group"],
        destination_connection=worker_data.get("destination_connection"),
        destination_path=worker_data.get("destination_path", ""),
        destination_method=worker_data.get("destination_method", "POST"),
        enrichment_handlers=worker_data.get("enrichment_handlers", []),
        batch_size=worker_data.get("batch_size", 100),
        error_topic=worker_data.get("error_topic"),
        success_topic=worker_data.get("success_topic"),
        max_retries=worker_data.get("max_retries", 3),
        enable_auto_commit=worker_data.get("enable_auto_commit", False),
    )


def load_connections() -> list[ConnectionConfig]:
    """Load connection configurations from YAML.

    Returns:
        List of ConnectionConfig instances
    """
    config_data = load_yaml_config(CONNECTIONS_CONFIG_PATH)

    # Expand environment variables in the entire config
    config_data = expand_env_vars_recursive(config_data)

    connections = []
    for conn_name, conn_data in config_data.get("connections", {}).items():
        # Skip commented out / empty connections
        if not conn_data or not conn_data.get("base_url"):
            logger.debug(f"Skipping connection '{conn_name}' (no base_url)")
            continue

        auth_type = conn_data.get("auth_type", "none")
        if isinstance(auth_type, str):
            auth_type = AuthType(auth_type)

        conn = ConnectionConfig(
            name=conn_data.get("name", conn_name),
            base_url=conn_data["base_url"],
            auth_type=auth_type,
            auth_token=conn_data.get("auth_token"),
            auth_header=conn_data.get("auth_header"),
            timeout_seconds=conn_data.get("timeout_seconds", 30),
            max_retries=conn_data.get("max_retries", 3),
            retry_backoff_base=conn_data.get("retry_backoff_base", 2),
            retry_backoff_max=conn_data.get("retry_backoff_max", 60),
            headers=conn_data.get("headers", {}),
        )
        connections.append(conn)
        logger.info(f"Loaded connection: {conn.name} -> {conn.base_url}")

    return connections


async def main() -> None:
    """Main entry point for iTel Cabinet Tracking Worker."""
    logger.info("=" * 70)
    logger.info("Starting iTel Cabinet Tracking Worker")
    logger.info("=" * 70)

    # Load worker configuration
    try:
        worker_config = load_worker_config()
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Load and setup connections
    try:
        connections = load_connections()
    except FileNotFoundError as e:
        logger.error(f"Connection configuration error: {e}")
        sys.exit(1)

    connection_manager = ConnectionManager()
    for conn in connections:
        connection_manager.add_connection(conn)

    # Kafka configuration from environment
    kafka_config = {
        "bootstrap_servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
    }

    logger.info(f"Kafka bootstrap servers: {kafka_config['bootstrap_servers']}")
    logger.info(f"Input topic: {worker_config.input_topic}")
    logger.info(f"Consumer group: {worker_config.consumer_group}")

    # Create worker
    worker = PluginActionWorker(
        config=worker_config,
        kafka_config=kafka_config,
        connection_manager=connection_manager,
    )

    # Start connection manager and run worker
    try:
        await connection_manager.start()
        await worker.start()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.exception(f"Worker failed with error: {e}")
        sys.exit(1)
    finally:
        # Ensure worker is properly stopped to clean up resources
        try:
            await worker.stop()
        except Exception as stop_error:
            logger.debug(f"Error during worker cleanup: {stop_error}")
        await connection_manager.close()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
