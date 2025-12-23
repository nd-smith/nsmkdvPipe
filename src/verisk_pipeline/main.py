#!/usr/bin/env python3
"""
Verisk Pipeline - Entry point.

Multi-domain pipeline for processing insurance claim webhook events.
Reads from Kusto/Eventhouse, writes to Fabric Lakehouse Delta tables,
and downloads attachments to OneLake.

Usage:
    python -m verisk_pipeline                    # Run continuously
    python -m verisk_pipeline --mode once        # Single cycle
    python -m verisk_pipeline --config path.yaml # Custom config
    python -m verisk_pipeline --help             # Show help
"""

import argparse
import sys
import os
import logging

os.environ["AZURE_TOKEN_FILE"] = "tokens.json"
os.environ["RUST_LOG"] = "error"
os.environ["POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY"] = "1"

from pathlib import Path
from typing import List, Optional, Tuple, Any

from dotenv import load_dotenv

from verisk_pipeline.common.config.root import load_root_config, RootConfig
from verisk_pipeline.common.logging.context_managers import set_log_context
from verisk_pipeline.common.logging.setup import get_logger, setup_logging
from verisk_pipeline.common.logging.utilities import log_exception, log_with_context
from verisk_pipeline.api.health import HealthServer
from verisk_pipeline.xact.xact_pipeline import Pipeline as XactPipeline

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="verisk_pipeline",
        description="Verisk Pipeline - Process insurance claim webhook events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m verisk_pipeline                     Run continuously
  python -m verisk_pipeline --mode once         Run single cycle and exit
  python -m verisk_pipeline --stages ingest     Run only ingest stage
  python -m verisk_pipeline --test              Test mode (single cycle, verbose)

Environment Variables:

  AZURE_AUTH_INTERACTIVE    Set to 'true' for Azure CLI auth
  AZURE_CLIENT_ID           Service principal client ID
  AZURE_CLIENT_SECRET       Service principal secret
  AZURE_TENANT_ID           Azure tenant ID
  EVENTHOUSE_CLUSTER_URI    Kusto cluster URI
  KQL_DATABASE              Kusto database name
  LAKEHOUSE_ABFSS_PATH      OneLake lakehouse path
  FILES_BASE_PATH           OneLake files path (optional)
  WORKER_ID                 Worker identifier (optional)
  TEST_MODE                 Set to 'true' for test mode
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["continuous", "once"],
        default="continuous",
        help="Run mode: 'continuous' (default) or 'once'",
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config YAML file (default: config.yaml)",
    )

    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["ingest", "enrich", "download", "retry"],
        default=None,
        help="Stages to run (default: all enabled in config)",
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: run single cycle with verbose output",
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate configuration and exit",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )

    parser.add_argument(
        "--domain",
        choices=["xact", "claimx"],
        help="Run only specified domain (default: all enabled)",
    )

    return parser.parse_args()


def _build_pipelines(
    root_config: RootConfig,
    stages: Optional[List[str]] = None,
    domain: Optional[str] = None,
) -> List[Tuple[str, Any]]:
    """
    Build list of (name, pipeline) tuples for enabled domains.

    Args:
        root_config: Root configuration
        stages: Optional stage filter

    Returns:
        List of (domain_name, pipeline_instance) tuples
    """
    pipelines = []

    # Xact pipeline
    if root_config.xact is not None and (domain is None or domain == "xact"):
        xact_config = root_config.xact
        if stages:
            xact_config.schedule.enabled_stages = stages

        pipelines.append(("xact", XactPipeline(xact_config)))

    # ClaimX pipeline
    if root_config.claimx is not None and (domain is None or domain == "claimx"):
        from verisk_pipeline.claimx.claimx_pipeline import Pipeline as ClaimXPipeline

        claimx_config = root_config.claimx
        if stages:
            # Map stage names (claimx has different stages)
            claimx_stages = []
            for s in stages:
                if s in ["ingest", "enrich", "download", "retry"]:
                    claimx_stages.append(s)
            if claimx_stages:
                claimx_config.schedule.enabled_stages = claimx_stages

        pipelines.append(("claimx", ClaimXPipeline(claimx_config)))

    return pipelines


def validate_configuration(root_config: RootConfig) -> bool:
    """
    Validate configuration and print results.

    Args:
        root_config: Configuration to validate

    Returns:
        True if valid, False otherwise
    """
    print("\nConfiguration Validation")
    print("=" * 40)

    errors = root_config.validate()

    if errors:
        print("Configuration INVALID\n")
        for error in errors:
            print(f"  - {error}")
        print()
        return False

    print("Configuration valid\n")
    print(f"Enabled domains: {', '.join(root_config.enabled_domains)}")
    print(f"Auth mode:       {_get_auth_mode()}")

    if root_config.xact:
        print("\nXact Pipeline:")
        print(f"  Kusto cluster:  {root_config.xact.kusto.cluster_uri or '(not set)'}")
        print(f"  Kusto database: {root_config.xact.kusto.database or '(not set)'}")
        print(
            f"  Lakehouse:      {root_config.xact.lakehouse.abfss_path or '(not set)'}"
        )
        print(
            f"  Enabled stages: {', '.join(root_config.xact.schedule.enabled_stages)}"
        )
        print(f"  Poll interval:  {root_config.xact.schedule.interval_seconds}s")
        print(f"  Batch size:     {root_config.xact.processing.batch_size}")

    print(
        f"\nHealth port:    {root_config.health.port if root_config.health.enabled else 'disabled'}"
    )
    print(f"JSON logs:      {root_config.observability.json_logs}")
    print()

    return True


def _get_auth_mode() -> str:
    """Determine current auth mode from environment."""
    if os.getenv("AZURE_AUTH_INTERACTIVE", "").lower() == "true":
        return "Azure CLI"
    elif all(
        [
            os.getenv("AZURE_CLIENT_ID"),
            os.getenv("AZURE_CLIENT_SECRET"),
            os.getenv("AZURE_TENANT_ID"),
        ]
    ):
        return "Service Principal"
    else:
        return "Not configured"


def _derive_stage_name(stages: Optional[list]) -> Optional[str]:
    """
    Derive stage name for logging from --stages argument.

    Returns stage name if single stage specified, else None.
    """
    if stages and len(stages) == 1:
        return stages[0]
    return None


def main() -> int:
    """
    Main entry point.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Load environment variables from .env file
    load_dotenv()

    # Parse arguments
    args = parse_args()

    # Determine mode
    mode = "once" if args.test else args.mode

    try:
        # Build overrides
        overrides = {}
        if args.test:
            overrides["test_mode"] = True

        # Load root configuration
        root_config = load_root_config(
            config_path=args.config,
            overrides=overrides if overrides else None,
        )

        # Validate only mode
        if args.validate:
            return 0 if validate_configuration(root_config) else 1

        # Build pipelines for enabled domains
        pipelines = _build_pipelines(root_config, args.stages, args.domain)

        if not pipelines:
            print("Error: No pipelines enabled", file=sys.stderr)
            return 1

        # Derive stage name for per-stage log files
        stage_name = _derive_stage_name(args.stages)

        # Derive domain name for logging
        domain_name = args.domain or (pipelines[0][0] if len(pipelines) == 1 else None)

        # Setup logging with stage-specific file and worker context
        console_level = logging.DEBUG if args.test else logging.INFO

        log_dir = Path(root_config.logging.log_dir)
        setup_logging(
            name="verisk_pipeline",
            stage=stage_name,
            domain=domain_name,
            log_dir=log_dir,
            json_format=root_config.observability.json_logs,
            console_level=console_level,
            worker_id=root_config.worker_id,
        )

        # Log successful initialization
        log_with_context(
            logger,
            logging.INFO,
            "Logging initialized",
            log_dir=str(log_dir),
            domain=domain_name or "all",
        )

        # Set global worker context
        set_log_context(worker_id=root_config.worker_id)

        # Start health server with pipeline list
        health_server = HealthServer(root_config.health, pipelines=pipelines)
        log_with_context(
            logger,
            logging.INFO,
            "Starting health server",
            host=root_config.health.host,
            port=root_config.health.port,
            enabled=root_config.health.enabled,
        )
        health_server.start()

        # Run pipelines
        # For now, single domain - run the first pipeline
        # Future: asyncio.gather for concurrent multi-domain
        try:
            _, pipeline = pipelines[0]

            log_with_context(
                logger,
                logging.INFO,
                "Starting pipeline",
                domain=domain_name or "all",
                cycle_interval=(
                    root_config.xact.schedule.interval_seconds
                    if root_config.xact
                    else root_config.claimx.schedule.interval_seconds
                ),
                mode=mode,
            )

            if mode == "once":
                exit_code = pipeline.run_once()
            else:
                exit_code = pipeline.run()

            log_with_context(
                logger, logging.INFO, "Pipeline completed", exit_code=exit_code
            )
            return exit_code
        finally:
            health_server.stop()

    except KeyboardInterrupt:
        log_with_context(logger, logging.INFO, "Pipeline interrupted by user")
        print("\nInterrupted by user")
        return 130  # Standard exit code for SIGINT

    except ValueError as e:
        # Config errors
        log_exception(logger, e, "Configuration error", include_traceback=False)
        print(f"\nConfiguration error: {e}", file=sys.stderr)
        return 1

    except Exception as e:
        log_exception(logger, e, "Fatal error during startup")
        print(f"\nFatal error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
