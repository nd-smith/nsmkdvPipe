"""

Token refresher daemon for local development.



Keeps a JSON token file fresh so multiple services can share authentication.

Stores tokens for multiple resources (storage, kusto, etc.).



Usage:

    python -m verisk_pipeline.common.token_refresher --token-file tokens.json



Services should set:

    export AZURE_TOKEN_FILE=tokens.json

"""

import argparse

import json

import logging

import os

import shutil

import subprocess

import sys

import time

from datetime import datetime, timezone

from pathlib import Path

from typing import Dict, List


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# Refresh every 45 minutes (tokens expire at 60 min)

REFRESH_INTERVAL_SECONDS = 45 * 60


# Resources to fetch tokens for

DEFAULT_RESOURCES = [
    "https://storage.azure.com/",
]


def fetch_token(resource: str, tenant_id: str = None) -> str:
    """Fetch token from Azure CLI."""

    az_path = shutil.which("az")

    if not az_path:

        raise RuntimeError("Azure CLI not found")

    cmd = [az_path, "account", "get-access-token", "--resource", resource]

    if tenant_id:

        cmd.extend(["--tenant", tenant_id])

    cmd.extend(["--query", "accessToken", "-o", "tsv"])

    logger.debug(f"Fetching token for {resource}")

    try:

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

    except subprocess.TimeoutExpired:

        raise RuntimeError(f"Azure CLI timed out for {resource}")

    if result.returncode != 0:

        stderr = result.stderr.strip()

        if "az login" in stderr.lower():

            raise RuntimeError(f"Azure CLI session expired. Run 'az login' first.")

        raise RuntimeError(f"Azure CLI failed for {resource}: {stderr}")

    token = result.stdout.strip()

    if not token:

        raise RuntimeError(f"Azure CLI returned empty token for {resource}")

    return token


def write_tokens_atomic(token_file: Path, tokens: Dict[str, str]) -> None:
    """Write tokens atomically."""

    token_file = Path(token_file)

    token_file.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = token_file.parent / f".tokens_{os.getpid()}.tmp"

    try:

        with open(tmp_path, "w", encoding="utf-8") as f:

            json.dump(tokens, f, indent=2)

        # On Windows, remove target first

        if token_file.exists():

            token_file.unlink()

        tmp_path.rename(token_file)

        logger.info(f"Tokens written to {token_file}")

    except Exception:

        if tmp_path.exists():

            tmp_path.unlink()

        raise


def read_existing_tokens(token_file: Path) -> Dict[str, str]:
    """Read existing tokens file if it exists."""

    try:

        if token_file.exists():

            with open(token_file, "r", encoding="utf-8") as f:

                return json.load(f)

    except (json.JSONDecodeError, IOError):

        pass

    return {}


def refresh_all_tokens(
    resources: List[str],
    tenant_id: str = None,
    existing_tokens: Dict[str, str] = None,
) -> Dict[str, str]:
    """Fetch tokens for all resources."""

    tokens = existing_tokens.copy() if existing_tokens else {}

    for resource in resources:

        try:

            token = fetch_token(resource, tenant_id)

            tokens[resource] = token

            logger.info(f"Refreshed token for {resource}")

        except Exception as e:

            logger.error(f"Failed to fetch token for {resource}: {e}")

            # Keep existing token if refresh fails

    return tokens


def refresh_loop(
    token_file: Path,
    resources: List[str],
    tenant_id: str = None,
) -> None:
    """Main refresh loop."""

    logger.info("Token refresher starting")

    logger.info(f"  Token file: {token_file}")

    logger.info(f"  Refresh interval: {REFRESH_INTERVAL_SECONDS // 60} minutes")

    logger.info(f"  Resources: {resources}")

    # Initial token fetch

    tokens = refresh_all_tokens(resources, tenant_id)

    if not tokens:

        logger.error("Failed to acquire any tokens")

        sys.exit(1)

    write_tokens_atomic(token_file, tokens)

    logger.info(f"Initial tokens acquired: {list(tokens.keys())}")

    # Refresh loop

    while True:

        time.sleep(REFRESH_INTERVAL_SECONDS)

        try:

            existing = read_existing_tokens(token_file)

            tokens = refresh_all_tokens(resources, tenant_id, existing)

            write_tokens_atomic(token_file, tokens)

            logger.info(f"Tokens refreshed at {datetime.now(timezone.utc).isoformat()}")

        except Exception as e:

            logger.error(f"Token refresh failed: {e}")


def main():

    parser = argparse.ArgumentParser(description="Token refresher daemon")

    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path("tokens.json"),
        help="Path to token file (default: tokens.json)",
    )

    parser.add_argument(
        "--tenant-id",
        type=str,
        default=os.getenv("AZURE_TENANT_ID"),
        help="Azure tenant ID",
    )

    parser.add_argument(
        "--resource",
        type=str,
        action="append",
        dest="resources",
        help="Resource to fetch token for (can specify multiple)",
    )

    parser.add_argument(
        "--kusto-cluster",
        type=str,
        help="Kusto cluster URI to add as a resource",
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch tokens once and exit",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.verbose:

        logging.getLogger().setLevel(logging.DEBUG)

    # Build resource list

    resources = args.resources if args.resources else DEFAULT_RESOURCES.copy()

    if args.kusto_cluster:

        resources.append(args.kusto_cluster)

    if args.once:

        tokens = refresh_all_tokens(resources, args.tenant_id)

        write_tokens_atomic(args.token_file, tokens)

        print(f"Tokens written to {args.token_file}")

        print(f"Resources: {list(tokens.keys())}")

        print(f"Set: export AZURE_TOKEN_FILE={args.token_file}")

    else:

        refresh_loop(args.token_file, resources, args.tenant_id)


if __name__ == "__main__":

    main()
