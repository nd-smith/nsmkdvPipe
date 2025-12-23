# verisk_pipeline/storage/kusto.py
"""
Generic Kusto/Eventhouse reader with auth, retry, and circuit breaker support.

Provides reusable Kusto access for any domain pipeline.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from collections import OrderedDict
import json

import polars as pl
from azure.kusto.data import (
    KustoClient,
    KustoConnectionStringBuilder,
    ClientRequestProperties,
)
from azure.identity import DefaultAzureCredential

from verisk_pipeline.common.auth import get_auth, clear_token_cache
from verisk_pipeline.common.exceptions import (
    AuthError,
    CircuitOpenError,
)

from verisk_pipeline.common.retry import RetryConfig, with_retry
from verisk_pipeline.common.circuit_breaker import (
    CircuitBreakerConfig,
    get_circuit_breaker,
    KUSTO_CIRCUIT_CONFIG,
)

# Import centralized error classifier (avoid circular import by importing after common modules)
from verisk_pipeline.storage.errors import StorageErrorClassifier
from verisk_pipeline.storage.kql_helpers import KQLQueryBuilder
from verisk_pipeline.common.logging.decorators import LoggedClass, logged_operation

# Retry config for Kusto operations
KUSTO_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=30.0,
)


class KustoReader(LoggedClass):
    """
    Generic Kusto/Eventhouse reader with automatic operation logging.

    Features:
    - Token caching with automatic refresh
    - Retry with exponential backoff
    - Circuit breaker for outage protection
    - Support for CLI and SPN authentication
    - Automatic operation timing and logging

    Usage:
        reader = KustoReader(cluster_uri, database)
        df = reader.execute_query("MyTable | take 10")
        reader.close()
    """

    log_component = "kusto"

    def __init__(
        self,
        cluster_uri: str,
        database: str,
        circuit_name: str = "kusto",
        circuit_config: Optional[CircuitBreakerConfig] = None,
        query_timeout: int = 300,  # 5 minutes default
        request_timeout: int = 30,  # 30 seconds default
    ):
        """
        Args:
            cluster_uri: Kusto cluster URI
            database: Database name
            circuit_name: Circuit breaker identifier (for separate breakers per cluster)
            circuit_config: Optional custom circuit breaker config
            query_timeout: Query execution timeout in seconds (default: 300s/5min)
            request_timeout: HTTP request timeout in seconds (default: 30s)
        """
        self.cluster_uri = cluster_uri
        self.database = database
        self.query_timeout = query_timeout
        self.request_timeout = request_timeout

        self._client: Optional[KustoClient] = None
        self._token_acquired_at: Optional[datetime] = None

        self._circuit_breaker = get_circuit_breaker(
            circuit_name, circuit_config or KUSTO_CIRCUIT_CONFIG
        )

        # Query result cache for fallback (Task G.1)
        self._query_cache: OrderedDict = OrderedDict()
        self._cache_max_size = 100
        self._cache_ttl = 300  # 5 minutes

        # Health monitoring statistics (P2.7)
        from datetime import timezone
        import threading

        self._health_stats = {
            "queries_executed": 0,
            "queries_succeeded": 0,
            "queries_failed": 0,
            "total_query_time_seconds": 0.0,
            "circuit_breaker_trips": 0,
            "last_reset": datetime.now(timezone.utc),
        }
        self._health_stats_lock = threading.Lock()

        super().__init__()

    def _create_client(self) -> KustoClient:
        """Create Kusto client with appropriate credentials."""
        auth = get_auth()

        # Check token file first (highest priority for local dev)
        if auth.token_file:
            try:
                token = auth.get_kusto_token(self.cluster_uri)
                kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(
                    self.cluster_uri, token
                )
                auth_mode = "file"
                self._client = KustoClient(kcsb)
                self._token_acquired_at = datetime.now()
                self._log(
                    logging.DEBUG,
                    "Kusto client initialized",
                    cluster_uri=self.cluster_uri,
                    database=self.database,
                    auth_mode=auth_mode,
                )
                return self._client
            except Exception as e:
                self._log(
                    logging.WARNING,
                    "Token file auth failed, trying other methods",
                    error=str(e)[:200],
                )
                # Fall through to try other auth methods

        if auth.use_cli:
            token = auth.get_kusto_token(self.cluster_uri)
            kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(
                self.cluster_uri, token
            )
            auth_mode = "cli"
        elif auth.has_spn_credentials:
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                self.cluster_uri,
                auth.client_id,
                auth.client_secret,
                auth.tenant_id,
            )
            auth_mode = "spn"
        else:
            self._log(logging.INFO, "Using DefaultAzureCredential for Kusto")
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                self.cluster_uri, DefaultAzureCredential()
            )
            auth_mode = "default"

        self._client = KustoClient(kcsb)
        self._token_acquired_at = datetime.now()
        self._log(
            logging.DEBUG,
            "Kusto client initialized",
            cluster_uri=self.cluster_uri,
            database=self.database,
            auth_mode=auth_mode,
        )
        return self._client

    def _get_client(self, force_refresh: bool = False) -> KustoClient:
        """Get client, refreshing if needed."""
        token_age_limit = timedelta(minutes=50)

        needs_refresh = (
            force_refresh
            or self._client is None
            or self._token_acquired_at is None
            or (datetime.now() - self._token_acquired_at) > token_age_limit
        )

        if needs_refresh:
            return self._create_client()
        return self._client  # type: ignore

    def _refresh_client(self) -> None:
        """Refresh client credentials (called on auth errors)."""
        clear_token_cache()
        if self._client is not None:
            try:
                self._client.close()
                self._log(
                    logging.DEBUG,
                    "Kusto client closed for refresh",
                    cluster_uri=self.cluster_uri,
                )
            except Exception as e:
                self._log_exception(
                    e,
                    "Error closing Kusto client during refresh",
                    level=logging.WARNING,
                    cluster_uri=self.cluster_uri,
                )
        self._client = None
        self._token_acquired_at = None
        self._log(
            logging.INFO,
            "Kusto client credentials cleared for refresh",
            cluster_uri=self.cluster_uri,
            database=self.database,
        )

    def close(self) -> None:
        """Close Kusto client."""
        if self._client is not None:
            try:
                self._client.close()
                self._log(
                    logging.DEBUG,
                    "Kusto client closed",
                    cluster_uri=self.cluster_uri,
                )
            except Exception as e:
                self._log_exception(
                    e,
                    "Error closing Kusto client",
                    level=logging.WARNING,
                    cluster_uri=self.cluster_uri,
                )
            self._client = None
            self._token_acquired_at = None

    def _cache_query_result(self, cache_key: str, result: pl.DataFrame) -> None:
        """Cache successful query result for fallback.

        Args:
            cache_key: Cache key for the query
            result: DataFrame result to cache
        """
        with_timestamp = {"result": result, "cached_at": time.time()}

        # Add to cache
        self._query_cache[cache_key] = with_timestamp

        # Evict oldest if cache too large
        while len(self._query_cache) > self._cache_max_size:
            self._query_cache.popitem(last=False)  # Remove oldest (FIFO)

        self._log(
            logging.DEBUG,
            "Query result cached",
            cache_key=cache_key,
            result_rows=len(result),
            cache_size=len(self._query_cache),
        )

    def _get_cached_result(self, cache_key: str) -> Optional[pl.DataFrame]:
        """Retrieve cached query result if available and not expired.

        Args:
            cache_key: Cache key for the query

        Returns:
            Cached DataFrame if available and valid, None otherwise
        """
        if cache_key not in self._query_cache:
            return None

        cached_entry = self._query_cache[cache_key]
        cached_at = cached_entry["cached_at"]
        age_seconds = time.time() - cached_at

        # Check TTL
        if age_seconds > self._cache_ttl:
            self._log(
                logging.DEBUG,
                "Cached result expired",
                cache_key=cache_key,
                age_seconds=age_seconds,
                ttl_seconds=self._cache_ttl,
            )
            del self._query_cache[cache_key]
            return None

        self._log(
            logging.DEBUG,
            "Using cached query result",
            cache_key=cache_key,
            age_seconds=age_seconds,
            result_rows=len(cached_entry["result"]),
        )

        return cached_entry["result"]

    def execute_query_with_fallback(
        self,
        query: str,
        fallback_result: Optional[pl.DataFrame] = None,
        cache_key: Optional[str] = None,
    ) -> pl.DataFrame:
        """Execute query with graceful fallback to cached result on circuit open.

        Args:
            query: KQL query string
            fallback_result: Optional fallback DataFrame if no cache available
            cache_key: Optional cache key (defaults to query hash)

        Returns:
            DataFrame with query results or cached/fallback data

        Raises:
            CircuitOpenError: If circuit is open and no fallback available
        """
        # Generate cache key if not provided
        if cache_key is None:
            import hashlib

            cache_key = hashlib.md5(query.encode()).hexdigest()

        try:
            # Try normal execution
            result = self.execute_query(query)

            # Cache successful result
            self._cache_query_result(cache_key, result)

            return result

        except CircuitOpenError as e:
            # Circuit is open - try fallback
            self._log(
                logging.WARNING,
                "Circuit breaker open, attempting fallback",
                cluster_uri=self.cluster_uri,
                database=self.database,
            )

            # Try cached result first
            cached = self._get_cached_result(cache_key)
            if cached is not None:
                self._log(
                    logging.INFO,
                    "Using cached result as fallback",
                    cache_key=cache_key,
                    result_rows=len(cached),
                )
                return cached

            # Use provided fallback if available
            if fallback_result is not None:
                self._log(
                    logging.INFO,
                    "Using provided fallback result",
                    result_rows=len(fallback_result),
                )
                return fallback_result

            # No fallback available
            self._log(
                logging.ERROR,
                "No fallback available for query",
                cluster_uri=self.cluster_uri,
                database=self.database,
            )
            raise e

    def _execute_raw(self, query: str) -> pl.DataFrame:
        """Execute query and return as DataFrame (no retry/circuit) with health tracking (P2.7)."""
        import time

        # Track query start (P2.7)
        start_time = time.time()
        with self._health_stats_lock:
            self._health_stats["queries_executed"] += 1

        client = self._get_client()

        # Log query start with truncated query text for debugging
        query_preview = query[:200] + "..." if len(query) > 200 else query
        self._log(
            logging.DEBUG,
            "Executing Kusto query",
            query_preview=query_preview,
            cluster_uri=self.cluster_uri,
            database=self.database,
            query_timeout=self.query_timeout,
            request_timeout=self.request_timeout,
        )

        # Configure timeouts via ClientRequestProperties
        properties = ClientRequestProperties()
        properties.set_option(
            ClientRequestProperties.results_defer_partial_query_failures_option_name,
            False,
        )
        properties.set_option(
            ClientRequestProperties.request_timeout_option_name,
            timedelta(seconds=self.request_timeout),
        )
        # Server timeout (query execution) should be slightly less than request timeout
        properties.set_option("servertimeout", timedelta(seconds=self.query_timeout))

        try:
            response = client.execute(self.database, query, properties=properties)
            primary_results = response.primary_results[0]

            columns = [col.column_name for col in primary_results.columns]
            rows = list(primary_results)

            if not rows:
                self._log(
                    logging.DEBUG,
                    "Query returned empty result",
                    rows_read=0,
                    cluster_uri=self.cluster_uri,
                    database=self.database,
                )
                # Track success even for empty result (P2.7)
                query_time = time.time() - start_time
                with self._health_stats_lock:
                    self._health_stats["queries_succeeded"] += 1
                    self._health_stats["total_query_time_seconds"] += query_time
                return pl.DataFrame()

            data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}

            # Convert dict columns to JSON strings to avoid Polars struct schema inference issues
            try:
                for col_name, values in data.items():
                    if values and isinstance(values[0], dict):
                        data[col_name] = [
                            json.dumps(v, default=str) if v is not None else None
                            for v in values
                        ]
            except Exception as e:
                self._log_exception(
                    e,
                    "Error converting dict columns to JSON",
                    level=logging.WARNING,
                    cluster_uri=self.cluster_uri,
                    database=self.database,
                )
                # Track failure (P2.7)
                query_time = time.time() - start_time
                with self._health_stats_lock:
                    self._health_stats["queries_failed"] += 1
                    self._health_stats["total_query_time_seconds"] += query_time
                return pl.DataFrame()

            df = pl.DataFrame(data)

            # Track success (P2.7)
            query_time = time.time() - start_time
            with self._health_stats_lock:
                self._health_stats["queries_succeeded"] += 1
                self._health_stats["total_query_time_seconds"] += query_time

            self._log(
                logging.DEBUG,
                "Query executed",
                rows_read=len(df),
                columns_count=len(columns),
                query_time_seconds=round(query_time, 3),
            )
            return df

        except Exception as e:
            # Track failure (P2.7)
            query_time = time.time() - start_time
            with self._health_stats_lock:
                self._health_stats["queries_failed"] += 1
                self._health_stats["total_query_time_seconds"] += query_time

            classified = StorageErrorClassifier.classify_kusto_error(e)
            self._log_exception(
                e,
                "Kusto query failed",
                query_preview=query_preview,
                cluster_uri=self.cluster_uri,
                database=self.database,
                query_time_seconds=round(query_time, 3),
            )
            if isinstance(classified, AuthError):
                self._refresh_client()
            raise classified from e

    @logged_operation(level=logging.DEBUG, slow_threshold_ms=5000)
    @with_retry(config=KUSTO_RETRY_CONFIG, on_auth_error=clear_token_cache)
    def execute_query(self, query: str) -> pl.DataFrame:
        """
        Execute a KQL query with retry and circuit breaker protection.

        Args:
            query: KQL query string

        Returns:
            DataFrame with query results

        Raises:
            CircuitOpenError: If circuit breaker is open
            KustoError: On query failure after retries
        """
        # Validate query (Task F.1)
        is_valid, error_msg = KQLQueryBuilder.validate_query(query)
        if not is_valid:
            self._log(
                logging.WARNING,
                "Query validation warning",
                error=error_msg,
                query_preview=query[:200],
            )

        # Time query execution for slow query detection
        start_time = time.time()

        try:
            result = self._circuit_breaker.call(lambda: self._execute_raw(query))

            # Check for slow queries (Task F.1)
            elapsed_seconds = time.time() - start_time
            slow_threshold = KQLQueryBuilder.get_slow_query_threshold()

            if elapsed_seconds > slow_threshold:
                self._log(
                    logging.WARNING,
                    "Slow query detected",
                    elapsed_seconds=round(elapsed_seconds, 2),
                    threshold_seconds=slow_threshold,
                    query_preview=query[:200],
                    rows_returned=len(result),
                )

            return result

        except Exception as e:
            elapsed_seconds = time.time() - start_time
            self._log(
                logging.ERROR,
                "Query execution failed",
                elapsed_seconds=round(elapsed_seconds, 2),
                query_preview=query[:200],
            )
            raise

    def query_incremental(
        self,
        table: str,
        time_column: str,
        watermark: datetime,
        columns: Optional[List[str]] = None,
        additional_filters: Optional[str] = None,
    ) -> pl.DataFrame:
        """Convenience method for incremental queries using KQLQueryBuilder.

        Args:
            table: Table name
            time_column: Time column for filtering
            watermark: Watermark timestamp for incremental processing
            columns: Optional list of columns to project
            additional_filters: Optional additional filter conditions

        Returns:
            DataFrame with incremental query results
        """
        query = KQLQueryBuilder.incremental_query(
            table=table,
            time_column=time_column,
            watermark=watermark,
            columns=columns,
            additional_filters=additional_filters,
        )

        self._log(
            logging.DEBUG,
            "Executing incremental query",
            table=table,
            time_column=time_column,
            watermark=watermark.isoformat(),
        )

        return self.execute_query(query)

    def get_circuit_status(self) -> dict:
        """Get circuit breaker status for health checks."""
        return self._circuit_breaker.get_diagnostics()

    def get_health_metrics(self) -> dict:
        """Get current Kusto client health metrics (P2.7)."""
        from datetime import datetime, timezone

        with self._health_stats_lock:
            stats = self._health_stats.copy()

            # Calculate derived metrics
            if stats["queries_executed"] > 0:
                stats["success_rate"] = round(
                    stats["queries_succeeded"] / stats["queries_executed"], 4
                )
                stats["failure_rate"] = round(
                    stats["queries_failed"] / stats["queries_executed"], 4
                )
                stats["avg_query_time_seconds"] = round(
                    stats["total_query_time_seconds"] / stats["queries_executed"], 3
                )
            else:
                stats["success_rate"] = 0.0
                stats["failure_rate"] = 0.0
                stats["avg_query_time_seconds"] = 0.0

            # Uptime
            stats["uptime_seconds"] = (
                datetime.now(timezone.utc) - stats["last_reset"]
            ).total_seconds()

            # Circuit breaker status
            stats["circuit_state"] = self._circuit_breaker.state

            return stats

    def log_health_metrics(self) -> None:
        """Log current health metrics (P2.7)."""
        metrics = self.get_health_metrics()

        self._log(
            logging.INFO,
            "Kusto client health metrics",
            queries_executed=metrics["queries_executed"],
            queries_succeeded=metrics["queries_succeeded"],
            queries_failed=metrics["queries_failed"],
            success_rate=metrics["success_rate"],
            avg_query_time_seconds=metrics["avg_query_time_seconds"],
            circuit_state=metrics["circuit_state"],
            uptime_seconds=round(metrics["uptime_seconds"], 1),
        )

    def reset_health_metrics(self) -> None:
        """Reset health monitoring statistics (P2.7)."""
        from datetime import datetime, timezone

        with self._health_stats_lock:
            self._health_stats = {
                "queries_executed": 0,
                "queries_succeeded": 0,
                "queries_failed": 0,
                "total_query_time_seconds": 0.0,
                "circuit_breaker_trips": 0,
                "last_reset": datetime.now(timezone.utc),
            }
