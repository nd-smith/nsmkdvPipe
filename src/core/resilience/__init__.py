"""
Resilience patterns module.

Provides fault tolerance primitives for distributed systems.

Components:
    - CircuitBreaker: State machine (closed/open/half-open)
    - @circuit_protected: Decorator for circuit breaker protection
    - Standard configs: KAFKA_CIRCUIT_CONFIG, KUSTO_CIRCUIT_CONFIG, etc.

Components to extract:
    - RetryConfig: Exponential backoff configuration
    - @with_retry decorator: Retry with jitter
"""

from .circuit_breaker import (
    CLAIMX_API_CIRCUIT_CONFIG,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    KAFKA_CIRCUIT_CONFIG,
    KUSTO_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    CircuitStats,
    circuit_protected,
    get_circuit_breaker,
)

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "circuit_protected",
    "get_circuit_breaker",
    # Standard configs
    "CLAIMX_API_CIRCUIT_CONFIG",
    "EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG",
    "KAFKA_CIRCUIT_CONFIG",
    "KUSTO_CIRCUIT_CONFIG",
    "ONELAKE_CIRCUIT_CONFIG",
]
