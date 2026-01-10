"""
Backwards-compatibility shim for circuit breaker imports.

This module re-exports the circuit breaker implementation from core.resilience.circuit_breaker
to maintain backwards compatibility for existing imports from kafka_pipeline.common.resilience.

New code should import directly from core.resilience.circuit_breaker.
"""

from core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitStats,
    circuit_protected,
    get_circuit_breaker,
    # Standard configs
    CLAIMX_API_CIRCUIT_CONFIG,
    EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG,
    KAFKA_CIRCUIT_CONFIG,
    KUSTO_CIRCUIT_CONFIG,
    ONELAKE_CIRCUIT_CONFIG,
)

__all__ = [
    # Core classes
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitStats",
    # Functions
    "circuit_protected",
    "get_circuit_breaker",
    # Standard configs
    "CLAIMX_API_CIRCUIT_CONFIG",
    "EXTERNAL_DOWNLOAD_CIRCUIT_CONFIG",
    "KAFKA_CIRCUIT_CONFIG",
    "KUSTO_CIRCUIT_CONFIG",
    "ONELAKE_CIRCUIT_CONFIG",
]
