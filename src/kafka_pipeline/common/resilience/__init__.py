"""Resilience patterns for kafka_pipeline."""

from kafka_pipeline.common.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CLAIMX_API_CIRCUIT_CONFIG,
    get_circuit_breaker,
    circuit_protected,
)

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CLAIMX_API_CIRCUIT_CONFIG",
    "get_circuit_breaker",
    "circuit_protected",
]
