"""
Resilience patterns module.

Provides fault tolerance primitives for distributed systems.

Components to extract and review from verisk_pipeline.common:
    - CircuitBreaker: State machine (closed/open/half-open)
    - RetryConfig: Exponential backoff configuration
    - @with_retry decorator: Retry with jitter

Review checklist:
    [ ] Circuit breaker state transitions are correct
    [ ] Half-open state properly tests recovery
    [ ] Retry jitter algorithm prevents thundering herd
    [ ] Async support for circuit breaker calls
    [ ] Metrics integration points
    [ ] Thread safety for shared circuit breaker instances
"""
