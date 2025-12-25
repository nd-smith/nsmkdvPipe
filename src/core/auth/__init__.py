"""
Authentication module.

Provides unified authentication for Azure services and Kafka.

Components:
    - TokenCache: Thread-safe token caching with expiry checks (WP-102 ✓)
    - Azure credential abstraction (CLI, SPN, managed identity) - WP-103
    - Token refresh logic - WP-103

Kafka integration:
    - Kafka OAUTHBEARER callback for SASL authentication - WP-104

Review checklist for WP-102:
    [x] Token refresh timing (50min buffer for 60min tokens - CORRECT)
    [x] Thread safety of token cache (threading.Lock added)
    [x] Proper timezone handling (UTC throughout)
"""

from .token_cache import TokenCache, CachedToken, TOKEN_REFRESH_MINS, TOKEN_EXPIRY_MINS

__all__ = ["TokenCache", "CachedToken", "TOKEN_REFRESH_MINS", "TOKEN_EXPIRY_MINS"]
