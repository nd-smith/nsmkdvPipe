"""
Authentication module.

Provides unified authentication for Azure services and Kafka.

Components to extract and review from verisk_pipeline.common.auth:
    - TokenCache: Token caching with expiry checks
    - Azure credential abstraction (CLI, SPN, managed identity)
    - Token refresh logic

New functionality to add:
    - Kafka OAUTHBEARER callback for SASL authentication
    - Scoped token requests (different audiences)

Review checklist:
    [ ] Token refresh timing (currently 50min, verify this is correct)
    [ ] Error handling on token acquisition failure
    [ ] Thread safety of token cache
    [ ] Support for multiple concurrent token types
"""
