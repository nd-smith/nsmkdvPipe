"""
Security validation module.

Provides input validation and sanitization for security-sensitive operations.

Components to extract and review from verisk_pipeline.common:
    - validate_download_url(): SSRF prevention with domain allowlist [DONE - WP-113]
    - check_presigned_url(): AWS S3 and ClaimX URL parsing/expiration
    - sanitize_filename(): Path traversal prevention
    - sanitize_url(): Remove auth tokens from logged URLs
    - sanitize_error_message(): Remove sensitive data from logs

Review checklist:
    [x] Domain allowlist is complete and correct
    [x] URL validation handles edge cases (unicode, encoding)
    [ ] Path traversal prevention is robust
    [ ] Presigned URL expiration detection is accurate
    [ ] No bypass vectors exist
"""

from core.security.url_validation import (
    ALLOWED_SCHEMES,
    BLOCKED_HOSTS,
    DEFAULT_ALLOWED_DOMAINS,
    PRIVATE_RANGES,
    get_allowed_domains,
    is_private_ip,
    validate_download_url,
)

__all__ = [
    "validate_download_url",
    "get_allowed_domains",
    "is_private_ip",
    "ALLOWED_SCHEMES",
    "BLOCKED_HOSTS",
    "DEFAULT_ALLOWED_DOMAINS",
    "PRIVATE_RANGES",
]
