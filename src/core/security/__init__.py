"""
Security validation module.

Provides input validation and sanitization for security-sensitive operations.

Components to extract and review from verisk_pipeline.common:
    - validate_download_url(): SSRF prevention with domain allowlist
    - check_presigned_url(): AWS S3 and ClaimX URL parsing/expiration
    - sanitize_filename(): Path traversal prevention
    - sanitize_url(): Remove auth tokens from logged URLs
    - sanitize_error_message(): Remove sensitive data from logs

Review checklist:
    [ ] Domain allowlist is complete and correct
    [ ] URL validation handles edge cases (unicode, encoding)
    [ ] Path traversal prevention is robust
    [ ] Presigned URL expiration detection is accurate
    [ ] No bypass vectors exist
"""
