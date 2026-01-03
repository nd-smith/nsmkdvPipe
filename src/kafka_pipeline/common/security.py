"""
Security utilities for the kafka pipeline.

Provides:
- URL sanitization (token removal for logs)
- Error message sanitization
"""

import re
from typing import Tuple
from urllib.parse import urlparse, urlunparse


# ---------------------------------------------------------------------------
# URL Parsing
# ---------------------------------------------------------------------------


def extract_filename_from_url(url: str) -> Tuple[str, str]:
    """
    Extract filename and file extension from URL.

    Args:
        url: URL containing filename in path

    Returns:
        Tuple of (filename, file_type)
        - filename: Extracted filename without extension
        - file_type: File extension in uppercase (e.g., "PDF")

    Examples:
        >>> extract_filename_from_url("https://example.com/path/file.pdf?token=abc")
        ("file", "PDF")
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        # Get the last component of the path
        filename_with_ext = path.split("/")[-1]

        # Split filename and extension
        if "." in filename_with_ext:
            filename, ext = filename_with_ext.rsplit(".", 1)
            file_type = ext.upper()
        else:
            filename = filename_with_ext or "unknown"
            file_type = "UNKNOWN"

        return filename, file_type
    except Exception:
        return "unknown", "UNKNOWN"


# ---------------------------------------------------------------------------
# URL Sanitization (for logging)
# ---------------------------------------------------------------------------

# Query parameters that may contain sensitive tokens
SENSITIVE_PARAMS = {
    "sig",
    "signature",
    "sv",
    "se",
    "st",
    "sp",
    "sr",
    "spr",  # Azure SAS
    "x-amz-signature",
    "x-amz-credential",
    "x-amz-security-token",  # AWS
    "token",
    "access_token",
    "api_key",
    "apikey",
    "key",
    "secret",
    "password",
    "pwd",
    "auth",
    "authorization",
}


def sanitize_url(url: str) -> str:
    """
    Remove sensitive query parameters from URL.

    Preserves the path and structure for debugging while removing
    tokens that could grant access if exposed in logs.

    Args:
        url: URL that may contain sensitive parameters

    Returns:
        URL with sensitive parameters replaced with [REDACTED]
    """
    if not url:
        return url

    try:
        parsed = urlparse(url)
    except Exception:
        return url  # Return as-is if parsing fails

    if not parsed.query:
        return url  # No query string, nothing to sanitize

    # Parse and sanitize query parameters
    sanitized_params = []
    for param in parsed.query.split("&"):
        if "=" in param:
            key, value = param.split("=", 1)
            if key.lower() in SENSITIVE_PARAMS:
                sanitized_params.append(f"{key}=[REDACTED]")
            else:
                sanitized_params.append(param)
        else:
            sanitized_params.append(param)

    # Rebuild URL with sanitized query
    sanitized_query = "&".join(sanitized_params)
    return urlunparse(parsed._replace(query=sanitized_query))


# ---------------------------------------------------------------------------
# Error Message Sanitization
# ---------------------------------------------------------------------------

# Patterns that may contain sensitive data in error messages
SENSITIVE_PATTERNS = [
    (re.compile(r'sig=[^&\s"\']+', re.IGNORECASE), "sig=[REDACTED]"),
    (re.compile(r'sv=[^&\s"\']+', re.IGNORECASE), "sv=[REDACTED]"),
    (re.compile(r'se=[^&\s"\']+', re.IGNORECASE), "se=[REDACTED]"),
    (re.compile(r'st=[^&\s"\']+', re.IGNORECASE), "st=[REDACTED]"),
    (re.compile(r'token=[^&\s"\']+', re.IGNORECASE), "token=[REDACTED]"),
    (re.compile(r'key=[^&\s"\']+', re.IGNORECASE), "key=[REDACTED]"),
    (re.compile(r'password=[^&\s"\']+', re.IGNORECASE), "password=[REDACTED]"),
    (re.compile(r'secret=[^&\s"\']+', re.IGNORECASE), "secret=[REDACTED]"),
    (
        re.compile(r'x-amz-signature=[^&\s"\']+', re.IGNORECASE),
        "x-amz-signature=[REDACTED]",
    ),
    (
        re.compile(r'x-amz-credential=[^&\s"\']+', re.IGNORECASE),
        "x-amz-credential=[REDACTED]",
    ),
    (re.compile(r"bearer\s+[a-zA-Z0-9\-_.]+", re.IGNORECASE), "bearer [REDACTED]"),
    (re.compile(r'api[_-]?key[=:]\s*[^\s"\'&]+', re.IGNORECASE), "api_key=[REDACTED]"),
]


def sanitize_error_message(msg: str, max_length: int = 500) -> str:
    """
    Remove potentially sensitive data from error messages.

    Applies pattern-based redaction and truncates to max_length.

    Args:
        msg: Error message that may contain sensitive data
        max_length: Maximum length of returned message

    Returns:
        Sanitized and truncated error message
    """
    if not msg:
        return msg

    # Apply all sanitization patterns
    for pattern, replacement in SENSITIVE_PATTERNS:
        msg = pattern.sub(replacement, msg)

    # Also sanitize any URLs in the message
    # Find URLs and sanitize them
    url_pattern = re.compile(r'https?://[^\s"\'<>]+')
    for match in url_pattern.finditer(msg):
        original_url = match.group(0)
        sanitized = sanitize_url(original_url)
        if sanitized != original_url:
            msg = msg.replace(original_url, sanitized)

    # Truncate if needed
    if len(msg) > max_length:
        msg = msg[: max_length - 3] + "..."

    return msg
