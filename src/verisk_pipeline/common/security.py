"""
Security utilities for the xact pipeline.

Provides:
- URL validation (SSRF prevention)
- URL sanitization (token removal for logs)
- Error message sanitization
- Filename sanitization (path traversal prevention)
"""

import ipaddress
import os
import re
import unicodedata
from typing import Set, Tuple, Optional
from urllib.parse import urlparse, urlunparse


# ---------------------------------------------------------------------------
# SSRF Prevention - URL Validation
# ---------------------------------------------------------------------------

# Allowed schemes for attachment downloads
ALLOWED_SCHEMES: Set[str] = {"https", "http"}

# Domains explicitly allowed for attachment downloads
# Configure via environment or extend this set
DEFAULT_ALLOWED_DOMAINS: Set[str] = {
    "usw2-prod-xn-exportreceiver-publish.s3.us-west-2.amazonaws.com",
    "claimxperience.s3.amazonaws.com",
    "claimxperience.s3.us-east-1.amazonaws.com",
    "www.claimxperience.com",
    "claimxperience.com",
    "xactware-claimx-us-prod.s3.us-west-1.amazonaws.com",
    "www.claimxperience.com",
}

# Hosts to block (metadata endpoints, localhost, etc.)
BLOCKED_HOSTS: Set[str] = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "metadata.google.internal",
    "metadata.aws.internal",
    "169.254.169.254",
}

# Private IP ranges (RFC 1918 + link-local + loopback)
PRIVATE_RANGES = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local (cloud metadata)
    ipaddress.ip_network("127.0.0.0/8"),  # Loopback
]


def get_allowed_domains() -> Set[str]:
    """
    Get allowed domains for attachment URLs.

    Reads from ALLOWED_ATTACHMENT_DOMAINS env var (comma-separated)
    or falls back to DEFAULT_ALLOWED_DOMAINS.
    """
    env_domains = os.getenv("ALLOWED_ATTACHMENT_DOMAINS", "")
    if env_domains:
        return {d.strip().lower() for d in env_domains.split(",") if d.strip()}
    return DEFAULT_ALLOWED_DOMAINS


def validate_download_url(
    url: str, allowed_domains: Optional[Set[str]] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validate URL against domain allowlist (strict mode).

    Use this for attachment downloads where source domains are known.

    Args:
        url: URL to validate

    Returns:
        (is_valid, error_message)
    """
    if not url:
        return False, "Empty URL"

    try:
        parsed = urlparse(url)
    except Exception as e:
        return False, f"Invalid URL format: {e}"

    if allowed_domains is None:
        allowed_domains = get_allowed_domains()
    else:
        allowed_domains = {d.lower() for d in allowed_domains}

    # Require HTTPS
    if parsed.scheme.lower() != "https":
        return False, f"Must be HTTPS, got {parsed.scheme}"

    hostname = parsed.hostname
    if not hostname:
        return False, "No hostname in URL"

    # Check allowlist
    if hostname.lower() not in allowed_domains:
        return False, f"Domain not in allowlist: {hostname}"

    return True, ""


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


# ---------------------------------------------------------------------------
# Filename Sanitization (Path Traversal Prevention)
# ---------------------------------------------------------------------------

# Characters not allowed in filenames (Windows + Unix unsafe)
UNSAFE_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

# Maximum filename length (common filesystem limit)
MAX_FILENAME_LENGTH = 255


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename to prevent path traversal and filesystem issues.

    - Strips directory components
    - Removes dangerous characters
    - Limits length
    - Provides fallback for empty results

    Args:
        filename: Potentially unsafe filename

    Returns:
        Safe filename string
    """
    if not filename:
        return "attachment"

    # Take only the basename (strip any path components)
    filename = os.path.basename(filename)

    # URL-decode if needed (handle %2F, %00, etc.)
    try:
        from urllib.parse import unquote

        filename = unquote(filename)
        # Re-apply basename after decode (catches encoded path separators)
        filename = os.path.basename(filename)
    except Exception:
        pass

    # Normalize Unicode (NFC form) and replace invalid chars
    filename = unicodedata.normalize("NFC", filename)

    # Replace replacement characters with underscore
    filename = filename.replace("\ufffd", "_")

    # Remove null bytes and other dangerous characters
    filename = UNSAFE_FILENAME_CHARS.sub("_", filename)

    # Remove leading/trailing dots and spaces
    filename = filename.strip(". ")

    # Limit length (preserve extension if possible)
    if len(filename) > MAX_FILENAME_LENGTH:
        if "." in filename:
            name, ext = filename.rsplit(".", 1)
            ext = ext[:10]  # Limit extension length too
            max_name = MAX_FILENAME_LENGTH - len(ext) - 1
            filename = f"{name[:max_name]}.{ext}"
        else:
            filename = filename[:MAX_FILENAME_LENGTH]

    # Fallback if empty after sanitization
    return filename or "attachment"


def extract_filename_from_url(url: str) -> Tuple[str, str]:
    """
    Extract and sanitize filename from URL, determine file type.

    Args:
        url: URL to extract filename from

    Returns:
        (sanitized_filename, file_type)
    """
    if not url:
        return "attachment", "UNKNOWN"

    try:
        parsed = urlparse(url)
        path = parsed.path
    except Exception:
        return "attachment", "UNKNOWN"

    # Get last path component
    filename = path.split("/")[-1] if path else ""

    # Sanitize
    filename = sanitize_filename(filename)

    # Determine file type from extension
    if "." in filename:
        file_type = filename.rsplit(".", 1)[-1].upper()
    else:
        file_type = "UNKNOWN"

    return filename, file_type
