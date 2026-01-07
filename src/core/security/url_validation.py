"""
URL validation for attachment downloads with SSRF prevention.

Provides strict URL validation against domain allowlists to prevent
Server-Side Request Forgery (SSRF) attacks and enforce secure download sources.
"""

import ipaddress
import os
from typing import Optional, Set, Tuple
from urllib.parse import urlparse


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

# Private IP ranges (RFC 1918 + link-local + loopback + IPv6)
PRIVATE_RANGES = [
    # IPv4 private ranges
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local (cloud metadata)
    ipaddress.ip_network("127.0.0.0/8"),  # Loopback
    # IPv6 private ranges
    ipaddress.ip_network("::1/128"),  # IPv6 loopback
    ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
    ipaddress.ip_network("fc00::/7"),  # IPv6 unique local addresses
]


def get_allowed_domains() -> Set[str]:
    """
    Get allowed domains for attachment URLs.

    Reads from ALLOWED_ATTACHMENT_DOMAINS env var (comma-separated)
    or falls back to DEFAULT_ALLOWED_DOMAINS.

    Returns:
        Set of allowed domain names (lowercase)
    """
    env_domains = os.getenv("ALLOWED_ATTACHMENT_DOMAINS", "")
    if env_domains:
        return {d.strip().lower() for d in env_domains.split(",") if d.strip()}
    return DEFAULT_ALLOWED_DOMAINS


def validate_download_url(
    url: str, allowed_domains: Optional[Set[str]] = None
) -> Tuple[bool, str]:
    """
    Validate URL against domain allowlist (strict mode).

    Use this for attachment downloads where source domains are known.
    Enforces HTTPS-only and domain allowlist to prevent SSRF attacks.

    Security considerations:
    - HTTPS required (no HTTP allowed)
    - Domain must be in allowlist (case-insensitive)
    - Hostname must be present and valid
    - Handles URL parsing errors safely

    Args:
        url: URL to validate
        allowed_domains: Optional set of allowed domains (defaults to get_allowed_domains())

    Returns:
        (is_valid, error_message)
        - (True, "") if valid
        - (False, "error description") if invalid

    Examples:
        >>> validate_download_url("https://example.s3.amazonaws.com/file.pdf")
        (False, "Domain not in allowlist: example.s3.amazonaws.com")

        >>> validate_download_url("http://claimxperience.com/file.pdf")
        (False, "Must be HTTPS, got http")

        >>> validate_download_url("https://claimxperience.com/file.pdf")
        (True, "")
    """
    if not url:
        return False, "Empty URL"

    # Parse URL safely
    try:
        parsed = urlparse(url)
    except Exception as e:
        return False, f"Invalid URL format: {e}"

    # Get allowed domains
    if allowed_domains is None:
        allowed_domains = get_allowed_domains()
    else:
        allowed_domains = {d.lower() for d in allowed_domains}

    # Require HTTPS
    if parsed.scheme.lower() != "https":
        return False, f"Must be HTTPS, got {parsed.scheme}"

    # Extract hostname
    hostname = parsed.hostname
    if not hostname:
        return False, "No hostname in URL"

    # Normalize hostname to lowercase for comparison
    hostname_lower = hostname.lower()

    # Check allowlist
    if hostname_lower not in allowed_domains:
        return False, f"Domain not in allowlist: {hostname}"

    # All checks passed
    return True, ""


def is_private_ip(hostname: str) -> bool:
    """
    Check if hostname resolves to a private/internal IP address.

    Args:
        hostname: Hostname or IP address to check

    Returns:
        True if hostname is a private IP or in BLOCKED_HOSTS

    Note:
        This function does NOT perform DNS resolution for security reasons.
        It only checks if the hostname string itself is a private IP.
    """
    # Check blocked hosts
    if hostname.lower() in BLOCKED_HOSTS:
        return True

    # Check if it's an IP address
    try:
        ip = ipaddress.ip_address(hostname)
        # Check against private ranges
        for network in PRIVATE_RANGES:
            if ip in network:
                return True
    except ValueError:
        # Not an IP address, hostname only
        # We do NOT perform DNS resolution here to avoid DNS rebinding attacks
        pass

    return False
