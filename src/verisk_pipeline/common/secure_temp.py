"""
Secure temporary file handling with optional encryption at rest.

Provides encrypted temp files for sensitive data that must be written to disk
during processing (e.g., large file downloads before upload to OneLake).

Usage:
    # With encryption enabled (production)
    with SecureTempFile(suffix=".jpg", encrypt=True) as stf:
        stf.write(data)
        stf.flush()
        upload_file(stf.path)  # File is encrypted on disk
    # File automatically deleted and key destroyed

    # Without encryption (development)
    with SecureTempFile(suffix=".jpg", encrypt=False) as stf:
        stf.write(data)
        # Same interface, just no encryption overhead

Security notes:
- Uses Fernet symmetric encryption (AES-128-CBC with HMAC)
- Key is generated per-file and held only in memory
- Key is destroyed when context manager exits
- File is securely deleted on exit (overwritten before unlink)
"""

import os
import secrets
import tempfile
from pathlib import Path
from typing import Optional, Union

# Optional import - only needed if encryption is enabled
try:
    from cryptography.fernet import Fernet
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False
    Fernet = None  # type: ignore


class SecureTempFile:
    """
    Secure temporary file with optional encryption.

    Attributes:
        path: Path to the temp file (for passing to upload functions)
        encrypt: Whether encryption is enabled
    """

    def __init__(
        self,
        suffix: str = "",
        prefix: str = "verisk_",
        dir: Optional[str] = None,
        encrypt: bool = False,
        secure_delete: bool = True,
    ):
        """
        Initialize secure temp file.

        Args:
            suffix: File suffix (e.g., ".jpg")
            prefix: File prefix
            dir: Directory for temp file (defaults to system temp)
            encrypt: Enable encryption at rest
            secure_delete: Overwrite file before deletion
        """
        self._suffix = suffix
        self._prefix = prefix
        self._dir = dir
        self._encrypt = encrypt
        self._secure_delete = secure_delete

        self._path: Optional[Path] = None
        self._fd: Optional[int] = None
        self._file = None
        self._key: Optional[bytes] = None
        self._fernet: Optional[Fernet] = None
        self._buffer: bytes = b""

        if encrypt and not CRYPTOGRAPHY_AVAILABLE:
            raise ImportError(
                "cryptography package required for encryption. "
                "Install with: pip install cryptography"
            )

    @property
    def path(self) -> str:
        """Get path to temp file (for upload functions)."""
        if self._path is None:
            raise RuntimeError("SecureTempFile not opened. Use as context manager.")
        return str(self._path)

    @property
    def encrypt(self) -> bool:
        """Whether encryption is enabled."""
        return self._encrypt

    def __enter__(self) -> "SecureTempFile":
        """Open the temp file."""
        # Create temp file with secure permissions (0600)
        self._fd, path_str = tempfile.mkstemp(
            suffix=self._suffix,
            prefix=self._prefix,
            dir=self._dir,
        )
        self._path = Path(path_str)

        # Set restrictive permissions
        os.chmod(self._fd, 0o600)

        if self._encrypt:
            # Generate encryption key
            self._key = Fernet.generate_key()
            self._fernet = Fernet(self._key)
            self._buffer = b""

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close and securely delete the temp file."""
        try:
            # Close file handle if open
            if self._fd is not None:
                try:
                    os.close(self._fd)
                except OSError:
                    pass
                self._fd = None

            # Secure delete: overwrite with random data before unlinking
            if self._secure_delete and self._path and self._path.exists():
                try:
                    size = self._path.stat().st_size
                    if size > 0:
                        with open(self._path, "wb") as f:
                            # Overwrite with random bytes
                            f.write(secrets.token_bytes(size))
                            f.flush()
                            os.fsync(f.fileno())
                except Exception:
                    pass  # Best effort

            # Delete the file
            if self._path and self._path.exists():
                try:
                    self._path.unlink()
                except Exception:
                    pass  # Best effort

        finally:
            # Destroy key material
            self._key = None
            self._fernet = None
            self._buffer = b""
            self._path = None

    def write(self, data: bytes) -> int:
        """
        Write data to the temp file.

        If encryption is enabled, data is encrypted before writing.

        Args:
            data: Bytes to write

        Returns:
            Number of bytes written (original size, not encrypted size)
        """
        if self._fd is None:
            raise RuntimeError("SecureTempFile not opened. Use as context manager.")

        original_size = len(data)

        if self._encrypt and self._fernet:
            # Encrypt the data
            encrypted = self._fernet.encrypt(data)
            os.write(self._fd, encrypted)
        else:
            os.write(self._fd, data)

        return original_size

    def write_all(self, data: bytes) -> int:
        """
        Write all data and prepare for reading/upload.

        For encrypted files, this finalizes the encryption.

        Args:
            data: All bytes to write

        Returns:
            Number of bytes written
        """
        return self.write(data)

    def flush(self) -> None:
        """Flush data to disk."""
        if self._fd is not None:
            os.fsync(self._fd)

    def read_decrypted(self) -> bytes:
        """
        Read and decrypt the file contents.

        Only needed if you need to re-read the data. For uploads,
        use self.path directly (encrypted files stay encrypted during upload).

        Returns:
            Decrypted file contents
        """
        if self._path is None:
            raise RuntimeError("SecureTempFile not opened.")

        with open(self._path, "rb") as f:
            data = f.read()

        if self._encrypt and self._fernet:
            return self._fernet.decrypt(data)
        return data


class SecureTempFileAsync:
    """
    Async version of SecureTempFile for use with aiofiles.

    Provides same interface but works with async write operations.
    """

    def __init__(
        self,
        suffix: str = "",
        prefix: str = "verisk_",
        dir: Optional[str] = None,
        encrypt: bool = False,
        secure_delete: bool = True,
    ):
        self._inner = SecureTempFile(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            encrypt=encrypt,
            secure_delete=secure_delete,
        )

    @property
    def path(self) -> str:
        return self._inner.path

    @property
    def encrypt(self) -> bool:
        return self._inner.encrypt

    async def __aenter__(self) -> "SecureTempFileAsync":
        self._inner.__enter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self._inner.__exit__(exc_type, exc_val, exc_tb)

    async def write(self, data: bytes) -> int:
        """Write data (runs sync write in current implementation)."""
        return self._inner.write(data)

    async def flush(self) -> None:
        self._inner.flush()


def get_secure_temp_file(
    suffix: str = "",
    encrypt: Optional[bool] = None,
    config: Optional[object] = None,
) -> SecureTempFile:
    """
    Factory function to create SecureTempFile with config-based defaults.

    Args:
        suffix: File suffix
        encrypt: Override encryption setting (None = use config)
        config: Config object with security.encrypt_temp_files attribute

    Returns:
        Configured SecureTempFile instance
    """
    if encrypt is None:
        # Check config for default
        if config is not None and hasattr(config, 'security'):
            encrypt = getattr(config.security, 'encrypt_temp_files', False)
        else:
            encrypt = False

    return SecureTempFile(suffix=suffix, encrypt=encrypt)
