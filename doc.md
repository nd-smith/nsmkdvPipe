# Auth Module Consolidation - Technical Debt

## Problem Statement

The codebase has **three separate implementations** for reading Azure authentication tokens, leading to inconsistent behavior and bugs that need to be fixed in multiple places.

## Current State

### 1. `src/core/auth/credentials.py` - `AzureCredentialProvider`
- Most complete implementation
- Handles: token file reading, SPN credentials, CLI auth, caching
- Used by: `dedup.py` (via `get_storage_options()`)
- Token file encoding: `utf-8-sig` (BOM-safe) ✅

### 2. `src/kafka_pipeline/common/auth.py` - `AzureAuth`
- Separate implementation with similar functionality
- Handles: token file reading, SPN credentials, CLI auth, caching
- Used by: Delta writers (`delta.py`), other pipeline components
- Token file encoding: `utf-8-sig` (BOM-safe, fixed 2026-01-04) ✅

### 3. `src/kafka_pipeline/common/eventhouse/kql_client.py` - `FileTokenCredential`
- Minimal implementation for Kusto/Eventhouse auth
- Handles: token file reading only
- Used by: KQL client for Eventhouse queries
- Token file encoding: `utf-8-sig` (BOM-safe, fixed 2026-01-04) ✅

## Issues Caused by Duplication

1. **BOM Bug (2026-01-04)**: PowerShell writes UTF-8 with BOM (`\ufeff`). This broke JSON parsing in all three modules. Required fixing in 3 separate files:
   - `credentials.py` line 359
   - `auth.py` line 161
   - `kql_client.py` line 81

2. **Inconsistent Caching**: Each module has its own token cache with different TTLs and refresh logic.

3. **Maintenance Burden**: Any token handling changes must be applied to 3 files.

4. **Testing Complexity**: Need to test auth behavior in 3 places.

## Recommended Solution

### Option 1: Consolidate to `core/auth/credentials.py` (Recommended)

Make `core/auth/credentials.py` the single source of truth:

```python
# In kafka_pipeline/common/auth.py
from core.auth.credentials import get_storage_options, get_credential_provider

# Remove duplicate AzureAuth class, use core module instead
```

```python
# In kafka_pipeline/common/eventhouse/kql_client.py
from core.auth.credentials import AzureCredentialProvider

class KustoTokenCredential:
    """Wraps AzureCredentialProvider for Kusto SDK compatibility."""

    def __init__(self):
        self._provider = AzureCredentialProvider()

    def get_token(self, *scopes, **kwargs):
        token = self._provider.get_kusto_token()
        return AccessToken(token, expires_on)
```

### Option 2: Create Shared Token File Reader

Create a minimal shared utility that all modules use:

```python
# core/auth/token_file.py
class TokenFileReader:
    """Reads tokens from JSON file with BOM handling and caching."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._cache = {}
        self._mtime = None

    def get_token(self, resource: str) -> str:
        """Get token for resource, with file change detection."""
        ...
```

## Implementation Steps

1. **Phase 1**: Add `get_kusto_token()` to `core/auth/credentials.py` if not present
2. **Phase 2**: Update `kql_client.py` to use `core/auth/credentials.py`
3. **Phase 3**: Update `kafka_pipeline/common/auth.py` to wrap `core/auth/credentials.py`
4. **Phase 4**: Remove duplicate token reading code
5. **Phase 5**: Update tests

## Files to Modify

| File | Action |
|------|--------|
| `src/core/auth/credentials.py` | Add Kusto token support if missing |
| `src/kafka_pipeline/common/auth.py` | Replace with wrapper around core module |
| `src/kafka_pipeline/common/eventhouse/kql_client.py` | Use core auth module |
| `src/kafka_pipeline/common/eventhouse/dedup.py` | Already uses core module ✅ |
| `src/kafka_pipeline/common/storage/delta.py` | Will use core via updated auth.py |

## Priority

**Medium** - Not blocking, but causes maintenance overhead and risk of future bugs.

## Related Issues

- BOM handling fix (2026-01-04)
- Token refresh script (`refresh_tokens.ps1`)
