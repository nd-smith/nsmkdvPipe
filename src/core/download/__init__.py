"""
Async download module.

Provides HTTP download logic decoupled from storage backends.

Components to extract and review from verisk_pipeline.xact.stages.xact_download:
    - Async HTTP download with aiohttp
    - Streaming for large files (>50MB)
    - Chunked transfer handling
    - Content-Length validation
    - Retry-After header parsing

New functionality to add:
    - Clean interface (DownloadTask -> DownloadOutcome)
    - Progress callbacks for observability
    - Configurable timeouts per-request

Review checklist:
    [ ] Memory usage is bounded for large files
    [ ] Timeouts are properly configured
    [ ] Connection pooling is efficient
    [ ] Error classification is correct
    [ ] Partial downloads are handled (resume or fail cleanly)
    [ ] SSL verification is enabled
"""
