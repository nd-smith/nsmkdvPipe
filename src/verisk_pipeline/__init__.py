"""Verisk Pipeline - event processing and enrichment."""

import os

os.environ["RUST_LOG"] = "error"
os.environ["POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY"] = "1"

__version__ = "1.0.0"

# Essential exports only
__all__ = ["__version__"]
