# verisk_pipeline/__main__.py
"""Entry point for python -m verisk_pipeline."""
import os

os.environ["RUST_LOG"] = "error"
os.environ["POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY"] = "1"
import sys
from verisk_pipeline.main import main

if __name__ == "__main__":
    sys.exit(main())
