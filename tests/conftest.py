"""
pytest configuration for core library tests.

Adds src directory to Python path for imports.
"""

import sys
from pathlib import Path

# Add src directory to Python path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))
