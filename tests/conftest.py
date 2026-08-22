# conftest.py — service packages imported via uv workspace deps (see tests/pyproject.toml)
import os
import sys

# Make spark-jobs scripts importable (bootstrap, streaming_job, etc.)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "spark-jobs"))

# Make API service scripts importable (main, deps, etc.)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "api"))
