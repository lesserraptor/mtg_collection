"""Shared project configuration constants."""

from pathlib import Path

# Absolute path to the project root (the directory containing src/).
# Derived from this file's location: src/config.py -> parent -> project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Runtime data directory — holds DB, Scryfall cache, and other generated files.
# Created on first use by consumers.
DATA_DIR = PROJECT_ROOT / "data"
