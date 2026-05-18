"""Shared pytest fixtures for the AI agent runtime tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the project root importable so `import ai_agent...` works under pytest.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture
def athena_rows():
    """Default canned Athena result rows used across tool tests."""
    return [
        {"title": "World Cup Final", "content_type": "Sports", "total_views": "900", "unique_viewers": "700"},
        {"title": "Mystery Manor", "content_type": "Movie", "total_views": "400", "unique_viewers": "350"},
    ]
