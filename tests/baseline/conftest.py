"""Fixtures and catalog loading for the Pension-Data baseline kit."""

from __future__ import annotations

import functools
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
SRC_ROOT = REPO_ROOT / "src"
CATALOG_PATH = HERE / "catalog.yaml"

# Ensure the package is importable under pytest (mirrors pyproject pythonpath).
for candidate in (SRC_ROOT, REPO_ROOT):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


@functools.lru_cache(maxsize=1)
def load_catalog_cached():
    from baseline_kit import load_catalog

    return load_catalog(CATALOG_PATH)


def base_fixture() -> dict:
    """The base fixture from the catalog (deep-copied per call downstream)."""
    return load_catalog_cached()["base"]


def scenarios_by_id() -> dict[str, dict]:
    return {s["id"]: s for s in load_catalog_cached()["scenarios"]}


@pytest.fixture(scope="session")
def catalog():
    return load_catalog_cached()
