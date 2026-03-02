"""Helpers for resolving plan-level system types from the registry seed."""

from __future__ import annotations

import csv
from pathlib import Path


def _project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    msg = "Unable to locate project root containing pyproject.toml"
    raise ValueError(msg)


def _default_registry_seed_path() -> Path:
    root = _project_root(Path(__file__).resolve())
    return root / "config" / "registry" / "pension_systems_v1.csv"


def load_system_type_by_plan_id(seed_path: Path | None = None) -> dict[str, str]:
    """Load lowercase stable-id-derived identifiers to system_type from registry seed."""
    if seed_path is not None:
        path = seed_path
    else:
        try:
            path = _default_registry_seed_path()
        except ValueError:
            # Installed/runtime contexts may not have a project-root pyproject.toml.
            return {}
    if not path.exists() or not path.is_file():
        return {}

    mapping: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            stable_id = (row.get("stable_id") or "").strip().lower()
            system_type = (row.get("system_type") or "").strip()
            if not stable_id or not system_type:
                continue
            if stable_id.startswith("ps-"):
                mapping[stable_id[3:]] = system_type
            mapping[stable_id] = system_type

    return mapping
