"""Definition loader for versioned saved analytical views."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SavedViewField:
    """Typed output field specification for a saved view."""

    name: str
    field_type: str


@dataclass(frozen=True, slots=True)
class SavedViewDefinition:
    """Versioned canonical definition for one saved analytical view."""

    view_name: str
    version: str
    description: str
    sql: str
    assumptions: tuple[str, ...]
    output_schema: tuple[SavedViewField, ...]

    @property
    def key(self) -> str:
        """Stable key `<view_name>:<version>` used in registries."""
        return f"{self.view_name}:{self.version}"


def _project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    msg = "Unable to locate project root containing pyproject.toml"
    raise ValueError(msg)


def _saved_query_dir(config_dir: Path | None) -> Path:
    if config_dir is not None:
        return config_dir
    root = _project_root(Path(__file__).resolve())
    return root / "config" / "saved_queries"


def _require_str(payload: dict[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        msg = f"Saved view definition missing non-empty string field: {field}"
        raise ValueError(msg)
    return value


def load_saved_view_definitions(config_dir: Path | None = None) -> dict[str, SavedViewDefinition]:
    """Load saved view definitions from JSON artifacts under `config/saved_queries`."""
    definition_dir = _saved_query_dir(config_dir)
    definitions: dict[str, SavedViewDefinition] = {}

    for path in sorted(definition_dir.glob("*_v*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            msg = f"Definition payload must be an object: {path}"
            raise ValueError(msg)

        assumptions_raw = payload.get("assumptions", [])
        output_raw = payload.get("output_schema", [])
        if not isinstance(assumptions_raw, list) or not all(
            isinstance(value, str) for value in assumptions_raw
        ):
            msg = f"`assumptions` must be a string list: {path}"
            raise ValueError(msg)
        if not isinstance(output_raw, list):
            msg = f"`output_schema` must be a list: {path}"
            raise ValueError(msg)

        output_schema: list[SavedViewField] = []
        for row in output_raw:
            if not isinstance(row, dict):
                msg = f"output_schema entries must be objects: {path}"
                raise ValueError(msg)
            output_schema.append(
                SavedViewField(
                    name=_require_str(row, "name"),
                    field_type=_require_str(row, "type"),
                )
            )

        definition = SavedViewDefinition(
            view_name=_require_str(payload, "view_name"),
            version=_require_str(payload, "version"),
            description=_require_str(payload, "description"),
            sql=_require_str(payload, "sql"),
            assumptions=tuple(assumptions_raw),
            output_schema=tuple(output_schema),
        )
        if definition.key in definitions:
            msg = f"Duplicate saved view key: {definition.key}"
            raise ValueError(msg)
        definitions[definition.key] = definition

    return definitions
