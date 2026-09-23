"""Stage Doc-Lineage ``tracked-variable/v1`` artifacts for pension joins."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCHEMA_ROOT = _REPO_ROOT / "docs" / "contracts" / "schemas"
_TRACKED_VARIABLE_SCHEMA = _SCHEMA_ROOT / "tracked-variable-v1.schema.json"
_EVIDENCE_SCHEMA = _SCHEMA_ROOT / "evidence-object-v1.schema.json"


@dataclass(frozen=True, slots=True)
class DocLineageVariableRow:
    """One validated tracked variable ready for Pension-Data staging joins."""

    variable_id: str
    ontology_key: str
    entity_ref: str
    ontology_family: str | None
    period: str | None
    status: str | None
    value_text: str | None
    value_structured: Mapping[str, Any] | None
    confidence: float | None
    supersedes_variable_id: str | None
    evidence: Mapping[str, Any]
    provenance: Mapping[str, Any]


def _load_schema(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):  # pragma: no cover - repository integrity guard
        raise RuntimeError(f"schema at {path} must be a JSON object")
    return payload


def _validator() -> Draft202012Validator:
    tracked_schema = copy.deepcopy(_load_schema(_TRACKED_VARIABLE_SCHEMA))
    evidence_schema = _load_schema(_EVIDENCE_SCHEMA)
    properties = tracked_schema.get("properties")
    if not isinstance(properties, dict):  # pragma: no cover - repository integrity guard
        raise RuntimeError("tracked-variable schema must define object properties")
    properties["evidence"] = evidence_schema
    Draft202012Validator.check_schema(tracked_schema)
    return Draft202012Validator(tracked_schema)


def _non_empty_string(payload: Mapping[str, Any], field: str, *, index: int) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"tracked variable at index {index} requires non-empty {field}")
    return value.strip()


def _optional_string(payload: Mapping[str, Any], field: str) -> str | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"tracked variable field {field} must be a string or null")
    stripped = value.strip()
    return stripped or None


def _objects(payload: object) -> list[Mapping[str, Any]]:
    candidates: Sequence[object]
    if isinstance(payload, Mapping):
        candidates = [payload]
    elif isinstance(payload, list):
        candidates = payload
    else:
        raise ValueError("tracked-variable input must be a JSON object or array of objects")

    objects: list[Mapping[str, Any]] = []
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, Mapping):
            raise ValueError(f"tracked variable at index {index} must be a JSON object")
        objects.append(candidate)
    return objects


def stage_tracked_variables(payload: object) -> tuple[DocLineageVariableRow, ...]:
    """Validate and normalize one Doc-Lineage variable object or array."""
    validator = _validator()
    rows: list[DocLineageVariableRow] = []
    seen_ids: set[str] = set()

    for index, item in enumerate(_objects(payload)):
        errors = sorted(validator.iter_errors(item), key=lambda error: list(error.absolute_path))
        if errors:
            error = errors[0]
            location = ".".join(str(part) for part in error.absolute_path) or "<root>"
            raise ValueError(
                f"tracked variable at index {index} fails tracked-variable/v1 "
                f"at {location}: {error.message}"
            )

        variable_id = _non_empty_string(item, "variable_id", index=index)
        ontology_key = _non_empty_string(item, "ontology_key", index=index)
        entity_ref = _non_empty_string(item, "entity_ref", index=index)
        if variable_id in seen_ids:
            raise ValueError(f"duplicate tracked variable_id: {variable_id}")
        seen_ids.add(variable_id)

        evidence = item["evidence"]
        provenance = item["provenance"]
        if not isinstance(evidence, Mapping) or not isinstance(provenance, Mapping):
            raise ValueError(f"tracked variable at index {index} has invalid evidence/provenance")

        value_structured = item.get("value_structured")
        if value_structured is not None and not isinstance(value_structured, Mapping):
            raise ValueError("tracked variable field value_structured must be an object or null")

        confidence = item.get("confidence")
        rows.append(
            DocLineageVariableRow(
                variable_id=variable_id,
                ontology_key=ontology_key,
                entity_ref=entity_ref,
                ontology_family=_optional_string(item, "ontology_family"),
                period=_optional_string(item, "period"),
                status=_optional_string(item, "status"),
                value_text=_optional_string(item, "value_text"),
                value_structured=(dict(value_structured) if value_structured is not None else None),
                confidence=(float(confidence) if confidence is not None else None),
                supersedes_variable_id=_optional_string(item, "supersedes_variable_id"),
                evidence=dict(evidence),
                provenance=dict(provenance),
            )
        )

    return tuple(rows)


def import_tracked_variables(path: str | Path) -> tuple[DocLineageVariableRow, ...]:
    """Load a UTF-8 Doc-Lineage JSON artifact into validated staging rows."""
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load tracked-variable JSON from {source}: {exc}") from exc
    return stage_tracked_variables(payload)
