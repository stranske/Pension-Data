"""Emit deterministic ``identity-map/v1`` cross-walk artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pension_data.normalize.entity_tokens import normalize_entity_token

_PLACEHOLDERS = {
    "not disclosed",
    "unknown",
    "unknown fund",
    "unknown manager",
}


def _normalized(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    token = normalize_entity_token(value)
    if not token or token in _PLACEHOLDERS:
        return None
    return token


def _canonical_id(entity_type: str, value: str) -> str:
    return f"{entity_type}:{value.replace(' ', '_')}"


def _observed_values(rows: Sequence[Mapping[str, object]], field: str) -> set[str]:
    values: set[str] = set()
    for row in rows:
        value = row.get(field)
        if isinstance(value, str) and _normalized(value) is not None:
            values.add(value.strip())
    return values


def _build_map(
    *,
    entity_type: str,
    values: Sequence[str],
    native_key: str,
    published_at: str,
    confidence: float,
) -> dict[str, Any] | None:
    grouped: dict[str, set[str]] = {}
    for value in values:
        normalized = _normalized(value)
        if normalized is None:
            continue
        grouped.setdefault(_canonical_id(entity_type, normalized), set()).add(value.strip())
    if not grouped:
        return None
    entries: list[dict[str, Any]] = []
    for canonical_id, native_values in sorted(grouped.items()):
        ordered_native = sorted(native_values, key=lambda item: (item.lower(), item))
        aliases = sorted(
            normalized for value in ordered_native if (normalized := _normalized(value)) is not None
        )
        entries.append(
            {
                "canonical_id": canonical_id,
                "aliases": aliases,
                "native_keys": {native_key: ordered_native[0]},
                "supersedes_ids": [],
                "confidence": confidence,
            }
        )
    return {
        "schema_version": "identity-map/v1",
        "publisher": "stranske/Pension-Data",
        "published_at": published_at,
        "entity_type": entity_type,
        "entries": entries,
    }


def build_identity_maps(
    *,
    pilot_input: Mapping[str, object],
    core_rows: Sequence[Mapping[str, object]],
    relationship_rows: Sequence[Mapping[str, object]],
    published_at: str,
) -> list[dict[str, Any]]:
    """Build maps only for pension, manager, and fund tokens observed in this run."""
    rows = [*core_rows, *relationship_rows]
    plan_values = _observed_values(rows, "plan_id")
    plan_id = pilot_input.get("plan_id")
    if isinstance(plan_id, str) and _normalized(plan_id) is not None:
        plan_values.add(plan_id.strip())
    candidates = (
        ("pension", sorted(plan_values), "plan_id", 1.0),
        ("manager", sorted(_observed_values(rows, "manager_name")), "manager_name", 0.5),
        ("fund", sorted(_observed_values(rows, "fund_name")), "fund_name", 0.5),
    )
    maps = [
        identity_map
        for entity_type, values, native_key, confidence in candidates
        if (
            identity_map := _build_map(
                entity_type=entity_type,
                values=values,
                native_key=native_key,
                published_at=published_at,
                confidence=confidence,
            )
        )
        is not None
    ]
    return sorted(maps, key=lambda item: str(item["entity_type"]))


def identity_refs_from_maps(identity_maps: Sequence[Mapping[str, object]]) -> list[str]:
    """Return the sorted canonical IDs published by identity maps."""
    refs: set[str] = set()
    for identity_map in identity_maps:
        entries = identity_map.get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            canonical_id = entry.get("canonical_id")
            if isinstance(canonical_id, str) and canonical_id:
                refs.add(canonical_id)
    return sorted(refs)
