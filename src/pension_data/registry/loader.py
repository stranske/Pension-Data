"""Registry loaders and validation for pension-system identity records."""

from __future__ import annotations

import csv
import hashlib
import re
from collections.abc import Iterable, Mapping
from pathlib import Path

from pension_data.db.models.registry import PensionSystemRecord

REQUIRED_COLUMNS: tuple[str, ...] = (
    "stable_id",
    "legal_name",
    "short_name",
    "system_type",
    "jurisdiction",
    "jurisdiction_type",
    "in_state_employee_universe",
    "in_sampled_50",
)


class RegistryValidationError(ValueError):
    """Raised when registry input violates schema or identity constraints."""


def normalize_identity_key(*parts: str) -> str:
    """Normalize registry identity tokens into a stable lowercase key."""
    combined = "::".join(parts).strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", combined).strip("-")


def make_stable_id(*parts: str) -> str:
    """Generate a deterministic fallback stable id when seed id is absent."""
    digest = hashlib.sha1("::".join(parts).encode("utf-8")).hexdigest()
    return f"ps-{digest[:12]}"


def parse_bool(value: str, *, column: str) -> bool:
    """Parse deterministic boolean values from registry seed files."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise RegistryValidationError(f"column '{column}' must be a boolean, got '{value}'")


def _require_columns(row: Mapping[str, str], *, row_number: int) -> None:
    missing = [
        column
        for column in REQUIRED_COLUMNS
        if (column not in row) or (row[column] is None) or (not row[column].strip())
    ]
    if missing:
        raise RegistryValidationError(
            f"row {row_number} missing required fields: {', '.join(sorted(missing))}"
        )


def _record_from_row(row: Mapping[str, str], *, row_number: int) -> PensionSystemRecord:
    _require_columns(row, row_number=row_number)
    legal_name = row["legal_name"].strip()
    short_name = row["short_name"].strip()
    jurisdiction = row["jurisdiction"].strip()
    jurisdiction_type = row["jurisdiction_type"].strip()
    system_type = row["system_type"].strip()
    stable_id = row["stable_id"].strip() or make_stable_id(legal_name, jurisdiction)
    identity_key = normalize_identity_key(jurisdiction, legal_name, system_type)
    return PensionSystemRecord(
        stable_id=stable_id,
        legal_name=legal_name,
        short_name=short_name,
        system_type=system_type,
        jurisdiction=jurisdiction,
        jurisdiction_type=jurisdiction_type,
        identity_key=identity_key,
        in_state_employee_universe=parse_bool(
            row["in_state_employee_universe"], column="in_state_employee_universe"
        ),
        in_sampled_50=parse_bool(row["in_sampled_50"], column="in_sampled_50"),
    )


def validate_metadata_completeness(records: Iterable[PensionSystemRecord]) -> None:
    """Validate required metadata completeness for registry rows."""
    errors: list[str] = []
    for record in records:
        missing: list[str] = []
        if not record.stable_id.strip():
            missing.append("stable_id")
        if not record.legal_name.strip():
            missing.append("legal_name")
        if not record.short_name.strip():
            missing.append("short_name")
        if not record.system_type.strip():
            missing.append("system_type")
        if not record.jurisdiction.strip():
            missing.append("jurisdiction")
        if not record.jurisdiction_type.strip():
            missing.append("jurisdiction_type")
        if not record.identity_key.strip():
            missing.append("identity_key")
        if missing:
            errors.append(
                f"{record.stable_id or '<missing-stable-id>'} missing fields: {', '.join(missing)}"
            )
    if errors:
        raise RegistryValidationError("\n".join(sorted(errors)))


def apply_registry_updates(
    base_records: Iterable[PensionSystemRecord],
    updates: Iterable[PensionSystemRecord],
) -> list[PensionSystemRecord]:
    """Apply incremental record updates with uniqueness and idempotency checks."""
    by_stable_id: dict[str, PensionSystemRecord] = {
        record.stable_id: record for record in base_records
    }
    identity_to_id: dict[str, str] = {
        record.identity_key: record.stable_id for record in by_stable_id.values()
    }

    for update in updates:
        if update.stable_id in by_stable_id:
            if by_stable_id[update.stable_id] != update:
                raise RegistryValidationError(
                    f"stable_id '{update.stable_id}' conflicts with existing registry payload"
                )
            continue

        if update.identity_key in identity_to_id:
            existing_id = identity_to_id[update.identity_key]
            raise RegistryValidationError(
                f"identity_key '{update.identity_key}' already mapped to stable_id '{existing_id}'"
            )

        by_stable_id[update.stable_id] = update
        identity_to_id[update.identity_key] = update.stable_id

    merged = [by_stable_id[key] for key in sorted(by_stable_id.keys())]
    validate_metadata_completeness(merged)
    return merged


def load_registry_from_seed(
    seed_path: str | Path,
    *,
    existing_records: Iterable[PensionSystemRecord] | None = None,
) -> list[PensionSystemRecord]:
    """Load canonical registry records from seed CSV with deterministic ordering."""
    path = Path(seed_path)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        records = [_record_from_row(row, row_number=index + 2) for index, row in enumerate(reader)]

    base = list(existing_records) if existing_records is not None else []
    return apply_registry_updates(base, records)
