"""Registry loaders and validation for pension-system identity records."""

from __future__ import annotations

import csv
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import cast

from pension_data.db.models.registry import JurisdictionType, PensionSystemRecord, SystemType

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
VALID_SYSTEM_TYPES: tuple[SystemType, ...] = ("public-pension", "teacher-pension")
VALID_JURISDICTION_TYPES: tuple[JurisdictionType, ...] = ("state", "territory")


class RegistryValidationError(ValueError):
    """Raised when registry input violates schema or identity constraints."""


def normalize_identity_key(*parts: str) -> str:
    """Normalize registry identity tokens into a stable lowercase key."""
    combined = "::".join(parts).strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", combined).strip("-")


def parse_bool(value: str, *, column: str) -> bool:
    """Parse deterministic boolean values from registry seed files."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise RegistryValidationError(f"column '{column}' must be a boolean, got '{value}'")


def parse_system_type(value: str, *, row_number: int) -> SystemType:
    """Parse and validate registry system_type literals."""
    normalized = value.strip()
    if normalized not in VALID_SYSTEM_TYPES:
        expected = ", ".join(VALID_SYSTEM_TYPES)
        raise RegistryValidationError(
            f"row {row_number} has invalid system_type '{value}'; expected one of: {expected}"
        )
    return cast(SystemType, normalized)


def parse_jurisdiction_type(value: str, *, row_number: int) -> JurisdictionType:
    """Parse and validate registry jurisdiction_type literals."""
    normalized = value.strip()
    if normalized not in VALID_JURISDICTION_TYPES:
        expected = ", ".join(VALID_JURISDICTION_TYPES)
        raise RegistryValidationError(
            f"row {row_number} has invalid jurisdiction_type '{value}'; "
            f"expected one of: {expected}"
        )
    return cast(JurisdictionType, normalized)


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
    jurisdiction_type = parse_jurisdiction_type(row["jurisdiction_type"], row_number=row_number)
    system_type = parse_system_type(row["system_type"], row_number=row_number)
    stable_id = row["stable_id"].strip()
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


def _validate_identity_key_normalization(record: PensionSystemRecord) -> None:
    expected_identity_key = normalize_identity_key(
        record.jurisdiction, record.legal_name, record.system_type
    )
    if record.identity_key != expected_identity_key:
        raise RegistryValidationError(
            f"stable_id '{record.stable_id}' has non-canonical identity_key "
            f"'{record.identity_key}'; expected '{expected_identity_key}'"
        )


def apply_registry_updates(
    base_records: Iterable[PensionSystemRecord],
    updates: Iterable[PensionSystemRecord],
) -> list[PensionSystemRecord]:
    """Apply incremental record updates with uniqueness and idempotency checks."""
    by_stable_id: dict[str, PensionSystemRecord] = {}
    identity_to_id: dict[str, str] = {}

    for record in base_records:
        _validate_identity_key_normalization(record)
        if record.stable_id in by_stable_id:
            raise RegistryValidationError(
                f"duplicate stable_id '{record.stable_id}' in base registry records"
            )
        if record.identity_key in identity_to_id:
            existing_id = identity_to_id[record.identity_key]
            raise RegistryValidationError(
                f"identity_key '{record.identity_key}' already mapped to stable_id "
                f"'{existing_id}' in base registry records"
            )
        by_stable_id[record.stable_id] = record
        identity_to_id[record.identity_key] = record.stable_id

    for update in updates:
        _validate_identity_key_normalization(update)
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
