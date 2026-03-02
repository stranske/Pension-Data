"""Tests for registry loading, validation, cohort filters, and audits."""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.db.models.registry import (
    PENSION_SYSTEM_BASE_FIELDS,
    PENSION_SYSTEM_SCHEMA_FIELDS,
    PENSION_SYSTEM_SCHEMA_VERSION,
    PensionSystemRecord,
    V1CohortMembership,
)
from pension_data.registry import (
    RegistryValidationError,
    apply_registry_updates,
    build_registry_audit,
    filter_v1_cohort,
    load_registry_from_seed,
)
from pension_data.registry.loader import normalize_identity_key

SEED_PATH = Path(__file__).resolve().parents[2] / "config" / "registry" / "pension_systems_v1.csv"


def test_pension_system_schema_v1_defines_canonical_identity_fields() -> None:
    assert PENSION_SYSTEM_SCHEMA_VERSION == "v1"
    assert PENSION_SYSTEM_BASE_FIELDS == (
        "stable_id",
        "legal_name",
        "short_name",
        "system_type",
        "jurisdiction",
    )
    assert (
        PENSION_SYSTEM_SCHEMA_FIELDS[: len(PENSION_SYSTEM_BASE_FIELDS)]
        == PENSION_SYSTEM_BASE_FIELDS
    )


def test_registry_persists_stable_ids_for_all_seeded_systems() -> None:
    records = load_registry_from_seed(SEED_PATH)
    stable_ids = [record.stable_id for record in records]
    assert len(stable_ids) == len(set(stable_ids))
    assert all(stable_id.startswith("ps-") for stable_id in stable_ids)
    assert stable_ids == sorted(stable_ids)


def test_v1_cohort_filters_are_deterministic() -> None:
    records = load_registry_from_seed(SEED_PATH)

    state_only = filter_v1_cohort(records, state_employee_only=True)
    sampled_only = filter_v1_cohort(records, sampled_50_only=True)
    intersection = filter_v1_cohort(
        records,
        state_employee_only=True,
        sampled_50_only=True,
    )

    assert len(state_only) == 5
    assert len(sampled_only) == 4
    assert len(intersection) == 4
    assert [record.stable_id for record in intersection] == sorted(
        record.stable_id for record in intersection
    )


def test_v1_cohort_membership_flags_are_explicit_on_records() -> None:
    records = load_registry_from_seed(SEED_PATH)
    calpers = next(record for record in records if record.stable_id == "ps-ca-calpers")
    copera = next(record for record in records if record.stable_id == "ps-co-pera")

    assert calpers.cohort == V1CohortMembership(
        in_state_employee_universe=True,
        in_sampled_50=True,
    )
    assert copera.cohort == V1CohortMembership(
        in_state_employee_universe=True,
        in_sampled_50=False,
    )


def test_loader_is_idempotent() -> None:
    first_load = load_registry_from_seed(SEED_PATH)
    second_load = load_registry_from_seed(SEED_PATH, existing_records=first_load)
    assert second_load == first_load


def test_loader_fails_on_missing_required_fields(tmp_path: Path) -> None:
    bad_seed = tmp_path / "bad_registry.csv"
    bad_seed.write_text(
        "\n".join(
            [
                (
                    "stable_id,legal_name,short_name,system_type,jurisdiction,"
                    "jurisdiction_type,in_state_employee_universe,in_sampled_50"
                ),
                "ps-test,,Test,public-pension,Testland,state,true,true",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(RegistryValidationError, match="missing required fields: legal_name"):
        load_registry_from_seed(bad_seed)


def test_uniqueness_rules_reject_identity_key_collisions() -> None:
    records = load_registry_from_seed(SEED_PATH)
    existing = records[0]
    collision = PensionSystemRecord(
        stable_id="ps-collision",
        legal_name=existing.legal_name,
        short_name="Collision",
        system_type=existing.system_type,
        jurisdiction=existing.jurisdiction,
        jurisdiction_type=existing.jurisdiction_type,
        identity_key=normalize_identity_key(
            existing.jurisdiction, existing.legal_name, existing.system_type
        ),
        in_state_employee_universe=True,
        in_sampled_50=False,
    )

    with pytest.raises(RegistryValidationError, match="identity_key"):
        apply_registry_updates(records, [collision])


def test_base_registry_duplicate_constraints_are_enforced() -> None:
    records = load_registry_from_seed(SEED_PATH)
    duplicate_stable_id = [
        records[0],
        PensionSystemRecord(
            stable_id=records[0].stable_id,
            legal_name="Duplicate Stable ID System",
            short_name="DupStable",
            system_type="public-pension",
            jurisdiction="Testland",
            jurisdiction_type="state",
            identity_key="testland-duplicate-stable-id-system-public-pension",
            in_state_employee_universe=False,
            in_sampled_50=False,
        ),
    ]
    with pytest.raises(RegistryValidationError, match="duplicate stable_id"):
        apply_registry_updates(duplicate_stable_id, [])

    duplicate_identity_key = [
        records[0],
        PensionSystemRecord(
            stable_id="ps-duplicate-identity",
            legal_name="Different Name",
            short_name="DupIdentity",
            system_type=records[0].system_type,
            jurisdiction=records[0].jurisdiction,
            jurisdiction_type=records[0].jurisdiction_type,
            identity_key=records[0].identity_key,
            in_state_employee_universe=False,
            in_sampled_50=False,
        ),
    ]
    with pytest.raises(RegistryValidationError, match="identity_key"):
        apply_registry_updates(duplicate_identity_key, [])


def test_registry_audit_output_counts_by_type_jurisdiction_and_segment() -> None:
    sampled_only_fixture = PensionSystemRecord(
        stable_id="ps-sampled-only",
        legal_name="Sampled Only Pension",
        short_name="SampledOnly",
        system_type="public-pension",
        jurisdiction="Sample State",
        jurisdiction_type="state",
        identity_key="sample-state-sampled-only-pension-public-pension",
        in_state_employee_universe=False,
        in_sampled_50=True,
    )
    audit = build_registry_audit(load_registry_from_seed(SEED_PATH) + [sampled_only_fixture])
    assert audit == {
        "total_records": 7,
        "counts_by_system_type": {
            "public-pension": 6,
            "teacher-pension": 1,
        },
        "counts_by_jurisdiction": {
            "American Samoa": 1,
            "California": 1,
            "Colorado": 1,
            "Illinois": 1,
            "New York": 1,
            "Sample State": 1,
            "Texas": 1,
        },
        "counts_by_cohort_segment": {
            "outside_v1": 1,
            "sampled_50_only": 1,
            "state_employee_only": 1,
            "state_employee_sampled_50": 4,
        },
    }
