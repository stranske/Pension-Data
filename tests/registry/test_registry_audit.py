"""Tests for registry audit output generation."""

from __future__ import annotations

from pension_data.db.models.registry import PensionSystemRecord
from pension_data.registry.audit import build_registry_audit


def _make_record(
    stable_id: str,
    system_type: str = "public-pension",
    jurisdiction: str = "California",
    in_state_employee_universe: bool = True,
    in_sampled_50: bool = True,
) -> PensionSystemRecord:
    return PensionSystemRecord(
        stable_id=stable_id,
        legal_name=f"System {stable_id}",
        short_name=stable_id.upper(),
        system_type=system_type,
        jurisdiction=jurisdiction,
        jurisdiction_type="state",
        identity_key=f"{jurisdiction.lower()}-system-{stable_id}-{system_type}",
        in_state_employee_universe=in_state_employee_universe,
        in_sampled_50=in_sampled_50,
    )


def test_empty_input_returns_zero_counts() -> None:
    audit = build_registry_audit([])
    assert audit == {
        "total_records": 0,
        "counts_by_system_type": {},
        "counts_by_jurisdiction": {},
        "counts_by_cohort_segment": {},
    }


def test_mixed_system_types_and_jurisdictions() -> None:
    records = [
        _make_record("a", system_type="public-pension", jurisdiction="California"),
        _make_record("b", system_type="teacher-pension", jurisdiction="Illinois"),
        _make_record("c", system_type="public-pension", jurisdiction="California"),
    ]
    audit = build_registry_audit(records)
    assert audit["total_records"] == 3
    assert audit["counts_by_system_type"] == {"public-pension": 2, "teacher-pension": 1}
    assert audit["counts_by_jurisdiction"] == {"California": 2, "Illinois": 1}


def test_cohort_segments_are_computed_correctly() -> None:
    records = [
        _make_record("a", in_state_employee_universe=True, in_sampled_50=True),
        _make_record("b", in_state_employee_universe=True, in_sampled_50=False),
        _make_record("c", in_state_employee_universe=False, in_sampled_50=True),
        _make_record("d", in_state_employee_universe=False, in_sampled_50=False),
    ]
    audit = build_registry_audit(records)
    assert audit["counts_by_cohort_segment"] == {
        "outside_v1": 1,
        "sampled_50_only": 1,
        "state_employee_only": 1,
        "state_employee_sampled_50": 1,
    }


def test_output_keys_are_deterministically_sorted() -> None:
    records = [
        _make_record("z", jurisdiction="Zephyr"),
        _make_record("a", jurisdiction="Alpha"),
        _make_record("m", jurisdiction="Middle"),
    ]
    audit = build_registry_audit(records)
    jurisdictions = list(audit["counts_by_jurisdiction"].keys())
    assert jurisdictions == sorted(jurisdictions)
