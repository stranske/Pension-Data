"""Tests for v1 cohort filtering."""

from __future__ import annotations

from pension_data.db.models.registry import PensionSystemRecord
from pension_data.registry.cohort import filter_v1_cohort


def _make_record(
    stable_id: str,
    in_state_employee_universe: bool = False,
    in_sampled_50: bool = False,
) -> PensionSystemRecord:
    return PensionSystemRecord(
        stable_id=stable_id,
        legal_name=f"System {stable_id}",
        short_name=stable_id.upper(),
        system_type="public-pension",
        jurisdiction="TestState",
        jurisdiction_type="state",
        identity_key=f"teststate-system-{stable_id}-public-pension",
        in_state_employee_universe=in_state_employee_universe,
        in_sampled_50=in_sampled_50,
    )


def test_no_flags_returns_all_sorted() -> None:
    records = [_make_record("c"), _make_record("a"), _make_record("b")]
    result = filter_v1_cohort(records)
    assert [r.stable_id for r in result] == ["a", "b", "c"]


def test_state_employee_only_filters() -> None:
    records = [
        _make_record("a", in_state_employee_universe=True),
        _make_record("b", in_state_employee_universe=False),
        _make_record("c", in_state_employee_universe=True),
    ]
    result = filter_v1_cohort(records, state_employee_only=True)
    assert [r.stable_id for r in result] == ["a", "c"]


def test_sampled_50_only_filters() -> None:
    records = [
        _make_record("a", in_sampled_50=True),
        _make_record("b", in_sampled_50=False),
        _make_record("c", in_sampled_50=True),
    ]
    result = filter_v1_cohort(records, sampled_50_only=True)
    assert [r.stable_id for r in result] == ["a", "c"]


def test_both_flags_combined() -> None:
    records = [
        _make_record("a", in_state_employee_universe=True, in_sampled_50=True),
        _make_record("b", in_state_employee_universe=True, in_sampled_50=False),
        _make_record("c", in_state_employee_universe=False, in_sampled_50=True),
        _make_record("d", in_state_employee_universe=True, in_sampled_50=True),
    ]
    result = filter_v1_cohort(records, state_employee_only=True, sampled_50_only=True)
    assert [r.stable_id for r in result] == ["a", "d"]


def test_empty_input_returns_empty() -> None:
    assert filter_v1_cohort([]) == []
