"""Tests for system type lookup from registry seed."""

from __future__ import annotations

from pathlib import Path

from pension_data.registry.system_type_lookup import load_system_type_by_plan_id


def test_default_seed_loads_all_registered_systems() -> None:
    mapping = load_system_type_by_plan_id()
    assert len(mapping) > 0
    assert "ps-ca-calpers" in mapping
    assert mapping["ps-ca-calpers"] == "public-pension"


def test_ps_prefix_stripping_creates_both_entries() -> None:
    mapping = load_system_type_by_plan_id()
    assert "ps-ca-calpers" in mapping
    assert "ca-calpers" in mapping
    assert mapping["ps-ca-calpers"] == mapping["ca-calpers"]


def test_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    result = load_system_type_by_plan_id(seed_path=tmp_path / "nonexistent.csv")
    assert result == {}


def test_explicit_seed_path(tmp_path: Path) -> None:
    seed = tmp_path / "test_registry.csv"
    seed.write_text(
        "stable_id,system_type,legal_name,short_name,jurisdiction,jurisdiction_type,"
        "in_state_employee_universe,in_sampled_50\n"
        "ps-test-system,teacher-pension,Test System,TS,TestState,state,true,true\n",
        encoding="utf-8",
    )
    mapping = load_system_type_by_plan_id(seed_path=seed)
    assert mapping["ps-test-system"] == "teacher-pension"
    assert mapping["test-system"] == "teacher-pension"


def test_teacher_pension_type_is_mapped() -> None:
    mapping = load_system_type_by_plan_id()
    assert mapping.get("ps-il-trs") == "teacher-pension"
