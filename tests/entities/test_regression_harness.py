"""Tests for entity regression fixture loading and deterministic diff output."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from tools.entity_regression.harness import (
    ENTITY_REGRESSION_ARTIFACT_TYPE,
    evaluate_lineage_case,
    load_fixture,
    run_entity_regression,
)

FIXTURE_PATH = Path(__file__).parent / "golden" / "entity_regression_cases.json"


def test_entity_regression_fixture_has_zero_mismatches() -> None:
    fixture = load_fixture(FIXTURE_PATH)
    report = run_entity_regression(
        fixture,
        generated_at=datetime(2026, 3, 2, tzinfo=UTC),
    )

    assert report["artifact_type"] == ENTITY_REGRESSION_ARTIFACT_TYPE
    assert report["schema_version"] == 1
    assert report["generated_at"] == "2026-03-02T00:00:00+00:00"
    assert report["total_cases"] == 7
    assert report["regressions"] == 0
    assert report["mismatches"] == []


def test_entity_regression_detects_expected_mismatch(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    payload["alias_cases"][0]["expected"]["entity_id"] = "mgr:wrong_entity"
    modified = tmp_path / "entity_regression_cases_modified.json"
    modified.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fixture = load_fixture(modified)
    report = run_entity_regression(fixture)

    assert report["regressions"] == 1
    mismatch = report["mismatches"][0]
    assert mismatch["suite"] == "alias"
    assert mismatch["case_id"] == "exact_alias_variant_auto_match"
    assert mismatch["field"] == "entity_id"
    assert mismatch["expected"] == "mgr:wrong_entity"
    assert mismatch["observed"] == "mgr:alpha_capital"


def test_lineage_split_case_emits_two_terminals() -> None:
    fixture = load_fixture(FIXTURE_PATH)
    split_case = [case for case in fixture.lineage_cases if case.case_id == "split_lineage_two_terminals"][
        0
    ]

    result = evaluate_lineage_case(split_case)
    assert result.reachable_entities == (
        "inv:credit_strategy_fund",
        "inv:equity_strategy_fund",
        "inv:multi_strategy_fund",
    )
    assert result.terminal_entities == (
        "inv:credit_strategy_fund",
        "inv:equity_strategy_fund",
    )
