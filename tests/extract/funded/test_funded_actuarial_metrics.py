"""Tests for funded/actuarial extraction, normalization, and diagnostics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pension_data.extract.actuarial.metrics import (
    PARSER_VERSION,
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.extract.funded.status import extract_funded_status_metrics

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "funded_actuarial_golden.json"


def _load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _raw(payload: dict[str, Any]) -> RawFundedActuarialInput:
    return RawFundedActuarialInput(
        source_document_id=payload["source_document_id"],
        source_url=payload["source_url"],
        effective_date=payload["effective_date"],
        ingestion_date=payload["ingestion_date"],
        default_money_unit_scale=payload["default_money_unit_scale"],
        text_blocks=tuple(payload["text_blocks"]),
        table_rows=tuple(payload["table_rows"]),
    )


def test_table_layout_extracts_all_required_metrics_with_confidence() -> None:
    fixture = _load_fixture()["table_layout_complete"]
    facts, diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )

    assert diagnostics == []
    assert len(facts) == 7
    metrics = {item.metric_name: item for item in facts}
    assert metrics["funded_ratio"].normalized_value == 0.784
    assert metrics["aal_usd"].normalized_value == 640_000_000.0
    assert metrics["ava_usd"].normalized_value == 501_800_000.0
    assert metrics["discount_rate"].normalized_value == 0.068
    assert metrics["participant_count"].normalized_value == 325_000.0
    assert all(item.confidence >= 0.9 for item in facts)
    assert all(item.extraction_method == "table_lookup" for item in facts)
    assert all(item.parser_version == PARSER_VERSION for item in facts)
    assert all(item.effective_date == "2024-06-30" for item in facts)
    assert all(item.ingestion_date == "2025-01-05" for item in facts)
    assert all(item.evidence_refs and item.evidence_refs[0].startswith("p.") for item in facts)


def test_text_layout_emits_missing_metric_warning_for_participant_count() -> None:
    fixture = _load_fixture()["text_layout_missing_participants"]
    facts, diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )
    metric_names = {item.metric_name for item in facts}
    assert "participant_count" not in metric_names
    metrics = {item.metric_name: item for item in facts}
    assert metrics["funded_ratio"].normalized_value == 0.812
    assert metrics["aal_usd"].normalized_value == 410_500_000.0
    assert metrics["ava_usd"].normalized_value == 333_700_000.0
    assert metrics["discount_rate"].normalized_value == 0.0675
    assert metrics["employer_contribution_rate"].normalized_value == 0.111
    assert metrics["employee_contribution_rate"].normalized_value == 0.071
    missing = [item for item in diagnostics if item.code == "missing_metric"]
    assert any(item.metric_name == "participant_count" for item in missing)
    assert all(item.severity == "warning" for item in diagnostics)
    assert all(item.evidence_refs and item.evidence_refs[0].startswith("text:") for item in facts)


def test_ambiguous_values_produce_diagnostic_and_choose_table_candidate() -> None:
    fixture = _load_fixture()["ambiguous_metric_layout"]
    facts, diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )

    ambiguous = [item for item in diagnostics if item.code == "ambiguous_metric"]
    assert ambiguous
    funded_ambiguous = [item for item in ambiguous if item.metric_name == "funded_ratio"][0]
    assert "0.765" in funded_ambiguous.message
    assert "0.79" in funded_ambiguous.message

    metrics = {item.metric_name: item for item in facts}
    assert metrics["funded_ratio"].normalized_value == 0.765
    assert metrics["funded_ratio"].extraction_method == "table_lookup"


def test_funded_status_wrapper_returns_funded_subset_only() -> None:
    fixture = _load_fixture()["table_layout_complete"]
    facts, diagnostics = extract_funded_status_metrics(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )

    assert {item.metric_name for item in facts} == {"funded_ratio", "aal_usd", "ava_usd"}
    assert diagnostics == []


def test_extraction_is_deterministic_for_same_input_payload() -> None:
    fixture = _load_fixture()["table_layout_complete"]
    raw = _raw(fixture["raw"])
    first = extract_funded_and_actuarial_metrics(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=raw,
    )
    second = extract_funded_and_actuarial_metrics(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=raw,
    )
    assert first == second
