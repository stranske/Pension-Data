"""Tests for derivatives and securities-lending risk disclosure extraction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pension_data.extract.investment.risk_disclosures import (
    DerivativesDisclosureInput,
    SecuritiesLendingDisclosureInput,
    extract_risk_exposure_observations,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "risk_disclosures_golden.json"


def _load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _derivatives(payload: list[dict[str, Any]]) -> list[DerivativesDisclosureInput]:
    return [
        DerivativesDisclosureInput(
            usage_type=row["usage_type"],
            policy_limit_value=row["policy_limit_value"],
            realized_exposure_value=row["realized_exposure_value"],
            value_unit=row["value_unit"],
            as_reported_text=row["as_reported_text"],
            source_kind=row["source_kind"],
            confidence=row["confidence"],
            evidence_refs=tuple(row["evidence_refs"]),
            source_url=row["source_url"],
        )
        for row in payload
    ]


def _lending(payload: list[dict[str, Any]]) -> list[SecuritiesLendingDisclosureInput]:
    return [
        SecuritiesLendingDisclosureInput(
            program_name=row["program_name"],
            policy_limit_value=row["policy_limit_value"],
            realized_exposure_value=row["realized_exposure_value"],
            collateral_value=row["collateral_value"],
            value_unit=row["value_unit"],
            as_reported_text=row["as_reported_text"],
            source_kind=row["source_kind"],
            confidence=row["confidence"],
            evidence_refs=tuple(row["evidence_refs"]),
            source_url=row["source_url"],
        )
        for row in payload
    ]


def test_extracts_numeric_table_and_narrative_risk_paths() -> None:
    fixture = _load_fixture()["with_disclosures"]
    observations, diagnostics = extract_risk_exposure_observations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        derivatives_disclosures=_derivatives(fixture["derivatives_disclosures"]),
        securities_lending_disclosures=_lending(fixture["securities_lending_disclosures"]),
    )

    assert len(observations) == 6
    assert len(diagnostics) == 1
    assert diagnostics[0].code == "realized_only"
    assert diagnostics[0].disclosure_type == "derivatives"
    assert diagnostics[0].metric_name == "derivatives:overlay_futures"

    policy = [
        row
        for row in observations
        if row.metric_name == "derivatives:interest_rate_swaps:policy_limit"
    ][0]
    realized_ratio = [
        row for row in observations if row.metric_name == "derivatives:overlay_futures:realized_exposure"
    ][0]
    collateral = [
        row
        for row in observations
        if row.metric_name == "securities_lending:domestic_equity_lending:collateral"
    ][0]

    assert policy.observation_kind == "policy_limit"
    assert policy.value_usd == 200_000_000.0
    assert policy.value_ratio is None
    assert realized_ratio.observation_kind == "realized_exposure"
    assert realized_ratio.value_usd is None
    assert realized_ratio.value_ratio == 0.14
    assert collateral.observation_kind == "collateral_context"
    assert collateral.value_usd == 56_000_000.0


def test_explicit_non_disclosure_rows_are_emitted_when_missing() -> None:
    fixture = _load_fixture()["without_disclosures"]
    observations, diagnostics = extract_risk_exposure_observations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        derivatives_disclosures=[],
        securities_lending_disclosures=[],
    )

    assert len(observations) == 2
    assert [row.metric_name for row in observations] == [
        "derivatives:not_disclosed",
        "securities_lending:not_disclosed",
    ]
    assert all(row.observation_kind == "not_disclosed" for row in observations)
    assert len(diagnostics) == 2
    assert [row.code for row in diagnostics] == ["not_disclosed", "not_disclosed"]


def test_extraction_output_is_reproducible() -> None:
    fixture = _load_fixture()["with_disclosures"]
    derivatives = _derivatives(fixture["derivatives_disclosures"])
    lending = _lending(fixture["securities_lending_disclosures"])

    first = extract_risk_exposure_observations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        derivatives_disclosures=derivatives,
        securities_lending_disclosures=lending,
    )
    second = extract_risk_exposure_observations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        derivatives_disclosures=derivatives,
        securities_lending_disclosures=lending,
    )

    assert first == second


def test_source_metadata_is_read_only() -> None:
    fixture = _load_fixture()["with_disclosures"]
    observations, _diagnostics = extract_risk_exposure_observations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        derivatives_disclosures=_derivatives(fixture["derivatives_disclosures"]),
        securities_lending_disclosures=_lending(fixture["securities_lending_disclosures"]),
    )
    with pytest.raises(TypeError):
        observations[0].source_metadata["unit"] = "usd"
