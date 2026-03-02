"""Tests for AUM and sponsor cash-flow extraction and normalization."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pension_data.extract.funded.financial_flows import (
    RawFinancialFlowInput,
    extract_plan_financial_flow,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "financial_flows_golden.json"


def _load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _raw(payload: dict[str, Any]) -> RawFinancialFlowInput:
    return RawFinancialFlowInput(
        source_document_id=payload["source_document_id"],
        source_url=payload["source_url"],
        effective_period=payload["effective_period"],
        reported_at=payload["reported_at"],
        unit_scale=payload["unit_scale"],
        outflows_reported_as_negative=payload["outflows_reported_as_negative"],
        beginning_aum=payload["beginning_aum"],
        ending_aum=payload["ending_aum"],
        employer_contributions=payload["employer_contributions"],
        employee_contributions=payload["employee_contributions"],
        benefit_payments=payload["benefit_payments"],
        refunds=payload["refunds"],
        evidence_refs=tuple(payload["evidence_refs"]),
    )


def test_complete_million_layout_derives_deterministic_net_flow_and_rate() -> None:
    fixture = _load_fixture()["complete_million_layout"]
    flow, warnings = extract_plan_financial_flow(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )

    assert warnings == []
    assert flow.disclosure_level == "complete"
    assert flow.beginning_aum_usd == 520_000_000.0
    assert flow.ending_aum_usd == 540_000_000.0
    assert flow.employer_contributions_usd == 18_000_000.0
    assert flow.employee_contributions_usd == 7_000_000.0
    assert flow.benefit_payments_usd == -4_000_000.0
    assert flow.refunds_usd == -1_000_000.0
    assert flow.net_external_cash_flow_usd == 20_000_000.0
    assert flow.net_external_cash_flow_rate_pct == 3.846154
    assert flow.consistency_gap_usd == 0.0


def test_complete_signed_layout_normalizes_sign_conventions() -> None:
    fixture = _load_fixture()["complete_signed_layout"]
    flow, warnings = extract_plan_financial_flow(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )

    assert any(warning.code == "consistency_gap" for warning in warnings)
    assert flow.beginning_aum_usd == 890_000_000.0
    assert flow.ending_aum_usd == 906_000_000.0
    assert flow.employer_contributions_usd == 18_500_000.0
    assert flow.employee_contributions_usd == 6_200_000.0
    assert flow.benefit_payments_usd == -7_600_000.0
    assert flow.refunds_usd == -1_600_000.0
    assert flow.net_external_cash_flow_usd == 15_500_000.0
    assert flow.net_external_cash_flow_rate_pct == 1.741573


def test_partial_layout_sets_partial_disclosure_and_preserves_components() -> None:
    fixture = _load_fixture()["partial_layout"]
    flow, warnings = extract_plan_financial_flow(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )
    warning_codes = [warning.code for warning in warnings]

    assert flow.disclosure_level == "partial"
    assert "partial_disclosure" in warning_codes
    assert flow.beginning_aum_usd == 300_000_000.0
    assert flow.ending_aum_usd is None
    assert flow.employee_contributions_usd is None
    assert flow.refunds_usd is None
    assert flow.net_external_cash_flow_usd == 7_000_000.0
    assert flow.net_external_cash_flow_rate_pct == 2.333333
    assert flow.evidence_refs == ("p9", "p10")


def test_output_is_reproducible_for_same_raw_payload() -> None:
    fixture = _load_fixture()["complete_million_layout"]
    raw = _raw(fixture["raw"])

    first = extract_plan_financial_flow(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=raw,
    )
    second = extract_plan_financial_flow(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=raw,
    )
    assert first == second


def test_source_metadata_is_read_only() -> None:
    fixture = _load_fixture()["complete_million_layout"]
    flow, _warnings = extract_plan_financial_flow(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        raw=_raw(fixture["raw"]),
    )
    with pytest.raises(TypeError):
        flow.source_metadata["unit_scale"] = "usd"
