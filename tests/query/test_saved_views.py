"""Tests for initial saved analytical views."""

from __future__ import annotations

from pathlib import Path

from pension_data.query.saved_views import (
    AllocationPeerInput,
    FundingTrendInput,
    HoldingsOverlapInput,
    execute_allocation_peer_compare_view,
    execute_funding_trend_view,
    execute_holdings_overlap_view,
    load_saved_view_definitions,
)


def test_load_saved_view_definitions_and_validate_schema() -> None:
    definitions = load_saved_view_definitions(
        config_dir=Path(__file__).resolve().parents[2] / "config" / "saved_queries"
    )

    assert sorted(definitions) == [
        "allocation_peer_compare:v1",
        "funding_trend:v1",
        "holdings_overlap:v1",
    ]

    funding = definitions["funding_trend:v1"]
    assert funding.output_schema[0].name == "plan_id"
    assert funding.output_schema[3].name == "funded_ratio_change"

    overlap = definitions["holdings_overlap:v1"]
    assert any("not_disclosed" in item.lower() for item in overlap.assumptions)


def test_funding_trend_view_outputs_stable_order_and_ratio_deltas() -> None:
    rows = [
        FundingTrendInput("TX-ERS", "FY2025", 0.72, 12_000_000.0, 5_000_000.0, 9_000_000.0),
        FundingTrendInput("CA-PERS", "FY2025", 0.81, 18_000_000.0, 7_000_000.0, 10_000_000.0),
        FundingTrendInput("CA-PERS", "FY2024", 0.78, 16_000_000.0, 6_000_000.0, 9_000_000.0),
    ]

    output = execute_funding_trend_view(rows)

    assert [(row.plan_id, row.plan_period) for row in output] == [
        ("CA-PERS", "FY2024"),
        ("CA-PERS", "FY2025"),
        ("TX-ERS", "FY2025"),
    ]
    assert output[0].funded_ratio_change is None
    assert output[1].funded_ratio_change == 0.03
    assert output[1].net_external_cash_flow_usd == 15_000_000.0


def test_allocation_peer_compare_view_computes_peer_stats() -> None:
    rows = [
        AllocationPeerInput("CA-PERS", "FY2025", "large_public", "public_equity", 0.52),
        AllocationPeerInput("WA-RETIRE", "FY2025", "large_public", "public_equity", 0.47),
        AllocationPeerInput("TX-ERS", "FY2025", "large_public", "public_equity", 0.49),
        AllocationPeerInput("CA-PERS", "FY2025", "large_public", "fixed_income", 0.31),
        AllocationPeerInput("WA-RETIRE", "FY2025", "large_public", "fixed_income", 0.35),
        AllocationPeerInput("TX-ERS", "FY2025", "large_public", "fixed_income", 0.33),
    ]

    output = execute_allocation_peer_compare_view(
        rows,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
    )

    assert [row.asset_class for row in output] == ["fixed_income", "public_equity"]
    assert output[0].peer_mean_pct == 0.34
    assert output[0].delta_vs_peer_mean_pct == -0.03
    assert output[1].peer_median_pct == 0.48


def test_holdings_overlap_view_is_coverage_aware() -> None:
    rows = [
        HoldingsOverlapInput(
            "CA-PERS",
            "FY2025",
            "Mercer",
            "Global Equity",
            120_000_000.0,
            "disclosed",
        ),
        HoldingsOverlapInput(
            "CA-PERS",
            "FY2025",
            "Aon",
            "Private Credit",
            None,
            "not_disclosed",
        ),
        HoldingsOverlapInput(
            "TX-ERS",
            "FY2025",
            "Mercer",
            "Global Equity",
            70_000_000.0,
            "disclosed",
        ),
        HoldingsOverlapInput(
            "TX-ERS",
            "FY2025",
            "Aon",
            "Private Credit",
            20_000_000.0,
            "disclosed",
        ),
        HoldingsOverlapInput(
            "WA-RETIRE",
            "FY2025",
            "Mercer",
            "Global Equity",
            0.0,
            "known_not_invested",
        ),
    ]

    output = execute_holdings_overlap_view(
        rows,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
    )

    tx_mercer = [
        row
        for row in output
        if row.counterparty_plan_id == "TX-ERS"
        and row.manager_name == "Mercer"
        and row.fund_name == "Global Equity"
    ][0]
    tx_aon = [
        row
        for row in output
        if row.counterparty_plan_id == "TX-ERS"
        and row.manager_name == "Aon"
        and row.fund_name == "Private Credit"
    ][0]
    wa_mercer = [
        row
        for row in output
        if row.counterparty_plan_id == "WA-RETIRE"
        and row.manager_name == "Mercer"
        and row.fund_name == "Global Equity"
    ][0]

    assert tx_mercer.overlap_status == "overlap"
    assert tx_mercer.overlap_usd == 70_000_000.0
    assert tx_aon.overlap_status == "unknown_due_to_non_disclosure"
    assert tx_aon.overlap_usd is None
    assert wa_mercer.overlap_status == "known_not_invested"
    assert wa_mercer.subject_disclosure_state == "disclosed"
    assert wa_mercer.counterparty_disclosure_state == "known_not_invested"
