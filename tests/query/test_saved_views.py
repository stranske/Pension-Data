"""Tests for initial saved analytical views."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pension_data.query.saved_views import (
    AllocationPeerInput,
    BenchmarkPanelInput,
    FundingTrendInput,
    HoldingsOverlapInput,
    execute_allocation_peer_compare_view,
    execute_benchmark_panel_view,
    execute_funding_trend_view,
    execute_holdings_overlap_view,
    load_saved_view_definitions,
)


def _seed_saved_view_sqlite() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE plan_funding_metrics (
            plan_id TEXT NOT NULL,
            plan_period TEXT NOT NULL,
            funded_ratio REAL NOT NULL,
            employer_contributions_usd REAL,
            employee_contributions_usd REAL,
            benefit_payments_usd REAL
        );
        CREATE TABLE plan_allocation_metrics (
            plan_id TEXT NOT NULL,
            plan_period TEXT NOT NULL,
            peer_group TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            allocation_pct REAL NOT NULL
        );
        CREATE TABLE manager_fund_positions (
            plan_id TEXT NOT NULL,
            plan_period TEXT NOT NULL,
            manager_name TEXT NOT NULL,
            fund_name TEXT NOT NULL,
            exposure_usd REAL,
            disclosure_state TEXT NOT NULL
        );
        """)
    connection.executemany(
        """
        INSERT INTO plan_funding_metrics (
            plan_id,
            plan_period,
            funded_ratio,
            employer_contributions_usd,
            employee_contributions_usd,
            benefit_payments_usd
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("CA-PERS", "FY2024", 0.78, 16_000_000.0, 6_000_000.0, 9_000_000.0),
            ("CA-PERS", "FY2025", 0.81, 18_000_000.0, 7_000_000.0, 10_000_000.0),
            ("TX-ERS", "FY2025", 0.72, 12_000_000.0, 5_000_000.0, 9_000_000.0),
        ],
    )
    connection.executemany(
        """
        INSERT INTO plan_allocation_metrics (
            plan_id,
            plan_period,
            peer_group,
            asset_class,
            allocation_pct
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("CA-PERS", "FY2025", "large_public", "public_equity", 0.52),
            ("WA-RETIRE", "FY2025", "large_public", "public_equity", 0.47),
            ("TX-ERS", "FY2025", "large_public", "public_equity", 0.49),
            ("CA-PERS", "FY2025", "large_public", "fixed_income", 0.31),
            ("WA-RETIRE", "FY2025", "large_public", "fixed_income", 0.35),
            ("TX-ERS", "FY2025", "large_public", "fixed_income", 0.33),
        ],
    )
    connection.executemany(
        """
        INSERT INTO manager_fund_positions (
            plan_id,
            plan_period,
            manager_name,
            fund_name,
            exposure_usd,
            disclosure_state
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("CA-PERS", "FY2025", "Mercer", "Global Equity", 120_000_000.0, "disclosed"),
            ("CA-PERS", "FY2025", "Aon", "Private Credit", None, "not_disclosed"),
            ("TX-ERS", "FY2025", "Mercer", "Global Equity", 70_000_000.0, "disclosed"),
            ("TX-ERS", "FY2025", "Aon", "Private Credit", 20_000_000.0, "disclosed"),
            (
                "WA-RETIRE",
                "FY2025",
                "Mercer",
                "Global Equity",
                0.0,
                "known_not_invested",
            ),
        ],
    )
    return connection


def _required_definition() -> dict[str, object]:
    return {
        "view_name": "test_view",
        "version": "v1",
        "description": "test definition",
        "sql": "SELECT 1 AS sample_field;",
        "assumptions": ["test"],
        "output_schema": [{"name": "sample_field", "type": "number"}],
    }


def test_load_saved_view_definitions_and_validate_schema() -> None:
    definitions = load_saved_view_definitions(
        config_dir=Path(__file__).resolve().parents[2] / "config" / "saved_queries"
    )

    assert sorted(definitions) == [
        "allocation_peer_compare:v1",
        "benchmark_panel:v1",
        "funding_trend:v1",
        "holdings_overlap:v1",
    ]

    funding = definitions["funding_trend:v1"]
    assert funding.output_schema[0].name == "plan_id"
    assert funding.output_schema[3].name == "funded_ratio_change"

    overlap = definitions["holdings_overlap:v1"]
    assert any("not_disclosed" in item.lower() for item in overlap.assumptions)

    benchmark = definitions["benchmark_panel:v1"]
    assert benchmark.output_schema[0].name == "plan_id"
    assert any(field.name == "peer_quartile_rank" for field in benchmark.output_schema)
    assert any(field.name == "tight_peer_quartile_rank" for field in benchmark.output_schema)


def test_saved_view_sql_definitions_execute_against_seeded_data() -> None:
    definitions = load_saved_view_definitions(
        config_dir=Path(__file__).resolve().parents[2] / "config" / "saved_queries"
    )
    connection = _seed_saved_view_sqlite()
    try:
        funding_definition = definitions["funding_trend:v1"]
        funding_cursor = connection.execute(funding_definition.sql)
        funding_columns = [column[0] for column in funding_cursor.description or []]
        assert funding_columns == [field.name for field in funding_definition.output_schema]
        funding_rows = funding_cursor.fetchall()
        assert funding_rows
        plan_id_idx = funding_columns.index("plan_id")
        plan_period_idx = funding_columns.index("plan_period")
        funded_ratio_change_idx = funding_columns.index("funded_ratio_change")
        ca_pers_fy2025_row = next(
            (
                row
                for row in funding_rows
                if row[plan_id_idx] == "CA-PERS" and row[plan_period_idx] == "FY2025"
            ),
            None,
        )
        assert ca_pers_fy2025_row is not None
        assert ca_pers_fy2025_row[funded_ratio_change_idx] == pytest.approx(0.03)

        allocation_definition = definitions["allocation_peer_compare:v1"]
        allocation_cursor = connection.execute(
            allocation_definition.sql,
            {"plan_period": "FY2025", "subject_plan_id": "CA-PERS"},
        )
        allocation_columns = [column[0] for column in allocation_cursor.description or []]
        assert allocation_columns == [field.name for field in allocation_definition.output_schema]
        allocation_rows = allocation_cursor.fetchall()
        assert allocation_rows
        assert allocation_rows[0][5] == pytest.approx(0.34)

        overlap_definition = definitions["holdings_overlap:v1"]
        overlap_cursor = connection.execute(
            overlap_definition.sql,
            {"plan_period": "FY2025", "subject_plan_id": "CA-PERS"},
        )
        overlap_columns = [column[0] for column in overlap_cursor.description or []]
        assert overlap_columns == [field.name for field in overlap_definition.output_schema]
        overlap_rows = overlap_cursor.fetchall()
        assert overlap_rows
        assert any(row[5] == "overlap" and row[6] == 70_000_000.0 for row in overlap_rows)
    finally:
        connection.close()


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
    wa_aon = [
        row
        for row in output
        if row.counterparty_plan_id == "WA-RETIRE"
        and row.manager_name == "Aon"
        and row.fund_name == "Private Credit"
    ][0]

    assert tx_mercer.overlap_status == "overlap"
    assert tx_mercer.overlap_usd == 70_000_000.0
    assert tx_aon.overlap_status == "unknown_due_to_non_disclosure"
    assert tx_aon.overlap_usd is None
    assert wa_mercer.overlap_status == "known_not_invested"
    assert wa_mercer.subject_disclosure_state == "disclosed"
    assert wa_mercer.counterparty_disclosure_state == "known_not_invested"
    assert wa_aon.overlap_status == "unknown_due_to_non_disclosure"
    assert wa_aon.counterparty_disclosure_state == "not_disclosed"


def test_benchmark_panel_view_returns_peer_stats_and_health_context() -> None:
    rows = [
        BenchmarkPanelInput(
            plan_id="CA-PERS",
            plan_period="FY2025",
            peer_group="large_public",
            funded_ratio_ava=0.82,
            funded_ratio_mva=0.78,
            funded_ratio_trend=0.015,
            aal_usd=100.0,
            uaal_usd=22.0,
            assumed_return=0.0725,
            discount_rate=0.07,
            inflation_rate=0.025,
            payroll_growth_rate=0.03,
            amortization_method="closed layered",
            amortization_period_years=18.0,
            mortality_table_year=2015,
            adc_usd=10.0,
            actual_contribution_usd=9.5,
            payroll_usd=80.0,
            normal_cost_usd=6.0,
            amortization_payment_usd=4.0,
            net_return_1yr=0.081,
            net_return_3yr=0.07,
            net_return_5yr=0.068,
            net_return_10yr=0.071,
            net_external_cash_flow_pct=-2.5,
            support_ratio=1.8,
            benefit_payments_pct=0.07,
            assets_payroll_ratio=3.1,
            policy_benchmark_return=0.076,
            realistic_return=0.068,
        ),
        BenchmarkPanelInput(
            plan_id="TX-ERS",
            plan_period="FY2025",
            peer_group="regional_public",
            funded_ratio_ava=0.75,
            funded_ratio_mva=0.72,
            funded_ratio_trend=-0.02,
            aal_usd=95.0,
            uaal_usd=26.0,
            assumed_return=0.07,
            discount_rate=0.07,
            amortization_method="open layered",
            amortization_period_years=25.0,
            mortality_table_year=2010,
            payroll_usd=60.0,
            adc_usd=9.0,
            actual_contribution_usd=8.0,
            net_return_1yr=0.064,
            net_return_3yr=0.061,
            net_external_cash_flow_pct=-4.5,
        ),
        BenchmarkPanelInput(
            plan_id="OR-PERS",
            plan_period="FY2025",
            peer_group="regional_public",
            assumed_return=0.068,
            net_return_1yr=0.079,
            net_return_3yr=0.066,
            net_external_cash_flow_pct=-2.8,
        ),
        BenchmarkPanelInput(
            plan_id="WA-RETIRE",
            plan_period="FY2025",
            peer_group="large_public",
            funded_ratio_ava=0.88,
            funded_ratio_mva=0.84,
            funded_ratio_trend=0.0,
            aal_usd=110.0,
            uaal_usd=18.0,
            assumed_return=0.069,
            discount_rate=0.069,
            amortization_method="closed layered",
            amortization_period_years=15.0,
            mortality_table_year=2020,
            payroll_usd=75.0,
            adc_usd=11.0,
            actual_contribution_usd=11.5,
            net_return_1yr=0.09,
            net_return_3yr=0.073,
            net_external_cash_flow_pct=-1.0,
        ),
    ]

    output = execute_benchmark_panel_view(
        rows,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
        tight_peer_group="regional_public",
    )

    metrics = {row.metric_name: row for row in output}
    assert set(metrics) >= {
        "funded_ratio_ava",
        "funded_ratio_mva",
        "funded_ratio_trend",
        "assumed_return",
        "amortization_period_years",
        "amortization_method_closed",
        "mortality_table_year",
        "adc_vs_actual_contribution_ratio",
        "net_return_1yr",
        "net_external_cash_flow_pct",
    }
    assert metrics["funded_ratio_mva"].metric_value == 0.78
    assert metrics["funded_ratio_mva"].peer_median == pytest.approx(0.78)
    assert metrics["funded_ratio_mva"].peer_percentile == pytest.approx(50.0)
    assert metrics["funded_ratio_mva"].peer_quartile_rank == 2
    assert metrics["funded_ratio_mva"].health_rating == "yellow"
    assert "MVA funded ratio" in (metrics["funded_ratio_mva"].health_basis or "")
    assert metrics["funded_ratio_mva"].health_dimension_name == "funded_ratio_mva"
    assert metrics["funded_ratio_trend"].metric_value == pytest.approx(0.015)
    assert metrics["funded_ratio_trend"].peer_percentile == pytest.approx(100.0)
    assert metrics["funded_ratio_trend"].health_rating == "green"
    assert metrics["funded_ratio_trend"].health_dimension_name == "funded_ratio_trend"
    assert metrics["amortization_period_years"].metric_value == pytest.approx(18.0)
    assert metrics["amortization_period_years"].peer_percentile == pytest.approx(50.0)
    assert metrics["amortization_period_years"].peer_median == pytest.approx(20.0)
    assert metrics["amortization_period_years"].peer_z_score == pytest.approx(-0.4)
    assert metrics["amortization_method_closed"].metric_value == pytest.approx(1.0)
    assert metrics["amortization_method_closed"].peer_percentile == pytest.approx(100.0)
    assert metrics["amortization_method_closed"].peer_median == pytest.approx(0.5)
    assert metrics["amortization_method_closed"].peer_z_score == pytest.approx(1.0)
    assert metrics["mortality_table_year"].metric_value == pytest.approx(2015.0)
    assert metrics["mortality_table_year"].peer_percentile == pytest.approx(50.0)
    assert metrics["mortality_table_year"].peer_median == pytest.approx(2015.0)
    assert metrics["mortality_table_year"].peer_z_score == pytest.approx(0.0)
    assert metrics["adc_vs_actual_contribution_ratio"].metric_value == pytest.approx(0.95)
    assert metrics["adc_vs_actual_contribution_ratio"].health_rating == "yellow"
    assert (
        metrics["adc_vs_actual_contribution_ratio"].health_dimension_name
        == "contribution_sufficiency"
    )
    assert "vs peer median 6.90%" in (metrics["assumed_return"].health_basis or "")
    assert metrics["net_return_1yr"].delta_vs_assumed_return == pytest.approx(0.0085)
    assert metrics["net_return_1yr"].delta_vs_policy_benchmark == pytest.approx(0.005)
    assert metrics["net_return_1yr"].tight_peer_percentile == pytest.approx(100.0)
    assert metrics["net_return_1yr"].tight_peer_quartile_rank == 4
    assert metrics["net_return_1yr"].tight_peer_z_score == pytest.approx(1.2667)
    assert metrics["net_external_cash_flow_pct"].health_rating == "green"
    assert {
        row.health_dimension_name
        for row in output
        if row.metric_name.startswith("health_scorecard.")
    } == {
        "funded_ratio_mva",
        "funded_ratio_trend",
        "assumed_return",
        "contribution_sufficiency",
        "tread_water",
        "amortization",
        "cash_flow_maturity",
        "gasb_crossover",
        "mortality_currency",
    }
    assert metrics["health_scorecard.tread_water"].health_rating == "green"
    assert metrics["health_scorecard.amortization"].health_rating == "green"
    assert metrics["health_scorecard.gasb_crossover"].health_rating == "red"
    assert metrics["health_scorecard.mortality_currency"].health_dimension_value == 2015.0


def test_benchmark_panel_view_filters_non_finite_metrics() -> None:
    rows = [
        BenchmarkPanelInput(
            plan_id="CA-PERS",
            plan_period="FY2025",
            peer_group="large_public",
            funded_ratio_mva=float("nan"),
            assumed_return=0.07,
        ),
        BenchmarkPanelInput(
            plan_id="TX-ERS",
            plan_period="FY2025",
            peer_group="large_public",
            funded_ratio_mva=0.80,
            assumed_return=float("inf"),
        ),
    ]

    output = execute_benchmark_panel_view(
        rows,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
    )

    metrics = {row.metric_name: row for row in output}
    assert metrics["funded_ratio_mva"].metric_value is None
    assert metrics["funded_ratio_mva"].peer_percentile is None
    assert metrics["funded_ratio_mva"].peer_quartile_rank is None
    assert metrics["funded_ratio_mva"].health_rating == "unknown"
    assert metrics["assumed_return"].peer_median is None


def test_load_saved_view_definitions_raises_when_config_dir_missing(tmp_path: Path) -> None:
    missing_dir = tmp_path / "missing-saved-views"
    with pytest.raises(FileNotFoundError, match="Saved view definition directory does not exist"):
        load_saved_view_definitions(config_dir=missing_dir)


def test_load_saved_view_definitions_raises_when_config_path_is_file(tmp_path: Path) -> None:
    config_file = tmp_path / "saved-views.json"
    config_file.write_text("{}", encoding="utf-8")
    with pytest.raises(NotADirectoryError, match="Saved view definition path is not a directory"):
        load_saved_view_definitions(config_dir=config_file)


def test_load_saved_view_definitions_raises_when_no_definition_artifacts(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No saved view definition artifacts found"):
        load_saved_view_definitions(config_dir=tmp_path)


def test_load_saved_view_definitions_raises_for_empty_output_schema(tmp_path: Path) -> None:
    payload = _required_definition()
    payload["output_schema"] = []
    path = tmp_path / "test_view_v1.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="`output_schema` must not be empty"):
        load_saved_view_definitions(config_dir=tmp_path)


def test_load_saved_view_definitions_raises_for_duplicate_output_schema_names(
    tmp_path: Path,
) -> None:
    payload = _required_definition()
    payload["output_schema"] = [
        {"name": "sample_field", "type": "number"},
        {"name": "sample_field", "type": "number"},
    ]
    path = tmp_path / "test_view_v1.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="`output_schema` contains duplicate field names"):
        load_saved_view_definitions(config_dir=tmp_path)
