"""Tests for saved views route adapter."""

from __future__ import annotations

from typing import cast

import pytest

from pension_data.api.auth import (
    SCOPE_EXPORT,
    SCOPE_QUERY,
    APIKeyStore,
    MissingAPIKeyError,
    ScopeDeniedError,
)
from pension_data.api.routes.saved_views import run_saved_view_endpoint
from pension_data.query.saved_views.models import (
    BenchmarkPanelInput,
    BenchmarkPanelRow,
    FundingTrendInput,
    FundingTrendRow,
)


def _make_store_and_secret(scopes: tuple[str, ...] = (SCOPE_QUERY,)) -> tuple[APIKeyStore, str]:
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=scopes, label="test-key")
    return store, secret


def test_missing_key_is_rejected() -> None:
    store = APIKeyStore()
    with pytest.raises(MissingAPIKeyError):
        run_saved_view_endpoint(
            api_key_header=None,
            key_store=store,
            view_name="funding_trend",
            view_inputs=[],
        )


def test_wrong_scope_is_rejected() -> None:
    store, secret = _make_store_and_secret(scopes=(SCOPE_EXPORT,))
    with pytest.raises(ScopeDeniedError):
        run_saved_view_endpoint(
            api_key_header=secret,
            key_store=store,
            view_name="funding_trend",
            view_inputs=[],
        )


def test_unknown_view_name_raises_value_error() -> None:
    store, secret = _make_store_and_secret()
    with pytest.raises(ValueError, match="unknown saved view"):
        run_saved_view_endpoint(
            api_key_header=secret,
            key_store=store,
            view_name="nonexistent_view",
            view_inputs=[],
        )


def test_funding_trend_view_returns_expected_structure() -> None:
    store, secret = _make_store_and_secret()
    inputs = [
        FundingTrendInput(
            plan_id="CA-PERS",
            plan_period="FY2023",
            funded_ratio=0.72,
            employer_contributions_usd=1_000_000.0,
            employee_contributions_usd=500_000.0,
            benefit_payments_usd=800_000.0,
        ),
        FundingTrendInput(
            plan_id="CA-PERS",
            plan_period="FY2024",
            funded_ratio=0.75,
            employer_contributions_usd=1_100_000.0,
            employee_contributions_usd=550_000.0,
            benefit_payments_usd=850_000.0,
        ),
    ]
    result = run_saved_view_endpoint(
        api_key_header=secret,
        key_store=store,
        view_name="funding_trend",
        view_inputs=inputs,
    )
    assert result.view_name == "funding_trend"
    assert len(result.rows) == 2
    rows = cast(list[FundingTrendRow], result.rows)
    assert rows[0].plan_id == "CA-PERS"
    assert rows[0].funded_ratio_change is None
    assert rows[1].funded_ratio_change == pytest.approx(0.03)


def test_audit_event_is_populated() -> None:
    store, secret = _make_store_and_secret()
    result = run_saved_view_endpoint(
        api_key_header=secret,
        key_store=store,
        view_name="funding_trend",
        view_inputs=[],
    )
    assert result.audit_event["operation"] == "query.saved_view"
    assert result.audit_event["view_name"] == "funding_trend"
    assert result.audit_event["view_row_count"] == 0
    assert "api_key_id" in result.audit_event


def test_allocation_peer_compare_requires_plan_id_and_period() -> None:
    store, secret = _make_store_and_secret()
    with pytest.raises(ValueError, match="requires subject_plan_id and plan_period"):
        run_saved_view_endpoint(
            api_key_header=secret,
            key_store=store,
            view_name="allocation_peer_compare",
            view_inputs=[],
        )


def test_holdings_overlap_requires_plan_id_and_period() -> None:
    store, secret = _make_store_and_secret()
    with pytest.raises(ValueError, match="requires subject_plan_id and plan_period"):
        run_saved_view_endpoint(
            api_key_header=secret,
            key_store=store,
            view_name="holdings_overlap",
            view_inputs=[],
        )


def test_benchmark_panel_requires_plan_id_and_period() -> None:
    store, secret = _make_store_and_secret()
    with pytest.raises(ValueError, match="requires subject_plan_id and plan_period"):
        run_saved_view_endpoint(
            api_key_header=secret,
            key_store=store,
            view_name="benchmark_panel",
            view_inputs=[],
        )


def test_benchmark_panel_route_returns_quartile_rank_and_scorecard() -> None:
    store, secret = _make_store_and_secret()
    inputs = [
        BenchmarkPanelInput(
            plan_id="CA-PERS",
            plan_period="FY2025",
            peer_group="large_public",
            funded_ratio_mva=0.78,
            assumed_return=0.072,
            realistic_return=0.068,
            net_return_1yr=0.081,
        ),
        BenchmarkPanelInput(
            plan_id="TX-ERS",
            plan_period="FY2025",
            peer_group="regional_public",
            funded_ratio_mva=0.72,
            assumed_return=0.070,
            net_return_1yr=0.064,
        ),
        BenchmarkPanelInput(
            plan_id="OR-PERS",
            plan_period="FY2025",
            peer_group="regional_public",
            funded_ratio_mva=0.74,
            assumed_return=0.068,
            net_return_1yr=0.079,
        ),
    ]

    result = run_saved_view_endpoint(
        api_key_header=secret,
        key_store=store,
        view_name="benchmark_panel",
        view_inputs=inputs,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
        tight_peer_group="regional_public",
    )

    output_rows = cast(list[BenchmarkPanelRow], result.rows)
    rows = {row.metric_name: row for row in output_rows}
    assert result.view_name == "benchmark_panel"
    assert rows["net_return_1yr"].peer_quartile_rank == 4
    assert rows["net_return_1yr"].tight_peer_quartile_rank == 4
    assert rows["health_scorecard.assumed_return"].health_dimension_name == "assumed_return"
