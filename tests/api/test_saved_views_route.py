"""Tests for saved views route adapter."""

from __future__ import annotations

import pytest

from pension_data.api.auth import (
    SCOPE_EXPORT,
    SCOPE_QUERY,
    APIKeyStore,
    MissingAPIKeyError,
    ScopeDeniedError,
)
from pension_data.api.routes.saved_views import run_saved_view_endpoint
from pension_data.query.saved_views.models import FundingTrendInput


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
    assert result.rows[0].plan_id == "CA-PERS"
    assert result.rows[0].funded_ratio_change is None
    assert result.rows[1].funded_ratio_change == pytest.approx(0.03)


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
