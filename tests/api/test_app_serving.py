"""Tests for the internal FastAPI serving layer."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pension_data.api.app import create_app
from pension_data.api.auth import SCOPE_QUERY, APIKeyStore


def _client_with_query_key() -> tuple[TestClient, str]:
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=(SCOPE_QUERY,), label="serving-test")
    return TestClient(create_app(key_store=store)), secret


def test_health_and_config_routes() -> None:
    client, _ = _client_with_query_key()

    assert client.get("/health").status_code == 200
    config = client.get("/config")

    assert config.status_code == 200
    assert config.json()["environment"]
    assert config.json()["apiBaseUrl"]
    assert config.json()["artifactBaseUrl"]


def test_saved_view_route_returns_funding_trend_payload() -> None:
    client, secret = _client_with_query_key()

    response = client.get(
        "/api/saved-views/funding-trend",
        headers={"Authorization": f"Bearer {secret}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["view_name"] == "funding_trend"
    assert payload["rows"][0]["plan_id"] == "CA-PERS"
    assert payload["rows"][0]["funded_ratio_change"] is None
    assert payload["rows"][1]["funded_ratio_change"] == pytest.approx(0.03)
    assert payload["audit_event"]["operation"] == "query.saved_view"


def test_saved_view_route_maps_auth_errors() -> None:
    client, _ = _client_with_query_key()

    response = client.get("/api/saved-views/funding-trend")

    assert response.status_code == 401
    assert "missing API key" in response.json()["detail"]


def test_metric_history_route_is_authenticated() -> None:
    client, secret = _client_with_query_key()

    response = client.get(
        "/api/metric-history/CA-PERS",
        headers={"Authorization": secret},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_rows"] == 1
    assert payload["rows"][0]["metric_name"] == "funded_ratio"


def test_llm_routes_are_disabled_in_proprietary_zone_without_authorized_base_url(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("ANTHROPIC_BASE_URL", raising=False)
    monkeypatch.delenv("PENSION_DATA_AUTHORIZED_LLM_BASE_URL", raising=False)
    client, _ = _client_with_query_key()

    response = client.post("/api/nl/query")

    assert response.status_code == 503
    assert "LLM disabled in proprietary zone" in response.json()["detail"]
