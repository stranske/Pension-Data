"""Tests for the internal FastAPI serving layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pension_data.api.app import create_app
from pension_data.api.auth import SCOPE_QUERY, APIKeyStore
from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PILOT_FIXTURE = _REPO_ROOT / "tests" / "golden" / "one_pdf_pilot" / "fixture_synthetic.pdf"


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


def test_saved_view_route_returns_funding_trend_payload(monkeypatch) -> None:
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "fixture")
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


def test_default_app_uses_environment_api_key(monkeypatch) -> None:
    monkeypatch.setenv("PENSION_DATA_API_KEY", "test-env-secret")
    monkeypatch.setenv("PENSION_DATA_API_KEY_SCOPES", SCOPE_QUERY)
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "fixture")

    client = TestClient(create_app())
    response = client.get(
        "/api/saved-views/funding-trend",
        headers={"Authorization": "Bearer test-env-secret"},
    )

    assert response.status_code == 200
    assert response.json()["view_name"] == "funding_trend"


def test_metric_history_route_is_authenticated(monkeypatch) -> None:
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "fixture")
    client, secret = _client_with_query_key()

    response = client.get(
        "/api/metric-history/CA-PERS",
        headers={"Authorization": secret},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_rows"] == 1
    assert payload["rows"][0]["metric_name"] == "funded_ratio"


def test_metric_history_serves_pilot_staging_not_hardcoded_fixture(
    tmp_path: Path, monkeypatch
) -> None:
    output_root = tmp_path / "pilot-output"
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=_PILOT_FIXTURE,
            plan_id="SYN-PLAN",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
            fetched_at="2026-01-01T00:00:00Z",
        ),
        output_root=output_root,
        run_id="api-probe",
    )
    persisted_rows = json.loads(
        Path(result["staging_core_metrics_json"]).read_text(encoding="utf-8")
    )
    expected = next(row for row in persisted_rows if row["metric_name"] == "funded_ratio")

    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.setenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", str(output_root))
    client, secret = _client_with_query_key()
    headers = {"Authorization": f"Bearer {secret}"}

    response = client.get("/api/metric-history/SYN-PLAN", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    funded_rows = [row for row in payload["rows"] if row["metric_name"] == "funded_ratio"]
    assert len(funded_rows) == 1
    assert funded_rows[0]["normalized_value"] == pytest.approx(expected["normalized_value"])
    assert funded_rows[0]["normalized_value"] != pytest.approx(0.81)
    assert funded_rows[0]["source_document_id"] == expected["source_document_id"]
    assert funded_rows[0]["report_id"] == expected["source_document_id"]
    assert funded_rows[0]["provenance_refs"]

    fixture_response = client.get("/api/metric-history/CA-PERS", headers=headers)
    assert fixture_response.status_code == 200
    assert fixture_response.json()["rows"] == []

    trend_response = client.get("/api/saved-views/funding-trend", headers=headers)
    assert trend_response.status_code == 200
    trend_rows = trend_response.json()["rows"]
    assert trend_rows == [
        {
            "plan_id": "SYN-PLAN",
            "plan_period": "FY2024",
            "funded_ratio": pytest.approx(expected["normalized_value"]),
            "funded_ratio_change": None,
            "net_external_cash_flow_usd": None,
        }
    ]


def test_authenticated_query_reports_missing_artifact_root(monkeypatch) -> None:
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.delenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", raising=False)
    client, secret = _client_with_query_key()

    response = client.get(
        "/api/metric-history/SYN-PLAN",
        headers={"Authorization": f"Bearer {secret}"},
    )

    assert response.status_code == 503
    assert "PENSION_DATA_QUERY_ARTIFACT_ROOT is required" in response.json()["detail"]


def test_llm_routes_are_disabled_in_proprietary_zone_without_authorized_base_url(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("ANTHROPIC_BASE_URL", raising=False)
    client, _ = _client_with_query_key()

    response = client.post("/api/nl/query")

    assert response.status_code == 503
    assert "LLM disabled in proprietary zone" in response.json()["detail"]
