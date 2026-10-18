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


@pytest.mark.parametrize("disclosure", ["disclosed", "missing", "mismatched", "oversized"])
def test_funding_trend_http_uses_identity_bound_flow_artifacts(
    tmp_path: Path, monkeypatch, disclosure: str
) -> None:
    from tests.api.test_artifact_data import _flow_artifact_path, _run_flow_pilot

    root, result = _run_flow_pilot(tmp_path)
    manifest_path = Path(result["run_manifest_json"])
    manifest = json.loads(manifest_path.read_text())
    path = _flow_artifact_path(
        result["run_manifest_json"], manifest["artifact_files"]["financial_flow_rows_json"]
    )
    if disclosure == "missing":
        del manifest["artifact_files"]["financial_flow_rows_json"]
        manifest_path.write_text(json.dumps(manifest))
    elif disclosure in {"mismatched", "oversized"}:
        rows = json.loads(path.read_text())
        if disclosure == "mismatched":
            rows[0]["source_document_id"] = "OTHER"
        else:
            rows[0]["employer_contributions_usd"] = 10**400
        path.write_text(json.dumps(rows))
    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.setenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", str(root))
    client, secret = _client_with_query_key()
    response = client.get(
        "/api/saved-views/funding-trend", headers={"Authorization": f"Bearer {secret}"}
    )
    if disclosure in {"mismatched", "oversized"}:
        assert response.status_code == 503
        field = "source_document_id" if disclosure == "mismatched" else "employer_contributions_usd"
        assert field in response.json()["detail"]
    else:
        assert response.status_code == 200
        rows = response.json()["rows"]
        assert len(rows) == 1
        assert rows[0]["net_external_cash_flow_usd"] == (
            85_000_000 if disclosure == "disclosed" else None
        )


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


def test_metric_history_serves_two_plans_under_one_artifact_root(
    tmp_path: Path, monkeypatch
) -> None:
    output_root = tmp_path / "pilot-output"
    pilot_input_base = {
        "pdf_path": _PILOT_FIXTURE,
        "plan_period": "FY2024",
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-01-01",
        "fetched_at": "2026-01-01T00:00:00Z",
    }
    result_alpha = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(plan_id="PLAN-ALPHA", **pilot_input_base),
        output_root=output_root,
        run_id="run-alpha",
    )
    result_beta = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(plan_id="PLAN-BETA", **pilot_input_base),
        output_root=output_root,
        run_id="run-beta",
    )

    def _expected_funded_ratio(result: dict[str, str]) -> float:
        persisted_rows = json.loads(
            Path(result["staging_core_metrics_json"]).read_text(encoding="utf-8")
        )
        row = next(r for r in persisted_rows if r["metric_name"] == "funded_ratio")
        return float(row["normalized_value"])

    expected_alpha = _expected_funded_ratio(result_alpha)
    expected_beta = _expected_funded_ratio(result_beta)

    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.setenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", str(output_root))
    client, secret = _client_with_query_key()
    headers = {"Authorization": f"Bearer {secret}"}

    alpha_response = client.get("/api/metric-history/PLAN-ALPHA", headers=headers)
    beta_response = client.get("/api/metric-history/PLAN-BETA", headers=headers)
    assert alpha_response.status_code == 200
    assert beta_response.status_code == 200

    alpha_rows = [
        row for row in alpha_response.json()["rows"] if row["metric_name"] == "funded_ratio"
    ]
    beta_rows = [
        row for row in beta_response.json()["rows"] if row["metric_name"] == "funded_ratio"
    ]
    assert len(alpha_rows) == 1
    assert len(beta_rows) == 1
    assert alpha_rows[0]["normalized_value"] == pytest.approx(expected_alpha)
    assert beta_rows[0]["normalized_value"] == pytest.approx(expected_beta)
    assert alpha_rows[0]["entity_id"] == "PLAN-ALPHA"
    assert beta_rows[0]["entity_id"] == "PLAN-BETA"

    trend_response = client.get("/api/saved-views/funding-trend", headers=headers)
    assert trend_response.status_code == 200
    trend_by_plan = {row["plan_id"]: row for row in trend_response.json()["rows"]}
    assert set(trend_by_plan) == {"PLAN-ALPHA", "PLAN-BETA"}
    assert trend_by_plan["PLAN-ALPHA"]["funded_ratio"] == pytest.approx(expected_alpha)
    assert trend_by_plan["PLAN-BETA"]["funded_ratio"] == pytest.approx(expected_beta)


def test_metric_history_returns_both_revised_versions_with_provenance(
    tmp_path: Path, monkeypatch
) -> None:
    from tests.api.test_artifact_data import _metric_row, _write_run

    artifact_root = tmp_path / "artifacts"
    _write_run(
        artifact_root,
        "run-original",
        [
            _metric_row(
                fact_id="fact:funded:original",
                normalized_value=0.784,
                source_document_id="doc:syn-plan:original",
            )
        ],
        source_document_id="doc:syn-plan:original",
    )
    _write_run(
        artifact_root,
        "run-revised",
        [
            _metric_row(
                fact_id="fact:funded:revised",
                normalized_value=0.8,
                source_document_id="doc:syn-plan:revised",
                ingestion_date="2026-02-01",
            )
        ],
        source_document_id="doc:syn-plan:revised",
    )

    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.setenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", str(artifact_root))
    client, secret = _client_with_query_key()
    headers = {"Authorization": f"Bearer {secret}"}

    response = client.get("/api/metric-history/SYN-PLAN", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    funded_rows = [row for row in payload["rows"] if row["metric_name"] == "funded_ratio"]
    assert len(funded_rows) == 2
    assert {row["normalized_value"] for row in funded_rows} == {0.784, 0.8}
    assert {row["source_document_id"] for row in funded_rows} == {
        "doc:syn-plan:original",
        "doc:syn-plan:revised",
    }
    assert {row["effective_date"] for row in funded_rows} == {"2024-06-30"}
    assert {row["ingestion_date"] for row in funded_rows} == {"2026-01-01", "2026-02-01"}


def test_conflicted_plan_period_does_not_disable_metric_history_for_other_plan(
    tmp_path: Path, monkeypatch
) -> None:
    from tests.api.test_artifact_data import _metric_row, _write_run

    artifact_root = tmp_path / "artifacts"
    _write_run(
        artifact_root,
        "run-conflicted-a",
        [
            _metric_row(
                fact_id="fact:conflict:a",
                plan_id="PLAN-CONFLICT",
                normalized_value=0.784,
                source_document_id="doc:conflict:a",
            )
        ],
        plan_id="PLAN-CONFLICT",
        source_document_id="doc:conflict:a",
    )
    _write_run(
        artifact_root,
        "run-conflicted-b",
        [
            _metric_row(
                fact_id="fact:conflict:b",
                plan_id="PLAN-CONFLICT",
                normalized_value=0.8,
                source_document_id="doc:conflict:b",
                ingestion_date="2026-02-01",
            )
        ],
        plan_id="PLAN-CONFLICT",
        source_document_id="doc:conflict:b",
    )
    _write_run(
        artifact_root,
        "run-clean",
        [_metric_row(fact_id="fact:clean", plan_id="PLAN-CLEAN", normalized_value=0.75)],
        plan_id="PLAN-CLEAN",
    )

    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.setenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", str(artifact_root))
    client, secret = _client_with_query_key()
    headers = {"Authorization": f"Bearer {secret}"}

    clean_response = client.get("/api/metric-history/PLAN-CLEAN", headers=headers)
    assert clean_response.status_code == 200
    assert clean_response.json()["total_rows"] == 1

    trend_response = client.get("/api/saved-views/funding-trend", headers=headers)
    assert trend_response.status_code == 200
    payload = trend_response.json()
    assert payload["rows"] == [
        {
            "plan_id": "PLAN-CLEAN",
            "plan_period": "FY2024",
            "funded_ratio": pytest.approx(0.75),
            "funded_ratio_change": None,
            "net_external_cash_flow_usd": None,
        }
    ]
    unresolved = payload["unresolved_periods"][0]
    assert unresolved["entity_id"] == "PLAN-CONFLICT"
    assert unresolved["plan_period"] == "FY2024"
    assert len(unresolved["conflicts"]) == 1
    conflict = unresolved["conflicts"][0]
    assert conflict["metric_target"] == "funded_ratio"
    versions = {version["source_document_id"]: version for version in conflict["versions"]}
    assert versions["doc:conflict:a"]["normalized_value"] == pytest.approx(0.784)
    assert versions["doc:conflict:b"]["normalized_value"] == pytest.approx(0.8)


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
