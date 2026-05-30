"""FastAPI serving layer for the internal Pension-Data workspace."""

from __future__ import annotations

import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from pension_data.api.auth import (
    APIKeyStore,
    AuthError,
    InvalidAPIKeyError,
    MissingAPIKeyError,
    ScopeDeniedError,
)
from pension_data.api.routes.metric_history import run_metric_history_endpoint
from pension_data.api.routes.saved_views import run_saved_view_endpoint
from pension_data.query.metric_history_service import (
    MetricHistoryProvenanceRef,
    MetricHistoryRequest,
    MetricHistoryRow,
)
from pension_data.query.saved_views.models import FundingTrendInput

PACKAGE_ROOT = Path(__file__).resolve().parents[3]
WEB_ROOT = PACKAGE_ROOT / "apps" / "web"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765


def create_app(*, key_store: APIKeyStore | None = None, web_root: Path | None = None) -> FastAPI:
    """Build the FastAPI app with deterministic routes enabled by default."""
    app = FastAPI(title="Pension-Data Internal API")
    store = key_store or APIKeyStore()
    static_root = web_root or WEB_ROOT

    @app.exception_handler(AuthError)
    async def _handle_auth_error(_: Any, exc: AuthError) -> JSONResponse:
        return JSONResponse(
            status_code=_auth_status(exc),
            content={"detail": str(exc) or exc.__class__.__name__},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "environment": _environment()}

    @app.get("/config")
    def config() -> dict[str, str]:
        return {
            "environment": _environment(),
            "apiBaseUrl": os.getenv("PENSION_DATA_API_BASE_URL", "/api"),
            "artifactBaseUrl": os.getenv("PENSION_DATA_ARTIFACT_BASE_URL", "/artifacts"),
        }

    @app.get("/api/saved-views/funding-trend")
    def funding_trend(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        result = run_saved_view_endpoint(
            api_key_header=authorization,
            key_store=store,
            view_name="funding_trend",
            view_inputs=_fixture_funding_trend_inputs(),
            event={"route": "GET /api/saved-views/funding-trend"},
        )
        return {
            "view_name": result.view_name,
            "rows": [_jsonable(row) for row in result.rows],
            "audit_event": result.audit_event,
        }

    @app.get("/api/metric-history/{entity_id}")
    def metric_history(
        entity_id: str,
        metric_name: str | None = None,
        metric_family: str | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        result = run_metric_history_endpoint(
            api_key_header=authorization,
            key_store=store,
            request=MetricHistoryRequest(
                entity_id=entity_id,
                metric_name=metric_name,
                metric_family=metric_family,
            ),
            rows=_fixture_metric_history_rows(),
            event={"route": "GET /api/metric-history/{entity_id}"},
        )
        return {
            "rows": [_jsonable(row) for row in result.response.rows],
            "total_rows": result.response.total_rows,
            "audit_event": result.audit_event,
        }

    @app.post("/api/nl/query")
    def nl_query() -> dict[str, str]:
        _ensure_llm_allowed()
        raise HTTPException(
            status_code=501,
            detail="NL chain injection is not configured in the internal serving layer",
        )

    @app.post("/api/findings/explain")
    def findings_explain() -> dict[str, str]:
        _ensure_llm_allowed()
        raise HTTPException(
            status_code=501,
            detail="findings chain injection is not configured in the internal serving layer",
        )

    @app.post("/api/findings/compare")
    def findings_compare() -> dict[str, str]:
        _ensure_llm_allowed()
        raise HTTPException(
            status_code=501,
            detail="findings chain injection is not configured in the internal serving layer",
        )

    if static_root.exists():
        app.mount("/", StaticFiles(directory=static_root, html=True), name="web")

    return app


def _auth_status(exc: AuthError) -> int:
    if isinstance(exc, MissingAPIKeyError | InvalidAPIKeyError):
        return 401
    if isinstance(exc, ScopeDeniedError):
        return 403
    return 401


def _environment() -> str:
    return os.getenv("PENSION_DATA_ENVIRONMENT", "internal")


def _data_zone() -> str:
    return os.getenv("PENSION_DATA_DATA_ZONE", "proprietary").strip().casefold() or "proprietary"


def _authorized_llm_base_url_configured() -> bool:
    return bool(
        os.getenv("OPENAI_BASE_URL", "").strip()
        or os.getenv("ANTHROPIC_BASE_URL", "").strip()
        or os.getenv("PENSION_DATA_AUTHORIZED_LLM_BASE_URL", "").strip()
    )


def _ensure_llm_allowed() -> None:
    if _data_zone() == "proprietary" and not _authorized_llm_base_url_configured():
        raise HTTPException(
            status_code=503,
            detail="LLM disabled in proprietary zone; configure an authorized no-train base_url",
        )


def _fixture_funding_trend_inputs() -> list[FundingTrendInput]:
    return [
        FundingTrendInput(
            plan_id="CA-PERS",
            plan_period="FY2023",
            funded_ratio=0.78,
            employer_contributions_usd=1_000_000.0,
            employee_contributions_usd=500_000.0,
            benefit_payments_usd=800_000.0,
        ),
        FundingTrendInput(
            plan_id="CA-PERS",
            plan_period="FY2024",
            funded_ratio=0.81,
            employer_contributions_usd=1_100_000.0,
            employee_contributions_usd=550_000.0,
            benefit_payments_usd=850_000.0,
        ),
    ]


def _fixture_metric_history_rows() -> tuple[MetricHistoryRow, ...]:
    return (
        MetricHistoryRow(
            entity_id="CA-PERS",
            plan_period="FY2024",
            metric_family="funded_status",
            metric_name="funded_ratio",
            as_reported_value=0.81,
            normalized_value=0.81,
            as_reported_unit="ratio",
            normalized_unit="ratio",
            confidence=0.96,
            effective_date="2024-06-30T00:00:00Z",
            ingestion_date="2026-03-02T23:45:00Z",
            benchmark_version="fixture-v1",
            report_id="doc:funded-2024",
            source_document_id="doc:funded-2024",
            provenance_refs=(
                MetricHistoryProvenanceRef(
                    evidence_ref_id="doc:funded-2024#page=52",
                    raw_ref="page=52",
                    page_number=52,
                    section_hint=None,
                    snippet_anchor=None,
                ),
            ),
        ),
    )


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, tuple | list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


app = create_app()


def run() -> None:
    """Launch the internal-only HTTP server."""
    host = os.getenv("PENSION_DATA_HOST", DEFAULT_HOST)
    port = int(os.getenv("PENSION_DATA_PORT", str(DEFAULT_PORT)))
    uvicorn.run("pension_data.api.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()
