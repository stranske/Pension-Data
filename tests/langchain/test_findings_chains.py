"""Tests for findings explain/compare chains and export artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from pension_data.api.auth import SCOPE_NL, SCOPE_QUERY, APIKeyStore, ScopeDeniedError
from pension_data.api.routes.findings import (
    run_findings_compare_endpoint,
    run_findings_explain_endpoint,
)
from pension_data.langchain.findings_common import FindingSlice
from pension_data.langchain.findings_compare import CompareRequest, run_findings_compare_chain
from pension_data.langchain.findings_explain import ExplainRequest, run_findings_explain_chain
from pension_data.langchain.findings_export import (
    build_findings_export_artifact,
    render_findings_export_text,
)


class _StaticChain:
    def __init__(self, output: Mapping[str, Any] | str) -> None:
        self.output = output

    def invoke(self, values: Mapping[str, Any]) -> Mapping[str, Any] | str:
        del values
        return self.output


def _slice(
    *,
    slice_id: str,
    plan_id: str,
    plan_period: str,
    funded_ratio: float,
    citation: str,
) -> FindingSlice:
    return FindingSlice(
        slice_id=slice_id,
        plan_id=plan_id,
        plan_period=plan_period,
        metrics={"funded_ratio": funded_ratio},
        citations=(citation,),
    )


def test_explain_chain_enforces_schema_and_filters_citations() -> None:
    response = run_findings_explain_chain(
        request=ExplainRequest(
            question="Explain why funded ratio improved year over year",
            finding_slice=_slice(
                slice_id="slice:ca",
                plan_id="CA-PERS",
                plan_period="FY2024",
                funded_ratio=0.81,
                citation="doc:1#p.12",
            ),
        ),
        chain=_StaticChain(
            {
                "summary": "Funded ratio improved due to higher contributions.",
                "key_drivers": ["Contributions rose", "Asset growth outpaced liabilities"],
                "caveats": ["One-year move may not be persistent"],
                "citations": ["doc:1#p.12", "doc:999#p.1"],
            }
        ),
    )

    assert response.status == "ok"
    assert response.error is None
    assert response.result is not None
    assert response.result.summary.startswith("Funded ratio improved")
    assert response.result.citations == ("doc:1#p.12",)
    assert response.metadata.request_id.startswith("fx:")


def test_explain_chain_redacts_secret_like_output() -> None:
    response = run_findings_explain_chain(
        request=ExplainRequest(
            question="Summarize funded ratio trend drivers",
            finding_slice=_slice(
                slice_id="slice:ca",
                plan_id="CA-PERS",
                plan_period="FY2024",
                funded_ratio=0.81,
                citation="doc:1#p.12",
            ),
        ),
        chain=_StaticChain(
            {
                "summary": "Token sk-secret-token should not appear in output.",
                "key_drivers": ["api_key: abcdef123456 should be redacted"],
                "caveats": [],
                "citations": ["doc:1#p.12"],
            }
        ),
    )

    assert response.status == "ok"
    assert response.result is not None
    assert "sk-secret-token" not in response.result.summary
    assert "[REDACTED]" in response.result.summary
    assert "[REDACTED]" in response.result.key_drivers[0]


def test_compare_chain_returns_structured_differences() -> None:
    response = run_findings_compare_chain(
        request=CompareRequest(
            question="Compare funded ratio between two periods",
            left_slice=_slice(
                slice_id="slice:left",
                plan_id="CA-PERS",
                plan_period="FY2023",
                funded_ratio=0.78,
                citation="doc:1#p.10",
            ),
            right_slice=_slice(
                slice_id="slice:right",
                plan_id="CA-PERS",
                plan_period="FY2024",
                funded_ratio=0.81,
                citation="doc:1#p.12",
            ),
        ),
        chain=_StaticChain(
            {
                "summary": "FY2024 funded ratio improved versus FY2023.",
                "key_differences": ["Funded ratio +3 percentage points"],
                "key_drivers": ["Employer contributions increased"],
                "caveats": ["Single-period comparison"],
                "citations": ["doc:1#p.10", "doc:1#p.12"],
            }
        ),
    )

    assert response.status == "ok"
    assert response.result is not None
    assert response.result.key_differences == ("Funded ratio +3 percentage points",)
    assert response.result.citations == ("doc:1#p.10", "doc:1#p.12")
    assert response.metadata.request_id.startswith("fc:")


def test_export_artifact_contains_trace_and_renders_text() -> None:
    artifact = build_findings_export_artifact(
        artifact_type="explain",
        request_id="fx:test",
        payload={
            "summary": "Improved funded ratio",
            "key_drivers": [" Contributions rose ", "Contributions rose"],
        },
        citations=("doc:1#p.12", " doc:1#p.12 "),
        trace={"trace_url": "https://smith.langchain.com/r/example"},
    )
    text = render_findings_export_text(artifact)

    assert artifact.generated_at
    assert artifact.trace["trace_url"].startswith("https://")
    assert artifact.payload["key_drivers"] == ("Contributions rose",)
    assert artifact.citations == ("doc:1#p.12",)
    assert "artifact_type: explain" in text
    assert "trace_url" in text


def test_findings_routes_require_nl_scope_and_emit_audit_fields() -> None:
    store = APIKeyStore()
    denied_secret, _ = store.create_key(scopes=(SCOPE_QUERY,))
    with pytest.raises(ScopeDeniedError):
        run_findings_explain_endpoint(
            api_key_header=denied_secret,
            key_store=store,
            request=ExplainRequest(
                question="Explain funded ratio shift",
                finding_slice=_slice(
                    slice_id="slice:ca",
                    plan_id="CA-PERS",
                    plan_period="FY2024",
                    funded_ratio=0.81,
                    citation="doc:1#p.12",
                ),
            ),
            chain=_StaticChain(
                {
                    "summary": "ok",
                    "key_drivers": [],
                    "caveats": [],
                    "citations": ["doc:1#p.12"],
                }
            ),
        )

    allowed_secret, _ = store.create_key(scopes=(SCOPE_NL,))
    compare_result = run_findings_compare_endpoint(
        api_key_header=allowed_secret,
        key_store=store,
        request=CompareRequest(
            question="Compare funded ratio for two periods",
            left_slice=_slice(
                slice_id="slice:left",
                plan_id="CA-PERS",
                plan_period="FY2023",
                funded_ratio=0.78,
                citation="doc:1#p.10",
            ),
            right_slice=_slice(
                slice_id="slice:right",
                plan_id="CA-PERS",
                plan_period="FY2024",
                funded_ratio=0.81,
                citation="doc:1#p.12",
            ),
        ),
        chain=_StaticChain(
            {
                "summary": "FY2024 improved over FY2023.",
                "key_differences": ["+3 percentage points"],
                "key_drivers": ["Contributions increased"],
                "caveats": ["Limited sample"],
                "citations": ["doc:1#p.10", "doc:1#p.12"],
            }
        ),
        event={
            "request_origin": "unit-test",
            "operation": "override-attempt",
            "api_key_id": "override-attempt",
        },
    )
    assert compare_result.response.status == "ok"
    assert compare_result.audit_event["operation"] == "nl.findings.compare"
    assert compare_result.audit_event["api_key_id"] != "override-attempt"
    assert compare_result.audit_event["request_origin"] == "unit-test"
    assert compare_result.audit_event["citation_count"] == 2
