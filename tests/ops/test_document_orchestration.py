"""Smoke tests for end-to-end document orchestration workflow."""

from __future__ import annotations

import pytest

import pension_data.ops.document_orchestration as document_orchestration_module
from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.ops.document_orchestration import (
    DocumentOrchestrationState,
    SourceDocumentJobItem,
    run_document_orchestration,
)


def _artifact_rows(artifacts: dict[str, object], key: str) -> list[object]:
    value = artifacts[key]
    assert isinstance(value, list)
    return value


def _job(
    *,
    source_url: str,
    fetched_at: str,
    source_document_id: str,
    content: bytes,
) -> SourceDocumentJobItem:
    return SourceDocumentJobItem(
        plan_id="CA-PERS",
        plan_period="FY2024",
        source_url=source_url,
        fetched_at=fetched_at,
        mime_type="application/pdf",
        content_bytes=content,
        source_document_id=source_document_id,
        effective_date="2024-06-30",
        ingestion_date="2026-03-03",
        default_money_unit_scale="million_usd",
    )


def _parser(item: SourceDocumentJobItem, _artifact: object) -> RawFundedActuarialInput:
    marker = item.content_bytes.decode("utf-8", errors="ignore")
    funded_ratio = "79.2%" if "rev2" in marker else "78.4%"
    text_blocks = (
        "consultant:Mercer|Investment consultant",
        "recommendation:Mercer|asset allocation|raise private credit|adopted",
        "attribution:Mercer|asset allocation|board approved in Q4|explicit",
    )
    table_rows = (
        {"label": "Funded Ratio", "value": funded_ratio, "evidence_ref": "p.40"},
        {"label": "AAL", "value": "$640 million", "evidence_ref": "p.40"},
        {"label": "AVA", "value": "$501.8 million", "evidence_ref": "p.40"},
        {"label": "Discount Rate", "value": "6.8%", "evidence_ref": "p.41"},
        {"label": "Employer Contribution Rate", "value": "12.4%", "evidence_ref": "p.41"},
        {"label": "Employee Contribution Rate", "value": "7.5%", "evidence_ref": "p.41"},
        {"label": "Participant Count", "value": "325000", "evidence_ref": "p.42"},
        {"label": "Beginning AUM", "value": "$480 million", "evidence_ref": "p.55"},
        {"label": "Ending AUM", "value": "$501 million", "evidence_ref": "p.55"},
        {"label": "Employer Contributions", "value": "$18 million", "evidence_ref": "p.55"},
        {"label": "Employee Contributions", "value": "$9 million", "evidence_ref": "p.55"},
        {"label": "Benefit Payments", "value": "$-5 million", "evidence_ref": "p.55"},
        {"label": "Refunds", "value": "$-1 million", "evidence_ref": "p.55"},
        {"label": "allocation:Public Equity", "value": "45%", "evidence_ref": "p.61"},
        {"label": "allocation:Private Equity", "value": "25%", "evidence_ref": "p.61"},
        {"label": "fee:Mercer:management_fee:rate", "value": "0.55%", "evidence_ref": "p.62"},
        {
            "label": "fee:Mercer:management_fee:amount",
            "value": "$2.4 million",
            "evidence_ref": "p.62",
        },
        {
            "label": "risk:derivatives:swaps:policy_limit",
            "value": "$45 million",
            "evidence_ref": "p.70",
        },
        {
            "label": "risk:derivatives:swaps:realized_exposure",
            "value": "$21 million",
            "evidence_ref": "p.70",
        },
        {
            "label": "risk:securities_lending:core:policy_limit",
            "value": "$30 million",
            "evidence_ref": "p.71",
        },
        {
            "label": "risk:securities_lending:core:realized_exposure",
            "value": "$12 million",
            "evidence_ref": "p.71",
        },
        {
            "label": "position:Alpha Capital:Fund I:market_value",
            "value": "$33 million",
            "evidence_ref": "p.88",
        },
        {
            "label": "position:Alpha Capital:Fund I:commitment",
            "value": "$40 million",
            "evidence_ref": "p.88",
        },
        {
            "label": "position:Alpha Capital:Fund I:unfunded",
            "value": "$7 million",
            "evidence_ref": "p.88",
        },
    )
    return RawFundedActuarialInput(
        source_document_id=item.source_document_id,
        source_url=item.source_url,
        effective_date=item.effective_date,
        ingestion_date=item.ingestion_date,
        default_money_unit_scale=item.default_money_unit_scale,
        text_blocks=text_blocks,
        table_rows=table_rows,
    )


def test_one_document_run_is_reproducible_and_idempotent() -> None:
    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )

    first_ledger, first_state, first_artifacts = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="run-1",
        max_retries_per_stage=1,
    )
    second_ledger, second_state, second_artifacts = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=first_state,
        run_id="run-2",
        max_retries_per_stage=1,
    )

    assert first_ledger.status == "success"
    assert first_ledger.document_outcomes[0].status == "processed"
    assert first_ledger.document_outcomes[0].promoted_fact_count > 0
    assert len(_artifact_rows(first_artifacts, "published_rows")) > 0
    assert len(_artifact_rows(first_artifacts, "financial_flow_rows")) == 1
    assert len(_artifact_rows(first_artifacts, "risk_exposure_rows")) >= 2
    assert len(_artifact_rows(first_artifacts, "consultant_engagement_rows")) == 1
    assert len(_artifact_rows(first_artifacts, "lifecycle_event_rows")) >= 1
    domain_metrics = {
        (metric.stage, metric.domain)
        for metric in first_ledger.stage_metrics
        if metric.domain is not None
    }
    for domain in (
        "funded_actuarial",
        "financial_flow",
        "allocation_fee",
        "risk_exposure",
        "consultant",
        "manager_position",
    ):
        assert ("parse_extract", domain) in domain_metrics
        assert ("validation", domain) in domain_metrics
        assert ("publish", domain) in domain_metrics
    assert first_ledger.failures == ()

    assert second_ledger.status == "success"
    assert second_ledger.document_outcomes[0].status == "skipped"
    assert _artifact_rows(second_artifacts, "published_rows") == []
    assert second_state.published_fact_ids == first_state.published_fact_ids


def test_domain_failure_is_isolated_and_other_domains_still_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )

    def _raise_risk(*_args: object, **_kwargs: object) -> tuple[list[object], list[object]]:
        raise RuntimeError("risk extraction unavailable")

    monkeypatch.setattr(
        document_orchestration_module,
        "extract_risk_exposure_observations",
        _raise_risk,
    )

    ledger, state, artifacts = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="run-risk-failure",
    )

    outcome = ledger.document_outcomes[0]
    assert outcome.status == "processed"
    assert "risk_exposure" in outcome.notes
    assert len(_artifact_rows(artifacts, "financial_flow_rows")) == 1
    assert len(_artifact_rows(artifacts, "consultant_engagement_rows")) == 1
    assert any("domain=risk_exposure" in failure.message for failure in ledger.failures)
    assert state.processed_artifact_ids == ()


def test_revised_document_reprocesses_with_lineage_preserved() -> None:
    first_doc = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )
    second_doc = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-05T00:00:00Z",
        source_document_id="doc:ca:2024:v2",
        content=b"doc-rev2",
    )

    _first_ledger, first_state, _first_artifacts = run_document_orchestration(
        documents=[first_doc],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="run-1",
    )
    second_ledger, second_state, second_artifacts = run_document_orchestration(
        documents=[second_doc],
        parser=_parser,
        state=first_state,
        run_id="run-2",
    )

    outcome = second_ledger.document_outcomes[0]
    assert second_ledger.status == "success"
    assert outcome.status == "processed"
    assert outcome.supersedes_artifact_id is not None
    assert "reprocessed revised artifact" in outcome.notes
    assert len(second_state.processed_artifact_ids) == 2
    assert len(_artifact_rows(second_artifacts, "published_rows")) > 0


def test_small_batch_run_records_actionable_stage_failure_diagnostics() -> None:
    first_doc = _job(
        source_url="https://example.org/ca-good.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:good",
        content=b"good",
    )
    second_doc = _job(
        source_url="https://example.org/ca-bad.pdf",
        fetched_at="2026-03-03T00:00:01Z",
        source_document_id="doc:ca:bad",
        content=b"bad",
    )

    def _flaky_parser(item: SourceDocumentJobItem, artifact: object) -> RawFundedActuarialInput:
        if item.source_url.endswith("ca-bad.pdf"):
            raise RuntimeError("synthetic parser failure for smoke test")
        return _parser(item, artifact)

    ledger, state, artifacts = run_document_orchestration(
        documents=[first_doc, second_doc],
        parser=_flaky_parser,
        state=DocumentOrchestrationState(),
        run_id="run-batch",
        max_retries_per_stage=0,
    )

    statuses = {item.source_url: item.status for item in ledger.document_outcomes}
    assert ledger.status == "failed"
    assert statuses["https://example.org/ca-good.pdf"] == "processed"
    assert statuses["https://example.org/ca-bad.pdf"] == "failed"
    assert any(failure.stage == "parse_extract" for failure in ledger.failures)
    assert any("RuntimeError" in failure.message for failure in ledger.failures)
    assert len(state.processed_artifact_ids) == 1
    assert len(_artifact_rows(artifacts, "published_rows")) > 0
