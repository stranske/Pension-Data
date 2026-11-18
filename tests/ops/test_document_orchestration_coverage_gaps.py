"""Targeted orchestration branches not covered by the primary smoke and isolation suites."""

from __future__ import annotations

from dataclasses import replace

import pytest

import pension_data.ops.document_orchestration as orchestration_module
from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.extract.governance.consultants import ConsultantExtractionWarning
from pension_data.extract.investment.manager_positions import (
    ExtractionWarning as ManagerPositionWarning,
)
from pension_data.extract.investment.risk_disclosures import RiskExtractionDiagnostic
from pension_data.ops.document_orchestration import (
    DocumentOrchestrationState,
    SourceDocumentJobItem,
    run_document_orchestration,
)
from pension_data.quality.parser_output_validation import (
    ParserOutputValidationFinding,
    ParserOutputValidationResult,
)

from .test_document_orchestration import _artifact_rows, _job, _parser


def _empty_raw(item: SourceDocumentJobItem, _artifact: object) -> RawFundedActuarialInput:
    typed = item
    return RawFundedActuarialInput(
        source_document_id=typed.source_document_id,
        source_url=typed.source_url,
        effective_date=typed.effective_date,
        ingestion_date=typed.ingestion_date,
        default_money_unit_scale=typed.default_money_unit_scale,
        text_blocks=(),
        table_rows=(),
    )


def test_invalid_fetched_at_records_ingestion_and_parse_failures() -> None:
    document = replace(
        _job(
            source_url="https://example.org/bad-timestamp.pdf",
            fetched_at="2026-03-03T00:00:00Z",
            source_document_id="doc:bad-ts",
            content=b"doc",
        ),
        fetched_at="not-a-timestamp",
    )
    ledger, state, artifacts = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="bad-fetched-at",
    )

    assert ledger.status == "failed"
    assert any(failure.stage == "ingestion" for failure in ledger.failures)
    assert any("failed_count=1" in failure.message for failure in ledger.failures)
    outcome = ledger.document_outcomes[0]
    assert outcome.status == "failed"
    assert outcome.artifact_id is None
    assert "ingestion did not produce active artifact" in outcome.notes
    assert state.processed_artifact_ids == ()
    assert _artifact_rows(artifacts, "published_rows") == []


def test_component_coverage_failure_without_list_missing_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )

    monkeypatch.setattr(
        orchestration_module,
        "validate_component_coverage",
        lambda **_kwargs: {
            "is_valid": False,
            "missing_components": "not-a-list",
            "invalid_state_rows": [],
            "metadata_violations": [],
        },
    )

    ledger, _, _ = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="coverage-non-list",
    )

    message = next(failure.message for failure in ledger.failures if failure.stage == "validation")
    assert "missing_components=0" in message
    assert "sample=" not in message


def test_component_coverage_failure_omits_sample_when_missing_names_blank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )

    monkeypatch.setattr(
        orchestration_module,
        "validate_component_coverage",
        lambda **_kwargs: {
            "is_valid": False,
            "missing_components": ["  ", "   "],
            "invalid_state_rows": [],
            "metadata_violations": [],
        },
    )

    ledger, _, _ = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="coverage-blank",
    )

    message = next(failure.message for failure in ledger.failures if failure.stage == "validation")
    assert "sample=" not in message
    assert "missing_components=2" in message


def test_component_coverage_failure_includes_structured_detail_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )

    def _invalid_report(**_kwargs: object) -> dict[str, object]:
        return {
            "is_valid": False,
            "missing_components": ["  ", "metric_observation"],
            "invalid_state_rows": [{"row_id": "bad"}],
            "metadata_violations": [{"field": "ingestion_date"}],
        }

    monkeypatch.setattr(orchestration_module, "validate_component_coverage", _invalid_report)

    ledger, _, _ = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="coverage-detail",
    )

    validation_failures = [failure for failure in ledger.failures if failure.stage == "validation"]
    assert len(validation_failures) == 1
    message = validation_failures[0].message
    assert "missing_components=2" in message
    assert "sample=metric_observation" in message
    assert "invalid_state_rows=1" in message
    assert "metadata_violations=1" in message


def test_funded_validation_skips_non_error_findings(monkeypatch: pytest.MonkeyPatch) -> None:
    original_validator = orchestration_module.validate_parser_outputs

    def _with_warning_finding(**kwargs: object) -> ParserOutputValidationResult:
        result = original_validator(**kwargs)
        warning = ParserOutputValidationFinding(
            finding_id="finding:warn",
            code="synthetic_warning",
            severity="warning",
            plan_id="CA-PERS",
            plan_period="FY2024",
            metric_name="discount_rate",
            message="synthetic warning only",
            evidence_refs=(),
            incident_class_id="parser_output_validation_failure",
            runbook_path="docs/runbooks/parser-output-validation-failure.md",
        )
        return replace(result, findings=(*result.findings, warning))

    monkeypatch.setattr(orchestration_module, "validate_parser_outputs", _with_warning_finding)
    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )
    ledger, _, artifacts = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="validation-warning",
    )

    assert ledger.status == "success"
    assert _artifact_rows(artifacts, "published_rows")


def test_orchestration_records_domain_warning_confidence_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_risk = orchestration_module.extract_risk_exposure_observations
    original_consultant = orchestration_module.extract_consultant_records
    original_manager = orchestration_module.build_manager_fund_positions
    original_lifecycle = orchestration_module.infer_lifecycle_events

    def _risk_with_diagnostic(*args: object, **kwargs: object):
        rows, _ = original_risk(*args, **kwargs)
        diagnostic = RiskExtractionDiagnostic(
            code="policy_only",
            disclosure_type="derivatives",
            metric_name="swaps",
            message="policy only",
            evidence_refs=("p.70",),
        )
        return rows, [diagnostic]

    def _consultant_with_warning(*args: object, **kwargs: object):
        payload = original_consultant(*args, **kwargs)
        payload["warnings"] = [
            ConsultantExtractionWarning(
                code="missing_topic",
                plan_id="CA-PERS",
                plan_period="FY2024",
                message="missing topic",
                evidence_refs=("p.consult",),
            )
        ]
        return payload

    def _manager_with_warning(*args: object, **kwargs: object):
        rows, _ = original_manager(*args, **kwargs)
        warning = ManagerPositionWarning(
            code="partial_disclosure",
            plan_id="CA-PERS",
            plan_period="FY2024",
            manager_name="Alpha Capital",
            fund_name="Fund I",
            message="partial",
            evidence_refs=("p.88",),
        )
        return rows, [warning]

    def _lifecycle_with_warning(*args: object, **kwargs: object):
        rows, _ = original_lifecycle(*args, **kwargs)
        warning = ManagerPositionWarning(
            code="non_disclosure",
            plan_id="CA-PERS",
            plan_period="FY2024",
            manager_name=None,
            fund_name=None,
            message="none",
            evidence_refs=(),
        )
        return rows, [warning]

    monkeypatch.setattr(
        orchestration_module, "extract_risk_exposure_observations", _risk_with_diagnostic
    )
    monkeypatch.setattr(
        orchestration_module, "extract_consultant_records", _consultant_with_warning
    )
    monkeypatch.setattr(orchestration_module, "build_manager_fund_positions", _manager_with_warning)
    monkeypatch.setattr(orchestration_module, "infer_lifecycle_events", _lifecycle_with_warning)

    document = _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )
    ledger, _, artifacts = run_document_orchestration(
        documents=[document],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="warning-inputs",
    )

    assert ledger.status == "success"
    review_rows = _artifact_rows(artifacts, "review_queue_rows")
    metric_names = {row["metric_name"] for row in review_rows}
    assert "risk_exposure:swaps:policy_only" in metric_names
    assert "consultant:missing_topic" in metric_names
    assert "manager_position:partial_disclosure" in metric_names
    assert "manager_position:non_disclosure" in metric_names


def test_no_publishable_domain_output_marks_document_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_validation = ParserOutputValidationResult(
        is_valid=True,
        publish_blocked=False,
        promotable_rows=(),
        findings=(),
        anomalies=(),
        confidence_decisions=(),
        review_queue_rows=(),
    )
    monkeypatch.setattr(
        orchestration_module,
        "validate_component_coverage",
        lambda **_kwargs: {
            "is_valid": True,
            "missing_components": [],
            "invalid_state_rows": [],
            "metadata_violations": [],
        },
    )
    monkeypatch.setattr(
        orchestration_module, "validate_parser_outputs", lambda **_kwargs: empty_validation
    )
    monkeypatch.setattr(
        orchestration_module, "extract_funded_and_actuarial_metrics", lambda **_kwargs: ([], [])
    )
    monkeypatch.setattr(
        orchestration_module, "extract_plan_financial_flow", lambda **_kwargs: (None, [])
    )
    monkeypatch.setattr(
        orchestration_module, "extract_risk_exposure_observations", lambda **_kwargs: ([], [])
    )
    monkeypatch.setattr(
        orchestration_module, "build_manager_fund_positions", lambda *_args, **_kwargs: ([], [])
    )
    monkeypatch.setattr(
        orchestration_module, "infer_lifecycle_events", lambda *_args, **_kwargs: ([], [])
    )
    monkeypatch.setattr(
        orchestration_module,
        "build_extraction_persistence_artifacts",
        lambda **_kwargs: {
            "staging_core_metrics_rows": [],
            "staging_manager_fund_vehicle_relationship_rows": [],
            "extraction_warning_rows": [],
        },
    )

    document = _job(
        source_url="https://example.org/empty.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:empty",
        content=b"doc",
    )
    ledger, state, artifacts = run_document_orchestration(
        documents=[document],
        parser=_empty_raw,
        state=DocumentOrchestrationState(),
        run_id="no-publish",
    )

    outcome = ledger.document_outcomes[0]
    assert outcome.status == "failed"
    assert outcome.notes == "no publishable domain output"
    assert outcome.promoted_fact_count == 0
    assert _artifact_rows(artifacts, "published_rows") == []
    assert state.processed_artifact_ids == ()
