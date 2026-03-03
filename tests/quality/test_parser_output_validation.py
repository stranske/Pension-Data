"""Tests for parser-output validation and review-queue integration."""

from __future__ import annotations

from pension_data.db.models.funded_actuarial import (
    ExtractionDiagnostic,
    FundedActuarialMetricName,
    FundedActuarialStagingFact,
)
from pension_data.quality.parser_output_validation import validate_parser_outputs


def _row(
    *,
    metric_name: FundedActuarialMetricName,
    normalized_value: float,
    confidence: float,
    evidence_refs: tuple[str, ...] = ("p.40#table",),
) -> FundedActuarialStagingFact:
    return FundedActuarialStagingFact(
        plan_id="CA-PERS",
        plan_period="FY2024",
        metric_name=metric_name,
        as_reported_value=normalized_value,
        normalized_value=normalized_value,
        as_reported_unit="ratio",
        normalized_unit="ratio",
        effective_date="2024-06-30",
        ingestion_date="2026-03-02",
        source_document_id="doc:ca:2024:acfr",
        source_url="https://example.org/ca-2024.pdf",
        extraction_method="table_lookup",
        confidence=confidence,
        parser_version="funded_actuarial_v1",
        evidence_refs=evidence_refs,
    )


def _complete_rows(
    *,
    low_confidence_metric: FundedActuarialMetricName | None = None,
) -> list[FundedActuarialStagingFact]:
    values: dict[FundedActuarialMetricName, float] = {
        "funded_ratio": 0.78,
        "aal_usd": 640_000_000.0,
        "ava_usd": 501_800_000.0,
        "discount_rate": 0.068,
        "employer_contribution_rate": 0.124,
        "employee_contribution_rate": 0.075,
        "participant_count": 325_000.0,
    }
    rows: list[FundedActuarialStagingFact] = []
    for metric_name, value in values.items():
        rows.append(
            _row(
                metric_name=metric_name,
                normalized_value=value,
                confidence=0.60 if metric_name == low_confidence_metric else 0.94,
            )
        )
    return rows


def test_valid_parser_outputs_are_not_blocked_and_do_not_enqueue_review_rows() -> None:
    result = validate_parser_outputs(rows=_complete_rows())

    assert result.is_valid is True
    assert result.publish_blocked is False
    assert len(result.promotable_rows) == 7
    assert result.findings == ()
    assert result.anomalies == ()
    assert result.review_queue_rows == ()


def test_invalid_parser_outputs_block_promotion_and_route_failures_for_review() -> None:
    rows = _complete_rows()
    rows = [row for row in rows if row.metric_name != "participant_count"]
    rows[0] = _row(
        metric_name="funded_ratio",
        normalized_value=2.5,
        confidence=0.91,
        evidence_refs=("page 40",),
    )
    diagnostics = [
        ExtractionDiagnostic(
            code="missing_metric",
            severity="warning",
            metric_name="participant_count",
            message="required metric 'participant_count' was not found in text or tables",
            evidence_refs=(),
        )
    ]

    result = validate_parser_outputs(rows=rows, diagnostics=diagnostics)

    assert result.is_valid is False
    assert result.publish_blocked is True
    assert result.promotable_rows == ()
    assert any(finding.code == "required_metric_missing" for finding in result.findings)
    assert any(finding.code == "numeric_out_of_range" for finding in result.findings)
    assert any(finding.code == "provenance_invalid" for finding in result.findings)
    assert any(
        finding.incident_class_id == "parser_fallback_exhaustion" for finding in result.findings
    )
    assert any(
        anomaly.incident_class_id == "parser_output_validation_failure"
        for anomaly in result.anomalies
    )
    assert all(row.priority == "high" for row in result.review_queue_rows)


def test_low_confidence_parser_outputs_are_visible_in_review_queue_without_blocking() -> None:
    result = validate_parser_outputs(rows=_complete_rows(low_confidence_metric="discount_rate"))

    assert result.is_valid is True
    assert result.publish_blocked is False
    assert len(result.promotable_rows) == 7
    assert any(row.priority == "high" for row in result.review_queue_rows)
    assert any(
        anomaly.incident_class_id == "parser_low_confidence_output"
        for anomaly in result.anomalies
    )
    assert all(anomaly.publish_blocked is False for anomaly in result.anomalies)


def test_validation_result_is_deterministic_and_review_queue_contract_safe() -> None:
    rows = _complete_rows(low_confidence_metric="discount_rate")
    first = validate_parser_outputs(rows=rows)
    second = validate_parser_outputs(rows=rows)

    assert first == second
    assert all(row.state == "new" for row in first.review_queue_rows)
    assert all(
        row.audit_trail and row.audit_trail[0].actor == "system"
        for row in first.review_queue_rows
    )
