"""Validation and review-queue routing for parser-derived funded outputs."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from pension_data.db.models.funded_actuarial import (
    FUNDED_ACTUARIAL_REQUIRED_METRICS,
    ExtractionDiagnostic,
    FundedActuarialStagingFact,
)
from pension_data.db.models.review_queue import ExtractionReviewQueueRecord
from pension_data.extract.common.ids import stable_id
from pension_data.quality.confidence import (
    ConfidenceRoutingDecision,
    ExtractionConfidenceInput,
    route_confidence_rows,
)
from pension_data.review_queue.extraction import build_extraction_review_queue

ParserValidationSeverity = Literal["warning", "error"]
ParserIncidentClassId = Literal[
    "parser_fallback_exhaustion",
    "parser_low_confidence_output",
    "parser_output_validation_failure",
]

PARSER_INCIDENT_RUNBOOKS: dict[ParserIncidentClassId, str] = {
    "parser_fallback_exhaustion": "docs/runbooks/parser-fallback-exhaustion.md#parser-fallback-exhaustion",
    "parser_output_validation_failure": (
        "docs/runbooks/parser-output-validation-failure.md#parser-output-validation-failure"
    ),
    "parser_low_confidence_output": (
        "docs/runbooks/parser-low-confidence-output.md#parser-low-confidence-output"
    ),
}

_ROW_REQUIRED_STRING_FIELDS: tuple[tuple[str, str], ...] = (
    ("plan_id", "plan_id"),
    ("plan_period", "plan_period"),
    ("metric_name", "metric_name"),
    ("source_document_id", "source_document_id"),
    ("source_url", "source_url"),
    ("effective_date", "effective_date"),
    ("ingestion_date", "ingestion_date"),
    ("parser_version", "parser_version"),
    ("extraction_method", "extraction_method"),
    ("normalized_unit", "normalized_unit"),
)
_VALID_EVIDENCE_REF = re.compile(r"^(?:p\.\d+(?:#.+)?|text:(?:\d+|unknown)|table:.+)$")


@dataclass(frozen=True, slots=True)
class ParserOutputValidationFinding:
    """One deterministic parser-validation finding."""

    finding_id: str
    code: str
    severity: ParserValidationSeverity
    plan_id: str
    plan_period: str
    metric_name: str | None
    message: str
    evidence_refs: tuple[str, ...]
    incident_class_id: ParserIncidentClassId
    runbook_path: str


@dataclass(frozen=True, slots=True)
class ParserOutputAnomaly:
    """Structured parser anomaly routed to operators and review queue tooling."""

    anomaly_id: str
    incident_class_id: ParserIncidentClassId
    severity: ParserValidationSeverity
    plan_id: str
    plan_period: str
    metric_name: str
    reason: str
    publish_blocked: bool
    evidence_refs: tuple[str, ...]
    runbook_path: str


@dataclass(frozen=True, slots=True)
class ParserOutputValidationResult:
    """Validation + review routing envelope for parser-derived output rows."""

    is_valid: bool
    publish_blocked: bool
    promotable_rows: tuple[FundedActuarialStagingFact, ...]
    findings: tuple[ParserOutputValidationFinding, ...]
    anomalies: tuple[ParserOutputAnomaly, ...]
    confidence_decisions: tuple[ConfidenceRoutingDecision, ...]
    review_queue_rows: tuple[ExtractionReviewQueueRecord, ...]


def _normalize_refs(evidence_refs: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for evidence_ref in evidence_refs:
        token = evidence_ref.strip()
        if not token or token in normalized:
            continue
        normalized.append(token)
    return tuple(normalized)


def _finding(
    *,
    code: str,
    severity: ParserValidationSeverity,
    plan_id: str,
    plan_period: str,
    metric_name: str | None,
    message: str,
    evidence_refs: Sequence[str],
    incident_class_id: ParserIncidentClassId,
) -> ParserOutputValidationFinding:
    refs = _normalize_refs(evidence_refs)
    return ParserOutputValidationFinding(
        finding_id=stable_id(
            "parser-finding",
            code,
            severity,
            plan_id,
            plan_period,
            metric_name,
            message,
            refs,
            incident_class_id,
        ),
        code=code,
        severity=severity,
        plan_id=plan_id,
        plan_period=plan_period,
        metric_name=metric_name,
        message=message,
        evidence_refs=refs,
        incident_class_id=incident_class_id,
        runbook_path=PARSER_INCIDENT_RUNBOOKS[incident_class_id],
    )


def _fallback_context(
    rows: Sequence[FundedActuarialStagingFact],
) -> tuple[str, str]:
    if not rows:
        return ("unknown_plan", "unknown_period")
    first = rows[0]
    return (first.plan_id, first.plan_period)


def _schema_findings(row: FundedActuarialStagingFact) -> list[ParserOutputValidationFinding]:
    findings: list[ParserOutputValidationFinding] = []
    for field_name, label in _ROW_REQUIRED_STRING_FIELDS:
        value = getattr(row, field_name)
        if isinstance(value, str) and value.strip():
            continue
        findings.append(
            _finding(
                code="schema_invalid",
                severity="error",
                plan_id=row.plan_id or "unknown_plan",
                plan_period=row.plan_period or "unknown_period",
                metric_name=row.metric_name,
                message=f"required field '{label}' is missing or empty",
                evidence_refs=row.evidence_refs,
                incident_class_id="parser_output_validation_failure",
            )
        )

    if not 0.0 <= row.confidence <= 1.0:
        findings.append(
            _finding(
                code="confidence_out_of_range",
                severity="error",
                plan_id=row.plan_id,
                plan_period=row.plan_period,
                metric_name=row.metric_name,
                message=f"confidence must be within [0,1], got {row.confidence}",
                evidence_refs=row.evidence_refs,
                incident_class_id="parser_output_validation_failure",
            )
        )
    return findings


def _metric_range_finding(row: FundedActuarialStagingFact) -> ParserOutputValidationFinding | None:
    value = row.normalized_value
    if value is None:
        return _finding(
            code="normalized_value_missing",
            severity="error",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            message="normalized_value is required for promotion",
            evidence_refs=row.evidence_refs,
            incident_class_id="parser_output_validation_failure",
        )

    if row.metric_name == "funded_ratio" and not 0.0 <= value <= 2.0:
        return _finding(
            code="numeric_out_of_range",
            severity="error",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            message=f"funded_ratio must be between 0 and 2, got {value}",
            evidence_refs=row.evidence_refs,
            incident_class_id="parser_output_validation_failure",
        )
    if (
        row.metric_name
        in {
            "discount_rate",
            "employer_contribution_rate",
            "employee_contribution_rate",
        }
        and not 0.0 <= value <= 1.0
    ):
        return _finding(
            code="numeric_out_of_range",
            severity="error",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            message=f"{row.metric_name} must be between 0 and 1, got {value}",
            evidence_refs=row.evidence_refs,
            incident_class_id="parser_output_validation_failure",
        )
    if row.metric_name in {"aal_usd", "ava_usd"} and value < 0.0:
        return _finding(
            code="numeric_out_of_range",
            severity="error",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            message=f"{row.metric_name} must be non-negative, got {value}",
            evidence_refs=row.evidence_refs,
            incident_class_id="parser_output_validation_failure",
        )
    if row.metric_name == "participant_count" and value < 0.0:
        return _finding(
            code="numeric_out_of_range",
            severity="error",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            message=f"participant_count must be non-negative, got {value}",
            evidence_refs=row.evidence_refs,
            incident_class_id="parser_output_validation_failure",
        )
    return None


def _provenance_findings(row: FundedActuarialStagingFact) -> list[ParserOutputValidationFinding]:
    if not row.evidence_refs:
        return [
            _finding(
                code="provenance_missing",
                severity="error",
                plan_id=row.plan_id,
                plan_period=row.plan_period,
                metric_name=row.metric_name,
                message="at least one evidence reference is required",
                evidence_refs=(),
                incident_class_id="parser_output_validation_failure",
            )
        ]

    invalid_refs = [
        ref for ref in row.evidence_refs if _VALID_EVIDENCE_REF.match(ref.strip()) is None
    ]
    if not invalid_refs:
        return []
    return [
        _finding(
            code="provenance_invalid",
            severity="error",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            message="evidence references must use canonical p./text:/table: formats",
            evidence_refs=invalid_refs,
            incident_class_id="parser_output_validation_failure",
        )
    ]


def _completeness_findings(
    rows: Sequence[FundedActuarialStagingFact],
) -> list[ParserOutputValidationFinding]:
    observed = {row.metric_name for row in rows}
    missing_metrics = [
        metric_name
        for metric_name in FUNDED_ACTUARIAL_REQUIRED_METRICS
        if metric_name not in observed
    ]
    plan_id, plan_period = _fallback_context(rows)
    return [
        _finding(
            code="required_metric_missing",
            severity="error",
            plan_id=plan_id,
            plan_period=plan_period,
            metric_name=metric_name,
            message=f"required parser metric '{metric_name}' is missing",
            evidence_refs=(),
            incident_class_id="parser_fallback_exhaustion",
        )
        for metric_name in missing_metrics
    ]


def _diagnostic_findings(
    diagnostics: Sequence[ExtractionDiagnostic],
    *,
    plan_id: str,
    plan_period: str,
) -> list[ParserOutputValidationFinding]:
    findings: list[ParserOutputValidationFinding] = []
    for item in diagnostics:
        incident_class: ParserIncidentClassId = (
            "parser_fallback_exhaustion"
            if item.code == "missing_metric"
            else "parser_output_validation_failure"
        )
        severity: ParserValidationSeverity = (
            "error" if item.code == "missing_metric" else item.severity
        )
        findings.append(
            _finding(
                code=f"extract_diagnostic:{item.code}",
                severity=severity,
                plan_id=plan_id,
                plan_period=plan_period,
                metric_name=item.metric_name,
                message=item.message,
                evidence_refs=item.evidence_refs,
                incident_class_id=incident_class,
            )
        )
    return findings


def _dedupe_findings(
    findings: Sequence[ParserOutputValidationFinding],
) -> tuple[ParserOutputValidationFinding, ...]:
    unique: dict[tuple[str, str, str, str, str, tuple[str, ...]], ParserOutputValidationFinding] = (
        {}
    )
    for finding in findings:
        key = (
            finding.code,
            finding.plan_id,
            finding.plan_period,
            finding.metric_name or "",
            finding.message,
            finding.evidence_refs,
        )
        unique[key] = finding
    return tuple(
        sorted(
            unique.values(),
            key=lambda item: (
                item.plan_id,
                item.plan_period,
                item.metric_name or "",
                item.code,
                item.severity,
            ),
        )
    )


def _row_id(row: FundedActuarialStagingFact) -> str:
    return stable_id(
        "parser-row",
        row.plan_id,
        row.plan_period,
        row.metric_name,
        row.source_document_id,
        row.evidence_refs,
    )


def _route_decisions(
    *,
    rows: Sequence[FundedActuarialStagingFact],
    findings: Sequence[ParserOutputValidationFinding],
) -> tuple[ConfidenceRoutingDecision, ...]:
    confidence_inputs = [
        ExtractionConfidenceInput(
            row_id=_row_id(row),
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            confidence=row.confidence,
            evidence_refs=row.evidence_refs,
        )
        for row in rows
    ]
    routed = route_confidence_rows(confidence_inputs)

    failure_decisions = [
        ConfidenceRoutingDecision(
            row_id=stable_id(
                "parser-validation-row",
                finding.code,
                finding.plan_id,
                finding.plan_period,
                finding.metric_name,
                finding.message,
            ),
            plan_id=finding.plan_id,
            plan_period=finding.plan_period,
            metric_name=finding.metric_name or "parser_validation",
            confidence=0.0,
            routing_outcome="high_priority_review",
            review_priority="high",
            publish_blocked=True,
            evidence_refs=finding.evidence_refs,
        )
        for finding in findings
        if finding.severity == "error"
    ]

    all_decisions = sorted(
        [*routed, *failure_decisions],
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.metric_name,
            row.row_id,
        ),
    )
    return tuple(all_decisions)


def _review_queue_timestamp(rows: Sequence[FundedActuarialStagingFact]) -> datetime:
    for row in rows:
        token = row.ingestion_date.strip()
        if not token:
            continue
        iso_candidate = f"{token[:-1]}+00:00" if token.endswith("Z") else token
        try:
            parsed = datetime.fromisoformat(iso_candidate)
        except ValueError:
            continue
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    return datetime.now(UTC)


def _build_anomalies(
    *,
    findings: Sequence[ParserOutputValidationFinding],
    decisions: Sequence[ConfidenceRoutingDecision],
) -> tuple[ParserOutputAnomaly, ...]:
    anomalies: list[ParserOutputAnomaly] = []
    for finding in findings:
        if finding.severity != "error":
            continue
        anomalies.append(
            ParserOutputAnomaly(
                anomaly_id=stable_id(
                    "parser-anomaly",
                    finding.incident_class_id,
                    finding.plan_id,
                    finding.plan_period,
                    finding.metric_name,
                    finding.code,
                ),
                incident_class_id=finding.incident_class_id,
                severity=finding.severity,
                plan_id=finding.plan_id,
                plan_period=finding.plan_period,
                metric_name=finding.metric_name or "parser_validation",
                reason=finding.message,
                publish_blocked=True,
                evidence_refs=finding.evidence_refs,
                runbook_path=finding.runbook_path,
            )
        )

    for decision in decisions:
        if decision.review_priority != "high" or decision.publish_blocked:
            continue
        anomalies.append(
            ParserOutputAnomaly(
                anomaly_id=stable_id(
                    "parser-anomaly",
                    "parser_low_confidence_output",
                    decision.plan_id,
                    decision.plan_period,
                    decision.metric_name,
                    decision.row_id,
                ),
                incident_class_id="parser_low_confidence_output",
                severity="warning",
                plan_id=decision.plan_id,
                plan_period=decision.plan_period,
                metric_name=decision.metric_name,
                reason=f"low confidence output ({decision.confidence:.2f}) routed to high-priority review",
                publish_blocked=False,
                evidence_refs=decision.evidence_refs,
                runbook_path=PARSER_INCIDENT_RUNBOOKS["parser_low_confidence_output"],
            )
        )

    deduped: dict[str, ParserOutputAnomaly] = {item.anomaly_id: item for item in anomalies}
    return tuple(
        sorted(
            deduped.values(),
            key=lambda item: (
                item.incident_class_id,
                item.plan_id,
                item.plan_period,
                item.metric_name,
                item.anomaly_id,
            ),
        )
    )


def validate_parser_outputs(
    *,
    rows: Sequence[FundedActuarialStagingFact],
    diagnostics: Sequence[ExtractionDiagnostic] = (),
) -> ParserOutputValidationResult:
    """Validate parser outputs and route failures/low-confidence records for review."""
    findings: list[ParserOutputValidationFinding] = []
    for row in rows:
        findings.extend(_schema_findings(row))
        range_finding = _metric_range_finding(row)
        if range_finding is not None:
            findings.append(range_finding)
        findings.extend(_provenance_findings(row))

    findings.extend(_completeness_findings(rows))
    plan_id, plan_period = _fallback_context(rows)
    findings.extend(_diagnostic_findings(diagnostics, plan_id=plan_id, plan_period=plan_period))
    deduped_findings = _dedupe_findings(findings)

    decisions = _route_decisions(rows=rows, findings=deduped_findings)
    review_queue_rows = tuple(
        build_extraction_review_queue(
            decisions,
            queued_at=_review_queue_timestamp(rows),
        )
    )
    anomalies = _build_anomalies(findings=deduped_findings, decisions=decisions)

    publish_blocked = any(finding.severity == "error" for finding in deduped_findings)
    promotable_rows = () if publish_blocked else tuple(rows)
    return ParserOutputValidationResult(
        is_valid=not publish_blocked,
        publish_blocked=publish_blocked,
        promotable_rows=promotable_rows,
        findings=deduped_findings,
        anomalies=anomalies,
        confidence_decisions=decisions,
        review_queue_rows=review_queue_rows,
    )
