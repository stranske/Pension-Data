"""Funded/actuarial staging fact models with bitemporal metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

MetricExtractionMethod = Literal["table_lookup", "text_pattern", "fallback_inference"]
DiagnosticSeverity = Literal["warning", "error"]
FundedActuarialMetricName = Literal[
    "funded_ratio",
    "aal_usd",
    "ava_usd",
    "discount_rate",
    "employer_contribution_rate",
    "employee_contribution_rate",
    "participant_count",
]

FUNDED_ACTUARIAL_REQUIRED_METRICS: tuple[FundedActuarialMetricName, ...] = (
    "funded_ratio",
    "aal_usd",
    "ava_usd",
    "discount_rate",
    "employer_contribution_rate",
    "employee_contribution_rate",
    "participant_count",
)


@dataclass(frozen=True, slots=True)
class FundedActuarialStagingFact:
    """Staging fact row for funded/actuarial extraction output."""

    plan_id: str
    plan_period: str
    metric_name: FundedActuarialMetricName
    as_reported_value: float | None
    normalized_value: float | None
    as_reported_unit: str | None
    normalized_unit: str | None
    effective_date: str
    ingestion_date: str
    source_document_id: str
    source_url: str
    extraction_method: MetricExtractionMethod
    confidence: float
    parser_version: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExtractionDiagnostic:
    """Missing/ambiguous parser diagnostic emitted by funded/actuarial extraction."""

    code: str
    severity: DiagnosticSeverity
    metric_name: FundedActuarialMetricName
    message: str
    evidence_refs: tuple[str, ...]
