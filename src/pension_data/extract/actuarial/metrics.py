"""Funded/actuarial metric extraction with diagnostics and normalization."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from pension_data.db.models.funded_actuarial import (
    FUNDED_ACTUARIAL_REQUIRED_METRICS,
    ExtractionDiagnostic,
    FundedActuarialMetricName,
    FundedActuarialStagingFact,
    MetricExtractionMethod,
)
from pension_data.normalize.financial_units import UnitScale, normalize_money_to_usd

ParserMetricKind = Literal["money", "ratio", "count"]

PARSER_VERSION = "funded_actuarial_v1"
_NUMBER_PATTERN = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")
_VALUE_TOLERANCE = 1e-9

_METRIC_DEFINITIONS: dict[FundedActuarialMetricName, tuple[ParserMetricKind, tuple[str, ...]]] = {
    "funded_ratio": ("ratio", ("funded ratio", "funding ratio")),
    "aal_usd": ("money", ("aal", "actuarial accrued liability")),
    "ava_usd": ("money", ("ava", "actuarial value of assets")),
    "discount_rate": ("ratio", ("discount rate", "assumed return")),
    "employer_contribution_rate": ("ratio", ("employer contribution rate", "adc rate")),
    "employee_contribution_rate": ("ratio", ("employee contribution rate",)),
    "participant_count": ("count", ("participant count", "active participants")),
}


@dataclass(frozen=True, slots=True)
class RawFundedActuarialInput:
    """Raw report fragments for funded/actuarial extraction."""

    source_document_id: str
    source_url: str
    effective_date: str
    ingestion_date: str
    default_money_unit_scale: UnitScale
    text_blocks: tuple[str, ...]
    table_rows: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class _CandidateMetric:
    metric_name: FundedActuarialMetricName
    as_reported_value: float | None
    normalized_value: float | None
    as_reported_unit: str | None
    normalized_unit: str | None
    extraction_method: MetricExtractionMethod
    confidence: float
    evidence_ref: str


def _parse_numeric_token(text: str) -> float | None:
    match = _NUMBER_PATTERN.search(text)
    if match is None:
        return None
    token = match.group(0).replace(",", "")
    return float(token)


def _detect_money_scale(text: str, *, fallback: UnitScale) -> UnitScale:
    lowered = text.lower()
    if "billion" in lowered or " bn" in lowered:
        return "billion_usd"
    if "million" in lowered or " mm" in lowered:
        return "million_usd"
    if "thousand" in lowered or " k" in lowered:
        return "thousand_usd"
    return fallback


def _normalize_metric_value(
    *,
    metric_kind: ParserMetricKind,
    raw_text: str,
    raw_value: float,
    fallback_money_scale: UnitScale,
) -> tuple[float, float, str, str]:
    lowered = raw_text.lower()
    if metric_kind == "money":
        scale = _detect_money_scale(raw_text, fallback=fallback_money_scale)
        normalized = normalize_money_to_usd(raw_value, unit_scale=scale)
        if normalized is None:
            raise ValueError("money normalization unexpectedly returned None")
        return raw_value, normalized, scale, "usd"

    if metric_kind == "ratio":
        if "%" in raw_text or "percent" in lowered:
            return raw_value, round(raw_value / 100.0, 9), "percent", "ratio"
        if raw_value > 1.0:
            return raw_value, round(raw_value / 100.0, 9), "percent_assumed", "ratio"
        return raw_value, raw_value, "ratio", "ratio"

    return raw_value, float(int(round(raw_value))), "count", "count"


def _find_candidate_in_text(
    *,
    metric_name: FundedActuarialMetricName,
    metric_kind: ParserMetricKind,
    aliases: tuple[str, ...],
    text: str,
    evidence_ref: str,
    fallback_money_scale: UnitScale,
) -> _CandidateMetric | None:
    lowered = text.lower()
    if not any(alias in lowered for alias in aliases):
        return None
    numeric = _parse_numeric_token(text)
    if numeric is None:
        return None
    as_reported, normalized, as_unit, normalized_unit = _normalize_metric_value(
        metric_kind=metric_kind,
        raw_text=text,
        raw_value=numeric,
        fallback_money_scale=fallback_money_scale,
    )
    return _CandidateMetric(
        metric_name=metric_name,
        as_reported_value=as_reported,
        normalized_value=normalized,
        as_reported_unit=as_unit,
        normalized_unit=normalized_unit,
        extraction_method="text_pattern",
        confidence=0.84,
        evidence_ref=evidence_ref,
    )


def _find_candidate_in_table_row(
    *,
    metric_name: FundedActuarialMetricName,
    metric_kind: ParserMetricKind,
    aliases: tuple[str, ...],
    row: dict[str, str],
    fallback_money_scale: UnitScale,
) -> _CandidateMetric | None:
    label = row.get("label", "").strip().lower()
    if not label:
        return None
    if not any(alias in label for alias in aliases):
        return None
    value_text = row.get("value", "").strip()
    if not value_text:
        return None
    numeric = _parse_numeric_token(value_text)
    if numeric is None:
        return None
    as_reported, normalized, as_unit, normalized_unit = _normalize_metric_value(
        metric_kind=metric_kind,
        raw_text=value_text,
        raw_value=numeric,
        fallback_money_scale=fallback_money_scale,
    )
    evidence_ref = row.get("evidence_ref", "").strip() or "table"
    return _CandidateMetric(
        metric_name=metric_name,
        as_reported_value=as_reported,
        normalized_value=normalized,
        as_reported_unit=as_unit,
        normalized_unit=normalized_unit,
        extraction_method="table_lookup",
        confidence=0.93,
        evidence_ref=evidence_ref,
    )


def _resolve_candidates(
    *,
    metric_name: FundedActuarialMetricName,
    candidates: list[_CandidateMetric],
) -> tuple[_CandidateMetric | None, list[ExtractionDiagnostic]]:
    if not candidates:
        return (
            None,
            [
                ExtractionDiagnostic(
                    code="missing_metric",
                    severity="warning",
                    metric_name=metric_name,
                    message=f"required metric '{metric_name}' was not found in text or tables",
                    evidence_refs=(),
                )
            ],
        )

    diagnostics: list[ExtractionDiagnostic] = []
    unique_values = sorted(
        {
            round(candidate.normalized_value or 0.0, 9)
            for candidate in candidates
            if candidate.normalized_value is not None
        }
    )
    if len(unique_values) > 1 and max(unique_values) - min(unique_values) > _VALUE_TOLERANCE:
        diagnostics.append(
            ExtractionDiagnostic(
                code="ambiguous_metric",
                severity="warning",
                metric_name=metric_name,
                message=(
                    f"multiple normalized values found for '{metric_name}': "
                    + ", ".join(str(value) for value in unique_values)
                ),
                evidence_refs=tuple(candidate.evidence_ref for candidate in candidates),
            )
        )

    chosen = sorted(
        candidates,
        key=lambda item: (
            item.confidence,
            item.extraction_method == "table_lookup",
            item.evidence_ref,
        ),
        reverse=True,
    )[0]
    return chosen, diagnostics


def extract_funded_and_actuarial_metrics(
    *,
    plan_id: str,
    plan_period: str,
    raw: RawFundedActuarialInput,
) -> tuple[list[FundedActuarialStagingFact], list[ExtractionDiagnostic]]:
    """Extract required funded/actuarial fields with diagnostics and confidence scores."""
    candidates_by_metric: dict[FundedActuarialMetricName, list[_CandidateMetric]] = {
        metric_name: [] for metric_name in FUNDED_ACTUARIAL_REQUIRED_METRICS
    }

    for metric_name, (metric_kind, aliases) in _METRIC_DEFINITIONS.items():
        for index, block in enumerate(raw.text_blocks):
            candidate = _find_candidate_in_text(
                metric_name=metric_name,
                metric_kind=metric_kind,
                aliases=aliases,
                text=block,
                evidence_ref=f"text:{index + 1}",
                fallback_money_scale=raw.default_money_unit_scale,
            )
            if candidate is not None:
                candidates_by_metric[metric_name].append(candidate)
        for row in raw.table_rows:
            candidate = _find_candidate_in_table_row(
                metric_name=metric_name,
                metric_kind=metric_kind,
                aliases=aliases,
                row=row,
                fallback_money_scale=raw.default_money_unit_scale,
            )
            if candidate is not None:
                candidates_by_metric[metric_name].append(candidate)

    staging_facts: list[FundedActuarialStagingFact] = []
    diagnostics: list[ExtractionDiagnostic] = []
    for metric_name in FUNDED_ACTUARIAL_REQUIRED_METRICS:
        selected, metric_diagnostics = _resolve_candidates(
            metric_name=metric_name,
            candidates=candidates_by_metric[metric_name],
        )
        diagnostics.extend(metric_diagnostics)
        if selected is None:
            continue
        staging_facts.append(
            FundedActuarialStagingFact(
                plan_id=plan_id,
                plan_period=plan_period,
                metric_name=metric_name,
                as_reported_value=selected.as_reported_value,
                normalized_value=selected.normalized_value,
                as_reported_unit=selected.as_reported_unit,
                normalized_unit=selected.normalized_unit,
                effective_date=raw.effective_date,
                ingestion_date=raw.ingestion_date,
                source_document_id=raw.source_document_id,
                source_url=raw.source_url,
                extraction_method=selected.extraction_method,
                confidence=selected.confidence,
                parser_version=PARSER_VERSION,
                evidence_refs=(selected.evidence_ref,),
            )
        )

    staging_facts.sort(key=lambda row: row.metric_name)
    diagnostics.sort(key=lambda item: (item.metric_name, item.code, item.message))
    return staging_facts, diagnostics
