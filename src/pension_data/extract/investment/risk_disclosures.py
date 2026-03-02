"""Derivatives and securities-lending risk disclosure extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from pension_data.db.models.risk_exposures import RiskDisclosureType, RiskExposureObservation

ValueUnit = Literal["usd", "thousand_usd", "million_usd", "billion_usd", "ratio"]
SourceKind = Literal["table", "narrative"]
RiskDiagnosticCode = Literal["policy_only", "realized_only", "not_disclosed"]

_UNIT_MULTIPLIER: dict[ValueUnit, float] = {
    "usd": 1.0,
    "thousand_usd": 1_000.0,
    "million_usd": 1_000_000.0,
    "billion_usd": 1_000_000_000.0,
    "ratio": 1.0,
}
_DIAGNOSTIC_MESSAGE: dict[RiskDiagnosticCode, str] = {
    "policy_only": "Policy limit disclosed without realized exposure.",
    "realized_only": "Realized exposure disclosed without policy limit.",
    "not_disclosed": "No structured disclosure for this risk topic.",
}


@dataclass(frozen=True, slots=True)
class DerivativesDisclosureInput:
    """Parsed derivatives disclosure from table or narrative source."""

    usage_type: str
    policy_limit_value: float | None
    realized_exposure_value: float | None
    value_unit: ValueUnit
    as_reported_text: str
    source_kind: SourceKind
    confidence: float
    evidence_refs: tuple[str, ...]
    source_url: str


@dataclass(frozen=True, slots=True)
class SecuritiesLendingDisclosureInput:
    """Parsed securities-lending disclosure from table or narrative source."""

    program_name: str
    policy_limit_value: float | None
    realized_exposure_value: float | None
    collateral_value: float | None
    value_unit: ValueUnit
    as_reported_text: str
    source_kind: SourceKind
    confidence: float
    evidence_refs: tuple[str, ...]
    source_url: str


@dataclass(frozen=True, slots=True)
class RiskExtractionDiagnostic:
    """Diagnostic emitted for policy-only, realized-only, and non-disclosure patterns."""

    code: RiskDiagnosticCode
    disclosure_type: RiskDisclosureType
    metric_name: str
    message: str
    evidence_refs: tuple[str, ...]


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return normalized.strip("_") or "not_specified"


def _dedupe_refs(evidence_refs: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(ref.strip() for ref in evidence_refs if ref.strip()))


def _bounded_confidence(confidence: float) -> float:
    return round(max(0.0, min(1.0, confidence)), 6)


def _source_metadata(
    *, source_url: str, source_kind: SourceKind, unit: ValueUnit
) -> MappingProxyType[str, str]:
    return MappingProxyType({"source_url": source_url, "source_kind": source_kind, "unit": unit})


def _to_usd(value: float | None, *, unit: ValueUnit) -> float | None:
    if value is None:
        return None
    if unit == "ratio":
        return None
    return round(value * _UNIT_MULTIPLIER[unit], 6)


def _to_ratio(value: float | None, *, unit: ValueUnit) -> float | None:
    if value is None:
        return None
    if unit == "ratio":
        return round(value, 6)
    return None


def _make_observation(
    *,
    plan_id: str,
    plan_period: str,
    disclosure_type: RiskDisclosureType,
    metric_name: str,
    observation_kind: Literal["policy_limit", "realized_exposure", "collateral_context"],
    value: float,
    unit: ValueUnit,
    as_reported_text: str,
    confidence: float,
    evidence_refs: tuple[str, ...],
    source_url: str,
    source_kind: SourceKind,
) -> RiskExposureObservation:
    return RiskExposureObservation(
        plan_id=plan_id,
        plan_period=plan_period,
        disclosure_type=disclosure_type,
        metric_name=metric_name,
        observation_kind=observation_kind,
        value_usd=_to_usd(value, unit=unit),
        value_ratio=_to_ratio(value, unit=unit),
        as_reported_text=as_reported_text.strip() or "not_disclosed",
        confidence=_bounded_confidence(confidence),
        evidence_refs=_dedupe_refs(evidence_refs),
        source_metadata=_source_metadata(source_url=source_url, source_kind=source_kind, unit=unit),
    )


def _diagnostic(
    *,
    code: RiskDiagnosticCode,
    disclosure_type: RiskDisclosureType,
    metric_name: str,
    evidence_refs: tuple[str, ...],
) -> RiskExtractionDiagnostic:
    return RiskExtractionDiagnostic(
        code=code,
        disclosure_type=disclosure_type,
        metric_name=metric_name,
        message=_DIAGNOSTIC_MESSAGE[code],
        evidence_refs=_dedupe_refs(evidence_refs),
    )


def extract_risk_exposure_observations(
    *,
    plan_id: str,
    plan_period: str,
    derivatives_disclosures: list[DerivativesDisclosureInput],
    securities_lending_disclosures: list[SecuritiesLendingDisclosureInput],
) -> tuple[list[RiskExposureObservation], list[RiskExtractionDiagnostic]]:
    """Extract normalized risk exposure observations and diagnostics."""
    observations: list[RiskExposureObservation] = []
    diagnostics: list[RiskExtractionDiagnostic] = []

    for disclosure in derivatives_disclosures:
        usage_slug = _slugify(disclosure.usage_type)
        base_metric = f"derivatives:{usage_slug}"
        has_policy = disclosure.policy_limit_value is not None
        has_realized = disclosure.realized_exposure_value is not None

        if has_policy and disclosure.policy_limit_value is not None:
            observations.append(
                _make_observation(
                    plan_id=plan_id,
                    plan_period=plan_period,
                    disclosure_type="derivatives",
                    metric_name=f"{base_metric}:policy_limit",
                    observation_kind="policy_limit",
                    value=disclosure.policy_limit_value,
                    unit=disclosure.value_unit,
                    as_reported_text=disclosure.as_reported_text,
                    confidence=disclosure.confidence,
                    evidence_refs=disclosure.evidence_refs,
                    source_url=disclosure.source_url,
                    source_kind=disclosure.source_kind,
                )
            )
        if has_realized and disclosure.realized_exposure_value is not None:
            observations.append(
                _make_observation(
                    plan_id=plan_id,
                    plan_period=plan_period,
                    disclosure_type="derivatives",
                    metric_name=f"{base_metric}:realized_exposure",
                    observation_kind="realized_exposure",
                    value=disclosure.realized_exposure_value,
                    unit=disclosure.value_unit,
                    as_reported_text=disclosure.as_reported_text,
                    confidence=disclosure.confidence,
                    evidence_refs=disclosure.evidence_refs,
                    source_url=disclosure.source_url,
                    source_kind=disclosure.source_kind,
                )
            )

        if has_policy and not has_realized:
            diagnostics.append(
                _diagnostic(
                    code="policy_only",
                    disclosure_type="derivatives",
                    metric_name=base_metric,
                    evidence_refs=disclosure.evidence_refs,
                )
            )
        if has_realized and not has_policy:
            diagnostics.append(
                _diagnostic(
                    code="realized_only",
                    disclosure_type="derivatives",
                    metric_name=base_metric,
                    evidence_refs=disclosure.evidence_refs,
                )
            )
        if not has_policy and not has_realized:
            diagnostics.append(
                _diagnostic(
                    code="not_disclosed",
                    disclosure_type="derivatives",
                    metric_name=base_metric,
                    evidence_refs=disclosure.evidence_refs,
                )
            )

    for lending_disclosure in securities_lending_disclosures:
        program_slug = _slugify(lending_disclosure.program_name)
        base_metric = f"securities_lending:{program_slug}"
        has_policy = lending_disclosure.policy_limit_value is not None
        has_realized = lending_disclosure.realized_exposure_value is not None
        has_collateral = lending_disclosure.collateral_value is not None

        if has_policy and lending_disclosure.policy_limit_value is not None:
            observations.append(
                _make_observation(
                    plan_id=plan_id,
                    plan_period=plan_period,
                    disclosure_type="securities_lending",
                    metric_name=f"{base_metric}:policy_limit",
                    observation_kind="policy_limit",
                    value=lending_disclosure.policy_limit_value,
                    unit=lending_disclosure.value_unit,
                    as_reported_text=lending_disclosure.as_reported_text,
                    confidence=lending_disclosure.confidence,
                    evidence_refs=lending_disclosure.evidence_refs,
                    source_url=lending_disclosure.source_url,
                    source_kind=lending_disclosure.source_kind,
                )
            )
        if has_realized and lending_disclosure.realized_exposure_value is not None:
            observations.append(
                _make_observation(
                    plan_id=plan_id,
                    plan_period=plan_period,
                    disclosure_type="securities_lending",
                    metric_name=f"{base_metric}:realized_exposure",
                    observation_kind="realized_exposure",
                    value=lending_disclosure.realized_exposure_value,
                    unit=lending_disclosure.value_unit,
                    as_reported_text=lending_disclosure.as_reported_text,
                    confidence=lending_disclosure.confidence,
                    evidence_refs=lending_disclosure.evidence_refs,
                    source_url=lending_disclosure.source_url,
                    source_kind=lending_disclosure.source_kind,
                )
            )
        if has_collateral and lending_disclosure.collateral_value is not None:
            observations.append(
                _make_observation(
                    plan_id=plan_id,
                    plan_period=plan_period,
                    disclosure_type="securities_lending",
                    metric_name=f"{base_metric}:collateral",
                    observation_kind="collateral_context",
                    value=lending_disclosure.collateral_value,
                    unit=lending_disclosure.value_unit,
                    as_reported_text=lending_disclosure.as_reported_text,
                    confidence=lending_disclosure.confidence,
                    evidence_refs=lending_disclosure.evidence_refs,
                    source_url=lending_disclosure.source_url,
                    source_kind=lending_disclosure.source_kind,
                )
            )

        if has_policy and not has_realized:
            diagnostics.append(
                _diagnostic(
                    code="policy_only",
                    disclosure_type="securities_lending",
                    metric_name=base_metric,
                    evidence_refs=lending_disclosure.evidence_refs,
                )
            )
        if has_realized and not has_policy:
            diagnostics.append(
                _diagnostic(
                    code="realized_only",
                    disclosure_type="securities_lending",
                    metric_name=base_metric,
                    evidence_refs=lending_disclosure.evidence_refs,
                )
            )
        if not has_policy and not has_realized and not has_collateral:
            diagnostics.append(
                _diagnostic(
                    code="not_disclosed",
                    disclosure_type="securities_lending",
                    metric_name=base_metric,
                    evidence_refs=lending_disclosure.evidence_refs,
                )
            )

    if not derivatives_disclosures:
        observations.append(
            RiskExposureObservation(
                plan_id=plan_id,
                plan_period=plan_period,
                disclosure_type="derivatives",
                metric_name="derivatives:not_disclosed",
                observation_kind="not_disclosed",
                value_usd=None,
                value_ratio=None,
                as_reported_text="not_disclosed",
                confidence=0.0,
                evidence_refs=(),
                source_metadata=_source_metadata(
                    source_url="not_disclosed",
                    source_kind="narrative",
                    unit="usd",
                ),
            )
        )
        diagnostics.append(
            _diagnostic(
                code="not_disclosed",
                disclosure_type="derivatives",
                metric_name="derivatives:not_disclosed",
                evidence_refs=(),
            )
        )

    if not securities_lending_disclosures:
        observations.append(
            RiskExposureObservation(
                plan_id=plan_id,
                plan_period=plan_period,
                disclosure_type="securities_lending",
                metric_name="securities_lending:not_disclosed",
                observation_kind="not_disclosed",
                value_usd=None,
                value_ratio=None,
                as_reported_text="not_disclosed",
                confidence=0.0,
                evidence_refs=(),
                source_metadata=_source_metadata(
                    source_url="not_disclosed",
                    source_kind="narrative",
                    unit="usd",
                ),
            )
        )
        diagnostics.append(
            _diagnostic(
                code="not_disclosed",
                disclosure_type="securities_lending",
                metric_name="securities_lending:not_disclosed",
                evidence_refs=(),
            )
        )

    observations = sorted(
        observations,
        key=lambda row: (
            row.disclosure_type,
            row.metric_name,
            row.observation_kind,
            row.as_reported_text,
            row.evidence_refs,
        ),
    )
    diagnostics = sorted(
        diagnostics,
        key=lambda row: (row.disclosure_type, row.metric_name, row.code, row.evidence_refs),
    )
    return observations, diagnostics
