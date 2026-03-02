"""Allocation and fee extraction with dual-value persistence and warnings."""

from __future__ import annotations

from dataclasses import dataclass

from pension_data.db.models.investment_allocations_fees import (
    AssetAllocationObservation,
    FeeDisclosureCompleteness,
    FeeType,
    InvestmentExtractionWarning,
    InvestmentWarningCode,
    ManagerFeeObservation,
)
from pension_data.normalize.financial_units import UnitScale
from pension_data.normalize.investment_normalization import (
    normalize_allocation_category,
    normalize_amount_to_usd,
    normalize_rate_to_ratio,
)

_WARNING_MESSAGES: dict[str, str] = {
    "partial_fee_disclosure": "Fee disclosure is partial for this manager/plan-period.",
    "ambiguous_manager_name": "Manager naming is ambiguous across fee disclosures.",
    "non_disclosure": "Fee disclosure is not available for this manager/plan-period.",
}


@dataclass(frozen=True, slots=True)
class AllocationDisclosureInput:
    """Raw allocation disclosure row."""

    category_label: str
    percent_value: float | None
    amount_value: float | None
    amount_unit: UnitScale = "usd"
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FeeDisclosureInput:
    """Raw fee disclosure row."""

    manager_name: str | None
    fee_type: FeeType
    rate_value: float | None
    amount_value: float | None
    amount_unit: UnitScale = "usd"
    explicit_not_disclosed: bool = False
    evidence_refs: tuple[str, ...] = ()


def _normalize_token(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.strip().lower().split())


def _dedupe_refs(evidence_refs: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(ref.strip() for ref in evidence_refs if ref.strip()))


def _fee_completeness(row: FeeDisclosureInput) -> FeeDisclosureCompleteness:
    if row.explicit_not_disclosed:
        return "not_disclosed"
    has_rate = row.rate_value is not None
    has_amount = row.amount_value is not None
    if has_rate and has_amount:
        return "complete"
    if has_rate or has_amount:
        return "partial"
    return "not_disclosed"


def _warning_for_fee_row(
    *,
    code: InvestmentWarningCode,
    plan_id: str,
    plan_period: str,
    manager_name: str | None,
    evidence_refs: tuple[str, ...],
) -> InvestmentExtractionWarning:
    return InvestmentExtractionWarning(
        code=code,
        plan_id=plan_id,
        plan_period=plan_period,
        manager_name=manager_name,
        message=_WARNING_MESSAGES[code],
        evidence_refs=evidence_refs,
    )


def extract_asset_allocations(
    *,
    plan_id: str,
    plan_period: str,
    effective_date: str,
    ingestion_date: str,
    source_document_id: str,
    rows: list[AllocationDisclosureInput],
) -> list[AssetAllocationObservation]:
    """Extract allocation observations with percent + nominal normalization."""
    ordered_rows = sorted(rows, key=lambda row: normalize_allocation_category(row.category_label))

    total_amount_usd = sum(
        amount
        for amount in (
            normalize_amount_to_usd(row.amount_value, unit_scale=row.amount_unit)
            for row in ordered_rows
        )
        if amount is not None
    )
    if total_amount_usd <= 0:
        total_amount_usd = 0.0

    observations: list[AssetAllocationObservation] = []
    for row in ordered_rows:
        normalized_amount = normalize_amount_to_usd(row.amount_value, unit_scale=row.amount_unit)
        normalized_weight = (
            normalize_rate_to_ratio(row.percent_value)
            if row.percent_value is not None
            else (
                round(normalized_amount / total_amount_usd, 9)
                if normalized_amount is not None and total_amount_usd > 0
                else None
            )
        )
        observations.append(
            AssetAllocationObservation(
                plan_id=plan_id,
                plan_period=plan_period,
                category=normalize_allocation_category(row.category_label),
                as_reported_percent=row.percent_value,
                normalized_weight=normalized_weight,
                as_reported_amount=row.amount_value,
                normalized_amount_usd=normalized_amount,
                effective_date=effective_date,
                ingestion_date=ingestion_date,
                source_document_id=source_document_id,
                evidence_refs=_dedupe_refs(row.evidence_refs),
            )
        )
    return observations


def extract_fee_observations(
    *,
    plan_id: str,
    plan_period: str,
    effective_date: str,
    ingestion_date: str,
    source_document_id: str,
    rows: list[FeeDisclosureInput],
) -> tuple[list[ManagerFeeObservation], list[InvestmentExtractionWarning]]:
    """Extract manager-level fee rows and disclosure/ambiguity warnings."""

    ordered_rows = sorted(
        rows,
        key=lambda row: (
            _normalize_token(row.manager_name),
            row.fee_type,
            row.amount_value if row.amount_value is not None else -1.0,
        ),
    )

    fee_rows: list[ManagerFeeObservation] = []
    warnings: list[InvestmentExtractionWarning] = []

    for row in ordered_rows:
        completeness = _fee_completeness(row)
        normalized_amount = normalize_amount_to_usd(row.amount_value, unit_scale=row.amount_unit)
        normalized_rate = normalize_rate_to_ratio(row.rate_value)
        manager_name = row.manager_name.strip() if row.manager_name else None
        evidence_refs = _dedupe_refs(row.evidence_refs)

        fee_row = ManagerFeeObservation(
            plan_id=plan_id,
            plan_period=plan_period,
            manager_name=manager_name,
            fee_type=row.fee_type,
            as_reported_rate_pct=row.rate_value,
            normalized_rate=normalized_rate,
            as_reported_amount=row.amount_value,
            normalized_amount_usd=normalized_amount,
            completeness=completeness,
            effective_date=effective_date,
            ingestion_date=ingestion_date,
            source_document_id=source_document_id,
            evidence_refs=evidence_refs,
        )
        fee_rows.append(fee_row)

        if completeness == "partial":
            warnings.append(
                _warning_for_fee_row(
                    code="partial_fee_disclosure",
                    plan_id=plan_id,
                    plan_period=plan_period,
                    manager_name=manager_name,
                    evidence_refs=evidence_refs,
                )
            )
        if completeness == "not_disclosed":
            warnings.append(
                _warning_for_fee_row(
                    code="non_disclosure",
                    plan_id=plan_id,
                    plan_period=plan_period,
                    manager_name=manager_name,
                    evidence_refs=evidence_refs,
                )
            )

    grouped_name_indices: dict[str, list[int]] = {}
    grouped_name_raw: dict[str, set[str | None]] = {}
    for index, fee_observation in enumerate(fee_rows):
        key = _normalize_token(fee_observation.manager_name)
        if not key:
            continue
        grouped_name_indices.setdefault(key, []).append(index)
        grouped_name_raw.setdefault(key, set()).add(fee_observation.manager_name)

    for key, raw_names in grouped_name_raw.items():
        if len(raw_names) < 2:
            continue
        indices = grouped_name_indices[key]
        for index in indices:
            fee_row = fee_rows[index]
            warnings.append(
                _warning_for_fee_row(
                    code="ambiguous_manager_name",
                    plan_id=plan_id,
                    plan_period=plan_period,
                    manager_name=fee_row.manager_name,
                    evidence_refs=fee_row.evidence_refs,
                )
            )

    warnings.sort(
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            _normalize_token(row.manager_name),
            row.code,
            row.message,
        )
    )
    return fee_rows, warnings
