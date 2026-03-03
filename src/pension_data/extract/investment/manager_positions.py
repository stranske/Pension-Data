"""Manager/fund position staging with disclosure completeness semantics."""

from __future__ import annotations

from dataclasses import dataclass, replace

from pension_data.db.models.investment_positions import (
    LinkageStatus,
    PlanManagerFundPosition,
    PositionCompleteness,
    PositionWarningCode,
)
from pension_data.extract.common.entity_ids import canonical_fund_id, canonical_manager_id

_WARNING_MESSAGES: dict[PositionWarningCode, str] = {
    "non_disclosure": "Investment exposure is not disclosed for this plan-period.",
    "partial_disclosure": "Investment exposure is partially disclosed.",
    "ambiguous_naming": "Manager/fund naming is ambiguous across disclosure rows.",
}


@dataclass(frozen=True, slots=True)
class ManagerFundDisclosureInput:
    """Raw extracted disclosure row for a manager/fund plan-period position."""

    plan_id: str
    plan_period: str
    manager_name: str | None
    fund_name: str | None
    commitment: float | None
    unfunded: float | None
    market_value: float | None
    explicit_not_disclosed: bool = False
    known_not_invested: bool = False
    confidence: float = 1.0
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExtractionWarning:
    """Extraction warning surfaced during staging and lifecycle inference."""

    code: PositionWarningCode
    plan_id: str
    plan_period: str
    manager_name: str | None
    fund_name: str | None
    message: str
    evidence_refs: tuple[str, ...]


def _normalize_token(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.strip().lower().split())


def _dedupe_refs(evidence_refs: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(ref.strip() for ref in evidence_refs if ref.strip()))


def _linkage_status(
    *, completeness: PositionCompleteness, known_not_invested: bool
) -> LinkageStatus:
    if completeness == "not_disclosed" and not known_not_invested:
        return "not_disclosed"
    return "resolved"


def _infer_completeness(row: ManagerFundDisclosureInput) -> PositionCompleteness:
    if row.explicit_not_disclosed:
        return "not_disclosed"
    has_manager = bool(row.manager_name and row.manager_name.strip())
    has_fund = bool(row.fund_name and row.fund_name.strip())
    numeric_values = (row.commitment, row.unfunded, row.market_value)
    has_all_numeric = all(value is not None for value in numeric_values)
    has_any_numeric = any(value is not None for value in numeric_values)

    if has_manager and has_fund and has_all_numeric:
        return "complete"
    if has_manager or has_fund or has_any_numeric:
        return "partial"
    return "not_disclosed"


def _base_warning_codes(
    *,
    completeness: PositionCompleteness,
    known_not_invested: bool,
) -> tuple[PositionWarningCode, ...]:
    warning_codes: list[PositionWarningCode] = []
    if completeness == "partial":
        warning_codes.append("partial_disclosure")
    if completeness == "not_disclosed" and not known_not_invested:
        warning_codes.append("non_disclosure")
    return tuple(warning_codes)


def _as_warning(position: PlanManagerFundPosition, code: PositionWarningCode) -> ExtractionWarning:
    return ExtractionWarning(
        code=code,
        plan_id=position.plan_id,
        plan_period=position.plan_period,
        manager_name=position.manager_name,
        fund_name=position.fund_name,
        message=_WARNING_MESSAGES[code],
        evidence_refs=position.evidence_refs,
    )


def build_manager_fund_positions(
    rows: list[ManagerFundDisclosureInput],
) -> tuple[list[PlanManagerFundPosition], list[ExtractionWarning]]:
    """Stage manager/fund positions and emit disclosure-quality warnings."""
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            (row.manager_name or "").strip().lower(),
            (row.fund_name or "").strip().lower(),
        ),
    )

    positions: list[PlanManagerFundPosition] = []
    warnings: list[ExtractionWarning] = []
    for row in ordered_rows:
        completeness = _infer_completeness(row)
        warning_codes = _base_warning_codes(
            completeness=completeness,
            known_not_invested=row.known_not_invested,
        )
        position = PlanManagerFundPosition(
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            manager_name=(row.manager_name.strip() if row.manager_name else None),
            fund_name=(row.fund_name.strip() if row.fund_name else None),
            commitment=row.commitment,
            unfunded=row.unfunded,
            market_value=row.market_value,
            completeness=completeness,
            manager_canonical_id=canonical_manager_id(row.manager_name),
            fund_canonical_id=canonical_fund_id(
                manager_name=row.manager_name,
                fund_name=row.fund_name,
            ),
            linkage_status=_linkage_status(
                completeness=completeness,
                known_not_invested=row.known_not_invested,
            ),
            known_not_invested=row.known_not_invested,
            confidence=max(0.0, min(1.0, row.confidence)),
            evidence_refs=_dedupe_refs(row.evidence_refs),
            warnings=warning_codes,
        )
        positions.append(position)
        warnings.extend(_as_warning(position, code) for code in warning_codes)

    naming_key_to_indices: dict[tuple[str, str, str, str], list[int]] = {}
    naming_key_to_raw_pairs: dict[tuple[str, str, str, str], set[tuple[str | None, str | None]]] = (
        {}
    )

    for index, position in enumerate(positions):
        manager_key = _normalize_token(position.manager_name)
        fund_key = _normalize_token(position.fund_name)
        if not manager_key and not fund_key:
            continue
        naming_key = (position.plan_id, position.plan_period, manager_key, fund_key)
        naming_key_to_indices.setdefault(naming_key, []).append(index)
        naming_key_to_raw_pairs.setdefault(naming_key, set()).add(
            (position.manager_name, position.fund_name)
        )

    for naming_key, raw_pairs in naming_key_to_raw_pairs.items():
        if len(raw_pairs) < 2:
            continue
        for index in naming_key_to_indices[naming_key]:
            position = positions[index]
            if "ambiguous_naming" in position.warnings:
                continue
            updated_warnings = (*position.warnings, "ambiguous_naming")
            updated = replace(position, warnings=updated_warnings, linkage_status="ambiguous")
            positions[index] = updated
            warnings.append(_as_warning(updated, "ambiguous_naming"))

    return positions, warnings
