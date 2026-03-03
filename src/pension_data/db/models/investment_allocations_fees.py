"""Investment allocation and fee disclosure staging models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FeeDisclosureCompleteness = Literal["complete", "partial", "not_disclosed"]
FeeType = Literal["management_fee", "performance_fee", "other_fee"]
InvestmentWarningCode = Literal[
    "partial_fee_disclosure",
    "ambiguous_manager_name",
    "non_disclosure",
]


@dataclass(frozen=True, slots=True)
class AssetAllocationObservation:
    """Asset allocation row with as-reported and normalized values."""

    plan_id: str
    plan_period: str
    category: str
    as_reported_percent: float | None
    normalized_weight: float | None
    as_reported_amount: float | None
    normalized_amount_usd: float | None
    effective_date: str
    ingestion_date: str
    source_document_id: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ManagerFeeObservation:
    """Manager-level fee row with disclosure completeness semantics."""

    plan_id: str
    plan_period: str
    manager_name: str | None
    fee_type: FeeType
    as_reported_rate_pct: float | None
    normalized_rate: float | None
    as_reported_amount: float | None
    normalized_amount_usd: float | None
    completeness: FeeDisclosureCompleteness
    effective_date: str
    ingestion_date: str
    source_document_id: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class InvestmentExtractionWarning:
    """Investment extraction warning for fee and manager-level completeness issues."""

    code: InvestmentWarningCode
    plan_id: str
    plan_period: str
    manager_name: str | None
    message: str
    evidence_refs: tuple[str, ...]
