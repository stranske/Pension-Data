"""Investment position staging models for manager/fund disclosures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

PositionCompleteness = Literal["complete", "partial", "not_disclosed"]
PositionWarningCode = Literal["non_disclosure", "partial_disclosure", "ambiguous_naming"]
LinkageStatus = Literal["resolved", "ambiguous", "not_disclosed"]


@dataclass(frozen=True, slots=True)
class PlanManagerFundPosition:
    """Normalized manager/fund exposure row for a single plan-period."""

    plan_id: str
    plan_period: str
    manager_name: str | None
    fund_name: str | None
    commitment: float | None
    unfunded: float | None
    market_value: float | None
    completeness: PositionCompleteness
    manager_canonical_id: str | None = None
    fund_canonical_id: str | None = None
    linkage_status: LinkageStatus = "resolved"
    known_not_invested: bool = False
    confidence: float = 1.0
    evidence_refs: tuple[str, ...] = ()
    warnings: tuple[PositionWarningCode, ...] = ()

    @property
    def is_disclosed(self) -> bool:
        """Whether this position row contains a disclosed investment."""
        return self.completeness != "not_disclosed"
