"""Investment position staging models for manager/fund disclosures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

PositionCompleteness = Literal["complete", "partial", "not_disclosed"]
SecurityDisclosureState = Literal["disclosed", "not_disclosed", "known_not_invested"]
SecurityPositionSource = Literal["13f", "own_holdings_file", "acfr", "ab2833", "manual"]
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


@dataclass(frozen=True, slots=True)
class PlanSecurityPosition:
    """Security-level holding row for a single plan-period and source."""

    plan_id: str
    plan_period: str
    security_id: str
    security_name: str | None
    cusip: str | None
    ticker: str | None
    shares: float | None
    market_value_usd: float | None
    asset_class: str
    source: SecurityPositionSource
    as_of: str
    disclosure_state: SecurityDisclosureState
    provenance_ref: str
    manager_name: str | None = None
    fund_name: str | None = None
    confidence: float = 1.0

    @property
    def is_disclosed(self) -> bool:
        """Whether this security row represents a disclosed holding."""
        return self.disclosure_state == "disclosed"


@dataclass(frozen=True, slots=True)
class HoldingsCoverageReport:
    """Coverage of collected security holdings against ACFR total-plan assets."""

    plan_id: str
    plan_period: str
    total_plan_assets_usd: float
    collected_market_value_usd: float
    coverage_ratio: float
    scope_label: str
    by_asset_class: dict[str, float]
    provenance_refs: tuple[str, ...]
