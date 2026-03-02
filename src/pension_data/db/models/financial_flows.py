"""Domain model for plan-level AUM and external cash-flow disclosures."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

FinancialDisclosureLevel = Literal["complete", "partial", "not_disclosed"]


@dataclass(frozen=True, slots=True)
class PlanFinancialFlow:
    """Source-linked financial flow row with derived net external cash-flow metrics."""

    plan_id: str
    plan_period: str
    effective_period: str
    reported_at: str
    source_document_id: str
    beginning_aum_usd: float | None
    ending_aum_usd: float | None
    employer_contributions_usd: float | None
    employee_contributions_usd: float | None
    benefit_payments_usd: float | None
    refunds_usd: float | None
    net_external_cash_flow_usd: float | None
    net_external_cash_flow_rate_pct: float | None
    consistency_gap_usd: float | None
    disclosure_level: FinancialDisclosureLevel
    evidence_refs: tuple[str, ...]
    source_metadata: Mapping[str, str] = field(compare=False, hash=False)
