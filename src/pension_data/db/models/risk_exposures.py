"""Risk exposure observation models for derivatives and securities lending."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RiskDisclosureType = Literal["derivatives", "securities_lending"]
RiskObservationKind = Literal["policy_limit", "realized_exposure", "collateral_context", "not_disclosed"]


@dataclass(frozen=True, slots=True)
class RiskExposureObservation:
    """Structured risk-disclosure observation with evidence and confidence metadata."""

    plan_id: str
    plan_period: str
    disclosure_type: RiskDisclosureType
    metric_name: str
    observation_kind: RiskObservationKind
    value_usd: float | None
    value_ratio: float | None
    as_reported_text: str
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: dict[str, str]
