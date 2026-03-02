"""Consultant entities, engagements, and recommendation domain models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

BoardDecisionStatus = Literal["adopted", "partially_adopted", "rejected", "not_disclosed"]


@dataclass(frozen=True, slots=True)
class ConsultantEntity:
    """Canonical consultant entity observed in plan report disclosures."""

    consultant_name: str
    normalized_name: str
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: Mapping[str, str] = field(compare=False, hash=False)


@dataclass(frozen=True, slots=True)
class PlanConsultantEngagement:
    """Plan-period consultant engagement record."""

    plan_id: str
    plan_period: str
    consultant_name: str
    role_description: str
    is_disclosed: bool
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: Mapping[str, str] = field(compare=False, hash=False)


@dataclass(frozen=True, slots=True)
class ConsultantRecommendation:
    """Recommendation observation linked to consultant engagement context."""

    plan_id: str
    plan_period: str
    consultant_name: str
    topic: str
    recommendation_text: str
    board_decision_status: BoardDecisionStatus
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: Mapping[str, str] = field(compare=False, hash=False)
