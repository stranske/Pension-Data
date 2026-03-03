"""Consultant entities, engagements, and recommendation domain models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

BoardDecisionStatus = Literal["adopted", "partially_adopted", "rejected", "not_disclosed"]
LinkageStatus = Literal["resolved", "ambiguous", "not_disclosed"]


@dataclass(frozen=True, slots=True)
class ConsultantEntity:
    """Canonical consultant entity observed in plan report disclosures."""

    consultant_name: str
    normalized_name: str
    consultant_canonical_id: str
    linkage_status: LinkageStatus
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: Mapping[str, str] = field(compare=False, hash=False)


@dataclass(frozen=True, slots=True)
class PlanConsultantEngagement:
    """Plan-period consultant engagement record."""

    plan_id: str
    plan_period: str
    consultant_name: str
    consultant_canonical_id: str
    linkage_status: LinkageStatus
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
    consultant_canonical_id: str
    linkage_status: LinkageStatus
    topic: str
    recommendation_text: str
    board_decision_status: BoardDecisionStatus
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: Mapping[str, str] = field(compare=False, hash=False)
