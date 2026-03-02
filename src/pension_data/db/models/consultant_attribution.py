"""Consultant recommendation attribution domain models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

AttributionStrength = Literal["explicit", "implied", "speculative"]


@dataclass(frozen=True, slots=True)
class ConsultantAttributionObservation:
    """Observed attribution signal linking recommendation to documented outcome."""

    plan_id: str
    plan_period: str
    consultant_name: str
    recommendation_topic: str
    observed_outcome: str
    strength: AttributionStrength
    confidence: float
    evidence_refs: tuple[str, ...]
    source_metadata: dict[str, str]
