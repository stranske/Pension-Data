"""Lifecycle event models for manager/fund investment changes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LifecycleEventType = Literal["entered", "exited", "still_invested"]
LifecycleEventBasis = Literal["explicit_text", "table_presence_change", "inferred"]


@dataclass(frozen=True, slots=True)
class ManagerLifecycleEvent:
    """Manager/fund lifecycle event with basis, confidence, and evidence."""

    plan_id: str
    plan_period: str
    manager_name: str
    fund_name: str | None
    event_type: LifecycleEventType
    basis: LifecycleEventBasis
    confidence: float
    evidence_refs: tuple[str, ...]
    manager_canonical_id: str | None = None
    fund_canonical_id: str | None = None
