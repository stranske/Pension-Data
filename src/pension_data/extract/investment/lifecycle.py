"""Manager/fund lifecycle inference with confidence and event-basis metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass

from pension_data.db.models.investment_positions import PlanManagerFundPosition
from pension_data.db.models.manager_lifecycle import (
    LifecycleEventBasis,
    LifecycleEventType,
    ManagerLifecycleEvent,
)
from pension_data.extract.investment.manager_positions import ExtractionWarning


@dataclass(frozen=True, slots=True)
class ExplicitLifecycleSignal:
    """Explicit text-derived lifecycle signal for a manager/fund pair."""

    plan_id: str
    plan_period: str
    manager_name: str
    fund_name: str | None
    event_type: LifecycleEventType
    confidence: float
    evidence_refs: tuple[str, ...]
    basis: LifecycleEventBasis = "explicit_text"


def _normalize_token(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.strip().lower().split())


def _plan_period_sort_key(plan_period: str) -> tuple[int, str]:
    normalized = plan_period.strip().upper()
    year_match = re.search(r"(19|20)\d{2}", normalized)
    parsed_year = int(year_match.group(0)) if year_match else -1
    return parsed_year, normalized


def _merge_evidence_refs(*refs: tuple[str, ...]) -> tuple[str, ...]:
    merged: list[str] = []
    for values in refs:
        for value in values:
            normalized = value.strip()
            if normalized and normalized not in merged:
                merged.append(normalized)
    return tuple(merged)


def _position_key(position: PlanManagerFundPosition) -> tuple[str, str, str]:
    return (
        position.plan_id,
        _normalize_token(position.manager_name),
        _normalize_token(position.fund_name),
    )


def _signal_key(signal: ExplicitLifecycleSignal) -> tuple[str, str, str, str]:
    return (
        signal.plan_id,
        signal.plan_period,
        _normalize_token(signal.manager_name),
        _normalize_token(signal.fund_name),
    )


def _event_key(event: ManagerLifecycleEvent) -> tuple[str, str, str, str]:
    return (
        event.plan_id,
        event.plan_period,
        _normalize_token(event.manager_name),
        _normalize_token(event.fund_name),
    )


def _should_include_for_inference(position: PlanManagerFundPosition) -> bool:
    if position.known_not_invested:
        return False
    if not position.is_disclosed:
        return False

    manager_token = _normalize_token(position.manager_name)
    fund_token = _normalize_token(position.fund_name)
    return bool(manager_token or fund_token)


def _warning_for_non_disclosure(position: PlanManagerFundPosition) -> ExtractionWarning:
    return ExtractionWarning(
        code="non_disclosure",
        plan_id=position.plan_id,
        plan_period=position.plan_period,
        manager_name=position.manager_name,
        fund_name=position.fund_name,
        message="Lifecycle inference is limited because investment disclosure is missing.",
        evidence_refs=position.evidence_refs,
    )


def infer_lifecycle_events(
    previous_positions: list[PlanManagerFundPosition],
    current_positions: list[PlanManagerFundPosition],
    *,
    explicit_signals: tuple[ExplicitLifecycleSignal, ...] = (),
) -> tuple[list[ManagerLifecycleEvent], list[ExtractionWarning]]:
    """Infer lifecycle events from position presence changes and explicit text signals."""
    previous_ordered = sorted(
        previous_positions,
        key=lambda row: (
            row.plan_id,
            _plan_period_sort_key(row.plan_period),
            _normalize_token(row.manager_name),
            _normalize_token(row.fund_name),
        ),
    )
    current_ordered = sorted(
        current_positions,
        key=lambda row: (
            row.plan_id,
            _plan_period_sort_key(row.plan_period),
            _normalize_token(row.manager_name),
            _normalize_token(row.fund_name),
        ),
    )

    warnings: list[ExtractionWarning] = []
    for position in (*previous_ordered, *current_ordered):
        if position.completeness == "not_disclosed" and not position.known_not_invested:
            warnings.append(_warning_for_non_disclosure(position))

    previous_disclosed = {
        _position_key(position): position
        for position in previous_ordered
        if _should_include_for_inference(position)
    }
    current_disclosed = {
        _position_key(position): position
        for position in current_ordered
        if _should_include_for_inference(position)
    }
    current_period_by_plan: dict[str, str] = {}
    for position in current_ordered:
        prior_period = current_period_by_plan.get(position.plan_id)
        if prior_period is None or _plan_period_sort_key(
            position.plan_period
        ) > _plan_period_sort_key(prior_period):
            current_period_by_plan[position.plan_id] = position.plan_period

    events_by_key: dict[tuple[str, str, str, str], ManagerLifecycleEvent] = {}
    for key, current in current_disclosed.items():
        previous = previous_disclosed.get(key)
        if previous is None:
            event = ManagerLifecycleEvent(
                plan_id=current.plan_id,
                plan_period=current.plan_period,
                manager_name=current.manager_name or "unknown-manager",
                fund_name=current.fund_name,
                event_type="entered",
                basis="table_presence_change",
                confidence=round(max(0.0, min(1.0, current.confidence)), 6),
                evidence_refs=_merge_evidence_refs(current.evidence_refs),
            )
        else:
            event = ManagerLifecycleEvent(
                plan_id=current.plan_id,
                plan_period=current.plan_period,
                manager_name=current.manager_name or previous.manager_name or "unknown-manager",
                fund_name=current.fund_name or previous.fund_name,
                event_type="still_invested",
                basis="inferred",
                confidence=round(
                    max(0.0, min(1.0, min(previous.confidence, current.confidence))),
                    6,
                ),
                evidence_refs=_merge_evidence_refs(previous.evidence_refs, current.evidence_refs),
            )
        events_by_key[_event_key(event)] = event

    for key, previous in previous_disclosed.items():
        if key in current_disclosed:
            continue
        inferred_period = current_period_by_plan.get(previous.plan_id, previous.plan_period)
        event = ManagerLifecycleEvent(
            plan_id=previous.plan_id,
            plan_period=inferred_period,
            manager_name=previous.manager_name or "unknown-manager",
            fund_name=previous.fund_name,
            event_type="exited",
            basis="table_presence_change",
            confidence=round(max(0.0, min(1.0, previous.confidence)), 6),
            evidence_refs=_merge_evidence_refs(previous.evidence_refs),
        )
        events_by_key[_event_key(event)] = event

    for signal in sorted(
        explicit_signals,
        key=lambda row: (
            row.plan_id,
            _plan_period_sort_key(row.plan_period),
            _normalize_token(row.manager_name),
            _normalize_token(row.fund_name),
            row.event_type,
        ),
    ):
        event = ManagerLifecycleEvent(
            plan_id=signal.plan_id,
            plan_period=signal.plan_period,
            manager_name=signal.manager_name.strip(),
            fund_name=signal.fund_name.strip() if signal.fund_name else None,
            event_type=signal.event_type,
            basis=signal.basis,
            confidence=round(max(0.0, min(1.0, signal.confidence)), 6),
            evidence_refs=_merge_evidence_refs(signal.evidence_refs),
        )
        events_by_key[_signal_key(signal)] = event

    events = sorted(
        events_by_key.values(),
        key=lambda row: (
            row.plan_id,
            _plan_period_sort_key(row.plan_period),
            _normalize_token(row.manager_name),
            _normalize_token(row.fund_name),
            row.event_type,
        ),
    )
    warnings = sorted(
        warnings,
        key=lambda row: (
            row.plan_id,
            _plan_period_sort_key(row.plan_period),
            _normalize_token(row.manager_name),
            _normalize_token(row.fund_name),
            row.code,
        ),
    )
    return events, warnings
