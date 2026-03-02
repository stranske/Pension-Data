"""Rule-based anomaly detection for funded status and allocation shifts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

Severity = Literal["warning", "critical"]
Priority = Literal["low", "medium", "high"]


@dataclass(frozen=True, slots=True)
class AnomalyThresholds:
    """Configurable anomaly thresholds for high-impact metric shifts."""

    funded_shift_warning: float = 0.05
    funded_shift_critical: float = 0.10
    allocation_shift_warning: float = 0.07
    allocation_shift_critical: float = 0.12
    min_confidence_for_medium_priority: float = 0.40


DEFAULT_THRESHOLDS = AnomalyThresholds()


@dataclass(frozen=True, slots=True)
class TimeSeriesPoint:
    """Single plan-period quality point with provenance context."""

    plan_id: str
    period: str
    observed_at: datetime
    funded_ratio: float | None
    allocations: dict[str, float]
    confidence: float
    evidence_refs: tuple[str, ...]
    provenance: dict[str, str]


@dataclass(frozen=True, slots=True)
class AnomalyRecord:
    """Detected anomaly with confidence-aware annotation and evidence context."""

    anomaly_id: str
    plan_id: str
    period: str
    metric: str
    shift: float
    severity: Severity
    confidence: float
    priority: Priority
    requires_review: bool
    evidence_context: dict[str, object]


def _severity_for_shift(*, shift: float, warning: float, critical: float) -> Severity | None:
    if shift >= critical:
        return "critical"
    if shift >= warning:
        return "warning"
    return None


def _priority_for_anomaly(
    *,
    severity: Severity,
    confidence: float,
    thresholds: AnomalyThresholds,
) -> Priority:
    if severity == "critical":
        return "high" if confidence >= thresholds.min_confidence_for_medium_priority else "medium"
    return "medium" if confidence >= thresholds.min_confidence_for_medium_priority else "low"


def _build_evidence_context(*, previous: TimeSeriesPoint, current: TimeSeriesPoint) -> dict[str, object]:
    return {
        "previous_period": previous.period,
        "current_period": current.period,
        "previous_observed_at": previous.observed_at.astimezone(UTC).isoformat(),
        "current_observed_at": current.observed_at.astimezone(UTC).isoformat(),
        "evidence_refs": list(dict.fromkeys(previous.evidence_refs + current.evidence_refs)),
        "previous_provenance": dict(sorted(previous.provenance.items())),
        "current_provenance": dict(sorted(current.provenance.items())),
    }


def _detect_funded_shift(
    *,
    previous: TimeSeriesPoint,
    current: TimeSeriesPoint,
    thresholds: AnomalyThresholds,
) -> list[AnomalyRecord]:
    if previous.funded_ratio is None or current.funded_ratio is None:
        return []

    shift = abs(current.funded_ratio - previous.funded_ratio)
    severity = _severity_for_shift(
        shift=shift,
        warning=thresholds.funded_shift_warning,
        critical=thresholds.funded_shift_critical,
    )
    if severity is None:
        return []

    confidence = min(previous.confidence, current.confidence)
    priority = _priority_for_anomaly(
        severity=severity,
        confidence=confidence,
        thresholds=thresholds,
    )
    evidence_context = _build_evidence_context(previous=previous, current=current)
    return [
        AnomalyRecord(
            anomaly_id=f"{current.plan_id}:{current.period}:funded_ratio_shift",
            plan_id=current.plan_id,
            period=current.period,
            metric="funded_ratio",
            shift=shift,
            severity=severity,
            confidence=confidence,
            priority=priority,
            requires_review=True,
            evidence_context=evidence_context,
        )
    ]


def _detect_allocation_shifts(
    *,
    previous: TimeSeriesPoint,
    current: TimeSeriesPoint,
    thresholds: AnomalyThresholds,
) -> list[AnomalyRecord]:
    anomalies: list[AnomalyRecord] = []
    all_asset_classes = sorted(set(previous.allocations) | set(current.allocations))
    confidence = min(previous.confidence, current.confidence)

    for asset_class in all_asset_classes:
        previous_value = previous.allocations.get(asset_class, 0.0)
        current_value = current.allocations.get(asset_class, 0.0)
        shift = abs(current_value - previous_value)
        severity = _severity_for_shift(
            shift=shift,
            warning=thresholds.allocation_shift_warning,
            critical=thresholds.allocation_shift_critical,
        )
        if severity is None:
            continue

        priority = _priority_for_anomaly(
            severity=severity,
            confidence=confidence,
            thresholds=thresholds,
        )
        evidence_context = _build_evidence_context(previous=previous, current=current)
        anomalies.append(
            AnomalyRecord(
                anomaly_id=(
                    f"{current.plan_id}:{current.period}:allocation_shift:{asset_class.lower()}"
                ),
                plan_id=current.plan_id,
                period=current.period,
                metric=f"allocation:{asset_class}",
                shift=shift,
                severity=severity,
                confidence=confidence,
                priority=priority,
                requires_review=True,
                evidence_context=evidence_context,
            )
        )

    return anomalies


def detect_anomalies(
    points: list[TimeSeriesPoint],
    *,
    thresholds: AnomalyThresholds = DEFAULT_THRESHOLDS,
) -> list[AnomalyRecord]:
    """Detect funded and allocation shift anomalies in time-series points."""
    ordered = sorted(points, key=lambda row: (row.plan_id, row.observed_at, row.period))
    anomalies: list[AnomalyRecord] = []

    for index in range(1, len(ordered)):
        previous = ordered[index - 1]
        current = ordered[index]
        if previous.plan_id != current.plan_id:
            continue

        anomalies.extend(
            _detect_funded_shift(
                previous=previous,
                current=current,
                thresholds=thresholds,
            )
        )
        anomalies.extend(
            _detect_allocation_shifts(
                previous=previous,
                current=current,
                thresholds=thresholds,
            )
        )

    return sorted(
        anomalies,
        key=lambda row: (row.plan_id, row.period, row.metric, row.severity, row.anomaly_id),
    )
