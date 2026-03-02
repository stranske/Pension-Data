"""Rule-based anomaly detection for funded status and allocation shifts."""

from __future__ import annotations

import re
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

    def __post_init__(self) -> None:
        _validate_threshold_range(
            "funded_shift_warning",
            self.funded_shift_warning,
        )
        _validate_threshold_range(
            "funded_shift_critical",
            self.funded_shift_critical,
        )
        _validate_threshold_range(
            "allocation_shift_warning",
            self.allocation_shift_warning,
        )
        _validate_threshold_range(
            "allocation_shift_critical",
            self.allocation_shift_critical,
        )
        _validate_threshold_range(
            "min_confidence_for_medium_priority",
            self.min_confidence_for_medium_priority,
        )
        if self.funded_shift_warning > self.funded_shift_critical:
            raise ValueError("funded_shift_warning must be <= funded_shift_critical")
        if self.allocation_shift_warning > self.allocation_shift_critical:
            raise ValueError("allocation_shift_warning must be <= allocation_shift_critical")


def _validate_threshold_range(name: str, value: float) -> None:
    if value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be within [0.0, 1.0]")


DEFAULT_THRESHOLDS = AnomalyThresholds()


_PERIOD_YEAR_PATTERN = re.compile(r"(?:19|20)\d{2}")
_PERIOD_FISCAL_PATTERN = re.compile(r"\bFY\s*([0-9]{1,4})\b", re.IGNORECASE)
_PERIOD_NUMBER_PATTERN = re.compile(r"\d+")


def _period_sort_key(period: str) -> tuple[int, int, str]:
    """Return a best-effort chronological sort key for period labels."""
    normalized = period.strip().upper()
    year_match = _PERIOD_YEAR_PATTERN.search(normalized)
    if year_match:
        return (0, int(year_match.group(0)), normalized)

    fiscal_match = _PERIOD_FISCAL_PATTERN.search(normalized)
    if fiscal_match:
        token = fiscal_match.group(1)
        parsed = int(token)
        if len(token) == 2:
            parsed += 2000
        return (0, parsed, normalized)

    number_match = _PERIOD_NUMBER_PATTERN.search(normalized)
    if number_match:
        return (0, int(number_match.group(0)), normalized)

    return (1, -1, normalized)


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
    score: float
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


def _score_anomaly(
    *,
    shift: float,
    confidence: float,
    warning: float,
    critical: float,
) -> float:
    """Return a bounded risk score for deterministic ranking and triage."""
    threshold_span = 1.0 if critical == warning else max(critical - warning, 1e-9)

    if shift < warning:
        normalized_shift = 0.0
    elif shift >= critical:
        normalized_shift = 1.0 + ((shift - critical) / threshold_span)
    else:
        normalized_shift = (shift - warning) / threshold_span

    capped_confidence = max(0.0, min(1.0, confidence))
    return round(normalized_shift * (0.5 + 0.5 * capped_confidence), 6)


def _to_utc_iso(dt: datetime) -> str:
    """Return a UTC-normalized ISO 8601 representation of a datetime."""
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return dt.isoformat()


def _build_evidence_context(
    *, previous: TimeSeriesPoint, current: TimeSeriesPoint
) -> dict[str, object]:
    return {
        "previous_period": previous.period,
        "current_period": current.period,
        "previous_observed_at": _to_utc_iso(previous.observed_at),
        "current_observed_at": _to_utc_iso(current.observed_at),
        "evidence_refs": list(dict.fromkeys(previous.evidence_refs + current.evidence_refs)),
        "previous_provenance": dict(sorted(previous.provenance.items())),
        "current_provenance": dict(sorted(current.provenance.items())),
    }


def _metric_evidence_context(
    *,
    metric: str,
    previous_value: float,
    current_value: float,
    warning_threshold: float,
    critical_threshold: float,
) -> dict[str, object]:
    signed_delta = current_value - previous_value
    return {
        "metric": metric,
        "previous_value": round(previous_value, 6),
        "current_value": round(current_value, 6),
        "signed_delta": round(signed_delta, 6),
        "absolute_delta": round(abs(signed_delta), 6),
        "thresholds": {
            "warning": warning_threshold,
            "critical": critical_threshold,
        },
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
    score = _score_anomaly(
        shift=shift,
        confidence=confidence,
        warning=thresholds.funded_shift_warning,
        critical=thresholds.funded_shift_critical,
    )
    evidence_context = _build_evidence_context(previous=previous, current=current)
    evidence_context["metric_evidence"] = _metric_evidence_context(
        metric="funded_ratio",
        previous_value=previous.funded_ratio,
        current_value=current.funded_ratio,
        warning_threshold=thresholds.funded_shift_warning,
        critical_threshold=thresholds.funded_shift_critical,
    )
    return [
        AnomalyRecord(
            anomaly_id=f"{current.plan_id}:{current.period}:funded_ratio_shift",
            plan_id=current.plan_id,
            period=current.period,
            metric="funded_ratio",
            shift=shift,
            score=score,
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
        score = _score_anomaly(
            shift=shift,
            confidence=confidence,
            warning=thresholds.allocation_shift_warning,
            critical=thresholds.allocation_shift_critical,
        )
        evidence_context = _build_evidence_context(previous=previous, current=current)
        evidence_context["metric_evidence"] = _metric_evidence_context(
            metric=f"allocation:{asset_class}",
            previous_value=previous_value,
            current_value=current_value,
            warning_threshold=thresholds.allocation_shift_warning,
            critical_threshold=thresholds.allocation_shift_critical,
        )
        anomalies.append(
            AnomalyRecord(
                anomaly_id=(
                    f"{current.plan_id}:{current.period}:allocation_shift:{asset_class.lower()}"
                ),
                plan_id=current.plan_id,
                period=current.period,
                metric=f"allocation:{asset_class}",
                shift=shift,
                score=score,
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
    ordered = sorted(
        points,
        key=lambda row: (row.plan_id, _period_sort_key(row.period), row.observed_at),
    )
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
        key=lambda row: (
            row.plan_id,
            _period_sort_key(row.period),
            -row.score,
            row.metric,
            row.anomaly_id,
        ),
    )
