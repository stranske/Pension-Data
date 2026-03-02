"""Extraction readiness artifacts and deterministic quality summaries."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Literal

from pension_data.quality.anomaly_rules import (
    AnomalyRecord,
    TimeSeriesPoint,
    detect_anomalies,
)
from pension_data.review_queue.anomalies import route_anomalies_to_review_queue
from pension_data.sources.schema import SourceMapRecord

ReadinessState = Literal["ready", "blocked_source", "blocked_quality"]


def derive_readiness_state(record: SourceMapRecord) -> ReadinessState:
    """Map source-quality signals to extraction-readiness status."""
    if record.official_resolution_state in {"not_found", "available_non_official_only"}:
        return "blocked_source"
    if record.mismatch_reason in {"wrong_plan", "stale_period", "non_official_only"}:
        return "blocked_quality"
    return "ready"


def build_readiness_artifacts(records: list[SourceMapRecord]) -> dict[str, object]:
    """Build machine-readable readiness rows and deterministic cohort summaries."""
    readiness_rows = [
        {
            "plan_id": record.plan_id,
            "plan_period": record.plan_period,
            "cohort": record.cohort,
            "official_resolution_state": record.official_resolution_state,
            "source_authority_tier": record.source_authority_tier,
            "mismatch_reason": record.mismatch_reason or "",
            "readiness_state": derive_readiness_state(record),
        }
        for record in records
    ]
    readiness_rows.sort(key=lambda row: (row["cohort"], row["plan_id"], row["plan_period"]))

    totals_by_cohort: defaultdict[str, int] = defaultdict(int)
    unresolved_official_by_cohort: defaultdict[str, int] = defaultdict(int)
    mismatches_by_cohort: defaultdict[str, int] = defaultdict(int)
    stale_period_by_cohort: defaultdict[str, int] = defaultdict(int)

    for record in records:
        cohort = record.cohort
        totals_by_cohort[cohort] += 1
        if record.official_resolution_state != "available_official":
            unresolved_official_by_cohort[cohort] += 1
        if record.mismatch_reason is not None:
            mismatches_by_cohort[cohort] += 1
        if record.mismatch_reason == "stale_period":
            stale_period_by_cohort[cohort] += 1

    cohorts = sorted(totals_by_cohort.keys())
    summary_rows: list[dict[str, float | int | str]] = []
    for cohort in cohorts:
        total = totals_by_cohort[cohort]
        unresolved = unresolved_official_by_cohort[cohort]
        mismatches = mismatches_by_cohort[cohort]
        stale_period = stale_period_by_cohort[cohort]
        summary_rows.append(
            {
                "cohort": cohort,
                "total_plan_periods": total,
                "unresolved_official_count": unresolved,
                "mismatch_count": mismatches,
                "unresolved_official_rate": round(unresolved / total, 6),
                "mismatch_rate": round(mismatches / total, 6),
                "stale_period_rate": round(stale_period / total, 6),
            }
        )

    return {
        "readiness_rows": readiness_rows,
        "summary_by_cohort": summary_rows,
    }


def _serialize_anomaly_rows(anomalies: list[AnomalyRecord]) -> list[dict[str, object]]:
    return [
        {
            "anomaly_id": anomaly.anomaly_id,
            "plan_id": anomaly.plan_id,
            "period": anomaly.period,
            "metric": anomaly.metric,
            "shift": anomaly.shift,
            "score": anomaly.score,
            "severity": anomaly.severity,
            "confidence": anomaly.confidence,
            "priority": anomaly.priority,
            "requires_review": anomaly.requires_review,
            "evidence_context": dict(anomaly.evidence_context),
        }
        for anomaly in anomalies
    ]


def build_publication_artifacts(
    records: list[SourceMapRecord],
    *,
    anomaly_points: list[TimeSeriesPoint] | None = None,
    queued_at: datetime | None = None,
) -> dict[str, object]:
    """Build publication artifacts with non-blocking anomaly review-queue routing."""
    artifacts = build_readiness_artifacts(records)
    points = anomaly_points or []

    try:
        anomalies = detect_anomalies(points)
        anomaly_rows = _serialize_anomaly_rows(anomalies)
        queue_items = route_anomalies_to_review_queue(
            anomalies,
            queued_at=queued_at,
        )
    except Exception as error:
        artifacts["anomaly_rows"] = []
        artifacts["review_queue_rows"] = []
        artifacts["anomaly_routing_status"] = "degraded"
        artifacts["anomaly_routing_error"] = f"{type(error).__name__}: {error}"
        return artifacts

    artifacts["anomaly_rows"] = anomaly_rows
    artifacts["review_queue_rows"] = [
        {
            "queue_id": item.queue_id,
            "anomaly_id": item.anomaly_id,
            "plan_id": item.plan_id,
            "period": item.period,
            "priority": item.priority,
            "metric": item.metric,
            "reason": item.reason,
            "evidence_context": dict(item.evidence_context),
            "created_at": item.created_at.astimezone(UTC).isoformat(),
        }
        for item in queue_items
    ]
    artifacts["anomaly_routing_status"] = "ok"
    artifacts["anomaly_routing_error"] = ""
    return artifacts
