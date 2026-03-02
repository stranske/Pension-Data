"""Extraction readiness artifacts and deterministic quality summaries."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
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
            "system_type": record.cohort,
            "official_resolution_state": record.official_resolution_state,
            "source_authority_tier": record.source_authority_tier,
            "mismatch_reason": record.mismatch_reason or "",
            "readiness_state": derive_readiness_state(record),
        }
        for record in records
    ]
    readiness_rows.sort(
        key=lambda row: (row["cohort"], row["system_type"], row["plan_id"], row["plan_period"])
    )

    totals_by_cohort: defaultdict[str, int] = defaultdict(int)
    unresolved_official_by_cohort: defaultdict[str, int] = defaultdict(int)
    mismatches_by_cohort: defaultdict[str, int] = defaultdict(int)
    stale_period_by_cohort: defaultdict[str, int] = defaultdict(int)
    totals_by_system_type: defaultdict[str, int] = defaultdict(int)
    unresolved_official_by_system_type: defaultdict[str, int] = defaultdict(int)
    mismatches_by_system_type: defaultdict[str, int] = defaultdict(int)
    stale_period_by_system_type: defaultdict[str, int] = defaultdict(int)

    for record in records:
        cohort = record.cohort
        system_type = record.cohort
        totals_by_cohort[cohort] += 1
        totals_by_system_type[system_type] += 1
        if record.official_resolution_state != "available_official":
            unresolved_official_by_cohort[cohort] += 1
            unresolved_official_by_system_type[system_type] += 1
        if record.mismatch_reason is not None:
            mismatches_by_cohort[cohort] += 1
            mismatches_by_system_type[system_type] += 1
        if record.mismatch_reason == "stale_period":
            stale_period_by_cohort[cohort] += 1
            stale_period_by_system_type[system_type] += 1

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

    system_types = sorted(totals_by_system_type.keys())
    summary_by_system_type: list[dict[str, float | int | str]] = []
    for system_type in system_types:
        total = totals_by_system_type[system_type]
        unresolved = unresolved_official_by_system_type[system_type]
        mismatches = mismatches_by_system_type[system_type]
        stale_period = stale_period_by_system_type[system_type]
        summary_by_system_type.append(
            {
                "system_type": system_type,
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
        "summary_by_system_type": summary_by_system_type,
    }


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, *, rows: list[dict[str, object]], fieldnames: tuple[str, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_coverage_artifacts(
    artifacts: Mapping[str, object], *, output_root: Path
) -> dict[str, str]:
    """Write deterministic machine-readable coverage artifacts under `artifacts/coverage`."""
    readiness_rows = artifacts.get("readiness_rows")
    summary_by_cohort = artifacts.get("summary_by_cohort")
    summary_by_system_type = artifacts.get("summary_by_system_type")
    if not isinstance(readiness_rows, list):
        raise ValueError("artifacts['readiness_rows'] must be a list")
    if not isinstance(summary_by_cohort, list):
        raise ValueError("artifacts['summary_by_cohort'] must be a list")
    if not isinstance(summary_by_system_type, list):
        raise ValueError("artifacts['summary_by_system_type'] must be a list")

    coverage_dir = output_root / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)

    readiness_json = coverage_dir / "readiness_rows.json"
    readiness_csv = coverage_dir / "readiness_rows.csv"
    cohort_json = coverage_dir / "summary_by_cohort.json"
    cohort_csv = coverage_dir / "summary_by_cohort.csv"
    system_type_json = coverage_dir / "summary_by_system_type.json"
    system_type_csv = coverage_dir / "summary_by_system_type.csv"

    _write_json(readiness_json, readiness_rows)
    _write_json(cohort_json, summary_by_cohort)
    _write_json(system_type_json, summary_by_system_type)
    _write_csv(
        readiness_csv,
        rows=readiness_rows,
        fieldnames=(
            "plan_id",
            "plan_period",
            "cohort",
            "system_type",
            "official_resolution_state",
            "source_authority_tier",
            "mismatch_reason",
            "readiness_state",
        ),
    )
    summary_fields = (
        "total_plan_periods",
        "unresolved_official_count",
        "mismatch_count",
        "unresolved_official_rate",
        "mismatch_rate",
        "stale_period_rate",
    )
    _write_csv(
        cohort_csv,
        rows=summary_by_cohort,
        fieldnames=("cohort", *summary_fields),
    )
    _write_csv(
        system_type_csv,
        rows=summary_by_system_type,
        fieldnames=("system_type", *summary_fields),
    )

    return {
        "readiness_rows_json": str(readiness_json),
        "readiness_rows_csv": str(readiness_csv),
        "summary_by_cohort_json": str(cohort_json),
        "summary_by_cohort_csv": str(cohort_csv),
        "summary_by_system_type_json": str(system_type_json),
        "summary_by_system_type_csv": str(system_type_csv),
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
