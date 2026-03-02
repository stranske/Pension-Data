"""Telemetry record emission and baseline reporting for SLA metrics."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pension_data.quality.sla_metrics import SLA_METRIC_CATALOG, SLAStage


@dataclass(frozen=True, slots=True)
class TelemetryRecord:
    """Persisted metric record with tags and timestamp."""

    metric: str
    value: float
    observed_at: datetime
    tags: dict[str, str]


def emit_sla_telemetry(
    metrics: Mapping[str, float],
    *,
    observed_at: datetime,
    tags: Mapping[str, str] | None = None,
) -> list[TelemetryRecord]:
    """Emit deterministic telemetry records for SLA metric map."""
    base_tags = dict(tags or {})
    observed_utc = observed_at.astimezone(UTC)
    records: list[TelemetryRecord] = []
    for metric in sorted(metrics.keys()):
        records.append(
            TelemetryRecord(
                metric=metric,
                value=float(metrics[metric]),
                observed_at=observed_utc,
                tags=dict(base_tags),
            )
        )
    return records


def emit_stage_sla_telemetry(
    metrics: Mapping[str, float],
    *,
    stage: SLAStage,
    observed_at: datetime,
    tags: Mapping[str, str] | None = None,
) -> list[TelemetryRecord]:
    """Emit telemetry for one pipeline stage and enforce the stage tag."""
    stage_metrics = {
        metric_name: float(metrics[metric_name])
        for metric_name, definition in sorted(SLA_METRIC_CATALOG.items())
        if definition.stage == stage and metric_name in metrics
    }
    stage_tags = dict(tags or {})
    stage_tags["stage"] = stage
    return emit_sla_telemetry(stage_metrics, observed_at=observed_at, tags=stage_tags)


def emit_ingestion_sla_telemetry(
    metrics: Mapping[str, float],
    *,
    observed_at: datetime,
    tags: Mapping[str, str] | None = None,
) -> list[TelemetryRecord]:
    """Emit ingestion workflow metrics."""
    return emit_stage_sla_telemetry(
        metrics,
        stage="ingestion",
        observed_at=observed_at,
        tags=tags,
    )


def emit_extraction_sla_telemetry(
    metrics: Mapping[str, float],
    *,
    observed_at: datetime,
    tags: Mapping[str, str] | None = None,
) -> list[TelemetryRecord]:
    """Emit extraction workflow metrics."""
    return emit_stage_sla_telemetry(
        metrics,
        stage="extraction",
        observed_at=observed_at,
        tags=tags,
    )


def emit_review_sla_telemetry(
    metrics: Mapping[str, float],
    *,
    observed_at: datetime,
    tags: Mapping[str, str] | None = None,
) -> list[TelemetryRecord]:
    """Emit review workflow metrics."""
    return emit_stage_sla_telemetry(
        metrics,
        stage="review",
        observed_at=observed_at,
        tags=tags,
    )


def emit_workflow_sla_telemetry(
    metrics: Mapping[str, float],
    *,
    observed_at: datetime,
    tags: Mapping[str, str] | None = None,
) -> dict[SLAStage, list[TelemetryRecord]]:
    """Emit SLA telemetry partitioned by ingestion, extraction, and review workflows."""
    return {
        "ingestion": emit_ingestion_sla_telemetry(metrics, observed_at=observed_at, tags=tags),
        "extraction": emit_extraction_sla_telemetry(metrics, observed_at=observed_at, tags=tags),
        "review": emit_review_sla_telemetry(metrics, observed_at=observed_at, tags=tags),
    }


def _record_to_json(record: TelemetryRecord) -> dict[str, object]:
    return {
        "metric": record.metric,
        "value": record.value,
        "observed_at": record.observed_at.isoformat(),
        "tags": dict(sorted(record.tags.items())),
    }


def write_telemetry_artifact(path: Path, records: list[TelemetryRecord]) -> None:
    """Persist telemetry records as sorted JSON array artifact."""
    serialized = [_record_to_json(record) for record in records]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(serialized, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def aggregate_metric_window(records: list[TelemetryRecord]) -> dict[str, dict[str, float]]:
    """Build baseline report aggregates for a telemetry window."""
    by_metric: dict[str, list[float]] = {}
    for record in records:
        by_metric.setdefault(record.metric, []).append(record.value)

    summary: dict[str, dict[str, float]] = {}
    for metric_name in sorted(by_metric.keys()):
        values = by_metric[metric_name]
        count = float(len(values))
        summary[metric_name] = {
            "count": count,
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / count,
        }
    return summary
