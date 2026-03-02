"""Telemetry record emission and baseline reporting for SLA metrics."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


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
                tags=base_tags,
            )
        )
    return records


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
