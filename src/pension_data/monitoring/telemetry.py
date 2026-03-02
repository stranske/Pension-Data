"""Telemetry record emission and baseline reporting for SLA metrics."""

from __future__ import annotations

import csv
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

from pension_data.quality.sla_metrics import SLA_METRIC_CATALOG, SLAStage


@dataclass(frozen=True, slots=True)
class TelemetryRecord:
    """Persisted metric record with tags and timestamp."""

    metric: str
    value: float
    observed_at: datetime
    tags: dict[str, str]


class BaselineReportRow(TypedDict):
    """Structured row for per-window SLA baseline report output."""

    window_start: str
    window_end: str
    run_ids: list[str]
    stages: list[str]
    record_count: int
    metrics: dict[str, dict[str, float]]


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


def _record_to_table_row(record: TelemetryRecord) -> dict[str, object]:
    tags = dict(sorted(record.tags.items()))
    return {
        "observed_at": record.observed_at.isoformat(),
        "metric": record.metric,
        "value": record.value,
        "run_id": tags.get("run_id", ""),
        "stage": tags.get("stage", ""),
        "window_start": tags.get("window_start", ""),
        "window_end": tags.get("window_end", ""),
        "tags": tags,
    }


def build_telemetry_table(records: list[TelemetryRecord]) -> list[dict[str, object]]:
    """Normalize telemetry into table rows with first-class dimension columns."""
    rows = [_record_to_table_row(record) for record in records]
    return sorted(
        rows,
        key=lambda row: (
            str(row["observed_at"]),
            str(row["metric"]),
            str(row["stage"]),
            str(row["run_id"]),
        ),
    )


def write_telemetry_artifact(path: Path, records: list[TelemetryRecord]) -> None:
    """Persist telemetry records as sorted JSON array artifact."""
    serialized = [_record_to_json(record) for record in records]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(serialized, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_telemetry_table_artifacts(
    output_dir: Path, records: list[TelemetryRecord]
) -> dict[str, Path]:
    """Persist normalized table artifacts for downstream SLA analytics."""
    table_rows = build_telemetry_table(records)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "sla_telemetry_table.json"
    json_path.write_text(json.dumps(table_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    csv_path = output_dir / "sla_telemetry_table.csv"
    fieldnames = [
        "observed_at",
        "metric",
        "value",
        "run_id",
        "stage",
        "window_start",
        "window_end",
        "tags",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in table_rows:
            csv_row = dict(row)
            csv_row["tags"] = json.dumps(row["tags"], sort_keys=True)
            writer.writerow(csv_row)

    return {"json": json_path, "csv": csv_path}


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


def _window_bounds(record: TelemetryRecord) -> tuple[str, str]:
    window_start = record.tags.get("window_start", "") or "unknown"
    window_end = record.tags.get("window_end", "") or "unknown"
    return window_start, window_end


def build_windowed_baseline_report(records: list[TelemetryRecord]) -> list[BaselineReportRow]:
    """Build baseline SLA summaries grouped by run window boundaries."""
    grouped: dict[tuple[str, str], list[TelemetryRecord]] = {}
    for record in records:
        grouped.setdefault(_window_bounds(record), []).append(record)

    report_rows: list[BaselineReportRow] = []
    for window_start, window_end in sorted(grouped.keys()):
        window_records = grouped[(window_start, window_end)]
        run_ids = sorted(
            {
                run_id
                for run_id in (record.tags.get("run_id", "") for record in window_records)
                if run_id
            }
        )
        stages = sorted(
            {
                stage
                for stage in (record.tags.get("stage", "") for record in window_records)
                if stage
            }
        )
        report_rows.append(
            {
                "window_start": window_start,
                "window_end": window_end,
                "run_ids": run_ids,
                "stages": stages,
                "record_count": len(window_records),
                "metrics": aggregate_metric_window(window_records),
            }
        )
    return report_rows


def write_baseline_report_artifacts(
    output_dir: Path, records: list[TelemetryRecord]
) -> dict[str, Path]:
    """Persist per-window baseline SLA report artifacts as JSON and CSV."""
    report_rows = build_windowed_baseline_report(records)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "sla_baseline_report.json"
    json_path.write_text(json.dumps(report_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    csv_path = output_dir / "sla_baseline_report.csv"
    fieldnames = [
        "window_start",
        "window_end",
        "run_ids",
        "stages",
        "metric",
        "count",
        "min",
        "max",
        "avg",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for report_row in report_rows:
            metric_rows = report_row["metrics"]
            if not isinstance(metric_rows, dict):
                continue
            for metric_name in sorted(metric_rows.keys()):
                stats = metric_rows[metric_name]
                writer.writerow(
                    {
                        "window_start": report_row["window_start"],
                        "window_end": report_row["window_end"],
                        "run_ids": json.dumps(report_row["run_ids"], sort_keys=True),
                        "stages": json.dumps(report_row["stages"], sort_keys=True),
                        "metric": metric_name,
                        "count": stats["count"],
                        "min": stats["min"],
                        "max": stats["max"],
                        "avg": stats["avg"],
                    }
                )

    return {"json": json_path, "csv": csv_path}
