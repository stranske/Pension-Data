"""Tests for SLA metric catalog, computations, and telemetry persistence."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pension_data.monitoring.telemetry import (
    aggregate_metric_window,
    emit_extraction_sla_telemetry,
    emit_ingestion_sla_telemetry,
    emit_review_sla_telemetry,
    emit_sla_telemetry,
    emit_workflow_sla_telemetry,
    write_telemetry_artifact,
)
from pension_data.quality.sla_metrics import (
    SLA_METRIC_CATALOG,
    CoverageObservation,
    RunQualitySnapshot,
    aggregate_disclosure_coverage_by_cohort_period,
    compute_sla_metrics,
    core_sla_metric_catalog,
)


def test_sla_catalog_includes_amended_metrics() -> None:
    required = {
        "completeness_rate",
        "freshness_lag_hours",
        "review_queue_latency_hours",
        "parse_warning_rate",
        "citation_density_per_10_pages",
        "source_mismatch_rate",
        "unresolved_official_source_rate",
        "manager_disclosure_coverage_rate",
        "consultant_disclosure_coverage_rate",
    }
    assert required.issubset(SLA_METRIC_CATALOG.keys())


def test_compute_sla_metrics_expected_values() -> None:
    snapshot = RunQualitySnapshot(
        records_total=100,
        records_complete=94,
        source_published_at=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        run_started_at=datetime(2026, 3, 2, 6, 0, tzinfo=UTC),
        review_queue_items=20,
        review_queue_wait_hours_sum=90.0,
        parse_warning_count=7,
        source_mismatch_count=4,
        unresolved_official_source_count=3,
        total_pages=250,
        cited_facts=35,
        manager_disclosure_total=50,
        manager_disclosure_covered=40,
        consultant_disclosure_total=50,
        consultant_disclosure_covered=30,
    )
    metrics = compute_sla_metrics(snapshot)
    assert metrics["completeness_rate"] == 0.94
    assert metrics["freshness_lag_hours"] == 30.0
    assert metrics["review_queue_latency_hours"] == 4.5
    assert metrics["parse_warning_rate"] == 0.07
    assert metrics["citation_density_per_10_pages"] == 1.4
    assert metrics["source_mismatch_rate"] == 0.04
    assert metrics["unresolved_official_source_rate"] == 0.03
    assert metrics["manager_disclosure_coverage_rate"] == 0.8
    assert metrics["consultant_disclosure_coverage_rate"] == 0.6


def test_compute_sla_metrics_handles_zero_denominators() -> None:
    snapshot = RunQualitySnapshot(
        records_total=0,
        records_complete=0,
        source_published_at=datetime(2026, 3, 2, 6, 0, tzinfo=UTC),
        run_started_at=datetime(2026, 3, 2, 6, 0, tzinfo=UTC),
        review_queue_items=0,
        review_queue_wait_hours_sum=0.0,
        parse_warning_count=0,
        source_mismatch_count=0,
        unresolved_official_source_count=0,
        total_pages=0,
        cited_facts=0,
        manager_disclosure_total=0,
        manager_disclosure_covered=0,
        consultant_disclosure_total=0,
        consultant_disclosure_covered=0,
    )
    metrics = compute_sla_metrics(snapshot)
    assert metrics["completeness_rate"] == 0.0
    assert metrics["review_queue_latency_hours"] == 0.0
    assert metrics["citation_density_per_10_pages"] == 0.0
    assert metrics["manager_disclosure_coverage_rate"] == 0.0
    assert metrics["consultant_disclosure_coverage_rate"] == 0.0


def test_aggregate_disclosure_coverage_by_cohort_period() -> None:
    aggregated = aggregate_disclosure_coverage_by_cohort_period(
        [
            CoverageObservation("state", "2025", True, True),
            CoverageObservation("state", "2025", False, True),
            CoverageObservation("state", "2024", True, False),
        ]
    )

    assert aggregated[("state", "2025")]["systems_count"] == 2.0
    assert aggregated[("state", "2025")]["manager_disclosure_coverage_rate"] == 0.5
    assert aggregated[("state", "2025")]["consultant_disclosure_coverage_rate"] == 1.0
    assert aggregated[("state", "2024")]["manager_disclosure_coverage_rate"] == 1.0
    assert aggregated[("state", "2024")]["consultant_disclosure_coverage_rate"] == 0.0


def test_emit_telemetry_and_build_baseline_report(tmp_path: Path) -> None:
    observed_at = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)
    records = emit_sla_telemetry(
        {"completeness_rate": 0.95, "parse_warning_rate": 0.05},
        observed_at=observed_at,
        tags={"cohort": "state", "period": "2025"},
    )
    assert [record.metric for record in records] == ["completeness_rate", "parse_warning_rate"]

    artifact_path = tmp_path / "telemetry" / "sla.json"
    write_telemetry_artifact(artifact_path, records)
    loaded = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert loaded[0]["metric"] == "completeness_rate"
    assert loaded[0]["tags"] == {"cohort": "state", "period": "2025"}

    summary = aggregate_metric_window(records)
    assert summary["completeness_rate"]["avg"] == 0.95
    assert summary["parse_warning_rate"]["max"] == 0.05


def test_emit_sla_telemetry_uses_distinct_tag_dict_per_record() -> None:
    observed_at = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)
    records = emit_sla_telemetry(
        {"completeness_rate": 0.95, "parse_warning_rate": 0.05},
        observed_at=observed_at,
        tags={"cohort": "state"},
    )
    records[0].tags["cohort"] = "mutated"
    assert records[1].tags["cohort"] == "state"


def test_compute_sla_metrics_rejects_naive_datetimes() -> None:
    snapshot = RunQualitySnapshot(
        records_total=10,
        records_complete=9,
        source_published_at=datetime(2026, 3, 1, 0, 0),
        run_started_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
        review_queue_items=1,
        review_queue_wait_hours_sum=1.0,
        parse_warning_count=1,
        source_mismatch_count=1,
        unresolved_official_source_count=1,
        total_pages=10,
        cited_facts=5,
        manager_disclosure_total=5,
        manager_disclosure_covered=4,
        consultant_disclosure_total=5,
        consultant_disclosure_covered=4,
    )
    with pytest.raises(ValueError, match="source_published_at"):
        compute_sla_metrics(snapshot)


def test_stage_emitters_filter_metrics_and_apply_stage_tag() -> None:
    observed_at = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)
    metrics = {
        "completeness_rate": 0.95,
        "freshness_lag_hours": 6.0,
        "parse_warning_rate": 0.05,
        "review_queue_latency_hours": 4.0,
    }

    ingestion = emit_ingestion_sla_telemetry(
        metrics, observed_at=observed_at, tags={"run_id": "run-1"}
    )
    extraction = emit_extraction_sla_telemetry(
        metrics, observed_at=observed_at, tags={"run_id": "run-1"}
    )
    review = emit_review_sla_telemetry(metrics, observed_at=observed_at, tags={"run_id": "run-1"})

    assert [record.metric for record in ingestion] == [
        "completeness_rate",
        "freshness_lag_hours",
    ]
    assert [record.metric for record in extraction] == ["parse_warning_rate"]
    assert [record.metric for record in review] == ["review_queue_latency_hours"]
    assert all(record.tags["stage"] == "ingestion" for record in ingestion)
    assert all(record.tags["stage"] == "extraction" for record in extraction)
    assert all(record.tags["stage"] == "review" for record in review)


def test_workflow_emitter_partitions_sla_metrics_by_stage() -> None:
    observed_at = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)
    staged = emit_workflow_sla_telemetry(
        {
            "completeness_rate": 0.95,
            "parse_warning_rate": 0.05,
            "review_queue_latency_hours": 4.0,
            "source_mismatch_rate": 0.04,
        },
        observed_at=observed_at,
        tags={"run_id": "run-2", "window_start": "2026-03-01", "window_end": "2026-03-02"},
    )

    assert [record.metric for record in staged["ingestion"]] == ["completeness_rate"]
    assert [record.metric for record in staged["extraction"]] == ["parse_warning_rate"]
    assert [record.metric for record in staged["review"]] == [
        "review_queue_latency_hours",
        "source_mismatch_rate",
    ]
    assert all(
        record.tags["run_id"] == "run-2" for records in staged.values() for record in records
    )
