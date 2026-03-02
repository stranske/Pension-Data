"""Tests for extraction-readiness outputs and cohort metrics."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.coverage.readiness import build_publication_artifacts, build_readiness_artifacts
from pension_data.quality.anomaly_rules import TimeSeriesPoint
from pension_data.sources.schema import SourceMapRecord


def _fixture_records() -> list[SourceMapRecord]:
    return [
        SourceMapRecord(
            plan_id="CA-PERS",
            plan_period="FY2024",
            cohort="state",
            source_url="https://example.gov/ca.pdf",
            source_authority_tier="official",
            official_resolution_state="available_official",
            expected_plan_identity="CA-PERS",
        ),
        SourceMapRecord(
            plan_id="TX-ERS",
            plan_period="FY2024",
            cohort="state",
            source_url="https://example.com/tx-third-party.pdf",
            source_authority_tier="high-confidence-third-party",
            official_resolution_state="available_non_official_only",
            expected_plan_identity="TX-ERS",
            mismatch_reason="non_official_only",
        ),
        SourceMapRecord(
            plan_id="AS-GERF",
            plan_period="FY2024",
            cohort="territory",
            source_url="https://example.com/as-not-found",
            source_authority_tier="high-confidence-third-party",
            official_resolution_state="not_found",
            expected_plan_identity="AS-GERF",
            mismatch_reason="stale_period",
        ),
    ]


def _fixture_points() -> list[TimeSeriesPoint]:
    return [
        TimeSeriesPoint(
            plan_id="CA-PERS",
            period="2024",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.82,
            allocations={"public_equity": 0.45, "fixed_income": 0.35},
            confidence=0.95,
            evidence_refs=("doc:ca:2024",),
            provenance={"source_url": "https://example.gov/ca-2024.pdf"},
        ),
        TimeSeriesPoint(
            plan_id="CA-PERS",
            period="2025",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            funded_ratio=0.69,
            allocations={"public_equity": 0.58, "fixed_income": 0.22},
            confidence=0.90,
            evidence_refs=("doc:ca:2025",),
            provenance={"source_url": "https://example.gov/ca-2025.pdf"},
        ),
    ]


def test_readiness_rows_and_summary_are_deterministic() -> None:
    records = _fixture_records()
    first = build_readiness_artifacts(records)
    second = build_readiness_artifacts(list(reversed(records)))
    assert first == second


def test_readiness_outputs_include_expected_states_and_cohort_metrics() -> None:
    artifacts = build_readiness_artifacts(_fixture_records())
    readiness_rows = artifacts["readiness_rows"]
    summary_rows = artifacts["summary_by_cohort"]

    assert readiness_rows == [
        {
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "cohort": "state",
            "official_resolution_state": "available_official",
            "source_authority_tier": "official",
            "mismatch_reason": "",
            "readiness_state": "ready",
        },
        {
            "plan_id": "TX-ERS",
            "plan_period": "FY2024",
            "cohort": "state",
            "official_resolution_state": "available_non_official_only",
            "source_authority_tier": "high-confidence-third-party",
            "mismatch_reason": "non_official_only",
            "readiness_state": "blocked_source",
        },
        {
            "plan_id": "AS-GERF",
            "plan_period": "FY2024",
            "cohort": "territory",
            "official_resolution_state": "not_found",
            "source_authority_tier": "high-confidence-third-party",
            "mismatch_reason": "stale_period",
            "readiness_state": "blocked_source",
        },
    ]
    assert summary_rows == [
        {
            "cohort": "state",
            "total_plan_periods": 2,
            "unresolved_official_count": 1,
            "mismatch_count": 1,
            "unresolved_official_rate": 0.5,
            "mismatch_rate": 0.5,
            "stale_period_rate": 0.0,
        },
        {
            "cohort": "territory",
            "total_plan_periods": 1,
            "unresolved_official_count": 1,
            "mismatch_count": 1,
            "unresolved_official_rate": 1.0,
            "mismatch_rate": 1.0,
            "stale_period_rate": 1.0,
        },
    ]


def test_publication_artifacts_include_prioritized_review_queue_rows() -> None:
    artifacts = build_publication_artifacts(
        _fixture_records(),
        anomaly_points=_fixture_points(),
        queued_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert artifacts["anomaly_routing_status"] == "ok"
    assert artifacts["anomaly_routing_error"] == ""
    anomaly_rows = artifacts["anomaly_rows"]
    assert isinstance(anomaly_rows, list)
    assert anomaly_rows
    assert all("priority" in row for row in anomaly_rows)

    queue_rows = artifacts["review_queue_rows"]
    assert isinstance(queue_rows, list)
    assert queue_rows
    assert queue_rows[0]["priority"] in {"high", "medium", "low"}
    assert queue_rows[0]["queue_id"].startswith("review:")
    assert queue_rows[0]["created_at"] == "2026-01-02T00:00:00+00:00"


def test_publication_artifacts_do_not_block_when_anomaly_routing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_failure(_: list[TimeSeriesPoint]) -> list[object]:
        raise RuntimeError("simulated anomaly failure")

    monkeypatch.setattr("pension_data.coverage.readiness.detect_anomalies", _raise_failure)
    artifacts = build_publication_artifacts(
        _fixture_records(),
        anomaly_points=_fixture_points(),
    )

    assert artifacts["readiness_rows"]
    assert artifacts["summary_by_cohort"]
    assert artifacts["anomaly_rows"] == []
    assert artifacts["review_queue_rows"] == []
    assert artifacts["anomaly_routing_status"] == "degraded"
    assert artifacts["anomaly_routing_error"] == "RuntimeError: simulated anomaly failure"
