"""Tests for extraction-readiness outputs and cohort metrics."""

from __future__ import annotations

from pension_data.coverage.readiness import build_readiness_artifacts
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
