"""Cross-module contract tests for the operations and quality layer."""

from __future__ import annotations

from datetime import UTC, datetime

from pension_data.monitoring.telemetry import emit_workflow_sla_telemetry
from pension_data.quality.anomaly_rules import TimeSeriesPoint, detect_anomalies
from pension_data.quality.sla_metrics import RunQualitySnapshot, compute_sla_metrics
from pension_data.review_queue.anomalies import route_anomalies_to_review_queue
from pension_data.scheduling.cadence import (
    PublicationEvent,
    build_cadence_profiles,
    latest_publications,
)
from pension_data.scheduling.planner import plan_refresh_windows
from tools.ci_quality.replay_gate import (
    build_report,
    evaluate_replay_diff,
    sample_unexpected_changes,
    summarize_replay_changes,
)
from tools.replay.harness import (
    CorpusDocument,
    FieldExtraction,
    build_snapshot,
    diff_snapshots,
    run_replay,
)


def _baseline_parser(document: CorpusDocument) -> dict[str, FieldExtraction]:
    return {
        "funded_ratio": FieldExtraction(value=f"{document.content}:0.79", confidence=0.92),
        "discount_rate": FieldExtraction(value="0.0675", confidence=0.88),
    }


def _regressed_parser(document: CorpusDocument) -> dict[str, FieldExtraction]:
    return {
        "funded_ratio": FieldExtraction(value=f"{document.content}:0.74", confidence=0.92),
        "discount_rate": FieldExtraction(value="0.0675", confidence=0.88),
    }


def test_ops_quality_layer_contract_remains_wired_end_to_end() -> None:
    cadence_events = [
        PublicationEvent("ps-ca", "official", datetime(2025, 1, 1, tzinfo=UTC)),
        PublicationEvent("ps-ca", "official", datetime(2025, 2, 1, tzinfo=UTC)),
        PublicationEvent("ps-ca", "official", datetime(2025, 3, 1, tzinfo=UTC)),
        PublicationEvent("ps-ca", "official", datetime(2025, 4, 1, tzinfo=UTC)),
    ]
    profiles = build_cadence_profiles(cadence_events)
    publications = latest_publications(cadence_events)
    plans = plan_refresh_windows(
        profiles=profiles,
        last_publications=publications,
        as_of=datetime(2025, 4, 15, tzinfo=UTC),
    )
    assert plans
    assert plans[0].recommended_interval_days >= 7.0
    assert plans[0].recommended_interval_days <= 120.0

    metrics = compute_sla_metrics(
        RunQualitySnapshot(
            records_total=100,
            records_complete=95,
            source_published_at=datetime(2025, 4, 10, tzinfo=UTC),
            run_started_at=datetime(2025, 4, 12, tzinfo=UTC),
            review_queue_items=10,
            review_queue_wait_hours_sum=40.0,
            parse_warning_count=5,
            source_mismatch_count=2,
            unresolved_official_source_count=1,
            total_pages=500,
            cited_facts=320,
            manager_disclosure_total=50,
            manager_disclosure_covered=48,
            consultant_disclosure_total=50,
            consultant_disclosure_covered=46,
        )
    )
    staged_telemetry = emit_workflow_sla_telemetry(
        metrics,
        observed_at=datetime(2025, 4, 12, tzinfo=UTC),
        tags={"run_id": "ops-contract", "window_start": "2025-04-01", "window_end": "2025-04-30"},
    )
    assert staged_telemetry["ingestion"]
    assert staged_telemetry["extraction"]
    assert staged_telemetry["review"]

    anomaly_points = [
        TimeSeriesPoint(
            plan_id="ps-ca",
            period="FY2024",
            observed_at=datetime(2025, 4, 1, tzinfo=UTC),
            funded_ratio=0.82,
            allocations={"public_equity": 0.51, "fixed_income": 0.30},
            confidence=0.95,
            evidence_refs=("p.40",),
            provenance={"source_url": "https://example.org/ca-2024.pdf"},
        ),
        TimeSeriesPoint(
            plan_id="ps-ca",
            period="FY2025",
            observed_at=datetime(2026, 4, 1, tzinfo=UTC),
            funded_ratio=0.68,
            allocations={"public_equity": 0.62, "fixed_income": 0.21},
            confidence=0.93,
            evidence_refs=("p.44",),
            provenance={"source_url": "https://example.org/ca-2025.pdf"},
        ),
    ]
    anomalies = detect_anomalies(anomaly_points)
    queue_items = route_anomalies_to_review_queue(
        anomalies, queued_at=datetime(2026, 4, 2, tzinfo=UTC)
    )
    assert anomalies
    assert queue_items
    assert queue_items[0].queue_id.startswith("review:")
    assert "confidence=" in queue_items[0].reason
    evidence_context = queue_items[0].evidence_context
    assert evidence_context["previous_period"] == "FY2024"
    assert evidence_context["current_period"] == "FY2025"
    assert evidence_context["current_provenance"] == {
        "source_url": "https://example.org/ca-2025.pdf"
    }
    assert evidence_context["metric_evidence"] == {
        "metric": "funded_ratio",
        "previous_value": 0.82,
        "current_value": 0.68,
        "signed_delta": -0.14,
        "absolute_delta": 0.14,
        "thresholds": {
            "warning": 0.05,
            "critical": 0.1,
        },
    }

    corpus = [CorpusDocument(document_id="doc-1", content="ca-pension")]
    baseline = build_snapshot(
        run_replay(corpus, _baseline_parser),
        parser_id="tests.ops:baseline",
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    current = build_snapshot(
        run_replay(corpus, _regressed_parser),
        parser_id="tests.ops:regressed",
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    diff = diff_snapshots(baseline=baseline, current=current)
    assert diff["unexpected_changes"] >= 1
    violations = evaluate_replay_diff(
        unexpected_changes=diff["unexpected_changes"], max_unexpected=0
    )
    assert violations
    assert "unexpected replay drift" in violations[0]
    diff_payload: dict[str, object] = dict(diff)
    classification_counts = summarize_replay_changes(diff_payload)
    assert classification_counts == {"unexpected_drift": 1}
    unexpected_examples = sample_unexpected_changes(diff_payload, limit=1)
    assert unexpected_examples == [
        {
            "document_id": "doc-1",
            "field": "funded_ratio",
            "attribute": "value",
            "baseline": "ca-pension:0.79",
            "current": "ca-pension:0.74",
            "classification": "unexpected_drift",
        }
    ]

    report = build_report(
        total_changes=diff["total_changes"],
        unexpected_changes=diff["unexpected_changes"],
        max_unexpected=0,
        violations=violations,
        classification_counts=classification_counts,
        unexpected_examples=unexpected_examples,
    )
    assert report["status"] == "fail"
    assert report["classification_counts"] == {"unexpected_drift": 1}
    assert report["unexpected_examples"] == unexpected_examples
