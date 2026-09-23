"""Tests for pilot-artifact-backed API query snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.api.artifact_data import QueryArtifactError, load_query_data_snapshot


def _metric_row(
    *,
    fact_id: str = "fact:funded",
    plan_id: str = "SYN-PLAN",
    normalized_value: float = 0.784,
    confidence: float = 0.9,
) -> dict[str, object]:
    return {
        "fact_id": fact_id,
        "plan_id": plan_id,
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "as_reported_value": 78.4,
        "normalized_value": normalized_value,
        "as_reported_unit": "percent",
        "normalized_unit": "ratio",
        "confidence": confidence,
        "evidence_refs": ["p.4#table"],
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-01-01",
        "benchmark_version": "parser-v1",
        "source_document_id": "doc:syn-plan",
    }


def _write_run(root: Path, run_id: str, rows: list[dict[str, object]]) -> Path:
    run_root = root / "one_pdf_pilot" / run_id
    persistence_root = run_root / "extraction_persistence"
    persistence_root.mkdir(parents=True)
    manifest = {
        "run_id": run_id,
        "ledger_status": "success",
        "input": {
            "plan_id": "SYN-PLAN",
            "plan_period": "FY2024",
            "source_document_id": "doc:syn-plan",
        },
    }
    (run_root / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (persistence_root / "staging_core_metrics.json").write_text(json.dumps(rows), encoding="utf-8")
    return run_root


def test_load_query_data_snapshot_maps_metric_and_funding_rows(tmp_path: Path) -> None:
    (tmp_path / "one_pdf_pilot" / "in-progress").mkdir(parents=True)
    _write_run(tmp_path, "run-a", [_metric_row()])

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 1
    metric = snapshot.metric_history_rows[0]
    assert metric.entity_id == "SYN-PLAN"
    assert metric.normalized_value == pytest.approx(0.784)
    assert metric.report_id == "doc:syn-plan"
    assert metric.valid_from == "2024-06-30"
    assert metric.asserted_at == "2026-01-01"
    assert metric.provenance_refs[0].page_number == 4
    assert snapshot.funding_trend_inputs[0].funded_ratio == pytest.approx(0.784)
    assert snapshot.funding_trend_inputs[0].employer_contributions_usd is None


def test_load_query_data_snapshot_rejects_conflicting_duplicate_fact_id(
    tmp_path: Path,
) -> None:
    _write_run(tmp_path, "run-a", [_metric_row()])
    _write_run(tmp_path, "run-b", [_metric_row(normalized_value=0.8)])

    with pytest.raises(QueryArtifactError, match="conflicting duplicate fact_id"):
        load_query_data_snapshot(tmp_path)


def test_load_query_data_snapshot_rejects_manifest_identity_mismatch(tmp_path: Path) -> None:
    _write_run(tmp_path, "run-a", [_metric_row(plan_id="OTHER")])

    with pytest.raises(QueryArtifactError, match="plan_id conflicts with run manifest"):
        load_query_data_snapshot(tmp_path)


def test_load_query_data_snapshot_rejects_nonfinite_confidence(tmp_path: Path) -> None:
    _write_run(tmp_path, "run-a", [_metric_row(confidence=float("nan"))])

    with pytest.raises(QueryArtifactError, match="confidence must be finite"):
        load_query_data_snapshot(tmp_path)


def test_load_query_data_snapshot_rejects_symlink_escape(tmp_path: Path) -> None:
    root = tmp_path / "root"
    pilot_root = root / "one_pdf_pilot"
    pilot_root.mkdir(parents=True)
    outside = tmp_path / "outside-run"
    outside.mkdir()
    (pilot_root / "escaped").symlink_to(outside, target_is_directory=True)

    with pytest.raises(QueryArtifactError, match="escapes configured root"):
        load_query_data_snapshot(root)
