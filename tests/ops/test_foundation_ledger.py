"""Tests for foundation run-ledger schema and failure taxonomy."""

from __future__ import annotations

import json
from pathlib import Path

from pension_data.ops.foundation import (
    FailureLedgerRow,
    FoundationRunLedger,
    StageLedgerMetric,
    categorize_failure,
    write_run_ledger,
)


def test_categorize_failure_maps_source_and_stage_specific_signals() -> None:
    assert (
        categorize_failure(stage="source_map", message="ValueError: invalid header")
        == "source_map_breakage"
    )
    assert (
        categorize_failure(stage="discovery", message="PermissionError: robots denied")
        == "robots_restriction"
    )
    assert (
        categorize_failure(stage="ingestion", message="RuntimeError: revised-file anomaly")
        == "revised_file_anomaly"
    )
    assert (
        categorize_failure(stage="ingestion", message="ValueError: malformed item")
        == "ingestion_data_error"
    )


def test_write_run_ledger_persists_latest_and_history_records(tmp_path: Path) -> None:
    ledger = FoundationRunLedger(
        run_id="foundation-test-run",
        fixture_path="tests/e2e/foundation/fixtures/foundation_fixture_success.json",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:30+00:00",
        status="failed",
        stage_metrics=(
            StageLedgerMetric(
                stage="registry",
                status="ok",
                record_count=2,
                error_count=0,
                notes="loaded registry fixture",
            ),
            StageLedgerMetric(
                stage="discovery",
                status="error",
                record_count=0,
                error_count=1,
                notes="PermissionError: robots restriction",
            ),
        ),
        failures=(
            FailureLedgerRow(
                stage="discovery",
                category="robots_restriction",
                message="PermissionError: robots restriction",
            ),
        ),
    )
    paths = write_run_ledger(ledger, output_root=tmp_path)

    latest_payload = json.loads(Path(paths["latest_run_ledger_json"]).read_text(encoding="utf-8"))
    history_lines = Path(paths["run_ledger_jsonl"]).read_text(encoding="utf-8").strip().splitlines()
    history_payload = json.loads(history_lines[-1])

    assert latest_payload["run_id"] == "foundation-test-run"
    assert latest_payload["status"] == "failed"
    assert latest_payload["stage_metrics"][1]["stage"] == "discovery"
    assert latest_payload["failures"][0]["category"] == "robots_restriction"
    assert history_payload == latest_payload
