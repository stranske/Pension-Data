"""Foundation fixture e2e pipeline tests."""

from __future__ import annotations

import json
from pathlib import Path

from pension_data.ops.foundation import run_foundation_fixture_pipeline

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_fixture_pipeline_runs_registry_to_coverage_and_writes_artifacts(tmp_path: Path) -> None:
    ledger, artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=FIXTURE_DIR / "foundation_fixture_success.json",
        output_root=tmp_path,
        run_id="fixture-success-run",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:30+00:00",
    )

    assert ledger.status == "success"
    assert [item.stage for item in ledger.stage_metrics] == [
        "registry",
        "source_map",
        "discovery",
        "ingestion",
        "coverage",
    ]
    assert all(item.status == "ok" for item in ledger.stage_metrics)
    assert ledger.failures == ()

    for key in (
        "discovery_rows_json",
        "ingestion_summary_json",
        "coverage_readiness_json",
        "latest_run_ledger_json",
        "run_ledger_jsonl",
    ):
        path = Path(artifact_paths[key])
        assert path.exists(), f"missing artifact file for key: {key}"

    coverage_payload = json.loads(
        Path(artifact_paths["coverage_readiness_json"]).read_text(encoding="utf-8")
    )
    assert len(coverage_payload["readiness_rows"]) == 3
    assert coverage_payload["summary_by_cohort"][0]["cohort"] == "state"


def test_fixture_pipeline_classifies_robots_restriction_and_skips_downstream_stages(
    tmp_path: Path,
) -> None:
    ledger, artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=FIXTURE_DIR / "foundation_fixture_robots_failure.json",
        output_root=tmp_path,
        run_id="fixture-robots-failure",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:10+00:00",
    )

    assert ledger.status == "failed"
    assert len(ledger.failures) == 1
    failure = ledger.failures[0]
    assert failure.stage == "discovery"
    assert failure.category == "robots_restriction"

    stage_status = {item.stage: item.status for item in ledger.stage_metrics}
    assert stage_status["registry"] == "ok"
    assert stage_status["source_map"] == "ok"
    assert stage_status["discovery"] == "error"
    assert stage_status["ingestion"] == "skipped"
    assert stage_status["coverage"] == "skipped"

    latest_ledger = json.loads(
        Path(artifact_paths["latest_run_ledger_json"]).read_text(encoding="utf-8")
    )
    assert latest_ledger["status"] == "failed"
    assert latest_ledger["failures"][0]["category"] == "robots_restriction"
