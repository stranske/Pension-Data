"""Tests for building generated web workspace bundles from pilot artifacts."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = ROOT / "scripts" / "web" / "build_workspace_bundle.py"
SMOKE_PATH = ROOT / "scripts" / "web" / "smoke_test.py"

builder_spec = importlib.util.spec_from_file_location("build_workspace_bundle", BUILDER_PATH)
assert builder_spec is not None and builder_spec.loader is not None
build_workspace_bundle = importlib.util.module_from_spec(builder_spec)
builder_spec.loader.exec_module(build_workspace_bundle)

smoke_spec = importlib.util.spec_from_file_location("web_smoke_test", SMOKE_PATH)
assert smoke_spec is not None and smoke_spec.loader is not None
web_smoke_test = importlib.util.module_from_spec(smoke_spec)
smoke_spec.loader.exec_module(web_smoke_test)


def _write_fixture_pilot_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "pilot-run"
    persistence_dir = run_dir / "persistence"
    persistence_dir.mkdir(parents=True)
    staging_path = persistence_dir / "staging_core_metrics.json"
    staging_path.write_text(
        json.dumps(
            [
                {
                    "confidence": 0.92,
                    "evidence_refs": ["p12:t4:r2", "p13:t1:r1"],
                    "metric_family": "funded_status",
                    "metric_name": "funded_ratio",
                    "normalized_value": 0.81,
                    "plan_id": "ca-pers",
                    "plan_period": "FY2024",
                    "source_document_id": "calpers-fy2024",
                },
                {
                    "as_reported_value": "$41000000",
                    "confidence": "0.87",
                    "evidence_refs": json.dumps(["p22:cashflow"]),
                    "metric_family": "cash_flow",
                    "metric_name": "employer_contributions",
                    "normalized_value": None,
                    "plan_id": "ca-pers",
                    "plan_period": "FY2024",
                    "source_document_id": "calpers-fy2024",
                },
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "artifact_files": {
                    "staging_core_metrics_json": str(staging_path),
                },
                "input": {
                    "effective_date": "2026-05-30",
                    "plan_id": "ca-pers",
                    "plan_period": "FY2024",
                    "source_document_id": "calpers-fy2024",
                },
                "run_id": "run-123",
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def test_generator_emits_runtime_valid_generated_bundle(tmp_path: Path) -> None:
    run_dir = _write_fixture_pilot_run(tmp_path)
    payload = build_workspace_bundle.build_workspace_bundle(run_dir)

    web_smoke_test._assert_workspace_bundle(
        payload,
        path_label="data/workspace.json",
        reject_fixture=True,
    )
    assert payload["data_origin"] == "generated"
    dataset = payload["datasets"][0]
    assert dataset["id"] == "one-pdf-pilot-run-123"
    assert dataset["lastUpdated"] == "2026-05-30"
    assert dataset["rows"]
    assert dataset["rows"][0] == {
        "confidence": 0.87,
        "entity": "ca-pers",
        "metric": "employer_contributions",
        "metric_family": "cash_flow",
        "plan_period": "FY2024",
        "provenance": {
            "evidence_refs": ["p22:cashflow"],
            "source_document": "calpers-fy2024",
        },
        "value": "$41000000",
    }


def test_cli_writes_bundle_that_runtime_smoke_accepts(tmp_path: Path) -> None:
    run_dir = _write_fixture_pilot_run(tmp_path)
    out = tmp_path / "workspace.json"

    result = subprocess.run(
        [
            sys.executable,
            str(BUILDER_PATH),
            "--pilot-run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "wrote generated workspace bundle" in result.stdout
    payload = json.loads(out.read_text(encoding="utf-8"))
    web_smoke_test._assert_workspace_bundle(
        payload,
        path_label="data/workspace.json",
        reject_fixture=True,
    )
