"""Tests for building generated web workspace bundles from pilot artifacts."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

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

SERVE_PATH = ROOT / "scripts" / "web" / "serve_local.py"
serve_spec = importlib.util.spec_from_file_location("web_serve_local", SERVE_PATH)
assert serve_spec is not None and serve_spec.loader is not None
web_serve_local = importlib.util.module_from_spec(serve_spec)
serve_spec.loader.exec_module(web_serve_local)


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
                "ledger_status": "success",
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
    # #639: the builder is the third contract consumer, so serve_local must accept
    # what the builder produced and smoke_test approved.
    web_serve_local._assert_workspace_bundle(
        payload,
        path_label="data/workspace.json",
        allow_fixture_demo=False,
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


def test_generator_accepts_paths_already_rooted_at_output_root(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "outputs" / "one_pdf_pilot" / "run-456"
    persistence_dir = run_dir / "extraction_persistence"
    persistence_dir.mkdir(parents=True)
    staging_path = persistence_dir / "staging_core_metrics.json"
    staging_path.write_text(
        json.dumps(
            [
                {
                    "confidence": 0.93,
                    "metric_family": "funded_status",
                    "metric_name": "funded_ratio",
                    "normalized_value": 0.82,
                    "plan_id": "ca-pers",
                    "plan_period": "FY2025",
                    "source_document_id": "calpers-fy2025",
                }
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "artifact_files": {
                    "staging_core_metrics_json": (
                        "outputs/one_pdf_pilot/run-456/"
                        "extraction_persistence/staging_core_metrics.json"
                    ),
                },
                "input": {
                    "effective_date": "2026-05-30",
                    "plan_id": "ca-pers",
                    "plan_period": "FY2025",
                    "source_document_id": "calpers-fy2025",
                },
                "ledger_status": "success",
                "run_id": "run-456",
            }
        ),
        encoding="utf-8",
    )
    contract_path = tmp_path / "apps" / "contracts" / "runtime-contract.json"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        (ROOT / "apps" / "contracts" / "runtime-contract.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    payload = build_workspace_bundle.build_workspace_bundle(run_dir)

    assert payload["datasets"][0]["rows"][0]["metric"] == "funded_ratio"


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
        cwd=tmp_path,
        text=True,
    )

    assert "wrote generated workspace bundle" in result.stdout
    payload = json.loads(out.read_text(encoding="utf-8"))
    web_smoke_test._assert_workspace_bundle(
        payload,
        path_label="data/workspace.json",
        reject_fixture=True,
    )


def _staging_row() -> dict[str, object]:
    return {
        "confidence": 0.92,
        "evidence_refs": ["p12:t4:r2"],
        "metric_family": "funded_status",
        "metric_name": "funded_ratio",
        "normalized_value": 0.81,
        "plan_id": "ca-pers",
        "plan_period": "FY2024",
        "source_document_id": "calpers-fy2024",
    }


def _valid_manifest(*, artifact_path: str) -> dict[str, object]:
    return {
        "artifact_files": {"staging_core_metrics_json": artifact_path},
        "input": {
            "effective_date": "2026-05-30",
            "plan_id": "ca-pers",
            "plan_period": "FY2024",
            "source_document_id": "calpers-fy2024",
        },
        "ledger_status": "success",
        "run_id": "run-guard",
    }


def _symlink_outside_run(
    manifest: dict[str, object], run: Path, other: Path, *_args: object
) -> None:
    link = run / "persistence" / "outside_link.json"
    if link.exists() or link.is_symlink():
        link.unlink()
    os.symlink(other / "persistence" / "staging_core_metrics.json", link)
    manifest["artifact_files"]["staging_core_metrics_json"] = "persistence/outside_link.json"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest, _run, _other: manifest.update(ledger_status="failed"),
        lambda manifest, _run, _other: manifest["input"].update(plan_id="other-plan"),
        lambda manifest, _run, _other: manifest["input"].update(plan_period="FY2099"),
        lambda manifest, _run, _other: manifest["input"].update(source_document_id="foreign-doc"),
        lambda manifest, _run, other: manifest["artifact_files"].update(
            staging_core_metrics_json=str(other / "persistence" / "staging_core_metrics.json")
        ),
        lambda manifest, _run, _other: manifest["artifact_files"].update(
            staging_core_metrics_json="../outside/staging_core_metrics.json"
        ),
        _symlink_outside_run,
    ],
    ids=[
        "failed-ledger",
        "mismatched-plan-id",
        "mismatched-plan-period",
        "mismatched-source-document",
        "absolute-outside-run",
        "traversal-outside-run",
        "symlink-outside-run",
    ],
)
def test_builder_rejects_failed_or_foreign_pilot_artifacts(tmp_path: Path, mutate) -> None:
    """Reject unsafe pilot inputs before writing a generated review bundle.

    Deliberate-break: reverting ``_artifact_path`` on ``origin/main`` (early return
    when ``candidate.is_absolute() or candidate.exists()``) accepts
    ``absolute-outside-run`` staging whose plan/period/document identity still
    matches the manifest, so this parametrized test fails until containment is
    restored.
    """
    run_dir = tmp_path / "selected-run"
    other_run = tmp_path / "other-run"
    for run in (run_dir, other_run):
        persistence = run / "persistence"
        persistence.mkdir(parents=True)
        (persistence / "staging_core_metrics.json").write_text(
            json.dumps([_staging_row()]),
            encoding="utf-8",
        )
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "staging_core_metrics.json").write_text(
        json.dumps([_staging_row()]),
        encoding="utf-8",
    )

    manifest = _valid_manifest(
        artifact_path="persistence/staging_core_metrics.json",
    )
    mutate(manifest, run_dir, other_run)
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        build_workspace_bundle.build_workspace_bundle(run_dir)

    out = tmp_path / "workspace.json"
    good_manifest = _valid_manifest(
        artifact_path="persistence/staging_core_metrics.json",
    )
    (run_dir / "run_manifest.json").write_text(json.dumps(good_manifest), encoding="utf-8")
    write_payload = build_workspace_bundle.build_workspace_bundle(run_dir)
    out.write_text(json.dumps(write_payload), encoding="utf-8")
    before = out.read_bytes()

    bad_manifest = _valid_manifest(
        artifact_path="persistence/staging_core_metrics.json",
    )
    mutate(bad_manifest, run_dir, other_run)
    (run_dir / "run_manifest.json").write_text(json.dumps(bad_manifest), encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(BUILDER_PATH),
            "--pilot-run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert out.read_bytes() == before
