"""Tests for core-component coverage CI gate."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pension_data.coverage.component_completeness import CORE_SCHEMA_COMPONENTS
from tools.ci_quality.component_coverage_gate import main, run_gate


def _write_component_manifest(
    *,
    root: Path,
    missing_component: str | None = None,
) -> Path:
    component_dir = root / "component_datasets"
    component_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "component_datasets_manifest.json"
    manifest_payload: dict[str, str] = {}

    for component in CORE_SCHEMA_COMPONENTS:
        if component == missing_component:
            continue
        row = {
            "component_name": component,
            "status": "present" if component == "metric_observation" else "not_disclosed",
            "row_count": 3 if component == "metric_observation" else 0,
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "effective_date": "2024-06-30",
            "ingestion_date": "2026-03-03",
            "source_document_id": "doc:1",
            "confidence": 1.0 if component == "metric_observation" else None,
            "evidence_refs": ["p.1"] if component == "metric_observation" else [],
            "notes": "synthetic",
        }
        component_path = component_dir / f"{component}.json"
        component_path.write_text(json.dumps([row], indent=2), encoding="utf-8")
        manifest_payload[component] = str(component_path.relative_to(root))

    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    return manifest_path


def test_component_coverage_gate_passes_and_writes_report(tmp_path: Path) -> None:
    manifest_path = _write_component_manifest(root=tmp_path)
    report_path = tmp_path / "component_coverage_report.json"

    passed = run_gate(
        component_manifest_path=manifest_path,
        run_id="ci-pass",
        report_path=report_path,
    )

    assert passed
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["is_valid"] is True
    assert report["expected_component_count"] == 19
    assert report["run_id"] == "ci-pass"
    assert len(report["per_component"]) == 19


def test_component_coverage_gate_fails_when_manifest_omits_core_component(tmp_path: Path) -> None:
    manifest_path = _write_component_manifest(root=tmp_path, missing_component="manager_entity")
    report_path = tmp_path / "component_coverage_report.json"

    passed = run_gate(
        component_manifest_path=manifest_path,
        run_id="ci-fail",
        report_path=report_path,
    )

    assert not passed
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["is_valid"] is False
    assert "manager_entity" in report["missing_components"]
    assert report["expected_component_count"] == 19


def test_component_coverage_gate_cli_writes_default_report_path(tmp_path: Path) -> None:
    manifest_path = _write_component_manifest(root=tmp_path)
    default_report_path = manifest_path.parent / "component_coverage_report.json"
    original_argv = sys.argv
    sys.argv = [
        "component_coverage_gate.py",
        "--manifest",
        str(manifest_path),
    ]
    try:
        exit_code = main()
    finally:
        sys.argv = original_argv

    assert exit_code == 0
    assert default_report_path.exists()
    report = json.loads(default_report_path.read_text(encoding="utf-8"))
    assert report["is_valid"] is True
    assert len(report["per_component"]) == 19
