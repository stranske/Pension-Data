"""Tests for one-PDF schema component completeness coverage validation."""

from __future__ import annotations

import json
from pathlib import Path

from pension_data.coverage.component_completeness import (
    CORE_SCHEMA_COMPONENTS,
    build_component_coverage_report_from_manifest,
    build_component_datasets,
    validate_component_coverage,
)


def test_validator_reports_missing_components() -> None:
    report = validate_component_coverage(component_datasets={})

    assert report["is_valid"] is False
    assert report["missing_components"] == sorted(CORE_SCHEMA_COMPONENTS)
    assert report["expected_component_count"] == 19


def test_validator_rejects_present_rows_without_required_evidence_metadata() -> None:
    invalid = {
        component: [
            {
                "component_name": component,
                "status": "present",
                "row_count": 1,
                "plan_id": "",
                "plan_period": "FY2024",
                "effective_date": "2024-06-30",
                "ingestion_date": "2026-03-03",
                "source_document_id": "doc:1",
                "confidence": 1.0,
                "evidence_refs": [],
                "notes": "synthetic",
            }
        ]
        for component in CORE_SCHEMA_COMPONENTS
    }

    report = validate_component_coverage(component_datasets=invalid)

    assert report["is_valid"] is False
    assert report["missing_components"] == []
    assert any(item["field"] == "plan_id" for item in report["metadata_violations"])
    assert any(item["field"] == "evidence_refs" for item in report["metadata_violations"])


def test_validator_rejects_invalid_state_specific_metadata() -> None:
    payload = {
        component: [
            {
                "component_name": component,
                "status": "not_disclosed",
                "row_count": 0,
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "effective_date": "2024-06-30",
                "ingestion_date": "2026-03-03",
                "source_document_id": "doc:1",
                "confidence": None,
                "evidence_refs": [],
                "notes": "synthetic",
            }
        ]
        for component in CORE_SCHEMA_COMPONENTS
    }
    payload["metric_observation"] = [
        {
            "component_name": "metric_observation",
            "status": "partial",
            "row_count": 1,
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "effective_date": "2024-06-30",
            "ingestion_date": "2026-03-03",
            "source_document_id": "doc:1",
            "confidence": 1.0,
            "evidence_refs": ["p.1"],
            "notes": "synthetic",
        }
    ]
    payload["benchmark_definition"] = [
        {
            "component_name": "benchmark_definition",
            "status": "not_disclosed",
            "row_count": 1,
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "effective_date": "2024-06-30",
            "ingestion_date": "2026-03-03",
            "source_document_id": "doc:1",
            "confidence": 0.0,
            "evidence_refs": ["p.2"],
            "notes": "synthetic",
        }
    ]

    report = validate_component_coverage(component_datasets=payload)

    assert report["is_valid"] is False
    assert any(
        item["component"] == "metric_observation"
        and item["field"] == "confidence"
        and "partial rows require confidence=0.5" in item["message"]
        for item in report["metadata_violations"]
    )
    assert any(
        item["component"] == "benchmark_definition"
        and item["field"] == "evidence_refs"
        and "not_disclosed rows require empty evidence_refs" in item["message"]
        for item in report["metadata_violations"]
    )
    assert any(
        item["component"] == "benchmark_definition"
        and item["field"] == "row_count"
        and "not_disclosed rows require row_count=0" in item["message"]
        for item in report["metadata_violations"]
    )


def test_validator_rejects_unexpected_components() -> None:
    payload = {
        component: [
            {
                "component_name": component,
                "status": "not_disclosed",
                "row_count": 0,
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "effective_date": "2024-06-30",
                "ingestion_date": "2026-03-03",
                "source_document_id": "doc:1",
                "confidence": None,
                "evidence_refs": [],
                "notes": "synthetic",
            }
        ]
        for component in CORE_SCHEMA_COMPONENTS
    }
    payload["unexpected_component"] = [
        {
            "component_name": "unexpected_component",
            "status": "present",
            "row_count": 1,
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "effective_date": "2024-06-30",
            "ingestion_date": "2026-03-03",
            "source_document_id": "doc:1",
            "confidence": 1.0,
            "evidence_refs": ["p.1"],
            "notes": "synthetic",
        }
    ]

    report = validate_component_coverage(component_datasets=payload)

    assert report["is_valid"] is False
    assert report["missing_components"] == []
    assert report["unexpected_components"] == ["unexpected_component"]


def test_build_component_datasets_is_deterministic_and_valid_for_core_metric_payload() -> None:
    core_rows = [
        {
            "fact_id": "fact:1",
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "metric_family": "funded",
            "metric_name": "funded_ratio",
            "evidence_refs": ["p.40", "p.41"],
            "source_document_id": "doc:ca:2024:v1",
        },
        {
            "fact_id": "fact:2",
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "metric_family": "fee",
            "metric_name": "management_fee_rate",
            "evidence_refs": ["p.61"],
            "source_document_id": "doc:ca:2024:v1",
        },
    ]
    relationship_rows = [
        {
            "relationship_id": "rel:1",
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "manager_name": "Manager A",
            "fund_name": "Fund A",
            "evidence_refs": ["p.58"],
            "source_document_id": "doc:ca:2024:v1",
        }
    ]
    warning_rows = [
        {
            "warning_id": "warn:1",
            "warning_domain": "funded_actuarial",
            "evidence_refs": ["p.40"],
        }
    ]

    datasets = build_component_datasets(
        persisted_core_metrics=core_rows,
        relationship_rows=relationship_rows,
        warning_rows=warning_rows,
        plan_id="CA-PERS",
        plan_period="FY2024",
        effective_date="2024-06-30",
        ingestion_date="2026-03-03",
        source_document_id="doc:ca:2024:v1",
    )
    report = validate_component_coverage(component_datasets=datasets)

    assert report["is_valid"] is True
    assert report["missing_components"] == []
    assert report["status_counts"]["present"] >= 1
    assert datasets == build_component_datasets(
        persisted_core_metrics=core_rows,
        relationship_rows=relationship_rows,
        warning_rows=warning_rows,
        plan_id="CA-PERS",
        plan_period="FY2024",
        effective_date="2024-06-30",
        ingestion_date="2026-03-03",
        source_document_id="doc:ca:2024:v1",
    )


def test_validator_rejects_mixed_status_rows_for_a_component() -> None:
    payload = {
        component: [
            {
                "component_name": component,
                "status": "not_disclosed",
                "row_count": 0,
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "effective_date": "2024-06-30",
                "ingestion_date": "2026-03-03",
                "source_document_id": "doc:1",
                "confidence": None,
                "evidence_refs": [],
                "notes": "synthetic",
            }
        ]
        for component in CORE_SCHEMA_COMPONENTS
    }
    payload["metric_observation"] = [
        {
            "component_name": "metric_observation",
            "status": "present",
            "row_count": 1,
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "effective_date": "2024-06-30",
            "ingestion_date": "2026-03-03",
            "source_document_id": "doc:1",
            "confidence": 1.0,
            "evidence_refs": ["p.1"],
            "notes": "synthetic",
        },
        {
            "component_name": "metric_observation",
            "status": "partial",
            "row_count": 1,
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "effective_date": "2024-06-30",
            "ingestion_date": "2026-03-03",
            "source_document_id": "doc:1",
            "confidence": 0.5,
            "evidence_refs": ["p.1"],
            "notes": "synthetic",
        },
    ]

    report = validate_component_coverage(component_datasets=payload)

    assert report["is_valid"] is False
    assert any(
        row["component"] == "metric_observation" and "single status value" in row["message"]
        for row in report["invalid_state_rows"]
    )


def test_build_component_coverage_report_from_manifest_reads_one_pdf_artifacts(
    tmp_path: Path,
) -> None:
    component_dir = tmp_path / "component_datasets"
    component_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = tmp_path / "component_datasets_manifest.json"

    manifest_payload: dict[str, str] = {}
    for component in CORE_SCHEMA_COMPONENTS:
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
        manifest_payload[component] = str(component_path.relative_to(tmp_path))

    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    report = build_component_coverage_report_from_manifest(
        component_manifest_path=manifest_path,
        run_id="pilot-run",
    )

    assert report["is_valid"] is True
    assert report["run_id"] == "pilot-run"
    per_component = report["per_component"]
    assert isinstance(per_component, list)
    assert len(per_component) == 19
    metric_row = [row for row in per_component if row["component_name"] == "metric_observation"][0]
    assert metric_row["status"] == "present"
    assert metric_row["row_count"] == 3


def test_component_coverage_report_is_diffable_across_artifact_roots(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_component_dir = first_root / "component_datasets"
    second_component_dir = second_root / "component_datasets"
    first_component_dir.mkdir(parents=True, exist_ok=True)
    second_component_dir.mkdir(parents=True, exist_ok=True)
    first_manifest_path = first_root / "component_datasets_manifest.json"
    second_manifest_path = second_root / "component_datasets_manifest.json"

    first_manifest_payload: dict[str, str] = {}
    second_manifest_payload: dict[str, str] = {}
    for component in CORE_SCHEMA_COMPONENTS:
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
        first_component_path = first_component_dir / f"{component}.json"
        second_component_path = second_component_dir / f"{component}.json"
        payload = json.dumps([row], indent=2)
        first_component_path.write_text(payload, encoding="utf-8")
        second_component_path.write_text(payload, encoding="utf-8")
        first_manifest_payload[component] = str(first_component_path.relative_to(first_root))
        second_manifest_payload[component] = str(second_component_path.relative_to(second_root))

    first_manifest_path.write_text(json.dumps(first_manifest_payload, indent=2), encoding="utf-8")
    second_manifest_path.write_text(json.dumps(second_manifest_payload, indent=2), encoding="utf-8")
    first_report = build_component_coverage_report_from_manifest(
        component_manifest_path=first_manifest_path
    )
    second_report = build_component_coverage_report_from_manifest(
        component_manifest_path=second_manifest_path
    )

    assert first_report == second_report
