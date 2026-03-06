"""Tests for one-PDF schema component completeness coverage validation."""

from __future__ import annotations

from pension_data.coverage.component_completeness import (
    CORE_SCHEMA_COMPONENTS,
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
        row["component"] == "metric_observation"
        and "single status value" in row["message"]
        for row in report["invalid_state_rows"]
    )
