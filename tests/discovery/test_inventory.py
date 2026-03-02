"""Tests for discovery inventory survey and annual-report coverage outputs."""

from __future__ import annotations

from pathlib import Path

from pension_data.discovery.inventory import (
    DiscoveredDocumentInput,
    build_inventory_artifacts,
    classify_document_type,
    detect_report_year,
    write_inventory_artifacts,
)
from pension_data.sources.schema import SourceMapRecord


def _source_records() -> list[SourceMapRecord]:
    return [
        SourceMapRecord(
            plan_id="CA-PERS",
            plan_period="FY2024",
            cohort="state",
            source_url="https://example.gov/ca-2024-acfr.pdf",
            source_authority_tier="official",
            official_resolution_state="available_official",
            expected_plan_identity="CA-PERS",
        ),
        SourceMapRecord(
            plan_id="CA-PERS",
            plan_period="FY2023",
            cohort="state",
            source_url="https://example.com/ca-2023-third-party.pdf",
            source_authority_tier="high-confidence-third-party",
            official_resolution_state="available_non_official_only",
            expected_plan_identity="CA-PERS",
            mismatch_reason="non_official_only",
        ),
        SourceMapRecord(
            plan_id="TX-ERS",
            plan_period="FY2024",
            cohort="state",
            source_url="https://example.gov/tx-2024-acfr.pdf",
            source_authority_tier="official",
            official_resolution_state="available_official",
            expected_plan_identity="TX-ERS",
        ),
    ]


def _discovered_documents() -> list[DiscoveredDocumentInput]:
    return [
        DiscoveredDocumentInput(
            plan_id="CA-PERS",
            source_url="https://example.gov/docs/FY2024-annual-report.pdf",
            title="FY2024 Annual Report",
            source_authority_tier="official",
        ),
        DiscoveredDocumentInput(
            plan_id="CA-PERS",
            source_url="https://example.gov/docs/board-packet-2024-09.pdf",
            title="Board Packet September 2024",
            source_authority_tier="official",
        ),
        DiscoveredDocumentInput(
            plan_id="CA-PERS",
            source_url="https://example.gov/docs/consultant-review-fy2024.pdf",
            title="Investment Consultant Review FY2024",
            source_authority_tier="official",
        ),
        DiscoveredDocumentInput(
            plan_id="CA-PERS",
            source_url="https://example.gov/docs/manager-holdings-fy2024.pdf",
            title="Manager Holdings Schedule FY2024",
            source_authority_tier="official",
        ),
        DiscoveredDocumentInput(
            plan_id="TX-ERS",
            source_url="https://example.gov/docs/2023-24-asset-liability-study.pdf",
            title="Asset Liability Management Study 2023-24",
            source_authority_tier="official",
        ),
    ]


def test_detect_report_year_handles_ranges_and_fallbacks() -> None:
    assert (
        detect_report_year(
            title="Annual Report FY2024",
            source_url="https://example.org/annual-report.pdf",
        )
        == 2024
    )
    assert (
        detect_report_year(
            title="Asset Liability Study 2023-24",
            source_url="https://example.org/alm-study.pdf",
        )
        == 2024
    )
    assert (
        detect_report_year(
            title="Consultant Memo",
            source_url="https://example.org/no-year-here.pdf",
        )
        is None
    )


def test_classify_document_type_detects_known_classes_and_fallbacks() -> None:
    assert classify_document_type(title="Comprehensive Annual Financial Report", source_url="") == (
        "annual_report"
    )
    assert classify_document_type(title="Board Packet - September Meeting", source_url="") == (
        "board_packet"
    )
    assert classify_document_type(title="ALM Study Update", source_url="") == "alm_study"
    assert classify_document_type(title="Investment Consultant Update", source_url="") == (
        "consultant_report"
    )
    assert classify_document_type(title="Quarterly Procurement Notice", source_url="") == "other"


def test_inventory_artifacts_include_coverage_states_and_side_survey_counts() -> None:
    artifacts = build_inventory_artifacts(
        source_records=_source_records(),
        discovered_documents=_discovered_documents(),
        target_years=(2022, 2023, 2024),
    )

    coverage_rows = artifacts["annual_report_coverage_rows"]
    assert len(coverage_rows) == 6
    ca_2024 = [
        row for row in coverage_rows if row["plan_id"] == "CA-PERS" and row["plan_year"] == 2024
    ][0]
    ca_2022 = [
        row for row in coverage_rows if row["plan_id"] == "CA-PERS" and row["plan_year"] == 2022
    ][0]
    assert ca_2024["official_resolution_state"] == "available_official"
    assert ca_2022["official_resolution_state"] == "not_found"
    assert ca_2024["manager_disclosure_available"] is True
    assert ca_2024["consultant_disclosure_available"] is True

    summary_rows = artifacts["summary_by_system"]
    ca_summary = [row for row in summary_rows if row["plan_id"] == "CA-PERS"][0]
    assert ca_summary["annual_report_count"] == 1
    assert ca_summary["board_packet_count"] == 1
    assert ca_summary["consultant_report_count"] == 1
    assert artifacts["target_year_window"] == [2022, 2023, 2024]


def test_inventory_artifacts_are_reproducible_for_same_inputs() -> None:
    first = build_inventory_artifacts(
        source_records=_source_records(),
        discovered_documents=_discovered_documents(),
        target_years=(2022, 2023, 2024),
    )
    second = build_inventory_artifacts(
        source_records=list(reversed(_source_records())),
        discovered_documents=list(reversed(_discovered_documents())),
        target_years=(2022, 2023, 2024),
    )
    assert first == second


def test_write_inventory_artifacts_is_deterministic(tmp_path: Path) -> None:
    artifacts = build_inventory_artifacts(
        source_records=_source_records(),
        discovered_documents=_discovered_documents(),
        target_years=(2022, 2023, 2024),
    )
    first_paths = write_inventory_artifacts(artifacts, output_root=tmp_path / "run-1")
    second_paths = write_inventory_artifacts(artifacts, output_root=tmp_path / "run-2")

    first_contents = {
        name: Path(path).read_text(encoding="utf-8") for name, path in first_paths.items()
    }
    second_contents = {
        name: Path(path).read_text(encoding="utf-8") for name, path in second_paths.items()
    }
    assert first_contents == second_contents
    assert "available_official" in first_contents["annual_report_coverage_rows_json"]
    assert "plan_id,cohort,annual_report_count" in first_contents["summary_by_system_csv"]
