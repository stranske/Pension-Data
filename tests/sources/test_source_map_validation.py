"""Tests for source-map schema validation, overrides, and linting."""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.sources.lint import main as lint_main
from pension_data.sources.validate import (
    SourceValidationError,
    assert_valid_source_map_entries,
    normalize_url,
    parse_source_map_rows,
    validate_source_map_entries,
)


def _valid_row() -> dict[str, str]:
    return {
        "plan_id": "ca-calpers",
        "plan_name": "California Public Employees' Retirement System",
        "expected_plan_identity": "ca-calpers",
        "seed_url": "https://www.calpers.ca.gov/reports",
        "allowed_domains": "calpers.ca.gov",
        "doc_type_hints": "annual_report;investment_report",
        "pagination_hints": "next_link",
        "max_pages": "20",
        "max_depth": "3",
        "source_authority_tier": "official",
        "mismatch_reason": "",
        "override_requires_js": "",
        "override_force_render_wait_ms": "",
        "override_pagination_mode": "",
        "override_notes": "",
    }


def test_malformed_maps_fail_validation() -> None:
    row = _valid_row()
    row["seed_url"] = "mailto:not-valid"
    row["allowed_domains"] = "invalid-domain"
    findings = validate_source_map_entries(parse_source_map_rows([row]))
    assert {finding.code for finding in findings} == {"invalid_allowed_domain", "invalid_url"}


def test_invalid_doc_type_hint_fails_validation() -> None:
    row = _valid_row()
    row["doc_type_hints"] = "annual_report;actuarial_valuation"
    findings = validate_source_map_entries(parse_source_map_rows([row]))
    assert any(finding.code == "invalid_doc_type_hint" for finding in findings)


def test_invalid_pagination_hint_fails_validation() -> None:
    row = _valid_row()
    row["pagination_hints"] = "cursor"
    findings = validate_source_map_entries(parse_source_map_rows([row]))
    assert any(finding.code == "invalid_pagination_hint" for finding in findings)


def test_duplicate_seed_urls_are_detected_after_normalization() -> None:
    row_a = _valid_row()
    row_b = _valid_row()
    row_b["seed_url"] = "https://www.calpers.ca.gov/reports/"
    row_b["source_authority_tier"] = "official_mirror"
    findings = validate_source_map_entries(parse_source_map_rows([row_a, row_b]))
    assert any(finding.code == "duplicate_seed_url" for finding in findings)


def test_conflicting_urls_across_plan_ids_are_detected() -> None:
    row_a = _valid_row()
    row_b = _valid_row()
    row_b["plan_id"] = "ny-nyslrs"
    row_b["expected_plan_identity"] = "ny-nyslrs"
    findings = validate_source_map_entries(parse_source_map_rows([row_a, row_b]))
    assert any(finding.code == "conflicting_seed_url" for finding in findings)


def test_annual_report_row_requires_authority_tier() -> None:
    row = _valid_row()
    row["source_authority_tier"] = ""
    findings = validate_source_map_entries(parse_source_map_rows([row]))
    assert any(finding.code == "missing_authority_tier" for finding in findings)
    assert all(finding.code != "invalid_authority_tier" for finding in findings)


def test_per_system_override_validation_and_behavior() -> None:
    valid = _valid_row()
    valid["override_requires_js"] = "true"
    valid["override_notes"] = "Known dynamic page"
    entries = parse_source_map_rows([valid])
    findings = validate_source_map_entries(entries)
    assert not findings
    assert entries[0].overrides == (("notes", "Known dynamic page"), ("requires_js", "true"))

    invalid = _valid_row()
    invalid["override_bad_field"] = "unexpected"
    invalid_findings = validate_source_map_entries(parse_source_map_rows([invalid]))
    assert any(finding.code == "invalid_override_key" for finding in invalid_findings)


def test_parse_source_map_rows_handles_non_string_csv_cells() -> None:
    row = _valid_row()
    malformed: dict[str | None, object] = dict(row)
    malformed[None] = ["extra-column"]
    malformed["override_notes"] = ["Known dynamic page", "Secondary note"]
    entries = parse_source_map_rows([malformed])
    assert entries[0].overrides == (("notes", "Known dynamic page;Secondary note"),)


def test_assert_valid_source_map_raises_actionable_error() -> None:
    row = _valid_row()
    row["max_pages"] = "0"
    with pytest.raises(SourceValidationError, match="invalid_crawl_constraints"):
        assert_valid_source_map_entries(parse_source_map_rows([row]))


def test_lint_command_is_deterministic_for_ci_usage(tmp_path: Path) -> None:
    source_map_file = tmp_path / "source_map.csv"
    header = (
        "plan_id,plan_name,expected_plan_identity,seed_url,allowed_domains,doc_type_hints,"
        "pagination_hints,max_pages,max_depth,source_authority_tier,mismatch_reason,"
        "override_requires_js,override_force_render_wait_ms,override_pagination_mode,"
        "override_notes"
    )
    row = (
        "ca-calpers,California Public Employees' Retirement System,ca-calpers,"
        "https://www.calpers.ca.gov/reports,calpers.ca.gov,annual_report,next_link,"
        "10,2,official,,,,"
    )
    source_map_file.write_text(f"{header}\n{row}\n", encoding="utf-8")

    assert lint_main([str(source_map_file)]) == 0


def test_url_normalization_preserves_query_parameters() -> None:
    normalized_a = normalize_url("https://example.gov/reports?year=2023")
    normalized_b = normalize_url("https://example.gov/reports?year=2024")
    assert normalized_a != normalized_b


def test_lint_command_handles_invalid_inputs_with_nonzero_exit(tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    assert lint_main([str(missing)]) == 1
    assert lint_main([str(tmp_path)]) == 1

    bad_numeric = tmp_path / "bad_numeric.csv"
    bad_numeric.write_text(
        "\n".join(
            [
                (
                    "plan_id,plan_name,expected_plan_identity,seed_url,allowed_domains,"
                    "doc_type_hints,pagination_hints,max_pages,max_depth,"
                    "source_authority_tier,mismatch_reason"
                ),
                (
                    "ca-calpers,California Public Employees' Retirement System,ca-calpers,"
                    "https://www.calpers.ca.gov/reports,calpers.ca.gov,annual_report,"
                    "next_link,abc,2,official,"
                ),
            ]
        ),
        encoding="utf-8",
    )
    assert lint_main([str(bad_numeric)]) == 1

    missing_required_header = tmp_path / "missing_required_header.csv"
    missing_required_header.write_text(
        "\n".join(
            [
                (
                    "plan_id,plan_name,expected_plan_identity,seed_url,allowed_domains,"
                    "doc_type_hints,pagination_hints,max_pages,source_authority_tier,"
                    "mismatch_reason"
                ),
                (
                    "ca-calpers,California Public Employees' Retirement System,ca-calpers,"
                    "https://www.calpers.ca.gov/reports,calpers.ca.gov,annual_report,"
                    "next_link,10,official,"
                ),
            ]
        ),
        encoding="utf-8",
    )
    assert lint_main([str(missing_required_header)]) == 1
