"""Discovery inventory survey outputs for annual reports and side-survey counts."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from pension_data.db.models.inventory import (
    AnnualReportCoverageRecord,
    DiscoveredInventoryRecord,
    InventoryDocumentType,
)
from pension_data.registry.system_type_lookup import load_system_type_by_plan_id
from pension_data.sources.schema import (
    OfficialResolutionState,
    SourceAuthorityTier,
    SourceMapRecord,
)

_YEAR_PATTERN = re.compile(r"(?:19|20)\d{2}")
_YEAR_RANGE_PATTERN = re.compile(r"((?:19|20)\d{2})\s*[-/]\s*((?:19|20)?\d{2})")

_ANNUAL_REPORT_HINTS = (
    "annual report",
    "acfr",
    "comprehensive annual financial report",
)
_BOARD_PACKET_HINTS = ("board packet", "board meeting", "trustee packet")
_ALM_HINTS = ("asset liability", "asset/liability", "alm study", "asset liability study")
_CONSULTANT_HINTS = ("consultant report", "consultant", "investment consultant")
_MANAGER_DISCLOSURE_HINTS = ("manager", "managers", "holding", "holdings")
_CONSULTANT_DISCLOSURE_HINTS = ("consultant report", "investment consultant")
_OFFICIAL_TIERS: tuple[SourceAuthorityTier, ...] = ("official", "official-mirror")

_RESOLUTION_PRIORITY: dict[OfficialResolutionState, int] = {
    "not_found": 0,
    "available_non_official_only": 1,
    "available_official": 2,
}


@dataclass(frozen=True, slots=True)
class DiscoveredDocumentInput:
    """Raw discovered document metadata used for inventory classification."""

    plan_id: str
    source_url: str
    title: str
    source_authority_tier: SourceAuthorityTier = "high-confidence-third-party"


def _parse_year_token(*, start_year: int, end_token: str) -> int:
    if len(end_token) == 4:
        return int(end_token)
    end_year = (start_year // 100) * 100 + int(end_token)
    if end_year < start_year:
        end_year += 100
    return end_year


def _parse_year_from_plan_period(plan_period: str) -> int | None:
    matches = [int(token) for token in _YEAR_PATTERN.findall(plan_period)]
    return max(matches) if matches else None


def detect_report_year(*, title: str, source_url: str) -> int | None:
    """Detect report year from title/url text using deterministic range/year heuristics."""
    haystack = f"{title} {source_url}"
    matches = [int(token) for token in _YEAR_PATTERN.findall(haystack)]
    for range_match in _YEAR_RANGE_PATTERN.finditer(haystack):
        start_year = int(range_match.group(1))
        end_token = range_match.group(2)
        matches.append(_parse_year_token(start_year=start_year, end_token=end_token))
    return max(matches) if matches else None


def _has_any_hint(*, text: str, hints: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(hint in lowered for hint in hints)


def classify_document_type(*, title: str, source_url: str) -> InventoryDocumentType:
    """Classify discovered document type for inventory-only side survey counts."""
    text = f"{title} {source_url}"
    if _has_any_hint(text=text, hints=_ANNUAL_REPORT_HINTS):
        return "annual_report"
    if _has_any_hint(text=text, hints=_BOARD_PACKET_HINTS):
        return "board_packet"
    if _has_any_hint(text=text, hints=_ALM_HINTS):
        return "alm_study"
    if _has_any_hint(text=text, hints=_CONSULTANT_HINTS):
        return "consultant_report"
    return "other"


def _is_manager_disclosure_available(*, title: str, source_url: str) -> bool:
    return _has_any_hint(text=f"{title} {source_url}", hints=_MANAGER_DISCLOSURE_HINTS)


def _is_consultant_disclosure_available(*, title: str, source_url: str) -> bool:
    return _has_any_hint(text=f"{title} {source_url}", hints=_CONSULTANT_DISCLOSURE_HINTS)


def _select_resolution_record(records: list[SourceMapRecord]) -> SourceMapRecord:
    return sorted(
        records,
        key=lambda row: (
            _RESOLUTION_PRIORITY[row.official_resolution_state],
            row.source_authority_tier in ("official", "official-mirror"),
            row.source_url,
        ),
        reverse=True,
    )[0]


def _resolve_system_type(
    *,
    plan_id: str,
    cohort: str,
    system_type_by_plan_id: Mapping[str, str],
) -> str:
    return system_type_by_plan_id.get(plan_id.strip().lower(), cohort)


def _resolution_from_discovered_document(
    document: DiscoveredInventoryRecord,
) -> OfficialResolutionState:
    if document.source_authority_tier in _OFFICIAL_TIERS:
        return "available_official"
    return "available_non_official_only"


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, *, rows: list[dict[str, object]], fieldnames: tuple[str, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serialized_row: dict[str, object] = {}
            for field in fieldnames:
                value = row.get(field)
                if value is None:
                    serialized_row[field] = ""
                elif isinstance(value, (dict, list)):
                    serialized_row[field] = json.dumps(value, sort_keys=True)
                else:
                    serialized_row[field] = value
            writer.writerow(serialized_row)


def build_inventory_artifacts(
    *,
    source_records: list[SourceMapRecord],
    discovered_documents: list[DiscoveredDocumentInput],
    target_years: tuple[int, ...] | None = None,
    system_type_by_plan_id: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Build deterministic discovery inventory rows, plan-year coverage, and summaries."""
    system_type_lookup = (
        {key.lower(): value for key, value in load_system_type_by_plan_id().items()}
        if system_type_by_plan_id is None
        else {key.lower(): value for key, value in system_type_by_plan_id.items()}
    )
    discovered_rows: list[DiscoveredInventoryRecord] = []
    for document in sorted(
        discovered_documents,
        key=lambda row: (row.plan_id, row.source_url, row.title.lower()),
    ):
        plan_year = detect_report_year(title=document.title, source_url=document.source_url)
        document_type = classify_document_type(title=document.title, source_url=document.source_url)
        manager_flag = _is_manager_disclosure_available(
            title=document.title, source_url=document.source_url
        )
        consultant_flag = _is_consultant_disclosure_available(
            title=document.title, source_url=document.source_url
        )
        discovered_rows.append(
            DiscoveredInventoryRecord(
                plan_id=document.plan_id,
                plan_year=plan_year,
                document_type=document_type,
                source_url=document.source_url,
                source_authority_tier=document.source_authority_tier,
                manager_disclosure_available=manager_flag,
                consultant_disclosure_available=consultant_flag,
                detection_metadata={
                    "title": document.title.strip(),
                    "year_detection": "title_url_pattern",
                },
            )
        )

    known_years = {
        year
        for year in (
            *(_parse_year_from_plan_period(row.plan_period) for row in source_records),
            *(row.plan_year for row in discovered_rows),
        )
        if year is not None
    }
    if target_years is None:
        anchor_year = max(known_years) if known_years else 2025
        target_years = tuple(range(anchor_year - 4, anchor_year + 1))
    ordered_target_years = tuple(sorted(dict.fromkeys(target_years)))

    coverage_by_plan_year: dict[tuple[str, int], list[SourceMapRecord]] = defaultdict(list)
    cohort_by_plan: dict[str, str] = {}
    for source_row in source_records:
        parsed_year = _parse_year_from_plan_period(source_row.plan_period)
        if parsed_year is None:
            continue
        coverage_by_plan_year[(source_row.plan_id, parsed_year)].append(source_row)
        cohort_by_plan.setdefault(source_row.plan_id, source_row.cohort)

    manager_disclosure_by_plan: dict[str, bool] = defaultdict(bool)
    consultant_disclosure_by_plan: dict[str, bool] = defaultdict(bool)
    for row in discovered_rows:
        manager_disclosure_by_plan[row.plan_id] = (
            manager_disclosure_by_plan[row.plan_id] or row.manager_disclosure_available
        )
        consultant_disclosure_by_plan[row.plan_id] = (
            consultant_disclosure_by_plan[row.plan_id] or row.consultant_disclosure_available
        )

    discovered_annual_reports_by_plan_year: dict[
        tuple[str, int], list[DiscoveredInventoryRecord]
    ] = defaultdict(list)
    for row in discovered_rows:
        if row.document_type != "annual_report" or row.plan_year is None:
            continue
        discovered_annual_reports_by_plan_year[(row.plan_id, row.plan_year)].append(row)

    all_plan_ids = sorted(
        {
            *cohort_by_plan.keys(),
            *(row.plan_id for row in discovered_rows),
        }
    )
    coverage_rows: list[AnnualReportCoverageRecord] = []
    for plan_id in all_plan_ids:
        cohort = cohort_by_plan.get(plan_id, "unknown")
        for plan_year in ordered_target_years:
            records = coverage_by_plan_year.get((plan_id, plan_year), [])
            if records:
                selected = _select_resolution_record(records)
                official_resolution_state = selected.official_resolution_state
                annual_report_source_url = selected.source_url
            elif discovered_annual_reports_by_plan_year.get((plan_id, plan_year)):
                selected_document = sorted(
                    discovered_annual_reports_by_plan_year[(plan_id, plan_year)],
                    key=lambda row: (
                        row.source_authority_tier in _OFFICIAL_TIERS,
                        row.source_url,
                    ),
                    reverse=True,
                )[0]
                official_resolution_state = _resolution_from_discovered_document(selected_document)
                annual_report_source_url = selected_document.source_url
            else:
                official_resolution_state = "not_found"
                annual_report_source_url = "not_found"
            system_type = _resolve_system_type(
                plan_id=plan_id,
                cohort=cohort,
                system_type_by_plan_id=system_type_lookup,
            )
            coverage_rows.append(
                AnnualReportCoverageRecord(
                    plan_id=plan_id,
                    plan_year=plan_year,
                    cohort=cohort,
                    system_type=system_type,
                    official_resolution_state=official_resolution_state,
                    annual_report_source_url=annual_report_source_url,
                    manager_disclosure_available=manager_disclosure_by_plan[plan_id],
                    consultant_disclosure_available=consultant_disclosure_by_plan[plan_id],
                )
            )

    summary_by_system: list[dict[str, object]] = []
    document_counts: dict[str, dict[InventoryDocumentType, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for discovered_row in discovered_rows:
        document_counts[discovered_row.plan_id][discovered_row.document_type] += 1

    for plan_id in all_plan_ids:
        counts = document_counts[plan_id]
        summary_by_system.append(
            {
                "plan_id": plan_id,
                "cohort": cohort_by_plan.get(plan_id, "unknown"),
                "system_type": _resolve_system_type(
                    plan_id=plan_id,
                    cohort=cohort_by_plan.get(plan_id, "unknown"),
                    system_type_by_plan_id=system_type_lookup,
                ),
                "annual_report_count": counts["annual_report"],
                "board_packet_count": counts["board_packet"],
                "alm_study_count": counts["alm_study"],
                "consultant_report_count": counts["consultant_report"],
                "other_document_count": counts["other"],
                "manager_disclosure_available": manager_disclosure_by_plan[plan_id],
                "consultant_disclosure_available": consultant_disclosure_by_plan[plan_id],
            }
        )

    return {
        "inventory_rows": [asdict(row) for row in discovered_rows],
        "annual_report_coverage_rows": [asdict(row) for row in coverage_rows],
        "summary_by_system": summary_by_system,
        "target_year_window": list(ordered_target_years),
    }


def write_inventory_artifacts(artifacts: dict[str, object], *, output_root: Path) -> dict[str, str]:
    """Write inventory run artifacts under `artifacts/inventory` as JSON and CSV."""
    inventory_rows = artifacts.get("inventory_rows")
    annual_report_coverage_rows = artifacts.get("annual_report_coverage_rows")
    summary_by_system = artifacts.get("summary_by_system")
    if not isinstance(inventory_rows, list):
        raise ValueError("artifacts['inventory_rows'] must be a list")
    if not isinstance(annual_report_coverage_rows, list):
        raise ValueError("artifacts['annual_report_coverage_rows'] must be a list")
    if not isinstance(summary_by_system, list):
        raise ValueError("artifacts['summary_by_system'] must be a list")

    inventory_dir = output_root / "inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)

    inventory_json = inventory_dir / "inventory_rows.json"
    inventory_csv = inventory_dir / "inventory_rows.csv"
    coverage_json = inventory_dir / "annual_report_coverage_rows.json"
    coverage_csv = inventory_dir / "annual_report_coverage_rows.csv"
    summary_json = inventory_dir / "summary_by_system.json"
    summary_csv = inventory_dir / "summary_by_system.csv"

    _write_json(inventory_json, inventory_rows)
    _write_json(coverage_json, annual_report_coverage_rows)
    _write_json(summary_json, summary_by_system)
    _write_csv(
        inventory_csv,
        rows=inventory_rows,
        fieldnames=(
            "plan_id",
            "plan_year",
            "document_type",
            "source_url",
            "source_authority_tier",
            "manager_disclosure_available",
            "consultant_disclosure_available",
            "detection_metadata",
        ),
    )
    _write_csv(
        coverage_csv,
        rows=annual_report_coverage_rows,
        fieldnames=(
            "plan_id",
            "plan_year",
            "cohort",
            "system_type",
            "official_resolution_state",
            "annual_report_source_url",
            "manager_disclosure_available",
            "consultant_disclosure_available",
        ),
    )
    _write_csv(
        summary_csv,
        rows=summary_by_system,
        fieldnames=(
            "plan_id",
            "cohort",
            "system_type",
            "annual_report_count",
            "board_packet_count",
            "alm_study_count",
            "consultant_report_count",
            "other_document_count",
            "manager_disclosure_available",
            "consultant_disclosure_available",
        ),
    )

    return {
        "inventory_rows_json": str(inventory_json),
        "inventory_rows_csv": str(inventory_csv),
        "annual_report_coverage_rows_json": str(coverage_json),
        "annual_report_coverage_rows_csv": str(coverage_csv),
        "summary_by_system_json": str(summary_json),
        "summary_by_system_csv": str(summary_csv),
    }
