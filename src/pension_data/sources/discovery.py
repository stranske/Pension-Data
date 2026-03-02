"""Discovery output helpers for official source-resolution reporting."""

from __future__ import annotations

from pension_data.sources.schema import OfficialResolutionState, SourceMapRecord


def classify_official_resolution(
    *,
    has_official_source: bool,
    has_non_official_source: bool,
) -> OfficialResolutionState:
    """Classify source-resolution status for a plan-period."""
    if has_official_source:
        return "available_official"
    if has_non_official_source:
        return "available_non_official_only"
    return "not_found"


def discovery_resolution_rows(records: list[SourceMapRecord]) -> list[dict[str, str]]:
    """Project discovery output rows with official source resolution state."""
    rows = [
        {
            "plan_id": record.plan_id,
            "plan_period": record.plan_period,
            "cohort": record.cohort,
            "official_resolution_state": record.official_resolution_state,
        }
        for record in records
    ]
    return sorted(rows, key=lambda row: (row["cohort"], row["plan_id"], row["plan_period"]))
