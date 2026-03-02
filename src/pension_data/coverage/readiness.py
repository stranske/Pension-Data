"""Extraction readiness artifacts and deterministic quality summaries."""

from __future__ import annotations

from collections import defaultdict
from typing import Literal

from pension_data.sources.schema import SourceMapRecord

ReadinessState = Literal["ready", "blocked_source", "blocked_quality"]


def derive_readiness_state(record: SourceMapRecord) -> ReadinessState:
    """Map source-quality signals to extraction-readiness status."""
    if record.official_resolution_state in {"not_found", "available_non_official_only"}:
        return "blocked_source"
    if record.mismatch_reason in {"wrong_plan", "stale_period", "non_official_only"}:
        return "blocked_quality"
    return "ready"


def build_readiness_artifacts(records: list[SourceMapRecord]) -> dict[str, object]:
    """Build machine-readable readiness rows and deterministic cohort summaries."""
    readiness_rows = [
        {
            "plan_id": record.plan_id,
            "plan_period": record.plan_period,
            "cohort": record.cohort,
            "official_resolution_state": record.official_resolution_state,
            "source_authority_tier": record.source_authority_tier,
            "mismatch_reason": record.mismatch_reason or "",
            "readiness_state": derive_readiness_state(record),
        }
        for record in records
    ]
    readiness_rows.sort(key=lambda row: (row["cohort"], row["plan_id"], row["plan_period"]))

    totals_by_cohort: defaultdict[str, int] = defaultdict(int)
    unresolved_official_by_cohort: defaultdict[str, int] = defaultdict(int)
    mismatches_by_cohort: defaultdict[str, int] = defaultdict(int)
    stale_period_by_cohort: defaultdict[str, int] = defaultdict(int)

    for record in records:
        cohort = record.cohort
        totals_by_cohort[cohort] += 1
        if record.official_resolution_state != "available_official":
            unresolved_official_by_cohort[cohort] += 1
        if record.mismatch_reason is not None:
            mismatches_by_cohort[cohort] += 1
        if record.mismatch_reason == "stale_period":
            stale_period_by_cohort[cohort] += 1

    cohorts = sorted(totals_by_cohort.keys())
    summary_rows: list[dict[str, float | int | str]] = []
    for cohort in cohorts:
        total = totals_by_cohort[cohort]
        unresolved = unresolved_official_by_cohort[cohort]
        mismatches = mismatches_by_cohort[cohort]
        stale_period = stale_period_by_cohort[cohort]
        summary_rows.append(
            {
                "cohort": cohort,
                "total_plan_periods": total,
                "unresolved_official_count": unresolved,
                "mismatch_count": mismatches,
                "unresolved_official_rate": round(unresolved / total, 6),
                "mismatch_rate": round(mismatches / total, 6),
                "stale_period_rate": round(stale_period / total, 6),
            }
        )

    return {
        "readiness_rows": readiness_rows,
        "summary_by_cohort": summary_rows,
    }
