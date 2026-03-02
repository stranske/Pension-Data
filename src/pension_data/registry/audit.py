"""Audit outputs for pension-system registry completeness and segmentation."""

from __future__ import annotations

from collections import Counter

from pension_data.db.models.registry import PensionSystemRecord


def _cohort_segment(record: PensionSystemRecord) -> str:
    if record.cohort.in_state_employee_universe and record.cohort.in_sampled_50:
        return "state_employee_sampled_50"
    if record.cohort.in_state_employee_universe:
        return "state_employee_only"
    if record.cohort.in_sampled_50:
        return "sampled_50_only"
    return "outside_v1"


def build_registry_audit(records: list[PensionSystemRecord]) -> dict[str, object]:
    """Build deterministic audit counts by type, jurisdiction, and cohort segment."""
    by_type = Counter(record.system_type for record in records)
    by_jurisdiction = Counter(record.jurisdiction for record in records)
    by_segment = Counter(_cohort_segment(record) for record in records)

    return {
        "total_records": len(records),
        "counts_by_system_type": dict(sorted(by_type.items())),
        "counts_by_jurisdiction": dict(sorted(by_jurisdiction.items())),
        "counts_by_cohort_segment": dict(sorted(by_segment.items())),
    }
