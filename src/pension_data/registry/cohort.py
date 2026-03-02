"""Cohort selectors for v1 pension-system coverage sets."""

from __future__ import annotations

from pension_data.db.models.registry import PensionSystemRecord


def filter_v1_cohort(
    records: list[PensionSystemRecord],
    *,
    state_employee_only: bool = False,
    sampled_50_only: bool = False,
) -> list[PensionSystemRecord]:
    """Filter records to deterministic v1 cohort slices."""
    filtered = records
    if state_employee_only:
        filtered = [record for record in filtered if record.in_state_employee_universe]
    if sampled_50_only:
        filtered = [record for record in filtered if record.in_sampled_50]
    return sorted(filtered, key=lambda record: record.stable_id)
