"""Local, provenance-preserving Form 5500 Schedule SB/MB fixture adapter."""

from __future__ import annotations

import csv
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from pension_data.entities.models import SponsorPlanKey

SUPPORTED_SCHEDULES = frozenset({"SB", "MB"})


@dataclass(frozen=True, slots=True)
class Form5500ScheduleRecord:
    """One local Schedule SB/MB row, retaining the source row for auditability."""

    sponsor_plan_key: SponsorPlanKey
    filing_year: int
    schedule: str
    source_row: int
    source_document_id: str
    raw_row: tuple[tuple[str, str], ...]

    @property
    def evidence_refs(self) -> tuple[str, ...]:
        return (
            f"{self.source_document_id}#row={self.source_row}",
            f"{self.source_document_id}#schedule={self.schedule}",
        )


def parse_form5500_schedule_rows(
    rows: Iterable[dict[str, str]], *, source_document_id: str
) -> list[Form5500ScheduleRecord]:
    """Parse local fixture rows only; invalid or unsupported rows fail explicitly."""
    parsed: list[Form5500ScheduleRecord] = []
    for source_row, row in enumerate(rows, start=2):
        schedule = row.get("schedule", "").strip().upper()
        if schedule not in SUPPORTED_SCHEDULES:
            raise ValueError(f"row {source_row}: schedule must be SB or MB")
        try:
            filing_year = int(row.get("filing_year", ""))
        except ValueError as error:
            raise ValueError(f"row {source_row}: filing_year must be an integer") from error
        parsed.append(
            Form5500ScheduleRecord(
                sponsor_plan_key=SponsorPlanKey(
                    ein=row.get("ein", ""), plan_number=row.get("plan_number", "")
                ),
                filing_year=filing_year,
                schedule=schedule,
                source_row=source_row,
                source_document_id=source_document_id,
                raw_row=tuple(sorted((key, value) for key, value in row.items())),
            )
        )
    return sorted(
        parsed,
        key=lambda item: (
            item.sponsor_plan_key.ein,
            item.sponsor_plan_key.plan_number,
            item.filing_year,
            item.schedule,
            item.source_row,
        ),
    )


def load_form5500_schedule_fixture(path: Path) -> list[Form5500ScheduleRecord]:
    """Load a CSV fixture without any network access."""
    with path.open(newline="", encoding="utf-8") as handle:
        return parse_form5500_schedule_rows(
            csv.DictReader(handle), source_document_id=f"fixture:{path.name}"
        )
