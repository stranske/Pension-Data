"""Local Form 5500 sponsor-plan join contract tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.entities.lookup_service import join_form5500_records
from pension_data.entities.models import SponsorPlanKey
from pension_data.sources.form5500 import (
    load_form5500_schedule_fixture,
    parse_form5500_schedule_rows,
)


def _records():
    return load_form5500_schedule_fixture(
        Path(__file__).parent / "fixtures" / "form5500" / "schedule_sb_mb.csv"
    )


def test_form5500_join_normalizes_ein_plan_and_retains_provenance() -> None:
    results = join_form5500_records(
        _records(),
        sponsor_plan_entities={SponsorPlanKey("123456789", "001"): "plan:alpha"},
    )
    assert results[0].status == "matched"
    assert results[0].canonical_entity_id == "plan:alpha"
    assert results[0].record.evidence_refs == (
        "fixture:schedule_sb_mb.csv#row=2",
        "fixture:schedule_sb_mb.csv#schedule=SB",
    )
    assert results[1].status == "review"


def test_form5500_join_never_auto_merges_ambiguous_sponsors() -> None:
    results = join_form5500_records(
        _records()[:1],
        sponsor_plan_entities={SponsorPlanKey("123456789", "1"): ("plan:alpha", "plan:beta")},
    )
    assert results[0].status == "review"
    assert results[0].canonical_entity_id is None
    assert results[0].reason == "ambiguous sponsor-plan key"


def test_form5500_schedule_rejects_unsupported_schedule() -> None:
    with pytest.raises(ValueError, match="schedule must be SB or MB"):
        parse_form5500_schedule_rows(
            [{"ein": "12-3456789", "plan_number": "1", "filing_year": "2024", "schedule": "H"}],
            source_document_id="fixture:bad.csv",
        )
