"""Tests for source-quality schema and validation gates."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from pension_data.sources.discovery import classify_official_resolution, discovery_resolution_rows
from pension_data.sources.schema import SourceMapRecord
from pension_data.sources.validate import (
    SourceValidationError,
    validate_source_map,
    validate_source_map_record,
)


def _valid_record() -> SourceMapRecord:
    return SourceMapRecord(
        plan_id="CA-PERS",
        plan_period="FY2024",
        cohort="state",
        source_url="https://example.gov/acfr.pdf",
        source_authority_tier="official",
        official_resolution_state="available_official",
        expected_plan_identity="CA-PERS",
        observed_plan_identity=None,
        mismatch_reason=None,
    )


def test_validate_source_map_record_requires_valid_authority_tier() -> None:
    record = replace(_valid_record(), source_authority_tier="high-confidence-third-party")
    errors = validate_source_map_record(record)
    assert "available_official requires source_authority_tier" in "\n".join(errors)


def test_validate_source_map_record_rejects_unknown_mismatch_reason() -> None:
    record = replace(
        _valid_record(),
        official_resolution_state="not_found",
        mismatch_reason=cast(Any, "unknown"),
    )
    errors = validate_source_map_record(record)
    assert "invalid mismatch_reason 'unknown'" in "\n".join(errors)


def test_validate_source_map_record_requires_wrong_plan_identity_delta() -> None:
    same_identity = replace(
        _valid_record(),
        official_resolution_state="available_non_official_only",
        source_authority_tier="high-confidence-third-party",
        mismatch_reason="wrong_plan",
        observed_plan_identity="CA-PERS",
    )
    errors = validate_source_map_record(same_identity)
    assert "wrong_plan mismatch requires different expected/observed identities" in "\n".join(errors)


def test_validate_source_map_raises_for_invalid_batch() -> None:
    valid = _valid_record()
    invalid = replace(valid, official_resolution_state="available_non_official_only")
    with pytest.raises(SourceValidationError):
        validate_source_map([valid, invalid])


@pytest.mark.parametrize(
    ("has_official_source", "has_non_official_source", "expected"),
    [
        (True, True, "available_official"),
        (False, True, "available_non_official_only"),
        (False, False, "not_found"),
    ],
)
def test_classify_official_resolution(
    *,
    has_official_source: bool,
    has_non_official_source: bool,
    expected: str,
) -> None:
    assert (
        classify_official_resolution(
            has_official_source=has_official_source,
            has_non_official_source=has_non_official_source,
        )
        == expected
    )


def test_discovery_output_includes_resolution_state_per_plan_period() -> None:
    rows = discovery_resolution_rows(
        [
            _valid_record(),
            replace(
                _valid_record(),
                plan_id="TX-ERS",
                plan_period="FY2023",
                official_resolution_state="available_non_official_only",
                source_authority_tier="high-confidence-third-party",
            ),
        ]
    )
    assert rows == [
        {
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "cohort": "state",
            "official_resolution_state": "available_official",
        },
        {
            "plan_id": "TX-ERS",
            "plan_period": "FY2023",
            "cohort": "state",
            "official_resolution_state": "available_non_official_only",
        },
    ]
