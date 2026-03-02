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
        official_resolution_state="available_official",
        source_authority_tier="official",
        mismatch_reason="wrong_plan",
        observed_plan_identity="CA-PERS",
    )
    errors = validate_source_map_record(same_identity)
    assert "wrong_plan mismatch requires different expected/observed identities" in "\n".join(
        errors
    )


def test_validate_source_map_record_allows_wrong_plan_with_available_official() -> None:
    record = replace(
        _valid_record(),
        official_resolution_state="available_official",
        source_authority_tier="official",
        mismatch_reason="wrong_plan",
        observed_plan_identity="CALPERS-ALT",
    )
    assert validate_source_map_record(record) == []


def test_validate_source_map_record_requires_non_official_mismatch_state_alignment() -> None:
    record = replace(
        _valid_record(),
        official_resolution_state="available_non_official_only",
        source_authority_tier="high-confidence-third-party",
        mismatch_reason=None,
    )
    errors = validate_source_map_record(record)
    assert "available_non_official_only requires mismatch_reason of non_official_only" in "\n".join(
        errors
    )


def test_validate_source_map_record_rejects_observed_identity_without_wrong_plan() -> None:
    record = replace(
        _valid_record(),
        mismatch_reason="stale_period",
        observed_plan_identity="CA-PERS-MISMATCH",
    )
    errors = validate_source_map_record(record)
    assert "observed_plan_identity is only valid when mismatch_reason is wrong_plan" in "\n".join(
        errors
    )


def test_validate_source_map_record_requires_stale_period_to_use_available_official() -> None:
    record = replace(
        _valid_record(),
        official_resolution_state="not_found",
        source_authority_tier="high-confidence-third-party",
        mismatch_reason="stale_period",
    )
    errors = validate_source_map_record(record)
    assert (
        "stale_period mismatch requires official_resolution_state of available_official"
        in "\n".join(errors)
    )


def test_validate_source_map_raises_for_invalid_batch() -> None:
    valid = _valid_record()
    invalid = replace(valid, official_resolution_state="available_non_official_only")
    with pytest.raises(SourceValidationError):
        validate_source_map([valid, invalid])


def test_validate_source_map_record_accepts_known_system_overrides() -> None:
    record = replace(
        _valid_record(),
        system_overrides={
            "requires_javascript": True,
            "pagination_mode": "next_link",
            "document_format_hint": "pdf",
            "request_timeout_seconds": 25,
            "rate_limit_per_minute": 10,
            "force_canonical_host": "example.gov",
        },
    )
    assert validate_source_map_record(record) == []


def test_validate_source_map_record_rejects_unknown_override_keys() -> None:
    record = replace(
        _valid_record(),
        system_overrides={"unsupported_key": "value"},
    )
    errors = validate_source_map_record(record)
    assert "unknown system_overrides keys ['unsupported_key']" in "\n".join(errors)


def test_validate_source_map_record_rejects_invalid_override_values() -> None:
    record = replace(
        _valid_record(),
        system_overrides={
            "requires_javascript": "yes",
            "pagination_mode": "cursor",
            "request_timeout_seconds": 0,
        },
    )
    errors = validate_source_map_record(record)
    joined = "\n".join(errors)
    assert "system_overrides.requires_javascript must be a boolean" in joined
    assert "system_overrides.pagination_mode must be one of" in joined
    assert "system_overrides.request_timeout_seconds must be a positive integer" in joined


def test_validate_source_map_rejects_conflicting_per_system_overrides() -> None:
    first = replace(
        _valid_record(),
        plan_period="FY2023",
        system_overrides={"pagination_mode": "single_page"},
    )
    second = replace(
        _valid_record(),
        plan_period="FY2024",
        system_overrides={"pagination_mode": "next_link"},
    )
    with pytest.raises(SourceValidationError) as exc_info:
        validate_source_map([first, second])

    assert "conflicting system_overrides for plan_id 'CA-PERS'" in str(exc_info.value)


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
