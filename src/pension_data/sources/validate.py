"""Validation helpers for source-map quality gates."""

from __future__ import annotations

from pension_data.sources.schema import (
    MISMATCH_REASONS,
    OFFICIAL_RESOLUTION_STATES,
    SOURCE_AUTHORITY_TIERS,
    SourceMapRecord,
)


class SourceValidationError(ValueError):
    """Raised when one or more source-map records are invalid."""


def validate_source_map_record(record: SourceMapRecord) -> list[str]:
    """Return validation errors for a single source-map record."""
    errors: list[str] = []

    if record.source_authority_tier not in SOURCE_AUTHORITY_TIERS:
        errors.append(
            f"invalid source_authority_tier '{record.source_authority_tier}' "
            f"for {record.plan_id}:{record.plan_period}"
        )

    if record.official_resolution_state not in OFFICIAL_RESOLUTION_STATES:
        errors.append(
            f"invalid official_resolution_state '{record.official_resolution_state}' "
            f"for {record.plan_id}:{record.plan_period}"
        )

    if record.mismatch_reason is not None and record.mismatch_reason not in MISMATCH_REASONS:
        errors.append(
            f"invalid mismatch_reason '{record.mismatch_reason}' "
            f"for {record.plan_id}:{record.plan_period}"
        )

    if record.official_resolution_state == "available_official" and (
        record.source_authority_tier == "high-confidence-third-party"
    ):
        errors.append(
            "available_official requires source_authority_tier of 'official' or 'official-mirror'"
        )

    if record.official_resolution_state == "available_non_official_only" and (
        record.source_authority_tier in {"official", "official-mirror"}
    ):
        errors.append("available_non_official_only cannot use an official authority tier")

    if record.mismatch_reason == "wrong_plan":
        if record.observed_plan_identity is None:
            errors.append("wrong_plan mismatch requires observed_plan_identity")
        elif record.observed_plan_identity == record.expected_plan_identity:
            errors.append("wrong_plan mismatch requires different expected/observed identities")

    if record.mismatch_reason != "wrong_plan" and record.observed_plan_identity is not None:
        errors.append("observed_plan_identity is only valid when mismatch_reason is wrong_plan")

    if (
        record.official_resolution_state == "available_non_official_only"
        and record.mismatch_reason != "non_official_only"
    ):
        errors.append(
            "available_non_official_only requires mismatch_reason of non_official_only"
        )

    if (
        record.mismatch_reason in {"wrong_plan", "stale_period"}
        and record.official_resolution_state != "available_official"
    ):
        errors.append(
            f"{record.mismatch_reason} mismatch requires official_resolution_state "
            "of available_official"
        )

    if (
        record.mismatch_reason == "non_official_only"
        and record.official_resolution_state != "available_non_official_only"
    ):
        errors.append(
            "non_official_only mismatch requires official_resolution_state "
            "of available_non_official_only"
        )

    return errors


def validate_source_map(records: list[SourceMapRecord]) -> None:
    """Validate all source-map records and raise if any error is found."""
    all_errors: list[str] = []
    for record in records:
        all_errors.extend(validate_source_map_record(record))

    if all_errors:
        message = "\n".join(sorted(set(all_errors)))
        raise SourceValidationError(message)
