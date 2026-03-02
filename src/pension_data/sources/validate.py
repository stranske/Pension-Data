"""Validation helpers for source-map quality gates."""

from __future__ import annotations

from pension_data.sources.schema import (
    DOCUMENT_FORMAT_HINTS,
    MISMATCH_REASONS,
    OFFICIAL_RESOLUTION_STATES,
    PAGINATION_MODES,
    SOURCE_AUTHORITY_TIERS,
    SYSTEM_OVERRIDE_KEYS,
    SourceMapRecord,
)


class SourceValidationError(ValueError):
    """Raised when one or more source-map records are invalid."""


def _normalized_overrides(
    record: SourceMapRecord,
    errors: list[str],
) -> tuple[tuple[str, bool | int | str], ...]:
    if record.system_overrides is None:
        return ()

    overrides = record.system_overrides
    unknown_keys = sorted(set(overrides).difference(SYSTEM_OVERRIDE_KEYS))
    if unknown_keys:
        errors.append(
            f"unknown system_overrides keys {unknown_keys} for {record.plan_id}:{record.plan_period}"
        )

    pagination_mode = overrides.get("pagination_mode")
    if pagination_mode is not None and pagination_mode not in PAGINATION_MODES:
        errors.append(
            "system_overrides.pagination_mode must be one of "
            f"{PAGINATION_MODES} for {record.plan_id}:{record.plan_period}"
        )

    doc_hint = overrides.get("document_format_hint")
    if doc_hint is not None and doc_hint not in DOCUMENT_FORMAT_HINTS:
        errors.append(
            "system_overrides.document_format_hint must be one of "
            f"{DOCUMENT_FORMAT_HINTS} for {record.plan_id}:{record.plan_period}"
        )

    requires_js = overrides.get("requires_javascript")
    if requires_js is not None and not isinstance(requires_js, bool):
        errors.append(
            "system_overrides.requires_javascript must be a boolean "
            f"for {record.plan_id}:{record.plan_period}"
        )

    request_timeout_seconds = overrides.get("request_timeout_seconds")
    if request_timeout_seconds is not None and (
        not isinstance(request_timeout_seconds, int) or request_timeout_seconds <= 0
    ):
        errors.append(
            "system_overrides.request_timeout_seconds must be a positive integer "
            f"for {record.plan_id}:{record.plan_period}"
        )

    rate_limit_per_minute = overrides.get("rate_limit_per_minute")
    if rate_limit_per_minute is not None and (
        not isinstance(rate_limit_per_minute, int) or rate_limit_per_minute <= 0
    ):
        errors.append(
            "system_overrides.rate_limit_per_minute must be a positive integer "
            f"for {record.plan_id}:{record.plan_period}"
        )

    canonical_host = overrides.get("force_canonical_host")
    if canonical_host is not None and (not isinstance(canonical_host, str) or not canonical_host):
        errors.append(
            "system_overrides.force_canonical_host must be a non-empty string "
            f"for {record.plan_id}:{record.plan_period}"
        )

    return tuple(sorted(overrides.items()))


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

    if (
        record.mismatch_reason is not None
        and record.official_resolution_state == "available_official"
    ):
        errors.append(
            "mismatch_reason must be empty when official_resolution_state is available_official"
        )

    _normalized_overrides(record, errors)

    return errors


def validate_source_map(records: list[SourceMapRecord]) -> None:
    """Validate all source-map records and raise if any error is found."""
    all_errors: list[str] = []
    overrides_by_plan_id: dict[str, tuple[tuple[str, bool | int | str], ...]] = {}
    for record in records:
        all_errors.extend(validate_source_map_record(record))
        normalized_overrides = tuple(sorted((record.system_overrides or {}).items()))
        prior_overrides = overrides_by_plan_id.setdefault(record.plan_id, normalized_overrides)
        if prior_overrides != normalized_overrides:
            all_errors.append(
                "conflicting system_overrides for plan_id "
                f"'{record.plan_id}' across source-map records"
            )

    if all_errors:
        message = "\n".join(sorted(set(all_errors)))
        raise SourceValidationError(message)
