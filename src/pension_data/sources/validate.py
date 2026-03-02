"""Validation helpers for source-map quality gates."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from pension_data.sources.schema import (
    DOCUMENT_FORMAT_HINTS,
    MISMATCH_REASONS,
    OFFICIAL_RESOLUTION_STATES,
    PAGINATION_MODES,
    SOURCE_AUTHORITY_TIERS,
    SOURCE_MAP_DOC_TYPE_HINTS,
    SOURCE_MAP_HEADERS,
    SOURCE_MAP_OVERRIDE_KEYS,
    SOURCE_MAP_PAGINATION_HINTS,
    SOURCE_MAP_REQUIRED_HEADERS,
    SYSTEM_OVERRIDE_KEYS,
    CrawlConstraints,
    SourceMapEntry,
    SourceMapRecord,
)


class SourceValidationError(ValueError):
    """Raised when one or more source-map records are invalid."""


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    """A single actionable source-map validation finding."""

    code: str
    plan_id: str
    message: str


def normalize_url(url: str) -> str:
    """Normalize URLs for stable duplicate/conflict detection."""
    parsed = urlparse(url.strip())
    cleaned = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
    )
    normalized = urlunparse(cleaned)
    if normalized.endswith("/"):
        return normalized[:-1]
    return normalized


def _cell(row: Mapping[str | None, object], key: str) -> str:
    value = row.get(key)
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return ";".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def _split_list(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw.split(";") if item.strip())


def _parse_non_negative_int(raw: str) -> int:
    try:
        return int(raw)
    except ValueError:
        return -1


def _canonical_authority_tier(raw: str) -> str:
    return raw.strip().replace("_", "-")


def _parse_overrides(row: Mapping[str | None, object]) -> tuple[tuple[str, str], ...]:
    parsed: dict[str, str] = {}
    for key, value in row.items():
        if not isinstance(key, str):
            continue
        if not key.startswith("override_"):
            continue
        if value is None:
            continue
        if isinstance(value, list):
            cleaned = ";".join(str(item).strip() for item in value if str(item).strip())
        elif isinstance(value, str):
            cleaned = value.strip()
        else:
            cleaned = str(value).strip()
        if not cleaned:
            continue
        override_key = key.removeprefix("override_")
        parsed[override_key] = cleaned
    return tuple(sorted(parsed.items()))


def parse_source_map_rows(rows: Sequence[Mapping[str | None, object]]) -> list[SourceMapEntry]:
    """Parse source-map rows into typed entries without side effects."""
    entries: list[SourceMapEntry] = []
    for row in rows:
        entries.append(
            SourceMapEntry(
                plan_id=_cell(row, "plan_id"),
                plan_name=_cell(row, "plan_name"),
                expected_plan_identity=_cell(row, "expected_plan_identity"),
                seed_url=_cell(row, "seed_url"),
                allowed_domains=_split_list(_cell(row, "allowed_domains")),
                doc_type_hints=_split_list(_cell(row, "doc_type_hints")),
                pagination_hints=_split_list(_cell(row, "pagination_hints")),
                crawl_constraints=CrawlConstraints(
                    max_pages=_parse_non_negative_int(_cell(row, "max_pages")),
                    max_depth=_parse_non_negative_int(_cell(row, "max_depth")),
                ),
                source_authority_tier=_canonical_authority_tier(
                    _cell(row, "source_authority_tier")
                ),
                mismatch_reason=_cell(row, "mismatch_reason") or None,
                overrides=_parse_overrides(row) or None,
            )
        )
    return entries


def _validate_source_map_headers(fieldnames: Sequence[str | None] | None) -> None:
    if not fieldnames:
        raise ValueError("source-map CSV must include a header row")

    normalized = [name.strip() for name in fieldnames if name and name.strip()]
    missing = sorted(set(SOURCE_MAP_REQUIRED_HEADERS).difference(normalized))
    if missing:
        raise ValueError("source-map CSV is missing required header(s): " + ", ".join(missing))

    unknown = sorted(set(normalized).difference(SOURCE_MAP_HEADERS))
    if unknown:
        raise ValueError("source-map CSV has unsupported header(s): " + ", ".join(unknown))


def load_source_map(path: str | Path) -> list[SourceMapEntry]:
    """Load source-map entries from CSV."""
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _validate_source_map_headers(reader.fieldnames)
        return parse_source_map_rows(list(reader))


def _validate_basic_entry_fields(entry: SourceMapEntry) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []

    parsed = urlparse(entry.seed_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        findings.append(
            ValidationFinding(
                code="invalid_url",
                plan_id=entry.plan_id,
                message=f"seed_url '{entry.seed_url}' must be an absolute http(s) URL",
            )
        )

    if not entry.allowed_domains:
        findings.append(
            ValidationFinding(
                code="missing_allowed_domains",
                plan_id=entry.plan_id,
                message="allowed_domains must include at least one domain",
            )
        )
    else:
        invalid_domains = sorted(
            domain
            for domain in entry.allowed_domains
            if "." not in domain or any(char.isspace() for char in domain)
        )
        if invalid_domains:
            findings.append(
                ValidationFinding(
                    code="invalid_allowed_domain",
                    plan_id=entry.plan_id,
                    message=f"invalid domain values: {', '.join(invalid_domains)}",
                )
            )

    if "annual_report" in entry.doc_type_hints and not entry.source_authority_tier:
        findings.append(
            ValidationFinding(
                code="missing_authority_tier",
                plan_id=entry.plan_id,
                message="annual_report rows require source_authority_tier",
            )
        )
    elif entry.source_authority_tier and entry.source_authority_tier not in SOURCE_AUTHORITY_TIERS:
        findings.append(
            ValidationFinding(
                code="invalid_authority_tier",
                plan_id=entry.plan_id,
                message=(
                    "source_authority_tier must be one of: " + ", ".join(SOURCE_AUTHORITY_TIERS)
                ),
            )
        )

    invalid_doc_type_hints = sorted(set(entry.doc_type_hints).difference(SOURCE_MAP_DOC_TYPE_HINTS))
    if invalid_doc_type_hints:
        findings.append(
            ValidationFinding(
                code="invalid_doc_type_hint",
                plan_id=entry.plan_id,
                message=(
                    "doc_type_hints must be one of: "
                    + ", ".join(SOURCE_MAP_DOC_TYPE_HINTS)
                    + f"; invalid values: {', '.join(invalid_doc_type_hints)}"
                ),
            )
        )

    invalid_pagination_hints = sorted(
        set(entry.pagination_hints).difference(SOURCE_MAP_PAGINATION_HINTS)
    )
    if invalid_pagination_hints:
        findings.append(
            ValidationFinding(
                code="invalid_pagination_hint",
                plan_id=entry.plan_id,
                message=(
                    "pagination_hints must be one of: "
                    + ", ".join(SOURCE_MAP_PAGINATION_HINTS)
                    + f"; invalid values: {', '.join(invalid_pagination_hints)}"
                ),
            )
        )

    if entry.mismatch_reason is not None and entry.mismatch_reason not in MISMATCH_REASONS:
        findings.append(
            ValidationFinding(
                code="invalid_mismatch_reason",
                plan_id=entry.plan_id,
                message=f"mismatch_reason '{entry.mismatch_reason}' is unsupported",
            )
        )

    if entry.crawl_constraints.max_pages < 1 or entry.crawl_constraints.max_depth < 0:
        findings.append(
            ValidationFinding(
                code="invalid_crawl_constraints",
                plan_id=entry.plan_id,
                message="max_pages must be >= 1 and max_depth must be >= 0",
            )
        )

    if entry.overrides is not None:
        override_map = dict(entry.overrides)
        invalid_keys = sorted(set(override_map) - set(SOURCE_MAP_OVERRIDE_KEYS))
        if invalid_keys:
            findings.append(
                ValidationFinding(
                    code="invalid_override_key",
                    plan_id=entry.plan_id,
                    message=f"unsupported override keys: {', '.join(invalid_keys)}",
                )
            )

        pagination_mode = override_map.get("pagination_mode")
        if pagination_mode is not None and pagination_mode not in PAGINATION_MODES:
            findings.append(
                ValidationFinding(
                    code="invalid_override_value",
                    plan_id=entry.plan_id,
                    message=(
                        "override pagination_mode must be one of: " + ", ".join(PAGINATION_MODES)
                    ),
                )
            )

        requires_js = override_map.get("requires_js")
        if requires_js is not None and requires_js not in {"true", "false"}:
            findings.append(
                ValidationFinding(
                    code="invalid_override_value",
                    plan_id=entry.plan_id,
                    message="override requires_js must be 'true' or 'false'",
                )
            )

        force_render_wait_ms = override_map.get("force_render_wait_ms")
        if force_render_wait_ms is not None:
            try:
                parsed_wait = int(force_render_wait_ms)
            except ValueError:
                parsed_wait = -1
            if parsed_wait <= 0:
                findings.append(
                    ValidationFinding(
                        code="invalid_override_value",
                        plan_id=entry.plan_id,
                        message="override force_render_wait_ms must be a positive integer",
                    )
                )

    return findings


def _validate_seed_duplicates(entries: Sequence[SourceMapEntry]) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    seen_by_plan_and_url: set[tuple[str, str]] = set()
    seen_url_to_plan: dict[str, str] = {}

    for entry in entries:
        normalized = normalize_url(entry.seed_url)
        key = (entry.plan_id, normalized)
        if key in seen_by_plan_and_url:
            findings.append(
                ValidationFinding(
                    code="duplicate_seed_url",
                    plan_id=entry.plan_id,
                    message=f"duplicate seed URL detected after normalization: {normalized}",
                )
            )
        seen_by_plan_and_url.add(key)

        if normalized in seen_url_to_plan and seen_url_to_plan[normalized] != entry.plan_id:
            findings.append(
                ValidationFinding(
                    code="conflicting_seed_url",
                    plan_id=entry.plan_id,
                    message=(
                        f"normalized URL {normalized} already assigned to "
                        f"{seen_url_to_plan[normalized]}"
                    ),
                )
            )
        else:
            seen_url_to_plan[normalized] = entry.plan_id

    return findings


def validate_source_map_entries(entries: Iterable[SourceMapEntry]) -> list[ValidationFinding]:
    """Return all validation findings for source-map seed entries."""
    rows = list(entries)
    findings: list[ValidationFinding] = []
    for entry in rows:
        findings.extend(_validate_basic_entry_fields(entry))
    findings.extend(_validate_seed_duplicates(rows))
    return sorted(findings, key=lambda finding: (finding.code, finding.plan_id, finding.message))


def assert_valid_source_map_entries(entries: Iterable[SourceMapEntry]) -> None:
    """Raise with actionable output if source-map seed entries are invalid."""
    findings = validate_source_map_entries(entries)
    if findings:
        details = "\n".join(
            f"[{finding.code}] {finding.plan_id}: {finding.message}" for finding in findings
        )
        raise SourceValidationError(details)


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


def _validate_source_map_record(
    record: SourceMapRecord,
) -> tuple[list[str], tuple[tuple[str, bool | int | str], ...]]:
    """Return validation errors and normalized overrides for one source-map record."""
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

    normalized_overrides = _normalized_overrides(record, errors)

    return errors, normalized_overrides


def validate_source_map_record(record: SourceMapRecord) -> list[str]:
    """Return validation errors for a single source-map record."""
    errors, _normalized = _validate_source_map_record(record)

    return errors


def validate_source_map(records: list[SourceMapRecord]) -> None:
    """Validate all source-map records and raise if any error is found."""
    all_errors: list[str] = []
    overrides_by_plan_id: dict[str, tuple[tuple[str, bool | int | str], ...]] = {}
    for record in records:
        record_errors, normalized_overrides = _validate_source_map_record(record)
        all_errors.extend(record_errors)
        prior_overrides = overrides_by_plan_id.setdefault(record.plan_id, normalized_overrides)
        if prior_overrides != normalized_overrides:
            all_errors.append(
                "conflicting system_overrides for plan_id "
                f"'{record.plan_id}' across source-map records"
            )

    if all_errors:
        message = "\n".join(sorted(set(all_errors)))
        raise SourceValidationError(message)
