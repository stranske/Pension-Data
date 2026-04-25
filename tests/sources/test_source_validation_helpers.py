"""Regression tests for source-map validation helpers."""

from __future__ import annotations

from pension_data.sources.schema import CrawlConstraints, SourceMapEntry
from pension_data.sources.validate import (
    _parse_overrides,
    normalize_url,
    parse_source_map_rows,
    validate_source_map_entries,
)

# ── normalize_url ───────────────────────────────────────────────────


class TestNormalizeUrl:
    def test_lowercases_scheme(self) -> None:
        assert normalize_url("HTTPS://Example.com/path") == "https://example.com/path"

    def test_lowercases_netloc(self) -> None:
        assert normalize_url("https://Example.COM/path") == "https://example.com/path"

    def test_removes_fragment(self) -> None:
        assert normalize_url("https://example.com/path#section") == "https://example.com/path"

    def test_removes_trailing_slash(self) -> None:
        assert normalize_url("https://example.com/path/") == "https://example.com/path"

    def test_strips_whitespace(self) -> None:
        assert normalize_url("  https://example.com/path  ") == "https://example.com/path"

    def test_preserves_query_params(self) -> None:
        result = normalize_url("https://example.com/path?q=1&b=2")
        assert "?q=1&b=2" in result

    def test_bare_domain_no_trailing_slash(self) -> None:
        result = normalize_url("https://example.com/")
        assert result == "https://example.com"


# ── _parse_overrides ────────────────────────────────────────────────


class TestParseOverrides:
    def test_extracts_override_prefixed_keys(self) -> None:
        row = {"override_requires_js": "true", "plan_id": "CA-PERS"}
        result = _parse_overrides(row)
        assert result == (("requires_js", "true"),)

    def test_strips_whitespace(self) -> None:
        row = {"override_notes": "  some note  "}
        result = _parse_overrides(row)
        assert result == (("notes", "some note"),)

    def test_skips_none_values(self) -> None:
        row = {"override_requires_js": None}
        result = _parse_overrides(row)
        assert result == ()

    def test_skips_empty_values(self) -> None:
        row = {"override_requires_js": ""}
        result = _parse_overrides(row)
        assert result == ()

    def test_handles_list_values(self) -> None:
        row = {"override_notes": ["note1", "note2"]}
        result = _parse_overrides(row)
        assert result == (("notes", "note1;note2"),)

    def test_non_string_keys_skipped(self) -> None:
        row = {None: "value", "override_notes": "hello"}
        result = _parse_overrides(row)
        assert result == (("notes", "hello"),)

    def test_sorted_alphabetically(self) -> None:
        row = {"override_z_key": "1", "override_a_key": "2"}
        result = _parse_overrides(row)
        assert result[0][0] == "a_key"
        assert result[1][0] == "z_key"


# ── parse_source_map_rows ───────────────────────────────────────────


class TestParseSourceMapRows:
    def test_parses_minimal_row(self) -> None:
        row = {
            "plan_id": "CA-PERS",
            "plan_name": "CalPERS",
            "expected_plan_identity": "CA-PERS",
            "seed_url": "https://calpers.ca.gov",
            "allowed_domains": "calpers.ca.gov",
            "doc_type_hints": "annual_report",
            "pagination_hints": "single_page",
            "max_pages": "5",
            "max_depth": "2",
            "source_authority_tier": "official",
            "mismatch_reason": "",
        }
        entries = parse_source_map_rows([row])
        assert len(entries) == 1
        entry = entries[0]
        assert entry.plan_id == "CA-PERS"
        assert entry.seed_url == "https://calpers.ca.gov"
        assert entry.allowed_domains == ("calpers.ca.gov",)
        assert entry.doc_type_hints == ("annual_report",)
        assert entry.crawl_constraints.max_pages == 5
        assert entry.crawl_constraints.max_depth == 2
        assert entry.source_authority_tier == "official"
        assert entry.mismatch_reason is None

    def test_semicolon_delimited_lists(self) -> None:
        row = {
            "plan_id": "CA-PERS",
            "plan_name": "CalPERS",
            "expected_plan_identity": "CA-PERS",
            "seed_url": "https://calpers.ca.gov",
            "allowed_domains": "calpers.ca.gov;treasury.ca.gov",
            "doc_type_hints": "annual_report;investment_report",
            "pagination_hints": "single_page;next_link",
            "max_pages": "10",
            "max_depth": "3",
            "source_authority_tier": "official",
            "mismatch_reason": "",
        }
        entries = parse_source_map_rows([row])
        entry = entries[0]
        assert len(entry.allowed_domains) == 2
        assert len(entry.doc_type_hints) == 2

    def test_override_columns_parsed(self) -> None:
        row = {
            "plan_id": "CA-PERS",
            "plan_name": "CalPERS",
            "expected_plan_identity": "CA-PERS",
            "seed_url": "https://calpers.ca.gov",
            "allowed_domains": "calpers.ca.gov",
            "doc_type_hints": "annual_report",
            "pagination_hints": "single_page",
            "max_pages": "5",
            "max_depth": "2",
            "source_authority_tier": "official",
            "mismatch_reason": "",
            "override_requires_js": "true",
        }
        entries = parse_source_map_rows([row])
        assert entries[0].overrides is not None
        overrides_dict = dict(entries[0].overrides)
        assert overrides_dict["requires_js"] == "true"

    def test_underscore_authority_tier_normalized(self) -> None:
        row = {
            "plan_id": "CA-PERS",
            "plan_name": "CalPERS",
            "expected_plan_identity": "CA-PERS",
            "seed_url": "https://calpers.ca.gov",
            "allowed_domains": "calpers.ca.gov",
            "doc_type_hints": "annual_report",
            "pagination_hints": "single_page",
            "max_pages": "5",
            "max_depth": "2",
            "source_authority_tier": "official_mirror",
            "mismatch_reason": "",
        }
        entries = parse_source_map_rows([row])
        assert entries[0].source_authority_tier == "official-mirror"


# ── validate_source_map_entries ─────────────────────────────────────


def _valid_entry(**overrides: object) -> SourceMapEntry:
    defaults = {
        "plan_id": "CA-PERS",
        "plan_name": "CalPERS",
        "expected_plan_identity": "CA-PERS",
        "seed_url": "https://calpers.ca.gov/reports",
        "allowed_domains": ("calpers.ca.gov",),
        "doc_type_hints": ("annual_report",),
        "pagination_hints": ("single_page",),
        "crawl_constraints": CrawlConstraints(max_pages=5, max_depth=2),
        "source_authority_tier": "official",
        "mismatch_reason": None,
        "overrides": None,
    }
    defaults.update(overrides)
    return SourceMapEntry(**defaults)  # type: ignore[arg-type]


class TestValidateSourceMapEntries:
    def test_valid_entry_no_findings(self) -> None:
        findings = validate_source_map_entries([_valid_entry()])
        assert findings == []

    def test_invalid_url_detected(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(seed_url="not-a-url"),
        ])
        codes = [f.code for f in findings]
        assert "invalid_url" in codes

    def test_missing_allowed_domains_detected(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(allowed_domains=()),
        ])
        codes = [f.code for f in findings]
        assert "missing_allowed_domains" in codes

    def test_invalid_domain_detected(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(allowed_domains=("nodot",)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_allowed_domain" in codes

    def test_invalid_authority_tier_detected(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(source_authority_tier="bogus"),
        ])
        codes = [f.code for f in findings]
        assert "invalid_authority_tier" in codes

    def test_annual_report_requires_authority_tier(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(
                doc_type_hints=("annual_report",),
                source_authority_tier="",
            ),
        ])
        codes = [f.code for f in findings]
        assert "missing_authority_tier" in codes

    def test_invalid_doc_type_hint(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(doc_type_hints=("unknown_type",)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_doc_type_hint" in codes

    def test_invalid_pagination_hint(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(pagination_hints=("unknown_pagination",)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_pagination_hint" in codes

    def test_invalid_crawl_constraints(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(
                crawl_constraints=CrawlConstraints(max_pages=0, max_depth=2),
            ),
        ])
        codes = [f.code for f in findings]
        assert "invalid_crawl_constraints" in codes

    def test_duplicate_seed_url_detected(self) -> None:
        entry = _valid_entry()
        findings = validate_source_map_entries([entry, entry])
        codes = [f.code for f in findings]
        assert "duplicate_seed_url" in codes

    def test_conflicting_seed_url_detected(self) -> None:
        e1 = _valid_entry(plan_id="CA-PERS")
        e2 = _valid_entry(plan_id="CA-STRS")
        findings = validate_source_map_entries([e1, e2])
        codes = [f.code for f in findings]
        assert "conflicting_seed_url" in codes

    def test_normalized_url_duplicate_detection(self) -> None:
        e1 = _valid_entry(seed_url="HTTPS://CALPERS.CA.GOV/reports/")
        e2 = _valid_entry(seed_url="https://calpers.ca.gov/reports")
        findings = validate_source_map_entries([e1, e2])
        codes = [f.code for f in findings]
        assert "duplicate_seed_url" in codes

    def test_invalid_override_key_detected(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(overrides=(("bad_key", "value"),)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_override_key" in codes

    def test_invalid_override_pagination_mode(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(overrides=(("pagination_mode", "bad_mode"),)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_override_value" in codes

    def test_invalid_override_requires_js(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(overrides=(("requires_js", "maybe"),)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_override_value" in codes

    def test_invalid_override_force_render_wait_ms(self) -> None:
        findings = validate_source_map_entries([
            _valid_entry(overrides=(("force_render_wait_ms", "not_a_number"),)),
        ])
        codes = [f.code for f in findings]
        assert "invalid_override_value" in codes
