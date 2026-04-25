"""Regression tests for evidence reference canonicalization and structured building."""

from __future__ import annotations

import pytest

from pension_data.extract.common.evidence import (
    build_evidence_reference,
    canonicalize_evidence_ref,
    table_evidence_ref,
    text_block_evidence_ref,
)

# ── canonicalize_evidence_ref ───────────────────────────────────────


class TestCanonicalizeEvidenceRef:
    def test_page_ref_simple(self) -> None:
        assert canonicalize_evidence_ref("page 5") == "p.5"

    def test_page_ref_with_section(self) -> None:
        result = canonicalize_evidence_ref("page 5 # Financial Summary")
        assert result == "p.5#Financial Summary"

    def test_page_ref_short_form(self) -> None:
        assert canonicalize_evidence_ref("p5") == "p.5"

    def test_page_ref_with_colon(self) -> None:
        result = canonicalize_evidence_ref("p.3: Table A")
        assert result == "p.3#Table A"

    def test_text_ref_normalized(self) -> None:
        result = canonicalize_evidence_ref("TEXT: some snippet here")
        assert result == "text:some snippet here"

    def test_text_ref_empty_suffix(self) -> None:
        result = canonicalize_evidence_ref("text:")
        assert result == "text:unknown"

    def test_whitespace_collapsed(self) -> None:
        result = canonicalize_evidence_ref("  some   free  form  ref  ")
        assert result == "some free form ref"

    def test_empty_string(self) -> None:
        assert canonicalize_evidence_ref("") == ""

    def test_whitespace_only(self) -> None:
        assert canonicalize_evidence_ref("   ") == ""

    def test_page_ref_case_insensitive(self) -> None:
        assert canonicalize_evidence_ref("Page 10") == "p.10"
        assert canonicalize_evidence_ref("PAGE 10") == "p.10"

    def test_section_whitespace_collapsed(self) -> None:
        result = canonicalize_evidence_ref("p 3 # lots   of   space")
        assert result == "p.3#lots of space"


# ── text_block_evidence_ref ─────────────────────────────────────────


class TestTextBlockEvidenceRef:
    def test_zero_indexed_produces_one_based(self) -> None:
        assert text_block_evidence_ref(0) == "text:1"

    def test_later_block(self) -> None:
        assert text_block_evidence_ref(4) == "text:5"


# ── table_evidence_ref ──────────────────────────────────────────────


class TestTableEvidenceRef:
    def test_none_produces_unknown(self) -> None:
        assert table_evidence_ref(None) == "table:unknown"

    def test_empty_produces_unknown(self) -> None:
        assert table_evidence_ref("") == "table:unknown"

    def test_page_ref_passthrough(self) -> None:
        result = table_evidence_ref("page 5")
        assert result == "p.5"

    def test_text_ref_passthrough(self) -> None:
        result = table_evidence_ref("text: column A")
        assert result == "text:column a"

    def test_arbitrary_ref_prefixed(self) -> None:
        result = table_evidence_ref("schedule of investments")
        assert result == "table:schedule of investments"


# ── build_evidence_reference ────────────────────────────────────────


class TestBuildEvidenceReference:
    def test_page_ref_fields(self) -> None:
        ref = build_evidence_reference(
            report_id="r1",
            source_document_id="d1",
            evidence_ref="page 5 # Balance Sheet",
        )
        assert ref.page_number == 5
        assert ref.section_hint == "Balance Sheet"
        assert ref.snippet_anchor is None
        assert ref.report_id == "r1"

    def test_text_ref_fields(self) -> None:
        ref = build_evidence_reference(
            report_id="r1",
            source_document_id="d1",
            evidence_ref="text: paragraph 3",
        )
        assert ref.page_number is None
        assert ref.section_hint is None
        assert ref.snippet_anchor == "text:paragraph 3"

    def test_table_ref_fields(self) -> None:
        ref = build_evidence_reference(
            report_id="r1",
            source_document_id="d1",
            evidence_ref="table:schedule of investments",
        )
        assert ref.snippet_anchor == "table:schedule of investments"
        assert ref.page_number is None

    def test_freeform_ref_becomes_section_hint(self) -> None:
        ref = build_evidence_reference(
            report_id="r1",
            source_document_id="d1",
            evidence_ref="Financial Summary",
        )
        assert ref.section_hint == "Financial Summary"
        assert ref.page_number is None
        assert ref.snippet_anchor is None

    def test_empty_ref_raises(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            build_evidence_reference(
                report_id="r1",
                source_document_id="d1",
                evidence_ref="",
            )

    def test_deterministic_id(self) -> None:
        ref1 = build_evidence_reference(
            report_id="r1", source_document_id="d1", evidence_ref="page 5"
        )
        ref2 = build_evidence_reference(
            report_id="r1", source_document_id="d1", evidence_ref="page 5"
        )
        assert ref1.evidence_ref_id == ref2.evidence_ref_id

    def test_different_refs_different_ids(self) -> None:
        ref1 = build_evidence_reference(
            report_id="r1", source_document_id="d1", evidence_ref="page 5"
        )
        ref2 = build_evidence_reference(
            report_id="r1", source_document_id="d1", evidence_ref="page 6"
        )
        assert ref1.evidence_ref_id != ref2.evidence_ref_id
