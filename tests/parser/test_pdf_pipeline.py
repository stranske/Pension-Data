"""Tests for deterministic PDF parser pipeline and fallback behavior."""

from __future__ import annotations

from pathlib import Path

from pension_data.extract.actuarial.metrics import extract_funded_and_actuarial_metrics
from pension_data.parser.pdf_pipeline import PDFParserInput, parse_pdf_to_funded_input

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "calpers_fy2024_excerpt.pdf"


def _base_input(*, pdf_bytes: bytes) -> PDFParserInput:
    return PDFParserInput(
        source_document_id="doc:calpers:2024:acfr",
        source_url=(
            "https://www.calpers.ca.gov/docs/board-agendas/"
            "2024/financeadmin/item-6a-00-a-fy24-acfr.pdf"
        ),
        effective_date="2024-06-30",
        ingestion_date="2026-03-02",
        default_money_unit_scale="million_usd",
        pdf_bytes=pdf_bytes,
    )


def test_real_pension_pdf_fixture_parses_end_to_end_into_extraction_ready_input() -> None:
    # Fixture is a deterministic excerpt from a real pension report URL for parser regression tests.
    parser_result = parse_pdf_to_funded_input(_base_input(pdf_bytes=FIXTURE_PATH.read_bytes()))

    assert parser_result.stage_name == "table_primary"
    assert parser_result.escalation_required is False
    assert parser_result.raw is not None
    assert parser_result.missing_metrics == ()
    assert "p.1#table" in parser_result.provenance_refs
    assert "p.2#table" in parser_result.provenance_refs

    facts, diagnostics = extract_funded_and_actuarial_metrics(
        plan_id="CA-PERS",
        plan_period="FY2024",
        raw=parser_result.raw,
    )
    assert diagnostics == []
    assert len(facts) == 7
    assert all("%pdf" not in block.lower() for block in parser_result.raw.text_blocks)
    assert all("startxref" not in block.lower() for block in parser_result.raw.text_blocks)


def test_text_fallback_carries_page_level_text_evidence_refs() -> None:
    text_only_pdf = (
        b"%PDF-1.4\n%%Page: 1 1\n"
        b"Funded ratio 81.2%\n"
        b"AAL was $410.5 million\n"
        b"AVA was $333.7 million\n"
        b"Discount rate was 6.75%\n"
        b"Employer contribution rate was 11.1%\n"
        b"Employee contribution rate was 7.1%\n"
        b"Participant count was 112,000\n"
    )
    parser_result = parse_pdf_to_funded_input(_base_input(pdf_bytes=text_only_pdf))

    assert parser_result.stage_name == "text_fallback"
    assert parser_result.escalation_required is False
    assert parser_result.raw is not None
    assert parser_result.raw.text_block_evidence_refs
    assert all(ref.startswith("p.1#text") for ref in parser_result.raw.text_block_evidence_refs)

    facts, _ = extract_funded_and_actuarial_metrics(
        plan_id="TX-ERS",
        plan_period="FY2025",
        raw=parser_result.raw,
    )
    assert len(facts) == 7
    assert all(fact.evidence_refs[0].startswith("p.1#text") for fact in facts)


def test_ocr_fallback_stage_handles_non_selectable_pdf_bytes() -> None:
    scanned_like_pdf = b"%PDF-1.5\n\x00\x01\x02\x03\xff\xfe\x00\x00"

    def _ocr_stub(_: bytes) -> tuple[str, ...]:
        return (
            "Funded ratio 79.5% AAL $605.0 million AVA $481.0 million",
            (
                "Discount rate 6.9% Employer contribution rate 12.5% "
                "Employee contribution rate 8.2% Participant count 209000"
            ),
        )

    parser_result = parse_pdf_to_funded_input(
        PDFParserInput(
            source_document_id="doc:calpers:2024:acfr",
            source_url=(
                "https://www.calpers.ca.gov/docs/board-agendas/"
                "2024/financeadmin/item-6a-00-a-fy24-acfr.pdf"
            ),
            effective_date="2024-06-30",
            ingestion_date="2026-03-02",
            default_money_unit_scale="million_usd",
            pdf_bytes=scanned_like_pdf,
            ocr_extract=_ocr_stub,
        )
    )
    assert parser_result.stage_name == "full_fallback"
    assert parser_result.escalation_required is False
    assert parser_result.raw is not None
    assert parser_result.missing_metrics == ()
    assert "p.1#text" in parser_result.provenance_refs
    assert "p.2#text" in parser_result.provenance_refs


def test_escalation_payload_is_actionable_when_all_stages_fail() -> None:
    parser_result = parse_pdf_to_funded_input(
        _base_input(pdf_bytes=b"%PDF-1.4\n%%Page: 1 1\nNo funded metrics disclosed\n")
    )

    assert parser_result.escalation_required is True
    assert parser_result.escalation is not None
    assert parser_result.escalation.reason == "parser_fallback_exhaustion"
    assert len(parser_result.attempts) == 3
    assert len(parser_result.missing_metrics) == 7
    assert "parser_fallback_exhaustion" in parser_result.actionable_flags
    assert "configure_ocr_fallback" in parser_result.actionable_flags


def test_parser_filters_pdf_internal_noise_lines_from_fallback_text() -> None:
    noisy_pdf = (
        b"%PDF-1.4\n"
        b"1 0 obj\n"
        b"<< /Type /Catalog /Pages 2 0 R >>\n"
        b"endobj\n"
        b"%%Page: 1 1\n"
        b"Funded ratio 80.0%\n"
        b"AAL 600 million\n"
        b"stream\n"
        b"xref\n"
        b"trailer\n"
        b"startxref\n"
        b"%%EOF\n"
    )

    parser_result = parse_pdf_to_funded_input(_base_input(pdf_bytes=noisy_pdf))
    assert parser_result.raw is not None
    lowered_blocks = [block.lower() for block in parser_result.raw.text_blocks]
    lowered_table_labels = [row.get("label", "").lower() for row in parser_result.raw.table_rows]
    assert any("funded ratio" in block for block in lowered_blocks + lowered_table_labels)
    assert all(" obj" not in block for block in lowered_blocks + lowered_table_labels)
    assert all("startxref" not in block for block in lowered_blocks + lowered_table_labels)
    assert all("%%eof" not in block for block in lowered_blocks + lowered_table_labels)


def test_table_primary_emits_normalized_metric_row_contract() -> None:
    table_like_pdf = (
        b"%PDF-1.4\n%%Page: 1 1\n"
        b"Funded ratio 83.4%\n"
        b"AAL $450.0 million\n"
        b"AVA $377.0 million\n"
        b"Discount rate 6.8%\n"
        b"Employer contribution rate 10.9%\n"
        b"Employee contribution rate 7.0%\n"
        b"Participant count 132000\n"
    )

    parser_result = parse_pdf_to_funded_input(_base_input(pdf_bytes=table_like_pdf))
    assert parser_result.stage_name == "table_primary"
    assert parser_result.raw is not None
    assert parser_result.raw.table_rows
    assert len(parser_result.raw.table_rows) >= 7
    assert all(
        set(row) == {"label", "value", "evidence_ref"} for row in parser_result.raw.table_rows
    )
    assert all(row["evidence_ref"].startswith("p.1#table") for row in parser_result.raw.table_rows)


def test_ocr_stage_runs_after_low_signal_native_text_stage() -> None:
    low_signal_pdf = (
        b"%PDF-1.4\n%%Page: 1 1\n"
        b"The annual report includes notes and accounting narratives only.\n"
        b"No funded metrics are disclosed in this page-level text block.\n"
    )

    def _ocr_stub(_: bytes) -> tuple[str, ...]:
        return (
            "Funded ratio 79.5% AAL $605.0 million AVA $481.0 million",
            (
                "Discount rate 6.9% Employer contribution rate 12.5% "
                "Employee contribution rate 8.2% Participant count 209000"
            ),
        )

    parser_result = parse_pdf_to_funded_input(
        PDFParserInput(
            source_document_id="doc:calpers:2024:acfr",
            source_url=(
                "https://www.calpers.ca.gov/docs/board-agendas/"
                "2024/financeadmin/item-6a-00-a-fy24-acfr.pdf"
            ),
            effective_date="2024-06-30",
            ingestion_date="2026-03-02",
            default_money_unit_scale="million_usd",
            pdf_bytes=low_signal_pdf,
            ocr_extract=_ocr_stub,
        )
    )
    assert parser_result.stage_name == "full_fallback"
    assert parser_result.raw is not None
    assert parser_result.missing_metrics == ()
    assert parser_result.attempts[0].stage_name == "table_primary"
    assert parser_result.attempts[0].failure_reason == "incomplete-required-fields"
    assert parser_result.attempts[1].stage_name == "text_fallback"
    assert parser_result.attempts[1].failure_reason == "incomplete-required-fields"
    assert parser_result.attempts[2].stage_name == "full_fallback"
    assert parser_result.attempts[2].succeeded is True


def test_table_primary_parses_pdf_tj_array_text_tokens() -> None:
    tj_array_pdf = (
        b"%PDF-1.4\n%%Page: 1 1\n"
        b"[(Funded ratio ) 10 (83.4%)] TJ\n"
        b"[(AAL ) 10 ($450.0 million)] TJ\n"
        b"[(AVA ) 10 ($377.0 million)] TJ\n"
        b"[(Discount rate ) 10 (6.8%)] TJ\n"
        b"[(Employer contribution rate ) 10 (10.9%)] TJ\n"
        b"[(Employee contribution rate ) 10 (7.0%)] TJ\n"
        b"[(Participant count ) 10 (132000)] TJ\n"
    )

    parser_result = parse_pdf_to_funded_input(_base_input(pdf_bytes=tj_array_pdf))
    assert parser_result.stage_name == "table_primary"
    assert parser_result.escalation_required is False
    assert parser_result.raw is not None
    assert parser_result.missing_metrics == ()
    assert len(parser_result.raw.table_rows) >= 7
    assert all(row["evidence_ref"].startswith("p.1#table") for row in parser_result.raw.table_rows)
