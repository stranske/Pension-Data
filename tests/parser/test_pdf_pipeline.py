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
