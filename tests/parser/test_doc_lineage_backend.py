"""Doc-Lineage integration uses real PDFs and preserves pension metric lines."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from pension_data.extract.actuarial.metrics import extract_funded_and_actuarial_metrics
from pension_data.parser.doc_lineage_backend import extract
from pension_data.parser.pdf_pipeline import PDFParserInput, parse_pdf_to_funded_input

pytest.importorskip("doc_lineage")

from doc_lineage.extract.ocr import CallableOCRBackend  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "doc_lineage"


def _input(path: Path, *, backend: object | None = None) -> PDFParserInput:
    return PDFParserInput(
        source_document_id="calpers-fy2024",
        source_url=path.as_uri(),
        effective_date="2024-06-30",
        ingestion_date="2026-09-22",
        default_money_unit_scale="million_usd",
        pdf_bytes=path.read_bytes(),
        parser_backend="doc-lineage",
        doc_lineage_ocr_backend=backend,
    )


def test_text_pdf_emits_page_pointers() -> None:
    path = FIXTURES / "calpers_fy2024_excerpt.pdf"
    result = parse_pdf_to_funded_input(
        replace(
            _input(path, backend=CallableOCRBackend(lambda *_a, **_kw: None)),
            parser_backend="auto",
        )
    )

    assert result.stage_name == "doc_lineage"
    assert result.raw is not None
    assert result.missing_metrics == ()
    assert "p.1#text" in result.provenance_refs
    assert "p.2#text" in result.provenance_refs
    assert any(row["value"] == "$607.5 million" for row in result.raw.table_rows)
    assert any(row["evidence_ref"] == "p.2#table" for row in result.raw.table_rows)


def test_scanned_fixture_triggers_ocr_path() -> None:
    path = FIXTURES / "scanned_actuarial.pdf"
    seen: list[tuple[int, int]] = []

    def recognize(image: object, *, rotation: int = 0) -> str:
        assert rotation == 0
        size = image.size  # type: ignore[attr-defined]
        seen.append(size)
        return (
            "AAL  $600 million\nAVA  $0.5 billion\nFunded ratio  0.8\n"
            "Discount rate  6%\nEmployer Contribution Rate  13.1%\n"
            "Employee Contribution Rate  8.0%\nParticipant Count  1,934,000"
        )

    backend = CallableOCRBackend(recognize)
    document = extract(path.read_bytes(), ocr_backend=backend)
    assert document.pages_recognized == 1
    assert document.pages_unreadable == 0
    assert seen and min(seen[0]) > 1
    assert document.page_lines[0][0] == 1

    result = parse_pdf_to_funded_input(_input(path, backend=backend))
    assert result.stage_name == "doc_lineage"
    assert result.raw is not None
    assert result.missing_metrics == ()
    assert any(row["value"] == "$0.5 billion" for row in result.raw.table_rows)
    assert "p.1#text" in result.provenance_refs
    facts, _ = extract_funded_and_actuarial_metrics(
        plan_id="CA-PERS", plan_period="FY2024", raw=result.raw
    )
    values = {fact.metric_name: fact.normalized_value for fact in facts}
    assert values["aal_usd"] == 600_000_000
    assert values["ava_usd"] == 500_000_000
    assert values["funded_ratio"] == 0.8


def test_explicit_doc_lineage_requires_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pension_data.parser.pdf_pipeline.find_spec", lambda _name: None)
    with pytest.raises(ValueError, match="extra is not installed"):
        parse_pdf_to_funded_input(_input(FIXTURES / "calpers_fy2024_excerpt.pdf"))
