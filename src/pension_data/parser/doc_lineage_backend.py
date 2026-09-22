"""Adapt Doc-Lineage page spans to the pension parser's line-oriented input."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from doc_lineage.extract.ocr import OCRBackend


@dataclass(frozen=True, slots=True)
class DocLineagePages:
    """Source lines and extraction coverage without losing page boundaries."""

    page_lines: tuple[tuple[int, tuple[str, ...]], ...]
    pages_recognized: int
    pages_unreadable: int


def extract(
    pdf_bytes: bytes,
    *,
    enable_ocr: bool = True,
    ocr_backend: object | None = None,
) -> DocLineagePages:
    """Extract a PDF with Doc-Lineage, retaining lines before normalization."""
    from doc_lineage.extract import ExtractCache
    from doc_lineage.extract import extract as extract_document

    with TemporaryDirectory(prefix="pension-doc-lineage-") as directory:
        pdf_path = Path(directory) / "source.pdf"
        pdf_path.write_bytes(pdf_bytes)
        document = extract_document(
            pdf_path,
            ocr_enabled=enable_ocr,
            ocr_backend=cast("OCRBackend | None", ocr_backend),
            cache=ExtractCache(),
        )

    pages: list[tuple[int, tuple[str, ...]]] = []
    for span in document.spans:
        if span.text_lines is None:
            raise ValueError("Doc-Lineage span lost source lines")
        pages.append((span.page, span.text_lines))
    return DocLineagePages(
        page_lines=tuple(pages),
        pages_recognized=document.coverage.pages_recognized,
        pages_unreadable=document.coverage.pages_unreadable,
    )
