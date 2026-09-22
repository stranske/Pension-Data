# Shared PDF extraction for the one-PDF pilot

Install `pension-data[doc_lineage]` to use the pinned Doc-Lineage extractor and its local OCR dependencies. The `one-pdf-pilot` command selects that backend automatically when installed; use `--parser-backend doc-lineage` to require it or `--parser-backend legacy` to compare with the existing parser. The legacy chain remains the fallback when the optional package is absent or its extraction cannot produce all required funded metrics.

The adapter passes source text lines through to the pension metric parser before Doc-Lineage's normalized page text can collapse units or ratios. It retains `p.<page>#text` and `p.<page>#table` evidence references, including page numbers after unreadable gaps. OCR runs only for pages without a text layer. Install the `tesseract` executable for the default local recognizer; unreadable pages are flagged in `parser_result.json` rather than counted as recognized.

Run the focused integration gate with the extra installed:

```sh
python -m pytest tests/parser/test_doc_lineage_backend.py tests/golden/test_one_pdf_pilot_golden.py -q
```

The committed PDFs under `tests/parser/fixtures/doc_lineage/` are synthetic. The scanned fixture checks OCR routing using a controlled recognizer after real page rasterization; it does not measure Tesseract accuracy.
