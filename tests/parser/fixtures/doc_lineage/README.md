# Doc-Lineage parser fixtures

Both PDFs are synthetic regression assets, not downloaded pension records.
`calpers_fy2024_excerpt.pdf` renders the same seven example values as the older
PDF-like text fixture in the parent folder, using two valid PDF pages.
`scanned_actuarial.pdf` contains those values as a raster image with no text layer.
The OCR regression uses a controlled recognizer after real page rasterization;
it verifies fallback routing and page attribution, not Tesseract accuracy.
