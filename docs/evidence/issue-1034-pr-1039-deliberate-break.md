# Issue #1034 / PR #1039 deliberate-break transcript

## Provenance

- Issue: `stranske/Pension-Data#1034`
- PR: `stranske/Pension-Data#1039`
- Named test: `tests/web/test_in_perimeter_path.py::test_pilot_default_document_id_citation_serves_source_pdf`
- Break target: `packages/renderer-shell/app.js` (`renderDetail` targeting the logical `pilot:` `source_document` ID instead of `provenance.source_locator`) and `src/pension_data/ops/one_pdf_pilot.py`

## 1. Deliberate break (target `pilot:` logical ID again -> HTTP 404 failure)

Temporarily changed `packages/renderer-shell/app.js` in `renderDetail` to target `sourceDocument` (the `pilot:` logical ID) instead of `provenance.source_locator || sourceDocument` and synced `apps/web/renderer-shell/app.js`:

```javascript
const artifactPath = encodeArtifactPath(sourceDocument);
```

Ran the named test:

```console
python3 -m pytest tests/web/test_in_perimeter_path.py::test_pilot_default_document_id_citation_serves_source_pdf -q
```

Output:

```text
F                                                                        [100%]
=================================== FAILURES ===================================
__________ test_pilot_default_document_id_citation_serves_source_pdf ___________
...
>           with urlopen(f"{base_url}{citation_href}", timeout=5) as pdf_response:
E           urllib.error.HTTPError: HTTP Error 404: Artifact file not found
----------------------------- Captured stderr call -----------------------------
127.0.0.1 - - [17/Nov/2026 04:55:12] "GET /config/runtime.json HTTP/1.1" 200 -
127.0.0.1 - - [17/Nov/2026 04:55:12] "GET /data/workspace.json HTTP/1.1" 200 -
127.0.0.1 - - [17/Nov/2026 04:55:12] "GET /renderer-shell/app.js HTTP/1.1" 200 -
127.0.0.1 - - [17/Nov/2026 04:55:12] code 404, message Artifact file not found
127.0.0.1 - - [17/Nov/2026 04:55:12] "GET /artifacts/pilot%3ACA-PERS%3AFY2024%3A6bd2797703aeb535 HTTP/1.1" 404 -
=========================== short test summary info ============================
FAILED tests/web/test_in_perimeter_path.py::test_pilot_default_document_id_citation_serves_source_pdf - urllib.error.HTTPError: HTTP Error 404: Artifact file not found
======================== 1 failed, 13 warnings in 0.84s ========================
```

## 2. Revert break and verify pass

Restored `const artifactPath = encodeArtifactPath(provenance.source_locator || sourceDocument);` in `packages/renderer-shell/app.js`, re-materialized `apps/web` with `python3 scripts/web/sync_renderer_shell.py`, and re-ran the named test and full file:

```console
python3 -m pytest tests/web/test_in_perimeter_path.py::test_pilot_default_document_id_citation_serves_source_pdf -q
python3 -m pytest tests/web/test_in_perimeter_path.py -q
```

Output:

```text
.                                                                        [100%]
======================== 1 passed, 13 warnings in 0.67s ========================

......................................................                   [100%]
======================= 54 passed, 13 warnings in 7.79s ========================
```

## 3. Automated `scripts/check_deliberate_break.py` verification against `origin/main`

```text
{
  "verdict": "PASS",
  "reason": "head-passed-base-failed",
  "test_id": "tests/web/test_in_perimeter_path.py::test_pilot_default_document_id_citation_serves_source_pdf",
  "test_file": "tests/web/test_in_perimeter_path.py",
  "break_file": "src/pension_data/ops/one_pdf_pilot.py"
}
```
