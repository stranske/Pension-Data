# UX Review Log — Pension-Data

Diff-anchored record of UX Review (`/ux-review`) passes. Detailed artifacts live in `Orchestrator/ux_reviews/`.

## 2026-06-22 — Offline static PWA (`apps/web`, via `scripts/web/serve_local.py`) — overall 3.0/10 (gate FAIL)

- **Scope:** the offline-first browser PWA (the Tier-A locked-down-PC artifact), fixture workspace bundle, `apiBaseUrl` empty.
- **Coverage:** landing/status ✓; Dataset Inventory + switch ✓ (Core Facts/Metric History/Review Queue); Table Explorer + Record Details ✓ (provenance drilldown: source doc + evidence page); Chart Studio ✓ (driven → broken). Global Filters / Saved-Views-Export / Load-Local-Bundle controls observed but not exhaustively driven.
- **Scores:** wired 5.5 / usability 6.5 / help_clarity **7.0** / workflow 6.0 (highest help_clarity in the fleet — clean light theme + provenance). Overall blocker-capped at 3.0 by one sev-4.
- **Headline:** polished, functional app — but **Chart Studio is broken offline because Plotly is CDN-loaded** (`index.html:181`), which also means an external call from a confidential-data workspace.
- **Findings → filed:** Plotly loaded from `cdn.plot.ly`, not vendored → Chart Studio + exported HTML break offline + Tier-A external-call concern → **#594** (vendor Plotly + add to SW + audit external refs).
- **Next focus:** after the fix, re-drive Chart Studio offline + exercise Global Filters / Saved Views / Export-CSV/JSON / Load-Local-Bundle.

## 2026-06-23 — Re-test after #594 — commit `6a6cae1` — overall 8.5/10 (gate PASS ✅)

- **Coverage (prior next-focus — Chart Studio offline — achieved):** landing/Zero-Install ✓; Dataset Inventory + switch ✓; Table Explorer ✓; Global Filters + Saved Views/Export controls ✓ (present); **Chart Studio ✓ renders OFFLINE** (Time Series on load, re-renders to Distribution on template switch) with **zero external requests**. **Not driven (→ next focus):** Export-to-file (CSV/JSON), Load-Local-Bundle round-trip, Saved-Views persistence (avoided triggering downloads this pass); Record Details drilldown not re-driven (confirmed working prior review).
- **Scores:** wired 9.0 / usability 7.5 / help_clarity 8.5 / workflow 8.0; **no blockers**; adversarial critic refuted nothing. Panel: claude 8/6/6/6 · codex 9/9/9/8 · cursor 9/8/8/8 · vibe 10/7/10/10.
- **Headline:** clean recovery 3.0 → 8.5. The #594 offline blocker is fully and well resolved — and a model for the offline-class fix (cf. IMI #639, still open).
- **Findings → disposition:** #594 (Plotly CDN) **FIXED** — vendored at `apps/web/vendor/plotly-2.35.2.min.js`; `index.html:181` + the Export-Interactive-HTML output (`app.js:970-983` inlines the vendored source) reference local copies; service worker (`sw.js:7`) caches it; graceful "Chart rendering unavailable…" fallback (`app.js:10`); no `cdn.plot.ly` anywhere; guard test `tests/web/test_no_external_cdn.py` passes 2/2; live network = only localhost + inline `data:` URIs. No new issues filed (panel mining returned only the coverage-gaps above).
- **Next focus:** Export CSV/JSON to file + verify content/filename; Load-Local-Bundle import round-trip + post-import render; Saved-Views persistence; consider adopting the synced design-system kit.
