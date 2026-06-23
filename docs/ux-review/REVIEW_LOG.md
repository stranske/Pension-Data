# UX Review Log — Pension-Data

Diff-anchored record of UX Review (`/ux-review`) passes. Detailed artifacts live in `Orchestrator/ux_reviews/`.

## 2026-06-22 — Offline static PWA (`apps/web`, via `scripts/web/serve_local.py`) — overall 3.0/10 (gate FAIL)
- **Scope:** the offline-first browser PWA (the Tier-A locked-down-PC artifact), fixture workspace bundle, `apiBaseUrl` empty.
- **Coverage:** landing/status ✓; Dataset Inventory + switch ✓ (Core Facts/Metric History/Review Queue); Table Explorer + Record Details ✓ (provenance drilldown: source doc + evidence page); Chart Studio ✓ (driven → broken). Global Filters / Saved-Views-Export / Load-Local-Bundle controls observed but not exhaustively driven.
- **Scores:** wired 5.5 / usability 6.5 / help_clarity **7.0** / workflow 6.0 (highest help_clarity in the fleet — clean light theme + provenance). Overall blocker-capped at 3.0 by one sev-4.
- **Headline:** polished, functional app — but **Chart Studio is broken offline because Plotly is CDN-loaded** (`index.html:181`), which also means an external call from a confidential-data workspace.
- **Findings → filed:** Plotly loaded from `cdn.plot.ly`, not vendored → Chart Studio + exported HTML break offline + Tier-A external-call concern → **#<PD-ISSUE>** (vendor Plotly + add to SW + audit external refs).
- **Next focus:** after the fix, re-drive Chart Studio offline + exercise Global Filters / Saved Views / Export-CSV/JSON / Load-Local-Bundle.
