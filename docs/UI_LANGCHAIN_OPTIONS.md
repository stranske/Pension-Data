# UI + LangChain Options (Privacy-Safe Browser Access)

This document compares practical options for adding a higher-quality UI and a LangChain findings interaction layer under these constraints:

- Cannot run the app directly from the repo in user environments.
- PC installs are constrained.
- Mac installs are possible, but Mac cannot fully cover work usage.
- Avoid paying for a separate server.

## Option 1: Zero-Egress Browser UI or Internal Hosting (Recommended for Real Data)

### Shape
- Run the existing `apps/web/` browser workspace against a user-selected local JSON bundle, or package the same workflow with stlite/Pyodide/JupyterLite so analysis executes in the work browser.
- For shared access to `data_origin: generated` or `data_origin: live` bundles, serve the static assets from `scripts/web/serve_local.py --bundle <workspace.json>` or serve deterministic API routes from the `pension-data-serve` FastAPI app bound to the organization network.
- Treat the prior GitHub Pages/Cloudflare + Actions model as fixture/synthetic demo-only; it is not the recommended path for real pension data.
- Generate findings JSON artifacts inside the organization boundary and publish them only to approved internal artifact locations.

### Pros
- No local install needed for end users.
- Works on locked-down work PCs (browser only).
- No separate hosting bill.
- Real pension data stays in the browser session or on an internal host.
- Easy audit trail (GitHub history + workflow logs).
- Can still be a high-quality, modern interface (advanced design system, rich charts,
  responsive layouts, keyboard-first interactions, and polished motion in a static SPA).

### Cons
- Public Pages can demonstrate only fixture/synthetic data.
- Internal hosting requires the organization's normal access-control and network review.
- LLM-dependent interactions require an authorized no-train endpoint or must be disabled.

### Data zones & LLM boundary

- `data_origin: fixture` is safe for public demo hosting when the bundle contains only checked-in fixture data.
- `data_origin: generated` and `data_origin: live` remain inside the organization boundary: browser-local file loading, client-side WASM/stlite/Pyodide, JupyterLite, or an internal host.
- Deterministic analysis routes, including `run_saved_view_endpoint` and `run_metric_history_endpoint`, may run on real data in the browser or on internal hosting because they do not require LLM egress.
- LLM-backed NL/query/findings features must either target an authorized no-train provider endpoint through `OPENAI_BASE_URL` or `ANTHROPIC_BASE_URL`, or remain disabled in `PENSION_DATA_DATA_ZONE=proprietary`.
- A public-hosted app must refuse to pass deployment smoke checks if its served `workspace.json` declares `data_origin: live`.

## Option 2: Packaged Mac Desktop App + Embedded Local LangChain

### Shape
- Build a desktop shell (Tauri preferred for lower footprint).
- Bundle a local Python service (FastAPI) and local vector store.
- Package app as signed `.app` with one-click startup.

### Pros
- Best UX quality and responsiveness.
- Rich local interaction with findings.
- Can support offline analysis.

### Cons
- Limited at work if Mac use is restricted.
- Ongoing desktop packaging/signing maintenance.

## Option 3: Hybrid Model (Fixture Demo + Internal App + Mac Pro App)

### Shape
- Option 1 as the baseline for real-data work access.
- Public Pages/Cloudflare as a fixture-only product demo.
- Option 2 as an advanced client for deeper analyst workflows.
- Keep shared schema for findings so both UIs read the same data model.

### Pros
- Covers both constrained work environments and high-end local workflows.
- Lets you incrementally raise UI quality without blocking on IT constraints.

### Cons
- Two frontends to maintain.

## Option 4: Internal Network Share Build (No Install, Local Browser App)

### Shape
- Build static app assets and distribute via shared drive, internal artifact channel, or internal web host.
- Users open `index.html` locally; data files updated by export scripts.
- Optional LangChain interactions run only through authorized no-train endpoints or are disabled.

### Pros
- No installer needed.
- Minimal infrastructure cost.

### Cons
- Harder update/version discipline than managed internal hosting.
- Cross-origin and file-access browser constraints can be finicky.

## Recommended Path

1. Start with **Option 1** for real work: browser-local/WASM or internal hosting for `generated` and `live` bundles. Use `docs/deploy/IN_PERIMETER_REAL_DATA_REVIEW.md` as the click-to-open review runbook.
2. Keep GitHub Pages / Cloudflare Pages as a fixture-only external demo surface.
3. Add **Option 3** over time by introducing a Mac desktop power-client when UX depth is needed.
4. Keep LLM execution behind authorized no-train endpoints; disable LLM features in the proprietary zone until that endpoint is configured.

## First Reviewable Artifact

The first static UI/LangChain slice is `extraction_quality_dashboard`.

- Schema contract: `docs/data/reviewable-findings/findings.schema.json`
- Published payload path: `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- Contract narrative: `docs/contracts/reviewable-findings-artifact-contract.md`
- Python validator: `pension_data.langchain.review_artifact.validate_reviewable_findings_artifact(...)`

This slice focuses on extraction quality because it can be generated from existing extraction
persistence and source-readiness outputs while still carrying the provenance, confidence, metric
family, entity, and period fields the static UI and LangChain explain/compare flows need.

## Project Inclusion Status

- Mac desktop packaging track is now included in this repo at `apps/mac-desktop/` as an implementation scaffold.

## Minimum Technical Contract for Any Option

- Canonical findings JSON schema (stable versioned contract).
- Provenance links (source doc, page/section anchors, confidence).
- Query filters: entity, period, metric family, severity, confidence.
- LangChain prompt/output contract with deterministic machine-readable result block.
- Export endpoint or artifact path that the UI can consume without repo execution.
