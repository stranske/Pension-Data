# UI + LangChain Options (No Dedicated Paid Server)

This document compares practical options for adding a higher-quality UI and a LangChain findings interaction layer under these constraints:

- Cannot run the app directly from the repo in user environments.
- PC installs are constrained.
- Mac installs are possible, but Mac cannot fully cover work usage.
- Avoid paying for a separate server.

## Option 1: Static Web UI on GitHub Pages + LangChain via GitHub Actions (Recommended)

### Shape
- Build a static frontend (React/Vite or SvelteKit static export).
- Publish to GitHub Pages.
- Generate findings JSON artifacts from CI/workflows and publish to `docs/data/` or release assets.
- Trigger LangChain workflows from issue comments/PR comments or manual dispatch.
- Write LangChain outputs back as JSON summaries and PR/issue comments.

### Pros
- No local install needed for end users.
- Works on locked-down work PCs (browser only).
- No separate hosting bill.
- Easy audit trail (GitHub history + workflow logs).
- Can still be a high-quality, modern interface (advanced design system, rich charts,
  responsive layouts, keyboard-first interactions, and polished motion in a static SPA).

### Cons
- No always-on conversational backend; interactions are asynchronous.
- Rate limits/token limits depend on workflow execution budgets.

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

## Option 3: Hybrid Model (Pages UI for Work + Mac Pro App for Power Use)

### Shape
- Option 1 as the baseline for universal access.
- Option 2 as an advanced client for deeper analyst workflows.
- Keep shared schema for findings so both UIs read the same data model.

### Pros
- Covers both constrained work environments and high-end local workflows.
- Lets you incrementally raise UI quality without blocking on IT constraints.

### Cons
- Two frontends to maintain.

## Option 4: Internal Network Share Build (No Install, Local Browser App)

### Shape
- Build static app assets and distribute via shared drive or internal artifact channel.
- Users open `index.html` locally; data files updated by export scripts.
- LangChain interactions run via workflow_dispatch and write result files.

### Pros
- No installer needed.
- Minimal infrastructure cost.

### Cons
- Harder update/version discipline than GitHub Pages.
- Cross-origin and file-access browser constraints can be finicky.

## Recommended Path

1. Start with **Option 1** to satisfy work constraints quickly.
2. Add **Option 3** over time by introducing a Mac desktop power-client when UX depth is needed.
3. Keep LangChain execution in CI first; only move to local embedded inference if latency becomes a hard blocker.

## Project Inclusion Status

- Mac desktop packaging track is now included in this repo at `apps/mac-desktop/` as an implementation scaffold.

## Minimum Technical Contract for Any Option

- Canonical findings JSON schema (stable versioned contract).
- Provenance links (source doc, page/section anchors, confidence).
- Query filters: entity, period, metric family, severity, confidence.
- LangChain prompt/output contract with deterministic machine-readable result block.
- Export endpoint or artifact path that the UI can consume without repo execution.
