# Pension-Data Mac Desktop App (Scaffold)

This directory tracks the packaged macOS desktop app track requested for Pension-Data.

## Why this exists

- Provides a high-quality native-feeling interface option for power users on Mac.
- Pairs with the browser-first GitHub Pages UI for work-PC accessibility.
- Keeps a shared findings schema so desktop and web experiences stay aligned.

## Planned stack

- Shell/runtime: Tauri (Rust + WebView) for low-memory footprint packaging.
- UI: React + Vite (same design system as Pages UI where possible).
- Local service: Python FastAPI sidecar for LangChain orchestration and retrieval.
- Data contract: versioned JSON findings schema emitted by repo workflows.

## Initial scope

1. Load findings bundles exported by CI/workflows.
2. Filter/search findings by entity, period, metric family, severity, confidence.
3. Open provenance anchors (report, page/section, evidence refs).
4. Run local LangChain-assisted "explain this finding" flows against local artifacts.

## Packaging targets

- macOS `.app` bundle for local installs.
- Signed/notarized distribution can be added after MVP validation.

## Status

Scaffold only in this PR. Implementation will be staged in follow-up PRs under `apps/mac-desktop/`.
