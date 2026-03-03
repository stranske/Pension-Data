# Mac Desktop Implementation Plan

## Phase 1: Shell + Contract Foundation

- [x] Add Tauri shell scaffold (`src-tauri/`).
- [x] Add Electron compatibility scaffold (`electron/`).
- [x] Add web-to-desktop UI sync script (`scripts/sync_web_ui.sh`).
- [x] Add runtime contract validator (`scripts/validate_runtime_contract.mjs`).

## Phase 2: Packaging + Operations

- [x] Add manual macOS packaging workflow (`.github/workflows/mac-desktop-tauri.yml`).
- [x] Add operator setup/release documentation.
- [ ] Add signed/notarized release flow for production distribution.

## Phase 3: Benchmarking

- [x] Add shell benchmark harness (`benchmarks/benchmark_shells.py`).
- [x] Add baseline report artifact (`benchmarks/latest_report.md`).
- [ ] Replace sample metrics with measured CI/device runs.

## Phase 4: Feature Parity

- [ ] Add desktop-only integration features behind explicit capability flags.
- [ ] Add LangChain local sidecar and evidence interaction parity with web UX.
- [ ] Add contract-regression tests for desktop bundle loading.
