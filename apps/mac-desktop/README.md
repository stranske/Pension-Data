# Pension-Data Mac Desktop Shell

Tauri-first desktop packaging track with Electron compatibility path.

## Goals

- Keep desktop and web on the same runtime data contract.
- Enable a macOS `.app` packaging path for power-user workflows.
- Preserve Electron as compatibility fallback, not default.

## Stack

- Default shell: Tauri (`apps/mac-desktop/src-tauri/`)
- Compatibility shell: Electron (`apps/mac-desktop/electron/`)
- Shared UI assets: synced from `apps/web` into `apps/mac-desktop/src-ui/`
- Shared runtime contract: `apps/contracts/runtime-contract.json`

## Commands

```bash
cd apps/mac-desktop
npm install
npm run sync:web-ui
npm run validate:contract
npm run tauri:build
```

Optional compatibility run:

```bash
npm run electron:dev
```

Benchmark report:

```bash
npm run bench:shells
```

## References

- Tauri packaging setup: `docs/deploy/MAC_DESKTOP_TAURI_SETUP.md`
- Electron compatibility criteria: `docs/ux/ELECTRON_COMPATIBILITY_TRACK.md`
- Shell benchmark output: `apps/mac-desktop/benchmarks/latest_report.md`
