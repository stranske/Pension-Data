# macOS Tauri Packaging Setup

## Prerequisites

- macOS with Xcode Command Line Tools.
- Rust toolchain (`rustup`) installed.
- Node.js 20+.

## Build Steps

```bash
cd apps/mac-desktop
npm install
npm run tauri:build
```

Build output is produced under `apps/mac-desktop/src-tauri/target/release/bundle/macos/`.

## Operator Checklist

- Confirm runtime contract check passes (`npm run validate:contract`).
- Confirm web UI sync completed (`npm run sync:web-ui`).
- Confirm benchmark report is updated when shell/runtime changes (`npm run bench:shells`).

## Release Steps

1. Build unsigned `.app` bundle using `npm run tauri:build`.
2. Execute smoke test with representative workspace bundle.
3. Package internal distribution artifact.
4. (Optional) Add codesign and notarization for broad distribution.

## CI Workflow

Use `.github/workflows/mac-desktop-tauri.yml` (manual dispatch) to run validation-only or full bundle build on macOS runner.
