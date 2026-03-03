# Electron Compatibility Track (Issue #119)

Tauri is the default desktop shell. Electron is maintained as a compatibility track, not the default.

## When to Consider Electron

Use Electron only when one or more of these conditions are true:

- Required third-party SDK is Electron-only.
- A mandatory desktop capability cannot be implemented in Tauri within acceptable risk/timeline.
- Benchmark data shows material UX benefit for a core workflow that Tauri cannot match.

## Decision Criteria

Before switching default shell for a workflow, capture:

- Functional gap statement (what Tauri cannot currently satisfy).
- Security and compliance review impact.
- Startup/memory/render benchmark deltas from `apps/mac-desktop/benchmarks/`.
- Operational impact (packaging, update channel, signing/notarization overhead).

## Operating Model

- Keep shared UI + runtime data contract identical across Tauri/Electron shells.
- Treat Electron as opt-in profile.
- Re-evaluate shell default each release using benchmark report and unresolved capability gaps.
