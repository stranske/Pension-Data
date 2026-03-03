# Mac Desktop Implementation Plan

## Phase 1: Bootstrap

- Create Tauri shell and React UI workspace.
- Add app-level routing, shell layout, and design tokens.
- Wire file-based findings loader from local exported bundle.

## Phase 2: Findings Experience

- Add dashboard cards for extraction health, unresolved anomalies, and confidence distribution.
- Add faceted findings explorer (entity, period, metric family, severity).
- Add provenance panel with source links and evidence snippets.

## Phase 3: LangChain Interaction

- Add local FastAPI sidecar endpoint for question-answer flows.
- Add deterministic prompt/output schema for answer + evidence citations.
- Add guardrails for unsupported SQL and data-exfiltration patterns.

## Phase 4: Packaging

- Add reproducible build script for `.app` output.
- Add release checklist (codesign/notarization optional for internal use).
