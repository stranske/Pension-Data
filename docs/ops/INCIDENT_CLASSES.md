# Incident Classes

Last reviewed: 2026-03-02

This document defines the top incident classes for ingestion and extraction
quality operations. Each class includes a canonical identifier used in pipeline
outputs and a runbook link for deterministic remediation.

| Incident class ID | Description | Primary runbook |
|---|---|---|
| `source_map_breakage` | Source map schema/lint failures or unreachable source-map seeds after config changes | [Source Map Breakage](../runbooks/source-map-breakage.md#source-map-breakage) |
| `revised_file_mismatch` | Revised annual report appears but supersession or period matching fails | [Revised File Mismatch](../runbooks/revised-file-mismatch.md#revised-file-mismatch) |
| `parser_fallback_exhaustion` | Primary parser and all fallbacks fail to extract required fields | [Parser Fallback Exhaustion](../runbooks/parser-fallback-exhaustion.md#parser-fallback-exhaustion) |
| `anomaly_flood` | Sudden spike of anomaly events overwhelms review queue | [Anomaly Flood](../runbooks/anomaly-flood.md#anomaly-flood) |

