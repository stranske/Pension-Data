# Incident Classes

Last reviewed: 2026-03-02

This document defines the top incident classes for ingestion and extraction
quality operations. Each class uses a canonical identifier in snake_case.
Pipeline failure outputs should report one of these IDs and include the linked
runbook for deterministic remediation.

| Incident class ID | Trigger signal | Primary runbook |
|---|---|---|
| `source_map_breakage` | Source-map lint/schema checks fail or source seed URLs become invalid after config changes | [Source Map Breakage](../runbooks/source-map-breakage.md#source-map-breakage) |
| `revised_file_mismatch` | Revised report is discovered but supersession or plan-period matching cannot reconcile | [Revised File Mismatch](../runbooks/revised-file-mismatch.md#revised-file-mismatch) |
| `parser_fallback_exhaustion` | Primary parser and all fallback stages fail to extract required fields | [Parser Fallback Exhaustion](../runbooks/parser-fallback-exhaustion.md#parser-fallback-exhaustion) |
| `anomaly_flood` | Anomaly event volume spikes and overwhelms queue throughput or alert channels | [Anomaly Flood](../runbooks/anomaly-flood.md#anomaly-flood) |
