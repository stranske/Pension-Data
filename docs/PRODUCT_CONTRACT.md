# Product contract — stranske/Pension-Data
_First draft generated 2026-09-20 from the audit scorecard; the repo owns this file from now on. A PR that adds a user-facing route, command or page adds a line here. The audit's Phase 1.5 scores every line below and prints any surface not listed as UNSCORED._

## Purpose
Extract, normalize and validate pension-report facts for pilot runs, analytics and browser workspace review.

## Primary journey
Run PDF pilot → inspect staged and published facts → view plan analytics → build and serve workspace bundle.

## Core functions
| id | a user can … and sees … | entry point | probe (how to exercise it; vary these determinants) | status 2026-09-20 |
|---|---|---|---|---|
| F1 | an analyst can ingest a pension PDF and sees staged/published facts, coverage and ledger artifacts | one-pdf-pilot CLI | run synthetic vs CalPERS; compare funded ratio, plan ID, provenance, staged/published facts, coverage and ledger | WORKS |
| F2 | an analyst can run plan analytics and sees funding trend or metric-history rows keyed by entity | library and saved-view/metric-history API | run pilots for two plans beneath one output root; set `PENSION_DATA_QUERY_ARTIFACT_ROOT` to that root; query both entity IDs and diff rows | WORKS |
| F3 | an analyst can build a report workspace and sees per-plan metric rows with provenance | workspace build and local serve scripts | build bundles from both runs; compare entity, funded ratio, data_origin and row evidence | WORKS |

## Known gaps at draft time
- None for F2's deterministic saved-view and metric-history path. In proprietary mode the operator must set `PENSION_DATA_QUERY_ARTIFACT_ROOT` to the one-PDF pilot output root; the API fails closed rather than falling back to fixtures.
