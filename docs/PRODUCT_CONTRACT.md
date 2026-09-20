# Product contract — stranske/Pension-Data
_First draft generated 2026-09-20 from the audit scorecard; the repo owns this file from now on. A PR that adds a user-facing route, command or page adds a line here. The audit's Phase 1.5 scores every line below and prints any surface not listed as UNSCORED._

## Purpose
Extract, normalize and validate pension-report facts for pilot runs, analytics and browser workspace review.

## Primary journey
Run PDF pilot → inspect staged and published facts → view plan analytics → build and serve workspace bundle.

## Core functions
| id | a <user> can … and sees … | entry point | probe (how to exercise it; vary these determinants) | status 2026-09-20 |
|---|---|---|---|---|
| F1 | an analyst can ingest a pension PDF and sees staged/published facts, coverage and ledger artifacts | one-pdf-pilot CLI | run synthetic vs CalPERS PDFs; compare funded ratio, plan ID and provenance | WORKS |
| F2 | an analyst can run plan analytics and sees funding trend or metric-history rows keyed by entity | library and saved-view/metric-history API | call library with two plans and query API for CA-PERS vs OTHER-PLAN; diff rows | PARTIAL |
| F3 | an analyst can build a report workspace and sees per-plan metric rows with provenance | workspace build and local serve scripts | build bundles from both pilot runs; compare entity, funded ratio and rows | WORKS |

## Known gaps at draft time
- F2: library analytics respond to inputs, but served API returns CA-PERS fixtures and ignores ingested pilot data.
