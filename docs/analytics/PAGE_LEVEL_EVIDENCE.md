# Page-Level Evidence and Provenance Linkage

Issue: #23

## Scope

This module slice adds structured evidence modeling and stable linkage from core metric rows to source evidence references.

## Evidence Model

`src/pension_data/db/models/provenance.py` defines:

- `EvidenceReference`
  - `report_id`
  - `source_document_id`
  - page/section/snippet metadata (`page_number`, `section_hint`, `snippet_anchor`)
  - canonical `raw_ref`
  - stable `evidence_ref_id`
  - `excerpt` (optional) — the quoted supporting text for a fact
  - `method` (optional) — the extraction path: `"table" | "text" | "fallback" | "ocr" | "llm"`
- `MetricEvidenceLink`
  - stable `link_id`
  - `metric_row_id`
  - `metric_family`
  - `metric_name`
  - `evidence_ref_id` foreign key
  - `confidence` (optional) — per-link confidence, independent of the fact's own
    confidence, so two evidence sources for one fact can carry differing scores

### `excerpt` / `method` enrichment

`excerpt` and `method` are **optional** (default `None`) so the many frozen
dataclasses that construct `EvidenceReference` keep compiling. They are
deliberately **excluded from `evidence_ref_id`**: `evidence_ref_id` is computed
by `stable_id("evidence", report_id, source_document_id, normalized_ref,
page_number, section_hint, snippet_anchor)`, so enriching an existing locator
with an excerpt or method never changes its stable identity.

`build_evidence_reference(...)` accepts optional `excerpt=` and `method=`
keyword arguments. When `method` is not supplied it is **inferred from the
canonical anchor form**: `table:` anchors → `"table"`, `text:` anchors and page
locators → `"text"`, and parser table locators such as `p.40#table` →
`"table"`. Free-form section hints leave `method` unset for the caller to
override. This makes table-derived metrics surface `method="table"` and
text-block metrics surface `method="text"` automatically.

`LLM`-sourced excerpts must originate from the deterministic parser's source
text — do not add a network LLM call to produce excerpts.

## Parser Hooks

`src/pension_data/extract/common/evidence.py` provides:

- `text_block_evidence_ref(...)`
- `table_evidence_ref(...)`
- `canonicalize_evidence_ref(...)`
- `build_evidence_reference(...)`

The funded/actuarial parser now uses table/text evidence hook helpers to keep references canonical and deterministic.

## Core Metric Linkage

`src/pension_data/provenance/metrics.py` builds deterministic artifacts from core metric facts:

- `evidence_references`
- `metric_evidence_links`
- validation warnings

High-impact metric families (`funded`, `actuarial`, `allocation`, `holding`, `fee`) fail validation when evidence refs are missing in strict mode.

## Citation Export

`src/pension_data/provenance/export.py` exports citation-ready payloads grouped by `metric_row_id`, with stable source locators:

- `source_document_id#page=<n>`
- `source_document_id#anchor=<snippet>`
- `source_document_id#section=<hint>`

Each citation entry also surfaces the new enrichment fields: `excerpt`,
`method`, and the per-link `confidence`.
