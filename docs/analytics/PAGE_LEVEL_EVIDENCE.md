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
- `MetricEvidenceLink`
  - stable `link_id`
  - `metric_row_id`
  - `metric_family`
  - `metric_name`
  - `evidence_ref_id` foreign key

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
