# Reviewable Findings Artifact

The first static UI and LangChain artifact slice is `extraction_quality_dashboard`.

- Contract: `docs/data/reviewable-findings/findings.schema.json`
- Published artifact path: `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- Python contract helper: `pension_data.langchain.review_artifact.reviewable_findings_schema()`
- Validator: `pension_data.langchain.review_artifact.validate_reviewable_findings_artifact(...)`

The artifact is generated from existing extraction persistence and readiness outputs, not from
hand-authored UI rows. The checked-in artifact at the published path provides a stable machine-
readable target for static UI hosting and reviewer workflows while generator wiring is finalized.

Required finding rows include `entity`, `period`, `metric_family`, `metric`, `value`, `confidence`,
`provenance_refs`, and `citations`. These are the minimum fields the static UI can filter/render and
the LangChain explain/compare chains can cite without repo-local execution.

LangChain actions are asynchronous. The artifact advertises `explain` and `compare` request shapes;
workflow or comment-driven runs write result artifacts back with request id, timestamp, summary,
citations, and an output artifact path.
