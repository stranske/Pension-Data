# LangChain Foundations

This document describes the reusable LangChain foundation layer in `src/pension_data/langchain/`.

## Installation

Install optional runtime dependencies:

```bash
pip install -e ".[langchain]"
```

## Provider Configuration

Use `resolve_provider_config()` and `create_llm()` from
`pension_data.langchain.foundation`.

Supported providers:
- `openai`
- `anthropic`

Provider resolution order:
1. Explicit function arguments
2. Provider-specific env vars
3. Safe defaults

Relevant env vars:
- `PENSION_DATA_LLM_PROVIDER`
- `PENSION_DATA_OPENAI_MODEL`
- `PENSION_DATA_ANTHROPIC_MODEL`
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `ANTHROPIC_API_KEY`
- `ANTHROPIC_BASE_URL`
- `PENSION_DATA_LLM_API_KEY` (global fallback)

## LangSmith Tracing

Use `langsmith_tracing_context()` from `pension_data.langchain.tracing`.

Tracing remains disabled unless either `LANGSMITH_API_KEY` or `LANGCHAIN_API_KEY`
is configured. When enabled, the helper sets `LANGCHAIN_TRACING_V2=true` and bridges
`LANGSMITH_API_KEY` into `LANGCHAIN_API_KEY` when needed.

To allow tracing in tests:
- set `PENSION_DATA_LANGSMITH_TRACE_TESTS=true`

## Prompt + Disclaimer Helpers

Use shared helpers from `pension_data.langchain.prompts`:
- `build_nl_query_system_prompt()`
- `build_findings_explainer_prompt()`
- `append_analytics_disclaimer()`

All findings-explainer outputs should include:

`This is analytical output, not financial advice. Always verify metrics independently.`

## Reviewable Findings Artifact

The first static UI/LangChain artifact slice is `extraction_quality_dashboard`.

- Schema: `docs/data/reviewable-findings/findings.schema.json`
- Published payload path: `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- Contract doc: `docs/contracts/reviewable-findings-artifact-contract.md`

Use `reviewable_findings_schema()` to inspect the machine-readable contract and
`validate_reviewable_findings_artifact(...)` before publishing generated artifacts. Rows must carry
entity, period, metric family, confidence, provenance refs, and citations so explain/compare outputs
remain source-bound.
