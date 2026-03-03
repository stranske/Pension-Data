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
