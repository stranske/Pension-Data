"""LangSmith tracing helpers with explicit opt-in env gating."""

from __future__ import annotations

import os
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from typing import Any, Literal

_TRUTHY = {"1", "true", "yes", "on"}


def _env_truthy(env: MutableMapping[str, str], key: str) -> bool:
    value = env.get(key)
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY


def configure_langsmith_env(env: MutableMapping[str, str] | None = None) -> bool:
    """Enable tracing env vars only when an API key is configured."""
    active_env = os.environ if env is None else env
    raw_api_key = active_env.get("LANGSMITH_API_KEY") or active_env.get("LANGCHAIN_API_KEY")
    if raw_api_key is None:
        return False
    api_key = raw_api_key.strip()
    if not api_key:
        return False
    active_env["LANGCHAIN_API_KEY"] = api_key
    active_env.setdefault("LANGCHAIN_TRACING_V2", "true")
    return True


def resolve_trace_url(run: Any) -> str | None:
    """Resolve a trace URL from a LangSmith run-like object."""
    if run is None:
        return None
    url_attr = getattr(run, "url", None)
    if isinstance(url_attr, str) and url_attr:
        return url_attr
    if callable(url_attr):
        try:
            value = url_attr()
        except TypeError:
            value = None
        if isinstance(value, str) and value:
            return value
    for method_name in ("get_url", "get_run_url"):
        method = getattr(run, method_name, None)
        if not callable(method):
            continue
        try:
            value = method()
        except TypeError:
            value = None
        if isinstance(value, str) and value:
            return value
    return None


@contextmanager
def langsmith_tracing_context(
    *,
    name: str = "pension_data_nl_operation",
    run_type: Literal[
        "retriever", "llm", "tool", "chain", "embedding", "prompt", "parser"
    ] = "chain",
    inputs: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    env: MutableMapping[str, str] | None = None,
) -> Iterator[Any]:
    """Return a no-op context unless LangSmith keys are configured."""
    active_env = os.environ if env is None else env
    if active_env.get("PYTEST_CURRENT_TEST") and not _env_truthy(
        active_env, "PENSION_DATA_LANGSMITH_TRACE_TESTS"
    ):
        yield None
        return
    if not configure_langsmith_env(active_env):
        yield None
        return
    try:
        from langsmith import run_helpers
    except Exception:
        yield None
        return

    project = active_env.get("LANGCHAIN_PROJECT") or active_env.get("LANGSMITH_PROJECT")
    try:
        trace_cm = run_helpers.trace(
            name,
            run_type=run_type,
            inputs=inputs,
            metadata=metadata,
            project_name=project,
        )
    except Exception:
        yield None
        return

    try:
        from langchain_core.tracers.context import tracing_v2_enabled
    except Exception:
        with trace_cm as run:
            yield run
        return

    with trace_cm as run:
        if project:
            try:
                with tracing_v2_enabled(project_name=project):
                    yield run
            except TypeError:
                with tracing_v2_enabled():
                    yield run
        else:
            with tracing_v2_enabled():
                yield run
