"""LangSmith tracing helpers with explicit opt-in env gating."""

from __future__ import annotations

import os
from collections.abc import Iterator, MutableMapping
from contextlib import ExitStack, contextmanager
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


class _QuietExit:
    """Wrap a context manager so its own exit failure cannot replace the caller's exception.

    Submitting a run happens on exit, so a network failure there would otherwise propagate out of
    a helper whose entire contract is that observability cannot break the operation observed.
    Returning False never suppresses the caller's exception — only the tracer's own.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __enter__(self) -> Any:
        return self._inner.__enter__()

    def __exit__(self, *exc_info: Any) -> bool:
        try:
            return bool(self._inner.__exit__(*exc_info))
        except Exception:
            return False


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

    tracer_factory: Any | None
    try:
        from langchain_core.tracers.context import tracing_v2_enabled

        tracer_factory = tracing_v2_enabled
    except Exception:
        tracer_factory = None

    with ExitStack() as stack:
        try:
            run = stack.enter_context(_QuietExit(trace_cm))
        except Exception:
            # Entering the trace is where LangSmith first contacts its backend, so it fails far
            # more often than constructing it. Losing the trace is acceptable; losing the
            # operation being traced is not.
            yield None
            return
        if tracer_factory is not None:
            _enter_tracer(stack, tracer_factory, project)
        yield run


def _enter_tracer(stack: ExitStack, tracing_v2_enabled: Any, project: str | None) -> None:
    """Enter the LangChain tracer, tolerating a client that predates `project_name`.

    The keyword probe belongs HERE, around the entry alone. Wrapping the caller's body in
    `except TypeError` instead swallows the caller's own TypeError and re-yields, which surfaces
    as `RuntimeError: generator didn't stop after throw()` — the caller's error replaced by one
    from the tracing helper.
    """

    attempts = [{"project_name": project}, {}] if project else [{}]
    for kwargs in attempts:
        try:
            stack.enter_context(_QuietExit(tracing_v2_enabled(**kwargs)))
            return
        except TypeError:
            continue
        except Exception:
            return
