"""LangSmith tracing helpers.

This module was at 34% coverage, and its own design is why: the context manager disables itself
whenever `PYTEST_CURRENT_TEST` is set, so every path below that guard is unreachable from an
ordinary test. The guard is right — a test run must not emit traces to a real project — but it
means the code it protects is only testable through the `env` parameter each helper accepts.
These tests use it, which is what that parameter is for.

The property that matters most is that **tracing never breaks the operation it observes**. Every
`except Exception: yield None` in here encodes it: a missing package, a LangSmith client that
raises on construction, an older `langchain_core` with a different signature — none of them may
turn an NL query into a traceback. A regression there fails a working query because its
observability side channel was misconfigured, which is the worst possible trade.
"""

from __future__ import annotations

import contextlib
import sys
import types

import pytest

from pension_data.langchain import tracing

_OPT_IN = {
    "LANGSMITH_API_KEY": "test-key",
    "PENSION_DATA_LANGSMITH_TRACE_TESTS": "1",
}


# ---------------------------------------------------------------------------------------------
# configure_langsmith_env
# ---------------------------------------------------------------------------------------------


def test_no_api_key_leaves_the_environment_untouched():
    """Enabling `LANGCHAIN_TRACING_V2` without a key makes every call try, and fail, to export."""
    env: dict[str, str] = {}
    assert tracing.configure_langsmith_env(env) is False
    assert env == {}


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_a_blank_api_key_counts_as_no_key(blank):
    env = {"LANGSMITH_API_KEY": blank}
    assert tracing.configure_langsmith_env(env) is False
    assert "LANGCHAIN_TRACING_V2" not in env


def test_a_configured_key_is_stripped_before_it_is_exported():
    """A trailing newline from a secret file becomes an auth header that fails opaquely."""
    env = {"LANGSMITH_API_KEY": "  key-with-space  \n"}
    assert tracing.configure_langsmith_env(env) is True
    assert env["LANGCHAIN_API_KEY"] == "key-with-space"
    assert env["LANGCHAIN_TRACING_V2"] == "true"


def test_the_langchain_variable_is_accepted_as_a_fallback():
    env = {"LANGCHAIN_API_KEY": "legacy-key"}
    assert tracing.configure_langsmith_env(env) is True
    assert env["LANGCHAIN_API_KEY"] == "legacy-key"


def test_langsmith_wins_when_both_variables_are_set():
    env = {"LANGSMITH_API_KEY": "primary", "LANGCHAIN_API_KEY": "stale"}
    assert tracing.configure_langsmith_env(env) is True
    assert env["LANGCHAIN_API_KEY"] == "primary"


def test_an_explicit_tracing_setting_is_not_overridden():
    """`setdefault`, not assignment. An operator who turned tracing off keeps it off, even with a
    key configured — otherwise the only way to disable it would be to delete the credential."""
    env = {"LANGSMITH_API_KEY": "key", "LANGCHAIN_TRACING_V2": "false"}
    assert tracing.configure_langsmith_env(env) is True
    assert env["LANGCHAIN_TRACING_V2"] == "false"


# ---------------------------------------------------------------------------------------------
# resolve_trace_url — four shapes of run object across LangSmith versions.
# ---------------------------------------------------------------------------------------------


def test_a_string_url_attribute_is_returned():
    assert tracing.resolve_trace_url(types.SimpleNamespace(url="https://smith/1")) == (
        "https://smith/1"
    )


def test_a_callable_url_attribute_is_invoked():
    class Run:
        def url(self) -> str:
            return "https://smith/2"

    assert tracing.resolve_trace_url(Run()) == "https://smith/2"


@pytest.mark.parametrize("method_name", ["get_url", "get_run_url"])
def test_the_accessor_methods_are_tried_in_turn(method_name):
    run = types.SimpleNamespace(url=None, **{method_name: lambda: "https://smith/3"})
    assert tracing.resolve_trace_url(run) == "https://smith/3"


def test_an_accessor_that_needs_arguments_is_skipped_not_raised():
    """A run object from a newer client may take arguments here. Returning no URL degrades the
    log line; propagating a TypeError takes down the query that produced the run."""

    class Run:
        def url(self, required):  # pragma: no cover - signature is the point
            return "never reached"

    assert tracing.resolve_trace_url(Run()) is None


@pytest.mark.parametrize(
    "run",
    [None, types.SimpleNamespace(), types.SimpleNamespace(url=None), types.SimpleNamespace(url="")],
)
def test_a_run_with_no_usable_url_resolves_to_none(run):
    assert tracing.resolve_trace_url(run) is None


# ---------------------------------------------------------------------------------------------
# langsmith_tracing_context — the guard, and what it protects.
# ---------------------------------------------------------------------------------------------


def test_a_test_run_is_not_traced_even_with_a_key_configured():
    """The whole reason this module is hard to cover, and it must stay this way: a suite run must
    never export runs to a real LangSmith project."""
    env = {"PYTEST_CURRENT_TEST": "tests/x.py::test_y", "LANGSMITH_API_KEY": "real-key"}
    with tracing.langsmith_tracing_context(env=env) as run:
        assert run is None


def test_the_test_guard_can_be_opted_out_of_deliberately():
    """`PENSION_DATA_LANGSMITH_TRACE_TESTS` is the escape hatch. Without it working, the code
    below the guard could never be exercised at all — a gate with no drain."""
    env = dict(_OPT_IN, PYTEST_CURRENT_TEST="tests/x.py::test_y")
    with (
        _fake_langsmith(lambda name, **kwargs: _yielding("RUN")),
        tracing.langsmith_tracing_context(env=env) as run,
    ):
        assert run == "RUN"


def test_without_a_key_the_context_is_a_no_op():
    with tracing.langsmith_tracing_context(env={}) as run:
        assert run is None


# ---------------------------------------------------------------------------------------------
# ...and the failures it must swallow.
# ---------------------------------------------------------------------------------------------


@contextlib.contextmanager
def _yielding(value):
    yield value


@contextlib.contextmanager
def _fake_langsmith(trace_impl, *, tracers=...):
    """Install a stand-in `langsmith` (and optionally `langchain_core.tracers.context`)."""
    module = types.ModuleType("langsmith")
    module.run_helpers = types.SimpleNamespace(trace=trace_impl)
    keys = ["langsmith"]
    saved = {"langsmith": sys.modules.get("langsmith")}
    sys.modules["langsmith"] = module
    if tracers is not ...:
        keys.append("langchain_core.tracers.context")
        saved["langchain_core.tracers.context"] = sys.modules.get("langchain_core.tracers.context")
        sys.modules["langchain_core.tracers.context"] = tracers
    try:
        yield
    finally:
        for key in keys:
            if saved[key] is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = saved[key]


def test_a_langsmith_client_that_raises_does_not_reach_the_caller():
    """`run_helpers.trace(...)` can raise on a bad project name or an unreachable endpoint."""

    def exploding(*args, **kwargs):
        raise RuntimeError("langsmith is unreachable")

    with (
        _fake_langsmith(exploding),
        tracing.langsmith_tracing_context(env=dict(_OPT_IN)) as run,
    ):
        assert run is None


def test_a_missing_langsmith_package_does_not_reach_the_caller():
    module = types.ModuleType("langsmith")  # no run_helpers attribute
    saved = sys.modules.get("langsmith")
    sys.modules["langsmith"] = module
    try:
        with tracing.langsmith_tracing_context(env=dict(_OPT_IN)) as run:
            assert run is None
    finally:
        if saved is None:
            sys.modules.pop("langsmith", None)
        else:
            sys.modules["langsmith"] = saved


def test_the_run_is_yielded_when_the_tracer_context_is_unavailable():
    """Without `langchain_core`, LangSmith's own context still runs — degraded, not disabled."""
    with (
        _fake_langsmith(lambda name, **kwargs: _yielding("RUN"), tracers=None),
        tracing.langsmith_tracing_context(env=dict(_OPT_IN)) as run,
    ):
        assert run == "RUN"


def test_the_project_name_is_passed_to_both_the_run_and_the_tracer():
    """Two independent consumers of the same setting. A trace recorded against the right project
    but enabled against the default one lands in a project nobody looks at."""
    calls: dict[str, object] = {}

    @contextlib.contextmanager
    def trace(name, **kwargs):
        calls["trace"] = kwargs
        yield "RUN"

    @contextlib.contextmanager
    def tracing_v2_enabled(**kwargs):
        calls["v2"] = kwargs
        yield

    tracers = types.ModuleType("langchain_core.tracers.context")
    tracers.tracing_v2_enabled = tracing_v2_enabled

    env = dict(_OPT_IN, LANGCHAIN_PROJECT="pension-data-nl")
    with (
        _fake_langsmith(trace, tracers=tracers),
        tracing.langsmith_tracing_context(env=env) as run,
    ):
        assert run == "RUN"

    assert calls["trace"]["project_name"] == "pension-data-nl"
    assert calls["v2"] == {"project_name": "pension-data-nl"}


def test_an_older_tracer_that_rejects_the_project_keyword_still_enables_tracing():
    """The compatibility shim. Losing it turns an old `langchain_core` into a TypeError escaping
    from a context manager whose whole contract is that it cannot fail."""
    calls: list[dict[str, object]] = []

    @contextlib.contextmanager
    def old_signature(**kwargs):
        if kwargs:
            raise TypeError("tracing_v2_enabled() got an unexpected keyword argument")
        calls.append(kwargs)
        yield

    tracers = types.ModuleType("langchain_core.tracers.context")
    tracers.tracing_v2_enabled = old_signature

    env = dict(_OPT_IN, LANGCHAIN_PROJECT="pension-data-nl")
    with (
        _fake_langsmith(lambda name, **kwargs: _yielding("RUN"), tracers=tracers),
        tracing.langsmith_tracing_context(env=env) as run,
    ):
        assert run == "RUN"

    assert calls == [{}]


def test_the_langsmith_project_variable_is_accepted_too():
    calls: dict[str, object] = {}

    @contextlib.contextmanager
    def trace(name, **kwargs):
        calls.update(kwargs)
        yield "RUN"

    env = dict(_OPT_IN, LANGSMITH_PROJECT="from-langsmith-var")
    with _fake_langsmith(trace, tracers=None), tracing.langsmith_tracing_context(env=env):
        pass

    assert calls["project_name"] == "from-langsmith-var"


def test_the_run_name_and_type_reach_the_tracer():
    calls: dict[str, object] = {}

    @contextlib.contextmanager
    def trace(name, **kwargs):
        calls["name"] = name
        calls.update(kwargs)
        yield "RUN"

    with (
        _fake_langsmith(trace, tracers=None),
        tracing.langsmith_tracing_context(
            name="custom_operation",
            run_type="tool",
            inputs={"question": "how many plans"},
            metadata={"caller": "test"},
            env=dict(_OPT_IN),
        ),
    ):
        pass

    assert calls["name"] == "custom_operation"
    assert calls["run_type"] == "tool"
    assert calls["inputs"] == {"question": "how many plans"}
    assert calls["metadata"] == {"caller": "test"}


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1", True),
        ("TRUE", True),
        ("Yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("", False),
        ("maybe", False),
    ],
)
def test_the_opt_in_flag_accepts_the_usual_truthy_spellings(value, expected):
    """An operator who writes `TRACE_TESTS=true` and gets silence has no way to tell whether the
    flag was rejected or tracing simply produced nothing."""
    env = dict(
        _OPT_IN,
        PYTEST_CURRENT_TEST="tests/x.py::test_y",
        PENSION_DATA_LANGSMITH_TRACE_TESTS=value,
    )
    with (
        _fake_langsmith(lambda name, **kwargs: _yielding("RUN"), tracers=None),
        tracing.langsmith_tracing_context(env=env) as run,
    ):
        assert (run == "RUN") is expected


# ---------------------------------------------------------------------------------------------
# Two failures that used to reach the caller. Both are regressions worth naming.
# ---------------------------------------------------------------------------------------------


def test_a_tracer_that_fails_on_entry_does_not_reach_the_caller():
    """Entering the trace is where LangSmith first contacts its backend, so it fails far more
    often than constructing it — and only the construction used to be guarded.

    The symptom was concrete: `langsmith` installed without `langchain-core` raises
    `ImportError: RunTree.from_runnable_config requires langchain-core` from `__enter__`, taking
    down the query it was only supposed to observe.
    """

    class EntryFails:
        def __enter__(self):
            raise RuntimeError("langsmith backend unreachable")

        def __exit__(self, *exc_info):
            return False

    with (
        _fake_langsmith(lambda *args, **kwargs: EntryFails()),
        tracing.langsmith_tracing_context(env=dict(_OPT_IN)) as run,
    ):
        assert run is None


def test_a_tracer_that_fails_on_exit_does_not_reach_the_caller():
    """The run is submitted on exit, so a network failure there is ordinary."""

    class ExitFails:
        def __enter__(self):
            return "RUN"

        def __exit__(self, *exc_info):
            raise RuntimeError("failed to submit run")

    with (
        _fake_langsmith(lambda *args, **kwargs: ExitFails(), tracers=None),
        tracing.langsmith_tracing_context(env=dict(_OPT_IN)) as run,
    ):
        assert run == "RUN"


def test_the_callers_own_typeerror_is_not_swallowed_by_the_compatibility_shim():
    """The `project_name` keyword probe must wrap the tracer entry, not the caller's body.

    Wrapping the body means a `TypeError` raised by the caller is caught, the generator yields a
    second time, and the caller receives `RuntimeError: generator didn't stop after throw()` —
    their real error replaced by one from the tracing helper, with a traceback pointing here.
    """
    calls: list[dict[str, object]] = []

    @contextlib.contextmanager
    def tracing_v2_enabled(**kwargs):
        calls.append(kwargs)
        yield

    tracers = types.ModuleType("langchain_core.tracers.context")
    tracers.tracing_v2_enabled = tracing_v2_enabled

    env = dict(_OPT_IN, LANGCHAIN_PROJECT="pension-data-nl")
    bodies_run = 0
    with (
        _fake_langsmith(lambda name, **kwargs: _yielding("RUN"), tracers=tracers),
        pytest.raises(TypeError, match="the caller's own bug"),
        tracing.langsmith_tracing_context(env=env),
    ):
        bodies_run += 1
        raise TypeError("the caller's own bug")

    assert bodies_run == 1  # not re-entered
    assert calls == [{"project_name": "pension-data-nl"}]


def test_an_error_in_the_body_still_reaches_the_tracer_so_the_run_records_it():
    """Swallowing the exception on the way out would leave a trace claiming the run succeeded."""
    seen: list[object] = []

    class Recording:
        def __enter__(self):
            return "RUN"

        def __exit__(self, exc_type, exc, tb):
            seen.append(exc_type)
            return False

    with (
        _fake_langsmith(lambda *args, **kwargs: Recording(), tracers=None),
        pytest.raises(ValueError),
        tracing.langsmith_tracing_context(env=dict(_OPT_IN)),
    ):
        raise ValueError("query failed")

    assert seen == [ValueError]


def test_a_tracer_failing_on_exit_never_swallows_the_error_it_was_recording():
    """The dangerous corner where both go wrong at once.

    Guarding the tracer's exit means returning *something* from a handler that runs while the
    caller's exception is in flight. Returning True there suppresses it — a failed query reported
    as a success because its trace failed to upload. Only False is safe, and it was the one branch
    no test reached until a deliberate break found it.
    """

    class ExitFailsWhileRecording:
        def __enter__(self):
            return "RUN"

        def __exit__(self, *exc_info):
            raise RuntimeError("failed to submit run")

    with (
        _fake_langsmith(lambda *args, **kwargs: ExitFailsWhileRecording(), tracers=None),
        pytest.raises(ValueError, match="query failed"),
        tracing.langsmith_tracing_context(env=dict(_OPT_IN)),
    ):
        raise ValueError("query failed")
