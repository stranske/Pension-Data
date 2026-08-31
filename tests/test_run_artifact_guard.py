"""Tests for the session guard that keeps run artifacts out of the checkout."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.conftest import _run_artifact_watch_paths

from pension_data.langchain.observability import default_nl_log_path
from pension_data.query.run_record import default_run_record_root


def _checkout() -> Path:
    return default_run_record_root().parent.resolve()


def test_the_run_record_surfaces_in_the_checkout_are_watched() -> None:
    """Assert the positive case, so an empty watch list cannot pass as a pass.

    The filter in ``_run_artifact_watch_paths`` excludes paths outside the
    checkout. A test that only checked for absences would still pass if that
    filter ever excluded everything -- a guard watching nothing reports no
    leaks, which is indistinguishable from a clean run.
    """
    watched = _run_artifact_watch_paths()
    root = default_run_record_root()

    assert watched, "the guard must watch something, or it silently stops guarding"
    assert root / "langchain" / "nl_runs" in watched
    assert root / "query" / "sql_runs" in watched
    assert default_nl_log_path() in watched


def test_an_nl_log_override_outside_the_checkout_is_not_watched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An external log destination is configuration, not checkout pollution.

    ``PENSION_DATA_NL_LOG_PATH`` can point anywhere. Watching it would fail the
    session over a write the operator asked for, and say the file leaked into a
    checkout it was never in.
    """
    external = tmp_path / "outside" / "nl_operations.jsonl"
    monkeypatch.setenv("PENSION_DATA_NL_LOG_PATH", str(external))

    watched = _run_artifact_watch_paths()

    assert default_nl_log_path() == external
    assert not external.resolve().is_relative_to(_checkout())
    assert external not in watched
    # The run-record surfaces are unaffected by the log override.
    assert default_run_record_root() / "langchain" / "nl_runs" in watched


def test_an_nl_log_override_inside_the_checkout_is_still_watched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The filter turns on location, not on whether an override was used.

    A relative override resolves against the working directory, so an operator
    pointing the log somewhere else *within* the checkout still gets the guard.
    """
    inside = _checkout() / "artifacts" / "langchain" / "elsewhere.jsonl"
    monkeypatch.setenv("PENSION_DATA_NL_LOG_PATH", str(inside))

    assert inside in _run_artifact_watch_paths()
