"""The one-PDF pilot CLI, exercised in-process.

`scripts/run_one_pdf_pilot.py` is a two-line shim over `one_pdf_pilot_cli:main`, and the only test
touching it ran the script as a SUBPROCESS — which means none of this module's behaviour was
observed by the test suite at all. It sat at 0% coverage while looking tested.

Three properties matter more than the happy path:

* A failed pilot must return 1. The `except Exception` arm is what stands between a crash and a
  silent success; if it ever returned 0, a run that parsed nothing would report done, and the
  caller writing the manifest downstream would never know.
* Every flag documents an `$ONE_PDF_PILOT_*` alternative in its own help string. A documented
  variable nobody reads is worse than no variable, so the help text is checked against the
  contract that defines the names.
* A flag must beat the environment. The reverse makes a stale exported variable silently override
  what the operator typed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.ops import one_pdf_pilot_cli as cli
from pension_data.ops.one_pdf_pilot import one_pdf_pilot_input_contract

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE = _REPO_ROOT / "tests" / "parser" / "fixtures" / "calpers_fy2024_excerpt.pdf"

_REQUIRED = (
    "--plan-id",
    "CA-PERS",
    "--plan-period",
    "FY2024",
    "--effective-date",
    "2024-06-30",
    "--ingestion-date",
    "2026-01-01",
)


def _run(monkeypatch, capsys, *argv: str) -> tuple[int, str, str]:
    monkeypatch.setattr("sys.argv", ["run_one_pdf_pilot.py", *argv])
    code = cli.main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


@pytest.fixture(autouse=True)
def no_leaked_pilot_env(monkeypatch):
    """The CLI reads a dozen `ONE_PDF_PILOT_*` variables; an inherited one would decide a test."""
    for env_var in one_pdf_pilot_input_contract()["env_var_by_field"].values():
        monkeypatch.delenv(str(env_var), raising=False)


# ---------------------------------------------------------------------------------------------
# The documented command.
# ---------------------------------------------------------------------------------------------


def test_the_documented_invocation_emits_a_manifest_and_a_backplane_run(
    tmp_path, monkeypatch, capsys
):
    output_root = tmp_path / "documented"
    code, out, err = _run(
        monkeypatch,
        capsys,
        "--pdf-path",
        str(_FIXTURE),
        *_REQUIRED,
        "--output-root",
        str(output_root),
        "--run-id",
        "pilot-cli-test",
    )

    assert code == 0, err
    result = json.loads(out)
    assert result["run_id"] == "pilot-cli-test"
    assert Path(result["run_manifest_json"]).is_file()
    assert Path(result["backplane_run_json"]).is_file()
    assert Path(result["backplane_manifest_json"]).is_file()


def test_the_backplane_paths_are_added_to_the_pilot_result_not_printed_separately(
    tmp_path, monkeypatch, capsys
):
    """The caller parses one JSON object from stdout. Emitting the backplane paths as a second
    document, or omitting them, breaks every consumer that reads the run manifest."""
    code, out, _ = _run(
        monkeypatch,
        capsys,
        "--pdf-path",
        str(_FIXTURE),
        *_REQUIRED,
        "--output-root",
        str(tmp_path / "single"),
    )

    assert code == 0
    result = json.loads(out)  # one object, not a stream
    assert {"backplane_run_json", "backplane_manifest_json"} <= set(result)


# ---------------------------------------------------------------------------------------------
# Failure.
# ---------------------------------------------------------------------------------------------


def test_a_missing_pdf_exits_nonzero_and_says_what_failed(tmp_path, monkeypatch, capsys):
    """Returning 0 here would report a successful pilot for a run that read nothing."""
    code, out, err = _run(
        monkeypatch,
        capsys,
        "--pdf-path",
        str(tmp_path / "absent.pdf"),
        *_REQUIRED,
        "--output-root",
        str(tmp_path / "failed"),
    )

    assert code == 1
    assert out == ""  # no half-written result for a caller to parse
    assert "one-pdf pilot failed" in err
    assert "FileNotFoundError" in err
    assert "absent.pdf" in err


def test_a_missing_required_field_is_refused_before_any_work(tmp_path, monkeypatch, capsys):
    """No `--plan-id` and no environment fallback: the run must not start and invent one."""
    output_root = tmp_path / "incomplete"
    code, out, err = _run(
        monkeypatch,
        capsys,
        "--pdf-path",
        str(_FIXTURE),
        "--plan-period",
        "FY2024",
        "--effective-date",
        "2024-06-30",
        "--ingestion-date",
        "2026-01-01",
        "--output-root",
        str(output_root),
    )

    assert code == 1
    assert out == ""
    assert "plan_id" in err
    assert not output_root.exists()


# ---------------------------------------------------------------------------------------------
# The environment fallbacks the help text promises.
# ---------------------------------------------------------------------------------------------


def test_every_field_in_the_contract_names_its_variable_in_the_help_text():
    """A `$ONE_PDF_PILOT_*` name that drifts from the contract documents a variable nobody reads."""
    help_text = cli._build_parser().format_help()
    env_vars = one_pdf_pilot_input_contract()["env_var_by_field"]
    assert isinstance(env_vars, dict)

    missing = [str(name) for name in env_vars.values() if f"${name}" not in help_text]
    assert not missing, f"documented nowhere in --help: {missing}"


def test_the_run_is_fully_configurable_from_the_environment(tmp_path, monkeypatch, capsys):
    output_root = tmp_path / "from-env"
    for field, value in {
        "PDF_PATH": str(_FIXTURE),
        "PLAN_ID": "CA-PERS",
        "PLAN_PERIOD": "FY2024",
        "EFFECTIVE_DATE": "2024-06-30",
        "INGESTION_DATE": "2026-01-01",
        "OUTPUT_ROOT": str(output_root),
        "RUN_ID": "from-env",
    }.items():
        monkeypatch.setenv(f"ONE_PDF_PILOT_{field}", value)

    code, out, err = _run(monkeypatch, capsys)

    assert code == 0, err
    result = json.loads(out)
    assert result["run_id"] == "from-env"
    assert str(output_root) in result["run_manifest_json"]


def test_a_flag_beats_an_exported_variable(tmp_path, monkeypatch, capsys):
    """The reverse lets a stale export silently override what the operator typed."""
    monkeypatch.setenv("ONE_PDF_PILOT_PDF_PATH", str(_FIXTURE))
    monkeypatch.setenv("ONE_PDF_PILOT_PLAN_ID", "CA-PERS")
    monkeypatch.setenv("ONE_PDF_PILOT_PLAN_PERIOD", "FY2024")
    monkeypatch.setenv("ONE_PDF_PILOT_EFFECTIVE_DATE", "2024-06-30")
    monkeypatch.setenv("ONE_PDF_PILOT_INGESTION_DATE", "2026-01-01")
    monkeypatch.setenv("ONE_PDF_PILOT_RUN_ID", "stale-export")
    monkeypatch.setenv("ONE_PDF_PILOT_OUTPUT_ROOT", str(tmp_path / "stale"))

    code, out, err = _run(
        monkeypatch,
        capsys,
        "--run-id",
        "typed-by-hand",
        "--output-root",
        str(tmp_path / "typed"),
    )

    assert code == 0, err
    result = json.loads(out)
    assert result["run_id"] == "typed-by-hand"
    assert str(tmp_path / "typed") in result["run_manifest_json"]


# ---------------------------------------------------------------------------------------------
# The repo root the backplane emitter is handed.
# ---------------------------------------------------------------------------------------------


def test_the_module_locates_the_checkout_root_not_its_own_package_dir():
    """`_REPO_ROOT` is passed to the backplane emitter, which reads `config/` and `docs/` from it.

    An off-by-one in the `parents[...]` index points it at `src/` — where those directories do not
    exist — and the only symptom is a backplane run that silently omits participant metadata.
    """
    assert cli._REPO_ROOT == _REPO_ROOT
    assert (cli._REPO_ROOT / "config" / "backplane_participants.json").is_file()
