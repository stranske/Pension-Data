"""How a fleet record decides what each stage of an NL query did.

These helpers turn one error code into a verdict for four separate stages, and every one of those
verdicts is a claim on a dashboard: `error` sends someone to debug a stage, `skipped` says it never
ran, and `read_only` is a SAFETY claim about a query that reached the database. All of them were
derived by untested code.

The truth table below is written out in full rather than computed, because that is the point — the
mapping is the specification, and generating the expectations from the same ordering the
implementation uses would assert nothing.
"""

from __future__ import annotations

import pytest

from pension_data.observability.langsmith_fleet import (
    _derive_read_only_status,
    _derive_sql_validation_status,
    _error_stage_for,
    _request_max_rows,
    _resolve_run_attr,
    _response_error_code,
    _response_evidence_available,
    _response_latency_ms,
    _response_row_count,
    _response_status,
    _response_trace_event_count,
    _stage_status,
)

_STAGES = ("sql-generation", "validation", "execution", "replay")

# error code -> the verdict for each stage in _STAGES order.
_STAGE_TABLE = {
    None: ("success", "success", "success", "success"),
    "AMBIGUOUS_PROMPT": ("error", "skipped", "skipped", "skipped"),
    "INVALID_REQUEST": ("success", "error", "skipped", "skipped"),
    "UNSAFE_SQL": ("success", "error", "skipped", "skipped"),
    "TIMEOUT": ("success", "success", "error", "skipped"),
    "EXECUTION_ERROR": ("success", "success", "error", "skipped"),
    "MAX_ROWS_EXCEEDED": ("success", "success", "error", "skipped"),
    "SYNTAX_ERROR": ("success", "success", "error", "skipped"),
}


class _Error:
    def __init__(self, code: object) -> None:
        self.code = code


class _Metadata:
    def __init__(self, **fields: object) -> None:
        self.__dict__.update(fields)


class _Response:
    def __init__(self, *, status: object = "ok", code: object = None, **fields: object) -> None:
        self.status = status
        self.error = _Error(code) if code is not None else None
        self.__dict__.update(fields)


# ---------------------------------------------------------------------------------------------
# Stage verdicts.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("error_code,expected", _STAGE_TABLE.items(), ids=lambda v: str(v)[:24])
def test_one_error_code_decides_every_stage(error_code, expected):
    verdicts = tuple(
        _stage_status("success", error_code=error_code, stage=stage, error_stage=None)
        for stage in _STAGES
    )
    assert verdicts == expected


def test_the_failing_stage_is_error_and_everything_after_it_is_skipped():
    """The distinction the dashboard depends on: `skipped` means nobody should go looking there.

    Reporting a later stage as `error` sends an engineer to debug code that never executed.
    """
    assert (
        _stage_status("success", error_code="UNSAFE_SQL", stage="validation", error_stage=None)
        == "error"
    )
    assert (
        _stage_status("success", error_code="UNSAFE_SQL", stage="execution", error_stage=None)
        == "skipped"
    )


def test_a_stage_before_the_failure_keeps_its_own_verdict():
    """SQL generation genuinely succeeded before validation rejected the result."""
    assert (
        _stage_status("success", error_code="UNSAFE_SQL", stage="sql-generation", error_stage=None)
        == "success"
    )
    assert (
        _stage_status("fallback", error_code="TIMEOUT", stage="sql-generation", error_stage=None)
        == "fallback"
    )


def test_an_unrecognised_error_code_marks_every_stage_skipped():
    """The conservative direction, and deliberately so: an error nobody has a stage for means no
    stage can be claimed to have succeeded — including one that plainly ran. Reporting `success`
    on an unknown failure is the mistake worth avoiding here."""
    verdicts = {
        stage: _stage_status(
            "success", error_code="NEW_CODE_FROM_A_LATER_RELEASE", stage=stage, error_stage=None
        )
        for stage in _STAGES
    }
    assert set(verdicts.values()) == {"skipped"}


def test_an_explicit_error_stage_overrides_the_code_lookup():
    """The caller knows where it failed; the code table is only the fallback."""
    assert (
        _stage_status("success", error_code="TIMEOUT", stage="validation", error_stage="validation")
        == "error"
    )
    assert (
        _stage_status("success", error_code="TIMEOUT", stage="execution", error_stage="validation")
        == "skipped"
    )


def test_no_error_code_leaves_every_stage_untouched():
    for stage in _STAGES:
        assert (
            _stage_status("no_secret", error_code=None, stage=stage, error_stage=None)
            == "no_secret"
        )


# ---------------------------------------------------------------------------------------------
# Which stage owns which code.
# ---------------------------------------------------------------------------------------------


def test_an_ambiguous_prompt_is_blamed_on_generation_not_validation():
    """`AMBIGUOUS_PROMPT` is a member of the VALIDATION failure set, and the check for it comes
    first anyway. Order is load-bearing: reordering these branches silently moves the blame to a
    stage that did its job."""
    assert _error_stage_for("AMBIGUOUS_PROMPT") == "sql-generation"


@pytest.mark.parametrize("code", ["INVALID_REQUEST", "UNSAFE_SQL"])
def test_validation_codes_are_blamed_on_validation(code):
    assert _error_stage_for(code) == "validation"


@pytest.mark.parametrize(
    "code", ["TIMEOUT", "EXECUTION_ERROR", "MAX_ROWS_EXCEEDED", "SYNTAX_ERROR"]
)
def test_execution_codes_are_blamed_on_execution(code):
    assert _error_stage_for(code) == "execution"


@pytest.mark.parametrize("code", [None, "", "SOMETHING_ELSE"])
def test_an_unknown_or_absent_code_blames_no_stage(code):
    assert _error_stage_for(code) is None


# ---------------------------------------------------------------------------------------------
# The two derived safety statuses.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code,expected",
    [
        (None, "pass"),
        ("AMBIGUOUS_PROMPT", "ambiguous"),
        ("INVALID_REQUEST", "invalid_request"),
        ("UNSAFE_SQL", "unsafe"),
        ("TIMEOUT", "pass"),
        ("EXECUTION_ERROR", "pass"),
        ("MAX_ROWS_EXCEEDED", "pass"),
        ("SYNTAX_ERROR", "pass"),
    ],
)
def test_sql_validation_status_per_error_code(code, expected):
    assert _derive_sql_validation_status(code, _Response(status="ok", code=code)) == expected


def test_an_execution_failure_still_counts_as_validated_sql():
    """The query passed validation and then died in the database — those are different verdicts,
    and collapsing them would report a timeout as a validation defect."""
    assert _derive_sql_validation_status("TIMEOUT", _Response(status="error")) == "pass"


def test_sql_validation_is_unknown_when_nothing_says_otherwise():
    """No error code and no successful response: the run tells us nothing, and `pass` would be an
    unearned claim."""
    assert _derive_sql_validation_status(None, _Response(status="error")) == "unknown"
    assert _derive_sql_validation_status(None, _Response(status=None)) == "unknown"


@pytest.mark.parametrize(
    "code,expected",
    [
        (None, "read_only"),
        ("UNSAFE_SQL", "blocked"),
        ("AMBIGUOUS_PROMPT", "unknown"),
        ("INVALID_REQUEST", "unknown"),
        ("TIMEOUT", "read_only"),
        ("MAX_ROWS_EXCEEDED", "read_only"),
    ],
)
def test_read_only_status_per_error_code(code, expected):
    assert _derive_read_only_status(code, _Response(status="ok", code=code)) == expected


def test_read_only_is_unknown_rather_than_asserted_when_the_query_never_ran():
    """`read_only` is a claim that the database was not modified. A prompt rejected before any SQL
    existed cannot support it — and `unknown` is the only honest answer."""
    assert _derive_read_only_status("AMBIGUOUS_PROMPT", _Response(status="error")) == "unknown"
    assert _derive_read_only_status(None, _Response(status="error")) == "unknown"


def test_blocked_unsafe_sql_is_not_reported_as_read_only():
    """It was refused, not executed. Recording `read_only` would hide the refusal in a column that
    reads as a clean run."""
    assert _derive_read_only_status("UNSAFE_SQL", _Response(status="error")) == "blocked"


# ---------------------------------------------------------------------------------------------
# Reading a response that may be anything at all.
# ---------------------------------------------------------------------------------------------


def test_a_missing_response_field_reads_as_absent_not_as_a_crash():
    """These run inside the recorder, after the query. An AttributeError here loses the record of
    a query that already happened."""
    empty = _Response(status=None)
    assert _response_status(empty) == ""
    assert _response_error_code(empty) is None
    assert _response_row_count(empty) == 0
    assert _response_trace_event_count(empty) is None
    assert _response_evidence_available(empty) is None
    assert _response_latency_ms(empty) is None


def test_zero_rows_and_no_row_count_are_different_answers():
    """One says the query returned nothing; the other says we do not know. `_response_row_count`
    collapses both to 0 by design — but the trace count and latency keep the distinction, and that
    asymmetry is worth pinning so it is not "tidied" away."""
    assert _response_row_count(_Response(metadata=_Metadata(returned_rows=0))) == 0
    assert _response_row_count(_Response(metadata=None)) == 0
    assert _response_trace_event_count(_Response(metadata=_Metadata(trace_event_count=0))) == 0
    assert _response_trace_event_count(_Response(metadata=None)) is None


@pytest.mark.parametrize("raw,expected", [(5, 5), ("7", 7), (3.9, 3), (-4, 0), (0, 0)])
def test_row_counts_are_coerced_and_never_negative(raw, expected):
    """A negative count reaching a dashboard renders as a nonsense figure nobody can act on."""
    assert _response_row_count(_Response(metadata=_Metadata(returned_rows=raw))) == expected


@pytest.mark.parametrize("raw", ["not a number", object(), [1]])
def test_an_uncoercible_row_count_reads_as_zero(raw):
    assert _response_row_count(_Response(metadata=_Metadata(returned_rows=raw))) == 0


@pytest.mark.parametrize("raw,expected", [(120, 120), ("250", 250), (-1, 0)])
def test_latency_is_coerced_and_never_negative(raw, expected):
    assert _response_latency_ms(_Response(metadata=_Metadata(duration_ms=raw))) == expected


@pytest.mark.parametrize("raw", ["soon", None])
def test_an_unusable_latency_reads_as_absent_not_as_zero(raw):
    """Zero milliseconds is a measurement. Absent is not — averaging the two together would drag
    every fleet latency figure toward zero."""
    assert _response_latency_ms(_Response(metadata=_Metadata(duration_ms=raw))) is None


@pytest.mark.parametrize("raw,expected", [(4, 4), ("9", 9), (-2, 0)])
def test_trace_event_counts_are_coerced_and_never_negative(raw, expected):
    assert _response_trace_event_count(_Response(metadata=_Metadata(trace_event_count=raw))) == (
        expected
    )


def test_an_uncoercible_trace_event_count_reads_as_absent():
    assert _response_trace_event_count(_Response(metadata=_Metadata(trace_event_count="lots"))) is (
        None
    )


@pytest.mark.parametrize("raw,expected", [(100, 100), ("50", 50), (-5, 0), (None, None)])
def test_the_requested_row_cap_is_coerced_and_never_negative(raw, expected):
    assert _request_max_rows(_Metadata(max_rows=raw)) == expected


def test_no_request_means_no_row_cap():
    assert _request_max_rows(None) is None
    assert _request_max_rows(_Metadata()) is None


def test_an_uncoercible_row_cap_reads_as_absent():
    assert _request_max_rows(_Metadata(max_rows="all of them")) is None


def test_an_error_without_a_code_is_not_reported_as_a_coded_failure():
    """An empty string would make `if error_code:` false everywhere downstream while still
    looking like a code in the record."""
    assert _response_error_code(_Response(status="error", code="")) is None
    assert _response_error_code(_Response(status="error", code=7)) is None
    assert _response_error_code(_Response(status="error", code="TIMEOUT")) == "TIMEOUT"


def test_a_non_string_status_is_not_reported_as_a_status():
    assert _response_status(_Response(status=200)) == ""
    assert _response_status(_Response(status="ok")) == "ok"


# ---------------------------------------------------------------------------------------------
# Evidence, which is a three-valued answer.
# ---------------------------------------------------------------------------------------------


def test_evidence_distinguishes_none_from_absent_provenance():
    """`False` says the query produced rows with no evidence refs — a data-quality finding.
    `None` says provenance was not recorded at all, which is a plumbing problem. Collapsing them
    files one as the other."""
    assert _response_evidence_available(_Response(provenance=None)) is None
    assert _response_evidence_available(_Response(provenance=())) is False


def test_evidence_is_true_when_any_row_carries_a_reference():
    rows = (_Metadata(evidence_refs=()), _Metadata(evidence_refs=("p.40",)))
    assert _response_evidence_available(_Response(provenance=rows)) is True


def test_blank_references_do_not_count_as_evidence():
    """An empty or whitespace ref satisfies `if refs` while pointing at nothing."""
    rows = (_Metadata(evidence_refs=("", "   ")),)
    assert _response_evidence_available(_Response(provenance=rows)) is False


def test_provenance_that_is_not_a_sequence_reads_as_absent():
    assert _response_evidence_available(_Response(provenance="p.40")) is None
    assert _response_evidence_available(_Response(provenance=42)) is None


# ---------------------------------------------------------------------------------------------
# Locating a trace on a run object of unknown shape.
# ---------------------------------------------------------------------------------------------


def test_the_first_named_attribute_that_has_a_value_wins():
    run = _Metadata(id="", run_id="run-123", trace_id="trace-456")
    assert _resolve_run_attr(run, "id", "run_id", "trace_id") == "run-123"


def test_a_mapping_run_is_read_by_key_when_it_has_no_attributes():
    assert _resolve_run_attr({"run_id": "run-123"}, "id", "run_id") == "run-123"


def test_attributes_are_preferred_over_mapping_keys():
    class Both(dict):
        run_id = "from-attribute"

    run = Both({"run_id": "from-key"})
    assert _resolve_run_attr(run, "run_id") == "from-attribute"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_a_blank_trace_reference_is_not_a_reference(value):
    """A whitespace id recorded as a trace ref produces a dashboard link to nothing."""
    assert _resolve_run_attr(_Metadata(run_id=value), "run_id") is None


def test_no_run_at_all_resolves_to_nothing():
    assert _resolve_run_attr(None, "id", "run_id") is None
    assert _resolve_run_attr(_Metadata(), "id", "run_id") is None


def test_a_non_string_reference_is_coerced_rather_than_dropped():
    """LangSmith run ids arrive as UUID objects as often as strings."""
    import uuid

    run_id = uuid.uuid4()
    assert _resolve_run_attr(_Metadata(run_id=run_id), "run_id") == str(run_id)
