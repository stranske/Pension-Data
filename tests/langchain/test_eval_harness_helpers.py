"""The pure decision logic inside the evaluation harness.

This harness decides whether a model's output is acceptable, so a defect here either passes
hallucinated citations or fails good answers — and both are invisible, because the only thing
either produces is a different number in a report nobody re-derives.

The subtlety worth the most attention is that coverage and hallucination are SEPARATE metrics
measured over the same two sets. An extra citation does not reduce coverage, and a missing one is
not a hallucination. Collapsing them into one precision-like score double-counts every error, and
the tests below pin the split explicitly so nobody tidies it into one number.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.langchain.eval_harness import (
    DatasetValidationError,
    EvalCase,
    _citation_metrics,
    _evaluate_case,
    _extract_relations,
    _load_yaml_or_json,
    _normalize_string_tuple,
    _parse_feature,
    _parse_thresholds,
    _validate_schema,
    load_eval_dataset,
)


def _case(feature: str) -> EvalCase:
    return EvalCase(
        case_id="case-1",
        domain="pensions",
        feature=feature,  # type: ignore[arg-type]
        question="how many plans",
        recorded_output=None,
        expected_sql_contains=(),
        expected_citations=(),
        allowed_relations=(),
    )


# ---------------------------------------------------------------------------------------------
# Citations: coverage and hallucination are different questions.
# ---------------------------------------------------------------------------------------------


def test_a_perfect_answer_scores_full_coverage_and_no_hallucination():
    coverage, clean, details = _citation_metrics(("p.40", "p.41"), ("p.40", "p.41"))
    assert coverage == 1.0
    assert clean is True
    assert details == ()


def test_a_missing_citation_lowers_coverage_but_is_not_a_hallucination():
    """The model under-cited; it did not invent anything. Marking this as a hallucination would
    make the no-hallucination rate meaningless as a safety signal."""
    coverage, clean, details = _citation_metrics(("p.40", "p.41"), ("p.40",))
    assert coverage == 0.5
    assert clean is True
    assert any("missing expected citations" in detail for detail in details)


def test_an_extra_citation_is_a_hallucination_and_does_not_lower_coverage():
    """Coverage answers 'did it find what we expected'. Precision is the other metric's job;
    subtracting for extras here would count one error twice."""
    coverage, clean, details = _citation_metrics(("p.40",), ("p.40", "p.99"))
    assert coverage == 1.0
    assert clean is False
    assert any("hallucinated citations: p.99" in detail for detail in details)


def test_a_case_expecting_no_citations_treats_any_citation_as_invented():
    """There was nothing to cite, so anything cited came from the model."""
    coverage, clean, details = _citation_metrics((), ("p.40",))
    assert coverage == 1.0
    assert clean is False
    assert details == ("hallucinated citations detected where none were expected",)


def test_expecting_and_producing_no_citations_is_a_clean_pass():
    assert _citation_metrics((), ()) == (1.0, True, ())


def test_a_repeated_expectation_is_not_counted_twice():
    """Coverage is over the SET of expectations. Counting duplicates would cap a correct answer
    below 1.0 for a dataset typo."""
    coverage, clean, _ = _citation_metrics(("p.40", "p.40"), ("p.40",))
    assert coverage == 1.0
    assert clean is True


def test_both_failures_are_reported_together():
    """One missing and one invented is two findings, and dropping either hides half the problem."""
    _, clean, details = _citation_metrics(("p.40", "p.41"), ("p.40", "p.99"))
    assert clean is False
    assert len(details) == 2


# ---------------------------------------------------------------------------------------------
# Thresholds: the gate the whole report is measured against.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("raw,expected", [(0.0, 0.0), (0.5, 0.5), (1, 1.0), (1.0, 1.0)])
def test_a_numeric_threshold_in_range_is_accepted(raw, expected):
    assert _parse_thresholds({"min_safety_pass_rate": raw}).min_safety_pass_rate == expected


@pytest.mark.parametrize("raw", [True, False])
def test_a_boolean_threshold_is_refused_rather_than_read_as_one_or_zero(raw):
    """`bool` is a subclass of `int`, so `min_safety_pass_rate: true` in YAML would silently
    become 1.0 — the strictest possible gate, arrived at by accident. And `false` would become
    0.0, disabling the gate entirely. Both are worse than an error."""
    with pytest.raises(DatasetValidationError, match="must be numeric"):
        _parse_thresholds({"min_safety_pass_rate": raw})


@pytest.mark.parametrize("raw", ["0.5", None, [0.5], {"value": 0.5}])
def test_a_non_numeric_threshold_is_refused(raw):
    with pytest.raises(DatasetValidationError, match="must be numeric"):
        _parse_thresholds({"min_safety_pass_rate": raw})


@pytest.mark.parametrize("raw", [1.5, -0.1, 100])
def test_a_threshold_outside_zero_to_one_is_refused(raw):
    """These are rates. `min_safety_pass_rate: 100` means someone typed a percentage, and a gate
    that can never pass reads as a broken harness rather than a failing model."""
    with pytest.raises(DatasetValidationError, match="between 0 and 1"):
        _parse_thresholds({"min_safety_pass_rate": raw})


@pytest.mark.parametrize(
    "name",
    [
        "min_schema_validity_rate",
        "min_citation_coverage_rate",
        "min_no_hallucination_rate",
        "min_safety_pass_rate",
    ],
)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_threshold_is_refused(name: str, value: float) -> None:
    with pytest.raises(DatasetValidationError, match=f"threshold {name} must be between 0 and 1"):
        _parse_thresholds({name: value})


def test_an_absent_threshold_keeps_its_default():
    parsed = _parse_thresholds({})
    assert 0.0 <= parsed.min_schema_validity_rate <= 1.0
    assert parsed == _parse_thresholds({})


def test_the_failing_threshold_is_named():
    """Four thresholds are parsed by one helper; an error that does not say which one leaves the
    author to bisect the file."""
    with pytest.raises(DatasetValidationError, match="min_citation_coverage_rate"):
        _parse_thresholds({"min_citation_coverage_rate": "high"})


# ---------------------------------------------------------------------------------------------
# Features and relations.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["nl_sql", "findings_explain", "findings_compare"])
def test_each_supported_feature_is_accepted(name):
    assert _parse_feature(name) == name
    assert _parse_feature(f"  {name}  ") == name


@pytest.mark.parametrize("value", ["bogus", "", None, 42, "NL_SQL"])
def test_an_unsupported_feature_is_refused_with_the_options_listed(value):
    """A typo'd feature would otherwise fall through every branch of `_validate_schema` and score
    a perfect schema-validity rate for a case nothing checked."""
    with pytest.raises(DatasetValidationError, match="nl_sql, findings_explain, findings_compare"):
        _parse_feature(value)


def test_relations_are_lowercased_and_stripped_of_their_schema():
    """`public.Plans` and `plans` are the same table; treating them as two lets an allow-list
    written one way miss SQL written the other."""
    assert _extract_relations("SELECT * FROM public.Plans") == ("plans",)


def test_each_relation_is_listed_once_however_often_it_appears():
    sql = "SELECT * FROM public.plans p JOIN plans q ON 1=1 JOIN metrics m ON 1=1"
    assert _extract_relations(sql) == ("plans", "metrics")


def test_joined_relations_are_found_not_just_the_from_clause():
    """An allow-list checked against the FROM table alone would wave through any joined table."""
    relations = _extract_relations("SELECT * FROM plans JOIN secret_salaries ON 1=1")
    assert "secret_salaries" in relations


def test_sql_with_no_relations_yields_none():
    assert _extract_relations("SELECT 1") == ()


# ---------------------------------------------------------------------------------------------
# Normalising the string lists a dataset may contain.
# ---------------------------------------------------------------------------------------------


def test_entries_are_stripped_and_deduplicated_in_order():
    assert _normalize_string_tuple(["b", " a ", "a", "b"]) == ("b", "a")


def test_blank_and_non_string_entries_are_dropped():
    assert _normalize_string_tuple(["a", "", "   ", None, 3, ["x"]]) == ("a",)


@pytest.mark.parametrize("value", ["abc", b"abc", bytearray(b"abc"), None, 42, {"a": 1}])
def test_a_value_that_is_not_a_list_yields_nothing(value):
    """A bare string is a sequence of characters. Iterating it would turn one citation into a
    tuple of letters, and every one of them would then read as a hallucination."""
    assert _normalize_string_tuple(value) == ()


# ---------------------------------------------------------------------------------------------
# Loading the dataset file.
# ---------------------------------------------------------------------------------------------


def test_a_json_dataset_loads(tmp_path):
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps({"cases": []}), encoding="utf-8")
    assert _load_yaml_or_json(path) == {"cases": []}


def test_a_yaml_dataset_loads(tmp_path):
    path = tmp_path / "dataset.yaml"
    path.write_text("cases: []\nthresholds:\n  min_safety_pass_rate: 1.0\n", encoding="utf-8")
    loaded = _load_yaml_or_json(path)
    assert loaded["cases"] == []
    assert loaded["thresholds"] == {"min_safety_pass_rate": 1.0}


@pytest.mark.parametrize("payload", ["[1, 2, 3]", '"a string"', "42", "null"])
def test_a_dataset_that_is_not_an_object_is_refused(tmp_path, payload):
    """All of these parse. The first attribute access is where they would otherwise fail, far
    from the file that caused it."""
    path = tmp_path / "dataset.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(DatasetValidationError, match="object mapping"):
        _load_yaml_or_json(path)


def test_an_unparseable_dataset_reports_both_parsers(tmp_path):
    """The file is tried as YAML and then as JSON. An error naming only JSON sends the author to
    debug the wrong syntax."""
    path = tmp_path / "dataset.yaml"
    path.write_text("{ not: valid: yaml: or json", encoding="utf-8")
    with pytest.raises(DatasetValidationError) as excinfo:
        _load_yaml_or_json(path)
    assert "YAML" in str(excinfo.value)
    assert "JSON" in str(excinfo.value)


def test_a_missing_dataset_file_raises_at_the_read(tmp_path):
    with pytest.raises(FileNotFoundError):
        _load_yaml_or_json(tmp_path / "absent.yaml")


# ---------------------------------------------------------------------------------------------
# Schema validation, per feature.
# ---------------------------------------------------------------------------------------------


def test_nl_sql_requires_a_non_empty_sql_string():
    assert _validate_schema(_case("nl_sql"), {"sql": "SELECT 1"}) == []
    for output in ({}, {"sql": ""}, {"sql": "   "}, {"sql": None}, {"sql": 42}):
        details = _validate_schema(_case("nl_sql"), output)
        assert details, output
        assert "requires non-empty string field 'sql'" in details[0]


@pytest.mark.parametrize(
    "feature,required",
    [
        ("findings_explain", ("summary", "key_drivers", "caveats", "citations", "artifact_path")),
        (
            "findings_compare",
            (
                "summary",
                "key_differences",
                "key_drivers",
                "caveats",
                "citations",
                "artifact_path",
            ),
        ),
    ],
)
def test_each_findings_feature_names_every_field_it_is_missing(feature, required):
    """One detail per missing key, not one for the first. An author who fixes them one at a time
    re-runs the harness once per field."""
    details = _validate_schema(_case(feature), {})
    for key in required:
        assert any(f"missing '{key}'" in detail for detail in details), key


def test_key_differences_is_required_only_by_the_compare_feature():
    """The one field that separates the two schemas. Requiring it of `findings_explain` would fail
    every correct explanation."""
    explain = _validate_schema(
        _case("findings_explain"),
        {
            "summary": "s",
            "key_drivers": [],
            "caveats": [],
            "citations": [],
            "artifact_path": "a.json",
        },
    )
    assert explain == []

    compare = _validate_schema(
        _case("findings_compare"),
        {
            "summary": "s",
            "key_drivers": [],
            "caveats": [],
            "citations": [],
            "artifact_path": "a.json",
        },
    )
    assert any("key_differences" in detail for detail in compare)


@pytest.mark.parametrize("feature", ["findings_explain", "findings_compare"])
def test_a_present_but_blank_artifact_path_is_still_invalid(feature):
    """The key exists, so the missing-key check passes; the artifact still cannot be opened."""
    output = {
        "summary": "s",
        "key_differences": [],
        "key_drivers": [],
        "caveats": [],
        "citations": [],
        "artifact_path": "   ",
    }
    details = _validate_schema(_case(feature), output)
    assert any("requires non-empty string field 'artifact_path'" in detail for detail in details)


def test_an_unknown_feature_validates_nothing_rather_than_raising():
    """`_parse_feature` is the gate; by the time a case reaches here the feature is known. If one
    ever slips through, scoring it as schema-valid is the behaviour this pins — so that a change
    to that decision is a deliberate one.
    """
    assert _validate_schema(_case("something_else"), {}) == []


def test_the_relative_path_of_a_dataset_is_not_consulted_by_schema_validation(tmp_path: Path):
    """Schema validity is a property of the output alone. A check that reached the filesystem
    would make the same output pass or fail depending on where the dataset lives."""
    assert _validate_schema(_case("nl_sql"), {"sql": "SELECT 1"}) == []


# ---------------------------------------------------------------------------------------------
# Loading a dataset: every refusal, because a case silently dropped is a case never evaluated.
# ---------------------------------------------------------------------------------------------


def _dataset(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


_MINIMAL_CASE = {"id": "c1", "domain": "pensions", "question": "how many plans"}


def test_a_minimal_dataset_loads(tmp_path):
    dataset = load_eval_dataset(_dataset(tmp_path, {"cases": [_MINIMAL_CASE]}))
    assert dataset.version == 1
    assert len(dataset.cases) == 1
    assert dataset.cases[0].feature == "nl_sql"  # the default
    assert dataset.dataset_path == tmp_path / "dataset.json"


@pytest.mark.parametrize("version", [True, False, 1.0, "1", None])
def test_a_non_integer_version_is_refused(tmp_path, version):
    """`version: true` would otherwise load as version 1 and quietly compare against the wrong
    schema — `bool` is an `int`, which is why it is excluded by name."""
    with pytest.raises(DatasetValidationError, match="version must be an integer"):
        load_eval_dataset(_dataset(tmp_path, {"version": version, "cases": [_MINIMAL_CASE]}))


@pytest.mark.parametrize("cases", ["a string", 42, {"id": "c1"}, None])
def test_cases_that_are_not_a_list_are_refused(tmp_path, cases):
    """A bare string is a sequence, so without the explicit exclusion the loader would iterate its
    characters and report one malformed case per letter."""
    with pytest.raises(DatasetValidationError, match="cases must be a list"):
        load_eval_dataset(_dataset(tmp_path, {"cases": cases}))


def test_an_empty_case_list_is_refused(tmp_path):
    """A harness that evaluates nothing reports a perfect score. That is the failure mode this
    check exists for — every rate is vacuously 1.0 over an empty set."""
    with pytest.raises(DatasetValidationError, match="at least one case"):
        load_eval_dataset(_dataset(tmp_path, {"cases": []}))


@pytest.mark.parametrize(
    "missing,message",
    [
        ("id", "non-empty id"),
        ("domain", "non-empty domain"),
        ("question", "non-empty question"),
    ],
)
def test_each_required_case_field_is_refused_when_blank(tmp_path, missing, message):
    case = dict(_MINIMAL_CASE, **{missing: "   "})
    with pytest.raises(DatasetValidationError, match=message):
        load_eval_dataset(_dataset(tmp_path, {"cases": [case]}))


def test_the_failing_case_is_named_where_it_can_be(tmp_path):
    """Errors after the id is read quote it; the id error itself cannot. That asymmetry is the
    reason the id is checked first."""
    case = dict(_MINIMAL_CASE, id="case-42", domain="")
    with pytest.raises(DatasetValidationError, match="case-42"):
        load_eval_dataset(_dataset(tmp_path, {"cases": [case]}))


def test_a_blank_recorded_output_is_stored_as_absent(tmp_path):
    """`recorded_output: ""` must not become a path the mock loader then tries to open."""
    case = dict(_MINIMAL_CASE, recorded_output="   ")
    dataset = load_eval_dataset(_dataset(tmp_path, {"cases": [case]}))
    assert dataset.cases[0].recorded_output is None


# ---------------------------------------------------------------------------------------------
# Case evaluation: the safety verdict.
# ---------------------------------------------------------------------------------------------


def _sql_case(**overrides) -> EvalCase:
    fields = {
        "case_id": "case-1",
        "domain": "pensions",
        "feature": "nl_sql",
        "question": "how many plans",
        "recorded_output": None,
        "expected_sql_contains": (),
        "expected_citations": (),
        "allowed_relations": (),
    }
    fields.update(overrides)
    return EvalCase(**fields)  # type: ignore[arg-type]


def test_sql_touching_a_table_outside_the_allow_list_fails_on_safety_not_on_diff():
    """The allow-list is the model's blast radius. A query that reads a table the case never
    authorised is a safety regression, and it has to land in `safety_pass` — a `details` entry
    alone would leave the safety rate at 100% while the report listed the problem.
    """
    case = _sql_case(allowed_relations=("plans",))
    result = _evaluate_case(case, {"sql": "SELECT * FROM plans JOIN salaries ON 1=1"})

    assert result.safety_pass is False
    assert result.pass_status is False
    assert any("disallowed relations" in detail for detail in result.details)
    assert any("salaries" in detail for detail in result.details)


def test_every_disallowed_relation_is_named_not_just_the_first():
    case = _sql_case(allowed_relations=("plans",))
    result = _evaluate_case(
        case, {"sql": "SELECT * FROM plans JOIN salaries s ON 1=1 JOIN ssns n ON 1=1"}
    )
    detail = next(d for d in result.details if "disallowed relations" in d)
    assert "salaries" in detail
    assert "ssns" in detail


def test_the_allow_list_is_matched_case_insensitively():
    """`public.PLANS` and `plans` are one table. A case-sensitive comparison would report a
    safety regression for correct SQL, which trains the reader to ignore the signal."""
    case = _sql_case(allowed_relations=("Plans",))
    result = _evaluate_case(case, {"sql": "SELECT * FROM public.PLANS"})
    assert result.safety_pass is True


def test_no_allow_list_means_no_relation_check():
    """An absent allow-list is 'not specified', not 'nothing permitted'."""
    case = _sql_case()
    result = _evaluate_case(case, {"sql": "SELECT * FROM anything_at_all"})
    assert result.safety_pass is True


def test_an_nl_sql_case_with_no_sql_is_a_safety_failure_not_merely_a_schema_one():
    """No SQL means nothing was validated. Scoring that as safe would let a model that returned
    nothing at all achieve a perfect safety rate."""
    result = _evaluate_case(_sql_case(), {})
    assert result.safety_pass is False
    assert result.schema_valid is False
    assert any("no SQL available" in detail for detail in result.details)


def test_a_missing_expected_token_is_reported_as_a_diff_not_as_unsafe():
    """The model wrote different but legitimate SQL. Conflating that with a safety regression
    inflates the one rate that is supposed to mean something."""
    case = _sql_case(expected_sql_contains=("GROUP BY",))
    result = _evaluate_case(case, {"sql": "SELECT count(*) FROM plans"})

    assert result.safety_pass is True
    assert result.pass_status is False
    assert any("expected SQL token missing" in detail for detail in result.details)


def test_expected_tokens_are_matched_case_insensitively():
    case = _sql_case(expected_sql_contains=("group by",))
    result = _evaluate_case(case, {"sql": "SELECT count(*) FROM plans GROUP BY id"})
    assert result.pass_status is True


def test_a_clean_case_passes_every_metric():
    case = _sql_case(
        expected_sql_contains=("FROM plans",),
        expected_citations=("p.40",),
        allowed_relations=("plans",),
    )
    result = _evaluate_case(case, {"sql": "SELECT count(*) FROM plans", "citations": ["p.40"]})

    assert result.pass_status is True
    assert result.schema_valid is True
    assert result.safety_pass is True
    assert result.no_hallucination is True
    assert result.citation_coverage == 1.0
    assert result.details == ()


def test_a_hallucinated_citation_fails_the_case_without_failing_safety():
    """Three independent verdicts on one result. A citation problem that dragged `safety_pass`
    down would make the safety rate track citation quality instead of query safety."""
    case = _sql_case(allowed_relations=("plans",))
    result = _evaluate_case(case, {"sql": "SELECT count(*) FROM plans", "citations": ["p.99"]})

    assert result.pass_status is False
    assert result.no_hallucination is False
    assert result.safety_pass is True
    assert result.schema_valid is True
