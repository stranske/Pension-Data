"""Keep repository-owned CI callers on the owner's tested Python standard."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
RUNTIME_KEYS = {
    "python-version",
    "python-versions",
    "primary-python-version",
    "coverage-python-version",
    "PYTHON_VERSION",
    "COVERAGE_REFERENCE_RUNTIME",
}
# Matrix axes that commonly feed actions/setup-python.
MATRIX_RUNTIME_KEYS = frozenset({"python", "python_version", "python-version"})
MATRIX_AXIS_RE = re.compile(
    r"""\bmatrix(?:\.([A-Za-z_][A-Za-z0-9_-]*)|\[\s*['"]([^'"]+)['"]\s*\])"""
)
FROMJSON_MATRIX_RE = re.compile(
    r"""^\$\{\{\s*fromJSON\(\s*(['"])(?P<payload>.*)\1\s*\)\s*\}\}$""",
    re.DOTALL,
)
OPAQUE_MATRIX = object()
OLD_RUNTIME_RE = re.compile(r"(?<![\d.])3\.12(?!\d)")


def _decode_fromjson_literal(value: str) -> Any | None:
    """Decode a literal ``${{ fromJSON('...') }}`` payload, else ``None``."""
    match = FROMJSON_MATRIX_RE.fullmatch(value.strip())
    if match is None:
        return None
    try:
        return json.loads(match.group("payload"))
    except (TypeError, ValueError):
        return None


def _inspect_fromjson_value(value: str) -> Any:
    """Decode literal fromJSON, else mark opaque fromJSON expressions uninspectable."""
    decoded = _decode_fromjson_literal(value)
    if decoded is not None:
        return decoded
    stripped = value.strip()
    if stripped.startswith("${{") and "fromJSON" in stripped:
        # Malformed literal or non-literal fromJSON — cannot prove absence of 3.12.
        return OPAQUE_MATRIX
    return value


# Back-compat alias used by matrix-level decoding paths and existing tests.
_decode_fromjson_matrix = _decode_fromjson_literal


def _runtime_pins(
    node: Any,
    location: str,
    *,
    in_matrix: bool = False,
    matrix_runtime_keys: frozenset[str] = MATRIX_RUNTIME_KEYS,
) -> Iterator[tuple[str, Any]]:
    if isinstance(node, dict):
        if "strategy" in node:
            # Resolve custom axes from this job's runtime inputs, keeping sibling
            # jobs independent even when they reuse the same matrix axis name.
            job_inputs = {key: value for key, value in node.items() if key != "strategy"}
            matrix_runtime_keys = matrix_runtime_keys | {
                match.group(1) or match.group(2)
                for _, pin in _runtime_pins(job_inputs, location)
                if isinstance(pin, str)
                for match in MATRIX_AXIS_RE.finditer(pin)
            }
        for key, value in node.items():
            path = f"{location}.{key}"
            entering_matrix = key == "matrix" and location.endswith(".strategy")
            if key in RUNTIME_KEYS or (in_matrix and key in matrix_runtime_keys):
                if isinstance(value, str):
                    yield path, _inspect_fromjson_value(value)
                else:
                    yield path, value
            if entering_matrix and isinstance(value, str):
                decoded = _decode_fromjson_literal(value)
                if decoded is None:
                    # Reject matrices the scanner cannot inspect so they cannot hide 3.12.
                    yield path, OPAQUE_MATRIX
                    continue
                yield from _runtime_pins(
                    decoded, path, in_matrix=True, matrix_runtime_keys=matrix_runtime_keys
                )
                continue
            yield from _runtime_pins(
                value,
                path,
                in_matrix=in_matrix or entering_matrix,
                matrix_runtime_keys=matrix_runtime_keys,
            )
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _runtime_pins(
                value,
                f"{location}[{index}]",
                in_matrix=in_matrix,
                matrix_runtime_keys=matrix_runtime_keys,
            )


def _contains_old_runtime(value: Any) -> bool:
    if value is OPAQUE_MATRIX:
        return True
    if isinstance(value, dict):
        return any(_contains_old_runtime(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_old_runtime(item) for item in value)
    if isinstance(value, str):
        inspected = _inspect_fromjson_value(value)
        if inspected is OPAQUE_MATRIX or inspected != value:
            return _contains_old_runtime(inspected)
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            pass
        else:
            if decoded != value:
                return _contains_old_runtime(decoded)
        text = str(value)
        if "${{" in text and OLD_RUNTIME_RE.search(text):
            return True
        return text == "3.12" or text.startswith("3.12.")
    return str(value) == "3.12" or str(value).startswith("3.12.")


@pytest.mark.parametrize("version, expected", [("3.12", True), ("3.12.9", True), ("3.14", False)])
def test_object_valued_python_matrix_axis(version: str, expected: bool) -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {"include": [{"python": {"version": version}}]}},
                "steps": [{"with": {"python-version": "${{ matrix.python.version }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == (["ci.yml.jobs.test.strategy.matrix.include[0].python"] if expected else [])


def test_yaml_escaped_object_valued_python_matrix_axis() -> None:
    workflow = yaml.safe_load(r"""jobs:
  test:
    strategy:
      matrix:
        include:
          - python:
              version: "\u0033\u002e12"
    steps:
      - with:
          python-version: ${{ matrix.python.version }}
""")
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == ["ci.yml.jobs.test.strategy.matrix.include[0].python"]


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("${{ inputs.python-version || '3.12' }}", True),
        ("${{ inputs.python-version || '3.14' }}", False),
        ("${{ inputs.python-version || '3.120' }}", False),
    ],
)
def test_runtime_fallback_expressions(expression: str, expected: bool) -> None:
    assert _contains_old_runtime(expression) is expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("${{ fromJSON('\"3.12\"') }}", True),
        (r"""${{ fromJSON('"\u0033\u002e12"') }}""", True),
        ("${{ fromJSON('\"3.14\"') }}", False),
        (r"""${{ fromJSON('"\u0033\u002e14"') }}""", False),
        ("${{ fromJSON(needs.setup.outputs.python) }}", True),
        ("${{ fromJSON('invalid-json') }}", True),
    ],
)
def test_expression_valued_object_python_axis(expression: str, expected: bool) -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {"include": [{"python": {"version": expression}}]}},
                "steps": [{"with": {"python-version": "${{ matrix.python.version }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == (["ci.yml.jobs.test.strategy.matrix.include[0].python"] if expected else [])


def test_workflow_python_pins_follow_standard() -> None:
    workflows = {
        path.name: yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in sorted(WORKFLOWS.iterdir())
        if path.is_file() and path.suffix in {".yml", ".yaml"}
    }
    assert workflows, "No workflow YAML was inspected"
    offenders = [
        location
        for filename, workflow in workflows.items()
        for location, value in _runtime_pins(workflow, filename)
        if _contains_old_runtime(value)
    ]
    assert not offenders, f"Python 3.12 runtime pins remain: {offenders}"
    for filename, job in (("ci.yml", "python"), ("pr-00-gate.yml", "python-ci")):
        config = workflows[filename]["jobs"][job]
        with_inputs = config["with"]
        assert json.loads(with_inputs["python-versions"]) == ["3.13", "3.14"]
        assert with_inputs.get("coverage-python-version") == "3.14"
        assert with_inputs.get("primary-python-version") == "3.14"
        assert not config.get("continue-on-error", False)
    # Match Template/create-only gate: explicit primary runtime, not an ambient default.
    assert (
        workflows["pr-00-gate.yml"]["jobs"]["python-ci"]["with"].get("primary-python-version")
        == "3.14"
    )


@pytest.mark.parametrize(
    "matrix, step_ref",
    [
        ({"python": ["3.12"]}, "${{ matrix.python }}"),
        ({"python": ["3.12.9", "3.14"]}, "${{ matrix.python }}"),
        ({"include": [{"python": "3.12", "os": "ubuntu-latest"}]}, "${{ matrix.python }}"),
        ({"python_version": ["3.12"]}, "${{ matrix.python_version }}"),
        ({"python-version": ["3.12", "3.14"]}, "${{ matrix.python-version }}"),
    ],
)
def test_named_python_matrix_cannot_hide_old_runtime(matrix: dict[str, Any], step_ref: str) -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": matrix},
                "steps": [{"with": {"python-version": step_ref}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert len(offenders) == 1
    assert ".strategy.matrix." in offenders[0]


@pytest.mark.parametrize(
    "matrix_expr",
    [
        '${{ fromJSON(\'{"python":["3.12"]}\') }}',
        '${{ fromJSON("{\\"python_version\\":[\\"3.12\\",\\"3.14\\"]}") }}',
        "${{ fromJSON(inputs.matrix) }}",
        "${{ inputs.python_matrix }}",
    ],
)
def test_expression_valued_matrix_cannot_hide_old_runtime(matrix_expr: str) -> None:
    """Literal fromJSON 3.12 pins and opaque matrix expressions must both fail the guard."""
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": matrix_expr},
                "steps": [{"with": {"python-version": "${{ matrix.python }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert len(offenders) == 1
    assert offenders[0].startswith("ci.yml.jobs.test.strategy.matrix")


def test_malformed_literal_fromjson_matrix_is_opaque() -> None:
    """A literal fromJSON payload that fails JSON decoding must be treated as opaque."""
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": "${{ fromJSON('{\"python\":[}') }}"},
            }
        }
    }
    values = [
        value
        for path, value in _runtime_pins(workflow, "ci.yml")
        if path == "ci.yml.jobs.test.strategy.matrix"
    ]
    assert values == [OPAQUE_MATRIX]
    assert _contains_old_runtime(OPAQUE_MATRIX)


@pytest.mark.parametrize(
    "axis_expr",
    [
        '${{ fromJSON(\'["3.12","3.14"]\') }}',
        "${{ fromJSON('[\"3.12\"]') }}",
        "${{ fromJSON(inputs.python_versions) }}",
    ],
)
def test_fromjson_matrix_axis_cannot_hide_old_runtime(axis_expr: str) -> None:
    """Literal or opaque fromJSON on a matrix axis must fail the named guard."""
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {"python": axis_expr}},
                "steps": [{"with": {"python-version": "${{ matrix.python }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == ["ci.yml.jobs.test.strategy.matrix.python"]


def test_literal_fromjson_matrix_axis_without_old_runtime_is_allowed() -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {"python": '${{ fromJSON(\'["3.13","3.14"]\') }}'}},
                "steps": [{"with": {"python-version": "${{ matrix.python }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == []


def _lines_matching_312(filename: str, text: str) -> list[str]:
    """Return ``file:line:text`` entries for the Nov 7 pass-number substring read."""
    return [
        f"{filename}:{index}:{line.rstrip()}"
        for index, line in enumerate(text.splitlines(), start=1)
        if "3.12" in line
    ]


@pytest.mark.parametrize("filename", ["ci.yml", "pr-00-gate.yml"])
def test_owned_caller_workflow_text_has_no_312_lines(filename: str) -> None:
    """Line-level guard matching the Nov 7 persistence acceptance read for owned callers."""
    text = (WORKFLOWS / filename).read_text(encoding="utf-8")
    offenders = _lines_matching_312(filename, text)
    assert not offenders, f"Lines matching 3.12 remain: {offenders}"


@pytest.mark.parametrize("filename", ["ci.yml", "pr-00-gate.yml"])
def test_line_level_312_deliberate_break_and_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filename: str
) -> None:
    """Pass-number substring guard must fail when a 3.12 line is reintroduced, then recover."""
    for name in ("ci.yml", "pr-00-gate.yml"):
        (tmp_path / name).write_bytes((WORKFLOWS / name).read_bytes())
    monkeypatch.setattr(f"{__name__}.WORKFLOWS", tmp_path)
    test_owned_caller_workflow_text_has_no_312_lines(filename)

    path = tmp_path / filename
    original = path.read_bytes()
    path.write_text(
        original.decode("utf-8") + "\n# keepalive pass-number probe: 3.12\n",
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="Lines matching 3.12 remain"):
        test_owned_caller_workflow_text_has_no_312_lines(filename)

    path.write_bytes(original)
    test_owned_caller_workflow_text_has_no_312_lines(filename)


@pytest.mark.parametrize("filename, job", [("ci.yml", "python"), ("pr-00-gate.yml", "python-ci")])
def test_json_matrix_deliberate_break_and_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filename: str, job: str
) -> None:
    """The named guard must reject a restored 3.12 JSON pin in either caller."""
    for name in ("ci.yml", "pr-00-gate.yml"):
        (tmp_path / name).write_bytes((WORKFLOWS / name).read_bytes())
    monkeypatch.setattr(f"{__name__}.WORKFLOWS", tmp_path)
    test_workflow_python_pins_follow_standard()

    path = tmp_path / filename
    original = path.read_bytes()
    workflow = yaml.safe_load(original)
    workflow["jobs"][job]["with"]["python-versions"] = '["3.12", "3.14"]'
    path.write_text(yaml.safe_dump(workflow), encoding="utf-8")
    with pytest.raises(AssertionError, match="Python 3.12 runtime pins remain") as failure:
        test_workflow_python_pins_follow_standard()
    assert str(failure.value).splitlines()[0] == (
        f"Python 3.12 runtime pins remain: ['{filename}.jobs.{job}.with.python-versions']"
    )

    path.write_bytes(original)
    test_workflow_python_pins_follow_standard()


@pytest.mark.parametrize("suffix", [".yml", ".yaml"])
@pytest.mark.parametrize("runtime", ["3.12", 3.12], ids=["quoted", "unquoted"])
@pytest.mark.parametrize(
    "container, runtime_key",
    [
        ("with", "python-version"),
        ("with", "python-versions"),
        ("with", "primary-python-version"),
        ("with", "coverage-python-version"),
        ("env", "PYTHON_VERSION"),
        ("env", "COVERAGE_REFERENCE_RUNTIME"),
    ],
)
def test_named_guard_checks_runtime_keys_in_additional_workflows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    runtime: str | float,
    container: str,
    runtime_key: str,
) -> None:
    """No runtime key in another workflow may bypass the named guard's YAML scan."""
    for name in ("ci.yml", "pr-00-gate.yml"):
        (tmp_path / name).write_bytes((WORKFLOWS / name).read_bytes())
    monkeypatch.setattr(f"{__name__}.WORKFLOWS", tmp_path)
    path = tmp_path / f"additional{suffix}"
    workflow = {"jobs": {"test": {"steps": [{container: {runtime_key: "3.14"}}]}}}
    path.write_text(yaml.safe_dump(workflow), encoding="utf-8")
    test_workflow_python_pins_follow_standard()

    original = path.read_bytes()
    workflow["jobs"]["test"]["steps"][0][container][runtime_key] = runtime
    path.write_text(yaml.safe_dump(workflow), encoding="utf-8")
    with pytest.raises(AssertionError, match="Python 3.12 runtime pins remain") as failure:
        test_workflow_python_pins_follow_standard()
    assert str(failure.value).splitlines()[0] == (
        f"Python 3.12 runtime pins remain: ['{path.name}.jobs.test.steps[0].{container}.{runtime_key}']"
    )

    path.write_bytes(original)
    test_workflow_python_pins_follow_standard()


def test_unquoted_yaml_float_runtime_pin_is_rejected() -> None:
    """Unquoted ``3.12`` becomes a YAML float and must still fail the named guard."""
    workflow = {"jobs": {"test": {"steps": [{"with": {"python-version": 3.12}}]}}}
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == ["ci.yml.jobs.test.steps[0].with.python-version"]


@pytest.mark.parametrize(
    "axis, values, reference",
    [
        ("runtime", ["3.12", "3.14"], "${{ matrix.runtime }}"),
        ("runtime", ["3.12", "3.14"], "${{ matrix['runtime'] }}"),
        ("runtime", ["3.12", "3.14"], '${{ matrix["runtime"] }}'),
        ("runtime", [{"version": "3.12"}], "${{ matrix.runtime.version }}"),
        ("test-python", ["3.12"], "${{ matrix.test-python }}"),
    ],
)
def test_named_guard_follows_custom_runtime_matrix_axes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    axis: str,
    values: list[Any],
    reference: str,
) -> None:
    """A setup-python reference makes a custom matrix axis a runtime pin."""
    for name in ("ci.yml", "pr-00-gate.yml"):
        (tmp_path / name).write_bytes((WORKFLOWS / name).read_bytes())
    monkeypatch.setattr(f"{__name__}.WORKFLOWS", tmp_path)
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {axis: values}},
                "steps": [{"with": {"python-version": reference}}],
            }
        }
    }
    (tmp_path / "custom.yml").write_text(yaml.safe_dump(workflow), encoding="utf-8")

    with pytest.raises(AssertionError, match="Python 3.12 runtime pins remain") as failure:
        test_workflow_python_pins_follow_standard()
    assert f"custom.yml.jobs.test.strategy.matrix.{axis}" in str(failure.value)


def test_unrelated_python_metadata_is_not_a_runtime_pin() -> None:
    assert list(_runtime_pins({"metadata": {"python": "3.12"}}, "ci.yml")) == []


def test_custom_runtime_axis_references_are_scoped_to_each_job() -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {"runtime": ["3.14"]}},
                "steps": [{"with": {"python-version": "${{ matrix.runtime }}"}}],
            },
            "other": {
                "strategy": {"matrix": {"runtime": ["3.12"]}},
                "steps": [{"with": {"python-version": "3.14"}}],
            },
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == []


def test_literal_fromjson_matrix_without_old_runtime_is_allowed() -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": '${{ fromJSON(\'{"python":["3.13","3.14"]}\') }}'},
                "steps": [{"with": {"python-version": "${{ matrix.python }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == []
