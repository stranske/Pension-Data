"""Economic invariants on the base scenario and every catalog variant."""

from __future__ import annotations

import pytest
from baseline_kit import assert_invariants, load_catalog

from . import invariants
from .conftest import CATALOG_PATH

_CATALOG = load_catalog(CATALOG_PATH)
_BASE = _CATALOG["base"]
_SCENARIOS = _CATALOG["scenarios"]


@pytest.mark.parametrize("scenario", _SCENARIOS, ids=[s["id"] for s in _SCENARIOS])
def test_scenario_invariants(scenario):
    assert_invariants(
        invariants.check_scenario(scenario, _BASE),
        context=scenario["id"],
    )
