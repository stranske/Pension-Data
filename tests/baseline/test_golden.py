"""Golden masters of each scenario's flattened quant metrics.

Re-bless after an intended change:
    pytest tests/baseline/test_golden.py --force-regen
then review and commit the updated baseline CSVs under test_golden/.
"""

from __future__ import annotations

import pytest
from baseline_kit import check_metrics, load_catalog

from . import adapter
from .conftest import CATALOG_PATH

_CATALOG = load_catalog(CATALOG_PATH)
_BASE = _CATALOG["base"]
_SCENARIOS = _CATALOG["scenarios"]


@pytest.mark.parametrize("scenario", _SCENARIOS, ids=[s["id"] for s in _SCENARIOS])
def test_quant_metrics_golden(scenario, num_regression):
    check_metrics(num_regression, adapter.run_scenario(scenario, _BASE))
