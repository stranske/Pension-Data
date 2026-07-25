"""Gate tests for the PPD peer-universe ingest (issue #645).

Fixtures are **recorded-shape** responses (network egress is sandboxed, so no
live PPD call is made). The saved QVariables fixture holds a representative
subset -- 8 plans x 2 fiscal years = 16 records -- enough to exercise mapping
and real peer statistics. The live acceptance criterion is a full pull of
>=200 plans across multiple years; the fixture stands in for it here.

Deliberate-break: renaming a mapped PPD variable in
``pension_data.sources.ppd.variables.PPD_VARIABLES`` makes the mapper look up a
key that is absent from the recorded response, so the funded-ratio mapping goes
missing and ``test_mapping_populates_funded_status`` fails. Restoring the name
makes it pass. See the demonstration test at the bottom of this module.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.query.saved_views.service import (
    execute_allocation_peer_compare_view,
    execute_funding_trend_view,
)
from pension_data.sources.ppd import variables as ppd_variables
from pension_data.sources.ppd.cache import PpdResponseCache
from pension_data.sources.ppd.client import (
    build_qvariables_url,
    parse_qvariables_json,
)
from pension_data.sources.ppd.codebook import missing_mapped_variables, parse_codebook_csv
from pension_data.sources.ppd.mapping import (
    derive_maturity,
    derive_plan_type,
    derive_size,
    derive_state,
    to_benchmark_panel_input,
    to_funding_trend_input,
)
from pension_data.sources.ppd.peer_universe import (
    PPD_SOURCE_NAME,
    PeerUniverseStore,
    ingest_records,
    to_allocation_peer_inputs,
    to_funding_trend_inputs,
)
from pension_data.sources.ppd.variables import variable_name

FIXTURE_DIR = Path(__file__).parent / "fixtures"
QVARIABLES_FIXTURE = FIXTURE_DIR / "ppd_qvariables_sample.json"
CODEBOOK_FIXTURE = FIXTURE_DIR / "ppd_codebook_sample.csv"

# Fixture size -- the ">=N plans" gate. Live acceptance criterion is >=200.
EXPECTED_PLAN_COUNT = 8
EXPECTED_ROW_COUNT = 16
SUBJECT_PPD_ID = "10"
SUBJECT_PERIOD = "2022"
AS_OF = "2026-07-25T00:00:00Z"
FY_START, FY_END = 2021, 2022


@pytest.fixture
def raw_records() -> list[dict[str, object]]:
    return parse_qvariables_json(QVARIABLES_FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture
def request_url() -> str:
    return build_qvariables_url(
        variables=list(ppd_variables.request_variable_names()),
        fy_start=FY_START,
        fy_end=FY_END,
    )


# --- Client / cache seam -------------------------------------------------------


def test_qvariables_url_construction() -> None:
    url = build_qvariables_url(variables=["ppd_id", "fy"], fy_start=2021, fy_end=2022)
    assert url.startswith("https://publicplansdata.org/api/?")
    assert "q=QVariables" in url
    assert "variables=ppd_id%2Cfy" in url
    assert "filterfystart=2021" in url and "filterfyend=2022" in url
    assert "format=json" in url


def test_cache_records_and_reads_offline(
    tmp_path: Path, raw_records: list[dict[str, object]]
) -> None:
    cache = PpdResponseCache(tmp_path / "ppd_cache")
    variables = list(ppd_variables.request_variable_names())
    cache.record_qvariables(
        QVARIABLES_FIXTURE.read_text(encoding="utf-8"),
        variables=variables,
        fy_start=FY_START,
        fy_end=FY_END,
    )
    # No client supplied -> proves the read is fully offline (cache hit only).
    loaded = cache.load_qvariables(variables=variables, fy_start=FY_START, fy_end=FY_END)
    assert len(loaded) == EXPECTED_ROW_COUNT

    # A cache miss with no client must not silently reach the network.
    with pytest.raises(FileNotFoundError):
        cache.load_qvariables(variables=["ppd_id"], fy_start=1999, fy_end=1999)


# --- Codebook parse ------------------------------------------------------------


def test_codebook_parses_variable_dictionary() -> None:
    codebook = parse_codebook_csv(CODEBOOK_FIXTURE.read_text(encoding="utf-8"))
    assert variable_name("ppd_id") in codebook
    assert variable_name("funded_ratio_ava") in codebook
    # Every mapped variable is present in this codebook fixture (no drift).
    assert missing_mapped_variables(codebook) == ()


# --- Field mapping -------------------------------------------------------------


def _subject_record(raw_records: list[dict[str, object]]) -> dict[str, object]:
    return next(
        r
        for r in raw_records
        if str(r[variable_name("ppd_id")]) == "10" and int(r[variable_name("fy")]) == 2022  # type: ignore[call-overload]
    )


def test_mapping_populates_funded_status(raw_records: list[dict[str, object]]) -> None:
    """Field mapping is correct for funded ratio (AVA & MVA) / contributions / benefits.

    This is the test the deliberate-break toggles. The funded-ratio-AVA assertion
    reads ``BenchmarkPanelInput.funded_ratio_ava`` -- a *direct* mapping with no
    MVA fallback -- so renaming the AVA variable in the mapping table drops it to
    ``None`` and this test fails.
    """
    subject = _subject_record(raw_records)

    panel = to_benchmark_panel_input(subject)
    assert panel.plan_id == "10"
    assert panel.plan_period == "2022"
    # fixture funded ratio for plan 10 FY2022 = 80.0% -> fraction 0.80 (direct, no fallback)
    assert panel.funded_ratio_ava == pytest.approx(0.80, abs=1e-9)
    assert panel.funded_ratio_mva is not None  # derived MktAssets / AAL
    assert panel.aal_usd is not None
    assert panel.uaal_usd is not None

    trend = to_funding_trend_input(subject)
    assert trend.employer_contributions_usd == pytest.approx(1.20e9)
    assert trend.employee_contributions_usd == pytest.approx(0.55e9)
    assert trend.benefit_payments_usd == pytest.approx(1.60e9)


def test_peer_group_derivation() -> None:
    general_large = {
        variable_name("plan_type"): 1,
        variable_name("mkt_assets"): 25.0e9,
        variable_name("state"): "ca",
        variable_name("act_liabilities"): 30.0e9,
        variable_name("act_assets"): 25.0e9,
    }
    assert derive_plan_type(general_large) == "general"
    assert derive_size(general_large) == "large"
    assert derive_state(general_large) == "CA"
    assert derive_maturity(general_large) == "mature"  # liab/assets = 1.2 >= 1.10

    teacher_small = {
        variable_name("plan_type"): 2,
        variable_name("mkt_assets"): 1.0e9,
        variable_name("active_members"): 10000,
        variable_name("beneficiaries"): 5000,
    }
    assert derive_plan_type(teacher_small) == "teacher"
    assert derive_size(teacher_small) == "small"
    assert derive_maturity(teacher_small) == "growing"  # support ratio 0.5 < 1.0


# --- Ingest: count, provenance, idempotency ------------------------------------


def test_ingest_row_count_and_plan_universe(
    raw_records: list[dict[str, object]], request_url: str
) -> None:
    dataset = ingest_records(raw_records, as_of=AS_OF, url=request_url)
    assert len(dataset) == EXPECTED_ROW_COUNT
    assert len(dataset.plan_ids) == EXPECTED_PLAN_COUNT


def test_every_row_carries_ppd_provenance(
    raw_records: list[dict[str, object]], request_url: str
) -> None:
    dataset = ingest_records(raw_records, as_of=AS_OF, url=request_url)
    for row in dataset.ordered_rows():
        assert row.provenance.source == PPD_SOURCE_NAME
        assert row.provenance.as_of == AS_OF
        assert row.provenance.url == request_url
        assert row.provenance.url.startswith("https://publicplansdata.org/api/")


def test_refresh_is_idempotent(
    tmp_path: Path, raw_records: list[dict[str, object]], request_url: str
) -> None:
    store = PeerUniverseStore(tmp_path / "peer_universe.json")

    first = ingest_records(raw_records, as_of=AS_OF, url=request_url)
    store.write(first)
    first_bytes = store.path.read_bytes()

    # Re-run over the SAME dataset object and the same source records.
    second = ingest_records(raw_records, as_of=AS_OF, url=request_url, dataset=first)
    store.write(second)
    second_bytes = store.path.read_bytes()

    assert len(second) == EXPECTED_ROW_COUNT  # no duplicate rows
    assert first_bytes == second_bytes  # byte-identical -> idempotent

    reloaded = store.read()
    assert len(reloaded) == EXPECTED_ROW_COUNT


# --- Saved views run on ingested peers (no fixtures) ---------------------------


def test_allocation_peer_compare_from_ingested_dataset(
    raw_records: list[dict[str, object]], request_url: str
) -> None:
    dataset = ingest_records(raw_records, as_of=AS_OF, url=request_url)
    inputs = to_allocation_peer_inputs(dataset)
    rows = execute_allocation_peer_compare_view(
        inputs, subject_plan_id=SUBJECT_PPD_ID, plan_period=SUBJECT_PERIOD
    )
    assert rows, "expected allocation peer rows for the subject plan"
    equity = next(r for r in rows if r.asset_class == "equity")
    # Subject (plan 10) is in the general:large cohort with plans 11 and 12,
    # so peer mean/median/delta must be real numbers.
    assert equity.peer_mean_pct is not None
    assert equity.peer_median_pct is not None
    assert equity.delta_vs_peer_mean_pct is not None
    # plan10 equity=52, peers 11=55, 12=48 -> mean 51.5, delta 0.5
    assert equity.plan_allocation_pct == pytest.approx(52.0)
    assert equity.peer_mean_pct == pytest.approx(51.5)
    assert equity.delta_vs_peer_mean_pct == pytest.approx(0.5)


def test_funding_trend_from_ingested_dataset(
    raw_records: list[dict[str, object]], request_url: str
) -> None:
    dataset = ingest_records(raw_records, as_of=AS_OF, url=request_url)
    inputs = to_funding_trend_inputs(dataset)
    rows = execute_funding_trend_view(inputs)
    subject_rows = [r for r in rows if r.plan_id == SUBJECT_PPD_ID]
    assert len(subject_rows) == 2  # two fiscal years
    # 2021 -> 0.78, 2022 -> 0.80, so the 2022 change is +0.02
    fy2022 = next(r for r in subject_rows if r.plan_period == "2022")
    assert fy2022.funded_ratio == pytest.approx(0.80, abs=1e-9)
    assert fy2022.funded_ratio_change == pytest.approx(0.02, abs=1e-9)
    assert fy2022.net_external_cash_flow_usd is not None


# --- Deliberate-break demonstration -------------------------------------------


def test_deliberate_break_variable_rename_breaks_mapping(
    monkeypatch: pytest.MonkeyPatch, raw_records: list[dict[str, object]]
) -> None:
    """Renaming a mapped PPD variable makes the mapping lose the funded ratio.

    Simulates the one-line edit in ``variables.py`` (here via monkeypatch so the
    file stays intact) and asserts the mapping breaks, mirroring the manual
    break->revert demonstration recorded in the PR.
    """
    subject = _subject_record(raw_records)
    # Sanity: correct mapping first (direct AVA path).
    assert to_benchmark_panel_input(subject).funded_ratio_ava == pytest.approx(0.80, abs=1e-9)

    broken = dict(ppd_variables.PPD_VARIABLES)
    original = broken["funded_ratio_ava"]
    broken["funded_ratio_ava"] = ppd_variables.PpdVariable(
        name="ActFundedRatio_GASB_WRONG",
        confidence=original.confidence,
        note=original.note,
    )
    monkeypatch.setattr(ppd_variables, "PPD_VARIABLES", broken)

    # With the renamed variable the AVA funded-ratio lookup misses the fixture key,
    # so the direct mapping drops to None -- exactly what breaks the mapping test.
    assert to_benchmark_panel_input(subject).funded_ratio_ava is None
