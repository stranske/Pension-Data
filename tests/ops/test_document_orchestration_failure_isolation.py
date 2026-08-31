"""What happens to a document orchestration run when one part of it breaks.

`run_document_orchestration` wraps each extraction domain in its own `try`, records the failure in
the ledger, and carries on. That design is the whole reason the ledger exists — one malformed
section of one filing must not cost the other domains' facts, nor stop the batch. All five of those
arms were unreached: 96 of the module's 124 uncovered units sit in this one function, and every
recovery path in it was unobserved.

The tests below assert the boundary in both directions, which is the part that actually decays. It
is easy to keep "the failure is recorded" working while quietly widening what a failure destroys —
a `raise` moved one level out, a shared accumulator reset — and no test of the happy path notices.
So each case names exactly which artifacts that domain owns, and asserts every other artifact is
untouched.
"""

from __future__ import annotations

import dataclasses

import pytest

import pension_data.ops.document_orchestration as orchestration
from pension_data.ops.document_orchestration import (
    DocumentOrchestrationState,
    SourceDocumentJobItem,
    run_document_orchestration,
)

from .test_document_orchestration import _job, _parser

_ARTIFACT_KEYS = (
    "published_rows",
    "financial_flow_rows",
    "risk_exposure_rows",
    "consultant_entity_rows",
    "consultant_engagement_rows",
    "consultant_recommendation_rows",
    "consultant_attribution_rows",
    "manager_relationship_rows",
    "lifecycle_event_rows",
    "review_queue_rows",
)

# extractor attribute -> (domain name in the ledger, artifact keys that domain is responsible for)
_DOMAINS = {
    "extract_funded_and_actuarial_metrics": ("funded_actuarial", {"published_rows"}),
    "extract_plan_financial_flow": (
        "financial_flow",
        {"financial_flow_rows", "review_queue_rows"},
    ),
    "extract_asset_allocations": ("allocation_fee", {"published_rows", "review_queue_rows"}),
    "extract_consultant_records": (
        "consultant",
        {
            "consultant_entity_rows",
            "consultant_engagement_rows",
            "consultant_recommendation_rows",
            "consultant_attribution_rows",
        },
    ),
    "build_manager_fund_positions": (
        "manager_position",
        {"published_rows", "manager_relationship_rows", "lifecycle_event_rows"},
    ),
}


def _document() -> SourceDocumentJobItem:
    return _job(
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        source_document_id="doc:ca:2024:v1",
        content=b"doc-v1",
    )


def _run(documents=None, **kwargs):
    return run_document_orchestration(
        documents=documents if documents is not None else [_document()],
        parser=_parser,
        state=DocumentOrchestrationState(),
        run_id="isolation-run",
        max_retries_per_stage=1,
        **kwargs,
    )


def _counts(artifacts: dict[str, object]) -> dict[str, int]:
    return {key: len(artifacts[key]) for key in _ARTIFACT_KEYS}  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def baseline() -> dict[str, int]:
    ledger, _, artifacts = _run()
    assert ledger.status == "success"
    assert not ledger.failures
    counts = _counts(artifacts)
    assert all(count > 0 for count in counts.values()), counts
    return counts


# ---------------------------------------------------------------------------------------------
# One broken domain costs that domain and nothing else.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("extractor", sorted(_DOMAINS))
def test_a_broken_domain_does_not_take_the_others_down(extractor, baseline, monkeypatch):
    domain, owned_keys = _DOMAINS[extractor]

    def explode(*args, **kwargs):
        raise RuntimeError(f"{domain} extractor blew up")

    monkeypatch.setattr(orchestration, extractor, explode)
    ledger, _, artifacts = _run()
    counts = _counts(artifacts)

    untouched = {key: counts[key] for key in _ARTIFACT_KEYS if key not in owned_keys}
    expected = {key: baseline[key] for key in untouched}
    assert untouched == expected, f"{domain} failure leaked into another domain's artifacts"

    assert any(
        counts[key] < baseline[key] for key in owned_keys
    ), f"{domain} was broken but produced its rows anyway — the test is not reaching the arm"


@pytest.mark.parametrize("extractor", sorted(_DOMAINS))
def test_a_broken_domain_is_named_in_exactly_one_failure(extractor, monkeypatch):
    """The ledger has to say WHICH domain failed, or the run reports a problem nobody can locate.

    Exactly one, too: a failure recorded per retry or per row turns one bad section into an
    unreadable ledger.
    """
    domain, _ = _DOMAINS[extractor]

    def explode(*args, **kwargs):
        raise RuntimeError(f"{domain} extractor blew up")

    monkeypatch.setattr(orchestration, extractor, explode)
    ledger, _, _ = _run()

    matching = [failure for failure in ledger.failures if f"domain={domain}" in failure.message]
    assert len(matching) == 1
    assert matching[0].stage == "parse_extract"
    assert matching[0].document_key is not None
    assert "RuntimeError" in matching[0].message
    assert "blew up" in matching[0].message


@pytest.mark.parametrize("extractor", sorted(_DOMAINS))
def test_the_document_is_still_processed_and_still_publishes_facts(extractor, monkeypatch):
    """The outcome that matters to the caller: a partial extraction is a partial success, not a
    discarded document. Marking it `failed` would send a filing back through ingestion for the
    sake of one broken section."""
    domain, _ = _DOMAINS[extractor]

    monkeypatch.setattr(
        orchestration, extractor, lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    ledger, _, _ = _run()

    outcome = ledger.document_outcomes[0]
    assert outcome.status == "processed"
    assert outcome.promoted_fact_count > 0


@pytest.mark.parametrize("extractor", sorted(_DOMAINS))
def test_the_run_reports_failed_so_the_partial_result_is_not_mistaken_for_a_clean_one(
    extractor, monkeypatch
):
    """The counterweight to the test above. Continuing is right; reporting `success` would let a
    run that silently dropped a whole domain look identical to one that extracted everything."""
    monkeypatch.setattr(
        orchestration, extractor, lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    ledger, _, _ = _run()
    assert ledger.status == "failed"


def test_two_broken_domains_are_reported_separately(monkeypatch):
    """Collapsing several domain failures into one entry loses the second one entirely."""
    monkeypatch.setattr(
        orchestration,
        "extract_plan_financial_flow",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("flow")),
    )
    monkeypatch.setattr(
        orchestration,
        "extract_consultant_records",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("consultant")),
    )
    ledger, _, _ = _run()

    domains = {
        failure.message.split("domain=", 1)[1].split(" ", 1)[0]
        for failure in ledger.failures
        if "domain=" in failure.message
    }
    assert domains == {"financial_flow", "consultant"}


# ---------------------------------------------------------------------------------------------
# Discovery: a malformed job item stops the run before anything is ingested.
# ---------------------------------------------------------------------------------------------


_REQUIRED_FIELDS = (
    "plan_id",
    "plan_period",
    "source_url",
    "source_document_id",
    "effective_date",
    "ingestion_date",
    "mime_type",
    "fetched_at",
)


@pytest.mark.parametrize("field", _REQUIRED_FIELDS)
@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_required_field_fails_the_run_at_discovery(field, blank):
    """Each of these is a join key or a provenance field. A row ingested without one cannot be
    superseded, deduplicated, or traced back to its filing — and whitespace has to count as blank,
    because a CSV export of an empty cell is a space more often than it is nothing.
    """
    broken = dataclasses.replace(_document(), **{field: blank})
    ledger, state, artifacts = _run(documents=[broken])

    assert ledger.status == "failed"
    assert len(ledger.failures) == 1
    assert ledger.failures[0].stage == "discovery"
    assert field in ledger.failures[0].message
    assert ledger.document_outcomes == ()
    assert artifacts == {}


def test_a_discovery_failure_leaves_the_state_untouched():
    """Nothing was ingested, so nothing may be recorded as ingested. A state advanced past a run
    that did no work makes the next run skip documents it never processed."""
    before = DocumentOrchestrationState()
    ledger, after, _ = run_document_orchestration(
        documents=[dataclasses.replace(_document(), plan_id="")],
        parser=_parser,
        state=before,
        run_id="discovery-failure",
        max_retries_per_stage=1,
    )
    assert ledger.status == "failed"
    assert after == before


def test_a_discovery_failure_still_records_the_stage_metric():
    """A run that reports a failure with no stage metric gives the operator a verdict and no place
    to look."""
    ledger, _, _ = _run(documents=[dataclasses.replace(_document(), source_url="")])

    discovery = [metric for metric in ledger.stage_metrics if metric.stage == "discovery"]
    assert len(discovery) == 1
    assert discovery[0].status == "error"
    assert discovery[0].error_count == 1
    assert discovery[0].record_count == 0


def test_one_malformed_document_stops_the_whole_batch():
    """Discovery validates the contract, not the content: if the CALLER built the job list wrong,
    processing the well-formed half writes a partial batch that looks complete.
    """
    good = _document()
    bad = dataclasses.replace(
        good,
        source_url="https://example.org/other.pdf",
        source_document_id="",
    )
    ledger, _, artifacts = _run(documents=[good, bad])

    assert ledger.status == "failed"
    assert artifacts == {}
    assert ledger.document_outcomes == ()


def test_an_empty_document_list_is_not_a_failure():
    """Distinct from every case above: there was nothing to do, and that is not an error."""
    ledger, _, artifacts = _run(documents=[])

    assert ledger.status != "failed"
    assert ledger.failures == ()
    discovery = [metric for metric in ledger.stage_metrics if metric.stage == "discovery"]
    assert discovery[0].status == "ok"
    assert discovery[0].record_count == 0
    assert artifacts != {}
