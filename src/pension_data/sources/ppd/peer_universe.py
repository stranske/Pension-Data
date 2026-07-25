"""Peer-universe dataset: ingest PPD records, persist, and feed the saved views.

Each persisted row is keyed by ``ppd_id`` + fiscal year and carries PPD
provenance (``source=PPD``, ``as_of``, ``url``). Ingest is idempotent: rows are
keyed, so re-running a refresh over the same source data yields the identical
dataset. The dataset projects back into the existing ``query/saved_views`` input
dataclasses, so ``allocation_peer_compare`` / ``funding_trend`` run on real
ingested peers rather than fixtures.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from pension_data.db.models.provenance import EvidenceReference, MetricEvidenceLink
from pension_data.extract.common.evidence import build_evidence_reference
from pension_data.extract.common.ids import stable_id
from pension_data.provenance.export import export_citation_ready_provenance_payload
from pension_data.query.saved_views.models import (
    AllocationPeerInput,
    BenchmarkPanelInput,
    FundingTrendInput,
)
from pension_data.sources.ppd.mapping import (
    _as_fraction,
    _funded_ratio_mva,
    _num,
    derive_peer_group,
    derive_peer_group_components,
    fiscal_year_of,
    ppd_id_of,
)
from pension_data.sources.ppd.variables import ALLOCATION_CLASS_SLUGS

PPD_SOURCE_NAME = "PPD"


@dataclass(frozen=True, slots=True)
class PpdProvenance:
    """Row-level provenance for a PPD-sourced record."""

    source: str
    as_of: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {"source": self.source, "as_of": self.as_of, "url": self.url}


@dataclass(frozen=True, slots=True)
class PeerUniverseRow:
    """One peer-universe row keyed by ``ppd_id`` + fiscal year."""

    ppd_id: str
    fy: int
    peer_group: str
    plan_type: str
    size: str
    state: str
    maturity: str
    funded_ratio_ava: float | None
    funded_ratio_mva: float | None
    employer_contributions_usd: float | None
    employee_contributions_usd: float | None
    benefit_payments_usd: float | None
    aal_usd: float | None
    uaal_usd: float | None
    allocations: dict[str, float]
    provenance: PpdProvenance

    @property
    def row_id(self) -> str:
        """Stable identity for dedup / idempotency."""
        return stable_id("ppd-peer", self.ppd_id, self.fy)

    @property
    def plan_period(self) -> str:
        return str(self.fy)

    def to_dict(self) -> dict[str, object]:
        return {
            "row_id": self.row_id,
            "ppd_id": self.ppd_id,
            "fy": self.fy,
            "peer_group": self.peer_group,
            "plan_type": self.plan_type,
            "size": self.size,
            "state": self.state,
            "maturity": self.maturity,
            "funded_ratio_ava": self.funded_ratio_ava,
            "funded_ratio_mva": self.funded_ratio_mva,
            "employer_contributions_usd": self.employer_contributions_usd,
            "employee_contributions_usd": self.employee_contributions_usd,
            "benefit_payments_usd": self.benefit_payments_usd,
            "aal_usd": self.aal_usd,
            "uaal_usd": self.uaal_usd,
            "allocations": dict(sorted(self.allocations.items())),
            "provenance": self.provenance.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> PeerUniverseRow:
        prov = data["provenance"]
        assert isinstance(prov, dict)
        raw_allocations = data.get("allocations") or {}
        assert isinstance(raw_allocations, dict)
        return cls(
            ppd_id=str(data["ppd_id"]),
            fy=int(_require_number(data["fy"])),
            peer_group=str(data["peer_group"]),
            plan_type=str(data["plan_type"]),
            size=str(data["size"]),
            state=str(data["state"]),
            maturity=str(data["maturity"]),
            funded_ratio_ava=_opt_float(data.get("funded_ratio_ava")),
            funded_ratio_mva=_opt_float(data.get("funded_ratio_mva")),
            employer_contributions_usd=_opt_float(data.get("employer_contributions_usd")),
            employee_contributions_usd=_opt_float(data.get("employee_contributions_usd")),
            benefit_payments_usd=_opt_float(data.get("benefit_payments_usd")),
            aal_usd=_opt_float(data.get("aal_usd")),
            uaal_usd=_opt_float(data.get("uaal_usd")),
            allocations={str(k): _require_number(v) for k, v in raw_allocations.items()},
            provenance=PpdProvenance(
                source=str(prov["source"]), as_of=str(prov["as_of"]), url=str(prov["url"])
            ),
        )


def _require_number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"expected a numeric value, got {value!r}")
    return float(value)


def _opt_float(value: object) -> float | None:
    return None if value is None else _require_number(value)


@dataclass(slots=True)
class PeerUniverseDataset:
    """A keyed peer-universe dataset with deterministic ordering."""

    rows: dict[tuple[str, int], PeerUniverseRow] = field(default_factory=dict)

    def upsert(self, row: PeerUniverseRow) -> None:
        """Insert or replace a row by its ``(ppd_id, fy)`` key (idempotent)."""
        self.rows[(row.ppd_id, row.fy)] = row

    def ordered_rows(self) -> list[PeerUniverseRow]:
        return [self.rows[key] for key in sorted(self.rows)]

    def __len__(self) -> int:
        return len(self.rows)

    @property
    def plan_ids(self) -> set[str]:
        return {ppd_id for (ppd_id, _fy) in self.rows}


def map_record_to_row(record: dict[str, object], *, provenance: PpdProvenance) -> PeerUniverseRow:
    """Map one raw PPD record to a fully-populated peer-universe row."""
    components = derive_peer_group_components(record)
    allocations: dict[str, float] = {}
    for asset_class, slug in ALLOCATION_CLASS_SLUGS.items():
        raw = _num(record, slug)
        if raw is None or raw < 0.0:
            continue
        allocations[asset_class] = raw * 100.0 if raw <= 1.0 else raw
    return PeerUniverseRow(
        ppd_id=ppd_id_of(record),
        fy=fiscal_year_of(record),
        peer_group=derive_peer_group(record),
        plan_type=components["plan_type"],
        size=components["size"],
        state=components["state"],
        maturity=components["maturity"],
        funded_ratio_ava=_as_fraction(_num(record, "funded_ratio_ava")),
        funded_ratio_mva=_funded_ratio_mva(record),
        employer_contributions_usd=_num(record, "employer_contrib"),
        employee_contributions_usd=_num(record, "employee_contrib"),
        benefit_payments_usd=_num(record, "benefit_payments"),
        aal_usd=_num(record, "act_liabilities"),
        uaal_usd=_num(record, "uaal"),
        allocations=allocations,
        provenance=provenance,
    )


def ingest_records(
    records: list[dict[str, object]],
    *,
    as_of: str,
    url: str,
    dataset: PeerUniverseDataset | None = None,
) -> PeerUniverseDataset:
    """Ingest raw PPD records into a keyed, provenance-bearing dataset.

    Passing an existing ``dataset`` re-runs the ingest in place; because rows are
    keyed by ``(ppd_id, fy)`` the operation is idempotent for identical source
    data (no duplicate rows).
    """
    dataset = dataset or PeerUniverseDataset()
    provenance = PpdProvenance(source=PPD_SOURCE_NAME, as_of=as_of, url=url)
    for record in records:
        dataset.upsert(map_record_to_row(record, provenance=provenance))
    return dataset


class PeerUniverseStore:
    """On-disk persistence of a peer-universe dataset as deterministic JSON."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def write(self, dataset: PeerUniverseDataset) -> Path:
        """Persist the dataset; identical datasets produce identical bytes."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": PPD_SOURCE_NAME,
            "row_count": len(dataset),
            "rows": [row.to_dict() for row in dataset.ordered_rows()],
        }
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return self.path

    def read(self) -> PeerUniverseDataset:
        """Load a persisted dataset back into memory."""
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        dataset = PeerUniverseDataset()
        for row_data in payload.get("rows", []):
            dataset.upsert(PeerUniverseRow.from_dict(row_data))
        return dataset


# --- Projection into the saved-views input dataclasses -------------------------


def to_funding_trend_inputs(dataset: PeerUniverseDataset) -> list[FundingTrendInput]:
    """Project the dataset into funding-trend inputs (real ingested peers)."""
    inputs: list[FundingTrendInput] = []
    for row in dataset.ordered_rows():
        funded_ratio = row.funded_ratio_ava
        if funded_ratio is None:
            funded_ratio = row.funded_ratio_mva
        inputs.append(
            FundingTrendInput(
                plan_id=row.ppd_id,
                plan_period=row.plan_period,
                funded_ratio=funded_ratio if funded_ratio is not None else 0.0,
                employer_contributions_usd=row.employer_contributions_usd,
                employee_contributions_usd=row.employee_contributions_usd,
                benefit_payments_usd=row.benefit_payments_usd,
            )
        )
    return inputs


def to_allocation_peer_inputs(dataset: PeerUniverseDataset) -> list[AllocationPeerInput]:
    """Project the dataset into allocation-peer inputs, one row per asset class."""
    inputs: list[AllocationPeerInput] = []
    for row in dataset.ordered_rows():
        for asset_class in sorted(row.allocations):
            inputs.append(
                AllocationPeerInput(
                    plan_id=row.ppd_id,
                    plan_period=row.plan_period,
                    peer_group=row.peer_group,
                    asset_class=asset_class,
                    allocation_pct=row.allocations[asset_class],
                )
            )
    return inputs


def to_benchmark_panel_inputs(dataset: PeerUniverseDataset) -> list[BenchmarkPanelInput]:
    """Project the dataset into benchmark-panel inputs."""
    return [
        BenchmarkPanelInput(
            plan_id=row.ppd_id,
            plan_period=row.plan_period,
            peer_group=row.peer_group,
            funded_ratio_ava=row.funded_ratio_ava,
            funded_ratio_mva=row.funded_ratio_mva,
            aal_usd=row.aal_usd,
            uaal_usd=row.uaal_usd,
        )
        for row in dataset.ordered_rows()
    ]


# --- Integration with the existing provenance/citation layer -------------------


def build_row_evidence_references(dataset: PeerUniverseDataset) -> tuple[EvidenceReference, ...]:
    """Build :class:`EvidenceReference` records for each row via the provenance layer.

    Ties the ingested rows into the repository's existing citation-export path:
    the request URL is the source document and each row anchors to its
    ``ppd_id``/fy, so ``export_citation_ready_provenance_payload`` can serialize
    PPD-sourced peer rows the same way as PDF-sourced metrics.
    """
    references: dict[str, EvidenceReference] = {}
    for row in dataset.ordered_rows():
        evidence = build_evidence_reference(
            report_id=row.provenance.source,
            source_document_id=row.provenance.url,
            evidence_ref=f"text:ppd_id={row.ppd_id};fy={row.fy}",
            excerpt=f"PPD peer row for plan {row.ppd_id} FY{row.fy}",
        )
        references[evidence.evidence_ref_id] = evidence
    return tuple(sorted(references.values(), key=lambda ref: ref.evidence_ref_id))


def build_citation_payload(dataset: PeerUniverseDataset) -> dict[str, dict[str, object]]:
    """Export a citation-ready provenance payload for the ingested dataset."""
    references = build_row_evidence_references(dataset)
    ref_by_row: dict[str, EvidenceReference] = {}
    links: list[MetricEvidenceLink] = []
    rows_by_anchor = {
        f"text:ppd_id={row.ppd_id};fy={row.fy}": row for row in dataset.ordered_rows()
    }
    for ref in references:
        anchor = ref.snippet_anchor
        row = rows_by_anchor.get(anchor) if anchor else None
        if row is None:
            continue
        metric_row_id = row.row_id
        ref_by_row[metric_row_id] = ref
        links.append(
            MetricEvidenceLink(
                link_id=stable_id("ppd-peer-link", metric_row_id, ref.evidence_ref_id),
                metric_row_id=metric_row_id,
                metric_family="peer_universe",
                metric_name="ppd_peer_row",
                evidence_ref_id=ref.evidence_ref_id,
            )
        )
    return export_citation_ready_provenance_payload(
        metric_evidence_links=links,
        evidence_references=references,
    )
