"""Entity registry, lookup, and lineage services for cross-plan discovery."""

from pension_data.entities.alias_pipeline import (
    AliasReviewQueueCandidate,
    AliasRoutingDecision,
    CapturedAliasObservation,
    build_alias_review_queue_candidates,
    capture_alias_observations,
    route_alias_observations,
)
from pension_data.entities.lineage import (
    current_canonical_entity_id,
    historical_predecessors,
    record_lineage_event,
    successor_chain,
)
from pension_data.entities.lookup_service import (
    EntityExposureIndex,
    LookupExecutionTrace,
    build_entity_exposure_index,
    lookup_entity_exposures,
    resolve_canonical_entity_id,
)
from pension_data.entities.matching import (
    AliasMatchCandidate,
    CanonicalEntityAliasRecord,
    generate_alias_match_candidates,
)
from pension_data.entities.models import CanonicalEntityDraft, SourceRecordProvenance
from pension_data.entities.service import (
    build_canonical_stable_id,
    create_canonical_entity,
    link_source_record,
    list_active_canonical_entities,
    merge_canonical_entities,
    update_canonical_entity_metadata,
)

__all__ = [
    "AliasMatchCandidate",
    "AliasReviewQueueCandidate",
    "AliasRoutingDecision",
    "CanonicalEntityAliasRecord",
    "CanonicalEntityDraft",
    "CapturedAliasObservation",
    "EntityExposureIndex",
    "LookupExecutionTrace",
    "SourceRecordProvenance",
    "build_alias_review_queue_candidates",
    "build_canonical_stable_id",
    "build_entity_exposure_index",
    "capture_alias_observations",
    "create_canonical_entity",
    "current_canonical_entity_id",
    "generate_alias_match_candidates",
    "historical_predecessors",
    "link_source_record",
    "list_active_canonical_entities",
    "lookup_entity_exposures",
    "merge_canonical_entities",
    "record_lineage_event",
    "resolve_canonical_entity_id",
    "route_alias_observations",
    "successor_chain",
    "update_canonical_entity_metadata",
]
