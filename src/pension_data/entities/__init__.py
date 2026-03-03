"""Entity lookup, alias, and lineage services for cross-plan discovery."""

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

__all__ = [
    "AliasMatchCandidate",
    "AliasReviewQueueCandidate",
    "AliasRoutingDecision",
    "CanonicalEntityAliasRecord",
    "CapturedAliasObservation",
    "EntityExposureIndex",
    "LookupExecutionTrace",
    "build_entity_exposure_index",
    "build_alias_review_queue_candidates",
    "capture_alias_observations",
    "generate_alias_match_candidates",
    "current_canonical_entity_id",
    "historical_predecessors",
    "lookup_entity_exposures",
    "record_lineage_event",
    "route_alias_observations",
    "resolve_canonical_entity_id",
    "successor_chain",
]
