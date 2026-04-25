"""Model namespace for pension data domain objects."""

from pension_data.db.models.api_keys import APIKeyRecord
from pension_data.db.models.artifacts import IngestionRunMetrics, RawArtifactRecord
from pension_data.db.models.consultant_attribution import ConsultantAttributionObservation
from pension_data.db.models.consultants import (
    ConsultantEntity,
    ConsultantRecommendation,
    PlanConsultantEngagement,
)
from pension_data.db.models.core_facts import (
    ActuarialFact,
    AllocationFact,
    BitemporalFactContext,
    CashFlowFact,
    ConsultantEngagementFact,
    DualReportedValue,
    FeeFact,
    FundedStatusFact,
    HoldingFact,
    ManagerFundVehicleRelationship,
    query_bitemporal_as_of,
)
from pension_data.db.models.entities import CanonicalEntityRecord, EntitySourceRecordLink
from pension_data.db.models.entity_lineage import EntityLineageEvent
from pension_data.db.models.financial_flows import PlanFinancialFlow
from pension_data.db.models.funded_actuarial import ExtractionDiagnostic, FundedActuarialStagingFact
from pension_data.db.models.inventory import AnnualReportCoverageRecord, DiscoveredInventoryRecord
from pension_data.db.models.investment_allocations_fees import (
    AssetAllocationObservation,
    InvestmentExtractionWarning,
    ManagerFeeObservation,
)
from pension_data.db.models.investment_positions import PlanManagerFundPosition
from pension_data.db.models.manager_lifecycle import ManagerLifecycleEvent
from pension_data.db.models.provenance import EvidenceReference, MetricEvidenceLink
from pension_data.db.models.registry import PensionSystemRecord, V1CohortMembership
from pension_data.db.models.review_queue_entities import (
    EntityReviewAuditEntry,
    EntityReviewQueueRecord,
    EntityReviewStateSnapshot,
    UnresolvedEntityCandidate,
)
from pension_data.db.models.risk_exposures import RiskExposureObservation

# review_queue imports are deferred to avoid circular dependency:
#   db.models.review_queue → quality.confidence → quality.__init__
#   → quality.parser_output_validation → db.models.review_queue
_LAZY_REVIEW_QUEUE_NAMES = frozenset({"ExtractionReviewQueueRecord", "ReviewQueueAuditEntry"})


def __getattr__(name: str) -> object:  # noqa: N807
    if name in _LAZY_REVIEW_QUEUE_NAMES:
        from pension_data.db.models import review_queue

        return getattr(review_queue, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


__all__ = [
    "APIKeyRecord",
    "ActuarialFact",
    "AllocationFact",
    "AnnualReportCoverageRecord",
    "AssetAllocationObservation",
    "BitemporalFactContext",
    "CanonicalEntityRecord",
    "CashFlowFact",
    "ConsultantAttributionObservation",
    "ConsultantEngagementFact",
    "ConsultantEntity",
    "ConsultantRecommendation",
    "DiscoveredInventoryRecord",
    "DualReportedValue",
    "EntityLineageEvent",
    "EntityReviewAuditEntry",
    "EntityReviewQueueRecord",
    "EntityReviewStateSnapshot",
    "EntitySourceRecordLink",
    "EvidenceReference",
    "ExtractionDiagnostic",
    "ExtractionReviewQueueRecord",
    "FeeFact",
    "FundedActuarialStagingFact",
    "FundedStatusFact",
    "HoldingFact",
    "IngestionRunMetrics",
    "InvestmentExtractionWarning",
    "ManagerFeeObservation",
    "ManagerFundVehicleRelationship",
    "ManagerLifecycleEvent",
    "MetricEvidenceLink",
    "PensionSystemRecord",
    "PlanConsultantEngagement",
    "PlanFinancialFlow",
    "PlanManagerFundPosition",
    "RawArtifactRecord",
    "ReviewQueueAuditEntry",
    "RiskExposureObservation",
    "UnresolvedEntityCandidate",
    "V1CohortMembership",
    "query_bitemporal_as_of",
]
