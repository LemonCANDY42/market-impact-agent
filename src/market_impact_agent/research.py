from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256

from market_impact_agent.domain import require_aware


class EvidenceTier(StrEnum):
    OFFICIAL = "official"
    REGULATED = "regulated"
    PRIMARY = "primary"
    ESTABLISHED_NEWS = "established_news"
    SPECIALIST = "specialist"
    COMMUNITY = "community"
    UNVERIFIED = "unverified"


class AssessmentMode(StrEnum):
    FAST = "fast"
    DEEP = "deep"
    COMBINED = "combined"


class EventArchetype(StrEnum):
    ISSUER_CORPORATE = "issuer_corporate"
    MACRO_REAL_ECONOMY = "macro_real_economy"
    POLICY_REGULATORY = "policy_regulatory"
    GEOPOLITICAL_SECURITY = "geopolitical_security"
    PHYSICAL_SUPPLY_LOGISTICS = "physical_supply_logistics"
    CLIMATE_NATURAL_HAZARD = "climate_natural_hazard"
    TECHNOLOGY_DEMAND_ADOPTION = "technology_demand_adoption"
    FINANCIAL_MARKET_MECHANICS = "financial_market_mechanics"


class RevelationMode(StrEnum):
    SCHEDULED = "scheduled"
    UNSCHEDULED = "unscheduled"
    CONTINUOUS = "continuous"
    RETROSPECTIVE_REVISION = "retrospective_revision"


class EventStage(StrEnum):
    PRE_EVENT = "pre_event"
    FIRST_OBSERVED = "first_observed"
    CORROBORATED = "corroborated"
    QUANTIFIED_OR_REALIZED = "quantified_or_realized"
    DIFFUSING = "diffusing"
    RESOLVED = "resolved"
    REVISED_OR_INVALIDATED = "revised_or_invalidated"


class TransmissionChannel(StrEnum):
    REVENUE_DEMAND = "revenue_demand"
    CAPACITY_COST_INVENTORY = "capacity_cost_inventory"
    CLAIMS_CAPITAL_ALLOCATION = "claims_capital_allocation"
    POLICY_ACCESS = "policy_access"
    FUNDING_DISCOUNT_FX = "funding_discount_fx"
    RISK_UNCERTAINTY_INSURANCE = "risk_uncertainty_insurance"
    EXPECTATIONS_ATTENTION = "expectations_attention"
    POSITIONING_LIQUIDITY_MECHANICS = "positioning_liquidity_mechanics"


class TransmissionDirectness(StrEnum):
    DIRECT = "direct"
    SECOND_ORDER = "second_order"
    THIRD_ORDER = "third_order"
    FOURTH_ORDER = "fourth_order"


_DIRECTNESS_BY_POSITION = (
    TransmissionDirectness.DIRECT,
    TransmissionDirectness.SECOND_ORDER,
    TransmissionDirectness.THIRD_ORDER,
    TransmissionDirectness.FOURTH_ORDER,
)


class ExpectedEffect(StrEnum):
    UP = "up"
    DOWN = "down"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class ExpectationDirection(StrEnum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    MIXED = "mixed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    evidence_id: str
    claim_id: str
    source_ref: str
    source_tier: EvidenceTier
    occurred_at: datetime
    published_at: datetime
    visible_at: datetime
    retrieved_at: datetime
    claim: str
    claim_hash: str
    supersedes_id: str | None = None

    def __post_init__(self) -> None:
        _require_nonempty(self.evidence_id, "evidence_id")
        _require_nonempty(self.claim_id, "claim_id")
        _require_nonempty(self.source_ref, "source_ref")
        _require_nonempty(self.claim, "claim")
        for name in ("occurred_at", "published_at", "visible_at", "retrieved_at"):
            require_aware(getattr(self, name), name)
        if self.published_at > self.visible_at:
            raise ValueError("published_at must not be after visible_at")
        if self.visible_at > self.retrieved_at:
            raise ValueError("visible_at must not be after retrieved_at")
        if self.claim_hash != sha256(self.claim.encode()).hexdigest():
            raise ValueError("claim_hash must match claim")
        if self.supersedes_id == self.evidence_id:
            raise ValueError("evidence cannot supersede itself")


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    envelope_id: str
    event_id: str
    as_of: datetime
    evidence: tuple[EvidenceItem, ...]

    def __post_init__(self) -> None:
        _require_nonempty(self.envelope_id, "envelope_id")
        _require_nonempty(self.event_id, "event_id")
        require_aware(self.as_of, "as_of")
        if not self.evidence:
            raise ValueError("event envelopes require at least one evidence item")
        evidence_ids = [item.evidence_id for item in self.evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("event envelope evidence_id values must be unique")
        if any(item.visible_at > self.as_of for item in self.evidence):
            raise ValueError("event envelope contains future-visible evidence")
        _validate_revision_lineage(self.evidence)

    @property
    def current_evidence(self) -> tuple[EvidenceItem, ...]:
        """Return the current leaf of each visible revision chain in stable order."""
        superseded_ids = {
            item.supersedes_id for item in self.evidence if item.supersedes_id is not None
        }
        return tuple(
            sorted(
                (item for item in self.evidence if item.evidence_id not in superseded_ids),
                key=lambda item: (item.visible_at, item.evidence_id),
            )
        )

    @property
    def independent_claim_count(self) -> int:
        return len({item.claim_id for item in self.evidence})


def materialize_event_envelope(
    *,
    envelope_id: str,
    event_id: str,
    as_of: datetime,
    evidence: tuple[EvidenceItem, ...],
) -> EventEnvelope:
    require_aware(as_of, "as_of")
    evidence_ids = [item.evidence_id for item in evidence]
    if len(evidence_ids) != len(set(evidence_ids)):
        raise ValueError("evidence_id values must be unique before materialization")
    _validate_revision_lineage(evidence)

    eligible = tuple(
        sorted(
            (item for item in evidence if item.visible_at <= as_of),
            key=lambda item: (item.visible_at, item.evidence_id),
        )
    )
    return EventEnvelope(
        envelope_id=envelope_id,
        event_id=event_id,
        as_of=as_of,
        evidence=eligible,
    )


@dataclass(frozen=True, slots=True)
class AssessmentRoute:
    mode: AssessmentMode
    max_depth: int
    max_branches: int
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.max_depth < 1:
            raise ValueError("max_depth must be positive")
        if self.mode is AssessmentMode.FAST and self.max_depth > 2:
            raise ValueError("fast assessment routes cannot exceed second-order depth")
        if self.max_branches < 1:
            raise ValueError("max_branches must be positive")
        if not self.reasons:
            raise ValueError("assessment routes require at least one reason")


_FAST_EVIDENCE_TIERS = frozenset(
    {EvidenceTier.OFFICIAL, EvidenceTier.REGULATED, EvidenceTier.PRIMARY}
)


def route_event_assessment(
    envelope: EventEnvelope,
    *,
    mapping_known: bool,
    facts_disputed: bool = False,
    market_state_conflicting: bool = False,
    high_impact: bool = False,
) -> AssessmentRoute:
    weak_evidence = any(
        item.source_tier not in _FAST_EVIDENCE_TIERS for item in envelope.current_evidence
    )
    if facts_disputed or not mapping_known or weak_evidence:
        reasons: list[str] = []
        if facts_disputed:
            reasons.append("facts are disputed")
        if not mapping_known:
            reasons.append("transmission mapping is not established")
        if weak_evidence:
            reasons.append("evidence requires corroboration")
        return AssessmentRoute(
            mode=AssessmentMode.DEEP,
            max_depth=4,
            max_branches=8,
            reasons=tuple(reasons),
        )
    if market_state_conflicting or high_impact:
        reason = (
            "market state conflicts with the known mapping"
            if market_state_conflicting
            else "high-impact event requires fast output and deep follow-up"
        )
        return AssessmentRoute(
            mode=AssessmentMode.COMBINED,
            max_depth=4,
            max_branches=6,
            reasons=(reason,),
        )
    return AssessmentRoute(
        mode=AssessmentMode.FAST,
        max_depth=2,
        max_branches=3,
        reasons=("verified evidence and an established transmission mapping",),
    )


@dataclass(frozen=True, slots=True)
class ExpectationDelta:
    baseline_source_ref: str | None
    expected: str | None
    observed: str | None
    direction: ExpectationDirection
    confidence: float

    def __post_init__(self) -> None:
        if not 0 <= self.confidence <= 1:
            raise ValueError("expectation confidence must be between zero and one")
        if self.direction is not ExpectationDirection.UNKNOWN and (
            not self.baseline_source_ref or self.expected is None or self.observed is None
        ):
            raise ValueError(
                "known expectation deltas require a baseline, expected, and observed value"
            )


@dataclass(frozen=True, slots=True)
class TransmissionStep:
    step_id: str
    from_node: str
    to_node: str
    channel: TransmissionChannel
    directness: TransmissionDirectness
    mechanism: str
    affected_variable: str
    expected_effect: ExpectedEffect
    horizon_sessions: int
    confidence: float
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("step_id", "from_node", "to_node", "mechanism", "affected_variable"):
            _require_nonempty(getattr(self, name), name)
        if self.horizon_sessions < 1:
            raise ValueError("horizon_sessions must be positive")
        if not 0 <= self.confidence <= 1:
            raise ValueError("transmission confidence must be between zero and one")
        _require_unique_nonempty(self.evidence_refs, "evidence_refs")


@dataclass(frozen=True, slots=True)
class TransmissionPath:
    path_id: str
    target_ref: str
    steps: tuple[TransmissionStep, ...]
    counterevidence_refs: tuple[str, ...]
    blockers: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_nonempty(self.path_id, "path_id")
        _require_nonempty(self.target_ref, "target_ref")
        if not self.steps:
            raise ValueError("transmission paths require at least one step")
        if len(self.steps) > len(_DIRECTNESS_BY_POSITION):
            raise ValueError("transmission paths cannot exceed fourth-order directness")
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("transmission step_id values must be unique within a path")
        for index, step in enumerate(self.steps):
            if step.directness is not _DIRECTNESS_BY_POSITION[index]:
                raise ValueError(
                    f"transmission step directness must be {_DIRECTNESS_BY_POSITION[index].value} "
                    f"at position {index + 1}"
                )
            if index and step.from_node != self.steps[index - 1].to_node:
                raise ValueError("transmission path steps must be adjacent")
        if self.steps[-1].to_node != self.target_ref:
            raise ValueError("transmission path steps must end at target_ref")
        _require_unique(self.counterevidence_refs, "counterevidence_refs")
        _require_unique(self.blockers, "blockers")
        _require_unique_nonempty(self.invalidation_conditions, "invalidation_conditions")


@dataclass(frozen=True, slots=True)
class EventAssessment:
    assessment_id: str
    envelope: EventEnvelope
    archetype: EventArchetype
    revelation_mode: RevelationMode
    stage: EventStage
    route: AssessmentRoute
    expectation_delta: ExpectationDelta
    paths: tuple[TransmissionPath, ...]
    blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_nonempty(self.assessment_id, "assessment_id")
        if not self.paths and not self.blockers:
            raise ValueError("event assessments require a path or an explicit blocker")
        if len(self.paths) > self.route.max_branches:
            raise ValueError("event assessment exceeds the route branch cap")

        known_evidence = {item.evidence_id for item in self.envelope.evidence}
        baseline_ref = self.expectation_delta.baseline_source_ref
        if baseline_ref is not None and baseline_ref not in known_evidence:
            raise ValueError(f"unknown expectation baseline reference: {baseline_ref}")
        path_ids: set[str] = set()
        for path in self.paths:
            if path.path_id in path_ids:
                raise ValueError("transmission path_id values must be unique")
            path_ids.add(path.path_id)
            if len(path.steps) > self.route.max_depth:
                raise ValueError("transmission path exceeds the route depth cap")
            supporting = {ref for step in path.steps for ref in step.evidence_refs}
            counterevidence = set(path.counterevidence_refs)
            overlap = sorted(supporting & counterevidence)
            if overlap:
                raise ValueError(
                    f"evidence cannot be both supporting and counterevidence: {', '.join(overlap)}"
                )
            referenced = supporting | counterevidence
            unknown = sorted(referenced - known_evidence)
            if unknown:
                raise ValueError(f"unknown evidence reference: {', '.join(unknown)}")


def _validate_revision_lineage(evidence: tuple[EvidenceItem, ...]) -> None:
    by_id = {item.evidence_id: item for item in evidence}
    successors: dict[str, int] = {}
    for item in evidence:
        target_id = item.supersedes_id
        if target_id is None:
            continue
        target = by_id.get(target_id)
        if target is None:
            raise ValueError(
                f"revision {item.evidence_id} supersedes unknown evidence: {target_id}"
            )
        if item.claim_id != target.claim_id:
            raise ValueError(f"revision {item.evidence_id} must retain claim_id from {target_id}")
        if item.visible_at <= target.visible_at:
            raise ValueError(
                f"revision {item.evidence_id} must be visible after superseded evidence {target_id}"
            )
        successors[target_id] = successors.get(target_id, 0) + 1
        if successors[target_id] > 1:
            raise ValueError(f"evidence {target_id} must have at most one direct revision")


def _require_nonempty(value: str, field_name: str) -> None:
    if not value:
        raise ValueError(f"{field_name} must not be empty")


def _require_unique(values: tuple[str, ...], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} values must be unique")
    if any(not value for value in values):
        raise ValueError(f"{field_name} values must not be empty")


def _require_unique_nonempty(values: tuple[str, ...], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must not be empty")
    _require_unique(values, field_name)
