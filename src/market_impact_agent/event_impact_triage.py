from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataPITLane, DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability
from market_impact_agent.prospective_checkpoint_readiness import (
    CheckpointReadinessStatus,
    ProspectiveCheckpointAdmissionStore,
    ProspectiveCheckpointReadinessReport,
)
from market_impact_agent.prospective_data import prospective_observation_version_id
from market_impact_agent.research import EventArchetype, EventStage, TransmissionChannel

EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA = "market-impact.event-impact-triage-candidate-set.v1"
EVENT_IMPACT_TRIAGE_PROPOSAL_SCHEMA = "market-impact.event-impact-triage-proposal.v1"
EVENT_IMPACT_TRIAGE_DECISION_SCHEMA = "market-impact.event-impact-triage-decision.v1"


class CheckpointEligibility(StrEnum):
    ELIGIBLE = "eligible"
    INELIGIBLE = "ineligible"
    NEEDS_REVIEW = "needs_review"


class TriageRoute(StrEnum):
    CHECKPOINT_CANDIDATE = "checkpoint_candidate"
    EVENT_ASSESSMENT = "event_assessment"
    ATTENTION_WATCH = "attention_watch"
    ARCHIVE = "archive"


class TriageDecisionStatus(StrEnum):
    ELIGIBLE_SELECTED = "eligible_selected"
    NEEDS_REVIEW = "needs_review"
    NO_ELIGIBLE_CANDIDATE = "no_eligible_candidate"


class TriageAgentRole(StrEnum):
    COORDINATOR = "coordinator"
    FACT_VERIFIER = "fact_verifier"
    TRANSMISSION_MAPPER = "transmission_mapper"
    PORTFOLIO_IMPACT = "portfolio_impact"
    HISTORICAL_ANALOGY = "historical_analogy"
    COUNTERCASE_REVIEWER = "countercase_reviewer"


@dataclass(frozen=True, slots=True)
class TriageObservationRef:
    version_id: str
    observation_id: str
    first_available_at: datetime
    authority_at: datetime
    provider_id: str
    provider_version: str
    upstream_source: str
    source_ref: str
    raw_content_hash: str
    normalized_payload_hash: str

    def __post_init__(self) -> None:
        _prefixed_hash(
            self.version_id,
            "prospective-observation-version-",
            "triage observation version_id",
        )
        _prefixed_hash(
            self.observation_id,
            "source-observation-",
            "triage observation observation_id",
        )
        _strict_utc(self.first_available_at, "triage observation first_available_at")
        _strict_utc(self.authority_at, "triage observation authority_at")
        if self.first_available_at != self.authority_at:
            raise ValueError("triage observations require actual-receipt availability authority")
        for name in ("provider_id", "provider_version", "upstream_source", "source_ref"):
            _trimmed(cast(str, getattr(self, name)), f"triage observation {name}")
        _sha256(self.raw_content_hash, "triage observation raw_content_hash")
        _sha256(self.normalized_payload_hash, "triage observation normalized_payload_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "version_id": self.version_id,
            "observation_id": self.observation_id,
            "first_available_at": _timestamp(self.first_available_at),
            "authority_at": _timestamp(self.authority_at),
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "upstream_source": self.upstream_source,
            "source_ref": self.source_ref,
            "raw_content_hash": self.raw_content_hash,
            "normalized_payload_hash": self.normalized_payload_hash,
        }


@dataclass(frozen=True, slots=True)
class EventImpactTriageCandidateSet:
    candidate_set_id: str
    registration_id: str
    checkpoint_key: str
    route_plan_id: str
    route_admission_id: str
    readiness_report_id: str
    data_snapshot_id: str
    admitted_at: datetime
    frozen_at: datetime
    observations: tuple[TriageObservationRef, ...]
    historical_pit_claim: bool = False
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA:
            raise ValueError("unsupported Event Impact Triage Candidate Set schema")
        _prefixed_hash(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "triage candidate registration_id",
        )
        _trimmed(self.checkpoint_key, "triage candidate checkpoint_key")
        _prefixed_hash(
            self.route_plan_id,
            "prospective-checkpoint-route-plan-",
            "triage candidate route_plan_id",
        )
        _prefixed_hash(
            self.route_admission_id,
            "prospective-checkpoint-route-admission-",
            "triage candidate route_admission_id",
        )
        _prefixed_hash(
            self.readiness_report_id,
            "prospective-checkpoint-readiness-report-",
            "triage candidate readiness_report_id",
        )
        _prefixed_hash(
            self.data_snapshot_id,
            "data-snapshot-",
            "triage candidate data_snapshot_id",
        )
        _strict_utc(self.admitted_at, "triage candidate admitted_at")
        _strict_utc(self.frozen_at, "triage candidate frozen_at")
        if self.frozen_at < self.admitted_at:
            raise ValueError("triage candidate cannot be frozen before route admission")
        if not self.observations:
            raise ValueError("triage candidate set requires at least one observation")
        order = tuple((item.first_available_at, item.version_id) for item in self.observations)
        if order != tuple(sorted(order)):
            raise ValueError("triage candidate observations must use stable receipt order")
        if len({item.version_id for item in self.observations}) != len(self.observations):
            raise ValueError("triage candidate observation versions must be unique")
        if len({item.observation_id for item in self.observations}) != len(self.observations):
            raise ValueError("triage candidate source observations must be unique")
        if any(
            item.first_available_at < self.admitted_at or item.first_available_at > self.frozen_at
            for item in self.observations
        ):
            raise ValueError("triage candidate observations must be post-admission and frozen")
        if (
            self.historical_pit_claim
            or self.judgment_model_calls_authorized
            or self.execution_capability
        ):
            raise ValueError(
                "triage candidate set cannot grant PIT, Judgment, or execution authority"
            )
        if self.candidate_set_id != self.expected_candidate_set_id:
            raise ValueError("triage candidate_set_id does not match content")

    @property
    def expected_candidate_set_id(self) -> str:
        return f"event-impact-triage-candidate-set-{canonical_hash(self.core_dict())}"

    @property
    def version_ids(self) -> tuple[str, ...]:
        return tuple(item.version_id for item in self.observations)

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "route_plan_id": self.route_plan_id,
            "route_admission_id": self.route_admission_id,
            "readiness_report_id": self.readiness_report_id,
            "data_snapshot_id": self.data_snapshot_id,
            "admitted_at": _timestamp(self.admitted_at),
            "frozen_at": _timestamp(self.frozen_at),
            "observations": [item.to_dict() for item in self.observations],
            "historical_pit_claim": self.historical_pit_claim,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "candidate_set_id": self.candidate_set_id}


def event_impact_triage_candidate_set_from_dict(
    value: object,
) -> EventImpactTriageCandidateSet:
    payload = _object(value, "Event Impact Triage Candidate Set")
    expected = {
        "schema_version",
        "candidate_set_id",
        "registration_id",
        "checkpoint_key",
        "route_plan_id",
        "route_admission_id",
        "readiness_report_id",
        "data_snapshot_id",
        "admitted_at",
        "frozen_at",
        "observations",
        "historical_pit_claim",
        "judgment_model_calls_authorized",
        "execution_capability",
    }
    if set(payload) != expected:
        raise ValueError("Event Impact Triage Candidate Set fields are invalid")
    observation_fields = {
        "version_id",
        "observation_id",
        "first_available_at",
        "authority_at",
        "provider_id",
        "provider_version",
        "upstream_source",
        "source_ref",
        "raw_content_hash",
        "normalized_payload_hash",
    }
    observations: list[TriageObservationRef] = []
    for raw in _array(payload.get("observations"), "triage candidate observations"):
        item = _object(raw, "triage candidate observation")
        if set(item) != observation_fields:
            raise ValueError("Event Impact Triage observation fields are invalid")
        observations.append(
            TriageObservationRef(
                version_id=_string(item, "version_id"),
                observation_id=_string(item, "observation_id"),
                first_available_at=_datetime(_string(item, "first_available_at")),
                authority_at=_datetime(_string(item, "authority_at")),
                provider_id=_string(item, "provider_id"),
                provider_version=_string(item, "provider_version"),
                upstream_source=_string(item, "upstream_source"),
                source_ref=_string(item, "source_ref"),
                raw_content_hash=_string(item, "raw_content_hash"),
                normalized_payload_hash=_string(item, "normalized_payload_hash"),
            )
        )
    result = EventImpactTriageCandidateSet(
        candidate_set_id=_string(payload, "candidate_set_id"),
        registration_id=_string(payload, "registration_id"),
        checkpoint_key=_string(payload, "checkpoint_key"),
        route_plan_id=_string(payload, "route_plan_id"),
        route_admission_id=_string(payload, "route_admission_id"),
        readiness_report_id=_string(payload, "readiness_report_id"),
        data_snapshot_id=_string(payload, "data_snapshot_id"),
        admitted_at=_datetime(_string(payload, "admitted_at")),
        frozen_at=_datetime(_string(payload, "frozen_at")),
        observations=tuple(observations),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        judgment_model_calls_authorized=_boolean(payload, "judgment_model_calls_authorized"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("Event Impact Triage Candidate Set is not canonical")
    return result


@dataclass(frozen=True, slots=True)
class TriageClusterProposal:
    cluster_id: str
    candidate_version_ids: tuple[str, ...]
    checkpoint_eligibility: CheckpointEligibility
    recommended_route: TriageRoute
    event_archetypes: tuple[EventArchetype, ...]
    event_stage: EventStage
    changed_facts: tuple[str, ...]
    rule_reasons: tuple[str, ...]
    evidence_version_ids: tuple[str, ...]
    uncertainty_notes: tuple[str, ...]
    countercases: tuple[str, ...]
    transmission_channels: tuple[TransmissionChannel, ...]
    affected_entity_refs: tuple[str, ...]
    watch_questions: tuple[str, ...]
    triage_confidence: float
    zero_financial_impact_claim: bool = False
    signal_or_execution_capability: bool = False

    def __post_init__(self) -> None:
        _sorted_unique(self.candidate_version_ids, "triage cluster candidate_version_ids")
        if not self.candidate_version_ids:
            raise ValueError("triage clusters require candidate versions")
        for version_id in self.candidate_version_ids:
            _prefixed_hash(
                version_id,
                "prospective-observation-version-",
                "triage cluster candidate version",
            )
        for name in (
            "changed_facts",
            "rule_reasons",
            "evidence_version_ids",
            "uncertainty_notes",
            "countercases",
            "affected_entity_refs",
            "watch_questions",
        ):
            _sorted_unique(cast(tuple[str, ...], getattr(self, name)), f"triage cluster {name}")
        if not self.evidence_version_ids:
            raise ValueError("triage clusters require cited candidate evidence")
        for version_id in self.evidence_version_ids:
            _prefixed_hash(
                version_id,
                "prospective-observation-version-",
                "triage cluster evidence version",
            )
        if self.event_archetypes != tuple(
            sorted(set(self.event_archetypes), key=lambda item: item.value)
        ):
            raise ValueError("triage cluster event_archetypes must be sorted and unique")
        if self.transmission_channels != tuple(
            sorted(set(self.transmission_channels), key=lambda item: item.value)
        ):
            raise ValueError("triage cluster transmission_channels must be sorted and unique")
        if not self.rule_reasons:
            raise ValueError("triage clusters require a checkpoint-rule reason")
        if not 0 <= self.triage_confidence <= 1:
            raise ValueError("triage confidence must be between zero and one")
        if self.zero_financial_impact_claim or self.signal_or_execution_capability:
            raise ValueError("triage proposals cannot rule out impact or grant trading authority")
        if self.checkpoint_eligibility is CheckpointEligibility.ELIGIBLE:
            if self.recommended_route is not TriageRoute.CHECKPOINT_CANDIDATE:
                raise ValueError("eligible triage clusters must route to checkpoint selection")
            if not self.changed_facts or not self.event_archetypes:
                raise ValueError("eligible triage clusters require changed facts and archetypes")
        elif self.checkpoint_eligibility is CheckpointEligibility.INELIGIBLE:
            if self.recommended_route is TriageRoute.CHECKPOINT_CANDIDATE:
                raise ValueError("ineligible triage clusters cannot route to checkpoint selection")
        else:
            if self.recommended_route not in {
                TriageRoute.EVENT_ASSESSMENT,
                TriageRoute.ATTENTION_WATCH,
            }:
                raise ValueError("needs_review triage clusters require assessment or Watch")
            if not self.uncertainty_notes:
                raise ValueError("needs_review triage clusters require uncertainty notes")
        if self.recommended_route is TriageRoute.EVENT_ASSESSMENT and (
            not self.changed_facts or not self.event_archetypes or not self.transmission_channels
        ):
            raise ValueError(
                "EventAssessment routing requires facts, archetypes, and a transmission channel"
            )
        if self.recommended_route is TriageRoute.ATTENTION_WATCH and (
            not self.changed_facts or not self.watch_questions
        ):
            raise ValueError("Attention Watch routing requires changed facts and watch questions")
        if self.cluster_id != self.expected_cluster_id:
            raise ValueError("triage cluster_id does not match content")

    @property
    def expected_cluster_id(self) -> str:
        return f"event-impact-triage-cluster-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "candidate_version_ids": list(self.candidate_version_ids),
            "checkpoint_eligibility": self.checkpoint_eligibility.value,
            "recommended_route": self.recommended_route.value,
            "event_archetypes": [item.value for item in self.event_archetypes],
            "event_stage": self.event_stage.value,
            "changed_facts": list(self.changed_facts),
            "rule_reasons": list(self.rule_reasons),
            "evidence_version_ids": list(self.evidence_version_ids),
            "uncertainty_notes": list(self.uncertainty_notes),
            "countercases": list(self.countercases),
            "transmission_channels": [item.value for item in self.transmission_channels],
            "affected_entity_refs": list(self.affected_entity_refs),
            "watch_questions": list(self.watch_questions),
            "triage_confidence": self.triage_confidence,
            "zero_financial_impact_claim": self.zero_financial_impact_claim,
            "signal_or_execution_capability": self.signal_or_execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "cluster_id": self.cluster_id}

    @classmethod
    def build(
        cls,
        *,
        candidate_version_ids: tuple[str, ...],
        checkpoint_eligibility: CheckpointEligibility,
        recommended_route: TriageRoute,
        event_archetypes: tuple[EventArchetype, ...],
        event_stage: EventStage,
        changed_facts: tuple[str, ...],
        rule_reasons: tuple[str, ...],
        evidence_version_ids: tuple[str, ...],
        uncertainty_notes: tuple[str, ...] = (),
        countercases: tuple[str, ...] = (),
        transmission_channels: tuple[TransmissionChannel, ...] = (),
        affected_entity_refs: tuple[str, ...] = (),
        watch_questions: tuple[str, ...] = (),
        triage_confidence: float,
    ) -> TriageClusterProposal:
        ordered_versions = tuple(sorted(candidate_version_ids))
        ordered_archetypes = tuple(sorted(set(event_archetypes), key=lambda item: item.value))
        ordered_channels = tuple(sorted(set(transmission_channels), key=lambda item: item.value))
        ordered_facts = tuple(sorted(set(changed_facts)))
        ordered_reasons = tuple(sorted(set(rule_reasons)))
        ordered_evidence = tuple(sorted(set(evidence_version_ids)))
        ordered_uncertainty = tuple(sorted(set(uncertainty_notes)))
        ordered_countercases = tuple(sorted(set(countercases)))
        ordered_entities = tuple(sorted(set(affected_entity_refs)))
        ordered_questions = tuple(sorted(set(watch_questions)))
        core = {
            "candidate_version_ids": list(ordered_versions),
            "checkpoint_eligibility": checkpoint_eligibility.value,
            "recommended_route": recommended_route.value,
            "event_archetypes": [item.value for item in ordered_archetypes],
            "event_stage": event_stage.value,
            "changed_facts": list(ordered_facts),
            "rule_reasons": list(ordered_reasons),
            "evidence_version_ids": list(ordered_evidence),
            "uncertainty_notes": list(ordered_uncertainty),
            "countercases": list(ordered_countercases),
            "transmission_channels": [item.value for item in ordered_channels],
            "affected_entity_refs": list(ordered_entities),
            "watch_questions": list(ordered_questions),
            "triage_confidence": triage_confidence,
            "zero_financial_impact_claim": False,
            "signal_or_execution_capability": False,
        }
        return cls(
            cluster_id=f"event-impact-triage-cluster-{canonical_hash(core)}",
            candidate_version_ids=ordered_versions,
            checkpoint_eligibility=checkpoint_eligibility,
            recommended_route=recommended_route,
            event_archetypes=ordered_archetypes,
            event_stage=event_stage,
            changed_facts=ordered_facts,
            rule_reasons=ordered_reasons,
            evidence_version_ids=ordered_evidence,
            uncertainty_notes=ordered_uncertainty,
            countercases=ordered_countercases,
            transmission_channels=ordered_channels,
            affected_entity_refs=ordered_entities,
            watch_questions=ordered_questions,
            triage_confidence=triage_confidence,
        )


@dataclass(frozen=True, slots=True)
class EventImpactTriageProposal:
    proposal_id: str
    candidate_set_id: str
    clusters: tuple[TriageClusterProposal, ...]
    schema_version: str = EVENT_IMPACT_TRIAGE_PROPOSAL_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_IMPACT_TRIAGE_PROPOSAL_SCHEMA:
            raise ValueError("unsupported Event Impact Triage Proposal schema")
        _prefixed_hash(
            self.candidate_set_id,
            "event-impact-triage-candidate-set-",
            "triage proposal candidate_set_id",
        )
        if not self.clusters:
            raise ValueError("triage proposal requires at least one cluster")
        cluster_ids = tuple(item.cluster_id for item in self.clusters)
        if cluster_ids != tuple(sorted(set(cluster_ids))):
            raise ValueError("triage proposal clusters must be sorted and unique")
        if self.proposal_id != self.expected_proposal_id:
            raise ValueError("triage proposal_id does not match content")

    @property
    def expected_proposal_id(self) -> str:
        return f"event-impact-triage-proposal-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "clusters": [item.to_dict() for item in self.clusters],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "proposal_id": self.proposal_id}

    @classmethod
    def build(
        cls,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        clusters: tuple[TriageClusterProposal, ...],
    ) -> EventImpactTriageProposal:
        ordered = tuple(sorted(clusters, key=lambda item: item.cluster_id))
        core = {
            "schema_version": EVENT_IMPACT_TRIAGE_PROPOSAL_SCHEMA,
            "candidate_set_id": candidate_set.candidate_set_id,
            "clusters": [item.to_dict() for item in ordered],
        }
        proposal = cls(
            proposal_id=f"event-impact-triage-proposal-{canonical_hash(core)}",
            candidate_set_id=candidate_set.candidate_set_id,
            clusters=ordered,
        )
        proposal.validate_against(candidate_set)
        return proposal

    def validate_against(self, candidate_set: EventImpactTriageCandidateSet) -> None:
        if self.candidate_set_id != candidate_set.candidate_set_id:
            raise ValueError("triage proposal belongs to another candidate set")
        expected = set(candidate_set.version_ids)
        assigned: list[str] = []
        for cluster in self.clusters:
            assigned.extend(cluster.candidate_version_ids)
            unknown_evidence = set(cluster.evidence_version_ids) - expected
            if unknown_evidence:
                raise ValueError("triage proposal cites evidence outside the frozen candidate set")
            if not set(cluster.evidence_version_ids) <= set(cluster.candidate_version_ids):
                raise ValueError("triage cluster evidence must belong to the same event cluster")
        if len(assigned) != len(set(assigned)):
            raise ValueError("triage proposal assigns a candidate version more than once")
        if set(assigned) != expected:
            raise ValueError("triage proposal must classify every frozen candidate version")


def event_impact_triage_proposal_from_dict(value: object) -> EventImpactTriageProposal:
    """Parse the coordinator's closed proposal contract without accepting extra fields."""

    payload = _object(value, "Event Impact Triage Proposal")
    if set(payload) != {"schema_version", "proposal_id", "candidate_set_id", "clusters"}:
        raise ValueError("Event Impact Triage Proposal fields are invalid")
    raw_clusters = _array(payload.get("clusters"), "Event Impact Triage Proposal clusters")
    clusters: list[TriageClusterProposal] = []
    expected_cluster_fields = {
        "cluster_id",
        "candidate_version_ids",
        "checkpoint_eligibility",
        "recommended_route",
        "event_archetypes",
        "event_stage",
        "changed_facts",
        "rule_reasons",
        "evidence_version_ids",
        "uncertainty_notes",
        "countercases",
        "transmission_channels",
        "affected_entity_refs",
        "watch_questions",
        "triage_confidence",
        "zero_financial_impact_claim",
        "signal_or_execution_capability",
    }
    for raw_cluster in raw_clusters:
        cluster = _object(raw_cluster, "Event Impact Triage cluster")
        if set(cluster) != expected_cluster_fields:
            raise ValueError("Event Impact Triage cluster fields are invalid")
        clusters.append(
            TriageClusterProposal(
                cluster_id=_string(cluster, "cluster_id"),
                candidate_version_ids=_string_tuple(
                    cluster.get("candidate_version_ids"), "candidate_version_ids"
                ),
                checkpoint_eligibility=CheckpointEligibility(
                    _string(cluster, "checkpoint_eligibility")
                ),
                recommended_route=TriageRoute(_string(cluster, "recommended_route")),
                event_archetypes=tuple(
                    EventArchetype(item)
                    for item in _string_tuple(cluster.get("event_archetypes"), "event_archetypes")
                ),
                event_stage=EventStage(_string(cluster, "event_stage")),
                changed_facts=_string_tuple(cluster.get("changed_facts"), "changed_facts"),
                rule_reasons=_string_tuple(cluster.get("rule_reasons"), "rule_reasons"),
                evidence_version_ids=_string_tuple(
                    cluster.get("evidence_version_ids"), "evidence_version_ids"
                ),
                uncertainty_notes=_string_tuple(
                    cluster.get("uncertainty_notes"), "uncertainty_notes"
                ),
                countercases=_string_tuple(cluster.get("countercases"), "countercases"),
                transmission_channels=tuple(
                    TransmissionChannel(item)
                    for item in _string_tuple(
                        cluster.get("transmission_channels"), "transmission_channels"
                    )
                ),
                affected_entity_refs=_string_tuple(
                    cluster.get("affected_entity_refs"), "affected_entity_refs"
                ),
                watch_questions=_string_tuple(cluster.get("watch_questions"), "watch_questions"),
                triage_confidence=_number(cluster, "triage_confidence"),
                zero_financial_impact_claim=_boolean(cluster, "zero_financial_impact_claim"),
                signal_or_execution_capability=_boolean(cluster, "signal_or_execution_capability"),
            )
        )
    result = EventImpactTriageProposal(
        proposal_id=_string(payload, "proposal_id"),
        candidate_set_id=_string(payload, "candidate_set_id"),
        clusters=tuple(clusters),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("Event Impact Triage Proposal is not canonical")
    return result


@dataclass(frozen=True, slots=True)
class TriageRunMemberEvidence:
    role: TriageAgentRole
    run_id: str
    terminal_artifact_hash: str
    metrics_hash: str
    validation_event_hash: str
    execution_binding_hash: str

    def __post_init__(self) -> None:
        _trimmed(self.run_id, "triage run_id")
        for name in (
            "terminal_artifact_hash",
            "metrics_hash",
            "validation_event_hash",
            "execution_binding_hash",
        ):
            _sha256(cast(str, getattr(self, name)), f"triage run {name}")

    def to_dict(self) -> dict[str, str]:
        return {
            "role": self.role.value,
            "run_id": self.run_id,
            "terminal_artifact_hash": self.terminal_artifact_hash,
            "metrics_hash": self.metrics_hash,
            "validation_event_hash": self.validation_event_hash,
            "execution_binding_hash": self.execution_binding_hash,
        }


@dataclass(frozen=True, slots=True)
class TriageRunEvidence:
    members: tuple[TriageRunMemberEvidence, ...]
    usage_ledger_hash: str

    def __post_init__(self) -> None:
        if not self.members:
            raise ValueError("triage run evidence requires at least one Agent run")
        roles = tuple(item.role.value for item in self.members)
        if roles != tuple(sorted(set(roles))):
            raise ValueError("triage run members must be sorted and use unique roles")
        if roles.count(TriageAgentRole.COORDINATOR.value) != 1:
            raise ValueError("triage run evidence requires exactly one coordinator")
        if len({item.run_id for item in self.members}) != len(self.members):
            raise ValueError("triage run evidence cannot reuse a run_id")
        _sha256(self.usage_ledger_hash, "triage run usage_ledger_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "members": [item.to_dict() for item in self.members],
            "usage_ledger_hash": self.usage_ledger_hash,
        }


class CompletedTriageRunAuthority(Protocol):
    """Trusted Harness boundary that reopens one sealed triage Agent run."""

    def assert_authoritative_completed_triage_run(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageRunEvidence,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class EventImpactTriageDecision:
    decision_id: str
    candidate_set_id: str
    proposal_id: str
    run_evidence: TriageRunEvidence
    status: TriageDecisionStatus
    selected_cluster_id: str | None
    blocking_review_cluster_ids: tuple[str, ...]
    unselected_eligible_cluster_ids: tuple[str, ...]
    event_assessment_cluster_ids: tuple[str, ...]
    attention_watch_cluster_ids: tuple[str, ...]
    archive_cluster_ids: tuple[str, ...]
    decided_at: datetime
    historical_pit_claim: bool = False
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = EVENT_IMPACT_TRIAGE_DECISION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_IMPACT_TRIAGE_DECISION_SCHEMA:
            raise ValueError("unsupported Event Impact Triage Decision schema")
        _prefixed_hash(
            self.candidate_set_id,
            "event-impact-triage-candidate-set-",
            "triage decision candidate_set_id",
        )
        _prefixed_hash(
            self.proposal_id,
            "event-impact-triage-proposal-",
            "triage decision proposal_id",
        )
        for name in (
            "blocking_review_cluster_ids",
            "unselected_eligible_cluster_ids",
            "event_assessment_cluster_ids",
            "attention_watch_cluster_ids",
            "archive_cluster_ids",
        ):
            values = cast(tuple[str, ...], getattr(self, name))
            _sorted_unique(values, f"triage decision {name}")
            for cluster_id in values:
                _prefixed_hash(
                    cluster_id,
                    "event-impact-triage-cluster-",
                    f"triage decision {name}",
                )
        routed = (
            self.event_assessment_cluster_ids
            + self.attention_watch_cluster_ids
            + self.archive_cluster_ids
        )
        if len(routed) != len(set(routed)):
            raise ValueError("triage decision impact routes must not overlap")
        if set(self.unselected_eligible_cluster_ids) & set(routed):
            raise ValueError("unselected eligible clusters cannot receive an impact route")
        if self.status is TriageDecisionStatus.ELIGIBLE_SELECTED:
            if self.selected_cluster_id is None:
                raise ValueError("eligible triage decision requires a selected cluster")
            _prefixed_hash(
                self.selected_cluster_id,
                "event-impact-triage-cluster-",
                "triage decision selected_cluster_id",
            )
            if self.blocking_review_cluster_ids:
                raise ValueError("eligible selection cannot retain an earlier review blocker")
        elif self.selected_cluster_id is not None:
            raise ValueError("non-selected triage decisions cannot carry a selected cluster")
        if self.status is TriageDecisionStatus.NEEDS_REVIEW and not (
            self.blocking_review_cluster_ids
        ):
            raise ValueError("needs_review triage decision requires a blocking review cluster")
        _strict_utc(self.decided_at, "triage decision decided_at")
        if (
            self.historical_pit_claim
            or self.judgment_model_calls_authorized
            or self.execution_capability
        ):
            raise ValueError("triage decision cannot grant PIT, Judgment, or execution authority")
        if self.decision_id != self.expected_decision_id:
            raise ValueError("triage decision_id does not match content")

    @property
    def expected_decision_id(self) -> str:
        return f"event-impact-triage-decision-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "proposal_id": self.proposal_id,
            "run_evidence": self.run_evidence.to_dict(),
            "status": self.status.value,
            "selected_cluster_id": self.selected_cluster_id,
            "blocking_review_cluster_ids": list(self.blocking_review_cluster_ids),
            "unselected_eligible_cluster_ids": list(self.unselected_eligible_cluster_ids),
            "event_assessment_cluster_ids": list(self.event_assessment_cluster_ids),
            "attention_watch_cluster_ids": list(self.attention_watch_cluster_ids),
            "archive_cluster_ids": list(self.archive_cluster_ids),
            "decided_at": _timestamp(self.decided_at),
            "historical_pit_claim": self.historical_pit_claim,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}


def freeze_event_impact_triage_candidate_set(
    *,
    readiness_report: ProspectiveCheckpointReadinessReport,
    checkpoint_key: str,
    snapshot: DataSnapshot,
    snapshot_store: LocalDataSnapshotStore,
    admission_store: ProspectiveCheckpointAdmissionStore,
    frozen_at: datetime,
) -> EventImpactTriageCandidateSet:
    """Freeze all readiness candidates for one checkpoint without semantic classification."""

    _strict_utc(frozen_at, "triage freeze frozen_at")
    if frozen_at < readiness_report.evaluated_at:
        raise ValueError("triage freeze cannot predate its readiness report")
    admission_store.assert_effective(
        route_plan_id=readiness_report.route_plan_id,
        admission_id=readiness_report.route_admission_id,
        registration_id=readiness_report.registration_id,
        at=frozen_at,
    )
    checkpoint = next(
        (item for item in readiness_report.checkpoints if item.checkpoint_key == checkpoint_key),
        None,
    )
    if checkpoint is None:
        raise KeyError(f"checkpoint is outside the readiness report: {checkpoint_key}")
    if checkpoint.status is not CheckpointReadinessStatus.UNCLASSIFIED_TRIGGER_CANDIDATE_OBSERVED:
        raise ValueError("triage freeze requires unclassified trigger candidates")
    stored_snapshot = snapshot_store.get(snapshot.snapshot_id)
    if stored_snapshot != snapshot:
        raise ValueError("triage freeze requires the authoritative persisted Data Snapshot")
    if (
        not snapshot.coverage_complete
        or snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE
        or snapshot.query.capability is not ObservationCapability.EVENT_REVELATION
    ):
        raise ValueError("triage freeze requires a complete prospective event Snapshot")
    if snapshot.completed_at > frozen_at:
        raise ValueError("triage freeze cannot predate the Data Snapshot")

    by_version = {prospective_observation_version_id(item): item for item in snapshot.observations}
    if len(by_version) != len(snapshot.observations):
        raise ValueError("triage Snapshot cannot repeat one content version across receipts")
    expected_versions = set(checkpoint.trigger_candidate_version_ids)
    if set(by_version) != expected_versions:
        raise ValueError(
            "triage Snapshot must contain every and only the readiness candidate version"
        )
    refs: list[TriageObservationRef] = []
    for version_id, observation in by_version.items():
        available_at = observation.times.available_at
        if (
            observation.times.availability_basis is not AvailabilityBasis.ACTUAL_RECEIPT
            or available_at is None
            or available_at != observation.times.retrieved_at
            or observation.authority_at != observation.times.retrieved_at
            or observation.authority_kind != "actual_receipt"
        ):
            raise ValueError("triage candidates require prospective actual-receipt observations")
        refs.append(
            TriageObservationRef(
                version_id=version_id,
                observation_id=observation.observation_id,
                first_available_at=available_at,
                authority_at=cast(datetime, observation.authority_at),
                provider_id=observation.provider_id,
                provider_version=observation.provider_version,
                upstream_source=observation.upstream_source,
                source_ref=observation.source_ref,
                raw_content_hash=observation.raw_content_hash,
                normalized_payload_hash=canonical_hash(observation.normalized_payload),
            )
        )
    ordered = tuple(sorted(refs, key=lambda item: (item.first_available_at, item.version_id)))
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA,
        "registration_id": readiness_report.registration_id,
        "checkpoint_key": checkpoint_key,
        "route_plan_id": readiness_report.route_plan_id,
        "route_admission_id": readiness_report.route_admission_id,
        "readiness_report_id": readiness_report.report_id,
        "data_snapshot_id": snapshot.snapshot_id,
        "admitted_at": _timestamp(readiness_report.admitted_at),
        "frozen_at": _timestamp(frozen_at),
        "observations": [item.to_dict() for item in ordered],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return EventImpactTriageCandidateSet(
        candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
        registration_id=readiness_report.registration_id,
        checkpoint_key=checkpoint_key,
        route_plan_id=readiness_report.route_plan_id,
        route_admission_id=readiness_report.route_admission_id,
        readiness_report_id=readiness_report.report_id,
        data_snapshot_id=snapshot.snapshot_id,
        admitted_at=readiness_report.admitted_at,
        frozen_at=frozen_at,
        observations=ordered,
    )


def admit_event_impact_triage(
    *,
    candidate_set: EventImpactTriageCandidateSet,
    proposal: EventImpactTriageProposal,
    run_evidence: TriageRunEvidence,
    run_authority: CompletedTriageRunAuthority,
    decided_at: datetime,
) -> EventImpactTriageDecision:
    """Deterministically admit a sealed triage proposal without granting trading authority."""

    proposal.validate_against(candidate_set)
    _strict_utc(decided_at, "triage decision decided_at")
    if decided_at < candidate_set.frozen_at:
        raise ValueError("triage decision cannot predate its frozen candidate set")
    run_authority.assert_authoritative_completed_triage_run(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=run_evidence,
    )

    availability = {item.version_id: item.first_available_at for item in candidate_set.observations}
    ready_at = {
        item.cluster_id: max(
            availability[version_id]
            for version_id in (*item.candidate_version_ids, *item.evidence_version_ids)
        )
        for item in proposal.clusters
    }
    ordered = tuple(
        sorted(
            proposal.clusters,
            key=lambda item: (ready_at[item.cluster_id], item.cluster_id),
        )
    )
    eligible = tuple(
        sorted(
            (
                item
                for item in ordered
                if item.checkpoint_eligibility is CheckpointEligibility.ELIGIBLE
            ),
            key=lambda item: (ready_at[item.cluster_id], item.cluster_id),
        )
    )
    needs_review = tuple(
        item
        for item in ordered
        if item.checkpoint_eligibility is CheckpointEligibility.NEEDS_REVIEW
    )
    first_eligible = eligible[0] if eligible else None
    if first_eligible is None:
        blockers = needs_review
    else:
        eligible_at = ready_at[first_eligible.cluster_id]
        blockers = tuple(item for item in needs_review if ready_at[item.cluster_id] <= eligible_at)
    if blockers:
        status = TriageDecisionStatus.NEEDS_REVIEW
        selected: TriageClusterProposal | None = None
    elif first_eligible is not None:
        status = TriageDecisionStatus.ELIGIBLE_SELECTED
        selected = first_eligible
    else:
        status = TriageDecisionStatus.NO_ELIGIBLE_CANDIDATE
        selected = None

    selected_id = None if selected is None else selected.cluster_id
    unselected_eligible = tuple(
        sorted(item.cluster_id for item in eligible if item.cluster_id != selected_id)
    )
    blocked_ids = tuple(sorted(item.cluster_id for item in blockers))
    event_assessment = tuple(
        sorted(
            item.cluster_id
            for item in ordered
            if item.recommended_route is TriageRoute.EVENT_ASSESSMENT
        )
    )
    attention_watch = tuple(
        sorted(
            item.cluster_id
            for item in ordered
            if item.recommended_route is TriageRoute.ATTENTION_WATCH
        )
    )
    archive = tuple(
        sorted(item.cluster_id for item in ordered if item.recommended_route is TriageRoute.ARCHIVE)
    )
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_DECISION_SCHEMA,
        "candidate_set_id": candidate_set.candidate_set_id,
        "proposal_id": proposal.proposal_id,
        "run_evidence": run_evidence.to_dict(),
        "status": status.value,
        "selected_cluster_id": selected_id,
        "blocking_review_cluster_ids": list(blocked_ids),
        "unselected_eligible_cluster_ids": list(unselected_eligible),
        "event_assessment_cluster_ids": list(event_assessment),
        "attention_watch_cluster_ids": list(attention_watch),
        "archive_cluster_ids": list(archive),
        "decided_at": _timestamp(decided_at),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return EventImpactTriageDecision(
        decision_id=f"event-impact-triage-decision-{canonical_hash(core)}",
        candidate_set_id=candidate_set.candidate_set_id,
        proposal_id=proposal.proposal_id,
        run_evidence=run_evidence,
        status=status,
        selected_cluster_id=selected_id,
        blocking_review_cluster_ids=blocked_ids,
        unselected_eligible_cluster_ids=unselected_eligible,
        event_assessment_cluster_ids=event_assessment,
        attention_watch_cluster_ids=attention_watch,
        archive_cluster_ids=archive,
        decided_at=decided_at,
    )


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("triage timestamp must use ISO 8601") from exc
    _strict_utc(parsed, "triage timestamp")
    return parsed


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")
    for value in values:
        _trimmed(value, name)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object with string keys")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError(f"{name} must be an object with string keys")
    return cast(dict[str, object], mapping)


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    items = _array(value, name)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must contain strings")
    return tuple(cast(list[str], items))


def _number(payload: dict[str, object], name: str) -> float:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be numeric")
    return float(value)


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value
