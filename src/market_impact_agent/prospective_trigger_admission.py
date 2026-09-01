from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.event_impact_triage import (
    CheckpointEligibility,
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    TriageClusterProposal,
    TriageDecisionStatus,
    TriageRoute,
)
from market_impact_agent.prospective_diagnostic import (
    DiagnosticMechanism,
    ProspectiveDiagnosticRegistration,
)
from market_impact_agent.research import TransmissionChannel

PROSPECTIVE_POSITION_SNAPSHOT_SCHEMA = "market-impact.prospective-position-snapshot.v1"
PROSPECTIVE_HISTORICAL_ANALOGY_PACK_SCHEMA = "market-impact.prospective-historical-analogy-pack.v1"
PROSPECTIVE_EVENT_ASSESSMENT_SCHEMA = "market-impact.prospective-event-assessment.v1"
PROSPECTIVE_MATERIALITY_GATE_SCHEMA = "market-impact.prospective-materiality-gate-result.v1"
PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA = "market-impact.prospective-trigger-admission.v1"


class HistoricalAnalogyMode(StrEnum):
    STRICT_PIT = "strict_pit"
    MODELED_PIT = "modeled_pit"
    OUTCOME_OPENED_REVIEW = "outcome_opened_review"


class MaterialityDisposition(StrEnum):
    ADMIT = "admit"
    WATCH = "watch"
    ARCHIVE = "archive"


class TriggerAdmissionKind(StrEnum):
    CHECKPOINT_ELIGIBLE = "checkpoint_eligible"
    MATERIAL_EVENT = "material_event"


class CompletedTriageDecisionAuthority(Protocol):
    def get_context(
        self,
        candidate_set_id: str,
    ) -> tuple[
        EventImpactTriageCandidateSet,
        EventImpactTriageProposal,
        EventImpactTriageDecision,
    ]: ...

    def route_epoch_contexts(
        self,
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
        at: datetime,
    ) -> tuple[
        tuple[
            EventImpactTriageCandidateSet,
            EventImpactTriageProposal,
            EventImpactTriageDecision,
            TriageClusterProposal,
        ],
        ...,
    ]: ...


class CompletedEventAssessmentAuthority(Protocol):
    def assert_authoritative_completed_event_assessment(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        decision: EventImpactTriageDecision,
        assessment: ProspectiveEventAssessmentArtifact,
    ) -> None: ...


class TriggerAdmissionAuthority(Protocol):
    def assert_authoritative(self, admission: ProspectiveTriggerAdmission) -> None: ...


def terminal_wake_resolution_parent_ids(
    resolution_contexts: tuple[
        tuple[
            EventImpactTriageCandidateSet,
            EventImpactTriageProposal,
            EventImpactTriageDecision,
            TriageClusterProposal,
        ],
        ...,
    ],
) -> tuple[str, ...]:
    """Return parents whose exact Wake child reached a terminal non-Watch route."""

    resolved: set[str] = set()
    for candidate_set, _, decision, _ in resolution_contexts:
        parent_cluster_id = candidate_set.parent_cluster_id
        if parent_cluster_id is None:
            continue
        if (
            decision.status is not TriageDecisionStatus.NEEDS_REVIEW
            and not decision.attention_watch_cluster_ids
        ):
            resolved.add(parent_cluster_id)
    return tuple(sorted(resolved))


def unresolved_route_review_cluster_ids(
    *,
    earlier_contexts: tuple[
        tuple[
            EventImpactTriageCandidateSet,
            EventImpactTriageProposal,
            EventImpactTriageDecision,
            TriageClusterProposal,
        ],
        ...,
    ],
    resolution_contexts: tuple[
        tuple[
            EventImpactTriageCandidateSet,
            EventImpactTriageProposal,
            EventImpactTriageDecision,
            TriageClusterProposal,
        ],
        ...,
    ],
) -> tuple[str, ...]:
    """Return earlier review clusters not terminally resolved by an exact Wake child."""

    unresolved = {
        cluster.cluster_id
        for _, _, _, cluster in earlier_contexts
        if cluster.checkpoint_eligibility is CheckpointEligibility.NEEDS_REVIEW
    }
    unresolved.difference_update(terminal_wake_resolution_parent_ids(resolution_contexts))
    return tuple(sorted(unresolved))


@dataclass(frozen=True, slots=True)
class PositionHolding:
    target_id: str
    venue: str
    instrument_class: str

    def __post_init__(self) -> None:
        _trimmed(self.target_id, "position target_id")
        _trimmed(self.venue, "position venue")
        _trimmed(self.instrument_class, "position instrument_class")

    def to_dict(self) -> dict[str, str]:
        return {
            "target_id": self.target_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
        }


@dataclass(frozen=True, slots=True)
class ProspectivePositionSnapshot:
    snapshot_id: str
    as_of: datetime
    holdings: tuple[PositionHolding, ...]
    schema_version: str = PROSPECTIVE_POSITION_SNAPSHOT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_POSITION_SNAPSHOT_SCHEMA:
            raise ValueError("unsupported prospective Position Snapshot schema")
        _strict_utc(self.as_of, "prospective Position Snapshot as_of")
        keys = tuple((item.target_id, item.venue, item.instrument_class) for item in self.holdings)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("prospective Position Snapshot holdings must be sorted and unique")
        if self.snapshot_id != self.expected_snapshot_id:
            raise ValueError("prospective Position Snapshot ID does not match content")

    @property
    def expected_snapshot_id(self) -> str:
        return f"prospective-position-snapshot-{canonical_hash(self.core_dict())}"

    @property
    def target_ids(self) -> tuple[str, ...]:
        return tuple(sorted({item.target_id for item in self.holdings}))

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "as_of": _timestamp(self.as_of),
            "holdings": [item.to_dict() for item in self.holdings],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_id": self.snapshot_id}

    @classmethod
    def build(
        cls,
        *,
        as_of: datetime,
        holdings: tuple[PositionHolding, ...],
    ) -> ProspectivePositionSnapshot:
        ordered = tuple(
            sorted(
                holdings,
                key=lambda item: (item.target_id, item.venue, item.instrument_class),
            )
        )
        core = {
            "schema_version": PROSPECTIVE_POSITION_SNAPSHOT_SCHEMA,
            "as_of": _timestamp(as_of),
            "holdings": [item.to_dict() for item in ordered],
        }
        return cls(
            snapshot_id=f"prospective-position-snapshot-{canonical_hash(core)}",
            as_of=as_of,
            holdings=ordered,
        )


@dataclass(frozen=True, slots=True)
class HistoricalAnalogyCase:
    case_ref: str
    mode: HistoricalAnalogyMode
    artifact_hash: str
    similarity_basis: str
    counterevidence: tuple[str, ...]

    def __post_init__(self) -> None:
        _trimmed(self.case_ref, "historical analogy case_ref")
        _sha256(self.artifact_hash, "historical analogy artifact_hash")
        _trimmed(self.similarity_basis, "historical analogy similarity_basis")
        _sorted_unique(self.counterevidence, "historical analogy counterevidence")

    def to_dict(self) -> dict[str, object]:
        return {
            "case_ref": self.case_ref,
            "mode": self.mode.value,
            "artifact_hash": self.artifact_hash,
            "similarity_basis": self.similarity_basis,
            "counterevidence": list(self.counterevidence),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveHistoricalAnalogyPack:
    pack_id: str
    cases: tuple[HistoricalAnalogyCase, ...]
    built_at: datetime
    schema_version: str = PROSPECTIVE_HISTORICAL_ANALOGY_PACK_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_HISTORICAL_ANALOGY_PACK_SCHEMA:
            raise ValueError("unsupported prospective Historical Analogy Pack schema")
        if not self.cases:
            raise ValueError("prospective Historical Analogy Pack requires at least one case")
        refs = tuple(item.case_ref for item in self.cases)
        if refs != tuple(sorted(set(refs))):
            raise ValueError("prospective historical analogy cases must be sorted and unique")
        _strict_utc(self.built_at, "prospective Historical Analogy Pack built_at")
        if self.pack_id != self.expected_pack_id:
            raise ValueError("prospective Historical Analogy Pack ID does not match content")

    @property
    def expected_pack_id(self) -> str:
        return f"prospective-historical-analogy-pack-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "cases": [item.to_dict() for item in self.cases],
            "built_at": _timestamp(self.built_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "pack_id": self.pack_id}

    @classmethod
    def build(
        cls,
        *,
        cases: tuple[HistoricalAnalogyCase, ...],
        built_at: datetime,
    ) -> ProspectiveHistoricalAnalogyPack:
        ordered = tuple(sorted(cases, key=lambda item: item.case_ref))
        core = {
            "schema_version": PROSPECTIVE_HISTORICAL_ANALOGY_PACK_SCHEMA,
            "cases": [item.to_dict() for item in ordered],
            "built_at": _timestamp(built_at),
        }
        return cls(
            pack_id=f"prospective-historical-analogy-pack-{canonical_hash(core)}",
            cases=ordered,
            built_at=built_at,
        )


@dataclass(frozen=True, slots=True)
class TransmissionPath:
    target_id: str
    venue: str
    instrument_class: str
    channels: tuple[TransmissionChannel, ...]
    causal_steps: tuple[str, ...]
    evidence_version_ids: tuple[str, ...]
    horizon_sessions: int

    def __post_init__(self) -> None:
        _trimmed(self.target_id, "event assessment target_id")
        _trimmed(self.venue, "event assessment venue")
        _trimmed(self.instrument_class, "event assessment instrument_class")
        if self.channels != tuple(sorted(set(self.channels), key=lambda item: item.value)):
            raise ValueError("event assessment channels must be sorted and unique")
        if not self.channels:
            raise ValueError("event assessment path requires a transmission channel")
        _ordered_nonempty(self.causal_steps, "event assessment causal_steps")
        _sorted_unique(self.evidence_version_ids, "event assessment evidence_version_ids")
        if not self.evidence_version_ids:
            raise ValueError("event assessment path requires cited evidence")
        if self.horizon_sessions < 1:
            raise ValueError("event assessment horizon_sessions must be positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "channels": [item.value for item in self.channels],
            "causal_steps": list(self.causal_steps),
            "evidence_version_ids": list(self.evidence_version_ids),
            "horizon_sessions": self.horizon_sessions,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveEventAssessmentArtifact:
    assessment_id: str
    triage_decision_id: str
    cluster_id: str
    event_assessment_artifact_hash: str
    paths: tuple[TransmissionPath, ...]
    counterevidence: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    assessed_at: datetime
    position_snapshot: ProspectivePositionSnapshot | None
    historical_analogy_pack: ProspectiveHistoricalAnalogyPack | None
    historical_pit_claim: bool = False
    signal_or_execution_capability: bool = False
    schema_version: str = PROSPECTIVE_EVENT_ASSESSMENT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_EVENT_ASSESSMENT_SCHEMA:
            raise ValueError("unsupported prospective EventAssessment schema")
        _prefixed_hash(
            self.triage_decision_id,
            "event-impact-triage-decision-",
            "EventAssessment triage_decision_id",
        )
        _prefixed_hash(
            self.cluster_id,
            "event-impact-triage-cluster-",
            "EventAssessment cluster_id",
        )
        _sha256(
            self.event_assessment_artifact_hash,
            "EventAssessment canonical artifact hash",
        )
        path_keys = tuple(
            (item.target_id, item.venue, item.instrument_class, item.horizon_sessions)
            for item in self.paths
        )
        if not path_keys or path_keys != tuple(sorted(set(path_keys))):
            raise ValueError("EventAssessment paths must be non-empty, sorted, and unique")
        _sorted_unique(self.counterevidence, "EventAssessment counterevidence")
        _sorted_unique(self.invalidation_conditions, "EventAssessment invalidation_conditions")
        if not self.counterevidence or not self.invalidation_conditions:
            raise ValueError("EventAssessment requires counterevidence and invalidation conditions")
        _strict_utc(self.assessed_at, "EventAssessment assessed_at")
        if self.position_snapshot is not None and self.position_snapshot.as_of > self.assessed_at:
            raise ValueError("EventAssessment Position Snapshot postdates assessment")
        if (
            self.historical_analogy_pack is not None
            and self.historical_analogy_pack.built_at > self.assessed_at
        ):
            raise ValueError("EventAssessment Historical Analogy Pack postdates assessment")
        if self.historical_pit_claim or self.signal_or_execution_capability:
            raise ValueError("EventAssessment cannot grant PIT, Signal, or execution authority")
        if self.assessment_id != self.expected_assessment_id:
            raise ValueError("EventAssessment ID does not match content")

    @property
    def expected_assessment_id(self) -> str:
        return f"prospective-event-assessment-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "triage_decision_id": self.triage_decision_id,
            "cluster_id": self.cluster_id,
            "event_assessment_artifact_hash": self.event_assessment_artifact_hash,
            "paths": [item.to_dict() for item in self.paths],
            "counterevidence": list(self.counterevidence),
            "invalidation_conditions": list(self.invalidation_conditions),
            "assessed_at": _timestamp(self.assessed_at),
            "position_snapshot": (
                None if self.position_snapshot is None else self.position_snapshot.to_dict()
            ),
            "historical_analogy_pack": (
                None
                if self.historical_analogy_pack is None
                else self.historical_analogy_pack.to_dict()
            ),
            "historical_pit_claim": self.historical_pit_claim,
            "signal_or_execution_capability": self.signal_or_execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "assessment_id": self.assessment_id}

    @classmethod
    def build(
        cls,
        *,
        triage_decision: EventImpactTriageDecision,
        cluster: TriageClusterProposal,
        event_assessment_artifact_hash: str,
        paths: tuple[TransmissionPath, ...],
        counterevidence: tuple[str, ...],
        invalidation_conditions: tuple[str, ...],
        assessed_at: datetime,
        position_snapshot: ProspectivePositionSnapshot | None = None,
        historical_analogy_pack: ProspectiveHistoricalAnalogyPack | None = None,
    ) -> ProspectiveEventAssessmentArtifact:
        cluster_id = cluster.cluster_id
        _sha256(event_assessment_artifact_hash, "EventAssessment canonical artifact hash")
        evidence_ids = set(cluster.evidence_version_ids)
        if cluster_id not in triage_decision.event_assessment_cluster_ids and not (
            triage_decision.status is TriageDecisionStatus.ELIGIBLE_SELECTED
            and triage_decision.selected_cluster_id == cluster_id
        ):
            raise ValueError("EventAssessment cluster was not routed by the Triage Decision")
        ordered = tuple(
            sorted(
                paths,
                key=lambda item: (
                    item.target_id,
                    item.venue,
                    item.instrument_class,
                    item.horizon_sessions,
                ),
            )
        )
        if any(not set(item.evidence_version_ids) <= evidence_ids for item in ordered):
            raise ValueError("EventAssessment path cites evidence outside its triage cluster")
        ordered_counterevidence = tuple(sorted(set(counterevidence)))
        ordered_invalidation = tuple(sorted(set(invalidation_conditions)))
        core = {
            "schema_version": PROSPECTIVE_EVENT_ASSESSMENT_SCHEMA,
            "triage_decision_id": triage_decision.decision_id,
            "cluster_id": cluster_id,
            "event_assessment_artifact_hash": event_assessment_artifact_hash,
            "paths": [item.to_dict() for item in ordered],
            "counterevidence": list(ordered_counterevidence),
            "invalidation_conditions": list(ordered_invalidation),
            "assessed_at": _timestamp(assessed_at),
            "position_snapshot": (
                None if position_snapshot is None else position_snapshot.to_dict()
            ),
            "historical_analogy_pack": (
                None if historical_analogy_pack is None else historical_analogy_pack.to_dict()
            ),
            "historical_pit_claim": False,
            "signal_or_execution_capability": False,
        }
        return cls(
            assessment_id=f"prospective-event-assessment-{canonical_hash(core)}",
            triage_decision_id=triage_decision.decision_id,
            cluster_id=cluster_id,
            event_assessment_artifact_hash=event_assessment_artifact_hash,
            paths=ordered,
            counterevidence=ordered_counterevidence,
            invalidation_conditions=ordered_invalidation,
            assessed_at=assessed_at,
            position_snapshot=position_snapshot,
            historical_analogy_pack=historical_analogy_pack,
        )


@dataclass(frozen=True, slots=True)
class ProspectiveMaterialityGateResult:
    result_id: str
    registration_id: str
    checkpoint_key: str
    assessment_id: str
    registered_target_venues: tuple[str, ...]
    registered_instrument_classes: tuple[str, ...]
    registered_horizon_sessions: tuple[int, ...]
    disposition: MaterialityDisposition
    admitted_target_ids: tuple[str, ...]
    held_target_ids: tuple[str, ...]
    blocking_gaps: tuple[str, ...]
    nonblocking_information_gaps: tuple[str, ...]
    evaluated_at: datetime
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_MATERIALITY_GATE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_MATERIALITY_GATE_SCHEMA:
            raise ValueError("unsupported prospective Materiality Gate schema")
        _prefixed_hash(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "Materiality Gate registration_id",
        )
        _trimmed(self.checkpoint_key, "Materiality Gate checkpoint_key")
        _prefixed_hash(
            self.assessment_id,
            "prospective-event-assessment-",
            "Materiality Gate assessment_id",
        )
        for name in (
            "admitted_target_ids",
            "held_target_ids",
            "blocking_gaps",
            "nonblocking_information_gaps",
        ):
            _sorted_unique(cast(tuple[str, ...], getattr(self, name)), f"Materiality Gate {name}")
        _sorted_unique(self.registered_target_venues, "Materiality Gate target venues")
        _sorted_unique(
            self.registered_instrument_classes,
            "Materiality Gate instrument classes",
        )
        if (
            not self.registered_horizon_sessions
            or tuple(sorted(set(self.registered_horizon_sessions)))
            != self.registered_horizon_sessions
        ):
            raise ValueError("Materiality Gate horizons must be sorted and unique")
        if not set(self.held_target_ids) <= set(self.admitted_target_ids):
            raise ValueError("Materiality Gate held targets must be admitted targets")
        expected = (
            MaterialityDisposition.ADMIT
            if self.admitted_target_ids and not self.blocking_gaps
            else MaterialityDisposition.WATCH
            if self.blocking_gaps
            else MaterialityDisposition.ARCHIVE
        )
        if self.disposition is not expected:
            raise ValueError("Materiality Gate disposition does not match its evidence")
        _strict_utc(self.evaluated_at, "Materiality Gate evaluated_at")
        if self.judgment_model_calls_authorized or self.execution_capability:
            raise ValueError("Materiality Gate cannot grant Judgment or execution authority")
        if self.result_id != self.expected_result_id:
            raise ValueError("Materiality Gate result ID does not match content")

    @property
    def expected_result_id(self) -> str:
        return f"prospective-materiality-gate-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "assessment_id": self.assessment_id,
            "registered_target_venues": list(self.registered_target_venues),
            "registered_instrument_classes": list(self.registered_instrument_classes),
            "registered_horizon_sessions": list(self.registered_horizon_sessions),
            "disposition": self.disposition.value,
            "admitted_target_ids": list(self.admitted_target_ids),
            "held_target_ids": list(self.held_target_ids),
            "blocking_gaps": list(self.blocking_gaps),
            "nonblocking_information_gaps": list(self.nonblocking_information_gaps),
            "evaluated_at": _timestamp(self.evaluated_at),
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "result_id": self.result_id}


def evaluate_event_materiality(
    *,
    registration: ProspectiveDiagnosticRegistration,
    checkpoint_key: str,
    assessment: ProspectiveEventAssessmentArtifact,
    evaluated_at: datetime,
) -> ProspectiveMaterialityGateResult:
    _strict_utc(evaluated_at, "Materiality Gate evaluated_at")
    if evaluated_at < assessment.assessed_at:
        raise ValueError("Materiality Gate cannot predate EventAssessment")
    checkpoint = registration.checkpoint(checkpoint_key)
    if checkpoint.mechanism is not DiagnosticMechanism.MATERIAL_EVENT:
        raise ValueError("Materiality Gate requires a registered material-event checkpoint")
    venues = set(checkpoint.target_venues)
    classes = set(checkpoint.allowed_instrument_classes)
    horizons = set(checkpoint.candidate_horizon_sessions)
    registered_venues = tuple(sorted(venues))
    registered_classes = tuple(sorted(classes))
    registered_horizons = tuple(sorted(horizons))
    blocking: list[str] = []
    information: list[str] = []
    admitted: list[str] = []
    for path in assessment.paths:
        if path.venue not in venues:
            information.append(f"target:{path.target_id}:venue_not_allowed")
        elif path.instrument_class not in classes:
            information.append(f"target:{path.target_id}:instrument_class_not_allowed")
        elif path.horizon_sessions not in horizons:
            information.append(f"target:{path.target_id}:horizon_not_registered")
        else:
            admitted.append(path.target_id)
    if assessment.position_snapshot is None:
        information.append("position_snapshot:absent")
        held: tuple[str, ...] = ()
    else:
        held = tuple(sorted(set(admitted) & set(assessment.position_snapshot.target_ids)))
    if assessment.historical_analogy_pack is None:
        information.append("historical_analogy_pack:absent")
    admitted_targets = tuple(sorted(set(admitted)))
    held_targets = held
    blocking_gaps = tuple(sorted(set(blocking)))
    information_gaps = tuple(sorted(set(information)))
    disposition = (
        MaterialityDisposition.ADMIT
        if admitted_targets
        else MaterialityDisposition.WATCH
        if blocking_gaps
        else MaterialityDisposition.ARCHIVE
    )
    core = {
        "schema_version": PROSPECTIVE_MATERIALITY_GATE_SCHEMA,
        "registration_id": registration.registration_id,
        "checkpoint_key": checkpoint_key,
        "assessment_id": assessment.assessment_id,
        "registered_target_venues": list(registered_venues),
        "registered_instrument_classes": list(registered_classes),
        "registered_horizon_sessions": list(registered_horizons),
        "disposition": disposition.value,
        "admitted_target_ids": list(admitted_targets),
        "held_target_ids": list(held_targets),
        "blocking_gaps": list(blocking_gaps),
        "nonblocking_information_gaps": list(information_gaps),
        "evaluated_at": _timestamp(evaluated_at),
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return ProspectiveMaterialityGateResult(
        result_id=f"prospective-materiality-gate-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint_key,
        assessment_id=assessment.assessment_id,
        registered_target_venues=registered_venues,
        registered_instrument_classes=registered_classes,
        registered_horizon_sessions=registered_horizons,
        disposition=disposition,
        admitted_target_ids=admitted_targets,
        held_target_ids=held_targets,
        blocking_gaps=blocking_gaps,
        nonblocking_information_gaps=information_gaps,
        evaluated_at=evaluated_at,
    )


@dataclass(frozen=True, slots=True)
class ProspectiveTriggerAdmission:
    admission_id: str
    kind: TriggerAdmissionKind
    registration_id: str
    checkpoint_key: str
    candidate_set_id: str
    proposal_id: str
    triage_decision_id: str
    cluster_id: str
    observation_version_ids: tuple[str, ...]
    event_assessment_id: str | None
    materiality_gate_result_id: str | None
    preceding_materiality_gate_result_ids: tuple[str, ...]
    admitted_target_ids: tuple[str, ...]
    held_target_ids: tuple[str, ...]
    admitted_at: datetime
    historical_pit_claim: bool = False
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA:
            raise ValueError("unsupported prospective Trigger Admission schema")
        _prefixed_hash(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "Trigger Admission registration_id",
        )
        _trimmed(self.checkpoint_key, "Trigger Admission checkpoint_key")
        _prefixed_hash(
            self.candidate_set_id,
            "event-impact-triage-candidate-set-",
            "Trigger Admission candidate_set_id",
        )
        _prefixed_hash(
            self.proposal_id,
            "event-impact-triage-proposal-",
            "Trigger Admission proposal_id",
        )
        _prefixed_hash(
            self.triage_decision_id,
            "event-impact-triage-decision-",
            "Trigger Admission triage_decision_id",
        )
        _prefixed_hash(
            self.cluster_id,
            "event-impact-triage-cluster-",
            "Trigger Admission cluster_id",
        )
        _sorted_unique(self.observation_version_ids, "Trigger Admission observation versions")
        if not self.observation_version_ids:
            raise ValueError("Trigger Admission requires observation versions")
        for version_id in self.observation_version_ids:
            _prefixed_hash(
                version_id,
                "prospective-observation-version-",
                "Trigger Admission observation version",
            )
        _sorted_unique(self.admitted_target_ids, "Trigger Admission targets")
        _sorted_unique(self.held_target_ids, "Trigger Admission held targets")
        if len(self.preceding_materiality_gate_result_ids) != len(
            set(self.preceding_materiality_gate_result_ids)
        ):
            raise ValueError("Trigger Admission preceding Materiality results must be unique")
        for result_id in self.preceding_materiality_gate_result_ids:
            _prefixed_hash(
                result_id,
                "prospective-materiality-gate-",
                "Trigger Admission preceding Materiality result",
            )
        if not set(self.held_target_ids) <= set(self.admitted_target_ids):
            raise ValueError("Trigger Admission held targets must be admitted targets")
        if self.kind is TriggerAdmissionKind.MATERIAL_EVENT:
            if self.event_assessment_id is None or self.materiality_gate_result_id is None:
                raise ValueError("material Trigger Admission requires assessment and gate")
            if not self.admitted_target_ids:
                raise ValueError("material Trigger Admission requires an admitted target")
        elif (
            self.event_assessment_id is not None
            or self.materiality_gate_result_id is not None
            or self.preceding_materiality_gate_result_ids
            or self.admitted_target_ids
            or self.held_target_ids
        ):
            raise ValueError("eligible checkpoint Trigger Admission cannot carry materiality state")
        _strict_utc(self.admitted_at, "Trigger Admission admitted_at")
        if (
            self.historical_pit_claim
            or self.judgment_model_calls_authorized
            or self.execution_capability
        ):
            raise ValueError("Trigger Admission cannot grant PIT, Judgment, or execution authority")
        if self.admission_id != self.expected_admission_id:
            raise ValueError("Trigger Admission ID does not match content")

    @property
    def expected_admission_id(self) -> str:
        return f"prospective-trigger-admission-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "candidate_set_id": self.candidate_set_id,
            "proposal_id": self.proposal_id,
            "triage_decision_id": self.triage_decision_id,
            "cluster_id": self.cluster_id,
            "observation_version_ids": list(self.observation_version_ids),
            "event_assessment_id": self.event_assessment_id,
            "materiality_gate_result_id": self.materiality_gate_result_id,
            "preceding_materiality_gate_result_ids": list(
                self.preceding_materiality_gate_result_ids
            ),
            "admitted_target_ids": list(self.admitted_target_ids),
            "held_target_ids": list(self.held_target_ids),
            "admitted_at": _timestamp(self.admitted_at),
            "historical_pit_claim": self.historical_pit_claim,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "admission_id": self.admission_id}


class ProspectiveTriggerAdmissionStore:
    """Durable, content-verified index for immutable Trigger Admissions."""

    def __init__(self, store: LocalDataSnapshotStore) -> None:
        self.store = store
        self.index_path = store.index_path
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_trigger_admissions (
                    admission_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    event_assessment_artifact_hash TEXT,
                    materiality_artifact_hash TEXT,
                    materiality_selection_artifact_hash TEXT,
                    registration_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    admitted_at TEXT NOT NULL
                )
                """
            )
            columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(prospective_trigger_admissions)")
            }
            for name in (
                "event_assessment_artifact_hash",
                "materiality_artifact_hash",
                "materiality_selection_artifact_hash",
            ):
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE prospective_trigger_admissions ADD COLUMN {name} TEXT"
                    )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS prospective_trigger_one_per_checkpoint
                ON prospective_trigger_admissions(registration_id, checkpoint_key)
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.index_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def record(
        self,
        admission: ProspectiveTriggerAdmission,
        *,
        registration: ProspectiveDiagnosticRegistration,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        decision: EventImpactTriageDecision,
        triage_authority: CompletedTriageDecisionAuthority,
        assessment: ProspectiveEventAssessmentArtifact | None = None,
        materiality: ProspectiveMaterialityGateResult | None = None,
        preceding_materiality_contexts: tuple[
            tuple[ProspectiveEventAssessmentArtifact, ProspectiveMaterialityGateResult], ...
        ] = (),
        assessment_authority: CompletedEventAssessmentAuthority | None = None,
    ) -> ProspectiveTriggerAdmission:
        reopened = triage_authority.get_context(candidate_set.candidate_set_id)
        if reopened != (candidate_set, proposal, decision):
            raise ValueError("Trigger Admission inputs do not match authoritative Triage Decision")
        if (
            admission.candidate_set_id != candidate_set.candidate_set_id
            or admission.proposal_id != proposal.proposal_id
            or admission.triage_decision_id != decision.decision_id
        ):
            raise ValueError("Trigger Admission does not bind authoritative Triage inputs")
        epoch_contexts = triage_authority.route_epoch_contexts(
            registration_id=candidate_set.registration_id,
            checkpoint_key=candidate_set.checkpoint_key,
            route_plan_id=candidate_set.route_plan_id,
            route_admission_id=candidate_set.route_admission_id,
            at=admission.admitted_at,
        )
        selected_indexes = tuple(
            index
            for index, (_, _, epoch_decision, epoch_cluster) in enumerate(epoch_contexts)
            if epoch_decision.decision_id == decision.decision_id
            and epoch_cluster.cluster_id == admission.cluster_id
        )
        if len(selected_indexes) != 1:
            raise ValueError("Trigger Admission cluster is not unique in its route epoch")
        selected_index = selected_indexes[0]
        selected_cluster = next(
            (item for item in proposal.clusters if item.cluster_id == admission.cluster_id),
            None,
        )
        if selected_cluster is None or epoch_contexts[selected_index] != (
            candidate_set,
            proposal,
            decision,
            selected_cluster,
        ):
            raise ValueError("Trigger Admission route epoch context differs from its Triage inputs")
        earlier_contexts = epoch_contexts[:selected_index]
        unresolved_review = unresolved_route_review_cluster_ids(
            earlier_contexts=earlier_contexts,
            resolution_contexts=epoch_contexts[: selected_index + 1],
        )
        if unresolved_review:
            raise ValueError("Trigger Admission has an earlier unresolved review candidate")
        expected_preceding_contexts: tuple[
            tuple[
                EventImpactTriageCandidateSet,
                EventImpactTriageProposal,
                EventImpactTriageDecision,
                TriageClusterProposal,
            ],
            ...,
        ] = ()
        if registration.checkpoint(candidate_set.checkpoint_key).mechanism is (
            DiagnosticMechanism.MATERIAL_EVENT
        ):
            expected_preceding_contexts = tuple(
                context
                for context in earlier_contexts
                if context[3].cluster_id in context[2].event_assessment_cluster_ids
            )
            expected_preceding_keys = tuple(
                (epoch_decision.decision_id, epoch_cluster.cluster_id)
                for _, _, epoch_decision, epoch_cluster in expected_preceding_contexts
            )
            actual_preceding_keys = tuple(
                (prior_assessment.triage_decision_id, prior_assessment.cluster_id)
                for prior_assessment, _ in preceding_materiality_contexts
            )
            if actual_preceding_keys != expected_preceding_keys:
                raise ValueError(
                    "material Trigger Admission requires every earlier route-epoch "
                    "EventAssessment result"
                )
        else:
            earlier_checkpoint_candidates = tuple(
                epoch_cluster.cluster_id
                for _, _, _, epoch_cluster in earlier_contexts
                if epoch_cluster.checkpoint_eligibility is CheckpointEligibility.ELIGIBLE
                and epoch_cluster.recommended_route is TriageRoute.CHECKPOINT_CANDIDATE
            )
            if earlier_checkpoint_candidates:
                raise ValueError(
                    "Trigger Admission is not the first eligible route-epoch candidate"
                )
        expected_admission = admit_prospective_trigger(
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            cluster_id=admission.cluster_id,
            admitted_at=admission.admitted_at,
            assessment=assessment,
            materiality=materiality,
            preceding_materiality_contexts=preceding_materiality_contexts,
        )
        if expected_admission != admission:
            raise ValueError("Trigger Admission differs from deterministic Harness admission")
        self._validate_context(
            admission,
            assessment=assessment,
            materiality=materiality,
            preceding_materiality_contexts=preceding_materiality_contexts,
        )
        if admission.kind is TriggerAdmissionKind.MATERIAL_EVENT:
            if assessment is None or assessment_authority is None:
                raise ValueError(
                    "material Trigger Admission requires completed EventAssessment authority"
                )
            assessment_authority.assert_authoritative_completed_event_assessment(
                candidate_set=candidate_set,
                proposal=proposal,
                decision=decision,
                assessment=assessment,
            )
            prior_authority_contexts = {
                (epoch_decision.decision_id, epoch_cluster.cluster_id): (
                    epoch_candidate_set,
                    epoch_proposal,
                    epoch_decision,
                )
                for (
                    epoch_candidate_set,
                    epoch_proposal,
                    epoch_decision,
                    epoch_cluster,
                ) in expected_preceding_contexts
            }
            for prior_assessment, _ in preceding_materiality_contexts:
                prior_candidate_set, prior_proposal, prior_decision = prior_authority_contexts[
                    (prior_assessment.triage_decision_id, prior_assessment.cluster_id)
                ]
                assessment_authority.assert_authoritative_completed_event_assessment(
                    candidate_set=prior_candidate_set,
                    proposal=prior_proposal,
                    decision=prior_decision,
                    assessment=prior_assessment,
                )
        elif assessment_authority is not None:
            raise ValueError("checkpoint Trigger Admission cannot use EventAssessment authority")
        artifact = self.store.artifacts.put_json(admission.to_dict())
        assessment_artifact = (
            None if assessment is None else self.store.artifacts.put_json(assessment.to_dict())
        )
        materiality_artifact = (
            None if materiality is None else self.store.artifacts.put_json(materiality.to_dict())
        )
        selection_artifact = (
            None
            if not preceding_materiality_contexts
            else self.store.artifacts.put_json(
                {
                    "schema_version": "market-impact.materiality-selection-context.v1",
                    "preceding_contexts": [
                        {
                            "assessment": prior_assessment.to_dict(),
                            "materiality": prior_materiality.to_dict(),
                        }
                        for prior_assessment, prior_materiality in preceding_materiality_contexts
                    ],
                }
            )
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT artifact_hash, event_assessment_artifact_hash,
                       materiality_artifact_hash, materiality_selection_artifact_hash,
                       registration_id, checkpoint_key,
                       cluster_id, admitted_at
                FROM prospective_trigger_admissions
                WHERE admission_id = ?
                """,
                (admission.admission_id,),
            ).fetchone()
            if row is not None:
                reopened = self._verified(admission.admission_id, row)
                if (
                    reopened != admission
                    or row["artifact_hash"] != artifact.content_hash
                    or row["event_assessment_artifact_hash"]
                    != (None if assessment_artifact is None else assessment_artifact.content_hash)
                    or row["materiality_artifact_hash"]
                    != (None if materiality_artifact is None else materiality_artifact.content_hash)
                    or row["materiality_selection_artifact_hash"]
                    != (None if selection_artifact is None else selection_artifact.content_hash)
                ):
                    raise ValueError("Trigger Admission durable identity conflicts with content")
                return reopened
            connection.execute(
                """
                INSERT INTO prospective_trigger_admissions(
                    admission_id, artifact_hash, event_assessment_artifact_hash,
                    materiality_artifact_hash, materiality_selection_artifact_hash,
                    registration_id, checkpoint_key, cluster_id, admitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    admission.admission_id,
                    artifact.content_hash,
                    (None if assessment_artifact is None else assessment_artifact.content_hash),
                    (None if materiality_artifact is None else materiality_artifact.content_hash),
                    (None if selection_artifact is None else selection_artifact.content_hash),
                    admission.registration_id,
                    admission.checkpoint_key,
                    admission.cluster_id,
                    _timestamp(admission.admitted_at),
                ),
            )
        return admission

    def get(self, admission_id: str) -> ProspectiveTriggerAdmission:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT artifact_hash, event_assessment_artifact_hash,
                       materiality_artifact_hash, materiality_selection_artifact_hash,
                       registration_id, checkpoint_key,
                       cluster_id, admitted_at
                FROM prospective_trigger_admissions
                WHERE admission_id = ?
                """,
                (admission_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective Trigger Admission: {admission_id}")
        return self._verified(admission_id, row)

    def get_context(
        self,
        admission_id: str,
    ) -> tuple[
        ProspectiveTriggerAdmission,
        ProspectiveEventAssessmentArtifact | None,
        ProspectiveMaterialityGateResult | None,
    ]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT artifact_hash, event_assessment_artifact_hash,
                       materiality_artifact_hash, materiality_selection_artifact_hash,
                       registration_id, checkpoint_key,
                       cluster_id, admitted_at
                FROM prospective_trigger_admissions
                WHERE admission_id = ?
                """,
                (admission_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective Trigger Admission: {admission_id}")
        admission = self._verified(admission_id, row)
        assessment_hash = cast(str | None, row["event_assessment_artifact_hash"])
        materiality_hash = cast(str | None, row["materiality_artifact_hash"])
        selection_hash = cast(str | None, row["materiality_selection_artifact_hash"])
        assessment = (
            None
            if assessment_hash is None
            else prospective_event_assessment_from_dict(
                json.loads(
                    self.store.artifacts.get(
                        assessment_hash,
                        media_type="application/json",
                    ).path.read_text(encoding="utf-8")
                )
            )
        )
        materiality = (
            None
            if materiality_hash is None
            else prospective_materiality_gate_result_from_dict(
                json.loads(
                    self.store.artifacts.get(
                        materiality_hash,
                        media_type="application/json",
                    ).path.read_text(encoding="utf-8")
                )
            )
        )
        preceding_contexts: tuple[
            tuple[ProspectiveEventAssessmentArtifact, ProspectiveMaterialityGateResult], ...
        ] = ()
        if selection_hash is not None:
            selection_payload = _object(
                self.store.artifacts.read_json(selection_hash),
                "materiality selection context",
            )
            _exact_keys(
                selection_payload,
                {"schema_version", "preceding_contexts"},
                "materiality selection context",
            )
            if selection_payload["schema_version"] != (
                "market-impact.materiality-selection-context.v1"
            ):
                raise ValueError("unsupported materiality selection context")
            parsed_contexts: list[
                tuple[ProspectiveEventAssessmentArtifact, ProspectiveMaterialityGateResult]
            ] = []
            for item in _list(
                selection_payload.get("preceding_contexts"),
                "materiality selection preceding contexts",
            ):
                context = _object(item, "materiality selection preceding context")
                _exact_keys(
                    context,
                    {"assessment", "materiality"},
                    "materiality selection preceding context",
                )
                parsed_contexts.append(
                    (
                        prospective_event_assessment_from_dict(context["assessment"]),
                        prospective_materiality_gate_result_from_dict(context["materiality"]),
                    )
                )
            preceding_contexts = tuple(parsed_contexts)
        self._validate_context(
            admission,
            assessment=assessment,
            materiality=materiality,
            preceding_materiality_contexts=preceding_contexts,
        )
        return admission, assessment, materiality

    def assert_authoritative(self, admission: ProspectiveTriggerAdmission) -> None:
        reopened, _, _ = self.get_context(admission.admission_id)
        if reopened != admission:
            raise ValueError("Trigger Admission differs from durable authority")

    @staticmethod
    def _validate_context(
        admission: ProspectiveTriggerAdmission,
        *,
        assessment: ProspectiveEventAssessmentArtifact | None,
        materiality: ProspectiveMaterialityGateResult | None,
        preceding_materiality_contexts: tuple[
            tuple[ProspectiveEventAssessmentArtifact, ProspectiveMaterialityGateResult], ...
        ],
    ) -> None:
        if admission.kind is TriggerAdmissionKind.CHECKPOINT_ELIGIBLE:
            if assessment is not None or materiality is not None or preceding_materiality_contexts:
                raise ValueError("checkpoint Trigger Admission cannot persist material context")
            return
        if assessment is None or materiality is None:
            raise ValueError("material Trigger Admission requires its full persisted context")
        if (
            admission.event_assessment_id != assessment.assessment_id
            or admission.materiality_gate_result_id != materiality.result_id
            or materiality.assessment_id != assessment.assessment_id
            or admission.triage_decision_id != assessment.triage_decision_id
            or admission.cluster_id != assessment.cluster_id
            or admission.admitted_target_ids != materiality.admitted_target_ids
            or admission.held_target_ids != materiality.held_target_ids
            or admission.preceding_materiality_gate_result_ids
            != tuple(item.result_id for _, item in preceding_materiality_contexts)
            or any(
                prior_materiality.assessment_id != prior_assessment.assessment_id
                or prior_materiality.disposition is MaterialityDisposition.ADMIT
                for prior_assessment, prior_materiality in preceding_materiality_contexts
            )
        ):
            raise ValueError("material Trigger Admission context does not match")

    def _verified(
        self,
        admission_id: str,
        row: sqlite3.Row,
    ) -> ProspectiveTriggerAdmission:
        artifact_hash = cast(str, row["artifact_hash"])
        stored = self.store.artifacts.get(artifact_hash, media_type="application/json")
        admission = prospective_trigger_admission_from_dict(
            json.loads(stored.path.read_text(encoding="utf-8"))
        )
        if (
            admission.admission_id != admission_id
            or admission.registration_id != row["registration_id"]
            or admission.checkpoint_key != row["checkpoint_key"]
            or admission.cluster_id != row["cluster_id"]
            or _timestamp(admission.admitted_at) != row["admitted_at"]
        ):
            raise ValueError("Trigger Admission durable index does not match its artifact")
        return admission


def admit_prospective_trigger(
    *,
    registration: ProspectiveDiagnosticRegistration,
    candidate_set: EventImpactTriageCandidateSet,
    proposal: EventImpactTriageProposal,
    decision: EventImpactTriageDecision,
    cluster_id: str,
    admitted_at: datetime,
    assessment: ProspectiveEventAssessmentArtifact | None = None,
    materiality: ProspectiveMaterialityGateResult | None = None,
    preceding_materiality_contexts: tuple[
        tuple[ProspectiveEventAssessmentArtifact, ProspectiveMaterialityGateResult], ...
    ] = (),
) -> ProspectiveTriggerAdmission:
    if candidate_set.registration_id != registration.registration_id:
        raise ValueError("Trigger Admission candidate set binds another registration")
    checkpoint = registration.checkpoint(candidate_set.checkpoint_key)
    proposal.validate_against(candidate_set)
    if decision.candidate_set_id != candidate_set.candidate_set_id:
        raise ValueError("Trigger Admission Triage Decision belongs to another candidate set")
    if decision.proposal_id != proposal.proposal_id:
        raise ValueError("Trigger Admission Triage Decision binds another proposal")
    cluster = next((item for item in proposal.clusters if item.cluster_id == cluster_id), None)
    if cluster is None:
        raise ValueError("Trigger Admission cluster is outside the Triage Proposal")
    _strict_utc(admitted_at, "Trigger Admission admitted_at")
    if admitted_at < decision.decided_at:
        raise ValueError("Trigger Admission cannot predate the Triage Decision")

    selected = (
        decision.status is TriageDecisionStatus.ELIGIBLE_SELECTED
        and decision.selected_cluster_id == cluster_id
        and cluster.checkpoint_eligibility is CheckpointEligibility.ELIGIBLE
        and cluster.recommended_route is TriageRoute.CHECKPOINT_CANDIDATE
    )
    if checkpoint.mechanism is DiagnosticMechanism.MATERIAL_EVENT:
        if decision.selected_cluster_id is not None or decision.unselected_eligible_cluster_ids:
            raise ValueError(
                "material checkpoint Triage must route candidates through EventAssessment"
            )
        if cluster_id not in decision.event_assessment_cluster_ids:
            raise ValueError("material checkpoint cluster was not routed to EventAssessment")
        if assessment is None or materiality is None:
            raise ValueError(
                "material checkpoint Trigger Admission requires EventAssessment and gate"
            )
        if assessment.triage_decision_id != decision.decision_id:
            raise ValueError("Trigger Admission EventAssessment binds another Triage Decision")
        if assessment.cluster_id != cluster_id:
            raise ValueError("Trigger Admission EventAssessment binds another cluster")
        if materiality.assessment_id != assessment.assessment_id:
            raise ValueError("Trigger Admission Materiality Gate binds another assessment")
        if (
            materiality.registration_id != registration.registration_id
            or materiality.checkpoint_key != candidate_set.checkpoint_key
        ):
            raise ValueError("Trigger Admission Materiality Gate binds another checkpoint")
        if materiality != evaluate_event_materiality(
            registration=registration,
            checkpoint_key=candidate_set.checkpoint_key,
            assessment=assessment,
            evaluated_at=materiality.evaluated_at,
        ):
            raise ValueError("Trigger Admission Materiality Gate was not deterministically derived")
        if materiality.disposition is not MaterialityDisposition.ADMIT:
            raise ValueError("Trigger Admission requires an admitted Materiality Gate")
        if admitted_at < materiality.evaluated_at:
            raise ValueError("Trigger Admission cannot predate the Materiality Gate")
        availability = {
            item.version_id: item.first_available_at for item in candidate_set.observations
        }
        routed = tuple(
            item
            for item in proposal.clusters
            if item.cluster_id in decision.event_assessment_cluster_ids
        )
        ordered_routed = tuple(
            sorted(
                routed,
                key=lambda item: (
                    max(
                        availability[version_id]
                        for version_id in (
                            *item.candidate_version_ids,
                            *item.evidence_version_ids,
                        )
                    ),
                    item.cluster_id,
                ),
            )
        )
        selected_index = next(
            index for index, item in enumerate(ordered_routed) if item.cluster_id == cluster_id
        )
        expected_preceding_ids = tuple(item.cluster_id for item in ordered_routed[:selected_index])
        actual_preceding_ids = tuple(
            prior_assessment.cluster_id
            for prior_assessment, _ in preceding_materiality_contexts
            if prior_assessment.triage_decision_id == decision.decision_id
        )
        if actual_preceding_ids != expected_preceding_ids:
            raise ValueError(
                "material Trigger Admission requires every earlier EventAssessment result"
            )
        for prior_assessment, prior_materiality in preceding_materiality_contexts:
            expected_prior_materiality = evaluate_event_materiality(
                registration=registration,
                checkpoint_key=candidate_set.checkpoint_key,
                assessment=prior_assessment,
                evaluated_at=prior_materiality.evaluated_at,
            )
            if (
                prior_materiality.assessment_id != prior_assessment.assessment_id
                or prior_materiality.registration_id != registration.registration_id
                or prior_materiality.checkpoint_key != candidate_set.checkpoint_key
                or prior_materiality.disposition is MaterialityDisposition.ADMIT
                or prior_materiality.evaluated_at > materiality.evaluated_at
                or prior_materiality != expected_prior_materiality
            ):
                raise ValueError("earlier materiality context is not a rejected prior candidate")
        kind = TriggerAdmissionKind.MATERIAL_EVENT
        assessment_id = assessment.assessment_id
        materiality_id = materiality.result_id
        preceding_materiality_ids = tuple(
            prior_materiality.result_id for _, prior_materiality in preceding_materiality_contexts
        )
        targets = materiality.admitted_target_ids
        held = materiality.held_target_ids
    elif selected:
        if assessment is not None or materiality is not None or preceding_materiality_contexts:
            raise ValueError("eligible checkpoint Trigger Admission cannot use Materiality Gate")
        kind = TriggerAdmissionKind.CHECKPOINT_ELIGIBLE
        assessment_id = None
        materiality_id = None
        preceding_materiality_ids = ()
        targets: tuple[str, ...] = ()
        held: tuple[str, ...] = ()
    else:
        raise ValueError(
            "non-material checkpoint Trigger Admission requires its selected eligible cluster"
        )

    versions = tuple(sorted(cluster.candidate_version_ids))
    core = {
        "schema_version": PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA,
        "kind": kind.value,
        "registration_id": candidate_set.registration_id,
        "checkpoint_key": candidate_set.checkpoint_key,
        "candidate_set_id": candidate_set.candidate_set_id,
        "proposal_id": proposal.proposal_id,
        "triage_decision_id": decision.decision_id,
        "cluster_id": cluster_id,
        "observation_version_ids": list(versions),
        "event_assessment_id": assessment_id,
        "materiality_gate_result_id": materiality_id,
        "preceding_materiality_gate_result_ids": list(preceding_materiality_ids),
        "admitted_target_ids": list(targets),
        "held_target_ids": list(held),
        "admitted_at": _timestamp(admitted_at),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return ProspectiveTriggerAdmission(
        admission_id=f"prospective-trigger-admission-{canonical_hash(core)}",
        kind=kind,
        registration_id=candidate_set.registration_id,
        checkpoint_key=candidate_set.checkpoint_key,
        candidate_set_id=candidate_set.candidate_set_id,
        proposal_id=proposal.proposal_id,
        triage_decision_id=decision.decision_id,
        cluster_id=cluster_id,
        observation_version_ids=versions,
        event_assessment_id=assessment_id,
        materiality_gate_result_id=materiality_id,
        preceding_materiality_gate_result_ids=preceding_materiality_ids,
        admitted_target_ids=targets,
        held_target_ids=held,
        admitted_at=admitted_at,
    )


def prospective_position_snapshot_from_dict(value: object) -> ProspectivePositionSnapshot:
    payload = _object(value, "prospective Position Snapshot")
    _exact_keys(
        payload,
        {"schema_version", "snapshot_id", "as_of", "holdings"},
        "prospective Position Snapshot",
    )
    return ProspectivePositionSnapshot(
        snapshot_id=_string(payload, "snapshot_id"),
        as_of=_datetime(payload.get("as_of"), "prospective Position Snapshot as_of"),
        holdings=tuple(
            _position_holding_from_dict(item)
            for item in _list(payload.get("holdings"), "prospective Position Snapshot holdings")
        ),
        schema_version=_string(payload, "schema_version"),
    )


def prospective_historical_analogy_pack_from_dict(
    value: object,
) -> ProspectiveHistoricalAnalogyPack:
    payload = _object(value, "prospective Historical Analogy Pack")
    _exact_keys(
        payload,
        {"schema_version", "pack_id", "cases", "built_at"},
        "prospective Historical Analogy Pack",
    )
    return ProspectiveHistoricalAnalogyPack(
        pack_id=_string(payload, "pack_id"),
        cases=tuple(
            _historical_analogy_case_from_dict(item)
            for item in _list(
                payload.get("cases"),
                "prospective Historical Analogy Pack cases",
            )
        ),
        built_at=_datetime(
            payload.get("built_at"),
            "prospective Historical Analogy Pack built_at",
        ),
        schema_version=_string(payload, "schema_version"),
    )


def prospective_event_assessment_from_dict(
    value: object,
) -> ProspectiveEventAssessmentArtifact:
    payload = _object(value, "prospective EventAssessment")
    _exact_keys(
        payload,
        {
            "schema_version",
            "assessment_id",
            "triage_decision_id",
            "cluster_id",
            "event_assessment_artifact_hash",
            "paths",
            "counterevidence",
            "invalidation_conditions",
            "assessed_at",
            "position_snapshot",
            "historical_analogy_pack",
            "historical_pit_claim",
            "signal_or_execution_capability",
        },
        "prospective EventAssessment",
    )
    position_value = payload.get("position_snapshot")
    analogy_value = payload.get("historical_analogy_pack")
    return ProspectiveEventAssessmentArtifact(
        assessment_id=_string(payload, "assessment_id"),
        triage_decision_id=_string(payload, "triage_decision_id"),
        cluster_id=_string(payload, "cluster_id"),
        event_assessment_artifact_hash=_string(
            payload,
            "event_assessment_artifact_hash",
        ),
        paths=tuple(
            _transmission_path_from_dict(item)
            for item in _list(payload.get("paths"), "prospective EventAssessment paths")
        ),
        counterevidence=_string_tuple(
            payload.get("counterevidence"),
            "prospective EventAssessment counterevidence",
        ),
        invalidation_conditions=_string_tuple(
            payload.get("invalidation_conditions"),
            "prospective EventAssessment invalidation_conditions",
        ),
        assessed_at=_datetime(
            payload.get("assessed_at"),
            "prospective EventAssessment assessed_at",
        ),
        position_snapshot=(
            None
            if position_value is None
            else prospective_position_snapshot_from_dict(position_value)
        ),
        historical_analogy_pack=(
            None
            if analogy_value is None
            else prospective_historical_analogy_pack_from_dict(analogy_value)
        ),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        signal_or_execution_capability=_boolean(
            payload,
            "signal_or_execution_capability",
        ),
        schema_version=_string(payload, "schema_version"),
    )


def prospective_materiality_gate_result_from_dict(
    value: object,
) -> ProspectiveMaterialityGateResult:
    payload = _object(value, "prospective Materiality Gate result")
    _exact_keys(
        payload,
        {
            "schema_version",
            "result_id",
            "registration_id",
            "checkpoint_key",
            "assessment_id",
            "registered_target_venues",
            "registered_instrument_classes",
            "registered_horizon_sessions",
            "disposition",
            "admitted_target_ids",
            "held_target_ids",
            "blocking_gaps",
            "nonblocking_information_gaps",
            "evaluated_at",
            "judgment_model_calls_authorized",
            "execution_capability",
        },
        "prospective Materiality Gate result",
    )
    return ProspectiveMaterialityGateResult(
        result_id=_string(payload, "result_id"),
        registration_id=_string(payload, "registration_id"),
        checkpoint_key=_string(payload, "checkpoint_key"),
        assessment_id=_string(payload, "assessment_id"),
        registered_target_venues=_string_tuple(
            payload.get("registered_target_venues"),
            "prospective Materiality Gate registered_target_venues",
        ),
        registered_instrument_classes=_string_tuple(
            payload.get("registered_instrument_classes"),
            "prospective Materiality Gate registered_instrument_classes",
        ),
        registered_horizon_sessions=tuple(
            _integer(item, "prospective Materiality Gate registered_horizon_sessions item")
            for item in _list(
                payload.get("registered_horizon_sessions"),
                "prospective Materiality Gate registered_horizon_sessions",
            )
        ),
        disposition=MaterialityDisposition(_string(payload, "disposition")),
        admitted_target_ids=_string_tuple(
            payload.get("admitted_target_ids"),
            "prospective Materiality Gate admitted_target_ids",
        ),
        held_target_ids=_string_tuple(
            payload.get("held_target_ids"),
            "prospective Materiality Gate held_target_ids",
        ),
        blocking_gaps=_string_tuple(
            payload.get("blocking_gaps"),
            "prospective Materiality Gate blocking_gaps",
        ),
        nonblocking_information_gaps=_string_tuple(
            payload.get("nonblocking_information_gaps"),
            "prospective Materiality Gate nonblocking_information_gaps",
        ),
        evaluated_at=_datetime(
            payload.get("evaluated_at"),
            "prospective Materiality Gate evaluated_at",
        ),
        judgment_model_calls_authorized=_boolean(
            payload,
            "judgment_model_calls_authorized",
        ),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )


def prospective_trigger_admission_from_dict(value: object) -> ProspectiveTriggerAdmission:
    payload = _object(value, "prospective Trigger Admission")
    _exact_keys(
        payload,
        {
            "schema_version",
            "admission_id",
            "kind",
            "registration_id",
            "checkpoint_key",
            "candidate_set_id",
            "proposal_id",
            "triage_decision_id",
            "cluster_id",
            "observation_version_ids",
            "event_assessment_id",
            "materiality_gate_result_id",
            "preceding_materiality_gate_result_ids",
            "admitted_target_ids",
            "held_target_ids",
            "admitted_at",
            "historical_pit_claim",
            "judgment_model_calls_authorized",
            "execution_capability",
        },
        "prospective Trigger Admission",
    )
    return ProspectiveTriggerAdmission(
        admission_id=_string(payload, "admission_id"),
        kind=TriggerAdmissionKind(_string(payload, "kind")),
        registration_id=_string(payload, "registration_id"),
        checkpoint_key=_string(payload, "checkpoint_key"),
        candidate_set_id=_string(payload, "candidate_set_id"),
        proposal_id=_string(payload, "proposal_id"),
        triage_decision_id=_string(payload, "triage_decision_id"),
        cluster_id=_string(payload, "cluster_id"),
        observation_version_ids=_string_tuple(
            payload.get("observation_version_ids"),
            "prospective Trigger Admission observation_version_ids",
        ),
        event_assessment_id=_optional_string(payload, "event_assessment_id"),
        materiality_gate_result_id=_optional_string(
            payload,
            "materiality_gate_result_id",
        ),
        preceding_materiality_gate_result_ids=_string_tuple(
            payload.get("preceding_materiality_gate_result_ids"),
            "prospective Trigger Admission preceding_materiality_gate_result_ids",
        ),
        admitted_target_ids=_string_tuple(
            payload.get("admitted_target_ids"),
            "prospective Trigger Admission admitted_target_ids",
        ),
        held_target_ids=_string_tuple(
            payload.get("held_target_ids"),
            "prospective Trigger Admission held_target_ids",
        ),
        admitted_at=_datetime(
            payload.get("admitted_at"),
            "prospective Trigger Admission admitted_at",
        ),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        judgment_model_calls_authorized=_boolean(
            payload,
            "judgment_model_calls_authorized",
        ),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )


def _position_holding_from_dict(value: object) -> PositionHolding:
    payload = _object(value, "prospective Position holding")
    _exact_keys(
        payload,
        {"target_id", "venue", "instrument_class"},
        "prospective Position holding",
    )
    return PositionHolding(
        target_id=_string(payload, "target_id"),
        venue=_string(payload, "venue"),
        instrument_class=_string(payload, "instrument_class"),
    )


def _historical_analogy_case_from_dict(value: object) -> HistoricalAnalogyCase:
    payload = _object(value, "prospective historical analogy case")
    _exact_keys(
        payload,
        {"case_ref", "mode", "artifact_hash", "similarity_basis", "counterevidence"},
        "prospective historical analogy case",
    )
    return HistoricalAnalogyCase(
        case_ref=_string(payload, "case_ref"),
        mode=HistoricalAnalogyMode(_string(payload, "mode")),
        artifact_hash=_string(payload, "artifact_hash"),
        similarity_basis=_string(payload, "similarity_basis"),
        counterevidence=_string_tuple(
            payload.get("counterevidence"),
            "prospective historical analogy counterevidence",
        ),
    )


def _transmission_path_from_dict(value: object) -> TransmissionPath:
    payload = _object(value, "prospective transmission path")
    _exact_keys(
        payload,
        {
            "target_id",
            "venue",
            "instrument_class",
            "channels",
            "causal_steps",
            "evidence_version_ids",
            "horizon_sessions",
        },
        "prospective transmission path",
    )
    horizon = payload.get("horizon_sessions")
    if not isinstance(horizon, int) or isinstance(horizon, bool):
        raise TypeError("prospective transmission path horizon_sessions must be an integer")
    return TransmissionPath(
        target_id=_string(payload, "target_id"),
        venue=_string(payload, "venue"),
        instrument_class=_string(payload, "instrument_class"),
        channels=tuple(
            TransmissionChannel(item)
            for item in _string_tuple(
                payload.get("channels"),
                "prospective transmission path channels",
            )
        ),
        causal_steps=_string_tuple(
            payload.get("causal_steps"),
            "prospective transmission path causal_steps",
        ),
        evidence_version_ids=_string_tuple(
            payload.get("evidence_version_ids"),
            "prospective transmission path evidence_version_ids",
        ),
        horizon_sessions=horizon,
    )


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _ordered_nonempty(values: tuple[str, ...], name: str) -> None:
    if not values:
        raise ValueError(f"{name} must be non-empty")
    for value in values:
        _trimmed(value, name)


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")
    for value in values:
        _trimmed(value, name)


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase sha256 hex digest")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} has an invalid prefix")
    _sha256(value.removeprefix(prefix), name)


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "prospective trigger timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    result: dict[str, object] = {}
    for key, item in raw.items():
        if not isinstance(key, str):
            raise TypeError(f"{name} field names must be strings")
        result[key] = item
    return result


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a list")
    return cast(list[object], value)


def _exact_keys(payload: dict[str, object], expected: set[str], name: str) -> None:
    if set(payload) != expected:
        raise ValueError(f"{name} fields do not match the contract")


def _string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a string")
    return value


def _optional_string(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{key} must be a string or null")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    values = _list(value, name)
    if any(not isinstance(item, str) for item in values):
        raise TypeError(f"{name} must contain strings")
    return tuple(cast(list[str], values))


def _boolean(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a boolean")
    return value


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an RFC3339 timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _strict_utc(parsed, name)
    return parsed
