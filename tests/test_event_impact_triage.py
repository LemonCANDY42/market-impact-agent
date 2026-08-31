from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
    sha256_bytes,
)
from market_impact_agent.event_impact_triage import (
    EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V2,
    EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
    CheckpointEligibility,
    CompletedTriageRunAuthority,
    CompletedTriageWorkRunAuthority,
    EventImpactTriageBatchSelection,
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    LegacyTriageWorkDecisionEvidence,
    TriageAgentRole,
    TriageClusterProposal,
    TriageDecisionStatus,
    TriageRoute,
    TriageRunEvidence,
    TriageRunMemberEvidence,
    TriageWorkDecisionEvidence,
    admit_event_impact_triage,
    admit_event_impact_triage_work,
    event_impact_triage_batch_selection_from_dict,
    event_impact_triage_candidate_set_from_dict,
    event_impact_triage_decision_from_dict,
    freeze_event_impact_triage_candidate_set,
)
from market_impact_agent.event_impact_triage_store import (
    EventImpactTriageDecisionStore,
)
from market_impact_agent.event_impact_triage_work_evaluation import (
    EventImpactTriageWorkComparisonRegistration,
    EventImpactTriageWorkComparisonReport,
    EventImpactTriageWorkComparisonStore,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_checkpoint_readiness import (
    CheckpointReadinessStatus,
    ProspectiveCheckpointAdmissionStore,
    ProspectiveCheckpointReadiness,
    ProspectiveCheckpointReadinessReport,
    ProspectiveCheckpointRouteAdmission,
    ProspectiveCheckpointRoutePlan,
)
from market_impact_agent.prospective_data import (
    ProspectiveDataJournal,
    ProspectiveObservationVersionRef,
    prospective_observation_version_id,
)
from market_impact_agent.research import (
    EventArchetype,
    EventStage,
    TransmissionChannel,
)

ADMITTED_AT = datetime(2026, 8, 30, 0, 0, tzinfo=UTC)
FROZEN_AT = ADMITTED_AT + timedelta(minutes=40)
DECIDED_AT = ADMITTED_AT + timedelta(minutes=41)
HASH = "1" * 64


@dataclass(frozen=True)
class StubSelectionJournal:
    refs: tuple[ProspectiveObservationVersionRef, ...]

    def observation_version_refs_by_ids(
        self,
        version_ids: tuple[str, ...],
    ) -> tuple[ProspectiveObservationVersionRef, ...]:
        if set(version_ids) != {item.version_id for item in self.refs}:
            raise KeyError("unexpected selection")
        return self.refs


@dataclass
class RecordingRunAuthority(CompletedTriageRunAuthority):
    expected_candidate_set_id: str
    expected_proposal_id: str
    called: bool = False

    def assert_authoritative_completed_triage_run(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageRunEvidence,
    ) -> None:
        assert candidate_set.candidate_set_id == self.expected_candidate_set_id
        assert proposal.proposal_id == self.expected_proposal_id
        coordinator = next(
            item for item in run_evidence.members if item.role is TriageAgentRole.COORDINATOR
        )
        assert coordinator.run_id == "triage-run-1"
        self.called = True


@dataclass
class RecordingWorkRunAuthority(CompletedTriageWorkRunAuthority):
    expected_candidate_set_id: str
    expected_proposal_id: str
    expected_evidence: TriageWorkDecisionEvidence
    called: bool = False

    def assert_authoritative_completed_triage_work_run(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageWorkDecisionEvidence,
    ) -> None:
        assert candidate_set.candidate_set_id == self.expected_candidate_set_id
        assert proposal.proposal_id == self.expected_proposal_id
        assert run_evidence == self.expected_evidence
        self.called = True


@dataclass(frozen=True)
class StubFailedComparison:
    comparison_id: str
    candidate_set_id: str

    def to_dict(self) -> dict[str, object]:
        return {
            "comparison_id": self.comparison_id,
            "candidate_set_id": self.candidate_set_id,
        }


@dataclass(frozen=True)
class StubFailedComparisonReport:
    report_id: str
    comparison_id: str
    batch_gate_passed: bool
    blockers: tuple[str, ...]
    evaluated_at: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "report_id": self.report_id,
            "comparison_id": self.comparison_id,
            "batch_gate_passed": self.batch_gate_passed,
            "blockers": list(self.blockers),
            "evaluated_at": self.evaluated_at.isoformat().replace("+00:00", "Z"),
        }


class LegacySeedTriageDecisionStore(EventImpactTriageDecisionStore):
    def persist_legacy(
        self,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        decision: EventImpactTriageDecision,
    ) -> EventImpactTriageDecision:
        return self._persist_decision(candidate_set, proposal, decision)


def _observation(index: int, *, available_at: datetime) -> SourceObservation:
    body = f"licensed-news-body-{index}".encode()
    return SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id=f"news-provider-{index}",
        provider_version="2026-08",
        upstream_source=f"publisher-{index}",
        upstream_record_id=f"article-{index}",
        source_ref=f"https://publisher-{index}.example/article-{index}",
        lineage_id=f"article-{index}",
        times=ObservationTimes(
            occurred_at=available_at,
            published_at=available_at,
            available_at=available_at,
            source_updated_at=available_at,
            aggregator_fetched_at=None,
            retrieved_at=available_at,
            occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        ),
        authority_at=available_at,
        authority_kind="actual_receipt",
        raw_content_hash=sha256_bytes(body),
        normalized_payload={
            "headline": f"Event {index}",
            "publisher": f"Publisher {index}",
        },
        license_scope="private_research",
    )


def _snapshot(store: LocalDataSnapshotStore) -> DataSnapshot:
    observations = tuple(
        _observation(index, available_at=ADMITTED_AT + timedelta(minutes=index * 10))
        for index in (1, 2, 3)
    )
    sources = tuple(
        DataSourceBinding(
            provider_id=item.provider_id,
            provider_version=item.provider_version,
            upstream_source=item.upstream_source,
            manifest_hash=canonical_hash({"provider": item.provider_id}),
            source_config_hash=canonical_hash({"source": item.upstream_source}),
            required=True,
        )
        for item in observations
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=observations[-1].times.retrieved_at,
        window_start=ADMITTED_AT,
        source_policy_id="triage-fixture-policy",
        parameters={"window": "post-admission"},
        sources=sources,
        minimum_data_sources=3,
    )
    attempts = tuple(
        DataProviderAttempt(
            provider_id=item.provider_id,
            provider_version=item.provider_version,
            upstream_source=item.upstream_source,
            required=True,
            status=DataFetchStatus.DATA,
            retrieved_at=item.times.retrieved_at,
            raw_response_hash=sha256_bytes(f"response-{index}".encode()),
            received_count=1,
            accepted_count=1,
            rejected_missing_availability=0,
            rejected_after_cutoff=0,
            rejected_missing_authority=0,
            rejected_authority_after_cutoff=0,
            rejected_lane_mismatch=0,
            error_kind=None,
        )
        for index, item in enumerate(observations, start=1)
    )
    snapshot_core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [item.to_dict() for item in attempts],
        "observations": [item.to_dict() for item in observations],
        "coverage_complete": True,
        "completed_at": observations[-1].times.retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(snapshot_core)}",
        query=query,
        attempts=attempts,
        observations=observations,
        coverage_complete=True,
        completed_at=observations[-1].times.retrieved_at,
    )
    store.put(snapshot)
    return snapshot


def _readiness(snapshot: DataSnapshot) -> ProspectiveCheckpointReadinessReport:
    version_ids = tuple(
        sorted(prospective_observation_version_id(x) for x in snapshot.observations)
    )
    candidate = ProspectiveCheckpointReadiness(
        checkpoint_key="next-a-share-policy-event",
        status=CheckpointReadinessStatus.UNCLASSIFIED_TRIGGER_CANDIDATE_OBSERVED,
        operational_trigger_route_job_ids=("prospective-collection-job-" + "2" * 64,),
        trigger_candidate_version_ids=version_ids,
        latest_trigger_available_at=max(item.times.retrieved_at for item in snapshot.observations),
        blocking_gaps=("event_revelation:trigger_candidate_requires_eligibility_selection",),
        information_gaps=(),
    )
    waiting = ProspectiveCheckpointReadiness(
        checkpoint_key="next-nbs-cpi-ppi-release",
        status=CheckpointReadinessStatus.TRIGGER_ROUTE_UNCONFIGURED,
        operational_trigger_route_job_ids=(),
        trigger_candidate_version_ids=(),
        latest_trigger_available_at=None,
        blocking_gaps=("event_revelation:trigger_route_unconfigured",),
        information_gaps=(),
    )
    checkpoints = tuple(sorted((candidate, waiting), key=lambda item: item.checkpoint_key))
    evaluated_at = ADMITTED_AT + timedelta(minutes=35)
    registration_id = "prospective-diagnostic-registration-" + "5" * 64
    route_plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=registration_id,
        bindings=(),
    )
    admission_core = {
        "route_plan_id": route_plan.plan_id,
        "registration_id": registration_id,
        "recorded_at": ADMITTED_AT.isoformat().replace("+00:00", "Z"),
    }
    route_admission = ProspectiveCheckpointRouteAdmission(
        admission_id="prospective-checkpoint-route-admission-" + canonical_hash(admission_core),
        route_plan_id=route_plan.plan_id,
        registration_id=registration_id,
        recorded_at=ADMITTED_AT,
    )
    core = {
        "schema_version": "market-impact.prospective-checkpoint-readiness-report.v1",
        "route_plan_id": route_plan.plan_id,
        "route_admission_id": route_admission.admission_id,
        "registration_id": registration_id,
        "admitted_at": ADMITTED_AT.isoformat().replace("+00:00", "Z"),
        "evaluated_at": evaluated_at.isoformat().replace("+00:00", "Z"),
        "checkpoints": [item.to_dict() for item in checkpoints],
        "operational_checkpoint_count": 1,
        "candidate_checkpoint_count": 1,
        "waiting_for_external_event": False,
        "model_calls_authorized": False,
        "historical_pit_claim": False,
        "execution_capability": False,
    }
    return ProspectiveCheckpointReadinessReport(
        report_id=f"prospective-checkpoint-readiness-report-{canonical_hash(core)}",
        route_plan_id=route_plan.plan_id,
        route_admission_id=route_admission.admission_id,
        registration_id=registration_id,
        admitted_at=ADMITTED_AT,
        evaluated_at=evaluated_at,
        checkpoints=checkpoints,
        operational_checkpoint_count=1,
        candidate_checkpoint_count=1,
        waiting_for_external_event=False,
    )


def _candidate_set(tmp_path: Path) -> EventImpactTriageCandidateSet:
    store = LocalDataSnapshotStore(tmp_path / "state")
    snapshot = _snapshot(store)
    readiness = _readiness(snapshot)
    return freeze_event_impact_triage_candidate_set(
        readiness_report=readiness,
        checkpoint_key="next-a-share-policy-event",
        snapshot=snapshot,
        snapshot_store=store,
        admission_store=_authorize_readiness(tmp_path / "state", readiness),
        frozen_at=FROZEN_AT,
    )


def test_caller_consistent_failed_comparison_stubs_cannot_terminalize_versions(
    tmp_path: Path,
) -> None:
    candidate_set = _candidate_set(tmp_path)
    store = EventImpactTriageDecisionStore(tmp_path / "terminal-state")
    comparison = StubFailedComparison(
        comparison_id="event-impact-triage-work-comparison-" + "a" * 64,
        candidate_set_id=candidate_set.candidate_set_id,
    )
    report = StubFailedComparisonReport(
        report_id="event-impact-triage-work-comparison-report-" + "b" * 64,
        comparison_id=comparison.comparison_id,
        batch_gate_passed=False,
        blockers=("treatment_missed_must_catch_eligible",),
        evaluated_at=DECIDED_AT,
    )

    authority = EventImpactTriageWorkComparisonStore(tmp_path / "comparison.sqlite3")
    with pytest.raises((TypeError, ValueError), match=r"durable|registered"):
        store.terminalize_failed_work_comparison(
            candidate_set=candidate_set,
            comparison=cast(EventImpactTriageWorkComparisonRegistration, comparison),
            report=cast(EventImpactTriageWorkComparisonReport, report),
            label_set=cast(Any, object()),
            work_manifest=cast(Any, object()),
            baseline=cast(Any, object()),
            treatment=cast(Any, object()),
            baseline_authority=cast(Any, object()),
            treatment_authority=cast(Any, object()),
            comparison_authority=authority,
            terminalized_at=DECIDED_AT + timedelta(seconds=1),
        )

    assert (
        store.classified_version_ids(
            registration_id=candidate_set.registration_id,
            checkpoint_key=candidate_set.checkpoint_key,
            route_plan_id=candidate_set.route_plan_id,
            route_admission_id=candidate_set.route_admission_id,
            at=DECIDED_AT + timedelta(seconds=1),
        )
        == ()
    )


def test_receipt_ordered_batch_selection_freezes_prefix_only(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    full_snapshot = _snapshot(store)
    readiness = _readiness(full_snapshot)
    refs = tuple(
        ProspectiveObservationVersionRef(
            version_id=prospective_observation_version_id(item),
            first_available_at=cast(datetime, item.times.available_at),
            provider_id=item.provider_id,
            provider_version=item.provider_version,
            upstream_source=item.upstream_source,
        )
        for item in full_snapshot.observations
    )
    stub = StubSelectionJournal(refs)
    journal = cast(ProspectiveDataJournal, stub)
    selection = EventImpactTriageBatchSelection.build(
        readiness_report=readiness,
        checkpoint_key="next-a-share-policy-event",
        journal=journal,
        selected_at=readiness.evaluated_at,
        maximum_candidate_count=2,
    )
    selected_observations = full_snapshot.observations[:2]
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=selected_observations[-1].times.retrieved_at,
        window_start=ADMITTED_AT,
        source_policy_id=selection.selection_id,
        parameters={
            "selection_id": selection.selection_id,
            "readiness_report_id": readiness.report_id,
        },
        sources=full_snapshot.query.sources[:2],
        minimum_data_sources=2,
    )
    attempts = full_snapshot.attempts[:2]
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [item.to_dict() for item in attempts],
        "observations": [item.to_dict() for item in selected_observations],
        "coverage_complete": True,
        "completed_at": attempts[-1].retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    selected_snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=attempts,
        observations=selected_observations,
        coverage_complete=True,
        completed_at=attempts[-1].retrieved_at,
    )
    store.put(selected_snapshot)

    candidate_set = freeze_event_impact_triage_candidate_set(
        readiness_report=readiness,
        checkpoint_key="next-a-share-policy-event",
        snapshot=selected_snapshot,
        snapshot_store=store,
        admission_store=_authorize_readiness(tmp_path / "state", readiness),
        frozen_at=FROZEN_AT,
        batch_selection=selection,
        selection_journal=journal,
    )

    assert candidate_set.version_ids == selection.selected_version_ids
    assert len(candidate_set.observations) == 2
    assert event_impact_triage_batch_selection_from_dict(selection.to_dict()) == selection
    assert (
        validate_agent_contract(
            selection.to_dict(), "event-impact-triage-batch-selection.schema.json"
        )
        == ()
    )


def _authorize_readiness(
    state_root: Path,
    readiness: ProspectiveCheckpointReadinessReport,
) -> ProspectiveCheckpointAdmissionStore:
    authority = ProspectiveCheckpointAdmissionStore(state_root)
    route_plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=readiness.registration_id,
        bindings=(),
    )
    admission_core = {
        "route_plan_id": route_plan.plan_id,
        "registration_id": readiness.registration_id,
        "recorded_at": ADMITTED_AT.isoformat().replace("+00:00", "Z"),
    }
    admission = ProspectiveCheckpointRouteAdmission(
        admission_id="prospective-checkpoint-route-admission-" + canonical_hash(admission_core),
        route_plan_id=route_plan.plan_id,
        registration_id=readiness.registration_id,
        recorded_at=ADMITTED_AT,
    )
    assert route_plan.plan_id == readiness.route_plan_id
    assert admission.admission_id == readiness.route_admission_id
    route_plan_artifact = authority.store.artifacts.put_json(route_plan.to_dict())
    admission_artifact = authority.store.artifacts.put_json(admission.to_dict())
    with sqlite3.connect(authority.index_path) as connection:
        connection.execute(
            """
            INSERT INTO prospective_checkpoint_route_admissions(
                route_plan_id, admission_id, registration_id, artifact_hash, recorded_at,
                route_plan_artifact_hash, superseded_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                readiness.route_plan_id,
                readiness.route_admission_id,
                readiness.registration_id,
                admission_artifact.content_hash,
                ADMITTED_AT.isoformat().replace("+00:00", "Z"),
                route_plan_artifact.content_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO prospective_checkpoint_route_heads(
                registration_id, route_plan_id, admission_id, effective_from,
                route_plan_artifact_hash
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                readiness.registration_id,
                readiness.route_plan_id,
                readiness.route_admission_id,
                ADMITTED_AT.isoformat().replace("+00:00", "Z"),
                route_plan_artifact.content_hash,
            ),
        )
    return authority


def _run_evidence() -> TriageRunEvidence:
    return TriageRunEvidence(
        members=(
            TriageRunMemberEvidence(
                role=TriageAgentRole.COORDINATOR,
                run_id="triage-run-1",
                terminal_artifact_hash=HASH,
                metrics_hash="2" * 64,
                validation_event_hash="3" * 64,
                execution_binding_hash="4" * 64,
            ),
            TriageRunMemberEvidence(
                role=TriageAgentRole.FACT_VERIFIER,
                run_id="triage-run-1-fact-verifier",
                terminal_artifact_hash="6" * 64,
                metrics_hash="7" * 64,
                validation_event_hash="8" * 64,
                execution_binding_hash="9" * 64,
            ),
        ),
        usage_ledger_hash="5" * 64,
    )


def _work_run_evidence() -> TriageWorkDecisionEvidence:
    return TriageWorkDecisionEvidence(
        plan_id="event-impact-triage-work-execution-plan-" + "a" * 64,
        work_manifest_id="event-impact-triage-work-manifest-" + "b" * 64,
        completed_member_count=13,
        finished_at=DECIDED_AT,
        usage_ledger_hash="c" * 64,
        authority_receipt_hash="d" * 64,
    )


def _legacy_work_decision(
    decision: EventImpactTriageDecision,
    *,
    decided_at: datetime = DECIDED_AT,
    authority_receipt_hash: str | None = None,
) -> EventImpactTriageDecision:
    payload = decision.to_dict()
    payload["schema_version"] = EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V2
    evidence = dict(cast(dict[str, object], payload["run_evidence"]))
    evidence.pop("finished_at")
    if authority_receipt_hash is not None:
        evidence["authority_receipt_hash"] = authority_receipt_hash
    payload["run_evidence"] = evidence
    payload["decided_at"] = decided_at.isoformat().replace("+00:00", "Z")
    core = {key: value for key, value in payload.items() if key != "decision_id"}
    payload["decision_id"] = "event-impact-triage-decision-" + canonical_hash(core)
    return event_impact_triage_decision_from_dict(payload)


def test_triage_selects_first_eligible_without_erasing_other_financial_impact(
    tmp_path: Path,
) -> None:
    candidate_set = _candidate_set(tmp_path)
    first, second, third = candidate_set.version_ids
    industry_event = TriageClusterProposal.build(
        candidate_version_ids=(first,),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.EVENT_ASSESSMENT,
        event_archetypes=(EventArchetype.PHYSICAL_SUPPLY_LOGISTICS,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("A product restriction changed channel access.",),
        rule_reasons=("The fact is not a capital-market policy change.",),
        evidence_version_ids=(first,),
        transmission_channels=(TransmissionChannel.POLICY_ACCESS,),
        affected_entity_refs=("industry:consumer-electronics",),
        triage_confidence=0.76,
    )
    policy_event = TriageClusterProposal.build(
        candidate_version_ids=(second,),
        checkpoint_eligibility=CheckpointEligibility.ELIGIBLE,
        recommended_route=TriageRoute.CHECKPOINT_CANDIDATE,
        event_archetypes=(EventArchetype.POLICY_REGULATORY,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("A capital-market access rule changed.",),
        rule_reasons=("The observation matches the registered policy rule.",),
        evidence_version_ids=(second,),
        transmission_channels=(TransmissionChannel.POLICY_ACCESS,),
        triage_confidence=0.88,
    )
    routine_item = TriageClusterProposal.build(
        candidate_version_ids=(third,),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.ARCHIVE,
        event_archetypes=(),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=(),
        rule_reasons=("The item repeats a routine calendar with no changed rule.",),
        evidence_version_ids=(third,),
        triage_confidence=0.91,
    )
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(industry_event, policy_event, routine_item),
    )
    authority = RecordingRunAuthority(candidate_set.candidate_set_id, proposal.proposal_id)
    decision = admit_event_impact_triage(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=_run_evidence(),
        run_authority=authority,
        decided_at=DECIDED_AT,
    )

    assert authority.called
    assert decision.status is TriageDecisionStatus.ELIGIBLE_SELECTED
    assert decision.selected_cluster_id == policy_event.cluster_id
    assert decision.event_assessment_cluster_ids == (industry_event.cluster_id,)
    assert decision.archive_cluster_ids == (routine_item.cluster_id,)
    assert industry_event.zero_financial_impact_claim is False
    assert (
        validate_agent_contract(
            candidate_set.to_dict(), "event-impact-triage-candidate-set.schema.json"
        )
        == ()
    )
    assert event_impact_triage_candidate_set_from_dict(candidate_set.to_dict()) == candidate_set
    assert (
        validate_agent_contract(proposal.to_dict(), "event-impact-triage-proposal.schema.json")
        == ()
    )
    assert (
        validate_agent_contract(decision.to_dict(), "event-impact-triage-decision.schema.json")
        == ()
    )


def test_work_decision_keeps_native_authority_and_store_reopens_v2(
    tmp_path: Path,
) -> None:
    candidate_set = _candidate_set(tmp_path)
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=candidate_set.version_ids,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("No candidate changes the registered checkpoint rule.",),
                evidence_version_ids=candidate_set.version_ids,
                triage_confidence=0.9,
            ),
        ),
    )
    evidence = _work_run_evidence()
    authority = RecordingWorkRunAuthority(
        candidate_set.candidate_set_id,
        proposal.proposal_id,
        evidence,
    )

    with pytest.raises(ValueError, match="must equal authoritative finished_at"):
        admit_event_impact_triage_work(
            candidate_set=candidate_set,
            proposal=proposal,
            run_evidence=evidence,
            run_authority=authority,
            decided_at=DECIDED_AT + timedelta(seconds=1),
        )
    decision = admit_event_impact_triage_work(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=authority,
        decided_at=DECIDED_AT,
    )

    assert authority.called
    assert decision.schema_version == EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3
    assert decision.run_evidence == evidence
    assert decision.status is TriageDecisionStatus.NO_ELIGIBLE_CANDIDATE
    assert event_impact_triage_decision_from_dict(decision.to_dict()) == decision
    assert not validate_agent_contract(
        decision.to_dict(), "event-impact-triage-decision.schema.json"
    )

    legacy = _legacy_work_decision(decision)
    legacy_bytes = canonical_json_bytes(legacy.to_dict())
    reopened_legacy = event_impact_triage_decision_from_dict(legacy.to_dict())
    assert type(reopened_legacy.run_evidence) is LegacyTriageWorkDecisionEvidence
    assert canonical_json_bytes(reopened_legacy.to_dict()) == legacy_bytes
    assert not validate_agent_contract(legacy.to_dict(), "event-impact-triage-decision.schema.json")

    store = EventImpactTriageDecisionStore(tmp_path / "work-state")
    stored = store.admit_work(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=authority,
        decided_at=DECIDED_AT,
    )
    with pytest.raises(ValueError, match="must equal authoritative finished_at"):
        store.admit_work(
            candidate_set=candidate_set,
            proposal=proposal,
            run_evidence=evidence,
            run_authority=authority,
            decided_at=DECIDED_AT + timedelta(minutes=1),
        )
    reopened = store.admit_work(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=authority,
        decided_at=DECIDED_AT,
    )
    assert stored == decision
    assert reopened == decision
    assert store.classified_version_ids(
        registration_id=candidate_set.registration_id,
        checkpoint_key=candidate_set.checkpoint_key,
        route_plan_id=candidate_set.route_plan_id,
        route_admission_id=candidate_set.route_admission_id,
        at=DECIDED_AT,
    ) == tuple(sorted(candidate_set.version_ids))
    assert store.route_epoch_contexts(
        registration_id=candidate_set.registration_id,
        checkpoint_key=candidate_set.checkpoint_key,
        route_plan_id=candidate_set.route_plan_id,
        route_admission_id=candidate_set.route_admission_id,
        at=DECIDED_AT,
    ) == ((candidate_set, proposal, decision, proposal.clusters[0]),)

    legacy_store = LegacySeedTriageDecisionStore(tmp_path / "legacy-work-state")
    assert legacy_store.persist_legacy(candidate_set, proposal, legacy) == legacy
    authority.called = False
    legacy_retry = legacy_store.admit_work(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=authority,
        decided_at=DECIDED_AT,
    )
    assert legacy_retry == legacy
    assert legacy_retry.schema_version == EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V2
    assert authority.called


@pytest.mark.parametrize(
    "legacy",
    ["evidence", "time"],
)
def test_current_work_retry_rejects_conflicting_legacy_decision(
    tmp_path: Path, legacy: str
) -> None:
    candidate_set = _candidate_set(tmp_path)
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=candidate_set.version_ids,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("No candidate changes the registered checkpoint rule.",),
                evidence_version_ids=candidate_set.version_ids,
                triage_confidence=0.9,
            ),
        ),
    )
    evidence = _work_run_evidence()
    authority = RecordingWorkRunAuthority(
        candidate_set.candidate_set_id,
        proposal.proposal_id,
        evidence,
    )
    current = admit_event_impact_triage_work(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=authority,
        decided_at=DECIDED_AT,
    )
    stored_legacy = _legacy_work_decision(
        current,
        decided_at=(DECIDED_AT + timedelta(seconds=1) if legacy == "time" else DECIDED_AT),
        authority_receipt_hash=("e" * 64 if legacy == "evidence" else None),
    )
    store = LegacySeedTriageDecisionStore(tmp_path / f"legacy-{legacy}")
    store.persist_legacy(candidate_set, proposal, stored_legacy)

    authority.called = False
    with pytest.raises(ValueError, match="conflicts"):
        store.admit_work(
            candidate_set=candidate_set,
            proposal=proposal,
            run_evidence=evidence,
            run_authority=authority,
            decided_at=DECIDED_AT,
        )
    assert authority.called


def test_concurrent_identical_admissions_reopen_the_exact_stored_decision(
    tmp_path: Path,
) -> None:
    candidate_set = _candidate_set(tmp_path)
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=candidate_set.version_ids,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("No candidate changes the registered checkpoint rule.",),
                evidence_version_ids=candidate_set.version_ids,
                triage_confidence=0.9,
            ),
        ),
    )
    evidence = _run_evidence()
    barrier = Barrier(2)

    @dataclass
    class ConcurrentAuthority(CompletedTriageRunAuthority):
        def assert_authoritative_completed_triage_run(
            self,
            *,
            candidate_set: EventImpactTriageCandidateSet,
            proposal: EventImpactTriageProposal,
            run_evidence: TriageRunEvidence,
        ) -> None:
            barrier.wait()

    store = EventImpactTriageDecisionStore(tmp_path / "concurrent-identical")

    def admit() -> EventImpactTriageDecision:
        return store.admit(
            candidate_set=candidate_set,
            proposal=proposal,
            run_evidence=evidence,
            run_authority=ConcurrentAuthority(),
            decided_at=DECIDED_AT,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(admit), executor.submit(admit))
        decisions = tuple(future.result() for future in futures)

    assert decisions[0] == decisions[1]
    assert decisions[0] is not decisions[1]
    assert decisions[0] == event_impact_triage_decision_from_dict(decisions[0].to_dict())


def test_concurrent_overlapping_candidate_sets_still_fail_closed(tmp_path: Path) -> None:
    first_candidate = _candidate_set(tmp_path)
    second_payload = first_candidate.to_dict()
    second_payload["frozen_at"] = (
        (FROZEN_AT + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    )
    second_core = {key: value for key, value in second_payload.items() if key != "candidate_set_id"}
    second_payload["candidate_set_id"] = "event-impact-triage-candidate-set-" + canonical_hash(
        second_core
    )
    second_candidate = event_impact_triage_candidate_set_from_dict(second_payload)

    def proposal_for(candidate_set: EventImpactTriageCandidateSet) -> EventImpactTriageProposal:
        return EventImpactTriageProposal.build(
            candidate_set=candidate_set,
            clusters=(
                TriageClusterProposal.build(
                    candidate_version_ids=candidate_set.version_ids,
                    checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                    recommended_route=TriageRoute.ARCHIVE,
                    event_archetypes=(),
                    event_stage=EventStage.FIRST_OBSERVED,
                    changed_facts=(),
                    rule_reasons=("No candidate changes the registered checkpoint rule.",),
                    evidence_version_ids=candidate_set.version_ids,
                    triage_confidence=0.9,
                ),
            ),
        )

    barrier = Barrier(2)

    @dataclass
    class ConcurrentAuthority(CompletedTriageRunAuthority):
        def assert_authoritative_completed_triage_run(
            self,
            *,
            candidate_set: EventImpactTriageCandidateSet,
            proposal: EventImpactTriageProposal,
            run_evidence: TriageRunEvidence,
        ) -> None:
            barrier.wait()

    store = EventImpactTriageDecisionStore(tmp_path / "concurrent-conflict")

    def admit(candidate_set: EventImpactTriageCandidateSet) -> EventImpactTriageDecision:
        return store.admit(
            candidate_set=candidate_set,
            proposal=proposal_for(candidate_set),
            run_evidence=_run_evidence(),
            run_authority=ConcurrentAuthority(),
            decided_at=DECIDED_AT,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(admit, candidate) for candidate in (first_candidate, second_candidate)
        )
        outcomes: list[EventImpactTriageDecision | Exception] = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except ValueError as error:
                outcomes.append(error)

    assert sum(isinstance(item, EventImpactTriageDecision) for item in outcomes) == 1
    conflicts = [item for item in outcomes if isinstance(item, ValueError)]
    assert len(conflicts) == 1
    assert "already classified by another Decision" in str(conflicts[0])


def test_earlier_needs_review_blocks_later_checkpoint_selection(tmp_path: Path) -> None:
    candidate_set = _candidate_set(tmp_path)
    first, second, third = candidate_set.version_ids
    review = TriageClusterProposal.build(
        candidate_version_ids=(first,),
        checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
        recommended_route=TriageRoute.ATTENTION_WATCH,
        event_archetypes=(EventArchetype.ISSUER_CORPORATE,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("An issuer event may be escalating.",),
        rule_reasons=("The current item does not prove whether the policy rule changed.",),
        evidence_version_ids=(first,),
        uncertainty_notes=("Primary-source confirmation is missing.",),
        watch_questions=("Did an authority or exchange publish a binding follow-up?",),
        triage_confidence=0.52,
    )
    eligible = TriageClusterProposal.build(
        candidate_version_ids=(second,),
        checkpoint_eligibility=CheckpointEligibility.ELIGIBLE,
        recommended_route=TriageRoute.CHECKPOINT_CANDIDATE,
        event_archetypes=(EventArchetype.POLICY_REGULATORY,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("A capital-market rule changed.",),
        rule_reasons=("The registered policy rule appears to match.",),
        evidence_version_ids=(second,),
        transmission_channels=(TransmissionChannel.POLICY_ACCESS,),
        triage_confidence=0.81,
    )
    archive = TriageClusterProposal.build(
        candidate_version_ids=(third,),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.ARCHIVE,
        event_archetypes=(),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=(),
        rule_reasons=("The item is a duplicate summary.",),
        evidence_version_ids=(third,),
        triage_confidence=0.9,
    )
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(review, eligible, archive),
    )
    decision = admit_event_impact_triage(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=_run_evidence(),
        run_authority=RecordingRunAuthority(candidate_set.candidate_set_id, proposal.proposal_id),
        decided_at=DECIDED_AT,
    )

    assert decision.status is TriageDecisionStatus.NEEDS_REVIEW
    assert decision.selected_cluster_id is None
    assert decision.blocking_review_cluster_ids == (review.cluster_id,)
    assert decision.attention_watch_cluster_ids == (review.cluster_id,)
    assert decision.unselected_eligible_cluster_ids == (eligible.cluster_id,)


def test_decision_store_reopens_authority_and_classifies_each_version_once(
    tmp_path: Path,
) -> None:
    candidate_set = _candidate_set(tmp_path)
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=candidate_set.version_ids,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("No candidate changes the registered checkpoint rule.",),
                evidence_version_ids=candidate_set.version_ids,
                triage_confidence=0.9,
            ),
        ),
    )
    authority = RecordingRunAuthority(candidate_set.candidate_set_id, proposal.proposal_id)
    store = EventImpactTriageDecisionStore(tmp_path / "state")

    decision = store.admit(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=_run_evidence(),
        run_authority=authority,
        decided_at=DECIDED_AT,
    )
    reopened = store.admit(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=_run_evidence(),
        run_authority=authority,
        decided_at=DECIDED_AT + timedelta(minutes=1),
    )

    assert reopened == decision
    assert event_impact_triage_decision_from_dict(decision.to_dict()) == decision
    assert (
        store.classified_version_ids(
            registration_id=candidate_set.registration_id,
            checkpoint_key=candidate_set.checkpoint_key,
            route_plan_id=candidate_set.route_plan_id,
            route_admission_id=candidate_set.route_admission_id,
            at=DECIDED_AT - timedelta(seconds=1),
        )
        == ()
    )
    assert store.classified_version_ids(
        registration_id=candidate_set.registration_id,
        checkpoint_key=candidate_set.checkpoint_key,
        route_plan_id=candidate_set.route_plan_id,
        route_admission_id=candidate_set.route_admission_id,
        at=DECIDED_AT,
    ) == tuple(sorted(candidate_set.version_ids))


def test_cluster_ready_time_uses_latest_member_before_blocking_selection(tmp_path: Path) -> None:
    candidate_set = _candidate_set(tmp_path)
    first, second, third = candidate_set.version_ids
    review = TriageClusterProposal.build(
        candidate_version_ids=(first, third),
        checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
        recommended_route=TriageRoute.ATTENTION_WATCH,
        event_archetypes=(EventArchetype.ISSUER_CORPORATE,),
        event_stage=EventStage.DIFFUSING,
        changed_facts=("A later follow-up makes the earlier issuer item unresolved.",),
        rule_reasons=("The completed cluster does not yet prove a policy-rule change.",),
        evidence_version_ids=(first, third),
        uncertainty_notes=("Primary-source confirmation is missing.",),
        watch_questions=("Will an authority publish a binding follow-up?",),
        triage_confidence=0.52,
    )
    eligible = TriageClusterProposal.build(
        candidate_version_ids=(second,),
        checkpoint_eligibility=CheckpointEligibility.ELIGIBLE,
        recommended_route=TriageRoute.CHECKPOINT_CANDIDATE,
        event_archetypes=(EventArchetype.POLICY_REGULATORY,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("A capital-market rule changed.",),
        rule_reasons=("The registered policy rule matches.",),
        evidence_version_ids=(second,),
        transmission_channels=(TransmissionChannel.POLICY_ACCESS,),
        triage_confidence=0.81,
    )
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(review, eligible),
    )

    decision = admit_event_impact_triage(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=_run_evidence(),
        run_authority=RecordingRunAuthority(candidate_set.candidate_set_id, proposal.proposal_id),
        decided_at=DECIDED_AT,
    )

    assert decision.status is TriageDecisionStatus.ELIGIBLE_SELECTED
    assert decision.selected_cluster_id == eligible.cluster_id
    assert decision.blocking_review_cluster_ids == ()
    assert decision.attention_watch_cluster_ids == (review.cluster_id,)


def test_triage_proposal_rejects_cross_cluster_evidence(tmp_path: Path) -> None:
    candidate_set = _candidate_set(tmp_path)
    first, second, third = candidate_set.version_ids
    first_cluster = TriageClusterProposal.build(
        candidate_version_ids=(first,),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.ARCHIVE,
        event_archetypes=(),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=(),
        rule_reasons=("The item is outside the registered checkpoint rule.",),
        evidence_version_ids=(second,),
        triage_confidence=0.7,
    )
    remaining = TriageClusterProposal.build(
        candidate_version_ids=(second, third),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.ARCHIVE,
        event_archetypes=(),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=(),
        rule_reasons=("The remaining items are outside the registered checkpoint rule.",),
        evidence_version_ids=(second, third),
        triage_confidence=0.7,
    )

    with pytest.raises(ValueError, match="evidence must belong to the same event cluster"):
        EventImpactTriageProposal.build(
            candidate_set=candidate_set,
            clusters=(first_cluster, remaining),
        )


def test_triage_proposal_cannot_omit_a_frozen_candidate(tmp_path: Path) -> None:
    candidate_set = _candidate_set(tmp_path)
    first = candidate_set.version_ids[0]
    cluster = TriageClusterProposal.build(
        candidate_version_ids=(first,),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.ARCHIVE,
        event_archetypes=(),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=(),
        rule_reasons=("The item is outside the registered checkpoint rule.",),
        evidence_version_ids=(first,),
        triage_confidence=0.7,
    )

    with pytest.raises(ValueError, match="classify every frozen candidate"):
        EventImpactTriageProposal.build(candidate_set=candidate_set, clusters=(cluster,))


def test_triage_freeze_rejects_a_snapshot_that_omits_readiness_candidates(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    snapshot = _snapshot(store)
    report = _readiness(snapshot)
    observations = snapshot.observations[:2]
    attempts = snapshot.attempts[:2]
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=observations[-1].times.retrieved_at,
        window_start=ADMITTED_AT,
        source_policy_id="triage-fixture-policy",
        parameters={"window": "post-admission"},
        sources=snapshot.query.sources[:2],
        minimum_data_sources=2,
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [item.to_dict() for item in attempts],
        "observations": [item.to_dict() for item in observations],
        "coverage_complete": True,
        "completed_at": observations[-1].times.retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    incomplete = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=attempts,
        observations=observations,
        coverage_complete=True,
        completed_at=observations[-1].times.retrieved_at,
    )
    store.put(incomplete)

    with pytest.raises(ValueError, match="every and only"):
        freeze_event_impact_triage_candidate_set(
            readiness_report=report,
            checkpoint_key="next-a-share-policy-event",
            snapshot=incomplete,
            snapshot_store=store,
            admission_store=_authorize_readiness(tmp_path / "state", report),
            frozen_at=FROZEN_AT,
        )


def test_triage_freeze_rechecks_route_effectiveness_after_readiness(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    snapshot = _snapshot(store)
    report = _readiness(snapshot)
    authority = _authorize_readiness(tmp_path / "state", report)
    with sqlite3.connect(authority.index_path) as connection:
        connection.execute(
            """
            UPDATE prospective_checkpoint_route_admissions
            SET superseded_at = ?
            WHERE route_plan_id = ?
            """,
            (FROZEN_AT.isoformat().replace("+00:00", "Z"), report.route_plan_id),
        )
        connection.execute(
            """
            UPDATE prospective_checkpoint_route_heads
            SET route_plan_id = ?, admission_id = ?, effective_from = ?,
                route_plan_artifact_hash = ?
            WHERE registration_id = ?
            """,
            (
                "prospective-checkpoint-route-plan-" + "6" * 64,
                "prospective-checkpoint-route-admission-" + "7" * 64,
                FROZEN_AT.isoformat().replace("+00:00", "Z"),
                "8" * 64,
                report.registration_id,
            ),
        )

    with pytest.raises(ValueError, match="not effective"):
        freeze_event_impact_triage_candidate_set(
            readiness_report=report,
            checkpoint_key="next-a-share-policy-event",
            snapshot=snapshot,
            snapshot_store=store,
            admission_store=authority,
            frozen_at=FROZEN_AT,
        )
