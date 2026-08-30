from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
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
    CheckpointEligibility,
    CompletedTriageRunAuthority,
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    TriageAgentRole,
    TriageClusterProposal,
    TriageDecisionStatus,
    TriageRoute,
    TriageRunEvidence,
    TriageRunMemberEvidence,
    admit_event_impact_triage,
    freeze_event_impact_triage_candidate_set,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_checkpoint_readiness import (
    CheckpointReadinessStatus,
    ProspectiveCheckpointReadiness,
    ProspectiveCheckpointReadinessReport,
)
from market_impact_agent.prospective_data import prospective_observation_version_id
from market_impact_agent.research import (
    EventArchetype,
    EventStage,
    TransmissionChannel,
)

ADMITTED_AT = datetime(2026, 8, 30, 0, 0, tzinfo=UTC)
FROZEN_AT = ADMITTED_AT + timedelta(minutes=40)
DECIDED_AT = ADMITTED_AT + timedelta(minutes=41)
HASH = "1" * 64


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
    route_plan_id = "prospective-checkpoint-route-plan-" + "3" * 64
    route_admission_id = "prospective-checkpoint-route-admission-" + "4" * 64
    registration_id = "prospective-diagnostic-registration-" + "5" * 64
    core = {
        "schema_version": "market-impact.prospective-checkpoint-readiness-report.v1",
        "route_plan_id": route_plan_id,
        "route_admission_id": route_admission_id,
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
        route_plan_id=route_plan_id,
        route_admission_id=route_admission_id,
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
    return freeze_event_impact_triage_candidate_set(
        readiness_report=_readiness(snapshot),
        checkpoint_key="next-a-share-policy-event",
        snapshot=snapshot,
        snapshot_store=store,
        frozen_at=FROZEN_AT,
    )


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
    assert (
        validate_agent_contract(proposal.to_dict(), "event-impact-triage-proposal.schema.json")
        == ()
    )
    assert (
        validate_agent_contract(decision.to_dict(), "event-impact-triage-decision.schema.json")
        == ()
    )


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
            frozen_at=FROZEN_AT,
        )
