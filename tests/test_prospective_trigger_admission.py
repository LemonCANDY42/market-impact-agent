from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.event_impact_triage import (
    EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA,
    EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
    CheckpointEligibility,
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    TriageClusterProposal,
    TriageDecisionStatus,
    TriageObservationRef,
    TriageRoute,
    TriageWorkDecisionEvidence,
)
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.prospective_trigger_admission import (
    HistoricalAnalogyCase,
    HistoricalAnalogyMode,
    PositionHolding,
    ProspectiveEventAssessmentArtifact,
    ProspectiveHistoricalAnalogyPack,
    ProspectivePositionSnapshot,
    ProspectiveTriggerAdmissionStore,
    TransmissionPath,
    TriggerAdmissionKind,
    admit_prospective_trigger,
    evaluate_event_materiality,
    prospective_event_assessment_from_dict,
    prospective_historical_analogy_pack_from_dict,
    prospective_materiality_gate_result_from_dict,
    prospective_position_snapshot_from_dict,
    prospective_trigger_admission_from_dict,
)
from market_impact_agent.research import (
    EventArchetype,
    EventStage,
    TransmissionChannel,
)


def _hex(seed: int) -> str:
    return f"{seed:064x}"


class _TriageAuthority:
    def __init__(
        self,
        context: tuple[
            EventImpactTriageCandidateSet,
            EventImpactTriageProposal,
            EventImpactTriageDecision,
        ],
        *,
        epoch_contexts: tuple[
            tuple[
                EventImpactTriageCandidateSet,
                EventImpactTriageProposal,
                EventImpactTriageDecision,
                TriageClusterProposal,
            ],
            ...,
        ]
        | None = None,
    ) -> None:
        self.context = context
        self.epoch_contexts = epoch_contexts

    def get_context(
        self,
        candidate_set_id: str,
    ) -> tuple[
        EventImpactTriageCandidateSet,
        EventImpactTriageProposal,
        EventImpactTriageDecision,
    ]:
        assert candidate_set_id == self.context[0].candidate_set_id
        return self.context

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
    ]:
        contexts = self.epoch_contexts or tuple(
            (*self.context, cluster) for cluster in self.context[1].clusters
        )
        assert all(
            candidate_set.registration_id == registration_id
            and candidate_set.checkpoint_key == checkpoint_key
            and candidate_set.route_plan_id == route_plan_id
            and candidate_set.route_admission_id == route_admission_id
            and decision.decided_at <= at
            for candidate_set, _, decision, _ in contexts
        )
        return contexts


class _AssessmentAuthority:
    def __init__(self, *assessments: ProspectiveEventAssessmentArtifact) -> None:
        self.assessments = assessments

    def assert_authoritative_completed_event_assessment(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        decision: EventImpactTriageDecision,
        assessment: ProspectiveEventAssessmentArtifact,
    ) -> None:
        assert candidate_set.candidate_set_id == decision.candidate_set_id
        assert proposal.proposal_id == decision.proposal_id
        assert assessment.triage_decision_id == decision.decision_id
        assert assessment in self.assessments


def _registration() -> ProspectiveDiagnosticRegistration:
    return load_prospective_diagnostic_registration(
        Path("examples/research/prospective-diagnostic-registration-v4.json")
    )


def _candidate_set(
    registration: ProspectiveDiagnosticRegistration,
    *,
    checkpoint_key: str = "next-material-a-share-event",
    seed_offset: int = 0,
    available_after_minutes: int = 1,
    frozen_after_minutes: int = 5,
) -> EventImpactTriageCandidateSet:
    admitted_at = datetime(2026, 8, 30, 1, 0, tzinfo=UTC)
    frozen_at = admitted_at + timedelta(minutes=frozen_after_minutes)
    observation = TriageObservationRef(
        version_id=f"prospective-observation-version-{_hex(1 + seed_offset)}",
        observation_id=f"source-observation-{_hex(2 + seed_offset)}",
        first_available_at=admitted_at + timedelta(minutes=available_after_minutes),
        authority_at=admitted_at + timedelta(minutes=available_after_minutes),
        provider_id="tushare-observation",
        provider_version="1",
        upstream_source="tushare-news-cls",
        source_ref=f"licensed://event/{1 + seed_offset}",
        raw_content_hash=_hex(3 + seed_offset),
        normalized_payload_hash=_hex(4 + seed_offset),
    )
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA,
        "registration_id": registration.registration_id,
        "checkpoint_key": checkpoint_key,
        "route_plan_id": f"prospective-checkpoint-route-plan-{_hex(6)}",
        "route_admission_id": f"prospective-checkpoint-route-admission-{_hex(7)}",
        "readiness_report_id": (f"prospective-checkpoint-readiness-report-{_hex(8 + seed_offset)}"),
        "data_snapshot_id": f"data-snapshot-{_hex(9 + seed_offset)}",
        "admitted_at": admitted_at.isoformat().replace("+00:00", "Z"),
        "frozen_at": frozen_at.isoformat().replace("+00:00", "Z"),
        "observations": [observation.to_dict()],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return EventImpactTriageCandidateSet(
        candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
        registration_id=cast(str, core["registration_id"]),
        checkpoint_key=cast(str, core["checkpoint_key"]),
        route_plan_id=cast(str, core["route_plan_id"]),
        route_admission_id=cast(str, core["route_admission_id"]),
        readiness_report_id=cast(str, core["readiness_report_id"]),
        data_snapshot_id=cast(str, core["data_snapshot_id"]),
        admitted_at=admitted_at,
        frozen_at=frozen_at,
        observations=(observation,),
    )


def _triage(
    registration: ProspectiveDiagnosticRegistration,
    *,
    checkpoint_key: str = "next-material-a-share-event",
    selected: bool = False,
    needs_review: bool = False,
    seed_offset: int = 0,
    available_after_minutes: int = 1,
    frozen_after_minutes: int = 5,
) -> tuple[
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    EventImpactTriageDecision,
    TriageClusterProposal,
]:
    candidate_set = _candidate_set(
        registration,
        checkpoint_key=checkpoint_key,
        seed_offset=seed_offset,
        available_after_minutes=available_after_minutes,
        frozen_after_minutes=frozen_after_minutes,
    )
    version_id = candidate_set.version_ids[0]
    if selected and needs_review:
        raise ValueError("test Triage helper cannot select and require review")
    eligibility = (
        CheckpointEligibility.ELIGIBLE
        if selected
        else CheckpointEligibility.NEEDS_REVIEW
        if needs_review
        else CheckpointEligibility.INELIGIBLE
    )
    cluster = TriageClusterProposal.build(
        candidate_version_ids=(version_id,),
        checkpoint_eligibility=eligibility,
        recommended_route=(
            TriageRoute.CHECKPOINT_CANDIDATE if selected else TriageRoute.EVENT_ASSESSMENT
        ),
        event_archetypes=(EventArchetype.GEOPOLITICAL_SECURITY,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("A shipping chokepoint suffered a reported tanker attack.",),
        rule_reasons=("The event is not an A-share market-policy change.",),
        evidence_version_ids=(version_id,),
        transmission_channels=(
            TransmissionChannel.CAPACITY_COST_INVENTORY,
            TransmissionChannel.RISK_UNCERTAINTY_INSURANCE,
        ),
        affected_entity_refs=("energy-logistics",),
        uncertainty_notes=(
            ("The checkpoint-eligibility boundary requires review.",) if needs_review else ()
        ),
        triage_confidence=0.82,
    )
    proposal = EventImpactTriageProposal.build(candidate_set=candidate_set, clusters=(cluster,))
    decided_at = candidate_set.frozen_at + timedelta(minutes=1)
    run_evidence = TriageWorkDecisionEvidence(
        plan_id=f"event-impact-triage-work-execution-plan-{_hex(10 + seed_offset)}",
        work_manifest_id=f"event-impact-triage-work-manifest-{_hex(11 + seed_offset)}",
        completed_member_count=6,
        finished_at=decided_at,
        usage_ledger_hash=_hex(12 + seed_offset),
        authority_receipt_hash=_hex(13 + seed_offset),
    )
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
        "candidate_set_id": candidate_set.candidate_set_id,
        "proposal_id": proposal.proposal_id,
        "run_evidence": run_evidence.to_dict(),
        "status": (
            TriageDecisionStatus.ELIGIBLE_SELECTED.value
            if selected
            else TriageDecisionStatus.NEEDS_REVIEW.value
            if needs_review
            else TriageDecisionStatus.NO_ELIGIBLE_CANDIDATE.value
        ),
        "selected_cluster_id": cluster.cluster_id if selected else None,
        "blocking_review_cluster_ids": [cluster.cluster_id] if needs_review else [],
        "unselected_eligible_cluster_ids": [],
        "event_assessment_cluster_ids": [] if selected else [cluster.cluster_id],
        "attention_watch_cluster_ids": [],
        "archive_cluster_ids": [],
        "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    decision = EventImpactTriageDecision(
        decision_id=f"event-impact-triage-decision-{canonical_hash(core)}",
        candidate_set_id=candidate_set.candidate_set_id,
        proposal_id=proposal.proposal_id,
        run_evidence=run_evidence,
        status=(
            TriageDecisionStatus.ELIGIBLE_SELECTED
            if selected
            else TriageDecisionStatus.NEEDS_REVIEW
            if needs_review
            else TriageDecisionStatus.NO_ELIGIBLE_CANDIDATE
        ),
        selected_cluster_id=cluster.cluster_id if selected else None,
        blocking_review_cluster_ids=(cluster.cluster_id,) if needs_review else (),
        unselected_eligible_cluster_ids=(),
        event_assessment_cluster_ids=() if selected else (cluster.cluster_id,),
        attention_watch_cluster_ids=(),
        archive_cluster_ids=(),
        decided_at=decided_at,
        schema_version=EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
    )
    return candidate_set, proposal, decision, cluster


def _two_cluster_triage(
    registration: ProspectiveDiagnosticRegistration,
) -> tuple[
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    EventImpactTriageDecision,
    tuple[TriageClusterProposal, TriageClusterProposal],
]:
    first = _candidate_set(registration)
    second_observation = TriageObservationRef(
        version_id=f"prospective-observation-version-{_hex(21)}",
        observation_id=f"source-observation-{_hex(22)}",
        first_available_at=first.observations[0].first_available_at + timedelta(minutes=1),
        authority_at=first.observations[0].authority_at + timedelta(minutes=1),
        provider_id="tushare-observation",
        provider_version="1",
        upstream_source="tushare-news-yicai",
        source_ref="licensed://event/2",
        raw_content_hash=_hex(23),
        normalized_payload_hash=_hex(24),
    )
    observations = (*first.observations, second_observation)
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA,
        "registration_id": first.registration_id,
        "checkpoint_key": first.checkpoint_key,
        "route_plan_id": first.route_plan_id,
        "route_admission_id": first.route_admission_id,
        "readiness_report_id": first.readiness_report_id,
        "data_snapshot_id": first.data_snapshot_id,
        "admitted_at": first.admitted_at.isoformat().replace("+00:00", "Z"),
        "frozen_at": first.frozen_at.isoformat().replace("+00:00", "Z"),
        "observations": [item.to_dict() for item in observations],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    candidate_set = EventImpactTriageCandidateSet(
        candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
        registration_id=first.registration_id,
        checkpoint_key=first.checkpoint_key,
        route_plan_id=first.route_plan_id,
        route_admission_id=first.route_admission_id,
        readiness_report_id=first.readiness_report_id,
        data_snapshot_id=first.data_snapshot_id,
        admitted_at=first.admitted_at,
        frozen_at=first.frozen_at,
        observations=observations,
    )
    clusters = tuple(
        TriageClusterProposal.build(
            candidate_version_ids=(observation.version_id,),
            checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
            recommended_route=TriageRoute.EVENT_ASSESSMENT,
            event_archetypes=(EventArchetype.GEOPOLITICAL_SECURITY,),
            event_stage=EventStage.FIRST_OBSERVED,
            changed_facts=(f"Potential impact event {index} was reported.",),
            rule_reasons=("Formal materiality assessment is required.",),
            evidence_version_ids=(observation.version_id,),
            transmission_channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
            affected_entity_refs=(f"candidate-{index}",),
            triage_confidence=0.75,
        )
        for index, observation in enumerate(observations, start=1)
    )
    proposal = EventImpactTriageProposal.build(candidate_set=candidate_set, clusters=clusters)
    decided_at = candidate_set.frozen_at + timedelta(minutes=1)
    run_evidence = TriageWorkDecisionEvidence(
        plan_id=f"event-impact-triage-work-execution-plan-{_hex(25)}",
        work_manifest_id=f"event-impact-triage-work-manifest-{_hex(26)}",
        completed_member_count=6,
        finished_at=decided_at,
        usage_ledger_hash=_hex(27),
        authority_receipt_hash=_hex(28),
    )
    event_assessment_ids = tuple(sorted(item.cluster_id for item in clusters))
    decision_core = {
        "schema_version": EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
        "candidate_set_id": candidate_set.candidate_set_id,
        "proposal_id": proposal.proposal_id,
        "run_evidence": run_evidence.to_dict(),
        "status": TriageDecisionStatus.NO_ELIGIBLE_CANDIDATE.value,
        "selected_cluster_id": None,
        "blocking_review_cluster_ids": [],
        "unselected_eligible_cluster_ids": [],
        "event_assessment_cluster_ids": list(event_assessment_ids),
        "attention_watch_cluster_ids": [],
        "archive_cluster_ids": [],
        "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    decision = EventImpactTriageDecision(
        decision_id=f"event-impact-triage-decision-{canonical_hash(decision_core)}",
        candidate_set_id=candidate_set.candidate_set_id,
        proposal_id=proposal.proposal_id,
        run_evidence=run_evidence,
        status=TriageDecisionStatus.NO_ELIGIBLE_CANDIDATE,
        selected_cluster_id=None,
        blocking_review_cluster_ids=(),
        unselected_eligible_cluster_ids=(),
        event_assessment_cluster_ids=event_assessment_ids,
        attention_watch_cluster_ids=(),
        archive_cluster_ids=(),
        decided_at=decided_at,
        schema_version=EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
    )
    return (
        candidate_set,
        proposal,
        decision,
        cast(tuple[TriageClusterProposal, TriageClusterProposal], clusters),
    )


def test_material_event_can_enter_the_checkpoint_path_without_becoming_policy_eligible(
    tmp_path: Path,
) -> None:
    registration = _registration()
    candidate_set, proposal, decision, cluster = _triage(registration)
    position_snapshot = ProspectivePositionSnapshot.build(
        as_of=decision.decided_at,
        holdings=(
            PositionHolding(
                target_id="601857.SH",
                venue="XSHG",
                instrument_class="equity",
            ),
        ),
    )
    analogy_pack = ProspectiveHistoricalAnalogyPack.build(
        cases=(
            HistoricalAnalogyCase(
                case_ref="outcome-opened-review:energy-logistics-2019",
                mode=HistoricalAnalogyMode.OUTCOME_OPENED_REVIEW,
                artifact_hash=_hex(14),
                similarity_basis=(
                    "Physical supply disruption with freight and risk-premium transmission."
                ),
                counterevidence=("The historical event resolved faster.",),
            ),
        ),
        built_at=decision.decided_at,
    )
    assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=cluster,
        event_assessment_artifact_hash=_hex(15),
        paths=(
            TransmissionPath(
                target_id="601857.SH",
                venue="XSHG",
                instrument_class="equity",
                channels=(TransmissionChannel.CAPACITY_COST_INVENTORY,),
                causal_steps=(
                    "Shipping disruption raises delivered energy costs.",
                    "The issuer has direct energy-price exposure.",
                ),
                evidence_version_ids=cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("The disruption may be contained before cargo schedules change.",),
        invalidation_conditions=("Official evidence shows normal chokepoint throughput.",),
        assessed_at=decision.decided_at + timedelta(minutes=1),
        position_snapshot=position_snapshot,
        historical_analogy_pack=analogy_pack,
    )
    materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key=candidate_set.checkpoint_key,
        assessment=assessment,
        evaluated_at=assessment.assessed_at + timedelta(minutes=1),
    )
    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=candidate_set,
        proposal=proposal,
        decision=decision,
        cluster_id=cluster.cluster_id,
        assessment=assessment,
        materiality=materiality,
        admitted_at=materiality.evaluated_at + timedelta(minutes=1),
    )

    assert admission.kind is TriggerAdmissionKind.MATERIAL_EVENT
    assert admission.triage_decision_id == decision.decision_id
    assert admission.cluster_id == cluster.cluster_id
    assert admission.materiality_gate_result_id == materiality.result_id
    assert admission.held_target_ids == ("601857.SH",)
    assert admission.judgment_model_calls_authorized is False
    assert admission.execution_capability is False
    durable = ProspectiveTriggerAdmissionStore(LocalDataSnapshotStore(tmp_path / "state"))
    with pytest.raises(ValueError, match="completed EventAssessment authority"):
        durable.record(
            admission,
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            triage_authority=_TriageAuthority((candidate_set, proposal, decision)),
            assessment=assessment,
            materiality=materiality,
        )
    assert (
        durable.record(
            admission,
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            triage_authority=_TriageAuthority((candidate_set, proposal, decision)),
            assessment=assessment,
            materiality=materiality,
            assessment_authority=_AssessmentAuthority(assessment),
        )
        == admission
    )
    assert (
        durable.record(
            admission,
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            triage_authority=_TriageAuthority((candidate_set, proposal, decision)),
            assessment=assessment,
            materiality=materiality,
            assessment_authority=_AssessmentAuthority(assessment),
        )
        == admission
    )
    assert durable.get(admission.admission_id) == admission
    assert durable.get_context(admission.admission_id) == (
        admission,
        assessment,
        materiality,
    )
    assert prospective_position_snapshot_from_dict(position_snapshot.to_dict()) == position_snapshot
    assert prospective_historical_analogy_pack_from_dict(analogy_pack.to_dict()) == analogy_pack
    assert prospective_event_assessment_from_dict(assessment.to_dict()) == assessment
    assert prospective_materiality_gate_result_from_dict(materiality.to_dict()) == materiality
    assert prospective_trigger_admission_from_dict(admission.to_dict()) == admission
    for payload, schema in (
        (position_snapshot.to_dict(), "prospective-position-snapshot.schema.json"),
        (analogy_pack.to_dict(), "prospective-historical-analogy-pack.schema.json"),
        (assessment.to_dict(), "prospective-event-assessment.schema.json"),
        (materiality.to_dict(), "prospective-materiality-gate-result.schema.json"),
        (admission.to_dict(), "prospective-trigger-admission.schema.json"),
    ):
        assert validate_agent_contract(payload, schema) == ()


def test_material_event_admission_rejects_a_gate_for_another_assessment() -> None:
    registration = _registration()
    candidate_set, proposal, decision, cluster = _triage(registration)
    assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=cluster,
        event_assessment_artifact_hash=_hex(15),
        paths=(
            TransmissionPath(
                target_id="512800.SH",
                venue="XSHG",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.RISK_UNCERTAINTY_INSURANCE,),
                causal_steps=("Risk premium changes the sector discount rate.",),
                evidence_version_ids=cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("No observable repricing has occurred.",),
        invalidation_conditions=("The event report is withdrawn.",),
        assessed_at=decision.decided_at + timedelta(minutes=1),
    )
    materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key=candidate_set.checkpoint_key,
        assessment=assessment,
        evaluated_at=assessment.assessed_at + timedelta(minutes=1),
    )
    other_assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=cluster,
        event_assessment_artifact_hash=_hex(16),
        paths=(
            TransmissionPath(
                target_id="159920.SZ",
                venue="XSHE",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.RISK_UNCERTAINTY_INSURANCE,),
                causal_steps=("A different market path is assessed.",),
                evidence_version_ids=cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("No observable repricing has occurred.",),
        invalidation_conditions=("The event report is withdrawn.",),
        assessed_at=decision.decided_at + timedelta(minutes=1),
    )

    with pytest.raises(ValueError, match="assessment"):
        admit_prospective_trigger(
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            cluster_id=cluster.cluster_id,
            assessment=other_assessment,
            materiality=materiality,
            admitted_at=materiality.evaluated_at + timedelta(minutes=1),
        )


def test_later_material_event_requires_every_earlier_rejected_result() -> None:
    registration = _registration()
    candidate_set, proposal, decision, clusters = _two_cluster_triage(registration)
    first_assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=clusters[0],
        event_assessment_artifact_hash=_hex(29),
        paths=(
            TransmissionPath(
                target_id="SPY.US",
                venue="ARCX",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The first event has no registered A-share target.",),
                evidence_version_ids=clusters[0].evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("No A-share exposure was identified.",),
        invalidation_conditions=("A direct A-share exposure is established.",),
        assessed_at=decision.decided_at + timedelta(minutes=1),
    )
    first_materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key=candidate_set.checkpoint_key,
        assessment=first_assessment,
        evaluated_at=first_assessment.assessed_at + timedelta(minutes=1),
    )
    second_assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=clusters[1],
        event_assessment_artifact_hash=_hex(30),
        paths=(
            TransmissionPath(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The later event has a cited registered A-share target.",),
                evidence_version_ids=clusters[1].evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("The effect may already be priced.",),
        invalidation_conditions=("The event report is withdrawn.",),
        assessed_at=first_materiality.evaluated_at + timedelta(minutes=1),
    )
    second_materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key=candidate_set.checkpoint_key,
        assessment=second_assessment,
        evaluated_at=second_assessment.assessed_at + timedelta(minutes=1),
    )

    with pytest.raises(ValueError, match="every earlier EventAssessment result"):
        admit_prospective_trigger(
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            cluster_id=clusters[1].cluster_id,
            assessment=second_assessment,
            materiality=second_materiality,
            admitted_at=second_materiality.evaluated_at + timedelta(minutes=1),
        )

    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=candidate_set,
        proposal=proposal,
        decision=decision,
        cluster_id=clusters[1].cluster_id,
        assessment=second_assessment,
        materiality=second_materiality,
        preceding_materiality_contexts=((first_assessment, first_materiality),),
        admitted_at=second_materiality.evaluated_at + timedelta(minutes=1),
    )

    assert admission.preceding_materiality_gate_result_ids == (first_materiality.result_id,)


def test_material_admission_preserves_first_candidate_order_across_decisions(
    tmp_path: Path,
) -> None:
    registration = _registration()
    first_candidate, first_proposal, first_decision, first_cluster = _triage(
        registration,
        seed_offset=100,
    )
    second_candidate, second_proposal, second_decision, second_cluster = _triage(
        registration,
        seed_offset=200,
        available_after_minutes=10,
        frozen_after_minutes=14,
    )
    first_assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=first_decision,
        cluster=first_cluster,
        event_assessment_artifact_hash=_hex(301),
        paths=(
            TransmissionPath(
                target_id="SPY.US",
                venue="ARCX",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The earlier event has no registered A-share target.",),
                evidence_version_ids=first_cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("No A-share exposure was identified.",),
        invalidation_conditions=("A direct A-share exposure is established.",),
        assessed_at=first_decision.decided_at + timedelta(minutes=1),
    )
    first_materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key=first_candidate.checkpoint_key,
        assessment=first_assessment,
        evaluated_at=first_assessment.assessed_at + timedelta(minutes=1),
    )
    second_assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=second_decision,
        cluster=second_cluster,
        event_assessment_artifact_hash=_hex(302),
        paths=(
            TransmissionPath(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The later event has a registered A-share transmission path.",),
                evidence_version_ids=second_cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("The effect may already be priced.",),
        invalidation_conditions=("The event report is withdrawn.",),
        assessed_at=second_decision.decided_at + timedelta(minutes=1),
    )
    second_materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key=second_candidate.checkpoint_key,
        assessment=second_assessment,
        evaluated_at=second_assessment.assessed_at + timedelta(minutes=1),
    )
    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=second_candidate,
        proposal=second_proposal,
        decision=second_decision,
        cluster_id=second_cluster.cluster_id,
        admitted_at=second_materiality.evaluated_at + timedelta(minutes=1),
        assessment=second_assessment,
        materiality=second_materiality,
        preceding_materiality_contexts=((first_assessment, first_materiality),),
    )
    authority = _TriageAuthority(
        (second_candidate, second_proposal, second_decision),
        epoch_contexts=(
            (first_candidate, first_proposal, first_decision, first_cluster),
            (second_candidate, second_proposal, second_decision, second_cluster),
        ),
    )
    durable = ProspectiveTriggerAdmissionStore(LocalDataSnapshotStore(tmp_path / "state"))

    with pytest.raises(ValueError, match="every earlier route-epoch"):
        durable.record(
            admission,
            registration=registration,
            candidate_set=second_candidate,
            proposal=second_proposal,
            decision=second_decision,
            triage_authority=authority,
            assessment=second_assessment,
            materiality=second_materiality,
            assessment_authority=_AssessmentAuthority(second_assessment),
        )

    assert (
        durable.record(
            admission,
            registration=registration,
            candidate_set=second_candidate,
            proposal=second_proposal,
            decision=second_decision,
            triage_authority=authority,
            assessment=second_assessment,
            materiality=second_materiality,
            preceding_materiality_contexts=((first_assessment, first_materiality),),
            assessment_authority=_AssessmentAuthority(first_assessment, second_assessment),
        )
        == admission
    )


def test_checkpoint_admission_rejects_an_earlier_unresolved_review(
    tmp_path: Path,
) -> None:
    registration = _registration()
    first_candidate, first_proposal, first_decision, first_cluster = _triage(
        registration,
        checkpoint_key="next-a-share-policy-event",
        needs_review=True,
        seed_offset=300,
    )
    second_candidate, second_proposal, second_decision, second_cluster = _triage(
        registration,
        checkpoint_key="next-a-share-policy-event",
        selected=True,
        seed_offset=400,
        available_after_minutes=10,
        frozen_after_minutes=14,
    )
    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=second_candidate,
        proposal=second_proposal,
        decision=second_decision,
        cluster_id=second_cluster.cluster_id,
        admitted_at=second_decision.decided_at + timedelta(minutes=1),
    )
    authority = _TriageAuthority(
        (second_candidate, second_proposal, second_decision),
        epoch_contexts=(
            (first_candidate, first_proposal, first_decision, first_cluster),
            (second_candidate, second_proposal, second_decision, second_cluster),
        ),
    )

    with pytest.raises(ValueError, match="earlier unresolved review"):
        ProspectiveTriggerAdmissionStore(LocalDataSnapshotStore(tmp_path / "state")).record(
            admission,
            registration=registration,
            candidate_set=second_candidate,
            proposal=second_proposal,
            decision=second_decision,
            triage_authority=authority,
        )


def test_material_checkpoint_cannot_bypass_event_assessment_and_materiality() -> None:
    registration = _registration()
    candidate_set, proposal, decision, cluster = _triage(registration)

    with pytest.raises(ValueError, match="requires EventAssessment and gate"):
        admit_prospective_trigger(
            registration=registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            cluster_id=cluster.cluster_id,
            admitted_at=decision.decided_at + timedelta(minutes=1),
        )


def test_non_material_checkpoint_cannot_evaluate_materiality() -> None:
    registration = _registration()
    candidate_set, _proposal, decision, cluster = _triage(
        registration,
        checkpoint_key="next-a-share-policy-event",
    )
    assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=cluster,
        event_assessment_artifact_hash=_hex(17),
        paths=(
            TransmissionPath(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The event changes broad-market expectations.",),
                evidence_version_ids=cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("The policy effect may already be priced.",),
        invalidation_conditions=("The policy announcement is withdrawn.",),
        assessed_at=decision.decided_at + timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="registered material-event checkpoint"):
        evaluate_event_materiality(
            registration=registration,
            checkpoint_key=candidate_set.checkpoint_key,
            assessment=assessment,
            evaluated_at=assessment.assessed_at + timedelta(minutes=1),
        )


def test_trigger_admission_rejects_another_registration() -> None:
    registration = _registration()
    other_registration = load_prospective_diagnostic_registration(
        Path("examples/research/prospective-diagnostic-registration-v3.json")
    )
    candidate_set, proposal, decision, cluster = _triage(registration)

    with pytest.raises(ValueError, match="another registration"):
        admit_prospective_trigger(
            registration=other_registration,
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            cluster_id=cluster.cluster_id,
            admitted_at=decision.decided_at + timedelta(minutes=1),
        )


def test_materiality_filters_an_out_of_scope_path_without_blocking_a_valid_target() -> None:
    registration = _registration()
    _candidate_set_value, _proposal, decision, cluster = _triage(registration)
    assessment = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=cluster,
        event_assessment_artifact_hash=_hex(15),
        paths=(
            TransmissionPath(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The event changes broad-market expectations.",),
                evidence_version_ids=cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
            TransmissionPath(
                target_id="SPY.US",
                venue="ARCX",
                instrument_class="exchange_traded_fund",
                channels=(TransmissionChannel.EXPECTATIONS_ATTENTION,),
                causal_steps=("The event may also affect an unregistered venue.",),
                evidence_version_ids=cluster.evidence_version_ids,
                horizon_sessions=5,
            ),
        ),
        counterevidence=("The event may be fully priced.",),
        invalidation_conditions=("The report is withdrawn.",),
        assessed_at=decision.decided_at + timedelta(minutes=1),
    )

    materiality = evaluate_event_materiality(
        registration=registration,
        checkpoint_key="next-material-a-share-event",
        assessment=assessment,
        evaluated_at=assessment.assessed_at + timedelta(minutes=1),
    )

    assert materiality.admitted_target_ids == ("510300.SH",)
    assert materiality.blocking_gaps == ()
    assert "target:SPY.US:venue_not_allowed" in materiality.nonblocking_information_gaps
