from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_watch_admission import (
    AgentDelegationContext,
    AgentDelegationContextStore,
    AgentWatchAdmissionService,
    AgentWatchRequest,
    EventImpactTriageWatchAuthority,
    WatchAdmissionBlocker,
    WatchAdmissionOutcome,
    WatchDelegateProfile,
    agent_delegation_context_from_dict,
    agent_watch_request_from_dict,
)
from market_impact_agent.attention_watch import AttentionWake, AttentionWatchService
from market_impact_agent.data_inputs import DataPITLane, DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.event_impact_triage import (
    CheckpointEligibility,
    EventImpactTriageProposal,
    TriageClusterProposal,
    TriageRoute,
)
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.monitoring_scope import (
    MonitoringMatchMode,
    MonitoringSubjectKind,
    MonitoringSubjectRef,
    MonitoringUseClass,
    ObservationMatchClause,
    ObservationMatcher,
    RegisteredQueryTemplate,
)
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_data import ProspectiveDataJournal
from market_impact_agent.research import EventArchetype, EventStage

from .test_attention_watch import (
    FIRST_RECEIPT,
    SECOND_RECEIPT,
    collection_policy_for_monitoring_test,
    snapshot_for_monitoring_test,
)
from .test_event_impact_triage import (
    RecordingRunAuthority,
    _candidate_set,  # pyright: ignore[reportPrivateUsage]
    _run_evidence,  # pyright: ignore[reportPrivateUsage]
)

TEMPLATE_REF = f"monitoring-query-template-{'a' * 64}"
PARENT_REF = "event-envelope-example"
PARENT_TYPE = "triage.coordinator"


def _profile(*, collection_policy_id: str) -> WatchDelegateProfile:
    return WatchDelegateProfile.build(
        name="follow-up fact verifier",
        description="Track a cited issuer until a named observable changes.",
        callback_agent_type="triage.fact-verifier",
        callback_agent_profile_ref=f"agent-profile-{'b' * 64}",
        allowed_parent_agent_types=(PARENT_TYPE,),
        allowed_subject_kinds=(MonitoringSubjectKind.ISSUER,),
        preloaded_skills=("news-evidence-assessment",),
        skill_manifest_hashes=("c" * 64,),
        required_capabilities=("event-revelation",),
        query_template=RegisteredQueryTemplate(
            template_ref=TEMPLATE_REF,
            capability=ObservationCapability.EVENT_REVELATION,
            pit_lane=DataPITLane.PROSPECTIVE,
            allowed_match_field_paths=("headline",),
            allowed_match_modes=(MonitoringMatchMode.CONTAINS_ANY,),
            maximum_match_clauses=1,
            maximum_terms_per_clause=4,
            maximum_term_length=64,
        ),
        collection_policy_id=collection_policy_id,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
        freshness_max_age_seconds=300,
        minimum_coverage_sources=1,
        maximum_bytes=1_000_000,
        maximum_polls=3,
        maximum_wakes=1,
        cooldown_seconds=60,
        active_duration_seconds=3600,
        maximum_lineage_depth=2,
        maximum_children_per_parent=2,
        maximum_active_watches=4,
        callback_max_turns=2,
        callback_max_input_tokens=20_000,
        callback_max_output_tokens=4_000,
        callback_max_cost_microusd=10_000,
    )


def _event_cluster_profile(*, collection_policy_id: str) -> WatchDelegateProfile:
    profile = _profile(collection_policy_id=collection_policy_id)
    return WatchDelegateProfile.build(
        name=profile.name,
        description=profile.description,
        callback_agent_type=profile.callback_agent_type,
        callback_agent_profile_ref=profile.callback_agent_profile_ref,
        allowed_parent_agent_types=profile.allowed_parent_agent_types,
        allowed_subject_kinds=(MonitoringSubjectKind.EVENT_CLUSTER,),
        preloaded_skills=profile.preloaded_skills,
        skill_manifest_hashes=profile.skill_manifest_hashes,
        required_capabilities=profile.required_capabilities,
        query_template=replace(
            profile.query_template,
            allowed_match_modes=(MonitoringMatchMode.CONTAINS_ALL,),
        ),
        collection_policy_id=profile.collection_policy_id,
        use_class=profile.use_class,
        freshness_max_age_seconds=profile.freshness_max_age_seconds,
        minimum_coverage_sources=profile.minimum_coverage_sources,
        maximum_polls=profile.maximum_polls,
        maximum_bytes=profile.maximum_bytes,
        maximum_wakes=profile.maximum_wakes,
        cooldown_seconds=profile.cooldown_seconds,
        active_duration_seconds=profile.active_duration_seconds,
        maximum_lineage_depth=profile.maximum_lineage_depth,
        maximum_children_per_parent=profile.maximum_children_per_parent,
        maximum_active_watches=profile.maximum_active_watches,
        callback_max_turns=profile.callback_max_turns,
        callback_max_input_tokens=profile.callback_max_input_tokens,
        callback_max_output_tokens=profile.callback_max_output_tokens,
        callback_max_cost_microusd=profile.callback_max_cost_microusd,
    )


def _matcher(*terms: str) -> ObservationMatcher:
    return ObservationMatcher(
        (
            ObservationMatchClause.build(
                field_path="headline",
                mode=MonitoringMatchMode.CONTAINS_ANY,
                terms=tuple(terms),
            ),
        )
    )


def _request(*, profile_id: str) -> AgentWatchRequest:
    return AgentWatchRequest.build(
        delegate_profile_id=profile_id,
        rationale="A cited new fact may alter this issuer's operating exposure.",
        watch_question="Did the named exposure become confirmed or contradicted?",
        evidence_refs=(PARENT_REF,),
        subject=MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
        matcher=_matcher("target"),
    )


def _context(
    *,
    authorized_evidence_refs: tuple[str, ...] = (PARENT_REF,),
    authorized_subjects: tuple[MonitoringSubjectRef, ...] = (
        MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
    ),
    authorized_matcher_terms: tuple[str, ...] = ("target",),
) -> AgentDelegationContext:
    return AgentDelegationContext(
        parent_ref=PARENT_REF,
        parent_agent_type=PARENT_TYPE,
        lineage_depth=0,
        created_at=FIRST_RECEIPT,
        authorized_evidence_refs=authorized_evidence_refs,
        authorized_subjects=authorized_subjects,
        authorized_matcher_terms=authorized_matcher_terms,
    )


def _setup(
    tmp_path: Path,
) -> tuple[
    LocalDataSnapshotStore,
    ProspectiveDataJournal,
    DataSnapshot,
    WatchDelegateProfile,
    AgentWatchAdmissionService,
]:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    collection_policy = collection_policy_for_monitoring_test()
    first = snapshot_for_monitoring_test(
        store,
        policy=collection_policy,
        retrieved_at=FIRST_RECEIPT,
    )
    journal.record_snapshot(first, policy=collection_policy)
    baseline = journal.freeze_snapshot(
        policy_id=collection_policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=collection_policy.window_start,
        frozen_at=FIRST_RECEIPT,
    )
    profile = _profile(collection_policy_id=collection_policy.policy_id)
    service = AgentWatchAdmissionService(
        store,
        profiles=(profile,),
        delegation_authority=AgentDelegationContextStore(store),
        journal=journal,
        watch_service=AttentionWatchService(store, journal=journal),
    )
    return store, journal, baseline, profile, service


def _triage_setup(
    tmp_path: Path,
) -> tuple[
    LocalDataSnapshotStore,
    ProspectiveDataJournal,
    DataSnapshot,
    WatchDelegateProfile,
    AgentWatchAdmissionService,
    EventImpactTriageWatchAuthority,
]:
    store, journal, baseline, _, legacy_service = _setup(tmp_path)
    candidate_set = _candidate_set(tmp_path)
    first, *remaining = candidate_set.version_ids
    review = TriageClusterProposal.build(
        candidate_version_ids=(first,),
        checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
        recommended_route=TriageRoute.ATTENTION_WATCH,
        event_archetypes=(EventArchetype.ISSUER_CORPORATE,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("Alpha product safety concerns may be escalating.",),
        rule_reasons=("Primary-source confirmation remains incomplete.",),
        evidence_version_ids=(first,),
        uncertainty_notes=("A binding authority response is still missing.",),
        affected_entity_refs=("issuer.alpha",),
        watch_questions=("Did an authority publish a binding follow-up?",),
        triage_confidence=0.52,
    )
    archive = TriageClusterProposal.build(
        candidate_version_ids=tuple(remaining),
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        recommended_route=TriageRoute.ARCHIVE,
        event_archetypes=(),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=(),
        rule_reasons=("The remaining items do not change the registered rule.",),
        evidence_version_ids=tuple(remaining),
        triage_confidence=0.9,
    )
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(review, archive),
    )
    decision_store = EventImpactTriageDecisionStore(store.root)
    decision_store.admit(
        candidate_set=candidate_set,
        proposal=proposal,
        run_evidence=_run_evidence(),
        run_authority=RecordingRunAuthority(
            candidate_set.candidate_set_id,
            proposal.proposal_id,
        ),
        decided_at=candidate_set.frozen_at + timedelta(seconds=1),
    )
    authority = EventImpactTriageWatchAuthority(
        store,
        decision_store=decision_store,
        candidate_set_id=candidate_set.candidate_set_id,
        cluster_id=review.cluster_id,
    )
    profile = _event_cluster_profile(
        collection_policy_id=legacy_service.profiles[
            next(iter(legacy_service.profiles))
        ].collection_policy_id
    )
    service = AgentWatchAdmissionService(
        store,
        profiles=(profile,),
        delegation_authority=authority,
        journal=journal,
        watch_service=legacy_service.watch_service,
    )
    return store, journal, baseline, profile, service, authority


def _triage_request(
    *,
    profile_id: str,
    context: AgentDelegationContext,
    evidence_refs: tuple[str, ...] | None = None,
    subject: MonitoringSubjectRef | None = None,
    terms: tuple[str, ...] = ("alpha", "safety"),
    rationale: str = "A cited event remains unresolved.",
) -> AgentWatchRequest:
    return AgentWatchRequest.build(
        delegate_profile_id=profile_id,
        rationale=rationale,
        watch_question="Did a binding follow-up resolve the event?",
        evidence_refs=(
            context.authorized_evidence_refs if evidence_refs is None else evidence_refs
        ),
        subject=(context.authorized_subjects[0] if subject is None else subject),
        matcher=ObservationMatcher(
            (
                ObservationMatchClause.build(
                    field_path="headline",
                    mode=MonitoringMatchMode.CONTAINS_ALL,
                    terms=terms,
                ),
            )
        ),
    )


def test_request_and_context_contracts_remain_canonical_and_bounded(tmp_path: Path) -> None:
    _, _, _, profile, _ = _setup(tmp_path)
    context = _context()
    request = _request(profile_id=profile.profile_id)

    assert agent_delegation_context_from_dict(context.to_dict()) == context
    assert agent_watch_request_from_dict(request.to_dict()) == request
    assert validate_agent_contract(request.to_dict(), "agent-watch-request.schema.json") == ()
    assert profile.to_dict()["callback_agent_profile_ref"] == profile.callback_agent_profile_ref
    for forbidden, value in (
        ("provider_id", "provider.example"),
        ("url", "https://example.invalid"),
        ("maximum_polls", 99),
        ("execution_capability", True),
    ):
        assert validate_agent_contract(
            {**request.to_dict(), forbidden: value},
            "agent-watch-request.schema.json",
        )


@pytest.mark.parametrize(
    "context",
    (
        _context(),
        _context(authorized_evidence_refs=(PARENT_REF, "evidence-extra")),
        _context(
            authorized_subjects=(
                MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
                MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.699999"),
            )
        ),
        _context(authorized_matcher_terms=("target", "unrelated")),
    ),
)
def test_caller_manufactured_parent_projection_cannot_admit(
    tmp_path: Path,
    context: AgentDelegationContext,
) -> None:
    store, _, baseline, profile, service = _setup(tmp_path)
    artifact = store.artifacts.put_json(context.to_dict())
    assert artifact.content_hash == context.authority_hash
    assert not hasattr(service.delegation_authority, "issue")

    with pytest.raises(ValueError, match="cannot be minted from caller data"):
        service.offered_profiles(context)
    with pytest.raises(ValueError, match="cannot be minted from caller data"):
        service.admit(
            _request(profile_id=profile.profile_id),
            context=context,
            initial_data_snapshot_id=baseline.snapshot_id,
            decided_at=SECOND_RECEIPT,
        )

    with service._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        assert connection.execute("SELECT COUNT(*) FROM agent_watch_requests").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM agent_watch_admissions").fetchone()[0] == 0
        assert (
            connection.execute("SELECT COUNT(*) FROM attention_watch_policies").fetchone()[0] == 0
        )


def test_caller_consistent_fake_or_subclass_authority_is_rejected(tmp_path: Path) -> None:
    store, journal, _, profile, service = _setup(tmp_path)

    class FakeAuthority:
        def __init__(self) -> None:
            self.store = store

        def reopen(self, context: AgentDelegationContext) -> AgentDelegationContext:
            return context

    class FakeConcreteSubclass(AgentDelegationContextStore):
        def reopen(self, context: AgentDelegationContext) -> AgentDelegationContext:
            return context

    for authority in (FakeAuthority(), FakeConcreteSubclass(store)):
        with pytest.raises(TypeError, match="concrete delegation store authority"):
            AgentWatchAdmissionService(
                store,
                profiles=(profile,),
                delegation_authority=cast(Any, authority),
                journal=journal,
                watch_service=service.watch_service,
            )


def test_callback_and_restart_activation_remain_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, journal, _, profile, service = _setup(tmp_path)

    with pytest.raises(ValueError, match="callbacks cannot be authorized"):
        service.callback_bindings(cast(AttentionWake, cast(Any, object())))
    with pytest.raises(ValueError, match="Admissions cannot be authorized"):
        service.admission("agent-watch-admission-" + "a" * 64)

    def unexpected_reconciliation(_service: AgentWatchAdmissionService) -> None:
        raise AssertionError("restart must not activate legacy Admissions without parent authority")

    monkeypatch.setattr(
        AgentWatchAdmissionService,
        "_reconcile_pending_watch_activations",
        unexpected_reconciliation,
    )
    AgentWatchAdmissionService(
        store,
        profiles=(profile,),
        delegation_authority=AgentDelegationContextStore(store),
        journal=journal,
        watch_service=AttentionWatchService(store, journal=journal),
    )


def test_concrete_triage_authority_derives_exact_event_cluster_projection(
    tmp_path: Path,
) -> None:
    _, _, _, _, service, authority = _triage_setup(tmp_path)
    context = authority.delegation_context()

    assert context.parent_agent_type == PARENT_TYPE
    assert context.parent_ref == authority.cluster_id
    assert context.authorized_subjects == (
        MonitoringSubjectRef(MonitoringSubjectKind.EVENT_CLUSTER, authority.cluster_id),
    )
    assert context.authorized_evidence_refs
    assert "event" not in context.authorized_matcher_terms
    assert {"alpha", "safety"} <= set(context.authorized_matcher_terms)
    assert service.offered_profiles(context)

    with pytest.raises(ValueError, match="differs from Triage authority"):
        service.offered_profiles(
            replace(
                context,
                authorized_matcher_terms=tuple(
                    sorted((*context.authorized_matcher_terms, "manufactured"))
                ),
            )
        )


def test_triage_watch_admission_rejects_unrelated_inputs_and_accepts_exact_cluster(
    tmp_path: Path,
) -> None:
    _, _, baseline, profile, service, authority = _triage_setup(tmp_path)
    context = authority.delegation_context()
    admitted_at = context.created_at

    unrelated_evidence = _triage_request(
        profile_id=profile.profile_id,
        context=context,
        evidence_refs=("prospective-observation-version-" + "f" * 64,),
    )
    assert (
        service.admit(
            unrelated_evidence,
            context=context,
            initial_data_snapshot_id=baseline.snapshot_id,
            decided_at=admitted_at + timedelta(seconds=1),
        ).blocker
        is WatchAdmissionBlocker.EVIDENCE_OUTSIDE_PARENT_VIEW
    )

    unrelated_subject = _triage_request(
        profile_id=profile.profile_id,
        context=context,
        subject=MonitoringSubjectRef(
            MonitoringSubjectKind.EVENT_CLUSTER,
            "event-impact-triage-cluster-" + "e" * 64,
        ),
    )
    assert (
        service.admit(
            unrelated_subject,
            context=context,
            initial_data_snapshot_id=baseline.snapshot_id,
            decided_at=admitted_at + timedelta(seconds=2),
        ).blocker
        is WatchAdmissionBlocker.SUBJECT_OUTSIDE_PARENT_VIEW
    )

    unrelated_matcher = _triage_request(
        profile_id=profile.profile_id,
        context=context,
        terms=("manufactured",),
    )
    assert (
        service.admit(
            unrelated_matcher,
            context=context,
            initial_data_snapshot_id=baseline.snapshot_id,
            decided_at=admitted_at + timedelta(seconds=3),
        ).blocker
        is WatchAdmissionBlocker.MATCHER_OUTSIDE_PARENT_VIEW
    )

    accepted = service.admit(
        _triage_request(profile_id=profile.profile_id, context=context),
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=admitted_at + timedelta(seconds=4),
    )
    assert accepted.outcome is WatchAdmissionOutcome.ADMITTED
    assert service.admission(accepted.admission_id) == accepted
    assert accepted.execution_capability is False
    assert (
        _triage_request(
            profile_id=profile.profile_id,
            context=context,
        ).matcher.matches({"headline": "Completely unrelated event elsewhere"})
        is None
    )


def test_triage_authority_rejects_other_roots_subclasses_and_unrouted_cluster(
    tmp_path: Path,
) -> None:
    store, _, _, _, _, authority = _triage_setup(tmp_path)

    class FakeDecisionStore(EventImpactTriageDecisionStore):
        pass

    with pytest.raises(TypeError, match="concrete Triage Decision store"):
        EventImpactTriageWatchAuthority(
            store,
            decision_store=FakeDecisionStore(store.root),
            candidate_set_id=authority.candidate_set_id,
            cluster_id=authority.cluster_id,
        )
    with pytest.raises(ValueError, match="share one exact state root"):
        EventImpactTriageWatchAuthority(
            store,
            decision_store=EventImpactTriageDecisionStore(tmp_path / "other-root"),
            candidate_set_id=authority.candidate_set_id,
            cluster_id=authority.cluster_id,
        )

    _, proposal, decision = authority.decision_store.get_context(authority.candidate_set_id)
    unrelated = next(
        item
        for item in proposal.clusters
        if item.cluster_id not in decision.attention_watch_cluster_ids
    )
    with pytest.raises(ValueError, match="not authorized for Attention Watch"):
        EventImpactTriageWatchAuthority(
            store,
            decision_store=authority.decision_store,
            candidate_set_id=authority.candidate_set_id,
            cluster_id=unrelated.cluster_id,
        ).delegation_context()


def test_exhausted_equivalent_watch_rejects_a_late_subscriber(tmp_path: Path) -> None:
    _, _, baseline, profile, service, authority = _triage_setup(tmp_path)
    context = authority.delegation_context()
    admitted_at = context.created_at + timedelta(seconds=1)
    accepted = service.admit(
        _triage_request(profile_id=profile.profile_id, context=context),
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=admitted_at,
    )
    assert accepted.outcome is WatchAdmissionOutcome.ADMITTED
    assert accepted.watch_id is not None
    with service.watch_service._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        connection.execute(
            """
            UPDATE attention_watch_policies
            SET status = ?, wake_count = ?
            WHERE watch_id = ?
            """,
            (
                "triggered",
                profile.maximum_wakes,
                accepted.watch_id,
            ),
        )

    late = service.admit(
        _triage_request(
            profile_id=profile.profile_id,
            context=context,
            rationale="A subscriber arrived only after the equivalent Watch exhausted its budget.",
        ),
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=admitted_at,
    )

    assert late.outcome is WatchAdmissionOutcome.REJECTED
    assert late.blocker is WatchAdmissionBlocker.WATCH_BUDGET_EXHAUSTED
    assert late.watch_id is None


def test_triage_service_restart_reopens_parent_authority_before_recovery(
    tmp_path: Path,
) -> None:
    store, journal, baseline, profile, service, authority = _triage_setup(tmp_path)
    context = authority.delegation_context()
    accepted = service.admit(
        _triage_request(profile_id=profile.profile_id, context=context),
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=context.created_at + timedelta(seconds=1),
    )
    assert accepted.outcome is WatchAdmissionOutcome.ADMITTED

    missing_authority = EventImpactTriageWatchAuthority(
        store,
        decision_store=authority.decision_store,
        candidate_set_id="event-impact-triage-candidate-set-" + "f" * 64,
        cluster_id="event-impact-triage-cluster-" + "e" * 64,
    )
    with pytest.raises(KeyError):
        AgentWatchAdmissionService(
            store,
            profiles=(profile,),
            delegation_authority=missing_authority,
            journal=journal,
            watch_service=service.watch_service,
        )
