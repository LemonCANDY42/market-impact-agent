from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_watch_admission import (
    AgentDelegationContext,
    AgentDelegationContextStore,
    AgentWatchAdmissionService,
    AgentWatchRequest,
    WatchDelegateProfile,
    agent_delegation_context_from_dict,
    agent_watch_request_from_dict,
)
from market_impact_agent.attention_watch import AttentionWake, AttentionWatchService
from market_impact_agent.data_inputs import DataPITLane, DataSnapshot, LocalDataSnapshotStore
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

from .test_attention_watch import (
    FIRST_RECEIPT,
    SECOND_RECEIPT,
    collection_policy_for_monitoring_test,
    snapshot_for_monitoring_test,
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
                delegation_authority=authority,
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
