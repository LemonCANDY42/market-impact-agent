from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.monitoring_scope import (
    EffectiveMembershipContext,
    MonitoringMatchMode,
    MonitoringScope,
    MonitoringSubjectKind,
    MonitoringSubjectRef,
    MonitoringUseClass,
    ObservationMatchClause,
    ObservationMatcher,
    RegisteredQueryTemplate,
    RetrievalBarrier,
    RetrievalGapKind,
    RetrievalOutcome,
    RetrievalPlan,
    assert_scope_aware_watch_admission,
    match_scope_observation,
    resolve_retrieval,
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
ASPECT_REF = f"information-aspect-{'b' * 64}"
TAXONOMY_REF = f"taxonomy-{'c' * 64}"
MAPPING_REF = f"membership-mapping-{'d' * 64}"


def _scope(
    *,
    subject: MonitoringSubjectRef | None = None,
    matcher: ObservationMatcher | None = None,
    freshness_max_age_seconds: int = 120,
    use_class: MonitoringUseClass = MonitoringUseClass.PRIVATE_INTERNAL,
    pit_lane: DataPITLane = DataPITLane.PROSPECTIVE,
    maximum_fetches: int = 3,
    maximum_bytes: int = 10_000,
) -> MonitoringScope:
    return MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=(
            MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000")
            if subject is None
            else subject
        ),
        query_template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=pit_lane,
        freshness_max_age_seconds=freshness_max_age_seconds,
        minimum_coverage_sources=1,
        maximum_fetches=maximum_fetches,
        maximum_bytes=maximum_bytes,
        use_class=use_class,
        matcher=matcher,
    )


def _headline_matcher(mode: MonitoringMatchMode, *terms: str) -> ObservationMatcher:
    return ObservationMatcher(
        (
            ObservationMatchClause.build(
                field_path="headline",
                mode=mode,
                terms=tuple(terms),
            ),
        )
    )


def test_scope_canonical_identity_normalizes_frozen_members_and_schema() -> None:
    subject = MonitoringSubjectRef(MonitoringSubjectKind.FROZEN_SET, "focus-list-2026")
    issuer = MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000")
    instrument = MonitoringSubjectRef(MonitoringSubjectKind.INSTRUMENT, "xshg.600000")
    scope_one = MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=subject,
        frozen_members=(instrument, issuer),
        query_template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        freshness_max_age_seconds=120,
        minimum_coverage_sources=1,
        maximum_fetches=2,
        maximum_bytes=2000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
    )
    scope_two = MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=subject,
        frozen_members=(issuer, instrument),
        query_template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        freshness_max_age_seconds=120,
        minimum_coverage_sources=1,
        maximum_fetches=2,
        maximum_bytes=2000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
    )

    assert scope_one.scope_id == scope_two.scope_id == scope_one.expected_scope_id
    assert scope_one.frozen_members == (instrument, issuer)
    assert "effective_context" not in scope_one.to_dict()
    assert "information_aspect_ref" not in scope_one.to_dict()
    assert validate_agent_contract(scope_one.to_dict(), "monitoring-scope.schema.json") == ()


def test_industry_and_etf_require_frozen_effective_context() -> None:
    industry = MonitoringSubjectRef(MonitoringSubjectKind.INDUSTRY, "csrc.c25")
    context = EffectiveMembershipContext(
        taxonomy_ref=TAXONOMY_REF,
        mapping_ref=MAPPING_REF,
        effective_at=FIRST_RECEIPT,
    )
    scope = MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=industry,
        effective_context=context,
        query_template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        freshness_max_age_seconds=120,
        minimum_coverage_sources=1,
        maximum_fetches=2,
        maximum_bytes=2000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
    )
    changed_context = EffectiveMembershipContext(
        taxonomy_ref=TAXONOMY_REF,
        mapping_ref=MAPPING_REF,
        effective_at=FIRST_RECEIPT + timedelta(minutes=1),
    )
    changed = MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=industry,
        effective_context=changed_context,
        query_template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        freshness_max_age_seconds=120,
        minimum_coverage_sources=1,
        maximum_fetches=2,
        maximum_bytes=2000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
    )

    assert scope.scope_id != changed.scope_id
    with pytest.raises(ValueError, match="effective context"):
        _scope(subject=MonitoringSubjectRef(MonitoringSubjectKind.ETF, "fund.510300"))


def test_scope_rejects_urls_unregistered_references_and_arbitrary_match_paths() -> None:
    with pytest.raises(ValueError, match="non-URL"):
        MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "https://example.invalid")
    with pytest.raises(ValueError, match="allowlisted"):
        ObservationMatchClause.build(
            field_path="raw_body",
            mode=MonitoringMatchMode.CONTAINS_ANY,
            terms=("earnings",),
        )
    with pytest.raises(ValueError, match="registered"):
        _scope().build(
            origin_refs=("event-envelope-example",),
            subject=MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
            query_template_ref="https://provider.invalid/query",
            capability=ObservationCapability.EVENT_REVELATION,
            pit_lane=DataPITLane.PROSPECTIVE,
            freshness_max_age_seconds=10,
            minimum_coverage_sources=1,
            maximum_fetches=1,
            maximum_bytes=1,
            use_class=MonitoringUseClass.PUBLIC,
        )


def test_local_matcher_has_exact_contains_modes_and_never_returns_bodies(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    policy = collection_policy_for_monitoring_test()
    snapshot = snapshot_for_monitoring_test(
        store,
        policy=policy,
        retrieved_at=FIRST_RECEIPT,
        headline="Target policy decision",
    )
    observation = snapshot.observations[0]

    assert _headline_matcher(MonitoringMatchMode.EXACT, "target policy decision").matches(
        observation.normalized_payload
    ) == ("headline",)
    assert _headline_matcher(MonitoringMatchMode.CONTAINS_ALL, "target", "decision").matches(
        observation.normalized_payload
    ) == ("headline",)
    assert _headline_matcher(MonitoringMatchMode.CONTAINS_ANY, "missing", "policy").matches(
        observation.normalized_payload
    ) == ("headline",)

    match = match_scope_observation(
        _scope(matcher=_headline_matcher(MonitoringMatchMode.CONTAINS_ANY, "target")),
        observation,
    )
    assert match is not None
    assert match.matched_field_paths == ("headline",)
    assert "normalized_payload" not in match.__dataclass_fields__
    assert (
        match_scope_observation(
            _scope(
                matcher=_headline_matcher(MonitoringMatchMode.CONTAINS_ANY, "target"),
                use_class=MonitoringUseClass.PUBLIC,
            ),
            observation,
        )
        is None
    )


def test_registered_template_binds_matcher_bounds_and_nested_tushare_paths() -> None:
    policy = collection_policy_for_monitoring_test()
    nested_matcher = ObservationMatcher(
        (
            ObservationMatchClause.build(
                field_path="record.ts_code",
                mode=MonitoringMatchMode.EXACT,
                terms=("000300.sh",),
            ),
        )
    )
    scope = _scope(matcher=nested_matcher)
    template = RegisteredQueryTemplate(
        template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        allowed_match_field_paths=("record.ts_code",),
        allowed_match_modes=(MonitoringMatchMode.EXACT,),
        maximum_match_clauses=1,
        maximum_terms_per_clause=1,
        maximum_term_length=16,
    )
    plan = RetrievalPlan.bind(scope=scope, template=template, collection_policy=policy)

    assert nested_matcher.matches(
        {
            "record": {
                "ts_code": "000300.SH",
                "content": "Private source body that is never returned",
            }
        }
    ) == ("record.ts_code",)
    assert ObservationMatcher(
        (
            ObservationMatchClause.build(
                field_path="record.content",
                mode=MonitoringMatchMode.CONTAINS_ANY,
                terms=("private source",),
            ),
        )
    ).matches(
        {
            "record": {
                "title": "Index observation",
                "channels": "finance",
                "content": "Private source body that is never returned",
            }
        }
    ) == ("record.content",)
    assert (
        _headline_matcher(MonitoringMatchMode.CONTAINS_ANY, "target").matches(
            {"record": {"content": "target detail"}}
        )
        is None
    )
    assert plan.template_matcher_contract_hash == template.matcher_contract_hash
    with pytest.raises(ValueError, match="field path"):
        RetrievalPlan.bind(
            scope=_scope(matcher=_headline_matcher(MonitoringMatchMode.EXACT, "policy")),
            template=template,
            collection_policy=policy,
        )
    with pytest.raises(ValueError, match="mode"):
        RetrievalPlan.bind(
            scope=_scope(
                matcher=ObservationMatcher(
                    (
                        ObservationMatchClause.build(
                            field_path="record.ts_code",
                            mode=MonitoringMatchMode.CONTAINS_ANY,
                            terms=("000300",),
                        ),
                    )
                )
            ),
            template=template,
            collection_policy=policy,
        )


def test_scope_aware_watch_rejects_non_prospective_lane() -> None:
    modeled_scope = _scope(pit_lane=DataPITLane.MODELED)

    with pytest.raises(ValueError, match="prospective PIT lane"):
        assert_scope_aware_watch_admission(
            modeled_scope,
            collection_policy_id="prospective-collection-policy-" + "a" * 64,
        )


def test_retrieval_plan_resolves_pit_freshness_and_shared_snapshot_fan_out(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    policy = collection_policy_for_monitoring_test()
    snapshot = snapshot_for_monitoring_test(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    journal = ProspectiveDataJournal(store)
    journal.record_snapshot(snapshot, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=policy.window_start,
        frozen_at=FIRST_RECEIPT,
    )
    template = RegisteredQueryTemplate(
        template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
    )
    first_plan = RetrievalPlan.bind(scope=_scope(), template=template, collection_policy=policy)
    second_plan = RetrievalPlan.bind(
        scope=_scope(matcher=_headline_matcher(MonitoringMatchMode.CONTAINS_ANY, "policy")),
        template=template,
        collection_policy=policy,
    )

    first = resolve_retrieval(
        first_plan,
        requested_at=FIRST_RECEIPT,
        cache=store,
        cached_snapshot_id=snapshot.snapshot_id,
    )
    second = resolve_retrieval(
        second_plan,
        requested_at=FIRST_RECEIPT,
        journal=journal,
        journal_snapshot_id=frozen.snapshot_id,
    )
    stale = resolve_retrieval(
        first_plan,
        requested_at=FIRST_RECEIPT + timedelta(minutes=3),
        cache=store,
        cached_snapshot_id=snapshot.snapshot_id,
    )
    later_same_snapshot = resolve_retrieval(
        first_plan,
        requested_at=FIRST_RECEIPT + timedelta(seconds=1),
        cache=store,
        cached_snapshot_id=snapshot.snapshot_id,
    )
    modeled_scope = MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
        query_template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.MODELED,
        freshness_max_age_seconds=120,
        minimum_coverage_sources=1,
        maximum_fetches=1,
        maximum_bytes=1_000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
    )
    modeled_plan = RetrievalPlan.bind(
        scope=modeled_scope,
        template=RegisteredQueryTemplate(
            template_ref=TEMPLATE_REF,
            capability=ObservationCapability.EVENT_REVELATION,
            pit_lane=DataPITLane.MODELED,
        ),
        collection_policy=policy,
    )
    pit_blocked = resolve_retrieval(
        modeled_plan,
        requested_at=FIRST_RECEIPT,
        cache=store,
        cached_snapshot_id=snapshot.snapshot_id,
    )

    assert first.outcome is RetrievalOutcome.EXACT_CACHE_HIT
    assert second.outcome is RetrievalOutcome.JOURNAL_FREEZE
    assert first.selected_snapshot_ids == (snapshot.snapshot_id,)
    assert second.selected_snapshot_ids == (frozen.snapshot_id,)
    assert first.requested_at == FIRST_RECEIPT
    assert later_same_snapshot.resolution_id != first.resolution_id
    assert later_same_snapshot.requested_at == FIRST_RECEIPT + timedelta(seconds=1)
    assert validate_agent_contract(first_plan.to_dict(), "retrieval-plan.schema.json") == ()
    assert validate_agent_contract(first.to_dict(), "retrieval-resolution.schema.json") == ()
    assert stale.outcome is RetrievalOutcome.UNAVAILABLE
    assert RetrievalGapKind.STALE in stale.gaps
    assert stale.barrier is RetrievalBarrier.FRESHNESS
    assert RetrievalGapKind.PIT_LANE_MISMATCH in pit_blocked.gaps
    assert pit_blocked.barrier is RetrievalBarrier.PIT


def test_retrieval_rejects_future_cutoffs_and_requires_journal_after_fetch(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    policy = collection_policy_for_monitoring_test()
    template = RegisteredQueryTemplate(
        template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
    )
    plan = RetrievalPlan.bind(
        scope=_scope(maximum_fetches=1, maximum_bytes=100),
        template=template,
        collection_policy=policy,
    )
    future_snapshot = snapshot_for_monitoring_test(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
    )
    journaled_snapshot = snapshot_for_monitoring_test(
        store,
        policy=policy,
        retrieved_at=FIRST_RECEIPT,
    )
    journal = ProspectiveDataJournal(store)
    journal.register_policy(policy)
    with pytest.raises(ValueError, match="Journal freeze"):
        resolve_retrieval(
            plan,
            requested_at=FIRST_RECEIPT,
            journal=journal,
            journal_snapshot_id=journaled_snapshot.snapshot_id,
        )
    journal.record_snapshot(journaled_snapshot, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=policy.window_start,
        frozen_at=FIRST_RECEIPT,
    )

    future = resolve_retrieval(
        plan,
        requested_at=FIRST_RECEIPT,
        cache=store,
        cached_snapshot_id=future_snapshot.snapshot_id,
    )
    fetch_required = resolve_retrieval(
        plan,
        requested_at=FIRST_RECEIPT,
        fetch_permitted=True,
    )
    resolved_after_journal = resolve_retrieval(
        plan,
        requested_at=FIRST_RECEIPT,
        journal=journal,
        journal_snapshot_id=frozen.snapshot_id,
    )

    assert future.outcome is RetrievalOutcome.UNAVAILABLE
    assert RetrievalGapKind.PIT_CUTOFF_EXCEEDED in future.gaps
    assert future.barrier is RetrievalBarrier.PIT
    assert fetch_required.outcome is RetrievalOutcome.FETCH_REQUIRED
    assert resolved_after_journal.outcome is RetrievalOutcome.JOURNAL_FREEZE
    assert resolved_after_journal.selected_snapshot_ids == (frozen.snapshot_id,)
    assert (
        validate_agent_contract(
            resolved_after_journal.to_dict(), "retrieval-resolution.schema.json"
        )
        == ()
    )


def test_fetch_required_fails_closed_when_plan_budget_is_zero() -> None:
    policy = collection_policy_for_monitoring_test()
    template = RegisteredQueryTemplate(
        template_ref=TEMPLATE_REF,
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
    )
    blocked_plan = RetrievalPlan.bind(
        scope=_scope(maximum_fetches=0, maximum_bytes=0),
        template=template,
        collection_policy=policy,
    )
    open_plan = RetrievalPlan.bind(
        scope=_scope(maximum_fetches=1, maximum_bytes=1),
        template=template,
        collection_policy=policy,
    )
    blocked = resolve_retrieval(
        blocked_plan,
        requested_at=FIRST_RECEIPT,
        fetch_permitted=True,
    )
    required = resolve_retrieval(
        open_plan,
        requested_at=FIRST_RECEIPT,
        fetch_permitted=True,
    )

    assert blocked.outcome is RetrievalOutcome.UNAVAILABLE
    assert RetrievalGapKind.FETCH_BUDGET_EXHAUSTED in blocked.gaps
    assert RetrievalGapKind.BYTE_BUDGET_EXHAUSTED in blocked.gaps
    assert blocked.barrier is RetrievalBarrier.ACQUISITION
    assert required.outcome is RetrievalOutcome.FETCH_REQUIRED
