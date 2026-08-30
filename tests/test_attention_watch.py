from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.attention_watch import (
    AttentionWatchPolicy,
    AttentionWatchRunResult,
    AttentionWatchService,
    AttentionWatchStatus,
    attention_watch_policy_from_dict,
)
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.monitoring_scope import (
    MonitoringMatchMode,
    MonitoringScope,
    MonitoringSubjectKind,
    MonitoringSubjectRef,
    MonitoringUseClass,
    ObservationMatchClause,
    ObservationMatcher,
    RegisteredQueryTemplate,
    RetrievalPlan,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
)

FIRST_RECEIPT = datetime(2026, 8, 28, 6, 0, tzinfo=UTC)
SECOND_RECEIPT = FIRST_RECEIPT + timedelta(minutes=1)
THIRD_RECEIPT = SECOND_RECEIPT + timedelta(minutes=1)
WINDOW_START = FIRST_RECEIPT - timedelta(minutes=1)


def _source() -> DataSourceBinding:
    return DataSourceBinding(
        provider_id="official-news",
        provider_version="1",
        upstream_source="official-news",
        manifest_hash=canonical_hash({"provider": "official-news"}),
        source_config_hash=canonical_hash({"source": "official-news"}),
        required=True,
    )


def _collection_policy() -> ProspectiveCollectionPolicy:
    return ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(_source(),),
        window_start=WINDOW_START,
        parameters={"keywords": ["policy"], "max_items": 20},
        poll_interval_seconds=60,
        maximum_gap_seconds=90,
    )


def _snapshot(
    store: LocalDataSnapshotStore,
    *,
    policy: ProspectiveCollectionPolicy,
    retrieved_at: datetime,
    raw_record: bytes = b'{"headline":"Policy decision"}',
    headline: str = "Policy decision",
    status: DataFetchStatus = DataFetchStatus.DATA,
) -> DataSnapshot:
    raw_response_hash = store.put_raw(b'{"results":["policy"]}')
    observations: tuple[SourceObservation, ...]
    if status is DataFetchStatus.DATA:
        raw_content_hash = store.put_raw(raw_record)
        observations = (
            SourceObservation.build(
                capability=ObservationCapability.EVENT_REVELATION,
                provider_id=_source().provider_id,
                provider_version=_source().provider_version,
                upstream_source=_source().upstream_source,
                upstream_record_id="release-1",
                source_ref="https://official.example/releases/1",
                lineage_id="release-1",
                times=ObservationTimes(
                    occurred_at=retrieved_at,
                    published_at=FIRST_RECEIPT - timedelta(minutes=5),
                    available_at=retrieved_at,
                    source_updated_at=None,
                    aggregator_fetched_at=None,
                    retrieved_at=retrieved_at,
                    occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
                    availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
                ),
                authority_at=retrieved_at,
                authority_kind="actual_receipt",
                raw_content_hash=raw_content_hash,
                normalized_payload={"headline": headline, "publisher": "Official Example"},
                license_scope="private_research",
            ),
        )
    else:
        observations = ()
    query = DataQuery.build(
        capability=policy.capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=retrieved_at,
        window_start=policy.window_start,
        source_policy_id=policy.policy_id,
        parameters=policy.parameters,
        sources=policy.sources,
        minimum_data_sources=1,
    )
    attempt = DataProviderAttempt(
        provider_id=_source().provider_id,
        provider_version=_source().provider_version,
        upstream_source=_source().upstream_source,
        required=True,
        status=status,
        retrieved_at=retrieved_at,
        raw_response_hash=(raw_response_hash if status.completed else None),
        received_count=len(observations),
        accepted_count=len(observations),
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None if status.completed else "provider_unavailable",
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [item.to_dict() for item in observations],
        "coverage_complete": status is DataFetchStatus.DATA,
        "completed_at": retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=(attempt,),
        observations=observations,
        coverage_complete=status is DataFetchStatus.DATA,
        completed_at=retrieved_at,
    )
    store.put(snapshot)
    return snapshot


def _watch_policy(
    *,
    collection_policy: ProspectiveCollectionPolicy,
    initial_snapshot: DataSnapshot,
    expires_at: datetime = FIRST_RECEIPT + timedelta(hours=1),
    starts_at: datetime = SECOND_RECEIPT,
    maximum_polls: int = 10,
    maximum_bytes: int = 1_000_000,
) -> AttentionWatchPolicy:
    return AttentionWatchPolicy.build(
        origin_ref="event-envelope-example",
        event_cluster_key="csrc-policy-example",
        collection_policy_id=collection_policy.policy_id,
        initial_data_snapshot_id=initial_snapshot.snapshot_id,
        starts_at=starts_at,
        expires_at=expires_at,
        maximum_polls=maximum_polls,
        maximum_bytes=maximum_bytes,
        maximum_wakes=3,
        cooldown_seconds=120,
    )


def _setup(
    tmp_path: Path,
) -> tuple[
    LocalDataSnapshotStore,
    ProspectiveDataJournal,
    ProspectiveCollectionPolicy,
    DataSnapshot,
    AttentionWatchService,
]:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _collection_policy()
    first_collection = _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    journal.record_snapshot(first_collection, policy=policy)
    initial = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=policy.window_start,
        frozen_at=FIRST_RECEIPT,
    )
    service = AttentionWatchService(store, journal=journal)
    service.create(
        _watch_policy(collection_policy=policy, initial_snapshot=initial),
        created_at=FIRST_RECEIPT,
    )
    return store, journal, policy, initial, service


def _collector(snapshot: DataSnapshot) -> Callable[[ProspectiveCollectionPolicy], DataSnapshot]:
    def collect(policy: ProspectiveCollectionPolicy) -> DataSnapshot:
        assert snapshot.query.source_policy_id == policy.policy_id
        return snapshot

    return collect


def collection_policy_for_monitoring_test() -> ProspectiveCollectionPolicy:
    return _collection_policy()


def snapshot_for_monitoring_test(
    store: LocalDataSnapshotStore,
    *,
    policy: ProspectiveCollectionPolicy,
    retrieved_at: datetime,
    headline: str = "Policy decision",
) -> DataSnapshot:
    return _snapshot(
        store,
        policy=policy,
        retrieved_at=retrieved_at,
        headline=headline,
    )


def _headline_scope() -> MonitoringScope:
    return MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
        query_template_ref=f"monitoring-query-template-{'a' * 64}",
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        freshness_max_age_seconds=300,
        minimum_coverage_sources=1,
        maximum_fetches=3,
        maximum_bytes=1_000_000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
        matcher=ObservationMatcher(
            (
                ObservationMatchClause.build(
                    field_path="headline",
                    mode=MonitoringMatchMode.CONTAINS_ANY,
                    terms=("target",),
                ),
            )
        ),
    )


def _headline_template(*, maximum_match_clauses: int = 8) -> RegisteredQueryTemplate:
    return RegisteredQueryTemplate(
        template_ref=f"monitoring-query-template-{'a' * 64}",
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        maximum_match_clauses=maximum_match_clauses,
    )


def test_watch_policy_is_content_identified_and_schema_valid(tmp_path: Path) -> None:
    _, _, collection_policy, initial, service = _setup(tmp_path)
    policy = _watch_policy(collection_policy=collection_policy, initial_snapshot=initial)

    assert policy.watch_id == policy.expected_watch_id
    assert attention_watch_policy_from_dict(policy.to_dict()).to_dict() == policy.to_dict()
    assert validate_agent_contract(policy.to_dict(), "attention-watch-policy.schema.json") == ()
    assert service.state(policy.watch_id).status is AttentionWatchStatus.ACTIVE


def test_v2_watch_only_wakes_for_exact_bound_monitoring_scope(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    collection_policy = _collection_policy()
    first_collection = _snapshot(store, policy=collection_policy, retrieved_at=FIRST_RECEIPT)
    journal.record_snapshot(first_collection, policy=collection_policy)
    initial = journal.freeze_snapshot(
        policy_id=collection_policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=collection_policy.window_start,
        frozen_at=FIRST_RECEIPT,
    )
    scope = _headline_scope()
    template = _headline_template()
    plan = RetrievalPlan.bind(
        scope=scope,
        template=template,
        collection_policy=collection_policy,
    )
    watch = AttentionWatchPolicy.build(
        origin_ref="event-envelope-example",
        collection_policy_id=collection_policy.policy_id,
        initial_data_snapshot_id=initial.snapshot_id,
        starts_at=SECOND_RECEIPT,
        expires_at=FIRST_RECEIPT + timedelta(hours=1),
        maximum_polls=3,
        maximum_bytes=1_000_000,
        maximum_wakes=1,
        cooldown_seconds=0,
        monitoring_scope=scope,
        retrieval_plan=plan,
        query_template=template,
    )
    service = AttentionWatchService(store, journal=journal)
    service.create(watch, created_at=FIRST_RECEIPT)
    unrelated = _snapshot(
        store,
        policy=collection_policy,
        retrieved_at=SECOND_RECEIPT,
        raw_record=b'{"headline":"Broad feed update"}',
        headline="Broad feed update",
    )

    no_wake = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(unrelated),
    )
    state_after_unrelated = service.state(watch.watch_id)
    target = _snapshot(
        store,
        policy=collection_policy,
        retrieved_at=THIRD_RECEIPT,
        raw_record=b'{"headline":"Target issuer policy update"}',
        headline="Target issuer policy update",
    )
    wake = service.run_due(
        watch.watch_id,
        now=THIRD_RECEIPT,
        collector=_collector(target),
    )

    assert watch.schema_version == "market-impact.attention-watch-policy.v2"
    assert "event_cluster_key" not in watch.to_dict()
    assert attention_watch_policy_from_dict(watch.to_dict()).to_dict() == watch.to_dict()
    assert validate_agent_contract(watch.to_dict(), "attention-watch-policy.schema.json") == ()
    assert no_wake.outcome == "no_change"
    assert state_after_unrelated.wake_count == 0
    assert service.state(watch.watch_id).wake_count == 1
    assert wake.outcome == "triggered"
    assert wake.wake is not None


def test_v2_watch_rejects_a_matcher_that_exceeds_its_registered_template_bound(
    tmp_path: Path,
) -> None:
    _, journal, collection_policy, initial, _ = _setup(tmp_path)
    scope = MonitoringScope.build(
        origin_refs=("event-envelope-example",),
        subject=MonitoringSubjectRef(MonitoringSubjectKind.ISSUER, "cn.600000"),
        query_template_ref=f"monitoring-query-template-{'a' * 64}",
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        freshness_max_age_seconds=300,
        minimum_coverage_sources=1,
        maximum_fetches=3,
        maximum_bytes=1_000_000,
        use_class=MonitoringUseClass.PRIVATE_INTERNAL,
        matcher=ObservationMatcher(
            (
                ObservationMatchClause.build(
                    field_path="headline",
                    mode=MonitoringMatchMode.CONTAINS_ANY,
                    terms=("target",),
                ),
                ObservationMatchClause.build(
                    field_path="content",
                    mode=MonitoringMatchMode.CONTAINS_ANY,
                    terms=("policy",),
                ),
            )
        ),
    )
    broad_template = _headline_template(maximum_match_clauses=2)
    plan = RetrievalPlan.bind(
        scope=scope,
        template=broad_template,
        collection_policy=collection_policy,
    )

    with pytest.raises(ValueError, match="clause bound"):
        AttentionWatchPolicy.build(
            origin_ref="event-envelope-example",
            collection_policy_id=collection_policy.policy_id,
            initial_data_snapshot_id=initial.snapshot_id,
            starts_at=SECOND_RECEIPT,
            expires_at=FIRST_RECEIPT + timedelta(hours=1),
            maximum_polls=3,
            maximum_bytes=1_000_000,
            maximum_wakes=1,
            cooldown_seconds=0,
            monitoring_scope=scope,
            retrieval_plan=plan,
            query_template=_headline_template(maximum_match_clauses=1),
        )

    assert journal.policy(collection_policy.policy_id) == collection_policy


def test_unchanged_content_does_not_enqueue_a_wake(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    unchanged = _snapshot(store, policy=policy, retrieved_at=SECOND_RECEIPT)

    result = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(unchanged),
    )

    assert result.outcome == "no_change"
    assert result.wake is None
    assert service.pending_wakes() == ()


def test_watch_wake_time_never_precedes_collection_receipt(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    completed_at = SECOND_RECEIPT + timedelta(seconds=15)
    changed = _snapshot(
        store,
        policy=policy,
        retrieved_at=completed_at,
        raw_record=b'{"headline":"Policy decision revised"}',
        headline="Policy decision revised",
    )

    result = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(changed),
    )

    assert result.outcome == "triggered"
    assert result.wake is not None
    assert result.wake.created_at == completed_at


def test_watch_cannot_wake_after_expiry_during_collection(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(
        collection_policy=policy,
        initial_snapshot=initial,
        expires_at=SECOND_RECEIPT + timedelta(seconds=10),
    )
    service.create(watch, created_at=FIRST_RECEIPT)
    changed = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT + timedelta(seconds=15),
        raw_record=b'{"headline":"Policy decision revised after expiry"}',
        headline="Policy decision revised after expiry",
    )

    result = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(changed),
    )

    assert result.outcome == "expired"
    assert result.wake is None
    assert result.error_kind == "watch_expired_during_collection"
    assert service.state(watch.watch_id).status is AttentionWatchStatus.EXPIRED
    assert service.pending_wakes(watch_id=watch.watch_id) == ()


def test_changed_content_freezes_snapshot_and_enqueues_one_wake(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    changed = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
        raw_record=b'{"headline":"Corrected policy decision"}',
        headline="Corrected policy decision",
    )

    result = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(changed),
    )

    assert result.outcome == "triggered"
    assert result.wake is not None
    assert result.wake.prior_data_snapshot_id == initial.snapshot_id
    assert result.wake.data_snapshot_id.startswith("data-snapshot-")
    assert result.wake.execution_capability is False
    assert validate_agent_contract(result.wake.to_dict(), "attention-watch-wake.schema.json") == ()
    assert store.get(result.wake.data_snapshot_id).coverage_complete is True
    assert service.state(watch.watch_id).status is AttentionWatchStatus.TRIGGERED


def test_restart_and_repeat_sighting_do_not_duplicate_a_wake(tmp_path: Path) -> None:
    store, journal, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    changed_bytes = b'{"headline":"Corrected policy decision"}'
    first_changed = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
        raw_record=changed_bytes,
        headline="Corrected policy decision",
    )
    first = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(first_changed),
    )
    restarted = AttentionWatchService(store, journal=journal)
    repeated = _snapshot(
        store,
        policy=policy,
        retrieved_at=THIRD_RECEIPT,
        raw_record=changed_bytes,
        headline="Corrected policy decision",
    )

    second = restarted.run_due(
        watch.watch_id,
        now=THIRD_RECEIPT,
        collector=_collector(repeated),
    )

    assert first.wake is not None
    assert second.outcome == "no_change"
    assert second.wake is None
    assert len(restarted.pending_wakes()) == 1


def test_initial_aggregate_baseline_does_not_report_an_older_version_as_new(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _collection_policy()
    first = _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    second = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
        raw_record=b'{"headline":"Correction"}',
        headline="Correction",
    )
    journal.record_snapshot(first, policy=policy)
    journal.record_snapshot(second, policy=policy)
    baseline = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=SECOND_RECEIPT,
        window_start=policy.window_start,
        frozen_at=SECOND_RECEIPT,
    )
    watch = _watch_policy(
        collection_policy=policy,
        initial_snapshot=baseline,
        starts_at=THIRD_RECEIPT,
    )
    service = AttentionWatchService(store, journal=journal)
    service.create(watch, created_at=SECOND_RECEIPT)
    unchanged = _snapshot(
        store,
        policy=policy,
        retrieved_at=THIRD_RECEIPT,
        raw_record=b'{"headline":"Correction"}',
        headline="Correction",
    )

    result = service.run_due(
        watch.watch_id,
        now=THIRD_RECEIPT,
        collector=_collector(unchanged),
    )

    assert len(baseline.observations) == 2
    assert result.outcome == "no_change"
    assert result.wake is None


def test_watch_rejects_a_collection_snapshot_as_its_initial_baseline(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _collection_policy()
    collection = _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    journal.record_snapshot(collection, policy=policy)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=collection)
    service = AttentionWatchService(store, journal=journal)

    with pytest.raises(ValueError, match="not a Journal freeze"):
        service.create(watch, created_at=FIRST_RECEIPT)


def test_watch_rejects_a_frozen_baseline_with_a_shorter_policy_window(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _collection_policy()
    collection = _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    journal.record_snapshot(collection, policy=policy)
    shortened = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=WINDOW_START + timedelta(seconds=30),
        frozen_at=FIRST_RECEIPT,
    )
    watch = _watch_policy(collection_policy=policy, initial_snapshot=shortened)
    service = AttentionWatchService(store, journal=journal)

    with pytest.raises(ValueError, match="window does not match policy"):
        service.create(watch, created_at=FIRST_RECEIPT)


def test_provider_failure_is_recorded_without_a_wake(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    failed = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
        status=DataFetchStatus.ERROR,
    )

    result = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(failed),
    )

    assert result.outcome == "source_failure"
    assert result.wake is None
    assert service.state(watch.watch_id).status is AttentionWatchStatus.BACKING_OFF
    assert service.pending_wakes() == ()


def test_transient_collector_exception_backs_off_without_terminating_watch(
    tmp_path: Path,
) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    calls = 0

    def collect(_policy: ProspectiveCollectionPolicy) -> DataSnapshot:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("temporary timeout")
        return _snapshot(
            store,
            policy=policy,
            retrieved_at=THIRD_RECEIPT + timedelta(minutes=1),
        )

    first = service.run_due(watch.watch_id, now=SECOND_RECEIPT, collector=collect)
    second = service.run_due(
        watch.watch_id,
        now=THIRD_RECEIPT + timedelta(minutes=1),
        collector=collect,
    )

    assert first.outcome == "collector_failure"
    assert service.state(watch.watch_id).status is AttentionWatchStatus.BACKING_OFF
    assert second.polled is True
    assert calls == 2


def test_due_claim_prevents_concurrent_supervisors_from_exceeding_poll_budget(
    tmp_path: Path,
) -> None:
    store, journal, policy, initial, first_service = _setup(tmp_path)
    watch = _watch_policy(
        collection_policy=policy,
        initial_snapshot=initial,
        maximum_polls=1,
    )
    first_service.create(watch, created_at=FIRST_RECEIPT)
    second_service = AttentionWatchService(store, journal=journal)
    started = Event()
    release = Event()
    calls_lock = Lock()
    calls = 0
    results: list[AttentionWatchRunResult] = []
    unchanged = _snapshot(store, policy=policy, retrieved_at=SECOND_RECEIPT)

    def collect(_policy: ProspectiveCollectionPolicy) -> DataSnapshot:
        nonlocal calls
        with calls_lock:
            calls += 1
        started.set()
        assert release.wait(timeout=5)
        return unchanged

    thread = Thread(
        target=lambda: results.append(
            first_service.run_due(watch.watch_id, now=SECOND_RECEIPT, collector=collect)
        )
    )
    thread.start()
    assert started.wait(timeout=5)
    concurrent = second_service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=collect,
    )
    release.set()
    thread.join(timeout=5)

    assert concurrent.outcome == "in_progress"
    assert calls == 1
    assert len(results) == 1
    assert first_service.state(watch.watch_id).poll_count == 1


def test_expired_claim_is_recoverable_and_stale_completion_cannot_mutate_state(
    tmp_path: Path,
) -> None:
    store, journal, policy, initial, _ = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    first_service = AttentionWatchService(
        store,
        journal=journal,
        lease_timeout_seconds=30,
    )
    second_service = AttentionWatchService(
        store,
        journal=journal,
        lease_timeout_seconds=30,
    )
    unchanged = _snapshot(store, policy=policy, retrieved_at=SECOND_RECEIPT)
    started = Event()
    release = Event()
    results: list[AttentionWatchRunResult] = []

    def blocked_collect(_policy: ProspectiveCollectionPolicy) -> DataSnapshot:
        started.set()
        assert release.wait(timeout=5)
        return unchanged

    thread = Thread(
        target=lambda: results.append(
            first_service.run_due(
                watch.watch_id,
                now=SECOND_RECEIPT,
                collector=blocked_collect,
            )
        )
    )
    thread.start()
    assert started.wait(timeout=5)
    recovered = second_service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT + timedelta(seconds=31),
        collector=_collector(unchanged),
    )
    release.set()
    thread.join(timeout=5)

    assert recovered.outcome == "no_change"
    assert len(results) == 1
    assert results[0].outcome == "stale_claim"
    assert second_service.state(watch.watch_id).poll_count == 1


def test_cancel_is_idempotent_and_prevents_future_collection(tmp_path: Path) -> None:
    _, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    service.cancel(watch.watch_id, cancelled_at=SECOND_RECEIPT)
    service.cancel(watch.watch_id, cancelled_at=SECOND_RECEIPT)
    calls = 0

    def collect(_policy: ProspectiveCollectionPolicy) -> DataSnapshot:
        nonlocal calls
        calls += 1
        raise AssertionError("cancelled watch must not collect")

    result = service.run_due(watch.watch_id, now=SECOND_RECEIPT, collector=collect)

    assert result.outcome == "cancelled"
    assert service.state(watch.watch_id).status is AttentionWatchStatus.CANCELLED
    assert calls == 0


def test_cooldown_suppresses_wake_but_continues_receipt_cadence(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    first_change = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
        raw_record=b'{"headline":"First correction"}',
        headline="First correction",
    )
    second_change = _snapshot(
        store,
        policy=policy,
        retrieved_at=THIRD_RECEIPT,
        raw_record=b'{"headline":"Second correction"}',
        headline="Second correction",
    )
    repeated_second_change = _snapshot(
        store,
        policy=policy,
        retrieved_at=THIRD_RECEIPT + timedelta(minutes=1),
        raw_record=b'{"headline":"Second correction"}',
        headline="Second correction",
    )

    first = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(first_change),
    )
    during_cooldown = service.run_due(
        watch.watch_id,
        now=THIRD_RECEIPT,
        collector=_collector(second_change),
    )
    after_cooldown = service.run_due(
        watch.watch_id,
        now=THIRD_RECEIPT + timedelta(minutes=1),
        collector=_collector(repeated_second_change),
    )

    assert first.outcome == "triggered"
    assert during_cooldown.outcome == "cooldown"
    assert during_cooldown.polled is True
    assert after_cooldown.outcome == "triggered"
    assert len(service.pending_wakes(watch_id=watch.watch_id)) == 2


def test_wake_delivery_acknowledgement_is_idempotent(tmp_path: Path) -> None:
    store, _, policy, initial, service = _setup(tmp_path)
    watch = _watch_policy(collection_policy=policy, initial_snapshot=initial)
    changed = _snapshot(
        store,
        policy=policy,
        retrieved_at=SECOND_RECEIPT,
        raw_record=b'{"headline":"Correction"}',
        headline="Correction",
    )
    result = service.run_due(
        watch.watch_id,
        now=SECOND_RECEIPT,
        collector=_collector(changed),
    )
    assert result.wake is not None

    service.mark_wake_delivered(result.wake.wake_id, delivered_at=THIRD_RECEIPT)
    service.mark_wake_delivered(result.wake.wake_id, delivered_at=THIRD_RECEIPT)

    assert service.pending_wakes(watch_id=watch.watch_id) == ()


def test_expired_or_exhausted_watch_does_not_call_provider(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _collection_policy()
    first_collection = _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    journal.record_snapshot(first_collection, policy=policy)
    initial = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=policy.window_start,
        frozen_at=FIRST_RECEIPT,
    )
    service = AttentionWatchService(store, journal=journal)
    expired = _watch_policy(
        collection_policy=policy,
        initial_snapshot=initial,
        expires_at=THIRD_RECEIPT,
    )
    exhausted = _watch_policy(
        collection_policy=policy,
        initial_snapshot=initial,
        maximum_polls=0,
    )
    service.create(expired, created_at=FIRST_RECEIPT)
    service.create(exhausted, created_at=FIRST_RECEIPT)
    calls = 0

    def collect(_policy: ProspectiveCollectionPolicy) -> DataSnapshot:
        nonlocal calls
        calls += 1
        raise AssertionError("collector must not run")

    expired_result = service.run_due(expired.watch_id, now=THIRD_RECEIPT, collector=collect)
    exhausted_result = service.run_due(
        exhausted.watch_id,
        now=SECOND_RECEIPT,
        collector=collect,
    )

    assert expired_result.outcome == "expired"
    assert exhausted_result.outcome == "budget_exhausted"
    assert calls == 0
    assert service.pending_wakes() == ()
