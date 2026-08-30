from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread

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
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_collection_runtime import (
    PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA_V1,
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionJob,
    ProspectiveCollectionRuntime,
    collection_usage_record_from_dict,
)
from market_impact_agent.prospective_collection_tracer import (
    qualify_prospective_collection_tracer,
    write_prospective_collection_tracer_report,
)
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
    ProspectiveRollingWindow,
)
from market_impact_agent.source_acceptance import (
    SourceAcceptanceGate,
    SourceAcceptanceGateResult,
    SourceAcceptanceStatus,
    SourceRouteAcceptanceDeclaration,
    SourceRouteAcceptanceReport,
)

START = datetime(2026, 8, 28, 13, 0, tzinfo=UTC)
WINDOW_START = START - timedelta(minutes=5)
MANIFEST_HASH = canonical_hash({"provider": "fixture-market"})
SOURCE_CONFIG = {
    "schema_version": "fixture.source.v1",
    "source_config_id": "fixture-market-source",
    "route": "index_daily",
}
SOURCE_CONFIG_HASH = canonical_hash(SOURCE_CONFIG)


def _source() -> DataSourceBinding:
    return DataSourceBinding(
        provider_id="fixture-market",
        provider_version="1",
        upstream_source="fixture-index-daily",
        manifest_hash=MANIFEST_HASH,
        source_config_hash=SOURCE_CONFIG_HASH,
        required=True,
    )


def _policy() -> ProspectiveCollectionPolicy:
    return ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.MARKET_CONTEXT,
        sources=(_source(),),
        window_start=WINDOW_START,
        parameters={
            "ts_code": "000300.SH",
            "start_date": "20260828",
            "end_date": "20270828",
        },
        poll_interval_seconds=60,
        maximum_gap_seconds=180,
    )


def _rolling_policy() -> ProspectiveCollectionPolicy:
    return ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.MARKET_CONTEXT,
        sources=(_source(),),
        window_start=WINDOW_START,
        parameters={"ts_code": "000300.SH"},
        poll_interval_seconds=60,
        maximum_gap_seconds=180,
        rolling_window=ProspectiveRollingWindow(
            lookback_seconds=600,
            timezone="Asia/Shanghai",
        ),
    )


def _accepted_report(
    *,
    source: DataSourceBinding | None = None,
    capability: ObservationCapability = ObservationCapability.MARKET_CONTEXT,
) -> SourceRouteAcceptanceReport:
    bound_source = _source() if source is None else source
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=bound_source.provider_id,
        provider_version=bound_source.provider_version,
        provider_manifest_hash=bound_source.manifest_hash,
        source_config_hash=bound_source.source_config_hash or "",
        upstream_source=bound_source.upstream_source,
        capability=capability,
        rights_basis_url="https://official.example/terms",
        rights_reviewed_at=WINDOW_START,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=False,
        semantic_scope="prospective market context",
        revision_strategy="append_only_content_versions",
    )
    gates = tuple(
        SourceAcceptanceGateResult(
            gate=gate.value,
            status=SourceAcceptanceStatus.PASS.value,
            reasons=(),
        )
        for gate in SourceAcceptanceGate
    )
    core = {
        "schema_version": "market-impact.source-route-acceptance-report.v1",
        "declaration": declaration.to_dict(),
        "rights_evidence": None,
        "data_snapshot_id": "data-snapshot-accepted-fixture",
        "deterministic_replay_snapshot_id": "data-snapshot-accepted-fixture",
        "evaluated_at": START.isoformat().replace("+00:00", "Z"),
        "gates": [item.to_dict() for item in gates],
        "accepted": True,
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }
    return SourceRouteAcceptanceReport(
        report_id=f"source-route-acceptance-report-{canonical_hash(core)}",
        declaration=declaration,
        rights_evidence=None,
        data_snapshot_id="data-snapshot-accepted-fixture",
        deterministic_replay_snapshot_id="data-snapshot-accepted-fixture",
        evaluated_at=START,
        gates=gates,
        accepted=True,
    )


def _job(
    *,
    starts_at: datetime = START,
    misfire_grace_seconds: int = 30,
) -> ProspectiveCollectionJob:
    return ProspectiveCollectionJob.build(
        adapter_kind=ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION,
        collection_policy=_policy(),
        source_acceptance_report=_accepted_report(),
        source_config=SOURCE_CONFIG,
        starts_at=starts_at,
        misfire_grace_seconds=misfire_grace_seconds,
        maximum_jitter_seconds=0,
        provider_timeout_seconds=5.0,
    )


@pytest.mark.parametrize(
    "adapter_kind",
    (
        ProspectiveCollectionAdapterKind.CSRC_NEWS,
        ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE,
    ),
)
def test_collection_registration_rejects_unsupported_rolling_windows(
    adapter_kind: ProspectiveCollectionAdapterKind,
) -> None:
    policy = _rolling_policy()

    with pytest.raises(ValueError, match="rolling windows"):
        ProspectiveCollectionJob.build(
            adapter_kind=adapter_kind,
            collection_policy=policy,
            source_acceptance_report=_accepted_report(),
            source_config=SOURCE_CONFIG,
            starts_at=START,
            misfire_grace_seconds=30,
            maximum_jitter_seconds=0,
            provider_timeout_seconds=5.0,
        )


def _snapshot(
    store: LocalDataSnapshotStore,
    *,
    policy: ProspectiveCollectionPolicy,
    retrieved_at: datetime,
    close: float = 4030.0,
) -> DataSnapshot:
    source = policy.sources[0]
    raw_response_hash = store.put_raw(b'{"data":{"items":[["000300.SH"]]}}')
    raw_content_hash = store.put_raw(f'{{"close":{close}}}'.encode())
    observation = SourceObservation.build(
        capability=policy.capability,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        upstream_record_id="000300.SH:20260828",
        source_ref="https://api.example/index_daily/000300.SH",
        lineage_id="000300.SH:20260828",
        times=ObservationTimes(
            occurred_at=retrieved_at,
            published_at=None,
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
        normalized_payload={
            "upstream_publisher": "Fixture Market",
            "record": {"ts_code": "000300.SH", "close": close},
        },
        license_scope="private_research",
    )
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
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        required=True,
        status=DataFetchStatus.DATA,
        retrieved_at=retrieved_at,
        raw_response_hash=raw_response_hash,
        received_count=1,
        accepted_count=1,
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None,
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [observation.to_dict()],
        "coverage_complete": True,
        "completed_at": retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=(attempt,),
        observations=(observation,),
        coverage_complete=True,
        completed_at=retrieved_at,
    )
    store.put(snapshot)
    return snapshot


def _no_data_snapshot(
    store: LocalDataSnapshotStore,
    *,
    policy: ProspectiveCollectionPolicy,
    retrieved_at: datetime,
) -> DataSnapshot:
    source = policy.sources[0]
    raw_response_hash = store.put_raw(b'{"data":{"fields":[],"items":[]}}')
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
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        required=True,
        status=DataFetchStatus.NO_DATA,
        retrieved_at=retrieved_at,
        raw_response_hash=raw_response_hash,
        received_count=0,
        accepted_count=0,
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None,
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [],
        "coverage_complete": False,
        "completed_at": retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=(attempt,),
        observations=(),
        coverage_complete=False,
        completed_at=retrieved_at,
    )
    store.put(snapshot)
    return snapshot


def _runtime(
    tmp_path: Path,
    *,
    lease_timeout_seconds: int = 30,
    clock: Callable[[], datetime] | None = None,
) -> tuple[
    LocalDataSnapshotStore,
    ProspectiveCollectionRuntime,
    ProspectiveCollectionPolicy,
    ProspectiveCollectionJob,
]:
    store = LocalDataSnapshotStore(tmp_path / "state")
    runtime = ProspectiveCollectionRuntime(
        store,
        lease_timeout_seconds=lease_timeout_seconds,
        clock=clock,
    )
    policy = _policy()
    job = _job()
    runtime.register(
        job,
        collection_policy=policy,
        source_acceptance_report=_accepted_report(),
        source_config=SOURCE_CONFIG,
        registered_at=START,
    )
    return store, runtime, policy, job


def _unexpected_collector(
    _policy: ProspectiveCollectionPolicy,
    _source_config: dict[str, object],
    _scheduled_for: datetime,
) -> DataSnapshot:
    raise AssertionError("collector must not be called")


def _timeout_collector(
    _policy: ProspectiveCollectionPolicy,
    _source_config: dict[str, object],
    _scheduled_for: datetime,
) -> DataSnapshot:
    raise TimeoutError("fixture timeout")


def test_due_job_records_actual_receipt_and_exposes_restart_safe_health(tmp_path: Path) -> None:
    store, runtime, policy, job = _runtime(tmp_path)

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=lambda bound_policy, source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START,
        ),
    )

    source_config = runtime.source_config(job.job_id)
    assert source_config == SOURCE_CONFIG
    assert result.outcome == "success"
    assert result.scheduled_for == START
    assert result.missed_opportunities == 0
    assert result.data_snapshot_id is not None
    assert result.usage_record_id is not None
    assert ProspectiveDataJournal(store).stats(policy_id=policy.policy_id) == {
        "policy_id": policy.policy_id,
        "collection_snapshot_count": 1,
        "source_receipt_count": 1,
        "observation_sighting_count": 1,
        "unique_observation_version_count": 1,
        "deduplicated_observation_count": 0,
    }

    restarted = ProspectiveCollectionRuntime(store, lease_timeout_seconds=30)
    health = restarted.health(job.job_id, now=START + timedelta(seconds=10))
    usage_records = restarted.usage_records(job.job_id)
    usage_summary = restarted.usage_summary(job.job_id)

    assert health.last_outcome == "success"
    assert health.successful_opportunities == 1
    assert health.missed_opportunities == 0
    assert health.next_due_at == START + timedelta(seconds=60)
    assert health.execution_capability is False
    assert len(usage_records) == 1
    assert usage_records[0].record_id == result.usage_record_id
    assert (
        validate_agent_contract(
            usage_records[0].to_dict(),
            "prospective-collection-usage-record.schema.json",
        )
        == ()
    )
    assert usage_records[0].provider_attempt_count == 1
    assert usage_records[0].request_count is None
    assert usage_records[0].incremental_cost_microusd is None
    assert usage_records[0].cost_basis == "flat_subscription_not_allocated_per_request"
    assert usage_summary["record_count"] == 1
    assert usage_summary["accepted_records"] == 1
    assert usage_summary["accepted_records_unknown_records"] == 0
    assert usage_summary["request_count"] is None
    assert usage_summary["request_count_unknown_records"] == 1
    assert usage_summary["incremental_cost_microusd"] is None
    assert usage_summary["cost_basis"] == "flat_subscription_not_allocated_per_request"


def test_due_job_records_healthy_no_data_without_failure_or_backoff(tmp_path: Path) -> None:
    store, runtime, _policy, job = _runtime(tmp_path)

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=lambda bound_policy, source_config, _scheduled_for: _no_data_snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START,
        ),
    )

    health = runtime.health(job.job_id, now=START + timedelta(seconds=10))
    usage = runtime.usage_summary(job.job_id)
    assert result.outcome == "no_data"
    assert result.error_kind is None
    assert health.last_outcome == "no_data"
    assert health.last_error_kind is None
    assert health.successful_opportunities == 1
    assert health.no_data_opportunities == 1
    assert health.source_failures == 0
    assert health.backoff_until is None
    assert usage["success_count"] == 1
    assert usage["no_data_count"] == 1
    assert usage["failure_count"] == 0


def test_usage_summary_keeps_all_unknown_measurements_null(tmp_path: Path) -> None:
    _store, runtime, _policy, job = _runtime(tmp_path)

    result = runtime.run_due(job.job_id, now=START, collector=_timeout_collector)
    usage = runtime.usage_summary(job.job_id)

    assert result.outcome == "collector_failure"
    for dimension in (
        "provider_attempt_count",
        "request_count",
        "page_count",
        "response_bytes",
        "raw_artifact_bytes",
        "received_records",
        "accepted_records",
    ):
        assert usage[dimension] is None
        assert usage[f"{dimension}_unknown_records"] == 1
    assert usage["incremental_cost_microusd"] is None
    assert usage["cost_basis"] == "flat_subscription_not_allocated_per_request"


@pytest.mark.parametrize("includes_cost_fields", [False, True])
def test_usage_record_reader_preserves_existing_v1_artifacts(
    includes_cost_fields: bool,
) -> None:
    core: dict[str, object] = {
        "schema_version": PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA_V1,
        "opportunity_id": f"collection-opportunity-{canonical_hash({'fixture': 'old'})}",
        "job_id": f"prospective-collection-job-{canonical_hash({'fixture': 'job'})}",
        "scheduled_for": START.isoformat().replace("+00:00", "Z"),
        "adapter_kind": "tushare_observation",
        "outcome": "success",
        "collection_attempt_count": 1,
        "provider_attempt_count": 1,
        "request_count": 1,
        "page_count": 1,
        "response_bytes": 100,
        "raw_artifact_bytes": 120,
        "received_records": 2,
        "accepted_records": 2,
        "latency_ms": 50.0,
        "error_kind": None,
        "recorded_at": START.isoformat().replace("+00:00", "Z"),
        "execution_capability": False,
    }
    if includes_cost_fields:
        core["incremental_cost_microusd"] = None
        core["cost_basis"] = "flat_subscription_not_allocated_per_request"
    payload = {
        **core,
        "record_id": f"prospective-collection-usage-{canonical_hash(core)}",
    }

    record = collection_usage_record_from_dict(payload)

    assert record.to_dict() == payload
    assert record.cost_basis == (
        "flat_subscription_not_allocated_per_request" if includes_cost_fields else None
    )
    assert validate_agent_contract(payload, "prospective-collection-usage-record.schema.json") == ()


def test_official_provider_usage_does_not_claim_a_tushare_subscription_cost(tmp_path: Path) -> None:
    store, runtime, _policy, _job = _runtime(tmp_path)
    event_job = _register_event_job(runtime)
    event_policy = runtime.journal.policy(event_job.collection_policy_id)

    result = runtime.run_due(
        event_job.job_id,
        now=START,
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START,
        ),
    )
    usage_record = runtime.usage_records(event_job.job_id)[0]
    usage_summary = runtime.usage_summary(event_job.job_id)

    assert event_policy.capability is ObservationCapability.EVENT_REVELATION
    assert result.outcome == "success"
    assert usage_record.incremental_cost_microusd is None
    assert usage_record.cost_basis == "not_applicable"
    assert (
        validate_agent_contract(
            usage_record.to_dict(),
            "prospective-collection-usage-record.schema.json",
        )
        == ()
    )
    assert usage_summary["incremental_cost_microusd"] is None
    assert usage_summary["cost_basis"] == "not_applicable"


def test_collection_job_is_schema_valid_and_round_trips_from_private_storage(
    tmp_path: Path,
) -> None:
    _, runtime, _, job = _runtime(tmp_path)

    assert validate_agent_contract(job.to_dict(), "prospective-collection-job.schema.json") == ()
    assert runtime.job(job.job_id) == job


def test_concurrent_workers_only_run_one_collector_for_the_due_opportunity(
    tmp_path: Path,
) -> None:
    store, runtime, policy, job = _runtime(tmp_path)
    second_runtime = ProspectiveCollectionRuntime(store, lease_timeout_seconds=30)
    entered = Event()
    release = Event()
    calls = 0
    calls_lock = Lock()
    results: list[str] = []

    def blocked_collector(
        bound_policy: ProspectiveCollectionPolicy,
        source_config: dict[str, object],
        _scheduled_for: datetime,
    ) -> DataSnapshot:
        nonlocal calls
        assert source_config == SOURCE_CONFIG
        with calls_lock:
            calls += 1
        entered.set()
        assert release.wait(timeout=5)
        return _snapshot(store, policy=bound_policy, retrieved_at=START)

    thread = Thread(
        target=lambda: results.append(
            runtime.run_due(job.job_id, now=START, collector=blocked_collector).outcome
        )
    )
    thread.start()
    assert entered.wait(timeout=5)

    duplicate = second_runtime.run_due(
        job.job_id,
        now=START,
        collector=_unexpected_collector,
    )
    release.set()
    thread.join(timeout=5)

    assert duplicate.outcome == "in_progress"
    assert results == ["success"]
    assert calls == 1
    assert (
        ProspectiveDataJournal(store).stats(policy_id=policy.policy_id)["collection_snapshot_count"]
        == 1
    )


def test_expired_lease_recovers_and_rejects_the_stale_worker_capture(tmp_path: Path) -> None:
    store, runtime, policy, job = _runtime(tmp_path, lease_timeout_seconds=1)
    recovered_runtime = ProspectiveCollectionRuntime(store, lease_timeout_seconds=1)
    entered = Event()
    release = Event()
    first_outcome: list[str] = []

    def stale_collector(
        bound_policy: ProspectiveCollectionPolicy,
        source_config: dict[str, object],
        _scheduled_for: datetime,
    ) -> DataSnapshot:
        assert source_config == SOURCE_CONFIG
        entered.set()
        assert release.wait(timeout=5)
        return _snapshot(store, policy=bound_policy, retrieved_at=START, close=4030.0)

    thread = Thread(
        target=lambda: first_outcome.append(
            runtime.run_due(job.job_id, now=START, collector=stale_collector).outcome
        )
    )
    thread.start()
    assert entered.wait(timeout=5)

    recovered = recovered_runtime.run_due(
        job.job_id,
        now=START + timedelta(seconds=2),
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START + timedelta(seconds=2),
            close=4040.0,
        ),
    )
    release.set()
    thread.join(timeout=5)

    assert recovered.outcome == "success"
    assert first_outcome == ["stale_claim"]
    assert (
        ProspectiveDataJournal(store).stats(policy_id=policy.policy_id)["collection_snapshot_count"]
        == 1
    )


def test_late_worker_classifies_each_missed_opportunity_before_collecting(
    tmp_path: Path,
) -> None:
    store, runtime, policy, job = _runtime(tmp_path)
    now = START + timedelta(seconds=121)

    result = runtime.run_due(
        job.job_id,
        now=now,
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=now,
        ),
    )
    health = runtime.health(job.job_id, now=now)

    assert result.outcome == "success"
    assert result.scheduled_for == START + timedelta(seconds=120)
    assert result.missed_opportunities == 2
    assert health.missed_opportunities == 2
    assert health.incomplete_interval is True
    assert health.last_error_kind == "collection_misfire"
    assert tuple(item.outcome for item in runtime.opportunities(job.job_id)) == (
        "missed",
        "missed",
        "success",
    )
    assert (
        ProspectiveDataJournal(store).stats(policy_id=policy.policy_id)["collection_snapshot_count"]
        == 1
    )


def test_graceful_cancellation_types_the_due_opportunity_without_collecting(
    tmp_path: Path,
) -> None:
    _, runtime, _, job = _runtime(tmp_path)
    called = False

    def collector(
        _policy: ProspectiveCollectionPolicy,
        _source_config: dict[str, object],
        _scheduled_for: datetime,
    ) -> DataSnapshot:
        nonlocal called
        called = True
        raise AssertionError("cancelled job must not call its collector")

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=collector,
        cancelled=lambda: True,
    )

    assert result.outcome == "cancelled"
    assert called is False
    assert runtime.health(job.job_id, now=START).cancelled_opportunities == 1


def test_cancellation_requested_during_collection_does_not_journal_the_snapshot(
    tmp_path: Path,
) -> None:
    store, runtime, policy, job = _runtime(tmp_path, clock=lambda: START + timedelta(seconds=2))
    cancellation_requested = False

    def collector(
        bound_policy: ProspectiveCollectionPolicy,
        _source_config: dict[str, object],
        _scheduled_for: datetime,
    ) -> DataSnapshot:
        nonlocal cancellation_requested
        snapshot = _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START + timedelta(seconds=1),
        )
        cancellation_requested = True
        return snapshot

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=collector,
        cancelled=lambda: cancellation_requested,
    )

    opportunity = runtime.opportunities(job.job_id)[0]
    assert result.outcome == "cancelled"
    assert result.data_snapshot_id is None
    assert opportunity.outcome == "cancelled"
    assert opportunity.data_snapshot_id is None
    assert (
        ProspectiveDataJournal(store).stats(policy_id=policy.policy_id)["collection_snapshot_count"]
        == 0
    )


def test_opportunity_completion_uses_finish_clock_not_invocation_time(tmp_path: Path) -> None:
    snapshot_receipt = START + timedelta(seconds=3)
    finished_at = START + timedelta(seconds=5)
    store, runtime, _, job = _runtime(tmp_path, clock=lambda: finished_at)

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=snapshot_receipt,
        ),
    )

    opportunity = runtime.opportunities(job.job_id)[0]
    assert result.outcome == "success"
    assert opportunity.completed_at == finished_at
    assert opportunity.completed_at is not None
    assert opportunity.completed_at >= snapshot_receipt


def test_opportunity_completion_cannot_precede_bound_snapshot_receipt(tmp_path: Path) -> None:
    snapshot_receipt = START + timedelta(seconds=3)
    store, runtime, _, job = _runtime(
        tmp_path,
        clock=lambda: START + timedelta(seconds=1),
    )

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=snapshot_receipt,
        ),
    )

    opportunity = runtime.opportunities(job.job_id)[0]
    assert result.outcome == "success"
    assert opportunity.completed_at == snapshot_receipt


def test_collector_failure_uses_typed_backoff_before_the_next_opportunity(
    tmp_path: Path,
) -> None:
    store, runtime, _, job = _runtime(tmp_path)

    failed = runtime.run_due(
        job.job_id,
        now=START,
        collector=_timeout_collector,
    )
    backing_off = runtime.run_due(
        job.job_id,
        now=START + timedelta(seconds=30),
        collector=_unexpected_collector,
    )
    recovered = runtime.run_due(
        job.job_id,
        now=START + timedelta(seconds=60),
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START + timedelta(seconds=60),
        ),
    )

    assert failed.outcome == "collector_failure"
    assert failed.error_kind == "collector_TimeoutError"
    assert backing_off.outcome == "backing_off"
    assert recovered.outcome == "success"
    assert tuple(item.outcome for item in runtime.opportunities(job.job_id)) == (
        "collector_failure",
        "success",
    )


def test_worker_rejects_a_receipt_observed_before_its_logical_due_time(
    tmp_path: Path,
) -> None:
    store, runtime, _, job = _runtime(tmp_path)

    result = runtime.run_due(
        job.job_id,
        now=START,
        collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
            store,
            policy=bound_policy,
            retrieved_at=START - timedelta(seconds=1),
        ),
    )

    assert result.outcome == "collector_failure"
    assert result.error_kind == "collector_ValueError"
    assert result.data_snapshot_id is None


class _FailOnceJournal(ProspectiveDataJournal):
    def __init__(self, store: LocalDataSnapshotStore) -> None:
        super().__init__(store)
        self.failed = False

    def record_snapshot(
        self,
        snapshot: DataSnapshot,
        *,
        policy: ProspectiveCollectionPolicy,
    ):  # type: ignore[no-untyped-def]
        if not self.failed:
            self.failed = True
            raise OSError("fixture journal interruption")
        return super().record_snapshot(snapshot, policy=policy)


def test_staged_snapshot_resumes_the_same_opportunity_after_journal_failure(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = _FailOnceJournal(store)
    runtime = ProspectiveCollectionRuntime(store, journal=journal, lease_timeout_seconds=30)
    policy = _policy()
    job = _job()
    runtime.register(
        job,
        collection_policy=policy,
        source_acceptance_report=_accepted_report(),
        source_config=SOURCE_CONFIG,
        registered_at=START,
    )
    calls = 0

    def collector(
        bound_policy: ProspectiveCollectionPolicy,
        _source_config: dict[str, object],
        _scheduled_for: datetime,
    ) -> DataSnapshot:
        nonlocal calls
        calls += 1
        return _snapshot(store, policy=bound_policy, retrieved_at=START)

    interrupted = runtime.run_due(job.job_id, now=START, collector=collector)
    resumed = runtime.run_due(
        job.job_id,
        now=START + timedelta(seconds=60),
        collector=collector,
    )

    assert interrupted.outcome == "storage_failure"
    assert interrupted.error_kind == "journal_OSError"
    assert resumed.outcome == "success"
    assert resumed.scheduled_for == START
    assert calls == 1
    assert tuple(item.outcome for item in runtime.opportunities(job.job_id)) == ("success",)
    assert runtime.health(job.job_id, now=START + timedelta(seconds=60)).missed_opportunities == 0


def _register_event_job(runtime: ProspectiveCollectionRuntime) -> ProspectiveCollectionJob:
    event_config = {
        "schema_version": "fixture.source.v1",
        "source_config_id": "fixture-csrc-source",
        "route": "official_event",
    }
    event_source = DataSourceBinding(
        provider_id="fixture-csrc",
        provider_version="1",
        upstream_source="fixture-csrc-official-event",
        manifest_hash=canonical_hash({"provider": "fixture-csrc"}),
        source_config_hash=canonical_hash(event_config),
        required=True,
    )
    event_policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(event_source,),
        window_start=WINDOW_START,
        parameters={"keywords": [], "max_items": 20},
        poll_interval_seconds=300,
        maximum_gap_seconds=900,
    )
    event_report = _accepted_report(
        source=event_source,
        capability=ObservationCapability.EVENT_REVELATION,
    )
    event_job = ProspectiveCollectionJob.build(
        adapter_kind=ProspectiveCollectionAdapterKind.CSRC_NEWS,
        collection_policy=event_policy,
        source_acceptance_report=event_report,
        source_config=event_config,
        starts_at=START,
        misfire_grace_seconds=30,
        maximum_jitter_seconds=0,
        provider_timeout_seconds=5.0,
    )
    runtime.register(
        event_job,
        collection_policy=event_policy,
        source_acceptance_report=event_report,
        source_config=event_config,
        registered_at=START,
    )
    return event_job


def _collect_tracer_routes(
    store: LocalDataSnapshotStore,
    runtime: ProspectiveCollectionRuntime,
    jobs: tuple[ProspectiveCollectionJob, ProspectiveCollectionJob],
    *,
    retrieved_at: datetime,
) -> None:
    for job in jobs:
        policy = runtime.journal.policy(job.collection_policy_id)
        result = runtime.run_due(
            job.job_id,
            now=START,
            collector=lambda bound_policy, _source_config, _scheduled_for: _snapshot(
                store,
                policy=bound_policy,
                retrieved_at=retrieved_at,
            ),
        )
        assert result.error_kind is None
        assert result.outcome == "success"
        assert policy.policy_id == job.collection_policy_id


def test_tracer_report_binds_one_real_opportunity_per_required_route(tmp_path: Path) -> None:
    store, runtime, _, market_job = _runtime(tmp_path, clock=lambda: START)
    event_job = _register_event_job(runtime)
    _collect_tracer_routes(store, runtime, (event_job, market_job), retrieved_at=START)

    report = qualify_prospective_collection_tracer(
        runtime=runtime,
        job_ids=(event_job.job_id, market_job.job_id),
        evaluated_at=START,
    )
    report_path = write_prospective_collection_tracer_report(
        report,
        state_root=tmp_path / "state",
    )

    assert report.accepted is True
    assert (
        validate_agent_contract(
            report.to_dict(), "prospective-collection-tracer-report.schema.json"
        )
        == ()
    )
    assert report_path.name == f"{report.report_id}.json"
    assert report_path.parent.name == "collection-tracers"
    assert report_path.read_text(encoding="utf-8").endswith("\n")
    assert {item.adapter_kind for item in report.routes} == {
        ProspectiveCollectionAdapterKind.CSRC_NEWS,
        ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION,
    }
    assert all(item.outcome == "success" for item in report.routes)
    assert report.historical_pit_claim is False
    assert report.execution_capability is False


def test_tracer_rejects_opportunity_and_receipt_evidence_after_evaluated_at(
    tmp_path: Path,
) -> None:
    future = START + timedelta(seconds=10)
    store, runtime, _, market_job = _runtime(tmp_path, clock=lambda: future)
    event_job = _register_event_job(runtime)
    _collect_tracer_routes(store, runtime, (event_job, market_job), retrieved_at=future)

    report = qualify_prospective_collection_tracer(
        runtime=runtime,
        job_ids=(event_job.job_id, market_job.job_id),
        evaluated_at=START,
    )

    gates = {item.gate: item for item in report.gates}
    assert report.accepted is False
    assert any(
        reason.endswith(":opportunity_completed_after_evaluation")
        for reason in gates[
            next(gate for gate in gates if gate.value == "typed_opportunities")
        ].reasons
    )
    assert any(
        reason.endswith(":snapshot_completed_after_evaluation")
        for reason in gates[
            next(gate for gate in gates if gate.value == "actual_receipt_snapshots")
        ].reasons
    )
    assert any(
        reason.endswith(":actual_receipt_after_evaluation")
        for reason in gates[
            next(gate for gate in gates if gate.value == "actual_receipt_snapshots")
        ].reasons
    )


def test_tracer_rejects_completion_that_predates_its_snapshot_receipt(tmp_path: Path) -> None:
    receipt_at = START + timedelta(seconds=5)
    store, runtime, _, market_job = _runtime(tmp_path, clock=lambda: receipt_at)
    event_job = _register_event_job(runtime)
    _collect_tracer_routes(store, runtime, (event_job, market_job), retrieved_at=receipt_at)
    with sqlite3.connect(store.index_path) as connection:
        connection.execute(
            "UPDATE prospective_collection_opportunities SET completed_at = ?",
            (START.isoformat().replace("+00:00", "Z"),),
        )

    report = qualify_prospective_collection_tracer(
        runtime=runtime,
        job_ids=(event_job.job_id, market_job.job_id),
        evaluated_at=receipt_at,
    )

    receipt_gate = next(
        item for item in report.gates if item.gate.value == "actual_receipt_snapshots"
    )
    assert report.accepted is False
    assert all(
        f"{job.job_id}:opportunity_completion_precedes_snapshot" in receipt_gate.reasons
        for job in (event_job, market_job)
    )


def test_tracer_rejects_overdue_unmaterialized_next_due_opportunity(tmp_path: Path) -> None:
    store, runtime, _, market_job = _runtime(tmp_path, clock=lambda: START)
    event_job = _register_event_job(runtime)
    _collect_tracer_routes(store, runtime, (event_job, market_job), retrieved_at=START)

    report = qualify_prospective_collection_tracer(
        runtime=runtime,
        job_ids=(event_job.job_id, market_job.job_id),
        evaluated_at=START + timedelta(seconds=91),
    )

    interval_gate = next(
        item for item in report.gates if item.gate.value == "interval_completeness"
    )
    assert report.accepted is False
    assert f"{market_job.job_id}:overdue_opportunity_unmaterialized" in interval_gate.reasons
