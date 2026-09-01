from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ToolAccessContext,
    ToolCall,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    FrozenDataSnapshotInput,
    FrozenDataSnapshotToolBinding,
    LocalDataSnapshotStore,
    SourceObservation,
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
    ProspectiveRollingWindow,
    prospective_collection_policy_from_dict,
)
from market_impact_agent.runtime_store import ArtifactStore

FIRST_RECEIPT = datetime(2026, 8, 28, 6, 0, tzinfo=UTC)
SECOND_RECEIPT = FIRST_RECEIPT + timedelta(minutes=1)
PUBLISHED = FIRST_RECEIPT - timedelta(minutes=5)


def _source() -> DataSourceBinding:
    return DataSourceBinding(
        provider_id="syndication-feed",
        provider_version="1.0.0",
        upstream_source="official-releases",
        manifest_hash=canonical_hash({"provider": "syndication-feed"}),
        source_config_hash=canonical_hash({"source": "official-releases"}),
        required=True,
    )


def _policy(*, maximum_gap_seconds: int = 90) -> ProspectiveCollectionPolicy:
    return ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(_source(),),
        window_start=PUBLISHED - timedelta(hours=1),
        parameters={"max_items": 20},
        poll_interval_seconds=60,
        maximum_gap_seconds=maximum_gap_seconds,
    )


def _snapshot(
    store: LocalDataSnapshotStore,
    *,
    policy: ProspectiveCollectionPolicy,
    retrieved_at: datetime,
    raw_record: bytes = b"<item>policy decision</item>",
    normalized_headline: str = "Policy decision",
    occurrence_basis: OccurrenceBasis = OccurrenceBasis.SOURCE_REPORTED,
    source: DataSourceBinding | None = None,
) -> DataSnapshot:
    selected_source = _source() if source is None else source
    raw_response_hash = store.put_raw(b"<rss>policy decision</rss>")
    raw_content_hash = store.put_raw(raw_record)
    times = ObservationTimes(
        occurred_at=(
            retrieved_at if occurrence_basis is OccurrenceBasis.RETRIEVAL_OBSERVED else PUBLISHED
        ),
        published_at=PUBLISHED,
        available_at=retrieved_at,
        source_updated_at=PUBLISHED,
        aggregator_fetched_at=None,
        retrieved_at=retrieved_at,
        occurrence_basis=occurrence_basis,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    observation = SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id=selected_source.provider_id,
        provider_version=selected_source.provider_version,
        upstream_source=selected_source.upstream_source,
        upstream_record_id="release-1",
        source_ref="https://official.example/releases/1",
        lineage_id="release-1",
        times=times,
        authority_at=retrieved_at,
        authority_kind="actual_receipt",
        raw_content_hash=raw_content_hash,
        normalized_payload={
            "headline": normalized_headline,
            "publisher": "Official Example",
        },
        license_scope="public_metadata_private_research",
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=retrieved_at,
        window_start=PUBLISHED - timedelta(hours=1),
        source_policy_id=policy.policy_id,
        parameters=policy.parameters,
        sources=policy.sources,
        minimum_data_sources=1,
    )
    attempt = DataProviderAttempt(
        provider_id=selected_source.provider_id,
        provider_version=selected_source.provider_version,
        upstream_source=selected_source.upstream_source,
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


def test_policy_is_content_identified_and_schema_valid() -> None:
    policy = _policy()
    different_scope = ProspectiveCollectionPolicy.build(
        capability=policy.capability,
        sources=policy.sources,
        window_start=policy.window_start,
        parameters={"max_items": 10},
        poll_interval_seconds=policy.poll_interval_seconds,
        maximum_gap_seconds=policy.maximum_gap_seconds,
    )

    assert policy.policy_id == policy.expected_policy_id
    assert different_scope.policy_id != policy.policy_id
    assert (
        validate_agent_contract(policy.to_dict(), "prospective-collection-policy.schema.json") == ()
    )


def test_rolling_policy_resolves_each_due_time_without_mutating_its_identity() -> None:
    rolling = ProspectiveRollingWindow(
        lookback_seconds=600,
        timezone="Asia/Shanghai",
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(_source(),),
        window_start=PUBLISHED - timedelta(hours=1),
        parameters={},
        poll_interval_seconds=120,
        maximum_gap_seconds=360,
        rolling_window=rolling,
    )

    window_start, parameters = policy.resolve_query(FIRST_RECEIPT)

    assert window_start == FIRST_RECEIPT - timedelta(minutes=10)
    assert parameters == {
        "start_date": "2026-08-28 13:50:00",
        "end_date": "2026-08-28 14:00:00",
    }
    assert policy.matches_snapshot_query(
        window_start=window_start,
        parameters=parameters,
    )
    assert prospective_collection_policy_from_dict(policy.to_dict()) == policy
    assert (
        validate_agent_contract(policy.to_dict(), "prospective-collection-policy.schema.json") == ()
    )


def test_rolling_policy_can_freeze_a_provider_date_format() -> None:
    rolling = ProspectiveRollingWindow(
        lookback_seconds=7 * 24 * 60 * 60,
        timezone="Asia/Shanghai",
        datetime_format="%Y%m%d",
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(_source(),),
        window_start=PUBLISHED - timedelta(hours=1),
        parameters={"api_name": "forecast_vip"},
        poll_interval_seconds=3600,
        maximum_gap_seconds=10800,
        rolling_window=rolling,
    )

    window_start, parameters = policy.resolve_query(FIRST_RECEIPT)

    assert window_start == datetime(2026, 8, 20, 16, 0, tzinfo=UTC)
    assert parameters == {
        "api_name": "forecast_vip",
        "start_date": "20260821",
        "end_date": "20260828",
    }
    assert policy.matches_snapshot_query(
        window_start=window_start,
        parameters=parameters,
    )
    assert policy.matches_snapshot_query(
        window_start=FIRST_RECEIPT - timedelta(days=7),
        parameters=parameters,
    )


def test_journal_deduplicates_versions_but_retains_every_actual_receipt(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy()

    first = journal.record_snapshot(
        _snapshot(
            store,
            policy=policy,
            retrieved_at=FIRST_RECEIPT,
            occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
        ),
        policy=policy,
    )
    second = journal.record_snapshot(
        _snapshot(
            store,
            policy=policy,
            retrieved_at=SECOND_RECEIPT,
            occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
        ),
        policy=policy,
    )

    assert first.new_observation_versions == 1
    assert second.new_observation_versions == 0
    assert second.duplicate_observation_versions == 1
    assert journal.stats(policy_id=policy.policy_id) == {
        "policy_id": policy.policy_id,
        "collection_snapshot_count": 2,
        "source_receipt_count": 2,
        "observation_sighting_count": 2,
        "unique_observation_version_count": 1,
        "deduplicated_observation_count": 1,
    }

    refs = journal.observation_version_refs(
        policy_id=policy.policy_id,
        capability=ObservationCapability.EVENT_REVELATION,
        not_before=FIRST_RECEIPT,
        not_after=SECOND_RECEIPT,
    )
    assert len(refs) == 1
    assert refs[0].first_available_at == FIRST_RECEIPT
    assert refs[0].provider_id == _source().provider_id

    visible = journal.observations_as_of(
        capability=ObservationCapability.EVENT_REVELATION,
        not_after=FIRST_RECEIPT,
    )
    assert len(visible) == 1
    assert visible[0][0] == refs[0]
    assert visible[0][1].normalized_payload["headline"] == "Policy decision"
    assert (
        journal.observations_as_of(
            capability=ObservationCapability.EVENT_REVELATION,
            not_after=FIRST_RECEIPT - timedelta(microseconds=1),
        )
        == ()
    )


def test_changed_record_content_creates_an_append_only_revision(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy()
    journal.record_snapshot(
        _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT),
        policy=policy,
    )

    result = journal.record_snapshot(
        _snapshot(
            store,
            policy=policy,
            retrieved_at=SECOND_RECEIPT,
            raw_record=b"<item>corrected policy decision</item>",
            normalized_headline="Corrected policy decision",
        ),
        policy=policy,
    )

    assert result.new_observation_versions == 1
    assert journal.stats(policy_id=policy.policy_id)["unique_observation_version_count"] == 2


def test_freeze_builds_standard_snapshot_and_authorized_agent_tool(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy()
    for receipt in (FIRST_RECEIPT, SECOND_RECEIPT):
        journal.record_snapshot(
            _snapshot(store, policy=policy, retrieved_at=receipt),
            policy=policy,
        )

    snapshot = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=SECOND_RECEIPT + timedelta(seconds=30),
        window_start=FIRST_RECEIPT,
        frozen_at=SECOND_RECEIPT + timedelta(minutes=1),
    )

    assert snapshot.coverage_complete is True
    assert snapshot.query.as_of == SECOND_RECEIPT
    assert len(snapshot.observations) == 1
    binding = FrozenDataSnapshotToolBinding(
        name="search_prospective_event_revelation",
        version="1.0.0",
        description="Search a frozen prospective event snapshot.",
        snapshot_id=snapshot.snapshot_id,
        required_capability="news.read",
    )
    registry = ToolRegistry(ArtifactStore(tmp_path / "tool-artifacts"))
    registry.register(
        binding.descriptor(
            store,
            frozen_input=FrozenDataSnapshotInput(
                authorized_snapshot_ids=frozenset({snapshot.snapshot_id})
            ),
        )
    )
    result = asyncio.run(
        registry.execute(
            ToolCall(
                call_id="call-prospective-1",
                name="search_prospective_event_revelation",
                arguments={"query": "policy", "limit": 10},
            ),
            access=ToolAccessContext(
                allowed_capabilities=frozenset({"news.read"}),
                allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                allowed_tools=frozenset({"search_prospective_event_revelation"}),
            ),
        )
    )
    assert snapshot.snapshot_id in result.model_content
    assert "Policy decision" in result.model_content


def test_freeze_version_selection_snapshot_spans_policies_in_receipt_order(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    first_policy = _policy()
    second_source = DataSourceBinding(
        provider_id="second-feed",
        provider_version="2.0.0",
        upstream_source="market-news",
        manifest_hash=canonical_hash({"provider": "second-feed"}),
        source_config_hash=canonical_hash({"source": "market-news"}),
        required=True,
    )
    second_policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(second_source,),
        window_start=PUBLISHED - timedelta(hours=1),
        parameters={"max_items": 20},
        poll_interval_seconds=60,
        maximum_gap_seconds=90,
    )
    journal.record_snapshot(
        _snapshot(store, policy=first_policy, retrieved_at=FIRST_RECEIPT),
        policy=first_policy,
    )
    journal.record_snapshot(
        _snapshot(
            store,
            policy=second_policy,
            retrieved_at=SECOND_RECEIPT,
            raw_record=b"<item>market event</item>",
            normalized_headline="Market event",
            source=second_source,
        ),
        policy=second_policy,
    )
    all_refs = tuple(
        sorted(
            (
                *journal.observation_version_refs(
                    policy_id=first_policy.policy_id,
                    capability=ObservationCapability.EVENT_REVELATION,
                    not_before=FIRST_RECEIPT,
                    not_after=SECOND_RECEIPT,
                ),
                *journal.observation_version_refs(
                    policy_id=second_policy.policy_id,
                    capability=ObservationCapability.EVENT_REVELATION,
                    not_before=FIRST_RECEIPT,
                    not_after=SECOND_RECEIPT,
                ),
            ),
            key=lambda item: (item.first_available_at, item.version_id),
        )
    )
    version_ids = tuple(item.version_id for item in all_refs)

    snapshot = journal.freeze_version_selection_snapshot(
        selection_id="event-impact-triage-batch-selection-" + "1" * 64,
        readiness_report_id="prospective-checkpoint-readiness-report-" + "2" * 64,
        version_ids=version_ids,
        as_of=SECOND_RECEIPT + timedelta(seconds=1),
        frozen_at=SECOND_RECEIPT + timedelta(seconds=2),
    )

    assert snapshot.coverage_complete is True
    assert len(snapshot.query.sources) == 2
    assert len(snapshot.attempts) == 2
    assert tuple(item.times.available_at for item in snapshot.observations) == (
        FIRST_RECEIPT,
        SECOND_RECEIPT,
    )
    assert store.get(snapshot.snapshot_id) == snapshot
    with pytest.raises(ValueError, match="stable actual-receipt order"):
        journal.freeze_version_selection_snapshot(
            selection_id="event-impact-triage-batch-selection-" + "3" * 64,
            readiness_report_id="prospective-checkpoint-readiness-report-" + "2" * 64,
            version_ids=tuple(reversed(version_ids)),
            as_of=SECOND_RECEIPT + timedelta(seconds=1),
            frozen_at=SECOND_RECEIPT + timedelta(seconds=2),
        )


def test_freeze_fails_closed_when_poll_cadence_has_a_gap(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy(maximum_gap_seconds=60)
    journal.record_snapshot(
        _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT),
        policy=policy,
    )

    snapshot = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT + timedelta(minutes=2),
        window_start=FIRST_RECEIPT,
        frozen_at=FIRST_RECEIPT + timedelta(minutes=3),
    )

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].error_kind == "journal_cutoff_coverage_gap"
    binding = FrozenDataSnapshotToolBinding(
        name="search_prospective_event_revelation",
        version="1.0.0",
        description="Search a frozen prospective event snapshot.",
        snapshot_id=snapshot.snapshot_id,
        required_capability="news.read",
    )
    with pytest.raises(ValueError, match="requires complete source coverage"):
        binding.descriptor(
            store,
            frozen_input=FrozenDataSnapshotInput(
                authorized_snapshot_ids=frozenset({snapshot.snapshot_id})
            ),
        )
    with pytest.raises(ValueError, match="requires a complete frozen Data Snapshot"):
        journal.materialize_snapshot_parquet(snapshot_id=snapshot.snapshot_id)


def test_dataset_projection_matches_the_frozen_snapshot_window(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy(maximum_gap_seconds=180)
    journal.record_snapshot(
        _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT),
        policy=policy,
    )
    journal.record_snapshot(
        _snapshot(
            store,
            policy=policy,
            retrieved_at=FIRST_RECEIPT + timedelta(minutes=2),
            raw_record=b"<item>new policy detail</item>",
            normalized_headline="New policy detail",
        ),
        policy=policy,
    )
    window_start = FIRST_RECEIPT + timedelta(minutes=1)
    snapshot = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT + timedelta(minutes=2),
        window_start=window_start,
        frozen_at=FIRST_RECEIPT + timedelta(minutes=3),
    )

    manifest = journal.materialize_snapshot_parquet(snapshot_id=snapshot.snapshot_id)

    assert len(snapshot.observations) == 1
    assert manifest.observation_count == len(snapshot.observations)
    assert manifest.data_snapshot_id == snapshot.snapshot_id
    assert manifest.window_start == window_start
    assert manifest.coverage_complete is True


def test_parquet_projection_is_zstd_content_identified_and_private(tmp_path: Path) -> None:
    parquet = pytest.importorskip("pyarrow.parquet")
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy()
    journal.record_snapshot(
        _snapshot(store, policy=policy, retrieved_at=FIRST_RECEIPT),
        policy=policy,
    )

    snapshot = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=FIRST_RECEIPT,
        window_start=FIRST_RECEIPT - timedelta(seconds=30),
        frozen_at=FIRST_RECEIPT + timedelta(seconds=1),
    )
    first = journal.materialize_snapshot_parquet(snapshot_id=snapshot.snapshot_id)
    second = journal.materialize_snapshot_parquet(snapshot_id=snapshot.snapshot_id)

    assert first == second
    assert first.observation_count == 1
    assert first.compression == "zstd"
    assert (
        validate_agent_contract(first.to_dict(), "prospective-dataset-manifest.schema.json") == ()
    )
    partition_path = journal.dataset_root / first.partitions[0].relative_path
    assert partition_path.stat().st_mode & 0o777 == 0o600
    metadata = parquet.ParquetFile(partition_path).metadata
    assert metadata.num_rows == 1
    assert metadata.row_group(0).column(0).compression == "ZSTD"
