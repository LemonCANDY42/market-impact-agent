from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import (
    ToolAccessContext,
    ToolCall,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSourceBinding,
    DataToolBinding,
    LocalDataSnapshotStore,
    ProviderDataResponse,
    SourceObservation,
    data_snapshot_from_dict,
    sha256_bytes,
)
from market_impact_agent.observations import (
    OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
    AvailabilityBasis,
    LatencyModelReference,
    ObservationCapability,
    ObservationProviderManifest,
    ObservationTimes,
    ObservationTrustTier,
    OccurrenceBasis,
)
from market_impact_agent.providers import ProviderTransport
from market_impact_agent.runtime_store import ArtifactStore

AS_OF = datetime(2024, 9, 24, 1, 25, tzinfo=UTC)
RETRIEVED = datetime(2026, 8, 28, 2, 0, tzinfo=UTC)


@dataclass
class FixtureProvider:
    manifest: ObservationProviderManifest
    response: ProviderDataResponse
    calls: int = 0
    delay_seconds: float = 0.0

    async def fetch(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
    ) -> ProviderDataResponse:
        del query, source
        self.calls += 1
        await asyncio.sleep(self.delay_seconds)
        return self.response


def _manifest(
    provider_id: str = "official-feed",
    *,
    version: str = "2024-09",
    source: str = "official.example",
) -> ObservationProviderManifest:
    return ObservationProviderManifest(
        schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
        provider_id=provider_id,
        provider_version=version,
        transport=ProviderTransport.HTTP,
        declared_capabilities=frozenset({ObservationCapability.EVENT_REVELATION}),
        verified_capabilities=frozenset({ObservationCapability.EVENT_REVELATION}),
        upstream_sources=(source,),
        auth_required=False,
        provides_source_updated_at=True,
        provides_aggregator_fetched_at=False,
        provides_historical_occurrence_at=True,
        provides_revision_history=True,
        enabled=True,
        trust_tier=ObservationTrustTier.SOURCE_VALIDATED,
        license_note="fixture metadata only",
    )


def _source(
    provider_id: str = "official-feed",
    *,
    version: str = "2024-09",
    upstream: str = "official.example",
    required: bool = True,
) -> DataSourceBinding:
    manifest = _manifest(provider_id, version=version, source=upstream)
    return DataSourceBinding(
        provider_id=provider_id,
        provider_version=version,
        upstream_source=upstream,
        manifest_hash=canonical_hash(manifest.to_dict()),
        required=required,
    )


def _observation(
    *,
    available_at: datetime = datetime(2024, 9, 24, 1, 0, tzinfo=UTC),
    authority_at: datetime | None = datetime(2024, 9, 24, 1, 5, tzinfo=UTC),
) -> SourceObservation:
    published_at = datetime(2024, 9, 24, 0, 55, tzinfo=UTC)
    times = ObservationTimes(
        occurred_at=published_at,
        published_at=published_at,
        available_at=available_at,
        source_updated_at=published_at,
        aggregator_fetched_at=None,
        retrieved_at=RETRIEVED,
        occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
        availability_basis=AvailabilityBasis.SOURCE_REPORTED,
    )
    return SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        upstream_record_id="release-17",
        source_ref="https://official.example/releases/17",
        lineage_id="release-17",
        times=times,
        authority_at=authority_at,
        authority_kind=None if authority_at is None else "verified_archive",
        raw_content_hash=sha256_bytes(b"source-body"),
        normalized_payload={"headline": "Policy release", "affected_market": "CN"},
        license_scope="private_research",
    )


def _response(
    observation: SourceObservation | None = None,
    *,
    provider_id: str = "official-feed",
    raw_body: bytes = b"source-body",
) -> ProviderDataResponse:
    observations = () if observation is None else (observation,)
    raw_records = () if observation is None else ((observation.observation_id, raw_body),)
    return ProviderDataResponse(
        status=DataFetchStatus.NO_DATA if observation is None else DataFetchStatus.DATA,
        provider_id=provider_id,
        provider_version="2024-09",
        upstream_source="official.example",
        retrieved_at=RETRIEVED,
        raw_payload=b"raw-response",
        observations=observations,
        raw_records=raw_records,
    )


def _query(
    *,
    parameters: dict[str, object] | None = None,
    sources: tuple[DataSourceBinding, ...] | None = None,
    pit_lane: DataPITLane = DataPITLane.STRICT,
    as_of: datetime = AS_OF,
) -> DataQuery:
    return DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=pit_lane,
        as_of=as_of,
        window_start=datetime(2024, 9, 23, 1, 25, tzinfo=UTC),
        source_policy_id="official-release-policy-v1",
        parameters={"event": "policy"} if parameters is None else parameters,
        sources=(_source(),) if sources is None else sources,
        minimum_data_sources=1,
    )


def test_data_query_copies_parameters_and_has_canonical_identity() -> None:
    parameters: dict[str, object] = {"b": 2, "a": ["x"]}
    query = _query(parameters=parameters)
    parameters["b"] = 3

    assert query.parameters == {"a": ["x"], "b": 2}
    assert query == _query(parameters={"a": ["x"], "b": 2})
    assert query.query_id.startswith("data-query-")


def test_harness_fetches_persists_and_reuses_complete_snapshot(tmp_path: Path) -> None:
    provider = FixtureProvider(_manifest(), _response(_observation()))
    store = LocalDataSnapshotStore(tmp_path / "data")
    harness = DataInputHarness(store)
    harness.register(provider)
    query = _query()

    first = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
    second = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
    reopened = asyncio.run(
        DataInputHarness(LocalDataSnapshotStore(tmp_path / "data")).execute(
            query,
            mode=DataQueryMode.CACHE_ONLY,
        )
    )

    assert first.coverage_complete is True
    assert len(first.observations) == 1
    assert validate_agent_contract(first.query.to_dict(), "data-query.schema.json") == ()
    assert validate_agent_contract(first.to_dict(), "data-snapshot.schema.json") == ()
    assert second.snapshot_id == first.snapshot_id
    assert reopened.to_dict() == first.to_dict()
    assert data_snapshot_from_dict(first.to_dict()) == first
    assert store.get(first.snapshot_id) == first
    raw_hash = first.attempts[0].raw_response_hash
    assert raw_hash is not None
    assert store.artifacts.get(raw_hash, media_type="application/octet-stream").size_bytes == 12
    assert provider.calls == 1


def test_harness_verifies_and_persists_each_observation_raw_record(tmp_path: Path) -> None:
    observation = _observation()
    raw_record = b"source-body"
    response = ProviderDataResponse(
        status=DataFetchStatus.DATA,
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        retrieved_at=RETRIEVED,
        raw_payload=b"raw-response",
        observations=(observation,),
        raw_records=((observation.observation_id, raw_record),),
    )
    store = LocalDataSnapshotStore(tmp_path / "data")
    harness = DataInputHarness(store)
    harness.register(FixtureProvider(_manifest(), response))

    snapshot = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    artifact = store.artifacts.get(
        observation.raw_content_hash,
        media_type="application/octet-stream",
    )
    assert artifact.path.read_bytes() == raw_record
    assert raw_record not in canonical_json_bytes(snapshot.to_dict())


@pytest.mark.parametrize(
    ("raw_records", "error_kind"),
    [
        ((), "raw_record_set_mismatch"),
        ((("placeholder", b"wrong-body"),), "raw_record_set_mismatch"),
    ],
)
def test_harness_rejects_missing_or_misbound_raw_records(
    tmp_path: Path,
    raw_records: tuple[tuple[str, bytes], ...],
    error_kind: str,
) -> None:
    observation = _observation()
    response = ProviderDataResponse(
        status=DataFetchStatus.DATA,
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        retrieved_at=RETRIEVED,
        raw_payload=b"raw-response",
        observations=(observation,),
        raw_records=raw_records,
    )
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(FixtureProvider(_manifest(), response))

    snapshot = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert snapshot.attempts[0].error_kind == error_kind


def test_harness_rejects_raw_record_hash_mismatch(tmp_path: Path) -> None:
    observation = _observation()
    response = ProviderDataResponse(
        status=DataFetchStatus.DATA,
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        retrieved_at=RETRIEVED,
        raw_payload=b"raw-response",
        observations=(observation,),
        raw_records=((observation.observation_id, b"wrong-body"),),
    )
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(FixtureProvider(_manifest(), response))

    snapshot = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert snapshot.attempts[0].error_kind == "raw_record_hash_mismatch"


def test_harness_preserves_provider_failure_and_does_not_cache_it_as_complete(
    tmp_path: Path,
) -> None:
    source = _source()
    failed = ProviderDataResponse(
        status=DataFetchStatus.RATE_LIMITED,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        retrieved_at=RETRIEVED,
        raw_payload=None,
        observations=(),
        error_kind="rate_limit",
    )
    provider = FixtureProvider(_manifest(), failed)
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    first = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))
    provider.response = _response(_observation())
    second = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    assert first.coverage_complete is False
    assert first.attempts[0].status is DataFetchStatus.RATE_LIMITED
    assert second.coverage_complete is True
    assert first.snapshot_id != second.snapshot_id
    assert harness.store.get(first.snapshot_id) == first
    assert provider.calls == 2


def test_harness_rejects_records_not_visible_at_query_cutoff(tmp_path: Path) -> None:
    future = _observation(
        available_at=datetime(2024, 9, 24, 1, 26, tzinfo=UTC),
        authority_at=datetime(2024, 9, 24, 1, 27, tzinfo=UTC),
    )
    provider = FixtureProvider(_manifest(), _response(future))
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.observations == ()
    assert snapshot.attempts[0].rejected_after_cutoff == 1


def test_strict_lane_rejects_modeled_latency_observations(tmp_path: Path) -> None:
    published_at = datetime(2024, 9, 24, 0, 55, tzinfo=UTC)
    modeled = SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        upstream_record_id="release-modeled",
        source_ref="https://official.example/releases/modeled",
        lineage_id="release-modeled",
        times=ObservationTimes(
            occurred_at=published_at,
            published_at=published_at,
            available_at=datetime(2024, 9, 24, 1, 0, tzinfo=UTC),
            source_updated_at=published_at,
            aggregator_fetched_at=None,
            retrieved_at=RETRIEVED,
            occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
            availability_basis=AvailabilityBasis.MODELED_LATENCY,
            latency_model=LatencyModelReference(
                source_class="official-release",
                model_id="receipt-lag",
                model_version="1",
                calibration_ref="calibration-1",
            ),
        ),
        authority_at=datetime(2024, 9, 24, 1, 5, tzinfo=UTC),
        authority_kind="verified_archive",
        raw_content_hash=sha256_bytes(b"modeled-source-body"),
        normalized_payload={"headline": "Modeled policy release"},
        license_scope="private_research",
    )
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(
        FixtureProvider(_manifest(), _response(modeled, raw_body=b"modeled-source-body"))
    )

    snapshot = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.observations == ()
    assert snapshot.attempts[0].rejected_lane_mismatch == 1


def test_strict_lane_rejects_authority_that_precedes_availability(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="authority_at must not precede available_at"):
        _observation(
            available_at=datetime(2024, 9, 24, 1, 10, tzinfo=UTC),
            authority_at=datetime(2024, 9, 24, 1, 5, tzinfo=UTC),
        )


def test_harness_turns_provider_identity_drift_into_a_typed_failure(tmp_path: Path) -> None:
    provider = FixtureProvider(_manifest(), _response(_observation(), provider_id="other-feed"))
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    assert snapshot.coverage_complete is False
    assert snapshot.attempts[0].status is DataFetchStatus.ERROR
    assert snapshot.attempts[0].error_kind == "response_identity_mismatch"


def test_harness_binds_full_manifest_and_times_out_provider_calls(tmp_path: Path) -> None:
    mismatched_source = DataSourceBinding(
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        manifest_hash="0" * 64,
        required=True,
    )
    provider = FixtureProvider(_manifest(), _response(_observation()), delay_seconds=0.02)
    harness = DataInputHarness(
        LocalDataSnapshotStore(tmp_path / "manifest"),
        provider_timeout_seconds=0.001,
    )
    harness.register(provider)

    mismatch = asyncio.run(
        harness.execute(
            _query(sources=(mismatched_source,)),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )
    timeout = asyncio.run(
        harness.execute(
            _query(parameters={"event": "another-policy"}),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )

    assert mismatch.attempts[0].error_kind == "provider_manifest_mismatch"
    assert timeout.attempts[0].status is DataFetchStatus.ERROR
    assert timeout.attempts[0].error_kind == "TimeoutError"


def test_strict_modeled_and_prospective_lanes_are_distinct(tmp_path: Path) -> None:
    missing_authority = _observation(authority_at=None)
    provider = FixtureProvider(_manifest(), _response(missing_authority))

    strict_harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "strict"))
    strict_harness.register(provider)
    strict = asyncio.run(strict_harness.execute(_query(), mode=DataQueryMode.FETCH_IF_MISSING))

    modeled_harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "modeled"))
    modeled_harness.register(FixtureProvider(_manifest(), _response(missing_authority)))
    modeled = asyncio.run(
        modeled_harness.execute(
            _query(pit_lane=DataPITLane.MODELED),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )

    actual_times = ObservationTimes(
        occurred_at=datetime(2026, 8, 28, 1, 55, tzinfo=UTC),
        published_at=datetime(2026, 8, 28, 1, 55, tzinfo=UTC),
        available_at=RETRIEVED,
        source_updated_at=datetime(2026, 8, 28, 1, 55, tzinfo=UTC),
        aggregator_fetched_at=None,
        retrieved_at=RETRIEVED,
        occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    actual = SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="official-feed",
        provider_version="2024-09",
        upstream_source="official.example",
        upstream_record_id="release-live",
        source_ref="https://official.example/releases/live",
        lineage_id="release-live",
        times=actual_times,
        authority_at=RETRIEVED,
        authority_kind="actual_receipt",
        raw_content_hash=sha256_bytes(b"live-body"),
        normalized_payload={"headline": "Live release"},
        license_scope="private_research",
    )
    prospective_harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "prospective"))
    prospective_harness.register(
        FixtureProvider(_manifest(), _response(actual, raw_body=b"live-body"))
    )
    prospective = asyncio.run(
        prospective_harness.execute(
            _query(
                pit_lane=DataPITLane.PROSPECTIVE,
                as_of=datetime(2026, 8, 28, 2, 1, tzinfo=UTC),
            ),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )

    assert strict.coverage_complete is False
    assert strict.attempts[0].rejected_missing_authority == 1
    assert modeled.coverage_complete is True
    assert prospective.coverage_complete is True
    assert prospective.observations == (actual,)


def test_concurrent_identical_queries_share_one_provider_fetch(tmp_path: Path) -> None:
    provider = FixtureProvider(_manifest(), _response(_observation()), delay_seconds=0.01)
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    async def run_pair() -> tuple[str, str]:
        query = _query()
        first, second = await asyncio.gather(
            harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING),
            harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING),
        )
        return first.snapshot_id, second.snapshot_id

    first_id, second_id = asyncio.run(run_pair())

    assert first_id == second_id
    assert provider.calls == 1


def test_agent_tool_uses_bound_cutoff_sources_and_cache_only_mode(tmp_path: Path) -> None:
    provider = FixtureProvider(_manifest(), _response(_observation()))
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)
    arguments: dict[str, object] = {"event": "policy"}
    snapshot = asyncio.run(
        harness.execute(_query(parameters=arguments), mode=DataQueryMode.FETCH_IF_MISSING)
    )
    binding = DataToolBinding(
        name="lookup_event_revelation",
        version="1.0.0",
        description="Read one frozen event-revelation Data Snapshot.",
        capability=ObservationCapability.EVENT_REVELATION,
        required_capability="news.read",
        input_schema={
            "type": "object",
            "properties": {"event": {"type": "string"}},
            "required": ["event"],
            "additionalProperties": False,
        },
        as_of=AS_OF,
        window_start=datetime(2024, 9, 23, 1, 25, tzinfo=UTC),
        source_policy_id="official-release-policy-v1",
        sources=(_source(),),
        minimum_data_sources=1,
    )
    registry = ToolRegistry(ArtifactStore(tmp_path / "tool-artifacts"))
    registry.register(binding.descriptor(harness))
    access = ToolAccessContext(
        allowed_capabilities=frozenset({"news.read"}),
        allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
        allowed_tools=frozenset({"lookup_event_revelation"}),
    )

    result = asyncio.run(
        registry.execute(
            ToolCall(
                call_id="call-event-1",
                name="lookup_event_revelation",
                arguments=arguments,
            ),
            access=access,
        )
    )

    assert result.untrusted is True
    assert result.result_artifact.content_hash
    assert snapshot.snapshot_id in result.model_content
    assert provider.calls == 1
    with pytest.raises(ValueError, match="schema validation"):
        asyncio.run(
            registry.execute(
                ToolCall(
                    call_id="call-event-2",
                    name="lookup_event_revelation",
                    arguments={"event": "policy", "as_of": "2099-01-01T00:00:00Z"},
                ),
                access=access,
            )
        )
