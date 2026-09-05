# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_json_bytes
from market_impact_agent.data_acquisition import AcquisitionPending, AcquisitionUncertain
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataQuery,
    DataSnapshot,
    LocalDataSnapshotStore,
    ProviderDataResponse,
)
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from market_impact_agent.tushare_range_cache import (
    TushareDailyRangeCache,
    load_saved_range_response,
    verify_range_projection,
)
from tests.test_tushare_observation import RETRIEVED, TOKEN, FakeTransport, _query, _response

CONFIG = Path(__file__).parents[1] / "examples/providers/tushare-observation-fund-daily-v1.json"


def test_range_reopen_overlap_and_original_cutoff(tmp_path: Path) -> None:
    config = load_tushare_observation_source(CONFIG)

    def row(day: str) -> list[object]:
        return [
            "510300.SH" if name == "ts_code" else day if name == "trade_date" else 1
            for name in config.fields
        ]

    transport = FakeTransport(
        [
            _response(config.fields, [row("20260825"), row("20260826")]),
            _response(config.fields, [row("20260827")]),
        ]
    )
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    cache = TushareDailyRangeCache(provider, store)

    def query(start: str, end: str) -> DataQuery:
        return _query(
            provider, config, {"ts_code": "510300.SH", "start_date": start, "end_date": end}
        )

    first = asyncio.run(_execute(cache, query("20260825", "20260826")))
    assert len(first.observations) == 2
    reopened_store = LocalDataSnapshotStore(tmp_path)
    reopened = TushareDailyRangeCache(provider, reopened_store)
    second = asyncio.run(_execute(reopened, query("20260826", "20260827")))
    assert len(second.observations) == 2
    assert transport.requests[1]["params"] == {
        "ts_code": "510300.SH",
        "start_date": "20260827",
        "end_date": "20260827",
        "offset": 0,
        "limit": 1000,
    }
    original = query("20260825", "20260827")
    earlier = DataQuery.build(
        capability=original.capability,
        pit_lane=original.pit_lane,
        as_of=RETRIEVED - timedelta(seconds=1),
        window_start=None,
        source_policy_id=original.source_policy_id,
        parameters=original.parameters,
        sources=original.sources,
        minimum_data_sources=1,
    )
    with pytest.raises(ValueError, match="after the query cutoff"):
        asyncio.run(_execute(reopened, earlier))
    assert len(transport.requests) == 2


def test_range_no_data_and_failure_are_durable(tmp_path: Path) -> None:
    config = load_tushare_observation_source(CONFIG)
    transport = FakeTransport([_response(config.fields, []), {"code": -1, "msg": "failed"}])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    cache = TushareDailyRangeCache(provider, store)
    for day in ("20260825", "20260826"):
        query = _query(
            provider, config, {"ts_code": "510300.SH", "start_date": day, "end_date": day}
        )
        first = asyncio.run(_execute(cache, query))
        again = asyncio.run(_execute(cache, query))
        assert first == again
    assert len(transport.requests) == 2


def test_range_scope_lock_is_shared_and_crash_is_uncertain(tmp_path: Path) -> None:
    config = load_tushare_observation_source(CONFIG)
    provider = TushareObservationProvider(TOKEN, (config,), transport=FakeTransport([]))
    store = LocalDataSnapshotStore(tmp_path)
    first = TushareDailyRangeCache(provider, store)
    second = TushareDailyRangeCache(provider, LocalDataSnapshotStore(tmp_path))
    first._claim("scope", "first")
    with pytest.raises(AcquisitionPending):
        second._claim("scope", "second")
    with store.authority_transaction() as connection:
        connection.execute("UPDATE tushare_range_owners SET expires_at = 0")
    with pytest.raises(AcquisitionUncertain):
        second._claim("scope", "second")


async def _execute(
    cache: TushareDailyRangeCache, query: DataQuery, *, saved_only: bool = False
) -> DataSnapshot:
    segments = await cache.acquire(query=query, source=query.sources[0], saved_only=saved_only)
    return await cache.project(query=query, source=query.sources[0], segments=segments)


@pytest.mark.parametrize("empty_suffix", [False, True])
def test_mixed_receipt_projection_preserves_physical_proof(
    tmp_path: Path, empty_suffix: bool
) -> None:
    config = load_tushare_observation_source(CONFIG)

    def row(day: str) -> list[object]:
        return [
            "510300.SH" if name == "ts_code" else day if name == "trade_date" else 1
            for name in config.fields
        ]

    now = RETRIEVED
    transport = FakeTransport(
        [
            _response(config.fields, [row("20260825")]),
            _response(config.fields, [] if empty_suffix else [row("20260826")]),
        ]
    )
    provider = TushareObservationProvider(TOKEN, (config,), transport=transport, clock=lambda: now)
    store = LocalDataSnapshotStore(tmp_path)
    cache = TushareDailyRangeCache(provider, store)
    first_query = _query(
        provider, config, {"ts_code": "510300.SH", "start_date": "20260825", "end_date": "20260825"}
    )
    first = asyncio.run(_execute(cache, first_query))
    now += timedelta(hours=1)
    query = DataQuery.build(
        capability=first_query.capability,
        pit_lane=first_query.pit_lane,
        as_of=now,
        window_start=None,
        source_policy_id=first_query.source_policy_id,
        sources=first_query.sources,
        parameters={**first_query.parameters, "end_date": "20260826"},
        minimum_data_sources=1,
    )
    second = asyncio.run(_execute(cache, query))
    assert second.coverage_complete
    assert second.completed_at == now
    assert len(second.attempts) == 1
    assert second.observations[0] == first.observations[0]
    assert (
        store.artifacts.get(
            first.observations[0].raw_content_hash, media_type="application/octet-stream"
        ).path.read_bytes()
        == store.artifacts.get(
            second.observations[0].raw_content_hash, media_type="application/octet-stream"
        ).path.read_bytes()
    )
    from market_impact_agent.historical_ashare_inputs import (
        HistoricalAShareInputs,
        ModeledHistoricalPolicy,
    )

    historical = HistoricalAShareInputs(
        store=LocalDataSnapshotStore(tmp_path),
        snapshot_ids=(second.snapshot_id,),
        rule_artifact_hashes=(),
        policy=ModeledHistoricalPolicy("range-test", Decimal("0.01")),
    )
    assert len(historical._tables()[0].rows) == (1 if empty_suffix else 2)
    hashes = verify_range_projection(LocalDataSnapshotStore(tmp_path), second)
    assert len(hashes) == 4
    manifest_hash = second.attempts[0].raw_response_hash
    assert manifest_hash is not None
    manifest = cast(dict[str, object], store.artifacts.read_json(manifest_hash))
    segments = cast(list[dict[str, str]], manifest["segments"])
    physical = load_saved_range_response(store, segments[0]["response_artifact_hash"])
    with pytest.raises(ValueError, match="receipt must match"):
        replace(physical, retrieved_at=now)
    assert hashes.issubset(set(historical._tables()[0].hashes))
    reopened = TushareDailyRangeCache(provider, LocalDataSnapshotStore(tmp_path))
    assert asyncio.run(_execute(reopened, query, saved_only=True)) == second
    assert len(transport.requests) == 2
    earlier = DataQuery.build(
        capability=query.capability,
        pit_lane=query.pit_lane,
        as_of=RETRIEVED,
        window_start=None,
        source_policy_id=query.source_policy_id,
        sources=query.sources,
        parameters=query.parameters,
        minimum_data_sources=1,
    )
    with pytest.raises(ValueError, match="after the query cutoff"):
        asyncio.run(_execute(reopened, earlier, saved_only=True))


@pytest.mark.parametrize(
    "damage",
    [
        "missing_response",
        "corrupt_response",
        "missing_raw",
        "interval",
        "instrument",
        "source",
        "active_owner",
        "gap",
    ],
)
@pytest.mark.parametrize("saved_only", [False, True])
def test_saved_only_recovery_reopens_evidence_and_never_fetches(
    tmp_path: Path, damage: str, saved_only: bool
) -> None:
    config = load_tushare_observation_source(CONFIG)
    transport = FakeTransport([_response(config.fields, [])])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    cache = TushareDailyRangeCache(provider, store)
    query = _query(
        provider, config, {"ts_code": "510300.SH", "start_date": "20260825", "end_date": "20260825"}
    )
    segments = asyncio.run(cache.acquire(query=query, source=query.sources[0]))
    snapshot = asyncio.run(cache.project(query=query, source=query.sources[0], segments=segments))
    digest = segments[0][2]
    response = load_saved_range_response(store, digest)
    with store.authority_transaction() as connection:
        connection.execute("UPDATE tushare_range_owners SET state = 'uncertain'")
        if damage == "active_owner":
            connection.execute(
                "UPDATE tushare_range_owners SET state = 'running', expires_at = 1e20"
            )
        if damage == "interval":
            connection.execute("UPDATE tushare_range_responses SET start_date = '20260824'")
        if damage == "gap":
            connection.execute("DELETE FROM tushare_range_responses")
    if damage == "missing_response":
        store.artifacts.get(digest, media_type="application/octet-stream").path.unlink()
    if damage == "corrupt_response":
        store.artifacts.get(digest, media_type="application/octet-stream").path.write_bytes(
            b"corrupt"
        )
    if damage == "missing_raw":
        assert response.raw_response_hash is not None
        store.artifacts.get(
            response.raw_response_hash, media_type="application/octet-stream"
        ).path.unlink()
    if damage in {"instrument", "source"}:
        value = cast(dict[str, object], store.artifacts.read_json(digest))
        if damage == "source":
            value["upstream_source"] = "different-source"
        else:
            # Valid CAS and DB metadata do not prove the captured instrument.
            raw = response.raw_payload
            assert raw is not None
            raw = raw.replace(b"510300.SH", b"510500.SH")
            import base64

            value["raw_payload"] = base64.b64encode(raw).decode()
            store.put_raw(raw)
        changed = store.artifacts.put_json(value)
        with store.authority_transaction() as connection:
            connection.execute(
                "UPDATE tushare_range_responses SET artifact_hash = ?", (changed.content_hash,)
            )
    reopened = TushareDailyRangeCache(provider, LocalDataSnapshotStore(tmp_path))
    for _ in range(2):
        with pytest.raises(
            (ValueError, LookupError, OSError, AcquisitionPending, AcquisitionUncertain)
        ):
            asyncio.run(_execute(reopened, query, saved_only=saved_only))
    assert len(transport.requests) == 1
    if damage in {"missing_response", "corrupt_response", "missing_raw"}:
        with pytest.raises((ValueError, LookupError, OSError)):
            verify_range_projection(LocalDataSnapshotStore(tmp_path), snapshot)


def test_new_request_uses_complete_uncertain_scope_without_reset(tmp_path: Path) -> None:
    config = load_tushare_observation_source(CONFIG)
    transport = FakeTransport([_response(config.fields, [])])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    cache = TushareDailyRangeCache(provider, store)
    query = _query(
        provider, config, {"ts_code": "510300.SH", "start_date": "20260825", "end_date": "20260825"}
    )
    original = asyncio.run(cache.acquire(query=query, source=query.sources[0]))
    with store.authority_transaction() as connection:
        connection.execute("UPDATE tushare_range_owners SET state = 'uncertain'")
    assert asyncio.run(cache.acquire(query=query, source=query.sources[0])) == original
    with store.authority_transaction() as connection:
        assert connection.execute("SELECT state FROM tushare_range_owners").fetchone()[0] == (
            "uncertain"
        )
    assert len(transport.requests) == 1


def _legacy_range_response(
    cache: TushareDailyRangeCache,
    query: DataQuery,
    segments: tuple[tuple[str, str, str, bool], ...],
) -> ProviderDataResponse:
    """The original v1 decorator's physical response shape, before v2 existed."""
    physical = [load_saved_range_response(cache.store, row[2]) for row in segments]
    observations = tuple(item for response in physical for item in response.observations)
    return replace(
        physical[0],
        status=DataFetchStatus.DATA if observations else DataFetchStatus.NO_DATA,
        retrieved_at=max(response.retrieved_at for response in physical),
        raw_payload=canonical_json_bytes(
            {
                "schema_version": "market-impact.tushare-range-projection.v1",
                "scope_id": cache._scope(query, query.sources[0])[0],
                "parameters": query.parameters,
                "raw_response_hashes": [response.raw_response_hash for response in physical],
            }
        ),
        observations=observations,
        raw_records=tuple(item for response in physical for item in response.raw_records),
    )


@pytest.mark.parametrize("multiple", [False, True])
@pytest.mark.parametrize("damage", [None, "response", "raw", "index", "interval", "record"])
def test_legacy_v1_projection_reopens_verified_constituent_closure(
    tmp_path: Path, multiple: bool, damage: str | None
) -> None:
    from market_impact_agent.historical_ashare_inputs import (
        HistoricalAShareInputs,
        ModeledHistoricalPolicy,
    )

    config = load_tushare_observation_source(CONFIG)

    def row(day: str) -> list[object]:
        return [
            "510300.SH" if name == "ts_code" else day if name == "trade_date" else 1
            for name in config.fields
        ]

    transport = FakeTransport(
        [
            _response(config.fields, [row("20260825")]),
            _response(config.fields, [row("20260826")]),
        ]
    )
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    cache = TushareDailyRangeCache(provider, store)
    query = _query(
        provider, config, {"ts_code": "510300.SH", "start_date": "20260825", "end_date": "20260825"}
    )
    segments = asyncio.run(cache.acquire(query=query, source=query.sources[0]))
    if multiple:
        query = cache.segment_query(query, "20260825", "20260826")
        segments = asyncio.run(cache.acquire(query=query, source=query.sources[0]))
    response = _legacy_range_response(cache, query, segments)
    harness = DataInputHarness(store)
    harness.register(provider)
    snapshot = asyncio.run(harness.snapshot_from_response(query, response))
    store.put(snapshot)
    original = snapshot.to_dict()
    proof_hash = segments[0][2]
    if damage == "response":
        store.artifacts.get(proof_hash, media_type="application/json").path.write_bytes(b"corrupt")
    elif damage == "raw":
        digest = load_saved_range_response(store, proof_hash).raw_response_hash
        assert digest is not None
        store.artifacts.get(digest, media_type="application/octet-stream").path.unlink()
    elif damage in {"index", "interval"}:
        with store.authority_transaction() as connection:
            if damage == "index":
                connection.execute("DELETE FROM tushare_range_responses")
            else:
                connection.execute(
                    "UPDATE tushare_range_responses SET start_date = '20260824' "
                    "WHERE artifact_hash = ?",
                    (proof_hash,),
                )
    elif damage == "record":
        artifact = cast(dict[str, object], store.artifacts.read_json(proof_hash))
        artifact["raw_records"] = []
        changed = store.artifacts.put_json(artifact)
        with store.authority_transaction() as connection:
            connection.execute(
                "UPDATE tushare_range_responses SET artifact_hash = ? WHERE artifact_hash = ?",
                (changed.content_hash, proof_hash),
            )
    reopened = LocalDataSnapshotStore(tmp_path)
    historical = HistoricalAShareInputs(
        store=reopened,
        snapshot_ids=(snapshot.snapshot_id,),
        rule_artifact_hashes=(),
        policy=ModeledHistoricalPolicy("legacy-range-test", Decimal("0.01")),
    )
    if damage is None:
        tables = historical._tables()
        assert len(tables[0].rows) == (2 if multiple else 1)
        assert proof_hash in tables[0].hashes
        assert verify_range_projection(reopened, snapshot).issubset(set(tables[0].hashes))
    else:
        with pytest.raises(ValueError, match="physical proof"):
            historical._tables()
    assert reopened.get(snapshot.snapshot_id).to_dict() == original
    assert len(transport.requests) == (2 if multiple else 1)
