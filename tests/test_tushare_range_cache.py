# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest

from market_impact_agent.data_acquisition import AcquisitionPending, AcquisitionUncertain
from market_impact_agent.data_inputs import (
    DataInputHarness,
    DataQuery,
    DataQueryMode,
    LocalDataSnapshotStore,
)
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from market_impact_agent.tushare_range_cache import TushareDailyRangeCache
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
    harness = DataInputHarness(store)
    harness.register(TushareDailyRangeCache(provider, store))

    def query(start: str, end: str) -> DataQuery:
        return _query(
            provider, config, {"ts_code": "510300.SH", "start_date": start, "end_date": end}
        )

    first = asyncio.run(
        harness.execute(query("20260825", "20260826"), mode=DataQueryMode.FETCH_IF_MISSING)
    )
    assert len(first.observations) == 2
    reopened_store = LocalDataSnapshotStore(tmp_path)
    reopened = DataInputHarness(reopened_store)
    reopened.register(TushareDailyRangeCache(provider, reopened_store))
    second = asyncio.run(
        reopened.execute(query("20260826", "20260827"), mode=DataQueryMode.FETCH_IF_MISSING)
    )
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
    hidden = asyncio.run(reopened.execute(earlier, mode=DataQueryMode.FETCH_IF_MISSING))
    assert hidden.observations == ()
    assert not hidden.coverage_complete
    assert len(transport.requests) == 2


def test_range_no_data_and_failure_are_durable(tmp_path: Path) -> None:
    config = load_tushare_observation_source(CONFIG)
    transport = FakeTransport([_response(config.fields, []), {"code": -1, "msg": "failed"}])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    harness = DataInputHarness(store)
    harness.register(TushareDailyRangeCache(provider, store))
    for day in ("20260825", "20260826"):
        query = _query(
            provider, config, {"ts_code": "510300.SH", "start_date": day, "end_date": day}
        )
        first = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
        again = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
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
