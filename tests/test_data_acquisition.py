# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from market_impact_agent.data_acquisition import (
    AcquisitionPending,
    AcquisitionUncertain,
    DurableDataAcquisition,
)
from market_impact_agent.data_inputs import DataInputHarness, DataQueryMode, LocalDataSnapshotStore
from tests.test_data_inputs import FixtureProvider, _manifest, _observation, _query, _response

MODE = DataQueryMode.DURABLE_FETCH_IF_MISSING


def test_durable_acquisition_reopens_and_separates_parameters(tmp_path: Path) -> None:
    provider = FixtureProvider(_manifest(), _response(_observation()))
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path))
    harness.register(provider)
    first = asyncio.run(harness.execute(_query(), mode=MODE))
    replay = DataInputHarness(LocalDataSnapshotStore(tmp_path))
    assert asyncio.run(replay.execute(_query(), mode=MODE)) == first
    asyncio.run(harness.execute(_query(parameters={"event": "other"}), mode=MODE))
    assert provider.calls == 2


def test_durable_failure_is_reused_without_network_retry(tmp_path: Path) -> None:
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path))
    first = asyncio.run(harness.execute(_query(), mode=MODE))
    assert first.attempts[0].error_kind == "provider_missing"
    provider = FixtureProvider(_manifest(), _response(_observation()))
    reopened = DataInputHarness(LocalDataSnapshotStore(tmp_path))
    reopened.register(provider)
    assert asyncio.run(reopened.execute(_query(), mode=MODE)) == first
    assert provider.calls == 0


def test_independent_harness_claims_and_does_not_hold_write_lock(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    first = DataInputHarness(store)
    second = DataInputHarness(LocalDataSnapshotStore(tmp_path))
    provider = FixtureProvider(_manifest(), _response(_observation()), delay_seconds=0.1)
    first.register(provider)
    second.register(provider)

    async def run() -> None:
        task = asyncio.create_task(first.execute(_query(), mode=MODE))
        while provider.calls == 0:
            await asyncio.sleep(0.001)
        # Independent SQLite writer progresses while provider I/O is pending.
        with store.authority_transaction() as connection:
            connection.execute("CREATE TABLE lock_probe (value INTEGER)")
        with pytest.raises(AcquisitionPending):
            await second.execute(_query(), mode=MODE)
        result = await task
        assert await second.execute(_query(), mode=MODE) == result

    asyncio.run(run())
    assert provider.calls == 1


def test_expired_claim_never_reissues_network(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    acquisition = DurableDataAcquisition(store)
    acquisition.claim(_query(), lease_seconds=10)
    with store.authority_transaction() as connection:
        connection.execute("UPDATE data_acquisitions SET expires_at = 0")
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path))
    provider = FixtureProvider(_manifest(), _response(_observation()))
    harness.register(provider)
    with pytest.raises(AcquisitionUncertain):
        asyncio.run(harness.execute(_query(), mode=MODE))
    assert provider.calls == 0
    with store.authority_transaction() as connection:
        assert (
            connection.execute("SELECT state FROM data_acquisitions").fetchone()[0] == "uncertain"
        )


def test_real_processes_share_the_acquisition_claim(tmp_path: Path) -> None:
    import subprocess
    import sys

    LocalDataSnapshotStore(tmp_path)
    program = """
import sys
from pathlib import Path
from market_impact_agent.data_acquisition import DurableDataAcquisition, AcquisitionPending
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from tests.test_data_inputs import _query
store = LocalDataSnapshotStore(Path(sys.argv[1]))
try:
    DurableDataAcquisition(store).claim(_query(), lease_seconds=60)
    print("claimed")
except AcquisitionPending:
    print("pending")
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", program, str(tmp_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = [process.communicate(timeout=20) for process in processes]
    assert all(process.returncode == 0 for process in processes), results
    assert sorted(output.strip() for output, _ in results) == ["claimed", "pending"]
