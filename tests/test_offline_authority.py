# pyright: reportPrivateUsage=false
from __future__ import annotations

import hmac
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_engine import RunMetrics, _PrivilegedEventSink
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.offline_authority import (
    ReadOnlyDataSnapshotStore,
    ReadOnlyRunJournal,
    open_read_only_runtime_authority,
)
from market_impact_agent.runtime_store import RunJournal, RunStatus, RuntimeEvent
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

NOW = datetime(2026, 9, 6, 1, tzinfo=UTC)


def _root_with_signed_records(
    tmp_path: Path,
    *,
    run_id: str = "offline-fixture",
    usage_journal_hash: str | None = None,
    usage_status: RunStatus = RunStatus.FAILED,
    usage_terminal_artifact_hash: str | None = None,
) -> LocalDataSnapshotStore:
    store = LocalDataSnapshotStore(tmp_path / "runtime")
    artifact = store.artifacts.put_json({"source": "fixture"})
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id=run_id, config_hash="a" * 64, created_at=NOW)
    key = (store.root / ".harness-event-hmac.key").read_bytes()
    sink = _PrivilegedEventSink(
        journal=journal,
        authority_id=store.harness_authority_id,
        signer=lambda value: hmac.new(key, value, sha256).hexdigest(),
    )
    sink.append(
        run_id=run_id,
        event_id=f"{run_id}.thesis",
        event_type="research.thesis.validated",
        observed_at=NOW,
        payload={"terminal_hash": artifact.content_hash, "binding_hash": "b" * 64},
    )
    journal.finish(
        run_id=run_id,
        status=RunStatus.FAILED,
        finished_at=NOW,
        terminal_artifact_id=artifact.content_hash,
    )
    UsageLedger(store.index_path).append(
        UsageRecord(
            experiment_id="offline-fixture",
            arm_id="fixture",
            run_id=run_id,
            recorded_at=NOW,
            status=usage_status,
            provider_profile_id="fixture",
            provider_profile_hash="c" * 64,
            execution_binding_hash="d" * 64,
            terminal_artifact_hash=(
                artifact.content_hash
                if usage_terminal_artifact_hash is None
                else usage_terminal_artifact_hash
            ),
            run_journal_hash=(
                journal.journal_hash(run_id) if usage_journal_hash is None else usage_journal_hash
            ),
            metrics=RunMetrics(
                turns=0,
                tool_calls=0,
                input_tokens=0,
                output_tokens=0,
                result_bytes=0,
                latency_ms=0,
                provider_attempts=0,
                estimated_cost_microusd=0,
            ),
        )
    )
    return store


def test_open_read_only_runtime_authority_preserves_existing_signed_state(tmp_path: Path) -> None:
    store = _root_with_signed_records(tmp_path)
    with closing(sqlite3.connect(f"{store.index_path.as_uri()}?mode=ro", uri=True)) as reader:
        before = tuple(reader.iterdump())
    artifacts_before = sorted(path.name for path in store.artifacts.root.iterdir())
    claim_root = store.index_path.parent / f".{store.index_path.name}.run-claims"
    assert not claim_root.exists()

    authority = open_read_only_runtime_authority(store.root)

    assert authority.store.artifacts.read_json(
        cast(str, authority.events[0].payload["terminal_hash"])
    ) == {"source": "fixture"}
    assert [event.event_id for event in authority.events] == ["offline-fixture.thesis"]
    assert len(authority.usage_ledger.records()) == 1
    with pytest.raises(PermissionError, match="cannot write artifacts"):
        authority.store.artifacts.put_json({"new": "artifact"})
    with pytest.raises(PermissionError, match="cannot claim runs"):
        authority.journal.try_claim_run("new-run")
    with (
        authority.store.authority_transaction() as connection,
        pytest.raises(sqlite3.OperationalError, match="readonly"),
    ):
        connection.execute("INSERT INTO harness_authority VALUES (2, 'forged')")
    with closing(sqlite3.connect(f"{store.index_path.as_uri()}?mode=ro", uri=True)) as reader:
        assert tuple(reader.iterdump()) == before
    assert sorted(path.name for path in store.artifacts.root.iterdir()) == artifacts_before
    assert not claim_root.exists()


def test_open_read_only_runtime_authority_rejects_tampered_signed_event(tmp_path: Path) -> None:
    store = _root_with_signed_records(tmp_path)
    with sqlite3.connect(store.index_path) as connection:
        connection.execute(
            "UPDATE events SET privileged_signature = ? WHERE event_id = ?",
            ("0" * 64, "offline-fixture.thesis"),
        )

    with pytest.raises(ValueError, match="signature"):
        open_read_only_runtime_authority(store.root)


def test_open_read_only_runtime_authority_rejects_foreign_usage_journal_hash(
    tmp_path: Path,
) -> None:
    store = _root_with_signed_records(tmp_path, usage_journal_hash="e" * 64)

    with pytest.raises(ValueError, match="Usage Ledger journal hash"):
        open_read_only_runtime_authority(store.root)


@pytest.mark.parametrize(
    ("usage_status", "usage_terminal_artifact_hash", "error"),
    (
        (RunStatus.COMPLETED, None, "Usage Ledger status"),
        (RunStatus.FAILED, "f" * 64, "Usage Ledger terminal artifact"),
    ),
)
def test_open_read_only_runtime_authority_rejects_usage_terminal_mismatch(
    tmp_path: Path,
    usage_status: RunStatus,
    usage_terminal_artifact_hash: str | None,
    error: str,
) -> None:
    store = _root_with_signed_records(
        tmp_path,
        usage_status=usage_status,
        usage_terminal_artifact_hash=usage_terminal_artifact_hash,
    )

    with pytest.raises(ValueError, match=error):
        open_read_only_runtime_authority(store.root)


def test_audit_keeps_one_snapshot_while_another_run_commits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _root_with_signed_records(tmp_path)
    original = ReadOnlyRunJournal._verify_privileged_event
    committed = False

    def verify_then_commit(
        journal: ReadOnlyRunJournal, row: sqlite3.Row, event: RuntimeEvent
    ) -> None:
        nonlocal committed
        original(journal, row, event)
        if not committed:
            committed = True
            # A separate WAL writer commits a valid complete Run during the audit.
            _root_with_signed_records(tmp_path, run_id="concurrent-run")

    monkeypatch.setattr(ReadOnlyRunJournal, "_verify_privileged_event", verify_then_commit)
    authority = open_read_only_runtime_authority(store.root)
    assert committed
    assert {event.run_id for event in authority.events} == {"offline-fixture"}
    assert {item.record.run_id for item in authority.usage_records} == {"offline-fixture"}
    later = open_read_only_runtime_authority(store.root)
    assert {event.run_id for event in later.events} == {"offline-fixture", "concurrent-run"}
    assert {item.record.run_id for item in later.usage_records} == {
        "offline-fixture",
        "concurrent-run",
    }


def test_readonly_compatibility_transaction_pins_its_first_read(tmp_path: Path) -> None:
    store = _root_with_signed_records(tmp_path)
    reader = ReadOnlyDataSnapshotStore(store.root)
    with reader.authority_transaction() as connection:
        assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 1
        _root_with_signed_records(tmp_path, run_id="later-run")
        assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 1
    with reader.authority_transaction() as connection:
        assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 2
