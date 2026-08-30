import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

from market_impact_agent.runtime_store import (
    ArtifactStore,
    RunJournal,
    RunStatus,
    runtime_event_from_dict,
)

NOW = datetime(2026, 8, 26, 5, tzinfo=UTC)


def test_artifact_store_is_private_content_addressed_and_tamper_evident(
    tmp_path: Path,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts")

    first = store.put_json({"evidence": ["official-outage"]})
    second = store.put_json({"evidence": ["official-outage"]})

    assert second == first
    assert first.path.name == first.content_hash
    assert first.path.stat().st_mode & 0o777 == 0o600
    assert store.root.stat().st_mode & 0o777 == 0o700
    assert store.read_json(first.content_hash) == {"evidence": ["official-outage"]}

    first.path.write_text('{"tampered":true}', encoding="utf-8")
    with pytest.raises(ValueError, match="does not match its identity"):
        store.get(first.content_hash, media_type="application/json")


def test_artifact_store_rejects_symlink_substitution(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    content_hash = sha256(b"outside").hexdigest()
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside")
    os.symlink(outside, store.root / content_hash)

    with pytest.raises(FileNotFoundError, match="regular file"):
        store.get(content_hash, media_type="application/octet-stream")


def test_run_journal_is_append_only_hash_chained_and_idempotent(tmp_path: Path) -> None:
    journal = RunJournal(tmp_path / "runtime" / "runs.sqlite3")
    config_hash = sha256(b"config").hexdigest()
    journal.start_run(run_id="run-1", config_hash=config_hash, created_at=NOW)

    first = journal.append(
        run_id="run-1",
        event_id="run-1:started",
        event_type="run.started",
        observed_at=NOW,
        payload={"config_hash": config_hash},
    )
    duplicate = journal.append(
        run_id="run-1",
        event_id="run-1:started",
        event_type="run.started",
        observed_at=NOW + timedelta(seconds=1),
        payload={"config_hash": config_hash},
    )
    second = journal.append(
        run_id="run-1",
        event_id="run-1:model-1",
        event_type="model.completed",
        observed_at=NOW + timedelta(seconds=2),
        payload={"response_hash": sha256(b"response").hexdigest()},
    )

    assert duplicate == first
    assert second.previous_hash == first.event_hash
    assert journal.events("run-1") == (first, second)
    assert journal.journal_hash("run-1") == second.event_hash
    assert runtime_event_from_dict(second.to_dict()).to_dict() == second.to_dict()

    tampered_event = second.to_dict()
    tampered_event["payload"] = {"response_hash": sha256(b"tampered").hexdigest()}
    with pytest.raises(ValueError, match="payload hash"):
        runtime_event_from_dict(tampered_event)

    with pytest.raises(ValueError, match="different content"):
        journal.append(
            run_id="run-1",
            event_id="run-1:started",
            event_type="run.started",
            observed_at=NOW,
            payload={"config_hash": sha256(b"other").hexdigest()},
        )


def test_run_journal_resume_and_terminal_identity_are_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "runtime" / "runs.sqlite3"
    config_hash = sha256(b"config").hexdigest()
    first = RunJournal(path)
    first.start_run(run_id="run-1", config_hash=config_hash, created_at=NOW)
    first.append(
        run_id="run-1",
        event_id="run-1:checkpoint-1",
        event_type="context.checkpointed",
        observed_at=NOW,
        payload={"checkpoint_hash": sha256(b"checkpoint").hexdigest()},
    )

    resumed = RunJournal(path)
    record = resumed.start_run(
        run_id="run-1",
        config_hash=config_hash,
        created_at=NOW + timedelta(minutes=1),
    )
    assert record.status is RunStatus.RUNNING
    assert len(resumed.events("run-1")) == 1

    completed = resumed.finish(
        run_id="run-1",
        status=RunStatus.COMPLETED,
        finished_at=NOW + timedelta(minutes=2),
        terminal_artifact_id="judgment-1",
    )
    assert completed.terminal_artifact_id == "judgment-1"

    with pytest.raises(ValueError, match="terminal run"):
        resumed.append(
            run_id="run-1",
            event_id="run-1:late",
            event_type="late",
            observed_at=NOW + timedelta(minutes=3),
            payload={},
        )
    with pytest.raises(ValueError, match="different result"):
        resumed.finish(
            run_id="run-1",
            status=RunStatus.COMPLETED,
            finished_at=NOW + timedelta(minutes=3),
            terminal_artifact_id="judgment-2",
        )


def test_sqlite_journal_never_stores_noncanonical_payload_text(tmp_path: Path) -> None:
    journal = RunJournal(tmp_path / "runtime" / "runs.sqlite3")
    config_hash = sha256(b"config").hexdigest()
    journal.start_run(run_id="run-1", config_hash=config_hash, created_at=NOW)
    event = journal.append(
        run_id="run-1",
        event_id="run-1:event",
        event_type="test",
        observed_at=NOW,
        payload={"z": 1, "a": [2, 3]},
    )

    assert json.dumps(event.payload, sort_keys=True) == '{"a": [2, 3], "z": 1}'
    assert event.payload_hash == sha256(b'{"a":[2,3],"z":1}').hexdigest()


@pytest.mark.parametrize(
    ("column", "replacement", "error"),
    [
        ("payload_json", '{"value":"tampered"}', "payload_hash"),
        ("payload_hash", "0" * 64, "payload_hash"),
        ("event_hash", "1" * 64, "event_hash"),
        ("previous_hash", "2" * 64, "event_hash"),
    ],
)
def test_run_journal_rejects_tampered_rows_during_lookup_and_recovery(
    tmp_path: Path,
    column: str,
    replacement: str,
    error: str,
) -> None:
    path = tmp_path / column / "runs.sqlite3"
    journal = RunJournal(path)
    config_hash = sha256(b"config").hexdigest()
    journal.start_run(run_id="tamper-run", config_hash=config_hash, created_at=NOW)
    journal.append(
        run_id="tamper-run",
        event_id="tamper-run:first",
        event_type="test.first",
        observed_at=NOW,
        payload={"value": "original"},
    )
    journal.append(
        run_id="tamper-run",
        event_id="tamper-run:second",
        event_type="test.second",
        observed_at=NOW + timedelta(seconds=1),
        payload={"value": "second"},
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE events SET {column} = ? WHERE event_id = ?",
            (replacement, "tamper-run:first" if column != "previous_hash" else "tamper-run:second"),
        )

    with pytest.raises(ValueError, match=error):
        journal.event("tamper-run:second")
    with pytest.raises(ValueError, match=error):
        RunJournal(path).start_run(
            run_id="tamper-run",
            config_hash=config_hash,
            created_at=NOW + timedelta(minutes=1),
        )


def test_run_journal_run_claim_is_nonblocking_and_releasable(tmp_path: Path) -> None:
    journal = RunJournal(tmp_path / "journal.sqlite")
    first = journal.try_claim_run("claimed-run")
    assert first is not None
    assert journal.try_claim_run("claimed-run") is None
    first.release()
    reopened = journal.try_claim_run("claimed-run")
    assert reopened is not None
    reopened.release()
