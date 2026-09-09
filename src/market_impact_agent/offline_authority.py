"""Domain read-only access to an existing Harness authority root.

These adapters intentionally skip the writable initializers on the durable
stores.  They only open existing regular files with SQLite's ``mode=ro`` and
reuse the established CAS, event-signature, and usage-ledger validators.
SQLite WAL readers may update shared-memory sidecars; filesystem byte identity
is not the read-only contract.
"""

from __future__ import annotations

import sqlite3
from collections import OrderedDict
from collections.abc import Generator, Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock

from market_impact_agent.data_inputs import DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.runtime_store import (
    _PRIVILEGED_EVENT_TYPES,  # pyright: ignore[reportPrivateUsage]
    ArtifactStore,
    RunClaim,
    RunJournal,
    RunRecord,
    RunStatus,
    RuntimeEvent,
    StoredArtifact,
    _run_record,  # pyright: ignore[reportPrivateUsage]
    _verified_event,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.usage_ledger import (
    StoredUsageRecord,
    UsageLedger,
    UsageRecord,
    _read_usage_records,  # pyright: ignore[reportPrivateUsage]
)


class ReadOnlyArtifactStore(ArtifactStore):
    """Read and verify an existing artifact directory without creating or chmodding it."""

    def __init__(self, root: Path) -> None:
        self.root = _required_directory(root, "artifact store")

    def put_bytes(self, payload: bytes, *, media_type: str) -> StoredArtifact:
        del payload, media_type
        raise PermissionError("read-only Artifact Store cannot write artifacts")


class ReadOnlyDataSnapshotStore(LocalDataSnapshotStore):
    """Read existing Data Snapshots and CAS records without initializing the store."""

    def __init__(self, root: Path) -> None:
        self._parsed_snapshots: OrderedDict[str, tuple[DataSnapshot, int]] = OrderedDict()
        self._parsed_snapshot_bytes = 0
        self._parsed_snapshot_lock = Lock()
        self.root = _required_directory(root, "runtime store")
        self.artifacts = ReadOnlyArtifactStore(self.root / "artifacts")
        self.index_path = _required_file(self.root / "index.sqlite3", "runtime index")
        self._event_signing_key_path = _required_file(
            self.root / ".harness-event-hmac.key", "Harness event signing key"
        )

    def _connect(self) -> sqlite3.Connection:
        connection = _readonly_connection(self.index_path)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def authority_transaction(self) -> Generator[sqlite3.Connection]:
        """Provide an existing-reader compatibility transaction with no write capability."""

        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
        finally:
            connection.close()

    def put(self, snapshot: DataSnapshot) -> None:
        del snapshot
        raise PermissionError("read-only Data Snapshot Store cannot write snapshots")

    def put_raw(self, payload: bytes) -> str:
        del payload
        raise PermissionError("read-only Data Snapshot Store cannot write raw records")

    def put_source_config(self, payload: Mapping[str, object], *, expected_hash: str) -> None:
        del payload, expected_hash
        raise PermissionError("read-only Data Snapshot Store cannot write source configuration")


class ReadOnlyRunJournal(RunJournal):
    """Verify an existing signed Run Journal without schema initialization."""

    def __init__(self, root: Path) -> None:
        self.path = _required_file(root / "index.sqlite3", "runtime index")
        key_path = _required_file(root / ".harness-event-hmac.key", "Harness event signing key")
        key = key_path.read_bytes()
        if len(key) != 32:
            raise ValueError("Harness event signing key has an invalid length")
        self._event_hmac_key = key
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT authority_id FROM harness_authority WHERE singleton = 1"
            ).fetchone()
        if row is None or not isinstance(row["authority_id"], str):
            raise ValueError("Harness authority identity is missing")
        self.harness_authority_id = row["authority_id"]

    def _connect(self) -> sqlite3.Connection:
        connection = _readonly_connection(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def authority_transaction(self) -> Generator[sqlite3.Connection]:
        raise PermissionError("read-only Run Journal cannot start an authority transaction")
        yield sqlite3.connect(":memory:")

    def try_claim_run(self, run_id: str) -> RunClaim | None:
        del run_id
        raise PermissionError("read-only Run Journal cannot claim runs")

    def start_run(
        self,
        *,
        run_id: str,
        config_hash: str,
        created_at: datetime,
        strategy_plan_artifact_hash: str | None = None,
    ) -> RunRecord:
        del run_id, config_hash, created_at, strategy_plan_artifact_hash
        raise PermissionError("read-only Run Journal cannot start runs")

    def append(
        self,
        *,
        run_id: str,
        event_id: str,
        event_type: str,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> RuntimeEvent:
        del run_id, event_id, event_type, observed_at, payload
        raise PermissionError("read-only Run Journal cannot append events")

    def finish(
        self,
        *,
        run_id: str,
        status: RunStatus,
        finished_at: datetime,
        terminal_artifact_id: str | None,
    ) -> RunRecord:
        del run_id, status, finished_at, terminal_artifact_id
        raise PermissionError("read-only Run Journal cannot finish runs")


class ReadOnlyUsageLedger(UsageLedger):
    """Verify an existing Usage Ledger chain without schema initialization."""

    def __init__(self, path: Path) -> None:
        self.path = _required_file(path, "usage ledger")

    def _connect(self) -> sqlite3.Connection:
        connection = _readonly_connection(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def append(self, record: UsageRecord) -> StoredUsageRecord:
        del record
        raise PermissionError("read-only Usage Ledger cannot append records")


@dataclass(frozen=True, slots=True)
class ReadOnlyRuntimeAuthority:
    """One audited record snapshot plus live, domain-read-only store handles."""

    store: ReadOnlyDataSnapshotStore
    journal: ReadOnlyRunJournal
    usage_ledger: ReadOnlyUsageLedger
    events: tuple[RuntimeEvent, ...]
    usage_records: tuple[StoredUsageRecord, ...]


def open_read_only_runtime_authority(runtime_store_root: Path) -> ReadOnlyRuntimeAuthority:
    """Validate one consistent read snapshot without mutating domain records.

    Every Run Journal chain, root signature, and Usage Ledger chain must verify
    before the returned source/CAS owner is exposed to a caller. Events and Usage
    records are materialized from one short SQLite read transaction. Store handles
    remain live; consumers of this audit must use its materialized record tuples.
    """

    root = _required_directory(runtime_store_root, "runtime store")
    store = ReadOnlyDataSnapshotStore(root)
    journal = ReadOnlyRunJournal(root)
    usage_ledger = ReadOnlyUsageLedger(store.index_path)
    with closing(journal._connect()) as connection:  # pyright: ignore[reportPrivateUsage]
        # BEGIN + the first SELECT pins SQLite's WAL snapshot, not the directory.
        connection.execute("BEGIN")
        events = _all_verified_events(journal, connection)
        usage_records = _read_usage_records(connection)
        runs = tuple(
            _run_record(row)
            for row in connection.execute("SELECT * FROM runs ORDER BY created_at, run_id")
        )
        _reconcile_usage_records(runs, events, usage_records)
    return ReadOnlyRuntimeAuthority(
        store=store,
        journal=journal,
        usage_ledger=usage_ledger,
        events=events,
        usage_records=usage_records,
    )


def _all_verified_events(
    journal: ReadOnlyRunJournal, connection: sqlite3.Connection
) -> tuple[RuntimeEvent, ...]:
    """Apply RunJournal's checks to every event from one read-only query."""

    rows = connection.execute(
        """
        SELECT events.*, runs.config_hash AS run_config_hash,
               runs.strategy_plan_artifact_hash AS run_strategy_plan_artifact_hash
        FROM events LEFT JOIN runs ON runs.run_id = events.run_id
        ORDER BY events.sequence
        """
    ).fetchall()
    previous_hashes: dict[str, str | None] = {}
    events: list[RuntimeEvent] = []
    for row in rows:
        if row["run_config_hash"] is None:
            raise ValueError("run journal event has no matching Run record")
        event = _verified_event(row)
        if row["run_strategy_plan_artifact_hash"] is not None or (
            journal.promotion_eligible and event.event_type in _PRIVILEGED_EVENT_TYPES
        ):
            journal._verify_privileged_event(row, event)  # pyright: ignore[reportPrivateUsage]
        previous_hash = previous_hashes.get(event.run_id)
        if event.previous_hash != previous_hash:
            raise ValueError("run journal hash chain is invalid")
        previous_hashes[event.run_id] = event.event_hash
        events.append(event)
    return tuple(events)


def _reconcile_usage_records(
    run_records: tuple[RunRecord, ...],
    events: tuple[RuntimeEvent, ...],
    usage_records: tuple[StoredUsageRecord, ...],
) -> None:
    """Bind every hash-chained Usage Record to its same-root terminal Run Record."""

    runs = {record.run_id: record for record in run_records}
    journal_hashes = {record.run_id: record.config_hash for record in runs.values()}
    for event in events:
        journal_hashes[event.run_id] = event.event_hash
    for stored in usage_records:
        usage = stored.record
        run = runs.get(usage.run_id)
        if run is None or not run.status.terminal:
            raise ValueError("Usage Ledger record has no matching terminal Run Record")
        if usage.status is not run.status:
            raise ValueError("Usage Ledger status differs from matching Run Record")
        if usage.terminal_artifact_hash != run.terminal_artifact_id:
            raise ValueError("Usage Ledger terminal artifact differs from matching Run Record")
        if usage.run_journal_hash != journal_hashes[usage.run_id]:
            raise ValueError("Usage Ledger journal hash differs from matching signed Run")


def _required_directory(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"{label} must be a real directory")
    return path.resolve()


def _required_file(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a real file")
    return path.resolve()


def _readonly_connection(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
