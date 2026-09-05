from __future__ import annotations

import fcntl
import hmac
import json
import os
import re
import sqlite3
import stat
import tempfile
import threading
from collections.abc import Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_json_bytes
from market_impact_agent.domain import require_aware


class RunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BUDGET_EXHAUSTED = "budget_exhausted"
    HUMAN_INPUT_REQUIRED = "human_input_required"

    @property
    def terminal(self) -> bool:
        return self is not RunStatus.RUNNING


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    content_hash: str
    media_type: str
    size_bytes: int
    path: Path


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    sequence: int
    run_id: str
    event_id: str
    event_type: str
    observed_at: datetime
    payload: dict[str, object]
    payload_hash: str
    previous_hash: str | None
    event_hash: str

    def core_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "observed_at": _timestamp(self.observed_at),
            "payload_hash": self.payload_hash,
            "previous_hash": self.previous_hash,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.runtime-event.v1",
            **self.core_dict(),
            "payload": self.payload,
            "event_hash": self.event_hash,
        }


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    status: RunStatus
    config_hash: str
    created_at: datetime
    updated_at: datetime
    terminal_artifact_id: str | None
    harness_authority_id: str | None = None
    strategy_plan_artifact_hash: str | None = None


_PROCESS_CLAIMS_GUARD = threading.Lock()
_PROCESS_CLAIMS: set[str] = set()
_AUTHORITATIVE_JOURNAL_TOKEN = object()
_PRIVILEGED_EVENT_TYPES = frozenset(
    {
        "ashare.rule_policy.accepted",
        "run.started",
        "run.failed",
        "model.turn.completed",
        "tool.call.completed",
        "model.turn.failed",
        "model.turn.started",
        "model.turn.interrupted",
        "model.attempt.dispatched",
        "model.attempt.failed",
        "model.attempt.succeeded",
        "context.checkpointed",
        "judgment.contract_correction",
        "judgment.validated",
        "portfolio.review.frozen",
        "portfolio.review.validated",
        "continuous.initial-adoption.authorized",
        "continuous.initial-adoption.validated",
        "portfolio.review.incomplete",
        "portfolio.model.attempt",
        "research.thesis.frozen",
        "research.thesis.validated",
        "research.thesis.incomplete",
        "research.thesis.model.attempt",
        "research.sse-fund-suspension.started",
        "research.sse-fund-suspension.received",
        "pi.role.response.completed",
        "pi.role.tool.completed",
        "pi.role.history.initial",
        "pi.context.frozen",
        "pi.response.received",
        "pi.context.compacted",
        "pi.agent.ended",
    }
)


class RunClaim:
    """Crash-safe, non-blocking ownership of one Run Journal run_id."""

    def __init__(self, *, key: str, descriptor: int) -> None:
        self._key = key
        self._descriptor = descriptor
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        finally:
            os.close(self._descriptor)
            with _PROCESS_CLAIMS_GUARD:
                _PROCESS_CLAIMS.remove(self._key)

    def __enter__(self) -> RunClaim:
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)

    def put_json(self, value: object, *, media_type: str = "application/json") -> StoredArtifact:
        payload = canonical_json_bytes(value)
        return self.put_bytes(payload, media_type=media_type)

    def put_bytes(self, payload: bytes, *, media_type: str) -> StoredArtifact:
        if not media_type or media_type != media_type.strip():
            raise ValueError("media_type must be a non-empty trimmed string")
        content_hash = sha256(payload).hexdigest()
        destination = self.root / content_hash
        if destination.exists():
            stored = self.get(content_hash, media_type=media_type)
            if stored.size_bytes != len(payload):
                raise ValueError("content-addressed artifact size mismatch")
            return stored
        descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-artifact-", dir=self.root)
        temporary_path = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            temporary_path.replace(destination)
            os.chmod(destination, 0o600)
        except Exception:
            with suppress(OSError):
                os.close(descriptor)
            temporary_path.unlink(missing_ok=True)
            raise
        return StoredArtifact(
            content_hash=content_hash,
            media_type=media_type,
            size_bytes=len(payload),
            path=destination,
        )

    def get(self, content_hash: str, *, media_type: str) -> StoredArtifact:
        payload = self.read_bytes(content_hash)
        return StoredArtifact(
            content_hash=content_hash,
            media_type=media_type,
            size_bytes=len(payload),
            path=self.root / content_hash,
        )

    def read_bytes(self, content_hash: str) -> bytes:
        """Read and verify the same regular-file bytes returned to the caller."""
        _sha256(content_hash, "content_hash")
        path = self.root / content_hash
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(f"artifact is not a regular file: {content_hash}")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise FileNotFoundError(f"artifact is not a regular file: {content_hash}")
            payload = stream.read()
        if sha256(payload).hexdigest() != content_hash:
            raise ValueError("artifact content does not match its identity")
        return payload

    def read_json(self, content_hash: str) -> object:
        return json.loads(self.read_bytes(content_hash).decode("utf-8"))


class RunJournal:
    def __init__(
        self,
        path: Path,
        *,
        harness_authority_id: str | None = None,
        _authority_token: object | None = None,
        _event_hmac_key: bytes | None = None,
    ) -> None:
        if (harness_authority_id is not None or _event_hmac_key is not None) and (
            _authority_token is not _AUTHORITATIVE_JOURNAL_TOKEN
        ):
            raise ValueError(
                "authoritative Run Journal identity can only come from "
                "RunJournal.authoritative(store)"
            )
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.path = path.resolve()
        self.harness_authority_id = harness_authority_id
        self._event_hmac_key = _event_hmac_key
        if self._event_hmac_key is not None and len(self._event_hmac_key) != 32:
            raise ValueError("authoritative Run Journal event key has an invalid length")
        self._initialize()
        os.chmod(self.path, 0o600)

    @classmethod
    def authoritative(cls, store: object) -> RunJournal:
        """Open the Run Journal inside one concrete LocalDataSnapshotStore root."""

        from market_impact_agent.data_inputs import LocalDataSnapshotStore

        if type(store) is not LocalDataSnapshotStore:
            raise TypeError("authoritative Run Journal requires a LocalDataSnapshotStore")
        path = getattr(store, "index_path", None)
        authority_id = getattr(store, "harness_authority_id", None)
        key_path = getattr(store, "_event_signing_key_path", None)
        if (
            not isinstance(path, Path)
            or not isinstance(authority_id, str)
            or not isinstance(key_path, Path)
            or key_path.is_symlink()
            or not key_path.is_file()
        ):
            raise TypeError("authoritative Run Journal requires a LocalDataSnapshotStore")
        key = key_path.read_bytes()
        return cls(
            path,
            harness_authority_id=authority_id,
            _authority_token=_AUTHORITATIVE_JOURNAL_TOKEN,
            _event_hmac_key=key,
        )

    @property
    def promotion_eligible(self) -> bool:
        return self.harness_authority_id is not None

    @contextmanager
    def authority_transaction(self) -> Generator[sqlite3.Connection]:
        if not self.promotion_eligible:
            raise ValueError("legacy path Run Journal has no Harness authority transaction")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    terminal_artifact_id TEXT,
                    harness_authority_id TEXT,
                    strategy_plan_artifact_hash TEXT
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(run_id),
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    previous_hash TEXT,
                    event_hash TEXT NOT NULL UNIQUE,
                    signer_authority_id TEXT,
                    privileged_signature TEXT
                );
                CREATE INDEX IF NOT EXISTS events_run_sequence
                    ON events(run_id, sequence);
                """
            )
            columns = {
                cast(str, row["name"]) for row in connection.execute("PRAGMA table_info(runs)")
            }
            if "harness_authority_id" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN harness_authority_id TEXT")
            if "strategy_plan_artifact_hash" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN strategy_plan_artifact_hash TEXT")
            event_columns = {
                cast(str, row["name"]) for row in connection.execute("PRAGMA table_info(events)")
            }
            if "signer_authority_id" not in event_columns:
                connection.execute("ALTER TABLE events ADD COLUMN signer_authority_id TEXT")
            if "privileged_signature" not in event_columns:
                connection.execute("ALTER TABLE events ADD COLUMN privileged_signature TEXT")

    def try_claim_run(self, run_id: str) -> RunClaim | None:
        """Return exclusive process ownership, or None when another caller owns the run."""

        _identifier(run_id, "run_id")
        claim_root = self.path.parent / f".{self.path.name}.run-claims"
        claim_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(claim_root, 0o700)
        key = f"{self.path}:{run_id}"
        with _PROCESS_CLAIMS_GUARD:
            if key in _PROCESS_CLAIMS:
                return None
            _PROCESS_CLAIMS.add(key)
        claim_path = claim_root / sha256(run_id.encode()).hexdigest()
        try:
            descriptor = os.open(claim_path, os.O_CREAT | os.O_RDWR, 0o600)
        except Exception:
            with _PROCESS_CLAIMS_GUARD:
                _PROCESS_CLAIMS.remove(key)
            raise
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            with _PROCESS_CLAIMS_GUARD:
                _PROCESS_CLAIMS.remove(key)
            return None
        return RunClaim(key=key, descriptor=descriptor)

    def start_run(
        self,
        *,
        run_id: str,
        config_hash: str,
        created_at: datetime,
        strategy_plan_artifact_hash: str | None = None,
    ) -> RunRecord:
        _identifier(run_id, "run_id")
        _sha256(config_hash, "config_hash")
        if strategy_plan_artifact_hash is not None:
            _sha256(strategy_plan_artifact_hash, "strategy_plan_artifact_hash")
            if not self.promotion_eligible:
                raise ValueError("legacy path Run Journal cannot start promotion-eligible runs")
        require_aware(created_at, "created_at")
        timestamp = _timestamp(created_at)
        resumed = False
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if existing is not None:
                record = _run_record(existing)
                if record.config_hash != config_hash:
                    raise ValueError("existing run_id has a different configuration")
                if (
                    record.harness_authority_id != self.harness_authority_id
                    or record.strategy_plan_artifact_hash != strategy_plan_artifact_hash
                ):
                    raise ValueError("existing run_id has a different authority binding")
                resumed = True
            else:
                connection.execute(
                    """
                    INSERT INTO runs(
                        run_id, status, config_hash, created_at, updated_at, terminal_artifact_id,
                        harness_authority_id, strategy_plan_artifact_hash
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        run_id,
                        RunStatus.RUNNING.value,
                        config_hash,
                        timestamp,
                        timestamp,
                        self.harness_authority_id,
                        strategy_plan_artifact_hash,
                    ),
                )
        if resumed:
            self.events(run_id)
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> RunRecord:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return _run_record(row)

    def records(self, *, status: RunStatus | None = None) -> tuple[RunRecord, ...]:
        """Return durable Run records in creation order for bounded recovery workers."""

        with self._connect() as connection:
            if status is None:
                rows = connection.execute(
                    "SELECT * FROM runs ORDER BY created_at, run_id"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM runs WHERE status = ? ORDER BY created_at, run_id",
                    (status.value,),
                ).fetchall()
        return tuple(_run_record(row) for row in rows)

    def append(
        self,
        *,
        run_id: str,
        event_id: str,
        event_type: str,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> RuntimeEvent:
        _identifier(run_id, "run_id")
        _identifier(event_id, "event_id")
        _identifier(event_type, "event_type")
        require_aware(observed_at, "observed_at")
        payload_json = canonical_json_bytes(payload).decode()
        payload_hash = sha256(payload_json.encode()).hexdigest()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT status, strategy_plan_artifact_hash FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(f"unknown run_id: {run_id}")
            if run["strategy_plan_artifact_hash"] is not None or (
                self.promotion_eligible and event_type in _PRIVILEGED_EVENT_TYPES
            ):
                raise PermissionError(
                    "privileged Run Journal events require a root-authenticated signer"
                )
            if run["status"] != RunStatus.RUNNING.value:
                raise ValueError("cannot append to a terminal run")
            existing = connection.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if existing is not None:
                event = _verified_event(existing)
                if (
                    event.run_id != run_id
                    or event.event_type != event_type
                    or event.payload_hash != payload_hash
                ):
                    raise ValueError("event_id already exists with different content")
                return event
            previous = connection.execute(
                """
                SELECT event_hash FROM events
                WHERE run_id = ? ORDER BY sequence DESC LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            previous_hash = None if previous is None else cast(str, previous["event_hash"])
            event_core = {
                "run_id": run_id,
                "event_id": event_id,
                "event_type": event_type,
                "observed_at": _timestamp(observed_at),
                "payload_hash": payload_hash,
                "previous_hash": previous_hash,
            }
            event_hash = sha256(canonical_json_bytes(event_core)).hexdigest()
            cursor = connection.execute(
                """
                INSERT INTO events(
                    run_id, event_id, event_type, observed_at, payload_json,
                    payload_hash, previous_hash, event_hash,
                    signer_authority_id, privileged_signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    run_id,
                    event_id,
                    event_type,
                    _timestamp(observed_at),
                    payload_json,
                    payload_hash,
                    previous_hash,
                    event_hash,
                ),
            )
            sequence = cursor.lastrowid
            if sequence is None:
                raise RuntimeError("SQLite did not return an event sequence")
            connection.execute(
                "UPDATE runs SET updated_at = ? WHERE run_id = ?",
                (_timestamp(observed_at), run_id),
            )
            row = connection.execute(
                "SELECT * FROM events WHERE sequence = ?", (sequence,)
            ).fetchone()
        if row is None:
            raise RuntimeError("appended event could not be read back")
        return _verified_event(row)

    def events(self, run_id: str) -> tuple[RuntimeEvent, ...]:
        with self._connect() as connection:
            run = connection.execute(
                "SELECT strategy_plan_artifact_hash FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(f"unknown run_id: {run_id}")
            rows = connection.execute(
                "SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (run_id,)
            ).fetchall()
        events_list: list[RuntimeEvent] = []
        for row in rows:
            event = _verified_event(row)
            if run["strategy_plan_artifact_hash"] is not None or (
                self.promotion_eligible and event.event_type in _PRIVILEGED_EVENT_TYPES
            ):
                self._verify_privileged_event(row, event)
            events_list.append(event)
        events = tuple(events_list)
        previous_hash: str | None = None
        for event in events:
            if event.previous_hash != previous_hash:
                raise ValueError("run journal hash chain is invalid")
            previous_hash = event.event_hash
        return events

    def _verify_privileged_event(self, row: sqlite3.Row, event: RuntimeEvent) -> None:
        if self._event_hmac_key is None or self.harness_authority_id is None:
            raise ValueError("privileged Run Journal event has no root verifier")
        authority_id = row["signer_authority_id"]
        signature = row["privileged_signature"]
        if authority_id != self.harness_authority_id or not isinstance(signature, str):
            raise ValueError("privileged Run Journal event has no matching root signature")
        signing_bytes = _privileged_event_signing_bytes(
            harness_authority_id=self.harness_authority_id,
            sequence=event.sequence,
            run_id=event.run_id,
            event_id=event.event_id,
            event_type=event.event_type,
            observed_at=_timestamp(event.observed_at),
            payload=event.payload,
            previous_hash=event.previous_hash,
        )
        expected = hmac.new(self._event_hmac_key, signing_bytes, sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("privileged Run Journal event signature is invalid")

    def event(self, event_id: str) -> RuntimeEvent | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT run_id FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        if row is None:
            return None
        events = self.events(cast(str, row["run_id"]))
        return next(item for item in events if item.event_id == event_id)

    def finish(
        self,
        *,
        run_id: str,
        status: RunStatus,
        finished_at: datetime,
        terminal_artifact_id: str | None,
    ) -> RunRecord:
        if not status.terminal:
            raise ValueError("finish requires a terminal status")
        require_aware(finished_at, "finished_at")
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(f"unknown run_id: {run_id}")
            current = _run_record(row)
            if current.status.terminal:
                if (
                    current.status is not status
                    or current.terminal_artifact_id != terminal_artifact_id
                ):
                    raise ValueError("run is already terminal with a different result")
                return current
            if status is RunStatus.COMPLETED and not terminal_artifact_id:
                raise ValueError("completed runs require a terminal artifact identity")
            connection.execute(
                """
                UPDATE runs
                SET status = ?, updated_at = ?, terminal_artifact_id = ?
                WHERE run_id = ?
                """,
                (status.value, _timestamp(finished_at), terminal_artifact_id, run_id),
            )
        return self.get_run(run_id)

    def journal_hash(self, run_id: str) -> str:
        events = self.events(run_id)
        if events:
            return events[-1].event_hash
        return self.get_run(run_id).config_hash


def _run_record(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=cast(str, row["run_id"]),
        status=RunStatus(cast(str, row["status"])),
        config_hash=cast(str, row["config_hash"]),
        created_at=_parse_timestamp(cast(str, row["created_at"])),
        updated_at=_parse_timestamp(cast(str, row["updated_at"])),
        terminal_artifact_id=cast(str | None, row["terminal_artifact_id"]),
        harness_authority_id=cast(str | None, row["harness_authority_id"]),
        strategy_plan_artifact_hash=cast(str | None, row["strategy_plan_artifact_hash"]),
    )


def _privileged_event_signing_bytes(
    *,
    harness_authority_id: str,
    sequence: int,
    run_id: str,
    event_id: str,
    event_type: str,
    observed_at: str,
    payload: dict[str, object],
    previous_hash: str | None,
) -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": "market-impact.privileged-runtime-event-signature.v1",
            "harness_authority_id": harness_authority_id,
            "sequence": sequence,
            "run_id": run_id,
            "event_id": event_id,
            "event_type": event_type,
            "observed_at": observed_at,
            "payload": payload,
            "previous_hash": previous_hash,
        }
    )


def _verified_event(row: sqlite3.Row) -> RuntimeEvent:
    payload_json = cast(str, row["payload_json"])
    try:
        decoded: object = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("run journal payload_json is invalid") from exc
    if not isinstance(decoded, dict):
        raise TypeError("journal event payload must be an object")
    payload = cast(dict[str, object], decoded)
    canonical_payload = canonical_json_bytes(payload).decode()
    if payload_json != canonical_payload:
        raise ValueError("run journal payload_json is not canonical")
    payload_hash = sha256(canonical_payload.encode()).hexdigest()
    stored_payload_hash = cast(str, row["payload_hash"])
    if stored_payload_hash != payload_hash:
        raise ValueError("run journal payload_hash is invalid")
    event_core = {
        "run_id": cast(str, row["run_id"]),
        "event_id": cast(str, row["event_id"]),
        "event_type": cast(str, row["event_type"]),
        "observed_at": cast(str, row["observed_at"]),
        "payload_hash": stored_payload_hash,
        "previous_hash": cast(str | None, row["previous_hash"]),
    }
    event_hash = sha256(canonical_json_bytes(event_core)).hexdigest()
    stored_event_hash = cast(str, row["event_hash"])
    if stored_event_hash != event_hash:
        raise ValueError("run journal event_hash is invalid")
    return RuntimeEvent(
        sequence=cast(int, row["sequence"]),
        run_id=cast(str, row["run_id"]),
        event_id=cast(str, row["event_id"]),
        event_type=cast(str, row["event_type"]),
        observed_at=_parse_timestamp(cast(str, row["observed_at"])),
        payload=payload,
        payload_hash=stored_payload_hash,
        previous_hash=cast(str | None, row["previous_hash"]),
        event_hash=stored_event_hash,
    )


def runtime_event_from_dict(value: object) -> RuntimeEvent:
    if not isinstance(value, dict):
        raise TypeError("runtime event must be an object")
    payload = cast(dict[object, object], value)
    expected = {
        "schema_version",
        "run_id",
        "event_id",
        "event_type",
        "observed_at",
        "payload",
        "payload_hash",
        "previous_hash",
        "event_hash",
    }
    if set(payload) != expected or payload.get("schema_version") != (
        "market-impact.runtime-event.v1"
    ):
        raise ValueError("runtime event fields or schema are invalid")
    raw_payload = payload.get("payload")
    if not isinstance(raw_payload, dict):
        raise TypeError("runtime event payload must be an object")
    event_payload = cast(dict[str, object], raw_payload)
    payload_hash = _required_string(payload, "payload_hash")
    _sha256(payload_hash, "runtime event payload_hash")
    if payload_hash != sha256(canonical_json_bytes(event_payload)).hexdigest():
        raise ValueError("runtime event payload hash is invalid")
    previous_hash = payload.get("previous_hash")
    if previous_hash is not None and not isinstance(previous_hash, str):
        raise TypeError("runtime event previous_hash must be text or null")
    if previous_hash is not None:
        _sha256(previous_hash, "runtime event previous_hash")
    event_hash = _required_string(payload, "event_hash")
    _sha256(event_hash, "runtime event event_hash")
    event = RuntimeEvent(
        sequence=0,
        run_id=_required_string(payload, "run_id"),
        event_id=_required_string(payload, "event_id"),
        event_type=_required_string(payload, "event_type"),
        observed_at=_parse_timestamp(_required_string(payload, "observed_at")),
        payload=event_payload,
        payload_hash=payload_hash,
        previous_hash=previous_hash,
        event_hash=event_hash,
    )
    if event.event_hash != sha256(canonical_json_bytes(event.core_dict())).hexdigest():
        raise ValueError("runtime event hash is invalid")
    if event.to_dict() != value:
        raise ValueError("runtime event is not canonical")
    return event


def _required_string(payload: dict[object, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise TypeError(f"runtime event {name} must be non-empty text")
    return value


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "timestamp")
    return parsed.astimezone(UTC)


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", value) is None:
        raise ValueError(f"{name} contains unsupported characters")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")
