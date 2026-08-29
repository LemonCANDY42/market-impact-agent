from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
from contextlib import suppress
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
        _sha256(content_hash, "content_hash")
        path = self.root / content_hash
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(f"artifact is not a regular file: {content_hash}")
        payload = path.read_bytes()
        if sha256(payload).hexdigest() != content_hash:
            raise ValueError("artifact content does not match its identity")
        return StoredArtifact(
            content_hash=content_hash,
            media_type=media_type,
            size_bytes=len(payload),
            path=path,
        )

    def read_json(self, content_hash: str) -> object:
        artifact = self.get(content_hash, media_type="application/json")
        return json.loads(artifact.path.read_text(encoding="utf-8"))


class RunJournal:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.path = path.resolve()
        self._initialize()
        os.chmod(self.path, 0o600)

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
                    terminal_artifact_id TEXT
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
                    event_hash TEXT NOT NULL UNIQUE
                );
                CREATE INDEX IF NOT EXISTS events_run_sequence
                    ON events(run_id, sequence);
                """
            )

    def start_run(
        self,
        *,
        run_id: str,
        config_hash: str,
        created_at: datetime,
    ) -> RunRecord:
        _identifier(run_id, "run_id")
        _sha256(config_hash, "config_hash")
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
                resumed = True
            else:
                connection.execute(
                    """
                    INSERT INTO runs(
                        run_id, status, config_hash, created_at, updated_at, terminal_artifact_id
                    ) VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    (run_id, RunStatus.RUNNING.value, config_hash, timestamp, timestamp),
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
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(f"unknown run_id: {run_id}")
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
                    payload_hash, previous_hash, event_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
            rows = connection.execute(
                "SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (run_id,)
            ).fetchall()
        events = tuple(_verified_event(row) for row in rows)
        previous_hash: str | None = None
        for event in events:
            if event.previous_hash != previous_hash:
                raise ValueError("run journal hash chain is invalid")
            previous_hash = event.event_hash
        return events

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
