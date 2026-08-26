from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_engine import AgentRunResult, RunMetrics
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import RunStatus


@dataclass(frozen=True, slots=True)
class UsageRecord:
    experiment_id: str
    arm_id: str
    run_id: str
    recorded_at: datetime
    status: RunStatus
    provider_profile_id: str
    provider_profile_hash: str
    execution_binding_hash: str
    terminal_artifact_hash: str | None
    run_journal_hash: str
    metrics: RunMetrics

    def __post_init__(self) -> None:
        require_aware(self.recorded_at, "usage recorded_at")
        for name in ("experiment_id", "arm_id", "run_id", "provider_profile_id"):
            value = cast(str, getattr(self, name))
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        for name in (
            "provider_profile_hash",
            "execution_binding_hash",
            "run_journal_hash",
        ):
            _sha256(cast(str, getattr(self, name)), name)
        if self.terminal_artifact_hash is not None:
            _sha256(self.terminal_artifact_hash, "terminal_artifact_hash")
        if not self.status.terminal:
            raise ValueError("Usage Ledger accepts only terminal runs")

    @classmethod
    def from_result(
        cls,
        *,
        experiment_id: str,
        arm_id: str,
        recorded_at: datetime,
        provider_profile_id: str,
        provider_profile_hash: str,
        execution_binding_hash: str,
        run_journal_hash: str,
        result: AgentRunResult,
    ) -> UsageRecord:
        if result.metrics is None:
            raise ValueError("terminal Agent result is missing usage metrics")
        return cls(
            experiment_id=experiment_id,
            arm_id=arm_id,
            run_id=result.run_id,
            recorded_at=recorded_at,
            status=result.status,
            provider_profile_id=provider_profile_id,
            provider_profile_hash=provider_profile_hash,
            execution_binding_hash=execution_binding_hash,
            terminal_artifact_hash=result.terminal_store_hash,
            run_journal_hash=run_journal_hash,
            metrics=result.metrics,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.usage-record.v1",
            "experiment_id": self.experiment_id,
            "arm_id": self.arm_id,
            "run_id": self.run_id,
            "recorded_at": self.recorded_at.isoformat().replace("+00:00", "Z"),
            "status": self.status.value,
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "execution_binding_hash": self.execution_binding_hash,
            "terminal_artifact_hash": self.terminal_artifact_hash,
            "run_journal_hash": self.run_journal_hash,
            "metrics": self.metrics.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class StoredUsageRecord:
    sequence: int
    record: UsageRecord
    payload_hash: str
    previous_hash: str | None
    record_hash: str


class UsageLedger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.path = path.resolve()
        self._initialize()
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS usage_records (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    previous_hash TEXT,
                    record_hash TEXT NOT NULL UNIQUE
                );
                CREATE TRIGGER IF NOT EXISTS usage_records_no_update
                BEFORE UPDATE ON usage_records
                BEGIN SELECT RAISE(ABORT, 'usage_records are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS usage_records_no_delete
                BEFORE DELETE ON usage_records
                BEGIN SELECT RAISE(ABORT, 'usage_records are append-only'); END;
                """
            )

    def append(self, record: UsageRecord) -> StoredUsageRecord:
        payload = record.to_dict()
        payload_json = canonical_json_bytes(payload).decode()
        payload_hash = canonical_hash(payload)
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM usage_records WHERE run_id = ?", (record.run_id,)
            ).fetchone()
            if existing is not None:
                stored = _stored(existing)
                if stored.payload_hash != payload_hash:
                    raise ValueError("Usage Ledger run_id already has different content")
                return stored
            prior = connection.execute(
                "SELECT record_hash FROM usage_records ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            previous_hash = None if prior is None else cast(str, prior["record_hash"])
            record_hash = canonical_hash(
                {
                    "payload_hash": payload_hash,
                    "previous_hash": previous_hash,
                }
            )
            cursor = connection.execute(
                """
                INSERT INTO usage_records(
                    run_id, payload_json, payload_hash, previous_hash, record_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (record.run_id, payload_json, payload_hash, previous_hash, record_hash),
            )
            sequence = cursor.lastrowid
            if sequence is None:
                raise RuntimeError("SQLite did not return a Usage Ledger sequence")
            row = connection.execute(
                "SELECT * FROM usage_records WHERE sequence = ?", (sequence,)
            ).fetchone()
        if row is None:
            raise RuntimeError("appended Usage Ledger record could not be read")
        return _stored(row)

    def records(self) -> tuple[StoredUsageRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM usage_records ORDER BY sequence").fetchall()
        records = tuple(_stored(row) for row in rows)
        previous: str | None = None
        for stored in records:
            if stored.previous_hash != previous:
                raise ValueError("Usage Ledger hash chain is broken")
            previous = stored.record_hash
        return records

    @property
    def ledger_hash(self) -> str:
        records = self.records()
        return canonical_hash(
            {
                "schema_version": "market-impact.usage-ledger.v1",
                "record_hashes": [item.record_hash for item in records],
            }
        )


def _stored(row: sqlite3.Row) -> StoredUsageRecord:
    raw = json.loads(cast(str, row["payload_json"]))
    if not isinstance(raw, dict):
        raise TypeError("stored Usage Ledger payload must be an object")
    mapping = cast(dict[object, object], raw)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError("stored Usage Ledger payload must have string keys")
    payload = cast(dict[str, object], mapping)
    if cast(str, row["payload_hash"]) != canonical_hash(payload):
        raise ValueError("stored Usage Ledger payload hash is invalid")
    previous_hash = cast(str | None, row["previous_hash"])
    expected_hash = canonical_hash(
        {
            "payload_hash": cast(str, row["payload_hash"]),
            "previous_hash": previous_hash,
        }
    )
    if cast(str, row["record_hash"]) != expected_hash:
        raise ValueError("stored Usage Ledger record hash is invalid")
    record = _record_from_dict(payload)
    if record.run_id != cast(str, row["run_id"]):
        raise ValueError("stored Usage Ledger run identity is invalid")
    return StoredUsageRecord(
        sequence=cast(int, row["sequence"]),
        record=record,
        payload_hash=cast(str, row["payload_hash"]),
        previous_hash=previous_hash,
        record_hash=expected_hash,
    )


def _record_from_dict(payload: dict[str, object]) -> UsageRecord:
    expected = {
        "schema_version",
        "experiment_id",
        "arm_id",
        "run_id",
        "recorded_at",
        "status",
        "provider_profile_id",
        "provider_profile_hash",
        "execution_binding_hash",
        "terminal_artifact_hash",
        "run_journal_hash",
        "metrics",
    }
    if set(payload) != expected or payload.get("schema_version") != "market-impact.usage-record.v1":
        raise ValueError("stored Usage Record fields are invalid")
    metrics_raw = payload.get("metrics")
    if not isinstance(metrics_raw, dict):
        raise TypeError("stored Usage Record metrics must be an object")
    metrics = cast(dict[str, object], metrics_raw)
    return UsageRecord(
        experiment_id=_string(payload, "experiment_id"),
        arm_id=_string(payload, "arm_id"),
        run_id=_string(payload, "run_id"),
        recorded_at=datetime.fromisoformat(_string(payload, "recorded_at").replace("Z", "+00:00")),
        status=RunStatus(_string(payload, "status")),
        provider_profile_id=_string(payload, "provider_profile_id"),
        provider_profile_hash=_string(payload, "provider_profile_hash"),
        execution_binding_hash=_string(payload, "execution_binding_hash"),
        terminal_artifact_hash=_optional_string(payload, "terminal_artifact_hash"),
        run_journal_hash=_string(payload, "run_journal_hash"),
        metrics=RunMetrics(
            turns=_integer(metrics, "turns"),
            tool_calls=_integer(metrics, "tool_calls"),
            input_tokens=_integer(metrics, "input_tokens"),
            output_tokens=_integer(metrics, "output_tokens"),
            result_bytes=_integer(metrics, "result_bytes"),
            latency_ms=_number(metrics, "latency_ms"),
            provider_attempts=_integer(metrics, "provider_attempts"),
            estimated_cost_microusd=_integer(metrics, "estimated_cost_microusd"),
        ),
    )


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _optional_string(payload: dict[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a string or null")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _number(payload: dict[str, object], name: str) -> float:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be numeric")
    return float(value)


def _sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")
