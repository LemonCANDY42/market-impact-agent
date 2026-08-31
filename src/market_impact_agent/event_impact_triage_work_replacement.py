from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import (
    ArtifactStore,
    RunJournal,
    RunRecord,
    RunStatus,
    RuntimeEvent,
)
from market_impact_agent.usage_ledger import UsageLedger


@dataclass(frozen=True, slots=True)
class EventImpactTriageWorkReplacementGrant:
    """One explicit substitute for one ambiguous model Run."""

    plan_id: str
    phase: str
    unit_id: str
    role: str
    original_run_id: str
    original_terminal_artifact_hash: str
    original_journal_hash: str
    original_usage_record_hash: str
    authorized_at: datetime
    schema_version: str = "market-impact.event-impact-triage-work-replacement-grant.v1"

    def __post_init__(self) -> None:
        if self.schema_version != "market-impact.event-impact-triage-work-replacement-grant.v1":
            raise ValueError("unsupported triage work replacement grant schema")
        for name in ("plan_id", "phase", "unit_id", "role", "original_run_id"):
            value = cast(str, getattr(self, name))
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        for name in (
            "original_terminal_artifact_hash",
            "original_journal_hash",
            "original_usage_record_hash",
        ):
            _sha256(cast(str, getattr(self, name)), name)
        require_aware(self.authorized_at, "replacement authorized_at")
        if self.authorized_at.utcoffset() != UTC.utcoffset(self.authorized_at):
            raise ValueError("replacement authorized_at must use UTC")

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "phase": self.phase,
            "unit_id": self.unit_id,
            "role": self.role,
            "original_run_id": self.original_run_id,
            "original_terminal_artifact_hash": self.original_terminal_artifact_hash,
            "original_journal_hash": self.original_journal_hash,
            "original_usage_record_hash": self.original_usage_record_hash,
            "authorized_at": _timestamp(self.authorized_at),
        }

    @property
    def grant_id(self) -> str:
        return f"event-impact-triage-work-replacement-grant-{canonical_hash(self.core_dict())}"

    @property
    def replacement_run_id(self) -> str:
        return f"triage-work-replacement-{canonical_hash({'grant_id': self.grant_id})}"

    def to_dict(self) -> dict[str, object]:
        return {
            **self.core_dict(),
            "grant_id": self.grant_id,
            "replacement_run_id": self.replacement_run_id,
        }


class EventImpactTriageWorkReplacementStore:
    """Append-only Harness authority for a single replacement of an ambiguous Run."""

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
                CREATE TABLE IF NOT EXISTS replacement_grants (
                    original_run_id TEXT PRIMARY KEY,
                    replacement_run_id TEXT NOT NULL UNIQUE,
                    grant_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL UNIQUE
                );
                CREATE TRIGGER IF NOT EXISTS replacement_grants_no_update
                BEFORE UPDATE ON replacement_grants
                BEGIN SELECT RAISE(ABORT, 'replacement grants are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS replacement_grants_no_delete
                BEFORE DELETE ON replacement_grants
                BEGIN SELECT RAISE(ABORT, 'replacement grants are append-only'); END;
                """
            )

    def authorize_once(
        self,
        *,
        plan_id: str,
        phase: str,
        unit_id: str,
        role: str,
        original_run_id: str,
        authorized_at: datetime,
        journal: RunJournal,
        artifact_store: ArtifactStore,
        usage_ledger: UsageLedger,
    ) -> EventImpactTriageWorkReplacementGrant:
        if self.for_replacement(original_run_id) is not None:
            raise ValueError("an ambiguous replacement Run cannot itself be replaced")
        existing = self.for_original(original_run_id)
        if existing is not None:
            if (
                existing.plan_id != plan_id
                or existing.phase != phase
                or existing.unit_id != unit_id
                or existing.role != role
            ):
                raise ValueError("ambiguous Run already has another replacement authority")
            self.assert_authoritative(
                existing,
                journal=journal,
                artifact_store=artifact_store,
                usage_ledger=usage_ledger,
            )
            return existing

        record, terminal, usage_record_hash = _ambiguous_run_authority(
            plan_id=plan_id,
            phase=phase,
            unit_id=unit_id,
            role=role,
            original_run_id=original_run_id,
            journal=journal,
            artifact_store=artifact_store,
            usage_ledger=usage_ledger,
        )
        require_aware(authorized_at, "replacement authorized_at")
        normalized_at = authorized_at.astimezone(UTC)
        if normalized_at < record.updated_at:
            raise ValueError("replacement cannot be authorized before the ambiguous Run finished")
        grant = EventImpactTriageWorkReplacementGrant(
            plan_id=plan_id,
            phase=phase,
            unit_id=unit_id,
            role=role,
            original_run_id=original_run_id,
            original_terminal_artifact_hash=cast(str, record.terminal_artifact_id),
            original_journal_hash=cast(str, terminal["journal_hash"]),
            original_usage_record_hash=usage_record_hash,
            authorized_at=normalized_at,
        )
        payload_json = canonical_json_bytes(grant.to_dict()).decode()
        payload_hash = canonical_hash(grant.to_dict())
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO replacement_grants(
                    original_run_id, replacement_run_id, grant_id, payload_json, payload_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    grant.original_run_id,
                    grant.replacement_run_id,
                    grant.grant_id,
                    payload_json,
                    payload_hash,
                ),
            )
        return grant

    def for_original(self, original_run_id: str) -> EventImpactTriageWorkReplacementGrant | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM replacement_grants WHERE original_run_id = ?",
                (original_run_id,),
            ).fetchone()
        return None if row is None else _grant_from_row(row)

    def for_replacement(
        self, replacement_run_id: str
    ) -> EventImpactTriageWorkReplacementGrant | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM replacement_grants WHERE replacement_run_id = ?",
                (replacement_run_id,),
            ).fetchone()
        return None if row is None else _grant_from_row(row)

    def assert_authoritative(
        self,
        grant: EventImpactTriageWorkReplacementGrant,
        *,
        journal: RunJournal,
        artifact_store: ArtifactStore,
        usage_ledger: UsageLedger,
    ) -> None:
        stored = self.for_original(grant.original_run_id)
        if stored != grant:
            raise ValueError("replacement grant differs from append-only authority")
        record, terminal, usage_record_hash = _ambiguous_run_authority(
            plan_id=grant.plan_id,
            phase=grant.phase,
            unit_id=grant.unit_id,
            role=grant.role,
            original_run_id=grant.original_run_id,
            journal=journal,
            artifact_store=artifact_store,
            usage_ledger=usage_ledger,
        )
        if (
            record.terminal_artifact_id != grant.original_terminal_artifact_hash
            or terminal.get("journal_hash") != grant.original_journal_hash
            or usage_record_hash != grant.original_usage_record_hash
            or grant.authorized_at < record.updated_at
        ):
            raise ValueError("replacement grant no longer matches the ambiguous Run authority")


def _ambiguous_run_authority(
    *,
    plan_id: str,
    phase: str,
    unit_id: str,
    role: str,
    original_run_id: str,
    journal: RunJournal,
    artifact_store: ArtifactStore,
    usage_ledger: UsageLedger,
) -> tuple[RunRecord, dict[str, object], str]:
    record = journal.get_run(original_run_id)
    if record.status is not RunStatus.HUMAN_INPUT_REQUIRED or record.terminal_artifact_id is None:
        raise ValueError("replacement requires one terminal ambiguous Run")
    events = journal.events(original_run_id)
    if not _terminal_event_is_ambiguous(events):
        raise ValueError("replacement requires an unresolved ambiguous model dispatch")
    terminal = _object(
        artifact_store.read_json(record.terminal_artifact_id),
        "ambiguous triage work terminal",
    )
    if (
        terminal.get("run_id") != original_run_id
        or terminal.get("plan_id") != plan_id
        or terminal.get("phase") != phase
        or terminal.get("unit_id") != unit_id
        or terminal.get("role") != role
        or terminal.get("status") != RunStatus.HUMAN_INPUT_REQUIRED.value
        or terminal.get("journal_hash") != journal.journal_hash(original_run_id)
    ):
        raise ValueError("ambiguous Run terminal identity or Journal binding drifted")
    usage_matches = tuple(
        item for item in usage_ledger.records() if item.record.run_id == original_run_id
    )
    if len(usage_matches) != 1:
        raise ValueError("replacement requires exactly one original Usage Record")
    usage = usage_matches[0]
    if (
        usage.record.experiment_id != plan_id
        or usage.record.status is not RunStatus.HUMAN_INPUT_REQUIRED
        or usage.record.execution_binding_hash != record.config_hash
        or usage.record.terminal_artifact_hash != record.terminal_artifact_id
        or usage.record.run_journal_hash != journal.journal_hash(original_run_id)
    ):
        raise ValueError("ambiguous Run Usage authority drifted")
    return record, terminal, usage.record_hash


def _terminal_event_is_ambiguous(events: tuple[RuntimeEvent, ...]) -> bool:
    if not events:
        return False
    terminal = events[-1]
    if terminal.event_type == "model.request.ambiguous":
        return _matching_preceding_dispatch(events[:-1], terminal.payload) is not None
    if terminal.event_type != "model.request.rejected" or len(events) < 2:
        return False
    failure = events[-2]
    if failure.event_type != "model.request.failed":
        return False
    terminal_payload = terminal.payload
    failure_payload = failure.payload
    keys = (
        "dispatch_event_hash",
        "error_class",
        "diagnostic_code",
        "http_status",
        "generation_state",
        "retry_disposition",
        "request_id",
    )
    return (
        all(terminal_payload.get(key) == failure_payload.get(key) for key in keys)
        and terminal_payload.get("error_class") == "http"
        and terminal_payload.get("diagnostic_code") == "http_408"
        and terminal_payload.get("http_status") == 408
        and terminal_payload.get("generation_state") == "not_started"
        and terminal_payload.get("retry_disposition") == "terminal"
        and _matching_preceding_dispatch(events[:-2], terminal_payload) is not None
    )


def _matching_preceding_dispatch(
    preceding: tuple[RuntimeEvent, ...], payload: dict[str, object]
) -> RuntimeEvent | None:
    dispatch_hash = payload.get("dispatch_event_hash")
    if not isinstance(dispatch_hash, str) or len(dispatch_hash) != 64:
        return None
    matches = tuple(
        event
        for event in preceding
        if event.event_type == "model.request.dispatched" and event.event_hash == dispatch_hash
    )
    if len(matches) != 1:
        return None
    dispatch = matches[0]
    request_id = payload.get("request_id")
    if request_id is not None and dispatch.payload.get("provider_request_id") != request_id:
        return None
    return dispatch


def _grant_from_row(row: sqlite3.Row) -> EventImpactTriageWorkReplacementGrant:
    payload = _object(json.loads(cast(str, row["payload_json"])), "replacement grant")
    if cast(str, row["payload_hash"]) != canonical_hash(payload):
        raise ValueError("stored replacement grant payload hash is invalid")
    expected = {
        "schema_version",
        "plan_id",
        "phase",
        "unit_id",
        "role",
        "original_run_id",
        "original_terminal_artifact_hash",
        "original_journal_hash",
        "original_usage_record_hash",
        "authorized_at",
        "grant_id",
        "replacement_run_id",
    }
    if set(payload) != expected:
        raise ValueError("stored replacement grant fields are invalid")
    grant = EventImpactTriageWorkReplacementGrant(
        schema_version=_string(payload, "schema_version"),
        plan_id=_string(payload, "plan_id"),
        phase=_string(payload, "phase"),
        unit_id=_string(payload, "unit_id"),
        role=_string(payload, "role"),
        original_run_id=_string(payload, "original_run_id"),
        original_terminal_artifact_hash=_string(payload, "original_terminal_artifact_hash"),
        original_journal_hash=_string(payload, "original_journal_hash"),
        original_usage_record_hash=_string(payload, "original_usage_record_hash"),
        authorized_at=_datetime(_string(payload, "authorized_at")),
    )
    if (
        row["original_run_id"] != grant.original_run_id
        or row["replacement_run_id"] != grant.replacement_run_id
        or row["grant_id"] != grant.grant_id
        or payload.get("grant_id") != grant.grant_id
        or payload.get("replacement_run_id") != grant.replacement_run_id
    ):
        raise ValueError("stored replacement grant identity is invalid")
    return grant


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError(f"{name} must have string keys")
    return cast(dict[str, object], mapping)


def _string(value: dict[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item or item != item.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return item


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "replacement timestamp")
    return parsed.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64:
        raise ValueError(f"{name} must be a SHA-256 hex digest")
    try:
        int(value, 16)
    except ValueError as exc:
        raise ValueError(f"{name} must be a SHA-256 hex digest") from exc
