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
from market_impact_agent.model_json import MODEL_JSON_REPAIR_POLICY_ID, load_model_json
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunRecord, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

_GRANT_SCHEMA = "market-impact.event-impact-triage-work-format-recovery-grant.v1"


@dataclass(frozen=True, slots=True)
class EventImpactTriageWorkFormatRecoveryGrant:
    """One deterministic, zero-Provider recovery of one received malformed response."""

    plan_id: str
    phase: str
    unit_id: str
    role: str
    original_run_id: str
    original_terminal_artifact_hash: str
    original_journal_hash: str
    original_usage_record_hash: str
    final_assistant_message_hash: str
    final_raw_response_hash: str
    repaired_json_hash: str
    authorized_at: datetime
    parser_id: str = "json-repair-0.63.4"
    repair_policy_id: str = MODEL_JSON_REPAIR_POLICY_ID
    schema_version: str = _GRANT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != _GRANT_SCHEMA:
            raise ValueError("unsupported triage Work format recovery Grant schema")
        for name in ("plan_id", "phase", "unit_id", "role", "original_run_id"):
            value = cast(str, getattr(self, name))
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        for name in (
            "original_terminal_artifact_hash",
            "original_journal_hash",
            "original_usage_record_hash",
            "final_assistant_message_hash",
            "final_raw_response_hash",
            "repaired_json_hash",
        ):
            _sha256(cast(str, getattr(self, name)), name)
        if self.parser_id != "json-repair-0.63.4":
            raise ValueError("format recovery parser identity is unsupported")
        if self.repair_policy_id != MODEL_JSON_REPAIR_POLICY_ID:
            raise ValueError("format recovery policy identity is unsupported")
        require_aware(self.authorized_at, "format recovery authorized_at")
        if self.authorized_at.utcoffset() != UTC.utcoffset(self.authorized_at):
            raise ValueError("format recovery authorized_at must use UTC")

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
            "final_assistant_message_hash": self.final_assistant_message_hash,
            "final_raw_response_hash": self.final_raw_response_hash,
            "repaired_json_hash": self.repaired_json_hash,
            "parser_id": self.parser_id,
            "repair_policy_id": self.repair_policy_id,
            "authorized_at": _timestamp(self.authorized_at),
        }

    @property
    def grant_id(self) -> str:
        return f"event-impact-triage-work-format-recovery-grant-{canonical_hash(self.core_dict())}"

    @property
    def recovery_run_id(self) -> str:
        return f"triage-work-format-recovery-{canonical_hash({'grant_id': self.grant_id})}"

    def to_dict(self) -> dict[str, object]:
        return {
            **self.core_dict(),
            "grant_id": self.grant_id,
            "recovery_run_id": self.recovery_run_id,
        }


class EventImpactTriageWorkFormatRecoveryStore:
    """Append-only authority for deterministic format recovery, never model retry."""

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
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS format_recovery_grants (
                    original_run_id TEXT PRIMARY KEY,
                    recovery_run_id TEXT NOT NULL UNIQUE,
                    grant_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL UNIQUE
                );
                CREATE TRIGGER IF NOT EXISTS format_recovery_grants_no_update
                BEFORE UPDATE ON format_recovery_grants
                BEGIN SELECT RAISE(ABORT, 'format recovery grants are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS format_recovery_grants_no_delete
                BEFORE DELETE ON format_recovery_grants
                BEGIN SELECT RAISE(ABORT, 'format recovery grants are append-only'); END;
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
    ) -> EventImpactTriageWorkFormatRecoveryGrant:
        if self.for_recovery(original_run_id) is not None:
            raise ValueError("a format recovery Run cannot itself be recovered")
        existing = self.for_original(original_run_id)
        if existing is not None:
            if (
                existing.plan_id != plan_id
                or existing.phase != phase
                or existing.unit_id != unit_id
                or existing.role != role
            ):
                raise ValueError("failed Run already has another format recovery authority")
            self.assert_authoritative(
                existing,
                journal=journal,
                artifact_store=artifact_store,
                usage_ledger=usage_ledger,
            )
            return existing

        record, terminal, usage_hash, assistant_hash, raw_hash, repaired_hash = (
            _failed_format_authority(
                plan_id=plan_id,
                phase=phase,
                unit_id=unit_id,
                role=role,
                original_run_id=original_run_id,
                journal=journal,
                artifact_store=artifact_store,
                usage_ledger=usage_ledger,
            )
        )
        require_aware(authorized_at, "format recovery authorized_at")
        normalized_at = authorized_at.astimezone(UTC)
        if normalized_at < record.updated_at:
            raise ValueError("format recovery cannot be authorized before the failed Run finished")
        grant = EventImpactTriageWorkFormatRecoveryGrant(
            plan_id=plan_id,
            phase=phase,
            unit_id=unit_id,
            role=role,
            original_run_id=original_run_id,
            original_terminal_artifact_hash=cast(str, record.terminal_artifact_id),
            original_journal_hash=cast(str, terminal["journal_hash"]),
            original_usage_record_hash=usage_hash,
            final_assistant_message_hash=assistant_hash,
            final_raw_response_hash=raw_hash,
            repaired_json_hash=repaired_hash,
            authorized_at=normalized_at,
        )
        payload_json = canonical_json_bytes(grant.to_dict()).decode()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO format_recovery_grants(
                    original_run_id, recovery_run_id, grant_id, payload_json, payload_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    grant.original_run_id,
                    grant.recovery_run_id,
                    grant.grant_id,
                    payload_json,
                    canonical_hash(grant.to_dict()),
                ),
            )
        return grant

    def for_original(self, original_run_id: str) -> EventImpactTriageWorkFormatRecoveryGrant | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM format_recovery_grants WHERE original_run_id = ?",
                (original_run_id,),
            ).fetchone()
        return None if row is None else _grant_from_row(row)

    def for_recovery(self, recovery_run_id: str) -> EventImpactTriageWorkFormatRecoveryGrant | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM format_recovery_grants WHERE recovery_run_id = ?",
                (recovery_run_id,),
            ).fetchone()
        return None if row is None else _grant_from_row(row)

    def assert_authoritative(
        self,
        grant: EventImpactTriageWorkFormatRecoveryGrant,
        *,
        journal: RunJournal,
        artifact_store: ArtifactStore,
        usage_ledger: UsageLedger,
    ) -> None:
        if self.for_original(grant.original_run_id) != grant:
            raise ValueError("format recovery Grant differs from append-only authority")
        record, terminal, usage_hash, assistant_hash, raw_hash, repaired_hash = (
            _failed_format_authority(
                plan_id=grant.plan_id,
                phase=grant.phase,
                unit_id=grant.unit_id,
                role=grant.role,
                original_run_id=grant.original_run_id,
                journal=journal,
                artifact_store=artifact_store,
                usage_ledger=usage_ledger,
            )
        )
        if (
            record.terminal_artifact_id != grant.original_terminal_artifact_hash
            or terminal.get("journal_hash") != grant.original_journal_hash
            or usage_hash != grant.original_usage_record_hash
            or assistant_hash != grant.final_assistant_message_hash
            or raw_hash != grant.final_raw_response_hash
            or repaired_hash != grant.repaired_json_hash
            or grant.authorized_at < record.updated_at
        ):
            raise ValueError("format recovery Grant no longer matches failed Run authority")


def _failed_format_authority(
    *,
    plan_id: str,
    phase: str,
    unit_id: str,
    role: str,
    original_run_id: str,
    journal: RunJournal,
    artifact_store: ArtifactStore,
    usage_ledger: UsageLedger,
) -> tuple[RunRecord, dict[str, object], str, str, str, str]:
    record = journal.get_run(original_run_id)
    if record.status is not RunStatus.FAILED or record.terminal_artifact_id is None:
        raise ValueError("format recovery requires one terminal failed Run")
    terminal = _object(
        artifact_store.read_json(record.terminal_artifact_id), "failed triage Work terminal"
    )
    if (
        terminal.get("run_id") != original_run_id
        or terminal.get("plan_id") != plan_id
        or terminal.get("phase") != phase
        or terminal.get("unit_id") != unit_id
        or terminal.get("role") != role
        or terminal.get("status") != RunStatus.FAILED.value
        or terminal.get("journal_hash") != journal.journal_hash(original_run_id)
        or terminal.get("error_class") != "ValueError"
        or terminal.get("message") != "model failed the closed triage work output contract"
    ):
        raise ValueError("format recovery source is not the exact closed-output failure")
    events = journal.events(original_run_id)
    response_events = tuple(
        item for item in events if item.event_type == "model.response.completed"
    )
    if not response_events or events[-1] != response_events[-1]:
        raise ValueError("format recovery requires a final fully received model response")
    final = response_events[-1]
    assistant_hash = _string(final.payload, "assistant_message_hash")
    raw_hash = _string(final.payload, "raw_response_hash")
    assistant = _object(
        artifact_store.read_json(assistant_hash), "format recovery final assistant response"
    )
    artifact_store.read_json(raw_hash)
    content = assistant.get("content")
    if not isinstance(content, str):
        raise ValueError("format recovery final assistant content is not text")
    parsed = load_model_json(content)
    if not parsed.evidence.repair_applied:
        raise ValueError("format recovery requires an actually repaired response")
    usage_matches = tuple(
        item for item in usage_ledger.records() if item.record.run_id == original_run_id
    )
    if len(usage_matches) != 1:
        raise ValueError("format recovery requires exactly one original Usage Record")
    usage = usage_matches[0]
    if (
        usage.record.experiment_id != plan_id
        or usage.record.status is not RunStatus.FAILED
        or usage.record.execution_binding_hash != record.config_hash
        or usage.record.terminal_artifact_hash != record.terminal_artifact_id
        or usage.record.run_journal_hash != journal.journal_hash(original_run_id)
    ):
        raise ValueError("format recovery source Usage authority drifted")
    return (
        record,
        terminal,
        usage.record_hash,
        assistant_hash,
        raw_hash,
        canonical_hash(parsed.value),
    )


def _grant_from_row(row: sqlite3.Row) -> EventImpactTriageWorkFormatRecoveryGrant:
    payload = _object(json.loads(cast(str, row["payload_json"])), "format recovery Grant")
    if cast(str, row["payload_hash"]) != canonical_hash(payload):
        raise ValueError("stored format recovery Grant payload hash is invalid")
    grant = EventImpactTriageWorkFormatRecoveryGrant(
        schema_version=_string(payload, "schema_version"),
        plan_id=_string(payload, "plan_id"),
        phase=_string(payload, "phase"),
        unit_id=_string(payload, "unit_id"),
        role=_string(payload, "role"),
        original_run_id=_string(payload, "original_run_id"),
        original_terminal_artifact_hash=_string(payload, "original_terminal_artifact_hash"),
        original_journal_hash=_string(payload, "original_journal_hash"),
        original_usage_record_hash=_string(payload, "original_usage_record_hash"),
        final_assistant_message_hash=_string(payload, "final_assistant_message_hash"),
        final_raw_response_hash=_string(payload, "final_raw_response_hash"),
        repaired_json_hash=_string(payload, "repaired_json_hash"),
        parser_id=_string(payload, "parser_id"),
        repair_policy_id=_string(payload, "repair_policy_id"),
        authorized_at=_datetime(_string(payload, "authorized_at")),
    )
    if (
        row["original_run_id"] != grant.original_run_id
        or row["recovery_run_id"] != grant.recovery_run_id
        or row["grant_id"] != grant.grant_id
        or payload != grant.to_dict()
    ):
        raise ValueError("stored format recovery Grant identity is invalid")
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
    require_aware(parsed, "format recovery timestamp")
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
