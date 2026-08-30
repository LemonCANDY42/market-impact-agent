from __future__ import annotations

import json
import math
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import ClassVar, cast

from market_impact_agent.domain import require_aware


class ProviderGenerationState(StrEnum):
    NOT_STARTED = "not_started"
    UNKNOWN = "unknown"
    RESPONSE_RECEIVED = "response_received"


class ProviderRetryDisposition(StrEnum):
    SAFE = "safe"
    FORBIDDEN = "forbidden"
    TERMINAL = "terminal"


class ProviderFailure(RuntimeError):
    """Sanitized failure contract safe for journals and operator notices."""

    def __init__(
        self,
        message: str,
        *,
        error_class: str,
        diagnostic_code: str | None = None,
        http_status: int | None = None,
        request_id: str | None = None,
        generation_state: ProviderGenerationState = ProviderGenerationState.UNKNOWN,
        retry_disposition: ProviderRetryDisposition | None = None,
        retry_after_seconds: float | None = None,
        attempts: int = 1,
        elapsed_latency_ms: float = 0.0,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(message)
        if retry_disposition is None:
            retry_disposition = (
                ProviderRetryDisposition.SAFE if retryable else ProviderRetryDisposition.FORBIDDEN
            )
        if attempts < 1:
            raise ValueError("provider failure attempts must be positive")
        if not math.isfinite(elapsed_latency_ms) or elapsed_latency_ms < 0:
            raise ValueError("provider failure latency must be finite and non-negative")
        if retry_after_seconds is not None and (
            not math.isfinite(retry_after_seconds) or retry_after_seconds < 0
        ):
            raise ValueError("provider Retry-After must be finite and non-negative")
        self.error_class = _safe_token(error_class, "error_class")
        self.diagnostic_code = _safe_token(diagnostic_code or error_class, "diagnostic_code")
        self.http_status = http_status
        self.request_id = request_id
        self.generation_state = generation_state
        self.retry_disposition = retry_disposition
        self.retry_after_seconds = retry_after_seconds
        self.attempts = attempts
        self.elapsed_latency_ms = elapsed_latency_ms

    @property
    def retryable(self) -> bool:
        return self.retry_disposition is ProviderRetryDisposition.SAFE

    def with_attempt_context(
        self,
        *,
        request_id: str,
        attempts: int,
        elapsed_latency_ms: float,
    ) -> ProviderFailure:
        return type(self)(
            str(self),
            error_class=self.error_class,
            diagnostic_code=self.diagnostic_code,
            http_status=self.http_status,
            request_id=request_id,
            generation_state=self.generation_state,
            retry_disposition=self.retry_disposition,
            retry_after_seconds=self.retry_after_seconds,
            attempts=attempts,
            elapsed_latency_ms=elapsed_latency_ms,
        )

    def safe_fields(self) -> dict[str, object]:
        return {
            "error_class": self.error_class,
            "diagnostic_code": self.diagnostic_code,
            "http_status": self.http_status,
            "request_id": self.request_id,
            "generation_state": self.generation_state.value,
            "retry_disposition": self.retry_disposition.value,
            "retry_after_seconds": self.retry_after_seconds,
            "attempts": self.attempts,
            "elapsed_latency_ms": self.elapsed_latency_ms,
        }


class ProviderAttemptPhase(StrEnum):
    DISPATCHED = "dispatched"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


@dataclass(frozen=True, slots=True)
class ProviderAttemptEvent:
    request_id: str
    method: str
    physical_attempt: int
    phase: ProviderAttemptPhase
    elapsed_latency_ms: float
    failure: ProviderFailure | None = None


ProviderAttemptObserver = Callable[[ProviderAttemptEvent], None]


@dataclass(frozen=True, slots=True)
class ProviderHealthPolicy:
    transient_failure_threshold: int = 3
    transient_cooldown_seconds: float = 60.0
    ambiguous_cooldown_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.transient_failure_threshold < 1:
            raise ValueError("provider transient threshold must be positive")
        for name in ("transient_cooldown_seconds", "ambiguous_cooldown_seconds"):
            value = cast(float, getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")


class ProviderCircuitState(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    COOLDOWN = "cooldown"
    OPEN = "open"


@dataclass(frozen=True, slots=True)
class ProviderAdmission:
    allowed: bool
    state: ProviderCircuitState
    diagnostic_code: str | None
    retry_after_seconds: float | None


@dataclass(frozen=True, slots=True)
class PendingProviderNotice:
    notice_id: int
    provider_id: str
    notice_kind: str
    created_at: datetime
    payload: dict[str, object]


class ProviderHealthStore:
    """Durable local provider incident, circuit, and operator-notice outbox."""

    _IMMEDIATE_CODES: ClassVar[set[str]] = {
        "auth_unavailable",
        "authentication_failed",
        "quota_exhausted",
    }

    def __init__(self, path: Path, *, policy: ProviderHealthPolicy | None = None) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.path = path.resolve()
        self.policy = policy or ProviderHealthPolicy()
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
                CREATE TABLE IF NOT EXISTS provider_health (
                    provider_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    consecutive_transient_failures INTEGER NOT NULL,
                    diagnostic_code TEXT,
                    cooldown_until TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS provider_incidents (
                    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_id TEXT NOT NULL,
                    request_id TEXT,
                    physical_attempt INTEGER NOT NULL,
                    error_class TEXT NOT NULL,
                    diagnostic_code TEXT NOT NULL,
                    http_status INTEGER,
                    generation_state TEXT NOT NULL,
                    retry_disposition TEXT NOT NULL,
                    retry_after_seconds REAL,
                    attempts INTEGER NOT NULL,
                    elapsed_latency_ms REAL NOT NULL,
                    observed_at TEXT NOT NULL,
                    UNIQUE(provider_id, request_id, physical_attempt)
                );
                CREATE TABLE IF NOT EXISTS pending_operator_notices (
                    notice_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_id TEXT NOT NULL,
                    notice_kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    delivered_at TEXT
                );
                """
            )

    def admission(self, provider_id: str, *, now: datetime | None = None) -> ProviderAdmission:
        observed_at = now or datetime.now(UTC)
        require_aware(observed_at, "provider admission time")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM provider_health WHERE provider_id = ?", (provider_id,)
            ).fetchone()
        if row is None:
            return ProviderAdmission(True, ProviderCircuitState.HEALTHY, None, None)
        state = ProviderCircuitState(cast(str, row["state"]))
        diagnostic = cast(str | None, row["diagnostic_code"])
        cooldown_text = cast(str | None, row["cooldown_until"])
        if state is ProviderCircuitState.OPEN:
            return ProviderAdmission(False, state, diagnostic, None)
        if state is ProviderCircuitState.COOLDOWN and cooldown_text is not None:
            cooldown_until = _parse_timestamp(cooldown_text)
            remaining = (cooldown_until - observed_at).total_seconds()
            if remaining > 0:
                return ProviderAdmission(False, state, diagnostic, remaining)
        return ProviderAdmission(True, state, diagnostic, None)

    def record_failure(
        self,
        *,
        provider_id: str,
        failure: ProviderFailure,
        physical_attempt: int,
        observed_at: datetime | None = None,
    ) -> None:
        now = observed_at or datetime.now(UTC)
        require_aware(now, "provider failure time")
        timestamp = _timestamp(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO provider_incidents(
                    provider_id, request_id, physical_attempt, error_class,
                    diagnostic_code, http_status, generation_state,
                    retry_disposition, retry_after_seconds, attempts,
                    elapsed_latency_ms, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider_id,
                    failure.request_id,
                    physical_attempt,
                    failure.error_class,
                    failure.diagnostic_code,
                    failure.http_status,
                    failure.generation_state.value,
                    failure.retry_disposition.value,
                    failure.retry_after_seconds,
                    failure.attempts,
                    failure.elapsed_latency_ms,
                    timestamp,
                ),
            ).rowcount
            if not inserted:
                return
            current = connection.execute(
                "SELECT * FROM provider_health WHERE provider_id = ?", (provider_id,)
            ).fetchone()
            if _is_stale_health_observation(current, now):
                return
            if current is not None and current["state"] == ProviderCircuitState.OPEN.value:
                return
            consecutive = 0 if current is None else int(current["consecutive_transient_failures"])
            notice_kind: str | None = None
            cooldown_until: datetime | None = None
            if failure.diagnostic_code in self._IMMEDIATE_CODES:
                state = ProviderCircuitState.OPEN
                notice_kind = "provider_action_required"
                consecutive = 0
            elif failure.generation_state is ProviderGenerationState.UNKNOWN:
                consecutive += 1
                state = ProviderCircuitState.COOLDOWN
                if consecutive >= self.policy.transient_failure_threshold:
                    cooldown_until = now + timedelta(seconds=self.policy.transient_cooldown_seconds)
                    notice_kind = "provider_persistent_failure"
                else:
                    cooldown_until = now + timedelta(seconds=self.policy.ambiguous_cooldown_seconds)
            elif _is_transient(failure):
                consecutive += 1
                if consecutive >= self.policy.transient_failure_threshold:
                    state = ProviderCircuitState.COOLDOWN
                    cooldown_until = now + timedelta(seconds=self.policy.transient_cooldown_seconds)
                    notice_kind = "provider_persistent_failure"
                else:
                    state = ProviderCircuitState.DEGRADED
            else:
                state = ProviderCircuitState.DEGRADED
                consecutive = 0
            connection.execute(
                """
                INSERT INTO provider_health(
                    provider_id, state, consecutive_transient_failures,
                    diagnostic_code, cooldown_until, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_id) DO UPDATE SET
                    state=excluded.state,
                    consecutive_transient_failures=excluded.consecutive_transient_failures,
                    diagnostic_code=excluded.diagnostic_code,
                    cooldown_until=excluded.cooldown_until,
                    updated_at=excluded.updated_at
                """,
                (
                    provider_id,
                    state.value,
                    consecutive,
                    failure.diagnostic_code,
                    None if cooldown_until is None else _timestamp(cooldown_until),
                    timestamp,
                ),
            )
            if notice_kind is not None:
                self._enqueue_notice(
                    connection,
                    provider_id=provider_id,
                    notice_kind=notice_kind,
                    created_at=timestamp,
                    payload={
                        "diagnostic_code": failure.diagnostic_code,
                        "error_class": failure.error_class,
                        "http_status": failure.http_status,
                        "request_id": failure.request_id,
                    },
                )

    def record_success(
        self,
        *,
        provider_id: str,
        request_id: str | None,
        observed_at: datetime | None = None,
    ) -> None:
        """Record an ordinary completion without overriding an OPEN circuit."""

        self._record_success(
            provider_id=provider_id,
            request_id=request_id,
            observed_at=observed_at,
            allow_open_recovery=False,
        )

    def record_probe_success(
        self,
        *,
        provider_id: str,
        request_id: str | None,
        observed_at: datetime | None = None,
    ) -> None:
        """Close a circuit only after an explicit safe health probe succeeds."""

        self._record_success(
            provider_id=provider_id,
            request_id=request_id,
            observed_at=observed_at,
            allow_open_recovery=True,
        )

    def _record_success(
        self,
        *,
        provider_id: str,
        request_id: str | None,
        observed_at: datetime | None,
        allow_open_recovery: bool,
    ) -> None:
        now = observed_at or datetime.now(UTC)
        require_aware(now, "provider success time")
        timestamp = _timestamp(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute(
                "SELECT state, updated_at FROM provider_health WHERE provider_id = ?",
                (provider_id,),
            ).fetchone()
            if _is_stale_health_observation(previous, now):
                return
            if (
                previous is not None
                and previous["state"] == ProviderCircuitState.OPEN.value
                and not allow_open_recovery
            ):
                return
            connection.execute(
                """
                INSERT INTO provider_health(
                    provider_id, state, consecutive_transient_failures,
                    diagnostic_code, cooldown_until, updated_at
                ) VALUES (?, ?, 0, NULL, NULL, ?)
                ON CONFLICT(provider_id) DO UPDATE SET
                    state=excluded.state,
                    consecutive_transient_failures=0,
                    diagnostic_code=NULL,
                    cooldown_until=NULL,
                    updated_at=excluded.updated_at
                """,
                (provider_id, ProviderCircuitState.HEALTHY.value, timestamp),
            )
            if previous is not None and previous["state"] != ProviderCircuitState.HEALTHY.value:
                self._enqueue_notice(
                    connection,
                    provider_id=provider_id,
                    notice_kind="provider_recovered",
                    created_at=timestamp,
                    payload={"request_id": request_id},
                )

    def operator_reset(self, *, provider_id: str, observed_at: datetime | None = None) -> None:
        """Explicitly re-admit a provider after the operator resolves an OPEN incident."""

        now = observed_at or datetime.now(UTC)
        require_aware(now, "provider operator reset time")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT updated_at FROM provider_health WHERE provider_id = ?", (provider_id,)
            ).fetchone()
            if current is None:
                raise KeyError(f"unknown provider health record: {provider_id}")
            if _is_stale_health_observation(current, now):
                return
            updated = connection.execute(
                """
                UPDATE provider_health
                SET state = ?, consecutive_transient_failures = 0,
                    diagnostic_code = NULL, cooldown_until = NULL, updated_at = ?
                WHERE provider_id = ?
                """,
                (ProviderCircuitState.HEALTHY.value, _timestamp(now), provider_id),
            ).rowcount
        if not updated:
            raise KeyError(f"unknown provider health record: {provider_id}")

    def mark_notice_delivered(
        self, *, notice_id: int, delivered_at: datetime | None = None
    ) -> None:
        """Acknowledge outbox delivery; this method never sends an external notice."""

        if notice_id < 1:
            raise ValueError("provider notice_id must be positive")
        now = delivered_at or datetime.now(UTC)
        require_aware(now, "provider notice delivery time")
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE pending_operator_notices SET delivered_at = ?
                WHERE notice_id = ? AND delivered_at IS NULL
                """,
                (_timestamp(now), notice_id),
            ).rowcount
        if not updated:
            raise KeyError(f"unknown or already delivered provider notice: {notice_id}")

    def pending_notices(
        self, *, provider_id: str | None = None
    ) -> tuple[PendingProviderNotice, ...]:
        query = "SELECT * FROM pending_operator_notices WHERE delivered_at IS NULL"
        parameters: tuple[object, ...] = ()
        if provider_id is not None:
            query += " AND provider_id = ?"
            parameters = (provider_id,)
        query += " ORDER BY notice_id"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return tuple(
            PendingProviderNotice(
                notice_id=int(row["notice_id"]),
                provider_id=cast(str, row["provider_id"]),
                notice_kind=cast(str, row["notice_kind"]),
                created_at=_parse_timestamp(cast(str, row["created_at"])),
                payload=cast(dict[str, object], json.loads(cast(str, row["payload_json"]))),
            )
            for row in rows
        )

    @staticmethod
    def _enqueue_notice(
        connection: sqlite3.Connection,
        *,
        provider_id: str,
        notice_kind: str,
        created_at: str,
        payload: dict[str, object],
    ) -> None:
        connection.execute(
            """
            INSERT INTO pending_operator_notices(
                provider_id, notice_kind, created_at, payload_json, delivered_at
            ) VALUES (?, ?, ?, ?, NULL)
            """,
            (provider_id, notice_kind, created_at, json.dumps(payload, sort_keys=True)),
        )


def _is_transient(failure: ProviderFailure) -> bool:
    return failure.error_class in {"http", "timeout", "transport", "tls"}


def _is_stale_health_observation(current: sqlite3.Row | None, observed_at: datetime) -> bool:
    if current is None:
        return False
    return observed_at < _parse_timestamp(cast(str, current["updated_at"]))


def _safe_token(value: str, name: str) -> str:
    if (
        not value
        or value != value.strip()
        or not all(character.isalnum() or character in {"_", "-", "."} for character in value)
    ):
        raise ValueError(f"provider {name} must be a safe token")
    return value


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "provider timestamp")
    return parsed.astimezone(UTC)
