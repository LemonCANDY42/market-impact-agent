from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.research import EvidenceTier

SOURCE_COVERAGE_REGISTRATION_SCHEMA = "market-impact.source-coverage-registration.v1"
COVERAGE_RECEIPT_SCHEMA = "market-impact.coverage-receipt.v1"


class CoverageRole(StrEnum):
    GLOBAL_DISCOVERY = "global_discovery"
    OFFICIAL_CONFIRMATION = "official_confirmation"


class CoverageFailureAction(StrEnum):
    RETAIN_AND_BLOCK_ACCRUAL = "retain_and_block_accrual"


@dataclass(frozen=True, slots=True)
class CoverageSource:
    provider_id: str
    provider_version: str
    endpoint: str
    role: CoverageRole
    required: bool
    occurrence_eligible: bool
    source_tier: EvidenceTier
    provides_revision_history: bool
    license_note: str

    def __post_init__(self) -> None:
        for name in ("provider_id", "provider_version", "endpoint", "license_note"):
            _nonempty(cast(str, getattr(self, name)), name)
        parsed = urlparse(self.endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("coverage source endpoint must be a fixed HTTPS URL")
        if self.occurrence_eligible and self.role is not CoverageRole.OFFICIAL_CONFIRMATION:
            raise ValueError("occurrence-eligible sources must be official confirmation sources")
        if self.occurrence_eligible and self.source_tier not in {
            EvidenceTier.OFFICIAL,
            EvidenceTier.PRIMARY,
        }:
            raise ValueError("occurrence-eligible sources must be official or primary")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "endpoint": self.endpoint,
            "role": self.role.value,
            "required": self.required,
            "occurrence_eligible": self.occurrence_eligible,
            "source_tier": self.source_tier.value,
            "provides_revision_history": self.provides_revision_history,
            "license_note": self.license_note,
        }


@dataclass(frozen=True, slots=True)
class SourceCoverageRegistration:
    coverage_registration_id: str
    registered_at: datetime
    prospective_registration_id: str
    prospective_registration_hash: str
    observable_universe: str
    polling_interval_minutes: int
    maximum_cycle_seconds: int
    failure_action: CoverageFailureAction
    known_blind_spots: tuple[str, ...]
    sources: tuple[CoverageSource, ...]

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "source coverage registered_at")
        for name in (
            "prospective_registration_id",
            "prospective_registration_hash",
            "observable_universe",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        _sha256(self.prospective_registration_hash, "prospective_registration_hash")
        if not 1 <= self.polling_interval_minutes <= 60:
            raise ValueError("polling_interval_minutes must be between 1 and 60")
        if not 1 <= self.maximum_cycle_seconds <= self.polling_interval_minutes * 60:
            raise ValueError("maximum_cycle_seconds must fit within one polling interval")
        _unique_nonempty(self.known_blind_spots, "known_blind_spots")
        if len(self.sources) < 2:
            raise ValueError("source coverage requires discovery and confirmation sources")
        provider_ids = tuple(item.provider_id for item in self.sources)
        if len(provider_ids) != len(set(provider_ids)):
            raise ValueError("source coverage provider_id values must be unique")
        if not any(
            item.required and item.role is CoverageRole.GLOBAL_DISCOVERY for item in self.sources
        ):
            raise ValueError("source coverage requires a mandatory global discovery source")
        if not any(
            item.required and item.role is CoverageRole.OFFICIAL_CONFIRMATION
            for item in self.sources
        ):
            raise ValueError("source coverage requires a mandatory confirmation source")
        if not any(item.occurrence_eligible for item in self.sources):
            raise ValueError("source coverage requires an occurrence-eligible source")
        if self.coverage_registration_id != self.expected_coverage_registration_id:
            raise ValueError("coverage_registration_id does not match content")

    @property
    def coverage_registration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_coverage_registration_id(self) -> str:
        return f"source-coverage-{self.coverage_registration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": SOURCE_COVERAGE_REGISTRATION_SCHEMA,
            "registered_at": _timestamp(self.registered_at),
            "prospective_registration_id": self.prospective_registration_id,
            "prospective_registration_hash": self.prospective_registration_hash,
            "observable_universe": self.observable_universe,
            "polling_interval_minutes": self.polling_interval_minutes,
            "maximum_cycle_seconds": self.maximum_cycle_seconds,
            "failure_action": self.failure_action.value,
            "known_blind_spots": list(self.known_blind_spots),
            "sources": [item.to_dict() for item in self.sources],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "coverage_registration_id": self.coverage_registration_id}

    def source(self, provider_id: str) -> CoverageSource:
        match = next((item for item in self.sources if item.provider_id == provider_id), None)
        if match is None:
            raise KeyError(f"provider is outside Source Coverage Registration: {provider_id}")
        return match


@dataclass(frozen=True, slots=True)
class CoverageAttempt:
    provider_id: str
    requested_at: datetime
    retrieved_at: datetime | None
    succeeded: bool
    content_hash: str | None
    record_count: int | None
    error_class: str | None
    error_summary: str | None

    def __post_init__(self) -> None:
        _nonempty(self.provider_id, "coverage attempt provider_id")
        require_aware(self.requested_at, "coverage attempt requested_at")
        if self.retrieved_at is not None:
            require_aware(self.retrieved_at, "coverage attempt retrieved_at")
            if self.retrieved_at < self.requested_at:
                raise ValueError("coverage attempt retrieval cannot precede request")
        if self.succeeded:
            if (
                self.retrieved_at is None
                or self.content_hash is None
                or self.record_count is None
                or self.error_class is not None
                or self.error_summary is not None
            ):
                raise ValueError("successful coverage attempt fields are incomplete")
            _sha256(self.content_hash, "coverage attempt content_hash")
            if self.record_count < 0:
                raise ValueError("coverage attempt record_count must be non-negative")
        elif (
            self.retrieved_at is not None
            or self.content_hash is not None
            or self.record_count is not None
            or self.error_class is None
            or self.error_summary is None
        ):
            raise ValueError("failed coverage attempt fields are incomplete")
        if self.error_class is not None:
            _nonempty(self.error_class, "coverage attempt error_class")
        if self.error_summary is not None:
            _nonempty(self.error_summary, "coverage attempt error_summary")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "requested_at": _timestamp(self.requested_at),
            "retrieved_at": _optional_timestamp(self.retrieved_at),
            "succeeded": self.succeeded,
            "content_hash": self.content_hash,
            "record_count": self.record_count,
            "error_class": self.error_class,
            "error_summary": self.error_summary,
        }


@dataclass(frozen=True, slots=True)
class CoverageReceipt:
    receipt_id: str
    coverage_registration_id: str
    coverage_registration_hash: str
    cycle_started_at: datetime
    cycle_completed_at: datetime
    attempts: tuple[CoverageAttempt, ...]

    def __post_init__(self) -> None:
        for name in ("coverage_registration_id", "coverage_registration_hash"):
            _nonempty(cast(str, getattr(self, name)), name)
        _sha256(self.coverage_registration_hash, "coverage_registration_hash")
        require_aware(self.cycle_started_at, "coverage cycle_started_at")
        require_aware(self.cycle_completed_at, "coverage cycle_completed_at")
        if self.cycle_completed_at < self.cycle_started_at:
            raise ValueError("coverage cycle completion cannot precede start")
        provider_ids = tuple(item.provider_id for item in self.attempts)
        if not provider_ids or len(provider_ids) != len(set(provider_ids)):
            raise ValueError("Coverage Receipt requires unique Provider attempts")
        if self.receipt_id != self.expected_receipt_id:
            raise ValueError("Coverage Receipt receipt_id does not match content")

    @property
    def receipt_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_receipt_id(self) -> str:
        return f"coverage-receipt-{self.receipt_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": COVERAGE_RECEIPT_SCHEMA,
            "coverage_registration_id": self.coverage_registration_id,
            "coverage_registration_hash": self.coverage_registration_hash,
            "cycle_started_at": _timestamp(self.cycle_started_at),
            "cycle_completed_at": _timestamp(self.cycle_completed_at),
            "attempts": [item.to_dict() for item in self.attempts],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "receipt_id": self.receipt_id}

    def validate_against(self, registration: SourceCoverageRegistration) -> None:
        if (
            self.coverage_registration_id != registration.coverage_registration_id
            or self.coverage_registration_hash != registration.coverage_registration_hash
        ):
            raise ValueError("Coverage Receipt does not match Source Coverage Registration")
        if tuple(item.provider_id for item in self.attempts) != tuple(
            item.provider_id for item in registration.sources
        ):
            raise ValueError("Coverage Receipt attempts do not match registered source order")

    def is_complete(self, registration: SourceCoverageRegistration) -> bool:
        self.validate_against(registration)
        attempts = {item.provider_id: item for item in self.attempts}
        within_duration = self.cycle_completed_at - self.cycle_started_at <= timedelta(
            seconds=registration.maximum_cycle_seconds
        )
        return within_duration and all(
            not source.required or attempts[source.provider_id].succeeded
            for source in registration.sources
        )

    def attempt(self, provider_id: str) -> CoverageAttempt:
        match = next((item for item in self.attempts if item.provider_id == provider_id), None)
        if match is None:
            raise KeyError(f"Coverage Receipt has no Provider attempt: {provider_id}")
        return match


def source_coverage_registration_from_dict(value: object) -> SourceCoverageRegistration:
    payload = _object(value, "Source Coverage Registration")
    _closed(
        payload,
        {
            "schema_version",
            "coverage_registration_id",
            "registered_at",
            "prospective_registration_id",
            "prospective_registration_hash",
            "observable_universe",
            "polling_interval_minutes",
            "maximum_cycle_seconds",
            "failure_action",
            "known_blind_spots",
            "sources",
        },
        "Source Coverage Registration",
    )
    if _string(payload, "schema_version") != SOURCE_COVERAGE_REGISTRATION_SCHEMA:
        raise ValueError("unsupported Source Coverage Registration schema_version")
    registration = SourceCoverageRegistration(
        coverage_registration_id=_string(payload, "coverage_registration_id"),
        registered_at=_datetime(payload, "registered_at"),
        prospective_registration_id=_string(payload, "prospective_registration_id"),
        prospective_registration_hash=_string(payload, "prospective_registration_hash"),
        observable_universe=_string(payload, "observable_universe"),
        polling_interval_minutes=_integer(payload, "polling_interval_minutes"),
        maximum_cycle_seconds=_integer(payload, "maximum_cycle_seconds"),
        failure_action=CoverageFailureAction(_string(payload, "failure_action")),
        known_blind_spots=_string_tuple(payload, "known_blind_spots"),
        sources=tuple(_coverage_source(item) for item in _object_list(payload, "sources")),
    )
    if registration.to_dict() != payload:
        raise ValueError("Source Coverage Registration does not match canonical contract")
    return registration


def coverage_receipt_from_dict(value: object) -> CoverageReceipt:
    payload = _object(value, "Coverage Receipt")
    _closed(
        payload,
        {
            "schema_version",
            "receipt_id",
            "coverage_registration_id",
            "coverage_registration_hash",
            "cycle_started_at",
            "cycle_completed_at",
            "attempts",
        },
        "Coverage Receipt",
    )
    if _string(payload, "schema_version") != COVERAGE_RECEIPT_SCHEMA:
        raise ValueError("unsupported Coverage Receipt schema_version")
    receipt = CoverageReceipt(
        receipt_id=_string(payload, "receipt_id"),
        coverage_registration_id=_string(payload, "coverage_registration_id"),
        coverage_registration_hash=_string(payload, "coverage_registration_hash"),
        cycle_started_at=_datetime(payload, "cycle_started_at"),
        cycle_completed_at=_datetime(payload, "cycle_completed_at"),
        attempts=tuple(_coverage_attempt(item) for item in _object_list(payload, "attempts")),
    )
    if receipt.to_dict() != payload:
        raise ValueError("Coverage Receipt does not match canonical contract")
    return receipt


def load_source_coverage_registration(path: Path) -> SourceCoverageRegistration:
    import json

    return source_coverage_registration_from_dict(json.loads(path.read_text(encoding="utf-8")))


def _coverage_source(payload: dict[str, object]) -> CoverageSource:
    _closed(
        payload,
        {
            "provider_id",
            "provider_version",
            "endpoint",
            "role",
            "required",
            "occurrence_eligible",
            "source_tier",
            "provides_revision_history",
            "license_note",
        },
        "coverage source",
    )
    return CoverageSource(
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        endpoint=_string(payload, "endpoint"),
        role=CoverageRole(_string(payload, "role")),
        required=_boolean(payload, "required"),
        occurrence_eligible=_boolean(payload, "occurrence_eligible"),
        source_tier=EvidenceTier(_string(payload, "source_tier")),
        provides_revision_history=_boolean(payload, "provides_revision_history"),
        license_note=_string(payload, "license_note"),
    )


def _coverage_attempt(payload: dict[str, object]) -> CoverageAttempt:
    _closed(
        payload,
        {
            "provider_id",
            "requested_at",
            "retrieved_at",
            "succeeded",
            "content_hash",
            "record_count",
            "error_class",
            "error_summary",
        },
        "coverage attempt",
    )
    return CoverageAttempt(
        provider_id=_string(payload, "provider_id"),
        requested_at=_datetime(payload, "requested_at"),
        retrieved_at=_nullable_datetime(payload, "retrieved_at"),
        succeeded=_boolean(payload, "succeeded"),
        content_hash=_nullable_string(payload, "content_hash"),
        record_count=_nullable_integer(payload, "record_count"),
        error_class=_nullable_string(payload, "error_class"),
        error_summary=_nullable_string(payload, "error_summary"),
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], value)


def _object_list(value: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    raw = value.get(name)
    if not isinstance(raw, list):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, name) for item in cast(list[object], raw))


def _closed(value: dict[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise ValueError(
            f"{name} fields do not match contract: "
            f"missing={sorted(expected - set(value))}, extra={sorted(set(value) - expected)}"
        )


def _string(value: dict[str, object], name: str) -> str:
    raw = value.get(name)
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return raw


def _nullable_string(value: dict[str, object], name: str) -> str | None:
    if value.get(name) is None:
        return None
    return _string(value, name)


def _integer(value: dict[str, object], name: str) -> int:
    raw = value.get(name)
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise TypeError(f"{name} must be an integer")
    return raw


def _nullable_integer(value: dict[str, object], name: str) -> int | None:
    if value.get(name) is None:
        return None
    return _integer(value, name)


def _boolean(value: dict[str, object], name: str) -> bool:
    raw = value.get(name)
    if not isinstance(raw, bool):
        raise TypeError(f"{name} must be a boolean")
    return raw


def _string_tuple(value: dict[str, object], name: str) -> tuple[str, ...]:
    raw = value.get(name)
    if not isinstance(raw, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], raw)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], items))


def _datetime(value: dict[str, object], name: str) -> datetime:
    raw = _string(value, name)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _nullable_datetime(value: dict[str, object], name: str) -> datetime | None:
    if value.get(name) is None:
        return None
    return _datetime(value, name)


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if not values or len(values) != len(set(values)):
        raise ValueError(f"{name} must contain unique values")
    for value in values:
        _nonempty(value, name)


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 hex digest")
