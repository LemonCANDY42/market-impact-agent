from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_study import AgentPhase2Preregistration, ExposureRegistry
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import AvailabilityBasis
from market_impact_agent.research import EvidenceTier
from market_impact_agent.runtime_store import ArtifactStore
from market_impact_agent.source_coverage import (
    CoverageReceipt,
    SourceCoverageRegistration,
    coverage_receipt_from_dict,
)

CANDIDATE_EVENT_OBSERVATION_SCHEMA = "market-impact.candidate-event-observation.v1"
PHYSICAL_ENERGY_COMMODITIES = frozenset({"crude_oil", "natural_gas"})
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")


class EventNature(StrEnum):
    PHYSICAL_PRODUCTION_LOSS = "physical_production_loss"
    PHYSICAL_TRANSPORT_LOSS = "physical_transport_loss"
    PHYSICAL_STORAGE_LOSS = "physical_storage_loss"
    POLICY_ONLY = "policy_only"
    DEMAND_ONLY = "demand_only"
    PLANNED_MAINTENANCE = "planned_maintenance"
    RETROSPECTIVE_ONLY = "retrospective_only"
    UNCLASSIFIED = "unclassified"

    @property
    def qualifying(self) -> bool:
        return self in {
            EventNature.PHYSICAL_PRODUCTION_LOSS,
            EventNature.PHYSICAL_TRANSPORT_LOSS,
            EventNature.PHYSICAL_STORAGE_LOSS,
        }


class LossUnit(StrEnum):
    BOE_PER_DAY = "boe_per_day"
    REGIONAL_SUPPLY_FRACTION = "regional_supply_fraction"


class AccrualDisposition(StrEnum):
    ACCRUED = "accrued"
    NOT_ACCRUED = "not_accrued"


class AccrualReason(StrEnum):
    ALREADY_ACCRUED = "already_accrued"
    COHORT_FULL = "cohort_full"
    DURATION_THRESHOLD_NOT_MET = "duration_threshold_not_met"
    EVENT_NATURE_EXCLUDED = "event_nature_excluded"
    LOSS_THRESHOLD_NOT_MET = "loss_threshold_not_met"
    MISSING_CRITICAL_DATA = "missing_critical_data"
    OUTSIDE_ACCRUAL_WINDOW = "outside_accrual_window"
    SEPARATION_WINDOW_NOT_MET = "separation_window_not_met"
    SOURCE_TIER_NOT_QUALIFYING = "source_tier_not_qualifying"
    SOURCE_COVERAGE_INCOMPLETE = "source_coverage_incomplete"
    UNSUPPORTED_COMMODITY = "unsupported_commodity"


@dataclass(frozen=True, slots=True)
class OccurrenceSourceObservation:
    provider_id: str
    upstream_source: str
    upstream_record_id: str
    source_ref: str
    source_tier: EvidenceTier
    occurred_at: datetime | None
    published_at: datetime
    source_updated_at: datetime | None
    available_at: datetime
    retrieved_at: datetime
    availability_basis: AvailabilityBasis
    raw_content_hash: str
    claim_summary: str
    claim_hash: str

    def __post_init__(self) -> None:
        for name in (
            "provider_id",
            "upstream_source",
            "upstream_record_id",
            "source_ref",
            "claim_summary",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        for name in ("published_at", "available_at", "retrieved_at"):
            require_aware(cast(datetime, getattr(self, name)), name)
        if self.occurred_at is not None:
            require_aware(self.occurred_at, "occurred_at")
        if self.source_updated_at is not None:
            require_aware(self.source_updated_at, "source_updated_at")
        if self.occurred_at is not None and self.occurred_at > self.retrieved_at:
            raise ValueError("occurred_at must not be after retrieved_at")
        if self.published_at > self.available_at:
            raise ValueError("published_at must not be after available_at")
        if self.source_updated_at is not None and not (
            self.published_at <= self.source_updated_at <= self.available_at
        ):
            raise ValueError("source_updated_at must be between published_at and available_at")
        if self.availability_basis is not AvailabilityBasis.ACTUAL_RECEIPT:
            raise ValueError("prospective accrual requires actual-receipt availability")
        if self.available_at != self.retrieved_at:
            raise ValueError("actual-receipt available_at must equal retrieved_at")
        _sha256(self.raw_content_hash, "raw_content_hash")
        expected_claim_hash = sha256(self.claim_summary.encode()).hexdigest()
        if self.claim_hash != expected_claim_hash:
            raise ValueError("claim_hash must match claim_summary")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "upstream_source": self.upstream_source,
            "upstream_record_id": self.upstream_record_id,
            "source_ref": self.source_ref,
            "source_tier": self.source_tier.value,
            "occurred_at": _optional_timestamp(self.occurred_at),
            "published_at": _timestamp(self.published_at),
            "source_updated_at": _optional_timestamp(self.source_updated_at),
            "available_at": _timestamp(self.available_at),
            "retrieved_at": _timestamp(self.retrieved_at),
            "availability_basis": self.availability_basis.value,
            "raw_content_hash": self.raw_content_hash,
            "claim_summary": self.claim_summary,
            "claim_hash": self.claim_hash,
        }


@dataclass(frozen=True, slots=True)
class CandidateEventObservation:
    observation_id: str
    event_id: str
    source_coverage_registration_id: str
    source_coverage_registration_hash: str
    coverage_receipt_id: str
    coverage_receipt_hash: str
    event_nature: EventNature
    affected_commodity: str | None
    loss_amount: Decimal | None
    loss_unit: LossUnit | None
    regional_denominator_source_ref: str | None
    regional_denominator_source_tier: EvidenceTier | None
    regional_denominator_available_at: datetime | None
    regional_denominator_raw_content_hash: str | None
    expected_duration_hours: Decimal | None
    source: OccurrenceSourceObservation
    supersedes_observation_id: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.event_id, "event_id")
        _nonempty(
            self.source_coverage_registration_id,
            "source_coverage_registration_id",
        )
        _sha256(
            self.source_coverage_registration_hash,
            "source_coverage_registration_hash",
        )
        _nonempty(self.coverage_receipt_id, "coverage_receipt_id")
        _sha256(self.coverage_receipt_hash, "coverage_receipt_hash")
        if self.affected_commodity is not None:
            _nonempty(self.affected_commodity, "affected_commodity")
        if self.loss_amount is not None and (
            not self.loss_amount.is_finite() or self.loss_amount <= 0
        ):
            raise ValueError("loss_amount must be finite and positive")
        if self.expected_duration_hours is not None and (
            not self.expected_duration_hours.is_finite() or self.expected_duration_hours <= 0
        ):
            raise ValueError("expected_duration_hours must be finite and positive")
        if (self.loss_amount is None) is not (self.loss_unit is None):
            raise ValueError("loss_amount and loss_unit must be present or missing together")
        if self.loss_unit is LossUnit.REGIONAL_SUPPLY_FRACTION:
            assert self.loss_amount is not None
            if self.loss_amount > Decimal("1"):
                raise ValueError("regional_supply_fraction must not exceed one")
            if (
                self.regional_denominator_source_ref is None
                or self.regional_denominator_source_tier is None
                or self.regional_denominator_available_at is None
                or self.regional_denominator_raw_content_hash is None
            ):
                raise ValueError("regional supply fractions require an official denominator source")
            _nonempty(
                self.regional_denominator_source_ref,
                "regional_denominator_source_ref",
            )
            if self.regional_denominator_source_tier is not EvidenceTier.OFFICIAL:
                raise ValueError("regional supply denominator source must be official")
            require_aware(
                self.regional_denominator_available_at,
                "regional_denominator_available_at",
            )
            if (
                self.source.occurred_at is not None
                and self.regional_denominator_available_at > self.source.occurred_at
            ):
                raise ValueError("regional supply denominator must be visible before the event")
            _sha256(
                self.regional_denominator_raw_content_hash,
                "regional_denominator_raw_content_hash",
            )
        elif any(
            value is not None
            for value in (
                self.regional_denominator_source_ref,
                self.regional_denominator_source_tier,
                self.regional_denominator_available_at,
                self.regional_denominator_raw_content_hash,
            )
        ):
            raise ValueError("boe_per_day observations must not define a regional denominator")
        if self.supersedes_observation_id is not None:
            _identifier(self.supersedes_observation_id, "supersedes_observation_id")
            if self.supersedes_observation_id == self.observation_id:
                raise ValueError("Candidate Event Observation cannot supersede itself")
        if self.observation_id != self.expected_observation_id:
            raise ValueError("Candidate Event Observation observation_id does not match content")

    @property
    def observation_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_observation_id(self) -> str:
        return f"candidate-observation-{self.observation_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": CANDIDATE_EVENT_OBSERVATION_SCHEMA,
            "event_id": self.event_id,
            "source_coverage_registration_id": self.source_coverage_registration_id,
            "source_coverage_registration_hash": self.source_coverage_registration_hash,
            "coverage_receipt_id": self.coverage_receipt_id,
            "coverage_receipt_hash": self.coverage_receipt_hash,
            "event_nature": self.event_nature.value,
            "affected_commodity": self.affected_commodity,
            "loss_amount": None if self.loss_amount is None else str(self.loss_amount),
            "loss_unit": None if self.loss_unit is None else self.loss_unit.value,
            "regional_denominator_source_ref": self.regional_denominator_source_ref,
            "regional_denominator_source_tier": (
                None
                if self.regional_denominator_source_tier is None
                else self.regional_denominator_source_tier.value
            ),
            "regional_denominator_available_at": _optional_timestamp(
                self.regional_denominator_available_at
            ),
            "regional_denominator_raw_content_hash": (self.regional_denominator_raw_content_hash),
            "expected_duration_hours": (
                None if self.expected_duration_hours is None else str(self.expected_duration_hours)
            ),
            "source": self.source.to_dict(),
            "supersedes_observation_id": self.supersedes_observation_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "observation_id": self.observation_id}


@dataclass(frozen=True, slots=True)
class AccrualDecision:
    sequence: int
    registration_id: str
    observation: CandidateEventObservation
    coverage_receipt: CoverageReceipt
    disposition: AccrualDisposition
    reasons: tuple[AccrualReason, ...]
    qualifying_visible_at: datetime | None
    evidence_cutoff_at: datetime | None
    accrued_event_id: str | None
    recorded_at: datetime
    previous_hash: str | None
    decision_hash: str

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("Accrual Decision sequence must be positive")
        _nonempty(self.registration_id, "registration_id")
        if (
            self.observation.coverage_receipt_id != self.coverage_receipt.receipt_id
            or self.observation.coverage_receipt_hash != self.coverage_receipt.receipt_hash
        ):
            raise ValueError("Accrual Decision coverage receipt does not match observation")
        require_aware(self.recorded_at, "recorded_at")
        if self.previous_hash is not None:
            _sha256(self.previous_hash, "previous_hash")
        _sha256(self.decision_hash, "decision_hash")
        if self.disposition is AccrualDisposition.ACCRUED:
            if self.reasons:
                raise ValueError("accrued decisions cannot contain rejection reasons")
            if (
                self.qualifying_visible_at is None
                or self.evidence_cutoff_at is None
                or self.accrued_event_id is None
            ):
                raise ValueError("accrued decisions require visibility, cutoff, and event ID")
        elif (
            not self.reasons
            or self.qualifying_visible_at is not None
            or self.evidence_cutoff_at is not None
            or self.accrued_event_id is not None
        ):
            raise ValueError("non-accrued decisions require reasons and no accrual identities")

    def core_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "registration_id": self.registration_id,
            "observation_hash": self.observation.observation_hash,
            "coverage_receipt_hash": self.coverage_receipt.receipt_hash,
            "disposition": self.disposition.value,
            "reasons": [item.value for item in self.reasons],
            "qualifying_visible_at": _optional_timestamp(self.qualifying_visible_at),
            "evidence_cutoff_at": _optional_timestamp(self.evidence_cutoff_at),
            "accrued_event_id": self.accrued_event_id,
            "recorded_at": _timestamp(self.recorded_at),
            "previous_hash": self.previous_hash,
        }


class AccrualLedger:
    def __init__(
        self,
        path: Path,
        *,
        registration: AgentPhase2Preregistration,
        registry: ExposureRegistry,
        coverage_registration: SourceCoverageRegistration,
        created_at: datetime,
    ) -> None:
        registration.validate_against(registry)
        if (
            coverage_registration.prospective_registration_id != registration.registration_id
            or coverage_registration.prospective_registration_hash != registration.registration_hash
        ):
            raise ValueError("Source Coverage Registration does not match prospective study")
        if coverage_registration.registered_at >= registration.accrual.opens_after:
            raise ValueError("Source Coverage Registration must be frozen before accrual opens")
        require_aware(created_at, "ledger created_at")
        if created_at < registration.registered_at:
            raise ValueError("Accrual Ledger cannot be created before study registration")
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ValueError("Accrual Ledger path must be a regular file")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.path = path.resolve()
        self.registration = registration
        self.registry = registry
        self.coverage_registration = coverage_registration
        self.source_artifacts = ArtifactStore(self.path.parent / "source-artifacts")
        self._initialize(created_at)
        os.chmod(self.path, 0o600)
        self.decisions()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self, created_at: datetime) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ledger_metadata (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    registration_id TEXT NOT NULL,
                    registration_hash TEXT NOT NULL,
                    registry_id TEXT NOT NULL,
                    registry_hash TEXT NOT NULL,
                    coverage_registration_id TEXT NOT NULL,
                    coverage_registration_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS accrual_decisions (
                    sequence INTEGER PRIMARY KEY,
                    observation_id TEXT NOT NULL UNIQUE,
                    event_id TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    observation_hash TEXT NOT NULL,
                    coverage_receipt_json TEXT NOT NULL,
                    coverage_receipt_hash TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    reasons_json TEXT NOT NULL,
                    qualifying_visible_at TEXT,
                    evidence_cutoff_at TEXT,
                    accrued_event_id TEXT UNIQUE,
                    recorded_at TEXT NOT NULL,
                    previous_hash TEXT,
                    decision_hash TEXT NOT NULL UNIQUE
                );
                CREATE INDEX IF NOT EXISTS accrual_decisions_event_sequence
                    ON accrual_decisions(event_id, sequence);
                """
            )
            existing = connection.execute(
                "SELECT * FROM ledger_metadata WHERE singleton = 1"
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO ledger_metadata(
                        singleton, registration_id, registration_hash,
                        registry_id, registry_hash,
                        coverage_registration_id, coverage_registration_hash,
                        created_at
                    ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.registration.registration_id,
                        self.registration.registration_hash,
                        self.registry.registry_id,
                        self.registry.registry_hash,
                        self.coverage_registration.coverage_registration_id,
                        self.coverage_registration.coverage_registration_hash,
                        _timestamp(created_at),
                    ),
                )
            else:
                self._validate_metadata(existing)

    def _validate_metadata(self, row: sqlite3.Row) -> None:
        expected = {
            "registration_id": self.registration.registration_id,
            "registration_hash": self.registration.registration_hash,
            "registry_id": self.registry.registry_id,
            "registry_hash": self.registry.registry_hash,
            "coverage_registration_id": (self.coverage_registration.coverage_registration_id),
            "coverage_registration_hash": (self.coverage_registration.coverage_registration_hash),
        }
        for name, value in expected.items():
            if cast(str, row[name]) != value:
                raise ValueError(f"Accrual Ledger {name} does not match frozen study")
        _parse_timestamp(cast(str, row["created_at"]))

    def record(
        self,
        observation: CandidateEventObservation,
        *,
        recorded_at: datetime,
        raw_source: bytes,
        coverage_receipt: CoverageReceipt,
        regional_denominator_source: bytes | None = None,
    ) -> AccrualDecision:
        require_aware(recorded_at, "recorded_at")
        if recorded_at < observation.source.retrieved_at:
            raise ValueError("recorded_at must not precede source retrieval")
        self._validate_coverage(observation, coverage_receipt)
        self._retain_source_artifacts(
            observation,
            raw_source=raw_source,
            coverage_receipt=coverage_receipt,
            regional_denominator_source=regional_denominator_source,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            decisions = self._decisions(connection)
            existing = next(
                (
                    item
                    for item in decisions
                    if item.observation.observation_id == observation.observation_id
                ),
                None,
            )
            if existing is not None:
                if existing.observation.to_dict() != observation.to_dict():
                    raise ValueError("observation_id already exists with different content")
                return existing
            if decisions and recorded_at < decisions[-1].recorded_at:
                raise ValueError("Accrual Decisions must be appended in recording order")
            self._validate_append_order(observation, decisions)
            sequence = len(decisions) + 1
            previous_hash = None if not decisions else decisions[-1].decision_hash
            disposition, reasons = _evaluate_observation(
                self.registration,
                self.coverage_registration,
                observation,
                coverage_receipt,
                decisions,
            )
            qualifying_visible_at: datetime | None = None
            evidence_cutoff_at: datetime | None = None
            accrued_event_id: str | None = None
            if disposition is AccrualDisposition.ACCRUED:
                qualifying_visible_at = observation.source.available_at.astimezone(UTC)
                evidence_cutoff_at = qualifying_visible_at + timedelta(
                    minutes=self.registration.agent_protocol.assessment_delay_minutes
                )
                accrued_event_id = "accrued-event-" + canonical_hash(
                    {
                        "registration_id": self.registration.registration_id,
                        "event_id": observation.event_id,
                        "observation_hash": observation.observation_hash,
                    }
                )
            decision_core = {
                "sequence": sequence,
                "registration_id": self.registration.registration_id,
                "observation_hash": observation.observation_hash,
                "coverage_receipt_hash": coverage_receipt.receipt_hash,
                "disposition": disposition.value,
                "reasons": [item.value for item in reasons],
                "qualifying_visible_at": _optional_timestamp(qualifying_visible_at),
                "evidence_cutoff_at": _optional_timestamp(evidence_cutoff_at),
                "accrued_event_id": accrued_event_id,
                "recorded_at": _timestamp(recorded_at),
                "previous_hash": previous_hash,
            }
            decision_hash = canonical_hash(decision_core)
            connection.execute(
                """
                INSERT INTO accrual_decisions(
                    sequence, observation_id, event_id, observation_json,
                    observation_hash, coverage_receipt_json, coverage_receipt_hash,
                    disposition, reasons_json,
                    qualifying_visible_at, evidence_cutoff_at, accrued_event_id,
                    recorded_at, previous_hash, decision_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    observation.observation_id,
                    observation.event_id,
                    canonical_json_bytes(observation.to_dict()).decode(),
                    observation.observation_hash,
                    canonical_json_bytes(coverage_receipt.to_dict()).decode(),
                    coverage_receipt.receipt_hash,
                    disposition.value,
                    canonical_json_bytes([item.value for item in reasons]).decode(),
                    _optional_timestamp(qualifying_visible_at),
                    _optional_timestamp(evidence_cutoff_at),
                    accrued_event_id,
                    _timestamp(recorded_at),
                    previous_hash,
                    decision_hash,
                ),
            )
            row = connection.execute(
                "SELECT * FROM accrual_decisions WHERE sequence = ?",
                (sequence,),
            ).fetchone()
        if row is None:
            raise RuntimeError("appended Accrual Decision could not be read back")
        return self._verified_decision(row)

    def _retain_source_artifacts(
        self,
        observation: CandidateEventObservation,
        *,
        raw_source: bytes,
        coverage_receipt: CoverageReceipt,
        regional_denominator_source: bytes | None,
    ) -> None:
        _retain_exact_artifact(
            self.source_artifacts,
            raw_source,
            observation.source.raw_content_hash,
            "raw source",
        )
        stored_receipt = self.source_artifacts.put_json(coverage_receipt.core_dict())
        if stored_receipt.content_hash != coverage_receipt.receipt_hash:
            raise ValueError("Coverage Receipt artifact hash does not match receipt")
        denominator_hash = observation.regional_denominator_raw_content_hash
        if denominator_hash is None:
            if regional_denominator_source is not None:
                raise ValueError(
                    "regional denominator bytes are invalid for boe_per_day observation"
                )
            return
        if regional_denominator_source is None:
            raise ValueError("regional denominator source bytes are required")
        _retain_exact_artifact(
            self.source_artifacts,
            regional_denominator_source,
            denominator_hash,
            "regional denominator source",
        )

    def _verify_source_artifacts(self, observation: CandidateEventObservation) -> None:
        self.source_artifacts.get(
            observation.source.raw_content_hash,
            media_type="application/octet-stream",
        )
        self.source_artifacts.get(
            observation.coverage_receipt_hash,
            media_type="application/json",
        )
        denominator_hash = observation.regional_denominator_raw_content_hash
        if denominator_hash is not None:
            self.source_artifacts.get(
                denominator_hash,
                media_type="application/octet-stream",
            )

    def _validate_append_order(
        self,
        observation: CandidateEventObservation,
        decisions: tuple[AccrualDecision, ...],
    ) -> None:
        if (
            decisions
            and observation.source.available_at < decisions[-1].observation.source.available_at
        ):
            raise ValueError("Candidate Event Observations must be appended in receipt order")
        same_event = tuple(
            item for item in decisions if item.observation.event_id == observation.event_id
        )
        if not same_event:
            if observation.supersedes_observation_id is not None:
                raise ValueError("superseded Candidate Event Observation is missing")
            return
        latest = same_event[-1].observation
        if observation.supersedes_observation_id != latest.observation_id:
            raise ValueError("event revisions must supersede the latest recorded observation")
        if observation.source.available_at <= latest.source.available_at:
            raise ValueError("event revisions must advance source availability")
        if latest.source.occurred_at is not None and (
            observation.source.occurred_at != latest.source.occurred_at
        ):
            raise ValueError("event revisions must preserve occurrence time")
        if latest.affected_commodity is not None and (
            observation.affected_commodity != latest.affected_commodity
        ):
            raise ValueError("event revisions must preserve affected commodity")

    def _validate_coverage(
        self,
        observation: CandidateEventObservation,
        receipt: CoverageReceipt,
    ) -> None:
        receipt.validate_against(self.coverage_registration)
        if (
            observation.source_coverage_registration_id
            != self.coverage_registration.coverage_registration_id
            or observation.source_coverage_registration_hash
            != self.coverage_registration.coverage_registration_hash
            or observation.coverage_receipt_id != receipt.receipt_id
            or observation.coverage_receipt_hash != receipt.receipt_hash
        ):
            raise ValueError("Candidate Event Observation coverage identity is invalid")
        source = self.coverage_registration.source(observation.source.provider_id)
        if observation.source.source_tier is not source.source_tier:
            raise ValueError("Candidate Event Observation source tier is not registered")
        attempt = receipt.attempt(source.provider_id)
        if (
            not attempt.succeeded
            or attempt.retrieved_at != observation.source.retrieved_at
            or attempt.content_hash != observation.source.raw_content_hash
        ):
            raise ValueError("Candidate Event Observation is not bound to its source attempt")

    def decisions(self) -> tuple[AccrualDecision, ...]:
        with self._connect() as connection:
            metadata = connection.execute(
                "SELECT * FROM ledger_metadata WHERE singleton = 1"
            ).fetchone()
            if metadata is None:
                raise ValueError("Accrual Ledger metadata is missing")
            self._validate_metadata(metadata)
            return self._decisions(connection)

    def _decisions(self, connection: sqlite3.Connection) -> tuple[AccrualDecision, ...]:
        rows = connection.execute("SELECT * FROM accrual_decisions ORDER BY sequence").fetchall()
        decisions = tuple(self._verified_decision(row) for row in rows)
        previous_hash: str | None = None
        for expected_sequence, decision in enumerate(decisions, start=1):
            self._verify_source_artifacts(decision.observation)
            if decision.sequence != expected_sequence:
                raise ValueError("Accrual Ledger sequence is not contiguous")
            if decision.previous_hash != previous_hash:
                raise ValueError("Accrual Ledger hash chain is invalid")
            previous_hash = decision.decision_hash
        _validate_recorded_history(
            self.registration,
            self.coverage_registration,
            decisions,
        )
        return decisions

    def _verified_decision(self, row: sqlite3.Row) -> AccrualDecision:
        observation_json = cast(str, row["observation_json"])
        try:
            decoded: object = json.loads(observation_json)
        except json.JSONDecodeError as exc:
            raise ValueError("Accrual Ledger observation_json is invalid") from exc
        observation = candidate_event_observation_from_dict(decoded)
        if observation_json != canonical_json_bytes(observation.to_dict()).decode():
            raise ValueError("Accrual Ledger observation_json is not canonical")
        stored_observation_hash = cast(str, row["observation_hash"])
        if stored_observation_hash != observation.observation_hash:
            raise ValueError("Accrual Ledger observation_hash is invalid")
        if cast(str, row["observation_id"]) != observation.observation_id:
            raise ValueError("Accrual Ledger observation_id is invalid")
        if cast(str, row["event_id"]) != observation.event_id:
            raise ValueError("Accrual Ledger event_id is invalid")
        receipt_json = cast(str, row["coverage_receipt_json"])
        try:
            receipt_payload: object = json.loads(receipt_json)
        except json.JSONDecodeError as exc:
            raise ValueError("Accrual Ledger coverage_receipt_json is invalid") from exc
        coverage_receipt = coverage_receipt_from_dict(receipt_payload)
        if receipt_json != canonical_json_bytes(coverage_receipt.to_dict()).decode():
            raise ValueError("Accrual Ledger coverage_receipt_json is not canonical")
        if cast(str, row["coverage_receipt_hash"]) != coverage_receipt.receipt_hash:
            raise ValueError("Accrual Ledger coverage_receipt_hash is invalid")
        self._validate_coverage(observation, coverage_receipt)
        reasons_json = cast(str, row["reasons_json"])
        try:
            reasons_value: object = json.loads(reasons_json)
        except json.JSONDecodeError as exc:
            raise ValueError("Accrual Ledger reasons_json is invalid") from exc
        if not isinstance(reasons_value, list):
            raise TypeError("Accrual Ledger reasons_json must be an array")
        reason_strings = cast(list[object], reasons_value)
        if any(not isinstance(item, str) for item in reason_strings):
            raise TypeError("Accrual Ledger reasons must be strings")
        reasons = tuple(AccrualReason(cast(str, item)) for item in reason_strings)
        if reasons_json != canonical_json_bytes([item.value for item in reasons]).decode():
            raise ValueError("Accrual Ledger reasons_json is not canonical")
        decision = AccrualDecision(
            sequence=cast(int, row["sequence"]),
            registration_id=self.registration.registration_id,
            observation=observation,
            coverage_receipt=coverage_receipt,
            disposition=AccrualDisposition(cast(str, row["disposition"])),
            reasons=reasons,
            qualifying_visible_at=_optional_parse_timestamp(
                cast(str | None, row["qualifying_visible_at"])
            ),
            evidence_cutoff_at=_optional_parse_timestamp(
                cast(str | None, row["evidence_cutoff_at"])
            ),
            accrued_event_id=cast(str | None, row["accrued_event_id"]),
            recorded_at=_parse_timestamp(cast(str, row["recorded_at"])),
            previous_hash=cast(str | None, row["previous_hash"]),
            decision_hash=cast(str, row["decision_hash"]),
        )
        if decision.decision_hash != canonical_hash(decision.core_dict()):
            raise ValueError("Accrual Ledger decision_hash is invalid")
        return decision

    @property
    def ledger_hash(self) -> str:
        decisions = self.decisions()
        return self.registration.registration_hash if not decisions else decisions[-1].decision_hash

    @property
    def accrued_event_count(self) -> int:
        return sum(item.disposition is AccrualDisposition.ACCRUED for item in self.decisions())


def candidate_event_observation_from_dict(value: object) -> CandidateEventObservation:
    payload = _object(value, "Candidate Event Observation")
    _closed(
        payload,
        {
            "schema_version",
            "observation_id",
            "event_id",
            "source_coverage_registration_id",
            "source_coverage_registration_hash",
            "coverage_receipt_id",
            "coverage_receipt_hash",
            "event_nature",
            "affected_commodity",
            "loss_amount",
            "loss_unit",
            "regional_denominator_source_ref",
            "regional_denominator_source_tier",
            "regional_denominator_available_at",
            "regional_denominator_raw_content_hash",
            "expected_duration_hours",
            "source",
            "supersedes_observation_id",
        },
        "Candidate Event Observation",
    )
    if _string(payload, "schema_version") != CANDIDATE_EVENT_OBSERVATION_SCHEMA:
        raise ValueError("unsupported Candidate Event Observation schema_version")
    source_raw = _object(payload.get("source"), "Candidate Event Observation source")
    _closed(
        source_raw,
        {
            "provider_id",
            "upstream_source",
            "upstream_record_id",
            "source_ref",
            "source_tier",
            "occurred_at",
            "published_at",
            "source_updated_at",
            "available_at",
            "retrieved_at",
            "availability_basis",
            "raw_content_hash",
            "claim_summary",
            "claim_hash",
        },
        "Candidate Event Observation source",
    )
    return CandidateEventObservation(
        observation_id=_string(payload, "observation_id"),
        event_id=_string(payload, "event_id"),
        source_coverage_registration_id=_string(
            payload,
            "source_coverage_registration_id",
        ),
        source_coverage_registration_hash=_string(
            payload,
            "source_coverage_registration_hash",
        ),
        coverage_receipt_id=_string(payload, "coverage_receipt_id"),
        coverage_receipt_hash=_string(payload, "coverage_receipt_hash"),
        event_nature=EventNature(_string(payload, "event_nature")),
        affected_commodity=_nullable_string(payload, "affected_commodity"),
        loss_amount=_nullable_decimal(payload, "loss_amount"),
        loss_unit=_nullable_loss_unit(payload, "loss_unit"),
        regional_denominator_source_ref=_nullable_string(
            payload,
            "regional_denominator_source_ref",
        ),
        regional_denominator_source_tier=_nullable_evidence_tier(
            payload,
            "regional_denominator_source_tier",
        ),
        regional_denominator_available_at=_nullable_datetime(
            payload,
            "regional_denominator_available_at",
        ),
        regional_denominator_raw_content_hash=_nullable_string(
            payload,
            "regional_denominator_raw_content_hash",
        ),
        expected_duration_hours=_nullable_decimal(payload, "expected_duration_hours"),
        source=OccurrenceSourceObservation(
            provider_id=_string(source_raw, "provider_id"),
            upstream_source=_string(source_raw, "upstream_source"),
            upstream_record_id=_string(source_raw, "upstream_record_id"),
            source_ref=_string(source_raw, "source_ref"),
            source_tier=EvidenceTier(_string(source_raw, "source_tier")),
            occurred_at=_nullable_datetime(source_raw, "occurred_at"),
            published_at=_datetime(source_raw, "published_at"),
            source_updated_at=_nullable_datetime(source_raw, "source_updated_at"),
            available_at=_datetime(source_raw, "available_at"),
            retrieved_at=_datetime(source_raw, "retrieved_at"),
            availability_basis=AvailabilityBasis(_string(source_raw, "availability_basis")),
            raw_content_hash=_string(source_raw, "raw_content_hash"),
            claim_summary=_string(source_raw, "claim_summary"),
            claim_hash=_string(source_raw, "claim_hash"),
        ),
        supersedes_observation_id=_nullable_string(
            payload,
            "supersedes_observation_id",
        ),
    )


def _evaluate_observation(
    registration: AgentPhase2Preregistration,
    coverage_registration: SourceCoverageRegistration,
    observation: CandidateEventObservation,
    coverage_receipt: CoverageReceipt,
    decisions: tuple[AccrualDecision, ...],
) -> tuple[AccrualDisposition, tuple[AccrualReason, ...]]:
    reasons: set[AccrualReason] = set()
    available_at = observation.source.available_at
    accrued = tuple(item for item in decisions if item.disposition is AccrualDisposition.ACCRUED)
    if not coverage_receipt.is_complete(coverage_registration) or any(
        item.observation.event_id == observation.event_id
        and item.reasons == (AccrualReason.SOURCE_COVERAGE_INCOMPLETE,)
        for item in decisions
    ):
        reasons.add(AccrualReason.SOURCE_COVERAGE_INCOMPLETE)
    if not (registration.accrual.opens_after < available_at <= registration.accrual.closes_at):
        reasons.add(AccrualReason.OUTSIDE_ACCRUAL_WINDOW)
    if len(accrued) >= registration.accrual.target_event_count:
        reasons.add(AccrualReason.COHORT_FULL)
    if any(item.observation.event_id == observation.event_id for item in accrued):
        reasons.add(AccrualReason.ALREADY_ACCRUED)
    if (
        observation.source.occurred_at is None
        or observation.affected_commodity is None
        or observation.loss_amount is None
        or observation.loss_unit is None
        or observation.expected_duration_hours is None
    ):
        reasons.add(AccrualReason.MISSING_CRITICAL_DATA)
    if (
        observation.affected_commodity is not None
        and observation.affected_commodity not in PHYSICAL_ENERGY_COMMODITIES
    ):
        reasons.add(AccrualReason.UNSUPPORTED_COMMODITY)
    if observation.source.source_tier not in (
        registration.event_eligibility.accepted_occurrence_source_tiers
    ):
        reasons.add(AccrualReason.SOURCE_TIER_NOT_QUALIFYING)
    if not coverage_registration.source(observation.source.provider_id).occurrence_eligible:
        reasons.add(AccrualReason.SOURCE_TIER_NOT_QUALIFYING)
    if not observation.event_nature.qualifying:
        reasons.add(AccrualReason.EVENT_NATURE_EXCLUDED)
    if observation.loss_unit is LossUnit.BOE_PER_DAY and observation.loss_amount is not None:
        if observation.loss_amount < Decimal("500000"):
            reasons.add(AccrualReason.LOSS_THRESHOLD_NOT_MET)
    elif (
        observation.loss_unit is LossUnit.REGIONAL_SUPPLY_FRACTION
        and observation.loss_amount is not None
        and observation.loss_amount < Decimal("0.05")
    ):
        reasons.add(AccrualReason.LOSS_THRESHOLD_NOT_MET)
    if (
        observation.expected_duration_hours is not None
        and observation.expected_duration_hours < Decimal("24")
    ):
        reasons.add(AccrualReason.DURATION_THRESHOLD_NOT_MET)
    if accrued and observation.source.occurred_at is not None:
        latest_occurred_at = accrued[-1].observation.source.occurred_at
        assert latest_occurred_at is not None
        minimum_next = latest_occurred_at + timedelta(
            days=registration.accrual.minimum_separation_days
        )
        if observation.source.occurred_at < minimum_next:
            reasons.add(AccrualReason.SEPARATION_WINDOW_NOT_MET)
    if reasons:
        return (
            AccrualDisposition.NOT_ACCRUED,
            tuple(sorted(reasons, key=lambda item: item.value)),
        )
    return AccrualDisposition.ACCRUED, ()


def _validate_recorded_history(
    registration: AgentPhase2Preregistration,
    coverage_registration: SourceCoverageRegistration,
    decisions: tuple[AccrualDecision, ...],
) -> None:
    prior_available_at: datetime | None = None
    prior_recorded_at: datetime | None = None
    prior_decision_hash: str | None = None
    replayed: list[AccrualDecision] = []
    latest_by_event: dict[str, CandidateEventObservation] = {}
    for decision in decisions:
        observation = decision.observation
        if prior_available_at is not None and observation.source.available_at < prior_available_at:
            raise ValueError("Accrual Ledger receipt order is invalid")
        if decision.recorded_at < observation.source.retrieved_at:
            raise ValueError("Accrual Ledger decision predates source retrieval")
        if prior_recorded_at is not None and decision.recorded_at < prior_recorded_at:
            raise ValueError("Accrual Ledger recorded_at order is invalid")
        if decision.previous_hash != prior_decision_hash:
            raise ValueError("Accrual Ledger previous_hash is invalid")
        previous = latest_by_event.get(observation.event_id)
        if previous is None:
            if observation.supersedes_observation_id is not None:
                raise ValueError("Accrual Ledger revision predecessor is missing")
        elif observation.supersedes_observation_id != previous.observation_id:
            raise ValueError("Accrual Ledger revision lineage is invalid")
        elif (
            previous.source.occurred_at is not None
            and observation.source.occurred_at != previous.source.occurred_at
        ) or (
            previous.affected_commodity is not None
            and observation.affected_commodity != previous.affected_commodity
        ):
            raise ValueError("Accrual Ledger revision changed stable event identity")
        expected_disposition, expected_reasons = _evaluate_observation(
            registration,
            coverage_registration,
            observation,
            decision.coverage_receipt,
            tuple(replayed),
        )
        if decision.disposition is not expected_disposition or decision.reasons != expected_reasons:
            raise ValueError("Accrual Ledger decision does not match frozen registration")
        if expected_disposition is AccrualDisposition.ACCRUED:
            expected_visible_at = observation.source.available_at
            expected_cutoff_at = expected_visible_at + timedelta(
                minutes=registration.agent_protocol.assessment_delay_minutes
            )
            expected_accrued_event_id = "accrued-event-" + canonical_hash(
                {
                    "registration_id": registration.registration_id,
                    "event_id": observation.event_id,
                    "observation_hash": observation.observation_hash,
                }
            )
            if (
                decision.qualifying_visible_at != expected_visible_at
                or decision.evidence_cutoff_at != expected_cutoff_at
                or decision.accrued_event_id != expected_accrued_event_id
            ):
                raise ValueError("Accrual Ledger admission identity is invalid")
        prior_available_at = observation.source.available_at
        prior_recorded_at = decision.recorded_at
        prior_decision_hash = decision.decision_hash
        latest_by_event[observation.event_id] = observation
        replayed.append(decision)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object with string keys")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} must be an object with string keys")
    return cast(dict[str, object], value)


def _closed(value: dict[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        raise ValueError(f"{name} fields do not match contract: missing={missing}, extra={extra}")


def _string(value: dict[str, object], name: str) -> str:
    raw = value.get(name)
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return raw


def _nullable_string(value: dict[str, object], name: str) -> str | None:
    raw = value.get(name)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise TypeError(f"{name} must be null or a non-empty trimmed string")
    return raw


def _nullable_evidence_tier(
    value: dict[str, object],
    name: str,
) -> EvidenceTier | None:
    raw = _nullable_string(value, name)
    return None if raw is None else EvidenceTier(raw)


def _decimal(value: dict[str, object], name: str) -> Decimal:
    raw = _string(value, name)
    try:
        parsed = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    return parsed


def _nullable_decimal(value: dict[str, object], name: str) -> Decimal | None:
    if value.get(name) is None:
        return None
    return _decimal(value, name)


def _nullable_loss_unit(value: dict[str, object], name: str) -> LossUnit | None:
    raw = _nullable_string(value, name)
    return None if raw is None else LossUnit(raw)


def _datetime(value: dict[str, object], name: str) -> datetime:
    parsed = datetime.fromisoformat(_string(value, name).replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _nullable_datetime(value: dict[str, object], name: str) -> datetime | None:
    raw = value.get(name)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise TypeError(f"{name} must be null or a non-empty timestamp")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "timestamp")
    return parsed.astimezone(UTC)


def _optional_parse_timestamp(value: str | None) -> datetime | None:
    return None if value is None else _parse_timestamp(value)


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} contains unsupported characters")


def _sha256(value: str, name: str) -> None:
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a SHA-256 hex digest")


def _retain_exact_artifact(
    store: ArtifactStore,
    payload: bytes,
    expected_hash: str,
    name: str,
) -> None:
    actual_hash = sha256(payload).hexdigest()
    if actual_hash != expected_hash:
        raise ValueError(f"{name} bytes do not match Candidate Event Observation hash")
    store.put_bytes(payload, media_type="application/octet-stream")
