from __future__ import annotations

import json
import math
import os
import re
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.domain import require_aware
from market_impact_agent.providers import ProviderTransport
from market_impact_agent.research import EvidenceItem, EvidenceTier

OBSERVATION_PROVIDER_MANIFEST_SCHEMA = "market-impact.observation-provider-manifest.v1"
PREDICTION_MARKET_BATCH_SCHEMA = "market-impact.prediction-market-batch.v1"


class ObservationCapability(StrEnum):
    PREDICTION_MARKET_DISCOVERY = "prediction_market_discovery"
    PREDICTION_MARKET_SNAPSHOT = "prediction_market_snapshot"
    PREDICTION_MARKET_HISTORY = "prediction_market_history"


class ObservationTrustTier(StrEnum):
    UNVERIFIED = "unverified"
    CONTRACT_VALIDATED = "contract_validated"
    SOURCE_VALIDATED = "source_validated"


class OccurrenceBasis(StrEnum):
    SOURCE_REPORTED = "source_reported"
    AGGREGATOR_SNAPSHOT = "aggregator_snapshot"
    RETRIEVAL_OBSERVED = "retrieval_observed"
    RETROSPECTIVE_SERIES = "retrospective_series"


class AvailabilityBasis(StrEnum):
    ACTUAL_RECEIPT = "actual_receipt"
    SOURCE_REPORTED = "source_reported"
    MODELED_LATENCY = "modeled_latency"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class LatencyModelReference:
    source_class: str
    model_id: str
    model_version: str
    calibration_ref: str

    def __post_init__(self) -> None:
        for name in ("source_class", "model_id", "model_version", "calibration_ref"):
            value = getattr(self, name)
            if not value or value != value.strip():
                raise ValueError(f"latency model {name} must be a non-empty stable reference")
        if self.model_version.casefold() in {"current", "head", "latest", "main", "master"}:
            raise ValueError("latency model model_version must identify an immutable version")

    def to_dict(self) -> dict[str, str]:
        return {
            "source_class": self.source_class,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "calibration_ref": self.calibration_ref,
        }


@dataclass(frozen=True, slots=True)
class ObservationProviderManifest:
    schema_version: str
    provider_id: str
    provider_version: str
    transport: ProviderTransport
    declared_capabilities: frozenset[ObservationCapability]
    verified_capabilities: frozenset[ObservationCapability]
    upstream_sources: tuple[str, ...]
    auth_required: bool
    provides_source_updated_at: bool
    provides_aggregator_fetched_at: bool
    provides_historical_occurrence_at: bool
    provides_revision_history: bool
    enabled: bool
    trust_tier: ObservationTrustTier
    license_note: str

    def validation_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if self.schema_version != OBSERVATION_PROVIDER_MANIFEST_SCHEMA:
            errors.append("unsupported observation provider manifest schema_version")
        if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", self.provider_id) is None:
            errors.append(
                "provider_id must use lowercase letters, digits, dot, dash, or underscore"
            )
        if not self.provider_version:
            errors.append("provider_version is required")
        if not self.declared_capabilities:
            errors.append("declared_capabilities must not be empty")
        if not self.verified_capabilities <= self.declared_capabilities:
            errors.append("verified_capabilities must be a subset of declared_capabilities")
        if not self.upstream_sources:
            errors.append("upstream_sources must not be empty")
        if any(not source for source in self.upstream_sources):
            errors.append("upstream_sources items must not be empty")
        if len(self.upstream_sources) != len(set(self.upstream_sources)):
            errors.append("upstream_sources must be unique")
        if self.enabled and not self.verified_capabilities:
            errors.append("enabled observation providers require a verified capability")
        if self.enabled and self.trust_tier is ObservationTrustTier.UNVERIFIED:
            errors.append("enabled observation providers require a validated trust tier")
        if not self.license_note:
            errors.append("license_note is required")
        return tuple(errors)

    def assert_valid(self) -> None:
        errors = self.validation_errors()
        if errors:
            raise ValueError("; ".join(errors))

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "transport": self.transport.value,
            "declared_capabilities": sorted(item.value for item in self.declared_capabilities),
            "verified_capabilities": sorted(item.value for item in self.verified_capabilities),
            "upstream_sources": list(self.upstream_sources),
            "auth_required": self.auth_required,
            "provides_source_updated_at": self.provides_source_updated_at,
            "provides_aggregator_fetched_at": self.provides_aggregator_fetched_at,
            "provides_historical_occurrence_at": self.provides_historical_occurrence_at,
            "provides_revision_history": self.provides_revision_history,
            "enabled": self.enabled,
            "trust_tier": self.trust_tier.value,
            "license_note": self.license_note,
        }


@dataclass(frozen=True, slots=True)
class ObservationTimes:
    occurred_at: datetime
    published_at: datetime | None
    available_at: datetime | None
    source_updated_at: datetime | None
    aggregator_fetched_at: datetime | None
    retrieved_at: datetime
    occurrence_basis: OccurrenceBasis
    availability_basis: AvailabilityBasis
    latency_model: LatencyModelReference | None = None

    def __post_init__(self) -> None:
        require_aware(self.occurred_at, "occurred_at")
        require_aware(self.retrieved_at, "retrieved_at")
        if self.occurred_at > self.retrieved_at:
            raise ValueError("occurred_at must not be after retrieved_at")
        for name in (
            "published_at",
            "available_at",
            "source_updated_at",
            "aggregator_fetched_at",
        ):
            value = getattr(self, name)
            if value is None:
                continue
            require_aware(value, name)
            if value > self.retrieved_at:
                raise ValueError(f"{name} must not be after retrieved_at")
        if (
            self.published_at is not None
            and self.available_at is not None
            and self.published_at > self.available_at
        ):
            raise ValueError("published_at must not be after available_at")
        if self.occurrence_basis is OccurrenceBasis.SOURCE_REPORTED:
            if self.source_updated_at is None:
                raise ValueError("source-reported occurrence requires source_updated_at")
            if self.occurred_at != self.source_updated_at:
                raise ValueError("source-reported occurred_at must equal source_updated_at")
        if self.occurrence_basis is OccurrenceBasis.AGGREGATOR_SNAPSHOT:
            if self.aggregator_fetched_at is None:
                raise ValueError("aggregator occurrence requires aggregator_fetched_at")
            if self.occurred_at != self.aggregator_fetched_at:
                raise ValueError("aggregator occurred_at must equal aggregator_fetched_at")
        if (
            self.occurrence_basis is OccurrenceBasis.RETRIEVAL_OBSERVED
            and self.occurred_at != self.retrieved_at
        ):
            raise ValueError("retrieval-observed occurred_at must equal retrieved_at")
        if (
            self.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
            and self.available_at != self.retrieved_at
        ):
            raise ValueError("actual-receipt available_at must equal retrieved_at")
        if self.availability_basis is AvailabilityBasis.UNKNOWN:
            if self.available_at is not None:
                raise ValueError("unknown availability requires available_at to be null")
        elif self.available_at is None:
            raise ValueError("known availability requires available_at")
        if self.availability_basis is AvailabilityBasis.MODELED_LATENCY:
            if self.latency_model is None:
                raise ValueError("modeled latency requires a versioned latency model reference")
        elif self.latency_model is not None:
            raise ValueError("latency_model is only valid for modeled latency")

    @property
    def evidence_ready(self) -> bool:
        return not self.evidence_promotion_errors()

    def evidence_promotion_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if self.published_at is None:
            errors.append("observation lacks a source publication time")
        if self.available_at is None:
            errors.append("observation lacks a point-in-time availability time")
        if (
            self.availability_basis is AvailabilityBasis.MODELED_LATENCY
            and self.latency_model is None
        ):
            errors.append("modeled availability lacks a versioned latency model reference")
        return tuple(errors)

    def assert_evidence_ready(self) -> None:
        errors = self.evidence_promotion_errors()
        if errors:
            raise ValueError("; ".join(errors))

    def to_dict(self) -> dict[str, object]:
        return {
            "occurred_at": _timestamp(self.occurred_at),
            "published_at": _optional_timestamp(self.published_at),
            "available_at": _optional_timestamp(self.available_at),
            "source_updated_at": _optional_timestamp(self.source_updated_at),
            "aggregator_fetched_at": _optional_timestamp(self.aggregator_fetched_at),
            "retrieved_at": _timestamp(self.retrieved_at),
            "occurrence_basis": self.occurrence_basis.value,
            "availability_basis": self.availability_basis.value,
            "latency_model": (None if self.latency_model is None else self.latency_model.to_dict()),
            "evidence_ready": self.evidence_ready,
        }


@dataclass(frozen=True, slots=True)
class PredictionMarketObservation:
    observation_id: str
    provider_id: str
    upstream_source: str
    source_tier: EvidenceTier
    market_id: str
    event_id: str | None
    title: str
    outcome: str
    probability: Decimal
    source_ref: str
    source_url: str | None
    token_id: str | None
    status: str | None
    rules: str | None
    resolution_source: str | None
    opened_at: datetime | None
    closes_at: datetime | None
    resolved_at: datetime | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    last_price: Decimal | None
    volume: Decimal | None
    open_interest: Decimal | None
    liquidity: Decimal | None
    times: ObservationTimes
    raw_content_hash: str

    def __post_init__(self) -> None:
        for name in (
            "observation_id",
            "provider_id",
            "upstream_source",
            "market_id",
            "title",
            "outcome",
            "source_ref",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        _probability(self.probability, "probability")
        for name in ("best_bid", "best_ask", "last_price"):
            value = getattr(self, name)
            if value is not None:
                _probability(value, name)
        for name in ("volume", "open_interest", "liquidity"):
            value = getattr(self, name)
            if value is not None and (not value.is_finite() or value < 0):
                raise ValueError(f"{name} must be finite and non-negative")
        for name in ("opened_at", "closes_at", "resolved_at"):
            value = getattr(self, name)
            if value is not None:
                require_aware(value, name)
        if re.fullmatch(r"[0-9a-f]{64}", self.raw_content_hash) is None:
            raise ValueError("raw_content_hash must be a sha256 hex digest")

    @property
    def claim_id(self) -> str:
        if self.provider_id == "world-monitor-predictions" and self.upstream_source == "polymarket":
            title_hash = sha256(self.title.strip().encode()).hexdigest()[:24]
            return (
                "prediction-market-discovery:polymarket:"
                f"event-{self.market_id}-question-{title_hash}:{self.outcome.lower()}"
            )
        return f"prediction-market:{self.upstream_source}:{self.market_id}:{self.outcome.lower()}"

    def to_evidence_item(self, *, supersedes_id: str | None = None) -> EvidenceItem:
        self.times.assert_evidence_ready()
        if self.times.published_at is None or self.times.available_at is None:
            raise AssertionError("validated evidence times unexpectedly missing")
        claim = (
            f"{self.upstream_source} market {self.market_id} priced "
            f"{self.outcome} at {self.probability}"
        )
        return EvidenceItem(
            evidence_id=self.observation_id,
            claim_id=self.claim_id,
            source_ref=self.source_ref,
            source_tier=self.source_tier,
            occurred_at=self.times.occurred_at,
            published_at=self.times.published_at,
            visible_at=self.times.available_at,
            retrieved_at=self.times.retrieved_at,
            claim=claim,
            claim_hash=sha256(claim.encode()).hexdigest(),
            supersedes_id=supersedes_id,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "observation_id": self.observation_id,
            "claim_id": self.claim_id,
            "provider_id": self.provider_id,
            "upstream_source": self.upstream_source,
            "source_tier": self.source_tier.value,
            "market_id": self.market_id,
            "event_id": self.event_id,
            "title": self.title,
            "outcome": self.outcome,
            "probability": str(self.probability),
            "source_ref": self.source_ref,
            "source_url": self.source_url,
            "token_id": self.token_id,
            "status": self.status,
            "rules": self.rules,
            "resolution_source": self.resolution_source,
            "opened_at": _optional_timestamp(self.opened_at),
            "closes_at": _optional_timestamp(self.closes_at),
            "resolved_at": _optional_timestamp(self.resolved_at),
            "best_bid": _optional_decimal(self.best_bid),
            "best_ask": _optional_decimal(self.best_ask),
            "last_price": _optional_decimal(self.last_price),
            "volume": _optional_decimal(self.volume),
            "open_interest": _optional_decimal(self.open_interest),
            "liquidity": _optional_decimal(self.liquidity),
            "times": self.times.to_dict(),
            "raw_content_hash": self.raw_content_hash,
        }


@dataclass(frozen=True, slots=True)
class PredictionMarketBatch:
    batch_id: str
    bundle_hash: str
    provider_manifest: ObservationProviderManifest
    retrieved_at: datetime
    query: tuple[tuple[str, str], ...]
    data_available: bool
    degraded_reasons: tuple[str, ...]
    observations: tuple[PredictionMarketObservation, ...]
    raw_payload: object

    def __post_init__(self) -> None:
        self.provider_manifest.assert_valid()
        require_aware(self.retrieved_at, "retrieved_at")
        if len(self.query) != len({key for key, _ in self.query}):
            raise ValueError("query keys must be unique")
        if not self.data_available and self.observations:
            raise ValueError("unavailable batches must not contain observations")
        if not self.data_available and not self.degraded_reasons:
            raise ValueError("unavailable batches require a degraded reason")
        if any(not reason for reason in self.degraded_reasons):
            raise ValueError("degraded_reasons items must not be empty")
        if len(self.degraded_reasons) != len(set(self.degraded_reasons)):
            raise ValueError("degraded_reasons must be unique")
        if len({item.observation_id for item in self.observations}) != len(self.observations):
            raise ValueError("observation_id values must be unique")
        if len({item.claim_id for item in self.observations}) != len(self.observations):
            raise ValueError("claim_id values must be unique")
        if any(
            item.provider_id != self.provider_manifest.provider_id for item in self.observations
        ):
            raise ValueError("observation provider_id must match the batch manifest")
        if any(
            item.upstream_source not in self.provider_manifest.upstream_sources
            for item in self.observations
        ):
            raise ValueError("observation upstream_source must be declared by the manifest")
        if any(item.times.retrieved_at != self.retrieved_at for item in self.observations):
            raise ValueError("observation retrieved_at must match the batch retrieved_at")
        if re.fullmatch(r"[0-9a-f]{64}", self.bundle_hash) is None:
            raise ValueError("bundle_hash must be a sha256 hex digest")
        if self.batch_id != _batch_id(self.provider_manifest.provider_id, self.bundle_hash):
            raise ValueError("batch_id must match provider_id and bundle_hash")

    @property
    def evidence_ready_count(self) -> int:
        return sum(item.times.evidence_ready for item in self.observations)

    def to_dict(self) -> dict[str, object]:
        return {
            **self.core_dict(),
            "bundle_hash": self.bundle_hash,
            "batch_id": self.batch_id,
        }

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": PREDICTION_MARKET_BATCH_SCHEMA,
            "provider_manifest": self.provider_manifest.to_dict(),
            "retrieved_at": _timestamp(self.retrieved_at),
            "query": {key: value for key, value in self.query},
            "data_available": self.data_available,
            "degraded_reasons": list(self.degraded_reasons),
            "observations": [item.to_dict() for item in self.observations],
            "raw_payload": self.raw_payload,
        }


@dataclass(frozen=True, slots=True)
class ValidatedObservationBundle:
    path: Path
    batch_id: str
    bundle_hash: str
    provider_id: str
    observation_count: int
    evidence_ready_count: int
    data_available: bool


def build_prediction_market_batch(
    *,
    provider_manifest: ObservationProviderManifest,
    retrieved_at: datetime,
    query: Mapping[str, object],
    data_available: bool,
    degraded_reasons: tuple[str, ...],
    observations: tuple[PredictionMarketObservation, ...],
    raw_payload: object,
) -> PredictionMarketBatch:
    normalized_query = tuple(sorted((str(key), str(value)) for key, value in query.items()))
    core = {
        "schema_version": PREDICTION_MARKET_BATCH_SCHEMA,
        "provider_manifest": provider_manifest.to_dict(),
        "retrieved_at": _timestamp(retrieved_at),
        "query": {key: value for key, value in normalized_query},
        "data_available": data_available,
        "degraded_reasons": list(degraded_reasons),
        "observations": [item.to_dict() for item in observations],
        "raw_payload": raw_payload,
    }
    bundle_hash = sha256(_canonical_json_bytes(core)).hexdigest()
    return PredictionMarketBatch(
        batch_id=_batch_id(provider_manifest.provider_id, bundle_hash),
        bundle_hash=bundle_hash,
        provider_manifest=provider_manifest,
        retrieved_at=retrieved_at,
        query=normalized_query,
        data_available=data_available,
        degraded_reasons=degraded_reasons,
        observations=observations,
        raw_payload=raw_payload,
    )


def write_prediction_market_batch(
    batch: PredictionMarketBatch,
    output_root: Path,
) -> ValidatedObservationBundle:
    output_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = output_root / f"{batch.batch_id}.json"
    payload = _pretty_json_bytes(batch.to_dict())
    if destination.exists():
        existing = validate_prediction_market_batch(destination)
        if existing.bundle_hash != batch.bundle_hash:
            raise FileExistsError(f"conflicting observation bundle exists: {destination}")
        return existing
    descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-observation-", dir=output_root)
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary_path.replace(destination)
    except Exception:
        with suppress(OSError):
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)
        raise
    return validate_prediction_market_batch(destination)


def validate_prediction_market_batch(path: Path) -> ValidatedObservationBundle:
    if path.is_symlink() or not path.is_file():
        raise ValueError("observation bundle must be a regular file")
    raw = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_unique_json_object,
    )
    if not isinstance(raw, dict):
        raise TypeError("observation bundle must be a JSON object")
    payload = cast(dict[str, object], raw)
    if payload.get("schema_version") != PREDICTION_MARKET_BATCH_SCHEMA:
        raise ValueError("unsupported observation bundle schema_version")
    batch_id = _required_string(payload, "batch_id")
    bundle_hash = _required_string(payload, "bundle_hash")
    core = {key: value for key, value in payload.items() if key not in {"batch_id", "bundle_hash"}}
    actual_hash = sha256(_canonical_json_bytes(core)).hexdigest()
    if bundle_hash != actual_hash:
        raise ValueError("observation bundle_hash does not match content")
    manifest = _provider_manifest_from_dict(payload.get("provider_manifest"))
    provider_id = manifest.provider_id
    if batch_id != _batch_id(provider_id, bundle_hash):
        raise ValueError("observation batch_id does not match content")
    retrieved_at = _required_timestamp(payload.get("retrieved_at"), "retrieved_at")
    query_payload = _string_mapping(payload.get("query"), "query")
    query = tuple(sorted(query_payload.items()))
    data_available = _required_boolean(payload.get("data_available"), "data_available")
    degraded_reasons = tuple(_string_array(payload.get("degraded_reasons"), "degraded_reasons"))
    observation_payloads = _object_array(payload.get("observations"), "observations")
    observations = tuple(
        _prediction_market_observation_from_dict(item) for item in observation_payloads
    )
    if "raw_payload" not in payload:
        raise ValueError("raw_payload is required")
    batch = PredictionMarketBatch(
        batch_id=batch_id,
        bundle_hash=bundle_hash,
        provider_manifest=manifest,
        retrieved_at=retrieved_at,
        query=query,
        data_available=data_available,
        degraded_reasons=degraded_reasons,
        observations=observations,
        raw_payload=payload["raw_payload"],
    )
    if batch.to_dict() != payload:
        raise ValueError("observation bundle does not match the canonical batch contract")
    return ValidatedObservationBundle(
        path=path,
        batch_id=batch_id,
        bundle_hash=bundle_hash,
        provider_id=provider_id,
        observation_count=len(observations),
        evidence_ready_count=batch.evidence_ready_count,
        data_available=data_available,
    )


def raw_content_hash(payload: object) -> str:
    return sha256(_canonical_json_bytes(payload)).hexdigest()


def observation_id(
    *,
    provider_id: str,
    upstream_source: str,
    market_id: str,
    outcome: str,
    probability: Decimal,
    times: ObservationTimes,
    raw_hash: str,
) -> str:
    payload = {
        "provider_id": provider_id,
        "upstream_source": upstream_source,
        "market_id": market_id,
        "outcome": outcome,
        "probability": str(probability),
        "times": times.to_dict(),
        "raw_content_hash": raw_hash,
    }
    return f"prediction-{sha256(_canonical_json_bytes(payload)).hexdigest()}"


def _batch_id(provider_id: str, bundle_hash: str) -> str:
    return f"{provider_id}-prediction-{bundle_hash[:24]}"


def _required_string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} must be a non-empty string")
    return value


def _provider_manifest_from_dict(value: object) -> ObservationProviderManifest:
    payload = _object(value, "provider_manifest")
    manifest = ObservationProviderManifest(
        schema_version=_required_string(payload, "schema_version"),
        provider_id=_required_string(payload, "provider_id"),
        provider_version=_required_string(payload, "provider_version"),
        transport=_enum_value(ProviderTransport, payload.get("transport"), "transport"),
        declared_capabilities=frozenset(
            _enum_value(ObservationCapability, item, "declared_capabilities item")
            for item in _string_array(payload.get("declared_capabilities"), "declared_capabilities")
        ),
        verified_capabilities=frozenset(
            _enum_value(ObservationCapability, item, "verified_capabilities item")
            for item in _string_array(payload.get("verified_capabilities"), "verified_capabilities")
        ),
        upstream_sources=tuple(_string_array(payload.get("upstream_sources"), "upstream_sources")),
        auth_required=_required_boolean(payload.get("auth_required"), "auth_required"),
        provides_source_updated_at=_required_boolean(
            payload.get("provides_source_updated_at"), "provides_source_updated_at"
        ),
        provides_aggregator_fetched_at=_required_boolean(
            payload.get("provides_aggregator_fetched_at"),
            "provides_aggregator_fetched_at",
        ),
        provides_historical_occurrence_at=_required_boolean(
            payload.get("provides_historical_occurrence_at"),
            "provides_historical_occurrence_at",
        ),
        provides_revision_history=_required_boolean(
            payload.get("provides_revision_history"), "provides_revision_history"
        ),
        enabled=_required_boolean(payload.get("enabled"), "enabled"),
        trust_tier=_enum_value(ObservationTrustTier, payload.get("trust_tier"), "trust_tier"),
        license_note=_required_string(payload, "license_note"),
    )
    manifest.assert_valid()
    return manifest


def _prediction_market_observation_from_dict(
    payload: Mapping[str, object],
) -> PredictionMarketObservation:
    times = _observation_times_from_dict(payload.get("times"))
    observation = PredictionMarketObservation(
        observation_id=_required_string(payload, "observation_id"),
        provider_id=_required_string(payload, "provider_id"),
        upstream_source=_required_string(payload, "upstream_source"),
        source_tier=_enum_value(EvidenceTier, payload.get("source_tier"), "source_tier"),
        market_id=_required_string(payload, "market_id"),
        event_id=_optional_string_value(payload.get("event_id"), "event_id"),
        title=_required_string(payload, "title"),
        outcome=_required_string(payload, "outcome"),
        probability=_required_decimal(payload.get("probability"), "probability"),
        source_ref=_required_string(payload, "source_ref"),
        source_url=_optional_string_value(payload.get("source_url"), "source_url"),
        token_id=_optional_string_value(payload.get("token_id"), "token_id"),
        status=_optional_string_value(payload.get("status"), "status"),
        rules=_optional_string_value(payload.get("rules"), "rules"),
        resolution_source=_optional_string_value(
            payload.get("resolution_source"), "resolution_source"
        ),
        opened_at=_optional_timestamp_value(payload.get("opened_at"), "opened_at"),
        closes_at=_optional_timestamp_value(payload.get("closes_at"), "closes_at"),
        resolved_at=_optional_timestamp_value(payload.get("resolved_at"), "resolved_at"),
        best_bid=_optional_decimal_value(payload.get("best_bid"), "best_bid"),
        best_ask=_optional_decimal_value(payload.get("best_ask"), "best_ask"),
        last_price=_optional_decimal_value(payload.get("last_price"), "last_price"),
        volume=_optional_decimal_value(payload.get("volume"), "volume"),
        open_interest=_optional_decimal_value(payload.get("open_interest"), "open_interest"),
        liquidity=_optional_decimal_value(payload.get("liquidity"), "liquidity"),
        times=times,
        raw_content_hash=_required_string(payload, "raw_content_hash"),
    )
    if payload.get("claim_id") != observation.claim_id:
        raise ValueError("observation claim_id does not match canonical upstream identity")
    expected_observation_id = observation_id(
        provider_id=observation.provider_id,
        upstream_source=observation.upstream_source,
        market_id=observation.market_id,
        outcome=observation.outcome,
        probability=observation.probability,
        times=observation.times,
        raw_hash=observation.raw_content_hash,
    )
    if observation.observation_id != expected_observation_id:
        raise ValueError("observation_id does not match observation content")
    return observation


def _observation_times_from_dict(value: object) -> ObservationTimes:
    payload = _object(value, "observation times")
    latency_model_payload = payload.get("latency_model")
    latency_model = (
        None
        if latency_model_payload is None
        else _latency_model_reference_from_dict(latency_model_payload)
    )
    times = ObservationTimes(
        occurred_at=_required_timestamp(payload.get("occurred_at"), "occurred_at"),
        published_at=_optional_timestamp_value(payload.get("published_at"), "published_at"),
        available_at=_optional_timestamp_value(payload.get("available_at"), "available_at"),
        source_updated_at=_optional_timestamp_value(
            payload.get("source_updated_at"), "source_updated_at"
        ),
        aggregator_fetched_at=_optional_timestamp_value(
            payload.get("aggregator_fetched_at"), "aggregator_fetched_at"
        ),
        retrieved_at=_required_timestamp(payload.get("retrieved_at"), "retrieved_at"),
        occurrence_basis=_enum_value(
            OccurrenceBasis, payload.get("occurrence_basis"), "occurrence_basis"
        ),
        availability_basis=_enum_value(
            AvailabilityBasis, payload.get("availability_basis"), "availability_basis"
        ),
        latency_model=latency_model,
    )
    evidence_ready = _required_boolean(payload.get("evidence_ready"), "times.evidence_ready")
    if evidence_ready is not times.evidence_ready:
        raise ValueError("times.evidence_ready must match the derived time contract")
    return times


def _latency_model_reference_from_dict(value: object) -> LatencyModelReference:
    payload = _object(value, "latency_model")
    return LatencyModelReference(
        source_class=_required_string(payload, "source_class"),
        model_id=_required_string(payload, "model_id"),
        model_version=_required_string(payload, "model_version"),
        calibration_ref=_required_string(payload, "calibration_ref"),
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], raw)


def _object_array(value: object, name: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return [_object(item, f"{name} item") for item in cast(list[object], value)]


def _string_array(value: object, name: str) -> list[str]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} items must be strings")
    return cast(list[str], items)


def _string_mapping(value: object, name: str) -> dict[str, str]:
    payload = _object(value, name)
    if any(not isinstance(item, str) for item in payload.values()):
        raise TypeError(f"{name} values must be strings")
    return cast(dict[str, str], payload)


def _required_boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _required_decimal(value: object, name: str) -> Decimal:
    result = _optional_decimal_value(value, name)
    if result is None:
        raise TypeError(f"{name} must be present")
    return result


def _optional_decimal_value(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError(f"{name} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must be finite")
    return parsed


def _required_timestamp(value: object, name: str) -> datetime:
    result = _optional_timestamp_value(value, name)
    if result is None:
        raise TypeError(f"{name} must be present")
    return result


def _optional_timestamp_value(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an ISO date-time string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid ISO date-time") from exc
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _optional_string_value(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} must be null or a non-empty string")
    return value


def _enum_value[T: StrEnum](enum_type: type[T], value: object, name: str) -> T:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"{name} has an unsupported value") from exc


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"duplicate JSON object key: {key}")
        payload[key] = value
    return payload


def _canonical_json_bytes(value: object) -> bytes:
    normalized = _normalize_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode()


def _pretty_json_bytes(value: object) -> bytes:
    normalized = _normalize_json(value)
    return (
        json.dumps(
            normalized,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode()


def _normalize_json(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON floats must be finite")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("JSON decimals must be finite")
        return str(value)
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {
            str(key): _normalize_json(inner)
            for key, inner in sorted(mapping.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        items = cast(list[object] | tuple[object, ...], value)
        return [_normalize_json(item) for item in items]
    raise TypeError(f"value is not JSON-compatible: {type(value).__name__}")


def _probability(value: Decimal, name: str) -> None:
    if not value.is_finite() or not Decimal("0") <= value <= Decimal("1"):
        raise ValueError(f"{name} must be between zero and one")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
