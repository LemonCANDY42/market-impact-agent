from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.observations import (
    OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
    AvailabilityBasis,
    LatencyModelReference,
    ObservationCapability,
    ObservationProviderManifest,
    ObservationTimes,
    ObservationTrustTier,
    OccurrenceBasis,
    PredictionMarketObservation,
    build_prediction_market_batch,
    observation_id,
    raw_content_hash,
    validate_prediction_market_batch,
    write_prediction_market_batch,
)
from market_impact_agent.providers import ProviderTransport
from market_impact_agent.research import EvidenceTier

NOW = datetime(2026, 8, 26, 2, tzinfo=UTC)


def rehash_bundle_payload(payload: dict[str, object]) -> None:
    core = {key: value for key, value in payload.items() if key not in {"batch_id", "bundle_hash"}}
    bundle_hash = sha256(
        json.dumps(
            core,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()
    manifest_payload = payload["provider_manifest"]
    assert isinstance(manifest_payload, dict)
    typed_manifest = cast(dict[str, object], manifest_payload)
    provider_id = typed_manifest.get("provider_id")
    assert isinstance(provider_id, str)
    payload["bundle_hash"] = bundle_hash
    payload["batch_id"] = f"{provider_id}-prediction-{bundle_hash[:24]}"


def manifest() -> ObservationProviderManifest:
    return ObservationProviderManifest(
        schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
        provider_id="test-observations",
        provider_version="1.0.0",
        transport=ProviderTransport.HTTP,
        declared_capabilities=frozenset({ObservationCapability.PREDICTION_MARKET_SNAPSHOT}),
        verified_capabilities=frozenset(),
        upstream_sources=("test-market",),
        auth_required=False,
        provides_source_updated_at=True,
        provides_aggregator_fetched_at=False,
        provides_historical_occurrence_at=False,
        provides_revision_history=False,
        enabled=False,
        trust_tier=ObservationTrustTier.UNVERIFIED,
        license_note="test-only data",
    )


def observation(times: ObservationTimes) -> PredictionMarketObservation:
    raw_hash = sha256(b"raw market").hexdigest()
    probability = Decimal("0.42")
    return PredictionMarketObservation(
        observation_id=observation_id(
            provider_id="test-observations",
            upstream_source="test-market",
            market_id="market-1",
            outcome="Yes",
            probability=probability,
            times=times,
            raw_hash=raw_hash,
        ),
        provider_id="test-observations",
        upstream_source="test-market",
        source_tier=EvidenceTier.PRIMARY,
        market_id="market-1",
        event_id="event-1",
        title="Will the event occur?",
        outcome="Yes",
        probability=probability,
        source_ref="test-market://market/market-1",
        source_url="https://example.test/market-1",
        token_id="yes-token",
        status="active",
        rules="Official resolution rules",
        resolution_source="https://example.test/resolution",
        opened_at=NOW - timedelta(days=1),
        closes_at=NOW + timedelta(days=1),
        resolved_at=None,
        best_bid=Decimal("0.41"),
        best_ask=Decimal("0.43"),
        last_price=Decimal("0.42"),
        volume=Decimal("1000"),
        open_interest=Decimal("500"),
        liquidity=Decimal("250"),
        times=times,
        raw_content_hash=raw_hash,
    )


def test_retrieval_time_is_audit_only_for_modeled_historical_availability() -> None:
    published_at = NOW - timedelta(days=30)
    available_at = published_at + timedelta(seconds=12)
    retrieved_at = NOW
    times = ObservationTimes(
        occurred_at=published_at - timedelta(minutes=2),
        published_at=published_at,
        available_at=available_at,
        source_updated_at=published_at,
        aggregator_fetched_at=None,
        retrieved_at=retrieved_at,
        occurrence_basis=OccurrenceBasis.RETROSPECTIVE_SERIES,
        availability_basis=AvailabilityBasis.MODELED_LATENCY,
        latency_model=LatencyModelReference(
            source_class="public-web",
            model_id="fixed-publication-delay",
            model_version="v1",
            calibration_ref="research/calibration/public-web-v1.json",
        ),
    )

    evidence = observation(times).to_evidence_item()

    assert evidence.visible_at == available_at
    assert evidence.retrieved_at == retrieved_at
    assert evidence.visible_at != evidence.retrieved_at


def test_modeled_latency_requires_an_auditable_versioned_model_reference() -> None:
    with pytest.raises(ValueError, match="versioned latency model reference"):
        ObservationTimes(
            occurred_at=NOW - timedelta(days=1),
            published_at=NOW - timedelta(days=1),
            available_at=NOW - timedelta(days=1) + timedelta(seconds=12),
            source_updated_at=None,
            aggregator_fetched_at=None,
            retrieved_at=NOW,
            occurrence_basis=OccurrenceBasis.RETROSPECTIVE_SERIES,
            availability_basis=AvailabilityBasis.MODELED_LATENCY,
        )

    with pytest.raises(ValueError, match="immutable version"):
        LatencyModelReference(
            source_class="public-web",
            model_id="fixed-publication-delay",
            model_version="latest",
            calibration_ref="research/calibration/public-web.json",
        )


def test_unknown_publication_or_availability_fails_closed() -> None:
    times = ObservationTimes(
        occurred_at=NOW,
        published_at=None,
        available_at=None,
        source_updated_at=None,
        aggregator_fetched_at=None,
        retrieved_at=NOW,
        occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
        availability_basis=AvailabilityBasis.UNKNOWN,
    )

    item = observation(times)

    assert item.times.evidence_ready is False
    with pytest.raises(ValueError, match="publication time"):
        item.to_evidence_item()


def test_actual_receipt_availability_must_equal_retrieval() -> None:
    with pytest.raises(ValueError, match="actual-receipt available_at"):
        ObservationTimes(
            occurred_at=NOW - timedelta(seconds=2),
            published_at=NOW - timedelta(seconds=2),
            available_at=NOW - timedelta(seconds=1),
            source_updated_at=NOW - timedelta(seconds=2),
            aggregator_fetched_at=None,
            retrieved_at=NOW,
            occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        )


def test_enabled_provider_requires_verified_capability() -> None:
    base = manifest()
    unsafe = ObservationProviderManifest(
        schema_version=base.schema_version,
        provider_id=base.provider_id,
        provider_version=base.provider_version,
        transport=base.transport,
        declared_capabilities=base.declared_capabilities,
        verified_capabilities=frozenset(),
        upstream_sources=base.upstream_sources,
        auth_required=base.auth_required,
        provides_source_updated_at=base.provides_source_updated_at,
        provides_aggregator_fetched_at=base.provides_aggregator_fetched_at,
        provides_historical_occurrence_at=base.provides_historical_occurrence_at,
        provides_revision_history=base.provides_revision_history,
        enabled=True,
        trust_tier=ObservationTrustTier.UNVERIFIED,
        license_note=base.license_note,
    )

    assert unsafe.validation_errors() == (
        "enabled observation providers require a verified capability",
        "enabled observation providers require a validated trust tier",
    )


def test_content_addressed_batch_roundtrip_and_tamper_detection(tmp_path: Path) -> None:
    times = ObservationTimes(
        occurred_at=NOW - timedelta(seconds=2),
        published_at=NOW - timedelta(seconds=2),
        available_at=NOW,
        source_updated_at=NOW - timedelta(seconds=2),
        aggregator_fetched_at=None,
        retrieved_at=NOW,
        occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    batch = build_prediction_market_batch(
        provider_manifest=manifest(),
        retrieved_at=NOW,
        query={"limit": 1},
        data_available=True,
        degraded_reasons=(),
        observations=(observation(times),),
        raw_payload={"markets": [{"id": "market-1"}]},
    )

    first = write_prediction_market_batch(batch, tmp_path)
    second = write_prediction_market_batch(batch, tmp_path)

    assert second == first
    assert first.observation_count == 1
    assert first.evidence_ready_count == 1
    assert first.path.stat().st_mode & 0o777 == 0o600

    payload = json.loads(first.path.read_text(encoding="utf-8"))
    payload["observations"][0]["probability"] = "0.99"
    first.path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="bundle_hash"):
        validate_prediction_market_batch(first.path)


def test_raw_content_hash_is_order_independent_for_json_objects() -> None:
    assert raw_content_hash({"b": 2, "a": 1}) == raw_content_hash({"a": 1, "b": 2})


def test_rehashed_enabled_unverified_manifest_fails_semantic_validation(
    tmp_path: Path,
) -> None:
    times = ObservationTimes(
        occurred_at=NOW,
        published_at=None,
        available_at=None,
        source_updated_at=None,
        aggregator_fetched_at=None,
        retrieved_at=NOW,
        occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
        availability_basis=AvailabilityBasis.UNKNOWN,
    )
    batch = build_prediction_market_batch(
        provider_manifest=manifest(),
        retrieved_at=NOW,
        query={"limit": 1},
        data_available=True,
        degraded_reasons=(),
        observations=(observation(times),),
        raw_payload={"markets": [{"id": "market-1"}]},
    )
    bundle = write_prediction_market_batch(batch, tmp_path)
    payload = json.loads(bundle.path.read_text(encoding="utf-8"))
    payload["provider_manifest"]["enabled"] = True
    rehash_bundle_payload(payload)
    bundle.path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="verified capability"):
        validate_prediction_market_batch(bundle.path)


def test_rehashed_forged_evidence_ready_fails_semantic_validation(tmp_path: Path) -> None:
    times = ObservationTimes(
        occurred_at=NOW,
        published_at=None,
        available_at=NOW,
        source_updated_at=None,
        aggregator_fetched_at=None,
        retrieved_at=NOW,
        occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    batch = build_prediction_market_batch(
        provider_manifest=manifest(),
        retrieved_at=NOW,
        query={"limit": 1},
        data_available=True,
        degraded_reasons=(),
        observations=(observation(times),),
        raw_payload={"markets": [{"id": "market-1"}]},
    )
    bundle = write_prediction_market_batch(batch, tmp_path)
    payload = json.loads(bundle.path.read_text(encoding="utf-8"))
    payload["observations"][0]["times"]["evidence_ready"] = True
    rehash_bundle_payload(payload)
    bundle.path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="derived time contract"):
        validate_prediction_market_batch(bundle.path)
