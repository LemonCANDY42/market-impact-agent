from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

from market_impact_agent.observations import AvailabilityBasis, OccurrenceBasis
from market_impact_agent.prediction_markets import (
    KALSHI_MARKETS_ENDPOINT,
    POLYMARKET_GAMMA_ENDPOINT,
    WORLD_MONITOR_PREDICTIONS_ENDPOINT,
    KalshiPublicAdapter,
    PolymarketPublicAdapter,
    WorldMonitorPredictionAdapter,
)

NOW = datetime(2026, 8, 26, 2, tzinfo=UTC)


class FakeTransport:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dict[str, object], dict[str, str], float]] = []

    def __call__(
        self,
        endpoint: str,
        params: Mapping[str, object],
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object:
        self.calls.append((endpoint, dict(params), dict(headers), timeout_seconds))
        return self.payload


POLYMARKET_PAYLOAD = [
    {
        "id": "event-1",
        "slug": "fed-decision",
        "title": "Fed Decision?",
        "description": "Event description",
        "updatedAt": "2026-08-26T01:59:58Z",
        "resolutionSource": "https://example.test/official",
        "markets": [
            {
                "id": "market-1",
                "conditionId": "condition-1",
                "question": "Will the Fed cut rates?",
                "active": True,
                "closed": False,
                "updatedAt": "2026-08-26T01:59:59Z",
                "startDate": "2026-05-01T00:00:00Z",
                "endDate": "2026-09-16T00:00:00Z",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0.42", "0.58"]',
                "clobTokenIds": '["yes-token", "no-token"]',
                "bestBid": 0.41,
                "bestAsk": 0.43,
                "lastTradePrice": 0.42,
                "volume": "10000.5",
                "liquidity": "2000.25",
            }
        ],
    }
]

KALSHI_PAYLOAD = {
    "markets": [
        {
            "ticker": "KXFED-TEST",
            "event_ticker": "KXFED",
            "title": "Fed target below 4%?",
            "status": "active",
            "updated_time": "2026-08-26T01:59:59Z",
            "open_time": "2026-05-01T00:00:00Z",
            "close_time": "2026-09-16T00:00:00Z",
            "rules_primary": "Resolves Yes if the target is below 4%.",
            "rules_secondary": "Official source controls.",
            "last_price_dollars": "0.4100",
            "yes_bid_dollars": "0.4000",
            "yes_ask_dollars": "0.4400",
            "volume_fp": "1234.50",
            "open_interest_fp": "800.00",
            "liquidity_dollars": "500.25",
        }
    ]
}

WORLD_MONITOR_PAYLOAD = {
    "markets": [
        {
            "id": "fed-decision",
            "title": "Will the Fed cut rates?",
            "yesPrice": 0.42,
            "volume": 10000,
            "url": "https://polymarket.com/event/fed-decision",
            "closesAt": 1789516800000,
            "category": "fed",
            "source": "MARKET_SOURCE_POLYMARKET",
        }
    ],
    "pagination": {"nextCursor": "", "totalCount": 1},
    "fetchedAt": 1787709599000,
    "dataAvailable": True,
}


def test_polymarket_normalizes_source_times_and_raw_fields() -> None:
    transport = FakeTransport(POLYMARKET_PAYLOAD)
    batch = PolymarketPublicAdapter(
        transport=transport,
        clock=lambda: NOW,
    ).fetch_markets(limit=1, query="Fed")

    assert batch.data_available is True
    assert batch.evidence_ready_count == 1
    item = batch.observations[0]
    assert item.probability == Decimal("0.42")
    assert item.best_bid == Decimal("0.41")
    assert item.token_id == "yes-token"
    assert item.resolution_source == "https://example.test/official"
    assert item.times.source_updated_at == datetime(2026, 8, 26, 1, 59, 59, tzinfo=UTC)
    assert item.times.available_at == NOW
    assert item.times.occurrence_basis is OccurrenceBasis.SOURCE_REPORTED
    assert item.times.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
    assert item.to_evidence_item().visible_at == NOW
    assert transport.calls[0][0] == POLYMARKET_GAMMA_ENDPOINT


def test_polymarket_missing_update_time_is_not_evidence_ready() -> None:
    payload = cast(list[dict[str, object]], POLYMARKET_PAYLOAD.copy())
    event = dict(payload[0])
    event.pop("updatedAt")
    market = dict(cast(list[dict[str, object]], event["markets"])[0])
    market.pop("updatedAt")
    event["markets"] = [market]
    transport = FakeTransport([event])

    item = (
        PolymarketPublicAdapter(
            transport=transport,
            clock=lambda: NOW,
        )
        .fetch_markets(limit=1)
        .observations[0]
    )

    assert item.times.published_at is None
    assert item.times.evidence_ready is False
    with pytest.raises(ValueError, match="publication time"):
        item.to_evidence_item()


def test_kalshi_uses_current_fixed_point_fields_and_midpoint() -> None:
    transport = FakeTransport(KALSHI_PAYLOAD)
    batch = KalshiPublicAdapter(
        transport=transport,
        clock=lambda: NOW,
    ).fetch_markets(limit=20, query="Fed")

    item = batch.observations[0]
    assert item.probability == Decimal("0.4200")
    assert item.last_price == Decimal("0.4100")
    assert item.volume == Decimal("1234.50")
    assert item.open_interest == Decimal("800.00")
    assert item.source_ref == "kalshi://market/KXFED-TEST"
    assert "Official source controls" in (item.rules or "")
    assert item.times.source_updated_at == datetime(2026, 8, 26, 1, 59, 59, tzinfo=UTC)
    assert item.times.occurred_at == NOW
    assert item.times.occurrence_basis is OccurrenceBasis.RETRIEVAL_OBSERVED
    assert item.times.published_at is None
    assert item.times.evidence_ready is False
    with pytest.raises(ValueError, match="publication time"):
        item.to_evidence_item()
    assert transport.calls[0][0] == KALSHI_MARKETS_ENDPOINT
    assert transport.calls[0][1]["mve_filter"] == "exclude"


def test_world_monitor_preserves_aggregator_time_and_upstream_identity() -> None:
    transport = FakeTransport(WORLD_MONITOR_PAYLOAD)
    batch = WorldMonitorPredictionAdapter(
        "secret-key",
        transport=transport,
        clock=lambda: NOW,
    ).fetch_markets(limit=1, query="Fed")

    item = batch.observations[0]
    assert item.upstream_source == "polymarket"
    assert item.market_id == "fed-decision"
    assert item.event_id == "fed-decision"
    assert item.times.source_updated_at is None
    assert item.times.published_at is None
    assert item.times.evidence_ready is False
    assert item.times.aggregator_fetched_at == datetime.fromtimestamp(
        1787709599,
        tz=UTC,
    )
    assert item.times.occurrence_basis is OccurrenceBasis.AGGREGATOR_SNAPSHOT
    endpoint, _params, headers, _timeout = transport.calls[0]
    assert endpoint == WORLD_MONITOR_PREDICTIONS_ENDPOINT
    assert headers == {"X-WorldMonitor-Key": "secret-key"}
    assert "secret-key" not in json.dumps(batch.to_dict())


def test_aggregator_event_identity_does_not_impersonate_child_market_identity() -> None:
    direct = (
        PolymarketPublicAdapter(
            transport=FakeTransport(POLYMARKET_PAYLOAD),
            clock=lambda: NOW,
        )
        .fetch_markets(limit=1)
        .observations[0]
    )
    aggregated = (
        WorldMonitorPredictionAdapter(
            "secret-key",
            transport=FakeTransport(WORLD_MONITOR_PAYLOAD),
            clock=lambda: NOW,
        )
        .fetch_markets(limit=1)
        .observations[0]
    )

    assert aggregated.market_id != direct.market_id
    assert aggregated.claim_id != direct.claim_id
    assert aggregated.claim_id.startswith("prediction-market-discovery:")
    assert aggregated.source_ref != direct.source_ref
    with pytest.raises(ValueError, match="publication time"):
        aggregated.to_evidence_item()


def test_polymarket_duplicate_titles_remain_distinct_market_claims() -> None:
    payload = copy.deepcopy(POLYMARKET_PAYLOAD)
    event = payload[0]
    markets = event["markets"]
    assert isinstance(markets, list)
    duplicate = copy.deepcopy(markets[0])
    duplicate["id"] = "market-2"
    duplicate["conditionId"] = "condition-2"
    markets.append(duplicate)

    batch = PolymarketPublicAdapter(
        transport=FakeTransport(payload),
        clock=lambda: NOW,
    ).fetch_markets(limit=1)

    assert len(batch.observations) == 2
    assert len({item.claim_id for item in batch.observations}) == 2


def test_world_monitor_fails_closed_when_upstream_identity_is_unresolved() -> None:
    payload = copy.deepcopy(WORLD_MONITOR_PAYLOAD)
    markets = payload["markets"]
    assert isinstance(markets, list)
    market = markets[0]
    assert isinstance(market, dict)
    market["id"] = "market-1"

    with pytest.raises(ValueError, match="canonical upstream URL identity"):
        WorldMonitorPredictionAdapter(
            "secret-key",
            transport=FakeTransport(payload),
            clock=lambda: NOW,
        ).fetch_markets(limit=1)


def test_polymarket_identity_binds_consumed_event_fields() -> None:
    baseline = (
        PolymarketPublicAdapter(
            transport=FakeTransport(POLYMARKET_PAYLOAD),
            clock=lambda: NOW,
        )
        .fetch_markets(limit=1)
        .observations[0]
    )
    payload = copy.deepcopy(POLYMARKET_PAYLOAD)
    payload[0]["description"] = "Corrected event description"
    revised = (
        PolymarketPublicAdapter(
            transport=FakeTransport(payload),
            clock=lambda: NOW,
        )
        .fetch_markets(limit=1)
        .observations[0]
    )

    assert revised.rules == "Corrected event description"
    assert revised.raw_content_hash != baseline.raw_content_hash
    assert revised.observation_id != baseline.observation_id


def test_world_monitor_unavailable_snapshot_is_degraded_not_empty_truth() -> None:
    payload: dict[str, object] = {
        "markets": [],
        "fetchedAt": 0,
        "dataAvailable": False,
    }
    batch = WorldMonitorPredictionAdapter(
        "secret-key",
        transport=FakeTransport(payload),
        clock=lambda: NOW,
    ).fetch_markets()

    assert batch.data_available is False
    assert batch.observations == ()
    assert batch.degraded_reasons == ("World Monitor seed snapshot is unavailable",)


def test_world_monitor_requires_key_and_pinned_endpoint() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        WorldMonitorPredictionAdapter("")
    with pytest.raises(ValueError, match="pinned official"):
        PolymarketPublicAdapter(endpoint="https://example.test/events")
