from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from market_impact_agent.observations import (
    OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
    AvailabilityBasis,
    ObservationCapability,
    ObservationProviderManifest,
    ObservationTimes,
    ObservationTrustTier,
    OccurrenceBasis,
    PredictionMarketBatch,
    PredictionMarketObservation,
    build_prediction_market_batch,
    observation_id,
    raw_content_hash,
)
from market_impact_agent.providers import ProviderTransport
from market_impact_agent.research import EvidenceTier

POLYMARKET_GAMMA_ENDPOINT = "https://gamma-api.polymarket.com/events"
KALSHI_MARKETS_ENDPOINT = "https://api.elections.kalshi.com/trade-api/v2/markets"
WORLD_MONITOR_PREDICTIONS_ENDPOINT = (
    "https://api.worldmonitor.app/api/prediction/v1/list-prediction-markets"
)

_MAX_RESPONSE_BYTES = 20 * 1024 * 1024


class JsonGetTransport(Protocol):
    def __call__(
        self,
        endpoint: str,
        params: Mapping[str, object],
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object: ...


class PredictionMarketAdapter(Protocol):
    @property
    def manifest(self) -> ObservationProviderManifest: ...

    def fetch_markets(
        self,
        *,
        limit: int = 20,
        query: str | None = None,
    ) -> PredictionMarketBatch: ...


def polymarket_provider_manifest() -> ObservationProviderManifest:
    return ObservationProviderManifest(
        schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
        provider_id="polymarket-public",
        provider_version="1.0.0",
        transport=ProviderTransport.HTTP,
        declared_capabilities=frozenset({ObservationCapability.PREDICTION_MARKET_SNAPSHOT}),
        verified_capabilities=frozenset(),
        upstream_sources=("polymarket",),
        auth_required=False,
        provides_source_updated_at=True,
        provides_aggregator_fetched_at=False,
        provides_historical_occurrence_at=False,
        provides_revision_history=False,
        enabled=False,
        trust_tier=ObservationTrustTier.UNVERIFIED,
        license_note=(
            "Public API access; retention and redistribution remain subject to source terms."
        ),
    )


def kalshi_provider_manifest() -> ObservationProviderManifest:
    return ObservationProviderManifest(
        schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
        provider_id="kalshi-public",
        provider_version="1.0.0",
        transport=ProviderTransport.HTTP,
        declared_capabilities=frozenset({ObservationCapability.PREDICTION_MARKET_SNAPSHOT}),
        verified_capabilities=frozenset(),
        upstream_sources=("kalshi",),
        auth_required=False,
        provides_source_updated_at=True,
        provides_aggregator_fetched_at=False,
        provides_historical_occurrence_at=False,
        provides_revision_history=False,
        enabled=False,
        trust_tier=ObservationTrustTier.UNVERIFIED,
        license_note=(
            "Public API access; retention and redistribution remain subject to source terms."
        ),
    )


def world_monitor_provider_manifest() -> ObservationProviderManifest:
    return ObservationProviderManifest(
        schema_version=OBSERVATION_PROVIDER_MANIFEST_SCHEMA,
        provider_id="world-monitor-predictions",
        provider_version="1.0.0",
        transport=ProviderTransport.HTTP,
        declared_capabilities=frozenset(
            {
                ObservationCapability.PREDICTION_MARKET_DISCOVERY,
                ObservationCapability.PREDICTION_MARKET_SNAPSHOT,
            }
        ),
        verified_capabilities=frozenset(),
        upstream_sources=("polymarket", "kalshi"),
        auth_required=True,
        provides_source_updated_at=False,
        provides_aggregator_fetched_at=True,
        provides_historical_occurrence_at=False,
        provides_revision_history=False,
        enabled=False,
        trust_tier=ObservationTrustTier.UNVERIFIED,
        license_note=(
            "AGPL service implementation and authenticated API; returned data remains subject "
            "to World Monitor and upstream source terms."
        ),
    )


class PolymarketPublicAdapter:
    def __init__(
        self,
        *,
        endpoint: str = POLYMARKET_GAMMA_ENDPOINT,
        timeout_seconds: float = 20.0,
        transport: JsonGetTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        _validate_adapter_settings(
            endpoint=endpoint,
            expected_endpoint=POLYMARKET_GAMMA_ENDPOINT,
            timeout_seconds=timeout_seconds,
        )
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _get_json if transport is None else transport
        self._clock = clock

    @property
    def manifest(self) -> ObservationProviderManifest:
        return polymarket_provider_manifest()

    def fetch_markets(
        self,
        *,
        limit: int = 20,
        query: str | None = None,
    ) -> PredictionMarketBatch:
        _limit(limit)
        params: dict[str, object] = {
            "limit": limit,
            "active": True,
            "closed": False,
            "order": "volume24hr",
            "ascending": False,
        }
        payload = self._transport(self._endpoint, params, {}, self._timeout_seconds)
        retrieved_at = _aware_clock(self._clock())
        events = _list(payload, "Polymarket events")
        observations: list[PredictionMarketObservation] = []
        for raw_event in events:
            event = _mapping(raw_event, "Polymarket event")
            event_title = _string(event.get("title"))
            event_slug = _string(event.get("slug"))
            if query and query.casefold() not in f"{event_title} {event_slug}".casefold():
                continue
            raw_markets = event.get("markets", [])
            for raw_market in _list(raw_markets, "Polymarket event markets"):
                market = _mapping(raw_market, "Polymarket market")
                observation = _polymarket_observation(
                    event=event,
                    market=market,
                    retrieved_at=retrieved_at,
                )
                if observation is not None:
                    observations.append(observation)
        return build_prediction_market_batch(
            provider_manifest=self.manifest,
            retrieved_at=retrieved_at,
            query={**params, "query": query or ""},
            data_available=True,
            degraded_reasons=(),
            observations=tuple(observations),
            raw_payload=payload,
        )


class KalshiPublicAdapter:
    def __init__(
        self,
        *,
        endpoint: str = KALSHI_MARKETS_ENDPOINT,
        timeout_seconds: float = 20.0,
        transport: JsonGetTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        _validate_adapter_settings(
            endpoint=endpoint,
            expected_endpoint=KALSHI_MARKETS_ENDPOINT,
            timeout_seconds=timeout_seconds,
        )
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _get_json if transport is None else transport
        self._clock = clock

    @property
    def manifest(self) -> ObservationProviderManifest:
        return kalshi_provider_manifest()

    def fetch_markets(
        self,
        *,
        limit: int = 20,
        query: str | None = None,
    ) -> PredictionMarketBatch:
        _limit(limit)
        params: dict[str, object] = {
            "limit": limit,
            "status": "open",
            "mve_filter": "exclude",
        }
        payload = self._transport(self._endpoint, params, {}, self._timeout_seconds)
        retrieved_at = _aware_clock(self._clock())
        outer = _mapping(payload, "Kalshi response")
        markets = _list(outer.get("markets"), "Kalshi markets")
        observations: list[PredictionMarketObservation] = []
        for raw_market in markets:
            market = _mapping(raw_market, "Kalshi market")
            title = _string(market.get("title"))
            ticker = _string(market.get("ticker"))
            if query and query.casefold() not in f"{title} {ticker}".casefold():
                continue
            observation = _kalshi_observation(market=market, retrieved_at=retrieved_at)
            if observation is not None:
                observations.append(observation)
        return build_prediction_market_batch(
            provider_manifest=self.manifest,
            retrieved_at=retrieved_at,
            query={**params, "query": query or ""},
            data_available=True,
            degraded_reasons=(),
            observations=tuple(observations),
            raw_payload=payload,
        )


class WorldMonitorPredictionAdapter:
    def __init__(
        self,
        api_key: str,
        *,
        endpoint: str = WORLD_MONITOR_PREDICTIONS_ENDPOINT,
        timeout_seconds: float = 20.0,
        transport: JsonGetTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not api_key:
            raise ValueError("World Monitor API key must not be empty")
        _validate_adapter_settings(
            endpoint=endpoint,
            expected_endpoint=WORLD_MONITOR_PREDICTIONS_ENDPOINT,
            timeout_seconds=timeout_seconds,
        )
        self._api_key = api_key
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _get_json if transport is None else transport
        self._clock = clock

    @property
    def manifest(self) -> ObservationProviderManifest:
        return world_monitor_provider_manifest()

    def fetch_markets(
        self,
        *,
        limit: int = 20,
        query: str | None = None,
        category: str | None = None,
    ) -> PredictionMarketBatch:
        _limit(limit)
        params: dict[str, object] = {"page_size": limit}
        if query:
            params["query"] = query
        if category:
            params["category"] = category
        payload = self._transport(
            self._endpoint,
            params,
            {"X-WorldMonitor-Key": self._api_key},
            self._timeout_seconds,
        )
        retrieved_at = _aware_clock(self._clock())
        outer = _mapping(payload, "World Monitor response")
        data_available = _boolean(outer.get("dataAvailable"), "dataAvailable")
        fetched_at_raw = outer.get("fetchedAt")
        fetched_at = _epoch_milliseconds(fetched_at_raw, "fetchedAt", allow_zero=True)
        if data_available and fetched_at is None:
            raise ValueError("available World Monitor snapshots require non-zero fetchedAt")
        if not data_available:
            return build_prediction_market_batch(
                provider_manifest=self.manifest,
                retrieved_at=retrieved_at,
                query=params,
                data_available=False,
                degraded_reasons=("World Monitor seed snapshot is unavailable",),
                observations=(),
                raw_payload=payload,
            )
        if fetched_at is None:
            raise AssertionError("validated fetchedAt unexpectedly missing")
        markets = _list(outer.get("markets"), "World Monitor markets")
        observations = tuple(
            _world_monitor_observation(
                market=_mapping(raw_market, "World Monitor market"),
                fetched_at=fetched_at,
                retrieved_at=retrieved_at,
            )
            for raw_market in markets
        )
        return build_prediction_market_batch(
            provider_manifest=self.manifest,
            retrieved_at=retrieved_at,
            query=params,
            data_available=True,
            degraded_reasons=(),
            observations=observations,
            raw_payload=payload,
        )


def _polymarket_observation(
    *,
    event: Mapping[str, object],
    market: Mapping[str, object],
    retrieved_at: datetime,
) -> PredictionMarketObservation | None:
    market_id = _required_string(market.get("id"), "Polymarket market.id")
    outcomes = _json_array(market.get("outcomes"), "Polymarket outcomes")
    prices = _json_array(market.get("outcomePrices"), "Polymarket outcomePrices")
    token_ids = _json_array(market.get("clobTokenIds"), "Polymarket clobTokenIds")
    yes_index = next(
        (index for index, outcome in enumerate(outcomes) if str(outcome).casefold() == "yes"),
        None,
    )
    if yes_index is None or yes_index >= len(prices):
        return None
    probability = _decimal(prices[yes_index], "Polymarket yes probability")
    if probability is None:
        return None
    source_updated_at = _optional_iso_datetime(market.get("updatedAt"), "market.updatedAt")
    if source_updated_at is None:
        source_updated_at = _optional_iso_datetime(event.get("updatedAt"), "event.updatedAt")
    if source_updated_at is None:
        occurred_at = retrieved_at
        occurrence_basis = OccurrenceBasis.RETRIEVAL_OBSERVED
    else:
        occurred_at = source_updated_at
        occurrence_basis = OccurrenceBasis.SOURCE_REPORTED
    times = ObservationTimes(
        occurred_at=occurred_at,
        published_at=source_updated_at,
        available_at=retrieved_at,
        source_updated_at=source_updated_at,
        aggregator_fetched_at=None,
        retrieved_at=retrieved_at,
        occurrence_basis=occurrence_basis,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    raw_hash = raw_content_hash({"event": event, "market": market})
    outcome = "Yes"
    provider_id = polymarket_provider_manifest().provider_id
    event_slug = _string(event.get("slug"))
    token_id = str(token_ids[yes_index]) if yes_index < len(token_ids) else None
    rules = _optional_string(market.get("description")) or _optional_string(
        event.get("description")
    )
    resolution_source = _optional_string(market.get("resolutionSource")) or _optional_string(
        event.get("resolutionSource")
    )
    return PredictionMarketObservation(
        observation_id=observation_id(
            provider_id=provider_id,
            upstream_source="polymarket",
            market_id=market_id,
            outcome=outcome,
            probability=probability,
            times=times,
            raw_hash=raw_hash,
        ),
        provider_id=provider_id,
        upstream_source="polymarket",
        source_tier=EvidenceTier.PRIMARY,
        market_id=market_id,
        event_id=_optional_string(event.get("id")),
        title=_required_string(market.get("question"), "Polymarket market.question"),
        outcome=outcome,
        probability=probability,
        source_ref=f"polymarket://market/{market_id}",
        source_url=f"https://polymarket.com/event/{event_slug}" if event_slug else None,
        token_id=token_id,
        status="closed" if bool(market.get("closed")) else "active",
        rules=rules,
        resolution_source=resolution_source,
        opened_at=_optional_iso_datetime(market.get("startDate"), "market.startDate"),
        closes_at=_optional_iso_datetime(market.get("endDate"), "market.endDate"),
        resolved_at=_optional_iso_datetime(market.get("closedTime"), "market.closedTime"),
        best_bid=_decimal(market.get("bestBid"), "bestBid"),
        best_ask=_decimal(market.get("bestAsk"), "bestAsk"),
        last_price=_decimal(market.get("lastTradePrice"), "lastTradePrice"),
        volume=_decimal(market.get("volumeNum") or market.get("volume"), "volume"),
        open_interest=_decimal(market.get("openInterest"), "openInterest"),
        liquidity=_decimal(market.get("liquidityNum") or market.get("liquidity"), "liquidity"),
        times=times,
        raw_content_hash=raw_hash,
    )


def _kalshi_observation(
    *,
    market: Mapping[str, object],
    retrieved_at: datetime,
) -> PredictionMarketObservation | None:
    market_id = _required_string(market.get("ticker"), "Kalshi market.ticker")
    best_bid = _dollar_or_cents(market, "yes_bid_dollars", "yes_bid")
    best_ask = _dollar_or_cents(market, "yes_ask_dollars", "yes_ask")
    last_price = _dollar_or_cents(market, "last_price_dollars", "last_price")
    if best_bid is not None and best_ask is not None and best_ask >= best_bid:
        probability = (best_bid + best_ask) / Decimal("2")
    else:
        probability = last_price
    if probability is None:
        return None
    source_updated_at = _optional_iso_datetime(market.get("updated_time"), "updated_time")
    times = ObservationTimes(
        occurred_at=retrieved_at,
        published_at=None,
        available_at=retrieved_at,
        source_updated_at=source_updated_at,
        aggregator_fetched_at=None,
        retrieved_at=retrieved_at,
        occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    raw_hash = raw_content_hash(market)
    provider_id = kalshi_provider_manifest().provider_id
    rules = (
        "\n\n".join(
            value
            for value in (
                _optional_string(market.get("rules_primary")),
                _optional_string(market.get("rules_secondary")),
            )
            if value
        )
        or None
    )
    outcome = "Yes"
    return PredictionMarketObservation(
        observation_id=observation_id(
            provider_id=provider_id,
            upstream_source="kalshi",
            market_id=market_id,
            outcome=outcome,
            probability=probability,
            times=times,
            raw_hash=raw_hash,
        ),
        provider_id=provider_id,
        upstream_source="kalshi",
        source_tier=EvidenceTier.REGULATED,
        market_id=market_id,
        event_id=_optional_string(market.get("event_ticker")),
        title=_required_string(market.get("title"), "Kalshi market.title"),
        outcome=outcome,
        probability=probability,
        source_ref=f"kalshi://market/{market_id}",
        source_url=f"https://kalshi.com/markets/{market_id}",
        token_id=market_id,
        status=_optional_string(market.get("status")),
        rules=rules,
        resolution_source=None,
        opened_at=_optional_iso_datetime(market.get("open_time"), "open_time"),
        closes_at=_optional_iso_datetime(market.get("close_time"), "close_time"),
        resolved_at=None,
        best_bid=best_bid,
        best_ask=best_ask,
        last_price=last_price,
        volume=_decimal(market.get("volume_fp") or market.get("volume"), "volume"),
        open_interest=_decimal(
            market.get("open_interest_fp") or market.get("open_interest"),
            "open_interest",
        ),
        liquidity=_decimal(
            market.get("liquidity_dollars") or market.get("liquidity"),
            "liquidity",
        ),
        times=times,
        raw_content_hash=raw_hash,
    )


def _world_monitor_observation(
    *,
    market: Mapping[str, object],
    fetched_at: datetime,
    retrieved_at: datetime,
) -> PredictionMarketObservation:
    source_value = _required_string(market.get("source"), "World Monitor market.source")
    source = {
        "MARKET_SOURCE_POLYMARKET": "polymarket",
        "polymarket": "polymarket",
        "MARKET_SOURCE_KALSHI": "kalshi",
        "kalshi": "kalshi",
    }.get(source_value)
    if source is None:
        raise ValueError("World Monitor market.source is not a supported upstream source")
    market_id = _required_string(market.get("id"), "World Monitor market.id")
    title = _required_string(market.get("title"), "World Monitor market.title")
    source_url = _required_string(market.get("url"), "World Monitor market.url")
    _validate_world_monitor_identity(
        source=source,
        record_id=market_id,
        source_url=source_url,
    )
    probability = _required_decimal(market.get("yesPrice"), "World Monitor yesPrice")
    times = ObservationTimes(
        occurred_at=fetched_at,
        published_at=None,
        available_at=retrieved_at,
        source_updated_at=None,
        aggregator_fetched_at=fetched_at,
        retrieved_at=retrieved_at,
        occurrence_basis=OccurrenceBasis.AGGREGATOR_SNAPSHOT,
        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
    )
    raw_hash = raw_content_hash(market)
    provider_id = world_monitor_provider_manifest().provider_id
    outcome = "Yes"
    return PredictionMarketObservation(
        observation_id=observation_id(
            provider_id=provider_id,
            upstream_source=source,
            market_id=market_id,
            outcome=outcome,
            probability=probability,
            times=times,
            raw_hash=raw_hash,
        ),
        provider_id=provider_id,
        upstream_source=source,
        source_tier=EvidenceTier.SPECIALIST,
        market_id=market_id,
        event_id=market_id if source == "polymarket" else None,
        title=title,
        outcome=outcome,
        probability=probability,
        source_ref=f"world-monitor://{source}/{market_id}",
        source_url=source_url,
        token_id=None,
        status="active",
        rules=None,
        resolution_source=None,
        opened_at=None,
        closes_at=_epoch_milliseconds(market.get("closesAt"), "closesAt", allow_zero=True),
        resolved_at=None,
        best_bid=None,
        best_ask=None,
        last_price=None,
        volume=_decimal(market.get("volume"), "volume"),
        open_interest=None,
        liquidity=None,
        times=times,
        raw_content_hash=raw_hash,
    )


def _validate_world_monitor_identity(
    *,
    source: str,
    record_id: str,
    source_url: str,
) -> None:
    parsed = urlparse(source_url)
    parts = [part for part in parsed.path.split("/") if part]
    expected_prefix = "event" if source == "polymarket" else "markets"
    expected_host = "polymarket.com" if source == "polymarket" else "kalshi.com"
    if (
        parsed.scheme != "https"
        or parsed.netloc.casefold() not in {expected_host, f"www.{expected_host}"}
        or len(parts) != 2
        or parts[0] != expected_prefix
        or parts[1] != record_id
    ):
        raise ValueError("World Monitor market.id must match its canonical upstream URL identity")


def _get_json(
    endpoint: str,
    params: Mapping[str, object],
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> object:
    filtered_params = {key: value for key, value in params.items() if value is not None}
    url = f"{endpoint}?{urlencode(filtered_params)}" if filtered_params else endpoint
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "market-impact-agent/0.1 read-only-observation-adapter",
        **headers,
    }
    request = Request(url, headers=request_headers, method="GET")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            content_type = response.headers.get_content_type()
            if content_type not in {"application/json", "text/json"}:
                raise RuntimeError(f"provider returned unexpected content type: {content_type}")
            body = response.read(_MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        raise RuntimeError(f"provider HTTP request failed with status {exc.code}") from exc
    except (TimeoutError, URLError) as exc:
        raise RuntimeError("provider HTTP request failed") from exc
    if len(body) > _MAX_RESPONSE_BYTES:
        raise RuntimeError("provider response exceeded the size limit")
    try:
        return cast(object, json.loads(body))
    except json.JSONDecodeError as exc:
        raise RuntimeError("provider returned invalid JSON") from exc


def _validate_adapter_settings(
    *,
    endpoint: str,
    expected_endpoint: str,
    timeout_seconds: float,
) -> None:
    if endpoint != expected_endpoint:
        raise ValueError("adapter endpoint must be the pinned official HTTPS endpoint")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")


def _limit(value: int) -> None:
    if isinstance(value, bool) or not 1 <= value <= 100:
        raise ValueError("limit must be between 1 and 100")


def _aware_clock(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("adapter clock must be timezone-aware")
    return value.astimezone(UTC)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a JSON object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(Mapping[str, object], raw)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a JSON array")
    return cast(list[object], value)


def _json_array(value: object, name: str) -> list[object]:
    if isinstance(value, list):
        return cast(list[object], value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{name} must contain a JSON array") from exc
        return _list(decoded, name)
    raise TypeError(f"{name} must be an array or encoded array")


def _string(value: object) -> str:
    return value if isinstance(value, str) else ""


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} must be a non-empty string")
    return value


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _decimal(value: object, name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must be finite")
    return parsed


def _required_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result is None:
        raise TypeError(f"{name} must be present")
    return result


def _dollar_or_cents(
    market: Mapping[str, object],
    dollar_field: str,
    cents_field: str,
) -> Decimal | None:
    dollars = _decimal(market.get(dollar_field), dollar_field)
    if dollars is not None:
        return dollars
    cents = _decimal(market.get(cents_field), cents_field)
    return None if cents is None else cents / Decimal("100")


def _optional_iso_datetime(value: object, name: str) -> datetime | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an ISO date-time string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid ISO date-time") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed.astimezone(UTC)


def _epoch_milliseconds(value: object, name: str, *, allow_zero: bool) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, str) and not value.isdigit():
        return _optional_iso_datetime(value, name)
    if isinstance(value, bool):
        raise TypeError(f"{name} must be epoch milliseconds")
    if not isinstance(value, (int, float, str)):
        raise TypeError(f"{name} must be epoch milliseconds")
    try:
        milliseconds = int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{name} must be epoch milliseconds") from exc
    if milliseconds == 0 and allow_zero:
        return None
    if milliseconds <= 0:
        raise ValueError(f"{name} must be positive epoch milliseconds")
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
