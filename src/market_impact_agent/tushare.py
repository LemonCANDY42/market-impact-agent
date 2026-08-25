from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from market_impact_agent.domain import TradingEnvironment, require_aware
from market_impact_agent.providers import (
    Capability,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)

TUSHARE_HTTP_ENDPOINT = "https://api.tushare.pro"
TUSHARE_PROVIDER_ID = "tushare-http"
TUSHARE_ADAPTER_VERSION = "0.1.0"

_DATE_PATTERN = re.compile(r"^\d{8}$")
_STOCK_FIELDS = (
    "ts_code",
    "symbol",
    "name",
    "exchange",
    "list_status",
    "list_date",
    "delist_date",
)
_CALENDAR_FIELDS = ("exchange", "cal_date", "is_open", "pretrade_date")
_DAILY_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "vol",
    "amount",
)
_LIST_STATUSES = ("L", "D", "P", "G")
_EXCHANGE_SUFFIXES = {"SSE": "XSHG", "SZSE": "XSHE"}
_ROW_LIMIT = 6000


class TushareTransport(Protocol):
    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes: ...


class TushareTransportError(RuntimeError):
    pass


class TushareApiError(RuntimeError):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"Tushare API error {code}: {message}")
        self.code = code


@dataclass(frozen=True, slots=True)
class TushareTable:
    api_name: str
    params: tuple[tuple[str, str], ...]
    fields: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    retrieved_at: datetime
    content_hash: str


@dataclass(frozen=True, slots=True)
class StockListing:
    instrument_id: str
    tushare_code: str
    symbol: str
    name: str
    exchange: str
    current_status: str
    listed_on: date
    delisted_on: date | None


@dataclass(frozen=True, slots=True)
class StockListingSnapshot:
    provider_id: str
    provider_version: str
    retrieved_at: datetime
    listings: tuple[StockListing, ...]
    query_hashes: tuple[str, ...]
    snapshot_hash: str


@dataclass(frozen=True, slots=True)
class FixedAshareUniverse:
    universe_id: str
    as_of_date: date
    exchanges: tuple[str, ...]
    instrument_ids: tuple[str, ...]
    listing_snapshot_hash: str
    built_at: datetime


class TushareHttpAdapter:
    def __init__(
        self,
        token: str,
        *,
        endpoint: str = TUSHARE_HTTP_ENDPOINT,
        timeout_seconds: float = 10.0,
        transport: TushareTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not token:
            raise ValueError("Tushare token must not be empty")
        if not endpoint.startswith("https://"):
            raise ValueError("Tushare endpoint must use HTTPS")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._token = token
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._transport = _post_json if transport is None else transport
        self._clock = clock

    @property
    def manifest(self) -> ProviderManifest:
        return ProviderManifest(
            schema_version="market-impact.provider-manifest.v1",
            provider_id=TUSHARE_PROVIDER_ID,
            provider_version=TUSHARE_ADAPTER_VERSION,
            transport=ProviderTransport.HTTP,
            environments=frozenset({TradingEnvironment.BACKTEST}),
            declared_capabilities=frozenset({Capability.MARKET_DATA}),
            verified_capabilities=frozenset(),
            markets=("CN",),
            order_types=(),
            supports_streaming=False,
            supports_reconciliation=False,
            enabled=False,
            trust_tier=TrustTier.UNVERIFIED,
        )

    def fetch_trade_calendar(
        self,
        *,
        exchange: str,
        start_date: str,
        end_date: str,
    ) -> TushareTable:
        _date_range(start_date, end_date)
        if exchange not in _EXCHANGE_SUFFIXES:
            raise ValueError("exchange must be SSE or SZSE")
        table = self._query(
            api_name="trade_cal",
            params={"exchange": exchange, "start_date": start_date, "end_date": end_date},
            fields=_CALENDAR_FIELDS,
        )
        _validate_trade_calendar(
            table,
            exchange=exchange,
            start_date=_date(start_date, "start_date"),
            end_date=_date(end_date, "end_date"),
        )
        return table

    def fetch_daily(
        self,
        *,
        tushare_code: str,
        start_date: str,
        end_date: str,
    ) -> TushareTable:
        _date_range(start_date, end_date)
        _canonical_instrument_id(tushare_code)
        table = self._query(
            api_name="daily",
            params={
                "ts_code": tushare_code,
                "start_date": start_date,
                "end_date": end_date,
            },
            fields=_DAILY_FIELDS,
        )
        _require_unique_rows(table, key_fields=("ts_code", "trade_date"))
        _validate_daily(
            table,
            tushare_code=tushare_code,
            start_date=_date(start_date, "start_date"),
            end_date=_date(end_date, "end_date"),
        )
        return table

    def fetch_stock_listings(self) -> StockListingSnapshot:
        tables = tuple(
            self._query(
                api_name="stock_basic",
                params={"exchange": exchange, "list_status": status},
                fields=_STOCK_FIELDS,
            )
            for exchange in _EXCHANGE_SUFFIXES
            for status in _LIST_STATUSES
        )
        listings_by_code: dict[str, StockListing] = {}
        for table in tables:
            _require_unique_rows(table, key_fields=("ts_code",))
            expected_status = dict(table.params)["list_status"]
            expected_exchange = dict(table.params)["exchange"]
            for row in table.rows:
                listing = _stock_listing(
                    table.fields,
                    row,
                    expected_exchange=expected_exchange,
                    expected_status=expected_status,
                )
                existing = listings_by_code.get(listing.tushare_code)
                if existing is not None and existing != listing:
                    raise ValueError(f"conflicting stock_basic rows for {listing.tushare_code}")
                listings_by_code[listing.tushare_code] = listing

        retrieved_at = max(table.retrieved_at for table in tables)
        listings = tuple(sorted(listings_by_code.values(), key=lambda item: item.instrument_id))
        query_hashes = tuple(table.content_hash for table in tables)
        snapshot_hash = _canonical_hash(
            {
                "provider_id": TUSHARE_PROVIDER_ID,
                "provider_version": TUSHARE_ADAPTER_VERSION,
                "retrieved_at": _canonical_timestamp(retrieved_at),
                "listings": [_listing_payload(listing) for listing in listings],
                "query_hashes": list(query_hashes),
            }
        )
        return StockListingSnapshot(
            provider_id=TUSHARE_PROVIDER_ID,
            provider_version=TUSHARE_ADAPTER_VERSION,
            retrieved_at=retrieved_at,
            listings=listings,
            query_hashes=query_hashes,
            snapshot_hash=snapshot_hash,
        )

    def _query(
        self,
        *,
        api_name: str,
        params: dict[str, str],
        fields: tuple[str, ...],
    ) -> TushareTable:
        request_payload: dict[str, object] = {
            "api_name": api_name,
            "token": self._token,
            "params": params,
            "fields": ",".join(fields),
        }
        body = json.dumps(
            request_payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        raw_response = self._transport(self._endpoint, body, self._timeout_seconds)
        response = _json_object(raw_response)
        code = _integer(response.get("code"), "response code")
        if code != 0:
            message = response.get("msg")
            safe_message = (
                message.replace(self._token, "[REDACTED]")
                if isinstance(message, str)
                else "unknown error"
            )
            raise TushareApiError(code, safe_message)

        response_fields, response_rows = _response_data(response.get("data"))
        if set(response_fields) != set(fields):
            raise ValueError("Tushare response fields do not match the requested fields")
        rows = _normalize_rows(
            response_fields=response_fields,
            response_rows=response_rows,
            requested_fields=fields,
        )
        if len(rows) >= _ROW_LIMIT:
            raise ValueError("Tushare response reached its row limit; split the query")
        retrieved_at = self._clock()
        require_aware(retrieved_at, "retrieved_at")
        params_tuple = tuple(sorted(params.items()))
        content_hash = _canonical_hash(
            {
                "api_name": api_name,
                "fields": list(fields),
                "params": dict(params_tuple),
                "rows": [list(row) for row in rows],
            }
        )
        return TushareTable(
            api_name=api_name,
            params=params_tuple,
            fields=fields,
            rows=rows,
            retrieved_at=retrieved_at,
            content_hash=content_hash,
        )


def build_pre_event_universe(
    snapshot: StockListingSnapshot,
    *,
    as_of_date: date,
    exchanges: tuple[str, ...] = ("SSE", "SZSE"),
) -> FixedAshareUniverse:
    if not exchanges or len(exchanges) != len(set(exchanges)):
        raise ValueError("exchanges must be non-empty and unique")
    if any(exchange not in _EXCHANGE_SUFFIXES for exchange in exchanges):
        raise ValueError("universe exchanges must be SSE or SZSE")
    canonical_exchanges = tuple(sorted(exchanges))
    selected_ids = tuple(
        listing.instrument_id
        for listing in snapshot.listings
        if listing.exchange in canonical_exchanges
        and listing.listed_on <= as_of_date
        and (listing.delisted_on is None or listing.delisted_on > as_of_date)
    )
    if len(selected_ids) != len(set(selected_ids)):
        raise ValueError("listing snapshot contains duplicate instruments")
    instrument_ids = tuple(sorted(selected_ids))
    if not instrument_ids:
        raise ValueError("pre-event universe must not be empty")
    universe_hash = _canonical_hash(
        {
            "as_of_date": as_of_date.isoformat(),
            "exchanges": list(canonical_exchanges),
            "instrument_ids": list(instrument_ids),
            "listing_snapshot_hash": snapshot.snapshot_hash,
        }
    )
    return FixedAshareUniverse(
        universe_id=f"a-share-{as_of_date.isoformat()}-{universe_hash[:16]}",
        as_of_date=as_of_date,
        exchanges=canonical_exchanges,
        instrument_ids=instrument_ids,
        listing_snapshot_hash=snapshot.snapshot_hash,
        built_at=snapshot.retrieved_at,
    )


def _post_json(endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
    request = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return response.read()
    except (HTTPError, URLError, TimeoutError) as exc:
        raise TushareTransportError("Tushare HTTPS request failed") from exc


def _json_object(raw_response: bytes) -> dict[str, object]:
    try:
        payload = json.loads(raw_response)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Tushare response must be valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Tushare response must be a JSON object")
    return cast(dict[str, object], payload)


def _response_data(value: object) -> tuple[tuple[str, ...], tuple[tuple[object, ...], ...]]:
    if not isinstance(value, dict):
        raise ValueError("successful Tushare response requires data")
    data = cast(dict[object, object], value)
    raw_fields = data.get("fields")
    raw_items = data.get("items")
    if not isinstance(raw_fields, list):
        raise ValueError("Tushare data fields must be non-empty strings")
    raw_fields_list = cast(list[object], raw_fields)
    if not all(isinstance(field, str) and field for field in raw_fields_list):
        raise ValueError("Tushare data fields must be non-empty strings")
    fields = tuple(cast(str, field) for field in raw_fields_list)
    if len(fields) != len(set(fields)):
        raise ValueError("Tushare data fields must be unique")
    if not isinstance(raw_items, list):
        raise ValueError("Tushare data items must be an array")
    rows: list[tuple[object, ...]] = []
    for item in cast(list[object], raw_items):
        if not isinstance(item, list):
            raise ValueError("Tushare data rows must match the fields")
        raw_row = cast(list[object], item)
        if len(raw_row) != len(fields):
            raise ValueError("Tushare data rows must match the fields")
        if any(isinstance(cell, (dict, list)) for cell in raw_row):
            raise ValueError("Tushare data cells must be scalar values")
        rows.append(tuple(raw_row))
    return fields, tuple(rows)


def _normalize_rows(
    *,
    response_fields: tuple[str, ...],
    response_rows: tuple[tuple[object, ...], ...],
    requested_fields: tuple[str, ...],
) -> tuple[tuple[object, ...], ...]:
    indexes = {field: response_fields.index(field) for field in requested_fields}
    normalized = tuple(
        tuple(row[indexes[field]] for field in requested_fields) for row in response_rows
    )
    return tuple(sorted(normalized, key=_canonical_row_key))


def _canonical_row_key(row: tuple[object, ...]) -> str:
    return json.dumps(row, ensure_ascii=True, separators=(",", ":"))


def _stock_listing(
    fields: tuple[str, ...],
    row: tuple[object, ...],
    *,
    expected_exchange: str,
    expected_status: str,
) -> StockListing:
    values = dict(zip(fields, row, strict=True))
    tushare_code = _string(values["ts_code"], "ts_code")
    exchange = _string(values["exchange"], "exchange")
    if exchange not in _EXCHANGE_SUFFIXES:
        raise ValueError(f"unsupported stock exchange: {exchange}")
    if exchange != expected_exchange:
        raise ValueError("stock_basic exchange conflicts with the query")
    instrument_id = _canonical_instrument_id(tushare_code)
    expected_exchange = "SSE" if instrument_id.endswith(".XSHG") else "SZSE"
    if exchange != expected_exchange:
        raise ValueError("stock_basic exchange conflicts with ts_code")
    symbol = _string(values["symbol"], "symbol")
    if symbol != tushare_code.split(".", 1)[0]:
        raise ValueError("stock_basic symbol conflicts with ts_code")
    current_status = _string(values["list_status"], "list_status")
    if current_status != expected_status:
        raise ValueError("stock_basic list_status conflicts with the query")
    listed_on = _date(_string(values["list_date"], "list_date"), "list_date")
    delisted_on = _optional_date(values["delist_date"], "delist_date")
    if delisted_on is not None and delisted_on < listed_on:
        raise ValueError("delist_date must not be before list_date")
    if current_status == "D" and delisted_on is None:
        raise ValueError("delisted stock requires delist_date")
    return StockListing(
        instrument_id=instrument_id,
        tushare_code=tushare_code,
        symbol=symbol,
        name=_string(values["name"], "name"),
        exchange=exchange,
        current_status=current_status,
        listed_on=listed_on,
        delisted_on=delisted_on,
    )


def _canonical_instrument_id(tushare_code: str) -> str:
    parts = tushare_code.split(".")
    if len(parts) != 2 or not parts[0].isdigit() or len(parts[0]) != 6:
        raise ValueError("ts_code must use Tushare stock format such as 600000.SH")
    suffix_to_mic = {"SH": "XSHG", "SZ": "XSHE"}
    mic = suffix_to_mic.get(parts[1])
    if mic is None:
        raise ValueError("the first Tushare slice supports SH and SZ stocks only")
    return f"{parts[0]}.{mic}"


def _require_unique_rows(table: TushareTable, *, key_fields: tuple[str, ...]) -> None:
    indexes = tuple(table.fields.index(field) for field in key_fields)
    keys = tuple(tuple(row[index] for index in indexes) for row in table.rows)
    if len(keys) != len(set(keys)):
        raise ValueError(f"{table.api_name} response contains duplicate primary keys")


def _validate_trade_calendar(
    table: TushareTable,
    *,
    exchange: str,
    start_date: date,
    end_date: date,
) -> None:
    _require_unique_rows(table, key_fields=("exchange", "cal_date"))
    indexes = {field: table.fields.index(field) for field in table.fields}
    for row in table.rows:
        if _string(row[indexes["exchange"]], "exchange") != exchange:
            raise ValueError("trade_cal exchange conflicts with the query")
        calendar_date = _date(_string(row[indexes["cal_date"]], "cal_date"), "cal_date")
        if not start_date <= calendar_date <= end_date:
            raise ValueError("trade_cal cal_date falls outside the query range")
        if row[indexes["is_open"]] not in (0, 1, "0", "1"):
            raise ValueError("trade_cal is_open must be 0 or 1")
        _optional_date(row[indexes["pretrade_date"]], "pretrade_date")


def _validate_daily(
    table: TushareTable,
    *,
    tushare_code: str,
    start_date: date,
    end_date: date,
) -> None:
    indexes = {field: table.fields.index(field) for field in table.fields}
    price_fields = ("open", "high", "low", "close", "pre_close")
    for row in table.rows:
        if _string(row[indexes["ts_code"]], "ts_code") != tushare_code:
            raise ValueError("daily ts_code conflicts with the query")
        trade_date = _date(_string(row[indexes["trade_date"]], "trade_date"), "trade_date")
        if not start_date <= trade_date <= end_date:
            raise ValueError("daily trade_date falls outside the query range")
        prices = {
            field: _number(row[indexes[field]], field, positive=True) for field in price_fields
        }
        _number(row[indexes["vol"]], "vol", positive=False)
        _number(row[indexes["amount"]], "amount", positive=False)
        if prices["high"] < max(prices["open"], prices["low"], prices["close"]):
            raise ValueError("daily high is below another OHLC price")
        if prices["low"] > min(prices["open"], prices["high"], prices["close"]):
            raise ValueError("daily low is above another OHLC price")


def _listing_payload(listing: StockListing) -> dict[str, object]:
    return {
        "current_status": listing.current_status,
        "delisted_on": listing.delisted_on.isoformat() if listing.delisted_on else None,
        "exchange": listing.exchange,
        "instrument_id": listing.instrument_id,
        "listed_on": listing.listed_on.isoformat(),
        "name": listing.name,
        "symbol": listing.symbol,
        "tushare_code": listing.tushare_code,
    }


def _date_range(start_date: str, end_date: str) -> None:
    start = _date(start_date, "start_date")
    end = _date(end_date, "end_date")
    if start > end:
        raise ValueError("start_date must not be after end_date")


def _date(value: str, field_name: str) -> date:
    if _DATE_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must use YYYYMMDD")
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid date") from exc


def _optional_date(value: object, field_name: str) -> date | None:
    if value in (None, ""):
        return None
    return _date(_string(value, field_name), field_name)


def _string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _integer(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _number(value: object, field_name: str, *, positive: bool) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a JSON number")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} must be finite") from exc
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{field_name} must be finite and {qualifier}")
    return number


def _canonical_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
