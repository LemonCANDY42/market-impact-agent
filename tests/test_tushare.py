import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.providers import Capability, ProviderManifest, TrustTier
from market_impact_agent.tushare import (
    TUSHARE_HTTP_ENDPOINT,
    TushareApiError,
    TushareHttpAdapter,
    build_pre_event_universe,
)

NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
LATER = datetime(2026, 8, 25, 12, 0, 1, tzinfo=UTC)
TOKEN = "secret-token-for-test"
ROOT = Path(__file__).parents[1]


class FakeTransport:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.requests: list[dict[str, object]] = []

    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
        assert endpoint == TUSHARE_HTTP_ENDPOINT
        assert timeout_seconds == 10.0
        self.requests.append(cast(dict[str, object], json.loads(body)))
        return json.dumps(self._responses.pop(0)).encode()


def response(fields: tuple[str, ...], items: list[list[object]]) -> dict[str, object]:
    return {"code": 0, "msg": None, "data": {"fields": list(fields), "items": items}}


def stock_row(
    ts_code: str,
    *,
    exchange: str,
    status: str,
    list_date: str,
    delist_date: str = "",
) -> list[object]:
    symbol = ts_code.split(".", 1)[0]
    return [ts_code, symbol, f"Stock {symbol}", exchange, status, list_date, delist_date]


def test_http_adapter_is_disabled_and_unverified_until_live_acceptance() -> None:
    adapter = TushareHttpAdapter(TOKEN, transport=FakeTransport([]))

    assert adapter.manifest.provider_id == "tushare-http"
    assert adapter.manifest.declared_capabilities == frozenset({Capability.MARKET_DATA})
    assert adapter.manifest.verified_capabilities == frozenset()
    assert not adapter.manifest.enabled
    assert adapter.manifest.trust_tier is TrustTier.UNVERIFIED
    assert TOKEN not in repr(adapter)

    payload = cast(
        dict[str, object],
        json.loads((ROOT / "examples/providers/tushare-http-unverified.json").read_bytes()),
    )
    assert ProviderManifest.from_dict(payload) == adapter.manifest


def test_adapter_rejects_unsafe_configuration() -> None:
    with pytest.raises(ValueError):
        TushareHttpAdapter("")
    with pytest.raises(ValueError):
        TushareHttpAdapter(TOKEN, endpoint="http://api.tushare.pro")
    with pytest.raises(ValueError, match="official HTTPS endpoint"):
        TushareHttpAdapter(TOKEN, endpoint="https://example.invalid/tushare")
    with pytest.raises(ValueError):
        TushareHttpAdapter(TOKEN, timeout_seconds=0.0)


def test_trade_calendar_uses_the_documented_http_contract() -> None:
    fields = ("exchange", "cal_date", "is_open", "pretrade_date")
    transport = FakeTransport([response(fields, [["SSE", "20260825", "1", "20260824"]])])
    adapter = TushareHttpAdapter(TOKEN, transport=transport, clock=lambda: NOW)

    table = adapter.fetch_trade_calendar(exchange="SSE", start_date="20260825", end_date="20260825")

    assert table.rows == (("SSE", "20260825", "1", "20260824"),)
    assert transport.requests == [
        {
            "api_name": "trade_cal",
            "token": TOKEN,
            "params": {
                "end_date": "20260825",
                "exchange": "SSE",
                "start_date": "20260825",
            },
            "fields": "exchange,cal_date,is_open,pretrade_date",
        }
    ]


def test_response_field_and_row_order_do_not_change_table_identity() -> None:
    requested_fields = ("exchange", "cal_date", "is_open", "pretrade_date")
    reordered_fields = ("cal_date", "exchange", "pretrade_date", "is_open")
    first = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport(
            [
                response(
                    requested_fields,
                    [
                        ["SSE", "20260826", 1, "20260825"],
                        ["SSE", "20260825", 1, "20260822"],
                    ],
                )
            ]
        ),
        clock=lambda: NOW,
    ).fetch_trade_calendar(exchange="SSE", start_date="20260825", end_date="20260826")
    second = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport(
            [
                response(
                    reordered_fields,
                    [
                        ["20260825", "SSE", "20260822", 1],
                        ["20260826", "SSE", "20260825", 1],
                    ],
                )
            ]
        ),
        clock=lambda: NOW,
    ).fetch_trade_calendar(exchange="SSE", start_date="20260825", end_date="20260826")

    assert first.fields == requested_fields
    assert first.rows == second.rows
    assert first.content_hash == second.content_hash


def test_daily_uses_the_documented_unadjusted_contract() -> None:
    fields = (
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
    row: list[object] = [
        "600028.SH",
        "20260825",
        10.0,
        10.2,
        9.9,
        10.1,
        10.0,
        1000.0,
        10000.0,
    ]
    transport = FakeTransport([response(fields, [row])])
    adapter = TushareHttpAdapter(TOKEN, transport=transport, clock=lambda: NOW)

    table = adapter.fetch_daily(
        tushare_code="600028.SH",
        start_date="20260825",
        end_date="20260825",
    )

    assert table.rows == (tuple(row),)
    assert transport.requests == [
        {
            "api_name": "daily",
            "token": TOKEN,
            "params": {
                "end_date": "20260825",
                "start_date": "20260825",
                "ts_code": "600028.SH",
            },
            "fields": "ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
        }
    ]


def test_adjustment_factor_and_stock_limit_use_source_bound_contracts() -> None:
    adj_fields = ("ts_code", "trade_date", "adj_factor")
    limit_fields = ("ts_code", "trade_date", "pre_close", "up_limit", "down_limit")
    transport = FakeTransport(
        [
            response(adj_fields, [["600028.SH", "20260825", 6.75]]),
            response(limit_fields, [["600028.SH", "20260825", 10.0, 11.0, 9.0]]),
        ]
    )
    adapter = TushareHttpAdapter(TOKEN, transport=transport, clock=lambda: NOW)

    adj = adapter.fetch_adj_factors(
        tushare_code="600028.SH", start_date="20260825", end_date="20260825"
    )
    limits = adapter.fetch_stock_limits(
        tushare_code="600028.SH", start_date="20260825", end_date="20260825"
    )

    assert adj.api_name == "adj_factor"
    assert limits.api_name == "stk_limit"
    assert [request["api_name"] for request in transport.requests] == [
        "adj_factor",
        "stk_limit",
    ]
    assert transport.requests[0]["fields"] == "ts_code,trade_date,adj_factor"
    assert transport.requests[1]["fields"] == ("ts_code,trade_date,pre_close,up_limit,down_limit")


def test_stock_snapshot_builds_a_fixed_pre_event_universe() -> None:
    fields = (
        "ts_code",
        "symbol",
        "name",
        "exchange",
        "list_status",
        "list_date",
        "delist_date",
    )

    def listing_responses() -> list[dict[str, object]]:
        return [
            response(
                fields,
                [
                    stock_row("600028.SH", exchange="SSE", status="L", list_date="20010808"),
                    stock_row("688999.SH", exchange="SSE", status="L", list_date="20270101"),
                ],
            ),
            response(
                fields,
                [
                    stock_row(
                        "600001.SH",
                        exchange="SSE",
                        status="D",
                        list_date="19901219",
                        delist_date="20200712",
                    )
                ],
            ),
            response(fields, []),
            response(fields, []),
            response(
                fields,
                [stock_row("000001.SZ", exchange="SZSE", status="L", list_date="19910403")],
            ),
            response(fields, []),
            response(fields, []),
            response(fields, []),
        ]

    transport = FakeTransport(listing_responses())
    adapter = TushareHttpAdapter(TOKEN, transport=transport, clock=lambda: NOW)

    snapshot = adapter.fetch_stock_listings()
    universe = build_pre_event_universe(snapshot, as_of_date=date(2026, 8, 25))
    reordered = build_pre_event_universe(
        snapshot,
        as_of_date=date(2026, 8, 25),
        exchanges=("SZSE", "SSE"),
    )

    assert universe.instrument_ids == ("000001.XSHE", "600028.XSHG")
    assert universe.listing_snapshot_hash == snapshot.snapshot_hash
    assert reordered.universe_id == universe.universe_id
    assert reordered.exchanges == ("SSE", "SZSE")
    assert len(snapshot.query_hashes) == 8
    assert [request["params"] for request in transport.requests] == [
        {"exchange": "SSE", "list_status": "L"},
        {"exchange": "SSE", "list_status": "D"},
        {"exchange": "SSE", "list_status": "P"},
        {"exchange": "SSE", "list_status": "G"},
        {"exchange": "SZSE", "list_status": "L"},
        {"exchange": "SZSE", "list_status": "D"},
        {"exchange": "SZSE", "list_status": "P"},
        {"exchange": "SZSE", "list_status": "G"},
    ]
    assert all(request["api_name"] == "stock_basic" for request in transport.requests)
    assert all(request["fields"] == ",".join(fields) for request in transport.requests)

    later_snapshot = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport(listing_responses()),
        clock=lambda: LATER,
    ).fetch_stock_listings()
    later_universe = build_pre_event_universe(later_snapshot, as_of_date=date(2026, 8, 25))

    assert later_snapshot.listings == snapshot.listings
    assert later_snapshot.query_hashes == snapshot.query_hashes
    assert later_snapshot.snapshot_hash != snapshot.snapshot_hash
    assert later_universe.universe_id != universe.universe_id


def test_daily_rejects_duplicate_primary_keys() -> None:
    fields = (
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
    row: list[object] = [
        "600028.SH",
        "20260825",
        10.0,
        10.2,
        9.9,
        10.1,
        10.0,
        1000.0,
        10000.0,
    ]
    adapter = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport([response(fields, [row, row])]),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="duplicate primary keys"):
        adapter.fetch_daily(tushare_code="600028.SH", start_date="20260825", end_date="20260825")


@pytest.mark.parametrize(
    ("row_update", "message"),
    [
        ({"ts_code": "000001.SZ"}, "ts_code conflicts"),
        ({"trade_date": "20260826"}, "outside the query range"),
        ({"high": 9.8}, "high is below"),
        ({"vol": -1.0}, "vol must be finite and non-negative"),
    ],
)
def test_daily_rejects_semantically_invalid_rows(
    row_update: dict[str, object], message: str
) -> None:
    fields = (
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
    values: dict[str, object] = {
        "ts_code": "600028.SH",
        "trade_date": "20260825",
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "pre_close": 10.0,
        "vol": 1000.0,
        "amount": 10000.0,
    }
    values.update(row_update)
    row = [values[field] for field in fields]
    adapter = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport([response(fields, [row])]),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match=message):
        adapter.fetch_daily(tushare_code="600028.SH", start_date="20260825", end_date="20260825")


def test_api_permission_error_is_explicit_and_does_not_leak_token() -> None:
    transport = FakeTransport([{"code": 2002, "msg": f"没有权限 {TOKEN}", "data": None}])
    adapter = TushareHttpAdapter(TOKEN, transport=transport)

    with pytest.raises(TushareApiError, match="2002") as caught:
        adapter.fetch_trade_calendar(exchange="SSE", start_date="20260825", end_date="20260825")

    assert caught.value.code == 2002
    assert TOKEN not in str(caught.value)
    assert len(transport.requests) == 1


def test_malformed_success_response_fails_closed() -> None:
    adapter = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport(
            [
                response(
                    ("exchange", "cal_date"),
                    [["SSE", "20260825"]],
                )
            ]
        ),
    )

    with pytest.raises(ValueError, match="fields do not match"):
        adapter.fetch_trade_calendar(exchange="SSE", start_date="20260825", end_date="20260825")


def test_response_at_documented_row_limit_fails_as_potentially_truncated() -> None:
    fields = ("exchange", "cal_date", "is_open", "pretrade_date")
    row: list[object] = ["SSE", "20260825", 1, "20260822"]
    adapter = TushareHttpAdapter(
        TOKEN,
        transport=FakeTransport([response(fields, [row] * 6000)]),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="row limit"):
        adapter.fetch_trade_calendar(exchange="SSE", start_date="20260825", end_date="20260825")


def test_stock_listing_status_must_match_each_query() -> None:
    fields = (
        "ts_code",
        "symbol",
        "name",
        "exchange",
        "list_status",
        "list_date",
        "delist_date",
    )
    transport = FakeTransport(
        [
            response(
                fields,
                [stock_row("600028.SH", exchange="SSE", status="D", list_date="20010808")],
            ),
            response(fields, []),
            response(fields, []),
            response(fields, []),
            response(fields, []),
            response(fields, []),
            response(fields, []),
            response(fields, []),
        ]
    )
    adapter = TushareHttpAdapter(TOKEN, transport=transport, clock=lambda: NOW)

    with pytest.raises(ValueError, match="list_status conflicts"):
        adapter.fetch_stock_listings()


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [("2026-08-25", "20260825"), ("20260826", "20260825"), ("20260230", "20260301")],
)
def test_date_ranges_are_validated_before_transport(start_date: str, end_date: str) -> None:
    transport = FakeTransport([])
    adapter = TushareHttpAdapter(TOKEN, transport=transport)

    with pytest.raises(ValueError):
        adapter.fetch_trade_calendar(exchange="SSE", start_date=start_date, end_date=end_date)

    assert transport.requests == []
