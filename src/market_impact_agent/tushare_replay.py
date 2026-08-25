from __future__ import annotations

import json
import os
import stat
from datetime import UTC, date, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from importlib import import_module
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from market_impact_agent.backtests import BacktestInputHash, BacktestRequest, BacktestResult
from market_impact_agent.nautilus_backtest import (
    AShareDailyBar,
    AShareDailyBarSnapshot,
    NautilusBacktestBridge,
    NautilusReplayContract,
    validate_a_share_daily_bar_snapshot,
)
from market_impact_agent.tushare_bundle import validate_tushare_data_bundle

TUSHARE_MODELED_OPEN_ADAPTER_NAME = "tushare-xshg-modeled-open"
TUSHARE_MODELED_OPEN_ADAPTER_VERSION = "1.0.0"
TUSHARE_MODELED_OPEN_DATA_GRANULARITY = "tushare_unadjusted_daily.v1"
TUSHARE_MODELED_OPEN_BOOK_TYPE = "modeled_open_one_lot.v1"
TUSHARE_MODELED_OPEN_FILL_MODEL = "modeled_open_one_lot_no_slippage.v1"
TUSHARE_MODELED_OPEN_FEE_MODEL = "xshg_2019_fee_assumption.v1"
TUSHARE_MODELED_OPEN_VENUE_RULESET = "xshg_600028_main_board_10pct.v1"
TUSHARE_TARGET_SELECTION_REF = "manual-integration-fixture:abqaiq-600028.v1"

_SUPPORTED_INSTRUMENT = "600028.XSHG"
_SUPPORTED_TUSHARE_CODE = "600028.SH"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_TICK = Decimal("0.01")
_LOT_SIZE = 100
_LIMIT_RATIO = Decimal("0.10")


def run_validated_tushare_replay(
    request: BacktestRequest,
    bundle_path: Path,
) -> BacktestResult:
    snapshot, contract = load_validated_tushare_modeled_open(bundle_path)
    return NautilusBacktestBridge.from_snapshot(snapshot, contract).run(request)


def load_validated_tushare_modeled_open(
    bundle_path: Path,
) -> tuple[AShareDailyBarSnapshot, NautilusReplayContract]:
    """Validate a private bundle before consuming its daily/calendar Parquet in memory."""
    bundle = validate_tushare_data_bundle(bundle_path)
    manifest = bundle.manifest
    request_fields = _mapping(manifest, "request")
    if bundle.instrument_id != _SUPPORTED_INSTRUMENT:
        raise ValueError(f"unsupported replay target: expected {_SUPPORTED_INSTRUMENT}")
    if _string(request_fields, "tushare_code") != _SUPPORTED_TUSHARE_CODE:
        raise ValueError(f"unsupported Tushare target: expected {_SUPPORTED_TUSHARE_CODE}")
    if _string(request_fields, "exchange") != "SSE":
        raise ValueError("the modeled-open adapter supports only the XSHG/SSE fixture")

    tables = _mapping(manifest, "tables")
    daily_manifest = _mapping(tables, "daily")
    calendar_manifest = _mapping(tables, "trade_calendar")
    daily_bytes = _read_manifest_bound_file(
        bundle.path / _string(daily_manifest, "file"),
        _string(daily_manifest, "sha256"),
    )
    calendar_bytes = _read_manifest_bound_file(
        bundle.path / _string(calendar_manifest, "file"),
        _string(calendar_manifest, "sha256"),
    )
    pa = cast(Any, import_module("pyarrow"))
    pq = cast(Any, import_module("pyarrow.parquet"))
    daily_rows = cast(
        list[dict[str, object]], pq.read_table(pa.BufferReader(daily_bytes)).to_pylist()
    )
    calendar_rows = cast(
        list[dict[str, object]], pq.read_table(pa.BufferReader(calendar_bytes)).to_pylist()
    )
    open_dates = {_date(row, "cal_date") for row in calendar_rows if _boolean(row, "is_open")}
    if {_date(row, "trade_date") for row in daily_rows} != open_dates:
        raise ValueError("validated calendar/daily open-session identities diverged during load")

    bars = _modeled_bars(daily_rows)
    input_hashes = (
        BacktestInputHash("bundle", bundle.bundle_hash),
        BacktestInputHash("daily.parquet", _string(daily_manifest, "sha256")),
        BacktestInputHash("trade_calendar.parquet", _string(calendar_manifest, "sha256")),
    )
    snapshot = AShareDailyBarSnapshot(
        snapshot_id=bundle.data_snapshot_id,
        instrument_id=bundle.instrument_id,
        currency="CNY",
        price_precision=2,
        price_increment=_TICK,
        lot_size=_LOT_SIZE,
        price_limit_ratio=_LIMIT_RATIO,
        commission_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5.00"),
        sell_stamp_tax_rate=Decimal("0.001"),
        slippage_ticks=0,
        bars=bars,
        content_hash=_adapter_output_hash(bundle.bundle_hash, bars),
    )
    validate_a_share_daily_bar_snapshot(snapshot)
    as_of_date = date.fromisoformat(_string(request_fields, "as_of_date"))
    contract = NautilusReplayContract(
        data_adapter_name=TUSHARE_MODELED_OPEN_ADAPTER_NAME,
        data_adapter_version=TUSHARE_MODELED_OPEN_ADAPTER_VERSION,
        input_hashes=input_hashes,
        data_granularity=TUSHARE_MODELED_OPEN_DATA_GRANULARITY,
        book_type=TUSHARE_MODELED_OPEN_BOOK_TYPE,
        fill_model=TUSHARE_MODELED_OPEN_FILL_MODEL,
        fee_model=TUSHARE_MODELED_OPEN_FEE_MODEL,
        venue_ruleset=TUSHARE_MODELED_OPEN_VENUE_RULESET,
        exact_as_of=datetime.combine(as_of_date, time(23, 59, 59), tzinfo=UTC),
        exact_start_at=bars[0].session_open_at,
        exact_end_at=bars[-1].session_close_at,
        target_selection_ref=TUSHARE_TARGET_SELECTION_REF,
    )
    return snapshot, contract


def _modeled_bars(rows: list[dict[str, object]]) -> tuple[AShareDailyBar, ...]:
    bars: list[AShareDailyBar] = []
    prior_close: Decimal | None = None
    for row in rows:
        trade_date = _date(row, "trade_date")
        raw_prices = tuple(
            _decimal(row, field, positive=True)
            for field in ("pre_close", "open", "high", "low", "close")
        )
        if any(price % _TICK != 0 for price in raw_prices):
            raise ValueError("daily prices must align to the XSHG 0.01 CNY tick")
        previous_close, open_price, high, low, close = (
            price.quantize(_TICK) for price in raw_prices
        )
        volume_hands = _decimal(row, "vol", positive=False)
        _decimal(row, "amount", positive=False)
        if prior_close is not None and previous_close != prior_close:
            raise ValueError(
                "daily pre_close discontinuity is ambiguous with a corporate action or source gap"
            )
        if high < max(open_price, close) or low > min(open_price, close):
            raise ValueError("daily OHLC values are inconsistent")
        lower = _limit_price(previous_close * (Decimal(1) - _LIMIT_RATIO))
        upper = _limit_price(previous_close * (Decimal(1) + _LIMIT_RATIO))
        if low < lower or high > upper:
            raise ValueError("daily OHLC breaches the modeled XSHG 10% price band")
        shares = volume_hands * Decimal(100)
        if shares != shares.to_integral_value():
            raise ValueError("Tushare daily vol cannot be converted exactly from hands to shares")
        share_volume = int(shares)
        positive_volume = share_volume > 0
        bid_quantity = _LOT_SIZE if positive_volume and open_price != lower else 0
        ask_quantity = _LOT_SIZE if positive_volume and open_price != upper else 0
        bars.append(
            AShareDailyBar(
                session_open_at=datetime.combine(trade_date, time(9, 30), tzinfo=_SHANGHAI),
                session_close_at=datetime.combine(trade_date, time(15), tzinfo=_SHANGHAI),
                previous_close=previous_close,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=share_volume,
                open_bid_quantity=bid_quantity,
                open_ask_quantity=ask_quantity,
                suspended=False,
            )
        )
        prior_close = close
    if not bars:
        raise ValueError("modeled-open adapter requires at least one daily row")
    return tuple(bars)


def _adapter_output_hash(bundle_hash: str, bars: tuple[AShareDailyBar, ...]) -> str:
    payload = {
        "adapter_name": TUSHARE_MODELED_OPEN_ADAPTER_NAME,
        "adapter_version": TUSHARE_MODELED_OPEN_ADAPTER_VERSION,
        "bundle_hash": bundle_hash,
        "bars": [
            {
                "ask": bar.open_ask_quantity,
                "bid": bar.open_bid_quantity,
                "close": str(bar.close),
                "high": str(bar.high),
                "low": str(bar.low),
                "open": str(bar.open),
                "previous_close": str(bar.previous_close),
                "session_close_at": bar.session_close_at.isoformat(),
                "session_open_at": bar.session_open_at.isoformat(),
                "volume_shares": bar.volume,
            }
            for bar in bars
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode()).hexdigest()


def _read_manifest_bound_file(path: Path, expected_hash: str) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise ValueError(f"{path.name} must be a real private file") from exc
    with os.fdopen(descriptor, "rb") as handle:
        file_stat = os.fstat(handle.fileno())
        if not stat.S_ISREG(file_stat.st_mode) or stat.S_IMODE(file_stat.st_mode) != 0o600:
            raise ValueError(f"{path.name} must be a real private file")
        content = handle.read()
    if sha256(content).hexdigest() != expected_hash:
        raise ValueError(f"{path.name} hash does not match")
    return content


def _limit_price(value: Decimal) -> Decimal:
    return (value / _TICK).quantize(Decimal(1), rounding=ROUND_HALF_UP) * _TICK


def _mapping(fields: dict[str, object], name: str) -> dict[str, object]:
    value = fields.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _string(fields: dict[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _date(fields: dict[str, object], name: str) -> date:
    value = fields.get(name)
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{name} must be a date")
    return value


def _boolean(fields: dict[str, object], name: str) -> bool:
    value = fields.get(name)
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _decimal(fields: dict[str, object], name: str, *, positive: bool) -> Decimal:
    value = fields.get(name)
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite decimal")
    if (positive and value <= 0) or (not positive and value < 0):
        raise ValueError(f"{name} has an unsupported sign")
    return value
