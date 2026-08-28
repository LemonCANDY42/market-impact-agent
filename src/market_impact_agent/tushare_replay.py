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
TUSHARE_HARDENED_MODELED_OPEN_ADAPTER_VERSION = "2.0.0"
TUSHARE_HARDENED_DATA_GRANULARITY = "tushare_unadjusted_daily_with_source_limits.v2"
TUSHARE_HARDENED_VENUE_RULESET = "xshg_main_board_source_limit.v2"
TUSHARE_HARDENED_TARGET_SELECTION_REF = "registered-a-share-integrated-oil-proxy:600028.v1"
TUSHARE_HARDENED_DEVELOPMENT_TARGET_SELECTION_REF = (
    "opened-development-integrated-upstream:601857.v1"
)

_LEGACY_SUPPORTED_INSTRUMENT = "600028.XSHG"
_LEGACY_SUPPORTED_TUSHARE_CODE = "600028.SH"
_HARDENED_TARGET_SELECTION_REFS = {
    "600028.XSHG": TUSHARE_HARDENED_TARGET_SELECTION_REF,
    "601857.XSHG": TUSHARE_HARDENED_DEVELOPMENT_TARGET_SELECTION_REF,
}
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
    hardened = manifest.get("schema_version") == "market-impact.tushare-data-bundle.v2"
    expected_tushare_code = bundle.instrument_id.removesuffix(".XSHG") + ".SH"
    actual_tushare_code = _string(request_fields, "tushare_code")
    if hardened:
        if bundle.instrument_id not in _HARDENED_TARGET_SELECTION_REFS:
            supported = ", ".join(sorted(_HARDENED_TARGET_SELECTION_REFS))
            raise ValueError(f"unsupported hardened replay target: expected one of {supported}")
        if actual_tushare_code != expected_tushare_code:
            raise ValueError("hardened Tushare target does not match canonical instrument id")
    else:
        if bundle.instrument_id != _LEGACY_SUPPORTED_INSTRUMENT:
            raise ValueError(
                f"unsupported legacy replay target: expected {_LEGACY_SUPPORTED_INSTRUMENT}"
            )
        if actual_tushare_code != _LEGACY_SUPPORTED_TUSHARE_CODE:
            raise ValueError(
                f"unsupported legacy Tushare target: expected {_LEGACY_SUPPORTED_TUSHARE_CODE}"
            )
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
    stock_limit_rows: list[dict[str, object]] | None = None
    adj_factor_rows: list[dict[str, object]] | None = None
    if hardened:
        adj_factor_manifest = _mapping(tables, "adj_factors")
        stock_limit_manifest = _mapping(tables, "stock_limits")
        adj_factor_bytes = _read_manifest_bound_file(
            bundle.path / _string(adj_factor_manifest, "file"),
            _string(adj_factor_manifest, "sha256"),
        )
        stock_limit_bytes = _read_manifest_bound_file(
            bundle.path / _string(stock_limit_manifest, "file"),
            _string(stock_limit_manifest, "sha256"),
        )
        adj_factor_rows = cast(
            list[dict[str, object]],
            pq.read_table(pa.BufferReader(adj_factor_bytes)).to_pylist(),
        )
        stock_limit_rows = cast(
            list[dict[str, object]],
            pq.read_table(pa.BufferReader(stock_limit_bytes)).to_pylist(),
        )
    open_dates = {_date(row, "cal_date") for row in calendar_rows if _boolean(row, "is_open")}
    if {_date(row, "trade_date") for row in daily_rows} != open_dates:
        raise ValueError("validated calendar/daily open-session identities diverged during load")

    as_of_date = date.fromisoformat(_string(request_fields, "as_of_date"))
    replay_start_date = date.fromisoformat(
        _string(request_fields, "evaluation_start_date")
        if hardened
        else _string(request_fields, "start_date")
    )
    bars = _modeled_bars(
        daily_rows,
        stock_limit_rows=stock_limit_rows,
        adj_factor_rows=adj_factor_rows,
        replay_start_date=replay_start_date if hardened else None,
    )
    if hardened:
        bars = tuple(bar for bar in bars if bar.session_open_at.date() >= replay_start_date)
    input_hash_values = {
        "bundle": bundle.bundle_hash,
        "daily.parquet": _string(daily_manifest, "sha256"),
        "trade_calendar.parquet": _string(calendar_manifest, "sha256"),
    }
    if hardened:
        for name in ("adj_factors", "stock_limits"):
            metadata = _mapping(tables, name)
            input_hash_values[f"{name}.parquet"] = _string(metadata, "sha256")
    input_hashes = tuple(
        BacktestInputHash(name, value) for name, value in sorted(input_hash_values.items())
    )
    adapter_version = (
        TUSHARE_HARDENED_MODELED_OPEN_ADAPTER_VERSION
        if hardened
        else TUSHARE_MODELED_OPEN_ADAPTER_VERSION
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
        content_hash=_adapter_output_hash(
            bundle.bundle_hash,
            bars,
            adapter_version=adapter_version,
        ),
    )
    validate_a_share_daily_bar_snapshot(snapshot)
    replay_start_at = next(
        (bar.session_open_at for bar in bars if bar.session_open_at.date() == replay_start_date),
        None,
    )
    if replay_start_at is None:
        raise ValueError("evaluation_start_date is not an exchange-open session")
    contract = NautilusReplayContract(
        data_adapter_name=TUSHARE_MODELED_OPEN_ADAPTER_NAME,
        data_adapter_version=adapter_version,
        input_hashes=input_hashes,
        data_granularity=(
            TUSHARE_HARDENED_DATA_GRANULARITY if hardened else TUSHARE_MODELED_OPEN_DATA_GRANULARITY
        ),
        book_type=TUSHARE_MODELED_OPEN_BOOK_TYPE,
        fill_model=TUSHARE_MODELED_OPEN_FILL_MODEL,
        fee_model=TUSHARE_MODELED_OPEN_FEE_MODEL,
        venue_ruleset=(
            TUSHARE_HARDENED_VENUE_RULESET if hardened else TUSHARE_MODELED_OPEN_VENUE_RULESET
        ),
        exact_as_of=datetime.combine(as_of_date, time(23, 59, 59), tzinfo=UTC),
        exact_start_at=replay_start_at,
        exact_end_at=bars[-1].session_close_at,
        target_selection_ref=(
            _HARDENED_TARGET_SELECTION_REFS[bundle.instrument_id]
            if hardened
            else TUSHARE_TARGET_SELECTION_REF
        ),
    )
    return snapshot, contract


def _modeled_bars(
    rows: list[dict[str, object]],
    *,
    stock_limit_rows: list[dict[str, object]] | None = None,
    adj_factor_rows: list[dict[str, object]] | None = None,
    replay_start_date: date | None = None,
) -> tuple[AShareDailyBar, ...]:
    bars: list[AShareDailyBar] = []
    limits_by_date = (
        {_date(row, "trade_date"): row for row in stock_limit_rows}
        if stock_limit_rows is not None
        else None
    )
    factors_by_date = (
        {
            _date(row, "trade_date"): _decimal(row, "adj_factor", positive=True)
            for row in adj_factor_rows
        }
        if adj_factor_rows is not None
        else None
    )
    if (factors_by_date is None) != (replay_start_date is None):
        raise ValueError("adjustment factors and replay start must be supplied together")
    prior_close: Decimal | None = None
    prior_factor: Decimal | None = None
    for row in rows:
        trade_date = _date(row, "trade_date")
        factor = factors_by_date.get(trade_date) if factors_by_date is not None else None
        if factors_by_date is not None and factor is None:
            raise ValueError("adjustment-factor table is missing a daily session")
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
            factor_changed = (
                factor is not None and prior_factor is not None and factor != prior_factor
            )
            before_evaluation = replay_start_date is not None and trade_date < replay_start_date
            if not (factor_changed and before_evaluation):
                raise ValueError(
                    "daily pre_close discontinuity is ambiguous with a corporate action "
                    "or source gap"
                )
        if high < max(open_price, close) or low > min(open_price, close):
            raise ValueError("daily OHLC values are inconsistent")
        if limits_by_date is None:
            lower = _limit_price(previous_close * (Decimal(1) - _LIMIT_RATIO))
            upper = _limit_price(previous_close * (Decimal(1) + _LIMIT_RATIO))
        else:
            limit_row = limits_by_date.get(trade_date)
            if limit_row is None:
                raise ValueError("source stock-limit table is missing a daily session")
            if _decimal(limit_row, "pre_close", positive=True) != previous_close:
                raise ValueError("source stock-limit previous close does not match daily data")
            lower = _decimal(limit_row, "down_limit", positive=True).quantize(_TICK)
            upper = _decimal(limit_row, "up_limit", positive=True).quantize(_TICK)
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
        prior_factor = factor
    if not bars:
        raise ValueError("modeled-open adapter requires at least one daily row")
    return tuple(bars)


def load_validated_tushare_adjusted_closes(
    bundle_path: Path,
    *,
    visible_at: datetime,
) -> tuple[tuple[datetime, Decimal], ...]:
    """Return cutoff-normalized research closes for sessions ending by the cutoff.

    This is a retrospective source reconstruction, not proof that the factor rows were
    historically received by ``visible_at``. Execution continues to use raw daily prices.
    """
    bundle = validate_tushare_data_bundle(bundle_path)
    manifest = bundle.manifest
    if manifest.get("schema_version") != "market-impact.tushare-data-bundle.v2":
        raise ValueError("adjusted close history requires a hardened Tushare bundle")
    tables = _mapping(manifest, "tables")
    pa = cast(Any, import_module("pyarrow"))
    pq = cast(Any, import_module("pyarrow.parquet"))

    def rows_for(name: str) -> list[dict[str, object]]:
        metadata = _mapping(tables, name)
        content = _read_manifest_bound_file(
            bundle.path / _string(metadata, "file"),
            _string(metadata, "sha256"),
        )
        return cast(
            list[dict[str, object]],
            pq.read_table(pa.BufferReader(content)).to_pylist(),
        )

    daily_rows = rows_for("daily")
    factor_rows = rows_for("adj_factors")
    factors = {
        _date(row, "trade_date"): _decimal(row, "adj_factor", positive=True) for row in factor_rows
    }
    if {_date(row, "trade_date") for row in daily_rows} != set(factors):
        raise ValueError("adjustment-factor history does not match daily sessions")
    visible: list[tuple[datetime, Decimal, Decimal]] = []
    for row in daily_rows:
        trade_date = _date(row, "trade_date")
        close_at = datetime.combine(trade_date, time(15), tzinfo=_SHANGHAI)
        if close_at <= visible_at:
            visible.append(
                (
                    close_at,
                    _decimal(row, "close", positive=True),
                    factors[trade_date],
                )
            )
    if not visible:
        return ()
    cutoff_factor = visible[-1][2]
    return tuple((close_at, close * factor / cutoff_factor) for close_at, close, factor in visible)


def _adapter_output_hash(
    bundle_hash: str,
    bars: tuple[AShareDailyBar, ...],
    *,
    adapter_version: str,
) -> str:
    payload = {
        "adapter_name": TUSHARE_MODELED_OPEN_ADAPTER_NAME,
        "adapter_version": adapter_version,
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
