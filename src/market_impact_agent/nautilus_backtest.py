# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false, reportUntypedBaseClass=false
# NautilusTrader 1.x exposes Cython extension types without static type information.
# Keep the suppression at this integration boundary; the harness contracts remain strict.

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import nautilus_trader
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FeeModel, FillModel
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import Bar, BarType, QuoteTick
from nautilus_trader.model.enums import AccountType, OmsType, OrderSide, TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import Equity, Instrument
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.trading.strategy import Strategy
from pandas.errors import Pandas4Warning  # pyright: ignore[reportMissingTypeStubs]

from market_impact_agent.backtests import (
    BacktestInputHash,
    BacktestMetric,
    BacktestRequest,
    BacktestResult,
    BacktestRunManifest,
    BacktestRunStatus,
    canonical_backtest_request_hash,
    canonical_backtest_result_hash,
)
from market_impact_agent.domain import Side, require_aware

_ENGINE_NAME = "nautilus_trader"
_ENGINE_VERSION = "1.231.0"
_BRIDGE_NAME = "nautilus-backtest"
_BRIDGE_VERSION = "0.3.0"
_SUPPORTED_DATA_GRANULARITY = "daily_bar.v1"
_SUPPORTED_BOOK_TYPE = "top_of_book"
_SUPPORTED_FILL_MODEL = "next_executable_open_one_tick_slippage.v1"
_SUPPORTED_FEE_MODEL = "a_share_fixture_fee.v1"
_SUPPORTED_VENUE_RULESET = "xshg_cash_equity_fixture.v1"
_SUPPORTED_STRATEGY = "event-impact-hold.v1"
_TIMESTAMP_UTCNOW_WARNING = (
    r"^Timestamp\.utcnow is deprecated and will be removed in a future version\. "
    r"Use Timestamp\.now\('UTC'\) instead\.$"
)


@dataclass(frozen=True, slots=True)
class AShareDailyBar:
    session_open_at: datetime
    session_close_at: datetime
    previous_close: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    open_bid_quantity: int
    open_ask_quantity: int
    suspended: bool


@dataclass(frozen=True, slots=True)
class AShareDailyBarSnapshot:
    snapshot_id: str
    instrument_id: str
    currency: str
    price_precision: int
    price_increment: Decimal
    lot_size: int
    price_limit_ratio: Decimal
    commission_rate: Decimal
    minimum_commission: Decimal
    sell_stamp_tax_rate: Decimal
    slippage_ticks: int
    bars: tuple[AShareDailyBar, ...]
    content_hash: str


@dataclass(frozen=True, slots=True)
class NautilusReplayContract:
    data_adapter_name: str
    data_adapter_version: str
    input_hashes: tuple[BacktestInputHash, ...]
    data_granularity: str
    book_type: str
    fill_model: str
    fee_model: str
    venue_ruleset: str
    exact_as_of: datetime | None
    exact_start_at: datetime | None
    exact_end_at: datetime | None
    target_selection_ref: str | None


_SYNTHETIC_REPLAY_CONTRACT = NautilusReplayContract(
    data_adapter_name="synthetic-a-share-daily-bar-fixture",
    data_adapter_version="1.0.0",
    input_hashes=(),
    data_granularity=_SUPPORTED_DATA_GRANULARITY,
    book_type=_SUPPORTED_BOOK_TYPE,
    fill_model=_SUPPORTED_FILL_MODEL,
    fee_model=_SUPPORTED_FEE_MODEL,
    venue_ruleset=_SUPPORTED_VENUE_RULESET,
    exact_as_of=None,
    exact_start_at=None,
    exact_end_at=None,
    target_selection_ref=None,
)


class AShareFixtureFeeModel(FeeModel):
    def __init__(
        self,
        *,
        commission_rate: Decimal,
        minimum_commission: Decimal,
        sell_stamp_tax_rate: Decimal,
    ) -> None:
        self._commission_rate = commission_rate
        self._minimum_commission = minimum_commission
        self._sell_stamp_tax_rate = sell_stamp_tax_rate

    def get_commission(
        self,
        order: Order,
        fill_qty: Quantity,
        fill_px: Price,
        instrument: Instrument,
    ) -> Money:
        notional = abs(instrument.notional_value(fill_qty, fill_px).as_decimal())
        commission = max(self._minimum_commission, notional * self._commission_rate)
        if order.side is OrderSide.SELL:
            commission += notional * self._sell_stamp_tax_rate
        rounded = commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return Money.from_decimal(rounded, instrument.quote_currency)


class _EventImpactHoldStrategy(Strategy):
    def __init__(
        self,
        *,
        instrument_id: InstrumentId,
        trade_quantity: Decimal,
        horizon_sessions: int,
        entry_side: OrderSide,
    ) -> None:
        super().__init__()
        self._instrument_id = instrument_id
        self._trade_quantity = trade_quantity
        self._horizon_sessions = horizon_sessions
        self._entry_side = entry_side
        self._exit_side = OrderSide.SELL
        self._instrument: Instrument | None = None
        self._session_count = 0
        self._entry_session: int | None = None
        self._exit_session: int | None = None
        self._entry_submitted = False
        self._exit_submitted = False
        self.fills: list[OrderFilled] = []

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self._instrument_id)
        if self._instrument is None:
            self.stop()
            return
        self.subscribe_quote_ticks(self._instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._session_count += 1
        if self._instrument is None:
            return

        if self._entry_session is None:
            if not self._entry_submitted and tick.ask_size.as_decimal() > 0:
                self._entry_submitted = True
                self._submit_market_order(self._entry_side)
            return

        holding_sessions = self._session_count - self._entry_session
        if (
            not self._exit_submitted
            and holding_sessions >= self._horizon_sessions
            and tick.bid_size.as_decimal() > 0
        ):
            self._exit_submitted = True
            self._submit_market_order(self._exit_side)

    def on_order_filled(self, event: OrderFilled) -> None:
        self.fills.append(event)
        if event.order_side is self._entry_side:
            self._entry_session = self._session_count
        elif event.order_side is self._exit_side:
            self._exit_session = self._session_count

    @property
    def entry_delay_sessions(self) -> int:
        if self._entry_session is None:
            raise ValueError("strategy has no filled entry")
        return self._entry_session - 1

    @property
    def holding_sessions(self) -> int:
        if self._entry_session is None or self._exit_session is None:
            raise ValueError("strategy does not have a closed holding period")
        return self._exit_session - self._entry_session

    def _submit_market_order(self, side: OrderSide) -> None:
        if self._instrument is None:
            return
        order = self.order_factory.market(
            instrument_id=self._instrument_id,
            order_side=side,
            quantity=self._instrument.make_qty(self._trade_quantity),
            time_in_force=TimeInForce.IOC,
        )
        self.submit_order(order)


class NautilusBacktestBridge:
    def __init__(self, snapshot_path: Path) -> None:
        self._snapshot = load_a_share_daily_bar_snapshot(snapshot_path)
        self._contract = replace(
            _SYNTHETIC_REPLAY_CONTRACT,
            input_hashes=(BacktestInputHash("snapshot", self._snapshot.content_hash),),
        )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: AShareDailyBarSnapshot,
        contract: NautilusReplayContract,
    ) -> NautilusBacktestBridge:
        instance = cls.__new__(cls)
        instance._snapshot = snapshot
        instance._contract = contract
        return instance

    def run(self, request: BacktestRequest) -> BacktestResult:
        manifest = self._manifest(request)
        artifact_refs = (self._snapshot_artifact_ref(),)
        try:
            self._validate_request(request)
            metrics = self._run_engine(request)
            return _result(
                manifest=manifest,
                status=BacktestRunStatus.COMPLETED,
                metrics=metrics,
                artifact_refs=artifact_refs,
                failure_reasons=(),
            )
        except Exception as exc:
            return _result(
                manifest=manifest,
                status=BacktestRunStatus.FAILED,
                metrics=(),
                artifact_refs=artifact_refs,
                failure_reasons=(f"{type(exc).__name__}: {exc}",),
            )

    def _manifest(self, request: BacktestRequest) -> BacktestRunManifest:
        return BacktestRunManifest(
            run_id=f"nautilus-{uuid4()}",
            request=request,
            request_hash=canonical_backtest_request_hash(request),
            engine_name=_ENGINE_NAME,
            engine_version=nautilus_trader.__version__,
            bridge_name=_BRIDGE_NAME,
            bridge_version=_BRIDGE_VERSION,
            data_adapter_name=self._contract.data_adapter_name,
            data_adapter_version=self._contract.data_adapter_version,
            input_hashes=self._contract.input_hashes,
            engine_config_hash=self._engine_config_hash(request),
            executed_at=datetime.now(UTC),
        )

    def _validate_request(self, request: BacktestRequest) -> None:
        expected = {
            "market": (request.market, "CN"),
            "data_snapshot_id": (request.data_snapshot_id, self._snapshot.snapshot_id),
            "strategy_ref": (request.strategy_ref, _SUPPORTED_STRATEGY),
            "data_granularity": (
                request.simulation.data_granularity,
                self._contract.data_granularity,
            ),
            "book_type": (request.simulation.book_type, self._contract.book_type),
            "fill_model": (request.simulation.fill_model, self._contract.fill_model),
            "fee_model": (request.simulation.fee_model, self._contract.fee_model),
            "venue_ruleset": (
                request.simulation.venue_ruleset,
                self._contract.venue_ruleset,
            ),
            "base_currency": (request.simulation.base_currency, self._snapshot.currency),
        }
        for name, (actual, wanted) in expected.items():
            if actual != wanted:
                raise ValueError(f"unsupported {name}: expected {wanted!r}, got {actual!r}")

        if nautilus_trader.__version__ != _ENGINE_VERSION:
            raise ValueError(
                f"unsupported NautilusTrader version: expected {_ENGINE_VERSION}, "
                f"got {nautilus_trader.__version__}"
            )
        if request.instrument_ids != (self._snapshot.instrument_id,):
            raise ValueError("the first replay supports exactly the snapshot instrument")
        for name, actual, wanted in (
            ("as_of", request.as_of, self._contract.exact_as_of),
            ("start_at", request.start_at, self._contract.exact_start_at),
            ("end_at", request.end_at, self._contract.exact_end_at),
        ):
            if wanted is not None and actual != wanted:
                raise ValueError(f"request {name} does not match the validated data window")
        if (
            self._contract.target_selection_ref is not None
            and request.target_selection_ref != self._contract.target_selection_ref
        ):
            raise ValueError("request target_selection_ref does not match the integration fixture")
        _entry_order_side(request.signal.side, self._contract.venue_ruleset)
        bars = self._selected_bars(request)
        required_sessions = max(request.horizons_sessions) + 1
        executable_buys = [
            bar
            for bar in bars
            if bar.open_ask_quantity > 0 and bar.session_open_at < request.signal.expires_at
        ]
        if not executable_buys:
            raise ValueError("replay window has no executable buy entry before signal expiry")
        entry_index = bars.index(executable_buys[0])
        if len(bars) - entry_index <= required_sessions - 1:
            raise ValueError("replay window does not cover the requested holding horizon")

    def _run_engine(self, request: BacktestRequest) -> tuple[BacktestMetric, ...]:
        multiple_horizons = len(request.horizons_sessions) > 1
        metrics: list[BacktestMetric] = []
        for horizon_sessions in request.horizons_sessions:
            horizon_metrics = self._run_horizon(request, horizon_sessions)
            metrics.extend(
                BacktestMetric(
                    name=(
                        f"horizon_{horizon_sessions}.{metric.name}"
                        if multiple_horizons
                        else metric.name
                    ),
                    value=metric.value,
                    unit=metric.unit,
                )
                for metric in horizon_metrics
            )
        return tuple(metrics)

    def _run_horizon(
        self,
        request: BacktestRequest,
        horizon_sessions: int,
    ) -> tuple[BacktestMetric, ...]:
        snapshot = self._snapshot
        bars = self._selected_bars(request)
        currency = Currency.from_str(snapshot.currency)
        instrument_id = InstrumentId.from_str(request.signal.instrument_id)
        venue = instrument_id.venue
        instrument = Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(snapshot.instrument_id.split(".", 1)[0]),
            currency=currency,
            price_precision=snapshot.price_precision,
            price_increment=Price.from_str(str(snapshot.price_increment)),
            lot_size=Quantity.from_int(snapshot.lot_size),
            min_quantity=Quantity.from_int(snapshot.lot_size),
            ts_event=0,
            ts_init=0,
        )
        bar_type = BarType.from_str(f"{instrument_id}-1-DAY-LAST-EXTERNAL")
        strategy = _EventImpactHoldStrategy(
            instrument_id=instrument_id,
            trade_quantity=Decimal(snapshot.lot_size),
            horizon_sessions=horizon_sessions,
            entry_side=_entry_order_side(request.signal.side, self._contract.venue_ruleset),
        )
        engine = BacktestEngine(
            config=BacktestEngineConfig(
                logging=LoggingConfig(log_level="ERROR"),
                run_analysis=False,
            )
        )
        try:
            engine.add_venue(
                venue=venue,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                starting_balances=[Money.from_decimal(request.simulation.starting_cash, currency)],
                base_currency=currency,
                fill_model=FillModel(
                    prob_fill_on_limit=1.0,
                    prob_slippage=1.0 if snapshot.slippage_ticks else 0.0,
                    random_seed=request.simulation.random_seed,
                ),
                fee_model=AShareFixtureFeeModel(
                    commission_rate=snapshot.commission_rate,
                    minimum_commission=snapshot.minimum_commission,
                    sell_stamp_tax_rate=snapshot.sell_stamp_tax_rate,
                ),
                bar_execution=True,
                allow_cash_borrowing=False,
            )
            engine.add_instrument(instrument)
            engine.add_data([_quote_tick(instrument_id, bar) for bar in bars])
            engine.add_data([_nautilus_bar(bar_type, bar) for bar in bars])
            engine.add_strategy(strategy)
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=_TIMESTAMP_UTCNOW_WARNING,
                        category=Pandas4Warning,
                    )
                    engine.run()
            except Exception:
                engine.end()
                raise
        finally:
            engine.dispose()

        return _metrics_from_strategy(strategy)

    def _selected_bars(self, request: BacktestRequest) -> tuple[AShareDailyBar, ...]:
        return tuple(
            bar
            for bar in self._snapshot.bars
            if bar.session_open_at >= request.start_at and bar.session_close_at <= request.end_at
        )

    def _engine_config_hash(self, request: BacktestRequest) -> str:
        return _canonical_sha256(
            {
                "allow_cash_borrowing": False,
                "data_adapter_name": self._contract.data_adapter_name,
                "data_adapter_version": self._contract.data_adapter_version,
                "bar_execution": True,
                "commission_rate": str(self._snapshot.commission_rate),
                "fill_model": request.simulation.fill_model,
                "fee_model": request.simulation.fee_model,
                "holding_horizons": list(request.horizons_sessions),
                "instrument_id": request.signal.instrument_id,
                "lot_size": self._snapshot.lot_size,
                "minimum_commission": str(self._snapshot.minimum_commission),
                "price_limit_ratio": str(self._snapshot.price_limit_ratio),
                "random_seed": request.simulation.random_seed,
                "run_analysis": False,
                "sell_stamp_tax_rate": str(self._snapshot.sell_stamp_tax_rate),
                "slippage_ticks": self._snapshot.slippage_ticks,
                "snapshot_hash": self._snapshot.content_hash,
                "strategy_side": request.signal.side.value,
                "venue_ruleset": request.simulation.venue_ruleset,
            }
        )

    def _snapshot_artifact_ref(self) -> str:
        return f"snapshot://{self._snapshot.snapshot_id}#sha256={self._snapshot.content_hash}"


def load_a_share_daily_bar_snapshot(path: Path) -> AShareDailyBarSnapshot:
    raw_bytes = path.read_bytes()
    payload = cast(dict[str, Any], json.loads(raw_bytes))
    bars = tuple(_parse_bar(cast(dict[str, Any], item)) for item in payload["bars"])
    snapshot = AShareDailyBarSnapshot(
        snapshot_id=str(payload["snapshot_id"]),
        instrument_id=str(payload["instrument_id"]),
        currency=str(payload["currency"]),
        price_precision=_integer(payload["price_precision"], "price_precision", positive=False),
        price_increment=Decimal(str(payload["price_increment"])),
        lot_size=_integer(payload["lot_size"], "lot_size"),
        price_limit_ratio=Decimal(str(payload["price_limit_ratio"])),
        commission_rate=Decimal(str(payload["commission_rate"])),
        minimum_commission=Decimal(str(payload["minimum_commission"])),
        sell_stamp_tax_rate=Decimal(str(payload["sell_stamp_tax_rate"])),
        slippage_ticks=_integer(payload["slippage_ticks"], "slippage_ticks", positive=False),
        bars=bars,
        content_hash=sha256(raw_bytes).hexdigest(),
    )
    validate_a_share_daily_bar_snapshot(snapshot)
    return snapshot


def _parse_bar(payload: dict[str, Any]) -> AShareDailyBar:
    return AShareDailyBar(
        session_open_at=_timestamp(payload["session_open_at"], "session_open_at"),
        session_close_at=_timestamp(payload["session_close_at"], "session_close_at"),
        previous_close=Decimal(str(payload["previous_close"])),
        open=Decimal(str(payload["open"])),
        high=Decimal(str(payload["high"])),
        low=Decimal(str(payload["low"])),
        close=Decimal(str(payload["close"])),
        volume=_integer(payload["volume"], "volume", positive=False),
        open_bid_quantity=_integer(
            payload["open_bid_quantity"], "open_bid_quantity", positive=False
        ),
        open_ask_quantity=_integer(
            payload["open_ask_quantity"], "open_ask_quantity", positive=False
        ),
        suspended=_boolean(payload["suspended"], "suspended"),
    )


def validate_a_share_daily_bar_snapshot(snapshot: AShareDailyBarSnapshot) -> None:
    if not snapshot.snapshot_id or not snapshot.instrument_id or not snapshot.currency:
        raise ValueError("snapshot identifiers and currency must not be empty")
    if not snapshot.bars:
        raise ValueError("snapshot bars must not be empty")
    if snapshot.price_increment <= 0 or snapshot.price_limit_ratio <= 0:
        raise ValueError("price increment and limit ratio must be positive")
    if snapshot.commission_rate < 0 or snapshot.minimum_commission < 0:
        raise ValueError("commission assumptions must not be negative")
    if snapshot.sell_stamp_tax_rate < 0:
        raise ValueError("sell stamp tax assumption must not be negative")
    if snapshot.slippage_ticks not in (0, 1):
        raise ValueError("the accepted replay contracts support zero or one slippage tick")

    prior_close: Decimal | None = None
    prior_close_at: datetime | None = None
    for bar in snapshot.bars:
        if bar.session_close_at <= bar.session_open_at:
            raise ValueError("session close must be after session open")
        if prior_close_at is not None and bar.session_open_at <= prior_close_at:
            raise ValueError("snapshot sessions must be strictly ascending and disjoint")
        if prior_close is not None and bar.previous_close != prior_close:
            raise ValueError("each previous_close must equal the prior session close")
        _validate_bar(snapshot, bar)
        prior_close = bar.close
        prior_close_at = bar.session_close_at


def _validate_bar(snapshot: AShareDailyBarSnapshot, bar: AShareDailyBar) -> None:
    prices = (bar.previous_close, bar.open, bar.high, bar.low, bar.close)
    if any(not price.is_finite() or price <= 0 for price in prices):
        raise ValueError("bar prices must be finite and positive")
    if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
        raise ValueError("bar OHLC values are inconsistent")
    if any(not _aligned(price, snapshot.price_increment) for price in prices):
        raise ValueError("bar prices must align to the price increment")

    lower = _limit_price(
        bar.previous_close * (Decimal(1) - snapshot.price_limit_ratio),
        snapshot.price_increment,
    )
    upper = _limit_price(
        bar.previous_close * (Decimal(1) + snapshot.price_limit_ratio),
        snapshot.price_increment,
    )
    if bar.low < lower or bar.high > upper:
        raise ValueError("bar prices breach the configured daily price limit")

    if bar.suspended:
        if (
            bar.volume != 0
            or bar.open_bid_quantity != 0
            or bar.open_ask_quantity != 0
            or any(price != bar.previous_close for price in prices[1:])
        ):
            raise ValueError("suspended sessions must be flat with no volume or liquidity")
        return

    if bar.volume == 0:
        if bar.open_bid_quantity != 0 or bar.open_ask_quantity != 0:
            raise ValueError("zero-volume sessions cannot have modeled opening liquidity")
        return

    for quantity in (bar.open_bid_quantity, bar.open_ask_quantity):
        if quantity % snapshot.lot_size != 0:
            raise ValueError("opening liquidity must align to the lot size")
    if bar.open_ask_quantity == 0 and bar.open != upper:
        raise ValueError("zero opening ask liquidity is only valid at the upper limit")
    if bar.open_bid_quantity == 0 and bar.open != lower:
        raise ValueError("zero opening bid liquidity is only valid at the lower limit")


def _quote_tick(instrument_id: InstrumentId, bar: AShareDailyBar) -> QuoteTick:
    timestamp_ns = _nanoseconds(bar.session_open_at)
    return QuoteTick(
        instrument_id=instrument_id,
        bid_price=Price.from_str(str(bar.open)),
        ask_price=Price.from_str(str(bar.open)),
        bid_size=Quantity.from_int(bar.open_bid_quantity),
        ask_size=Quantity.from_int(bar.open_ask_quantity),
        ts_event=timestamp_ns,
        ts_init=timestamp_ns,
    )


def _nautilus_bar(bar_type: BarType, bar: AShareDailyBar) -> Bar:
    timestamp_ns = _nanoseconds(bar.session_close_at)
    return Bar(
        bar_type=bar_type,
        open=Price.from_str(str(bar.open)),
        high=Price.from_str(str(bar.high)),
        low=Price.from_str(str(bar.low)),
        close=Price.from_str(str(bar.close)),
        volume=Quantity.from_int(bar.volume),
        ts_event=timestamp_ns,
        ts_init=timestamp_ns,
    )


def _metrics_from_strategy(
    strategy: _EventImpactHoldStrategy,
) -> tuple[BacktestMetric, ...]:
    fills = strategy.fills
    if len(fills) != 2:
        raise ValueError(f"expected exactly two fills, got {len(fills)}")
    entry, exit_fill = fills
    if entry.order_side is not OrderSide.BUY or exit_fill.order_side is not OrderSide.SELL:
        raise ValueError("expected one buy fill followed by one sell fill")
    if entry.last_qty != exit_fill.last_qty:
        raise ValueError("entry and exit fill quantities must match")

    entry_price = entry.last_px.as_decimal()
    exit_price = exit_fill.last_px.as_decimal()
    quantity = entry.last_qty.as_decimal()
    commission = entry.commission.as_decimal() + exit_fill.commission.as_decimal()
    gross_pnl = (exit_price - entry_price) * quantity
    net_pnl = gross_pnl - commission
    entry_notional = entry_price * quantity
    return (
        BacktestMetric("commission", commission, "CNY"),
        BacktestMetric("entry_delay_sessions", Decimal(strategy.entry_delay_sessions), "sessions"),
        BacktestMetric("entry_price", entry_price, "CNY/share"),
        BacktestMetric("exit_price", exit_price, "CNY/share"),
        BacktestMetric("gross_pnl", gross_pnl, "CNY"),
        BacktestMetric("gross_return", (exit_price - entry_price) / entry_price, "ratio"),
        BacktestMetric("holding_sessions", Decimal(strategy.holding_sessions), "sessions"),
        BacktestMetric("net_pnl", net_pnl, "CNY"),
        BacktestMetric("net_return", net_pnl / entry_notional, "ratio"),
        BacktestMetric("order_count", Decimal(2), "orders"),
        BacktestMetric("quantity", quantity, "shares"),
    )


def _result(
    *,
    manifest: BacktestRunManifest,
    status: BacktestRunStatus,
    metrics: tuple[BacktestMetric, ...],
    artifact_refs: tuple[str, ...],
    failure_reasons: tuple[str, ...],
) -> BacktestResult:
    result_hash = canonical_backtest_result_hash(
        manifest=manifest,
        status=status,
        metrics=metrics,
        artifact_refs=artifact_refs,
        failure_reasons=failure_reasons,
    )
    return BacktestResult(
        manifest=manifest,
        status=status,
        result_hash=result_hash,
        metrics=metrics,
        artifact_refs=artifact_refs,
        failure_reasons=failure_reasons,
    )


def _timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string")
    parsed = datetime.fromisoformat(value)
    require_aware(parsed, field_name)
    return parsed


def _integer(value: object, field_name: str, *, positive: bool = True) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    if (positive and value <= 0) or (not positive and value < 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{field_name} must be {qualifier}")
    return value


def _boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _aligned(value: Decimal, increment: Decimal) -> bool:
    return value % increment == 0


def _limit_price(value: Decimal, increment: Decimal) -> Decimal:
    return (value / increment).quantize(Decimal(1), rounding=ROUND_HALF_UP) * increment


def _nanoseconds(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1_000_000_000)


def _canonical_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _entry_order_side(signal_side: Side, venue_ruleset: str) -> OrderSide:
    if signal_side is Side.BUY:
        return OrderSide.BUY
    raise ValueError(f"{venue_ruleset} is long-only and does not support SELL signals")
