# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false, reportUntypedBaseClass=false
"""One pinned Nautilus account per historical arm, with a durable input prefix.

The caller owns policy admission. This adapter owns execution, settlement evidence and
projection; it never invokes an Agent, broker or a second accounting engine.
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import nautilus_trader
from nautilus_trader.backtest.config import BacktestEngineConfig, SimulationModuleConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FeeModel, FillModel
from nautilus_trader.backtest.modules import SimulationModule
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType, OmsType, OrderSide, TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Equity, Instrument
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.trading.strategy import Strategy

from market_impact_agent.account_state import (
    AccountPosition,
    AccountStateSnapshot,
    CashBalance,
    RecentFill,
    capture_account_state_snapshot,
    opaque_account_reference_hash,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import (
    ExecutableOrder,
    OrderIntent,
    OrderKind,
    PortfolioOrderIntent,
    Side,
    TradingEnvironment,
)
from market_impact_agent.nautilus_backtest import AShareDailyBar, AShareFixtureFeeModel
from market_impact_agent.providers import Capability, ProviderManifest, ProviderTransport, TrustTier

_CNY = Currency.from_str("CNY")
_VENUE = Venue("HIST")


@dataclass(frozen=True)
class HistoricalInstrumentSpec:
    target_id: str
    instrument_class: str
    source_ref: str
    price_increment: Decimal = Decimal("0.01")
    lot_size: int = 100
    price_limit_ratio: Decimal = Decimal("0.10")
    commission_rate: Decimal = Decimal("0.0003")
    minimum_commission: Decimal = Decimal("5")
    sell_stamp_tax_rate: Decimal = Decimal("0.0005")

    def __post_init__(self) -> None:
        if self.target_id.rsplit(".", 1)[-1] not in {"XSHG", "XSHE", "SH", "SZ"}:
            raise ValueError("historical instrument requires XSHG/XSHE identity")
        if self.instrument_class not in {"equity", "exchange_traded_fund"} or not self.source_ref:
            raise ValueError("source-backed equity/exchange_traded_fund specification required")
        values = (
            self.price_increment,
            self.price_limit_ratio,
            self.commission_rate,
            self.minimum_commission,
            self.sell_stamp_tax_rate,
        )
        if any(not x.is_finite() or x < 0 for x in values):
            raise ValueError("instrument numeric rules must be finite and nonnegative")
        if self.price_increment <= 0 or self.lot_size <= 0 or self.price_limit_ratio >= 1:
            raise ValueError("invalid tick, lot or price limit")
        if self.instrument_class == "exchange_traded_fund" and self.sell_stamp_tax_rate:
            raise ValueError("equity ETF must explicitly use zero stamp tax")

    @property
    def venue(self) -> str:
        suffix = self.target_id.rsplit(".", 1)[1]
        return {"SH": "XSHG", "SZ": "XSHE"}.get(suffix, suffix)

    def execution_rules_compatible(self, other: HistoricalInstrumentSpec) -> bool:
        """Compare every rule and identity field, excluding only source provenance."""
        return self == replace(other, source_ref=self.source_ref)

    @property
    def engine_id(self) -> InstrumentId:
        # One CNY settlement account spans both source exchanges; source identity is retained.
        symbol = self.target_id.rsplit(".", 1)[0]
        return InstrumentId.from_str(f"{symbol}-{self.venue}.HIST")


@dataclass(frozen=True)
class HistoricalCorporateAction:
    action_id: str
    target_id: str
    kind: str
    effective_at: datetime
    source_ref: str
    cash_per_share: Decimal = Decimal(0)
    split_ratio: Decimal = Decimal(1)
    entitlement_at: datetime | None = None


@dataclass(frozen=True)
class HistoricalFill:
    order_id: str
    target_id: str
    side: Side
    quantity: Decimal
    price: Decimal
    commission: Decimal
    filled_at: datetime


@dataclass(frozen=True)
class HistoricalNoFill:
    order_id: str
    reason: str


@dataclass(frozen=True)
class HistoricalSessionResult:
    account_state: AccountStateSnapshot
    cash: Decimal
    nav: Decimal
    positions: Mapping[str, Decimal]
    fills: tuple[HistoricalFill, ...]
    no_fills: tuple[HistoricalNoFill, ...]
    input_hash: str

    @property
    def result_hash(self) -> str:
        return canonical_hash(_json_value(asdict(self)))


class _Fees(FeeModel):
    def __init__(self, specs: tuple[HistoricalInstrumentSpec, ...]) -> None:
        self.models = {
            spec.engine_id: AShareFixtureFeeModel(
                commission_rate=spec.commission_rate,
                minimum_commission=spec.minimum_commission,
                sell_stamp_tax_rate=spec.sell_stamp_tax_rate,
            )
            for spec in specs
        }

    def get_commission(
        self, order: Order, fill_qty: Quantity, fill_px: Price, instrument: Instrument
    ) -> Money:
        return self.models[instrument.id].get_commission(order, fill_qty, fill_px, instrument)


class _CorporateActions(SimulationModule):
    def __init__(self) -> None:
        super().__init__(SimulationModuleConfig())
        self.pending: list[tuple[int, Decimal]] = []

    def process(self, ts_now: int) -> None:
        while self.pending and self.pending[0][0] <= ts_now:
            _, amount = self.pending.pop(0)
            self.exchange.adjust_account(Money.from_decimal(amount, _CNY))

    def log_diagnostics(self, logger: Any) -> None:
        pass

    def reset(self) -> None:
        self.pending.clear()


class _Orders(Strategy):
    def __init__(self, specs: tuple[HistoricalInstrumentSpec, ...]) -> None:
        super().__init__()
        self.specs = specs
        self.pending: dict[InstrumentId, list[ExecutableOrder]] = {}
        self.fills: list[OrderFilled] = []

    def on_start(self) -> None:
        for spec in self.specs:
            self.subscribe_quote_ticks(spec.engine_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        for intent in self.pending.pop(tick.instrument_id, []):
            self.submit_order(
                self.order_factory.market(
                    instrument_id=tick.instrument_id,
                    order_side=OrderSide.BUY if intent.side is Side.BUY else OrderSide.SELL,
                    quantity=Quantity.from_str(str(intent.quantity)),
                    time_in_force=TimeInForce.IOC,
                    client_order_id=ClientOrderId(intent.client_order_id),
                )
            )

    def on_order_filled(self, event: OrderFilled) -> None:
        self.fills.append(event)


class HistoricalStreamingAccount:
    """Append before execution; replay that exact prefix after interrupted execution.

    New admitted instruments can be registered between batches on the same engine.
    No model call is part of reconstruction. A failed advance
    poisons this instance: reopen the journal to recover before continuing.
    """

    def __init__(
        self,
        *,
        specs: tuple[HistoricalInstrumentSpec, ...],
        journal_path: Path,
        account_reference: str,
        account_reference_key: bytes,
        initial_cash: Decimal = Decimal("100000"),
    ) -> None:
        if nautilus_trader.__version__ != "1.231.0":
            raise RuntimeError("historical streaming requires pinned NautilusTrader 1.231.0")
        if not initial_cash.is_finite() or initial_cash <= 0 or not specs:
            raise ValueError("positive CNY opening cash and instrument specs required")
        self.specs = {spec.target_id: spec for spec in specs}
        if len(self.specs) != len(specs) or len({spec.engine_id for spec in specs}) != len(specs):
            raise ValueError("duplicate instrument specification")
        self.journal_path = journal_path
        self.account_reference = account_reference
        self.account_reference_key = account_reference_key
        self.initial_cash = initial_cash
        self.account_id = opaque_account_reference_hash(
            account_reference, key=account_reference_key
        )
        self._last_close: datetime | None = None
        self._seen_orders: set[str] = set()
        self._seen_actions: set[str] = set()
        self._closed = False
        self._poisoned = False
        self.results: list[HistoricalSessionResult] = []
        self.strategy = _Orders(specs)
        self.fees = _Fees(specs)
        self.actions = _CorporateActions()
        self.engine = BacktestEngine(
            config=BacktestEngineConfig(
                logging=LoggingConfig(log_level="ERROR"), run_analysis=False
            )
        )
        self.engine.add_venue(
            venue=_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_decimal(initial_cash, _CNY)],
            base_currency=_CNY,
            fee_model=self.fees,
            fill_model=FillModel(prob_slippage=0.0, random_seed=0),
            modules=[self.actions],
            bar_execution=False,
            liquidity_consumption=True,
            allow_cash_borrowing=False,
        )
        for spec in specs:
            self._add_engine_instrument(spec)
        self.engine.add_strategy(self.strategy)
        config = _json_value(
            {
                "schema": "historical-streaming-account.v1",
                "account": self.account_id,
                "cash": initial_cash,
                "specs": [asdict(s) for s in specs],
            }
        )
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        self._journal_lock = journal_path.with_suffix(journal_path.suffix + ".lock").open("a")
        try:
            fcntl.flock(self._journal_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            if journal_path.exists():
                records = [json.loads(line) for line in journal_path.read_text().splitlines()]
                if not records or records[0] != config:
                    raise ValueError(
                        "historical replay configuration differs from persisted account"
                    )
                for record in records[1:]:
                    self._replay_record(record)
            else:
                self._append(config)
        except Exception:
            self.engine.dispose()
            self._journal_lock.close()
            raise

    def _add_engine_instrument(self, spec: HistoricalInstrumentSpec) -> None:
        self.engine.add_instrument(
            Equity(
                instrument_id=spec.engine_id,
                raw_symbol=Symbol(spec.target_id),
                currency=_CNY,
                price_precision=max(0, -int(spec.price_increment.as_tuple().exponent)),
                price_increment=Price.from_str(str(spec.price_increment)),
                lot_size=Quantity.from_int(spec.lot_size),
                ts_event=0,
                ts_init=0,
            )
        )

    def register_instrument(self, spec: HistoricalInstrumentSpec, *, _persist: bool = True) -> None:
        """Register a newly admitted source-backed target on the existing engine."""
        if self._closed or self._poisoned:
            raise RuntimeError("closed/interrupted account")
        if spec.target_id in self.specs:
            if not self.specs[spec.target_id].execution_rules_compatible(spec):
                raise ValueError("registered historical instrument rules are immutable")
            return
        if any(existing.engine_id == spec.engine_id for existing in self.specs.values()):
            raise ValueError("duplicate source-exchange instrument alias")
        self._poisoned = True
        if _persist:
            self._append({"register_instrument": _json_value(asdict(spec))})
        self._add_engine_instrument(spec)
        self.specs[spec.target_id] = spec
        self.fees.models[spec.engine_id] = AShareFixtureFeeModel(
            commission_rate=spec.commission_rate,
            minimum_commission=spec.minimum_commission,
            sell_stamp_tax_rate=spec.sell_stamp_tax_rate,
        )
        self.strategy.specs += (spec,)
        if self.results:
            self.strategy.subscribe_quote_ticks(spec.engine_id)
        self._poisoned = False

    def _append(self, value: dict[str, Any]) -> None:
        # Atomic publication leaves either the old or new complete decision prefix.
        prefix = self.journal_path.read_bytes() if self.journal_path.exists() else b""
        payload = (
            prefix + (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )
        descriptor, temporary = tempfile.mkstemp(dir=self.journal_path.parent)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.journal_path)
            directory = os.open(self.journal_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def bootstrap_half_hs300(self, prior_session_bar: AShareDailyBar) -> HistoricalSessionResult:
        """Seed through a real prior-session fill; fees reduce the initial CNY NAV.

        The supplied raw bar must precede the first decision session. This is an
        explicit historical opening allocation, not an Agent research signal.
        """
        spec = self.specs.get("510300.SH") or self.specs.get("510300.XSHG")
        if spec is None or spec.instrument_class != "exchange_traded_fund":
            raise ValueError("opening allocation requires source-backed 510300 equity ETF")
        lots = self.initial_cash / 2 / prior_session_bar.open // spec.lot_size
        quantity = lots * spec.lot_size
        if quantity <= 0:
            raise ValueError("opening allocation cannot purchase one lot")
        intent = OrderIntent(
            client_order_id="historical-opening-510300",
            signal_id="historical-opening-allocation",
            account_id=self.account_id,
            environment=TradingEnvironment.BACKTEST,
            instrument_id=spec.target_id,
            side=Side.BUY,
            quantity=quantity,
            order_kind=OrderKind.MARKET,
            created_at=prior_session_bar.session_open_at - timedelta(microseconds=1),
            expires_at=prior_session_bar.session_close_at,
        )
        if self.results:
            result = self.results[0]
            expected = _json_value(
                {
                    "bars": {spec.target_id: asdict(prior_session_bar)},
                    "intents": [intent.to_dict()],
                    "actions": [],
                }
            )
            if result.input_hash != canonical_hash(expected):
                raise ValueError("persisted opening allocation differs from requested seed")
        else:
            result = self.advance_session({spec.target_id: prior_session_bar}, intents=(intent,))
        if sum((fill.quantity for fill in result.fills), Decimal(0)) != quantity:
            raise ValueError("opening allocation did not fill completely; account remains unready")
        return result

    def reopen_session_intents(
        self, result: HistoricalSessionResult
    ) -> tuple[ExecutableOrder, ...]:
        """Reopen commands from the same durable input used to reconstruct a result."""
        if result not in self.results:
            raise ValueError("session result does not belong to this account")
        for line in self.journal_path.read_text().splitlines()[1:]:
            record = json.loads(line)
            if "bars" in record and canonical_hash(record) == result.input_hash:
                return tuple(_intent(value) for value in record["intents"])
        raise ValueError("session result is missing its persisted commands")

    def _replay_record(self, record: dict[str, Any]) -> None:
        if "register_instrument" in record:
            values = record["register_instrument"].copy()
            for field in (
                "price_increment",
                "price_limit_ratio",
                "commission_rate",
                "minimum_commission",
                "sell_stamp_tax_rate",
            ):
                values[field] = Decimal(values[field])
            self.register_instrument(HistoricalInstrumentSpec(**values), _persist=False)
            return
        bars = {key: _bar(value) for key, value in record["bars"].items()}
        intents = tuple(_intent(item) for item in record["intents"])
        actions = tuple(
            HistoricalCorporateAction(
                action_id=item["action_id"],
                target_id=item["target_id"],
                kind=item["kind"],
                effective_at=datetime.fromisoformat(item["effective_at"]),
                source_ref=item["source_ref"],
                cash_per_share=Decimal(item["cash_per_share"]),
                split_ratio=Decimal(item["split_ratio"]),
                entitlement_at=(
                    datetime.fromisoformat(item["entitlement_at"])
                    if item.get("entitlement_at")
                    else None
                ),
            )
            for item in record["actions"]
        )
        self._advance(bars, intents, actions, persist=False)

    def advance_session(
        self,
        bars: Mapping[str, AShareDailyBar],
        *,
        intents: tuple[ExecutableOrder, ...] = (),
        corporate_actions: tuple[HistoricalCorporateAction, ...] = (),
    ) -> HistoricalSessionResult:
        return self._advance(bars, intents, corporate_actions, persist=True)

    def _advance(
        self,
        bars: Mapping[str, AShareDailyBar],
        intents: tuple[ExecutableOrder, ...],
        actions: tuple[HistoricalCorporateAction, ...],
        *,
        persist: bool,
    ) -> HistoricalSessionResult:
        if self._closed or self._poisoned:
            raise RuntimeError("closed/interrupted engine; reopen persisted prefix for recovery")
        if not bars or not set(bars) <= self.specs.keys():
            raise ValueError("session requires registered instruments and raw bars")
        opens = {bar.session_open_at for bar in bars.values()}
        closes = {bar.session_close_at for bar in bars.values()}
        if len(opens) != 1 or len(closes) != 1:
            raise ValueError("session batch must have aligned open/close times")
        opened, closed = next(iter(opens)), next(iter(closes))
        if opened.utcoffset() is None or closed.utcoffset() is None or opened >= closed:
            raise ValueError("session requires aware increasing timestamps")
        if self._last_close is not None and (
            opened <= self._last_close
            or opened.astimezone(ZoneInfo("Asia/Shanghai")).date()
            <= self._last_close.astimezone(ZoneInfo("Asia/Shanghai")).date()
        ):
            raise ValueError("streaming sessions must strictly advance the persisted prefix")
        quantities = self._quantities()
        if not {key for key, qty in quantities.items() if qty} <= bars.keys():
            raise ValueError("daily valuation requires a raw bar for every held instrument")
        for key, bar in bars.items():
            spec = self.specs[key]
            prices = (bar.previous_close, bar.open, bar.high, bar.low, bar.close)
            if any(not x.is_finite() or x <= 0 or x % spec.price_increment for x in prices):
                raise ValueError("raw OHLC prices must be positive and tick-aligned")
            if not bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high:
                raise ValueError("inconsistent raw OHLC")
            if min(bar.volume, bar.open_bid_quantity, bar.open_ask_quantity) < 0:
                raise ValueError("negative market quantity")
        action_ids = [a.action_id for a in actions]
        if len(set(action_ids)) != len(action_ids) or self._seen_actions.intersection(action_ids):
            raise ValueError("duplicate corporate action")
        adjustments: list[tuple[int, Decimal]] = []
        for action in actions:
            if (
                not action.action_id
                or not action.source_ref
                or action.target_id not in bars
                or action.effective_at != opened
            ):
                raise ValueError("corporate action requires source and exact effective session")
            if action.kind != "cash_dividend" or action.split_ratio != 1:
                raise ValueError(
                    "unsupported corporate action: pinned engine split transition unaccepted"
                )
            if not action.cash_per_share.is_finite() or action.cash_per_share < 0:
                raise ValueError("invalid cash dividend")
            entitled = quantities.get(action.target_id, Decimal(0))
            if action.entitlement_at is not None:
                entitlement = next(
                    (r for r in self.results if r.account_state.as_of == action.entitlement_at),
                    None,
                )
                if entitlement is None or action.entitlement_at >= opened:
                    raise ValueError(
                        "corporate action record-date holdings are not in the persisted prefix"
                    )
                entitled = entitlement.positions.get(action.target_id, Decimal(0))
            adjustments.append(
                (_ns(opened), (entitled * action.cash_per_share).quantize(Decimal("0.01")))
            )
        ids = [intent.client_order_id for intent in intents]
        if len(set(ids)) != len(ids) or self._seen_orders.intersection(ids):
            raise ValueError("duplicate durable order identity")
        no_fills: list[HistoricalNoFill] = []
        pending: dict[InstrumentId, list[ExecutableOrder]] = {}
        remaining = quantities.copy()
        for intent in intents:
            if (
                intent.environment is not TradingEnvironment.BACKTEST
                or intent.account_id != self.account_id
                or intent.instrument_id not in bars
                or intent.order_kind is not OrderKind.MARKET
            ):
                raise ValueError(
                    "intent must bind this historical account and raw session instrument"
                )
            if intent.created_at >= opened or intent.expires_at <= opened:
                raise ValueError("intent must precede and remain valid at executable open")
            spec, bar = self.specs[intent.instrument_id], bars[intent.instrument_id]
            reason = None
            if intent.quantity % spec.lot_size and not (
                intent.side is Side.SELL
                and intent.quantity == remaining.get(intent.instrument_id, Decimal(0))
            ):
                reason = "lot_size"
            elif bar.suspended:
                reason = "suspended"
            elif not bar.volume:
                reason = "zero_volume"
            elif intent.side is Side.SELL and intent.quantity > remaining.get(
                intent.instrument_id, Decimal(0)
            ):
                reason = "t_plus_one_or_insufficient_overnight_position"
            else:
                upper = (
                    bar.previous_close * (1 + spec.price_limit_ratio) / spec.price_increment
                ).quantize(Decimal(1), rounding=ROUND_HALF_UP) * spec.price_increment
                lower = (
                    bar.previous_close * (1 - spec.price_limit_ratio) / spec.price_increment
                ).quantize(Decimal(1), rounding=ROUND_HALF_UP) * spec.price_increment
                if intent.side is Side.BUY and (bar.open >= upper or not bar.open_ask_quantity):
                    reason = "limit_up_or_no_ask"
                elif intent.side is Side.SELL and (bar.open <= lower or not bar.open_bid_quantity):
                    reason = "limit_down_or_no_bid"
            if reason:
                no_fills.append(HistoricalNoFill(intent.client_order_id, reason))
            else:
                pending.setdefault(spec.engine_id, []).append(intent)
                if intent.side is Side.SELL:
                    remaining[intent.instrument_id] -= intent.quantity
        record = _json_value(
            {
                "bars": {key: asdict(bar) for key, bar in sorted(bars.items())},
                "intents": [intent.to_dict() for intent in intents],
                "actions": [asdict(action) for action in actions],
            }
        )
        # Any publication/execution failure requires recovery, never a same-instance retry.
        self._poisoned = True
        if persist:
            self._append(record)
        self.strategy.pending = pending
        self.actions.pending = adjustments
        before = len(self.strategy.fills)
        ticks: list[QuoteTick] = []
        for key, bar in sorted(bars.items()):
            spec = self.specs[key]
            ticks.extend(
                (
                    _tick(spec, opened, bar.open, bar.open_bid_quantity, bar.open_ask_quantity),
                    _tick(spec, closed, bar.close, 0, 0),
                )
            )
        ticks.sort(key=_tick_timestamp)
        self.engine.add_data(ticks)
        self.engine.run(streaming=True)
        self.engine.clear_data()
        by_id = {spec.engine_id: key for key, spec in self.specs.items()}
        fills = tuple(
            HistoricalFill(
                order_id=str(event.client_order_id),
                target_id=by_id[event.instrument_id],
                side=Side.BUY if event.order_side is OrderSide.BUY else Side.SELL,
                quantity=event.last_qty.as_decimal(),
                price=event.last_px.as_decimal(),
                commission=event.commission.as_decimal(),
                filled_at=datetime.fromtimestamp(event.ts_event / 1e9, UTC),
            )
            for event in self.strategy.fills[before:]
        )
        filled_ids = {fill.order_id for fill in fills}
        blocked_ids = {item.order_id for item in no_fills}
        no_fills.extend(
            HistoricalNoFill(intent.client_order_id, "engine_no_fill")
            for intent in intents
            if intent.client_order_id not in filled_ids | blocked_ids
        )
        no_fills.extend(
            HistoricalNoFill(intent.client_order_id, "engine_partial_fill_unfilled_remainder")
            for intent in intents
            if intent.client_order_id in filled_ids
            and sum(
                (fill.quantity for fill in fills if fill.order_id == intent.client_order_id),
                Decimal(0),
            )
            < intent.quantity
        )
        if self.engine.cache.orders_open():
            raise RuntimeError("IOC session retained open orders; reconciliation required")
        quantities = self._quantities()
        cash = self.engine.cache.account_for_venue(_VENUE).balance_total(_CNY).as_decimal()
        nav = cash + sum((qty * bars[key].close for key, qty in quantities.items()), Decimal(0))
        positions = tuple(
            AccountPosition(
                target_id=key,
                venue=self.specs[key].venue,
                instrument_class=self.specs[key].instrument_class,
                side=Side.BUY,
                quantity=qty,
                concentration=qty * bars[key].close / nav,
                concentration_gap=None,
            )
            for key, qty in sorted(quantities.items())
            if qty
        )
        input_hash = canonical_hash(record)
        account = capture_account_state_snapshot(
            provider=_provider(),
            account_reference=self.account_reference,
            account_reference_key=self.account_reference_key,
            environment=TradingEnvironment.BACKTEST,
            as_of=closed.astimezone(UTC),
            reconciled_at=closed.astimezone(UTC),
            reconciliation_reference="historical-session-" + input_hash,
            cash=(CashBalance("CNY", cash, cash),),
            positions=positions,
            open_orders=(),
            recent_fills=tuple(
                RecentFill(
                    fill_reference=f"historical-fill-{input_hash}-{index}",
                    order_reference=fill.order_id,
                    target_id=fill.target_id,
                    venue=self.specs[fill.target_id].venue,
                    instrument_class=self.specs[fill.target_id].instrument_class,
                    side=fill.side,
                    quantity=fill.quantity,
                    filled_at=fill.filled_at,
                )
                for index, fill in enumerate(fills)
            ),
            recent_fills_since=opened.astimezone(UTC),
        )
        result = HistoricalSessionResult(
            account, cash, nav, quantities, fills, tuple(no_fills), input_hash
        )
        self.results.append(result)
        self._last_close = closed
        self._seen_orders.update(ids)
        self._seen_actions.update(action_ids)
        self._poisoned = False
        return result

    def _quantities(self) -> dict[str, Decimal]:
        by_id = {spec.engine_id: key for key, spec in self.specs.items()}
        return {
            by_id[position.instrument_id]: position.quantity.as_decimal()
            for position in self.engine.cache.positions_open()
        }

    def close(self) -> None:
        if not self._closed:
            try:
                if self.results:
                    self.engine.end()
            finally:
                self.engine.dispose()
                self._journal_lock.close()
                self._closed = True


def _provider() -> ProviderManifest:
    return ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="nautilus-historical-account",
        provider_version="1.231.0",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.BACKTEST}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("XSHG", "XSHE"),
        order_types=("market",),
        supports_streaming=True,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.MOCK,
    )


def _ns(at: datetime) -> int:
    return int(at.timestamp() * 1_000_000_000)


def _tick(
    spec: HistoricalInstrumentSpec, at: datetime, price: Decimal, bid: int, ask: int
) -> QuoteTick:
    return QuoteTick(
        instrument_id=spec.engine_id,
        bid_price=Price.from_str(str(price.quantize(spec.price_increment))),
        ask_price=Price.from_str(str(price.quantize(spec.price_increment))),
        bid_size=Quantity.from_int(bid),
        ask_size=Quantity.from_int(ask),
        ts_event=_ns(at),
        ts_init=_ns(at),
    )


def _json_value(value: Any) -> Any:
    return json.loads(
        json.dumps(value, default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x))
    )


def _tick_timestamp(tick: QuoteTick) -> int:
    return cast(int, tick.ts_init)


def _bar(value: dict[str, Any]) -> AShareDailyBar:
    values: dict[str, Any] = {
        key: datetime.fromisoformat(item)
        if key in {"session_open_at", "session_close_at"}
        else Decimal(item)
        if key in {"previous_close", "open", "high", "low", "close"}
        else item
        for key, item in value.items()
    }
    return AShareDailyBar(**values)


def _intent(value: dict[str, Any]) -> ExecutableOrder:
    values = {key: item for key, item in value.items() if key != "schema_version"}
    values.update(
        environment=TradingEnvironment(value["environment"]),
        side=Side(value["side"]),
        order_kind=OrderKind(value["order_kind"]),
        quantity=Decimal(value["quantity"]),
        created_at=datetime.fromisoformat(value["created_at"]),
        expires_at=datetime.fromisoformat(value["expires_at"]),
    )
    return (
        PortfolioOrderIntent(**values)
        if "portfolio_decision_id" in values
        else OrderIntent(**values)
    )
