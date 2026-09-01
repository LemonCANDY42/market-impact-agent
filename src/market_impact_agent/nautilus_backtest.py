# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false, reportUntypedBaseClass=false
# NautilusTrader 1.x exposes Cython extension types without static type information.
# Keep the suppression at this integration boundary; the harness contracts remain strict.

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from hashlib import sha256
from itertools import pairwise
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
    _NAUTILUS_OUTCOME_PRODUCER_TOKEN,  # pyright: ignore[reportPrivateUsage]
    BacktestInputHash,
    BacktestMetric,
    BacktestRequest,
    BacktestResult,
    BacktestRunManifest,
    BacktestRunStatus,
    StrategyAdverseExcursionPoint,
    StrategyBacktestArm,
    StrategyBacktestCost,
    StrategyBacktestFill,
    StrategyBacktestMissingMetric,
    StrategyBacktestOutcomeMissing,
    StrategyBacktestOutcomeReceipt,
    StrategyBacktestVariant,
    StrategyCapitalPoint,
    _record_strategy_backtest_outcome,  # pyright: ignore[reportPrivateUsage]
    backtest_result_to_dict,
    canonical_backtest_request_hash,
    canonical_backtest_result_hash,
    strategy_backtest_cost_model_hash,
    strategy_backtest_fill_model_hash,
    strategy_backtest_universe_hash,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import Side, require_aware
from market_impact_agent.runtime_store import ArtifactStore

_ENGINE_NAME = "nautilus_trader"
_ENGINE_VERSION = "1.231.0"
_BRIDGE_NAME = "nautilus-backtest"
_BRIDGE_VERSION = "0.3.0"
_SUPPORTED_DATA_GRANULARITY = "daily_bar.v1"
_SUPPORTED_BOOK_TYPE = "top_of_book"
_SUPPORTED_FILL_MODEL = "next_executable_open_one_tick_slippage.v1"
_SUPPORTED_FEE_MODEL = "a_share_fixture_fee.v1"
_SUPPORTED_VENUE_RULESET = "xshg_cash_equity_fixture.v1"
_EVENT_IMPACT_HOLD_STRATEGY = "event-impact-hold.v1"
_CASH_NO_ACTION_STRATEGY = "cash-no-action.v1"
_SUPPORTED_STRATEGIES = frozenset({_EVENT_IMPACT_HOLD_STRATEGY, _CASH_NO_ACTION_STRATEGY})
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


@dataclass(frozen=True, slots=True)
class _ExecutedHorizon:
    metrics: tuple[BacktestMetric, ...]
    fills: tuple[OrderFilled, ...]
    bars: tuple[AShareDailyBar, ...]


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
    def __init__(
        self,
        snapshot_path: Path,
        *,
        snapshot_store: LocalDataSnapshotStore | None = None,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self._snapshot = load_a_share_daily_bar_snapshot(snapshot_path)
        self._contract = replace(
            _SYNTHETIC_REPLAY_CONTRACT,
            input_hashes=(BacktestInputHash("snapshot", self._snapshot.content_hash),),
        )
        self._snapshot_store = snapshot_store
        self._artifact_store = artifact_store
        self._configure_authority(snapshot_path.read_bytes())

    @classmethod
    def from_snapshot(
        cls,
        snapshot: AShareDailyBarSnapshot,
        contract: NautilusReplayContract,
        *,
        snapshot_store: LocalDataSnapshotStore | None = None,
        artifact_store: ArtifactStore | None = None,
    ) -> NautilusBacktestBridge:
        instance = cls.__new__(cls)
        instance._snapshot = snapshot
        instance._contract = contract
        instance._snapshot_store = snapshot_store
        instance._artifact_store = artifact_store
        instance._configure_authority(None)
        return instance

    def _configure_authority(self, raw_snapshot: bytes | None) -> None:
        if self._snapshot_store is None and self._artifact_store is None:
            return
        if type(self._snapshot_store) is not LocalDataSnapshotStore:
            raise TypeError("authoritative backtests require the concrete Harness Snapshot store")
        assert self._snapshot_store is not None
        if self._artifact_store is None:
            self._artifact_store = self._snapshot_store.artifacts
        if type(self._artifact_store) is not ArtifactStore:
            raise TypeError("authoritative backtests require the concrete Artifact store")
        if self._artifact_store.root != self._snapshot_store.artifacts.root:
            raise ValueError("backtest Snapshot and Artifact stores must share one Harness root")
        if raw_snapshot is not None:
            stored = self._artifact_store.put_bytes(
                raw_snapshot, media_type="application/octet-stream"
            )
            if stored.content_hash != self._snapshot.content_hash:
                raise ValueError("stored Nautilus input differs from its content hash")

    def run_strategy_outcome(
        self,
        request: BacktestRequest,
        *,
        case_id: str,
        variant: StrategyBacktestVariant,
    ) -> StrategyBacktestOutcomeReceipt | StrategyBacktestOutcomeMissing:
        """Execute and persist the promotion-capable, same-root outcome path."""

        if self._snapshot_store is None or self._artifact_store is None:
            raise PermissionError(
                "strategy outcome receipts require canonical Snapshot and Artifact stores"
            )
        if len(request.horizons_sessions) != 1:
            raise ValueError("strategy outcome receipts require exactly one holding horizon")
        if not variant.matches_request(request):
            raise ValueError("backtest request differs from its frozen strategy variant")
        if request.strategy_ref not in _SUPPORTED_STRATEGIES:
            return StrategyBacktestOutcomeMissing(
                case_id=case_id,
                arm=variant.arm,
                strategy_variant_hash=variant.strategy_variant_hash,
                reason=f"unsupported_strategy_ref:{request.strategy_ref}",
            )
        source = self._snapshot_store.get(request.data_snapshot_id)
        if not source.coverage_complete:
            raise ValueError("strategy outcome source Snapshot must be coverage-complete")
        source_artifact = self._artifact_store.put_json(source.to_dict())
        raw_input_hashes = {item.value for item in self._contract.input_hashes}
        if not any(item.raw_content_hash in raw_input_hashes for item in source.observations):
            raise ValueError(
                "strategy outcome source Snapshot does not bind the Nautilus input artifact"
            )
        manifest = self._manifest(request)
        if manifest.engine_config_hash != self._engine_config_hash(request):
            raise ValueError("backtest manifest differs from the frozen engine configuration")
        self._validate_request(request)
        executed = self._run_horizon(request, request.horizons_sessions[0])
        result = _result(
            manifest=manifest,
            status=BacktestRunStatus.COMPLETED,
            metrics=executed.metrics,
            artifact_refs=(self._snapshot_artifact_ref(),),
            failure_reasons=(),
        )
        result_artifact = self._artifact_store.put_json(backtest_result_to_dict(result))
        stress_artifact_hash: str | None = None
        stressed_net_return: Decimal | None = None
        missing: list[StrategyBacktestMissingMetric] = []
        try:
            stressed = self._run_horizon(
                request,
                request.horizons_sessions[0],
                fee_multiplier=Decimal(2),
            )
            stressed_outcome = _derive_strategy_outcome(
                request=request,
                executed=stressed,
                stressed_net_return=None,
                stress_artifact_hash=None,
                missing=[],
            )
            stressed_net_return = Decimal(cast(str, stressed_outcome["portfolio_net_return"]))
            stress_payload = {
                "schema_version": "market-impact.strategy-backtest-stress-evidence.v1",
                "stress_kind": "doubled_fee_actual_nautilus_run",
                "request_hash": manifest.request_hash,
                "engine_config_hash": self._engine_config_hash(request, fee_multiplier=Decimal(2)),
                "net_return": str(stressed_net_return),
                "capital_path": stressed_outcome["capital_path"],
                "fills": [
                    _fill_from_event(item, stressed.bars).to_dict() for item in stressed.fills
                ],
                "adverse_excursion_path": stressed_outcome["adverse_excursion_path"],
            }
            stress_artifact_hash = self._artifact_store.put_json(stress_payload).content_hash
        except Exception as exc:
            missing.append(
                StrategyBacktestMissingMetric(
                    "stressed_net_return", f"stress_run_failed:{type(exc).__name__}"
                )
            )
        outcome = _derive_strategy_outcome(
            request=request,
            executed=executed,
            stressed_net_return=stressed_net_return,
            stress_artifact_hash=stress_artifact_hash,
            missing=missing,
        )
        manifest_payload = cast(dict[str, object], backtest_result_to_dict(result)["manifest"])
        values: dict[str, object] = {
            "schema_version": "market-impact.strategy-backtest-outcome.v1",
            "harness_authority_id": self._snapshot_store.harness_authority_id,
            "case_id": case_id,
            "arm": variant.arm.value,
            "strategy_variant_hash": variant.strategy_variant_hash,
            "strategy_ref": request.strategy_ref,
            "target_selection_ref": request.target_selection_ref,
            "engine_config_hash": manifest.engine_config_hash,
            "simulation": {
                "data_granularity": request.simulation.data_granularity,
                "book_type": request.simulation.book_type,
                "fill_model": request.simulation.fill_model,
                "fee_model": request.simulation.fee_model,
                "venue_ruleset": request.simulation.venue_ruleset,
                "base_currency": request.simulation.base_currency,
                "starting_cash": _decimal_text(request.simulation.starting_cash),
                "random_seed": request.simulation.random_seed,
            },
            "result_hash": result.result_hash,
            "result_artifact_hash": result_artifact.content_hash,
            "manifest_hash": _canonical_sha256(manifest_payload),
            "request_hash": manifest.request_hash,
            "input_hashes": [
                {"name": item.name, "value": item.value} for item in manifest.input_hashes
            ],
            "source_snapshot_id": source.snapshot_id,
            "source_snapshot_artifact_hash": source_artifact.content_hash,
            "universe_hash": strategy_backtest_universe_hash(request.instrument_ids),
            "cost_model_hash": strategy_backtest_cost_model_hash(request.simulation),
            "fill_model_hash": strategy_backtest_fill_model_hash(request.simulation),
            **outcome,
        }
        receipt = _strategy_receipt_from_values(values)
        _record_strategy_backtest_outcome(
            store=self._snapshot_store,
            receipt=receipt,
            result=result,
            producer_token=_NAUTILUS_OUTCOME_PRODUCER_TOKEN,
        )
        return receipt

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
            "data_snapshot_id": (
                request.data_snapshot_id,
                (
                    self._snapshot.snapshot_id
                    if self._snapshot_store is None
                    else request.data_snapshot_id
                ),
            ),
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
        if request.strategy_ref not in _SUPPORTED_STRATEGIES:
            raise ValueError(f"unsupported strategy_ref: {request.strategy_ref}")
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
        bars = self._selected_bars(request)
        if request.strategy_ref == _CASH_NO_ACTION_STRATEGY:
            if len(bars) < 2:
                raise ValueError("cash-no-action replay requires at least two capital timestamps")
            return
        _entry_order_side(request.signal.side, self._contract.venue_ruleset)
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
            horizon_metrics = self._run_horizon(request, horizon_sessions).metrics
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
        *,
        fee_multiplier: Decimal = Decimal(1),
    ) -> _ExecutedHorizon:
        snapshot = self._snapshot
        bars = self._selected_bars(request)
        if request.strategy_ref == _CASH_NO_ACTION_STRATEGY:
            return _ExecutedHorizon(metrics=_cash_metrics(), fills=(), bars=bars)
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
                    commission_rate=snapshot.commission_rate * fee_multiplier,
                    minimum_commission=snapshot.minimum_commission * fee_multiplier,
                    sell_stamp_tax_rate=snapshot.sell_stamp_tax_rate * fee_multiplier,
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

        return _ExecutedHorizon(
            metrics=_metrics_from_strategy(strategy),
            fills=tuple(strategy.fills),
            bars=bars,
        )

    def _selected_bars(self, request: BacktestRequest) -> tuple[AShareDailyBar, ...]:
        return tuple(
            bar
            for bar in self._snapshot.bars
            if bar.session_open_at >= request.start_at and bar.session_close_at <= request.end_at
        )

    def _engine_config_hash(
        self, request: BacktestRequest, *, fee_multiplier: Decimal = Decimal(1)
    ) -> str:
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
                "strategy_ref": request.strategy_ref,
                "strategy_side": request.signal.side.value,
                "venue_ruleset": request.simulation.venue_ruleset,
                **({"fee_multiplier": str(fee_multiplier)} if fee_multiplier != Decimal(1) else {}),
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


def _cash_metrics() -> tuple[BacktestMetric, ...]:
    return (
        BacktestMetric("net_pnl", Decimal(0), "CNY"),
        BacktestMetric("net_return", Decimal(0), "ratio"),
        BacktestMetric("order_count", Decimal(0), "orders"),
    )


def _fill_from_event(event: OrderFilled, bars: tuple[AShareDailyBar, ...]) -> StrategyBacktestFill:
    filled_at = datetime.fromtimestamp(event.ts_event / 1_000_000_000, tz=UTC)
    bar = next((item for item in bars if item.session_open_at == filled_at), None)
    available_liquidity = None
    if bar is not None:
        available_liquidity = Decimal(
            bar.open_ask_quantity if event.order_side is OrderSide.BUY else bar.open_bid_quantity
        )
    return StrategyBacktestFill(
        side="buy" if event.order_side is OrderSide.BUY else "sell",
        filled_at=filled_at,
        quantity=event.last_qty.as_decimal(),
        price=event.last_px.as_decimal(),
        commission=event.commission.as_decimal(),
        available_liquidity_quantity=available_liquidity,
    )


def _metric_value(metrics: tuple[BacktestMetric, ...], name: str) -> Decimal:
    try:
        return next(item.value for item in metrics if item.name == name)
    except StopIteration as exc:
        raise ValueError(f"Nautilus outcome is missing metric {name}") from exc


def _derive_strategy_outcome(
    *,
    request: BacktestRequest,
    executed: _ExecutedHorizon,
    stressed_net_return: Decimal | None,
    stress_artifact_hash: str | None,
    missing: list[StrategyBacktestMissingMetric],
) -> dict[str, object]:
    if request.strategy_ref == _CASH_NO_ACTION_STRATEGY:
        starting_cash = request.simulation.starting_cash
        capital_path = (
            StrategyCapitalPoint(request.start_at, starting_cash),
            StrategyCapitalPoint(request.end_at, starting_cash),
        )
        adverse_excursion_path = (
            StrategyAdverseExcursionPoint(request.start_at, Decimal(0)),
            StrategyAdverseExcursionPoint(request.end_at, Decimal(0)),
        )
        return {
            "capital_path": [item.to_dict() for item in capital_path],
            "adverse_excursion_path": [item.to_dict() for item in adverse_excursion_path],
            "fills": [],
            "costs": [],
            "net_return": "0",
            "net_pnl": "0",
            "portfolio_net_return": "0",
            "max_drawdown": "0",
            "cvar95": "0",
            "sharpe": "0",
            "sortino": "0",
            "turnover": "0",
            "adverse_excursion": "0",
            "liquidity_cost": "0",
            "stressed_net_return": (
                None if stressed_net_return is None else _decimal_text(stressed_net_return)
            ),
            "stress_evidence_artifact_hash": stress_artifact_hash,
            "missing_metrics": [
                item.to_dict() for item in sorted(missing, key=lambda item: item.name)
            ],
        }
    fills = tuple(_fill_from_event(item, executed.bars) for item in executed.fills)
    entry, exit_fill = fills
    starting_cash = request.simulation.starting_cash
    cash_after_entry = starting_cash - entry.quantity * entry.price - entry.commission
    capital: list[StrategyCapitalPoint] = [
        StrategyCapitalPoint(request.start_at, starting_cash),
        StrategyCapitalPoint(
            entry.filled_at,
            cash_after_entry + entry.quantity * entry.price,
        ),
    ]
    holding_bars = tuple(
        bar
        for bar in executed.bars
        if entry.filled_at <= bar.session_close_at < exit_fill.filled_at
    )
    capital.extend(
        StrategyCapitalPoint(
            bar.session_close_at,
            cash_after_entry + entry.quantity * bar.close,
        )
        for bar in holding_bars
    )
    final_cash = cash_after_entry + exit_fill.quantity * exit_fill.price - exit_fill.commission
    capital.append(StrategyCapitalPoint(exit_fill.filled_at, final_cash))
    capital_path = tuple(sorted(set(capital), key=lambda item: item.observed_at))
    step_returns = tuple(
        (current.equity - previous.equity) / previous.equity
        for previous, current in pairwise(capital_path)
    )
    max_drawdown = _max_drawdown(capital_path)
    cvar95 = _cvar95(step_returns)
    sharpe = _sharpe(step_returns)
    sortino = _sortino(step_returns)
    for name, value, reason in (
        ("max_drawdown", max_drawdown, "capital_path_too_short"),
        ("cvar95", cvar95, "return_path_too_short"),
        ("sharpe", sharpe, "return_variance_unavailable"),
        ("sortino", sortino, "downside_deviation_unavailable"),
    ):
        if value is None:
            missing.append(StrategyBacktestMissingMetric(name, reason))
    commission = sum((item.commission for item in fills), Decimal(0))
    slippage = _adverse_slippage_cost(fills, executed.bars)
    costs = tuple(
        sorted(
            (
                StrategyBacktestCost("commission", commission, request.simulation.base_currency),
                StrategyBacktestCost(
                    "modeled_adverse_slippage", slippage, request.simulation.base_currency
                ),
            ),
            key=lambda item: item.name,
        )
    )
    adverse_by_time = {
        request.start_at: Decimal(0),
        entry.filled_at: Decimal(0),
        **{
            bar.session_close_at: max(Decimal(0), (entry.price - bar.low) / entry.price)
            for bar in executed.bars
            if entry.filled_at <= bar.session_open_at < exit_fill.filled_at
        },
        exit_fill.filled_at: max(Decimal(0), (entry.price - exit_fill.price) / entry.price),
    }
    adverse_excursion_path = tuple(
        StrategyAdverseExcursionPoint(observed_at, adverse_by_time[observed_at])
        for observed_at in sorted(adverse_by_time)
    )
    adverse_excursion = max(item.adverse_excursion for item in adverse_excursion_path)
    net_pnl = _metric_value(executed.metrics, "net_pnl")
    return {
        "capital_path": [item.to_dict() for item in capital_path],
        "adverse_excursion_path": [item.to_dict() for item in adverse_excursion_path],
        "fills": [item.to_dict() for item in fills],
        "costs": [item.to_dict() for item in costs],
        "net_return": _decimal_text(_metric_value(executed.metrics, "net_return")),
        "net_pnl": _decimal_text(net_pnl),
        "portfolio_net_return": _decimal_text(net_pnl / starting_cash),
        "max_drawdown": None if max_drawdown is None else _decimal_text(max_drawdown),
        "cvar95": None if cvar95 is None else _decimal_text(cvar95),
        "sharpe": None if sharpe is None else _decimal_text(sharpe),
        "sortino": None if sortino is None else _decimal_text(sortino),
        "turnover": _decimal_text(
            sum((item.quantity * item.price for item in fills), Decimal(0)) / starting_cash
        ),
        "adverse_excursion": _decimal_text(adverse_excursion),
        "liquidity_cost": _decimal_text(commission + slippage),
        "stressed_net_return": (
            None if stressed_net_return is None else _decimal_text(stressed_net_return)
        ),
        "stress_evidence_artifact_hash": stress_artifact_hash,
        "missing_metrics": [item.to_dict() for item in sorted(missing, key=lambda item: item.name)],
    }


def _max_drawdown(capital_path: tuple[StrategyCapitalPoint, ...]) -> Decimal | None:
    if len(capital_path) < 2:
        return None
    peak = capital_path[0].equity
    result = Decimal(0)
    for point in capital_path[1:]:
        peak = max(peak, point.equity)
        result = max(result, (peak - point.equity) / peak)
    return result


def _cvar95(returns: tuple[Decimal, ...]) -> Decimal | None:
    if not returns:
        return None
    tail_count = max(1, (len(returns) + 19) // 20)
    losses = sorted((-item for item in returns), reverse=True)[:tail_count]
    return max(Decimal(0), sum(losses, Decimal(0)) / Decimal(tail_count))


def _sharpe(returns: tuple[Decimal, ...]) -> Decimal | None:
    if len(returns) < 2:
        return None
    with localcontext() as context:
        context.prec = 50
        count = Decimal(len(returns))
        mean = sum(returns, Decimal(0)) / count
        variance = sum(((item - mean) ** 2 for item in returns), Decimal(0)) / Decimal(
            len(returns) - 1
        )
        if variance == 0:
            return None
        return mean / variance.sqrt() * Decimal(252).sqrt()


def _sortino(returns: tuple[Decimal, ...]) -> Decimal | None:
    if not returns:
        return None
    downside = tuple(min(item, Decimal(0)) for item in returns)
    with localcontext() as context:
        context.prec = 50
        downside_deviation = (
            sum((item * item for item in downside), Decimal(0)) / Decimal(len(downside))
        ).sqrt()
        if downside_deviation == 0:
            return None
        mean = sum(returns, Decimal(0)) / Decimal(len(returns))
        return mean / downside_deviation * Decimal(252).sqrt()


def _adverse_slippage_cost(
    fills: tuple[StrategyBacktestFill, ...], bars: tuple[AShareDailyBar, ...]
) -> Decimal:
    total = Decimal(0)
    for fill in fills:
        bar = next(
            (item for item in bars if item.session_open_at == fill.filled_at),
            None,
        )
        if bar is None:
            continue
        adverse = fill.price - bar.open if fill.side == "buy" else bar.open - fill.price
        total += max(Decimal(0), adverse) * fill.quantity
    return total


def _strategy_receipt_from_values(values: dict[str, object]) -> StrategyBacktestOutcomeReceipt:
    receipt_id = f"strategy-backtest-outcome-{_canonical_sha256(values)}"
    return StrategyBacktestOutcomeReceipt(
        receipt_id=receipt_id,
        harness_authority_id=cast(str, values["harness_authority_id"]),
        case_id=cast(str, values["case_id"]),
        arm=StrategyBacktestArm(cast(str, values["arm"])),
        strategy_variant_hash=cast(str, values["strategy_variant_hash"]),
        strategy_ref=cast(str, values["strategy_ref"]),
        target_selection_ref=cast(str, values["target_selection_ref"]),
        engine_config_hash=cast(str, values["engine_config_hash"]),
        simulation_data_granularity=cast(
            str, cast(dict[str, object], values["simulation"])["data_granularity"]
        ),
        simulation_book_type=cast(str, cast(dict[str, object], values["simulation"])["book_type"]),
        simulation_fill_model=cast(
            str, cast(dict[str, object], values["simulation"])["fill_model"]
        ),
        simulation_fee_model=cast(str, cast(dict[str, object], values["simulation"])["fee_model"]),
        simulation_venue_ruleset=cast(
            str, cast(dict[str, object], values["simulation"])["venue_ruleset"]
        ),
        simulation_base_currency=cast(
            str, cast(dict[str, object], values["simulation"])["base_currency"]
        ),
        simulation_starting_cash=Decimal(
            cast(str, cast(dict[str, object], values["simulation"])["starting_cash"])
        ),
        simulation_random_seed=cast(
            int, cast(dict[str, object], values["simulation"])["random_seed"]
        ),
        result_hash=cast(str, values["result_hash"]),
        result_artifact_hash=cast(str, values["result_artifact_hash"]),
        manifest_hash=cast(str, values["manifest_hash"]),
        request_hash=cast(str, values["request_hash"]),
        input_hashes=tuple(
            BacktestInputHash(cast(str, item["name"]), cast(str, item["value"]))
            for item in cast(list[dict[str, object]], values["input_hashes"])
        ),
        source_snapshot_id=cast(str, values["source_snapshot_id"]),
        source_snapshot_artifact_hash=cast(str, values["source_snapshot_artifact_hash"]),
        universe_hash=cast(str, values["universe_hash"]),
        cost_model_hash=cast(str, values["cost_model_hash"]),
        fill_model_hash=cast(str, values["fill_model_hash"]),
        capital_path=tuple(
            StrategyCapitalPoint(
                datetime.fromisoformat(cast(str, item["observed_at"]).replace("Z", "+00:00")),
                Decimal(cast(str, item["equity"])),
            )
            for item in cast(list[dict[str, object]], values["capital_path"])
        ),
        adverse_excursion_path=tuple(
            StrategyAdverseExcursionPoint(
                datetime.fromisoformat(cast(str, item["observed_at"]).replace("Z", "+00:00")),
                Decimal(cast(str, item["adverse_excursion"])),
            )
            for item in cast(list[dict[str, object]], values["adverse_excursion_path"])
        ),
        fills=tuple(
            StrategyBacktestFill(
                side=cast(str, item["side"]),
                filled_at=datetime.fromisoformat(
                    cast(str, item["filled_at"]).replace("Z", "+00:00")
                ),
                quantity=Decimal(cast(str, item["quantity"])),
                price=Decimal(cast(str, item["price"])),
                commission=Decimal(cast(str, item["commission"])),
                available_liquidity_quantity=_decimal_or_none(item["available_liquidity_quantity"]),
            )
            for item in cast(list[dict[str, object]], values["fills"])
        ),
        costs=tuple(
            StrategyBacktestCost(
                cast(str, item["name"]),
                Decimal(cast(str, item["amount"])),
                cast(str, item["currency"]),
            )
            for item in cast(list[dict[str, object]], values["costs"])
        ),
        net_return=Decimal(cast(str, values["net_return"])),
        net_pnl=Decimal(cast(str, values["net_pnl"])),
        portfolio_net_return=Decimal(cast(str, values["portfolio_net_return"])),
        max_drawdown=_decimal_or_none(values["max_drawdown"]),
        cvar95=_decimal_or_none(values["cvar95"]),
        sharpe=_decimal_or_none(values["sharpe"]),
        sortino=_decimal_or_none(values["sortino"]),
        turnover=Decimal(cast(str, values["turnover"])),
        adverse_excursion=Decimal(cast(str, values["adverse_excursion"])),
        liquidity_cost=Decimal(cast(str, values["liquidity_cost"])),
        stressed_net_return=_decimal_or_none(values["stressed_net_return"]),
        stress_evidence_artifact_hash=cast(str | None, values["stress_evidence_artifact_hash"]),
        missing_metrics=tuple(
            StrategyBacktestMissingMetric(cast(str, item["name"]), cast(str, item["reason"]))
            for item in cast(list[dict[str, object]], values["missing_metrics"])
        ),
    )


def _decimal_or_none(value: object) -> Decimal | None:
    return None if value is None else Decimal(cast(str, value))


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


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
