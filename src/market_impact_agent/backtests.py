from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import TYPE_CHECKING, Protocol, cast

from market_impact_agent.domain import SignalIntent, require_aware

if TYPE_CHECKING:
    from market_impact_agent.data_inputs import LocalDataSnapshotStore

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
BACKTEST_REQUEST_SCHEMA_VERSION = "market-impact.backtest-request.v1"
BACKTEST_RESULT_SCHEMA_VERSION = "market-impact.backtest-result.v1"
STRATEGY_BACKTEST_OUTCOME_SCHEMA_VERSION = "market-impact.strategy-backtest-outcome.v1"

_NAUTILUS_OUTCOME_PRODUCER_TOKEN = object()


class BacktestRunStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


class StrategyBacktestArm(StrEnum):
    CANDIDATE = "candidate"
    PRIMARY_BASELINE = "primary_baseline"


@dataclass(frozen=True, slots=True)
class StrategyBacktestRequestTemplate:
    """Case-independent executable request fields frozen before validation."""

    market: str
    instrument_ids: tuple[str, ...]
    horizons_sessions: tuple[int, ...]
    signal_side: str

    def __post_init__(self) -> None:
        _require_nonempty(self.market, "strategy request template market")
        if self.signal_side not in {"buy", "sell"}:
            raise ValueError("strategy request template signal side is invalid")
        _require_unique_nonempty(self.instrument_ids, "strategy request template instrument_ids")
        horizons = cast(tuple[object, ...], self.horizons_sessions)
        if (
            not horizons
            or any(not _is_positive_integer(value) for value in horizons)
            or self.horizons_sessions != tuple(sorted(set(self.horizons_sessions)))
        ):
            raise ValueError(
                "strategy request template horizons must be positive, unique, and ascending"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "instrument_ids": list(self.instrument_ids),
            "horizons_sessions": list(self.horizons_sessions),
            "signal_side": self.signal_side,
        }

    @classmethod
    def from_request(cls, request: BacktestRequest) -> StrategyBacktestRequestTemplate:
        return cls(
            market=request.market,
            instrument_ids=request.instrument_ids,
            horizons_sessions=request.horizons_sessions,
            signal_side=request.signal.side.value,
        )


@dataclass(frozen=True, slots=True)
class StrategyBacktestVariant:
    """Frozen executable identity for one registered comparison arm."""

    strategy_variant_hash: str
    arm: StrategyBacktestArm
    baseline_id: str | None
    strategy_ref: str
    target_selection_ref: str
    request_market: str
    request_instrument_ids: tuple[str, ...]
    request_horizons_sessions: tuple[int, ...]
    request_signal_side: str
    data_granularity: str
    book_type: str
    fill_model: str
    fee_model: str
    venue_ruleset: str
    base_currency: str
    starting_cash: Decimal
    random_seed: int

    def __post_init__(self) -> None:
        if self.arm is StrategyBacktestArm.CANDIDATE:
            if self.baseline_id is not None:
                raise ValueError("candidate strategy variant cannot name a baseline")
        elif self.baseline_id is None:
            raise ValueError("baseline strategy variant requires its frozen baseline ID")
        for name in (
            "strategy_ref",
            "target_selection_ref",
            "data_granularity",
            "book_type",
            "fill_model",
            "fee_model",
            "venue_ruleset",
            "base_currency",
        ):
            _require_nonempty(getattr(self, name), f"strategy variant {name}")
        if self.baseline_id is not None:
            _require_nonempty(self.baseline_id, "strategy variant baseline_id")
        _request_template = self.request_template
        if not self.starting_cash.is_finite() or self.starting_cash <= 0:
            raise ValueError("strategy variant starting_cash must be finite and positive")
        if not _is_non_negative_integer(cast(object, self.random_seed)):
            raise ValueError("strategy variant random_seed must be a non-negative integer")
        _require_sha256(self.strategy_variant_hash, "strategy_variant_hash")
        if self.strategy_variant_hash != _canonical_sha256(self.core_dict()):
            raise ValueError("strategy variant hash does not match frozen content")

    @property
    def configuration_hash(self) -> str:
        return _canonical_sha256(self.configuration_dict())

    def configuration_dict(self) -> dict[str, object]:
        return {
            "strategy_ref": self.strategy_ref,
            "request_template": self.request_template.to_dict(),
            "simulation": {
                "data_granularity": self.data_granularity,
                "book_type": self.book_type,
                "fill_model": self.fill_model,
                "fee_model": self.fee_model,
                "venue_ruleset": self.venue_ruleset,
                "base_currency": self.base_currency,
                "starting_cash": _canonical_decimal(self.starting_cash),
                "random_seed": self.random_seed,
            },
        }

    def core_dict(self) -> dict[str, object]:
        return {
            "arm": self.arm.value,
            "baseline_id": self.baseline_id,
            "target_selection_ref": self.target_selection_ref,
            **self.configuration_dict(),
        }

    @property
    def request_template(self) -> StrategyBacktestRequestTemplate:
        return StrategyBacktestRequestTemplate(
            market=self.request_market,
            instrument_ids=self.request_instrument_ids,
            horizons_sessions=self.request_horizons_sessions,
            signal_side=self.request_signal_side,
        )

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "strategy_variant_hash": self.strategy_variant_hash}

    @classmethod
    def build(
        cls,
        *,
        arm: StrategyBacktestArm,
        baseline_id: str | None,
        strategy_ref: str,
        target_selection_ref: str,
        request_template: StrategyBacktestRequestTemplate,
        simulation: SimulationSpec,
    ) -> StrategyBacktestVariant:
        configuration = {
            "strategy_ref": strategy_ref,
            "request_template": request_template.to_dict(),
            "simulation": _simulation_dict(simulation),
        }
        core: dict[str, object] = {
            "arm": arm.value,
            "baseline_id": baseline_id,
            "target_selection_ref": target_selection_ref,
            **configuration,
        }
        return cls(
            strategy_variant_hash=_canonical_sha256(core),
            arm=arm,
            baseline_id=baseline_id,
            strategy_ref=strategy_ref,
            target_selection_ref=target_selection_ref,
            request_market=request_template.market,
            request_instrument_ids=request_template.instrument_ids,
            request_horizons_sessions=request_template.horizons_sessions,
            request_signal_side=request_template.signal_side,
            data_granularity=simulation.data_granularity,
            book_type=simulation.book_type,
            fill_model=simulation.fill_model,
            fee_model=simulation.fee_model,
            venue_ruleset=simulation.venue_ruleset,
            base_currency=simulation.base_currency,
            starting_cash=simulation.starting_cash,
            random_seed=simulation.random_seed,
        )

    def matches_request(self, request: BacktestRequest) -> bool:
        return self.configuration_dict() == {
            "strategy_ref": request.strategy_ref,
            "request_template": StrategyBacktestRequestTemplate.from_request(request).to_dict(),
            "simulation": _simulation_dict(request.simulation),
        }


@dataclass(frozen=True, slots=True)
class StrategyBacktestOutcomeMissing:
    case_id: str
    arm: StrategyBacktestArm
    strategy_variant_hash: str
    reason: str

    def __post_init__(self) -> None:
        _require_nonempty(self.case_id, "missing strategy outcome case_id")
        _require_sha256(self.strategy_variant_hash, "strategy_variant_hash")
        _require_nonempty(self.reason, "missing strategy outcome reason")


@dataclass(frozen=True, slots=True)
class StrategyBacktestFill:
    side: str
    filled_at: datetime
    quantity: Decimal
    price: Decimal
    commission: Decimal
    available_liquidity_quantity: Decimal | None

    def __post_init__(self) -> None:
        if self.side not in {"buy", "sell"}:
            raise ValueError("strategy backtest fill side is invalid")
        require_aware(self.filled_at, "strategy backtest fill filled_at")
        for name in ("quantity", "price"):
            value = getattr(self, name)
            if not value.is_finite() or value <= 0:
                raise ValueError(f"strategy backtest fill {name} must be finite and positive")
        if not self.commission.is_finite() or self.commission < 0:
            raise ValueError("strategy backtest fill commission must be finite and non-negative")
        if self.available_liquidity_quantity is not None and (
            not self.available_liquidity_quantity.is_finite()
            or self.available_liquidity_quantity < self.quantity
        ):
            raise ValueError(
                "strategy backtest fill available liquidity must cover the executed quantity"
            )

    def to_dict(self) -> dict[str, str | None]:
        return {
            "side": self.side,
            "filled_at": _canonical_timestamp(self.filled_at),
            "quantity": _canonical_decimal(self.quantity),
            "price": _canonical_decimal(self.price),
            "commission": _canonical_decimal(self.commission),
            "available_liquidity_quantity": _optional_decimal(self.available_liquidity_quantity),
        }


@dataclass(frozen=True, slots=True)
class StrategyCapitalPoint:
    observed_at: datetime
    equity: Decimal

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "strategy capital point observed_at")
        if not self.equity.is_finite() or self.equity <= 0:
            raise ValueError("strategy capital point equity must be finite and positive")

    def to_dict(self) -> dict[str, str]:
        return {
            "observed_at": _canonical_timestamp(self.observed_at),
            "equity": _canonical_decimal(self.equity),
        }


@dataclass(frozen=True, slots=True)
class StrategyAdverseExcursionPoint:
    observed_at: datetime
    adverse_excursion: Decimal

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "strategy adverse excursion point observed_at")
        if not self.adverse_excursion.is_finite() or self.adverse_excursion < 0:
            raise ValueError("strategy adverse excursion point must be finite and non-negative")

    def to_dict(self) -> dict[str, str]:
        return {
            "observed_at": _canonical_timestamp(self.observed_at),
            "adverse_excursion": _canonical_decimal(self.adverse_excursion),
        }


@dataclass(frozen=True, slots=True)
class StrategyBacktestCost:
    name: str
    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        _require_nonempty(self.name, "strategy backtest cost name")
        _require_nonempty(self.currency, "strategy backtest cost currency")
        if not self.amount.is_finite() or self.amount < 0:
            raise ValueError("strategy backtest cost amount must be finite and non-negative")

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "amount": _canonical_decimal(self.amount),
            "currency": self.currency,
        }


@dataclass(frozen=True, slots=True)
class StrategyBacktestMissingMetric:
    name: str
    reason: str

    def __post_init__(self) -> None:
        _require_nonempty(self.name, "missing strategy metric name")
        _require_nonempty(self.reason, "missing strategy metric reason")

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "reason": self.reason}


@dataclass(frozen=True, slots=True)
class StrategyBacktestOutcomeReceipt:
    """Promotion evidence emitted only by the configured Nautilus bridge.

    A legacy ``BacktestResult`` remains replayable, but it has no same-root
    authority row and therefore cannot be reopened as this receipt.
    """

    receipt_id: str
    harness_authority_id: str
    case_id: str
    arm: StrategyBacktestArm
    strategy_variant_hash: str
    strategy_ref: str
    target_selection_ref: str
    engine_config_hash: str
    simulation_data_granularity: str
    simulation_book_type: str
    simulation_fill_model: str
    simulation_fee_model: str
    simulation_venue_ruleset: str
    simulation_base_currency: str
    simulation_starting_cash: Decimal
    simulation_random_seed: int
    result_hash: str
    result_artifact_hash: str
    manifest_hash: str
    request_hash: str
    input_hashes: tuple[BacktestInputHash, ...]
    source_snapshot_id: str
    source_snapshot_artifact_hash: str
    universe_hash: str
    cost_model_hash: str
    fill_model_hash: str
    capital_path: tuple[StrategyCapitalPoint, ...]
    adverse_excursion_path: tuple[StrategyAdverseExcursionPoint, ...]
    fills: tuple[StrategyBacktestFill, ...]
    costs: tuple[StrategyBacktestCost, ...]
    net_return: Decimal
    net_pnl: Decimal
    portfolio_net_return: Decimal
    max_drawdown: Decimal | None
    cvar95: Decimal | None
    sharpe: Decimal | None
    sortino: Decimal | None
    turnover: Decimal
    adverse_excursion: Decimal
    liquidity_cost: Decimal
    stressed_net_return: Decimal | None
    stress_evidence_artifact_hash: str | None
    missing_metrics: tuple[StrategyBacktestMissingMetric, ...]
    schema_version: str = STRATEGY_BACKTEST_OUTCOME_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.harness_authority_id.startswith("harness-authority-"):
            raise ValueError("strategy backtest receipt requires a Harness authority")
        _require_nonempty(self.case_id, "strategy backtest receipt case_id")
        for name in (
            "strategy_variant_hash",
            "engine_config_hash",
            "result_hash",
            "result_artifact_hash",
            "manifest_hash",
            "request_hash",
            "source_snapshot_artifact_hash",
            "universe_hash",
            "cost_model_hash",
            "fill_model_hash",
        ):
            _require_sha256(getattr(self, name), name)
        for name in (
            "strategy_ref",
            "target_selection_ref",
            "simulation_data_granularity",
            "simulation_book_type",
            "simulation_fill_model",
            "simulation_fee_model",
            "simulation_venue_ruleset",
            "simulation_base_currency",
        ):
            _require_nonempty(getattr(self, name), name)
        if not self.simulation_starting_cash.is_finite() or self.simulation_starting_cash <= 0:
            raise ValueError("receipt simulation_starting_cash must be finite and positive")
        if not _is_non_negative_integer(cast(object, self.simulation_random_seed)):
            raise ValueError("receipt simulation_random_seed must be a non-negative integer")
        if self.stress_evidence_artifact_hash is not None:
            _require_sha256(self.stress_evidence_artifact_hash, "stress_evidence_artifact_hash")
        _require_nonempty(self.source_snapshot_id, "strategy receipt source_snapshot_id")
        expected_fill_sides = () if self.strategy_ref == "cash-no-action.v1" else ("buy", "sell")
        if tuple(item.side for item in self.fills) != expected_fill_sides:
            raise ValueError("strategy backtest receipt fills differ from executable strategy")
        if self.strategy_ref == "cash-no-action.v1" and self.costs:
            raise ValueError("cash-no-action strategy cannot report execution costs")
        if len(self.capital_path) < 2:
            raise ValueError("strategy backtest receipt requires a capital path")
        if tuple(item.observed_at for item in self.capital_path) != tuple(
            sorted(item.observed_at for item in self.capital_path)
        ):
            raise ValueError("strategy backtest capital path must be chronological")
        if tuple(item.observed_at for item in self.adverse_excursion_path) != tuple(
            sorted(item.observed_at for item in self.adverse_excursion_path)
        ):
            raise ValueError("strategy backtest adverse excursion path must be chronological")
        if any(
            previous.observed_at >= current.observed_at
            for previous, current in zip(
                self.adverse_excursion_path,
                self.adverse_excursion_path[1:],
                strict=False,
            )
        ):
            raise ValueError(
                "strategy backtest adverse excursion path must be strictly chronological"
            )
        if any(
            not self.capital_path[0].observed_at
            <= item.filled_at
            <= self.capital_path[-1].observed_at
            for item in self.fills
        ) or any(
            not self.capital_path[0].observed_at
            <= item.observed_at
            <= self.capital_path[-1].observed_at
            for item in self.adverse_excursion_path
        ):
            raise ValueError("strategy fill or adverse observation falls outside its capital path")
        if tuple(item.name for item in self.costs) != tuple(
            sorted(item.name for item in self.costs)
        ):
            raise ValueError("strategy backtest costs must use canonical name order")
        missing_names = tuple(item.name for item in self.missing_metrics)
        if missing_names != tuple(sorted(set(missing_names))):
            raise ValueError("missing strategy metrics must be unique and sorted")
        liquidity_missing = any(item.available_liquidity_quantity is None for item in self.fills)
        if liquidity_missing != ("liquidity" in missing_names):
            raise ValueError("strategy liquidity missing state is inconsistent")
        if (not self.adverse_excursion_path) != ("adverse_excursion_path" in missing_names):
            raise ValueError("strategy adverse excursion path missing state is inconsistent")
        optional_metrics = {
            "max_drawdown": self.max_drawdown,
            "cvar95": self.cvar95,
            "sharpe": self.sharpe,
            "sortino": self.sortino,
            "stressed_net_return": self.stressed_net_return,
        }
        for name, value in optional_metrics.items():
            if (value is None) != (name in missing_names):
                raise ValueError(f"strategy metric {name} missing state is inconsistent")
            if value is not None and not value.is_finite():
                raise ValueError(f"strategy metric {name} must be finite")
        for name in (
            "net_return",
            "net_pnl",
            "portfolio_net_return",
            "turnover",
            "adverse_excursion",
            "liquidity_cost",
        ):
            if not getattr(self, name).is_finite():
                raise ValueError(f"strategy metric {name} must be finite")
        for name in ("max_drawdown", "cvar95", "turnover", "adverse_excursion", "liquidity_cost"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"strategy metric {name} must be non-negative")
        if self.adverse_excursion_path and self.adverse_excursion != max(
            item.adverse_excursion for item in self.adverse_excursion_path
        ):
            raise ValueError("strategy adverse excursion differs from its marked path")
        if self.receipt_id != self.expected_receipt_id:
            raise ValueError("strategy backtest receipt identity does not match content")

    @property
    def receipt_hash(self) -> str:
        return self.receipt_id.removeprefix("strategy-backtest-outcome-")

    @property
    def expected_receipt_id(self) -> str:
        return f"strategy-backtest-outcome-{_canonical_sha256(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "harness_authority_id": self.harness_authority_id,
            "case_id": self.case_id,
            "arm": self.arm.value,
            "strategy_variant_hash": self.strategy_variant_hash,
            "strategy_ref": self.strategy_ref,
            "target_selection_ref": self.target_selection_ref,
            "engine_config_hash": self.engine_config_hash,
            "simulation": {
                "data_granularity": self.simulation_data_granularity,
                "book_type": self.simulation_book_type,
                "fill_model": self.simulation_fill_model,
                "fee_model": self.simulation_fee_model,
                "venue_ruleset": self.simulation_venue_ruleset,
                "base_currency": self.simulation_base_currency,
                "starting_cash": _canonical_decimal(self.simulation_starting_cash),
                "random_seed": self.simulation_random_seed,
            },
            "result_hash": self.result_hash,
            "result_artifact_hash": self.result_artifact_hash,
            "manifest_hash": self.manifest_hash,
            "request_hash": self.request_hash,
            "input_hashes": [
                {"name": item.name, "value": item.value} for item in self.input_hashes
            ],
            "source_snapshot_id": self.source_snapshot_id,
            "source_snapshot_artifact_hash": self.source_snapshot_artifact_hash,
            "universe_hash": self.universe_hash,
            "cost_model_hash": self.cost_model_hash,
            "fill_model_hash": self.fill_model_hash,
            "capital_path": [item.to_dict() for item in self.capital_path],
            "adverse_excursion_path": [item.to_dict() for item in self.adverse_excursion_path],
            "fills": [item.to_dict() for item in self.fills],
            "costs": [item.to_dict() for item in self.costs],
            "net_return": _canonical_decimal(self.net_return),
            "net_pnl": _canonical_decimal(self.net_pnl),
            "portfolio_net_return": _canonical_decimal(self.portfolio_net_return),
            "max_drawdown": _optional_decimal(self.max_drawdown),
            "cvar95": _optional_decimal(self.cvar95),
            "sharpe": _optional_decimal(self.sharpe),
            "sortino": _optional_decimal(self.sortino),
            "turnover": _canonical_decimal(self.turnover),
            "adverse_excursion": _canonical_decimal(self.adverse_excursion),
            "liquidity_cost": _canonical_decimal(self.liquidity_cost),
            "stressed_net_return": _optional_decimal(self.stressed_net_return),
            "stress_evidence_artifact_hash": self.stress_evidence_artifact_hash,
            "missing_metrics": [item.to_dict() for item in self.missing_metrics],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "receipt_id": self.receipt_id}


@dataclass(frozen=True, slots=True)
class SimulationSpec:
    data_granularity: str
    book_type: str
    fill_model: str
    fee_model: str
    venue_ruleset: str
    base_currency: str
    starting_cash: Decimal
    random_seed: int

    def __post_init__(self) -> None:
        for name in (
            "data_granularity",
            "book_type",
            "fill_model",
            "fee_model",
            "venue_ruleset",
            "base_currency",
        ):
            _require_nonempty(getattr(self, name), name)
        if not self.starting_cash.is_finite() or self.starting_cash <= 0:
            raise ValueError("starting_cash must be finite and positive")
        if not _is_non_negative_integer(cast(object, self.random_seed)):
            raise ValueError("random_seed must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class BacktestRequest:
    request_id: str
    signal: SignalIntent
    as_of: datetime
    start_at: datetime
    end_at: datetime
    market: str
    instrument_ids: tuple[str, ...]
    data_snapshot_id: str
    target_selection_ref: str
    strategy_ref: str
    horizons_sessions: tuple[int, ...]
    simulation: SimulationSpec

    def __post_init__(self) -> None:
        for name in (
            "request_id",
            "market",
            "data_snapshot_id",
            "target_selection_ref",
            "strategy_ref",
        ):
            _require_nonempty(getattr(self, name), name)
        for name in ("as_of", "start_at", "end_at"):
            require_aware(getattr(self, name), name)
        if self.start_at < self.as_of:
            raise ValueError("start_at must not be before as_of")
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be after start_at")
        _require_unique_nonempty(self.instrument_ids, "instrument_ids")
        if self.signal.instrument_id not in self.instrument_ids:
            raise ValueError("signal instrument_id must belong to the request instrument_ids")
        if not self.signal.valid_from <= self.as_of < self.signal.expires_at:
            raise ValueError("as_of must be within signal validity")
        if not self.signal.valid_from <= self.start_at < self.signal.expires_at:
            raise ValueError("start_at must be within signal validity")
        horizons = cast(tuple[object, ...], self.horizons_sessions)
        if (
            not horizons
            or any(not _is_positive_integer(value) for value in horizons)
            or self.horizons_sessions != tuple(sorted(set(self.horizons_sessions)))
        ):
            raise ValueError("horizons_sessions must be positive, unique, and ascending")


@dataclass(frozen=True, slots=True)
class BacktestInputHash:
    name: str
    value: str

    def __post_init__(self) -> None:
        _require_nonempty(self.name, "input hash name")
        _require_sha256(self.value, "input hash value")


@dataclass(frozen=True, slots=True)
class BacktestRunManifest:
    run_id: str
    request: BacktestRequest
    request_hash: str
    engine_name: str
    engine_version: str
    bridge_name: str
    bridge_version: str
    data_adapter_name: str
    data_adapter_version: str
    input_hashes: tuple[BacktestInputHash, ...]
    engine_config_hash: str
    executed_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "engine_name",
            "engine_version",
            "bridge_name",
            "bridge_version",
            "data_adapter_name",
            "data_adapter_version",
        ):
            _require_nonempty(getattr(self, name), name)
        _require_sha256(self.request_hash, "request_hash")
        _require_sha256(self.engine_config_hash, "engine_config_hash")
        input_names = tuple(item.name for item in self.input_hashes)
        _require_unique_nonempty(input_names, "input hash names")
        if input_names != tuple(sorted(input_names)):
            raise ValueError("input_hashes must use canonical name order")
        require_aware(self.executed_at, "executed_at")
        if self.request_hash != canonical_backtest_request_hash(self.request):
            raise ValueError("request_hash must match canonical request content")


@dataclass(frozen=True, slots=True)
class BacktestMetric:
    name: str
    value: Decimal
    unit: str

    def __post_init__(self) -> None:
        _require_nonempty(self.name, "metric name")
        _require_nonempty(self.unit, "metric unit")
        if not self.value.is_finite():
            raise ValueError("metric value must be finite")


@dataclass(frozen=True, slots=True)
class BacktestResult:
    manifest: BacktestRunManifest
    status: BacktestRunStatus
    result_hash: str
    metrics: tuple[BacktestMetric, ...]
    artifact_refs: tuple[str, ...]
    failure_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_sha256(self.result_hash, "result_hash")
        if self.status is BacktestRunStatus.COMPLETED:
            if self.failure_reasons:
                raise ValueError("completed results cannot include failure_reasons")
        elif not self.failure_reasons:
            raise ValueError("failed results require failure_reasons")

        metric_names = tuple(metric.name for metric in self.metrics)
        _require_unique(metric_names, "metric names")
        _require_unique_nonempty(self.artifact_refs, "artifact_refs", allow_empty=True)
        _require_unique_nonempty(self.failure_reasons, "failure_reasons", allow_empty=True)
        if self.result_hash != canonical_backtest_result_hash(
            manifest=self.manifest,
            status=self.status,
            metrics=self.metrics,
            artifact_refs=self.artifact_refs,
            failure_reasons=self.failure_reasons,
        ):
            raise ValueError("result_hash must match canonical result content")


class BacktestBridge(Protocol):
    def run(self, request: BacktestRequest) -> BacktestResult: ...


def canonical_backtest_request_hash(request: BacktestRequest) -> str:
    """Return the stable identity for an engine-neutral backtest request."""
    return _canonical_sha256(
        {
            "as_of": _canonical_timestamp(request.as_of),
            "data_snapshot_id": request.data_snapshot_id,
            "end_at": _canonical_timestamp(request.end_at),
            "horizons_sessions": list(request.horizons_sessions),
            "instrument_ids": sorted(request.instrument_ids),
            "market": request.market,
            "request_id": request.request_id,
            "signal": {
                "event_id": request.signal.event_id,
                "evidence_refs": sorted(request.signal.evidence_refs),
                "expires_at": _canonical_timestamp(request.signal.expires_at),
                "invalidation_conditions": sorted(request.signal.invalidation_conditions),
                "instrument_id": request.signal.instrument_id,
                "side": request.signal.side.value,
                "signal_id": request.signal.signal_id,
                "valid_from": _canonical_timestamp(request.signal.valid_from),
            },
            "simulation": {
                "base_currency": request.simulation.base_currency,
                "book_type": request.simulation.book_type,
                "data_granularity": request.simulation.data_granularity,
                "fee_model": request.simulation.fee_model,
                "fill_model": request.simulation.fill_model,
                "random_seed": request.simulation.random_seed,
                "starting_cash": _canonical_decimal(request.simulation.starting_cash),
                "venue_ruleset": request.simulation.venue_ruleset,
            },
            "start_at": _canonical_timestamp(request.start_at),
            "strategy_ref": request.strategy_ref,
            "target_selection_ref": request.target_selection_ref,
        }
    )


def canonical_backtest_result_hash(
    *,
    manifest: BacktestRunManifest,
    status: BacktestRunStatus,
    metrics: tuple[BacktestMetric, ...],
    artifact_refs: tuple[str, ...],
    failure_reasons: tuple[str, ...],
) -> str:
    """Return replay identity without per-run metadata such as run_id or execution time."""
    return _canonical_sha256(
        {
            "artifact_refs": sorted(artifact_refs),
            "engine": {
                "bridge_name": manifest.bridge_name,
                "bridge_version": manifest.bridge_version,
                "data_adapter_name": manifest.data_adapter_name,
                "data_adapter_version": manifest.data_adapter_version,
                "engine_config_hash": manifest.engine_config_hash,
                "engine_name": manifest.engine_name,
                "engine_version": manifest.engine_version,
                "input_hashes": [
                    {"name": item.name, "value": item.value} for item in manifest.input_hashes
                ],
            },
            "failure_reasons": sorted(failure_reasons),
            "metrics": [
                {
                    "name": metric.name,
                    "unit": metric.unit,
                    "value": _canonical_decimal(metric.value),
                }
                for metric in sorted(metrics, key=lambda metric: metric.name)
            ],
            "request_hash": manifest.request_hash,
            "status": status.value,
        }
    )


def backtest_request_to_dict(request: BacktestRequest) -> dict[str, object]:
    return {
        "schema_version": BACKTEST_REQUEST_SCHEMA_VERSION,
        "request_id": request.request_id,
        "signal": {
            "schema_version": "market-impact.signal-intent.v1",
            "signal_id": request.signal.signal_id,
            "event_id": request.signal.event_id,
            "instrument_id": request.signal.instrument_id,
            "side": request.signal.side.value,
            "valid_from": _canonical_timestamp(request.signal.valid_from),
            "expires_at": _canonical_timestamp(request.signal.expires_at),
            "evidence_refs": list(request.signal.evidence_refs),
            "invalidation_conditions": list(request.signal.invalidation_conditions),
        },
        "as_of": _canonical_timestamp(request.as_of),
        "start_at": _canonical_timestamp(request.start_at),
        "end_at": _canonical_timestamp(request.end_at),
        "market": request.market,
        "instrument_ids": list(request.instrument_ids),
        "data_snapshot_id": request.data_snapshot_id,
        "target_selection_ref": request.target_selection_ref,
        "strategy_ref": request.strategy_ref,
        "horizons_sessions": list(request.horizons_sessions),
        "simulation": {
            "data_granularity": request.simulation.data_granularity,
            "book_type": request.simulation.book_type,
            "fill_model": request.simulation.fill_model,
            "fee_model": request.simulation.fee_model,
            "venue_ruleset": request.simulation.venue_ruleset,
            "base_currency": request.simulation.base_currency,
            "starting_cash": _canonical_decimal(request.simulation.starting_cash),
            "random_seed": request.simulation.random_seed,
        },
    }


def backtest_request_from_dict(payload: object) -> BacktestRequest:
    fields = _object(payload, "Backtest Request")
    _require_exact_keys(
        fields,
        {
            "as_of",
            "data_snapshot_id",
            "end_at",
            "horizons_sessions",
            "instrument_ids",
            "market",
            "request_id",
            "schema_version",
            "signal",
            "simulation",
            "start_at",
            "strategy_ref",
            "target_selection_ref",
        },
        "Backtest Request",
    )
    if _string(fields, "schema_version") != BACKTEST_REQUEST_SCHEMA_VERSION:
        raise ValueError("unsupported Backtest Request schema_version")
    signal_fields = _object(fields.get("signal"), "Signal Intent")
    _require_exact_keys(
        signal_fields,
        {
            "event_id",
            "evidence_refs",
            "expires_at",
            "instrument_id",
            "invalidation_conditions",
            "schema_version",
            "side",
            "signal_id",
            "valid_from",
        },
        "Signal Intent",
    )
    if _string(signal_fields, "schema_version") != "market-impact.signal-intent.v1":
        raise ValueError("unsupported Signal Intent schema_version")
    simulation_fields = _object(fields.get("simulation"), "Simulation Specification")
    _require_exact_keys(
        simulation_fields,
        {
            "base_currency",
            "book_type",
            "data_granularity",
            "fee_model",
            "fill_model",
            "random_seed",
            "starting_cash",
            "venue_ruleset",
        },
        "Simulation Specification",
    )
    from market_impact_agent.domain import Side, SignalIntent

    signal = SignalIntent(
        signal_id=_string(signal_fields, "signal_id"),
        event_id=_string(signal_fields, "event_id"),
        instrument_id=_string(signal_fields, "instrument_id"),
        side=Side(_string(signal_fields, "side")),
        valid_from=_parse_timestamp(signal_fields, "valid_from"),
        expires_at=_parse_timestamp(signal_fields, "expires_at"),
        evidence_refs=_string_tuple(signal_fields, "evidence_refs"),
        invalidation_conditions=_string_tuple(signal_fields, "invalidation_conditions"),
    )
    return BacktestRequest(
        request_id=_string(fields, "request_id"),
        signal=signal,
        as_of=_parse_timestamp(fields, "as_of"),
        start_at=_parse_timestamp(fields, "start_at"),
        end_at=_parse_timestamp(fields, "end_at"),
        market=_string(fields, "market"),
        instrument_ids=_string_tuple(fields, "instrument_ids"),
        data_snapshot_id=_string(fields, "data_snapshot_id"),
        target_selection_ref=_string(fields, "target_selection_ref"),
        strategy_ref=_string(fields, "strategy_ref"),
        horizons_sessions=_integer_tuple(fields, "horizons_sessions"),
        simulation=SimulationSpec(
            data_granularity=_string(simulation_fields, "data_granularity"),
            book_type=_string(simulation_fields, "book_type"),
            fill_model=_string(simulation_fields, "fill_model"),
            fee_model=_string(simulation_fields, "fee_model"),
            venue_ruleset=_string(simulation_fields, "venue_ruleset"),
            base_currency=_string(simulation_fields, "base_currency"),
            starting_cash=_decimal_string(simulation_fields, "starting_cash"),
            random_seed=_integer(simulation_fields, "random_seed"),
        ),
    )


def backtest_result_to_dict(result: BacktestResult) -> dict[str, object]:
    manifest = result.manifest
    return {
        "schema_version": BACKTEST_RESULT_SCHEMA_VERSION,
        "manifest": {
            "run_id": manifest.run_id,
            "request": backtest_request_to_dict(manifest.request),
            "request_hash": manifest.request_hash,
            "engine_name": manifest.engine_name,
            "engine_version": manifest.engine_version,
            "bridge_name": manifest.bridge_name,
            "bridge_version": manifest.bridge_version,
            "data_adapter_name": manifest.data_adapter_name,
            "data_adapter_version": manifest.data_adapter_version,
            "input_hashes": [
                {"name": item.name, "value": item.value} for item in manifest.input_hashes
            ],
            "engine_config_hash": manifest.engine_config_hash,
            "executed_at": _canonical_timestamp(manifest.executed_at),
        },
        "status": result.status.value,
        "result_hash": result.result_hash,
        "metrics": [
            {"name": item.name, "value": _canonical_decimal(item.value), "unit": item.unit}
            for item in result.metrics
        ],
        "artifact_refs": list(result.artifact_refs),
        "failure_reasons": list(result.failure_reasons),
    }


def backtest_result_from_dict(payload: object) -> BacktestResult:
    fields = _object(payload, "Backtest Result")
    _require_exact_keys(
        fields,
        {
            "artifact_refs",
            "failure_reasons",
            "manifest",
            "metrics",
            "result_hash",
            "schema_version",
            "status",
        },
        "Backtest Result",
    )
    if _string(fields, "schema_version") != BACKTEST_RESULT_SCHEMA_VERSION:
        raise ValueError("unsupported Backtest Result schema_version")
    manifest_fields = _object(fields.get("manifest"), "Backtest Run Manifest")
    _require_exact_keys(
        manifest_fields,
        {
            "bridge_name",
            "bridge_version",
            "data_adapter_name",
            "data_adapter_version",
            "engine_config_hash",
            "engine_name",
            "engine_version",
            "executed_at",
            "input_hashes",
            "request",
            "request_hash",
            "run_id",
        },
        "Backtest Run Manifest",
    )
    input_hashes = tuple(
        BacktestInputHash(name=_string(item, "name"), value=_string(item, "value"))
        for item in _object_tuple(manifest_fields, "input_hashes", {"name", "value"})
    )
    manifest = BacktestRunManifest(
        run_id=_string(manifest_fields, "run_id"),
        request=backtest_request_from_dict(manifest_fields.get("request")),
        request_hash=_string(manifest_fields, "request_hash"),
        engine_name=_string(manifest_fields, "engine_name"),
        engine_version=_string(manifest_fields, "engine_version"),
        bridge_name=_string(manifest_fields, "bridge_name"),
        bridge_version=_string(manifest_fields, "bridge_version"),
        data_adapter_name=_string(manifest_fields, "data_adapter_name"),
        data_adapter_version=_string(manifest_fields, "data_adapter_version"),
        input_hashes=input_hashes,
        engine_config_hash=_string(manifest_fields, "engine_config_hash"),
        executed_at=_parse_timestamp(manifest_fields, "executed_at"),
    )
    metrics = tuple(
        BacktestMetric(
            name=_string(item, "name"),
            value=_decimal_string(item, "value"),
            unit=_string(item, "unit"),
        )
        for item in _object_tuple(fields, "metrics", {"name", "unit", "value"})
    )
    return BacktestResult(
        manifest=manifest,
        status=BacktestRunStatus(_string(fields, "status")),
        result_hash=_string(fields, "result_hash"),
        metrics=metrics,
        artifact_refs=_string_tuple(fields, "artifact_refs", allow_empty=True),
        failure_reasons=_string_tuple(fields, "failure_reasons", allow_empty=True),
    )


def strategy_backtest_universe_hash(instrument_ids: tuple[str, ...]) -> str:
    _require_unique_nonempty(instrument_ids, "strategy backtest instrument_ids")
    return _canonical_sha256({"instrument_ids": sorted(instrument_ids)})


def _simulation_dict(simulation: SimulationSpec) -> dict[str, object]:
    return {
        "data_granularity": simulation.data_granularity,
        "book_type": simulation.book_type,
        "fill_model": simulation.fill_model,
        "fee_model": simulation.fee_model,
        "venue_ruleset": simulation.venue_ruleset,
        "base_currency": simulation.base_currency,
        "starting_cash": _canonical_decimal(simulation.starting_cash),
        "random_seed": simulation.random_seed,
    }


def strategy_backtest_cost_model_hash(simulation: SimulationSpec) -> str:
    return _canonical_sha256(
        {
            "base_currency": simulation.base_currency,
            "fee_model": simulation.fee_model,
        }
    )


def strategy_backtest_fill_model_hash(simulation: SimulationSpec) -> str:
    return _canonical_sha256(
        {
            "book_type": simulation.book_type,
            "data_granularity": simulation.data_granularity,
            "fill_model": simulation.fill_model,
            "venue_ruleset": simulation.venue_ruleset,
        }
    )


def _record_strategy_backtest_outcome(  # pyright: ignore[reportUnusedFunction]
    *,
    store: LocalDataSnapshotStore,
    receipt: StrategyBacktestOutcomeReceipt,
    result: BacktestResult,
    producer_token: object,
) -> None:
    """Private write boundary used only by ``NautilusBacktestBridge``."""

    if producer_token is not _NAUTILUS_OUTCOME_PRODUCER_TOKEN:
        raise PermissionError("strategy outcome receipts require the Nautilus producer")
    from market_impact_agent.data_inputs import LocalDataSnapshotStore

    if type(store) is not LocalDataSnapshotStore:
        raise TypeError("strategy outcome receipts require the concrete Harness store")
    if receipt.harness_authority_id != store.harness_authority_id:
        raise ValueError("strategy outcome receipt belongs to a different Harness authority")
    result_artifact = store.artifacts.put_json(backtest_result_to_dict(result))
    receipt_artifact = store.artifacts.put_json(receipt.to_dict())
    if result_artifact.content_hash != receipt.result_artifact_hash:
        raise ValueError("strategy outcome receipt result artifact differs from content")
    with store.authority_transaction() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_backtest_outcomes (
                receipt_id TEXT PRIMARY KEY,
                receipt_hash TEXT NOT NULL,
                artifact_hash TEXT NOT NULL UNIQUE,
                harness_authority_id TEXT NOT NULL,
                case_id TEXT NOT NULL,
                arm TEXT NOT NULL,
                strategy_variant_hash TEXT NOT NULL,
                result_hash TEXT NOT NULL,
                result_artifact_hash TEXT NOT NULL,
                source_snapshot_id TEXT NOT NULL,
                source_snapshot_artifact_hash TEXT NOT NULL
            )
            """
        )
        values = (
            receipt.receipt_id,
            receipt.receipt_hash,
            receipt_artifact.content_hash,
            receipt.harness_authority_id,
            receipt.case_id,
            receipt.arm.value,
            receipt.strategy_variant_hash,
            receipt.result_hash,
            receipt.result_artifact_hash,
            receipt.source_snapshot_id,
            receipt.source_snapshot_artifact_hash,
        )
        existing = connection.execute(
            "SELECT * FROM strategy_backtest_outcomes WHERE receipt_id = ?",
            (receipt.receipt_id,),
        ).fetchone()
        if existing is not None:
            if tuple(existing) != values:
                raise ValueError("strategy outcome receipt identity conflicts with durable content")
            return
        connection.execute(
            "INSERT INTO strategy_backtest_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )


def reopen_strategy_backtest_outcome(
    store: LocalDataSnapshotStore,
    receipt_id: str,
) -> tuple[StrategyBacktestOutcomeReceipt, BacktestResult]:
    """Reopen a receipt, its result, and its exact source Snapshot from one root."""

    from market_impact_agent.data_inputs import LocalDataSnapshotStore

    if type(store) is not LocalDataSnapshotStore:
        raise TypeError("strategy outcome receipts require the concrete Harness store")
    try:
        with store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM strategy_backtest_outcomes WHERE receipt_id = ?",
                (receipt_id,),
            ).fetchone()
    except sqlite3.OperationalError as exc:
        raise KeyError(f"unknown strategy backtest outcome receipt: {receipt_id}") from exc
    if row is None:
        raise KeyError(f"unknown strategy backtest outcome receipt: {receipt_id}")
    if row["harness_authority_id"] != store.harness_authority_id:
        raise PermissionError("strategy outcome receipt belongs to another Harness authority")
    receipt = strategy_backtest_outcome_from_dict(
        store.artifacts.read_json(cast(str, row["artifact_hash"]))
    )
    result = backtest_result_from_dict(store.artifacts.read_json(receipt.result_artifact_hash))
    source = store.get(receipt.source_snapshot_id)
    source_artifact = store.artifacts.put_json(source.to_dict())
    result_payload = backtest_result_to_dict(result)
    manifest_payload = cast(dict[str, object], result_payload["manifest"])
    result_metrics = {item.name: item.value for item in result.metrics}
    if (
        receipt.receipt_id != row["receipt_id"]
        or receipt.receipt_hash != row["receipt_hash"]
        or receipt.case_id != row["case_id"]
        or receipt.arm.value != row["arm"]
        or receipt.strategy_variant_hash != row["strategy_variant_hash"]
        or receipt.result_hash != row["result_hash"]
        or receipt.result_artifact_hash != row["result_artifact_hash"]
        or receipt.source_snapshot_id != row["source_snapshot_id"]
        or receipt.source_snapshot_artifact_hash != row["source_snapshot_artifact_hash"]
        or source_artifact.content_hash != receipt.source_snapshot_artifact_hash
        or result.result_hash != receipt.result_hash
        or result.manifest.request_hash != receipt.request_hash
        or result.manifest.engine_config_hash != receipt.engine_config_hash
        or result.manifest.input_hashes != receipt.input_hashes
        or _canonical_sha256(manifest_payload) != receipt.manifest_hash
        or result_metrics.get("net_return") != receipt.net_return
        or result_metrics.get("net_pnl") != receipt.net_pnl
        or strategy_backtest_universe_hash(result.manifest.request.instrument_ids)
        != receipt.universe_hash
        or strategy_backtest_cost_model_hash(result.manifest.request.simulation)
        != receipt.cost_model_hash
        or strategy_backtest_fill_model_hash(result.manifest.request.simulation)
        != receipt.fill_model_hash
        or result.manifest.request.strategy_ref != receipt.strategy_ref
        or result.manifest.request.target_selection_ref != receipt.target_selection_ref
        or _simulation_dict(result.manifest.request.simulation)
        != {
            "data_granularity": receipt.simulation_data_granularity,
            "book_type": receipt.simulation_book_type,
            "fill_model": receipt.simulation_fill_model,
            "fee_model": receipt.simulation_fee_model,
            "venue_ruleset": receipt.simulation_venue_ruleset,
            "base_currency": receipt.simulation_base_currency,
            "starting_cash": _canonical_decimal(receipt.simulation_starting_cash),
            "random_seed": receipt.simulation_random_seed,
        }
    ):
        raise ValueError("strategy outcome receipt differs from its authoritative owners")
    if receipt.stress_evidence_artifact_hash is not None:
        stress = store.artifacts.read_json(receipt.stress_evidence_artifact_hash)
        stress_fields = cast(dict[str, object], stress) if isinstance(stress, dict) else {}
        if (
            not isinstance(stress, dict)
            or stress_fields.get("schema_version")
            != "market-impact.strategy-backtest-stress-evidence.v1"
            or stress_fields.get("stress_kind") != "doubled_fee_actual_nautilus_run"
            or stress_fields.get("request_hash") != receipt.request_hash
            or Decimal(cast(str, stress_fields.get("net_return"))) != receipt.stressed_net_return
        ):
            raise ValueError("strategy outcome stress evidence is invalid")
    return receipt, result


def strategy_backtest_outcome_from_dict(payload: object) -> StrategyBacktestOutcomeReceipt:
    fields = _object(payload, "Strategy Backtest Outcome Receipt")
    input_hashes_raw = fields.get("input_hashes")
    capital_path_raw = fields.get("capital_path")
    adverse_excursion_path_raw = fields.get("adverse_excursion_path")
    fills_raw = fields.get("fills")
    costs_raw = fields.get("costs")
    missing_raw = fields.get("missing_metrics")
    if not all(
        isinstance(item, list)
        for item in (
            input_hashes_raw,
            capital_path_raw,
            adverse_excursion_path_raw,
            fills_raw,
            costs_raw,
            missing_raw,
        )
    ):
        raise ValueError("strategy backtest receipt arrays are invalid")
    return StrategyBacktestOutcomeReceipt(
        receipt_id=_string(fields, "receipt_id"),
        harness_authority_id=_string(fields, "harness_authority_id"),
        case_id=_string(fields, "case_id"),
        arm=StrategyBacktestArm(_string(fields, "arm")),
        strategy_variant_hash=_string(fields, "strategy_variant_hash"),
        strategy_ref=_string(fields, "strategy_ref"),
        target_selection_ref=_string(fields, "target_selection_ref"),
        engine_config_hash=_string(fields, "engine_config_hash"),
        simulation_data_granularity=_string(
            _object(fields.get("simulation"), "simulation"), "data_granularity"
        ),
        simulation_book_type=_string(_object(fields.get("simulation"), "simulation"), "book_type"),
        simulation_fill_model=_string(
            _object(fields.get("simulation"), "simulation"), "fill_model"
        ),
        simulation_fee_model=_string(_object(fields.get("simulation"), "simulation"), "fee_model"),
        simulation_venue_ruleset=_string(
            _object(fields.get("simulation"), "simulation"), "venue_ruleset"
        ),
        simulation_base_currency=_string(
            _object(fields.get("simulation"), "simulation"), "base_currency"
        ),
        simulation_starting_cash=_decimal_string(
            _object(fields.get("simulation"), "simulation"), "starting_cash"
        ),
        simulation_random_seed=_integer(
            _object(fields.get("simulation"), "simulation"), "random_seed"
        ),
        result_hash=_string(fields, "result_hash"),
        result_artifact_hash=_string(fields, "result_artifact_hash"),
        manifest_hash=_string(fields, "manifest_hash"),
        request_hash=_string(fields, "request_hash"),
        input_hashes=tuple(
            BacktestInputHash(
                name=_string(_object(item, "input hash"), "name"),
                value=_string(_object(item, "input hash"), "value"),
            )
            for item in cast(list[object], input_hashes_raw)
        ),
        source_snapshot_id=_string(fields, "source_snapshot_id"),
        source_snapshot_artifact_hash=_string(fields, "source_snapshot_artifact_hash"),
        universe_hash=_string(fields, "universe_hash"),
        cost_model_hash=_string(fields, "cost_model_hash"),
        fill_model_hash=_string(fields, "fill_model_hash"),
        capital_path=tuple(
            StrategyCapitalPoint(
                observed_at=_parse_timestamp(_object(item, "capital point"), "observed_at"),
                equity=_decimal_string(_object(item, "capital point"), "equity"),
            )
            for item in cast(list[object], capital_path_raw)
        ),
        adverse_excursion_path=tuple(
            StrategyAdverseExcursionPoint(
                observed_at=_parse_timestamp(
                    _object(item, "adverse excursion point"), "observed_at"
                ),
                adverse_excursion=_decimal_string(
                    _object(item, "adverse excursion point"), "adverse_excursion"
                ),
            )
            for item in cast(list[object], adverse_excursion_path_raw)
        ),
        fills=tuple(
            StrategyBacktestFill(
                side=_string(_object(item, "fill"), "side"),
                filled_at=_parse_timestamp(_object(item, "fill"), "filled_at"),
                quantity=_decimal_string(_object(item, "fill"), "quantity"),
                price=_decimal_string(_object(item, "fill"), "price"),
                commission=_decimal_string(_object(item, "fill"), "commission"),
                available_liquidity_quantity=_optional_decimal_field(
                    _object(item, "fill"), "available_liquidity_quantity"
                ),
            )
            for item in cast(list[object], fills_raw)
        ),
        costs=tuple(
            StrategyBacktestCost(
                name=_string(_object(item, "cost"), "name"),
                amount=_decimal_string(_object(item, "cost"), "amount"),
                currency=_string(_object(item, "cost"), "currency"),
            )
            for item in cast(list[object], costs_raw)
        ),
        net_return=_decimal_string(fields, "net_return"),
        net_pnl=_decimal_string(fields, "net_pnl"),
        portfolio_net_return=_decimal_string(fields, "portfolio_net_return"),
        max_drawdown=_optional_decimal_field(fields, "max_drawdown"),
        cvar95=_optional_decimal_field(fields, "cvar95"),
        sharpe=_optional_decimal_field(fields, "sharpe"),
        sortino=_optional_decimal_field(fields, "sortino"),
        turnover=_decimal_string(fields, "turnover"),
        adverse_excursion=_decimal_string(fields, "adverse_excursion"),
        liquidity_cost=_decimal_string(fields, "liquidity_cost"),
        stressed_net_return=_optional_decimal_field(fields, "stressed_net_return"),
        stress_evidence_artifact_hash=cast(str | None, fields.get("stress_evidence_artifact_hash")),
        missing_metrics=tuple(
            StrategyBacktestMissingMetric(
                name=_string(_object(item, "missing metric"), "name"),
                reason=_string(_object(item, "missing metric"), "reason"),
            )
            for item in cast(list[object], missing_raw)
        ),
        schema_version=_string(fields, "schema_version"),
    )


def _canonical_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else _canonical_decimal(value)


def _optional_decimal_field(fields: dict[str, object], name: str) -> Decimal | None:
    if fields.get(name) is None:
        return None
    return _decimal_string(fields, name)


def _is_non_negative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_positive_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _require_nonempty(value: str, field_name: str) -> None:
    if not value:
        raise ValueError(f"{field_name} must not be empty")


def _require_sha256(value: str, field_name: str) -> None:
    raw_value = cast(object, value)
    if not isinstance(raw_value, str) or _SHA256_PATTERN.fullmatch(raw_value) is None:
        raise ValueError(f"{field_name} must be a SHA-256 hex digest")


def _require_unique(values: tuple[str, ...], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} values must be unique")


def _require_unique_nonempty(
    values: tuple[str, ...], field_name: str, *, allow_empty: bool = False
) -> None:
    if not values and not allow_empty:
        raise ValueError(f"{field_name} must not be empty")
    _require_unique(values, field_name)
    if any(not value for value in values):
        raise ValueError(f"{field_name} values must not be empty")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object with string fields")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise ValueError(f"{name} must be an object with string fields")
    return cast(dict[str, object], value)


def _require_exact_keys(fields: dict[str, object], expected: set[str], name: str) -> None:
    missing = sorted(expected - fields.keys())
    unknown = sorted(fields.keys() - expected)
    if missing:
        raise ValueError(f"{name} missing fields: {', '.join(missing)}")
    if unknown:
        raise ValueError(f"{name} has unknown fields: {', '.join(unknown)}")


def _string(fields: dict[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _string_tuple(
    fields: dict[str, object], name: str, *, allow_empty: bool = False
) -> tuple[str, ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of non-empty strings")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) or not item for item in raw):
        raise ValueError(f"{name} must be an array of non-empty strings")
    result = tuple(cast(list[str], value))
    if not result and not allow_empty:
        raise ValueError(f"{name} must not be empty")
    return result


def _integer(fields: dict[str, object], name: str) -> int:
    value = fields.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _integer_tuple(fields: dict[str, object], name: str) -> tuple[int, ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of integers")
    return tuple(_integer({name: item}, name) for item in cast(list[object], value))


def _decimal_string(fields: dict[str, object], name: str) -> Decimal:
    raw = _string(fields, name)
    try:
        value = Decimal(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    if not value.is_finite() or _canonical_decimal(value) != raw:
        raise ValueError(f"{name} must be a canonical finite decimal string")
    return value


def _parse_timestamp(fields: dict[str, object], name: str) -> datetime:
    raw = _string(fields, name)
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(value, name)
    if _canonical_timestamp(value) != raw:
        raise ValueError(f"{name} must use canonical UTC format")
    return value


def _object_tuple(
    fields: dict[str, object], name: str, expected_fields: set[str]
) -> tuple[dict[str, object], ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of objects")
    result = tuple(_object(item, f"{name} item") for item in cast(list[object], value))
    for item in result:
        _require_exact_keys(item, expected_fields, f"{name} item")
    return result
