# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportPrivateUsage=false, reportUntypedBaseClass=false, reportUnknownLambdaType=false
from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from importlib.metadata import version
from pathlib import Path
from typing import Any, Protocol, TypeVar, cast

from nautilus_trader.adapters.interactive_brokers.common import IB
from nautilus_trader.adapters.interactive_brokers.config import (
    InteractiveBrokersExecClientConfig,
    InteractiveBrokersInstrumentProviderConfig,
)
from nautilus_trader.adapters.interactive_brokers.factories import (
    InteractiveBrokersLiveExecClientFactory,
)
from nautilus_trader.config import (
    LiveExecEngineConfig,
    LoggingConfig,
    RoutingConfig,
    StrategyConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, StrategyId, TraderId
from nautilus_trader.trading.strategy import Strategy

from market_impact_agent.account_state import opaque_account_reference_hash
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import OrderKind, Side, require_aware
from market_impact_agent.ibkr_nautilus_execution import (
    IbkrNautilusInstrumentRoute,
    IbkrNautilusPaperAcceptanceAuthority,
    IbkrNautilusPaperProviderAcceptance,
    NautilusPaperCancelCommand,
    NautilusPaperCashBalance,
    NautilusPaperExecutionObservation,
    NautilusPaperMutationReference,
    NautilusPaperOrderObservation,
    NautilusPaperPositionObservation,
    NautilusPaperRuntimeSnapshot,
    NautilusPaperRuntimeStatus,
    NautilusPaperSubmitCommand,
    _record_ibkr_nautilus_paper_activation,
    _reopen_canonical_activation,
    hash_ibkr_nautilus_instrument_routes,
)
from market_impact_agent.ibkr_nautilus_paper import (
    IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
    IBKR_NAUTILUS_VERSION,
)

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperRuntimeConfig:
    """Private runtime configuration; raw account identity is never serialized or exposed."""

    root: Path
    account_reference: str = field(repr=False)
    account_reference_key: bytes = field(repr=False)
    acceptance_authority_id: str
    instrument_ids: tuple[str, ...]
    host: str = "127.0.0.1"
    port: int = 4002
    client_id: int = 0
    fetch_all_open_orders: bool = True
    time_in_force: str = "DAY"
    startup_timeout_seconds: float = 45.0
    command_timeout_seconds: float = 15.0
    session_scope_ttl_seconds: float = 5.0

    def __post_init__(self) -> None:
        installed_nautilus = version("nautilus-trader")
        if installed_nautilus != IBKR_NAUTILUS_VERSION:
            raise RuntimeError(
                "IBKR Paper execution runtime requires NautilusTrader "
                f"{IBKR_NAUTILUS_VERSION}, found {installed_nautilus}"
            )
        if self.host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("IBKR Paper execution runtime only accepts a loopback Gateway")
        if self.port != 4002:
            raise ValueError("IBKR Paper execution runtime requires Paper Gateway port 4002")
        if self.client_id != 0:
            raise ValueError("external TWS order discovery requires IB client ID 0")
        if not self.fetch_all_open_orders:
            raise ValueError("external order discovery requires fetch_all_open_orders")
        if not self.account_reference or self.account_reference != self.account_reference.strip():
            raise ValueError("IBKR account reference must be non-empty and trimmed")
        if len(self.account_reference_key) < 32:
            raise ValueError("IBKR account reference key must contain at least 32 bytes")
        if (
            not self.acceptance_authority_id.startswith("ibkr-nautilus-paper-authority-")
            or len(self.acceptance_authority_id) != 94
            or any(
                char not in "0123456789abcdef"
                for char in self.acceptance_authority_id.removeprefix(
                    "ibkr-nautilus-paper-authority-"
                )
            )
        ):
            raise ValueError("IBKR acceptance authority identity is invalid")
        if self.instrument_ids != tuple(sorted(set(self.instrument_ids))) or any(
            not item or item != item.strip() for item in self.instrument_ids
        ):
            raise ValueError("Nautilus instrument IDs must be sorted, unique, and non-empty")
        if not self.instrument_ids:
            raise ValueError("the IBKR runtime requires at least one accepted instrument")
        if self.time_in_force != "DAY":
            raise ValueError("the bounded IBKR Paper runtime supports DAY only")
        if (
            self.startup_timeout_seconds <= 0
            or self.command_timeout_seconds <= 0
            or self.session_scope_ttl_seconds <= 0
        ):
            raise ValueError("IBKR runtime timeouts must be positive")

    @property
    def account_reference_hash(self) -> str:
        return opaque_account_reference_hash(
            self.account_reference,
            key=self.account_reference_key,
        )

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(
            {
                "runtime_version": IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
                "nautilus_version": IBKR_NAUTILUS_VERSION,
                "nautilus_ibapi_version": version("nautilus_ibapi"),
                "account_reference_hash": self.account_reference_hash,
                "acceptance_authority_id": self.acceptance_authority_id,
                "gateway_host": self.host,
                "gateway_port": self.port,
                "client_id": self.client_id,
                "fetch_all_open_orders": self.fetch_all_open_orders,
                "instrument_ids": list(self.instrument_ids),
                "time_in_force": self.time_in_force,
                "startup_timeout_seconds": self.startup_timeout_seconds,
                "command_timeout_seconds": self.command_timeout_seconds,
                "session_scope_ttl_seconds": self.session_scope_ttl_seconds,
            }
        )


class IbkrNautilusPaperRuntimeState(StrEnum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    FAULTED = "faulted"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class _ConnectionScope:
    connected: bool
    marker_valid: bool
    last_disconnection_ns: int | None
    generation: int


class _NautilusNodeDriver(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...

    def submit(self, command: object) -> NautilusPaperOrderObservation: ...

    def cancel(self, command: NautilusPaperCancelCommand) -> NautilusPaperOrderObservation: ...

    def reconcile(self) -> NautilusPaperRuntimeSnapshot: ...


class IbkrNautilusPaperRuntime:
    """Long-lived synchronous Harness seam over one official Nautilus TradingNode."""

    def __init__(
        self,
        config: IbkrNautilusPaperRuntimeConfig,
        *,
        driver: _NautilusNodeDriver | None = None,
    ) -> None:
        self._config = config
        self._driver = driver or _TradingNodeDriver(config)
        self._state = IbkrNautilusPaperRuntimeState.CREATED
        self._lock = threading.RLock()
        self._last_snapshot: NautilusPaperRuntimeSnapshot | None = None
        self._activation_store: LocalDataSnapshotStore | None = None
        self._activation_acceptance_id: str | None = None
        self._activation_head_id: str | None = None

    @property
    def runtime_version(self) -> str:
        return IBKR_NAUTILUS_PAPER_RUNTIME_VERSION

    @property
    def nautilus_version(self) -> str:
        return IBKR_NAUTILUS_VERSION

    @property
    def nautilus_ibapi_version(self) -> str:
        return version("nautilus_ibapi")

    @property
    def configuration_hash(self) -> str:
        return self._config.configuration_hash

    @property
    def account_reference_hash(self) -> str:
        return self._config.account_reference_hash

    @property
    def acceptance_authority_id(self) -> str:
        return self._config.acceptance_authority_id

    @property
    def time_in_force(self) -> str:
        return self._config.time_in_force

    @property
    def session_scope_valid(self) -> bool:
        with self._lock:
            snapshot = self._last_snapshot
        return snapshot is not None and snapshot.all_facets_complete and not snapshot.gaps

    @property
    def activation_runtime_active(self) -> bool:
        return self.state is IbkrNautilusPaperRuntimeState.RUNNING

    @property
    def session_scope_generation(self) -> int | None:
        with self._lock:
            snapshot = self._last_snapshot
        return None if snapshot is None else snapshot.connection_generation

    @property
    def session_scope_observed_at(self) -> datetime | None:
        with self._lock:
            snapshot = self._last_snapshot
        return None if snapshot is None else snapshot.observed_at

    @property
    def session_scope_last_disconnection_ns(self) -> int | None:
        with self._lock:
            snapshot = self._last_snapshot
        return None if snapshot is None else snapshot.last_disconnection_ns

    @property
    def session_scope_ttl_seconds(self) -> float:
        return self._config.session_scope_ttl_seconds

    @property
    def state(self) -> IbkrNautilusPaperRuntimeState:
        with self._lock:
            return self._state

    def start(self) -> None:
        with self._lock:
            if self._state is not IbkrNautilusPaperRuntimeState.CREATED:
                raise RuntimeError("one IBKR Nautilus runtime instance can start only once")
            self._state = IbkrNautilusPaperRuntimeState.STARTING
        try:
            self._driver.start()
        except BaseException:
            with suppress(BaseException):
                self._driver.stop()
            with self._lock:
                self._state = IbkrNautilusPaperRuntimeState.FAULTED
            raise
        with self._lock:
            self._state = IbkrNautilusPaperRuntimeState.RUNNING

    def stop(self) -> None:
        with self._lock:
            if self._state is IbkrNautilusPaperRuntimeState.STOPPED:
                return
            should_stop = self._state in {
                IbkrNautilusPaperRuntimeState.STARTING,
                IbkrNautilusPaperRuntimeState.RUNNING,
                IbkrNautilusPaperRuntimeState.FAULTED,
            }
        if should_stop:
            self._driver.stop()
        with self._lock:
            self._state = IbkrNautilusPaperRuntimeState.STOPPED

    def submit(self, reference: NautilusPaperMutationReference) -> NautilusPaperOrderObservation:
        self._require_running()
        command = self._resolve_mutation(reference, expected_kind="submit")
        if not isinstance(command, NautilusPaperSubmitCommand):
            raise RuntimeError("canonical Nautilus mutation is not a submission")
        return self._driver.submit(command)

    def cancel(self, reference: NautilusPaperMutationReference) -> NautilusPaperOrderObservation:
        self._require_running()
        command = self._resolve_mutation(reference, expected_kind="cancel")
        if not isinstance(command, NautilusPaperCancelCommand):
            raise RuntimeError("canonical Nautilus mutation is not a cancellation")
        return self._driver.cancel(command)

    def bind_canonical_activation(
        self,
        store: LocalDataSnapshotStore,
        *,
        acceptance_id: str,
        head_id: str,
    ) -> None:
        with self._lock:
            if self._activation_store is not None and (
                self._activation_store.root != store.root
                or self._activation_acceptance_id != acceptance_id
                or self._activation_head_id != head_id
            ):
                raise RuntimeError("IBKR runtime is already bound to another activation head")
            self._activation_store = store
            self._activation_acceptance_id = acceptance_id
            self._activation_head_id = head_id

    def _resolve_mutation(
        self,
        reference: NautilusPaperMutationReference,
        *,
        expected_kind: str,
    ) -> NautilusPaperSubmitCommand | NautilusPaperCancelCommand:
        store = self._activation_store
        acceptance_id = self._activation_acceptance_id
        head_id = self._activation_head_id
        if store is None or acceptance_id is None or head_id is None:
            raise RuntimeError("IBKR runtime lacks canonical activation state")
        if (
            reference.harness_authority_id != store.harness_authority_id
            or reference.mutation_kind != expected_kind
        ):
            raise RuntimeError("Nautilus mutation reference authority mismatch")
        with store.authority_transaction() as connection:
            row = connection.execute(
                """
                SELECT activation_head_id, acceptance_id, mutation_kind, payload_json
                FROM ibkr_nautilus_mutation_outbox
                WHERE mutation_id = ? AND harness_authority_id = ?
                """,
                (reference.mutation_id, store.harness_authority_id),
            ).fetchone()
        if (
            row is None
            or cast(str, row["activation_head_id"]) != head_id
            or cast(str, row["acceptance_id"]) != acceptance_id
            or cast(str, row["mutation_kind"]) != expected_kind
        ):
            raise RuntimeError("Nautilus mutation is absent from the canonical outbox")
        now = datetime.now(UTC)
        require_aware(now, "Nautilus mutation resolution time")
        acceptance, runtime_payload, _route_payload = _reopen_canonical_activation(
            store,
            acceptance_id=acceptance_id,
            head_id=head_id,
            now=now,
        )
        expected_runtime = {
            "runtime_version": self.runtime_version,
            "nautilus_version": self.nautilus_version,
            "nautilus_ibapi_version": self.nautilus_ibapi_version,
            "configuration_hash": self.configuration_hash,
            "account_reference_hash": self.account_reference_hash,
            "acceptance_authority_id": self.acceptance_authority_id,
            "time_in_force": self.time_in_force,
        }
        if (
            runtime_payload != expected_runtime
            or acceptance.configuration_hash != self.configuration_hash
            or acceptance.account_reference_hash != self.account_reference_hash
            or acceptance.acceptance_id != acceptance_id
        ):
            raise RuntimeError("Nautilus mutation runtime scope is not canonical")
        payload = json.loads(cast(str, row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("canonical Nautilus mutation payload is invalid")
        fields = cast(dict[str, object], payload)
        expected_mutation_id = "ibkr-nautilus-mutation-" + canonical_hash(
            {
                "harness_authority_id": store.harness_authority_id,
                "activation_head_id": head_id,
                "acceptance_id": acceptance_id,
                "mutation_kind": expected_kind,
                "payload": fields,
            }
        )
        if reference.mutation_id != expected_mutation_id:
            raise RuntimeError("canonical Nautilus mutation content identity mismatch")
        generation = int(cast(int, fields["connection_generation"]))
        if "last_disconnection_ns" not in fields:
            raise RuntimeError("canonical Nautilus mutation disconnect marker is missing")
        marker_value = fields["last_disconnection_ns"]
        if marker_value is None:
            last_disconnection_ns = None
        elif (
            isinstance(marker_value, int)
            and not isinstance(marker_value, bool)
            and marker_value >= 0
        ):
            last_disconnection_ns = marker_value
        else:
            raise RuntimeError("canonical Nautilus mutation disconnect marker is invalid")
        scope_observed_at = _runtime_datetime(cast(str, fields["scope_observed_at"]))
        scope_valid_until = _runtime_datetime(cast(str, fields["scope_valid_until"]))
        with self._lock:
            snapshot = self._last_snapshot
        if (
            snapshot is None
            or not snapshot.all_facets_complete
            or snapshot.gaps
            or snapshot.connection_generation != generation
            or snapshot.last_disconnection_ns != last_disconnection_ns
            or snapshot.observed_at != scope_observed_at
            or not scope_observed_at <= now < scope_valid_until
            or (scope_valid_until - scope_observed_at).total_seconds()
            > self.session_scope_ttl_seconds
        ):
            raise RuntimeError("Nautilus mutation runtime scope is stale or changed")
        if expected_kind == "submit":
            limit_value = fields.get("limit_price")
            return NautilusPaperSubmitCommand(
                submission_id=cast(str, fields["submission_id"]),
                nautilus_client_order_id=cast(str, fields["nautilus_client_order_id"]),
                instrument_id=cast(str, fields["instrument_id"]),
                side=Side(cast(str, fields["side"])),
                quantity=Decimal(cast(str, fields["quantity"])),
                order_kind=OrderKind(cast(str, fields["order_kind"])),
                limit_price=None if limit_value is None else Decimal(cast(str, limit_value)),
                created_at=_runtime_datetime(cast(str, fields["created_at"])),
                expires_at=_runtime_datetime(cast(str, fields["expires_at"])),
                connection_generation=generation,
                scope_observed_at=scope_observed_at,
                scope_valid_until=scope_valid_until,
                last_disconnection_ns=last_disconnection_ns,
            )
        return NautilusPaperCancelCommand(
            cancellation_id=cast(str, fields["cancellation_id"]),
            nautilus_client_order_id=cast(str, fields["nautilus_client_order_id"]),
            provider_order_id=cast(str, fields["provider_order_id"]),
            connection_generation=generation,
            scope_observed_at=scope_observed_at,
            scope_valid_until=scope_valid_until,
            last_disconnection_ns=last_disconnection_ns,
        )

    def reconcile(self) -> NautilusPaperRuntimeSnapshot:
        self._require_running()
        snapshot = _normalize_snapshot(self._driver.reconcile())
        with self._lock:
            self._last_snapshot = snapshot
        return snapshot

    def _require_running(self) -> None:
        if self.state is not IbkrNautilusPaperRuntimeState.RUNNING:
            raise RuntimeError("IBKR Nautilus runtime is not running")


class _HarnessExecutionStrategy(Strategy):
    def __init__(self, config: StrategyConfig, ready: threading.Event) -> None:
        super().__init__(config)
        self._ready = ready

    def on_start(self) -> None:
        self._ready.set()


class _TradingNodeDriver:
    """Native adapter; Nautilus owns command routing, OMS state, and reconciliation."""

    def __init__(
        self,
        config: IbkrNautilusPaperRuntimeConfig,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._clock = clock or (lambda: datetime.now(UTC))
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._thread_error: BaseException | None = None
        self._disposed = False
        self._node: TradingNode | None = None
        self._strategy: _HarnessExecutionStrategy | None = None
        self._thread: threading.Thread | None = None
        self._was_connected = False
        self._connection_generation = 0
        self._last_disconnection_marker_known = False
        self._last_disconnection_ns: int | None = None

    def start(self) -> None:
        self._config.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._loop = asyncio.new_event_loop()
        self._node = TradingNode(config=self._node_config(), loop=self._loop)
        self._node.add_exec_client_factory(IB, InteractiveBrokersLiveExecClientFactory)
        strategy_config = StrategyConfig(
            strategy_id=StrategyId("HARNESS-IBKR-PAPER"),
            order_id_tag="MIA",
            oms_type="NETTING",
            external_order_claims=None,
            manage_gtd_expiry=False,
            manage_stop=False,
            log_events=False,
            log_commands=False,
        )
        self._strategy = _HarnessExecutionStrategy(strategy_config, self._ready)
        self._node.trader.add_strategy(self._strategy)
        self._node.build()
        self._thread = threading.Thread(
            target=self._run,
            name="ibkr-nautilus-paper-runtime",
            daemon=True,
        )
        self._thread.start()
        deadline = time.monotonic() + self._config.startup_timeout_seconds
        while time.monotonic() < deadline:
            if self._thread_error is not None:
                raise RuntimeError(
                    "Nautilus TradingNode failed during startup"
                ) from self._thread_error
            if (
                self._ready.wait(0.05)
                and self._node.kernel.exec_engine.check_connected()
                and self._node.portfolio.initialized
            ):
                effective_client_id, collision = self._effective_client_state()
                if effective_client_id != 0 or collision:
                    raise RuntimeError(
                        "IB client ID 0 was not retained; external-order scope is invalid"
                    )
                return
        raise TimeoutError("Nautilus TradingNode did not complete startup reconciliation")

    def stop(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        node = self._node
        if node is None:
            return
        loop = self._loop_required()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(node.stop_async(), loop)
            try:
                future.result(timeout=min(self._config.command_timeout_seconds, 10.0))
            except BaseException:
                loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=min(self._config.command_timeout_seconds, 10.0))
        node.dispose()
        if not loop.is_closed():
            loop.close()

    def submit(self, command: object) -> NautilusPaperOrderObservation:
        from market_impact_agent.ibkr_nautilus_execution import NautilusPaperSubmitCommand

        if not isinstance(command, NautilusPaperSubmitCommand):
            raise TypeError("native Nautilus submission requires a submit command")
        strategy = self._strategy_required()

        def dispatch() -> None:
            node = self._node_required()
            instrument_id = InstrumentId.from_str(command.instrument_id)
            instrument = node.cache.instrument(instrument_id)
            if instrument is None:
                raise RuntimeError("Nautilus instrument is not loaded")
            client_order_id = ClientOrderId(command.nautilus_client_order_id)
            side = OrderSide.BUY if command.side is Side.BUY else OrderSide.SELL
            quantity = instrument.make_qty(command.quantity)
            if command.order_kind is OrderKind.MARKET:
                order = strategy.order_factory.market(
                    instrument_id=instrument_id,
                    order_side=side,
                    quantity=quantity,
                    time_in_force=TimeInForce.DAY,
                    client_order_id=client_order_id,
                    tags=[f"submission:{command.submission_id}"],
                )
            else:
                if command.limit_price is None:  # pragma: no cover - command invariant
                    raise RuntimeError("limit command lost its price")
                order = strategy.order_factory.limit(
                    instrument_id=instrument_id,
                    order_side=side,
                    quantity=quantity,
                    price=instrument.make_price(command.limit_price),
                    time_in_force=TimeInForce.DAY,
                    client_order_id=client_order_id,
                    tags=[f"submission:{command.submission_id}"],
                )
            now = self._clock()
            require_aware(now, "native submission mutation time")
            if not command.created_at <= now < command.expires_at:
                raise RuntimeError("Nautilus submission is outside its active mutation window")
            self._require_current_mutation_scope(command, now=now)
            strategy.submit_order(order)

        self._call(dispatch)
        return self._wait_for_order(
            command.nautilus_client_order_id,
            statuses={
                NautilusPaperRuntimeStatus.ACCEPTED,
                NautilusPaperRuntimeStatus.PARTIALLY_FILLED,
                NautilusPaperRuntimeStatus.FILLED,
                NautilusPaperRuntimeStatus.REJECTED,
                NautilusPaperRuntimeStatus.EXPIRED,
            },
        )

    def cancel(self, command: NautilusPaperCancelCommand) -> NautilusPaperOrderObservation:
        strategy = self._strategy_required()

        def dispatch() -> None:
            order = self._node_required().cache.order(
                ClientOrderId(command.nautilus_client_order_id)
            )
            if order is None:
                raise RuntimeError("Nautilus cancellation target is not in the reconciled cache")
            if (
                order.venue_order_id is None
                or str(order.venue_order_id) != command.provider_order_id
            ):
                raise RuntimeError("Nautilus cancellation target broker identity mismatch")
            now = self._clock()
            require_aware(now, "native cancellation mutation time")
            self._require_current_mutation_scope(command, now=now)
            strategy.cancel_order(order)

        self._call(dispatch)
        return self._wait_for_order(
            command.nautilus_client_order_id,
            statuses={
                NautilusPaperRuntimeStatus.PENDING_CANCEL,
                NautilusPaperRuntimeStatus.CANCELED,
            },
        )

    def _require_current_mutation_scope(
        self,
        command: NautilusPaperSubmitCommand | NautilusPaperCancelCommand,
        *,
        now: datetime,
    ) -> None:
        connected = self._node_required().kernel.exec_engine.check_connected()
        effective_client_id, collision = self._effective_client_state()
        last_disconnection_ns = self._current_disconnection_marker()
        if (
            not connected
            or effective_client_id != 0
            or collision
            or self._connection_generation != command.connection_generation
            or last_disconnection_ns != command.last_disconnection_ns
            or not command.scope_observed_at <= now < command.scope_valid_until
        ):
            raise RuntimeError("Nautilus mutation runtime generation or scope changed")

    def reconcile(self) -> NautilusPaperRuntimeSnapshot:
        return self._call_async(self._refresh_and_snapshot())

    async def _refresh_and_snapshot(self) -> NautilusPaperRuntimeSnapshot:
        node = self._node_required()
        observed_at = datetime.now(UTC)
        gaps: list[str] = []
        scope = self._observe_connection_scope()
        if not scope.marker_valid:
            gaps.append("nautilus_disconnect_marker_invalid")
        effective_client_id, client_id_collision = self._effective_client_state()
        cash_generation = 0
        position_generation = 0
        order_generation = 0
        execution_generation = 0
        positions: tuple[NautilusPaperPositionObservation, ...] = ()
        orders: tuple[NautilusPaperOrderObservation, ...] = ()
        executions: tuple[NautilusPaperExecutionObservation, ...] = ()
        if scope.connected and effective_client_id == 0 and not client_id_collision:
            execution_client = self._execution_client()
            cash_scope = scope
            cash_barrier_succeeded = False
            try:
                await execution_client._query_account(None)
                cash_barrier_succeeded = True
            except Exception:
                gaps.append("nautilus_cash_barrier_failed")
            scope = self._observe_connection_scope()
            if not scope.marker_valid:
                gaps.append("nautilus_disconnect_marker_invalid")
            if cash_barrier_succeeded:
                if scope == cash_scope:
                    cash_generation = scope.generation
                else:
                    gaps.append("nautilus_cash_barrier_scope_changed")
            effective_client_id, client_id_collision = self._effective_client_state()
        if scope.connected and effective_client_id == 0 and not client_id_collision:
            execution_client = self._execution_client()
            mass_status: object | None = None
            mass_status_succeeded = False
            mass_status_scope = scope
            try:
                mass_status = await execution_client.generate_mass_status()
                mass_status_succeeded = True
            except Exception:
                gaps.append("nautilus_execution_mass_status_failed")
            scope = self._observe_connection_scope()
            if not scope.marker_valid:
                gaps.append("nautilus_disconnect_marker_invalid")
            if mass_status_succeeded:
                if mass_status is None:
                    gaps.append("nautilus_execution_mass_status_unavailable")
                else:
                    positions, orders, executions = _mass_status_observations(
                        mass_status,
                        observed_at=observed_at,
                    )
                    gaps.extend(
                        "external_nautilus_order:"
                        + canonical_hash(order.nautilus_client_order_id)[:12]
                        for order in orders
                        if order.external
                    )
                    gaps.extend(
                        "unsupported_nautilus_order_status:"
                        + canonical_hash(order.nautilus_client_order_id)[:12]
                        for order in orders
                        if order.status is NautilusPaperRuntimeStatus.UNKNOWN
                    )
                    if scope == mass_status_scope:
                        position_generation = scope.generation
                        order_generation = scope.generation
                        execution_generation = scope.generation
                    else:
                        gaps.append("nautilus_execution_mass_status_scope_changed")
        elif scope.connected:
            gaps.append("nautilus_effective_client_id_invalid")
        accounts = tuple(node.cache.accounts())
        if len(accounts) != 1:
            gaps.append("nautilus_account_section_count_invalid")
        cash: list[NautilusPaperCashBalance] = []
        for account in accounts:
            for currency, total in account.balances().items():
                free = account.balance_free(currency)
                locked = account.balance_locked(currency)
                if free is None or locked is None:
                    gaps.append("nautilus_cash_component_unavailable")
                    continue
                cash.append(
                    NautilusPaperCashBalance(
                        currency=str(currency),
                        total=total.as_decimal(),
                        free=free.as_decimal(),
                        locked=locked.as_decimal(),
                    )
                )
        final_scope = self._observe_connection_scope()
        if not final_scope.marker_valid:
            gaps.append("nautilus_disconnect_marker_invalid")
        if final_scope != scope:
            gaps.append("nautilus_reconciliation_scope_changed")
        scope = final_scope
        effective_client_id, client_id_collision = self._effective_client_state()
        if not scope.connected:
            gaps.append("nautilus_runtime_disconnected")
        portfolio_initialized = node.portfolio.initialized
        if not portfolio_initialized:
            gaps.append("nautilus_portfolio_not_initialized")
        complete = scope.connected and portfolio_initialized and len(accounts) == 1 and not gaps
        return NautilusPaperRuntimeSnapshot(
            observed_at=observed_at,
            connected=scope.connected,
            reconciled=self._ready.is_set(),
            complete=complete,
            orders=orders,
            cash=tuple(sorted(cash, key=lambda item: item.currency)),
            positions=positions,
            executions=executions,
            cash_complete=(
                scope.connected
                and scope.marker_valid
                and cash_generation == scope.generation
                and len(accounts) == 1
                and "nautilus_cash_component_unavailable" not in gaps
            ),
            positions_complete=(
                scope.connected
                and scope.marker_valid
                and position_generation == scope.generation
                and portfolio_initialized
            ),
            orders_complete=(
                scope.connected and scope.marker_valid and order_generation == scope.generation
            ),
            executions_complete=(
                scope.connected and scope.marker_valid and execution_generation == scope.generation
            ),
            external_order_discovery_complete=(
                scope.connected
                and scope.marker_valid
                and order_generation == scope.generation
                and effective_client_id == 0
                and not client_id_collision
            ),
            effective_client_id=effective_client_id,
            client_id_collision=client_id_collision,
            connection_generation=scope.generation,
            last_disconnection_ns=scope.last_disconnection_ns,
            cash_reconciliation_generation=cash_generation,
            positions_reconciliation_generation=position_generation,
            orders_reconciliation_generation=order_generation,
            executions_reconciliation_generation=execution_generation,
            gaps=tuple(sorted(set(gaps))),
        )

    def _wait_for_order(
        self,
        client_order_id: str,
        *,
        statuses: set[NautilusPaperRuntimeStatus],
    ) -> NautilusPaperOrderObservation:
        deadline = time.monotonic() + self._config.command_timeout_seconds
        while time.monotonic() < deadline:
            order = self._call(
                lambda: self._node_required().cache.order(ClientOrderId(client_order_id))
            )
            if order is not None:
                observation = _order_observation(order)
                if observation.status in statuses:
                    return observation
            time.sleep(0.02)
        raise TimeoutError("Nautilus order acknowledgement is ambiguous")

    def _call(self, callback: Callable[[], _T]) -> _T:
        if self._thread_error is not None:
            raise RuntimeError("Nautilus TradingNode stopped unexpectedly") from self._thread_error

        async def invoke() -> _T:
            return callback()

        concurrent = asyncio.run_coroutine_threadsafe(invoke(), self._loop_required())
        return concurrent.result(timeout=self._config.command_timeout_seconds)

    def _call_async(self, coroutine: Any) -> Any:
        if self._thread_error is not None:
            raise RuntimeError("Nautilus TradingNode stopped unexpectedly") from self._thread_error
        concurrent = asyncio.run_coroutine_threadsafe(coroutine, self._loop_required())
        return concurrent.result(timeout=self._config.command_timeout_seconds)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop_required())
        try:
            self._node_required().run(raise_exception=True)
        except BaseException as exc:
            self._thread_error = exc
        finally:
            asyncio.set_event_loop(None)

    def _node_required(self) -> TradingNode:
        if self._node is None:
            raise RuntimeError("Nautilus TradingNode is not built")
        return self._node

    def _loop_required(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError("Nautilus event loop is not built")
        return self._loop

    def _strategy_required(self) -> _HarnessExecutionStrategy:
        if self._strategy is None:
            raise RuntimeError("Nautilus execution strategy is not built")
        return self._strategy

    def _execution_client(self) -> Any:
        clients = tuple(self._node_required().kernel.exec_engine.registered_clients)
        if len(clients) != 1:
            raise RuntimeError("Nautilus runtime requires exactly one execution client")
        return clients[0]

    def _effective_client_state(self) -> tuple[int | None, bool]:
        try:
            ib_client = self._execution_client()._client
        except BaseException:
            return None, True
        effective_client_id = cast(int | None, getattr(ib_client, "_client_id", None))
        configured_client_id = cast(
            int | None,
            getattr(ib_client, "_configured_client_id", None),
        )
        collision_count = cast(int, getattr(ib_client, "_client_id_collision_count", 0))
        return (
            effective_client_id,
            effective_client_id != configured_client_id or collision_count > 0,
        )

    def _observe_connection_scope(self) -> _ConnectionScope:
        connected = self._node_required().kernel.exec_engine.check_connected()
        try:
            last_disconnection_ns = self._current_disconnection_marker()
            marker_valid = True
        except RuntimeError:
            last_disconnection_ns = None
            marker_valid = False
        marker_changed = (
            marker_valid
            and self._last_disconnection_marker_known
            and last_disconnection_ns != self._last_disconnection_ns
        )
        if (
            connected
            and marker_valid
            and (
                not self._was_connected
                or not self._last_disconnection_marker_known
                or marker_changed
            )
        ):
            self._connection_generation += 1
        self._was_connected = connected
        if marker_valid:
            self._last_disconnection_ns = last_disconnection_ns
            self._last_disconnection_marker_known = True
        return _ConnectionScope(
            connected=connected,
            marker_valid=marker_valid,
            last_disconnection_ns=last_disconnection_ns,
            generation=self._connection_generation,
        )

    def _current_disconnection_marker(self) -> int | None:
        # This private IB client field is part of the pinned 1.231.0 adapter contract.
        # Any dependency drift or field-shape drift closes reconciliation and mutation.
        if version("nautilus-trader") != IBKR_NAUTILUS_VERSION:
            raise RuntimeError("Nautilus disconnect marker version guard failed")
        try:
            ib_client = self._execution_client()._client
        except BaseException as exc:
            raise RuntimeError("Nautilus disconnect marker client is unavailable") from exc
        missing = object()
        marker = getattr(ib_client, "_last_disconnection_ns", missing)
        if marker is missing or (
            marker is not None
            and (isinstance(marker, bool) or not isinstance(marker, int) or marker < 0)
        ):
            raise RuntimeError("Nautilus disconnect marker is unavailable or invalid")
        return marker

    def _node_config(self) -> TradingNodeConfig:
        log_root = self._config.root / "logs"
        log_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        return TradingNodeConfig(
            trader_id=TraderId("HARNESS-IBKR-PAPER"),
            logging=LoggingConfig(
                log_level="INFO",
                log_level_file="INFO",
                log_directory=str(log_root),
                log_file_name="ibkr-nautilus-paper-runtime",
                log_file_format="json",
                log_colors=False,
            ),
            exec_engine=LiveExecEngineConfig(
                reconciliation=True,
                reconciliation_startup_delay_secs=0.1,
                filter_unclaimed_external_orders=False,
                generate_missing_orders=True,
                open_check_interval_secs=5.0,
                open_check_open_only=True,
                position_check_interval_secs=30.0,
            ),
            exec_clients={
                IB: InteractiveBrokersExecClientConfig(
                    ibg_host=self._config.host,
                    ibg_port=self._config.port,
                    ibg_client_id=self._config.client_id,
                    account_id=self._config.account_reference,
                    fetch_all_open_orders=self._config.fetch_all_open_orders,
                    connection_timeout=min(int(self._config.startup_timeout_seconds), 30),
                    request_timeout_secs=min(int(self._config.startup_timeout_seconds), 30),
                    instrument_provider=InteractiveBrokersInstrumentProviderConfig(
                        load_all=False,
                        load_ids=frozenset(
                            InstrumentId.from_str(item) for item in self._config.instrument_ids
                        ),
                    ),
                    routing=RoutingConfig(default=True),
                )
            },
            timeout_connection=min(self._config.startup_timeout_seconds, 30.0),
            timeout_reconciliation=min(self._config.startup_timeout_seconds, 30.0),
            timeout_portfolio=min(self._config.startup_timeout_seconds, 30.0),
            timeout_disconnection=10.0,
            timeout_post_stop=2.0,
            timeout_shutdown=5.0,
        )


class IbkrNautilusPaperAcceptanceHarness:
    """Resolve sealed durable observations into one exact-scope acceptance."""

    @staticmethod
    def build(
        *,
        runtime: IbkrNautilusPaperRuntime,
        authority: IbkrNautilusPaperAcceptanceAuthority,
        instrument_routes_hash: str,
        markets: tuple[str, ...],
        order_types: tuple[str, ...],
        observation_ids: tuple[str, ...],
        accepted_at: datetime,
        valid_until: datetime,
        canonical_store: LocalDataSnapshotStore | None = None,
        instrument_routes: Mapping[str, IbkrNautilusInstrumentRoute] | None = None,
        activation_valid_until: datetime | None = None,
        gaps: tuple[str, ...] = (),
    ) -> IbkrNautilusPaperProviderAcceptance:
        require_aware(accepted_at, "acceptance accepted_at")
        require_aware(valid_until, "acceptance valid_until")
        if authority.authority_id != runtime.acceptance_authority_id:
            raise ValueError("runtime is pinned to another acceptance authority")
        acceptance = authority.build_acceptance(
            observation_ids=observation_ids,
            configuration_hash=runtime.configuration_hash,
            account_reference_hash=runtime.account_reference_hash,
            instrument_routes_hash=instrument_routes_hash,
            markets=markets,
            order_types=order_types,
            time_in_force=(runtime.time_in_force,),
            nautilus_ibapi_version=runtime.nautilus_ibapi_version,
            accepted_at=accepted_at,
            valid_until=valid_until,
            gaps=gaps,
        )
        activation_fields = (canonical_store, instrument_routes, activation_valid_until)
        if any(item is not None for item in activation_fields):
            if any(item is None for item in activation_fields):
                raise ValueError("canonical activation requires store, routes, and validity")
            assert canonical_store is not None
            assert instrument_routes is not None
            assert activation_valid_until is not None
            if hash_ibkr_nautilus_instrument_routes(instrument_routes) != instrument_routes_hash:
                raise ValueError("canonical activation routes do not match acceptance scope")
            _record_ibkr_nautilus_paper_activation(
                store=canonical_store,
                authority=authority,
                acceptance=acceptance,
                runtime=runtime,
                instrument_routes=instrument_routes,
                activation_valid_until=activation_valid_until,
            )
        return acceptance


def _mass_status_observations(
    mass_status: object,
    *,
    observed_at: datetime,
) -> tuple[
    tuple[NautilusPaperPositionObservation, ...],
    tuple[NautilusPaperOrderObservation, ...],
    tuple[NautilusPaperExecutionObservation, ...],
]:
    """Normalize the exact report set returned by one mass-status barrier."""

    raw = cast(Any, mass_status)
    order_reports = tuple(raw.order_reports.values())
    order_ids_by_venue: dict[str, str] = {}
    external_by_venue: dict[str, bool] = {}
    for report in order_reports:
        venue_order_id = str(report.venue_order_id)
        reported_client_id = report.client_order_id
        client_order_id = (
            "EXTERNAL-"
            + canonical_hash(
                {
                    "venue_order_id": venue_order_id,
                    "instrument_id": str(report.instrument_id),
                }
            )[:24]
            if reported_client_id is None
            else str(reported_client_id)
        )
        order_ids_by_venue[venue_order_id] = client_order_id
        external_by_venue[venue_order_id] = (
            reported_client_id is None or not client_order_id.startswith("MIA-")
        )

    executions: list[NautilusPaperExecutionObservation] = []
    for venue_order_id, reports in raw.fill_reports.items():
        provider_order_id = str(venue_order_id)
        for report in reports:
            reported_client_id = report.client_order_id
            client_order_id = (
                order_ids_by_venue.get(provider_order_id)
                if reported_client_id is None
                else str(reported_client_id)
            )
            if client_order_id is None:
                client_order_id = (
                    "EXTERNAL-"
                    + canonical_hash(
                        {
                            "venue_order_id": provider_order_id,
                            "instrument_id": str(report.instrument_id),
                        }
                    )[:24]
                )
            executions.append(
                NautilusPaperExecutionObservation(
                    fill_id=str(report.trade_id),
                    nautilus_client_order_id=client_order_id,
                    provider_order_id=provider_order_id,
                    quantity=report.last_qty.as_decimal(),
                    price=report.last_px.as_decimal(),
                    observed_at=_from_ns(report.ts_event),
                )
            )
    executions_tuple = tuple(sorted(executions, key=lambda item: item.fill_id))
    fills_by_order: dict[str, list[NautilusPaperExecutionObservation]] = {}
    for execution in executions_tuple:
        fills_by_order.setdefault(execution.nautilus_client_order_id, []).append(execution)

    orders = tuple(
        sorted(
            (
                NautilusPaperOrderObservation(
                    nautilus_client_order_id=order_ids_by_venue[str(report.venue_order_id)],
                    provider_order_id=str(report.venue_order_id),
                    status=_runtime_status(report.order_status),
                    observed_at=_from_ns(report.ts_last),
                    filled_quantity=sum(
                        (
                            item.quantity
                            for item in fills_by_order.get(
                                order_ids_by_venue[str(report.venue_order_id)],
                                [],
                            )
                        ),
                        Decimal(0),
                    ),
                    fill_ids=tuple(
                        item.fill_id
                        for item in fills_by_order.get(
                            order_ids_by_venue[str(report.venue_order_id)],
                            [],
                        )
                    ),
                    external=external_by_venue[str(report.venue_order_id)],
                )
                for report in order_reports
            ),
            key=lambda item: item.nautilus_client_order_id,
        )
    )

    signed_positions: dict[str, Decimal] = {}
    position_times: dict[str, datetime] = {}
    for reports in raw.position_reports.values():
        for report in reports:
            instrument_id = str(report.instrument_id)
            side = str(getattr(report.position_side, "name", report.position_side)).upper()
            quantity = report.quantity.as_decimal()
            if side == "SHORT":
                quantity = -quantity
            elif side == "FLAT":
                quantity = Decimal(0)
            signed_positions[instrument_id] = (
                signed_positions.get(instrument_id, Decimal(0)) + quantity
            )
            position_times[instrument_id] = max(
                position_times.get(instrument_id, observed_at),
                _from_ns(report.ts_last),
            )
    positions = tuple(
        NautilusPaperPositionObservation(
            instrument_id=instrument_id,
            signed_quantity=quantity,
            observed_at=position_times[instrument_id],
        )
        for instrument_id, quantity in sorted(signed_positions.items())
    )
    return positions, orders, executions_tuple


def _normalize_snapshot(snapshot: NautilusPaperRuntimeSnapshot) -> NautilusPaperRuntimeSnapshot:
    unique: dict[str, NautilusPaperExecutionObservation] = {}
    gaps = list(snapshot.gaps)
    for execution in snapshot.executions:
        prior = unique.get(execution.fill_id)
        if prior is None:
            unique[execution.fill_id] = execution
        elif prior != execution:
            gaps.append("conflicting_duplicate_fill:" + canonical_hash(execution.fill_id)[:12])
    executions = tuple(sorted(unique.values(), key=lambda item: item.fill_id))
    fills_by_order: dict[str, list[NautilusPaperExecutionObservation]] = {}
    for execution in executions:
        fills_by_order.setdefault(execution.nautilus_client_order_id, []).append(execution)
    normalized_orders: list[NautilusPaperOrderObservation] = []
    known_orders = {item.nautilus_client_order_id for item in snapshot.orders}
    for execution in executions:
        if execution.nautilus_client_order_id not in known_orders:
            gaps.append("orphan_execution:" + canonical_hash(execution.fill_id)[:12])
    for order in snapshot.orders:
        if order.external:
            gaps.append(
                "external_nautilus_order:" + canonical_hash(order.nautilus_client_order_id)[:12]
            )
        if order.status is NautilusPaperRuntimeStatus.UNKNOWN:
            gaps.append(
                "unsupported_nautilus_order_status:"
                + canonical_hash(order.nautilus_client_order_id)[:12]
            )
        order_fills = fills_by_order.get(order.nautilus_client_order_id, [])
        normalized_quantity = sum((item.quantity for item in order_fills), Decimal(0))
        normalized_fill_ids = tuple(item.fill_id for item in order_fills)
        if order_fills and any(
            item.provider_order_id != order.provider_order_id for item in order_fills
        ):
            gaps.append(
                "execution_order_identity_mismatch:"
                + canonical_hash(order.nautilus_client_order_id)[:12]
            )
        if order.filled_quantity != normalized_quantity or order.fill_ids != normalized_fill_ids:
            order = replace(
                order,
                filled_quantity=normalized_quantity,
                fill_ids=normalized_fill_ids,
            )
        normalized_orders.append(order)
    unique_gaps = tuple(sorted(set(gaps)))
    return replace(
        snapshot,
        complete=snapshot.complete and not unique_gaps,
        orders=tuple(normalized_orders),
        executions=executions,
        gaps=unique_gaps,
    )


def _order_observation(order: object) -> NautilusPaperOrderObservation:
    raw = cast(Any, order)
    status = _runtime_status(raw.status)
    fills = [event for event in raw.events if isinstance(event, OrderFilled)]
    provider_order_id = raw.venue_order_id
    strategy_id = str(raw.strategy_id)
    return NautilusPaperOrderObservation(
        nautilus_client_order_id=str(raw.client_order_id),
        provider_order_id=None if provider_order_id is None else str(provider_order_id),
        status=status,
        observed_at=_from_ns(cast(int, raw.ts_last)),
        filled_quantity=sum((event.last_qty.as_decimal() for event in fills), Decimal(0)),
        fill_ids=tuple(sorted({str(event.trade_id) for event in fills})),
        external=strategy_id == "EXTERNAL",
    )


def _runtime_status(status: object) -> NautilusPaperRuntimeStatus:
    name = str(getattr(status, "name", status)).upper()
    mapping = {
        "ACCEPTED": NautilusPaperRuntimeStatus.ACCEPTED,
        "TRIGGERED": NautilusPaperRuntimeStatus.ACCEPTED,
        "PENDING_UPDATE": NautilusPaperRuntimeStatus.ACCEPTED,
        "PENDING_CANCEL": NautilusPaperRuntimeStatus.PENDING_CANCEL,
        "CANCELED": NautilusPaperRuntimeStatus.CANCELED,
        "PARTIALLY_FILLED": NautilusPaperRuntimeStatus.PARTIALLY_FILLED,
        "FILLED": NautilusPaperRuntimeStatus.FILLED,
        "REJECTED": NautilusPaperRuntimeStatus.REJECTED,
        "DENIED": NautilusPaperRuntimeStatus.REJECTED,
        "EXPIRED": NautilusPaperRuntimeStatus.EXPIRED,
    }
    return mapping.get(name, NautilusPaperRuntimeStatus.UNKNOWN)


def _from_ns(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)


def _runtime_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "runtime timestamp")
    return parsed
