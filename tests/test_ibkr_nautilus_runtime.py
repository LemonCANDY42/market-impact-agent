# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false
from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import cast

import pytest

from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import OrderKind, Side
from market_impact_agent.ibkr_nautilus_execution import (
    IBKR_NAUTILUS_PAPER_SCENARIO_RESULT_SCHEMA,
    IbkrNautilusInstrumentRoute,
    IbkrNautilusPaperAcceptanceAuthority,
    IbkrNautilusPaperAcceptanceRunner,
    NautilusPaperCancelCommand,
    NautilusPaperCashBalance,
    NautilusPaperExecutionObservation,
    NautilusPaperOrderObservation,
    NautilusPaperPositionObservation,
    NautilusPaperRuntimeSnapshot,
    NautilusPaperRuntimeStatus,
    NautilusPaperSubmitCommand,
    hash_ibkr_nautilus_instrument_routes,
    issue_ibkr_nautilus_paper_provider_from_harness_state,
)
from market_impact_agent.ibkr_nautilus_runtime import (
    IbkrNautilusPaperAcceptanceHarness,
    IbkrNautilusPaperRuntime,
    IbkrNautilusPaperRuntimeConfig,
    IbkrNautilusPaperRuntimeState,
    _TradingNodeDriver,
)

NOW = datetime(2026, 9, 2, 8, tzinfo=UTC)
ACCOUNT_KEY = b"fixture-harness-account-pseudonymization-key-32-bytes"
RUNNER_ID = "fixture-harness-acceptance-runner"
RUNNER_KEY = b"fixture-harness-acceptance-runner-key-at-least-32-bytes"
RUNNER = IbkrNautilusPaperAcceptanceRunner(RUNNER_ID, RUNNER_KEY)
SCENARIOS = tuple(
    sorted(
        {
            "account_reconciliation",
            "ambiguous_acknowledgement",
            "cancel",
            "disconnect",
            "duplicate_fill",
            "external_order",
            "gateway_restart",
            "partial_fill",
            "process_restart",
            "replace",
            "submit",
        }
    )
)


def _snapshot(
    *,
    orders: tuple[NautilusPaperOrderObservation, ...] = (),
    executions: tuple[NautilusPaperExecutionObservation, ...] = (),
    cash: tuple[NautilusPaperCashBalance, ...] = (),
    positions: tuple[NautilusPaperPositionObservation, ...] = (),
    connected: bool = True,
) -> NautilusPaperRuntimeSnapshot:
    return NautilusPaperRuntimeSnapshot(
        observed_at=NOW,
        connected=connected,
        reconciled=True,
        complete=connected,
        orders=orders,
        executions=executions,
        cash=cash,
        positions=positions,
        cash_complete=True,
        positions_complete=True,
        orders_complete=connected,
        executions_complete=connected,
        external_order_discovery_complete=True,
        effective_client_id=0 if connected else None,
        connection_generation=1,
        cash_reconciliation_generation=1 if connected else 0,
        positions_reconciliation_generation=1 if connected else 0,
        orders_reconciliation_generation=1 if connected else 0,
        executions_reconciliation_generation=1 if connected else 0,
    )


class _Driver:
    def __init__(self, snapshot: NautilusPaperRuntimeSnapshot | None = None) -> None:
        self.snapshot = snapshot or _snapshot()
        self.starts = 0
        self.stops = 0

    def start(self) -> None:
        self.starts += 1

    def stop(self) -> None:
        self.stops += 1

    def submit(self, command: object) -> NautilusPaperOrderObservation:
        raise AssertionError(f"unexpected submit: {command!r}")

    def cancel(self, command: NautilusPaperCancelCommand) -> NautilusPaperOrderObservation:
        raise AssertionError(f"unexpected cancel: {command!r}")

    def reconcile(self) -> NautilusPaperRuntimeSnapshot:
        return self.snapshot


def _config(root: Path) -> IbkrNautilusPaperRuntimeConfig:
    return IbkrNautilusPaperRuntimeConfig(
        root=root,
        account_reference="fixture-paper-account",
        account_reference_key=ACCOUNT_KEY,
        acceptance_authority_id=RUNNER.authority_id,
        instrument_ids=("AAPL.NASDAQ",),
    )


def _observations(
    runtime: IbkrNautilusPaperRuntime,
    authority: IbkrNautilusPaperAcceptanceAuthority,
    runner: IbkrNautilusPaperAcceptanceRunner,
    evidence_root: Path,
    *,
    effective_client_id: int = 0,
    client_id_collision: bool = False,
    manual_order_auto_bind_observed: bool = True,
    exclusive_api_client_scope_observed: bool = True,
    instrument_routes_hash: str = "3" * 64,
) -> tuple[str, ...]:
    identities: list[str] = []
    evidence_root.mkdir(exist_ok=True)
    for scenario in SCENARIOS:
        artifact_path = evidence_root / f"{scenario}.artifact.json"
        result_path = evidence_root / f"{scenario}.result.json"
        artifact_path.write_text(
            json.dumps({"scenario": scenario, "events": [f"observed-{scenario}"]}),
            encoding="utf-8",
        )
        result_path.write_text(
            json.dumps(
                {
                    "schema_version": IBKR_NAUTILUS_PAPER_SCENARIO_RESULT_SCHEMA,
                    "scenario": scenario,
                    "configuration_hash": runtime.configuration_hash,
                    "account_reference_hash": runtime.account_reference_hash,
                    "instrument_routes_hash": instrument_routes_hash,
                    "markets": ["US"],
                    "order_types": ["limit", "market"],
                    "time_in_force": ["DAY"],
                    "nautilus_ibapi_version": runtime.nautilus_ibapi_version,
                    "effective_client_id": effective_client_id,
                    "client_id_collision": client_id_collision,
                    "manual_order_auto_bind_observed": manual_order_auto_bind_observed,
                    "exclusive_api_client_scope_observed": (exclusive_api_client_scope_observed),
                    "passed": True,
                    "observed_at": "2026-09-02T08:00:00Z",
                }
            ),
            encoding="utf-8",
        )
        observation = authority.record_scenario_evidence(
            artifact_path=artifact_path,
            result_path=result_path,
            runner_seal=runner.seal_evidence(
                artifact_path=artifact_path,
                result_path=result_path,
            ),
        )
        identities.append(observation.observation_id)
    return tuple(sorted(identities))


def test_runtime_requires_client_zero_all_open_orders_and_explicit_scope(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="client ID 0"):
        IbkrNautilusPaperRuntimeConfig(
            root=tmp_path,
            account_reference="fixture-paper-account",
            account_reference_key=ACCOUNT_KEY,
            acceptance_authority_id=RUNNER.authority_id,
            instrument_ids=("AAPL.NASDAQ",),
            client_id=7,
        )
    with pytest.raises(ValueError, match="fetch_all_open_orders"):
        IbkrNautilusPaperRuntimeConfig(
            root=tmp_path,
            account_reference="fixture-paper-account",
            account_reference_key=ACCOUNT_KEY,
            acceptance_authority_id=RUNNER.authority_id,
            instrument_ids=("AAPL.NASDAQ",),
            fetch_all_open_orders=False,
        )

    config = _config(tmp_path)
    assert "fixture-paper-account" not in config.configuration_hash
    assert config.account_reference_hash.startswith("account-ref-")

    changed_instruments = IbkrNautilusPaperRuntimeConfig(
        root=tmp_path,
        account_reference="fixture-paper-account",
        account_reference_key=ACCOUNT_KEY,
        acceptance_authority_id=RUNNER.authority_id,
        instrument_ids=("MSFT.NASDAQ",),
    )
    changed_timeout = IbkrNautilusPaperRuntimeConfig(
        root=tmp_path,
        account_reference="fixture-paper-account",
        account_reference_key=ACCOUNT_KEY,
        acceptance_authority_id=RUNNER.authority_id,
        instrument_ids=("AAPL.NASDAQ",),
        command_timeout_seconds=16,
    )
    changed_key = IbkrNautilusPaperRuntimeConfig(
        root=tmp_path,
        account_reference="fixture-paper-account",
        account_reference_key=b"another-fixture-harness-key-with-at-least-32-bytes",
        acceptance_authority_id=RUNNER.authority_id,
        instrument_ids=("AAPL.NASDAQ",),
    )
    assert changed_instruments.configuration_hash != config.configuration_hash
    assert changed_timeout.configuration_hash != config.configuration_hash
    assert changed_key.account_reference_hash != config.account_reference_hash
    assert changed_key.configuration_hash != config.configuration_hash


def test_runtime_lifecycle_is_long_lived_and_restart_keeps_exact_configuration(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    first_driver = _Driver()
    first = IbkrNautilusPaperRuntime(config, driver=first_driver)
    with pytest.raises(RuntimeError, match="not running"):
        first.reconcile()
    first.start()
    assert first.state is IbkrNautilusPaperRuntimeState.RUNNING
    assert first.reconcile().all_facets_complete
    first.stop()
    first.stop()
    assert first_driver.starts == 1
    assert first_driver.stops == 1

    restarted_driver = _Driver()
    restarted = IbkrNautilusPaperRuntime(config, driver=restarted_driver)
    restarted.start()
    assert restarted.configuration_hash == first.configuration_hash
    assert restarted.account_reference_hash == first.account_reference_hash
    assert restarted.reconcile().all_facets_complete
    restarted.stop()


def test_native_driver_rechecks_submission_window_at_mutation_point(tmp_path: Path) -> None:
    submitted: list[object] = []

    class _Instrument:
        def make_qty(self, quantity: Decimal) -> Decimal:
            return quantity

    class _OrderFactory:
        def market(self, **fields: object) -> object:
            return fields

    strategy = SimpleNamespace(
        order_factory=_OrderFactory(),
        submit_order=submitted.append,
    )

    def instrument(_: object) -> _Instrument:
        return _Instrument()

    node = SimpleNamespace(
        cache=SimpleNamespace(instrument=instrument),
    )
    driver = object.__new__(_TradingNodeDriver)
    driver._config = _config(tmp_path)
    driver._clock = lambda: NOW
    driver._strategy = strategy
    driver._node = node
    driver._thread_error = None
    driver._call = lambda callback: callback()
    expired = NautilusPaperSubmitCommand(
        submission_id="expired-submission",
        nautilus_client_order_id="MIA-expired-order",
        instrument_id="AAPL.NASDAQ",
        side=Side.BUY,
        quantity=Decimal("1"),
        order_kind=OrderKind.MARKET,
        limit_price=None,
        created_at=NOW - timedelta(seconds=2),
        expires_at=NOW - timedelta(seconds=1),
        connection_generation=1,
        scope_observed_at=NOW - timedelta(seconds=1),
        scope_valid_until=NOW + timedelta(seconds=1),
    )

    with pytest.raises(RuntimeError, match="active mutation window"):
        driver.submit(expired)

    assert submitted == []


def test_native_driver_rechecks_connection_generation_at_mutation_point(tmp_path: Path) -> None:
    submitted: list[object] = []

    class _Instrument:
        def make_qty(self, quantity: Decimal) -> Decimal:
            return quantity

    class _OrderFactory:
        def market(self, **fields: object) -> object:
            return fields

    strategy = SimpleNamespace(
        order_factory=_OrderFactory(),
        submit_order=submitted.append,
    )

    def instrument(_: object) -> _Instrument:
        return _Instrument()

    driver = object.__new__(_TradingNodeDriver)
    driver._config = _config(tmp_path)
    driver._clock = lambda: NOW
    driver._strategy = strategy
    driver._node = SimpleNamespace(
        cache=SimpleNamespace(instrument=instrument),
        kernel=SimpleNamespace(
            exec_engine=SimpleNamespace(check_connected=lambda: True),
        ),
    )
    driver._connection_generation = 1
    driver._effective_client_state = lambda: (0, False)
    driver._current_disconnection_marker = lambda: None
    driver._thread_error = None
    driver._call = lambda callback: callback()
    unauthorized = NautilusPaperSubmitCommand(
        submission_id="capability-expired-submission",
        nautilus_client_order_id="MIA-capability-expired-order",
        instrument_id="AAPL.NASDAQ",
        side=Side.BUY,
        quantity=Decimal("1"),
        order_kind=OrderKind.MARKET,
        limit_price=None,
        created_at=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=1),
        connection_generation=2,
        scope_observed_at=NOW - timedelta(seconds=1),
        scope_valid_until=NOW + timedelta(seconds=1),
    )

    with pytest.raises(RuntimeError, match="generation or scope changed"):
        driver.submit(unauthorized)

    assert submitted == []


def test_native_driver_rejects_disconnect_reconnect_missed_by_connected_flag(
    tmp_path: Path,
) -> None:
    submitted: list[object] = []

    class _Instrument:
        def make_qty(self, quantity: Decimal) -> Decimal:
            return quantity

    class _OrderFactory:
        def market(self, **fields: object) -> object:
            return fields

    strategy = SimpleNamespace(
        order_factory=_OrderFactory(),
        submit_order=submitted.append,
    )

    def instrument(_: object) -> _Instrument:
        return _Instrument()

    driver = object.__new__(_TradingNodeDriver)
    driver._config = _config(tmp_path)
    driver._clock = lambda: NOW
    driver._strategy = strategy
    driver._node = SimpleNamespace(
        cache=SimpleNamespace(instrument=instrument),
        kernel=SimpleNamespace(
            exec_engine=SimpleNamespace(check_connected=lambda: True),
        ),
    )
    driver._connection_generation = 1
    driver._effective_client_state = lambda: (0, False)
    driver._current_disconnection_marker = lambda: 200
    driver._thread_error = None
    driver._call = lambda callback: callback()
    stale_session = NautilusPaperSubmitCommand(
        submission_id="pre-reconnect-submission",
        nautilus_client_order_id="MIA-pre-reconnect-order",
        instrument_id="AAPL.NASDAQ",
        side=Side.BUY,
        quantity=Decimal("1"),
        order_kind=OrderKind.MARKET,
        limit_price=None,
        created_at=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=1),
        connection_generation=1,
        scope_observed_at=NOW - timedelta(seconds=1),
        scope_valid_until=NOW + timedelta(seconds=1),
        last_disconnection_ns=100,
    )

    with pytest.raises(RuntimeError, match="generation or scope changed"):
        driver.submit(stale_session)

    assert submitted == []


def test_native_disconnect_marker_contract_fails_closed_on_shape_drift() -> None:
    driver = object.__new__(_TradingNodeDriver)
    ib_client = SimpleNamespace(_last_disconnection_ns=None)
    driver._execution_client = lambda: SimpleNamespace(_client=ib_client)

    assert driver._current_disconnection_marker() is None

    ib_client._last_disconnection_ns = "invalid"
    with pytest.raises(RuntimeError, match="unavailable or invalid"):
        driver._current_disconnection_marker()

    del ib_client._last_disconnection_ns
    with pytest.raises(RuntimeError, match="unavailable or invalid"):
        driver._current_disconnection_marker()


def test_gateway_disconnect_recovers_in_place_only_after_all_facets_return(
    tmp_path: Path,
) -> None:
    cash = (
        NautilusPaperCashBalance(
            currency="USD",
            total=Decimal("1000"),
            free=Decimal("900"),
            locked=Decimal("100"),
        ),
    )
    positions = (
        NautilusPaperPositionObservation(
            instrument_id="AAPL.NASDAQ",
            signed_quantity=Decimal("2"),
            observed_at=NOW,
        ),
    )
    driver = _Driver(_snapshot())
    runtime = IbkrNautilusPaperRuntime(_config(tmp_path), driver=driver)
    runtime.start()

    driver.snapshot = _snapshot(connected=False)
    assert not runtime.reconcile().all_facets_complete
    driver.snapshot = _snapshot(cash=cash, positions=positions)
    recovered = runtime.reconcile()

    assert recovered.all_facets_complete
    assert recovered.cash == cash
    assert recovered.positions == positions
    assert driver.starts == 1
    runtime.stop()


def test_stale_reconnect_barrier_or_observed_client_collision_fails_closed(
    tmp_path: Path,
) -> None:
    stale = replace(_snapshot(), connection_generation=2)
    collision = replace(_snapshot(), client_id_collision=True)
    fallback = replace(_snapshot(), effective_client_id=1)
    driver = _Driver(stale)
    runtime = IbkrNautilusPaperRuntime(_config(tmp_path), driver=driver)
    runtime.start()

    assert not runtime.reconcile().all_facets_complete
    assert not runtime.session_scope_valid
    driver.snapshot = collision
    assert not runtime.reconcile().all_facets_complete
    assert not runtime.session_scope_valid
    driver.snapshot = fallback
    assert not runtime.reconcile().all_facets_complete
    assert not runtime.session_scope_valid
    runtime.stop()


def test_concrete_driver_uses_mass_status_external_order_even_when_cache_is_empty(
    tmp_path: Path,
) -> None:
    timestamp_ns = int(NOW.timestamp() * 1_000_000_000)

    class _Amount:
        def __init__(self, value: str) -> None:
            self._value = Decimal(value)

        def as_decimal(self) -> Decimal:
            return self._value

    class _Account:
        def balances(self) -> dict[str, _Amount]:
            return {"USD": _Amount("1000")}

        def balance_free(self, currency: str) -> _Amount:
            assert currency == "USD"
            return _Amount("900")

        def balance_locked(self, currency: str) -> _Amount:
            assert currency == "USD"
            return _Amount("100")

    external_report = SimpleNamespace(
        venue_order_id="broker-order-external",
        client_order_id=None,
        instrument_id="AAPL.NASDAQ",
        order_status=SimpleNamespace(name="ACCEPTED"),
        ts_last=timestamp_ns,
    )
    unsupported_report = SimpleNamespace(
        venue_order_id="broker-order-owned",
        client_order_id="MIA-owned-order",
        instrument_id="AAPL.NASDAQ",
        order_status=SimpleNamespace(name="BROKER_STATE_NOT_SUPPORTED"),
        ts_last=timestamp_ns,
    )
    mass_status = SimpleNamespace(
        order_reports={
            "broker-order-external": external_report,
            "broker-order-owned": unsupported_report,
        },
        fill_reports={},
        position_reports={},
    )

    class _ExecutionClient:
        def __init__(self) -> None:
            self._client = SimpleNamespace(
                _client_id=0,
                _configured_client_id=0,
                _client_id_collision_count=0,
                _last_disconnection_ns=None,
            )

        async def _query_account(self, account_id: object) -> None:
            assert account_id is None

        async def generate_mass_status(self) -> object:
            return mass_status

    execution_client = _ExecutionClient()
    exec_engine = SimpleNamespace(
        check_connected=lambda: True,
        registered_clients=(execution_client,),
    )
    cache = SimpleNamespace(
        accounts=lambda: (_Account(),),
        orders=lambda: (),
    )
    node = SimpleNamespace(
        kernel=SimpleNamespace(exec_engine=exec_engine),
        cache=cache,
        portfolio=SimpleNamespace(initialized=True),
    )
    driver = object.__new__(_TradingNodeDriver)
    driver._config = _config(tmp_path)
    driver._node = node
    driver._was_connected = False
    driver._connection_generation = 0
    driver._last_disconnection_marker_known = False
    driver._last_disconnection_ns = None
    driver._ready = Event()
    driver._ready.set()

    snapshot = asyncio.run(driver._refresh_and_snapshot())

    assert node.cache.orders() == ()
    assert len(snapshot.orders) == 2
    assert any(order.external for order in snapshot.orders)
    assert any(order.provider_order_id == "broker-order-external" for order in snapshot.orders)
    assert not snapshot.complete
    assert any(gap.startswith("external_nautilus_order:") for gap in snapshot.gaps)
    assert any(gap.startswith("unsupported_nautilus_order_status:") for gap in snapshot.gaps)
    assert snapshot.orders_reconciliation_generation == snapshot.connection_generation

    execution_client._client._last_disconnection_ns = 123
    post_reconnect = asyncio.run(driver._refresh_and_snapshot())

    assert post_reconnect.connected
    assert post_reconnect.last_disconnection_ns == 123
    assert post_reconnect.connection_generation == snapshot.connection_generation + 1
    assert post_reconnect.orders_reconciliation_generation == post_reconnect.connection_generation


@pytest.mark.parametrize("barrier", ["account", "mass_status"])
def test_concrete_driver_invalidates_in_barrier_disconnect_reconnect(
    tmp_path: Path,
    barrier: str,
) -> None:
    class _Amount:
        def __init__(self, value: str) -> None:
            self.value = Decimal(value)

        def as_decimal(self) -> Decimal:
            return self.value

    class _Account:
        def balances(self) -> dict[str, _Amount]:
            return {"USD": _Amount("1000")}

        def balance_free(self, currency: str) -> _Amount:
            assert currency == "USD"
            return _Amount("900")

        def balance_locked(self, currency: str) -> _Amount:
            assert currency == "USD"
            return _Amount("100")

    connected = [True]

    class _ExecutionClient:
        def __init__(self) -> None:
            self._client = SimpleNamespace(
                _client_id=0,
                _configured_client_id=0,
                _client_id_collision_count=0,
                _last_disconnection_ns=None,
            )

        def disconnect_and_reconnect(self) -> None:
            connected[0] = False
            self._client._last_disconnection_ns = 123
            connected[0] = True

        async def _query_account(self, account_id: object) -> None:
            assert account_id is None
            if barrier == "account":
                self.disconnect_and_reconnect()

        async def generate_mass_status(self) -> object:
            if barrier == "mass_status":
                self.disconnect_and_reconnect()
            return SimpleNamespace(order_reports={}, fill_reports={}, position_reports={})

    execution_client = _ExecutionClient()
    node = SimpleNamespace(
        kernel=SimpleNamespace(
            exec_engine=SimpleNamespace(
                check_connected=lambda: connected[0],
                registered_clients=(execution_client,),
            )
        ),
        cache=SimpleNamespace(accounts=lambda: (_Account(),)),
        portfolio=SimpleNamespace(initialized=True),
    )
    driver = object.__new__(_TradingNodeDriver)
    driver._config = _config(tmp_path)
    driver._node = node
    driver._was_connected = False
    driver._connection_generation = 0
    driver._last_disconnection_marker_known = False
    driver._last_disconnection_ns = None
    driver._ready = Event()
    driver._ready.set()

    snapshot = asyncio.run(driver._refresh_and_snapshot())

    assert snapshot.connected
    assert snapshot.last_disconnection_ns == 123
    assert snapshot.connection_generation == 2
    assert not snapshot.complete
    assert not snapshot.all_facets_complete
    expected_gap = (
        "nautilus_cash_barrier_scope_changed"
        if barrier == "account"
        else "nautilus_execution_mass_status_scope_changed"
    )
    assert expected_gap in snapshot.gaps
    if barrier == "account":
        assert not snapshot.cash_complete
    else:
        assert not snapshot.positions_complete
        assert not snapshot.orders_complete
        assert not snapshot.executions_complete


def test_unsupported_order_state_closes_runtime_session_before_provider_reconcile(
    tmp_path: Path,
) -> None:
    unknown_order = NautilusPaperOrderObservation(
        nautilus_client_order_id="MIA-owned-order",
        provider_order_id="broker-order-owned",
        status=NautilusPaperRuntimeStatus.UNKNOWN,
        observed_at=NOW,
    )
    runtime = IbkrNautilusPaperRuntime(
        _config(tmp_path),
        driver=_Driver(_snapshot(orders=(unknown_order,))),
    )
    runtime.start()

    snapshot = runtime.reconcile()

    assert not snapshot.complete
    assert any(gap.startswith("unsupported_nautilus_order_status:") for gap in snapshot.gaps)
    assert not runtime.session_scope_valid
    runtime.stop()


def test_reconciliation_normalizes_duplicate_fills_and_preserves_external_classification(
    tmp_path: Path,
) -> None:
    order = NautilusPaperOrderObservation(
        nautilus_client_order_id="MIA-client-order-1",
        provider_order_id="private-broker-order-1",
        status=NautilusPaperRuntimeStatus.PARTIALLY_FILLED,
        observed_at=NOW,
        filled_quantity=Decimal("2"),
        fill_ids=("stale-fill",),
    )
    external = NautilusPaperOrderObservation(
        nautilus_client_order_id="external-order-1",
        provider_order_id="private-broker-order-2",
        status=NautilusPaperRuntimeStatus.ACCEPTED,
        observed_at=NOW,
        external=True,
    )
    fill = NautilusPaperExecutionObservation(
        fill_id="private-fill-1",
        nautilus_client_order_id=order.nautilus_client_order_id,
        provider_order_id=cast(str, order.provider_order_id),
        quantity=Decimal("1"),
        price=Decimal("100"),
        observed_at=NOW,
    )
    driver = _Driver(_snapshot(orders=(order, external), executions=(fill, fill)))
    runtime = IbkrNautilusPaperRuntime(_config(tmp_path), driver=driver)
    runtime.start()

    snapshot = runtime.reconcile()

    assert not snapshot.complete
    assert not runtime.session_scope_valid
    assert snapshot.executions == (fill,)
    assert snapshot.orders[0].filled_quantity == Decimal("1")
    assert snapshot.orders[0].fill_ids == ("private-fill-1",)
    assert snapshot.orders[1].external
    runtime.stop()


def test_disconnect_and_conflicting_duplicate_fill_fail_reconciliation_closed(
    tmp_path: Path,
) -> None:
    first = NautilusPaperExecutionObservation(
        fill_id="private-fill-1",
        nautilus_client_order_id="MIA-client-order-1",
        provider_order_id="private-broker-order-1",
        quantity=Decimal("1"),
        price=Decimal("100"),
        observed_at=NOW,
    )
    conflict = NautilusPaperExecutionObservation(
        fill_id=first.fill_id,
        nautilus_client_order_id=first.nautilus_client_order_id,
        provider_order_id=first.provider_order_id,
        quantity=Decimal("2"),
        price=first.price,
        observed_at=NOW,
    )
    driver = _Driver(_snapshot(executions=(first, conflict), connected=False))
    runtime = IbkrNautilusPaperRuntime(_config(tmp_path), driver=driver)
    runtime.start()

    snapshot = runtime.reconcile()

    assert not snapshot.complete
    assert not snapshot.all_facets_complete
    assert any(gap.startswith("conflicting_duplicate_fill:") for gap in snapshot.gaps)
    runtime.stop()


def test_acceptance_harness_binds_runtime_dependencies_tif_scope_and_fault_evidence(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "canonical-state")
    runtime = IbkrNautilusPaperRuntime(_config(tmp_path), driver=_Driver())
    runtime.start()
    assert runtime.reconcile().all_facets_complete
    authority = IbkrNautilusPaperAcceptanceAuthority(
        store.root / "ibkr-nautilus-paper-acceptance.sqlite3",
        runner_id=RUNNER_ID,
        verification_key=RUNNER_KEY,
    )
    runner = IbkrNautilusPaperAcceptanceRunner(RUNNER_ID, RUNNER_KEY)
    routes = {
        "AAPL.XNAS": IbkrNautilusInstrumentRoute(
            nautilus_instrument_id="AAPL.NASDAQ",
            market="US",
        )
    }
    routes_hash = hash_ibkr_nautilus_instrument_routes(routes)
    observation_ids = _observations(
        runtime,
        authority,
        runner,
        tmp_path / "evidence",
        instrument_routes_hash=routes_hash,
    )

    acceptance = IbkrNautilusPaperAcceptanceHarness.build(
        runtime=runtime,
        authority=authority,
        instrument_routes_hash=routes_hash,
        markets=("US",),
        order_types=("limit", "market"),
        observation_ids=observation_ids,
        accepted_at=NOW,
        valid_until=NOW + timedelta(days=1),
    )

    assert acceptance.execution_accepted
    assert acceptance.runtime_version == runtime.runtime_version
    assert acceptance.nautilus_version == runtime.nautilus_version
    assert acceptance.nautilus_ibapi_version == runtime.nautilus_ibapi_version
    assert acceptance.environment == "paper"
    assert acceptance.time_in_force == ("DAY",)
    assert acceptance.exclusive_api_client_scope
    assert acceptance.manual_order_auto_bind
    assert acceptance.accepted_scenarios == SCENARIOS

    unproven_runtime = IbkrNautilusPaperRuntime(_config(tmp_path / "unproven"), driver=_Driver())
    unproven_authority = IbkrNautilusPaperAcceptanceAuthority(
        tmp_path / "unproven-authority.sqlite3",
        runner_id=RUNNER_ID,
        verification_key=RUNNER_KEY,
    )
    unproven_ids = _observations(
        unproven_runtime,
        unproven_authority,
        runner,
        tmp_path / "unproven-evidence",
        effective_client_id=1,
        client_id_collision=True,
        manual_order_auto_bind_observed=False,
        exclusive_api_client_scope_observed=False,
    )
    unproven = IbkrNautilusPaperAcceptanceHarness.build(
        runtime=unproven_runtime,
        authority=unproven_authority,
        instrument_routes_hash="3" * 64,
        markets=("US",),
        order_types=("limit", "market"),
        observation_ids=unproven_ids,
        accepted_at=NOW,
        valid_until=NOW + timedelta(days=1),
    )
    assert not unproven.execution_accepted


def test_caller_fresh_root_fake_evidence_and_matching_runtime_pin_cannot_register_activation(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "caller-root")
    runner_id = "caller-created-runner"
    runner_key = b"caller-created-runner-key-with-at-least-32-bytes"
    runner = IbkrNautilusPaperAcceptanceRunner(runner_id, runner_key)
    config = IbkrNautilusPaperRuntimeConfig(
        root=tmp_path / "caller-runtime",
        account_reference="caller-paper-account",
        account_reference_key=ACCOUNT_KEY,
        acceptance_authority_id=runner.authority_id,
        instrument_ids=("AAPL.NASDAQ",),
    )
    runtime = IbkrNautilusPaperRuntime(config, driver=_Driver())
    runtime.start()
    assert runtime.reconcile().all_facets_complete
    authority = IbkrNautilusPaperAcceptanceAuthority(
        store.root / "ibkr-nautilus-paper-acceptance.sqlite3",
        runner_id=runner_id,
        verification_key=runner_key,
    )
    routes = {
        "AAPL.XNAS": IbkrNautilusInstrumentRoute(
            nautilus_instrument_id="AAPL.NASDAQ",
            market="US",
        )
    }
    routes_hash = hash_ibkr_nautilus_instrument_routes(routes)
    observations = _observations(
        runtime,
        authority,
        runner,
        tmp_path / "caller-evidence",
        instrument_routes_hash=routes_hash,
    )
    issued_at = datetime.now(UTC)

    with pytest.raises(PermissionError, match="runner authority is not registered"):
        IbkrNautilusPaperAcceptanceHarness.build(
            runtime=runtime,
            authority=authority,
            instrument_routes_hash=routes_hash,
            markets=("US",),
            order_types=("limit", "market"),
            observation_ids=observations,
            accepted_at=issued_at - timedelta(minutes=1),
            valid_until=issued_at + timedelta(days=1),
            canonical_store=store,
            instrument_routes=routes,
            activation_valid_until=issued_at + timedelta(days=365),
        )
    with pytest.raises(PermissionError, match="activation head is missing"):
        issue_ibkr_nautilus_paper_provider_from_harness_state(
            canonical_store=store,
            accepted_evidence_content_id=("ibkr-nautilus-paper-acceptance-" + "0" * 64),
        )
    runtime.stop()
