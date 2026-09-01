# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

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
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId

from market_impact_agent.account_state import opaque_account_reference_hash
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.ibkr_account_read import (
    IbkrPaperAccountReader,
    IbkrPaperAccountReadReport,
)

IBKR_NAUTILUS_PAPER_PROVIDER_ID = "ibkr-nautilus-paper"
IBKR_NAUTILUS_PAPER_PROVIDER_VERSION = "0.2.0-candidate"
IBKR_NAUTILUS_PAPER_RUNTIME_VERSION = "0.2.0-candidate"
IBKR_NAUTILUS_VERSION = "1.231.0"
IBKR_NAUTILUS_PAPER_READINESS_SCHEMA = "market-impact.ibkr-nautilus-paper-readiness.v1"
_BRIDGE_READINESS_BLOCKERS = frozenset(
    {
        "source_open_orders_unavailable",
        "source_positions_unavailable",
        "nautilus_account_section_count_invalid",
        "nautilus_open_order_count_mismatch",
        "nautilus_open_position_count_mismatch",
        "nautilus_probe_strategy_present",
    }
)


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperReadinessReport:
    report_id: str
    provider_id: str
    provider_version: str
    nautilus_version: str
    nautilus_ibapi_version: str
    account_reference_hash: str
    gateway_server_version: int
    gateway_timezone: str
    gateway_host: str
    gateway_port: int
    account_reader_client_id: int
    nautilus_client_id: int
    source_account_barriers_complete: bool
    nautilus_connected: bool
    nautilus_reconciled: bool
    portfolio_initialized: bool
    account_section_count: int
    open_order_count: int
    open_position_count: int
    strategy_count: int
    observed_at: datetime
    gaps: tuple[str, ...]

    @property
    def read_only_accepted(self) -> bool:
        return (
            self.source_account_barriers_complete
            and self.nautilus_connected
            and self.nautilus_reconciled
            and self.portfolio_initialized
            and self.account_section_count == 1
            and self.strategy_count == 0
            and not _BRIDGE_READINESS_BLOCKERS.intersection(self.gaps)
        )

    @property
    def exposure_increase_ready(self) -> bool:
        return self.read_only_accepted and not self.gaps

    def assert_read_only_accepted(self) -> None:
        if not self.read_only_accepted:
            raise RuntimeError("Nautilus-to-IBKR paper read-only readiness did not complete")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": IBKR_NAUTILUS_PAPER_READINESS_SCHEMA,
            "report_id": self.report_id,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "nautilus_version": self.nautilus_version,
            "nautilus_ibapi_version": self.nautilus_ibapi_version,
            "account_reference_hash": self.account_reference_hash,
            "gateway_server_version": self.gateway_server_version,
            "gateway_timezone": self.gateway_timezone,
            "gateway_host": self.gateway_host,
            "gateway_port": self.gateway_port,
            "account_reader_client_id": self.account_reader_client_id,
            "nautilus_client_id": self.nautilus_client_id,
            "source_account_barriers_complete": self.source_account_barriers_complete,
            "nautilus_connected": self.nautilus_connected,
            "nautilus_reconciled": self.nautilus_reconciled,
            "portfolio_initialized": self.portfolio_initialized,
            "account_section_count": self.account_section_count,
            "open_order_count": self.open_order_count,
            "open_position_count": self.open_position_count,
            "strategy_count": self.strategy_count,
            "read_only_accepted": self.read_only_accepted,
            "exposure_increase_ready": self.exposure_increase_ready,
            "observed_at": _timestamp(self.observed_at),
            "gaps": list(self.gaps),
        }


@dataclass(frozen=True, slots=True)
class _NautilusReadSnapshot:
    connected: bool
    reconciled: bool
    portfolio_initialized: bool
    account_section_count: int
    open_order_count: int
    open_position_count: int
    strategy_count: int


class IbkrNautilusPaperReadinessProbe:
    """One-shot, mutation-free Nautilus-to-IBKR Paper readiness probe."""

    def __init__(
        self,
        root: Path,
        *,
        host: str = "127.0.0.1",
        port: int = 4002,
        account_reader_client_id: int = 193,
        nautilus_client_id: int = 194,
        timeout_seconds: float = 45.0,
        gateway_timezone: str = "Asia/Shanghai",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("IBKR Nautilus Paper probe only accepts a loopback Gateway")
        if port != 4002:
            raise ValueError("IBKR Nautilus Paper probe requires the Paper Gateway port 4002")
        if account_reader_client_id <= 0 or nautilus_client_id <= 0:
            raise ValueError("IBKR probe client IDs must be positive")
        if account_reader_client_id == nautilus_client_id:
            raise ValueError("IBKR probe client IDs must be distinct")
        if timeout_seconds <= 0:
            raise ValueError("IBKR Nautilus Paper probe timeout must be positive")
        self._root = root.resolve()
        self._host = host
        self._port = port
        self._account_reader_client_id = account_reader_client_id
        self._nautilus_client_id = nautilus_client_id
        self._timeout_seconds = timeout_seconds
        self._gateway_timezone = gateway_timezone
        self._clock = clock or (lambda: datetime.now(UTC))
        self._used = False

    def run(self, *, account_reference_key: bytes) -> IbkrNautilusPaperReadinessReport:
        if self._used:
            raise RuntimeError("one Nautilus readiness probe instance can run only once")
        self._used = True
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        account_report = IbkrPaperAccountReader(
            host=self._host,
            port=self._port,
            client_id=self._account_reader_client_id,
            timeout_seconds=min(self._timeout_seconds, 30.0),
            gateway_timezone=self._gateway_timezone,
            clock=self._clock,
        ).read(reference_key=account_reference_key)
        account_report.assert_accepted_read()
        snapshot = self._run_nautilus(account_report)
        observed_at = self._clock()
        require_aware(observed_at, "Nautilus-to-IBKR readiness observation time")
        return _build_report(
            account_report=account_report,
            account_reference_key=account_reference_key,
            snapshot=snapshot,
            observed_at=observed_at.astimezone(UTC),
            host=self._host,
            port=self._port,
            account_reader_client_id=self._account_reader_client_id,
            nautilus_client_id=self._nautilus_client_id,
        )

    def _run_nautilus(self, account_report: IbkrPaperAccountReadReport) -> _NautilusReadSnapshot:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        node: TradingNode | None = None
        observer_task: asyncio.Task[None] | None = None
        snapshots: list[_NautilusReadSnapshot] = []
        observer_errors: list[str] = []
        try:
            node = TradingNode(
                config=self._node_config(account_report),
                loop=loop,
            )
            node.add_exec_client_factory(IB, InteractiveBrokersLiveExecClientFactory)
            node.build()
            expected_open_orders = (
                None if account_report.open_orders is None else len(account_report.open_orders)
            )
            expected_positions = (
                None if account_report.positions is None else len(account_report.positions)
            )

            async def observe_and_stop() -> None:
                deadline = loop.time() + self._timeout_seconds
                latest_snapshot: _NautilusReadSnapshot | None = None
                try:
                    while loop.time() < deadline:
                        if node.trader.is_running:
                            latest_snapshot = _NautilusReadSnapshot(
                                connected=node.kernel.exec_engine.check_connected(),
                                reconciled=True,
                                portfolio_initialized=node.portfolio.initialized,
                                account_section_count=len(node.cache.accounts()),
                                open_order_count=len(node.cache.orders_open()),
                                open_position_count=len(node.cache.positions_open()),
                                strategy_count=len(node.trader.strategies()),
                            )
                            if (
                                latest_snapshot.connected
                                and latest_snapshot.portfolio_initialized
                                and latest_snapshot.account_section_count == 1
                                and expected_open_orders is not None
                                and latest_snapshot.open_order_count == expected_open_orders
                                and expected_positions is not None
                                and latest_snapshot.open_position_count == expected_positions
                                and latest_snapshot.strategy_count == 0
                            ):
                                snapshots.append(latest_snapshot)
                                break
                        await asyncio.sleep(0.1)
                    if not snapshots and latest_snapshot is not None:
                        snapshots.append(latest_snapshot)
                    elif not snapshots:
                        observer_errors.append("nautilus_startup_timeout")
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    observer_errors.append(f"observer_error:{type(exc).__name__}")
                finally:
                    try:
                        await node.stop_async()
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        observer_errors.append(f"nautilus_stop_error:{type(exc).__name__}")
                        loop.stop()

            observer_task = loop.create_task(
                observe_and_stop(),
                name="ibkr-nautilus-paper-readiness-observer",
            )
            try:
                node.run(raise_exception=True)
            except RuntimeError as exc:
                if observer_errors:
                    raise RuntimeError(observer_errors[0]) from exc
                raise
            observer_task.result()
            if observer_errors:
                raise RuntimeError(observer_errors[0])
            if not snapshots:
                raise RuntimeError("Nautilus readiness did not reach a running Trader")
            return snapshots[0]
        finally:
            if observer_task is not None and not observer_task.done():
                observer_task.cancel()
                if not loop.is_closed():
                    with suppress(BaseException):
                        loop.run_until_complete(observer_task)
            elif observer_task is not None:
                with suppress(BaseException):
                    observer_task.result()
            if node is not None:
                with suppress(BaseException):
                    node.stop()
                with suppress(BaseException):
                    node.dispose()
            if not loop.is_closed():
                with suppress(BaseException):
                    loop.close()
            asyncio.set_event_loop(previous_loop)

    def _node_config(
        self,
        account_report: IbkrPaperAccountReadReport,
    ) -> TradingNodeConfig:
        log_root = self._root / "logs"
        log_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        return TradingNodeConfig(
            trader_id=TraderId("HARNESS-PAPER-PROBE"),
            logging=LoggingConfig(
                log_level="ERROR",
                log_level_file="ERROR",
                log_directory=str(log_root),
                log_file_name="ibkr-nautilus-paper-readiness",
                log_file_format="json",
                log_colors=False,
            ),
            exec_engine=LiveExecEngineConfig(
                reconciliation=True,
                reconciliation_startup_delay_secs=0.1,
                open_check_interval_secs=None,
                position_check_interval_secs=None,
            ),
            exec_clients={
                IB: InteractiveBrokersExecClientConfig(
                    ibg_host=self._host,
                    ibg_port=self._port,
                    ibg_client_id=self._nautilus_client_id,
                    account_id=account_report.account_reference,
                    fetch_all_open_orders=True,
                    connection_timeout=min(int(self._timeout_seconds), 30),
                    request_timeout_secs=min(int(self._timeout_seconds), 30),
                    instrument_provider=InteractiveBrokersInstrumentProviderConfig(
                        load_all=False,
                    ),
                    routing=RoutingConfig(default=True),
                ),
            },
            timeout_connection=min(self._timeout_seconds, 30.0),
            timeout_reconciliation=min(self._timeout_seconds, 30.0),
            timeout_portfolio=min(self._timeout_seconds, 30.0),
            timeout_disconnection=10.0,
            timeout_post_stop=2.0,
            timeout_shutdown=5.0,
        )


def _build_report(
    *,
    account_report: IbkrPaperAccountReadReport,
    account_reference_key: bytes,
    snapshot: _NautilusReadSnapshot,
    observed_at: datetime,
    host: str,
    port: int,
    account_reader_client_id: int,
    nautilus_client_id: int,
) -> IbkrNautilusPaperReadinessReport:
    gaps = list(account_report.reconciliation_gaps)
    expected_open_orders = (
        None if account_report.open_orders is None else len(account_report.open_orders)
    )
    expected_positions = None if account_report.positions is None else len(account_report.positions)
    if expected_open_orders is None:
        gaps.append("source_open_orders_unavailable")
    elif snapshot.open_order_count != expected_open_orders:
        gaps.append("nautilus_open_order_count_mismatch")
    if expected_positions is None:
        gaps.append("source_positions_unavailable")
    elif snapshot.open_position_count != expected_positions:
        gaps.append("nautilus_open_position_count_mismatch")
    if snapshot.account_section_count != 1:
        gaps.append("nautilus_account_section_count_invalid")
    if snapshot.strategy_count != 0:
        gaps.append("nautilus_probe_strategy_present")
    unique_gaps = tuple(sorted(set(gaps)))
    material = {
        "schema_version": IBKR_NAUTILUS_PAPER_READINESS_SCHEMA,
        "provider_id": IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        "provider_version": IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
        "nautilus_version": version("nautilus-trader"),
        "nautilus_ibapi_version": version("nautilus_ibapi"),
        "account_reference_hash": opaque_account_reference_hash(
            account_report.account_reference,
            key=account_reference_key,
        ),
        "gateway_server_version": account_report.gateway_server_version,
        "gateway_timezone": account_report.gateway_timezone,
        "gateway_host": host,
        "gateway_port": port,
        "account_reader_client_id": account_reader_client_id,
        "nautilus_client_id": nautilus_client_id,
        "source_account_barriers_complete": all(
            (
                account_report.account_barrier_complete,
                account_report.account_summary_barrier_complete,
                account_report.open_order_barrier_complete,
                account_report.execution_barrier_complete,
            )
        ),
        "nautilus_connected": snapshot.connected,
        "nautilus_reconciled": snapshot.reconciled,
        "portfolio_initialized": snapshot.portfolio_initialized,
        "account_section_count": snapshot.account_section_count,
        "open_order_count": snapshot.open_order_count,
        "open_position_count": snapshot.open_position_count,
        "strategy_count": snapshot.strategy_count,
        "observed_at": _timestamp(observed_at),
        "gaps": list(unique_gaps),
    }
    report = IbkrNautilusPaperReadinessReport(
        report_id="ibkr-nautilus-paper-readiness-" + canonical_hash(material),
        provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
        nautilus_version=str(material["nautilus_version"]),
        nautilus_ibapi_version=str(material["nautilus_ibapi_version"]),
        account_reference_hash=str(material["account_reference_hash"]),
        gateway_server_version=account_report.gateway_server_version,
        gateway_timezone=account_report.gateway_timezone,
        gateway_host=host,
        gateway_port=port,
        account_reader_client_id=account_reader_client_id,
        nautilus_client_id=nautilus_client_id,
        source_account_barriers_complete=bool(material["source_account_barriers_complete"]),
        nautilus_connected=snapshot.connected,
        nautilus_reconciled=snapshot.reconciled,
        portfolio_initialized=snapshot.portfolio_initialized,
        account_section_count=snapshot.account_section_count,
        open_order_count=snapshot.open_order_count,
        open_position_count=snapshot.open_position_count,
        strategy_count=snapshot.strategy_count,
        observed_at=observed_at,
        gaps=unique_gaps,
    )
    return report


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
