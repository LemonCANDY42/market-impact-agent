# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUntypedBaseClass=false
from __future__ import annotations

import hashlib
import hmac
import socket
import threading
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.execution import Execution, ExecutionFilter
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.wrapper import EWrapper

from market_impact_agent.account_state import (
    AccountPosition,
    AccountStateSnapshot,
    CashBalance,
    OpenOrder,
    RecentFill,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import Side, TradingEnvironment, require_aware
from market_impact_agent.providers import (
    Capability,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)

IBKR_ACCOUNT_PROVIDER_ID = "ibkr-paper-account-read"
IBKR_ACCOUNT_PROVIDER_VERSION = "1.0.0"
_INFORMATIONAL_ERROR_CODES = frozenset({2104, 2106, 2107, 2108, 2158})


class Clock(Protocol):
    def __call__(self) -> datetime: ...


@dataclass(frozen=True, slots=True)
class IbkrPaperAccountReadReport:
    account_reference: str = field(repr=False)
    as_of: datetime
    reconciled_at: datetime
    gateway_server_version: int
    gateway_timezone: str
    account_barrier_complete: bool
    account_summary_barrier_complete: bool
    open_order_barrier_complete: bool
    execution_barrier_complete: bool
    cash: tuple[CashBalance, ...] | None
    positions: tuple[AccountPosition, ...] | None
    open_orders: tuple[OpenOrder, ...] | None
    recent_fills: tuple[RecentFill, ...] | None
    recent_fills_since: datetime
    reconciliation_gaps: tuple[str, ...]

    def assert_accepted_read(self) -> None:
        if not (
            self.account_barrier_complete
            and self.account_summary_barrier_complete
            and self.open_order_barrier_complete
            and self.execution_barrier_complete
        ):
            raise RuntimeError("IBKR paper account read did not reach every reconciliation barrier")
        if self.gateway_server_version <= 0:
            raise RuntimeError("IBKR Gateway did not report a valid server version")


class IbkrPaperAccountReader:
    """One-shot IBKR paper reader. It intentionally exposes no mutation method."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 193,
        timeout_seconds: float = 15.0,
        gateway_timezone: str = "Asia/Shanghai",
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("IBKR paper account reader only accepts a loopback Gateway")
        if not 1 <= port <= 65535:
            raise ValueError("IBKR Gateway port must be valid")
        if client_id <= 0:
            raise ValueError("IBKR account reader requires a nonzero client_id")
        if timeout_seconds <= 0:
            raise ValueError("IBKR account reader timeout must be positive")
        try:
            configured_timezone = ZoneInfo(gateway_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("IBKR Gateway timezone must be an IANA timezone") from exc
        self._host = host
        self._port = port
        self._client_id = client_id
        self._timeout_seconds = timeout_seconds
        self._gateway_timezone = gateway_timezone
        self._configured_timezone = configured_timezone
        self._clock = clock

    def read(self, *, reference_key: bytes) -> IbkrPaperAccountReadReport:
        if len(reference_key) < 16:
            raise ValueError("IBKR reference key must contain at least 16 bytes")
        collector = _IbkrAccountCollector(
            reference_key=reference_key,
            gateway_timezone=self._gateway_timezone,
            configured_timezone=self._configured_timezone,
            clock=self._clock,
        )
        collector.connect(self._host, self._port, self._client_id)
        if not collector.isConnected():
            raise ConnectionError("IBKR Gateway rejected the local API connection")
        message_loop_errors: list[BaseException] = []

        def run_message_loop() -> None:
            try:
                collector.run()
            except BaseException as exc:
                message_loop_errors.append(exc)

        thread = threading.Thread(
            target=run_message_loop,
            name="ibkr-account-read",
            daemon=True,
        )
        thread.start()
        report: IbkrPaperAccountReadReport | None = None
        try:
            if not collector.api_ready.wait(self._timeout_seconds):
                raise TimeoutError("IBKR Gateway API session did not become ready before timeout")
            collector.reqManagedAccts()
            if not collector.ready.wait(self._timeout_seconds):
                raise TimeoutError("IBKR Gateway did not publish one paper account before timeout")
            collector.request_snapshot()
            if not collector.complete.wait(self._timeout_seconds):
                raise TimeoutError("IBKR account reconciliation barriers did not complete in time")
            report = collector.report()
        finally:
            collector.stop_snapshot()
            collector.close_transport()
            thread.join(timeout=2.0)
            if thread.is_alive():
                raise RuntimeError("IBKR account message loop did not stop after disconnect")
        if message_loop_errors:
            raise RuntimeError("IBKR account message loop failed") from message_loop_errors[0]
        return report


def capture_ibkr_paper_account_snapshot(
    *,
    reader: IbkrPaperAccountReader,
    account_reference_key: bytes,
) -> AccountStateSnapshot:
    """Harness boundary: accept all three read barriers, then mint the durable snapshot."""

    if type(reader) is not IbkrPaperAccountReader:
        raise TypeError("IBKR account capture requires the concrete read-only adapter")
    report = reader.read(reference_key=account_reference_key)
    report.assert_accepted_read()
    manifest = ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id=IBKR_ACCOUNT_PROVIDER_ID,
        provider_version=IBKR_ACCOUNT_PROVIDER_VERSION,
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("IBKR",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        # ACCOUNT read acceptance is deliberately narrower than broker-paper
        # execution/recovery acceptance.
        trust_tier=TrustTier.UNVERIFIED,
    )
    reconciliation_reference = "ibkr-account-read-" + canonical_hash(
        {
            "provider_version": IBKR_ACCOUNT_PROVIDER_VERSION,
            "gateway_server_version": report.gateway_server_version,
            "gateway_timezone": report.gateway_timezone,
            "as_of": report.as_of.isoformat(),
            "reconciled_at": report.reconciled_at.isoformat(),
            "barriers": [
                report.account_barrier_complete,
                report.account_summary_barrier_complete,
                report.open_order_barrier_complete,
                report.execution_barrier_complete,
            ],
            "section_counts": {
                "cash": None if report.cash is None else len(report.cash),
                "positions": None if report.positions is None else len(report.positions),
                "open_orders": None if report.open_orders is None else len(report.open_orders),
                "recent_fills": None if report.recent_fills is None else len(report.recent_fills),
            },
            "gaps": list(report.reconciliation_gaps),
        }
    )
    return capture_account_state_snapshot(
        provider=manifest,
        account_reference=report.account_reference,
        account_reference_key=account_reference_key,
        environment=TradingEnvironment.PAPER,
        as_of=report.as_of,
        reconciled_at=report.reconciled_at,
        reconciliation_reference=reconciliation_reference,
        cash=report.cash,
        positions=report.positions,
        open_orders=report.open_orders,
        recent_fills=report.recent_fills,
        recent_fills_since=report.recent_fills_since,
        reconciliation_gaps=report.reconciliation_gaps,
    )


@dataclass(slots=True)
class _PortfolioItem:
    contract: Contract
    position: Decimal
    market_value: Decimal


@dataclass(slots=True)
class _OpenOrderItem:
    order_id: int
    contract: Contract
    order: Order
    order_state: OrderState


@dataclass(slots=True)
class _ExecutionItem:
    contract: Contract
    execution: Execution


class _IbkrAccountCollector(EWrapper, EClient):
    def __init__(
        self,
        *,
        reference_key: bytes,
        gateway_timezone: str = "Asia/Shanghai",
        configured_timezone: ZoneInfo | None = None,
        clock: Clock,
    ) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self._reference_key = reference_key
        self._gateway_timezone = gateway_timezone
        self._configured_timezone = configured_timezone or ZoneInfo(gateway_timezone)
        self._clock = clock
        self._account_reference = ""
        self._account_values: dict[tuple[str, str], str] = {}
        self._account_summary_values: dict[tuple[str, str], str] = {}
        self._portfolio: dict[tuple[object, ...], _PortfolioItem] = {}
        self._open_orders: dict[str, _OpenOrderItem] = {}
        self._executions: dict[str, _ExecutionItem] = {}
        self._error_codes: set[int] = set()
        self._state_lock = threading.RLock()
        self._frozen = False
        started_at = clock()
        require_aware(started_at, "IBKR account read start time")
        self._started_at = started_at.astimezone(UTC)
        self._account_completed_at: datetime | None = None
        self._account_summary_completed_at: datetime | None = None
        self._orders_completed_at: datetime | None = None
        self._executions_completed_at: datetime | None = None
        self.api_ready = threading.Event()
        self.ready = threading.Event()
        self.complete = threading.Event()

    def close_transport(self) -> None:
        """Stop both official ibapi threads without clearing decode state before queue drain."""

        reader_thread = self.reader
        connection = self.conn
        raw_socket = None if connection is None else connection.socket
        self.setConnState(EClient.DISCONNECTED)
        if raw_socket is not None:
            with suppress(OSError):
                raw_socket.shutdown(socket.SHUT_RDWR)
        if connection is not None:
            connection.disconnect()
        if reader_thread is not None and reader_thread is not threading.current_thread():
            reader_thread.join(timeout=2.0)
            if reader_thread.is_alive():
                raise RuntimeError("IBKR socket reader did not stop after disconnect")

    def nextValidId(self, orderId: int) -> None:
        del orderId
        self.api_ready.set()

    def managedAccounts(self, accountsList: str) -> None:
        with self._state_lock:
            if self._frozen:
                return
            accounts = tuple(item.strip() for item in accountsList.split(",") if item.strip())
            if len(accounts) != 1 or not accounts[0].startswith("DU"):
                self._error_codes.add(-1001)
                self.ready.set()
                return
            if self._account_reference and accounts[0] != self._account_reference:
                self._error_codes.add(-1004)
                self.ready.set()
                return
            self._account_reference = accounts[0]
            self.ready.set()

    def request_snapshot(self) -> None:
        if not self._account_reference:
            raise RuntimeError("IBKR Gateway did not expose exactly one paper account")
        self.reqAccountUpdates(True, self._account_reference)
        self.reqAccountSummary(
            8141,
            "All",
            "NetLiquidation,TotalCashValue,SettledCash,AvailableFunds",
        )
        self.reqAllOpenOrders()
        execution_filter = ExecutionFilter()
        execution_filter.acctCode = self._account_reference
        self.reqExecutions(8142, execution_filter)

    def stop_snapshot(self) -> None:
        if self.isConnected() and self._account_reference:
            self.reqAccountUpdates(False, self._account_reference)
            self.cancelAccountSummary(8141)

    def updateAccountValue(
        self,
        key: str,
        val: str,
        currency: str,
        accountName: str,
    ) -> None:
        with self._state_lock:
            if not self._frozen and accountName == self._account_reference:
                self._account_values[(key, currency)] = val

    def updatePortfolio(
        self,
        contract: Contract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ) -> None:
        del marketPrice, averageCost, unrealizedPNL, realizedPNL
        with self._state_lock:
            if self._frozen or accountName != self._account_reference:
                return
            key = _contract_key(contract)
            normalized_position = Decimal(position)
            if normalized_position == 0:
                self._portfolio.pop(key, None)
                return
            self._portfolio[key] = _PortfolioItem(
                contract=contract,
                position=normalized_position,
                market_value=Decimal(str(marketValue)),
            )

    def accountDownloadEnd(self, accountName: str) -> None:
        with self._state_lock:
            if not self._frozen and accountName == self._account_reference:
                self._account_completed_at = self._clock()
                self._mark_complete_if_ready()

    def accountSummary(
        self,
        reqId: int,
        account: str,
        tag: str,
        value: str,
        currency: str,
    ) -> None:
        with self._state_lock:
            if not self._frozen and reqId == 8141 and account == self._account_reference:
                self._account_summary_values[(tag, currency)] = value

    def accountSummaryEnd(self, reqId: int) -> None:
        with self._state_lock:
            if not self._frozen and reqId == 8141:
                self._account_summary_completed_at = self._clock()
                self._mark_complete_if_ready()

    def openOrder(
        self,
        orderId: int,
        contract: Contract,
        order: Order,
        orderState: OrderState,
    ) -> None:
        with self._state_lock:
            if not self._frozen and order.account in {"", self._account_reference}:
                key = (
                    f"perm:{order.permId}"
                    if order.permId > 0
                    else f"client:{order.clientId}:order:{orderId}"
                )
                self._open_orders[key] = _OpenOrderItem(
                    order_id=orderId,
                    contract=contract,
                    order=order,
                    order_state=orderState,
                )

    def openOrderEnd(self) -> None:
        with self._state_lock:
            if not self._frozen:
                self._orders_completed_at = self._clock()
                self._mark_complete_if_ready()

    def execDetails(self, reqId: int, contract: Contract, execution: Execution) -> None:
        with self._state_lock:
            if (
                not self._frozen
                and reqId == 8142
                and execution.acctNumber == self._account_reference
            ):
                execution_key = execution.execId.strip()
                if not execution_key:
                    self._error_codes.add(-1003)
                    return
                self._executions[execution_key] = _ExecutionItem(
                    contract=contract,
                    execution=execution,
                )

    def execDetailsEnd(self, reqId: int) -> None:
        with self._state_lock:
            if not self._frozen and reqId == 8142:
                self._executions_completed_at = self._clock()
                self._mark_complete_if_ready()

    def error(
        self,
        reqId: int,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        del reqId, errorTime, errorString, advancedOrderRejectJson
        with self._state_lock:
            if not self._frozen and errorCode not in _INFORMATIONAL_ERROR_CODES:
                self._error_codes.add(errorCode)

    def connectionClosed(self) -> None:
        with self._state_lock:
            if not self._frozen:
                self._error_codes.add(-1002)

    def report(self) -> IbkrPaperAccountReadReport:
        with self._state_lock:
            if not self._frozen:
                raise RuntimeError("IBKR paper account read is not frozen at its barriers")
            return self._report_locked()

    def _report_locked(self) -> IbkrPaperAccountReadReport:
        if not self._account_reference:
            raise RuntimeError("IBKR Gateway did not expose exactly one paper account")
        completed_times = (
            self._account_completed_at,
            self._account_summary_completed_at,
            self._orders_completed_at,
            self._executions_completed_at,
        )
        if any(item is None for item in completed_times):
            raise RuntimeError("IBKR paper account read did not reach every reconciliation barrier")
        reconciled_at = max(item for item in completed_times if item is not None)
        cash, cash_gaps = self._cash()
        positions, position_gaps = self._positions()
        open_orders, order_gaps = self._normalized_open_orders()
        recent_fills, fill_gaps = self._recent_fills()
        error_gaps = tuple(f"ibkr_error_code:{code}" for code in sorted(self._error_codes))
        return IbkrPaperAccountReadReport(
            account_reference=self._account_reference,
            as_of=reconciled_at,
            reconciled_at=reconciled_at,
            gateway_server_version=self.serverVersion() or 0,
            gateway_timezone=self._gateway_timezone,
            account_barrier_complete=self._account_completed_at is not None,
            account_summary_barrier_complete=self._account_summary_completed_at is not None,
            open_order_barrier_complete=self._orders_completed_at is not None,
            execution_barrier_complete=self._executions_completed_at is not None,
            cash=cash,
            positions=positions,
            open_orders=open_orders,
            recent_fills=recent_fills,
            recent_fills_since=self._started_at.astimezone(self._configured_timezone)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .astimezone(UTC),
            reconciliation_gaps=tuple(
                sorted(set(cash_gaps + position_gaps + order_gaps + fill_gaps + error_gaps))
            ),
        )

    def _cash(self) -> tuple[tuple[CashBalance, ...] | None, tuple[str, ...]]:
        currencies = {
            currency
            for key, currency in self._account_summary_values
            if key in {"AvailableFunds", "SettledCash"} and currency not in {"", "BASE"}
        }
        balances: list[CashBalance] = []
        gaps: list[str] = []
        for currency in sorted(currencies):
            available = self._summary_decimal("AvailableFunds", currency)
            settled = self._summary_decimal("SettledCash", currency)
            if settled is None:
                settled = self._settled_cash_by_date(currency=currency, currencies=currencies)
            if available is None or settled is None:
                gaps.append(f"cash_fields_missing:{currency}")
                continue
            balances.append(CashBalance(currency=currency, available=available, settled=settled))
        if not balances:
            return None, tuple(gaps or ["cash_not_reported"])
        return tuple(balances), tuple(gaps)

    def _summary_decimal(self, key: str, currency: str) -> Decimal | None:
        raw = self._account_summary_values.get((key, currency))
        if raw is None:
            return None
        try:
            return Decimal(raw)
        except InvalidOperation:
            return None

    def _settled_cash_by_date(
        self,
        *,
        currency: str,
        currencies: set[str],
    ) -> Decimal | None:
        if len(currencies) != 1 or currency not in currencies:
            return None
        raw_values = [
            value
            for (key, _reported_currency), value in self._account_values.items()
            if key == "SettledCashByDate"
        ]
        if len(raw_values) != 1:
            return None
        date_text, separator, amount_text = raw_values[0].partition(":")
        if not separator:
            return None
        try:
            settlement_date = datetime.strptime(date_text, "%Y%m%d").date()
            amount = Decimal(amount_text)
        except (InvalidOperation, ValueError):
            return None
        completed_at = self._account_completed_at
        if (
            completed_at is None
            or settlement_date > completed_at.astimezone(self._configured_timezone).date()
        ):
            return None
        return amount

    def _positions(self) -> tuple[tuple[AccountPosition, ...], tuple[str, ...]]:
        net_liquidation = self._net_liquidation()
        positions: list[AccountPosition] = []
        gaps: list[str] = []
        for item in self._portfolio.values():
            target_id, venue, instrument_class = _instrument_identity(item.contract)
            side = Side.BUY if item.position > 0 else Side.SELL
            concentration: Decimal | None = None
            concentration_gap: str | None = None
            if net_liquidation is None or net_liquidation <= 0:
                concentration_gap = "net_liquidation_not_reconciled"
                gaps.append(f"position_concentration:{target_id}:{venue}:{instrument_class}:{side}")
            else:
                concentration = abs(item.market_value) / net_liquidation
                if concentration > 1:
                    concentration_gap = "market_value_exceeds_net_liquidation"
                    concentration = None
                    gaps.append(
                        f"position_concentration:{target_id}:{venue}:{instrument_class}:{side}"
                    )
            positions.append(
                AccountPosition(
                    target_id=target_id,
                    venue=venue,
                    instrument_class=instrument_class,
                    side=side,
                    quantity=abs(item.position),
                    concentration=concentration,
                    concentration_gap=concentration_gap,
                )
            )
        return tuple(positions), tuple(gaps)

    def _net_liquidation(self) -> Decimal | None:
        candidates = [
            value
            for (key, _currency), value in self._account_summary_values.items()
            if key == "NetLiquidation"
        ]
        candidates.extend(
            value
            for (key, _currency), value in self._account_values.items()
            if key == "NetLiquidation"
        )
        for raw in candidates:
            try:
                value = Decimal(raw)
            except InvalidOperation:
                continue
            if value > 0:
                return value
        return None

    def _normalized_open_orders(self) -> tuple[tuple[OpenOrder, ...] | None, tuple[str, ...]]:
        # A nonzero API client can request all API-originated open orders without
        # binding them, but IBKR does not expose manually submitted TWS orders
        # through that request. Binding those orders requires client 0 and can
        # change broker order ownership/queue state, so this read-only adapter
        # preserves the coverage limit as a hard reconciliation gap.
        gaps = ["manual_tws_open_orders_not_observed"]
        if self._open_orders:
            # openOrder does not carry a true submission timestamp. activeStartTime
            # is an activation condition, not order provenance, so the whole typed
            # section stays absent rather than fabricating submitted_at.
            gaps.append("api_open_order_submission_time_not_reported")
            return None, tuple(gaps)
        return (), tuple(gaps)

    def _recent_fills(self) -> tuple[tuple[RecentFill, ...] | None, tuple[str, ...]]:
        result: list[RecentFill] = []
        for item in self._executions.values():
            filled_at = _parse_ibkr_time(
                item.execution.time,
                fallback_timezone=self._configured_timezone,
            )
            if filled_at is None:
                return None, ("execution_time_not_parseable",)
            target_id, venue, instrument_class = _instrument_identity(item.contract)
            result.append(
                RecentFill(
                    fill_reference=_opaque_reference(
                        "ibkr-fill", item.execution.execId, self._reference_key
                    ),
                    order_reference=_opaque_reference(
                        "ibkr-order",
                        str(item.execution.permId or item.execution.orderId),
                        self._reference_key,
                    ),
                    target_id=target_id,
                    venue=venue,
                    instrument_class=instrument_class,
                    side=_side(item.execution.side),
                    quantity=abs(Decimal(item.execution.shares)),
                    filled_at=filled_at,
                )
            )
        return tuple(result), ()

    def _mark_complete_if_ready(self) -> None:
        if (
            self._account_completed_at is not None
            and self._account_summary_completed_at is not None
            and self._orders_completed_at is not None
            and self._executions_completed_at is not None
        ):
            self._frozen = True
            self.complete.set()


def _opaque_reference(prefix: str, raw: str, key: bytes) -> str:
    return f"{prefix}-{hmac.new(key, raw.encode(), hashlib.sha256).hexdigest()}"


def _instrument_identity(contract: Contract) -> tuple[str, str, str]:
    target_id = str(contract.localSymbol or contract.symbol or f"conid-{contract.conId}").strip()
    venue = str(contract.primaryExchange or contract.exchange or "SMART").strip()
    instrument_class = str(contract.secType or "unknown").strip().lower()
    if not target_id or not venue or not instrument_class:
        raise ValueError("IBKR contract lacks a provider-neutral instrument identity")
    return target_id, venue, instrument_class


def _contract_key(contract: Contract) -> tuple[object, ...]:
    return (
        int(contract.conId),
        str(contract.localSymbol),
        str(contract.symbol),
        str(contract.secType),
        str(contract.primaryExchange),
        str(contract.exchange),
    )


def _side(value: str) -> Side:
    normalized = value.upper()
    if normalized in {"BUY", "BOT"}:
        return Side.BUY
    if normalized in {"SELL", "SLD", "SSHORT"}:
        return Side.SELL
    raise ValueError("IBKR side is not supported")


def _parse_ibkr_time(value: str, *, fallback_timezone: ZoneInfo) -> datetime | None:
    normalized = " ".join(value.strip().split())
    if not normalized:
        return None
    parts = normalized.split(" ")
    if len(parts) not in {2, 3}:
        return None
    core = " ".join(parts[:2])
    timezone = fallback_timezone
    if len(parts) == 3:
        try:
            timezone = ZoneInfo(parts[2])
        except ZoneInfoNotFoundError:
            return None
    for pattern in ("%Y%m%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(core, pattern).replace(tzinfo=timezone).astimezone(UTC)
        except ValueError:
            continue
    return None
