# pyright: reportMissingTypeStubs=false, reportPrivateUsage=false
from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest
from ibapi.contract import Contract
from ibapi.execution import Execution
from ibapi.order import Order
from ibapi.order_state import OrderState

from market_impact_agent.ibkr_account_read import (
    IbkrPaperAccountReader,
    IbkrPaperAccountReadReport,
    _IbkrAccountCollector,
    capture_ibkr_paper_account_snapshot,
)


def _contract() -> Contract:
    contract = Contract()
    contract.symbol = "DEMO"
    contract.localSymbol = "DEMO"
    contract.secType = "STK"
    contract.primaryExchange = "NASDAQ"
    return contract


def test_ibkr_callback_barriers_normalize_and_remove_raw_broker_identifiers() -> None:
    at = datetime(2026, 9, 1, 8, tzinfo=UTC)
    collector = _IbkrAccountCollector(
        reference_key=b"fixture-ibkr-reference-key-material",
        clock=lambda: at,
    )
    collector.nextValidId(1)
    assert collector.api_ready.is_set()
    raw_account = "DU-fixture-private-account"
    collector.managedAccounts(raw_account)
    collector.updateAccountValue("Currency", "USD", "", raw_account)
    collector.updateAccountValue("AvailableFunds", "1000", "BASE", raw_account)
    collector.updateAccountValue("SettledCash", "900", "BASE", raw_account)
    collector.updateAccountValue("NetLiquidation", "10000", "BASE", raw_account)
    collector.accountSummary(8141, raw_account, "AvailableFunds", "1000", "USD")
    collector.updateAccountValue("SettledCashByDate", "20260901:900", "", raw_account)
    collector.accountSummary(8141, raw_account, "NetLiquidation", "10000", "USD")
    collector.updatePortfolio(_contract(), Decimal("5"), 20, 100, 10, 50, 0, raw_account)

    order = Order()
    order.account = raw_account
    order.permId = 123456
    order.action = "SELL"
    order.totalQuantity = Decimal("2")
    order.activeStartTime = "20260901 07:55:00 UTC"
    order_state = OrderState()
    order_state.status = "Submitted"
    collector.openOrder(12, _contract(), order, order_state)

    execution = Execution()
    execution.acctNumber = raw_account
    execution.execId = "private-execution-id"
    execution.permId = 123456
    execution.side = "SLD"
    execution.shares = Decimal("1")
    execution.time = "20260901 07:58:00 UTC"
    collector.execDetails(8142, _contract(), execution)
    collector.accountDownloadEnd(raw_account)
    collector.accountSummaryEnd(8141)
    collector.openOrderEnd()
    collector.execDetailsEnd(8142)

    report = replace(collector.report(), gateway_server_version=188)
    report.assert_accepted_read()
    assert report.cash is not None and report.cash[0].available == Decimal("1000")
    assert report.positions is not None and report.positions[0].concentration == Decimal("0.01")
    assert report.open_orders is None
    assert report.recent_fills is not None and len(report.recent_fills) == 1
    assert report.recent_fills[0].filled_at == datetime(2026, 9, 1, 7, 58, tzinfo=UTC)
    assert report.recent_fills_since == datetime(2026, 8, 31, 16, tzinfo=UTC)
    assert report.reconciliation_gaps == (
        "api_open_order_submission_time_not_reported",
        "manual_tws_open_orders_not_observed",
    )
    serialized = json.dumps(
        {
            "cash": [item.to_dict() for item in report.cash],
            "positions": [item.to_dict() for item in report.positions],
            "orders": None,
            "fills": [item.to_dict() for item in report.recent_fills],
        },
        sort_keys=True,
    )
    assert raw_account not in serialized
    assert "private-execution-id" not in serialized
    assert "123456" not in serialized


def test_unknown_open_order_time_degrades_section_instead_of_fabricating() -> None:
    at = datetime(2026, 9, 1, 8, tzinfo=UTC)
    collector = _IbkrAccountCollector(
        reference_key=b"fixture-ibkr-reference-key-material",
        clock=lambda: at,
    )
    raw_account = "DU-fixture-private-account"
    collector.managedAccounts(raw_account)
    collector.updateAccountValue("Currency", "USD", "", raw_account)
    collector.updateAccountValue("AvailableFunds", "1000", "BASE", raw_account)
    collector.updateAccountValue("SettledCash", "900", "BASE", raw_account)
    collector.accountSummary(8141, raw_account, "AvailableFunds", "1000", "USD")
    collector.updateAccountValue("SettledCashByDate", "20260901:900", "", raw_account)
    order = Order()
    order.account = raw_account
    order.action = "BUY"
    order.totalQuantity = Decimal("1")
    order_state = OrderState()
    order_state.status = "Submitted"
    collector.openOrder(1, _contract(), order, order_state)
    collector.accountDownloadEnd(raw_account)
    collector.accountSummaryEnd(8141)
    collector.openOrderEnd()
    collector.execDetailsEnd(8142)

    report = collector.report()
    assert report.open_orders is None
    assert "api_open_order_submission_time_not_reported" in report.reconciliation_gaps
    assert "manual_tws_open_orders_not_observed" in report.reconciliation_gaps


def test_barrier_freeze_keeps_repeated_position_snapshot_stable() -> None:
    at = datetime(2026, 9, 1, 8, tzinfo=UTC)
    collector = _IbkrAccountCollector(
        reference_key=b"fixture-ibkr-reference-key-material",
        clock=lambda: at,
    )
    raw_account = "DU-fixture-private-account"
    collector.managedAccounts(raw_account)
    collector.accountSummary(8141, raw_account, "AvailableFunds", "1000", "USD")
    collector.updateAccountValue("SettledCashByDate", "20260901:900", "", raw_account)
    collector.accountSummary(8141, raw_account, "NetLiquidation", "10000", "USD")
    collector.updatePortfolio(_contract(), Decimal("5"), 20, 100, 10, 50, 0, raw_account)
    collector.updatePortfolio(_contract(), Decimal("6"), 20, 120, 10, 50, 0, raw_account)
    collector.accountDownloadEnd(raw_account)
    collector.accountSummaryEnd(8141)
    collector.openOrderEnd()
    collector.execDetailsEnd(8142)

    report = collector.report()
    assert report.positions is not None and len(report.positions) == 1
    assert report.positions[0].quantity == Decimal("6")
    assert report.positions[0].concentration == Decimal("0.012")

    collector.updatePortfolio(_contract(), Decimal("0"), 20, 0, 10, 50, 0, raw_account)
    collector.managedAccounts("DU-different-private-account")
    frozen = collector.report()
    assert frozen.positions is not None and frozen.positions[0].quantity == Decimal("6")
    assert frozen.account_reference == raw_account


def test_account_identity_cannot_change_during_an_active_read() -> None:
    at = datetime(2026, 9, 1, 8, tzinfo=UTC)
    collector = _IbkrAccountCollector(
        reference_key=b"fixture-ibkr-reference-key-material",
        clock=lambda: at,
    )
    raw_account = "DU-fixture-private-account"
    collector.managedAccounts(raw_account)
    collector.managedAccounts("DU-different-private-account")
    collector.accountDownloadEnd(raw_account)
    collector.accountSummaryEnd(8141)
    collector.openOrderEnd()
    collector.execDetailsEnd(8142)

    report = collector.report()
    assert report.account_reference == raw_account
    assert "ibkr_error_code:-1004" in report.reconciliation_gaps


def test_account_capture_rejects_substituted_reader() -> None:
    with pytest.raises(TypeError, match="concrete read-only adapter"):
        capture_ibkr_paper_account_snapshot(
            reader=cast(IbkrPaperAccountReader, object()),
            account_reference_key=b"fixture-ibkr-reference-key-material",
        )


def test_reader_rejects_message_loop_failure_after_barriers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at = datetime(2026, 9, 1, 8, tzinfo=UTC)

    class _FailingCollector:
        def __init__(self, **_: object) -> None:
            self.api_ready = threading.Event()
            self.ready = threading.Event()
            self.complete = threading.Event()
            self.api_ready.set()
            self.ready.set()
            self.complete.set()

        def connect(self, *_: object) -> None:
            pass

        def isConnected(self) -> bool:
            return True

        def run(self) -> None:
            raise TypeError("fixture transport failure")

        def reqManagedAccts(self) -> None:
            pass

        def request_snapshot(self) -> None:
            pass

        def report(self) -> IbkrPaperAccountReadReport:
            return IbkrPaperAccountReadReport(
                account_reference="DU-fixture-private-account",
                as_of=at,
                reconciled_at=at,
                gateway_server_version=188,
                gateway_timezone="Asia/Shanghai",
                account_barrier_complete=True,
                account_summary_barrier_complete=True,
                open_order_barrier_complete=True,
                execution_barrier_complete=True,
                cash=(),
                positions=(),
                open_orders=(),
                recent_fills=(),
                recent_fills_since=at,
                reconciliation_gaps=(),
            )

        def stop_snapshot(self) -> None:
            pass

        def close_transport(self) -> None:
            pass

    from market_impact_agent import ibkr_account_read

    monkeypatch.setattr(ibkr_account_read, "_IbkrAccountCollector", _FailingCollector)
    reader = IbkrPaperAccountReader(clock=lambda: at)
    with pytest.raises(RuntimeError, match="message loop failed") as exc_info:
        reader.read(reference_key=b"fixture-ibkr-reference-key-material")
    assert isinstance(exc_info.value.__cause__, TypeError)
