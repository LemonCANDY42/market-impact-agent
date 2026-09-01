# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.ibkr_account_read import IbkrPaperAccountReadReport
from market_impact_agent.ibkr_nautilus_paper import (
    IbkrNautilusPaperReadinessProbe,
    _build_report,
    _NautilusReadSnapshot,
)

AT = datetime(2026, 9, 1, 8, tzinfo=UTC)
REFERENCE_KEY = b"fixture-account-reference-key-32b"


def _account_report(
    *,
    gaps: tuple[str, ...] = ("manual_tws_open_orders_not_observed",),
) -> IbkrPaperAccountReadReport:
    return IbkrPaperAccountReadReport(
        account_reference="DU-fixture-paper-account",
        as_of=AT,
        reconciled_at=AT,
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
        recent_fills_since=AT,
        reconciliation_gaps=gaps,
    )


def _snapshot(**changes: object) -> _NautilusReadSnapshot:
    values: dict[str, object] = {
        "connected": True,
        "reconciled": True,
        "portfolio_initialized": True,
        "account_section_count": 1,
        "open_order_count": 0,
        "open_position_count": 0,
        "strategy_count": 0,
    }
    values.update(changes)
    return _NautilusReadSnapshot(**values)  # type: ignore[arg-type]


def test_readiness_report_accepts_read_side_but_keeps_exposure_fail_closed() -> None:
    report = _build_report(
        account_report=_account_report(),
        account_reference_key=REFERENCE_KEY,
        snapshot=_snapshot(),
        observed_at=AT,
        host="127.0.0.1",
        port=4002,
        account_reader_client_id=193,
        nautilus_client_id=194,
    )

    report.assert_read_only_accepted()
    assert report.read_only_accepted
    assert not report.exposure_increase_ready
    assert report.gaps == ("manual_tws_open_orders_not_observed",)
    assert report.account_reference_hash != "DU-fixture-paper-account"
    assert (
        validate_agent_contract(
            report.to_dict(),
            "ibkr-nautilus-paper-readiness-report.schema.json",
        )
        == ()
    )


def test_readiness_report_requires_matching_account_order_and_position_facts() -> None:
    report = _build_report(
        account_report=_account_report(gaps=()),
        account_reference_key=REFERENCE_KEY,
        snapshot=_snapshot(
            account_section_count=0,
            open_order_count=1,
            open_position_count=1,
            strategy_count=1,
        ),
        observed_at=AT,
        host="127.0.0.1",
        port=4002,
        account_reader_client_id=193,
        nautilus_client_id=194,
    )

    assert not report.read_only_accepted
    assert not report.exposure_increase_ready
    assert report.gaps == (
        "nautilus_account_section_count_invalid",
        "nautilus_open_order_count_mismatch",
        "nautilus_open_position_count_mismatch",
        "nautilus_probe_strategy_present",
    )
    with pytest.raises(RuntimeError, match="read-only readiness did not complete"):
        report.assert_read_only_accepted()


def test_readiness_report_rejects_count_mismatch_without_other_blocker() -> None:
    report = _build_report(
        account_report=_account_report(gaps=()),
        account_reference_key=REFERENCE_KEY,
        snapshot=_snapshot(open_order_count=1),
        observed_at=AT,
        host="127.0.0.1",
        port=4002,
        account_reader_client_id=193,
        nautilus_client_id=194,
    )

    assert report.gaps == ("nautilus_open_order_count_mismatch",)
    assert not report.read_only_accepted
    with pytest.raises(RuntimeError, match="read-only readiness did not complete"):
        report.assert_read_only_accepted()


def test_probe_rejects_live_port_remote_host_and_client_collision(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="loopback"):
        IbkrNautilusPaperReadinessProbe(tmp_path, host="gateway.example")
    with pytest.raises(ValueError, match="Paper Gateway port 4002"):
        IbkrNautilusPaperReadinessProbe(tmp_path, port=4001)
    with pytest.raises(ValueError, match="client IDs must be distinct"):
        IbkrNautilusPaperReadinessProbe(
            tmp_path,
            account_reader_client_id=193,
            nautilus_client_id=193,
        )


def test_nautilus_probe_restores_caller_loop_and_joins_observer_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingNode:
        def __init__(self, **_: object) -> None:
            self.trader = SimpleNamespace(is_running=True, strategies=lambda: ())
            self.kernel = SimpleNamespace(
                exec_engine=SimpleNamespace(check_connected=lambda: True),
            )
            self.portfolio = SimpleNamespace(initialized=True)
            self.cache = SimpleNamespace(
                accounts=lambda: (object(),),
                orders_open=lambda: (),
                positions_open=lambda: (),
            )

        def add_exec_client_factory(self, *_: object) -> None:
            pass

        def build(self) -> None:
            pass

        def run(self, *, raise_exception: bool) -> None:
            assert raise_exception
            raise RuntimeError("fixture node failure")

        def stop(self) -> None:
            pass

        async def stop_async(self) -> None:
            pass

        def dispose(self) -> None:
            pass

    from market_impact_agent import ibkr_nautilus_paper

    monkeypatch.setattr(ibkr_nautilus_paper, "TradingNode", _FailingNode)
    probe = IbkrNautilusPaperReadinessProbe(tmp_path)
    previous_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(previous_loop)
    try:
        with pytest.raises(RuntimeError, match="fixture node failure"):
            probe._run_nautilus(_account_report())
        assert asyncio.get_event_loop() is previous_loop
    finally:
        asyncio.set_event_loop(None)
        previous_loop.close()


def test_nautilus_probe_rejects_async_stop_failure_without_hanging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StopFailingNode:
        def __init__(self, **_: object) -> None:
            self.trader = SimpleNamespace(is_running=True, strategies=lambda: ())
            self.kernel = SimpleNamespace(
                exec_engine=SimpleNamespace(check_connected=lambda: True),
            )
            self.portfolio = SimpleNamespace(initialized=True)
            self.cache = SimpleNamespace(
                accounts=lambda: (object(),),
                orders_open=lambda: (),
                positions_open=lambda: (),
            )

        def add_exec_client_factory(self, *_: object) -> None:
            pass

        def build(self) -> None:
            pass

        def run(self, *, raise_exception: bool) -> None:
            assert raise_exception
            asyncio.get_event_loop().run_forever()

        def stop(self) -> None:
            pass

        async def stop_async(self) -> None:
            raise RuntimeError("fixture stop failure")

        def dispose(self) -> None:
            pass

    from market_impact_agent import ibkr_nautilus_paper

    monkeypatch.setattr(ibkr_nautilus_paper, "TradingNode", _StopFailingNode)
    probe = IbkrNautilusPaperReadinessProbe(tmp_path, timeout_seconds=1.0)
    with pytest.raises(RuntimeError, match="nautilus_stop_error:RuntimeError"):
        probe._run_nautilus(_account_report())
