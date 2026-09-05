from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs

from .test_ashare_security_qualification import CUTOFF, capture_rows, captured_inputs


def executable_inputs(
    tmp_path: Path,
    *,
    symbol: str = "600519.SH",
    etf: bool = False,
    factor: str = "1",
    limit: str = "11",
    quote_time: str = "2026-08-31 09:30:00",
    halted: bool = False,
    future: bool = False,
) -> ExecutableProspectiveAShareInputs:
    base = captured_inputs(tmp_path, symbol=symbol, etf=etf)
    received = CUTOFF + timedelta(seconds=1) if future else CUTOFF - timedelta(seconds=10)
    interval: dict[str, object] = {
        "ts_code": symbol,
        "start_date": "20260828",
        "end_date": "20260831",
    }
    snapshots = list(base.snapshot_ids)
    sources: tuple[tuple[str, dict[str, object], list[dict[str, object]]], ...] = (
        (
            "rt_etf_min" if etf else "rt_min",
            {"ts_code": symbol, "freq": "1MIN"},
            [
                {
                    "ts_code": symbol,
                    "time": quote_time,
                    "open": "10",
                    "close": "10.01",
                    "high": "10.02",
                    "low": "9.99",
                    "vol": "10000",
                    "amount": "100100",
                }
            ],
        ),
        (
            "stk_limit",
            interval,
            [
                {
                    "ts_code": symbol,
                    "trade_date": "20260831",
                    "pre_close": "10",
                    "down_limit": "9",
                    "up_limit": limit,
                }
            ],
        ),
        (
            "suspend_d",
            interval,
            [
                {
                    "ts_code": symbol,
                    "trade_date": "20260831",
                    "suspend_type": "S",
                    "suspend_timing": None,
                }
            ]
            if halted
            else [],
        ),
        ("fund_div" if etf else "dividend", {"ts_code": symbol}, []),
        (
            "fund_adj" if etf else "adj_factor",
            interval,
            [
                {"ts_code": symbol, "trade_date": day, "adj_factor": value}
                for day, value in (("20260828", "1"), ("20260831", factor))
            ],
        ),
    )
    for api, params, rows in sources:
        snapshots.append(capture_rows(base.store, api, params, rows, received))
    return ExecutableProspectiveAShareInputs(
        store=base.store,
        snapshot_ids=tuple(snapshots),
        qualification_policy=base.qualification_policy,
    )


@pytest.mark.parametrize(("symbol", "etf"), [("600519.SH", False), ("159919.SZ", True)])
def test_nonseed_current_security_from_acquired_sources_and_generic_rules(
    tmp_path: Path, symbol: str, etf: bool
) -> None:
    market = executable_inputs(tmp_path, symbol=symbol, etf=etf)
    admission = DynamicAShareAdmission(market).discover((symbol,), CUTOFF)[0]
    assert admission.execution_ready, admission.gaps
    assert admission.evidence is not None
    assert admission.evidence.raw_price_observed_at == CUTOFF - timedelta(minutes=1)
    assert admission.evidence.effective_until == CUTOFF + timedelta(minutes=1)
    assert market.with_snapshots(()).reopen_security(symbol, CUTOFF) == admission.evidence


@pytest.mark.parametrize(
    ("argument", "value", "gap"),
    [
        ("factor", "1.2", "current_corporate_action_factor_change_unresolved"),
        ("limit", "12", "current_limit_regime_unsupported"),
        ("quote_time", "2026-08-28 15:00:00", "current_quote_stale"),
        ("future", True, "fresh_intraday_quote_missing"),
        ("halted", True, "halted"),
    ],
)
def test_current_execution_gaps_do_not_invalidate_research(
    tmp_path: Path, argument: str, value: object, gap: str
) -> None:
    market = executable_inputs(tmp_path, **{argument: value})  # type: ignore[arg-type]
    assert market.qualification("600519.SH", CUTOFF).qualified
    admission = DynamicAShareAdmission(market).discover(("600519.SH",), CUTOFF)[0]
    assert not admission.execution_ready and gap in admission.gaps


def test_current_quote_cannot_authorize_lunch_or_later_market_day(tmp_path: Path) -> None:
    market = executable_inputs(tmp_path)
    for at in (CUTOFF + timedelta(hours=3), CUTOFF + timedelta(days=1)):
        admission = DynamicAShareAdmission(market).discover(("600519.SH",), at)[0]
        assert not admission.execution_ready
        assert "current_quote_stale" in admission.gaps


def test_identical_source_rows_reused_across_snapshots_do_not_conflict(tmp_path: Path) -> None:
    market = executable_inputs(tmp_path)
    duplicated = ExecutableProspectiveAShareInputs(
        store=market.store,
        snapshot_ids=(*market.snapshot_ids, *market.snapshot_ids),
        qualification_policy=market.qualification_policy,
    )
    expected = market.reopen_security("600519.SH", CUTOFF)
    assert duplicated.reopen_security("600519.SH", CUTOFF) == expected
