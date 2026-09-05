# pyright: reportPrivateUsage=false
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.account_state import AccountPosition, CashBalance
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import (
    ExecutionStatus,
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
)
from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
from market_impact_agent.prospective_mock_fills import record_prospective_mock_fill
from market_impact_agent.providers import MockExecutionProvider, _issue_submission_capability

from .test_ashare_security_qualification import CUTOFF, capture_rows
from .test_prospective_ashare_quotes import executable_inputs


def accepted_order(tmp_path: Path, side: Side = Side.BUY) -> MockExecutionProvider:
    at = CUTOFF - timedelta(minutes=2)
    provider = MockExecutionProvider(tmp_path / "mock.sqlite3", clock=lambda: at)
    provider.configure_simulated_account(
        seed="source-fill-test",
        cash=(CashBalance("CNY", Decimal(100000), Decimal(100000)),),
        positions=(
            AccountPosition(
                "600519.SH", "XSHG", "equity", Side.BUY, Decimal(100), Decimal("0.01"), None
            ),
        )
        if side is Side.SELL
        else (),
        instruments={"600519.SH": ("XSHG", "equity")},
        opened_at=at,
        opening_authority={
            "version": "cny-local-mock.v1",
            "source_reference": "synthetic-opening",
            "opening_inventory": "overnight_sellable",
        },
    )
    provider.bind_submission_validator(lambda capability: True)
    order = OrderIntent(
        "buy",
        "signal",
        "synthetic",
        TradingEnvironment.PAPER,
        "600519.SH",
        side,
        Decimal(100),
        OrderKind.MARKET,
        at,
        CUTOFF + timedelta(minutes=5),
    )
    receipt = provider.submit(
        _issue_submission_capability(
            order=order,
            submission_id="buy",
            provider_id=provider.manifest.provider_id,
            provider_version=provider.manifest.provider_version,
            order_hash=canonical_hash(order.to_dict()),
            mandate_hash="a" * 64,
            price_basis_hash="b" * 64,
            policy_evaluation_hash="c" * 64,
            approval_hash="d" * 64,
        )
    )
    assert receipt.status is ExecutionStatus.ACCEPTED and receipt.filled_quantity == 0
    return MockExecutionProvider(tmp_path / "mock.sqlite3", clock=lambda: CUTOFF)


def calendar(market: ExecutableProspectiveAShareInputs, *, skip_day: bool = False):
    snapshot = capture_rows(
        market.store,
        "trade_cal",
        {"exchange": "SSE", "start_date": "20260901", "end_date": "20260902"},
        cast(
            list[dict[str, object]],
            (
                [
                    {
                        "exchange": "SSE",
                        "cal_date": "20260901",
                        "is_open": 0,
                        "pretrade_date": "20260831",
                    }
                ]
                if not skip_day
                else []
            )
            + [
                {
                    "exchange": "SSE",
                    "cal_date": "20260902",
                    "is_open": 1,
                    "pretrade_date": "20260831",
                }
            ],
        ),
        CUTOFF - timedelta(seconds=10),
    )
    return market.with_snapshots((snapshot,))


def test_later_source_fill_fee_t1_and_restart_are_single_economic_fact(tmp_path: Path) -> None:
    provider = accepted_order(tmp_path)
    market = calendar(executable_inputs(tmp_path))
    result = record_prospective_mock_fill(provider, market, "buy")
    assert not result.gaps
    assert result.receipt is not None and result.receipt.status is ExecutionStatus.FILLED
    assert result.receipt.filled_quantity == 100
    assert result.evidence_artifact_hash is not None
    evidence = cast(
        dict[str, object], market.store.artifacts.read_json(result.evidence_artifact_hash)
    )
    assert isinstance(evidence, dict)
    assert evidence["fee"] == "5.00"
    assert evidence["sellable_at"] == "2026-09-02T09:30:00+08:00"
    assert str(evidence["fee_rule_ref"]).startswith("sha256:")
    assert provider.simulated_sellable_quantity("600519.SH") == 0
    assert record_prospective_mock_fill(provider, market, "buy") == result
    restarted = MockExecutionProvider(
        tmp_path / "mock.sqlite3", clock=lambda: CUTOFF + timedelta(days=2)
    )
    assert record_prospective_mock_fill(restarted, market, "buy") == result
    assert restarted.simulated_sellable_quantity("600519.SH") == 100
    with restarted._connect() as connection:
        rows = connection.execute("SELECT * FROM mock_execution_fills").fetchall()
    assert len(rows) == 1
    assert Decimal(rows[0]["quantity"]) * Decimal(rows[0]["price"]) + Decimal(
        rows[0]["fee"]
    ) == Decimal("1006")


@pytest.mark.parametrize(
    "mode,gap",
    [
        ("calendar", "next_open_trading_date_unverified"),
        ("calendar_hole", "next_open_trading_date_unverified"),
        ("before_submission", "post_submission_quote_required"),
        ("corporate_action", "current_corporate_action_factor_change_unresolved"),
        ("future_receipt", "fresh_intraday_quote_missing"),
    ],
)
def test_insufficient_sources_do_not_mutate_account(tmp_path: Path, mode: str, gap: str) -> None:
    provider = accepted_order(tmp_path)
    market = executable_inputs(
        tmp_path,
        factor="1.2" if mode == "corporate_action" else "1",
        future=mode == "future_receipt",
    )
    if mode == "calendar_hole":
        market = calendar(market, skip_day=True)
    elif mode != "calendar":
        market = calendar(market)
    if mode == "before_submission":
        # The same valid quote cannot fill an order accepted at that quote's timestamp.
        with provider._connect() as connection:
            connection.execute(
                "UPDATE mock_execution_receipts SET observed_at = ?",
                ((CUTOFF - timedelta(minutes=1)).isoformat(),),
            )
    result = record_prospective_mock_fill(provider, market, "buy")
    assert result.receipt is None and gap in result.gaps
    assert provider.reconcile().receipts[0].status is ExecutionStatus.ACCEPTED
    with provider._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM mock_execution_fills").fetchone()[0] == 0


def test_source_qualified_sell_includes_stamp_tax_without_t1_calendar(tmp_path: Path) -> None:
    provider = accepted_order(tmp_path, Side.SELL)
    market = executable_inputs(tmp_path)
    result = record_prospective_mock_fill(provider, market, "buy")
    assert result.receipt is not None and result.receipt.status is ExecutionStatus.FILLED
    assert result.evidence_artifact_hash is not None
    authority = cast(
        dict[str, object], market.store.artifacts.read_json(result.evidence_artifact_hash)
    )
    assert isinstance(authority, dict)
    assert authority["fee"] == "5.50"
    assert authority["sellable_at"] is None
    assert provider.simulated_sellable_quantity("600519.SH") == 0
