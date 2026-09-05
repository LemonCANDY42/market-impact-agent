# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from market_impact_agent.domain import OrderIntent, OrderKind, Side, TradingEnvironment
from market_impact_agent.nautilus_backtest import AShareDailyBar
from market_impact_agent.streaming_nautilus_account import (
    HistoricalCorporateAction,
    HistoricalInstrumentSpec,
    HistoricalStreamingAccount,
)

D = Decimal
ETF = "510300.SH"
STOCK = "000001.SZ"


def bar(day: int, price: str = "4.00", **changes: object) -> AShareDailyBar:
    values: dict[str, object] = dict(
        session_open_at=datetime(2025, 1, day, 1, 30, tzinfo=UTC),
        session_close_at=datetime(2025, 1, day, 7, tzinfo=UTC),
        previous_close=D(price),
        open=D(price),
        high=D(price),
        low=D(price),
        close=D(price),
        volume=1000000,
        open_bid_quantity=100000,
        open_ask_quantity=100000,
        suspended=False,
    )
    values.update(changes)
    return AShareDailyBar(**values)  # pyright: ignore[reportArgumentType]


def account(path: Path) -> HistoricalStreamingAccount:
    return HistoricalStreamingAccount(
        specs=(
            HistoricalInstrumentSpec(
                ETF, "exchange_traded_fund", "fixture:etf", sell_stamp_tax_rate=D(0)
            ),
        ),
        journal_path=path,
        account_reference="synthetic-test-arm",
        account_reference_key=b"a" * 32,
    )


def intent(
    a: HistoricalStreamingAccount,
    identity: str,
    b: AShareDailyBar,
    side: Side,
    qty: str,
    target: str = ETF,
) -> OrderIntent:
    return OrderIntent(
        identity,
        "test-signal",
        a.account_id,
        TradingEnvironment.BACKTEST,
        target,
        side,
        D(qty),
        OrderKind.MARKET,
        b.session_open_at - timedelta(hours=1),
        b.session_close_at,
    )


def test_streaming_account_real_engine_t1_fees_and_dividend_recovery(tmp_path: Path) -> None:
    path = tmp_path / "account.jsonl"
    a = account(path)
    first = bar(2)
    first_result = a.bootstrap_half_hs300(first)
    engine_id = id(a.engine)
    assert first_result.cash == D("49985.00")
    assert first_result.nav == D("99985.00")
    assert first_result.positions == {ETF: D(12500)}
    assert a.bootstrap_half_hs300(first) == first_result
    a.register_instrument(HistoricalInstrumentSpec(STOCK, "equity", "fixture:stock"))
    second = bar(3)
    stock = bar(3, "10.00")
    action = HistoricalCorporateAction(
        "dividend-1",
        ETF,
        "cash_dividend",
        second.session_open_at,
        "fixture:cash-dividend",
        cash_per_share=D("0.10"),
    )
    result = a.advance_session(
        {ETF: second, STOCK: stock},
        intents=(
            intent(a, "trim", second, Side.SELL, "1000"),
            intent(a, "stock-buy", stock, Side.BUY, "100", STOCK),
            intent(a, "stock-same-day-sell", stock, Side.SELL, "100", STOCK),
        ),
        corporate_actions=(action,),
    )
    assert id(a.engine) == engine_id
    assert result.positions == {ETF: D(11500), STOCK: D(100)}
    assert result.cash == D("54225.00")
    assert len(result.fills) == 2
    assert result.no_fills[0].reason == "t_plus_one_or_insufficient_overnight_position"
    assert result.account_state.complete
    third = bar(6)
    stock3 = bar(6, "10.00")
    sold = a.advance_session(
        {ETF: third, STOCK: stock3},
        intents=(intent(a, "stock-sell", stock3, Side.SELL, "100", STOCK),),
    )
    assert sold.fills[0].commission == D("5.50")
    expected = [r.result_hash for r in a.results]
    a.close()
    recovered = account(path)
    assert [r.result_hash for r in recovered.results] == expected
    recovered.close()


def test_crash_after_durable_input_replays_no_models(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "crash.jsonl"
    a = account(path)
    b = bar(2)
    append = a._append  # pyright: ignore[reportPrivateUsage]

    def crash(value: dict[str, object]) -> None:
        append(value)
        raise RuntimeError("injected process death after durable decision")

    monkeypatch.setattr(a, "_append", crash)
    with pytest.raises(RuntimeError, match="process death"):
        a.advance_session({ETF: b}, intents=(intent(a, "buy", b, Side.BUY, "100"),))
    a.close()
    recovered = account(path)
    clean = account(tmp_path / "clean.jsonl")
    expected = clean.advance_session({ETF: b}, intents=(intent(clean, "buy", b, Side.BUY, "100"),))
    assert recovered.results[-1].result_hash == expected.result_hash
    recovered.close()
    clean.close()


def test_no_fill_and_unsupported_action_do_not_fabricate_execution(tmp_path: Path) -> None:
    a = account(tmp_path / "account.jsonl")
    b = bar(2, suspended=True)
    result = a.advance_session({ETF: b}, intents=(intent(a, "suspended", b, Side.BUY, "100"),))
    assert not result.fills and result.cash == D(100000)
    assert result.no_fills[0].reason == "suspended"
    b3 = bar(3, "4.40", previous_close=D("4.00"))
    result = a.advance_session({ETF: b3}, intents=(intent(a, "limit", b3, Side.BUY, "100"),))
    assert result.no_fills[0].reason == "limit_up_or_no_ask"
    size = a.journal_path.stat().st_size
    b6 = bar(6)
    with pytest.raises(ValueError, match="split transition unaccepted"):
        a.advance_session(
            {ETF: b6},
            corporate_actions=(
                HistoricalCorporateAction(
                    "split", ETF, "split", b6.session_open_at, "fixture:split", split_ratio=D(2)
                ),
            ),
        )
    assert a.journal_path.stat().st_size == size
    a.close()


def test_partial_fill_overnight_odd_lot_close_and_single_writer(tmp_path: Path) -> None:
    path = tmp_path / "partial.jsonl"
    a = account(path)
    with pytest.raises(BlockingIOError):
        account(path)
    b = bar(2, open_ask_quantity=50)
    bought = a.advance_session({ETF: b}, intents=(intent(a, "partial", b, Side.BUY, "100"),))
    assert bought.positions == {ETF: D(50)}
    assert bought.no_fills[0].reason == "engine_partial_fill_unfilled_remainder"
    b3 = bar(3)
    closed = a.advance_session(
        {ETF: b3}, intents=(intent(a, "close-odd-lot", b3, Side.SELL, "50"),)
    )
    assert not closed.positions and len(closed.fills) == 1
    assert closed.cash == D(99990)
    a.close()


@pytest.mark.parametrize("quantity", [0, 100])
def test_failed_opening_allocation_stays_unready_after_restart(
    tmp_path: Path, quantity: int
) -> None:
    path = tmp_path / "failed-seed.jsonl"
    seed = bar(2, open_ask_quantity=quantity)
    a = account(path)
    with pytest.raises(ValueError, match="did not fill completely"):
        a.bootstrap_half_hs300(seed)
    prefix = path.read_bytes()
    a.close()
    restored = account(path)
    try:
        with pytest.raises(ValueError, match="did not fill completely"):
            restored.bootstrap_half_hs300(seed)
        assert path.read_bytes() == prefix
        with pytest.raises(ValueError, match="differs from requested seed"):
            restored.bootstrap_half_hs300(bar(2))
    finally:
        restored.close()
