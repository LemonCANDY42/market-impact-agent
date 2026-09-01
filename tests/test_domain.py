from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_impact_agent.domain import (
    ApprovalMode,
    OrderIntent,
    OrderKind,
    Side,
    SignalIntent,
    TradingEnvironment,
    TradingMandate,
    TradingMandateV2,
)

NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)


def test_signal_requires_aware_ordered_window_and_exit_condition() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        SignalIntent(
            signal_id="sig-1",
            event_id="evt-1",
            instrument_id="TEST",
            side=Side.BUY,
            valid_from=datetime(2026, 8, 25),
            expires_at=NOW + timedelta(hours=1),
            evidence_refs=("evidence://1",),
            invalidation_conditions=("source retracted",),
        )

    with pytest.raises(ValueError, match="invalidation condition"):
        SignalIntent(
            signal_id="sig-1",
            event_id="evt-1",
            instrument_id="TEST",
            side=Side.BUY,
            valid_from=NOW,
            expires_at=NOW + timedelta(hours=1),
            evidence_refs=("evidence://1",),
            invalidation_conditions=(),
        )


def test_order_kind_and_price_are_consistent() -> None:
    with pytest.raises(ValueError, match="limit orders require"):
        make_order(order_kind=OrderKind.LIMIT)

    with pytest.raises(ValueError, match="market orders cannot"):
        make_order(limit_price=Decimal("10"))

    limit_order = make_order(order_kind=OrderKind.LIMIT, limit_price=Decimal("10"))
    assert limit_order.limit_price == Decimal("10")


@pytest.mark.parametrize("quantity", [Decimal("NaN"), Decimal("Infinity"), Decimal("0")])
def test_order_quantity_must_be_finite_and_positive(quantity: Decimal) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        make_order(quantity=quantity)


def test_order_requires_aware_ordered_expiry() -> None:
    with pytest.raises(ValueError, match="expires_at must be timezone-aware"):
        make_order(expires_at=datetime(2026, 8, 25, 13))

    with pytest.raises(ValueError, match="expires_at must be after created_at"):
        make_order(expires_at=NOW)


def test_mandate_requires_nonempty_scope() -> None:
    with pytest.raises(ValueError, match="allowed_instruments"):
        TradingMandate(
            mandate_id="mandate-1",
            account_id="paper-account",
            environment=TradingEnvironment.PAPER,
            approval_mode=ApprovalMode.TIMEBOXED,
            valid_from=NOW,
            expires_at=NOW + timedelta(hours=1),
            allowed_instruments=frozenset(),
            allowed_sides=frozenset({Side.BUY}),
            max_order_notional=Decimal("1000"),
        )


def test_v2_mandate_owns_one_day_paper_portfolio_limits() -> None:
    mandate = TradingMandateV2(
        mandate_id="mandate-v2",
        account_id="paper-account",
        harness_authority_id="harness-authority-" + "a" * 32,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=NOW,
        valid_until=NOW + timedelta(hours=8),
        allowed_instruments=frozenset({"SPY.ARCA"}),
        allowed_instrument_classes=frozenset({"unlevered_exchange_traded_fund"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        currency="USD",
        gross_exposure_limit=Decimal("10000"),
        minimum_net_exposure=Decimal("-10000"),
        maximum_net_exposure=Decimal("10000"),
        maximum_position_count=10,
        maximum_single_position_fraction=Decimal("1"),
        daily_turnover_limit=Decimal("50000"),
        daily_submission_limit=50,
        daily_loss_kill_threshold=Decimal("300"),
        strategy_peak_drawdown_kill_threshold=Decimal("1000"),
    )

    assert mandate.to_dict()["schema_version"] == "market-impact.trading-mandate.v2"
    assert mandate.to_dict()["kill_on_unknown_ack"] is True
    with pytest.raises(ValueError, match="cannot exceed one day"):
        replace(mandate, valid_until=NOW + timedelta(days=2))

    with pytest.raises(ValueError, match="finite and positive"):
        TradingMandate(
            mandate_id="mandate-1",
            account_id="paper-account",
            environment=TradingEnvironment.PAPER,
            approval_mode=ApprovalMode.TIMEBOXED,
            valid_from=NOW,
            expires_at=NOW + timedelta(hours=1),
            allowed_instruments=frozenset({"TEST"}),
            allowed_sides=frozenset({Side.BUY}),
            max_order_notional=Decimal("Infinity"),
        )


def make_order(
    *,
    order_kind: OrderKind = OrderKind.MARKET,
    limit_price: Decimal | None = None,
    expires_at: datetime = NOW + timedelta(minutes=5),
    quantity: Decimal = Decimal("10"),
) -> OrderIntent:
    return OrderIntent(
        client_order_id="order-1",
        signal_id="sig-1",
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        instrument_id="TEST",
        side=Side.BUY,
        quantity=quantity,
        order_kind=order_kind,
        limit_price=limit_price,
        created_at=NOW,
        expires_at=expires_at,
    )
