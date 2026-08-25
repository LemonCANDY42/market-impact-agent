from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_impact_agent.domain import (
    ApprovalMode,
    HardPolicyOutcome,
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.policy import HardPolicyEvaluator

NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (ApprovalMode.DISABLED, HardPolicyOutcome.DENY),
        (ApprovalMode.MANUAL_EACH, HardPolicyOutcome.REQUIRE_MANUAL),
        (ApprovalMode.TIMEBOXED, HardPolicyOutcome.ELIGIBLE),
        (ApprovalMode.POLICY_AUTO, HardPolicyOutcome.ELIGIBLE),
        (ApprovalMode.AUTONOMOUS, HardPolicyOutcome.ELIGIBLE),
    ],
)
def test_approval_mode_cannot_bypass_hard_policy(
    mode: ApprovalMode, expected: HardPolicyOutcome
) -> None:
    decision = HardPolicyEvaluator().evaluate(
        make_order(),
        make_mandate(mode=mode),
        now=NOW,
        reference_price=Decimal("10"),
    )
    assert decision.outcome is expected


def test_missing_market_reference_requires_manual() -> None:
    decision = HardPolicyEvaluator().evaluate(
        make_order(),
        make_mandate(),
        now=NOW,
    )
    assert decision.outcome is HardPolicyOutcome.REQUIRE_MANUAL
    assert decision.reasons == ("reference_price_required",)


def test_notional_and_account_violations_are_denied_together() -> None:
    order = make_order(account_id="wrong-account", quantity=Decimal("101"))
    decision = HardPolicyEvaluator().evaluate(
        order,
        make_mandate(),
        now=NOW,
        reference_price=Decimal("10"),
    )
    assert decision.outcome is HardPolicyOutcome.DENY
    assert decision.reasons == ("account_mismatch", "max_order_notional_exceeded")


@pytest.mark.parametrize(
    "reference_price",
    [
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("0"),
        Decimal("-1"),
    ],
)
def test_invalid_reference_price_is_denied_before_notional_math(
    reference_price: Decimal,
) -> None:
    decision = HardPolicyEvaluator().evaluate(
        make_order(),
        make_mandate(),
        now=NOW,
        reference_price=reference_price,
    )
    assert decision.outcome is HardPolicyOutcome.DENY
    assert decision.reasons == ("reference_price_invalid",)


@pytest.mark.parametrize(
    ("created_at", "expires_at", "reason"),
    [
        (
            NOW + timedelta(seconds=1),
            NOW + timedelta(minutes=5),
            "order_intent_not_active",
        ),
        (
            NOW - timedelta(minutes=5),
            NOW,
            "order_intent_expired",
        ),
    ],
)
def test_inactive_order_intent_is_denied(
    created_at: datetime,
    expires_at: datetime,
    reason: str,
) -> None:
    decision = HardPolicyEvaluator().evaluate(
        make_order(created_at=created_at, expires_at=expires_at),
        make_mandate(),
        now=NOW,
        reference_price=Decimal("10"),
    )
    assert decision.outcome is HardPolicyOutcome.DENY
    assert decision.reasons == (reason,)


def make_order(
    *,
    account_id: str = "paper-account",
    quantity: Decimal = Decimal("10"),
    created_at: datetime = NOW,
    expires_at: datetime = NOW + timedelta(minutes=5),
) -> OrderIntent:
    return OrderIntent(
        client_order_id="order-1",
        signal_id="sig-1",
        account_id=account_id,
        environment=TradingEnvironment.PAPER,
        instrument_id="TEST",
        side=Side.BUY,
        quantity=quantity,
        order_kind=OrderKind.MARKET,
        created_at=created_at,
        expires_at=expires_at,
    )


def make_mandate(*, mode: ApprovalMode = ApprovalMode.TIMEBOXED) -> TradingMandate:
    return TradingMandate(
        mandate_id="mandate-1",
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        approval_mode=mode,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        allowed_instruments=frozenset({"TEST"}),
        allowed_sides=frozenset({Side.BUY}),
        max_order_notional=Decimal("1000"),
    )
