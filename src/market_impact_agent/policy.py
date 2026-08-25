from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from market_impact_agent.domain import (
    ApprovalMode,
    HardPolicyDecision,
    HardPolicyOutcome,
    OrderIntent,
    TradingMandate,
    require_aware,
)


class HardPolicyEvaluator:
    """Evaluate non-overridable order preconditions before semantic approval."""

    def evaluate(
        self,
        order: OrderIntent,
        mandate: TradingMandate,
        *,
        now: datetime,
        reference_price: Decimal | None = None,
    ) -> HardPolicyDecision:
        require_aware(now, "now")
        denial_reasons: list[str] = []

        if mandate.approval_mode is ApprovalMode.DISABLED:
            denial_reasons.append("mandate_disabled")
        if order.account_id != mandate.account_id:
            denial_reasons.append("account_mismatch")
        if order.environment is not mandate.environment:
            denial_reasons.append("environment_mismatch")
        if now < order.created_at:
            denial_reasons.append("order_intent_not_active")
        if now >= order.expires_at:
            denial_reasons.append("order_intent_expired")
        if not mandate.valid_from <= now < mandate.expires_at:
            denial_reasons.append("mandate_not_active")
        if order.instrument_id not in mandate.allowed_instruments:
            denial_reasons.append("instrument_not_allowed")
        if order.side not in mandate.allowed_sides:
            denial_reasons.append("side_not_allowed")

        price = order.limit_price if order.limit_price is not None else reference_price
        if price is not None:
            if not price.is_finite() or price <= 0:
                denial_reasons.append("reference_price_invalid")
            elif order.quantity * price > mandate.max_order_notional:
                denial_reasons.append("max_order_notional_exceeded")

        if denial_reasons:
            return HardPolicyDecision(HardPolicyOutcome.DENY, tuple(denial_reasons))
        if price is None:
            return HardPolicyDecision(
                HardPolicyOutcome.REQUIRE_MANUAL,
                ("reference_price_required",),
            )
        if mandate.approval_mode is ApprovalMode.MANUAL_EACH:
            return HardPolicyDecision(
                HardPolicyOutcome.REQUIRE_MANUAL,
                ("manual_approval_required",),
            )
        return HardPolicyDecision(HardPolicyOutcome.ELIGIBLE, ())
