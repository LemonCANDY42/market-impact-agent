from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from market_impact_agent.account_state import (
    AccountPosition,
    CashBalance,
    OpenOrder,
    OpenOrderStatus,
    PositionSnapshot,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRuleSet,
    load_exchange_instrument_rule_set,
)
from market_impact_agent.domain import (
    ApprovalMode,
    OrderKind,
    Side,
    SignalIntent,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.portfolio_decision import (
    OrderSizingOutcome,
    OrderSizingPolicy,
    PortfolioAction,
    PortfolioDecisionOutcome,
    build_order_intent_from_sizing,
    evaluate_portfolio_decision,
    size_portfolio_decision,
)
from market_impact_agent.providers import (
    Capability,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)

ROOT = Path(__file__).parents[1]
AT = datetime(2026, 9, 1, 8, tzinfo=UTC)
TARGET = "600028.SH"


def _provider() -> ProviderManifest:
    return ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="fixture-account",
        provider_version="1",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("XSHG",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )


def _position_snapshot(
    *,
    position: AccountPosition | None = None,
    open_order: OpenOrder | None = None,
    gaps: tuple[str, ...] = (),
    available: Decimal = Decimal("10000"),
) -> PositionSnapshot:
    account = capture_account_state_snapshot(
        provider=_provider(),
        account_reference="fixture-portfolio-account",
        account_reference_key=b"fixture-portfolio-key-material-32-bytes",
        environment=TradingEnvironment.PAPER,
        as_of=AT,
        reconciled_at=AT,
        reconciliation_reference="fixture-portfolio-reconciliation",
        cash=(CashBalance(currency="CNY", available=available, settled=available),),
        positions=() if position is None else (position,),
        open_orders=() if open_order is None else (open_order,),
        recent_fills=(),
        recent_fills_since=AT - timedelta(days=1),
        reconciliation_gaps=gaps,
    )
    return account.project_positions(evaluated_at=AT, max_age=timedelta(minutes=5))


def _view(position: PositionSnapshot) -> AuthorizedDecisionView:
    return AuthorizedDecisionView.build(
        cutoff=AT,
        frozen_at=AT + timedelta(seconds=1),
        data_snapshot_ids=("data-snapshot-fixture",),
        decision_input_ids=("decision-input-fixture",),
        position_snapshot=position,
    )


def _signal(*, side: Side = Side.BUY) -> SignalIntent:
    return SignalIntent(
        signal_id="signal-portfolio-fixture",
        event_id="event-fixture",
        instrument_id=TARGET,
        side=side,
        valid_from=AT,
        expires_at=AT + timedelta(minutes=10),
        evidence_refs=("evidence-1",),
        invalidation_conditions=("event_reversed",),
    )


def _mandate(
    position: PositionSnapshot,
    *,
    sides: frozenset[Side],
    max_order_notional: Decimal = Decimal("1000"),
) -> TradingMandate:
    return TradingMandate(
        mandate_id="mandate-portfolio-fixture",
        account_id=position.account_reference_hash,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=AT,
        expires_at=AT + timedelta(hours=1),
        allowed_instruments=frozenset({TARGET}),
        allowed_sides=sides,
        max_order_notional=max_order_notional,
    )


def _price(*, kind: str = "raw_reference_quote") -> PriceBasis:
    return PriceBasis(
        instrument_id=TARGET,
        currency="CNY",
        unit="per_share",
        basis_kind=kind,
        price=Decimal("10"),
        source_id="fixture-quote",
        source_version="1",
        observed_at=AT,
        valid_until=AT + timedelta(minutes=5),
    )


def _rules() -> ExchangeInstrumentRuleSet:
    return load_exchange_instrument_rule_set(
        ROOT / "examples" / "research" / "a-share-exchange-instrument-rules-v1.json"
    )


def _policy() -> OrderSizingPolicy:
    return OrderSizingPolicy(
        max_available_cash_fraction=Decimal("0.20"),
        reduction_fraction=Decimal("0.50"),
    )


def test_open_decision_sizes_from_cash_mandate_raw_price_and_buy_lot() -> None:
    position = _position_snapshot()
    signal = _signal()
    decision = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=_view(position),
        position_snapshot=position,
        requested_action=PortfolioAction.OPEN,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )
    mandate = _mandate(position, sides=frozenset({Side.BUY}))
    sizing = size_portfolio_decision(
        portfolio_decision=decision,
        position_snapshot=position,
        mandate=mandate,
        price_basis=_price(),
        rule_set=_rules(),
        sizing_policy=_policy(),
        order_kind=OrderKind.MARKET,
        decided_at=AT + timedelta(seconds=3),
    )
    order = build_order_intent_from_sizing(
        sizing_decision=sizing,
        signal=signal,
        mandate=mandate,
        expires_at=AT + timedelta(minutes=5),
    )

    assert decision.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING
    assert sizing.outcome is OrderSizingOutcome.READY
    assert sizing.quantity == Decimal("100")
    assert sizing.order_notional == Decimal("1000")
    assert order.quantity == Decimal("100")
    assert order.client_order_id.startswith("portfolio-order-")
    assert "confidence" not in sizing.to_dict()
    assert validate_agent_contract(decision.to_dict(), "portfolio-decision.schema.json") == ()
    assert validate_agent_contract(sizing.to_dict(), "order-sizing-decision.schema.json") == ()


def test_exposure_gap_and_same_target_open_order_block_increase_before_sizing() -> None:
    open_order = OpenOrder(
        order_reference="order-existing",
        target_id=TARGET,
        venue="XSHG",
        instrument_class="equity",
        side=Side.BUY,
        quantity=Decimal("100"),
        status=OpenOrderStatus.WORKING,
        submitted_at=AT - timedelta(minutes=1),
    )
    position = _position_snapshot(
        open_order=open_order,
        gaps=("manual_tws_open_orders_not_observed",),
    )
    decision = evaluate_portfolio_decision(
        signal=_signal(),
        authorized_view=_view(position),
        position_snapshot=position,
        requested_action=PortfolioAction.OPEN,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=("evidence-1",),
        decided_at=AT + timedelta(seconds=2),
    )

    assert decision.outcome is PortfolioDecisionOutcome.REJECTED
    assert decision.blockers == ("exposure_increase_not_ready", "open_order_conflict")
    with pytest.raises(PermissionError, match="ready Portfolio Decision"):
        size_portfolio_decision(
            portfolio_decision=decision,
            position_snapshot=position,
            mandate=_mandate(position, sides=frozenset({Side.BUY})),
            price_basis=_price(),
            rule_set=_rules(),
            sizing_policy=_policy(),
            order_kind=OrderKind.MARKET,
            decided_at=AT + timedelta(seconds=3),
        )


def test_risk_reduction_is_blocked_when_manual_order_coverage_is_unknown() -> None:
    holding = AccountPosition(
        target_id=TARGET,
        venue="XSHG",
        instrument_class="equity",
        side=Side.BUY,
        quantity=Decimal("1000"),
        concentration=Decimal("0.25"),
        concentration_gap=None,
    )
    position = _position_snapshot(
        position=holding,
        gaps=("manual_tws_open_orders_not_observed",),
    )
    signal = _signal(side=Side.SELL)
    decision = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=_view(position),
        position_snapshot=position,
        requested_action=PortfolioAction.REDUCE,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )
    assert decision.outcome is PortfolioDecisionOutcome.REJECTED
    assert decision.blockers == ("order_state_not_authoritative",)


def test_long_reduction_waits_for_an_accepted_sell_rule() -> None:
    holding = AccountPosition(
        target_id=TARGET,
        venue="XSHG",
        instrument_class="equity",
        side=Side.BUY,
        quantity=Decimal("1000"),
        concentration=Decimal("0.25"),
        concentration_gap=None,
    )
    position = _position_snapshot(position=holding)
    signal = _signal(side=Side.SELL)
    decision = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=_view(position),
        position_snapshot=position,
        requested_action=PortfolioAction.REDUCE,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )
    sizing = size_portfolio_decision(
        portfolio_decision=decision,
        position_snapshot=position,
        mandate=_mandate(
            position,
            sides=frozenset({Side.SELL}),
            max_order_notional=Decimal("10000"),
        ),
        price_basis=_price(),
        rule_set=_rules(),
        sizing_policy=_policy(),
        order_kind=OrderKind.MARKET,
        decided_at=AT + timedelta(seconds=3),
    )

    assert decision.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING
    assert sizing.outcome is OrderSizingOutcome.REJECTED
    assert sizing.blockers == ("sell_tradability_rule_not_accepted",)


def test_short_close_obeys_the_accepted_buy_lot_rule() -> None:
    holding = AccountPosition(
        target_id=TARGET,
        venue="XSHG",
        instrument_class="equity",
        side=Side.SELL,
        quantity=Decimal("150"),
        concentration=Decimal("0.25"),
        concentration_gap=None,
    )
    position = _position_snapshot(position=holding)
    signal = _signal(side=Side.BUY)
    decision = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=_view(position),
        position_snapshot=position,
        requested_action=PortfolioAction.CLOSE,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )
    sizing = size_portfolio_decision(
        portfolio_decision=decision,
        position_snapshot=position,
        mandate=_mandate(
            position,
            sides=frozenset({Side.BUY}),
            max_order_notional=Decimal("10000"),
        ),
        price_basis=_price(),
        rule_set=_rules(),
        sizing_policy=_policy(),
        order_kind=OrderKind.MARKET,
        decided_at=AT + timedelta(seconds=3),
    )

    assert decision.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING
    assert sizing.outcome is OrderSizingOutcome.REJECTED
    assert sizing.blockers == ("close_quantity_not_lot_aligned",)


def test_hold_is_no_action_and_rotation_is_an_explicit_linked_decision_blocker() -> None:
    holding = AccountPosition(
        target_id=TARGET,
        venue="XSHG",
        instrument_class="equity",
        side=Side.BUY,
        quantity=Decimal("100"),
        concentration=Decimal("0.10"),
        concentration_gap=None,
    )
    position = _position_snapshot(position=holding)
    view = _view(position)
    signal = _signal()
    hold = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=view,
        position_snapshot=position,
        requested_action=PortfolioAction.HOLD,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )
    rotate = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=view,
        position_snapshot=position,
        requested_action=PortfolioAction.ROTATE,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )

    assert hold.outcome is PortfolioDecisionOutcome.NO_ACTION
    assert rotate.outcome is PortfolioDecisionOutcome.REJECTED
    assert rotate.blockers == ("rotate_requires_linked_portfolio_decisions",)


def test_sizing_rejects_adjusted_price_and_unbound_account() -> None:
    position = _position_snapshot()
    signal = _signal()
    decision = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=_view(position),
        position_snapshot=position,
        requested_action=PortfolioAction.OPEN,
        venue="XSHG",
        instrument_class="equity",
        evidence_refs=signal.evidence_refs,
        decided_at=AT + timedelta(seconds=2),
    )
    wrong_account = TradingMandate(
        mandate_id="wrong-account",
        account_id="account-ref-" + "0" * 64,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=AT,
        expires_at=AT + timedelta(hours=1),
        allowed_instruments=frozenset({TARGET}),
        allowed_sides=frozenset({Side.BUY}),
        max_order_notional=Decimal("1000"),
    )
    with pytest.raises(PermissionError, match="reconciled account"):
        size_portfolio_decision(
            portfolio_decision=decision,
            position_snapshot=position,
            mandate=wrong_account,
            price_basis=_price(),
            rule_set=_rules(),
            sizing_policy=_policy(),
            order_kind=OrderKind.MARKET,
            decided_at=AT + timedelta(seconds=3),
        )

    rejected = size_portfolio_decision(
        portfolio_decision=decision,
        position_snapshot=position,
        mandate=_mandate(position, sides=frozenset({Side.BUY})),
        price_basis=_price(kind="adjusted_close"),
        rule_set=_rules(),
        sizing_policy=_policy(),
        order_kind=OrderKind.MARKET,
        decided_at=AT + timedelta(seconds=3),
    )
    assert rejected.outcome is OrderSizingOutcome.REJECTED
    assert rejected.blockers == ("price_basis_not_raw_tradable",)
