from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from market_impact_agent.account_state import (
    AccountPosition,
    CashBalance,
    PositionSnapshot,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRule,
    ExchangeInstrumentRuleSet,
)
from market_impact_agent.domain import (
    ApprovalMode,
    Side,
    SignalIntent,
    TradingEnvironment,
    TradingMandateV2,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.portfolio_decision import (
    AgentPortfolioProposalV2,
    BearishExpressionBinding,
    BearishExpressionMode,
    OrderSizingOutcome,
    PortfolioAction,
    PortfolioDecisionOutcome,
    PortfolioExposureViewV2,
    PortfolioLegRole,
    RawMarkedPositionV2,
    RegisteredBearishExpressionAuthorityV2,
    RegisteredPortfolioExposureViewAuthorityV2,
    TargetExposureDirection,
    agent_portfolio_proposal_v2_from_dict,
    evaluate_portfolio_decision_v2,
    size_portfolio_decision_v2,
)
from market_impact_agent.providers import (
    Capability,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)

AT = datetime(2026, 9, 2, 14, tzinfo=UTC)
TARGET = "SPY.ARCA"
SOURCE = "QQQ.ARCA"
HASH = "a" * 64


def _provider() -> ProviderManifest:
    return ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="fixture-us-account",
        provider_version="1",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("ARCX",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )


def _snapshot(*positions: AccountPosition) -> PositionSnapshot:
    account = capture_account_state_snapshot(
        provider=_provider(),
        account_reference="fixture-v2-account",
        account_reference_key=b"fixture-v2-account-key-material-32b",
        environment=TradingEnvironment.PAPER,
        as_of=AT,
        reconciled_at=AT,
        reconciliation_reference="fixture-v2-reconciliation",
        cash=(
            CashBalance(
                currency="USD",
                available=Decimal("20000"),
                settled=Decimal("20000"),
            ),
        ),
        positions=positions,
        open_orders=(),
        recent_fills=(),
        recent_fills_since=AT - timedelta(days=1),
    )
    return account.project_positions(evaluated_at=AT, max_age=timedelta(minutes=5))


def _view(snapshot: PositionSnapshot) -> AuthorizedDecisionView:
    return AuthorizedDecisionView.build(
        cutoff=AT,
        frozen_at=AT + timedelta(seconds=1),
        data_snapshot_ids=("data-v2",),
        decision_input_ids=("input-v2",),
        position_snapshot=snapshot,
    )


def _signal(*, instrument: str = TARGET, side: Side = Side.BUY) -> SignalIntent:
    return SignalIntent(
        signal_id=f"signal-{instrument}",
        event_id="event-v2",
        instrument_id=instrument,
        side=side,
        valid_from=AT,
        expires_at=AT + timedelta(minutes=15),
        evidence_refs=("counter-1", "evidence-1"),
        invalidation_conditions=("thesis_invalidated",),
    )


def _proposal(
    signal: SignalIntent,
    *,
    action: PortfolioAction = PortfolioAction.OPEN,
    direction: TargetExposureDirection = TargetExposureDirection.LONG,
    ratio: Decimal = Decimal("0.50"),
) -> AgentPortfolioProposalV2:
    return AgentPortfolioProposalV2.build(
        signal=signal,
        requested_action=action,
        venue="ARCX",
        instrument_class="exchange_traded_fund",
        direction=direction,
        horizon_sessions=5,
        target_gross_exposure_ratio=ratio,
        rationale="The cited event changes the bounded exposure thesis.",
        evidence_refs=("evidence-1",),
        counterevidence_refs=("counter-1",),
        invalidation_conditions=signal.invalidation_conditions,
    )


def _mandate(*, instruments: frozenset[str] | None = None) -> TradingMandateV2:
    return TradingMandateV2(
        mandate_id="paper-v2",
        account_id=_snapshot().account_reference_hash,
        harness_authority_id="harness-authority-" + "a" * 32,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=AT,
        valid_until=AT + timedelta(hours=8),
        allowed_instruments=instruments or frozenset({TARGET, SOURCE}),
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


def _price(
    instrument: str,
    *,
    kind: str = "raw_reference_quote",
    source_version: str = "1",
) -> PriceBasis:
    return PriceBasis(
        instrument_id=instrument,
        currency="USD",
        unit="per_share",
        basis_kind=kind,
        price=Decimal("10"),
        source_id="fixture-raw-quote",
        source_version=source_version,
        observed_at=AT,
        valid_until=AT + timedelta(minutes=5),
    )


def _rules() -> ExchangeInstrumentRuleSet:
    return ExchangeInstrumentRuleSet(
        rule_set_id="exchange-instrument-rule-set-" + HASH,
        effective_from=date(2026, 1, 1),
        source_documents=({"source": "fixture"},),
        rules=(
            ExchangeInstrumentRule(
                rule_key="arcx-unlevered-etf-v1",
                venue="ARCX",
                instrument_class="exchange_traded_fund",
                buy_lot_size=1,
                price_tick=0.01,
                currency="USD",
                scope="ordinary_auction_buy_and_sell_order",
                exceptions=(),
            ),
        ),
    )


def _exposure(
    snapshot: PositionSnapshot,
    view: AuthorizedDecisionView,
    *marks: RawMarkedPositionV2,
    active_kill_reasons: tuple[str, ...] = (),
    daily_turnover_used: Decimal = Decimal("0"),
) -> PortfolioExposureViewV2:
    return PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=snapshot,
        raw_mark_set_hash="b" * 64,
        execution_ledger_snapshot_hash="c" * 64,
        reconciliation_ledger_snapshot_hash="d" * 64,
        currency="USD",
        marked_positions=marks,
        daily_turnover_used=daily_turnover_used,
        daily_submissions_used=0,
        active_kill_reasons=active_kill_reasons,
        observed_at=AT + timedelta(seconds=1),
        valid_until=AT + timedelta(minutes=5),
    )


def _exposure_authority(
    exposure_view: PortfolioExposureViewV2,
) -> RegisteredPortfolioExposureViewAuthorityV2:
    return RegisteredPortfolioExposureViewAuthorityV2(
        {exposure_view.exposure_view_id: exposure_view}
    )


def _bearish_authority(
    binding: BearishExpressionBinding,
) -> RegisteredBearishExpressionAuthorityV2:
    return RegisteredBearishExpressionAuthorityV2({binding.binding_id: binding})


def _mark(position: AccountPosition) -> RawMarkedPositionV2:
    return RawMarkedPositionV2(
        instrument_id=position.target_id,
        venue=position.venue,
        instrument_class=position.instrument_class,
        side=position.side,
        quantity=position.quantity,
        raw_price=Decimal("10"),
        raw_price_basis_hash=HASH,
    )


def _position(instrument: str, side: Side, quantity: str) -> AccountPosition:
    return AccountPosition(
        target_id=instrument,
        venue="ARCX",
        instrument_class="exchange_traded_fund",
        side=side,
        quantity=Decimal(quantity),
        concentration=Decimal("0.10"),
        concentration_gap=None,
    )


def _decision(
    snapshot: PositionSnapshot,
    proposal: AgentPortfolioProposalV2,
    signal: SignalIntent,
    *,
    binding: BearishExpressionBinding | None = None,
):
    return evaluate_portfolio_decision_v2(
        signal=signal,
        proposal=proposal,
        authorized_view=_view(snapshot),
        position_snapshot=snapshot,
        bearish_expression_binding=binding,
        bearish_expression_authority=(None if binding is None else _bearish_authority(binding)),
        decided_at=AT + timedelta(seconds=2),
    )


def test_agent_cannot_write_quantity_or_bypass_mandate() -> None:
    snapshot = _snapshot()
    signal = _signal()
    proposal = _proposal(signal)
    forged = {**proposal.to_dict(), "quantity": "500"}
    with pytest.raises(ValueError, match=r"unknown=\['quantity'\]"):
        agent_portfolio_proposal_v2_from_dict(forged)

    view = _view(snapshot)
    decision = _decision(snapshot, proposal, signal)
    sizing = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(instruments=frozenset({SOURCE})),
        exposure_view=_exposure(snapshot, view),
        exposure_view_authority=_exposure_authority(_exposure(snapshot, view)),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )

    assert sizing.outcome is OrderSizingOutcome.REJECTED
    assert sizing.legs[0].blockers == ("instrument_not_allowed_by_mandate",)
    assert "quantity" not in proposal.to_dict()
    assert (
        validate_agent_contract(proposal.to_dict(), "agent-portfolio-proposal-v2.schema.json") == ()
    )
    assert validate_agent_contract(decision.to_dict(), "portfolio-decision-v2.schema.json") == ()
    assert validate_agent_contract(sizing.to_dict(), "order-sizing-decision-v2.schema.json") == ()
    assert (
        validate_agent_contract(
            _exposure(snapshot, view).to_dict(),
            "portfolio-exposure-view-v2.schema.json",
        )
        == ()
    )
    assert validate_agent_contract(_mandate().to_dict(), "trading-mandate.schema.json") == ()


def test_raw_price_is_required_and_target_delta_is_idempotent() -> None:
    holding = _position(TARGET, Side.BUY, "200")
    snapshot = _snapshot(holding)
    view = _view(snapshot)
    signal = _signal()
    proposal = _proposal(signal, action=PortfolioAction.INCREASE)
    decision = _decision(snapshot, proposal, signal)
    exposure = _exposure(snapshot, view, _mark(holding))
    first = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )
    replay = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )
    adjusted = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET, kind="adjusted_close")},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )

    assert first.decision_id == replay.decision_id
    assert first.legs[0].target_notional == Decimal("5000.00")
    assert first.legs[0].delta_notional == Decimal("3000.00")
    assert first.legs[0].quantity == Decimal("300")
    assert adjusted.legs[0].blockers == ("price_basis_not_raw_tradable",)


@pytest.mark.parametrize(
    ("current_side", "direction", "ratio", "expected_blocker"),
    (
        (
            Side.BUY,
            TargetExposureDirection.LONG,
            Decimal("0.10"),
            "increase_target_does_not_strictly_increase_exposure",
        ),
        (
            Side.BUY,
            TargetExposureDirection.SHORT,
            Decimal("0.50"),
            "increase_target_changes_exposure_direction",
        ),
        (
            Side.SELL,
            TargetExposureDirection.SHORT,
            Decimal("0.10"),
            "increase_target_does_not_strictly_increase_exposure",
        ),
        (
            Side.SELL,
            TargetExposureDirection.LONG,
            Decimal("0.50"),
            "increase_target_changes_exposure_direction",
        ),
    ),
)
def test_increase_rejects_reductions_and_direction_reversals(
    current_side: Side,
    direction: TargetExposureDirection,
    ratio: Decimal,
    expected_blocker: str,
) -> None:
    holding = _position(TARGET, current_side, "200")
    snapshot = _snapshot(holding)
    view = _view(snapshot)
    signal = _signal(side=Side.BUY if direction is TargetExposureDirection.LONG else Side.SELL)
    proposal = _proposal(
        signal,
        action=PortfolioAction.INCREASE,
        direction=direction,
        ratio=ratio,
    )
    binding = (
        None
        if direction is TargetExposureDirection.LONG
        else BearishExpressionBinding.build(
            proposal_id=proposal.proposal_id,
            account_reference_hash=snapshot.account_reference_hash,
            instrument_id=TARGET,
            mode=BearishExpressionMode.BORROWED_ORDINARY_ETF,
            account_permission_confirmed=True,
            shortable_quantity=Decimal("1000"),
            allowlisted_inverse_etf=False,
            leverage_magnitude=None,
            evidence_refs=("borrow-proof",),
            observed_at=AT,
            valid_until=AT + timedelta(minutes=5),
        )
    )
    decision = _decision(snapshot, proposal, signal, binding=binding)
    exposure = _exposure(snapshot, view, _mark(holding))
    sizing = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        bearish_expression_authority=(None if binding is None else _bearish_authority(binding)),
        decided_at=AT + timedelta(seconds=3),
    )

    assert decision.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING
    assert sizing.outcome is OrderSizingOutcome.REJECTED
    assert sizing.legs[0].blockers == (expected_blocker,)
    assert sizing.legs[0].quantity is None


@pytest.mark.parametrize(
    ("current_side", "direction", "expected_order_side"),
    (
        (Side.BUY, TargetExposureDirection.LONG, Side.BUY),
        (Side.SELL, TargetExposureDirection.SHORT, Side.SELL),
    ),
)
def test_increase_preserves_direction_and_strictly_increases_exposure(
    current_side: Side,
    direction: TargetExposureDirection,
    expected_order_side: Side,
) -> None:
    holding = _position(TARGET, current_side, "200")
    snapshot = _snapshot(holding)
    view = _view(snapshot)
    signal = _signal(side=expected_order_side)
    proposal = _proposal(
        signal,
        action=PortfolioAction.INCREASE,
        direction=direction,
    )
    binding = (
        None
        if direction is TargetExposureDirection.LONG
        else BearishExpressionBinding.build(
            proposal_id=proposal.proposal_id,
            account_reference_hash=snapshot.account_reference_hash,
            instrument_id=TARGET,
            mode=BearishExpressionMode.BORROWED_ORDINARY_ETF,
            account_permission_confirmed=True,
            shortable_quantity=Decimal("1000"),
            allowlisted_inverse_etf=False,
            leverage_magnitude=None,
            evidence_refs=("borrow-proof",),
            observed_at=AT,
            valid_until=AT + timedelta(minutes=5),
        )
    )
    decision = _decision(snapshot, proposal, signal, binding=binding)
    exposure = _exposure(snapshot, view, _mark(holding))
    sizing = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        bearish_expression_authority=(None if binding is None else _bearish_authority(binding)),
        decided_at=AT + timedelta(seconds=3),
    )

    assert sizing.outcome is OrderSizingOutcome.READY
    assert sizing.legs[0].side is expected_order_side
    assert sizing.legs[0].quantity == Decimal("300")


def test_reduce_close_and_rotation_are_target_based_and_linked() -> None:
    holding = _position(TARGET, Side.BUY, "600")
    snapshot = _snapshot(holding)
    view = _view(snapshot)
    signal = _signal(side=Side.SELL)
    reduce_proposal = _proposal(
        signal,
        action=PortfolioAction.REDUCE,
        ratio=Decimal("0.40"),
    )
    reduce_decision = _decision(snapshot, reduce_proposal, signal)
    reduce_sizing = size_portfolio_decision_v2(
        portfolio_decision=reduce_decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=_exposure(snapshot, view, _mark(holding)),
        exposure_view_authority=_exposure_authority(_exposure(snapshot, view, _mark(holding))),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )
    close_proposal = _proposal(
        signal,
        action=PortfolioAction.CLOSE,
        ratio=Decimal("0"),
    )
    close_sizing = size_portfolio_decision_v2(
        portfolio_decision=_decision(snapshot, close_proposal, signal),
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=_exposure(snapshot, view, _mark(holding)),
        exposure_view_authority=_exposure_authority(_exposure(snapshot, view, _mark(holding))),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )

    assert reduce_sizing.legs[0].side is Side.SELL
    assert reduce_sizing.legs[0].quantity == Decimal("200")
    assert close_sizing.legs[0].quantity == Decimal("600")

    source = _position(SOURCE, Side.BUY, "300")
    rotation_snapshot = _snapshot(source)
    rotation_view = _view(rotation_snapshot)
    rotation_signal = _signal()
    rotation = _decision(
        rotation_snapshot,
        _proposal(rotation_signal, action=PortfolioAction.ROTATE),
        rotation_signal,
    )
    rotation_sizing = size_portfolio_decision_v2(
        portfolio_decision=rotation,
        authorized_view=rotation_view,
        position_snapshot=rotation_snapshot,
        mandate=_mandate(),
        exposure_view=_exposure(rotation_snapshot, rotation_view, _mark(source)),
        exposure_view_authority=_exposure_authority(
            _exposure(rotation_snapshot, rotation_view, _mark(source))
        ),
        price_bases={SOURCE: _price(SOURCE), TARGET: _price(TARGET)},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )

    assert rotation.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING
    assert tuple(item.role for item in rotation.legs) == (
        PortfolioLegRole.ROTATION_SOURCE,
        PortfolioLegRole.ROTATION_DESTINATION,
    )
    assert rotation_sizing.legs[0].outcome is OrderSizingOutcome.READY
    assert rotation_sizing.legs[1].blockers == ("blocked_pending_source_reconciliation",)


def test_short_requires_current_prebound_proof_and_unknown_fails_closed() -> None:
    snapshot = _snapshot()
    view = _view(snapshot)
    signal = _signal(side=Side.SELL)
    proposal = _proposal(
        signal,
        direction=TargetExposureDirection.SHORT,
        ratio=Decimal("0.25"),
    )
    missing = _decision(snapshot, proposal, signal)
    assert missing.outcome is PortfolioDecisionOutcome.REJECTED
    assert missing.blockers == ("bearish_expression_binding_missing",)

    proof = BearishExpressionBinding.build(
        proposal_id=proposal.proposal_id,
        account_reference_hash=snapshot.account_reference_hash,
        instrument_id=TARGET,
        mode=BearishExpressionMode.BORROWED_ORDINARY_ETF,
        account_permission_confirmed=True,
        shortable_quantity=Decimal("100"),
        allowlisted_inverse_etf=False,
        leverage_magnitude=None,
        evidence_refs=("borrow-proof",),
        observed_at=AT,
        valid_until=AT + timedelta(minutes=5),
    )
    decision = _decision(snapshot, proposal, signal, binding=proof)
    sizing = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=_exposure(snapshot, view),
        exposure_view_authority=_exposure_authority(_exposure(snapshot, view)),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        bearish_expression_authority=_bearish_authority(proof),
        decided_at=AT + timedelta(seconds=3),
    )
    killed_view = _exposure(
        snapshot,
        view,
        active_kill_reasons=("unknown_ack",),
    )
    killed = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=killed_view,
        exposure_view_authority=_exposure_authority(killed_view),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        bearish_expression_authority=_bearish_authority(proof),
        decided_at=AT + timedelta(seconds=3),
    )

    assert sizing.legs[0].blockers == ("borrow_proof_quantity_insufficient",)
    assert killed.outcome is OrderSizingOutcome.REJECTED
    assert "unknown_ack" in killed.legs[0].blockers


def test_allowlisted_nonlevered_inverse_etf_expresses_bearish_view_as_long_buy() -> None:
    snapshot = _snapshot()
    view = _view(snapshot)
    signal = _signal(side=Side.SELL)
    proposal = _proposal(
        signal,
        direction=TargetExposureDirection.SHORT,
        ratio=Decimal("0.25"),
    )
    binding = BearishExpressionBinding.build(
        proposal_id=proposal.proposal_id,
        account_reference_hash=snapshot.account_reference_hash,
        instrument_id=TARGET,
        mode=BearishExpressionMode.NONLEVERED_INVERSE_ETF,
        account_permission_confirmed=False,
        shortable_quantity=None,
        allowlisted_inverse_etf=True,
        leverage_magnitude=Decimal("1"),
        evidence_refs=("inverse-etf-allowlist",),
        observed_at=AT,
        valid_until=AT + timedelta(minutes=5),
    )
    decision = _decision(snapshot, proposal, signal, binding=binding)
    sizing = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=_exposure(snapshot, view),
        exposure_view_authority=_exposure_authority(_exposure(snapshot, view)),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        bearish_expression_authority=_bearish_authority(binding),
        decided_at=AT + timedelta(seconds=3),
    )

    assert sizing.outcome is OrderSizingOutcome.READY
    assert sizing.legs[0].side is Side.BUY
    assert sizing.legs[0].quantity == Decimal("250")


def test_hold_is_no_action_and_existing_position_blocks_open() -> None:
    holding = _position(TARGET, Side.BUY, "100")
    snapshot = _snapshot(holding)
    signal = _signal()
    hold = _decision(
        snapshot,
        _proposal(signal, action=PortfolioAction.HOLD, ratio=Decimal("0.10")),
        signal,
    )
    duplicate_open = _decision(snapshot, _proposal(signal), signal)

    assert hold.outcome is PortfolioDecisionOutcome.NO_ACTION
    assert hold.legs == ()
    assert duplicate_open.outcome is PortfolioDecisionOutcome.REJECTED
    assert duplicate_open.blockers == ("position_already_open",)


def test_forged_destination_and_rotation_source_cannot_rewrite_bound_legs() -> None:
    source = _position(SOURCE, Side.BUY, "300")
    snapshot = _snapshot(source)
    view = _view(snapshot)
    signal = _signal()
    rotation = _decision(
        snapshot,
        _proposal(signal, action=PortfolioAction.ROTATE),
        signal,
    )
    forged_destination = replace(rotation.legs[1], instrument_id="ATTACK.ARCA")
    forged_destination_core = {
        **rotation.core_dict(),
        "legs": [rotation.legs[0].to_dict(), forged_destination.to_dict()],
    }
    with pytest.raises(ValueError, match="differs from Agent proposal"):
        replace(
            rotation,
            legs=(rotation.legs[0], forged_destination),
            decision_id="portfolio-decision-v2-" + canonical_hash(forged_destination_core),
        )

    fake_source = _position("ATTACK.ARCA", Side.BUY, "300")
    forged_source = replace(
        rotation.legs[0],
        instrument_id=fake_source.target_id,
        position_snapshot_position_hash=canonical_hash(fake_source.to_dict()),
    )
    forged_source_core = {
        **rotation.core_dict(),
        "legs": [forged_source.to_dict(), rotation.legs[1].to_dict()],
    }
    forged_rotation = replace(
        rotation,
        legs=(forged_source, rotation.legs[1]),
        decision_id="portfolio-decision-v2-" + canonical_hash(forged_source_core),
    )
    exposure = _exposure(snapshot, view, _mark(source))
    with pytest.raises(ValueError, match="not bound to its Position Snapshot"):
        size_portfolio_decision_v2(
            portfolio_decision=forged_rotation,
            authorized_view=view,
            position_snapshot=snapshot,
            mandate=_mandate(instruments=frozenset({TARGET, SOURCE, "ATTACK.ARCA"})),
            exposure_view=exposure,
            exposure_view_authority=_exposure_authority(exposure),
            price_bases={
                TARGET: _price(TARGET),
                "ATTACK.ARCA": _price("ATTACK.ARCA"),
            },
            rule_set=_rules(),
            decided_at=AT + timedelta(seconds=3),
        )


def test_caller_created_exposure_view_cannot_replace_harness_ledger_view() -> None:
    snapshot = _snapshot()
    view = _view(snapshot)
    signal = _signal()
    decision = _decision(snapshot, _proposal(signal), signal)
    authoritative = _exposure(
        snapshot,
        view,
        daily_turnover_used=Decimal("49000"),
    )
    caller_created = _exposure(snapshot, view, daily_turnover_used=Decimal("0"))

    with pytest.raises(PermissionError, match="lacks Harness ledger authority"):
        size_portfolio_decision_v2(
            portfolio_decision=decision,
            authorized_view=view,
            position_snapshot=snapshot,
            mandate=_mandate(),
            exposure_view=caller_created,
            exposure_view_authority=_exposure_authority(authoritative),
            price_bases={TARGET: _price(TARGET)},
            rule_set=_rules(),
            decided_at=AT + timedelta(seconds=3),
        )


def test_price_basis_identity_changes_sizing_identity_even_at_same_price() -> None:
    snapshot = _snapshot()
    view = _view(snapshot)
    signal = _signal()
    decision = _decision(snapshot, _proposal(signal), signal)
    exposure = _exposure(snapshot, view)
    first = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET, source_version="1")},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )
    second = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET, source_version="2")},
        rule_set=_rules(),
        decided_at=AT + timedelta(seconds=3),
    )

    assert first.legs[0].quantity == second.legs[0].quantity
    assert first.legs[0].price_basis_hash != second.legs[0].price_basis_hash
    assert first.price_basis_hashes != second.price_basis_hashes
    assert first.decision_id != second.decision_id


def test_bearish_evidence_requires_harness_authority_and_is_rechecked_at_sizing() -> None:
    snapshot = _snapshot()
    view = _view(snapshot)
    signal = _signal(side=Side.SELL)
    proposal = _proposal(
        signal,
        direction=TargetExposureDirection.SHORT,
        ratio=Decimal("0.25"),
    )
    binding = BearishExpressionBinding.build(
        proposal_id=proposal.proposal_id,
        account_reference_hash=snapshot.account_reference_hash,
        instrument_id=TARGET,
        mode=BearishExpressionMode.BORROWED_ORDINARY_ETF,
        account_permission_confirmed=True,
        shortable_quantity=Decimal("1000"),
        allowlisted_inverse_etf=False,
        leverage_magnitude=None,
        evidence_refs=("borrow-proof",),
        observed_at=AT,
        valid_until=AT + timedelta(seconds=2, microseconds=500_000),
    )
    with pytest.raises(PermissionError, match="Harness evidence authority"):
        evaluate_portfolio_decision_v2(
            signal=signal,
            proposal=proposal,
            authorized_view=view,
            position_snapshot=snapshot,
            bearish_expression_binding=binding,
            decided_at=AT + timedelta(seconds=2),
        )
    decision = evaluate_portfolio_decision_v2(
        signal=signal,
        proposal=proposal,
        authorized_view=view,
        position_snapshot=snapshot,
        bearish_expression_binding=binding,
        bearish_expression_authority=_bearish_authority(binding),
        decided_at=AT + timedelta(seconds=2),
    )
    exposure = _exposure(snapshot, view)
    with pytest.raises(PermissionError, match="Harness evidence authority"):
        size_portfolio_decision_v2(
            portfolio_decision=decision,
            authorized_view=view,
            position_snapshot=snapshot,
            mandate=_mandate(),
            exposure_view=exposure,
            exposure_view_authority=_exposure_authority(exposure),
            price_bases={TARGET: _price(TARGET)},
            rule_set=_rules(),
            decided_at=AT + timedelta(seconds=3),
        )
    stale = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=snapshot,
        mandate=_mandate(),
        exposure_view=exposure,
        exposure_view_authority=_exposure_authority(exposure),
        price_bases={TARGET: _price(TARGET)},
        rule_set=_rules(),
        bearish_expression_authority=_bearish_authority(binding),
        decided_at=AT + timedelta(seconds=3),
    )

    assert stale.outcome is OrderSizingOutcome.REJECTED
    assert stale.legs[0].blockers == ("bearish_expression_binding_not_current",)
