from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, cast
from zoneinfo import ZoneInfo

from market_impact_agent.account_state import PositionSnapshot
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRule,
    ExchangeInstrumentRuleSet,
)
from market_impact_agent.domain import (
    OrderIntent,
    OrderKind,
    Side,
    SignalIntent,
    TradingMandate,
    TradingMandateV2,
    require_aware,
)

if TYPE_CHECKING:
    from market_impact_agent.portfolio_review import PortfolioDecisionV3

PORTFOLIO_DECISION_SCHEMA = "market-impact.portfolio-decision.v1"
ORDER_SIZING_DECISION_SCHEMA = "market-impact.order-sizing-decision.v1"
AGENT_PORTFOLIO_PROPOSAL_V2_SCHEMA = "market-impact.agent-portfolio-proposal.v2"
PORTFOLIO_DECISION_V2_SCHEMA = "market-impact.portfolio-decision.v2"
ORDER_SIZING_DECISION_V2_SCHEMA = "market-impact.order-sizing-decision.v2"
PORTFOLIO_KILL_REASONS_V2 = frozenset(
    {
        "daily_loss_threshold_exceeded",
        "incomplete_order_coverage",
        "provider_loss",
        "reconciliation_difference",
        "stale_account_snapshot",
        "strategy_peak_drawdown_threshold_exceeded",
        "unknown_ack",
    }
)


class PortfolioAction(StrEnum):
    ABSTAIN = "abstain"
    OBSERVE = "observe"
    HOLD = "hold"
    OPEN = "open"
    INCREASE = "increase"
    REDUCE = "reduce"
    CLOSE = "close"
    ROTATE = "rotate"


class PortfolioDecisionOutcome(StrEnum):
    NO_ACTION = "no_action"
    REJECTED = "rejected"
    READY_FOR_SIZING = "ready_for_sizing"


class OrderSizingOutcome(StrEnum):
    REJECTED = "rejected"
    READY = "ready"


class PriceBasisLike(Protocol):
    @property
    def instrument_id(self) -> str: ...

    @property
    def currency(self) -> str: ...

    @property
    def unit(self) -> str: ...

    @property
    def basis_kind(self) -> str: ...

    @property
    def price(self) -> Decimal: ...

    @property
    def observed_at(self) -> datetime: ...

    @property
    def valid_until(self) -> datetime: ...

    def to_dict(self) -> dict[str, object]: ...


class PortfolioExposureViewAuthorityV2(Protocol):
    """Harness composition-root authority for one exact exposure view."""

    def assert_authoritative_exposure_view(self, view: PortfolioExposureViewV2) -> None: ...


class BearishExpressionAuthorityV2(Protocol):
    """Harness composition-root authority for borrow and inverse-ETF evidence."""

    def assert_authoritative_bearish_expression(
        self,
        binding: BearishExpressionBinding,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class PortfolioDecision:
    decision_id: str
    signal_id: str
    signal_hash: str
    authorized_decision_view_id: str
    authorized_decision_view_hash: str
    position_snapshot_id: str
    position_snapshot_hash: str
    requested_action: PortfolioAction
    instrument_id: str
    venue: str
    instrument_class: str
    evidence_refs: tuple[str, ...]
    outcome: PortfolioDecisionOutcome
    order_side: Side | None
    current_position_quantity: Decimal | None
    blockers: tuple[str, ...]
    decided_at: datetime
    execution_capability: bool = False
    schema_version: str = PORTFOLIO_DECISION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PORTFOLIO_DECISION_SCHEMA:
            raise ValueError("unsupported Portfolio Decision schema")
        _trimmed(self.signal_id, "Portfolio Decision signal_id")
        _sha256(self.signal_hash, "Portfolio Decision Signal hash")
        _prefixed_hash(
            self.authorized_decision_view_id,
            "authorized-decision-view-",
            "Portfolio Decision view ID",
        )
        _sha256(self.authorized_decision_view_hash, "Portfolio Decision view hash")
        _prefixed_hash(
            self.position_snapshot_id,
            "position-snapshot-",
            "Portfolio Decision Position Snapshot ID",
        )
        _sha256(self.position_snapshot_hash, "Portfolio Decision Position Snapshot hash")
        _instrument_identity(self.instrument_id, self.venue, self.instrument_class)
        _sorted_unique(self.evidence_refs, "Portfolio Decision evidence_refs")
        _sorted_unique(self.blockers, "Portfolio Decision blockers")
        _strict_utc(self.decided_at, "Portfolio Decision decided_at")
        if self.current_position_quantity is not None:
            _positive(self.current_position_quantity, "Portfolio Decision current position")
        if self.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING:
            if (
                self.requested_action
                not in {
                    PortfolioAction.OPEN,
                    PortfolioAction.INCREASE,
                    PortfolioAction.REDUCE,
                    PortfolioAction.CLOSE,
                }
                or self.order_side is None
                or self.blockers
            ):
                raise ValueError("ready Portfolio Decision lacks one actionable order side")
        elif self.outcome is PortfolioDecisionOutcome.NO_ACTION:
            if (
                self.requested_action
                not in {
                    PortfolioAction.ABSTAIN,
                    PortfolioAction.OBSERVE,
                    PortfolioAction.HOLD,
                }
                or self.order_side is not None
                or self.blockers
            ):
                raise ValueError("no-action Portfolio Decision is inconsistent")
        elif not self.blockers or self.order_side is not None:
            raise ValueError("rejected Portfolio Decision requires blockers and no order side")
        if self.execution_capability:
            raise ValueError("Portfolio Decision cannot grant execution capability")
        if self.decision_id != self.expected_decision_id:
            raise ValueError("Portfolio Decision ID does not match content")

    @property
    def expected_decision_id(self) -> str:
        return f"portfolio-decision-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "signal_id": self.signal_id,
            "signal_hash": self.signal_hash,
            "authorized_decision_view_id": self.authorized_decision_view_id,
            "authorized_decision_view_hash": self.authorized_decision_view_hash,
            "position_snapshot_id": self.position_snapshot_id,
            "position_snapshot_hash": self.position_snapshot_hash,
            "requested_action": self.requested_action.value,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "evidence_refs": list(self.evidence_refs),
            "outcome": self.outcome.value,
            "order_side": None if self.order_side is None else self.order_side.value,
            "current_position_quantity": (
                None
                if self.current_position_quantity is None
                else _decimal_text(self.current_position_quantity)
            ),
            "blockers": list(self.blockers),
            "decided_at": _timestamp(self.decided_at),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}


@dataclass(frozen=True, slots=True)
class OrderSizingPolicy:
    max_available_cash_fraction: Decimal
    reduction_fraction: Decimal

    def __post_init__(self) -> None:
        _fraction(
            self.max_available_cash_fraction,
            "Order Sizing Policy max_available_cash_fraction",
            allow_one=True,
        )
        _fraction(
            self.reduction_fraction,
            "Order Sizing Policy reduction_fraction",
            allow_one=False,
        )

    @property
    def policy_id(self) -> str:
        return f"order-sizing-policy-{canonical_hash(self.to_dict())}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.order-sizing-policy.v1",
            "max_available_cash_fraction": _decimal_text(self.max_available_cash_fraction),
            "reduction_fraction": _decimal_text(self.reduction_fraction),
            "confidence_affects_size": False,
        }


@dataclass(frozen=True, slots=True)
class OrderSizingDecision:
    decision_id: str
    portfolio_decision_id: str
    portfolio_decision_hash: str
    position_snapshot_id: str
    position_snapshot_hash: str
    trading_mandate_hash: str
    price_basis_hash: str
    instrument_rule_set_id: str
    instrument_rule_set_hash: str
    instrument_rule_key: str
    sizing_policy_id: str
    max_available_cash_fraction: Decimal
    reduction_fraction: Decimal
    instrument_id: str
    side: Side
    order_kind: OrderKind
    limit_price: Decimal | None
    reference_price: Decimal
    quantity: Decimal | None
    order_notional: Decimal | None
    outcome: OrderSizingOutcome
    blockers: tuple[str, ...]
    decided_at: datetime
    execution_capability: bool = False
    schema_version: str = ORDER_SIZING_DECISION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != ORDER_SIZING_DECISION_SCHEMA:
            raise ValueError("unsupported Order Sizing Decision schema")
        _prefixed_hash(
            self.portfolio_decision_id,
            "portfolio-decision-",
            "Order Sizing Portfolio Decision ID",
        )
        _sha256(self.portfolio_decision_hash, "Order Sizing Portfolio Decision hash")
        _prefixed_hash(
            self.position_snapshot_id,
            "position-snapshot-",
            "Order Sizing Position Snapshot ID",
        )
        _sha256(self.position_snapshot_hash, "Order Sizing Position Snapshot hash")
        _sha256(self.trading_mandate_hash, "Order Sizing Trading Mandate hash")
        _sha256(self.price_basis_hash, "Order Sizing Price Basis hash")
        _prefixed_hash(
            self.instrument_rule_set_id,
            "exchange-instrument-rule-set-",
            "Order Sizing instrument rule-set ID",
        )
        _sha256(self.instrument_rule_set_hash, "Order Sizing instrument rule-set hash")
        _trimmed(self.instrument_rule_key, "Order Sizing instrument rule key")
        _prefixed_hash(
            self.sizing_policy_id,
            "order-sizing-policy-",
            "Order Sizing Policy ID",
        )
        sizing_policy = OrderSizingPolicy(
            max_available_cash_fraction=self.max_available_cash_fraction,
            reduction_fraction=self.reduction_fraction,
        )
        if self.sizing_policy_id != sizing_policy.policy_id:
            raise ValueError("Order Sizing Policy ID does not match content")
        _trimmed(self.instrument_id, "Order Sizing instrument_id")
        _positive(self.reference_price, "Order Sizing reference price")
        _sorted_unique(self.blockers, "Order Sizing blockers")
        _strict_utc(self.decided_at, "Order Sizing decided_at")
        if self.order_kind is OrderKind.LIMIT:
            if self.limit_price is None:
                raise ValueError("limit sizing requires a limit price")
            _positive(self.limit_price, "Order Sizing limit price")
        elif self.limit_price is not None:
            raise ValueError("market sizing cannot set a limit price")
        if self.outcome is OrderSizingOutcome.READY:
            if self.quantity is None or self.order_notional is None or self.blockers:
                raise ValueError("ready Order Sizing Decision lacks quantity and notional")
            _positive(self.quantity, "Order Sizing quantity")
            _positive(self.order_notional, "Order Sizing notional")
        elif self.quantity is not None or self.order_notional is not None or not self.blockers:
            raise ValueError("rejected Order Sizing Decision must omit size and record blockers")
        if self.execution_capability:
            raise ValueError("Order Sizing Decision cannot grant execution capability")
        if self.decision_id != self.expected_decision_id:
            raise ValueError("Order Sizing Decision ID does not match content")

    @property
    def expected_decision_id(self) -> str:
        return f"order-sizing-decision-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "portfolio_decision_id": self.portfolio_decision_id,
            "portfolio_decision_hash": self.portfolio_decision_hash,
            "position_snapshot_id": self.position_snapshot_id,
            "position_snapshot_hash": self.position_snapshot_hash,
            "trading_mandate_hash": self.trading_mandate_hash,
            "price_basis_hash": self.price_basis_hash,
            "instrument_rule_set_id": self.instrument_rule_set_id,
            "instrument_rule_set_hash": self.instrument_rule_set_hash,
            "instrument_rule_key": self.instrument_rule_key,
            "sizing_policy_id": self.sizing_policy_id,
            "sizing_policy": {
                **OrderSizingPolicy(
                    max_available_cash_fraction=self.max_available_cash_fraction,
                    reduction_fraction=self.reduction_fraction,
                ).to_dict(),
            },
            "instrument_id": self.instrument_id,
            "side": self.side.value,
            "order_kind": self.order_kind.value,
            "limit_price": None if self.limit_price is None else _decimal_text(self.limit_price),
            "reference_price": _decimal_text(self.reference_price),
            "quantity": None if self.quantity is None else _decimal_text(self.quantity),
            "order_notional": (
                None if self.order_notional is None else _decimal_text(self.order_notional)
            ),
            "outcome": self.outcome.value,
            "blockers": list(self.blockers),
            "decided_at": _timestamp(self.decided_at),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}


def evaluate_portfolio_decision(
    *,
    signal: SignalIntent,
    authorized_view: AuthorizedDecisionView,
    position_snapshot: PositionSnapshot,
    requested_action: PortfolioAction,
    venue: str,
    instrument_class: str,
    evidence_refs: tuple[str, ...],
    decided_at: datetime,
) -> PortfolioDecision:
    _strict_utc(decided_at, "Portfolio Decision decided_at")
    if authorized_view.position_snapshot_id != position_snapshot.snapshot_id:
        raise ValueError("Authorized Decision View binds a different Position Snapshot")
    if decided_at < authorized_view.frozen_at:
        raise ValueError("Portfolio Decision cannot predate its Authorized Decision View")
    if not signal.valid_from <= decided_at < signal.expires_at:
        raise PermissionError("Portfolio Decision requires a currently valid Signal")
    ordered_evidence = tuple(sorted(set(evidence_refs)))
    if not ordered_evidence or not set(ordered_evidence) <= set(signal.evidence_refs):
        raise ValueError("Portfolio Decision evidence must come from the Signal")
    blockers: set[str] = set()
    positions = position_snapshot.positions
    matching_positions = (
        ()
        if positions is None
        else tuple(item for item in positions if item.target_id == signal.instrument_id)
    )
    if len(matching_positions) > 1:
        blockers.add("ambiguous_position_identity")
    position = matching_positions[0] if len(matching_positions) == 1 else None
    if position is not None and (
        position.venue != venue or position.instrument_class != instrument_class
    ):
        blockers.add("position_instrument_identity_mismatch")

    no_action = requested_action in {
        PortfolioAction.ABSTAIN,
        PortfolioAction.OBSERVE,
        PortfolioAction.HOLD,
    }
    if requested_action is PortfolioAction.HOLD and position is None:
        blockers.add("position_missing")
    if requested_action is PortfolioAction.ROTATE:
        blockers.add("rotate_requires_linked_portfolio_decisions")

    order_side: Side | None = None
    if requested_action in {
        PortfolioAction.OPEN,
        PortfolioAction.INCREASE,
        PortfolioAction.REDUCE,
        PortfolioAction.CLOSE,
    }:
        if position_snapshot.open_orders is None:
            blockers.add("open_orders_unavailable")
        elif any(item.target_id == signal.instrument_id for item in position_snapshot.open_orders):
            blockers.add("open_order_conflict")
        if requested_action in {PortfolioAction.OPEN, PortfolioAction.INCREASE}:
            if not authorized_view.exposure_increase_ready:
                blockers.add("exposure_increase_not_ready")
            if requested_action is PortfolioAction.OPEN and position is not None:
                blockers.add("position_already_open")
            if requested_action is PortfolioAction.INCREASE:
                if position is None:
                    blockers.add("position_missing")
                elif position.side is not signal.side:
                    blockers.add("position_side_conflict")
            order_side = signal.side
        else:
            if not authorized_view.risk_observation_ready:
                blockers.add("risk_observation_not_ready")
            if _order_state_not_authoritative(authorized_view.observation_gaps):
                blockers.add("order_state_not_authoritative")
            if position is None:
                blockers.add("position_missing")
            elif position.side is signal.side:
                blockers.add("risk_reduction_signal_not_opposing_position")
            order_side = signal.side

    ordered_blockers = tuple(sorted(blockers))
    if ordered_blockers:
        outcome = PortfolioDecisionOutcome.REJECTED
        order_side = None
    elif no_action:
        outcome = PortfolioDecisionOutcome.NO_ACTION
    else:
        outcome = PortfolioDecisionOutcome.READY_FOR_SIZING
    current_quantity = None if position is None else position.quantity
    core = {
        "schema_version": PORTFOLIO_DECISION_SCHEMA,
        "signal_id": signal.signal_id,
        "signal_hash": canonical_hash(signal.to_dict()),
        "authorized_decision_view_id": authorized_view.view_id,
        "authorized_decision_view_hash": canonical_hash(authorized_view.to_dict()),
        "position_snapshot_id": position_snapshot.snapshot_id,
        "position_snapshot_hash": canonical_hash(position_snapshot.to_dict()),
        "requested_action": requested_action.value,
        "instrument_id": signal.instrument_id,
        "venue": venue,
        "instrument_class": instrument_class,
        "evidence_refs": list(ordered_evidence),
        "outcome": outcome.value,
        "order_side": None if order_side is None else order_side.value,
        "current_position_quantity": (
            None if current_quantity is None else _decimal_text(current_quantity)
        ),
        "blockers": list(ordered_blockers),
        "decided_at": _timestamp(decided_at),
        "execution_capability": False,
    }
    return PortfolioDecision(
        decision_id=f"portfolio-decision-{canonical_hash(core)}",
        signal_id=signal.signal_id,
        signal_hash=canonical_hash(signal.to_dict()),
        authorized_decision_view_id=authorized_view.view_id,
        authorized_decision_view_hash=canonical_hash(authorized_view.to_dict()),
        position_snapshot_id=position_snapshot.snapshot_id,
        position_snapshot_hash=canonical_hash(position_snapshot.to_dict()),
        requested_action=requested_action,
        instrument_id=signal.instrument_id,
        venue=venue,
        instrument_class=instrument_class,
        evidence_refs=ordered_evidence,
        outcome=outcome,
        order_side=order_side,
        current_position_quantity=current_quantity,
        blockers=ordered_blockers,
        decided_at=decided_at,
    )


def size_portfolio_decision(
    *,
    portfolio_decision: PortfolioDecision,
    position_snapshot: PositionSnapshot,
    mandate: TradingMandate,
    price_basis: PriceBasisLike,
    rule_set: ExchangeInstrumentRuleSet,
    sizing_policy: OrderSizingPolicy,
    order_kind: OrderKind,
    decided_at: datetime,
) -> OrderSizingDecision:
    _strict_utc(decided_at, "Order Sizing decided_at")
    if portfolio_decision.outcome is not PortfolioDecisionOutcome.READY_FOR_SIZING:
        raise PermissionError("Order Sizing requires a ready Portfolio Decision")
    if portfolio_decision.position_snapshot_id != position_snapshot.snapshot_id:
        raise ValueError("Order Sizing received a different Position Snapshot")
    if decided_at < portfolio_decision.decided_at:
        raise ValueError("Order Sizing cannot predate the Portfolio Decision")
    if mandate.account_id != position_snapshot.account_reference_hash:
        raise PermissionError("Trading Mandate does not bind the reconciled account")
    if mandate.environment is not position_snapshot.environment:
        raise PermissionError("Trading Mandate environment differs from account state")
    side = portfolio_decision.order_side
    if side is None:
        raise AssertionError("ready Portfolio Decision lacks an order side")
    rule = _instrument_rule(rule_set, portfolio_decision)
    blockers: set[str] = set()
    if not mandate.valid_from <= decided_at < mandate.expires_at:
        blockers.add("trading_mandate_not_current")
    if portfolio_decision.instrument_id not in mandate.allowed_instruments:
        blockers.add("instrument_not_allowed_by_mandate")
    if side not in mandate.allowed_sides:
        blockers.add("side_not_allowed_by_mandate")
    if price_basis.instrument_id != portfolio_decision.instrument_id:
        blockers.add("price_basis_instrument_mismatch")
    if price_basis.currency != rule.currency:
        blockers.add("price_basis_currency_mismatch")
    if price_basis.unit != "per_share":
        blockers.add("price_basis_unit_unsupported")
    if price_basis.basis_kind not in {
        "reference_quote",
        "raw_reference_quote",
        "limit_price",
    }:
        blockers.add("price_basis_not_raw_tradable")
    if not price_basis.observed_at <= decided_at < price_basis.valid_until:
        blockers.add("price_basis_not_current")
    if rule_set.effective_from > decided_at.astimezone(ZoneInfo("Asia/Shanghai")).date():
        blockers.add("instrument_rule_not_effective")
    if order_kind is OrderKind.LIMIT and price_basis.basis_kind != "limit_price":
        blockers.add("limit_order_requires_limit_price_basis")
    if order_kind is OrderKind.LIMIT:
        price_tick = Decimal(str(rule.price_tick))
        if price_basis.price % price_tick:
            blockers.add("limit_price_not_tick_aligned")
    if side is Side.BUY and rule.scope != "ordinary_auction_buy_order":
        blockers.add("buy_tradability_rule_not_accepted")
    if side is Side.SELL and rule.scope not in {
        "ordinary_auction_sell_order",
        "ordinary_auction_buy_and_sell_order",
    }:
        blockers.add("sell_tradability_rule_not_accepted")

    quantity: Decimal | None = None
    notional: Decimal | None = None
    if not blockers:
        quantity = _quantity(
            portfolio_decision=portfolio_decision,
            position_snapshot=position_snapshot,
            mandate=mandate,
            price=price_basis.price,
            currency=price_basis.currency,
            rule=rule,
            policy=sizing_policy,
            blockers=blockers,
        )
        if quantity is not None:
            notional = quantity * price_basis.price
            if notional > mandate.max_order_notional:
                blockers.add("sized_notional_exceeds_mandate")
                quantity = None
                notional = None
    if blockers:
        quantity = None
        notional = None
        outcome = OrderSizingOutcome.REJECTED
    else:
        outcome = OrderSizingOutcome.READY
    ordered_blockers = tuple(sorted(blockers))
    limit_price = price_basis.price if order_kind is OrderKind.LIMIT else None
    policy_payload = sizing_policy.to_dict()
    core = {
        "schema_version": ORDER_SIZING_DECISION_SCHEMA,
        "portfolio_decision_id": portfolio_decision.decision_id,
        "portfolio_decision_hash": canonical_hash(portfolio_decision.to_dict()),
        "position_snapshot_id": position_snapshot.snapshot_id,
        "position_snapshot_hash": canonical_hash(position_snapshot.to_dict()),
        "trading_mandate_hash": canonical_hash(mandate.to_dict()),
        "price_basis_hash": canonical_hash(price_basis.to_dict()),
        "instrument_rule_set_id": rule_set.rule_set_id,
        "instrument_rule_set_hash": canonical_hash(rule_set.to_dict()),
        "instrument_rule_key": rule.rule_key,
        "sizing_policy_id": sizing_policy.policy_id,
        "sizing_policy": policy_payload,
        "instrument_id": portfolio_decision.instrument_id,
        "side": side.value,
        "order_kind": order_kind.value,
        "limit_price": None if limit_price is None else _decimal_text(limit_price),
        "reference_price": _decimal_text(price_basis.price),
        "quantity": None if quantity is None else _decimal_text(quantity),
        "order_notional": None if notional is None else _decimal_text(notional),
        "outcome": outcome.value,
        "blockers": list(ordered_blockers),
        "decided_at": _timestamp(decided_at),
        "execution_capability": False,
    }
    return OrderSizingDecision(
        decision_id=f"order-sizing-decision-{canonical_hash(core)}",
        portfolio_decision_id=portfolio_decision.decision_id,
        portfolio_decision_hash=canonical_hash(portfolio_decision.to_dict()),
        position_snapshot_id=position_snapshot.snapshot_id,
        position_snapshot_hash=canonical_hash(position_snapshot.to_dict()),
        trading_mandate_hash=canonical_hash(mandate.to_dict()),
        price_basis_hash=canonical_hash(price_basis.to_dict()),
        instrument_rule_set_id=rule_set.rule_set_id,
        instrument_rule_set_hash=canonical_hash(rule_set.to_dict()),
        instrument_rule_key=rule.rule_key,
        sizing_policy_id=sizing_policy.policy_id,
        max_available_cash_fraction=sizing_policy.max_available_cash_fraction,
        reduction_fraction=sizing_policy.reduction_fraction,
        instrument_id=portfolio_decision.instrument_id,
        side=side,
        order_kind=order_kind,
        limit_price=limit_price,
        reference_price=price_basis.price,
        quantity=quantity,
        order_notional=notional,
        outcome=outcome,
        blockers=ordered_blockers,
        decided_at=decided_at,
    )


def build_order_intent_from_sizing(
    *,
    sizing_decision: OrderSizingDecision,
    signal: SignalIntent,
    mandate: TradingMandate,
    expires_at: datetime,
) -> OrderIntent:
    if sizing_decision.outcome is not OrderSizingOutcome.READY:
        raise PermissionError("rejected Order Sizing Decision cannot create an Order Intent")
    if sizing_decision.quantity is None:
        raise AssertionError("ready Order Sizing Decision lacks quantity")
    if (
        sizing_decision.instrument_id != signal.instrument_id
        or sizing_decision.side is not signal.side
    ):
        raise ValueError("Order Sizing Decision differs from the Signal")
    if sizing_decision.trading_mandate_hash != canonical_hash(mandate.to_dict()):
        raise ValueError("Order Sizing Decision binds another Trading Mandate")
    _strict_utc(expires_at, "Order Intent expires_at")
    if not sizing_decision.decided_at < expires_at <= signal.expires_at:
        raise PermissionError("Order Intent expiry must fit inside Signal validity")
    identity = {
        "order_sizing_decision_id": sizing_decision.decision_id,
        "signal_id": signal.signal_id,
        "account_id": mandate.account_id,
        "environment": mandate.environment.value,
        "expires_at": _timestamp(expires_at),
    }
    return OrderIntent(
        client_order_id=f"portfolio-order-{canonical_hash(identity)}",
        signal_id=signal.signal_id,
        account_id=mandate.account_id,
        environment=mandate.environment,
        instrument_id=signal.instrument_id,
        side=sizing_decision.side,
        quantity=sizing_decision.quantity,
        order_kind=sizing_decision.order_kind,
        limit_price=sizing_decision.limit_price,
        created_at=sizing_decision.decided_at,
        expires_at=expires_at,
    )


def _quantity(
    *,
    portfolio_decision: PortfolioDecision,
    position_snapshot: PositionSnapshot,
    mandate: TradingMandate,
    price: Decimal,
    currency: str,
    rule: ExchangeInstrumentRule,
    policy: OrderSizingPolicy,
    blockers: set[str],
) -> Decimal | None:
    action = portfolio_decision.requested_action
    if action in {PortfolioAction.OPEN, PortfolioAction.INCREASE}:
        if portfolio_decision.order_side is Side.SELL:
            blockers.add("short_exposure_increase_not_supported")
            return None
        if position_snapshot.cash is None:
            blockers.add("cash_unavailable")
            return None
        balances = tuple(item for item in position_snapshot.cash if item.currency == currency)
        if len(balances) != 1:
            blockers.add("cash_currency_not_unique")
            return None
        available = balances[0].available
        if available <= 0:
            blockers.add("available_cash_not_positive")
            return None
        budget = min(
            mandate.max_order_notional,
            available * policy.max_available_cash_fraction,
        )
        lot = Decimal(rule.buy_lot_size)
        quantity = ((budget / price) / lot).to_integral_value(rounding=ROUND_DOWN) * lot
        if quantity <= 0:
            blockers.add("budget_below_one_buy_lot")
            return None
        return quantity
    current = portfolio_decision.current_position_quantity
    if current is None:
        blockers.add("position_quantity_missing")
        return None
    if action is PortfolioAction.CLOSE:
        lot = Decimal(rule.buy_lot_size)
        if current % lot:
            blockers.add("close_quantity_not_lot_aligned")
            return None
        return current
    if action is PortfolioAction.REDUCE:
        lot = Decimal(rule.buy_lot_size)
        quantity = ((current * policy.reduction_fraction) / lot).to_integral_value(
            rounding=ROUND_DOWN
        ) * lot
        if quantity <= 0:
            blockers.add("reduction_below_one_lot")
            return None
        if quantity >= current:
            blockers.add("reduction_would_close_position")
            return None
        return quantity
    raise AssertionError("ready Portfolio Decision has an unsupported action")


class TargetExposureDirection(StrEnum):
    LONG = "long"
    SHORT = "short"


class BearishExpressionMode(StrEnum):
    BORROWED_ORDINARY_ETF = "borrowed_ordinary_etf"
    NONLEVERED_INVERSE_ETF = "nonlevered_inverse_etf"


class PortfolioLegRole(StrEnum):
    PRIMARY = "primary"
    ROTATION_SOURCE = "rotation_source"
    ROTATION_DESTINATION = "rotation_destination"


@dataclass(frozen=True, slots=True)
class AgentPortfolioProposalV2:
    proposal_id: str
    signal_id: str
    signal_hash: str
    requested_action: PortfolioAction
    instrument_id: str
    venue: str
    instrument_class: str
    direction: TargetExposureDirection
    horizon_sessions: int
    target_gross_exposure_ratio: Decimal
    rationale: str
    evidence_refs: tuple[str, ...]
    counterevidence_refs: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    schema_version: str = AGENT_PORTFOLIO_PROPOSAL_V2_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != AGENT_PORTFOLIO_PROPOSAL_V2_SCHEMA:
            raise ValueError("unsupported Agent Portfolio Proposal schema")
        _trimmed(self.signal_id, "Agent Portfolio Proposal signal_id")
        _sha256(self.signal_hash, "Agent Portfolio Proposal Signal hash")
        _instrument_identity(self.instrument_id, self.venue, self.instrument_class)
        if self.horizon_sessions <= 0:
            raise ValueError("Agent Portfolio Proposal horizon must be positive")
        if (
            not self.target_gross_exposure_ratio.is_finite()
            or self.target_gross_exposure_ratio < 0
            or self.target_gross_exposure_ratio > 1
        ):
            raise ValueError("target gross exposure ratio must be between zero and one")
        if self.requested_action is PortfolioAction.CLOSE:
            if self.target_gross_exposure_ratio != 0:
                raise ValueError("close proposal target gross exposure must be zero")
        elif (
            self.requested_action
            not in {
                PortfolioAction.ABSTAIN,
                PortfolioAction.OBSERVE,
            }
            and self.target_gross_exposure_ratio == 0
        ):
            raise ValueError("actionable proposal target gross exposure must be positive")
        _trimmed(self.rationale, "Agent Portfolio Proposal rationale")
        _sorted_unique(self.evidence_refs, "Agent Portfolio Proposal evidence_refs")
        _sorted_unique(
            self.counterevidence_refs,
            "Agent Portfolio Proposal counterevidence_refs",
        )
        _sorted_unique(
            self.invalidation_conditions,
            "Agent Portfolio Proposal invalidation_conditions",
        )
        if not self.evidence_refs or not self.invalidation_conditions:
            raise ValueError("Agent Portfolio Proposal requires evidence and invalidation")
        if set(self.evidence_refs) & set(self.counterevidence_refs):
            raise ValueError("proposal evidence and counterevidence must not overlap")
        if self.proposal_id != self.expected_proposal_id:
            raise ValueError("Agent Portfolio Proposal ID does not match content")

    @property
    def expected_proposal_id(self) -> str:
        return f"agent-portfolio-proposal-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "signal_id": self.signal_id,
            "signal_hash": self.signal_hash,
            "requested_action": self.requested_action.value,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "direction": self.direction.value,
            "horizon_sessions": self.horizon_sessions,
            "target_gross_exposure_ratio": _decimal_text(self.target_gross_exposure_ratio),
            "rationale": self.rationale,
            "evidence_refs": list(self.evidence_refs),
            "counterevidence_refs": list(self.counterevidence_refs),
            "invalidation_conditions": list(self.invalidation_conditions),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "proposal_id": self.proposal_id}

    @classmethod
    def build(
        cls,
        *,
        signal: SignalIntent,
        requested_action: PortfolioAction,
        venue: str,
        instrument_class: str,
        direction: TargetExposureDirection,
        horizon_sessions: int,
        target_gross_exposure_ratio: Decimal,
        rationale: str,
        evidence_refs: tuple[str, ...],
        counterevidence_refs: tuple[str, ...],
        invalidation_conditions: tuple[str, ...],
    ) -> AgentPortfolioProposalV2:
        ordered_evidence = tuple(sorted(set(evidence_refs)))
        ordered_counter = tuple(sorted(set(counterevidence_refs)))
        ordered_invalidation = tuple(sorted(set(invalidation_conditions)))
        if not set(ordered_evidence) | set(ordered_counter) <= set(signal.evidence_refs):
            raise ValueError("Agent Portfolio Proposal cites evidence outside its Signal")
        if not set(ordered_invalidation) <= set(signal.invalidation_conditions):
            raise ValueError("Agent Portfolio Proposal invalidation differs from its Signal")
        core = {
            "schema_version": AGENT_PORTFOLIO_PROPOSAL_V2_SCHEMA,
            "signal_id": signal.signal_id,
            "signal_hash": canonical_hash(signal.to_dict()),
            "requested_action": requested_action.value,
            "instrument_id": signal.instrument_id,
            "venue": venue,
            "instrument_class": instrument_class,
            "direction": direction.value,
            "horizon_sessions": horizon_sessions,
            "target_gross_exposure_ratio": _decimal_text(target_gross_exposure_ratio),
            "rationale": rationale,
            "evidence_refs": list(ordered_evidence),
            "counterevidence_refs": list(ordered_counter),
            "invalidation_conditions": list(ordered_invalidation),
        }
        return cls(
            proposal_id=f"agent-portfolio-proposal-{canonical_hash(core)}",
            signal_id=signal.signal_id,
            signal_hash=canonical_hash(signal.to_dict()),
            requested_action=requested_action,
            instrument_id=signal.instrument_id,
            venue=venue,
            instrument_class=instrument_class,
            direction=direction,
            horizon_sessions=horizon_sessions,
            target_gross_exposure_ratio=target_gross_exposure_ratio,
            rationale=rationale,
            evidence_refs=ordered_evidence,
            counterevidence_refs=ordered_counter,
            invalidation_conditions=ordered_invalidation,
        )


def agent_portfolio_proposal_v2_from_dict(value: object) -> AgentPortfolioProposalV2:
    if not isinstance(value, dict):
        raise TypeError("Agent Portfolio Proposal must be a JSON object")
    raw_payload = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw_payload):
        raise TypeError("Agent Portfolio Proposal field names must be strings")
    payload = cast(dict[str, object], raw_payload)
    expected = {
        "schema_version",
        "proposal_id",
        "signal_id",
        "signal_hash",
        "requested_action",
        "instrument_id",
        "venue",
        "instrument_class",
        "direction",
        "horizon_sessions",
        "target_gross_exposure_ratio",
        "rationale",
        "evidence_refs",
        "counterevidence_refs",
        "invalidation_conditions",
    }
    if payload.keys() != expected:
        unknown = sorted(payload.keys() - expected)
        missing = sorted(expected - payload.keys())
        raise ValueError(
            f"Agent Portfolio Proposal fields differ: missing={missing}, unknown={unknown}"
        )
    proposal = AgentPortfolioProposalV2(
        proposal_id=_mapping_string(payload, "proposal_id"),
        signal_id=_mapping_string(payload, "signal_id"),
        signal_hash=_mapping_string(payload, "signal_hash"),
        requested_action=PortfolioAction(_mapping_string(payload, "requested_action")),
        instrument_id=_mapping_string(payload, "instrument_id"),
        venue=_mapping_string(payload, "venue"),
        instrument_class=_mapping_string(payload, "instrument_class"),
        direction=TargetExposureDirection(_mapping_string(payload, "direction")),
        horizon_sessions=_mapping_int(payload, "horizon_sessions"),
        target_gross_exposure_ratio=_mapping_decimal(payload, "target_gross_exposure_ratio"),
        rationale=_mapping_string(payload, "rationale"),
        evidence_refs=_mapping_strings(payload, "evidence_refs"),
        counterevidence_refs=_mapping_strings(payload, "counterevidence_refs"),
        invalidation_conditions=_mapping_strings(payload, "invalidation_conditions"),
        schema_version=_mapping_string(payload, "schema_version"),
    )
    if proposal.to_dict() != payload:
        raise ValueError("Agent Portfolio Proposal is not canonically serialized")
    return proposal


@dataclass(frozen=True, slots=True)
class BearishExpressionBinding:
    binding_id: str
    proposal_id: str
    account_reference_hash: str
    instrument_id: str
    mode: BearishExpressionMode
    account_permission_confirmed: bool
    shortable_quantity: Decimal | None
    allowlisted_inverse_etf: bool
    leverage_magnitude: Decimal | None
    evidence_refs: tuple[str, ...]
    observed_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        _prefixed_hash(
            self.proposal_id,
            "agent-portfolio-proposal-",
            "Bearish Expression proposal ID",
        )
        _prefixed_hash(self.account_reference_hash, "account-ref-", "account reference")
        _trimmed(self.instrument_id, "Bearish Expression instrument")
        _sorted_unique(self.evidence_refs, "Bearish Expression evidence_refs")
        _strict_utc(self.observed_at, "Bearish Expression observed_at")
        _strict_utc(self.valid_until, "Bearish Expression valid_until")
        if self.valid_until <= self.observed_at or not self.evidence_refs:
            raise ValueError("Bearish Expression requires current evidence")
        if self.mode is BearishExpressionMode.BORROWED_ORDINARY_ETF:
            if (
                not self.account_permission_confirmed
                or self.shortable_quantity is None
                or not self.shortable_quantity.is_finite()
                or self.shortable_quantity <= 0
                or self.allowlisted_inverse_etf
                or self.leverage_magnitude is not None
            ):
                raise ValueError("borrowed ETF binding lacks exact permission or borrow proof")
        elif (
            self.account_permission_confirmed
            or self.shortable_quantity is not None
            or not self.allowlisted_inverse_etf
            or self.leverage_magnitude != 1
        ):
            raise ValueError("inverse ETF binding must be allowlisted and exactly non-levered")
        if self.binding_id != self.expected_binding_id:
            raise ValueError("Bearish Expression Binding ID does not match content")

    @property
    def expected_binding_id(self) -> str:
        return f"bearish-expression-binding-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.bearish-expression-binding.v1",
            "proposal_id": self.proposal_id,
            "account_reference_hash": self.account_reference_hash,
            "instrument_id": self.instrument_id,
            "mode": self.mode.value,
            "account_permission_confirmed": self.account_permission_confirmed,
            "shortable_quantity": (
                None if self.shortable_quantity is None else _decimal_text(self.shortable_quantity)
            ),
            "allowlisted_inverse_etf": self.allowlisted_inverse_etf,
            "leverage_magnitude": (
                None if self.leverage_magnitude is None else _decimal_text(self.leverage_magnitude)
            ),
            "evidence_refs": list(self.evidence_refs),
            "observed_at": _timestamp(self.observed_at),
            "valid_until": _timestamp(self.valid_until),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "binding_id": self.binding_id}

    @classmethod
    def build(
        cls,
        *,
        proposal_id: str,
        account_reference_hash: str,
        instrument_id: str,
        mode: BearishExpressionMode,
        account_permission_confirmed: bool,
        shortable_quantity: Decimal | None,
        allowlisted_inverse_etf: bool,
        leverage_magnitude: Decimal | None,
        evidence_refs: tuple[str, ...],
        observed_at: datetime,
        valid_until: datetime,
    ) -> BearishExpressionBinding:
        ordered_evidence = tuple(sorted(set(evidence_refs)))
        core = {
            "schema_version": "market-impact.bearish-expression-binding.v1",
            "proposal_id": proposal_id,
            "account_reference_hash": account_reference_hash,
            "instrument_id": instrument_id,
            "mode": mode.value,
            "account_permission_confirmed": account_permission_confirmed,
            "shortable_quantity": (
                None if shortable_quantity is None else _decimal_text(shortable_quantity)
            ),
            "allowlisted_inverse_etf": allowlisted_inverse_etf,
            "leverage_magnitude": (
                None if leverage_magnitude is None else _decimal_text(leverage_magnitude)
            ),
            "evidence_refs": list(ordered_evidence),
            "observed_at": _timestamp(observed_at),
            "valid_until": _timestamp(valid_until),
        }
        return cls(
            binding_id=f"bearish-expression-binding-{canonical_hash(core)}",
            proposal_id=proposal_id,
            account_reference_hash=account_reference_hash,
            instrument_id=instrument_id,
            mode=mode,
            account_permission_confirmed=account_permission_confirmed,
            shortable_quantity=shortable_quantity,
            allowlisted_inverse_etf=allowlisted_inverse_etf,
            leverage_magnitude=leverage_magnitude,
            evidence_refs=ordered_evidence,
            observed_at=observed_at,
            valid_until=valid_until,
        )


@dataclass(frozen=True, slots=True)
class RawMarkedPositionV2:
    instrument_id: str
    venue: str
    instrument_class: str
    side: Side
    quantity: Decimal
    raw_price: Decimal
    raw_price_basis_hash: str

    def __post_init__(self) -> None:
        _instrument_identity(self.instrument_id, self.venue, self.instrument_class)
        _positive(self.quantity, "marked position quantity")
        _positive(self.raw_price, "marked position raw price")
        _sha256(self.raw_price_basis_hash, "marked position raw price hash")

    @property
    def signed_notional(self) -> Decimal:
        notional = self.quantity * self.raw_price
        return notional if self.side is Side.BUY else -notional

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "side": self.side.value,
            "quantity": _decimal_text(self.quantity),
            "raw_price": _decimal_text(self.raw_price),
            "raw_price_basis_hash": self.raw_price_basis_hash,
        }


@dataclass(frozen=True, slots=True)
class PortfolioExposureViewV2:
    exposure_view_id: str
    authorized_decision_view_id: str
    authorized_decision_view_hash: str
    position_snapshot_id: str
    position_snapshot_hash: str
    raw_mark_set_hash: str
    execution_ledger_snapshot_hash: str
    reconciliation_ledger_snapshot_hash: str
    currency: str
    marked_positions: tuple[RawMarkedPositionV2, ...]
    current_gross_exposure: Decimal
    current_net_exposure: Decimal
    daily_turnover_used: Decimal
    daily_submissions_used: int
    active_kill_reasons: tuple[str, ...]
    observed_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        _prefixed_hash(
            self.authorized_decision_view_id,
            "authorized-decision-view-",
            "Exposure View Authorized Decision View ID",
        )
        _sha256(self.authorized_decision_view_hash, "Exposure View decision view hash")
        _prefixed_hash(
            self.position_snapshot_id,
            "position-snapshot-",
            "Exposure View Position Snapshot ID",
        )
        for value, name in (
            (self.position_snapshot_hash, "position snapshot"),
            (self.raw_mark_set_hash, "raw mark set"),
            (self.execution_ledger_snapshot_hash, "execution ledger snapshot"),
            (self.reconciliation_ledger_snapshot_hash, "reconciliation ledger snapshot"),
        ):
            _sha256(value, f"Exposure View {name} hash")
        _trimmed(self.currency, "Exposure View currency")
        identities = tuple(
            (item.instrument_id, item.venue, item.instrument_class, item.side.value)
            for item in self.marked_positions
        )
        if identities != tuple(sorted(set(identities))):
            raise ValueError("Exposure View marked positions must be sorted and unique")
        if len({item.instrument_id for item in self.marked_positions}) != len(
            self.marked_positions
        ):
            raise ValueError("Exposure View requires one net position per instrument")
        expected_gross = sum(
            (abs(item.signed_notional) for item in self.marked_positions),
            Decimal(0),
        )
        expected_net = sum((item.signed_notional for item in self.marked_positions), Decimal(0))
        if (
            self.current_gross_exposure != expected_gross
            or self.current_net_exposure != expected_net
        ):
            raise ValueError("Exposure View gross/net totals must derive from raw marks")
        if not self.daily_turnover_used.is_finite() or self.daily_turnover_used < 0:
            raise ValueError("Exposure View turnover must be finite and non-negative")
        if self.daily_submissions_used < 0:
            raise ValueError("Exposure View submissions must be non-negative")
        _sorted_unique(self.active_kill_reasons, "Exposure View active kill reasons")
        if not set(self.active_kill_reasons) <= PORTFOLIO_KILL_REASONS_V2:
            raise ValueError("Exposure View contains an unsupported kill reason")
        _strict_utc(self.observed_at, "Exposure View observed_at")
        _strict_utc(self.valid_until, "Exposure View valid_until")
        if self.valid_until <= self.observed_at:
            raise ValueError("Exposure View validity must be positive")
        if self.exposure_view_id != self.expected_exposure_view_id:
            raise ValueError("Portfolio Exposure View ID does not match content")

    @property
    def expected_exposure_view_id(self) -> str:
        return f"portfolio-exposure-view-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.portfolio-exposure-view.v2",
            "authorized_decision_view_id": self.authorized_decision_view_id,
            "authorized_decision_view_hash": self.authorized_decision_view_hash,
            "position_snapshot_id": self.position_snapshot_id,
            "position_snapshot_hash": self.position_snapshot_hash,
            "raw_mark_set_hash": self.raw_mark_set_hash,
            "execution_ledger_snapshot_hash": self.execution_ledger_snapshot_hash,
            "reconciliation_ledger_snapshot_hash": self.reconciliation_ledger_snapshot_hash,
            "currency": self.currency,
            "marked_positions": [item.to_dict() for item in self.marked_positions],
            "current_gross_exposure": _decimal_text(self.current_gross_exposure),
            "current_net_exposure": _decimal_text(self.current_net_exposure),
            "daily_turnover_used": _decimal_text(self.daily_turnover_used),
            "daily_submissions_used": self.daily_submissions_used,
            "active_kill_reasons": list(self.active_kill_reasons),
            "observed_at": _timestamp(self.observed_at),
            "valid_until": _timestamp(self.valid_until),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "exposure_view_id": self.exposure_view_id}

    @classmethod
    def build(
        cls,
        *,
        authorized_view: AuthorizedDecisionView,
        position_snapshot: PositionSnapshot,
        raw_mark_set_hash: str,
        execution_ledger_snapshot_hash: str,
        reconciliation_ledger_snapshot_hash: str,
        currency: str,
        marked_positions: tuple[RawMarkedPositionV2, ...],
        daily_turnover_used: Decimal,
        daily_submissions_used: int,
        active_kill_reasons: tuple[str, ...],
        observed_at: datetime,
        valid_until: datetime,
    ) -> PortfolioExposureViewV2:
        ordered_marks = tuple(
            sorted(
                marked_positions,
                key=lambda item: (
                    item.instrument_id,
                    item.venue,
                    item.instrument_class,
                    item.side.value,
                ),
            )
        )
        gross = sum((abs(item.signed_notional) for item in ordered_marks), Decimal(0))
        net = sum((item.signed_notional for item in ordered_marks), Decimal(0))
        ordered_kills = tuple(sorted(set(active_kill_reasons)))
        common = {
            "authorized_decision_view_id": authorized_view.view_id,
            "authorized_decision_view_hash": canonical_hash(authorized_view.to_dict()),
            "position_snapshot_id": position_snapshot.snapshot_id,
            "position_snapshot_hash": canonical_hash(position_snapshot.to_dict()),
            "raw_mark_set_hash": raw_mark_set_hash,
            "execution_ledger_snapshot_hash": execution_ledger_snapshot_hash,
            "reconciliation_ledger_snapshot_hash": reconciliation_ledger_snapshot_hash,
            "currency": currency,
            "marked_positions": [item.to_dict() for item in ordered_marks],
            "current_gross_exposure": _decimal_text(gross),
            "current_net_exposure": _decimal_text(net),
            "daily_turnover_used": _decimal_text(daily_turnover_used),
            "daily_submissions_used": daily_submissions_used,
            "active_kill_reasons": list(ordered_kills),
            "observed_at": _timestamp(observed_at),
            "valid_until": _timestamp(valid_until),
        }
        core = {"schema_version": "market-impact.portfolio-exposure-view.v2", **common}
        return cls(
            exposure_view_id=f"portfolio-exposure-view-{canonical_hash(core)}",
            authorized_decision_view_id=authorized_view.view_id,
            authorized_decision_view_hash=canonical_hash(authorized_view.to_dict()),
            position_snapshot_id=position_snapshot.snapshot_id,
            position_snapshot_hash=canonical_hash(position_snapshot.to_dict()),
            raw_mark_set_hash=raw_mark_set_hash,
            execution_ledger_snapshot_hash=execution_ledger_snapshot_hash,
            reconciliation_ledger_snapshot_hash=reconciliation_ledger_snapshot_hash,
            currency=currency,
            marked_positions=ordered_marks,
            current_gross_exposure=gross,
            current_net_exposure=net,
            daily_turnover_used=daily_turnover_used,
            daily_submissions_used=daily_submissions_used,
            active_kill_reasons=ordered_kills,
            observed_at=observed_at,
            valid_until=valid_until,
        )


class RegisteredPortfolioExposureViewAuthorityV2:
    """Reopens only views registered by the Harness-owned ledger composition root."""

    def __init__(self, views: Mapping[str, PortfolioExposureViewV2]) -> None:
        registered = dict(views)
        if any(key != value.exposure_view_id for key, value in registered.items()):
            raise ValueError("Exposure View authority registry key differs from content identity")
        self.__views = registered

    def assert_authoritative_exposure_view(self, view: PortfolioExposureViewV2) -> None:
        registered = self.__views.get(view.exposure_view_id)
        if registered is None or registered.to_dict() != view.to_dict():
            raise PermissionError("Portfolio Exposure View lacks Harness ledger authority")


class RegisteredBearishExpressionAuthorityV2:
    """Reopens exact account-bound borrow or Instrument Master allowlist evidence."""

    def __init__(self, bindings: Mapping[str, BearishExpressionBinding]) -> None:
        registered = dict(bindings)
        if any(key != value.binding_id for key, value in registered.items()):
            raise ValueError("Bearish Expression registry key differs from content identity")
        self.__bindings = registered

    def assert_authoritative_bearish_expression(
        self,
        binding: BearishExpressionBinding,
    ) -> None:
        registered = self.__bindings.get(binding.binding_id)
        if registered is None or registered.to_dict() != binding.to_dict():
            raise PermissionError("Bearish Expression lacks Harness evidence authority")


@dataclass(frozen=True, slots=True)
class PortfolioDecisionLegV2:
    role: PortfolioLegRole
    action: PortfolioAction
    instrument_id: str
    venue: str
    instrument_class: str
    direction: TargetExposureDirection
    target_gross_exposure_ratio: Decimal
    current_side: Side | None
    current_quantity: Decimal
    current_concentration: Decimal | None
    current_concentration_gap: str | None
    position_snapshot_position_hash: str | None
    physical_target_side: Side
    gate: str | None

    def __post_init__(self) -> None:
        _instrument_identity(self.instrument_id, self.venue, self.instrument_class)
        if not self.current_quantity.is_finite() or self.current_quantity < 0:
            raise ValueError("Portfolio Decision leg current quantity must be non-negative")
        has_position = self.current_quantity > 0
        if has_position != (self.current_side is not None):
            raise ValueError("Portfolio Decision leg current side and quantity differ")
        if has_position:
            if (self.current_concentration is None) == (self.current_concentration_gap is None):
                raise ValueError("Portfolio Decision leg requires exact concentration state")
            if self.position_snapshot_position_hash is None:
                raise ValueError("Portfolio Decision leg lacks Position Snapshot binding")
            if self.current_concentration is not None and (
                not self.current_concentration.is_finite()
                or self.current_concentration < 0
                or self.current_concentration > 1
            ):
                raise ValueError("Portfolio Decision leg concentration is invalid")
            if self.current_concentration_gap is not None:
                _trimmed(
                    self.current_concentration_gap,
                    "Portfolio Decision leg concentration gap",
                )
            if self.position_snapshot_position_hash != canonical_hash(self.current_position_dict()):
                raise ValueError("Portfolio Decision leg Position Snapshot binding drifted")
        elif any(
            value is not None
            for value in (
                self.current_concentration,
                self.current_concentration_gap,
                self.position_snapshot_position_hash,
            )
        ):
            raise ValueError("absent Portfolio Decision position cannot carry binding state")
        if self.role is PortfolioLegRole.ROTATION_SOURCE and (
            self.action is not PortfolioAction.CLOSE
            or self.current_side is None
            or self.physical_target_side
            is (Side.BUY if self.current_side is Side.BUY else Side.SELL)
        ):
            raise ValueError("rotation source must close the exact bound position")

    def current_position_dict(self) -> dict[str, object]:
        if self.current_side is None:
            raise ValueError("Portfolio Decision leg has no current position")
        return {
            "target_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "side": self.current_side.value,
            "quantity": _normalized_decimal_text(self.current_quantity),
            "concentration": (
                None
                if self.current_concentration is None
                else _normalized_decimal_text(self.current_concentration)
            ),
            "concentration_gap": self.current_concentration_gap,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role.value,
            "action": self.action.value,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "direction": self.direction.value,
            "target_gross_exposure_ratio": _decimal_text(self.target_gross_exposure_ratio),
            "current_side": None if self.current_side is None else self.current_side.value,
            "current_quantity": _decimal_text(self.current_quantity),
            "current_concentration": (
                None
                if self.current_concentration is None
                else _decimal_text(self.current_concentration)
            ),
            "current_concentration_gap": self.current_concentration_gap,
            "position_snapshot_position_hash": self.position_snapshot_position_hash,
            "physical_target_side": self.physical_target_side.value,
            "gate": self.gate,
        }


@dataclass(frozen=True, slots=True)
class PortfolioDecisionV2:
    decision_id: str
    proposal: AgentPortfolioProposalV2
    authorized_decision_view_id: str
    authorized_decision_view_hash: str
    position_snapshot_id: str
    position_snapshot_hash: str
    bearish_expression_binding: BearishExpressionBinding | None
    legs: tuple[PortfolioDecisionLegV2, ...]
    outcome: PortfolioDecisionOutcome
    blockers: tuple[str, ...]
    decided_at: datetime
    execution_capability: bool = False
    schema_version: str = PORTFOLIO_DECISION_V2_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PORTFOLIO_DECISION_V2_SCHEMA:
            raise ValueError("unsupported Portfolio Decision v2 schema")
        _sorted_unique(self.blockers, "Portfolio Decision v2 blockers")
        _strict_utc(self.decided_at, "Portfolio Decision v2 decided_at")
        if self.execution_capability:
            raise ValueError("Portfolio Decision v2 cannot grant execution capability")
        if self.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING:
            if self.blockers or not self.legs:
                raise ValueError("ready Portfolio Decision v2 is inconsistent")
        elif self.outcome is PortfolioDecisionOutcome.REJECTED and (not self.blockers or self.legs):
            raise ValueError("rejected Portfolio Decision v2 is inconsistent")
        elif self.outcome is PortfolioDecisionOutcome.NO_ACTION and (self.blockers or self.legs):
            raise ValueError("no-action Portfolio Decision v2 is inconsistent")
        if self.outcome is PortfolioDecisionOutcome.READY_FOR_SIZING:
            self._validate_actionable_legs()
        if self.decision_id != self.expected_decision_id:
            raise ValueError("Portfolio Decision v2 ID does not match content")

    def _validate_actionable_legs(self) -> None:
        proposal = self.proposal
        binding = self.bearish_expression_binding
        expected_physical_side = (
            Side.BUY
            if proposal.direction is TargetExposureDirection.LONG
            or (
                binding is not None and binding.mode is BearishExpressionMode.NONLEVERED_INVERSE_ETF
            )
            else Side.SELL
        )
        if proposal.direction is TargetExposureDirection.SHORT:
            if (
                binding is None
                or binding.proposal_id != proposal.proposal_id
                or binding.instrument_id != proposal.instrument_id
            ):
                raise ValueError("actionable bearish proposal lacks its exact expression binding")
        elif binding is not None:
            raise ValueError("actionable long proposal cannot carry bearish expression evidence")
        destination = tuple(
            item
            for item in self.legs
            if item.role in {PortfolioLegRole.PRIMARY, PortfolioLegRole.ROTATION_DESTINATION}
        )
        if len(destination) != 1:
            raise ValueError("Portfolio Decision v2 requires one proposal-bound destination leg")
        target = destination[0]
        if (
            target.instrument_id != proposal.instrument_id
            or target.venue != proposal.venue
            or target.instrument_class != proposal.instrument_class
            or target.direction is not proposal.direction
            or target.target_gross_exposure_ratio != proposal.target_gross_exposure_ratio
            or target.physical_target_side is not expected_physical_side
        ):
            raise ValueError("Portfolio Decision v2 destination leg differs from Agent proposal")
        if proposal.requested_action is PortfolioAction.ROTATE:
            source = tuple(
                item for item in self.legs if item.role is PortfolioLegRole.ROTATION_SOURCE
            )
            if (
                len(self.legs) != 2
                or len(source) != 1
                or target.role is not PortfolioLegRole.ROTATION_DESTINATION
                or target.action not in {PortfolioAction.OPEN, PortfolioAction.INCREASE}
                or target.gate != "blocked_pending_source_reconciliation"
                or source[0].action is not PortfolioAction.CLOSE
                or source[0].gate is not None
            ):
                raise ValueError("Portfolio Decision v2 rotation legs are inconsistent")
        elif (
            len(self.legs) != 1
            or target.role is not PortfolioLegRole.PRIMARY
            or target.action is not proposal.requested_action
            or target.gate is not None
        ):
            raise ValueError("Portfolio Decision v2 primary leg differs from Agent action")

    @property
    def expected_decision_id(self) -> str:
        return f"portfolio-decision-v2-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "proposal": self.proposal.to_dict(),
            "authorized_decision_view_id": self.authorized_decision_view_id,
            "authorized_decision_view_hash": self.authorized_decision_view_hash,
            "position_snapshot_id": self.position_snapshot_id,
            "position_snapshot_hash": self.position_snapshot_hash,
            "bearish_expression_binding": (
                None
                if self.bearish_expression_binding is None
                else self.bearish_expression_binding.to_dict()
            ),
            "legs": [item.to_dict() for item in self.legs],
            "outcome": self.outcome.value,
            "blockers": list(self.blockers),
            "decided_at": _timestamp(self.decided_at),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}


def evaluate_portfolio_decision_v2(
    *,
    signal: SignalIntent,
    proposal: AgentPortfolioProposalV2,
    authorized_view: AuthorizedDecisionView,
    position_snapshot: PositionSnapshot,
    bearish_expression_binding: BearishExpressionBinding | None,
    bearish_expression_authority: BearishExpressionAuthorityV2 | None = None,
    decided_at: datetime,
) -> PortfolioDecisionV2:
    _strict_utc(decided_at, "Portfolio Decision v2 decided_at")
    blockers: set[str] = set()
    if proposal.signal_id != signal.signal_id or proposal.signal_hash != canonical_hash(
        signal.to_dict()
    ):
        raise ValueError("Agent Portfolio Proposal does not bind the Signal")
    if authorized_view.position_snapshot_id != position_snapshot.snapshot_id:
        raise ValueError("Authorized Decision View binds another Position Snapshot")
    if decided_at < authorized_view.frozen_at or not (
        signal.valid_from <= decided_at < signal.expires_at
    ):
        blockers.add("signal_or_view_not_current")
    if position_snapshot.open_orders is None:
        blockers.add("open_orders_unavailable")
    elif any(item.target_id == proposal.instrument_id for item in position_snapshot.open_orders):
        blockers.add("open_order_conflict")
    if (
        proposal.requested_action
        in {
            PortfolioAction.OPEN,
            PortfolioAction.INCREASE,
            PortfolioAction.ROTATE,
        }
        and not authorized_view.exposure_increase_ready
    ):
        blockers.add("exposure_increase_not_ready")
    elif not authorized_view.risk_observation_ready:
        blockers.add("risk_observation_not_ready")
    positions = position_snapshot.positions or ()
    matching = tuple(item for item in positions if item.target_id == proposal.instrument_id)
    if len(matching) > 1:
        blockers.add("ambiguous_destination_position")
    current = matching[0] if len(matching) == 1 else None
    no_action = proposal.requested_action in {
        PortfolioAction.ABSTAIN,
        PortfolioAction.OBSERVE,
        PortfolioAction.HOLD,
    }
    if proposal.requested_action is PortfolioAction.OPEN and current is not None:
        blockers.add("position_already_open")
    if (
        proposal.requested_action
        in {
            PortfolioAction.INCREASE,
            PortfolioAction.REDUCE,
            PortfolioAction.CLOSE,
        }
        and current is None
    ):
        blockers.add("position_missing")
    if proposal.requested_action is PortfolioAction.HOLD and current is None:
        blockers.add("position_missing")
    binding = bearish_expression_binding
    if binding is not None:
        if bearish_expression_authority is None:
            raise PermissionError("Bearish Expression requires Harness evidence authority")
        bearish_expression_authority.assert_authoritative_bearish_expression(binding)
    if proposal.direction is TargetExposureDirection.SHORT and not no_action:
        if binding is None:
            blockers.add("bearish_expression_binding_missing")
        else:
            if (
                binding.proposal_id != proposal.proposal_id
                or binding.account_reference_hash != position_snapshot.account_reference_hash
                or binding.instrument_id != proposal.instrument_id
            ):
                blockers.add("bearish_expression_binding_mismatch")
            if not binding.observed_at <= decided_at < binding.valid_until:
                blockers.add("bearish_expression_binding_not_current")
            if proposal.instrument_class != "exchange_traded_fund":
                blockers.add("bearish_expression_instrument_class_not_etf")
    elif binding is not None and not no_action:
        blockers.add("bearish_expression_binding_unexpected")
    physical_target_side = (
        Side.BUY
        if proposal.direction is TargetExposureDirection.LONG
        or (binding is not None and binding.mode is BearishExpressionMode.NONLEVERED_INVERSE_ETF)
        else Side.SELL
    )
    legs: tuple[PortfolioDecisionLegV2, ...] = ()
    if not blockers and not no_action:
        destination = PortfolioDecisionLegV2(
            role=(
                PortfolioLegRole.ROTATION_DESTINATION
                if proposal.requested_action is PortfolioAction.ROTATE
                else PortfolioLegRole.PRIMARY
            ),
            action=(
                (PortfolioAction.OPEN if current is None else PortfolioAction.INCREASE)
                if proposal.requested_action is PortfolioAction.ROTATE
                else proposal.requested_action
            ),
            instrument_id=proposal.instrument_id,
            venue=proposal.venue,
            instrument_class=proposal.instrument_class,
            direction=proposal.direction,
            target_gross_exposure_ratio=proposal.target_gross_exposure_ratio,
            current_side=None if current is None else current.side,
            current_quantity=Decimal(0) if current is None else current.quantity,
            current_concentration=None if current is None else current.concentration,
            current_concentration_gap=(None if current is None else current.concentration_gap),
            position_snapshot_position_hash=(
                None if current is None else canonical_hash(current.to_dict())
            ),
            physical_target_side=physical_target_side,
            gate=(
                "blocked_pending_source_reconciliation"
                if proposal.requested_action is PortfolioAction.ROTATE
                else None
            ),
        )
        if proposal.requested_action is PortfolioAction.ROTATE:
            sources = tuple(item for item in positions if item.target_id != proposal.instrument_id)
            if len(sources) != 1:
                blockers.add("rotation_source_not_unique")
            else:
                source = sources[0]
                source_leg = PortfolioDecisionLegV2(
                    role=PortfolioLegRole.ROTATION_SOURCE,
                    action=PortfolioAction.CLOSE,
                    instrument_id=source.target_id,
                    venue=source.venue,
                    instrument_class=source.instrument_class,
                    direction=(
                        TargetExposureDirection.LONG
                        if source.side is Side.BUY
                        else TargetExposureDirection.SHORT
                    ),
                    target_gross_exposure_ratio=Decimal(0),
                    current_side=source.side,
                    current_quantity=source.quantity,
                    current_concentration=source.concentration,
                    current_concentration_gap=source.concentration_gap,
                    position_snapshot_position_hash=canonical_hash(source.to_dict()),
                    physical_target_side=(Side.SELL if source.side is Side.BUY else Side.BUY),
                    gate=None,
                )
                legs = (source_leg, destination)
        else:
            legs = (destination,)
    if blockers:
        legs = ()
        outcome = PortfolioDecisionOutcome.REJECTED
    elif no_action:
        outcome = PortfolioDecisionOutcome.NO_ACTION
    else:
        outcome = PortfolioDecisionOutcome.READY_FOR_SIZING
    core = {
        "schema_version": PORTFOLIO_DECISION_V2_SCHEMA,
        "proposal": proposal.to_dict(),
        "authorized_decision_view_id": authorized_view.view_id,
        "authorized_decision_view_hash": canonical_hash(authorized_view.to_dict()),
        "position_snapshot_id": position_snapshot.snapshot_id,
        "position_snapshot_hash": canonical_hash(position_snapshot.to_dict()),
        "bearish_expression_binding": None if binding is None else binding.to_dict(),
        "legs": [item.to_dict() for item in legs],
        "outcome": outcome.value,
        "blockers": sorted(blockers),
        "decided_at": _timestamp(decided_at),
        "execution_capability": False,
    }
    return PortfolioDecisionV2(
        decision_id=f"portfolio-decision-v2-{canonical_hash(core)}",
        proposal=proposal,
        authorized_decision_view_id=authorized_view.view_id,
        authorized_decision_view_hash=canonical_hash(authorized_view.to_dict()),
        position_snapshot_id=position_snapshot.snapshot_id,
        position_snapshot_hash=canonical_hash(position_snapshot.to_dict()),
        bearish_expression_binding=binding,
        legs=legs,
        outcome=outcome,
        blockers=tuple(sorted(blockers)),
        decided_at=decided_at,
    )


@dataclass(frozen=True, slots=True)
class SizedPortfolioLegV2:
    role: PortfolioLegRole
    instrument_id: str
    price_basis_hash: str | None
    side: Side | None
    target_notional: Decimal
    current_signed_notional: Decimal
    delta_notional: Decimal
    quantity: Decimal | None
    outcome: OrderSizingOutcome
    blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.price_basis_hash is not None:
            _sha256(self.price_basis_hash, "Sized Portfolio leg Price Basis hash")
        if self.outcome is OrderSizingOutcome.READY and self.price_basis_hash is None:
            raise ValueError("ready Sized Portfolio leg lacks exact Price Basis hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role.value,
            "instrument_id": self.instrument_id,
            "price_basis_hash": self.price_basis_hash,
            "side": None if self.side is None else self.side.value,
            "target_notional": _decimal_text(self.target_notional),
            "current_signed_notional": _decimal_text(self.current_signed_notional),
            "delta_notional": _decimal_text(self.delta_notional),
            "quantity": None if self.quantity is None else _decimal_text(self.quantity),
            "outcome": self.outcome.value,
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class OrderSizingDecisionV2:
    decision_id: str
    portfolio_decision_id: str
    portfolio_decision_hash: str
    trading_mandate_hash: str
    exposure_view_id: str
    exposure_view_hash: str
    instrument_rule_set_id: str
    instrument_rule_set_hash: str
    price_basis_hashes: tuple[str | None, ...]
    legs: tuple[SizedPortfolioLegV2, ...]
    outcome: OrderSizingOutcome
    blockers: tuple[str, ...]
    decided_at: datetime
    execution_capability: bool = False
    schema_version: str = ORDER_SIZING_DECISION_V2_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != ORDER_SIZING_DECISION_V2_SCHEMA:
            raise ValueError("unsupported Order Sizing Decision v2 schema")
        _sorted_unique(self.blockers, "Order Sizing Decision v2 blockers")
        _strict_utc(self.decided_at, "Order Sizing Decision v2 decided_at")
        if self.execution_capability:
            raise ValueError("Order Sizing Decision v2 cannot grant execution capability")
        if self.price_basis_hashes != tuple(item.price_basis_hash for item in self.legs):
            raise ValueError("Order Sizing Decision v2 Price Basis bindings differ from legs")
        if self.decision_id != self.expected_decision_id:
            raise ValueError("Order Sizing Decision v2 ID does not match content")

    @property
    def expected_decision_id(self) -> str:
        return f"order-sizing-decision-v2-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "portfolio_decision_id": self.portfolio_decision_id,
            "portfolio_decision_hash": self.portfolio_decision_hash,
            "trading_mandate_hash": self.trading_mandate_hash,
            "exposure_view_id": self.exposure_view_id,
            "exposure_view_hash": self.exposure_view_hash,
            "instrument_rule_set_id": self.instrument_rule_set_id,
            "instrument_rule_set_hash": self.instrument_rule_set_hash,
            "price_basis_hashes": list(self.price_basis_hashes),
            "legs": [item.to_dict() for item in self.legs],
            "outcome": self.outcome.value,
            "blockers": list(self.blockers),
            "decided_at": _timestamp(self.decided_at),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}


def size_portfolio_decision_v2(
    *,
    portfolio_decision: PortfolioDecisionV2 | PortfolioDecisionV3,
    authorized_view: AuthorizedDecisionView,
    position_snapshot: PositionSnapshot,
    mandate: TradingMandateV2,
    exposure_view: PortfolioExposureViewV2,
    exposure_view_authority: PortfolioExposureViewAuthorityV2,
    price_bases: Mapping[str, PriceBasisLike],
    rule_set: ExchangeInstrumentRuleSet,
    bearish_expression_authority: BearishExpressionAuthorityV2 | None = None,
    decided_at: datetime,
) -> OrderSizingDecisionV2:
    _strict_utc(decided_at, "Order Sizing Decision v2 decided_at")
    if portfolio_decision.outcome is not PortfolioDecisionOutcome.READY_FOR_SIZING:
        raise PermissionError("Order Sizing v2 requires a ready Portfolio Decision v2")
    if portfolio_decision.proposal.target_gross_exposure_ratio is None:
        raise PermissionError("actionable portfolio target requires a target ratio")
    exposure_view_authority.assert_authoritative_exposure_view(exposure_view)
    expected_bindings = (
        portfolio_decision.authorized_decision_view_id == authorized_view.view_id,
        portfolio_decision.authorized_decision_view_hash
        == canonical_hash(authorized_view.to_dict()),
        portfolio_decision.position_snapshot_id == position_snapshot.snapshot_id,
        portfolio_decision.position_snapshot_hash == canonical_hash(position_snapshot.to_dict()),
        exposure_view.authorized_decision_view_id == authorized_view.view_id,
        exposure_view.authorized_decision_view_hash == canonical_hash(authorized_view.to_dict()),
        exposure_view.position_snapshot_id == position_snapshot.snapshot_id,
        exposure_view.position_snapshot_hash == canonical_hash(position_snapshot.to_dict()),
    )
    if not all(expected_bindings):
        raise ValueError("Order Sizing v2 input bindings differ")
    if mandate.account_id != position_snapshot.account_reference_hash:
        raise PermissionError("Trading Mandate v2 does not bind the reconciled account")
    blockers: set[str] = set(exposure_view.active_kill_reasons)
    bearish_binding = portfolio_decision.bearish_expression_binding
    if bearish_binding is not None:
        if bearish_expression_authority is None:
            raise PermissionError("Bearish Expression requires Harness evidence authority")
        bearish_expression_authority.assert_authoritative_bearish_expression(bearish_binding)
        if not bearish_binding.observed_at <= decided_at < bearish_binding.valid_until:
            blockers.add("bearish_expression_binding_not_current")
    if not mandate.valid_from <= decided_at < mandate.valid_until:
        blockers.add("trading_mandate_not_current")
    if not exposure_view.observed_at <= decided_at < exposure_view.valid_until:
        blockers.add("portfolio_exposure_view_not_current")
    if exposure_view.currency != mandate.currency:
        blockers.add("portfolio_exposure_currency_mismatch")
    if authorized_view.observation_gaps:
        blockers.add("account_snapshot_not_execution_ready")
    if exposure_view.daily_submissions_used >= mandate.daily_submission_limit:
        blockers.add("daily_submission_limit_reached")

    marked_by_id = {item.instrument_id: item for item in exposure_view.marked_positions}
    snapshot_positions = position_snapshot.positions or ()
    snapshot_position_hashes = {canonical_hash(item.to_dict()) for item in snapshot_positions}
    if {
        (item.target_id, item.venue, item.instrument_class, item.side, item.quantity)
        for item in snapshot_positions
    } != {
        (item.instrument_id, item.venue, item.instrument_class, item.side, item.quantity)
        for item in exposure_view.marked_positions
    }:
        raise ValueError("Exposure View marks do not cover the exact Position Snapshot")

    sized_legs: list[SizedPortfolioLegV2] = []
    projected_gross = exposure_view.current_gross_exposure
    projected_net = exposure_view.current_net_exposure
    projected_turnover = exposure_view.daily_turnover_used
    projected_count = len(exposure_view.marked_positions)
    for leg in portfolio_decision.legs:
        leg_blockers = set(blockers)
        if (
            leg.position_snapshot_position_hash is not None
            and leg.position_snapshot_position_hash not in snapshot_position_hashes
        ):
            raise ValueError("Portfolio Decision leg is not bound to its Position Snapshot")
        if leg.gate is not None:
            leg_blockers.add(leg.gate)
        if leg.instrument_id not in mandate.allowed_instruments:
            leg_blockers.add("instrument_not_allowed_by_mandate")
        mandate_class = _mandate_instrument_class(leg.instrument_class)
        if mandate_class not in mandate.allowed_instrument_classes:
            leg_blockers.add("instrument_class_not_allowed_by_mandate")
        price_basis = price_bases.get(leg.instrument_id)
        if price_basis is None:
            leg_blockers.add("raw_price_basis_missing")
            raw_price = Decimal(0)
            price_basis_hash = None
            rule = None
        else:
            raw_price = price_basis.price
            price_basis_hash = canonical_hash(price_basis.to_dict())
            rule = _instrument_rule_for_identity(rule_set, leg.venue, leg.instrument_class)
            if (
                price_basis.instrument_id != leg.instrument_id
                or price_basis.currency != mandate.currency
                or price_basis.unit != "per_share"
                or price_basis.basis_kind
                not in {"reference_quote", "raw_reference_quote", "limit_price"}
            ):
                leg_blockers.add("price_basis_not_raw_tradable")
            elif not price_basis.observed_at <= decided_at < price_basis.valid_until:
                leg_blockers.add("price_basis_not_current")
        current_mark = marked_by_id.get(leg.instrument_id)
        current_marked_signed = Decimal(0) if current_mark is None else current_mark.signed_notional
        current_signed = (
            Decimal(0)
            if current_mark is None or raw_price <= 0
            else current_mark.quantity
            * raw_price
            * (Decimal(1) if current_mark.side is Side.BUY else Decimal(-1))
        )
        target_abs = (
            portfolio_decision.proposal.target_gross_exposure_ratio * mandate.gross_exposure_limit
        )
        if leg.action is PortfolioAction.CLOSE:
            target_signed = Decimal(0)
        elif leg.physical_target_side is Side.BUY:
            target_signed = target_abs
        else:
            target_signed = -target_abs
        delta = target_signed - current_signed
        side = Side.BUY if delta > 0 else Side.SELL if delta < 0 else None
        quantity: Decimal | None = None
        if leg.action is PortfolioAction.INCREASE and current_signed != 0:
            if target_signed * current_signed < 0:
                leg_blockers.add("increase_target_changes_exposure_direction")
            elif abs(target_signed) <= abs(current_signed):
                leg_blockers.add("increase_target_does_not_strictly_increase_exposure")
        if side is None:
            leg_blockers.add("target_delta_is_zero")
        elif side not in mandate.allowed_sides:
            leg_blockers.add("side_not_allowed_by_mandate")
        if rule is not None and side is not None:
            if side is Side.BUY and rule.scope not in {
                "ordinary_auction_buy_order",
                "ordinary_auction_buy_and_sell_order",
            }:
                leg_blockers.add("buy_tradability_rule_not_accepted")
            if side is Side.SELL and rule.scope not in {
                "ordinary_auction_sell_order",
                "ordinary_auction_buy_and_sell_order",
            }:
                leg_blockers.add("sell_tradability_rule_not_accepted")
            lot = Decimal(rule.buy_lot_size)
            if leg.action is PortfolioAction.CLOSE:
                if leg.current_quantity % lot:
                    leg_blockers.add("close_quantity_not_lot_aligned")
                else:
                    quantity = leg.current_quantity
            elif raw_price > 0:
                quantity = ((abs(delta) / raw_price) / lot).to_integral_value(
                    rounding=ROUND_DOWN
                ) * lot
                if quantity <= 0:
                    leg_blockers.add("target_delta_below_one_lot")
                    quantity = None
        if (
            portfolio_decision.proposal.direction is TargetExposureDirection.SHORT
            and portfolio_decision.bearish_expression_binding is not None
            and portfolio_decision.bearish_expression_binding.mode
            is BearishExpressionMode.BORROWED_ORDINARY_ETF
            and quantity is not None
            and quantity
            > cast(
                Decimal,
                portfolio_decision.bearish_expression_binding.shortable_quantity,
            )
        ):
            leg_blockers.add("borrow_proof_quantity_insufficient")
            quantity = None
        if leg.action is PortfolioAction.REDUCE and abs(target_signed) >= abs(current_signed):
            leg_blockers.add("reduce_target_does_not_reduce")
        if quantity is not None and raw_price > 0:
            rounded_notional = quantity * raw_price
            target_effective = (
                current_signed + rounded_notional
                if side is Side.BUY
                else current_signed - rounded_notional
            )
            next_gross = projected_gross - abs(current_marked_signed) + abs(target_effective)
            next_net = projected_net - current_marked_signed + target_effective
            next_turnover = projected_turnover + rounded_notional
            if next_gross > mandate.gross_exposure_limit:
                leg_blockers.add("gross_exposure_limit_exceeded")
            if not mandate.minimum_net_exposure <= next_net <= mandate.maximum_net_exposure:
                leg_blockers.add("net_exposure_band_exceeded")
            if abs(target_effective) > (
                mandate.maximum_single_position_fraction * mandate.gross_exposure_limit
            ):
                leg_blockers.add("single_position_limit_exceeded")
            next_count = projected_count + (1 if current_mark is None and target_effective else 0)
            next_count -= 1 if current_mark is not None and target_effective == 0 else 0
            if next_count > mandate.maximum_position_count:
                leg_blockers.add("position_count_limit_exceeded")
            if next_turnover > mandate.daily_turnover_limit:
                leg_blockers.add("daily_turnover_limit_exceeded")
            if side is Side.BUY and target_effective > current_signed:
                balances = (
                    ()
                    if position_snapshot.cash is None
                    else tuple(
                        item for item in position_snapshot.cash if item.currency == mandate.currency
                    )
                )
                if len(balances) != 1:
                    leg_blockers.add("cash_currency_not_unique")
                elif rounded_notional > min(balances[0].available, balances[0].settled):
                    leg_blockers.add("settled_or_available_funds_insufficient")
            if not leg_blockers:
                projected_gross = next_gross
                projected_net = next_net
                projected_turnover = next_turnover
                projected_count = next_count
        ordered_leg_blockers = tuple(sorted(leg_blockers))
        leg_outcome = (
            OrderSizingOutcome.REJECTED if ordered_leg_blockers else OrderSizingOutcome.READY
        )
        sized_legs.append(
            SizedPortfolioLegV2(
                role=leg.role,
                instrument_id=leg.instrument_id,
                price_basis_hash=price_basis_hash,
                side=side,
                target_notional=target_signed,
                current_signed_notional=current_signed,
                delta_notional=delta,
                quantity=None if ordered_leg_blockers else quantity,
                outcome=leg_outcome,
                blockers=ordered_leg_blockers,
            )
        )
    ready = tuple(item for item in sized_legs if item.outcome is OrderSizingOutcome.READY)
    global_blockers = tuple(sorted(blockers))
    outcome = OrderSizingOutcome.READY if ready else OrderSizingOutcome.REJECTED
    core = {
        "schema_version": ORDER_SIZING_DECISION_V2_SCHEMA,
        "portfolio_decision_id": portfolio_decision.decision_id,
        "portfolio_decision_hash": canonical_hash(portfolio_decision.to_dict()),
        "trading_mandate_hash": canonical_hash(mandate.to_dict()),
        "exposure_view_id": exposure_view.exposure_view_id,
        "exposure_view_hash": canonical_hash(exposure_view.to_dict()),
        "instrument_rule_set_id": rule_set.rule_set_id,
        "instrument_rule_set_hash": canonical_hash(rule_set.to_dict()),
        "price_basis_hashes": [item.price_basis_hash for item in sized_legs],
        "legs": [item.to_dict() for item in sized_legs],
        "outcome": outcome.value,
        "blockers": list(global_blockers),
        "decided_at": _timestamp(decided_at),
        "execution_capability": False,
    }
    return OrderSizingDecisionV2(
        decision_id=f"order-sizing-decision-v2-{canonical_hash(core)}",
        portfolio_decision_id=portfolio_decision.decision_id,
        portfolio_decision_hash=canonical_hash(portfolio_decision.to_dict()),
        trading_mandate_hash=canonical_hash(mandate.to_dict()),
        exposure_view_id=exposure_view.exposure_view_id,
        exposure_view_hash=canonical_hash(exposure_view.to_dict()),
        instrument_rule_set_id=rule_set.rule_set_id,
        instrument_rule_set_hash=canonical_hash(rule_set.to_dict()),
        price_basis_hashes=tuple(item.price_basis_hash for item in sized_legs),
        legs=tuple(sized_legs),
        outcome=outcome,
        blockers=global_blockers,
        decided_at=decided_at,
    )


def _instrument_rule_for_identity(
    rule_set: ExchangeInstrumentRuleSet,
    venue: str,
    instrument_class: str,
) -> ExchangeInstrumentRule:
    matches = tuple(
        item
        for item in rule_set.rules
        if item.venue == venue and item.instrument_class == instrument_class
    )
    if len(matches) != 1:
        raise ValueError("Order Sizing v2 requires one exact venue/class instrument rule")
    return matches[0]


def _mandate_instrument_class(instrument_class: str) -> str:
    if instrument_class == "equity":
        return "cash_equity"
    if instrument_class == "exchange_traded_fund":
        return "unlevered_exchange_traded_fund"
    return instrument_class


def _mapping_string(payload: Mapping[str, object], field: str) -> str:
    value = payload[field]
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    return value


def _mapping_int(payload: Mapping[str, object], field: str) -> int:
    value = payload[field]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    return value


def _mapping_decimal(payload: Mapping[str, object], field: str) -> Decimal:
    value = payload[field]
    if not isinstance(value, str):
        raise TypeError(f"{field} must be decimal text")
    return Decimal(value)


def _mapping_strings(payload: Mapping[str, object], field: str) -> tuple[str, ...]:
    value = payload[field]
    if not isinstance(value, list):
        raise TypeError(f"{field} must be a string array")
    raw_values = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw_values):
        raise TypeError(f"{field} must be a string array")
    return tuple(cast(list[str], raw_values))


def _normalized_decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _instrument_rule(
    rule_set: ExchangeInstrumentRuleSet,
    decision: PortfolioDecision,
) -> ExchangeInstrumentRule:
    matches = tuple(
        item
        for item in rule_set.rules
        if item.venue == decision.venue and item.instrument_class == decision.instrument_class
    )
    if len(matches) != 1:
        raise ValueError("Order Sizing requires one exact venue/class instrument rule")
    return matches[0]


def _order_state_not_authoritative(observation_gaps: tuple[str, ...]) -> bool:
    return any(
        gap == "stale"
        or gap == "manual_tws_open_orders_not_observed"
        or gap == "missing_section:open_orders"
        or gap.startswith("open_order_")
        for gap in observation_gaps
    )


def _instrument_identity(instrument_id: str, venue: str, instrument_class: str) -> None:
    _trimmed(instrument_id, "instrument_id")
    _trimmed(venue, "venue")
    _trimmed(instrument_class, "instrument_class")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be lowercase SHA-256 text")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _positive(value: Decimal, name: str) -> None:
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def _fraction(value: Decimal, name: str, *, allow_one: bool) -> None:
    if not value.is_finite() or value <= 0 or value > 1 or (not allow_one and value == 1):
        boundary = "(0, 1]" if allow_one else "(0, 1)"
        raise ValueError(f"{name} must be in {boundary}")


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")
    for value in values:
        _trimmed(value, name)


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
