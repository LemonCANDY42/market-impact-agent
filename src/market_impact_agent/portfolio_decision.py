from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from typing import Protocol
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
    require_aware,
)

PORTFOLIO_DECISION_SCHEMA = "market-impact.portfolio-decision.v1"
ORDER_SIZING_DECISION_SCHEMA = "market-impact.order-sizing-decision.v1"


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
