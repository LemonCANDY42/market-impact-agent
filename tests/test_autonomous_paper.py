from __future__ import annotations

import json
import sqlite3
import warnings
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from multiprocessing import get_context
from pathlib import Path
from threading import Event, Thread
from typing import cast

import pytest

from market_impact_agent.account_state import (
    AccountPosition,
    AccountStateSnapshot,
    CashBalance,
    OpenOrder,
    OpenOrderStatus,
    RecentFill,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.autonomous_paper import (
    AutonomousCancellationState,
    AutonomousOperationState,
    AutonomousPaperExecutionServiceV2,
    AutonomousPaperProviderLeaseAuthorityV2,
    AutonomousReconciliationAuthorityV2,
    _issue_autonomous_provider_lease,  # pyright: ignore[reportPrivateUsage]
    _record_accepted_provider_capability,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRule,
    ExchangeInstrumentRuleSet,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionReceipt,
    ExecutionStatus,
    Side,
    SignalIntent,
    TradingEnvironment,
    TradingMandate,
    TradingMandateV2,
)
from market_impact_agent.paper_execution import PaperExecutionService, PriceBasis
from market_impact_agent.portfolio_decision import (
    AgentPortfolioProposalV2,
    OrderSizingDecisionV2,
    PortfolioAction,
    PortfolioDecisionV2,
    PortfolioExposureViewV2,
    RawMarkedPositionV2,
    RegisteredPortfolioExposureViewAuthorityV2,
    TargetExposureDirection,
    evaluate_portfolio_decision_v2,
    size_portfolio_decision_v2,
)
from market_impact_agent.providers import (
    CancellationCapability,
    CancellationCommandReceipt,
    CancellationCommandStatus,
    Capability,
    ProviderManifest,
    ProviderTransport,
    ReconciliationSnapshot,
    SubmissionCapability,
    SubmissionCapabilityRejected,
    TrustTier,
)

AT = datetime(2026, 9, 2, 14, tzinfo=UTC)
TARGET = "SPY.ARCA"
SECOND_TARGET = "QQQ.ARCA"
ROUTES = {
    TARGET: {"provider_instrument_id": "SPY.ARCA", "market": "US"},
    SECOND_TARGET: {"provider_instrument_id": "QQQ.ARCA", "market": "US"},
}
ROUTES_HASH = canonical_hash(ROUTES)
HASH = "a" * 64


class _Provider:
    def __init__(
        self,
        *,
        ambiguous_once: bool = False,
        crash_once: bool = False,
        cancel_ambiguous_once: bool = False,
        cancel_crash_once: bool = False,
        malformed_cancel_once: bool = False,
    ) -> None:
        self.receipts: dict[str, ExecutionReceipt] = {}
        self.submission_validator: Callable[[SubmissionCapability], bool] = lambda _: False
        self.cancellation_validator: Callable[[CancellationCapability], bool] = lambda _: False
        self.ambiguous_once = ambiguous_once
        self.crash_once = crash_once
        self.cancel_ambiguous_once = cancel_ambiguous_once
        self.cancel_crash_once = cancel_crash_once
        self.malformed_cancel_once = malformed_cancel_once
        self.cancel_calls = 0
        self.reconcile_calls = 0
        self.cancel_hook: Callable[[], None] | None = None
        self.reconcile_hook: Callable[[], None] | None = None

    def make_terminal(self) -> None:
        self.receipts = {
            client_order_id: ExecutionReceipt(
                client_order_id=receipt.client_order_id,
                provider_order_id=receipt.provider_order_id,
                status=ExecutionStatus.FILLED,
                observed_at=AT + timedelta(seconds=3),
                filled_quantity=Decimal("1"),
                fill_ids=("fill-" + client_order_id[-12:],),
            )
            for client_order_id, receipt in self.receipts.items()
        }

    def make_canceled(self) -> None:
        self.receipts = {
            client_order_id: ExecutionReceipt(
                client_order_id=receipt.client_order_id,
                provider_order_id=receipt.provider_order_id,
                status=ExecutionStatus.CANCELED,
                observed_at=AT + timedelta(seconds=3),
            )
            for client_order_id, receipt in self.receipts.items()
        }

    @property
    def manifest(self) -> ProviderManifest:
        return ProviderManifest(
            schema_version="market-impact.provider-manifest.v1",
            provider_id="fixture-paper-provider",
            provider_version="2",
            transport=ProviderTransport.NATIVE,
            environments=frozenset({TradingEnvironment.PAPER}),
            declared_capabilities=frozenset({Capability.ACCOUNT, Capability.PAPER_EXECUTION}),
            verified_capabilities=frozenset({Capability.ACCOUNT, Capability.PAPER_EXECUTION}),
            markets=("US",),
            order_types=("market",),
            supports_streaming=False,
            supports_reconciliation=True,
            enabled=True,
            trust_tier=TrustTier.PAPER_VALIDATED,
        )

    def bind_submission_validator(self, validator: Callable[[SubmissionCapability], bool]) -> None:
        self.submission_validator = validator

    def bind_cancellation_validator(
        self, validator: Callable[[CancellationCapability], bool]
    ) -> None:
        self.cancellation_validator = validator

    def submit(self, capability: SubmissionCapability) -> ExecutionReceipt:
        if not self.submission_validator(capability):
            raise SubmissionCapabilityRejected("not durable")
        existing = self.receipts.get(capability.order.client_order_id)
        if existing is None:
            existing = ExecutionReceipt(
                client_order_id=capability.order.client_order_id,
                provider_order_id="provider-" + capability.order.client_order_id[-12:],
                status=ExecutionStatus.ACCEPTED,
                observed_at=AT + timedelta(seconds=4),
            )
            self.receipts[capability.order.client_order_id] = existing
        if self.ambiguous_once:
            self.ambiguous_once = False
            raise TimeoutError("acknowledgement lost after accepted submit")
        if self.crash_once:
            self.crash_once = False
            raise KeyboardInterrupt("simulated process crash after provider acceptance")
        return existing

    def cancel(self, capability: CancellationCapability) -> CancellationCommandReceipt:
        self.cancel_calls += 1
        if not self.cancellation_validator(capability):
            raise PermissionError("not durable")
        if self.cancel_hook is not None:
            self.cancel_hook()
        if self.cancel_ambiguous_once:
            self.cancel_ambiguous_once = False
            raise TimeoutError("cancel acknowledgement lost")
        prior = self.receipts[capability.client_order_id]
        canceled = ExecutionReceipt(
            client_order_id=prior.client_order_id,
            provider_order_id=prior.provider_order_id,
            status=ExecutionStatus.CANCELED,
            observed_at=AT + timedelta(seconds=5),
        )
        self.receipts[capability.client_order_id] = canceled
        if self.cancel_crash_once:
            self.cancel_crash_once = False
            raise KeyboardInterrupt("simulated process crash after Provider cancellation")
        receipt = CancellationCommandReceipt(
            client_order_id=capability.client_order_id,
            provider_order_id=capability.provider_order_id,
            cancellation_id=capability.cancellation_id,
            status=CancellationCommandStatus.CANCELED,
            observed_at=canceled.observed_at,
        )
        if self.malformed_cancel_once:
            self.malformed_cancel_once = False
            return replace(receipt, cancellation_id="wrong-cancellation-id")
        return receipt

    def reconcile(self) -> ReconciliationSnapshot:
        self.reconcile_calls += 1
        if self.reconcile_hook is not None:
            self.reconcile_hook()
        return ReconciliationSnapshot.build(
            provider_id=self.manifest.provider_id,
            observed_at=AT + timedelta(seconds=3),
            complete=True,
            receipts=tuple(self.receipts[key] for key in sorted(self.receipts)),
        )


class _ExposureAuthority:
    def __init__(self, box: list[PortfolioExposureViewV2]) -> None:
        self.box = box

    def assert_authoritative_exposure_view(self, view: PortfolioExposureViewV2) -> None:
        if view.to_dict() != self.box[0].to_dict():
            raise PermissionError("Exposure View lacks current Harness authority")


@dataclass
class _Fixture:
    store: LocalDataSnapshotStore
    provider: _Provider
    mandate: TradingMandateV2
    account: AccountStateSnapshot
    exposure: PortfolioExposureViewV2
    proposal: AgentPortfolioProposalV2
    decision: PortfolioDecisionV2
    sizing: OrderSizingDecisionV2
    price: PriceBasis
    account_box: list[AccountStateSnapshot]
    exposure_box: list[PortfolioExposureViewV2]
    clock_box: list[datetime]


def _fixture(
    root: Path,
    *,
    store: LocalDataSnapshotStore | None = None,
    action: PortfolioAction = PortfolioAction.OPEN,
    ambiguous: bool = False,
    crash: bool = False,
    instrument: str = TARGET,
    ratio: Decimal = Decimal("0.40"),
    signal_id: str = "signal-v2",
    cancel_ambiguous: bool = False,
    cancel_crash: bool = False,
    malformed_cancel: bool = False,
    exposure_nonce: str = "b",
    maximum_single_position_fraction: Decimal = Decimal("1"),
) -> _Fixture:
    store = store or LocalDataSnapshotStore(root / "harness")
    provider = _Provider(
        ambiguous_once=ambiguous,
        crash_once=crash,
        cancel_ambiguous_once=cancel_ambiguous,
        cancel_crash_once=cancel_crash,
        malformed_cancel_once=malformed_cancel,
    )
    position = (
        AccountPosition(
            target_id=instrument,
            venue="ARCX",
            instrument_class="exchange_traded_fund",
            side=Side.BUY,
            quantity=Decimal("600"),
            concentration=Decimal("0.60"),
            concentration_gap=None,
        )
        if action in {PortfolioAction.REDUCE, PortfolioAction.CLOSE}
        else None
    )
    account = capture_account_state_snapshot(
        provider=provider.manifest,
        account_reference="fixture-account",
        account_reference_key=b"fixture-account-key-material-32bytes",
        environment=TradingEnvironment.PAPER,
        as_of=AT,
        reconciled_at=AT,
        reconciliation_reference="fixture-reconciliation",
        cash=(CashBalance(currency="USD", available=Decimal("20000"), settled=Decimal("20000")),),
        positions=() if position is None else (position,),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=AT - timedelta(days=1),
    )
    position_snapshot = account.project_positions(
        evaluated_at=AT,
        max_age=timedelta(minutes=5),
    )
    view = AuthorizedDecisionView.build(
        cutoff=AT,
        frozen_at=AT + timedelta(seconds=1),
        data_snapshot_ids=("data",),
        decision_input_ids=("input",),
        position_snapshot=position_snapshot,
    )
    signal = SignalIntent(
        signal_id=signal_id,
        event_id="event-v2",
        instrument_id=instrument,
        side=(Side.SELL if action in {PortfolioAction.REDUCE, PortfolioAction.CLOSE} else Side.BUY),
        valid_from=AT,
        expires_at=AT + timedelta(minutes=10),
        evidence_refs=("evidence",),
        invalidation_conditions=("invalidated",),
    )
    proposal = AgentPortfolioProposalV2.build(
        signal=signal,
        requested_action=action,
        venue="ARCX",
        instrument_class="exchange_traded_fund",
        direction=TargetExposureDirection.LONG,
        horizon_sessions=1,
        target_gross_exposure_ratio=(Decimal("0") if action is PortfolioAction.CLOSE else ratio),
        rationale="Bounded Paper thesis.",
        evidence_refs=signal.evidence_refs,
        counterevidence_refs=(),
        invalidation_conditions=signal.invalidation_conditions,
    )
    decision = evaluate_portfolio_decision_v2(
        signal=signal,
        proposal=proposal,
        authorized_view=view,
        position_snapshot=position_snapshot,
        bearish_expression_binding=None,
        decided_at=AT + timedelta(seconds=2),
    )
    exposure = PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=position_snapshot,
        raw_mark_set_hash=exposure_nonce * 64,
        execution_ledger_snapshot_hash="c" * 64,
        reconciliation_ledger_snapshot_hash="d" * 64,
        currency="USD",
        marked_positions=(
            ()
            if position is None
            else (
                RawMarkedPositionV2(
                    instrument_id=position.target_id,
                    venue=position.venue,
                    instrument_class=position.instrument_class,
                    side=position.side,
                    quantity=position.quantity,
                    raw_price=Decimal("10"),
                    raw_price_basis_hash=HASH,
                ),
            )
        ),
        daily_turnover_used=Decimal(0),
        daily_submissions_used=0,
        active_kill_reasons=(),
        observed_at=AT + timedelta(seconds=1),
        valid_until=AT + timedelta(minutes=5),
    )
    mandate = TradingMandateV2(
        mandate_id="autonomous-paper-2026-09-02",
        account_id=account.account_reference_hash,
        harness_authority_id=store.harness_authority_id,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.AUTONOMOUS,
        valid_from=AT,
        valid_until=AT + timedelta(hours=8),
        allowed_instruments=frozenset(ROUTES),
        allowed_instrument_classes=frozenset({"unlevered_exchange_traded_fund"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        currency="USD",
        gross_exposure_limit=Decimal("10000"),
        minimum_net_exposure=Decimal("-10000"),
        maximum_net_exposure=Decimal("10000"),
        maximum_position_count=10,
        maximum_single_position_fraction=maximum_single_position_fraction,
        daily_turnover_limit=Decimal("50000"),
        daily_submission_limit=50,
        daily_loss_kill_threshold=Decimal("300"),
        strategy_peak_drawdown_kill_threshold=Decimal("1000"),
    )
    price = PriceBasis(
        instrument_id=instrument,
        currency="USD",
        unit="per_share",
        basis_kind="raw_reference_quote",
        price=Decimal("10"),
        source_id="fixture-price",
        source_version="1",
        observed_at=AT,
        valid_until=AT + timedelta(minutes=5),
    )
    rules = ExchangeInstrumentRuleSet(
        rule_set_id="exchange-instrument-rule-set-" + HASH,
        effective_from=date(2026, 1, 1),
        source_documents=({"source": "fixture"},),
        rules=(
            ExchangeInstrumentRule(
                rule_key="arcx-etf",
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
    authority = RegisteredPortfolioExposureViewAuthorityV2({exposure.exposure_view_id: exposure})
    sizing = size_portfolio_decision_v2(
        portfolio_decision=decision,
        authorized_view=view,
        position_snapshot=position_snapshot,
        mandate=mandate,
        exposure_view=exposure,
        exposure_view_authority=authority,
        price_bases={instrument: price},
        rule_set=rules,
        decided_at=AT + timedelta(seconds=3),
    )
    return _Fixture(
        store=store,
        provider=provider,
        mandate=mandate,
        account=account,
        exposure=exposure,
        proposal=proposal,
        decision=decision,
        sizing=sizing,
        price=price,
        account_box=[account],
        exposure_box=[exposure],
        clock_box=[AT + timedelta(seconds=3)],
    )


def _service(tmp_path: Path, fixture: _Fixture) -> AutonomousPaperExecutionServiceV2:
    _ = tmp_path
    store = fixture.store
    provider_acceptance_id = _paper_provider_acceptance(store, fixture)
    capability_id = _record_accepted_provider_capability(
        store,
        provider_acceptance_id=provider_acceptance_id,
    )
    lease = _issue_autonomous_provider_lease(
        store,
        accepted_capability_id=capability_id,
        provider=fixture.provider,
        mandate=fixture.mandate,
        instrument_routes=ROUTES,
    )
    return _open_service(store, fixture, lease.lease_id)


def _paper_provider_acceptance(store: LocalDataSnapshotStore, fixture: _Fixture) -> str:
    owner = PaperExecutionService(
        store.root / "paper-provider-owner",
        provider=fixture.provider,
        mandate=TradingMandate(
            mandate_id="provider-owner-" + fixture.mandate.mandate_id,
            account_id=fixture.mandate.account_id,
            environment=TradingEnvironment.PAPER,
            approval_mode=ApprovalMode.MANUAL_EACH,
            valid_from=fixture.mandate.valid_from,
            expires_at=fixture.mandate.valid_until,
            allowed_instruments=fixture.mandate.allowed_instruments,
            allowed_sides=fixture.mandate.allowed_sides,
            max_order_notional=fixture.mandate.gross_exposure_limit,
        ),
        price_source=lambda order: fixture.price,
        clock=lambda: fixture.clock_box[0],
        account_state_source=lambda: fixture.account,
    )
    return owner.record_provider_acceptance(store)


def _open_service(
    store: LocalDataSnapshotStore,
    fixture: _Fixture,
    lease_id: str,
) -> AutonomousPaperExecutionServiceV2:
    return AutonomousPaperExecutionServiceV2(
        store,
        provider=fixture.provider,
        provider_lease_id=lease_id,
        mandate=fixture.mandate,
        account_state_source=lambda: fixture.account_box[0],
        exposure_view_source=lambda: fixture.exposure_box[0],
        exposure_view_authority=_ExposureAuthority(fixture.exposure_box),
        price_basis_source=lambda instrument: (
            fixture.price if instrument == fixture.price.instrument_id else None
        ),
        reconciliation_authority=AutonomousReconciliationAuthorityV2(
            lambda snapshot: (fixture.account_box[0], fixture.exposure_box[0])
        ),
        instrument_routes_hash=ROUTES_HASH,
        instrument_routes=ROUTES,
        clock=lambda: fixture.clock_box[0],
    )


def _admit(service: AutonomousPaperExecutionServiceV2, fixture: _Fixture):
    return service.admit(
        proposal=fixture.proposal,
        portfolio_decision=fixture.decision,
        sizing_decision=fixture.sizing,
        account_state=fixture.account,
        exposure_view=fixture.exposure,
        price_bases={fixture.price.instrument_id: fixture.price},
    )


def _refresh_reconciliation_authorities(
    fixture: _Fixture,
    *,
    reflect_provider: bool = True,
) -> None:
    snapshot = fixture.provider.reconcile()
    original = fixture.account
    receipt = next(iter(snapshot.receipts), None)
    assert original.cash is not None
    cash = original.cash
    positions = original.positions or ()
    open_orders: tuple[OpenOrder, ...] = ()
    recent_fills: tuple[RecentFill, ...] = ()
    if receipt is not None and reflect_provider:
        side = fixture.proposal.requested_action
        order_side = (
            Side.SELL if side in {PortfolioAction.REDUCE, PortfolioAction.CLOSE} else Side.BUY
        )
        quantity = next(item.quantity for item in fixture.sizing.legs if item.quantity is not None)
        if receipt.status is ExecutionStatus.ACCEPTED:
            assert receipt.provider_order_id is not None
            open_orders = (
                OpenOrder(
                    order_reference=receipt.provider_order_id,
                    target_id=fixture.proposal.instrument_id,
                    venue=fixture.proposal.venue,
                    instrument_class=fixture.proposal.instrument_class,
                    side=order_side,
                    quantity=quantity,
                    status=OpenOrderStatus.WORKING,
                    submitted_at=AT + timedelta(seconds=2),
                ),
            )
        if receipt.filled_quantity:
            prior_quantity = next(
                (
                    item.quantity if item.side is Side.BUY else -item.quantity
                    for item in positions
                    if item.target_id == fixture.proposal.instrument_id
                ),
                Decimal(0),
            )
            signed_quantity = prior_quantity + (
                receipt.filled_quantity if order_side is Side.BUY else -receipt.filled_quantity
            )
            positions = tuple(
                item for item in positions if item.target_id != fixture.proposal.instrument_id
            )
            if signed_quantity:
                positions += (
                    AccountPosition(
                        target_id=fixture.proposal.instrument_id,
                        venue=fixture.proposal.venue,
                        instrument_class=fixture.proposal.instrument_class,
                        side=Side.BUY if signed_quantity > 0 else Side.SELL,
                        quantity=abs(signed_quantity),
                        concentration=None,
                        concentration_gap="recomputed_after_fill",
                    ),
                )
            assert receipt.provider_order_id is not None
            recent_fills = tuple(
                RecentFill(
                    fill_reference=fill_id,
                    order_reference=receipt.provider_order_id,
                    target_id=fixture.proposal.instrument_id,
                    venue=fixture.proposal.venue,
                    instrument_class=fixture.proposal.instrument_class,
                    side=order_side,
                    quantity=receipt.filled_quantity / len(receipt.fill_ids),
                    filled_at=receipt.observed_at,
                )
                for fill_id in receipt.fill_ids
            )
            cash_delta = receipt.filled_quantity * fixture.price.price
            cash = tuple(
                CashBalance(
                    currency=item.currency,
                    available=item.available
                    + (cash_delta if order_side is Side.SELL else -cash_delta),
                    settled=item.settled + (cash_delta if order_side is Side.SELL else -cash_delta),
                )
                if item.currency == fixture.mandate.currency
                else item
                for item in cash
            )
    account = capture_account_state_snapshot(
        provider=fixture.provider.manifest,
        account_reference="fixture-account",
        account_reference_key=b"fixture-account-key-material-32bytes",
        environment=TradingEnvironment.PAPER,
        as_of=AT + timedelta(seconds=3),
        reconciled_at=AT + timedelta(seconds=3),
        reconciliation_reference=snapshot.snapshot_id,
        cash=cash,
        positions=positions,
        open_orders=open_orders,
        recent_fills=recent_fills,
        recent_fills_since=original.recent_fills_since,
    )
    position_snapshot = account.project_positions(
        evaluated_at=AT + timedelta(seconds=3),
        max_age=timedelta(minutes=5),
    )
    view = AuthorizedDecisionView.build(
        cutoff=AT + timedelta(seconds=3),
        frozen_at=AT + timedelta(seconds=3),
        data_snapshot_ids=("data-rebuilt",),
        decision_input_ids=("input-rebuilt",),
        position_snapshot=position_snapshot,
    )
    marks = tuple(
        RawMarkedPositionV2(
            instrument_id=item.target_id,
            venue=item.venue,
            instrument_class=item.instrument_class,
            side=item.side,
            quantity=item.quantity,
            raw_price=Decimal("10"),
            raw_price_basis_hash="8" * 64,
        )
        for item in (account.positions or ())
    )
    exposure = PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=position_snapshot,
        raw_mark_set_hash="7" * 64,
        execution_ledger_snapshot_hash="6" * 64,
        reconciliation_ledger_snapshot_hash=canonical_hash(snapshot.to_dict()),
        currency="USD",
        marked_positions=marks,
        daily_turnover_used=fixture.exposure.daily_turnover_used,
        daily_submissions_used=fixture.exposure.daily_submissions_used,
        active_kill_reasons=(),
        observed_at=AT + timedelta(seconds=3),
        valid_until=AT + timedelta(minutes=5),
    )
    fixture.account_box[0] = account
    fixture.exposure_box[0] = exposure


def _set_equity_change(
    fixture: _Fixture,
    *,
    change: Decimal,
    observed_at: datetime = AT + timedelta(seconds=4),
    valid_until: datetime = AT + timedelta(minutes=5),
) -> None:
    original = fixture.account
    assert original.cash is not None
    account = capture_account_state_snapshot(
        provider=fixture.provider.manifest,
        account_reference="fixture-account",
        account_reference_key=b"fixture-account-key-material-32bytes",
        environment=TradingEnvironment.PAPER,
        as_of=observed_at,
        reconciled_at=observed_at,
        reconciliation_reference="risk-observation-" + canonical_hash(str(change)),
        cash=tuple(
            CashBalance(
                currency=item.currency,
                available=item.available + change,
                settled=item.settled + change,
            )
            for item in original.cash
        ),
        positions=original.positions,
        open_orders=original.open_orders,
        recent_fills=original.recent_fills,
        recent_fills_since=original.recent_fills_since,
    )
    position_snapshot = account.project_positions(
        evaluated_at=observed_at,
        max_age=timedelta(minutes=5),
    )
    view = AuthorizedDecisionView.build(
        cutoff=observed_at,
        frozen_at=observed_at,
        data_snapshot_ids=("risk-data-" + canonical_hash(str(change)),),
        decision_input_ids=("risk-input-" + canonical_hash(str(change)),),
        position_snapshot=position_snapshot,
    )
    marks = tuple(
        RawMarkedPositionV2(
            instrument_id=item.target_id,
            venue=item.venue,
            instrument_class=item.instrument_class,
            side=item.side,
            quantity=item.quantity,
            raw_price=Decimal("10"),
            raw_price_basis_hash="8" * 64,
        )
        for item in (account.positions or ())
    )
    exposure = PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=position_snapshot,
        raw_mark_set_hash=canonical_hash({"change": str(change), "kind": "marks"}),
        execution_ledger_snapshot_hash=canonical_hash({"change": str(change), "kind": "execution"}),
        reconciliation_ledger_snapshot_hash=canonical_hash(
            {"change": str(change), "kind": "reconciliation"}
        ),
        currency="USD",
        marked_positions=marks,
        daily_turnover_used=fixture.exposure.daily_turnover_used,
        daily_submissions_used=fixture.exposure.daily_submissions_used,
        active_kill_reasons=(),
        observed_at=observed_at,
        valid_until=valid_until,
    )
    fixture.account_box[0] = account
    fixture.exposure_box[0] = exposure


def test_autonomous_admission_dispatch_reconciliation_and_replay(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)

    first = _admit(service, fixture)
    replay = _admit(service, fixture)
    assert first == replay
    assert first[0].state is AutonomousOperationState.QUEUED
    assert service.dispatch_next().state is AutonomousOperationState.ACCEPTED  # type: ignore[union-attr]
    assert "incomplete_order_coverage" in service.active_kill_reasons
    missing_rebuild = service.reconcile()
    assert not missing_rebuild.complete
    assert "fresh_account_exposure_rebuild_required" in missing_rebuild.gaps
    _refresh_reconciliation_authorities(fixture)
    assert service.reconcile().complete
    assert service.get(first[0].client_order_id).state is AutonomousOperationState.RECONCILED

    service.close()
    restarted = _service(tmp_path, fixture)
    assert restarted.dispatch_next() is None
    assert restarted.get(first[0].client_order_id).state is AutonomousOperationState.RECONCILED


def test_unknown_ack_is_not_retried_and_resolves_only_by_reconciliation(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture", ambiguous=True)
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]

    assert service.dispatch_next().state is AutonomousOperationState.UNKNOWN  # type: ignore[union-attr]
    assert {"unknown_ack", "incomplete_order_coverage"} <= set(service.active_kill_reasons)
    assert service.dispatch_next() is None
    _refresh_reconciliation_authorities(fixture)
    incomplete = service.reconcile()
    assert not incomplete.complete
    assert "unknown_ack" in service.active_kill_reasons
    fixture.provider.make_terminal()
    _refresh_reconciliation_authorities(fixture)
    assert service.reconcile().complete
    assert service.get(operation.client_order_id).state is AutonomousOperationState.RECONCILED


def test_process_crash_recovers_submitting_lease_as_unknown_without_resubmit(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture", crash=True)
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]

    with pytest.raises(KeyboardInterrupt, match="simulated process crash"):
        service.dispatch_next()
    service.close()
    restarted = _service(tmp_path, fixture)
    assert restarted.get(operation.client_order_id).state is AutonomousOperationState.UNKNOWN
    assert restarted.dispatch_next() is None
    fixture.provider.make_terminal()
    _refresh_reconciliation_authorities(fixture)
    assert restarted.reconcile().complete
    assert restarted.get(operation.client_order_id).state is AutonomousOperationState.RECONCILED


def test_kill_blocks_increase_but_keeps_exact_cancel_and_reconcile_open(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    service.activate_kill("unknown_ack")

    cancellation = service.request_cancel(operation.client_order_id)
    assert service.dispatch_next_cancellation().cancellation_id == cancellation.cancellation_id  # type: ignore[union-attr]
    _refresh_reconciliation_authorities(fixture)
    assert service.reconcile().complete

    second = _fixture(tmp_path / "second")
    blocked_service = _service(tmp_path / "blocked", second)
    blocked = _admit(blocked_service, second)[0]
    blocked_service.activate_kill("unknown_ack")
    assert blocked_service.dispatch_next().state is AutonomousOperationState.BLOCKED  # type: ignore[union-attr]
    assert blocked_service.get(blocked.client_order_id).provider_order_id is None

    reduction = _fixture(tmp_path / "reduction-fixture", action=PortfolioAction.REDUCE)
    reduction_service = _service(tmp_path / "reduction", reduction)
    reduced = _admit(reduction_service, reduction)[0]
    reduction_service.activate_kill("unknown_ack")
    assert (
        reduction_service.dispatch_next().state  # type: ignore[union-attr]
        is AutonomousOperationState.ACCEPTED
    )
    assert reduction_service.get(reduced.client_order_id).provider_order_id is not None


def test_fresh_harness_root_cannot_mint_or_reopen_original_provider_lease(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    original = fixture.store
    provider_acceptance_id = _paper_provider_acceptance(original, fixture)
    capability_id = _record_accepted_provider_capability(
        original,
        provider_acceptance_id=provider_acceptance_id,
    )
    lease = _issue_autonomous_provider_lease(
        original,
        accepted_capability_id=capability_id,
        provider=fixture.provider,
        mandate=fixture.mandate,
        instrument_routes=ROUTES,
    )
    original_service = _open_service(original, fixture, lease.lease_id)
    original_service.close()

    fresh = LocalDataSnapshotStore(tmp_path / "fresh")
    fresh_authority = AutonomousPaperProviderLeaseAuthorityV2(fresh)
    assert fresh.harness_authority_id != original.harness_authority_id
    assert not hasattr(fresh_authority, "issue")
    with pytest.raises(PermissionError, match="same-root accepted Provider capability"):
        _issue_autonomous_provider_lease(
            fresh,
            accepted_capability_id=capability_id,
            provider=fixture.provider,
            mandate=fixture.mandate,
            instrument_routes=ROUTES,
        )
    fresh_acceptance_id = _paper_provider_acceptance(fresh, fixture)
    fresh_capability_id = _record_accepted_provider_capability(
        fresh,
        provider_acceptance_id=fresh_acceptance_id,
    )
    with pytest.raises(PermissionError, match="same-root accepted Provider capability"):
        _issue_autonomous_provider_lease(
            fresh,
            accepted_capability_id=fresh_capability_id,
            provider=fixture.provider,
            mandate=fixture.mandate,
            instrument_routes=ROUTES,
        )
    with pytest.raises(KeyError, match="unknown autonomous Paper provider lease"):
        fresh_authority.resolve(lease.lease_id)
    with pytest.raises(PermissionError, match="another Harness authority root"):
        _open_service(fresh, fixture, lease.lease_id)


def test_provider_lease_cannot_reopen_with_more_permissive_mandate(tmp_path: Path) -> None:
    fixture = _fixture(
        tmp_path / "fixture",
        maximum_single_position_fraction=Decimal("0.5"),
    )
    provider_acceptance_id = _paper_provider_acceptance(fixture.store, fixture)
    capability_id = _record_accepted_provider_capability(
        fixture.store,
        provider_acceptance_id=provider_acceptance_id,
    )
    lease = _issue_autonomous_provider_lease(
        fixture.store,
        accepted_capability_id=capability_id,
        provider=fixture.provider,
        mandate=fixture.mandate,
        instrument_routes=ROUTES,
    )
    assert lease.mandate_hash == canonical_hash(fixture.mandate.to_dict())
    original = _open_service(fixture.store, fixture, lease.lease_id)
    original.close()

    permissive = replace(
        fixture,
        mandate=replace(
            fixture.mandate,
            maximum_single_position_fraction=Decimal("1"),
        ),
    )
    with pytest.raises(PermissionError, match="exact Provider, account, and mandate"):
        _open_service(fixture.store, permissive, lease.lease_id)


def test_legacy_provider_lease_without_mandate_hash_fails_closed(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture")
    provider_acceptance_id = _paper_provider_acceptance(fixture.store, fixture)
    capability_id = _record_accepted_provider_capability(
        fixture.store,
        provider_acceptance_id=provider_acceptance_id,
    )
    lease = _issue_autonomous_provider_lease(
        fixture.store,
        accepted_capability_id=capability_id,
        provider=fixture.provider,
        mandate=fixture.mandate,
        instrument_routes=ROUTES,
    )
    legacy_core = lease.core_dict()
    del legacy_core["mandate_hash"]
    legacy_id = "autonomous-paper-provider-lease-" + canonical_hash(legacy_core)
    legacy_payload = json.dumps(
        {"lease_id": legacy_id, **legacy_core},
        sort_keys=True,
        separators=(",", ":"),
    )
    with fixture.store.authority_transaction() as connection:
        connection.execute(
            """
            INSERT INTO autonomous_provider_acceptances (
                lease_id, harness_authority_id, payload_json, provider_manifest_hash,
                account_state_hash, mandate_hash
            )
            SELECT ?, harness_authority_id, ?, provider_manifest_hash,
                   account_state_hash, mandate_hash
            FROM autonomous_provider_acceptances WHERE lease_id = ?
            """,
            (legacy_id, legacy_payload, lease.lease_id),
        )
    authority = AutonomousPaperProviderLeaseAuthorityV2(fixture.store)
    with pytest.raises(KeyError, match="mandate_hash"):
        authority.resolve(legacy_id)
    with pytest.raises(PermissionError, match="lacks durable Harness authority"):
        _open_service(fixture.store, fixture, legacy_id)


def test_stale_or_changed_authoritative_account_blocks_before_provider_mutation(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    fixture.account_box[0] = capture_account_state_snapshot(
        provider=fixture.provider.manifest,
        account_reference="fixture-account",
        account_reference_key=b"fixture-account-key-material-32bytes",
        environment=TradingEnvironment.PAPER,
        as_of=AT - timedelta(minutes=10),
        reconciled_at=AT - timedelta(minutes=10),
        reconciliation_reference="stale-reconciliation",
        cash=(
            CashBalance(
                currency="USD",
                available=Decimal("20000"),
                settled=Decimal("20000"),
            ),
        ),
        positions=(),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=AT - timedelta(days=1),
    )

    assert service.dispatch_next().state is AutonomousOperationState.BLOCKED  # type: ignore[union-attr]
    assert fixture.provider.receipts == {}
    assert "stale_risk_measurement" in service.active_kill_reasons
    assert service.get(operation.client_order_id).provider_order_id is None


def test_dispatch_rechecks_mandate_and_order_expiry_before_provider_mutation(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    fixture.clock_box[0] = fixture.mandate.valid_until

    assert service.dispatch_next().state is AutonomousOperationState.BLOCKED  # type: ignore[union-attr]
    assert fixture.provider.receipts == {}
    assert service.get(operation.client_order_id).provider_order_id is None

    order_fixture = _fixture(tmp_path / "order-fixture", signal_id="order-expiry")
    order_service = _service(tmp_path / "order", order_fixture)
    order_operation = _admit(order_service, order_fixture)[0]
    order_fixture.clock_box[0] = AT + timedelta(minutes=5)
    assert order_service.dispatch_next().state is AutonomousOperationState.BLOCKED  # type: ignore[union-attr]
    assert order_fixture.provider.receipts == {}
    assert order_service.get(order_operation.client_order_id).provider_order_id is None


def test_exposure_generation_and_reserved_gross_budget_cannot_be_reused(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture", ratio=Decimal("0.40"))
    service = _service(tmp_path, fixture)
    _admit(service, fixture)
    replay = _fixture(
        tmp_path / "replay",
        store=fixture.store,
        ratio=Decimal("0.70"),
        signal_id="signal-second",
    )

    with pytest.raises(PermissionError, match="already consumed"):
        _admit(service, replay)

    fresh = _fixture(
        tmp_path / "fresh",
        store=fixture.store,
        instrument=SECOND_TARGET,
        ratio=Decimal("0.70"),
        signal_id="signal-second-instrument",
        exposure_nonce="1",
    )
    fixture.exposure_box[0] = fresh.exposure
    with pytest.raises(PermissionError, match="gross exposure budget"):
        _admit(service, fresh)


def test_measured_loss_and_drawdown_are_required_for_risk_limit_kills(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    _admit(service, fixture)
    with pytest.raises(ValueError, match="measured P&L evidence"):
        service.activate_kill("daily_loss_threshold_exceeded")

    _set_equity_change(fixture, change=Decimal("-301"))
    fixture.clock_box[0] = AT + timedelta(seconds=4)
    assert service.dispatch_next().state is AutonomousOperationState.BLOCKED  # type: ignore[union-attr]
    assert "daily_loss_threshold_exceeded" in service.active_kill_reasons
    assert fixture.provider.receipts == {}

    drawdown = _fixture(tmp_path / "drawdown-fixture", signal_id="drawdown-baseline-reset")
    baseline = _service(tmp_path / "drawdown", drawdown)
    baseline.close()
    _set_equity_change(drawdown, change=Decimal("1001"))
    drawdown.clock_box[0] = AT + timedelta(seconds=4)
    peak = _service(tmp_path / "drawdown", drawdown)
    peak.close()
    replay = _service(tmp_path / "drawdown", drawdown)
    replay.close()
    _set_equity_change(drawdown, change=Decimal("-1"))
    drawdown.clock_box[0] = AT + timedelta(seconds=4)
    restarted = _service(tmp_path / "drawdown", drawdown)
    assert "strategy_peak_drawdown_threshold_exceeded" in restarted.active_kill_reasons


def test_unknown_cancel_requires_terminal_reconciliation_and_malformed_receipt_fails_closed(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture", cancel_ambiguous=True)
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    _refresh_reconciliation_authorities(fixture)
    assert service.reconcile().complete

    cancellation = service.request_cancel(operation.client_order_id)
    assert (
        service.dispatch_next_cancellation().state  # type: ignore[union-attr]
        is AutonomousCancellationState.UNKNOWN
    )
    _refresh_reconciliation_authorities(fixture)
    incomplete = service.reconcile()
    assert not incomplete.complete
    assert "unknown_ack" in service.active_kill_reasons
    assert service.get_cancellation(cancellation.cancellation_id).state is (
        AutonomousCancellationState.UNKNOWN
    )

    fixture.provider.make_canceled()
    _refresh_reconciliation_authorities(fixture)
    assert service.reconcile().complete
    assert "unknown_ack" not in service.active_kill_reasons

    malformed = _fixture(tmp_path / "malformed-fixture", malformed_cancel=True)
    malformed_service = _service(tmp_path / "malformed", malformed)
    malformed_operation = _admit(malformed_service, malformed)[0]
    malformed_service.dispatch_next()
    malformed_cancel = malformed_service.request_cancel(malformed_operation.client_order_id)
    assert malformed_service.dispatch_next_cancellation().state is (  # type: ignore[union-attr]
        AutonomousCancellationState.UNKNOWN
    )
    assert "unknown_ack" in malformed_service.active_kill_reasons
    assert malformed_service.get_cancellation(malformed_cancel.cancellation_id).state is (
        AutonomousCancellationState.UNKNOWN
    )


def test_process_crash_recovers_cancellation_claim_as_unknown_without_retry(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture", cancel_crash=True)
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    cancellation = service.request_cancel(operation.client_order_id)

    with pytest.raises(KeyboardInterrupt, match="after Provider cancellation"):
        service.dispatch_next_cancellation()
    cancel_calls = fixture.provider.cancel_calls
    service.close()
    restarted = _service(tmp_path, fixture)
    assert restarted.get_cancellation(cancellation.cancellation_id).state is (
        AutonomousCancellationState.UNKNOWN
    )
    assert {"unknown_ack", "incomplete_order_coverage"} <= set(restarted.active_kill_reasons)
    assert restarted.dispatch_next_cancellation() is None
    assert fixture.provider.cancel_calls == cancel_calls
    with restarted.store.authority_transaction() as connection:
        lease_row = connection.execute(
            """
            SELECT active_mutation_id FROM autonomous_provider_acceptances
            WHERE lease_id = ?
            """,
            (restarted.provider_lease.lease_id,),
        ).fetchone()
    assert lease_row is not None
    assert lease_row["active_mutation_id"] is None


def test_exact_snapshot_link_with_copied_prefill_state_cannot_release_reservation(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    fixture.provider.make_terminal()

    _refresh_reconciliation_authorities(fixture, reflect_provider=False)
    copied = service.reconcile()
    assert not copied.complete
    assert "rebuilt_state_does_not_reflect_provider_snapshot" in copied.gaps
    assert service.get(operation.client_order_id).state is AutonomousOperationState.ACCEPTED

    _refresh_reconciliation_authorities(fixture)
    assert service.reconcile().complete


def test_stale_risk_kills_increase_but_cancel_reconcile_and_reduction_continue(
    tmp_path: Path,
) -> None:
    increase = _fixture(tmp_path / "increase-fixture")
    increase_service = _service(tmp_path / "increase", increase)
    _admit(increase_service, increase)
    increase.clock_box[0] = AT + timedelta(seconds=32)
    assert increase_service.dispatch_next().state is AutonomousOperationState.BLOCKED  # type: ignore[union-attr]
    assert "stale_risk_measurement" in increase_service.active_kill_reasons
    assert increase.provider.receipts == {}

    reduction = _fixture(tmp_path / "reduction-fixture", action=PortfolioAction.REDUCE)
    reduction_service = _service(tmp_path / "reduction", reduction)
    _admit(reduction_service, reduction)
    reduction.clock_box[0] = AT + timedelta(seconds=32)
    assert reduction_service.dispatch_next().state is AutonomousOperationState.ACCEPTED  # type: ignore[union-attr]
    assert "stale_risk_measurement" in reduction_service.active_kill_reasons

    cancel = _fixture(tmp_path / "cancel-fixture")
    cancel_service = _service(tmp_path / "cancel", cancel)
    cancel_operation = _admit(cancel_service, cancel)[0]
    cancel_service.dispatch_next()
    cancel.clock_box[0] = AT + timedelta(seconds=32)
    cancellation = cancel_service.request_cancel(cancel_operation.client_order_id)
    assert cancel_service.dispatch_next_cancellation().cancellation_id == (  # type: ignore[union-attr]
        cancellation.cancellation_id
    )
    _refresh_reconciliation_authorities(cancel)
    assert cancel_service.reconcile().complete


def test_revoked_canonical_lease_blocks_cancel_and_reconcile_provider_calls(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    cancellation = service.request_cancel(operation.client_order_id)
    assert service.provider_lease_authority.request_revocation(
        service.provider_lease.lease_id,
        requested_at=fixture.clock_box[0],
    )

    cancel_calls = fixture.provider.cancel_calls
    blocked = service.dispatch_next_cancellation()
    assert blocked is not None
    assert blocked.cancellation_id == cancellation.cancellation_id
    assert blocked.state is AutonomousCancellationState.QUEUED
    assert fixture.provider.cancel_calls == cancel_calls
    assert "provider_loss" in service.active_kill_reasons

    reconcile_calls = fixture.provider.reconcile_calls
    result = service.reconcile()
    assert not result.complete
    assert result.gaps == ("provider_lease_unavailable",)
    assert fixture.provider.reconcile_calls == reconcile_calls


def test_provider_runtime_reconciliation_failure_is_durably_fail_closed(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)

    def fail_reconciliation() -> None:
        raise RuntimeError("Nautilus TradingNode stopped unexpectedly")

    fixture.provider.reconcile_hook = fail_reconciliation

    result = service.reconcile()

    assert not result.complete
    assert result.gaps == ("provider_reconciliation_failed",)
    assert result.active_kill_reasons == (
        "incomplete_order_coverage",
        "provider_loss",
    )
    assert service.active_kill_reasons == result.active_kill_reasons
    payload_value = service.artifacts.read_json(result.reconciliation_hash)
    assert isinstance(payload_value, dict)
    payload = cast(dict[str, object], payload_value)
    assert payload["provider_snapshot"] is None
    assert payload["gaps"] == ["provider_reconciliation_failed"]
    with fixture.store.authority_transaction() as connection:
        row = connection.execute(
            """
            SELECT complete, payload_hash FROM autonomous_reconciliations
            WHERE reconciliation_hash = ?
            """,
            (result.reconciliation_hash,),
        ).fetchone()
    assert row is not None
    assert row["complete"] == 0
    assert row["payload_hash"] == canonical_hash(payload)


def test_provider_storage_corruption_is_not_reclassified_as_operational_loss(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)

    def corrupt_provider_storage() -> None:
        raise sqlite3.DatabaseError("provider state database is malformed")

    fixture.provider.reconcile_hook = corrupt_provider_storage

    with pytest.raises(sqlite3.DatabaseError, match="database is malformed"):
        service.reconcile()

    assert service.active_kill_reasons == ()
    with fixture.store.authority_transaction() as connection:
        reconciliation_count = connection.execute(
            "SELECT COUNT(*) FROM autonomous_reconciliations"
        ).fetchone()[0]
    assert reconciliation_count == 0


def test_provider_lease_authority_corruption_is_not_reclassified_as_provider_loss(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    with fixture.store.authority_transaction() as connection:
        connection.execute(
            """
            UPDATE autonomous_provider_acceptances SET payload_json = ?
            WHERE lease_id = ?
            """,
            ("{", service.provider_lease.lease_id),
        )

    with pytest.raises(json.JSONDecodeError):
        service.reconcile()

    assert service.active_kill_reasons == ()
    with fixture.store.authority_transaction() as connection:
        reconciliation_count = connection.execute(
            "SELECT COUNT(*) FROM autonomous_reconciliations"
        ).fetchone()[0]
    assert reconciliation_count == 0


def test_reconciliation_call_and_lease_revocation_are_linearized(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    provider_entered = Event()
    release_provider = Event()
    revoked = Event()
    errors: list[BaseException] = []
    revocation_result: list[bool] = []

    def block_provider() -> None:
        provider_entered.set()
        if not release_provider.wait(2):
            raise TimeoutError("test did not release Provider reconciliation")

    def reconcile() -> None:
        try:
            service.reconcile()
        except BaseException as error:
            errors.append(error)

    def revoke() -> None:
        try:
            revocation_result.append(
                service.provider_lease_authority.request_revocation(
                    service.provider_lease.lease_id,
                    requested_at=fixture.clock_box[0],
                )
            )
            revoked.set()
        except BaseException as error:
            errors.append(error)

    fixture.provider.reconcile_hook = block_provider
    reconcile_thread = Thread(target=reconcile)
    reconcile_thread.start()
    assert provider_entered.wait(1)
    revoke_thread = Thread(target=revoke)
    revoke_thread.start()
    assert not revoked.wait(0.1)
    release_provider.set()
    reconcile_thread.join(2)
    revoke_thread.join(2)
    assert not reconcile_thread.is_alive()
    assert not revoke_thread.is_alive()
    assert revoked.is_set()
    assert errors == []
    assert revocation_result == [True]


def test_cancellation_call_and_lease_revocation_are_linearized(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    cancellation = service.request_cancel(operation.client_order_id)
    validator_passed = Event()
    release_cancel = Event()
    cancel_finished = Event()
    errors: list[BaseException] = []
    result: list[AutonomousCancellationState] = []

    def block_after_validator() -> None:
        validator_passed.set()
        if not release_cancel.wait(10):
            raise TimeoutError("test did not release Provider cancellation")

    def cancel() -> None:
        try:
            dispatched = service.dispatch_next_cancellation()
            assert dispatched is not None
            result.append(dispatched.state)
        except BaseException as error:
            errors.append(error)
        finally:
            cancel_finished.set()

    fixture.provider.cancel_hook = block_after_validator
    cancel_thread = Thread(target=cancel)
    cancel_thread.start()
    assert validator_passed.wait(1)
    with pytest.raises(RuntimeError, match="another autonomous Paper service"):
        _open_service(
            fixture.store,
            fixture,
            service.provider_lease.lease_id,
        )
    with pytest.raises(RuntimeError, match="during Provider mutation"):
        service.close()
    assert not service.provider_lease_authority.request_revocation(
        service.provider_lease.lease_id,
        requested_at=fixture.clock_box[0],
    )
    with service.store.authority_transaction() as connection:
        lease_row = connection.execute(
            """
            SELECT active_mutation_id, revoke_requested, revoked_at
            FROM autonomous_provider_acceptances WHERE lease_id = ?
            """,
            (service.provider_lease.lease_id,),
        ).fetchone()
        assert lease_row is not None
        assert lease_row["active_mutation_id"] is not None
        assert bool(lease_row["revoke_requested"])
        assert lease_row["revoked_at"] is None
        with pytest.raises(sqlite3.IntegrityError, match="active provider mutation claim"):
            connection.execute(
                "DELETE FROM autonomous_provider_acceptances WHERE lease_id = ?",
                (service.provider_lease.lease_id,),
            )
    assert not cancel_finished.wait(5.1)
    release_cancel.set()
    cancel_thread.join(2)
    assert not cancel_thread.is_alive()
    assert errors == []
    assert result == [AutonomousCancellationState.ACKNOWLEDGED]
    assert service.get_cancellation(cancellation.cancellation_id).state is (
        AutonomousCancellationState.ACKNOWLEDGED
    )
    with service.store.authority_transaction() as connection:
        lease_row = connection.execute(
            """
            SELECT active_mutation_id, revoke_requested, revoked_at
            FROM autonomous_provider_acceptances WHERE lease_id = ?
            """,
            (service.provider_lease.lease_id,),
        ).fetchone()
    assert lease_row is not None
    assert lease_row["active_mutation_id"] is None
    assert bool(lease_row["revoke_requested"])
    assert lease_row["revoked_at"] is not None
    with pytest.raises(KeyError, match="unknown autonomous Paper provider lease"):
        service.provider_lease_authority.resolve(service.provider_lease.lease_id)


def test_forked_child_cannot_use_or_unlock_parent_service(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture")
    service = _service(tmp_path, fixture)
    operation = _admit(service, fixture)[0]
    service.dispatch_next()
    service.request_cancel(operation.client_order_id)
    provider_entered = Event()
    release_provider = Event()
    errors: list[BaseException] = []

    def block_provider() -> None:
        provider_entered.set()
        if not release_provider.wait(5):
            raise TimeoutError("test did not release Provider cancellation")

    def cancel() -> None:
        try:
            service.dispatch_next_cancellation()
        except BaseException as error:
            errors.append(error)

    fixture.provider.cancel_hook = block_provider
    cancel_thread = Thread(target=cancel)
    cancel_thread.start()
    assert provider_entered.wait(1)

    context = get_context("fork")
    receive_connection, send_connection = context.Pipe(duplex=False)

    def child_attempt() -> None:
        outcomes: list[str] = []
        try:
            service.request_cancel(operation.client_order_id)
        except RuntimeError as error:
            outcomes.append(str(error))
        service.close()
        service.__del__()
        send_connection.send(tuple(outcomes))
        send_connection.close()

    child = context.Process(target=child_attempt)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="This process .* is multi-threaded",
            category=DeprecationWarning,
        )
        child.start()
    send_connection.close()
    child.join(2)
    assert not child.is_alive()
    assert child.exitcode == 0
    assert receive_connection.poll(1)
    assert receive_connection.recv() == ("autonomous Paper service belongs to another process",)
    receive_connection.close()

    with pytest.raises(RuntimeError, match="another autonomous Paper service"):
        _open_service(fixture.store, fixture, service.provider_lease.lease_id)
    with service.store.authority_transaction() as connection:
        row = connection.execute(
            """
            SELECT active_mutation_id FROM autonomous_provider_acceptances
            WHERE lease_id = ?
            """,
            (service.provider_lease.lease_id,),
        ).fetchone()
    assert row is not None
    assert row["active_mutation_id"] is not None

    release_provider.set()
    cancel_thread.join(2)
    assert not cancel_thread.is_alive()
    assert errors == []
    service.close()
    successor = _open_service(fixture.store, fixture, service.provider_lease.lease_id)
    successor.close()
