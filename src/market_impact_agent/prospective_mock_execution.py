"""Existing execution-owner composition for approved local CNY Mock reviews.

Dispatch records an acknowledgement only. A later explicit source-qualified Mock
fill and exact provider reconciliation remain separate lifecycle transitions.
"""

from __future__ import annotations

from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.autonomous_paper import (
    AutonomousPaperExecutionServiceV2,
    AutonomousReconciliationAuthorityV2,
    _issue_autonomous_provider_lease,  # pyright: ignore[reportPrivateUsage]
    _record_accepted_provider_capability,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.data_inputs import FrozenDataSnapshotInput
from market_impact_agent.domain import (
    ApprovalMode,
    TradingEnvironment,
    TradingMandate,
    TradingMandateV3,
)
from market_impact_agent.paper_execution import PaperExecutionService
from market_impact_agent.portfolio_decision import (
    PortfolioExposureViewV2,
    RegisteredPortfolioExposureViewAuthorityV2,
)
from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition
from market_impact_agent.providers import MockExecutionProvider, ReconciliationSnapshot
from market_impact_agent.runtime_store import RunJournal


def open_prospective_mock_execution(
    composition: ProspectiveMockComposition,
    *,
    reconciliation_input: FrozenDataSnapshotInput | None = None,
) -> AutonomousPaperExecutionServiceV2:
    """Bind the existing lease, risk and reconciliation authorities to this review."""
    initial, portfolio = composition.inputs, composition.portfolio
    if initial is None or portfolio is None:
        raise PermissionError("local Mock execution requires an actual portfolio composition")
    mandate = initial.mandate
    if (
        type(composition.provider) is not MockExecutionProvider
        or not isinstance(mandate, TradingMandateV3)
        or mandate.currency != "CNY"
        or mandate.execution_scope != "local_mock"
        or mandate.approval_mode is not ApprovalMode.AUTONOMOUS
    ):
        raise PermissionError("this composition accepts only the authorized autonomous CNY Mock")
    provider_path = (
        composition.store.root / "prospective-mock" / composition.seed / "account.sqlite3"
    )
    # Provider validators are deliberately bind-once. The acceptance owner and the
    # autonomous owner therefore get distinct adapters over the same durable facts.
    composition.provider = MockExecutionProvider(provider_path, clock=composition.clock)
    acceptance_provider = MockExecutionProvider(provider_path, clock=composition.clock)
    # Reconstruct the old snapshot against today's durable provider facts. A new
    # receipt/fill cannot be concealed behind the account injected into the model.
    receipts = composition.provider.reconcile()
    original_receipts = ReconciliationSnapshot.build(
        provider_id=receipts.provider_id,
        observed_at=initial.account_state.as_of,
        complete=receipts.complete,
        receipts=receipts.receipts,
        gaps=receipts.gaps,
    )
    current = [initial]
    refresh_gaps: list[str] = []
    try:
        if original_receipts.snapshot_id == initial.account_state.reconciliation_reference:
            rebuilt = composition.provider.simulated_account_snapshot(
                price_bases=initial.price_bases, reconciliation_snapshot=original_receipts
            )
            if rebuilt != initial.account_state:
                raise PermissionError("durable Mock facts differ from the frozen account")
        else:
            current[0] = composition.refresh_execution_inputs(
                reconciliation_snapshot=receipts, frozen=reconciliation_input
            )
    except PermissionError as exc:
        refresh_gaps.append(str(exc))

    def account_source():
        if refresh_gaps:
            raise PermissionError("current_mock_account_unavailable:" + ",".join(refresh_gaps))
        return current[0].account_state

    views = {initial.exposure_view.exposure_view_id: initial.exposure_view}
    views[current[0].exposure_view.exposure_view_id] = current[0].exposure_view

    class ExposureAuthority:
        def assert_authoritative_exposure_view(self, view: PortfolioExposureViewV2) -> None:
            RegisteredPortfolioExposureViewAuthorityV2(views).assert_authoritative_exposure_view(
                view
            )

    exposure_authority = ExposureAuthority()

    def reconcile(snapshot: ReconciliationSnapshot):
        fresh = composition.refresh_execution_inputs(
            reconciliation_snapshot=snapshot, frozen=reconciliation_input
        )
        refresh_gaps.clear()
        current[0] = fresh
        views[fresh.exposure_view.exposure_view_id] = fresh.exposure_view
        return fresh.account_state, fresh.exposure_view

    routes = {
        symbol: {"provider_instrument_id": symbol, "market": "SYNTHETIC"}
        for symbol in mandate.allowed_instruments
    }
    owner = PaperExecutionService(
        composition.store.root / "prospective-mock" / composition.seed / "provider-acceptance",
        provider=acceptance_provider,
        mandate=TradingMandate(
            "local-mock-provider-acceptance",
            mandate.account_id,
            TradingEnvironment.PAPER,
            ApprovalMode.MANUAL_EACH,
            mandate.valid_from,
            mandate.valid_until,
            mandate.allowed_instruments,
            mandate.allowed_sides,
            mandate.gross_exposure_limit,
        ),
        price_source=lambda order: current[0].price_bases.get(order.instrument_id),
        clock=composition.clock,
        account_state_source=account_source,
    )
    journal = RunJournal.authoritative(composition.store)
    lease_event_id = "prospective.mock.execution.lease." + canonical_hash(mandate.to_dict())
    existing_lease = journal.event(lease_event_id)
    if existing_lease is None:
        acceptance = owner.record_provider_acceptance(composition.store)
        capability = _record_accepted_provider_capability(
            composition.store, provider_acceptance_id=acceptance
        )
        lease = _issue_autonomous_provider_lease(
            composition.store,
            accepted_capability_id=capability,
            provider=composition.provider,
            mandate=mandate,
            instrument_routes=routes,
        )
        lease_id = lease.lease_id
        journal.append(
            run_id=composition.parent_run_id,
            event_id=lease_event_id,
            event_type="prospective.mock.execution.lease.bound",
            observed_at=composition.clock(),
            payload={"lease_id": lease_id},
        )
    else:
        lease_id = str(existing_lease.payload["lease_id"])
    return AutonomousPaperExecutionServiceV2(
        composition.store,
        provider=composition.provider,
        provider_lease_id=lease_id,
        mandate=mandate,
        account_state_source=account_source,
        exposure_view_source=lambda: current[0].exposure_view,
        exposure_view_authority=exposure_authority,
        price_basis_source=lambda symbol: current[0].price_bases.get(symbol),
        reconciliation_authority=AutonomousReconciliationAuthorityV2(reconcile),
        instrument_routes_hash=canonical_hash(routes),
        instrument_routes=routes,
        clock=composition.clock,
        portfolio_review_authority=portfolio,
    )


def dispatch_prospective_mock_review(
    composition: ProspectiveMockComposition, run_id: str
) -> dict[str, object]:
    """Dispatch one real review; report ACK separately from any later fill."""
    from market_impact_agent.autonomous_paper import AutonomousOperationState

    if composition.portfolio is None:
        raise PermissionError("local Mock dispatch requires a completed portfolio authority")
    terminal = composition.portfolio.replay(run_id)
    proposal = terminal.get("parsed_proposal")
    if terminal.get("status") != "completed":
        return {"execution_status": "incomplete_portfolio", "execution_dispatched": False}
    if (
        isinstance(proposal, dict)
        and cast(dict[str, object], proposal).get("requested_action") == "hold"
    ):
        return {"execution_status": "completed_hold", "execution_dispatched": False}
    service = None
    try:
        service = open_prospective_mock_execution(composition)
        operation = service.get_portfolio_review_operation(run_id)
        if operation is None:
            operation = service.admit_portfolio_review(run_id)
        if operation.state is AutonomousOperationState.QUEUED:
            dispatched = service.dispatch_next()
            if dispatched is not None:
                if dispatched.operation_id != operation.operation_id:
                    raise PermissionError("dispatch returned a different portfolio operation")
                operation = dispatched
        return {
            "execution_status": operation.state.value,
            "execution_dispatched": operation.state
            in {
                AutonomousOperationState.ACCEPTED,
                AutonomousOperationState.UNKNOWN,
                AutonomousOperationState.RECONCILED,
            },
            "operation_id": operation.operation_id,
            "client_order_id": operation.client_order_id,
            "fill_acceptance": "pending_source_qualified_fill_and_reconciliation",
            "execution_gaps": list(service.active_kill_reasons),
        }
    except PermissionError as exc:
        return {
            "execution_status": "incomplete_execution_authority",
            "execution_dispatched": False,
            "execution_gaps": [str(exc)],
        }
    finally:
        if service is not None:
            service.close()


def reconcile_prospective_mock_review(
    composition: ProspectiveMockComposition,
    run_id: str,
    frozen: FrozenDataSnapshotInput,
) -> dict[str, object]:
    """Consume a later frozen source input under the original order authority."""
    from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
    from market_impact_agent.prospective_mock_fills import record_prospective_mock_fill

    market = composition.market_factory(frozen)
    if not isinstance(market, ExecutableProspectiveAShareInputs):
        raise PermissionError("current Mock fills require executable prospective sources")
    service = open_prospective_mock_execution(composition, reconciliation_input=frozen)
    try:
        operation = service.get_portfolio_review_operation(run_id)
        if operation is None:
            return {"fill_status": "pending_order", "reconciliation_complete": False}
        fill = record_prospective_mock_fill(composition.provider, market, operation.client_order_id)
        reconciliation = service.reconcile()
        return {
            "client_order_id": operation.client_order_id,
            "fill_status": "filled" if fill.receipt is not None else "pending_source_fill",
            "fill_gaps": list(fill.gaps),
            "fill_evidence_artifact_hash": fill.evidence_artifact_hash,
            "reconciliation_complete": reconciliation.complete,
            "reconciliation_hash": reconciliation.reconciliation_hash,
            "reconciliation_gaps": list(reconciliation.gaps),
        }
    finally:
        service.close()
