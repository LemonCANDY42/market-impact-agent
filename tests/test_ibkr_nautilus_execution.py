# pyright: reportPrivateUsage=false
from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionReceipt,
    ExecutionStatus,
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.ibkr_nautilus_execution import (
    IbkrNautilusInstrumentRoute,
    IbkrNautilusPaperExecutionProvider,
    IbkrNautilusPaperProviderAcceptance,
    NautilusPaperCancelCommand,
    NautilusPaperOrderObservation,
    NautilusPaperRuntimeSnapshot,
    NautilusPaperRuntimeStatus,
    NautilusPaperSubmitCommand,
    hash_ibkr_nautilus_instrument_routes,
)
from market_impact_agent.ibkr_nautilus_paper import (
    IBKR_NAUTILUS_PAPER_PROVIDER_ID,
    IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
)
from market_impact_agent.paper_execution import PaperExecutionService, PriceBasis
from market_impact_agent.providers import (
    CancellationCapabilityRejected,
    Capability,
    ReconciliationSnapshot,
    SubmissionCapabilityRejected,
    _issue_cancellation_capability,
    _issue_submission_capability,
)

NOW = datetime(2026, 9, 1, 8, tzinfo=UTC)
SCENARIOS = tuple(
    sorted(
        {
            "account_reconciliation",
            "ambiguous_acknowledgement",
            "cancel",
            "disconnect",
            "duplicate_fill",
            "external_order",
            "gateway_restart",
            "partial_fill",
            "process_restart",
            "replace",
            "submit",
        }
    )
)
ROUTES = {
    "AAPL.XNAS": IbkrNautilusInstrumentRoute(
        nautilus_instrument_id="AAPL.NASDAQ",
        market="US",
    )
}
ROUTES_HASH = hash_ibkr_nautilus_instrument_routes(ROUTES)


class _Runtime:
    def __init__(
        self,
        *,
        configuration_hash: str = "1" * 64,
        account_reference_hash: str = "account-ref-" + "2" * 64,
    ) -> None:
        self.configuration_hash = configuration_hash
        self.account_reference_hash = account_reference_hash
        self.submit_calls: list[NautilusPaperSubmitCommand] = []
        self.cancel_calls: list[NautilusPaperCancelCommand] = []
        self.submit_error: BaseException | None = None
        self.cancel_error: BaseException | None = None
        self.snapshot = NautilusPaperRuntimeSnapshot(
            observed_at=NOW,
            connected=True,
            reconciled=True,
            complete=True,
            orders=(),
        )

    def submit(self, command: NautilusPaperSubmitCommand) -> NautilusPaperOrderObservation:
        self.submit_calls.append(command)
        if self.submit_error is not None:
            raise self.submit_error
        return NautilusPaperOrderObservation(
            nautilus_client_order_id=command.nautilus_client_order_id,
            provider_order_id="IB-42",
            status=NautilusPaperRuntimeStatus.ACCEPTED,
            observed_at=NOW,
        )

    def cancel(self, command: NautilusPaperCancelCommand) -> NautilusPaperOrderObservation:
        self.cancel_calls.append(command)
        if self.cancel_error is not None:
            raise self.cancel_error
        return NautilusPaperOrderObservation(
            nautilus_client_order_id=command.nautilus_client_order_id,
            provider_order_id=command.provider_order_id,
            status=NautilusPaperRuntimeStatus.PENDING_CANCEL,
            observed_at=NOW,
        )

    def reconcile(self) -> NautilusPaperRuntimeSnapshot:
        return self.snapshot


def _acceptance(
    *,
    complete: bool = True,
    gaps: tuple[str, ...] = (),
    configuration_hash: str = "1" * 64,
    account_reference_hash: str = "account-ref-" + "2" * 64,
    instrument_routes_hash: str = ROUTES_HASH,
    markets: tuple[str, ...] = ("HK", "US"),
    order_types: tuple[str, ...] = ("limit", "market"),
    valid_until: datetime = NOW + timedelta(days=1),
) -> IbkrNautilusPaperProviderAcceptance:
    return IbkrNautilusPaperProviderAcceptance.build(
        configuration_hash=configuration_hash,
        account_reference_hash=account_reference_hash,
        instrument_routes_hash=instrument_routes_hash,
        markets=markets,
        order_types=order_types,
        accepted_scenarios=SCENARIOS,
        evidence_hashes=("3" * 64,),
        accepted_at=NOW - timedelta(minutes=1),
        valid_until=valid_until,
        complete=complete,
        gaps=gaps,
    )


def _order() -> OrderIntent:
    return OrderIntent(
        client_order_id="harness-order-1",
        signal_id="signal-1",
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        instrument_id="AAPL.XNAS",
        side=Side.BUY,
        quantity=Decimal("2"),
        order_kind=OrderKind.LIMIT,
        limit_price=Decimal("100"),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def _submission(order: OrderIntent | None = None):  # type: ignore[no-untyped-def]
    selected = order or _order()
    return _issue_submission_capability(
        order=selected,
        submission_id="submission-1",
        provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
        order_hash=canonical_hash(selected.to_dict()),
        mandate_hash="4" * 64,
        price_basis_hash="5" * 64,
        policy_evaluation_hash="6" * 64,
        approval_hash="7" * 64,
    )


def _cancellation():  # type: ignore[no-untyped-def]
    return _issue_cancellation_capability(
        client_order_id="harness-order-1",
        provider_order_id="IB-42",
        cancellation_id="cancel-1",
        attempt_id="cancel-attempt-1",
        provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
        request_hash="8" * 64,
        approval_hash="9" * 64,
    )


def _provider(
    root: Path,
    runtime: _Runtime,
    *,
    acceptance: IbkrNautilusPaperProviderAcceptance | None = None,
    routes: dict[str, IbkrNautilusInstrumentRoute] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> IbkrNautilusPaperExecutionProvider:
    provider = IbkrNautilusPaperExecutionProvider(
        root / "provider.sqlite3",
        runtime=runtime,
        instrument_routes=routes or ROUTES,
        acceptance=acceptance,
        clock=clock or (lambda: NOW),
    )
    provider.bind_submission_validator(lambda _: True)
    provider.bind_cancellation_validator(lambda _: True)
    return provider


def test_provider_acceptance_is_content_identified_and_schema_valid() -> None:
    acceptance = _acceptance()

    assert acceptance.execution_accepted
    assert acceptance.is_current(NOW)
    assert IbkrNautilusPaperProviderAcceptance.from_dict(acceptance.to_dict()) == acceptance
    assert (
        validate_agent_contract(
            acceptance.to_dict(),
            "ibkr-nautilus-paper-provider-acceptance.schema.json",
        )
        == ()
    )


def test_expired_acceptance_blocks_new_submit_but_preserves_exact_scope_cancel(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    acceptance = _acceptance(valid_until=NOW + timedelta(seconds=1))
    provider = _provider(tmp_path, runtime, acceptance=acceptance)
    provider.submit(_submission())

    later = _provider(
        tmp_path,
        runtime,
        acceptance=acceptance,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert later.manifest.enabled
    assert not later.new_order_admission_open
    with pytest.raises(SubmissionCapabilityRejected, match="current acceptance"):
        later.submit(_submission())
    receipt = later.cancel(_cancellation())
    assert receipt.status.value == "dispatched"


def test_paper_service_restart_after_acceptance_expiry_keeps_reconcile_and_cancel(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    acceptance = _acceptance(valid_until=NOW + timedelta(seconds=1))
    provider = _provider(tmp_path / "adapter", runtime, acceptance=acceptance)
    mandate = TradingMandate(
        mandate_id="paper-mandate",
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.TIMEBOXED,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        allowed_instruments=frozenset({"AAPL.XNAS"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        max_order_notional=Decimal("1000"),
    )
    basis = PriceBasis(
        instrument_id="AAPL.XNAS",
        currency="USD",
        unit="share",
        basis_kind="raw_tradable",
        price=Decimal("100"),
        source_id="test-price",
        source_version="v1",
        observed_at=NOW,
        valid_until=NOW + timedelta(hours=1),
    )
    service = PaperExecutionService(
        tmp_path / "paper",
        provider=provider,
        mandate=mandate,
        price_source=lambda _: basis,
        clock=lambda: NOW,
    )
    service.admit(_order())
    assert service.dispatch_next() is not None
    nautilus_id = runtime.submit_calls[0].nautilus_client_order_id
    runtime.snapshot = NautilusPaperRuntimeSnapshot(
        observed_at=NOW,
        connected=True,
        reconciled=True,
        complete=True,
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id="IB-42",
                status=NautilusPaperRuntimeStatus.ACCEPTED,
                observed_at=NOW,
            ),
        ),
    )

    assert service.reconcile().complete

    later = NOW + timedelta(seconds=2)
    restarted_provider = _provider(
        tmp_path / "adapter",
        runtime,
        acceptance=acceptance,
        clock=lambda: later,
    )
    restarted = PaperExecutionService(
        tmp_path / "paper",
        provider=restarted_provider,
        mandate=mandate,
        price_source=lambda _: basis,
        clock=lambda: later,
    )

    assert restarted.reconcile().complete
    with pytest.raises(PermissionError, match="closed for new orders"):
        restarted.admit(
            replace(
                _order(),
                client_order_id="harness-order-2",
                signal_id="signal-2",
            )
        )
    cancellation = restarted.request_cancel(
        "harness-order-1",
        cancellation_id="cancel-after-expiry",
        reason="risk reduction",
    )
    assert cancellation.state.value == "pending_approval"


@pytest.mark.parametrize(
    "acceptance",
    [
        _acceptance(markets=("HK",)),
        _acceptance(order_types=("market",)),
    ],
)
def test_submit_is_limited_to_accepted_market_and_order_type(
    tmp_path: Path,
    acceptance: IbkrNautilusPaperProviderAcceptance,
) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=acceptance)

    with pytest.raises(SubmissionCapabilityRejected, match="outside accepted Provider scope"):
        provider.submit(_submission())

    assert runtime.submit_calls == []


def test_durable_order_scope_cannot_be_reused_by_another_account(tmp_path: Path) -> None:
    first_runtime = _Runtime()
    first = _provider(tmp_path, first_runtime, acceptance=_acceptance())
    first.submit(_submission())

    second_account = "account-ref-" + "a" * 64
    second_runtime = _Runtime(account_reference_hash=second_account)
    second = _provider(
        tmp_path,
        second_runtime,
        acceptance=_acceptance(account_reference_hash=second_account),
    )

    with pytest.raises(ValueError, match="runtime scope conflict"):
        second.submit(_submission())
    with pytest.raises(CancellationCapabilityRejected, match="exact known order"):
        second.cancel(_cancellation())
    assert second_runtime.submit_calls == []
    assert second_runtime.cancel_calls == []


def test_provider_stays_disabled_without_complete_current_acceptance(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime)

    assert not provider.manifest.enabled
    assert provider.manifest.verified_capabilities == frozenset()
    with pytest.raises(SubmissionCapabilityRejected, match="lacks current acceptance"):
        provider.submit(_submission())
    with pytest.raises(CancellationCapabilityRejected, match="risk-reduction acceptance"):
        provider.cancel(_cancellation())
    assert runtime.submit_calls == []
    assert runtime.cancel_calls == []


@pytest.mark.parametrize(
    "acceptance",
    [
        _acceptance(complete=False),
        _acceptance(gaps=("unresolved",)),
    ],
)
def test_incomplete_or_gapped_acceptance_cannot_enable_provider(
    tmp_path: Path,
    acceptance: IbkrNautilusPaperProviderAcceptance,
) -> None:
    provider = _provider(tmp_path, _Runtime(), acceptance=acceptance)

    assert not provider.manifest.enabled
    assert Capability.PAPER_EXECUTION not in provider.manifest.verified_capabilities


def test_submit_uses_stable_identity_and_is_idempotent_across_restart(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    capability = _submission()

    first = provider.submit(capability)
    second = provider.submit(capability)
    restarted_runtime = _Runtime()
    restarted = _provider(tmp_path, restarted_runtime, acceptance=_acceptance())
    third = restarted.submit(capability)

    assert first == second == third
    assert first.provider_order_id == "IB-42"
    assert len(runtime.submit_calls) == 1
    assert runtime.submit_calls[0].nautilus_client_order_id.startswith("MIA-")
    assert restarted_runtime.submit_calls == []


def test_ambiguous_submit_is_never_redispatched_after_restart(tmp_path: Path) -> None:
    runtime = _Runtime()
    runtime.submit_error = TimeoutError("response lost")
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    capability = _submission()

    with pytest.raises(TimeoutError, match="response lost"):
        provider.submit(capability)
    restarted_runtime = _Runtime()
    restarted = _provider(tmp_path, restarted_runtime, acceptance=_acceptance())
    with pytest.raises(RuntimeError, match="ambiguous; reconcile only"):
        restarted.submit(capability)

    assert len(runtime.submit_calls) == 1
    assert restarted_runtime.submit_calls == []


def test_reconciliation_can_resolve_ambiguous_submit_as_rejected_without_broker_id(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    runtime.submit_error = TimeoutError("response lost")
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())

    with pytest.raises(TimeoutError, match="response lost"):
        provider.submit(_submission())
    nautilus_id = runtime.submit_calls[0].nautilus_client_order_id
    runtime.snapshot = NautilusPaperRuntimeSnapshot(
        observed_at=NOW + timedelta(seconds=1),
        connected=True,
        reconciled=True,
        complete=True,
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id=None,
                status=NautilusPaperRuntimeStatus.REJECTED,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
    )

    snapshot = provider.reconcile()

    assert snapshot.complete
    assert snapshot.gaps == ()
    assert snapshot.receipts[0].status.value == "rejected"
    assert snapshot.receipts[0].provider_order_id is None
    assert (
        validate_agent_contract(
            snapshot.to_dict(),
            "provider-reconciliation-snapshot-v2.schema.json",
        )
        == ()
    )


def test_reconciliation_schema_rejects_accepted_order_without_broker_identity() -> None:
    payload: dict[str, object] = {
        "schema_version": "market-impact.provider-reconciliation-snapshot.v2",
        "provider_id": IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        "snapshot_id": "provider-reconciliation-schema-only-fixture",
        "observed_at": "2026-09-01T08:00:00Z",
        "complete": True,
        "receipts": [
            {
                "client_order_id": "harness-order-1",
                "provider_order_id": None,
                "status": "accepted",
                "observed_at": "2026-09-01T08:00:00Z",
                "filled_quantity": "0",
                "fill_ids": [],
            }
        ],
        "gaps": [],
    }

    assert validate_agent_contract(
        payload,
        "provider-reconciliation-snapshot-v2.schema.json",
    )


@pytest.mark.parametrize(
    ("status", "filled_quantity", "fill_ids"),
    [
        ("filled", "0", []),
        ("partially_filled", "1", []),
        ("accepted", "1", ["fill-1"]),
        ("rejected", "1", ["fill-1"]),
        ("expired", "1", []),
    ],
)
def test_reconciliation_schema_rejects_inconsistent_fill_evidence(
    status: str,
    filled_quantity: str,
    fill_ids: list[str],
) -> None:
    payload: dict[str, object] = {
        "schema_version": "market-impact.provider-reconciliation-snapshot.v2",
        "provider_id": IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        "snapshot_id": "provider-reconciliation-schema-only-fixture",
        "observed_at": "2026-09-01T08:00:00Z",
        "complete": True,
        "receipts": [
            {
                "client_order_id": "harness-order-1",
                "provider_order_id": "IB-42",
                "status": status,
                "observed_at": "2026-09-01T08:00:00Z",
                "filled_quantity": filled_quantity,
                "fill_ids": fill_ids,
            }
        ],
        "gaps": [],
    }

    assert validate_agent_contract(
        payload,
        "provider-reconciliation-snapshot-v2.schema.json",
    )


def test_reconciliation_canonicalizes_provider_fill_identity_order(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())
    nautilus_id = runtime.submit_calls[0].nautilus_client_order_id
    runtime.snapshot = NautilusPaperRuntimeSnapshot(
        observed_at=NOW + timedelta(seconds=1),
        connected=True,
        reconciled=True,
        complete=True,
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id="IB-42",
                status=NautilusPaperRuntimeStatus.FILLED,
                observed_at=NOW + timedelta(seconds=1),
                filled_quantity=Decimal("2"),
                fill_ids=("fill-z", "fill-a"),
            ),
        ),
    )

    snapshot = provider.reconcile()

    assert snapshot.receipts[0].fill_ids == ("fill-a", "fill-z")
    assert (
        validate_agent_contract(
            snapshot.to_dict(),
            "provider-reconciliation-snapshot-v2.schema.json",
        )
        == ()
    )


@pytest.mark.parametrize(
    ("quantity", "expected"),
    [
        (Decimal("1E+2"), "100"),
        (Decimal("-0"), "0"),
        (Decimal("1.2300"), "1.23"),
        (
            Decimal("12345678901234567890123456789"),
            "12345678901234567890123456789",
        ),
    ],
)
def test_reconciliation_canonicalizes_decimal_for_public_contract(
    quantity: Decimal,
    expected: str,
) -> None:
    snapshot = ReconciliationSnapshot.build(
        provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        observed_at=NOW,
        complete=True,
        receipts=(
            ExecutionReceipt(
                client_order_id="harness-order-1",
                provider_order_id="IB-42",
                status=ExecutionStatus.EXPIRED,
                observed_at=NOW,
                filled_quantity=quantity,
                fill_ids=("fill-1",) if quantity > 0 else (),
            ),
        ),
    )

    payload = snapshot.to_dict()

    receipts = payload["receipts"]
    assert isinstance(receipts, list)
    assert receipts[0]["filled_quantity"] == expected
    assert (
        validate_agent_contract(
            payload,
            "provider-reconciliation-snapshot-v2.schema.json",
        )
        == ()
    )


def test_ambiguous_cancel_is_never_redispatched_after_restart(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())
    runtime.cancel_error = ConnectionError("disconnect after cancel")

    with pytest.raises(ConnectionError, match="disconnect after cancel"):
        provider.cancel(_cancellation())
    restarted_runtime = _Runtime()
    restarted = _provider(tmp_path, restarted_runtime, acceptance=_acceptance())
    with pytest.raises(RuntimeError, match="ambiguous; reconcile only"):
        restarted.cancel(_cancellation())

    assert len(runtime.cancel_calls) == 1
    assert restarted_runtime.cancel_calls == []


def test_reconciliation_classifies_external_order_and_preserves_partial_fill(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())
    nautilus_id = runtime.submit_calls[0].nautilus_client_order_id
    runtime.snapshot = NautilusPaperRuntimeSnapshot(
        observed_at=NOW + timedelta(seconds=1),
        connected=True,
        reconciled=True,
        complete=True,
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id="IB-42",
                status=NautilusPaperRuntimeStatus.PARTIALLY_FILLED,
                observed_at=NOW + timedelta(seconds=1),
                filled_quantity=Decimal("1"),
                fill_ids=("fill-1",),
            ),
            NautilusPaperOrderObservation(
                nautilus_client_order_id="EXTERNAL-1",
                provider_order_id="IB-99",
                status=NautilusPaperRuntimeStatus.ACCEPTED,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
    )

    snapshot = provider.reconcile()

    assert snapshot.complete
    assert snapshot.receipts[0].status.value == "partially_filled"
    assert snapshot.receipts[0].filled_quantity == Decimal("1")
    assert snapshot.receipts[0].fill_ids == ("fill-1",)
    assert snapshot.gaps == ("external_nautilus_order:EXTERNAL-1",)


def test_complete_reconciliation_reports_missing_accepted_order(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())

    snapshot = provider.reconcile()

    assert snapshot.gaps == ("accepted_nautilus_order_missing:harness-order-1",)
