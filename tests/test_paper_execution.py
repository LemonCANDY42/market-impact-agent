from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from stat import S_IMODE
from threading import Event, Thread

import pytest

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
from market_impact_agent.paper_execution import (
    ApprovalState,
    CancellationState,
    OutboxState,
    PaperExecutionService,
    PriceBasis,
)
from market_impact_agent.providers import (
    CancellationCapability,
    Capability,
    ExecutionProvider,
    MockExecutionProvider,
    ReconciliationSnapshot,
    SubmissionCapability,
    _issue_cancellation_capability,  # pyright: ignore[reportPrivateUsage]
    _issue_submission_capability,  # pyright: ignore[reportPrivateUsage]
)

NOW = datetime(2026, 8, 29, 2, tzinfo=UTC)


class AcceptThenTimeout:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        self.delegate.submit(capability)
        raise TimeoutError("acknowledgement was lost")

    def reconcile(self):  # type: ignore[no-untyped-def]
        return self.delegate.reconcile()

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)


class CancelThenTimeout:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        return self.delegate.submit(capability)

    def cancel(self, capability: CancellationCapability):  # type: ignore[no-untyped-def]
        self.delegate.cancel(capability)
        raise TimeoutError("cancellation acknowledgement was lost")

    def reconcile(self):  # type: ignore[no-untyped-def]
        return self.delegate.reconcile()

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)

    def bind_cancellation_validator(
        self,
        validator: Callable[[CancellationCapability], bool],
    ) -> None:
        self.delegate.bind_cancellation_validator(validator)


class CrashBeforeProvider:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        raise SystemExit("simulated process crash")

    def reconcile(self):  # type: ignore[no-untyped-def]
        return self.delegate.reconcile()

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)


class ExternalOrderSnapshot:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        return self.delegate.submit(capability)

    def reconcile(self) -> ReconciliationSnapshot:
        snapshot = self.delegate.reconcile()
        external = ExecutionReceipt(
            client_order_id="external-order",
            provider_order_id="external-1",
            status=ExecutionStatus.ACCEPTED,
            observed_at=NOW,
        )
        return ReconciliationSnapshot.build(
            provider_id=snapshot.provider_id,
            observed_at=snapshot.observed_at,
            complete=True,
            receipts=(*snapshot.receipts, external),
        )

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)


class BlockingProvider:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate
        self.entered = Event()
        self.release = Event()

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        self.entered.set()
        assert self.release.wait(timeout=5)
        return self.delegate.submit(capability)

    def reconcile(self) -> ReconciliationSnapshot:
        return self.delegate.reconcile()

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)


class SnapshotThenBlockReconciliation:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate
        self.entered = Event()
        self.release = Event()
        self._blocked_once = False

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        return self.delegate.submit(capability)

    def reconcile(self) -> ReconciliationSnapshot:
        snapshot = self.delegate.reconcile()
        if not self._blocked_once:
            self._blocked_once = True
            self.entered.set()
            assert self.release.wait(timeout=5)
        return snapshot

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)


class ProviderIdentityOverride:
    def __init__(self, delegate: MockExecutionProvider) -> None:
        self.delegate = delegate
        self.cancel_calls = 0

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return replace(self.delegate.manifest, provider_id="alternate-mock-execution")

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        return self.delegate.submit(capability)

    def cancel(self, capability: CancellationCapability):  # type: ignore[no-untyped-def]
        self.cancel_calls += 1
        return self.delegate.cancel(capability)

    def reconcile(self) -> ReconciliationSnapshot:
        snapshot = self.delegate.reconcile()
        return ReconciliationSnapshot.build(
            provider_id=self.manifest.provider_id,
            observed_at=snapshot.observed_at,
            complete=snapshot.complete,
            receipts=snapshot.receipts,
            gaps=snapshot.gaps,
        )

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)

    def bind_cancellation_validator(
        self,
        validator: Callable[[CancellationCapability], bool],
    ) -> None:
        self.delegate.bind_cancellation_validator(validator)


class ReconciliationOverride:
    def __init__(
        self,
        delegate: MockExecutionProvider,
        snapshot: ReconciliationSnapshot,
    ) -> None:
        self.delegate = delegate
        self.snapshot = snapshot

    @property
    def manifest(self):  # type: ignore[no-untyped-def]
        return self.delegate.manifest

    def submit(self, capability: SubmissionCapability):  # type: ignore[no-untyped-def]
        return self.delegate.submit(capability)

    def reconcile(self) -> ReconciliationSnapshot:
        return self.snapshot

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        self.delegate.bind_submission_validator(validator)


class MutableClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current


def test_timeboxed_intent_survives_restart_and_reconciles_without_fill_claim(
    tmp_path: Path,
) -> None:
    provider_root = tmp_path / "provider.sqlite3"
    provider = MockExecutionProvider(provider_root)
    service = make_service(tmp_path / "paper", provider)

    admitted = service.admit(make_order())
    assert admitted.approval_state is ApprovalState.APPROVED
    assert admitted.outbox_state is OutboxState.QUEUED
    assert admitted.agent_admission_hash is None

    restarted = make_service(
        tmp_path / "paper",
        MockExecutionProvider(provider_root, clock=lambda: NOW + timedelta(seconds=1)),
    )
    assert restarted.get("order-1") == admitted

    accepted = restarted.dispatch_next()
    assert accepted is not None
    assert accepted.outbox_state is OutboxState.ACCEPTED
    assert accepted.provider_status == "accepted"
    assert accepted.fill_status is None
    assert restarted.execution_blocked

    run = restarted.reconcile()
    assert run.complete
    assert run.gaps == ()
    assert restarted.get("order-1").outbox_state is OutboxState.RECONCILED
    assert not restarted.execution_blocked


def test_exact_intent_identity_is_idempotent_but_conflicts_fail_closed(
    tmp_path: Path,
) -> None:
    service = make_service(tmp_path / "paper", MockExecutionProvider())
    first = service.admit(make_order())
    assert service.admit(make_order()) == first

    with pytest.raises(ValueError, match=r"client_order_id.*different content"):
        service.admit(replace(make_order(), quantity=Decimal("11")))


def test_identical_admission_retry_remains_idempotent_as_clock_advances(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    service = make_service(tmp_path / "paper", MockExecutionProvider(), clock=clock)
    first = service.admit(make_order())
    clock.current = NOW + timedelta(seconds=1)
    assert service.admit(make_order()) == first


def test_existing_paper_database_adds_nullable_agent_admission_binding(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    root.mkdir()
    database_path = root / "paper-execution.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE paper_intents (
                client_order_id TEXT PRIMARY KEY,
                order_hash TEXT NOT NULL,
                mandate_hash TEXT NOT NULL,
                price_basis_hash TEXT NOT NULL,
                policy_evaluation_hash TEXT NOT NULL,
                approval_hash TEXT,
                approval_state TEXT NOT NULL,
                outbox_state TEXT,
                provider_order_id TEXT,
                provider_status TEXT,
                fill_status TEXT,
                order_expires_at TEXT NOT NULL,
                mandate_expires_at TEXT NOT NULL,
                price_valid_until TEXT NOT NULL,
                lease_token TEXT,
                lease_expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO paper_intents (
                client_order_id, order_hash, mandate_hash, price_basis_hash,
                policy_evaluation_hash, approval_hash, approval_state, outbox_state,
                provider_order_id, provider_status, fill_status,
                order_expires_at, mandate_expires_at, price_valid_until,
                lease_token, lease_expires_at, created_at, updated_at
            ) VALUES (
                'legacy-open-order', 'legacy-order-hash', 'legacy-mandate-hash',
                'legacy-price-hash', 'legacy-policy-hash', 'legacy-approval-hash',
                'approved', 'reconciled', 'legacy-provider-order', 'accepted', NULL,
                '2027-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
                '2027-01-01T00:00:00Z', NULL, NULL,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
            )
            """
        )

    service = make_service(root, MockExecutionProvider())
    with sqlite3.connect(service.database_path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(paper_intents)").fetchall()
        }

    assert "agent_admission_hash" in columns
    assert "provider_id" in columns
    assert "provider_version" in columns
    assert service.execution_blocked
    with pytest.raises(PermissionError, match="blocked pending reconciliation"):
        service.admit(make_order())


def test_manual_approval_binds_exact_artifacts_and_survives_restart(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3")
    service = make_service(root, provider, mode=ApprovalMode.MANUAL_EACH)

    pending = service.admit(make_order())
    assert pending.approval_state is ApprovalState.PENDING_APPROVAL
    assert pending.outbox_state is None
    approved = service.decide(
        "order-1",
        approve=True,
        actor_ref="local-human",
    )
    assert approved.approval_state is ApprovalState.APPROVED
    assert approved.outbox_state is OutboxState.QUEUED
    assert approved.approval_hash is not None

    restarted = make_service(
        root,
        MockExecutionProvider(tmp_path / "provider.sqlite3"),
        mode=ApprovalMode.MANUAL_EACH,
    )
    assert restarted.get("order-1") == approved


def test_changed_mandate_invalidates_existing_admission(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    provider = MockExecutionProvider()
    make_service(root, provider).admit(make_order())
    changed = replace(make_mandate(), max_order_notional=Decimal("2000"))

    with pytest.raises(ValueError, match=r"client_order_id.*different binding"):
        make_service(root, provider, mandate=changed).admit(make_order())


def test_ambiguous_submit_becomes_unknown_until_complete_reconciliation(
    tmp_path: Path,
) -> None:
    provider_root = tmp_path / "provider.sqlite3"
    durable_provider = MockExecutionProvider(provider_root)
    service = make_service(tmp_path / "paper", AcceptThenTimeout(durable_provider))
    service.admit(make_order())

    unknown = service.dispatch_next()
    assert unknown is not None
    assert unknown.outbox_state is OutboxState.UNKNOWN
    assert service.execution_blocked

    restarted = make_service(
        tmp_path / "paper",
        MockExecutionProvider(provider_root, clock=lambda: NOW + timedelta(seconds=1)),
    )
    run = restarted.reconcile()
    assert run.complete
    assert run.gaps == ()
    record = restarted.get("order-1")
    assert record.outbox_state is OutboxState.RECONCILED
    assert record.provider_order_id == "mock-000001"
    assert len(MockExecutionProvider(provider_root).reconcile().receipts) == 1


def test_expired_submit_lease_becomes_unknown_and_is_not_retried(tmp_path: Path) -> None:
    provider_root = tmp_path / "provider.sqlite3"
    root = tmp_path / "paper"
    provider = MockExecutionProvider(provider_root)
    service = make_service(root, CrashBeforeProvider(provider), lease_timeout_seconds=1)
    service.admit(make_order())

    with pytest.raises(SystemExit, match="simulated process crash"):
        service.dispatch_next()

    clock = MutableClock(NOW + timedelta(seconds=2))
    restarted = make_service(
        root,
        MockExecutionProvider(provider_root),
        lease_timeout_seconds=1,
        clock=clock,
    )
    assert restarted.dispatch_next() is None
    assert restarted.get("order-1").outbox_state is OutboxState.UNKNOWN
    assert restarted.execution_blocked
    assert MockExecutionProvider(provider_root).reconcile().receipts == ()


def test_external_provider_order_keeps_global_execution_gate_closed(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    provider = MockExecutionProvider(
        tmp_path / "provider.sqlite3",
        clock=lambda: NOW + timedelta(seconds=1),
    )
    service = make_service(root, provider)
    service.admit(make_order())
    service.dispatch_next()

    restarted = make_service(root, ExternalOrderSnapshot(provider))
    run = restarted.reconcile()
    assert not run.complete
    assert run.gaps == ("external_provider_order:external-order",)
    assert restarted.execution_blocked
    with pytest.raises(PermissionError, match="pending reconciliation"):
        restarted.admit(replace(make_order(), client_order_id="order-2"))


def test_claim_atomically_closes_gate_across_service_instances(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    blocking = BlockingProvider(MockExecutionProvider())
    first_service = make_service(root, blocking)
    first_service.admit(make_order())
    first_service.admit(replace(make_order(), client_order_id="order-2", signal_id="signal-2"))
    second_service = make_service(root, blocking)
    results: list[object] = []
    worker = Thread(target=lambda: results.append(first_service.dispatch_next()))
    worker.start()
    assert blocking.entered.wait(timeout=5)
    try:
        assert second_service.dispatch_next() is None
    finally:
        blocking.release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert len(results) == 1
    assert len(blocking.delegate.reconcile().receipts) == 1


def test_provider_rejects_forged_capability_without_durable_outbox() -> None:
    provider = MockExecutionProvider()
    forged = _issue_submission_capability(
        order=make_order(),
        submission_id="forged",
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        order_hash="a" * 64,
        mandate_hash="b" * 64,
        price_basis_hash="c" * 64,
        policy_evaluation_hash="d" * 64,
        approval_hash="e" * 64,
    )
    with pytest.raises(PermissionError, match="durable outbox"):
        provider.submit(forged)


def test_provider_submission_validator_cannot_be_replaced_after_binding(
    tmp_path: Path,
) -> None:
    provider = MockExecutionProvider()
    make_service(tmp_path / "paper", provider)
    provider.bind_submission_validator(lambda _capability: True)
    forged = _issue_submission_capability(
        order=make_order(),
        submission_id="forged",
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        order_hash="a" * 64,
        mandate_hash="b" * 64,
        price_basis_hash="c" * 64,
        policy_evaluation_hash="d" * 64,
        approval_hash="e" * 64,
    )
    with pytest.raises(PermissionError, match="durable outbox"):
        provider.submit(forged)


def test_provider_rejects_forged_cancel_capability_and_validator_replacement(
    tmp_path: Path,
) -> None:
    provider = MockExecutionProvider()
    make_service(tmp_path / "paper", provider)
    provider.bind_cancellation_validator(lambda _capability: True)
    forged = _issue_cancellation_capability(
        client_order_id="order-1",
        provider_order_id="mock-000001",
        cancellation_id="cancel-order-1",
        attempt_id="forged",
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        request_hash="a" * 64,
        approval_hash="b" * 64,
    )

    with pytest.raises(PermissionError, match="active durable outbox lease"):
        provider.cancel(forged)


def test_reconciliation_rejects_missing_acknowledged_order(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3")
    service = make_service(root, provider)
    service.admit(make_order())
    service.dispatch_next()
    actual = provider.reconcile()
    missing = ReconciliationSnapshot.build(
        provider_id=actual.provider_id,
        observed_at=NOW + timedelta(seconds=1),
        complete=True,
        receipts=(),
    )

    restarted = make_service(root, ReconciliationOverride(provider, missing))
    run = restarted.reconcile()
    assert not run.complete
    assert run.gaps == ("acknowledged_order_missing:order-1",)
    assert restarted.get("order-1").outbox_state is OutboxState.ACCEPTED
    assert restarted.execution_blocked


def test_complete_reconciliation_keeps_checking_reconciled_open_orders(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(root, provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    missing = ReconciliationSnapshot.build(
        provider_id=provider.manifest.provider_id,
        observed_at=NOW + timedelta(seconds=1),
        complete=True,
        receipts=(),
    )

    restarted = make_service(root, ReconciliationOverride(provider, missing))
    run = restarted.reconcile()

    assert not run.complete
    assert run.gaps == ("reconciled_open_order_missing:order-1",)
    assert restarted.get("order-1").provider_status == ExecutionStatus.ACCEPTED.value
    assert restarted.execution_blocked


def test_pre_activation_reconciliation_cannot_clear_kill_switch(tmp_path: Path) -> None:
    blocking = SnapshotThenBlockReconciliation(MockExecutionProvider(clock=lambda: NOW))
    service = make_service(tmp_path / "paper", blocking)
    results: list[object] = []
    worker = Thread(target=lambda: results.append(service.reconcile()))
    worker.start()
    assert blocking.entered.wait(timeout=5)
    service.activate_kill_switch(actor_ref="local-human", reason="operator stop")
    blocking.release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert len(results) == 1

    with pytest.raises(PermissionError, match="new complete reconciliation"):
        service.clear_kill_switch(actor_ref="local-human")
    assert service.reconcile().complete
    service.clear_kill_switch(actor_ref="local-human")
    assert not service.kill_switch_active


def test_reconciliation_rejects_stale_or_wrong_provider_snapshot(tmp_path: Path) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3")
    root = tmp_path / "paper"
    service = make_service(root, AcceptThenTimeout(provider))
    service.admit(make_order())
    service.dispatch_next()
    actual = provider.reconcile()
    stale = ReconciliationSnapshot.build(
        provider_id=actual.provider_id,
        observed_at=NOW - timedelta(seconds=1),
        complete=True,
        receipts=actual.receipts,
    )
    stale_service = make_service(root, ReconciliationOverride(provider, stale))
    stale_run = stale_service.reconcile()
    assert not stale_run.complete
    assert stale_run.gaps == ("stale_provider_snapshot:order-1",)
    assert stale_service.get("order-1").outbox_state is OutboxState.UNKNOWN

    wrong = ReconciliationSnapshot.build(
        provider_id="other-provider",
        observed_at=actual.observed_at,
        complete=True,
        receipts=actual.receipts,
    )
    wrong_service = make_service(root, ReconciliationOverride(provider, wrong))
    wrong_run = wrong_service.reconcile()
    assert not wrong_run.complete
    assert wrong_run.gaps == ("provider_identity_mismatch:other-provider",)
    assert wrong_service.execution_blocked


@pytest.mark.parametrize("mode", [ApprovalMode.POLICY_AUTO, ApprovalMode.AUTONOMOUS])
def test_unimplemented_automatic_modes_never_enter_the_outbox(
    tmp_path: Path,
    mode: ApprovalMode,
) -> None:
    service = make_service(tmp_path / mode.value, MockExecutionProvider(), mode=mode)
    record = service.admit(make_order())
    assert record.approval_state is ApprovalState.DENIED
    assert record.outbox_state is None


def test_missing_or_stale_price_basis_fails_before_admission(tmp_path: Path) -> None:
    provider = MockExecutionProvider()
    missing = PaperExecutionService(
        tmp_path / "missing",
        provider=provider,
        mandate=make_mandate(),
        price_source=lambda _order: None,
        clock=lambda: NOW,
    )
    with pytest.raises(PermissionError, match="price basis"):
        missing.admit(make_order())

    stale = make_price_basis(valid_until=NOW)
    service = make_service(tmp_path / "stale", provider, price_basis=stale)
    with pytest.raises(PermissionError, match="price basis"):
        service.admit(make_order())


def test_price_basis_expiry_blocks_delayed_approval_and_dispatch(tmp_path: Path) -> None:
    basis = make_price_basis(valid_until=NOW + timedelta(seconds=1))
    automatic_clock = MutableClock()
    automatic = make_service(
        tmp_path / "automatic",
        MockExecutionProvider(),
        price_basis=basis,
        clock=automatic_clock,
    )
    automatic.admit(make_order())
    automatic_clock.current = NOW + timedelta(seconds=2)
    assert automatic.dispatch_next() is None
    assert automatic.get("order-1").outbox_state is OutboxState.EXPIRED

    current = [NOW]
    manual = PaperExecutionService(
        tmp_path / "manual",
        provider=MockExecutionProvider(),
        mandate=make_mandate(mode=ApprovalMode.MANUAL_EACH),
        price_source=lambda _order: basis,
        clock=lambda: current[0],
    )
    manual.admit(make_order())
    current[0] = NOW + timedelta(seconds=2)
    expired = manual.decide(
        "order-1",
        approve=True,
        actor_ref="local-human",
    )
    assert expired.approval_state is ApprovalState.EXPIRED
    assert expired.outbox_state is None


def test_mock_provider_exposes_no_account_or_live_capability() -> None:
    manifest = MockExecutionProvider().manifest
    assert Capability.ACCOUNT not in manifest.declared_capabilities
    assert Capability.ACCOUNT not in manifest.verified_capabilities
    assert Capability.LIVE_EXECUTION not in manifest.verified_capabilities


def test_persisted_admission_and_reconciliation_artifacts_are_versioned(
    tmp_path: Path,
) -> None:
    service = make_service(tmp_path / "paper", MockExecutionProvider())
    record = service.admit(make_order())
    contracts = {
        record.order_hash: "order-intent.schema.json",
        record.mandate_hash: "trading-mandate.schema.json",
        record.price_basis_hash: "price-basis.schema.json",
        record.policy_evaluation_hash: "hard-policy-evaluation.schema.json",
        record.approval_hash: "approval-decision.schema.json",
    }
    for artifact_hash, schema_name in contracts.items():
        assert artifact_hash is not None
        assert (
            validate_agent_contract(
                service.artifacts.read_json(artifact_hash),
                schema_name,
            )
            == ()
        )

    service.dispatch_next()
    run = service.reconcile()
    assert (
        validate_agent_contract(
            service.artifacts.read_json(run.reconciliation_hash),
            "execution-reconciliation-v2.schema.json",
        )
        == ()
    )


def test_manual_cancel_is_durable_and_reconciliation_establishes_terminal_state(
    tmp_path: Path,
) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete

    requested = service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="risk invalidation",
    )
    assert requested.state is CancellationState.PENDING_APPROVAL
    approved = service.decide_cancellation(
        requested.cancellation_id,
        approve=True,
        actor_ref="local-human",
    )
    assert approved.state is CancellationState.QUEUED

    acknowledged = service.dispatch_next_cancellation()
    assert acknowledged is not None
    assert acknowledged.state is CancellationState.ACKNOWLEDGED
    assert service.execution_blocked
    with sqlite3.connect(service.database_path) as connection:
        receipt_hash = connection.execute(
            "SELECT receipt_hash FROM paper_cancellation_attempts"
        ).fetchone()[0]
    contracts = {
        requested.request_hash: "cancellation-request.schema.json",
        approved.approval_hash: "cancellation-approval.schema.json",
        receipt_hash: "cancellation-command-receipt.schema.json",
    }
    for artifact_hash, schema_name in contracts.items():
        assert artifact_hash is not None
        assert (
            validate_agent_contract(
                service.artifacts.read_json(artifact_hash),
                schema_name,
            )
            == ()
        )

    run = service.reconcile()
    assert run.complete
    assert service.get_cancellation(requested.cancellation_id).state is (
        CancellationState.RECONCILED
    )
    assert (
        service.request_cancel(
            "order-1",
            cancellation_id="cancel-order-1",
            reason="risk invalidation",
        ).state
        is CancellationState.RECONCILED
    )
    assert service.get("order-1").provider_status == "canceled"
    assert not service.execution_blocked


def test_cancel_identity_is_idempotent_but_cannot_change_reason(tmp_path: Path) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    service.dispatch_next()
    assert service.reconcile().complete
    first = service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="risk invalidation",
    )
    assert (
        service.request_cancel(
            "order-1",
            cancellation_id="cancel-order-1",
            reason="risk invalidation",
        )
        == first
    )
    with pytest.raises(ValueError, match="different content"):
        service.request_cancel(
            "order-1",
            cancellation_id="cancel-order-1",
            reason="different reason",
        )


def test_ambiguous_cancel_is_never_retried_and_resolves_only_by_reconciliation(
    tmp_path: Path,
) -> None:
    provider_root = tmp_path / "provider.sqlite3"
    delegate = MockExecutionProvider(provider_root, clock=lambda: NOW)
    service = make_service(tmp_path / "paper", CancelThenTimeout(delegate))
    service.admit(make_order())
    service.dispatch_next()
    assert service.reconcile().complete
    service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="risk invalidation",
    )
    service.decide_cancellation(
        "cancel-order-1",
        approve=True,
        actor_ref="local-human",
    )

    ambiguous = service.dispatch_next_cancellation()
    assert ambiguous is not None
    assert ambiguous.state is CancellationState.UNKNOWN
    assert service.dispatch_next_cancellation() is None

    restarted = make_service(
        tmp_path / "paper",
        MockExecutionProvider(provider_root, clock=lambda: NOW + timedelta(seconds=1)),
        clock=lambda: NOW + timedelta(seconds=1),
    )
    assert restarted.dispatch_next_cancellation() is None
    assert restarted.reconcile().complete
    assert restarted.get_cancellation("cancel-order-1").state is CancellationState.RECONCILED


def test_incomplete_snapshot_cannot_terminalize_cancellation(tmp_path: Path) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="risk invalidation",
    )
    service.decide_cancellation(
        "cancel-order-1",
        approve=True,
        actor_ref="local-human",
    )
    assert service.dispatch_next_cancellation() is not None
    actual = provider.reconcile()
    incomplete = ReconciliationSnapshot.build(
        provider_id=actual.provider_id,
        observed_at=actual.observed_at,
        complete=False,
        receipts=actual.receipts,
        gaps=("provider_section_incomplete",),
    )
    service.provider = ReconciliationOverride(provider, incomplete)

    run = service.reconcile()

    assert not run.complete
    assert service.get_cancellation("cancel-order-1").state is CancellationState.ACKNOWLEDGED
    assert service.get("order-1").provider_status == ExecutionStatus.ACCEPTED.value


def test_provider_identity_drift_expires_cancellation_before_provider_call(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    provider_root = tmp_path / "provider.sqlite3"
    provider = MockExecutionProvider(provider_root, clock=lambda: NOW)
    service = make_service(root, provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="risk invalidation",
    )
    drifted = ProviderIdentityOverride(
        MockExecutionProvider(provider_root, clock=lambda: NOW + timedelta(seconds=1))
    )
    restarted = make_service(
        root,
        drifted,
        clock=lambda: NOW + timedelta(seconds=1),
    )

    expired = restarted.decide_cancellation(
        "cancel-order-1",
        approve=True,
        actor_ref="local-human",
    )

    assert expired.state is CancellationState.EXPIRED
    assert restarted.dispatch_next_cancellation() is None
    assert drifted.cancel_calls == 0


def test_kill_switch_blocks_new_submits_but_keeps_exact_cancel_and_reconciliation_open(
    tmp_path: Path,
) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    service.admit(replace(make_order(), client_order_id="order-2", signal_id="signal-2"))
    first = service.dispatch_next()
    assert first is not None and first.client_order_id == "order-1"
    assert service.reconcile().complete
    service.activate_kill_switch(actor_ref="local-human", reason="operator stop")
    assert service.kill_switch_active
    assert service.execution_blocked
    assert service.dispatch_next() is None
    with pytest.raises(PermissionError, match="new complete reconciliation"):
        service.clear_kill_switch(actor_ref="local-human")

    service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="kill switch cleanup",
    )
    service.decide_cancellation(
        "cancel-order-1",
        approve=True,
        actor_ref="local-human",
    )
    assert service.dispatch_next_cancellation() is not None
    assert service.reconcile().complete
    assert service.kill_switch_active
    assert service.execution_blocked

    restarted = make_service(tmp_path / "paper", provider)
    assert restarted.kill_switch_active
    restarted.clear_kill_switch(actor_ref="local-human")
    assert not restarted.kill_switch_active
    second = restarted.dispatch_next()
    assert second is not None and second.client_order_id == "order-2"


def test_pending_cancel_has_priority_over_new_exposure(tmp_path: Path) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    service.admit(replace(make_order(), client_order_id="order-2", signal_id="signal-2"))
    service.request_cancel(
        "order-1",
        cancellation_id="cancel-order-1",
        reason="risk invalidation",
    )

    assert service.dispatch_next() is None
    service.decide_cancellation(
        "cancel-order-1",
        approve=False,
        actor_ref="local-human",
    )
    second = service.dispatch_next()
    assert second is not None and second.client_order_id == "order-2"


def test_replace_is_cancel_then_new_identity_after_complete_reconciliation(
    tmp_path: Path,
) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    replacement_order = replace(
        make_order(),
        client_order_id="order-1-replacement",
        quantity=Decimal("5"),
        order_kind=OrderKind.LIMIT,
        limit_price=Decimal("9.5"),
    )

    replacement = service.request_replace(
        "order-1",
        replacement_order,
        replacement_id="replace-order-1",
        cancellation_id="cancel-order-1",
        reason="reduce quantity and cap price",
    )
    assert replacement.admitted_client_order_id is None
    with pytest.raises(PermissionError, match="before cancellation reconciliation"):
        service.admit_replacement("replace-order-1")

    service.decide_cancellation(
        "cancel-order-1",
        approve=True,
        actor_ref="local-human",
    )
    assert service.dispatch_next_cancellation() is not None
    assert service.reconcile().complete
    admitted = service.admit_replacement("replace-order-1")
    assert admitted.client_order_id == "order-1-replacement"
    assert (
        service.get_replacement("replace-order-1").admitted_client_order_id
        == admitted.client_order_id
    )
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    assert service.get("order-1").provider_status == ExecutionStatus.CANCELED.value
    assert service.get("order-1-replacement").provider_status == ExecutionStatus.ACCEPTED.value


def test_replacement_and_its_cancellation_are_one_sqlite_transaction(tmp_path: Path) -> None:
    provider = MockExecutionProvider(tmp_path / "provider.sqlite3", clock=lambda: NOW)
    service = make_service(tmp_path / "paper", provider)
    service.admit(make_order())
    assert service.dispatch_next() is not None
    assert service.reconcile().complete
    with sqlite3.connect(service.database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_replacement_insert
            BEFORE INSERT ON paper_replacements
            BEGIN
                SELECT RAISE(ABORT, 'injected replacement failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected replacement failure"):
        service.request_replace(
            "order-1",
            replace(make_order(), client_order_id="order-1-replacement"),
            replacement_id="replace-order-1",
            cancellation_id="cancel-order-1",
            reason="change order",
        )

    with sqlite3.connect(service.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_cancellations").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_replacements").fetchone()[0] == 0


def test_paper_state_and_artifacts_are_private(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    service = make_service(root, MockExecutionProvider())
    record = service.admit(make_order())

    assert S_IMODE(root.stat().st_mode) == 0o700
    assert S_IMODE(service.database_path.stat().st_mode) == 0o600
    artifact = service.artifacts.get(
        record.order_hash,
        media_type="application/json",
    )
    assert S_IMODE(artifact.path.stat().st_mode) == 0o600


def make_service(
    root: Path,
    provider: ExecutionProvider,
    *,
    mode: ApprovalMode = ApprovalMode.TIMEBOXED,
    mandate: TradingMandate | None = None,
    price_basis: PriceBasis | None = None,
    lease_timeout_seconds: int = 30,
    clock: Callable[[], datetime] | None = None,
) -> PaperExecutionService:
    basis = price_basis or make_price_basis()
    return PaperExecutionService(
        root,
        provider=provider,
        mandate=mandate or make_mandate(mode=mode),
        price_source=lambda _order: basis,
        clock=clock or (lambda: NOW),
        lease_timeout_seconds=lease_timeout_seconds,
    )


def make_order() -> OrderIntent:
    return OrderIntent(
        client_order_id="order-1",
        signal_id="signal-1",
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        instrument_id="TEST",
        side=Side.BUY,
        quantity=Decimal("10"),
        order_kind=OrderKind.MARKET,
        created_at=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(minutes=5),
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


def make_price_basis(*, valid_until: datetime | None = None) -> PriceBasis:
    return PriceBasis(
        instrument_id="TEST",
        currency="USD",
        unit="per_share",
        basis_kind="reference_quote",
        price=Decimal("10"),
        source_id="mock-price",
        source_version="quote-1",
        observed_at=NOW - timedelta(seconds=1),
        valid_until=valid_until or NOW + timedelta(seconds=30),
    )
