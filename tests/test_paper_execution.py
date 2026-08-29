from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from stat import S_IMODE
from threading import Event, Thread

import pytest

from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    canonical_hash,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionReceipt,
    ExecutionStatus,
    OrderIntent,
    OrderKind,
    Side,
    SignalIntent,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.experimental_paper_admission import (
    prepare_experimental_paper_admission,
)
from market_impact_agent.paper_execution import (
    ApprovalState,
    OutboxState,
    PaperExecutionService,
    PriceBasis,
)
from market_impact_agent.prospective_query_gate import ProspectiveQueryGateResult
from market_impact_agent.providers import (
    Capability,
    ExecutionProvider,
    MockExecutionProvider,
    ReconciliationSnapshot,
    SubmissionCapability,
    _issue_submission_capability,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.research import EvidenceTier

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


def test_experimental_agent_paper_binds_query_gate_without_claiming_strategy_or_live(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    service = make_service(root, MockExecutionProvider(tmp_path / "provider.sqlite3"))
    order = make_order()
    signal = SignalIntent(
        signal_id=order.signal_id,
        event_id="prospective-policy-event",
        instrument_id=order.instrument_id,
        side=order.side,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=order.expires_at,
        evidence_refs=("prospective-event-observation",),
        invalidation_conditions=("event is retracted",),
    )
    evidence_pack = make_evidence_pack()
    query_gate = make_query_gate(evidence_pack)
    admission = prepare_experimental_paper_admission(
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
        order=order,
        created_at=NOW,
    )

    record = service.admit_experimental(
        order,
        admission,
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
    )

    assert record.agent_admission_hash == canonical_hash(admission.to_dict())
    assert admission.signal_intent_hash == canonical_hash(signal.to_dict())
    assert admission.query_gate_result_hash == canonical_hash(query_gate.to_dict())
    assert admission.evidence_pack_hash == canonical_hash(evidence_pack.to_dict())
    assert admission.strategy_admission_id is None
    assert admission.alpha_claim is False
    assert admission.live_capability is False
    assert admission.execution_authority is False
    assert (
        validate_agent_contract(
            admission.to_dict(),
            "experimental-paper-admission.schema.json",
        )
        == ()
    )
    restarted = make_service(
        root,
        MockExecutionProvider(tmp_path / "provider.sqlite3"),
    )
    assert restarted.get(order.client_order_id) == record


def test_experimental_agent_paper_rejects_ineligible_gate_and_low_level_rebinding(
    tmp_path: Path,
) -> None:
    order = make_order()
    signal = SignalIntent(
        signal_id=order.signal_id,
        event_id="prospective-policy-event",
        instrument_id=order.instrument_id,
        side=order.side,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=order.expires_at,
        evidence_refs=("prospective-event-observation",),
        invalidation_conditions=("event is retracted",),
    )
    with pytest.raises(PermissionError, match="eligible prospective Query Gate"):
        prepare_experimental_paper_admission(
            query_gate=make_query_gate(make_evidence_pack(), eligible=False),
            evidence_pack=make_evidence_pack(),
            signal=signal,
            order=order,
            created_at=NOW,
        )

    service = make_service(tmp_path / "paper", MockExecutionProvider())
    service.admit(order)
    evidence_pack = make_evidence_pack()
    admission = prepare_experimental_paper_admission(
        query_gate=make_query_gate(evidence_pack),
        evidence_pack=evidence_pack,
        signal=signal,
        order=order,
        created_at=NOW,
    )
    with pytest.raises(ValueError, match="different Agent admission binding"):
        service.admit_experimental(
            order,
            admission,
            query_gate=make_query_gate(evidence_pack),
            evidence_pack=evidence_pack,
            signal=signal,
        )


def test_experimental_agent_paper_rejects_changed_signal_content(tmp_path: Path) -> None:
    order = make_order()
    signal = SignalIntent(
        signal_id=order.signal_id,
        event_id="prospective-policy-event",
        instrument_id=order.instrument_id,
        side=order.side,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=order.expires_at,
        evidence_refs=("prospective-event-observation",),
        invalidation_conditions=("event is retracted",),
    )
    evidence_pack = make_evidence_pack()
    query_gate = make_query_gate(evidence_pack)
    admission = prepare_experimental_paper_admission(
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
        order=order,
        created_at=NOW,
    )

    service = make_service(tmp_path / "paper", MockExecutionProvider())
    with pytest.raises(ValueError, match="Signal evidence_refs are not all in the Evidence Pack"):
        service.admit_experimental(
            order,
            admission,
            query_gate=query_gate,
            evidence_pack=evidence_pack,
            signal=replace(signal, evidence_refs=("different-observation",)),
        )


@pytest.mark.parametrize(
    ("mismatch", "message"),
    [
        (
            "event",
            "Signal event differs from the Evidence Pack",
        ),
        (
            "evidence_refs",
            "Signal evidence_refs are not all in the Evidence Pack",
        ),
        (
            "instrument",
            "Signal instrument is not an allowed Evidence Pack target",
        ),
    ],
)
def test_experimental_agent_paper_requires_pack_provenance(
    mismatch: str,
    message: str,
) -> None:
    order = make_order()
    evidence_pack = make_evidence_pack()
    signal = order_signal(order)
    if mismatch == "event":
        signal = replace(signal, event_id="different-event")
    elif mismatch == "evidence_refs":
        signal = replace(signal, evidence_refs=("missing-evidence",))
    else:
        signal = replace(signal, instrument_id="OTHER")

    with pytest.raises(ValueError, match=message):
        prepare_experimental_paper_admission(
            query_gate=make_query_gate(evidence_pack),
            evidence_pack=evidence_pack,
            signal=signal,
            order=order,
            created_at=NOW,
        )


def test_experimental_agent_paper_rejects_query_gate_for_a_different_pack() -> None:
    order = make_order()
    evidence_pack = make_evidence_pack()

    with pytest.raises(ValueError, match="different Evidence Pack"):
        prepare_experimental_paper_admission(
            query_gate=make_query_gate(make_evidence_pack(event_id="other-event")),
            evidence_pack=evidence_pack,
            signal=order_signal(order),
            order=order,
            created_at=NOW,
        )


def test_experimental_agent_paper_restart_requires_persisted_evidence_pack(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    service = make_service(root, MockExecutionProvider(tmp_path / "provider.sqlite3"))
    order = make_order()
    signal = order_signal(order)
    evidence_pack = make_evidence_pack()
    query_gate = make_query_gate(evidence_pack)
    admission = prepare_experimental_paper_admission(
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
        order=order,
        created_at=NOW,
    )
    service.admit_experimental(
        order,
        admission,
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
    )
    service.artifacts.get(admission.evidence_pack_hash, media_type="application/json").path.unlink()

    restarted = make_service(
        root,
        MockExecutionProvider(tmp_path / "provider.sqlite3"),
    )
    with pytest.raises(FileNotFoundError):
        restarted.get(order.client_order_id)


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

    service = make_service(root, MockExecutionProvider())
    with sqlite3.connect(service.database_path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(paper_intents)").fetchall()
        }

    assert "agent_admission_hash" in columns
    assert service.admit(make_order()).agent_admission_hash is None


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
        order_hash="a" * 64,
        mandate_hash="b" * 64,
        price_basis_hash="c" * 64,
        policy_evaluation_hash="d" * 64,
        approval_hash="e" * 64,
    )
    with pytest.raises(PermissionError, match="durable outbox"):
        provider.submit(forged)


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
            "execution-reconciliation.schema.json",
        )
        == ()
    )


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


def order_signal(order: OrderIntent) -> SignalIntent:
    return SignalIntent(
        signal_id=order.signal_id,
        event_id="prospective-policy-event",
        instrument_id=order.instrument_id,
        side=order.side,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=order.expires_at,
        evidence_refs=("prospective-event-observation",),
        invalidation_conditions=("event is retracted",),
    )


def make_evidence_pack(*, event_id: str = "prospective-policy-event") -> EvidencePack:
    return EvidencePack.build(
        event_id=event_id,
        as_of=NOW - timedelta(minutes=2),
        research_question="Which registered paper target may be affected?",
        evidence=(
            EvidenceReference(
                evidence_id="prospective-event-observation",
                claim_id="policy-event",
                source_ref="prospective://official/policy-event",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=NOW - timedelta(minutes=3),
                content_hash=sha256(b"prospective-event-observation").hexdigest(),
                summary="An official prospective event observation.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("TEST",),
    )


def make_query_gate(
    evidence_pack: EvidencePack,
    *,
    eligible: bool = True,
) -> ProspectiveQueryGateResult:
    blocking = () if eligible else ("event_revelation:missing_required_input",)
    snapshot_ids = ("data-snapshot-" + "1" * 64,) if eligible else ()
    barrier_at = NOW - timedelta(minutes=2)
    evaluated_at = NOW - timedelta(minutes=1)
    core = {
        "schema_version": "market-impact.prospective-query-gate-result.v1",
        "registration_id": "prospective-diagnostic-registration-" + "2" * 64,
        "checkpoint_key": "policy-event-v2",
        "checkpoint_snapshot_set_id": "prospective-checkpoint-snapshot-set-" + "3" * 64,
        "evidence_pack_id": evidence_pack.pack_id,
        "agent_execution_binding_hash": "5" * 64,
        "model_profile_id": "cliproxyapi-luna-xhigh-v1",
        "model_cost_limit_usd": "5.00",
        "barrier_at": barrier_at.isoformat().replace("+00:00", "Z"),
        "evaluated_at": evaluated_at.isoformat().replace("+00:00", "Z"),
        "authorized_snapshot_ids": list(snapshot_ids),
        "blocking_required_gaps": list(blocking),
        "nonblocking_information_gaps": ["positioning:missing_registered_routes"],
        "model_run_eligible": eligible,
        "claim_scope": "process_diagnostic_only_no_alpha_or_execution_claim",
        "historical_pit_claim": False,
        "strategy_promotion_claim": False,
        "execution_capability": False,
    }
    return ProspectiveQueryGateResult(
        result_id=f"prospective-query-gate-{canonical_hash(core)}",
        registration_id="prospective-diagnostic-registration-" + "2" * 64,
        checkpoint_key="policy-event-v2",
        checkpoint_snapshot_set_id="prospective-checkpoint-snapshot-set-" + "3" * 64,
        evidence_pack_id=evidence_pack.pack_id,
        agent_execution_binding_hash="5" * 64,
        model_profile_id="cliproxyapi-luna-xhigh-v1",
        model_cost_limit_usd="5.00",
        barrier_at=barrier_at,
        evaluated_at=evaluated_at,
        authorized_snapshot_ids=snapshot_ids,
        blocking_required_gaps=blocking,
        nonblocking_information_gaps=("positioning:missing_registered_routes",),
        model_run_eligible=eligible,
    )
