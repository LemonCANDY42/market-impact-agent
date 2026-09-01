# pyright: reportPrivateUsage=false
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import LocalDataSnapshotStore
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
    _PROVIDER_FACTORY_SEAL,
    IBKR_NAUTILUS_PAPER_SCENARIO_RESULT_SCHEMA,
    IbkrNautilusInstrumentRoute,
    IbkrNautilusPaperAcceptanceAuthority,
    IbkrNautilusPaperAcceptanceRunner,
    IbkrNautilusPaperAcceptanceVerifier,
    IbkrNautilusPaperExecutionProvider,
    IbkrNautilusPaperProviderAcceptance,
    IbkrNautilusPaperScenarioObservation,
    NautilusPaperCancelCommand,
    NautilusPaperOrderObservation,
    NautilusPaperRuntimeSnapshot,
    NautilusPaperRuntimeStatus,
    NautilusPaperSubmitCommand,
    hash_ibkr_nautilus_instrument_routes,
    issue_ibkr_nautilus_paper_provider_from_harness_state,
)
from market_impact_agent.ibkr_nautilus_paper import (
    IBKR_NAUTILUS_PAPER_PROVIDER_ID,
    IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
)
from market_impact_agent.paper_execution import PaperExecutionService, PriceBasis
from market_impact_agent.providers import (
    CancellationCapabilityRejected,
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
_AUTHORITY_ROOTS: list[TemporaryDirectory[str]] = []
_AUTHORITIES: dict[str, IbkrNautilusPaperAcceptanceAuthority] = {}
_VERIFIERS: dict[str, IbkrNautilusPaperAcceptanceVerifier] = {}
RUNNER_ID = "fixture-harness-acceptance-runner"
RUNNER_KEY = b"fixture-harness-acceptance-runner-key-at-least-32-bytes"
TRUSTED_RUNNER = IbkrNautilusPaperAcceptanceRunner(RUNNER_ID, RUNNER_KEY)


class _Runtime:
    def __init__(
        self,
        *,
        configuration_hash: str = "1" * 64,
        account_reference_hash: str = "account-ref-" + "2" * 64,
        acceptance_authority_id: str = TRUSTED_RUNNER.authority_id,
    ) -> None:
        self.runtime_version = "0.2.0-candidate"
        self.nautilus_version = "1.231.0"
        self.nautilus_ibapi_version = "10.37.2"
        self.configuration_hash = configuration_hash
        self.account_reference_hash = account_reference_hash
        self.acceptance_authority_id = acceptance_authority_id
        self.time_in_force = "DAY"
        self.session_scope_valid = True
        self.activation_runtime_active = True
        self.session_scope_generation = 1
        self.session_scope_observed_at = NOW
        self.session_scope_last_disconnection_ns = None
        self.session_scope_ttl_seconds = 60.0
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
            cash_complete=True,
            positions_complete=True,
            orders_complete=True,
            executions_complete=True,
            external_order_discovery_complete=True,
            effective_client_id=0,
            connection_generation=1,
            cash_reconciliation_generation=1,
            positions_reconciliation_generation=1,
            orders_reconciliation_generation=1,
            executions_reconciliation_generation=1,
        )

    def submit(self, reference: object) -> NautilusPaperOrderObservation:
        if not isinstance(reference, NautilusPaperSubmitCommand):
            raise AssertionError(f"unexpected canonical submit reference: {reference!r}")
        command = reference
        self.submit_calls.append(command)
        if self.submit_error is not None:
            raise self.submit_error
        return NautilusPaperOrderObservation(
            nautilus_client_order_id=command.nautilus_client_order_id,
            provider_order_id="IB-42",
            status=NautilusPaperRuntimeStatus.ACCEPTED,
            observed_at=NOW,
        )

    def cancel(self, reference: object) -> NautilusPaperOrderObservation:
        if not isinstance(reference, NautilusPaperCancelCommand):
            raise AssertionError(f"unexpected canonical cancel reference: {reference!r}")
        command = reference
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

    def bind_canonical_activation(
        self,
        store: LocalDataSnapshotStore,
        *,
        acceptance_id: str,
        head_id: str,
    ) -> None:
        raise AssertionError(
            f"mechanics-only runtime cannot bind activation: {store.root}, "
            f"{acceptance_id}, {head_id}"
        )


class _MechanicsOnlyExecutionProvider(IbkrNautilusPaperExecutionProvider):
    """Synthetic trusted seam for execution-state mechanics; never activation evidence."""

    def _acceptance_scope_matches_at(
        self,
        acceptance: IbkrNautilusPaperProviderAcceptance,
        *,
        now: datetime,
    ) -> bool:
        return (
            acceptance.allows_risk_reduction(now)
            and self._acceptance_verifier.authority_id == self._runtime.acceptance_authority_id
            and self._acceptance_verifier.verify(acceptance)
            and acceptance.configuration_hash == self._runtime.configuration_hash
            and acceptance.account_reference_hash == self._runtime.account_reference_hash
            and acceptance.instrument_routes_hash == self._instrument_routes_hash
            and acceptance.runtime_version == self._runtime.runtime_version
            and acceptance.nautilus_version == self._runtime.nautilus_version
            and acceptance.nautilus_ibapi_version == self._runtime.nautilus_ibapi_version
            and acceptance.time_in_force == (self._runtime.time_in_force,)
            and self._runtime.session_scope_valid
        )

    def _dispatch_submission(
        self,
        command: NautilusPaperSubmitCommand,
    ) -> NautilusPaperOrderObservation:
        return self._runtime.submit(command)  # type: ignore[arg-type]

    def _dispatch_cancellation(
        self,
        command: NautilusPaperCancelCommand,
    ) -> NautilusPaperOrderObservation:
        return self._runtime.cancel(command)  # type: ignore[arg-type]


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
    runner_id: str = RUNNER_ID,
    runner_key: bytes = RUNNER_KEY,
) -> IbkrNautilusPaperProviderAcceptance:
    authority_root = TemporaryDirectory()
    _AUTHORITY_ROOTS.append(authority_root)
    authority = IbkrNautilusPaperAcceptanceAuthority(
        Path(authority_root.name) / "authority.sqlite3",
        runner_id=runner_id,
        verification_key=runner_key,
    )
    runner = IbkrNautilusPaperAcceptanceRunner(runner_id, runner_key)
    observations: list[str] = []
    for index, scenario in enumerate(SCENARIOS):
        evidence_root = Path(authority_root.name) / "evidence"
        evidence_root.mkdir(exist_ok=True)
        artifact_path = evidence_root / f"{scenario}.artifact.json"
        result_path = evidence_root / f"{scenario}.result.json"
        artifact_path.write_text(
            json.dumps({"scenario": scenario, "events": [f"observed-{scenario}"]}),
            encoding="utf-8",
        )
        result_path.write_text(
            json.dumps(
                {
                    "schema_version": IBKR_NAUTILUS_PAPER_SCENARIO_RESULT_SCHEMA,
                    "scenario": scenario,
                    "configuration_hash": configuration_hash,
                    "account_reference_hash": account_reference_hash,
                    "instrument_routes_hash": instrument_routes_hash,
                    "markets": list(markets),
                    "order_types": list(order_types),
                    "time_in_force": ["DAY"],
                    "nautilus_ibapi_version": "10.37.2",
                    "effective_client_id": 0,
                    "client_id_collision": False,
                    "manual_order_auto_bind_observed": True,
                    "exclusive_api_client_scope_observed": True,
                    "passed": complete or index > 0,
                    "observed_at": "2026-09-01T07:58:00Z",
                }
            ),
            encoding="utf-8",
        )
        observation = authority.record_scenario_evidence(
            artifact_path=artifact_path,
            result_path=result_path,
            runner_seal=runner.seal_evidence(
                artifact_path=artifact_path,
                result_path=result_path,
            ),
        )
        observations.append(observation.observation_id)
    acceptance = authority.build_acceptance(
        observation_ids=tuple(sorted(observations)),
        configuration_hash=configuration_hash,
        account_reference_hash=account_reference_hash,
        instrument_routes_hash=instrument_routes_hash,
        markets=markets,
        order_types=order_types,
        time_in_force=("DAY",),
        nautilus_ibapi_version="10.37.2",
        accepted_at=NOW - timedelta(minutes=1),
        valid_until=valid_until,
        gaps=gaps,
    )
    _AUTHORITIES[acceptance.acceptance_id] = authority
    _VERIFIERS[acceptance.acceptance_id] = authority.verifier()
    return acceptance


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


def _submission(  # type: ignore[no-untyped-def]
    order: OrderIntent | None = None,
    *,
    submission_id: str = "submission-1",
):
    selected = order or _order()
    return _issue_submission_capability(
        order=selected,
        submission_id=submission_id,
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
    verifier_acceptance = acceptance or _acceptance()
    verifier = _VERIFIERS[verifier_acceptance.acceptance_id]
    provider = _MechanicsOnlyExecutionProvider(
        root / "provider.sqlite3",
        runtime=runtime,
        instrument_routes=routes or ROUTES,
        acceptance=acceptance,
        _acceptance_verifier=verifier,
        _factory_seal=_PROVIDER_FACTORY_SEAL,
        clock=clock or (lambda: NOW),
    )
    provider.bind_submission_validator(lambda _: True)
    provider.bind_cancellation_validator(lambda _: True)
    return provider


def test_provider_acceptance_is_content_identified_and_schema_valid() -> None:
    acceptance = _acceptance()
    verifier = _VERIFIERS[acceptance.acceptance_id]

    assert acceptance.execution_accepted
    assert acceptance.is_current(NOW)
    assert (
        IbkrNautilusPaperProviderAcceptance.from_dict(
            acceptance.to_dict(),
            authority=verifier,
        )
        == acceptance
    )
    assert (
        validate_agent_contract(
            acceptance.to_dict(),
            "ibkr-nautilus-paper-provider-acceptance.schema.json",
        )
        == ()
    )


def test_provider_acceptance_claims_safe_cancel_reconcile_new_replace() -> None:
    acceptance = _acceptance()

    assert "replace" in acceptance.accepted_scenarios
    assert (
        validate_agent_contract(
            acceptance.to_dict(),
            "ibkr-nautilus-paper-provider-acceptance.schema.json",
        )
        == ()
    )


def test_acceptance_rejects_unsealed_observation_and_unknown_evidence(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="issued by the Harness evidence resolver"):
        IbkrNautilusPaperScenarioObservation(
            observation_id="fabricated",
            scenario="submit",
            artifact_hash="1" * 64,
            result_hash="2" * 64,
            runner_id=RUNNER_ID,
            runner_seal="f" * 64,
            configuration_hash="3" * 64,
            account_reference_hash="account-ref-" + "4" * 64,
            instrument_routes_hash="5" * 64,
            markets=("US",),
            order_types=("limit",),
            time_in_force=("DAY",),
            nautilus_ibapi_version="10.37.2",
            effective_client_id=0,
            client_id_collision=False,
            manual_order_auto_bind_observed=True,
            exclusive_api_client_scope_observed=True,
            passed=True,
            observed_at=NOW,
            _seal=object(),
        )

    authority = IbkrNautilusPaperAcceptanceAuthority(
        tmp_path / "authority.sqlite3",
        runner_id=RUNNER_ID,
        verification_key=RUNNER_KEY,
    )
    artifact_path = tmp_path / "locally-authored-artifact.json"
    result_path = tmp_path / "locally-authored-result.json"
    artifact_path.write_text('{"scenario":"submit"}', encoding="utf-8")
    result_path.write_text("{}", encoding="utf-8")
    with pytest.raises(PermissionError, match="trusted runner provenance"):
        authority.record_scenario_evidence(
            artifact_path=artifact_path,
            result_path=result_path,
            runner_seal="0" * 64,
        )
    with pytest.raises(KeyError, match="unknown scenario observation"):
        authority.build_acceptance(
            observation_ids=("fabricated-observation",),
            configuration_hash="1" * 64,
            account_reference_hash="account-ref-" + "2" * 64,
            instrument_routes_hash=ROUTES_HASH,
            markets=("US",),
            order_types=("limit",),
            time_in_force=("DAY",),
            nautilus_ibapi_version="10.37.2",
            accepted_at=NOW,
            valid_until=NOW + timedelta(days=1),
        )


def test_acceptance_authority_rejects_tampered_payload_and_artifacts(tmp_path: Path) -> None:
    acceptance = _acceptance()
    authority = _AUTHORITIES[acceptance.acceptance_id]
    payload = acceptance.to_dict()
    payload["complete"] = False

    with pytest.raises(ValueError, match="does not match durable authority"):
        IbkrNautilusPaperProviderAcceptance.from_dict(
            payload,
            authority=authority.verifier(),
        )

    with authority._connect() as connection:
        connection.execute(
            """
            UPDATE ibkr_nautilus_scenario_artifacts
            SET artifact_bytes = ?
            WHERE observation_id = (
                SELECT observation_id
                FROM ibkr_nautilus_acceptance_observations
                WHERE acceptance_id = ?
                ORDER BY scenario
                LIMIT 1
            )
            """,
            (b'{"scenario":"account_reconciliation","tampered":true}', acceptance.acceptance_id),
        )

    assert not authority.verifier().verify(acceptance)
    provider = _provider(tmp_path, _Runtime(), acceptance=acceptance)
    assert not provider.manifest.enabled
    assert not provider.new_order_admission_open


def test_provider_rejects_acceptance_without_matching_durable_authority(tmp_path: Path) -> None:
    acceptance = _acceptance()
    with pytest.raises(TypeError, match="unexpected keyword argument 'acceptance_verifier'"):
        IbkrNautilusPaperExecutionProvider(
            tmp_path / "provider.sqlite3",
            runtime=_Runtime(),
            instrument_routes=ROUTES,
            acceptance=acceptance,
            acceptance_verifier=_VERIFIERS[acceptance.acceptance_id],  # type: ignore[call-arg]
            clock=lambda: NOW,
        )


def test_caller_minted_full_chain_and_matching_runtime_pin_lack_harness_activation(
    tmp_path: Path,
) -> None:
    attacker = _acceptance(
        runner_id="locally-authored-runner",
        runner_key=b"locally-authored-runner-key-with-at-least-32-bytes",
    )
    assert attacker.execution_accepted
    attacker_verifier = _VERIFIERS[attacker.acceptance_id]
    attacker_runtime = _Runtime(acceptance_authority_id=attacker.authority_id)

    attacker_root = LocalDataSnapshotStore(tmp_path / "caller-fresh-root")
    with pytest.raises(PermissionError, match="activation head is missing"):
        issue_ibkr_nautilus_paper_provider_from_harness_state(
            canonical_store=attacker_root,
            accepted_evidence_content_id=attacker.acceptance_id,
        )

    assert attacker_verifier.verify(attacker)
    assert attacker_runtime.acceptance_authority_id == attacker.authority_id
    closed = IbkrNautilusPaperExecutionProvider(
        tmp_path / "provider.sqlite3",
        runtime=attacker_runtime,
        instrument_routes=ROUTES,
        acceptance=attacker,
        _acceptance_verifier=attacker_verifier,
        _factory_seal=_PROVIDER_FACTORY_SEAL,
        clock=lambda: NOW,
    )
    closed.bind_submission_validator(lambda _: True)
    assert not closed.manifest.enabled
    assert not closed.new_order_admission_open
    with pytest.raises(SubmissionCapabilityRejected, match="lacks current acceptance"):
        closed.submit(_submission())
    assert attacker_runtime.submit_calls == []


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
        cash_complete=True,
        positions_complete=True,
        orders_complete=True,
        executions_complete=True,
        external_order_discovery_complete=True,
        effective_client_id=0,
        connection_generation=1,
        cash_reconciliation_generation=1,
        positions_reconciliation_generation=1,
        orders_reconciliation_generation=1,
        executions_reconciliation_generation=1,
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
    assert not provider.new_order_admission_open


def test_invalid_runtime_session_closes_new_order_before_provider_reconcile(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    runtime.session_scope_valid = False
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    assert not provider.manifest.enabled
    assert not provider.new_order_admission_open


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
        cash_complete=True,
        positions_complete=True,
        orders_complete=True,
        executions_complete=True,
        external_order_discovery_complete=True,
        effective_client_id=0,
        connection_generation=1,
        cash_reconciliation_generation=1,
        positions_reconciliation_generation=1,
        orders_reconciliation_generation=1,
        executions_reconciliation_generation=1,
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
        cash_complete=True,
        positions_complete=True,
        orders_complete=True,
        executions_complete=True,
        external_order_discovery_complete=True,
        effective_client_id=0,
        connection_generation=1,
        cash_reconciliation_generation=1,
        positions_reconciliation_generation=1,
        orders_reconciliation_generation=1,
        executions_reconciliation_generation=1,
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


def test_safe_replace_reconciles_canceled_before_dispatching_new_intent(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())
    nautilus_id = runtime.submit_calls[0].nautilus_client_order_id
    runtime.snapshot = NautilusPaperRuntimeSnapshot(
        observed_at=NOW + timedelta(seconds=1),
        connected=True,
        reconciled=True,
        complete=True,
        cash_complete=True,
        positions_complete=True,
        orders_complete=True,
        executions_complete=True,
        external_order_discovery_complete=True,
        effective_client_id=0,
        connection_generation=1,
        cash_reconciliation_generation=1,
        positions_reconciliation_generation=1,
        orders_reconciliation_generation=1,
        executions_reconciliation_generation=1,
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id="IB-42",
                status=NautilusPaperRuntimeStatus.CANCELED,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
    )
    replacement_order = replace(
        _order(),
        client_order_id="harness-order-2",
        signal_id="signal-2",
        quantity=Decimal("3"),
        limit_price=Decimal("101"),
    )

    receipt = provider.replace(
        cancellation=_cancellation(),
        replacement=_submission(replacement_order, submission_id="replacement-submission-1"),
    )

    assert receipt.client_order_id == "harness-order-2"
    assert len(runtime.cancel_calls) == 1
    assert len(runtime.submit_calls) == 2


def test_safe_replace_never_dispatches_new_intent_before_cancel_is_final(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())
    nautilus_id = runtime.submit_calls[0].nautilus_client_order_id
    runtime.snapshot = replace(
        runtime.snapshot,
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id="IB-42",
                status=NautilusPaperRuntimeStatus.PENDING_CANCEL,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
    )
    replacement_order = replace(
        _order(),
        client_order_id="harness-order-2",
        signal_id="signal-2",
    )

    with pytest.raises(RuntimeError, match="cancellation reconciliation"):
        provider.replace(
            cancellation=_cancellation(),
            replacement=_submission(replacement_order, submission_id="replacement-submission-1"),
        )

    assert len(runtime.cancel_calls) == 1
    assert len(runtime.submit_calls) == 1


def test_safe_replace_resumes_after_ambiguous_cancel_reconciles_terminal(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance()
    first_runtime = _Runtime()
    first = _provider(tmp_path, first_runtime, acceptance=acceptance)
    first.submit(_submission())
    nautilus_id = first_runtime.submit_calls[0].nautilus_client_order_id
    first_runtime.cancel_error = TimeoutError("cancel response lost")
    replacement_order = replace(
        _order(),
        client_order_id="harness-order-2",
        signal_id="signal-2",
    )
    replacement = _submission(
        replacement_order,
        submission_id="replacement-submission-after-restart",
    )

    with pytest.raises(TimeoutError, match="cancel response lost"):
        first.replace(cancellation=_cancellation(), replacement=replacement)

    restarted_runtime = _Runtime()
    restarted_runtime.snapshot = replace(
        restarted_runtime.snapshot,
        observed_at=NOW + timedelta(seconds=1),
        orders=(
            NautilusPaperOrderObservation(
                nautilus_client_order_id=nautilus_id,
                provider_order_id="IB-42",
                status=NautilusPaperRuntimeStatus.CANCELED,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
    )
    restarted = _provider(tmp_path, restarted_runtime, acceptance=acceptance)

    receipt = restarted.replace(cancellation=_cancellation(), replacement=replacement)
    terminal = restarted.cancel(_cancellation())

    assert receipt.client_order_id == "harness-order-2"
    assert terminal.status.value == "canceled"
    assert restarted_runtime.cancel_calls == []
    assert len(restarted_runtime.submit_calls) == 1


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
        cash_complete=True,
        positions_complete=True,
        orders_complete=True,
        executions_complete=True,
        external_order_discovery_complete=True,
        effective_client_id=0,
        connection_generation=1,
        cash_reconciliation_generation=1,
        positions_reconciliation_generation=1,
        orders_reconciliation_generation=1,
        executions_reconciliation_generation=1,
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

    assert not snapshot.complete
    assert snapshot.receipts[0].status.value == "partially_filled"
    assert snapshot.receipts[0].filled_quantity == Decimal("1")
    assert snapshot.receipts[0].fill_ids == ("fill-1",)
    assert snapshot.gaps == ("external_nautilus_order:" + canonical_hash("EXTERNAL-1")[:12],)


def test_complete_reconciliation_reports_missing_accepted_order(tmp_path: Path) -> None:
    runtime = _Runtime()
    provider = _provider(tmp_path, runtime, acceptance=_acceptance())
    provider.submit(_submission())

    snapshot = provider.reconcile()

    assert snapshot.gaps == ("accepted_nautilus_order_missing:harness-order-1",)
