"""The real portfolio producer and execution owner; network I/O alone is synthetic."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import compose_authoritative_agent_engine
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.autonomous_paper import (
    AutonomousOperationState,
    AutonomousPaperExecutionServiceV2,
    AutonomousReconciliationAuthorityV2,
    _issue_autonomous_provider_lease,  # pyright: ignore[reportPrivateUsage]
    _record_accepted_provider_capability,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionStatus,
    Side,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.paper_execution import PaperExecutionService
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.portfolio_decision import (
    PortfolioAction,
    PortfolioExposureViewV2,
    RawMarkedPositionV2,
)
from market_impact_agent.portfolio_review import (
    PORTFOLIO_EVIDENCE_SCOPE_VERSION,
    PORTFOLIO_PROMPT_PROJECTION_VERSION,
    PortfolioReviewAuthority,
    PortfolioReviewInputs,
    parse_portfolio_proposal_v4,
    portfolio_prompt_projection,
    portfolio_proposal_text_normalizations,
)
from market_impact_agent.prospective_decision_pipeline import run_portfolio_review_pipeline
from market_impact_agent.provider_reliability import ProviderAttemptEvent, ProviderAttemptPhase
from market_impact_agent.providers import MockExecutionProvider
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
    reopen_completed_research_thesis,
)
from market_impact_agent.runtime_store import RunStatus

from .test_agent_engine import (
    NOW,
    FixtureProvider,
    SimulatedCrash,
    abstention,
    final_turn,
    make_engine,
    proposal,
    request,
)
from .test_autonomous_paper import (
    AT,
    TARGET,
    _ExposureAuthority,  # pyright: ignore[reportPrivateUsage]
    _fixture,  # pyright: ignore[reportPrivateUsage]
)
from .test_pi_runtime import pi_profile
from .test_portfolio_decision_v2 import _rules  # pyright: ignore[reportPrivateUsage]
from .test_research_thesis_runtime import (
    _answer as _thesis_answer,  # pyright: ignore[reportPrivateUsage]
)
from .test_research_thesis_runtime import (
    _repository as _thesis_repository,  # pyright: ignore[reportPrivateUsage]
)

type NativePortfolio = tuple[ModelProviderProfile, list[dict[str, object]], list[str]]


def _answer(action: str = "hold") -> dict[str, object]:
    result: dict[str, object] = {
        "requested_action": action,
        "rationale": (
            "The positive thesis remains uncertain. Maintain cash rather than incur "
            "exposure costs; review after the next release."
        ),
        "horizon_band": "immediate",
        "primary_horizon_sessions": 3,
        "priced_in_assessment": "The positive surprise is only partly reflected in price.",
        "transmission": ["new fact -> expected cash flow -> target exposure"],
        "counter_scenario": "The next release could reverse the expected cash-flow change.",
        "review_after_sessions": 1,
        "evidence_refs": ["account_state", "exposure_view"],
        "invalidation_conditions": ["Review after the next confirmed company release."],
    }
    if action != "hold":
        result.update(
            instrument_id=TARGET,
            venue="ARCX",
            instrument_class="exchange_traded_fund",
            direction="long",
            target_gross_exposure_ratio="0.40",
            rationale=(
                "The outlook is positive but the account is overconcentrated; "
                "reduce to the mandate target."
            ),
        )
    return result


@pytest.fixture
def native_portfolio(monkeypatch: pytest.MonkeyPatch) -> NativePortfolio:
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-portfolio-key")
    permit = PiRuntimePermit(
        canonical_hash(runtime_identity()), (profile.route_identity,), "synthetic-portfolio-proof"
    )

    def installed(_root: Path) -> PiRuntimePermit:
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec
    answer = [_answer()]
    spawns: list[str] = []

    async def spawn(program: str, *args: str, **kwargs: Any):
        spawns.append(program)
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(answer[0])
        if capture_path := os.environ.get("PORTFOLIO_FIXTURE_REQUEST_PATH"):
            kwargs["env"]["PORTFOLIO_FIXTURE_REQUEST_PATH"] = capture_path
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    return profile, answer, spawns


def _setup(root: Path, *, action: PortfolioAction = PortfolioAction.OPEN):
    base = _fixture(root, action=action)
    clock = base.clock_box
    clock[0] = AT
    provider_path = root / "mock.sqlite3"
    provider = MockExecutionProvider(provider_path, clock=lambda: clock[0])
    assert base.account.cash is not None and base.account.positions is not None
    provider.configure_simulated_account(
        seed="portfolio-acceptance",
        cash=base.account.cash,
        positions=base.account.positions,
        instruments={TARGET: ("ARCX", "exchange_traded_fund")},
        opened_at=AT,
    )
    account = provider.simulated_account_snapshot(price_bases={TARGET: base.price})
    mandate = replace(
        base.mandate,
        account_id=account.account_reference_hash,
        approval_mode=ApprovalMode.MANUAL_EACH,
    )
    position = account.project_positions(evaluated_at=AT, max_age=timedelta(minutes=5))
    view = AuthorizedDecisionView.build(
        cutoff=AT,
        frozen_at=AT,
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )
    exposure = PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=position,
        raw_mark_set_hash=canonical_hash(base.price.to_dict()),
        execution_ledger_snapshot_hash=base.exposure.execution_ledger_snapshot_hash,
        reconciliation_ledger_snapshot_hash=base.exposure.reconciliation_ledger_snapshot_hash,
        currency="USD",
        marked_positions=tuple(
            replace(item, raw_price_basis_hash=canonical_hash(base.price.to_dict()))
            for item in base.exposure.marked_positions
        ),
        daily_turnover_used=Decimal(0),
        daily_submissions_used=0,
        active_kill_reasons=(),
        observed_at=AT,
        valid_until=AT + timedelta(minutes=5),
    )
    inputs = [
        PortfolioReviewInputs(
            account,
            position,
            view,
            exposure,
            mandate,
            {TARGET: base.price},
            _rules(),
            AT,
            AT + timedelta(minutes=5),
        )
    ]
    accounts = [account]
    exposures = [exposure]
    exposure_authority = _ExposureAuthority(exposures)
    authority = PortfolioReviewAuthority(
        base.store,
        input_source=lambda: inputs[0],
        exposure_authority=exposure_authority,
        clock=lambda: clock[0],
    )
    routes = {TARGET: {"provider_instrument_id": TARGET, "market": "SYNTHETIC"}}
    owner = PaperExecutionService(
        root / "legacy",
        provider=provider,
        mandate=TradingMandate(
            "synthetic-owner",
            mandate.account_id,
            TradingEnvironment.PAPER,
            ApprovalMode.MANUAL_EACH,
            AT,
            mandate.valid_until,
            mandate.allowed_instruments,
            mandate.allowed_sides,
            mandate.gross_exposure_limit,
        ),
        price_source=lambda _: base.price,
        clock=lambda: clock[0],
        account_state_source=lambda: account,
    )
    acceptance = owner.record_provider_acceptance(base.store)
    capability = _record_accepted_provider_capability(base.store, provider_acceptance_id=acceptance)
    lease = _issue_autonomous_provider_lease(
        base.store,
        accepted_capability_id=capability,
        provider=provider,
        mandate=mandate,
        instrument_routes=routes,
    )

    def open_service():
        # Each process/service restart gets a fresh adapter with the same durable Provider facts.
        provider = MockExecutionProvider(provider_path, clock=lambda: clock[0])
        service = AutonomousPaperExecutionServiceV2(
            base.store,
            provider=provider,
            provider_lease_id=lease.lease_id,
            mandate=mandate,
            account_state_source=lambda: accounts[0],
            exposure_view_source=lambda: exposures[0],
            exposure_view_authority=exposure_authority,
            price_basis_source=lambda _: base.price,
            reconciliation_authority=AutonomousReconciliationAuthorityV2(
                lambda _: (accounts[0], exposures[0])
            ),
            instrument_routes_hash=canonical_hash(routes),
            instrument_routes=routes,
            clock=lambda: clock[0],
            portfolio_review_authority=authority,
        )
        return service, provider

    clock[0] = AT + timedelta(seconds=3)
    return authority, inputs, accounts, exposures, clock, open_service, owner


async def seed_authoritative_research(
    authority: PortfolioReviewAuthority, root: Path, *, uncertain: bool
) -> str:
    """Actual composed research Run; only its model answer is supplied by the test."""
    model = FixtureProvider([final_turn(abstention() if uncertain else proposal(), 1)])
    fixture = make_engine(root / "research-fixture", model, handler_calls=[])
    engine = compose_authoritative_agent_engine(
        store=authority.store,
        provider=model,
        config=fixture.config,
        tool_registry=fixture.tool_registry,
        skill_registry=fixture.skill_registry,
        clock=lambda: NOW,
    )
    result = await engine.run(request("research-uncertain" if uncertain else "research-positive"))
    assert result.status is RunStatus.COMPLETED
    return result.run_id


def test_native_cash_hold_replays_exactly_without_regeneration(
    tmp_path: Path, native_portfolio: NativePortfolio
):
    profile, _, spawns = native_portfolio
    authority, _, _, _, _, open_service, _ = _setup(tmp_path)

    async def scenario():
        provider = PiRuntimeProvider(profile)
        service, broker = open_service()
        try:
            result = await run_portfolio_review_pipeline(
                authority=authority, provider=provider, run_id="cash-review", paper_service=service
            )
            assert result.terminal["status"] == "completed"
            assert result.operation is None
            proposal = cast(dict[str, object], result.terminal["proposal"])
            assert (
                validate_agent_contract(proposal, "agent-portfolio-proposal-v4.schema.json") == ()
            )
            assert proposal["requested_action"] == "hold"
            assert (
                proposal["instrument_id"] is None
                and proposal["target_gross_exposure_ratio"] is None
            )
            before = len(spawns)
            assert (
                await authority.review_account(run_id="cash-review", provider=provider)
                == result.terminal
            )
            assert len(spawns) == before == 1
            assert broker.reconcile().receipts == ()
        finally:
            service.close()
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("reference, accepted", [("release", True), ("invented", False)])
def test_dynamic_research_thesis_is_accepted_by_same_root_portfolio_review(
    tmp_path: Path, native_portfolio: NativePortfolio, reference: str, accepted: bool
) -> None:
    profile, answer, _ = native_portfolio
    authority, _, _, _, _, _, _ = _setup(tmp_path)

    async def scenario() -> None:
        research_provider = PiRuntimeProvider(profile)
        try:
            answer[0] = {
                **_thesis_answer(),
                "evidence_refs": ["release"],
                "counterevidence_refs": ["market"],
            }
            thesis_authority = ResearchThesisAuthority(
                authority.store,
                experiment_id="dynamic-effectiveness-v1",
                arm_id="luna-max",
                clock=lambda: AT - timedelta(seconds=1),
            )
            thesis_run = "dynamic-research-for-portfolio"
            terminal = await thesis_authority.analyze(
                run_id=thesis_run,
                provider=research_provider,
                inputs=ResearchThesisRunInputs(
                    _thesis_repository(TARGET),
                    TARGET,
                    "dynamic-thesis-v1",
                    frozenset({1, 3, 5, 10}),
                ),
            )
            assert terminal["status"] == "completed"
            await research_provider.close()
            answer[0] = {
                **_answer(),
                "evidence_refs": [reference],
                "counterevidence_refs": ["market"],
            }
            portfolio_provider = PiRuntimeProvider(profile)
            try:
                result = await authority.review(
                    run_id="portfolio-from-dynamic-thesis",
                    provider=portfolio_provider,
                    research_run_ids=(),
                    research_thesis_run_ids=(thesis_run,),
                )
                assert result["status"] == ("completed" if accepted else "incomplete")
                assert authority.replay("portfolio-from-dynamic-thesis") == result
                binding = cast(
                    dict[str, object],
                    authority.store.artifacts.read_json(
                        authority.journal.get_run("portfolio-from-dynamic-thesis").config_hash
                    ),
                )
                research_theses = cast(list[dict[str, object]], binding["research_theses"])
                assert research_theses[0]["run_id"] == thesis_run
                assert binding["evidence_scope_version"] == PORTFOLIO_EVIDENCE_SCOPE_VERSION
                projected = cast(dict[str, object], binding["prompt_projection"])
                assert "release" in cast(list[str], projected["evidence_ids"])
                assert "invented" not in cast(list[str], projected["evidence_ids"])
                legacy = {k: v for k, v in binding.items() if k != "evidence_scope_version"}
                legacy_projection = portfolio_prompt_projection(legacy)
                assert "release" not in cast(list[str], legacy_projection["evidence_ids"])
                assert thesis_run in cast(list[str], legacy_projection["evidence_ids"])
                with pytest.raises(ValueError, match="requires bound evidence"):
                    parse_portfolio_proposal_v4(
                        {**_answer(), "evidence_refs": ["release"]},
                        binding_hash="legacy-binding",
                        evidence_ids=frozenset(cast(list[str], legacy_projection["evidence_ids"])),
                    )
            finally:
                await portfolio_provider.close()
        finally:
            await research_provider.close()

    asyncio.run(scenario())


def test_portfolio_rejects_reopened_thesis_after_its_cutoff(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, answer, _ = native_portfolio
    authority, _, _, _, _, _, _ = _setup(tmp_path)

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile)
        try:
            answer[0] = _thesis_answer()
            thesis_authority = ResearchThesisAuthority(
                authority.store,
                experiment_id="dynamic-effectiveness-v1",
                arm_id="luna-max",
                clock=lambda: AT - timedelta(seconds=1),
            )
            run_id = "future-thesis-for-portfolio"
            terminal = await thesis_authority.analyze(
                run_id=run_id,
                provider=provider,
                inputs=ResearchThesisRunInputs(
                    _thesis_repository(TARGET),
                    TARGET,
                    "dynamic-thesis-v1",
                    frozenset({1, 3, 5, 10}),
                ),
            )
            assert terminal["status"] == "completed"
            thesis, source = reopen_completed_research_thesis(
                journal=authority.journal,
                artifact_store=authority.store.artifacts,
                run_id=run_id,
            )

            def future_reopen(**_: object):
                return replace(thesis, as_of=AT + timedelta(days=1)), source

            monkeypatch.setattr(
                "market_impact_agent.portfolio_review.reopen_completed_research_thesis",
                future_reopen,
            )
            with pytest.raises(PermissionError, match="after the portfolio cutoff"):
                authority._research_theses((run_id,), AT)  # pyright: ignore[reportPrivateUsage]
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_native_reduction_manual_restart_fill_reconciliation(
    tmp_path: Path, native_portfolio: NativePortfolio
):
    profile, answer, _ = native_portfolio
    answer[0] = _answer("reduce")
    authority, inputs, accounts, exposures, clock, open_service, _ = _setup(
        tmp_path, action=PortfolioAction.REDUCE
    )

    async def scenario():
        model = PiRuntimeProvider(profile)
        service, mock = open_service()
        try:
            research_run = await seed_authoritative_research(authority, tmp_path, uncertain=False)
            result = await run_portfolio_review_pipeline(
                authority=authority,
                provider=model,
                run_id="concentration-review",
                research_run_ids=(research_run,),
                paper_service=service,
            )
            operation = result.operation
            assert result.terminal["status"] == "completed", result.terminal
            assert (
                operation is not None
                and operation.state is AutonomousOperationState.PENDING_APPROVAL
            )
            admission = authority.execution_admission("concentration-review")
            assert (
                validate_agent_contract(admission.order.to_dict(), "order-intent-v2.schema.json")
                == ()
            )
            assert admission.order.side is Side.SELL and admission.order.quantity == 200
            assert "signal_id" not in admission.order.to_dict()
            assert service.dispatch_next() is None
            service.close()
            service, mock = open_service()
            assert service.admit_portfolio_review("concentration-review") == operation
            assert (
                service.get(operation.client_order_id).state
                is AutonomousOperationState.PENDING_APPROVAL
            )
            service.decide_portfolio_approval(
                operation.client_order_id, approved=True, actor_ref="synthetic-human"
            )
            submitted = service.dispatch_next()
            assert submitted is not None and submitted.state is AutonomousOperationState.ACCEPTED
            assert service.dispatch_next() is None
            clock[0] += timedelta(seconds=1)
            partial = mock.record_simulated_fill(
                operation.client_order_id, fill_id="fill-1", quantity=Decimal(80), price=Decimal(10)
            )
            assert partial.status is ExecutionStatus.PARTIALLY_FILLED
            assert (
                mock.record_simulated_fill(
                    operation.client_order_id,
                    fill_id="fill-1",
                    quantity=Decimal(80),
                    price=Decimal(10),
                )
                == partial
            )
            with pytest.raises(ValueError, match="different content"):
                mock.record_simulated_fill(
                    operation.client_order_id,
                    fill_id="fill-1",
                    quantity=Decimal(81),
                    price=Decimal(10),
                )
            with pytest.raises(ValueError, match="overfill"):
                mock.record_simulated_fill(
                    operation.client_order_id,
                    fill_id="fill-too-much",
                    quantity=Decimal(121),
                    price=Decimal(10),
                )
            mock.record_simulated_fill(
                operation.client_order_id,
                fill_id="fill-2",
                quantity=Decimal(120),
                price=Decimal(10),
            )
            with pytest.raises(ValueError, match="accepted durable order"):
                mock.record_simulated_fill(
                    "unknown", fill_id="fill-3", quantity=Decimal(1), price=Decimal(10)
                )
            simulated = mock.simulated_fills(operation.client_order_id)
            receipt = mock.reconcile().receipts[0]
            assert receipt.status is ExecutionStatus.FILLED and receipt.filled_quantity == 200
            snapshot = mock.reconcile()
            prior = accounts[0]
            assert (
                prior.positions is not None and prior.cash is not None and receipt.provider_order_id
            )
            proceeds = sum(
                (
                    Decimal(cast(str, fill["quantity"])) * Decimal(cast(str, fill["price"]))
                    for fill in simulated
                ),
                Decimal(0),
            )
            remaining = prior.positions[0].quantity - receipt.filled_quantity
            account = mock.simulated_account_snapshot(price_bases=inputs[0].price_bases)
            assert account.positions is not None and account.positions[0].quantity == remaining
            assert (
                account.cash is not None
                and account.cash[0].settled == prior.cash[0].settled + proceeds
            )
            position = account.project_positions(
                evaluated_at=clock[0], max_age=timedelta(minutes=5)
            )
            view = AuthorizedDecisionView.build(
                cutoff=clock[0],
                frozen_at=clock[0],
                data_snapshot_ids=(),
                decision_input_ids=(),
                position_snapshot=position,
            )
            exposure = PortfolioExposureViewV2.build(
                authorized_view=view,
                position_snapshot=position,
                raw_mark_set_hash=canonical_hash("post-fill"),
                execution_ledger_snapshot_hash=canonical_hash("post-fill-ledger"),
                reconciliation_ledger_snapshot_hash=canonical_hash(snapshot.to_dict()),
                currency="USD",
                marked_positions=(
                    RawMarkedPositionV2(
                        TARGET,
                        "ARCX",
                        "exchange_traded_fund",
                        Side.BUY,
                        remaining,
                        Decimal(10),
                        canonical_hash(inputs[0].price_bases[TARGET].to_dict()),
                    ),
                ),
                daily_turnover_used=proceeds,
                daily_submissions_used=1,
                active_kill_reasons=(),
                observed_at=clock[0],
                valid_until=AT + timedelta(minutes=5),
            )
            accounts[0], exposures[0] = account, exposure
            reconciled = service.reconcile()
            assert reconciled.complete, reconciled
            assert (
                service.get(operation.client_order_id).state is AutonomousOperationState.RECONCILED
            )
            with sqlite3.connect(authority.store.index_path) as connection:
                assert connection.execute(
                    "SELECT reservation_active, submission_consumed FROM autonomous_operations"
                ).fetchone() == (0, 1)
            assert authority.replay("concentration-review") == result.terminal
        finally:
            service.close()
            await model.close()

    asyncio.run(scenario())


def test_uncertain_research_still_completes_account_review_and_signed_crash_replays(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
    monkeypatch: pytest.MonkeyPatch,
):
    profile, _, spawns = native_portfolio
    authority, _, _, _, _, _, _ = _setup(tmp_path)

    async def scenario():
        research_run = await seed_authoritative_research(authority, tmp_path, uncertain=True)
        model = PiRuntimeProvider(profile)
        finish = authority.journal.finish
        crashed = False

        def crash_finish(**kwargs: Any):
            nonlocal crashed
            if not crashed:
                crashed = True
                raise SimulatedCrash()
            return finish(**kwargs)

        monkeypatch.setattr(authority.journal, "finish", crash_finish)
        try:
            with pytest.raises(SimulatedCrash):
                await run_portfolio_review_pipeline(
                    authority=authority,
                    provider=model,
                    run_id="uncertain-account",
                    research_run_ids=(research_run,),
                )
            assert authority.journal.get_run("uncertain-account").status is RunStatus.RUNNING
            result = await run_portfolio_review_pipeline(
                authority=authority,
                provider=model,
                run_id="uncertain-account",
                research_run_ids=(research_run,),
            )
            assert result.terminal["status"] == "completed"
            assert (
                cast(dict[str, object], result.terminal["proposal"])["requested_action"] == "hold"
            )
            assert len(spawns) == 1
            usage = authority.usage_ledger.records()
            assert len(usage) == 1 and usage[0].record.metrics.provider_attempts == 1
            assert usage[0].record.metrics.input_tokens == 100
            assert usage[0].record.metrics.estimated_cost_microusd > 0
        finally:
            await model.close()

    asyncio.run(scenario())


def test_missing_account_invalid_model_and_generic_forgery_cannot_complete_hold(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
):
    profile, answer, spawns = native_portfolio
    authority, inputs, _, _, clock, _, _ = _setup(tmp_path)
    original = inputs[0]

    async def scenario():
        model = PiRuntimeProvider(profile)
        try:
            inputs[0] = replace(original, expires_at=clock[0])
            with pytest.raises(PermissionError, match="not current"):
                await authority.review_account(run_id="missing-authority", provider=model)
            assert spawns == []
            inputs[0] = original
            answer[0] = _answer("abstain")
            terminal = await authority.review_account(run_id="invalid-model", provider=model)
            assert terminal["status"] == "incomplete" and "proposal" not in terminal
            usage = authority.usage_ledger.records()[0].record
            assert usage.status is RunStatus.FAILED and usage.metrics.provider_attempts == 1
            assert usage.metrics.estimated_cost_microusd > 0
            authority.journal.start_run(run_id="forgery", config_hash="a" * 64, created_at=clock[0])
            with pytest.raises(PermissionError, match="root-authenticated"):
                authority.journal.append(
                    run_id="forgery",
                    event_id="forgery.portfolio.terminal",
                    event_type="portfolio.review.validated",
                    observed_at=clock[0],
                    payload={"terminal_hash": "a" * 64},
                )
            with pytest.raises(KeyError):
                authority.execution_admission("wrong-run")
        finally:
            await model.close()

    asyncio.run(scenario())


def test_unknown_native_generation_retains_budget_and_terminal_usage(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
):
    profile, answer, spawns = native_portfolio
    answer[0] = {"__network_failure": True}
    authority, _, _, _, _, _, _ = _setup(tmp_path)

    async def scenario():
        model = PiRuntimeProvider(profile)
        try:
            terminal = await authority.review_account(run_id="unknown-generation", provider=model)
            assert terminal["status"] == "incomplete" and "proposal" not in terminal
            events = authority.journal.events("unknown-generation")
            assert sum(event.event_type == "pi.budget.reserved" for event in events) == 1
            assert not any(event.event_type == "pi.budget.settled" for event in events)
            usage = authority.usage_ledger.records()[0].record
            assert (
                usage.metrics.provider_attempts == 1 and usage.metrics.estimated_cost_microusd > 0
            )
            assert (
                await authority.review_account(run_id="unknown-generation", provider=model)
                == terminal
            )
            assert len(spawns) == 1
        finally:
            await model.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("crash_before_finish", [False, True])
def test_cancelled_native_review_closes_terminal_usage_and_never_regenerates(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
    monkeypatch: pytest.MonkeyPatch,
    crash_before_finish: bool,
):
    profile, answer, spawns = native_portfolio
    answer[0] = {"__hang": True}
    authority, _, _, _, _, _, _ = _setup(tmp_path)
    run_id = "cancelled-generation"

    async def scenario():
        model = PiRuntimeProvider(profile)
        dispatched = asyncio.Event()
        observe = authority._observe_attempt  # pyright: ignore[reportPrivateUsage]
        finish = authority.journal.finish

        def observed(run_id: str, event: ProviderAttemptEvent):
            observe(run_id, event)
            if event.phase is ProviderAttemptPhase.DISPATCHED:
                dispatched.set()

        def crash_finish(**kwargs: Any):
            raise SimulatedCrash()

        monkeypatch.setattr(authority, "_observe_attempt", observed)
        if crash_before_finish:
            monkeypatch.setattr(authority.journal, "finish", crash_finish)
        task = asyncio.create_task(authority.review_account(run_id=run_id, provider=model))
        try:
            await asyncio.wait_for(dispatched.wait(), 5)
            task.cancel()
            with pytest.raises(SimulatedCrash if crash_before_finish else asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
            assert model._process is None  # pyright: ignore[reportPrivateUsage]
            event = authority.journal.event(f"{run_id}.portfolio.terminal")
            assert event is not None and event.event_type == "portfolio.review.incomplete"
            if crash_before_finish:
                assert authority.journal.get_run(run_id).status is RunStatus.RUNNING
                monkeypatch.setattr(authority.journal, "finish", finish)
                terminal = await authority.review_account(run_id=run_id, provider=model)
            else:
                terminal = authority.replay(run_id)
            assert terminal["status"] == "incomplete" and terminal["reason"] == "CancelledError"
            assert "proposal" not in terminal
            assert authority.journal.get_run(run_id).status is RunStatus.CANCELLED
            usage = authority.usage_ledger.records()
            assert len(usage) == 1 and usage[0].record.status is RunStatus.CANCELLED
            assert usage[0].record.metrics.provider_attempts == 1
            assert usage[0].record.metrics.estimated_cost_microusd > 0
            events = authority.journal.events(run_id)
            assert sum(event.event_type == "pi.budget.reserved" for event in events) == 1
            assert not any(event.event_type == "pi.budget.settled" for event in events)
            assert await authority.review_account(run_id=run_id, provider=model) == terminal
            assert authority.journal.events(run_id) == events
            assert authority.usage_ledger.records() == usage
            assert len(spawns) == 1
        finally:
            if not task.done():
                task.cancel()
            await model.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("unit", ["per_share", "per_lot", "unknown"])
def test_mock_account_mark_requires_per_share_unit(tmp_path: Path, unit: str):
    _, inputs, _, _, clock, _, _ = _setup(tmp_path, action=PortfolioAction.REDUCE)
    provider = MockExecutionProvider(tmp_path / "mock.sqlite3", clock=lambda: clock[0])
    price = replace(inputs[0].price_bases[TARGET], unit=unit)
    if unit != "per_share":
        with pytest.raises(PermissionError, match="explicit current simulated raw mark"):
            provider.simulated_account_snapshot(price_bases={TARGET: price})
    else:
        account = provider.simulated_account_snapshot(price_bases={TARGET: price})
        assert account.positions is not None and len(account.positions) == 1
        assert account.positions[0].quantity == Decimal(600)
        assert account.positions[0].concentration == Decimal(6000) / Decimal(26000)


def test_manual_ambiguous_dispatch_never_regenerates_and_rounding_is_reserved(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
    monkeypatch: pytest.MonkeyPatch,
):
    profile, answer, _ = native_portfolio
    answer[0] = {**_answer("open"), "target_gross_exposure_ratio": "0.4005"}
    authority, _, _, _, _, open_service, _ = _setup(tmp_path)

    async def scenario():
        model = PiRuntimeProvider(profile)
        service, mock = open_service()
        original = mock.submit
        calls = 0

        def ambiguous(capability: object):
            nonlocal calls
            calls += 1
            original(capability)
            raise RuntimeError("synthetic ACK lost after durable Provider accept")

        monkeypatch.setattr(mock, "submit", ambiguous)
        try:
            result = await run_portfolio_review_pipeline(
                authority=authority, provider=model, run_id="ambiguous-order", paper_service=service
            )
            assert result.operation is not None
            with sqlite3.connect(authority.store.index_path) as connection:
                row = connection.execute(
                    "SELECT signed_delta, turnover_reserved FROM autonomous_operations"
                ).fetchone()
                assert tuple(Decimal(item) for item in row) == (Decimal(4000), Decimal(4000))
            service.decide_portfolio_approval(
                result.operation.client_order_id, approved=True, actor_ref="synthetic-human"
            )
            submitted = service.dispatch_next()
            assert submitted is not None and submitted.state is AutonomousOperationState.UNKNOWN
            service.close()
            service, mock = open_service()
            assert service.dispatch_next() is None
            assert (
                service.admit_portfolio_review("ambiguous-order").state
                is AutonomousOperationState.UNKNOWN
            )
            assert calls == 1 and len(mock.reconcile().receipts) == 1
            with sqlite3.connect(authority.store.index_path) as connection:
                assert connection.execute(
                    "SELECT reservation_active, submission_consumed FROM autonomous_operations"
                ).fetchone() == (1, 1)
        finally:
            service.close()
            await model.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["abstain", "observe"])
def test_no_abstention_contract(action: str):
    with pytest.raises(ValueError, match="cannot abstain or observe"):
        parse_portfolio_proposal_v4(
            _answer(action),
            binding_hash="a" * 64,
            evidence_ids=frozenset({"account_state", "exposure_view"}),
        )


def test_dynamic_portfolio_allows_one_fact_to_support_competing_interpretations() -> None:
    answer = _answer()
    answer["counterevidence_refs"] = ["exposure_view"]

    proposal = parse_portfolio_proposal_v4(
        answer,
        binding_hash="a" * 64,
        evidence_ids=frozenset({"account_state", "exposure_view"}),
    )

    assert proposal.evidence_refs == ("account_state", "exposure_view")
    assert proposal.counterevidence_refs == ("exposure_view",)


def test_dynamic_portfolio_trims_only_narrative_text() -> None:
    answer = _answer()
    answer["rationale"] = "  Maintain cash while the evidence develops.  "
    answer["transmission"] = ["  release -> cash flow -> exposure  "]
    answer["invalidation_conditions"] = ["  Review after the next release.  "]

    proposal = parse_portfolio_proposal_v4(
        answer,
        binding_hash="a" * 64,
        evidence_ids=frozenset({"account_state", "exposure_view"}),
    )

    assert proposal.rationale == "Maintain cash while the evidence develops."
    assert proposal.transmission == ("release -> cash flow -> exposure",)
    assert proposal.invalidation_conditions == ("Review after the next release.",)
    assert portfolio_proposal_text_normalizations(answer) == (
        {"path": "rationale", "operation": "trim_surrounding_whitespace"},
        {"path": "transmission[0]", "operation": "trim_surrounding_whitespace"},
        {
            "path": "invalidation_conditions[0]",
            "operation": "trim_surrounding_whitespace",
        },
    )

    answer["evidence_refs"] = ["account_state "]
    with pytest.raises(ValueError, match="references must be strings"):
        parse_portfolio_proposal_v4(
            answer,
            binding_hash="a" * 64,
            evidence_ids=frozenset({"account_state", "exposure_view"}),
        )


@pytest.mark.parametrize("mutation", ["account", "quantity", "kill", "expiry", "reject"])
def test_manual_review_revalidates_authority_and_releases_unsubmitted_reserves(
    tmp_path: Path, native_portfolio: NativePortfolio, mutation: str
):
    profile, answer, _ = native_portfolio
    answer[0] = _answer("open")
    authority, inputs, accounts, _, clock, open_service, _ = _setup(tmp_path)

    async def scenario():
        model = PiRuntimeProvider(profile)
        service, mock = open_service()
        try:
            result = await run_portfolio_review_pipeline(
                authority=authority, provider=model, run_id="manual-review", paper_service=service
            )
            assert result.operation is not None, result.terminal
            client_id = result.operation.client_order_id
            if mutation == "account":
                clock[0] += timedelta(seconds=1)
                accounts[0] = mock.simulated_account_snapshot(price_bases=inputs[0].price_bases)
            elif mutation == "quantity":
                with sqlite3.connect(authority.store.index_path) as connection:
                    order = replace(
                        authority.execution_admission("manual-review").order, quantity=Decimal(1)
                    )
                    order_hash = authority.store.artifacts.put_json(order.to_dict()).content_hash
                    connection.execute(
                        "UPDATE autonomous_operations SET order_hash = ?", (order_hash,)
                    )
            elif mutation == "kill":
                service.activate_kill("provider_loss")
            elif mutation == "expiry":
                clock[0] = inputs[0].expires_at
            if mutation == "reject":
                service.decide_portfolio_approval(
                    client_id, approved=False, actor_ref="synthetic-human"
                )
            else:
                with pytest.raises((PermissionError, ValueError)):
                    service.decide_portfolio_approval(
                        client_id, approved=True, actor_ref="synthetic-human"
                    )
            assert service.dispatch_next() is None
            assert mock.reconcile().receipts == ()
            if mutation in {"expiry", "reject"}:
                assert service.get(client_id).state is AutonomousOperationState.BLOCKED
                with sqlite3.connect(authority.store.index_path) as connection:
                    assert connection.execute(
                        "SELECT reservation_active, submission_consumed FROM autonomous_operations"
                    ).fetchone() == (0, 0)
        finally:
            service.close()
            await model.close()

    asyncio.run(scenario())


def test_native_portfolio_compacts_only_large_record_lineage_and_replays(
    tmp_path: Path, native_portfolio: NativePortfolio, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile, _, spawns = native_portfolio
    authority, inputs, _, _, _, _, _ = _setup(tmp_path)
    hashes = [canonical_hash({"record": index}) for index in range(22000)]
    sources = (
        {
            "symbol": TARGET,
            "source_record_hashes": hashes,
            "economic_metadata": {"lot_size": 100, "price_limit": "0.10"},
        },
    )
    inputs[0] = replace(inputs[0], rule_set=replace(inputs[0].rule_set, source_documents=sources))
    original = inputs[0].to_dict()
    capture = tmp_path / "native-request.json"
    monkeypatch.setenv("PORTFOLIO_FIXTURE_REQUEST_PATH", str(capture))

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile)
        try:
            terminal = await authority.review_account(run_id="large-lineage", provider=provider)
            assert terminal["status"] == "completed"
            binding = cast(
                dict[str, object],
                authority.store.artifacts.read_json(
                    authority.journal.get_run("large-lineage").config_hash
                ),
            )
            projection = portfolio_prompt_projection(binding)
            assert binding["prompt_projection"] == projection
            assert projection["schema_version"] == PORTFOLIO_PROMPT_PROJECTION_VERSION
            assert binding["inputs"] == original == inputs[0].to_dict()
            projected_inputs = cast(dict[str, Any], projection["inputs"])
            source = projected_inputs["rule_set"]["source_documents"][0]
            compact = source["source_record_hashes_provenance"]
            assert compact["count"] == 22000
            assert compact["content_hash"] == canonical_hash(hashes)
            reopened = authority.store.artifacts.read_json(compact["inputs_artifact_hash"])
            assert reopened == original
            assert compact["json_pointer"] == "/rule_set/source_documents/0/source_record_hashes"
            restored_source = dict(source)
            del restored_source["source_record_hashes_provenance"]
            restored_source["source_record_hashes"] = hashes
            assert restored_source == sources[0]
            restored_inputs = dict(projected_inputs)
            restored_inputs["rule_set"] = {
                **projected_inputs["rule_set"],
                "source_documents": [restored_source],
            }
            assert restored_inputs == original
            # Assert the physical native request, not a test-only context estimate.
            assert len(json.dumps(original).encode()) > 1_400_000
            assert capture.stat().st_size < 30_000
            native_request = json.loads(capture.read_text())
            user = next(item for item in native_request["input"] if item.get("role") == "user")
            content = user["content"]
            text = (
                content if isinstance(content, str) else "".join(item["text"] for item in content)
            )
            assert json.loads(text) == projection
            assert authority.replay("large-lineage") == terminal
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())
