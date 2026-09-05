from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.autonomous_paper import AutonomousOperationState
from market_impact_agent.data_inputs import DataPITLane, FrozenDataSnapshotInput
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
from market_impact_agent.prospective_discovery_runtime import (
    ProspectiveDiscoveryResult,
    run_prospective_discovery,
)
from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition
from market_impact_agent.prospective_mock_execution import (
    dispatch_prospective_mock_review,
    open_prospective_mock_execution,
    reconcile_prospective_mock_review,
)
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)

from .test_ashare_security_qualification import capture_rows
from .test_pi_runtime import pi_profile
from .test_prospective_ashare_quotes import CUTOFF, executable_inputs
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]
from .test_tushare_observation import (  # pyright: ignore[reportPrivateUsage]
    TOKEN,
    FakeTransport,
    _response,  # pyright: ignore[reportPrivateUsage]
)


@pytest.mark.parametrize("fresh_seed", [True, False])
def test_native_current_cny_mock_composition_and_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fresh_seed: bool
) -> None:
    market = executable_inputs(tmp_path, symbol="600519.SH")
    seed = executable_inputs(
        tmp_path,
        symbol="510300.SH",
        etf=True,
        quote_time="2026-08-31 09:30:00" if fresh_seed else "2026-08-28 15:00:00",
    )
    store = market.store
    frozen = FrozenDataSnapshotInput(frozenset((*market.snapshot_ids[1:], *seed.snapshot_ids)))
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="prospective-budget", config_hash=canonical_hash("study"), created_at=CUTOFF
    )
    budget = ModelBudget(journal, "prospective-budget", 12, 1000000)
    clock = [CUTOFF]

    def now():
        clock[0] += timedelta(microseconds=1)
        return clock[0]

    def factory(value: FrozenDataSnapshotInput) -> ExecutableProspectiveAShareInputs:
        return ExecutableProspectiveAShareInputs(
            store=store,
            snapshot_ids=tuple(sorted(value.authorized_snapshot_ids)),
            qualification_policy=market.qualification_policy,
        )

    def compose() -> ProspectiveMockComposition:
        return ProspectiveMockComposition(
            store=store,
            profile_id="model",
            study_registration_id="study",
            opening_authority_ref="study",
            parent_run_id="prospective-budget",
            market_factory=factory,
            clock=now,
        )

    composition = compose()
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-stock-basic-v1.json")
    )
    source = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport(
            [
                _response(
                    config.fields,
                    [
                        [
                            {
                                "ts_code": "600519.SH",
                                "symbol": "600519",
                                "name": "Synthetic Mainboard",
                                "exchange": "SSE",
                                "list_status": "L",
                                "list_date": "20100101",
                            }.get(field)
                            for field in config.fields
                        ]
                    ],
                )
            ]
        ),
        clock=now,
    )
    templates = (ResearchSourceTemplate.from_tushare(source, config.source_id),)
    inputs = ResearchThesisRunInputs(
        _repository("ASHARE.RESEARCH", at=CUTOFF, event_id="news-discovery"),
        "ASHARE.RESEARCH",
        "epoch",
        frozenset({1}),
    )
    authority = ResearchThesisAuthority(
        store,
        experiment_id="prospective",
        arm_id="model",
        account_scope=composition.account_scope,
        clock=now,
    )
    acquisition = OnDemandResearch(
        store=store,
        parent_budget=budget,
        episode_deadline=CUTOFF + timedelta(hours=1),
        episode_id="prospective-case",
        run_id="prospective-initial",
        cutoff=CUTOFF,
        pit_lane=DataPITLane.PROSPECTIVE,
        templates=templates,
        frozen_input=frozen,
        clock=now,
    )
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-key")

    def installed(_: Path) -> PiRuntimePermit:
        return PiRuntimePermit(
            canonical_hash(runtime_identity()), (profile.route_identity,), "fixture"
        )

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    wire = tmp_path / "wire.mjs"
    wire.write_text(
        Path(__file__)
        .with_name("prospective_discovery_network.mjs")
        .read_text()
        .replace("000001.SZ", "600519.SH")
        .replace("XSHE", "XSHG")
        .replace("Synthetic discovered company", "Synthetic Mainboard")
    )
    original = asyncio.create_subprocess_exec

    async def spawn(program: str, *args: str, **kwargs: Any):
        return await original(program, "--import", str(wire), *args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    async def run() -> ProspectiveDiscoveryResult:
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            return await run_prospective_discovery(
                authority=authority,
                provider=provider,
                inputs=inputs,
                acquisition=acquisition,
                account_source=composition.account_source,
                account_max_age=timedelta(minutes=5),
                admission_authority_factory=composition.admission_source,
                portfolio_authority_factory=composition.portfolio_authority,
                portfolio_context_source=composition.capture_context,
                maximum_runs=3,
            )
        finally:
            await provider.close()

    result = asyncio.run(run())
    if not fresh_seed:
        assert result.status == "admission_refused", result.to_dict()
        assert any("current_quote_stale" in gap for gap in result.gaps)
        assert result.portfolio_run_id is None
        return
    assert result.status == "portfolio_completed", result.to_dict()
    assert result.candidate == "600519.SH"
    assert len(result.acquisition.run_ids) == 2
    assert composition.inputs is not None
    current = composition.inputs
    assert current.mandate.allowed_instruments == {"600519.SH", "510300.SH"}
    assert current.account_state.positions and current.account_state.positions[0].quantity > 0
    assert current.account_state.as_of <= current.cutoff
    assert result.thesis_run_id is not None
    assert journal.get_run(result.thesis_run_id).updated_at < current.cutoff
    assert current.account_state.cash and current.account_state.cash[0].currency == "CNY"
    clock[0] += timedelta(seconds=10)
    reopened = compose()
    assert result.acquisition.frozen_input is not None
    assert reopened.capture_context(
        result.acquisition.final_inputs, result.acquisition.frozen_input
    ) == (current.account_state, current.cutoff)
    assert result.portfolio_run_id is not None

    final_frozen: FrozenDataSnapshotInput = result.acquisition.frozen_input
    final_inputs: ResearchThesisRunInputs = result.acquisition.final_inputs

    def rebuild(
        captured_input: FrozenDataSnapshotInput = final_frozen,
        research_input: ResearchThesisRunInputs = final_inputs,
    ) -> ProspectiveMockComposition:
        rebuilt = compose()
        rebuilt.capture_context(research_input, captured_input)
        security = DynamicAShareAdmission(factory(captured_input)).discover(
            (research_input.target_id,), current.cutoff
        )[0]
        rebuilt.portfolio_authority(
            research_input,
            captured_input,
            current.account_state,
            security,
        )
        assert rebuilt.inputs == current
        return rebuilt

    service = open_prospective_mock_execution(composition)
    try:
        operation = service.admit_portfolio_review(result.portfolio_run_id)
        assert operation.state is AutonomousOperationState.QUEUED
        dispatched = service.dispatch_next()
        assert dispatched is not None and dispatched.state is AutonomousOperationState.ACCEPTED, (
            service.active_kill_reasons
        )
        assert service.dispatch_next() is None
        # A process dies after durable ACK, before the caller saves its outcome.
        service.close()
        composition = rebuild()
        resumed_ack = dispatch_prospective_mock_review(composition, result.portfolio_run_id)
        assert resumed_ack["execution_status"] == "accepted", resumed_ack
        assert resumed_ack["client_order_id"] == operation.client_order_id
        service = open_prospective_mock_execution(composition)
        before = composition.provider.simulated_account_snapshot(price_bases=current.price_bases)
        assert before.cash == current.account_state.cash
        assert all(item.target_id != "600519.SH" for item in before.positions or ())
        # Explicit Mock venue facts; acceptance/ACK alone did not change cash.
        clock[0] += timedelta(seconds=1)
        composition.provider.record_simulated_fill(
            operation.client_order_id,
            fill_id="current-cny-partial",
            quantity=Decimal(100),
            price=Decimal("10.01"),
            fee=Decimal(5),
            sellable_at=CUTOFF + timedelta(days=1),
        )
        first = composition.provider.simulated_account_snapshot(price_bases=current.price_bases)
        composition.provider.record_simulated_fill(
            operation.client_order_id,
            fill_id="current-cny-partial",
            quantity=Decimal(100),
            price=Decimal("10.01"),
            fee=Decimal(5),
            sellable_at=CUTOFF + timedelta(days=1),
        )
        repeated = composition.provider.simulated_account_snapshot(price_bases=current.price_bases)
        assert repeated.cash == first.cash
        assert repeated.positions == first.positions
        assert composition.provider.simulated_sellable_quantity("600519.SH") == 0
        reconciled = service.reconcile()
        assert reconciled.complete, reconciled
    finally:
        service.close()
    composition = rebuild()
    resumed = dispatch_prospective_mock_review(composition, result.portfolio_run_id)
    assert resumed["execution_status"] in {"accepted", "reconciled"}, resumed
    assert resumed["client_order_id"] == operation.client_order_id
    assert composition.provider.simulated_sellable_quantity("600519.SH") == 0

    # Recovery is available after quote expiry; stale evidence grants no new authority.
    clock[0] += timedelta(minutes=3)
    composition = rebuild()
    expired = dispatch_prospective_mock_review(composition, result.portfolio_run_id)
    assert expired["execution_dispatched"] is True, expired
    assert expired["client_order_id"] == operation.client_order_id
    recovered = open_prospective_mock_execution(composition)
    try:
        assert recovered.get(operation.client_order_id).state is AutonomousOperationState.RECONCILED
        with pytest.raises(PermissionError):
            recovered.admit_portfolio_review(result.portfolio_run_id)
        pending = recovered.reconcile()
        assert not pending.complete
        assert "fresh_account_exposure_rebuild_required" in pending.gaps
    finally:
        recovered.close()

    # Newly observed source quotes can reconcile actual fills while the old review expires.
    refreshed_ids: list[str] = []
    for symbol, api in (("600519.SH", "rt_min"), ("510300.SH", "rt_etf_min")):
        refreshed_ids.append(
            capture_rows(
                store,
                api,
                {"ts_code": symbol, "freq": "1MIN"},
                [
                    {
                        "ts_code": symbol,
                        "time": "2026-08-31 09:34:00",
                        "open": "10",
                        "close": "10.01",
                        "high": "10.02",
                        "low": "9.99",
                        "vol": "10000",
                        "amount": "100100",
                    }
                ],
                now(),
            )
        )
    fresh_input = FrozenDataSnapshotInput(
        frozenset(
            (
                *result.acquisition.frozen_input.authorized_snapshot_ids,
                *refreshed_ids,
            )
        )
    )
    recovered = open_prospective_mock_execution(composition, reconciliation_input=fresh_input)
    try:
        completed = recovered.reconcile()
        assert completed.complete, completed
        assert composition.inputs == current
        with pytest.raises(PermissionError):
            recovered.admit_portfolio_review(result.portfolio_run_id)
    finally:
        recovered.close()
    continued = reconcile_prospective_mock_review(composition, result.portfolio_run_id, fresh_input)
    assert continued["fill_status"] == "pending_source_fill"
    assert continued["fill_gaps"] == ["unfilled_accepted_order_required"]
    assert continued["reconciliation_complete"] is True
