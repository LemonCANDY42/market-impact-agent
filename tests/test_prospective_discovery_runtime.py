"""Production-shaped source discovery: native tools, real CAS, signed successor and policy."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.account_state import AccountStateSnapshot
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.continuous_decision import ReviewFrame
from market_impact_agent.continuous_portfolio_runtime import (
    ContinuousPortfolioRuntime,
    build_continuous_review_frame,
)
from market_impact_agent.data_inputs import (
    DataPITLane,
    FrozenDataSnapshotInput,
    LocalDataSnapshotStore,
)
from market_impact_agent.domain import ApprovalMode, Side, TradingEnvironment, TradingMandateV3
from market_impact_agent.dynamic_ashare_admission import (
    HistoricalSecurityEvidenceAuthority,
    SecurityAdmission,
)
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.portfolio_review import PortfolioReviewAuthority
from market_impact_agent.prospective_discovery_runtime import run_prospective_discovery
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)

from . import test_historical_ashare_inputs as historical_fixture
from . import test_tushare_observation as tushare_fixture
from .test_pi_runtime import pi_profile
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]
from .test_tushare_observation import (  # pyright: ignore[reportPrivateUsage]
    TOKEN,
    FakeTransport,
    _response,  # pyright: ignore[reportPrivateUsage]
)

D = Decimal


@pytest.mark.parametrize(
    "mode",
    ["refused", "qualified", "watch", "account_missing", "no_candidate", "wrong_identity", "wait"],
)
def test_native_discovery_successor_and_source_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    qualified = mode in {"qualified", "watch", "account_missing"}
    cutoff = (
        datetime(2025, 1, 3, 1, 24, tzinfo=UTC)
        if qualified
        else datetime(2026, 9, 5, 4, 6, tzinfo=UTC)
    )
    receipt_at = cutoff + timedelta(minutes=1)
    clock = [receipt_at]
    historical: HistoricalAShareInputs | None = None
    engine: HistoricalStreamingAccount | None = None
    if qualified:
        monkeypatch.setattr(historical_fixture, "RETRIEVED", cutoff)
        monkeypatch.setattr(tushare_fixture, "RETRIEVED", cutoff)
        historical = historical_fixture._source(tmp_path)  # pyright: ignore[reportPrivateUsage]
        store = historical.store
        seed = historical.session("510300.SH", date(2025, 1, 2))
        assert seed.spec is not None and seed.bar is not None
        engine = HistoricalStreamingAccount(
            specs=(seed.spec,),
            journal_path=tmp_path / "account.jsonl",
            account_reference="prospective-case-account",
            account_reference_key=b"a" * 32,
        )
        engine.bootstrap_half_hs300(seed.bar)
    else:
        store = LocalDataSnapshotStore(tmp_path / "authority")
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="discovery-budget", config_hash=canonical_hash("discovery"), created_at=cutoff
    )
    budget = ModelBudget(journal, "discovery-budget", 10, 1_000_000)
    transports: list[FakeTransport] = []
    templates: list[ResearchSourceTemplate] = []
    for api in (
        "stock_basic",
        "daily",
        "suspend_d",
        "stk_limit",
        "trade_cal",
        "dividend",
        "index_member_all",
    ):
        config = load_tushare_observation_source(
            Path(f"examples/providers/tushare-observation-{api.replace('_', '-')}-v1.json")
        )
        row = {
            "ts_code": "000002.SZ" if mode == "wrong_identity" else "000001.SZ",
            "symbol": "000001",
            "name": "Synthetic discovered company",
            "exchange": "SZSE",
            "list_status": "L",
            "list_date": "20100101",
        }
        rows: list[list[object]] = (
            [[row.get(name) for name in config.fields]] if api == "stock_basic" else []
        )

        class SealedTransport(FakeTransport):
            def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
                assert journal.get_run("prospective-initial").status.terminal
                return super().__call__(endpoint, body, timeout_seconds)

        transport = SealedTransport([_response(config.fields, rows)])
        transports.append(transport)
        source_provider = TushareObservationProvider(
            TOKEN, (config,), transport=transport, clock=lambda: receipt_at
        )
        templates.append(ResearchSourceTemplate.from_tushare(source_provider, config.source_id))
    acquisition = OnDemandResearch(
        store=store,
        parent_budget=budget,
        episode_deadline=receipt_at + timedelta(hours=1),
        episode_id="prospective-case",
        run_id="prospective-initial",
        cutoff=cutoff,
        pit_lane=DataPITLane.PROSPECTIVE,
        templates=tuple(templates),
        frozen_input=None
        if historical is None
        else FrozenDataSnapshotInput(frozenset(historical.snapshot_ids)),
        clock=lambda: clock[0],
    )
    if mode == "wait":
        from market_impact_agent.data_acquisition import AcquisitionUncertain
        from market_impact_agent.tushare_range_cache import TushareDailyRangeCache

        persist = TushareDailyRangeCache._persist  # pyright: ignore[reportPrivateUsage]
        interrupted = False

        def interrupted_persist(self: TushareDailyRangeCache, *args: Any) -> str:
            nonlocal interrupted
            digest = persist(self, *args)
            if not interrupted:
                interrupted = True
                raise AcquisitionUncertain("synthetic interruption after durable range receipt")
            return digest

        monkeypatch.setattr(TushareDailyRangeCache, "_persist", interrupted_persist)
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-key")

    def installed(_root: Path) -> PiRuntimePermit:
        return PiRuntimePermit(
            canonical_hash(runtime_identity()), (profile.route_identity,), "fixture"
        )

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec

    async def spawn(program: str, *args: str, **kwargs: Any):
        kwargs["env"]["DISCOVERY_WATCH"] = "1" if mode == "watch" else "0"
        kwargs["env"]["DISCOVERY_NO_CANDIDATE"] = "1" if mode == "no_candidate" else "0"
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("prospective_discovery_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    account_id = "account-ref-" + "a" * 64 if engine is None else engine.account_id
    authority = ResearchThesisAuthority(
        store,
        experiment_id="prospective",
        arm_id="model",
        account_scope=account_id,
        clock=lambda: clock[0],
    )
    inputs = ResearchThesisRunInputs(
        _repository("510300.SH", at=cutoff, event_id="news-discovery"),
        "510300.SH",
        "epoch",
        frozenset({1}),
        research_question=(
            "Identify a company or ETF implicated by the news "
            "and investigate it through the source tools."
        ),
    )
    watch_policy = None
    if mode == "watch":
        from market_impact_agent.agent_watch_admission import (
            WatchDelegateProfile,
            build_callback_agent_profile_ref,
        )
        from market_impact_agent.monitoring_scope import MonitoringSubjectKind, MonitoringSubjectRef
        from market_impact_agent.prospective_data import ProspectiveDataJournal
        from market_impact_agent.research_thesis_watch import (
            RESEARCH_WATCH_PARENT_TYPE,
            ResearchThesisWatchDelegation,
        )

        from . import test_attention_watch as attention_fixture
        from .test_agent_watch_admission import (
            _event_cluster_profile,  # pyright: ignore[reportPrivateUsage]
        )

        monkeypatch.setattr(attention_fixture, "FIRST_RECEIPT", receipt_at)
        monkeypatch.setattr(attention_fixture, "WINDOW_START", cutoff)
        watch_policy = attention_fixture.collection_policy_for_monitoring_test()
        watch_journal = ProspectiveDataJournal(store)
        watch_journal.register_policy(watch_policy)
        baseline = attention_fixture.snapshot_for_monitoring_test(
            store, policy=watch_policy, retrieved_at=receipt_at
        )
        watch_journal.record_snapshot(baseline, policy=watch_policy)
        base_profile = _event_cluster_profile(collection_policy_id=watch_policy.policy_id)
        profile_fields: dict[str, Any] = {
            key: getattr(base_profile, key)
            for key in base_profile.__dataclass_fields__
            if key not in {"profile_id", "execution_capability"}
        }
        profile_fields.update(
            allowed_parent_agent_types=(RESEARCH_WATCH_PARENT_TYPE,),
            preloaded_skills=(),
            skill_manifest_hashes=(),
            callback_max_turns=4,
            callback_max_input_tokens=64000,
            callback_max_output_tokens=16000,
            callback_max_cost_microusd=400000,
        )
        profile_fields["callback_agent_profile_ref"] = build_callback_agent_profile_ref(
            callback_agent_type=base_profile.callback_agent_type,
            model_profile_id=profile.profile_id,
            model_profile_hash=profile.profile_hash,
            preloaded_skills=(),
            skill_manifest_hashes=(),
            max_turns=4,
            max_input_tokens=64000,
            max_output_tokens=16000,
            max_cost_microusd=400000,
        )
        watch_profile = WatchDelegateProfile.build(**profile_fields)
        inputs = replace(
            inputs,
            watch_delegation=ResearchThesisWatchDelegation.bind(
                acquisition,
                subject=MonitoringSubjectRef(MonitoringSubjectKind.EVENT_CLUSTER, "news-discovery"),
                matcher_terms=("policy", "decision"),
                profiles=(watch_profile,),
            ),
        )
    original_pack = inputs.repository.evidence_pack.to_dict()
    account_reads: list[bool] = []

    def account_source() -> AccountStateSnapshot:
        account_reads.append(True)
        if mode == "account_missing":
            raise PermissionError("No authoritative current account is configured")
        assert engine is not None, "unqualified source must not access or invent an account"
        return engine.results[-1].account_state

    def admission_source(
        final: ResearchThesisRunInputs, frozen: FrozenDataSnapshotInput
    ) -> HistoricalSecurityEvidenceAuthority:
        if historical is not None:
            assert set(historical.snapshot_ids) <= frozen.authorized_snapshot_ids
            return historical
        return HistoricalAShareInputs(
            store=store,
            snapshot_ids=tuple(sorted(frozen.authorized_snapshot_ids)),
            rule_artifact_hashes=(),
            policy=ModeledHistoricalPolicy("current-refusal-only", D("0.01")),
        )

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)

        def portfolio_source(
            final: ResearchThesisRunInputs,
            frozen: FrozenDataSnapshotInput,
            account: AccountStateSnapshot,
            security: SecurityAdmission,
        ) -> PortfolioReviewAuthority:
            assert engine is not None and historical is not None and security.execution_ready

            market = historical

            def mandate(frame: ReviewFrame) -> TradingMandateV3:
                return TradingMandateV3(
                    mandate_id="template",
                    account_id=account.account_reference_hash,
                    harness_authority_id=store.harness_authority_id,
                    environment=TradingEnvironment.BACKTEST,
                    approval_mode=ApprovalMode.MANUAL_EACH,
                    valid_from=frame.cutoff,
                    valid_until=frame.cutoff + timedelta(minutes=10),
                    allowed_instruments=frozenset({"510300.SH", "000001.SZ"}),
                    allowed_instrument_classes=frozenset(
                        {"cash_equity", "unlevered_exchange_traded_fund"}
                    ),
                    allowed_sides=frozenset({Side.BUY, Side.SELL}),
                    currency="CNY",
                    gross_exposure_limit=D(100000),
                    minimum_net_exposure=D(0),
                    maximum_net_exposure=D(100000),
                    maximum_position_count=5,
                    maximum_single_position_fraction=D(1),
                    daily_turnover_limit=D(100000),
                    daily_submission_limit=10,
                    daily_loss_kill_threshold=D(10000),
                    strategy_peak_drawdown_kill_threshold=D(20000),
                    universe_binding_hash="0" * 64,
                )

            runtime = ContinuousPortfolioRuntime(
                store=store,
                experiment_id="prospective",
                arm_id="model",
                account=engine,
                research_repository=lambda _: final.repository,
                market_inputs=lambda _: market,
                mandate_template=mandate,
                symbols=lambda _: ("510300.SH", "000001.SZ"),
                account_max_age=lambda _: timedelta(days=2),
                provider=provider,
            )
            frame = build_continuous_review_frame(repository=final.repository, market=historical)
            return runtime._portfolio_authority(frame)  # pyright: ignore[reportPrivateUsage]

        try:
            result = await run_prospective_discovery(
                authority=authority,
                provider=provider,
                inputs=inputs,
                acquisition=acquisition,
                account_source=account_source,
                account_max_age=timedelta(days=2),
                admission_authority_factory=admission_source,
                portfolio_authority_factory=portfolio_source if qualified else None,
                maximum_runs=4 if qualified else 2,
            )
            if mode == "wait":
                assert result.acquisition.status == "acquisition_wait"
                assert result.acquisition.run_ids == ("prospective-initial",)
                original_proof = result.proof_artifact_hash
                assert budget.summary()["physical_requests"] == 1
                received_calls = [len(t.requests) for t in transports]
                result = await run_prospective_discovery(
                    authority=authority,
                    provider=provider,
                    inputs=inputs,
                    acquisition=acquisition,
                    account_source=account_source,
                    account_max_age=timedelta(days=2),
                    admission_authority_factory=admission_source,
                    maximum_runs=2,
                )
                assert [len(t.requests) for t in transports] == received_calls
                reports = [
                    event
                    for event in journal.events(budget.owner_run_id)
                    if event.event_type == "prospective.discovery.reported"
                ]
                assert len(reports) == 2
                assert reports[0].payload["proof_artifact_hash"] == original_proof
                assert reports[1].payload["previous_report_event_id"] == reports[0].event_id
            assert inputs.repository.evidence_pack.to_dict() == original_pack
            if mode in {"no_candidate", "wrong_identity"}:
                assert result.status == "incomplete" and result.candidate is None
                assert result.portfolio_run_id is None and account_reads == []
                assert budget.summary()["physical_requests"] == 1
            else:
                assert result.candidate == "000001.SZ", result.to_dict()
                assert result.acquisition.final_inputs.target_id == "000001.SZ"
                assert (
                    "000001.SZ"
                    in result.acquisition.final_inputs.repository.evidence_pack.allowed_targets
                )
                assert result.thesis_run_id == "prospective-initial.continuation.1"
                assert (
                    authority.replay("prospective-initial")["reason"]
                    == "ResearchAcquisitionRequired"
                )
                assert len([t for t in transports if t.requests]) == 7
                assert result.status == (
                    "portfolio_completed" if mode in {"qualified", "watch"} else "admission_refused"
                ), result.to_dict()
                assert budget.summary()["physical_requests"] == (
                    5 if mode == "watch" else 4 if mode == "qualified" else 3
                )
                if mode == "account_missing":
                    assert result.gaps == ("account_authority_missing",)
                    assert account_reads == [True] and result.portfolio_run_id is None
                if not qualified:
                    assert account_reads == [] and result.portfolio_run_id is None and result.gaps
            if mode == "watch":
                from market_impact_agent.agent_watch_admission import AgentWatchAdmissionService
                from market_impact_agent.agent_watch_wake_dispatch import AgentWatchWakeDispatcher
                from market_impact_agent.prospective_watch_review import (
                    run_prospective_watch_review,
                )
                from market_impact_agent.research_thesis_watch import (
                    ResearchThesisWatchAuthorityResolver,
                )

                from . import test_attention_watch as attention_fixture

                assert watch_policy is not None and historical is not None
                assert len(result.watch_admission_ids) == 1
                resolver = ResearchThesisWatchAuthorityResolver(
                    store,
                    experiment_id=authority.experiment_id,
                    arm_id=authority.arm_id,
                    account_scope=account_id,
                    target_id="000001.SZ",
                    parent_budget=budget,
                    episode_id=acquisition.episode_id,
                    clock=lambda: clock[0],
                )
                watches = AgentWatchAdmissionService(
                    store, profiles=(), delegation_authority=resolver
                )
                admitted = watches.admission(result.watch_admission_ids[0])
                assert admitted.watch_id is not None
                clock[0] = receipt_at + timedelta(minutes=1)
                changed = attention_fixture.snapshot_for_monitoring_test(
                    store,
                    policy=watch_policy,
                    retrieved_at=clock[0],
                    headline="Policy decision revised",
                    raw_record=b'{"headline":"Policy decision revised"}',
                )
                watches.journal.record_snapshot(changed, policy=watch_policy)
                poll = watches.watch_service.run_due_from_snapshot(
                    admitted.watch_id, now=clock[0], collection_snapshot_id=changed.snapshot_id
                )
                assert poll.wake is not None
                dispatcher = AgentWatchWakeDispatcher(
                    watches, run_journal=RunJournal(tmp_path / "watch-dispatch.sqlite3")
                )
                dispatch = dispatcher.dispatch_wake(poll.wake, dispatched_at=clock[0])[0]
                callback_args: dict[str, Any] = dict(
                    dispatcher=dispatcher,
                    resolver=resolver,
                    run_id=dispatch.run.run_id,
                    provider=provider,
                    account_source=account_source,
                    account_max_age=timedelta(days=2),
                    admission_authority_factory=admission_source,
                    portfolio_authority_factory=portfolio_source,
                    source_snapshot_ids=historical.snapshot_ids,
                )
                followup = await run_prospective_watch_review(**callback_args)
                assert followup["status"] == "portfolio_completed", followup
                followup_run = str(followup["thesis_run_id"])
                followup_binding = cast(
                    dict[str, Any],
                    store.artifacts.read_json(journal.get_run(followup_run).config_hash),
                )
                assert followup_binding["prior_thesis"]["run_id"] == result.thesis_run_id
                assert followup_binding["account_scope"] == account_id
                assert followup_binding["profile"]["reasoning_effort"] == profile.reasoning_effort
                assert followup_binding["profile"]["pricing"] == profile.pricing.to_dict()
                assert followup_binding["profile"]["budget"]["max_turns"] <= 2
                after_callback = budget.summary()
                assert after_callback["physical_requests"] == 7
                assert await run_prospective_watch_review(**callback_args) == followup
                assert budget.summary() == after_callback
            before = [len(t.requests) for t in transports], budget.summary()
            clock[0] = acquisition.deadline + timedelta(seconds=1)
            replay = await run_prospective_discovery(
                authority=authority,
                provider=provider,
                inputs=inputs,
                acquisition=acquisition,
                account_source=account_source,
                account_max_age=timedelta(days=2),
                admission_authority_factory=admission_source,
                portfolio_authority_factory=portfolio_source if qualified else None,
                maximum_runs=4 if qualified else 2,
            )
            assert replay.to_dict() == result.to_dict()
            assert ([len(t.requests) for t in transports], budget.summary()) == before
        finally:
            await provider.close()

    try:
        asyncio.run(scenario())
    finally:
        if engine is not None:
            engine.close()
