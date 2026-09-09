# pyright: reportPrivateUsage=false
"""V2 acceptance: real pinned pi, source CAS and Nautilus; fake wire only."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.continuous_decision import ContinuousDecision, ReviewFrame
from market_impact_agent.continuous_portfolio_runtime import (
    ContinuousPortfolioRuntime,
    build_continuous_review_frame,
)
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.domain import ApprovalMode, Side, TradingEnvironment, TradingMandateV3
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.nautilus_backtest import AShareDailyBar
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import reopen_completed_research_thesis
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)

from .test_ashare_security_qualification import accepted_policy, capture_rows
from .test_pi_runtime import pi_profile
from .test_tushare_observation import RETRIEVED, TOKEN, FakeTransport, _response

D = Decimal


@pytest.mark.parametrize("mode", ["cache", "acquisition", "unknown"])
def test_v2_candidate_to_reconciled_execution_and_offline_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "source")
    snapshots: list[str] = []
    for symbol, exchange, price, etf in (
        ("510300.SH", "SSE", 4, True),
        ("000001.SZ", "SZSE", 10, False),
    ):
        date_params: dict[str, object] = dict(
            ts_code=symbol, start_date="20260831", end_date="20260901"
        )

        def capture(api: str, params: dict[str, object], rows: list[dict[str, object]]) -> None:
            received = (
                RETRIEVED
                if api in {"stock_basic", "etf_basic", "fund_basic"}
                else datetime(2026, 9, 2, 8, tzinfo=UTC)
            )
            snapshots.append(capture_rows(store, api, params, rows, received))

        capture(
            "etf_basic" if etf else "stock_basic",
            {"ts_code": symbol},
            [
                dict(
                    ts_code=symbol,
                    symbol=symbol[:6],
                    name="Synthetic",
                    csname="Synthetic",
                    exchange=exchange,
                    list_date="20100101",
                    list_status="L",
                    etf_type="境内",
                    index_code="000300.SH",
                )
            ],
        )
        if etf:
            capture(
                "fund_basic",
                {"ts_code": symbol},
                [
                    dict(
                        ts_code=symbol,
                        fund_type="股票型",
                        market="E",
                        status="L",
                        list_date="20100101",
                    )
                ],
            )
        capture(
            "fund_daily" if etf else "daily",
            date_params,
            [
                dict(
                    ts_code=symbol,
                    trade_date=day,
                    pre_close=price,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    change=0,
                    pct_chg=0,
                    vol=200000,
                    amount=80000,
                )
                for day in ("20260831", "20260901")
            ],
        )
        capture(
            "trade_cal",
            dict(exchange=exchange, start_date="20260824", end_date="20260901"),
            [
                dict(exchange=exchange, cal_date=day, is_open=1, pretrade_date=previous)
                for day, previous in (
                    ("20260824", "20260821"),
                    ("20260825", "20260824"),
                    ("20260826", "20260825"),
                    ("20260827", "20260826"),
                    ("20260828", "20260827"),
                    ("20260831", "20260828"),
                    ("20260901", "20260831"),
                )
            ],
        )
        capture(
            "suspend_d",
            date_params,
            [
                dict(ts_code=symbol, trade_date=day, suspend_type="R", suspend_timing=None)
                for day in ("20260831", "20260901")
            ],
        )
        capture(
            "stk_limit",
            date_params,
            [
                dict(
                    ts_code=symbol,
                    trade_date=day,
                    pre_close=price,
                    up_limit=price * 1.1,
                    down_limit=price * 0.9,
                )
                for day in ("20260831", "20260901")
            ],
        )
        capture(
            "fund_adj" if etf else "adj_factor",
            dict(ts_code=symbol, start_date="20260828", end_date="20260901"),
            [
                dict(ts_code=symbol, trade_date=day, adj_factor=1)
                for day in ("20260828", "20260831", "20260901")
            ],
        )
        capture("fund_div" if etf else "dividend", {"ts_code": symbol}, [])
    source = HistoricalAShareInputs(
        store=store,
        snapshot_ids=tuple(snapshots),
        rule_artifact_hashes=(),
        qualification_policy=accepted_policy(store, RETRIEVED),
        policy=ModeledHistoricalPolicy(
            "continuous-v2-test", D("0.01"), research_projection="dynamic_ashare_sources_v1"
        ),
    )
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-v2-key")
    permit = PiRuntimePermit(
        canonical_hash(runtime_identity()), (profile.route_identity,), "fixture"
    )

    def installed(_root: Path) -> PiRuntimePermit:
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec
    spawns: list[str] = []

    async def spawn(program: str, *args: str, **kwargs: Any):
        spawns.append(program)
        kwargs["env"]["V2_MODE"] = mode
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("continuous_v2_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-stock-basic-v1.json")
    )
    row = dict(
        ts_code="000001.SZ",
        symbol="000001",
        name="Synthetic",
        exchange="SZSE",
        list_status="L",
        list_date="20100101",
    )
    transport = FakeTransport(
        [_response(config.fields, [[row.get(field) for field in config.fields]])]
    )
    raw = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: datetime(2026, 9, 1, 1, 25, tzinfo=UTC)
    )
    seed = source.session("510300.SH", date(2026, 8, 31))
    assert seed.spec is not None and seed.bar is not None
    engine = HistoricalStreamingAccount(
        specs=(seed.spec,),
        journal_path=tmp_path / "account.jsonl",
        account_reference="v2-account",
        account_reference_key=b"a" * 32,
    )
    engine.bootstrap_half_hs300(seed.bar)
    journal = RunJournal.authoritative(source.store)
    journal.start_run(run_id="budget", config_hash=canonical_hash("v2"), created_at=RETRIEVED)
    budget = ModelBudget(journal, "budget", max_requests=20, max_cost_microusd=1000000)
    cutoff = datetime(2026, 9, 1, 1, 25, tzinfo=UTC)
    if mode == "cache":

        async def seed_profile() -> None:
            nonlocal source
            seed_research = OnDemandResearch(
                store=store,
                parent_budget=budget,
                episode_deadline=cutoff + timedelta(hours=1),
                run_id="seed-profile",
                cutoff=cutoff,
                pit_lane=DataPITLane.PROSPECTIVE,
                templates=(ResearchSourceTemplate.from_tushare(raw, config.source_id),),
                clock=lambda: cutoff,
            )
            await seed_research.request("lookup_company_profile", {"ts_code": "000001.SZ"})
            receipts = await seed_research.fulfill_pending()
            source = source.with_snapshots(
                tuple(seed_research.successor_input(receipts)[1].authorized_snapshot_ids)
            )

        asyncio.run(seed_profile())
    prior_source_calls = len(transport.requests)
    documents: dict[str, dict[str, object]] = {
        "release": {
            "published_at": (cutoff - timedelta(minutes=10)).isoformat(),
            "industry": "electronics",
            "headline": "Industry orders exceed prior expectations.",
            "company_exposure": {
                "000001.SZ": "Synthetic supplier earns all revenue from these orders."
            },
        },
        "market": {
            "as_of": (cutoff - timedelta(minutes=2)).isoformat(),
            "prior_industry_orders": 100,
            "reported_industry_orders": 112,
        },
    }
    repository = FrozenResearchRepository(
        evidence_pack=EvidencePack.build(
            event_id="industry-orders",
            as_of=cutoff,
            research_question="Which security has a defensible exposure to the industry release?",
            evidence=tuple(
                EvidenceReference(
                    key,
                    "industry-evidence",
                    "fixture://" + key,
                    EvidenceTier.OFFICIAL,
                    cutoff - timedelta(minutes=10),
                    canonical_hash(value),
                    "Synthetic frozen industry evidence.",
                )
                for key, value in documents.items()
            ),
            pattern_packs=(),
            allowed_targets=("industry:electronics",),
            data_gaps=(),
        ),
        evidence_documents=documents,
        pattern_packs={},
    )
    frame = build_continuous_review_frame(repository=repository, market=source)

    def mandate(frame: ReviewFrame) -> TradingMandateV3:
        return TradingMandateV3(
            mandate_id="registered-cny-template",
            account_id=engine.account_id,
            harness_authority_id=source.store.harness_authority_id,
            environment=TradingEnvironment.BACKTEST,
            approval_mode=ApprovalMode.MANUAL_EACH,
            valid_from=frame.cutoff,
            valid_until=frame.cutoff + timedelta(minutes=10),
            allowed_instruments=frozenset({"510300.SH", "000001.SZ"}),
            allowed_instrument_classes=frozenset({"cash_equity", "unlevered_exchange_traded_fund"}),
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

    def runtime(provider: PiRuntimeProvider) -> ContinuousPortfolioRuntime:
        return ContinuousPortfolioRuntime(
            store=source.store,
            experiment_id="v2",
            arm_id="model",
            account=engine,
            research_repository=lambda _: repository,
            market_inputs=lambda _: source,
            mandate_template=mandate,
            symbols=lambda _: ("510300.SH",),
            account_max_age=lambda _: timedelta(days=4),
            provider=provider,
            protocol_version="research-continuous-v2",
            historical_research_templates=(
                ResearchSourceTemplate.from_tushare(raw, config.source_id),
            ),
            research_episode_deadline=datetime(2026, 9, 1, 2, tzinfo=UTC),
            acquisition_clock=lambda: datetime(2026, 9, 1, 1, 25, tzinfo=UTC),
        )

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)
        owner = runtime(provider)
        try:
            decision = await owner.decide(frame, None, "review", frozenset({1}), False)
            assert isinstance(decision, ContinuousDecision), decision
            context, effective = owner.resolve_decision_context(decision, frame)
            binding = cast(
                dict[str, Any],
                source.store.artifacts.read_json(
                    journal.get_run(decision.research_run_id).config_hash
                ),
            )
            assert binding["inputs"]["schema_version"] == "market-impact.research-thesis-inputs.v2"
            proofs = binding["inputs"]["candidate_proofs"]
            thesis, _ = reopen_completed_research_thesis(
                journal=journal, artifact_store=store.artifacts, run_id=decision.research_run_id
            )
            comparisons = cast(list[dict[str, Any]], thesis.to_dict()["candidate_comparisons"])
            assert thesis.to_dict()["schema_version"] == "market-impact.research-thesis.v2"
            assert len(comparisons) == (0 if mode == "unknown" else 1)
            if mode != "unknown":
                assert comparisons[0]["candidate_ref"] == "000001.SZ"
                assert "release" in comparisons[0]["support_refs"]
            else:
                assert thesis.base_case_direction == "unknown"
            if mode == "unknown":
                assert proofs == {}
                assert decision.action == "hold"
                assert owner.admitted_intents(decision, frame) == ()
            else:
                assert set(proofs) == {"000001.SZ"}
                assert (
                    "000001.SZ"
                    in context.repository_source(effective).evidence_pack.allowed_targets
                )
                assert "000001.SZ" in context.symbol_source(effective)
                assert decision.action == "open"
            orders = owner.admitted_intents(decision, frame)
            await provider.close()
            before = (budget.summary(), len(spawns), len(transport.requests))
            replay_provider = PiRuntimeProvider(profile, budget=budget, dispatch_allowed=False)
            reopened = runtime(replay_provider)
            try:
                assert (
                    await reopened.decide(frame, None, "review", frozenset({1}), True) == decision
                )
                replay_context, replay_frame = reopened.resolve_decision_context(decision, frame)
                assert (
                    replay_context.repository_source(replay_frame).evidence_pack
                    == context.repository_source(effective).evidence_pack
                )
                assert reopened.admitted_intents(decision, frame) == orders
                assert (budget.summary(), len(spawns), len(transport.requests)) == before
            finally:
                await replay_provider.close()
            assert len(transport.requests) - prior_source_calls == (
                1 if mode == "acquisition" else 0
            )
            if mode != "unknown":
                assert len(orders) == 1 and orders[0].side is Side.BUY
                market = owner.source_market(decision, frame)
                bars: dict[str, AShareDailyBar] = {}
                for symbol in ("510300.SH", "000001.SZ"):
                    session = market.session(symbol, cutoff.date())
                    assert session.spec is not None and session.bar is not None
                    engine.register_instrument(session.spec)
                    bars[symbol] = session.bar
                result = engine.advance_session(bars, intents=orders)
                assert len(result.fills) == 1 and result.fills[0].target_id == "000001.SZ"
                assert "000001.SZ" in result.positions
                assert result.account_state.complete
                assert result.account_state.reconciliation_gaps == ()
                assert result.account_state.reconciled_at == result.account_state.as_of
        finally:
            await provider.close()

    try:
        asyncio.run(scenario())
    finally:
        engine.close()
