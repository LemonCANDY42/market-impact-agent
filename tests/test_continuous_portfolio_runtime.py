"""Real source CAS + pi loop + signed decisions + streaming engine; fake network only."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.continuous_decision import ContinuousDecision, PendingReview, ReviewFrame
from market_impact_agent.continuous_portfolio_runtime import (
    ContinuousPortfolioRuntime,
    build_continuous_review_frame,
)
from market_impact_agent.domain import ApprovalMode, Side, TradingEnvironment, TradingMandateV3
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.nautilus_backtest import AShareDailyBar
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount

from .test_historical_ashare_inputs import _capture, _source  # pyright: ignore[reportPrivateUsage]
from .test_pi_runtime import pi_profile
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]

D = Decimal


@pytest.fixture
def native_network(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    from market_impact_agent.model_provider import load_model_provider_profile

    profile = (
        load_model_provider_profile(Path("examples/providers/pi-cpa-luna-max-v2.json"))
        if getattr(request, "param", None) == "registered"
        else pi_profile()
    )
    monkeypatch.setenv(profile.credential_env, "synthetic-continuous-key")
    permit = PiRuntimePermit(canonical_hash(runtime_identity()), (profile.route_identity,), "test")

    def installed(_root: Path):
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec

    async def spawn(program: str, *args: str, **kwargs: Any):
        for key in ("HISTORY_SEED", "HISTORY_DAY", "HISTORY_SYMBOL"):
            if key in os.environ:
                kwargs["env"][key] = os.environ[key]
        kwargs["env"]["CONTINUOUS_UNKNOWN_FIXTURE"] = os.environ.get(
            "CONTINUOUS_UNKNOWN_FIXTURE", "0"
        )
        kwargs["env"]["CONTINUOUS_INITIAL_ROTATE"] = os.environ.get(
            "CONTINUOUS_INITIAL_ROTATE", "0"
        )
        kwargs["env"]["CONTINUOUS_BUY_RATIO"] = os.environ.get("CONTINUOUS_BUY_RATIO", "0.30")
        return await original(
            program,
            "--import",
            str(
                Path(__file__).with_name(
                    "historical_acquisition_network.mjs"
                    if os.environ.get("CONTINUOUS_HISTORICAL")
                    else "continuous_network.mjs"
                )
            ),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    return profile


@pytest.mark.parametrize(
    "mode",
    [
        "safe",
        "provenance_shift",
        "execution_rule_shift",
        "unknown",
        "unsafe_buy",
        "fee_overrun",
        "research_only",
        "historical",
        "historical_gap",
    ],
)
def test_source_to_signed_recall_update_account_action_and_restart(
    tmp_path: Path,
    native_network: ModelProviderProfile,
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if mode in {"historical", "historical_gap"}:
        monkeypatch.setenv("CONTINUOUS_HISTORICAL", "1")
        if mode == "historical_gap":
            monkeypatch.setenv("HISTORY_SYMBOL", "000002.SZ")
    if mode == "unknown":
        monkeypatch.setenv("CONTINUOUS_UNKNOWN_FIXTURE", "1")
    if mode == "unsafe_buy":
        monkeypatch.setenv("CONTINUOUS_BUY_RATIO", "0.99")
    source = _source(tmp_path)
    if mode == "fee_overrun":
        monkeypatch.setenv("CONTINUOUS_BUY_RATIO", "0.95")
        rules: list[str] = []
        for rule_hash in source.rule_artifact_hashes:
            rule = cast(dict[str, object], source.store.artifacts.read_json(rule_hash))
            if rule["symbol"] == "000001.SZ":
                rule["minimum_commission"] = "1000"
                rule_hash = source.store.artifacts.put_json(rule).content_hash
            rules.append(rule_hash)
        source = HistoricalAShareInputs(
            store=source.store,
            snapshot_ids=source.snapshot_ids,
            rule_artifact_hashes=tuple(rules),
            policy=source.policy,
        )
    # Add only the next preopen operational facts; no next-day outcome bar enters research.
    snapshots: list[str] = []
    for symbol, exchange, price in (("510300.SH", "SSE", 4), ("000001.SZ", "SZSE", 10)):
        snapshots.append(
            _capture(
                source.store,
                "trade_cal",
                {"exchange": exchange, "start_date": "20250106", "end_date": "20250106"},
                [dict(exchange=exchange, cal_date="20250106", is_open=1, pretrade_date="20250103")],
            )
        )
        snapshots.append(
            _capture(
                source.store,
                "stk_limit",
                {"ts_code": symbol, "start_date": "20250106", "end_date": "20250106"},
                [
                    dict(
                        ts_code=symbol,
                        trade_date="20250106",
                        pre_close=price,
                        up_limit=price * 1.1,
                        down_limit=price * 0.9,
                    )
                ],
            )
        )
        snapshots.append(
            _capture(
                source.store,
                "suspend_d",
                {"ts_code": symbol, "start_date": "20250106", "end_date": "20250106"},
                [
                    dict(
                        ts_code=symbol, trade_date="20250106", suspend_type="R", suspend_timing=None
                    )
                ],
            )
        )
    source = source.with_snapshots(tuple(snapshots))
    first_session = source.session("510300.SH", date(2025, 1, 2))
    assert first_session.spec is not None and first_session.bar is not None
    engine = HistoricalStreamingAccount(
        specs=(first_session.spec,),
        journal_path=tmp_path / "account.jsonl",
        account_reference="continuous-arm-account",
        account_reference_key=b"a" * 32,
    )
    assert first_session.execution_ready, first_session.gaps
    try:
        engine.bootstrap_half_hs300(first_session.bar)
    except ValueError:
        raise AssertionError(engine.results) from None
    journal = RunJournal.authoritative(source.store)
    journal.start_run(
        run_id="shared-budget",
        config_hash=canonical_hash("budget"),
        created_at=datetime(2025, 1, 3, tzinfo=UTC),
    )
    budget = ModelBudget(journal, "shared-budget", max_requests=8, max_cost_microusd=1000000)
    first_at, second_at = (
        datetime(2025, 1, 3, 1, 25, tzinfo=UTC),
        datetime(2025, 1, 6, 1, 25, tzinfo=UTC),
    )
    repositories = {
        at: _repository("510300.SH", at=at, event_id="continuous-market")
        for at in (first_at, second_at)
    }
    if mode in {"historical", "historical_gap"}:
        from market_impact_agent.continuous_research_inputs import continuous_research_repository

        repositories = {
            at: asyncio.run(
                continuous_research_repository(
                    market=source,
                    cutoff=at,
                    event_scope="continuous-market",
                    symbols=("510300.SH", "510500.SH"),
                )
            )
            for at in (first_at, second_at)
        }
    frames = [
        build_continuous_review_frame(repository=repositories[at], market=source)
        for at in (first_at, second_at)
    ]

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

    def runtime(provider: PiRuntimeProvider, arm: str = "model-arm") -> ContinuousPortfolioRuntime:
        return ContinuousPortfolioRuntime(
            store=source.store,
            experiment_id="continuous-fixture",
            arm_id=arm,
            account=engine,
            research_repository=lambda frame: repositories[frame.cutoff],
            market_inputs=lambda _: source,
            mandate_template=mandate,
            symbols=lambda _: ("510300.SH", "000001.SZ"),
            account_max_age=lambda _: timedelta(days=4),
            provider=provider,
        )

    async def scenario():
        nonlocal engine
        provider = PiRuntimeProvider(native_network, budget=budget)
        owner = runtime(provider)
        if mode == "research_only":
            # Empty operational candidate coverage still permits signed research.
            def missing_symbols(_: ReviewFrame) -> tuple[str, ...]:
                return ("999999.SZ",)

            owner.symbol_source = missing_symbols
            # Remove the held symbol's halt coverage through a frozen source fixture.
            unavailable = HistoricalAShareInputs(
                store=source.store,
                snapshot_ids=(source.snapshot_ids[0],),
                rule_artifact_hashes=source.rule_artifact_hashes,
                policy=source.policy,
            )

            def missing_market(_: ReviewFrame) -> HistoricalAShareInputs:
                return unavailable

            owner.market_source = missing_market
            gap_frame = build_continuous_review_frame(
                repository=repositories[first_at], market=unavailable
            )
            pending = await owner.decide(gap_frame, None, "gap", frozenset({1}), False)
            assert isinstance(pending, PendingReview)
            assert pending.continuation_ref == "gap.research"
            assert journal.get_run("gap.research").terminal_artifact_id is not None
            assert budget.summary()["physical_requests"] == 1
            assert await owner.decide(gap_frame, None, "gap", frozenset({1}), True) == pending
            assert budget.summary()["physical_requests"] == 1
            await provider.close()
            return
        if mode in {"historical", "historical_gap"}:
            from market_impact_agent.on_demand_research import ResearchSourceTemplate
            from market_impact_agent.tushare_observation import (
                TushareObservationProvider,
                load_tushare_observation_source,
            )

            from .test_tushare_observation import (  # pyright: ignore[reportPrivateUsage]
                RETRIEVED,
                TOKEN,
                FakeTransport,
                _response,  # pyright: ignore[reportPrivateUsage]
            )

            config = load_tushare_observation_source(
                Path("examples/providers/tushare-observation-daily-v1.json")
            )
            record = dict(
                ts_code="000002.SZ" if mode == "historical_gap" else "000001.SZ",
                trade_date="20250102",
                pre_close=10,
                open=10,
                high=10,
                low=10,
                close=10,
                change=0,
                pct_chg=0,
                vol=200000,
                amount=80000,
            )

            class SealedTransport(FakeTransport):
                def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
                    assert journal.get_run("cutoff-1.research").status.terminal
                    return super().__call__(endpoint, body, timeout_seconds)

            transport = SealedTransport(
                [_response(config.fields, [[record.get(field) for field in config.fields]])]
            )
            raw_provider = TushareObservationProvider(
                TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
            )

            def seed_only(_: ReviewFrame) -> tuple[str, ...]:
                return ("510300.SH",)

            owner.symbol_source = seed_only
            owner.historical_research_templates = (
                ResearchSourceTemplate.from_tushare(raw_provider, config.source_id),
            )
            owner.research_episode_deadline = RETRIEVED + timedelta(hours=1)
            owner.acquisition_clock = lambda: RETRIEVED
            first = await owner.decide(frames[0], None, "cutoff-1", frozenset({1}), False)
            if mode == "historical_gap":
                assert isinstance(first, PendingReview) and "000002.SZ" in first.reason
                assert first.continuation_ref == "cutoff-1.research.continuation.1"
                before = budget.summary()
                assert (
                    await owner.decide(frames[0], None, "cutoff-1", frozenset({1}), True) == first
                )
                assert budget.summary() == before and len(transport.requests) == 1
                await provider.close()
                return
            assert isinstance(first, ContinuousDecision), first
            assert first.research_successor_ref is not None
            assert first.research_run_id == "cutoff-1.research.continuation.1"
            context, effective = owner.resolve_decision_context(first, frames[0])
            assert effective.cutoff == frames[0].cutoff
            assert "000001.SZ" in context.repository_source(effective).evidence_pack.allowed_targets
            assert (
                "A-share-equity-universe"
                in context.repository_source(effective).evidence_pack.allowed_targets
            )
            assert set(context.symbol_source(effective)) == {"510300.SH", "000001.SZ"}
            assert len(transport.requests) == 1
            before = budget.summary()
            assert await owner.decide(frames[0], None, "cutoff-1", frozenset({1}), True) == first
            assert budget.summary() == before and len(transport.requests) == 1
            owner.validate_decision(first, frames[0])
            with pytest.raises(PermissionError, match="arm"):
                runtime(provider, "foreign-arm").validate_decision(first, frames[0])
            orders = owner.admitted_intents(first, frames[0])
            assert len(orders) == 1 and orders[0].side is Side.BUY
            resolved_market = owner.source_market(first, frames[0])
            assert resolved_market.snapshot_ids != source.snapshot_ids
            bars: dict[str, AShareDailyBar] = {}
            for symbol in ("510300.SH", "000001.SZ"):
                session = resolved_market.session(symbol, frames[0].cutoff.date())
                assert session.spec is not None and session.bar is not None
                engine.register_instrument(session.spec)
                bars[symbol] = session.bar
            filled = engine.advance_session(bars, intents=orders)
            assert filled.fills and filled.fills[0].target_id == "000001.SZ"
            await provider.close()
            return
        first = await owner.decide(frames[0], None, "cutoff-1", frozenset({1}), False)
        if mode == "unknown":
            assert isinstance(first, PendingReview)
            assert budget.summary()["physical_requests"] == 1
            assert budget.summary()["unsettled_requests"] == 1
            assert (
                await runtime(provider).decide(frames[0], None, "cutoff-1", frozenset({1}), True)
                == first
            )
            assert budget.summary()["physical_requests"] == 1
            await provider.close()
            return
        assert isinstance(first, ContinuousDecision), first
        assert first.action == "close"
        wrong_account = runtime(provider)

        def wrong_mandate(frame: ReviewFrame) -> TradingMandateV3:
            return replace(mandate(frame), account_id="account-ref-" + "b" * 64)

        wrong_account.mandate_source = wrong_mandate
        with pytest.raises(PermissionError, match="exact same-root"):
            await wrong_account.decide(frames[0], None, "wrong-account", frozenset({1}), False)
        assert budget.summary()["physical_requests"] == 2
        orders = owner.admitted_intents(first, frames[0])
        assert len(orders) == 1 and orders[0].side is Side.SELL
        day = source.session("510300.SH", date(2025, 1, 3))
        assert day.bar is not None
        closed = engine.advance_session({"510300.SH": day.bar}, intents=orders)
        assert closed.positions == {} and len(closed.fills) == 1
        if mode in {"provenance_shift", "execution_rule_shift"}:
            current_spec = source.instrument_spec("000001.SZ", frames[1].cutoff)
            assert current_spec is not None
            prior_spec = replace(current_spec, source_ref="fixture:prior-rule-provenance")
            if mode == "execution_rule_shift":
                prior_spec = replace(prior_spec, commission_rate=D("0.0004"))
            engine.register_instrument(prior_spec)
        second = await owner.decide(frames[1], first, "cutoff-2", frozenset({1}), False)
        if mode == "execution_rule_shift":
            assert isinstance(second, PendingReview)
            assert second.reason == "opening_buy_bounds_not_admitted"
            portfolio = owner._portfolio_authority(frames[1])  # pyright: ignore[reportPrivateUsage]
            signed_order = portfolio.execution_admission("cutoff-2.portfolio").order
            with pytest.raises(PermissionError, match="rules differ from source"):
                owner._assert_opening_buy_bounds(signed_order, frames[1])  # pyright: ignore[reportPrivateUsage]
            await provider.close()
            return
        if mode in {"unsafe_buy", "fee_overrun"}:
            assert isinstance(second, PendingReview)
            assert second.reason == "opening_buy_bounds_not_admitted"
            portfolio = owner._portfolio_authority(frames[1])  # pyright: ignore[reportPrivateUsage]
            signed_order = portfolio.execution_admission("cutoff-2.portfolio").order
            assert signed_order.quantity == D(9400 if mode == "unsafe_buy" else 9000)
            expected = "single-position cap" if mode == "unsafe_buy" else "plus fees"
            with pytest.raises(PermissionError, match=expected):
                owner._assert_opening_buy_bounds(signed_order, frames[1])  # pyright: ignore[reportPrivateUsage]
            assert portfolio.execution_admission("cutoff-2.portfolio").order == signed_order
            assert await owner.decide(frames[1], first, "cutoff-2", frozenset({1}), True) == second
            assert budget.summary()["physical_requests"] == 5
            await provider.close()
            return
        assert isinstance(second, ContinuousDecision), second
        assert second.action == "open"
        destination = owner.admitted_intents(second, frames[1])
        assert destination[0].instrument_id == "000001.SZ"
        if mode == "provenance_shift":
            assert destination[0].side is Side.BUY
            assert engine.specs["000001.SZ"].source_ref == "fixture:prior-rule-provenance"
        assert destination[0].client_order_id != orders[0].client_order_id
        assert budget.summary()["physical_requests"] == 5
        with pytest.raises(PermissionError, match="scope"):
            runtime(provider, "foreign-arm").validate_decision(second, frames[1])
        with pytest.raises(PermissionError, match="frozen"):
            owner.validate_decision(second, replace(frames[1], input_hash="f" * 64))
        await provider.close()
        engine.close()
        assert first_session.spec is not None
        engine = HistoricalStreamingAccount(
            specs=(first_session.spec,),
            journal_path=tmp_path / "account.jsonl",
            account_reference="continuous-arm-account",
            account_reference_key=b"a" * 32,
        )
        provider = PiRuntimeProvider(native_network, budget=budget)
        assert (
            await runtime(provider).decide(frames[1], first, "cutoff-2", frozenset({1}), True)
            == second
        )
        assert budget.summary()["physical_requests"] == 5
        await provider.close()

    try:
        asyncio.run(scenario())
    finally:
        engine.close()
