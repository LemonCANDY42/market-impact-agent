"""One actual native initial decision, registered signed reuse, then a scoped update."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent import continuous_study_runner
from market_impact_agent.continuous_decision import ContinuousDecision, ReviewFrame
from market_impact_agent.continuous_initial_adoption import (
    InitialAdoptionAuthority,
    initial_economic_contract,
)
from market_impact_agent.continuous_portfolio_runtime import (
    ContinuousPortfolioRuntime,
    build_continuous_review_frame,
)
from market_impact_agent.continuous_study_runner import (
    continuous_study_scope,
    load_prepared_continuous_registration,
    prepare_continuous_study,
    study_budget,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutableOrder,
    Side,
    TradingEnvironment,
    TradingMandateV3,
)
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.portfolio_review import RotationSourceCompletion
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount

from . import test_historical_ashare_inputs as source_fixture
from .test_continuous_portfolio_runtime import (
    native_network,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)
from .test_continuous_study import require_private_continuous_study_inputs
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]

D = Decimal


@pytest.mark.parametrize("native_network", ["registered"], indirect=True)
@pytest.mark.parametrize("rotation,historical", [(False, False), (True, False), (False, True)])
def test_registered_initial_receipt_three_arms_and_fresh_scoped_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_network: ModelProviderProfile,  # noqa: F811
    rotation: bool,
    historical: bool,
) -> None:
    require_private_continuous_study_inputs()
    if rotation:
        monkeypatch.setenv("CONTINUOUS_INITIAL_ROTATE", "1")
    monkeypatch.setattr(
        continuous_study_runner, "shared_admission_root", lambda: tmp_path / "shared"
    )
    study_root = tmp_path / "study"
    prepare_continuous_study(
        study_root,
        dataset_path=Path("examples/research/market-regime-dataset-v1.json"),
        panel_root=Path(".market-impact/regime"),
        prior_usage_audit_path=Path(".market-impact/continuous-20260905/prior-budget-audit.json"),
    )
    registration = load_prepared_continuous_registration(study_root)
    registration_id = str(registration["registration_id"])
    budget = study_budget(study_root, "rolling")
    before_requests = budget.summary()["physical_requests"]
    if historical:
        shared_store = LocalDataSnapshotStore(budget.journal.path.parent)

        def shared_source_store(_root: Path) -> LocalDataSnapshotStore:
            return shared_store

        monkeypatch.setattr(source_fixture, "LocalDataSnapshotStore", shared_source_store)
    original_capture = source_fixture._capture  # pyright: ignore[reportPrivateUsage]
    dates = {"20241231": "20240920", "20250102": "20240923", "20250103": "20240924"}

    def capture(
        store: LocalDataSnapshotStore,
        api: str,
        params: dict[str, object],
        rows: list[dict[str, object]],
    ) -> str:
        def shifted(value: object) -> object:
            return dates.get(value, value) if isinstance(value, str) else value

        return original_capture(
            store,
            api,
            {k: shifted(v) for k, v in params.items()},
            [{k: shifted(v) for k, v in row.items()} for row in rows],
        )

    monkeypatch.setattr(source_fixture, "_capture", capture)
    source = source_fixture._source(tmp_path)  # pyright: ignore[reportPrivateUsage]
    snapshots: list[str] = []
    for symbol, exchange, price in (("510300.SH", "SSE", 4), ("000001.SZ", "SZSE", 10)):
        snapshots.append(
            capture(
                source.store,
                "trade_cal",
                {
                    "exchange": exchange,
                    "start_date": "20240925",
                    "end_date": "20240925",
                },
                [dict(exchange=exchange, cal_date="20240925", is_open=1, pretrade_date="20240924")],
            )
        )
        snapshots.append(
            capture(
                source.store,
                "stk_limit",
                {
                    "ts_code": symbol,
                    "start_date": "20240925",
                    "end_date": "20240925",
                },
                [
                    dict(
                        ts_code=symbol,
                        trade_date="20240925",
                        pre_close=price,
                        up_limit=price * 1.1,
                        down_limit=price * 0.9,
                    )
                ],
            )
        )
        snapshots.append(
            capture(
                source.store,
                "suspend_d",
                {
                    "ts_code": symbol,
                    "start_date": "20240925",
                    "end_date": "20240925",
                },
                [
                    dict(
                        ts_code=symbol, trade_date="20240925", suspend_type="R", suspend_timing=None
                    )
                ],
            )
        )
    if rotation:
        for symbol, exchange, price in (("510300.SH", "SSE", 4), ("000001.SZ", "SZSE", 10)):
            snapshots.append(
                capture(
                    source.store,
                    "trade_cal",
                    {
                        "exchange": exchange,
                        "start_date": "20240926",
                        "end_date": "20240926",
                    },
                    [
                        dict(
                            exchange=exchange,
                            cal_date="20240926",
                            is_open=1,
                            pretrade_date="20240925",
                        )
                    ],
                )
            )
            snapshots.append(
                capture(
                    source.store,
                    "stk_limit",
                    {
                        "ts_code": symbol,
                        "start_date": "20240926",
                        "end_date": "20240926",
                    },
                    [
                        dict(
                            ts_code=symbol,
                            trade_date="20240926",
                            pre_close=price,
                            up_limit=price * 1.1,
                            down_limit=price * 0.9,
                        )
                    ],
                )
            )
            snapshots.append(
                capture(
                    source.store,
                    "suspend_d",
                    {
                        "ts_code": symbol,
                        "start_date": "20240926",
                        "end_date": "20240926",
                    },
                    [
                        dict(
                            ts_code=symbol,
                            trade_date="20240926",
                            suspend_type="R",
                            suspend_timing=None,
                        )
                    ],
                )
            )
            snapshots.append(
                capture(
                    source.store,
                    "fund_daily" if symbol == "510300.SH" else "daily",
                    {
                        "ts_code": symbol,
                        "start_date": "20240925",
                        "end_date": "20240925",
                    },
                    [
                        dict(
                            ts_code=symbol,
                            trade_date="20240925",
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
                    ],
                )
            )
            snapshots.append(
                capture(
                    source.store,
                    "fund_adj" if symbol == "510300.SH" else "adj_factor",
                    {
                        "ts_code": symbol,
                        "start_date": "20240925",
                        "end_date": "20240925",
                    },
                    [dict(ts_code=symbol, trade_date="20240925", adj_factor=1)],
                )
            )
    source = source.with_snapshots(tuple(snapshots))
    seed = source.session("510300.SH", date(2024, 9, 23))
    assert seed.spec is not None and seed.bar is not None, seed.gaps
    at = datetime(2024, 9, 24, 1, 25, tzinfo=UTC)
    later = at + timedelta(days=2 if rotation else 1)
    repositories = {
        t: _repository("510300.SH", at=t, event_id="registered-case") for t in (at, later)
    }
    if historical:
        from market_impact_agent.continuous_research_inputs import continuous_research_repository

        repositories = {
            t: asyncio.run(
                continuous_research_repository(
                    market=source,
                    cutoff=t,
                    event_scope="registered-case",
                    symbols=("510300.SH", "510500.SH"),
                )
            )
            for t in (at, later)
        }
    frames = {
        t: build_continuous_review_frame(repository=repositories[t], market=source)
        for t in repositories
    }
    engines: list[HistoricalStreamingAccount] = []
    provider = PiRuntimeProvider(native_network, budget=budget)

    def runtime(
        cadence: str, initial: InitialAdoptionAuthority | None = None
    ) -> ContinuousPortfolioRuntime:
        experiment, arm = continuous_study_scope(
            registration_id, "cn-2024-policy-melt-up", "luna_max", cadence
        )
        assert seed.spec is not None
        registered_spec = seed.spec
        if not rotation and not historical:
            registered_spec = replace(seed.spec, source_ref="fixture:prior-seed-rule-provenance")
        engine = HistoricalStreamingAccount(
            specs=(registered_spec,),
            journal_path=tmp_path / (cadence + ".jsonl"),
            account_reference=arm,
            account_reference_key=b"a" * 32,
        )
        engines.append(engine)
        assert seed.bar is not None
        if not engine.results:
            engine.bootstrap_half_hs300(seed.bar)

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

        return ContinuousPortfolioRuntime(
            store=source.store,
            experiment_id=experiment,
            arm_id=arm,
            account=engine,
            research_repository=lambda f: repositories[f.cutoff],
            market_inputs=lambda _: source,
            mandate_template=mandate,
            symbols=lambda _: ("510300.SH", "000001.SZ"),
            account_max_age=lambda _: timedelta(days=4),
            provider=provider,
            initial_adoption_authority=initial,
        )

    async def scenario() -> None:
        nonlocal before_requests
        origin = runtime("coverage")
        if historical:
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

            monkeypatch.setenv("CONTINUOUS_HISTORICAL", "1")
            monkeypatch.setenv("HISTORY_SEED", "1")
            monkeypatch.setenv("HISTORY_DAY", "20240923")
            config = load_tushare_observation_source(
                Path("examples/providers/tushare-observation-fund-daily-v1.json")
            )
            row = dict(
                ts_code="510300.SH",
                trade_date="20240923",
                pre_close=4,
                open=4,
                high=4,
                low=4,
                close=4,
                change=0,
                pct_chg=0,
                vol=200000,
                amount=80000,
            )
            transport = FakeTransport(
                [_response(config.fields, [[row.get(field) for field in config.fields]])]
            )
            raw_provider = TushareObservationProvider(
                TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
            )
            origin.historical_research_templates = (
                ResearchSourceTemplate.from_tushare(raw_provider, config.source_id),
            )
            origin.research_episode_deadline = RETRIEVED + timedelta(hours=1)
            origin.acquisition_clock = lambda: RETRIEVED
        if not rotation and not historical:
            contract = initial_economic_contract(origin, frames[at])
            execution_specs = cast(dict[str, dict[str, object]], contract["execution_specs"])
            current_spec = source.instrument_spec("510300.SH", at)
            assert current_spec is not None
            assert execution_specs["510300.SH"]["source_ref"] == current_spec.source_ref
            assert origin.account.specs["510300.SH"].source_ref != current_spec.source_ref
            with monkeypatch.context() as changed_rules:
                changed_rules.setitem(
                    origin.account.specs,
                    "510300.SH",
                    replace(origin.account.specs["510300.SH"], commission_rate=D("0.0004")),
                )
                with pytest.raises(PermissionError, match="rules differ from frozen rules"):
                    initial_economic_contract(origin, frames[at])
        stale_source: ContinuousDecision | None = None
        if not historical:
            # Same budget owner, scope and T0; a previous frozen research generation
            # retains real signed receipts but cannot authorize the current account.
            current_repository = repositories[at]
            repositories[at] = _repository("510300.SH", at=at, event_id="previous-generation")
            stale_frame = build_continuous_review_frame(repository=repositories[at], market=source)
            stale_result = await origin.decide(
                stale_frame, None, "registered-previous-initial", frozenset({1}), False
            )
            assert isinstance(stale_result, ContinuousDecision), stale_result
            stale_source = stale_result
            stale_authority = InitialAdoptionAuthority(
                study_root=study_root,
                source_runtime=origin,
                coverage_window_id="cn-2024-policy-melt-up",
                profile_arm="luna_max",
                cadence="expiry_only",
            )
            stale_destination = runtime("expiry_only", stale_authority)
            stale_decision = stale_destination.adopt_initial(stale_source, stale_frame)
            assert (
                stale_authority.recall_source(stale_destination, later)
                == stale_source.research_run_id
            )
            repositories[at] = current_repository
            assert stale_authority.recall_source(stale_destination, later) is None
            with pytest.raises(PermissionError, match="no unique adopted"):
                stale_authority.rotation_order(stale_destination, stale_source.portfolio_run_id)
            with pytest.raises(PermissionError, match="frozen research/source policy"):
                stale_destination.validate_decision(stale_decision, stale_frame)
            stale_destination.account.close()
            before_requests += 2
        first = await origin.decide(frames[at], None, "registered-initial", frozenset({1}), False)
        if historical:
            monkeypatch.delenv("CONTINUOUS_HISTORICAL")
            assert isinstance(first, ContinuousDecision) and first.research_successor_ref
        assert isinstance(first, ContinuousDecision), first
        if not historical:
            portfolio = origin._portfolio_authority(frames[at])  # pyright: ignore[reportPrivateUsage]
            parsed = portfolio.replay(first.portfolio_run_id)["parsed_proposal"]
            assert isinstance(parsed, dict) and "release" in parsed["evidence_refs"]
        assert budget.summary()["physical_requests"] == before_requests + 2 + (
            2 if historical else 0
        )
        adopted: list[tuple[ContinuousPortfolioRuntime, ContinuousDecision, ExecutableOrder]] = []
        for cadence in ("expiry_only", "scheduled", "event"):
            authority = InitialAdoptionAuthority(
                study_root=study_root,
                source_runtime=origin,
                coverage_window_id="cn-2024-policy-melt-up",
                profile_arm="luna_max",
                cadence=cadence,
            )
            destination = runtime(cadence, authority)
            own_account = destination.account
            event_count = len(authority.budget.journal.events(authority.budget.owner_run_id))
            destination.account = origin.account
            with pytest.raises(PermissionError, match="distinct"):
                destination.adopt_initial(first, frames[at])
            destination.account = own_account
            account_id = own_account.account_id
            own_account.account_id = origin.account.account_id
            with pytest.raises(PermissionError, match="distinct"):
                destination.adopt_initial(first, frames[at])
            own_account.account_id = account_id
            if adopted:
                destination.account = adopted[0][0].account
                with pytest.raises(PermissionError, match="scope"):
                    destination.adopt_initial(first, frames[at])
                destination.account = own_account
            assert (
                len(authority.budget.journal.events(authority.budget.owner_run_id)) == event_count
            )
            with pytest.raises(PermissionError, match="root-authenticated"):
                authority.budget.journal.append(
                    run_id=authority.budget.owner_run_id,
                    event_id="fake-permission",
                    event_type="continuous.initial-adoption.authorized",
                    observed_at=at,
                    payload={"artifact_hash": "0" * 64},
                )
            if cadence == "event":
                persist = authority._persist  # pyright: ignore[reportPrivateUsage]

                def crash_receipt(
                    value: dict[str, object],
                    suffix: str,
                    f: ReviewFrame,
                    persist: Callable[[dict[str, object], str, ReviewFrame], str] = persist,
                ) -> str:
                    if suffix == "validated":
                        raise OSError("synthetic crash after permission, before receipt")
                    return persist(value, suffix, f)

                with monkeypatch.context() as fault:
                    fault.setattr(authority, "_persist", crash_receipt)
                    with pytest.raises(OSError, match="synthetic crash"):
                        destination.adopt_initial(first, frames[at])
            decision = destination.adopt_initial(first, frames[at])
            assert decision.research_run_id == first.research_run_id
            assert decision.portfolio_run_id == first.portfolio_run_id
            assert decision.initial_adoption_ref is not None
            assert destination.adopt_initial(first, frames[at]) == decision
            order = destination.admitted_intents(decision, frames[at])[0]
            assert order.quantity == origin.admitted_intents(first, frames[at])[0].quantity
            assert order.account_id == destination.account.account_id
            assert (
                order.client_order_id
                != origin.admitted_intents(first, frames[at])[0].client_order_id
            )
            adopted.append((destination, decision, order))
        assert len({item[2].client_order_id for item in adopted}) == 3
        assert budget.summary()["physical_requests"] == before_requests + 2 + (
            2 if historical else 0
        )
        destination, decision, order = adopted[0]
        destination.account.close()
        # Reopen engine and receipt authority without adopting or calling a model again.
        destination = runtime(
            "expiry_only",
            InitialAdoptionAuthority(
                study_root=study_root,
                source_runtime=origin,
                coverage_window_id="cn-2024-policy-melt-up",
                profile_arm="luna_max",
                cadence="expiry_only",
            ),
        )
        assert destination.admitted_intents(decision, frames[at]) == (order,)
        assert destination.initial_adoption_authority is not None
        recovered_authority = destination.initial_adoption_authority
        assert recovered_authority.recall_source(destination, later) == first.research_run_id
        assert recovered_authority.rotation_order(destination, first.portfolio_run_id) == order
        if stale_source is not None:
            with pytest.raises(PermissionError, match="no unique adopted"):
                recovered_authority.rotation_order(destination, stale_source.portfolio_run_id)
        receipt_store = destination.initial_adoption_authority.store
        forged = receipt_store.artifacts.put_json({"forged": "receipt"}).content_hash
        with pytest.raises(PermissionError, match="signature"):
            destination.validate_decision(
                replace(decision, initial_adoption_ref=forged), frames[at]
            )
        old_mandate = destination.mandate_source

        def changed_policy(f: ReviewFrame) -> TradingMandateV3:
            return replace(old_mandate(f), daily_turnover_limit=D(100001))

        destination.mandate_source = changed_policy
        with pytest.raises(PermissionError, match="permission"):
            destination.validate_decision(decision, frames[at])
        destination.mandate_source = old_mandate
        with pytest.raises(PermissionError, match="registered T0"):
            destination.validate_decision(decision, frames[later])
        with pytest.raises(PermissionError):
            adopted[1][0].validate_decision(decision, frames[at])
        with pytest.raises(PermissionError, match="references"):
            destination.validate_decision(replace(decision, horizon_sessions=3), frames[at])
        day = source.session("510300.SH", date(2024, 9, 24))
        assert day.bar is not None
        result = destination.account.advance_session({"510300.SH": day.bar}, intents=(order,))
        assert result.positions == {} and len(result.fills) == 1
        if rotation:
            blank_day = source.session("510300.SH", date(2024, 9, 25))
            assert blank_day.bar is not None, blank_day.gaps
            blank = destination.account.advance_session({"510300.SH": blank_day.bar})
            assert blank.account_state.recent_fills == ()
            assert blank.positions == {} and not blank.fills
            # Native and adopted orders both resolve through exact original signed sizing.
            native_order = origin.admitted_intents(first, frames[at])[0]
            origin.account.advance_session({"510300.SH": day.bar}, intents=(native_order,))
            origin.account.advance_session({"510300.SH": blank_day.bar})
            native_authority = origin._portfolio_authority(frames[later])  # pyright: ignore[reportPrivateUsage]
            assert native_authority.rotation_authority is not None
            native_completion = native_authority.rotation_authority.reopen_source_completion(
                first.portfolio_run_id
            )
            assert native_completion.source_order_reference == native_order.client_order_id
            assert native_completion.reconciled_source_account is not None
            complete = False

            class Completion:
                def reopen_source_completion(self, source_run_id: str) -> RotationSourceCompletion:
                    return RotationSourceCompletion(
                        source_run_id,
                        destination.account.account_id,
                        "510300.SH",
                        order.client_order_id,
                        order.quantity if complete else order.quantity - D(100),
                        result.fills[0].filled_at,
                    )

            destination.rotation_authority = Completion()
            with pytest.raises(PermissionError, match="reconciliation"):
                await destination.decide(
                    frames[later], decision, "registered-update", frozenset({1}), False
                )
            assert budget.summary()["physical_requests"] == before_requests + 4 + (
                2 if historical else 0
            )
            destination.rotation_authority = None
        updated = await destination.decide(
            frames[later], decision, "registered-update", frozenset({1}), False
        )
        assert isinstance(updated, ContinuousDecision), updated
        assert updated.initial_adoption_ref is None
        assert updated.action == "open"
        foreign_recall = adopted[1][0]._recall_runs(frames[later])  # pyright: ignore[reportPrivateUsage]
        assert first.research_run_id in foreign_recall
        assert updated.research_run_id not in foreign_recall
        assert destination.admitted_intents(updated, frames[later])[0].instrument_id == "000001.SZ"
        assert budget.summary()["physical_requests"] == before_requests + 5 + (
            2 if historical else 0
        )
        assert (
            await destination.decide(
                frames[later], decision, "registered-update", frozenset({1}), True
            )
            == updated
        )
        assert budget.summary()["physical_requests"] == before_requests + 5 + (
            2 if historical else 0
        )
        if not historical:
            assert decision.initial_adoption_ref is not None
            receipt, _ = recovered_authority.reopen(decision.initial_adoption_ref, destination)
            # A current permission with a tampered receipt must not be mistaken for
            # historical data and silently skipped by generation selection.
            receipt["frame"] = replace(frames[at], input_hash="f" * 64).to_dict()
            cast(dict[str, object], receipt["source_decision"])["portfolio_run_id"] = "tampered"
            recovered_authority._persist(receipt, "validated", frames[at])  # pyright: ignore[reportPrivateUsage]
            with pytest.raises(PermissionError):
                recovered_authority.recall_source(destination, later)
            with pytest.raises(PermissionError):
                recovered_authority.rotation_order(destination, first.portfolio_run_id)
        await provider.close()

    try:
        asyncio.run(scenario())
    finally:
        for engine in engines:
            engine.close()
