# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ToolAccessContext,
    ToolCall,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from tests.test_tushare_observation import RETRIEVED, TOKEN, FakeTransport, _response

CONFIG = Path(__file__).parents[1] / "examples/providers/tushare-observation-fund-daily-v1.json"
PARAMS: dict[str, object] = {
    "ts_code": "510300.SH",
    "start_date": "20260827",
    "end_date": "20260827",
}


def test_stock_adjustments_after_cutoff_do_not_consume_a_continuation(tmp_path: Path) -> None:
    parent, _ = _setup(tmp_path)
    config = load_tushare_observation_source(
        CONFIG.with_name("tushare-observation-adj-factor-v1.json")
    )
    transport = FakeTransport([])
    provider = TushareObservationProvider(TOKEN, (config,), transport=transport)
    acquisition = OnDemandResearch(
        store=parent.store,
        parent_budget=parent.budget,
        episode_deadline=parent.deadline,
        run_id="stock-adjustments",
        cutoff=parent.cutoff,
        pit_lane=parent.pit_lane,
        templates=(ResearchSourceTemplate.from_tushare(provider, config.source_id),),
        clock=parent.clock,
    )
    after = (parent.cutoff + timedelta(days=1)).strftime("%Y%m%d")

    async def run() -> None:
        result = await acquisition.descriptors()[0].handler(
            {
                "ts_code": "600000.SH",
                "start_date": after,
                "end_date": after,
            }
        )
        assert result == {"status": "data_gap", "error_kind": "query_window_after_cutoff"}
        assert await acquisition.fulfill_pending() == ()

    asyncio.run(run())
    assert not transport.requests
    assert not any(
        event.event_type == "research.data.requested"
        for event in parent.budget.journal.events(parent.budget.owner_run_id)
    )


def _setup(tmp_path: Path) -> tuple[OnDemandResearch, FakeTransport]:
    config = load_tushare_observation_source(CONFIG)
    row: list[object] = [
        "510300.SH" if name == "ts_code" else "20260827" if name == "trade_date" else 1
        for name in config.fields
    ]
    transport = FakeTransport([_response(config.fields, [row])])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id="episode", config_hash=canonical_hash("episode"), created_at=RETRIEVED)
    budget = ModelBudget(journal, "episode", 10, 40_000_000)
    return OnDemandResearch(
        store=store,
        parent_budget=budget,
        episode_deadline=RETRIEVED + timedelta(hours=1),
        run_id="research-1",
        cutoff=RETRIEVED - timedelta(minutes=1),
        pit_lane=DataPITLane.PROSPECTIVE,
        templates=(ResearchSourceTemplate.from_tushare(provider, config.source_id),),
        clock=lambda: RETRIEVED,
    ), transport


def test_request_acquire_once_and_successor_frozen_input(tmp_path: Path) -> None:
    research, transport = _setup(tmp_path)
    tool = research.descriptors()[0]
    registry = ToolRegistry(research.store.artifacts)
    registry.register(tool)
    access = ToolAccessContext(
        allowed_capabilities=frozenset({"read_market_context"}),
        allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
        allowed_tools=frozenset({tool.name}),
    )

    async def run() -> None:
        invocation = await registry.execute(
            ToolCall(call_id="model-query-1", name=tool.name, arguments=PARAMS), access=access
        )
        assert invocation.untrusted
        requested = cast(
            dict[str, object],
            research.store.artifacts.read_json(invocation.result_artifact.content_hash),
        )
        assert requested["status"] == "continuation_required"
        assert transport.requests == []
        assert await tool.handler(PARAMS) == requested
        results = await research.fulfill_pending()
        assert len(results) == 1
        assert results[0].status == "fulfilled"
        assert results[0].successor_cutoff == RETRIEVED
        assert await research.fulfill_pending() == results
        # Current model input is immutable, even after acquisition completed.
        assert await tool.handler(PARAMS) == requested
        assert len(transport.requests) == 1
        cutoff, frozen = research.successor_input(results)
        successor = OnDemandResearch(
            store=research.store,
            parent_budget=research.budget,
            episode_deadline=research.deadline,
            run_id="research-2",
            cutoff=cutoff,
            pit_lane=research.pit_lane,
            templates=tuple(research.templates.values()),
            frozen_input=frozen,
            clock=lambda: RETRIEVED,
        )
        available = cast(dict[str, object], await successor.descriptors()[0].handler(PARAMS))
        assert available["status"] == "available"
        assert successor.snapshots[0].observations[0].times.retrieved_at == RETRIEVED
        assert research.budget.summary()["physical_requests"] == 0
        with pytest.raises(ValueError, match="exceed cutoff"):
            OnDemandResearch(
                store=research.store,
                parent_budget=research.budget,
                episode_deadline=research.deadline,
                run_id="past",
                cutoff=research.cutoff,
                pit_lane=research.pit_lane,
                templates=tuple(research.templates.values()),
                frozen_input=frozen,
                clock=lambda: RETRIEVED,
            )

    asyncio.run(run())


def test_durable_received_stage_recovers_without_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    research, transport = _setup(tmp_path)
    original = research._append

    def crash_after_receipt(suffix: str, kind: str, payload: dict[str, object]) -> None:
        original(suffix, kind, payload)
        if kind == "research.data.received":
            raise RuntimeError("simulated process interruption after durable receipt")

    async def run() -> None:
        await research.descriptors()[0].handler(PARAMS)
        monkeypatch.setattr(research, "_append", crash_after_receipt)
        with pytest.raises(RuntimeError, match="simulated"):
            await research.fulfill_pending()
        monkeypatch.setattr(research, "_append", original)
        results = await research.fulfill_pending()
        assert results[0].status == "fulfilled"
        assert len(transport.requests) == 1

    asyncio.run(run())


def test_unstaged_started_request_is_uncertain_and_never_retries(tmp_path: Path) -> None:
    research, transport = _setup(tmp_path)

    async def run() -> None:
        requested = cast(dict[str, object], await research.descriptors()[0].handler(PARAMS))
        request_id = cast(str, requested["request_id"])
        research._append(
            request_id + ".started.0", "research.data.started", {"request_id": request_id}
        )
        results = await research.fulfill_pending()
        assert results[0].status == "uncertain"
        assert transport.requests == []

    asyncio.run(run())


def test_parent_deadline_and_historical_gaps_do_not_fetch(tmp_path: Path) -> None:
    research, transport = _setup(tmp_path)

    async def run() -> None:
        await research.descriptors()[0].handler(PARAMS)
        research.clock = lambda: research.deadline
        with pytest.raises(TimeoutError, match="deadline"):
            await research.fulfill_pending()
        historical = OnDemandResearch(
            store=research.store,
            parent_budget=research.budget,
            episode_deadline=research.deadline,
            run_id="historical",
            cutoff=research.cutoff,
            pit_lane=DataPITLane.STRICT,
            templates=tuple(research.templates.values()),
            clock=lambda: RETRIEVED,
        )
        gap = cast(dict[str, object], await historical.descriptors()[0].handler(PARAMS))
        assert gap["error_kind"] == "planned_external_historical_acquisition"
        assert await historical.fulfill_pending() == ()
        assert transport.requests == []
        missing = next(tool for tool in research.descriptors() if tool.name == "lookup_tradability")
        gap = cast(dict[str, object], await missing.handler({"subject": "510300.SH"}))
        assert gap["error_kind"] == "source_route_unconfigured"

    asyncio.run(run())


def test_reopen_cannot_reset_parent_deadline(tmp_path: Path) -> None:
    research, _ = _setup(tmp_path)
    with pytest.raises(ValueError, match="different content"):
        OnDemandResearch(
            store=research.store,
            parent_budget=research.budget,
            episode_deadline=research.deadline + timedelta(hours=1),
            run_id=research.run_id,
            cutoff=research.cutoff,
            pit_lane=research.pit_lane,
            templates=tuple(research.templates.values()),
            clock=lambda: RETRIEVED,
        )


def test_company_profile_uses_persistent_exact_query_acquisition(tmp_path: Path) -> None:
    research, _ = _setup(tmp_path)
    config = load_tushare_observation_source(
        CONFIG.with_name("tushare-observation-stock-basic-v1.json")
    )
    transport = FakeTransport([_response(config.fields, [])])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    template = ResearchSourceTemplate.from_tushare(provider, config.source_id)

    async def run() -> None:
        for index in range(2):
            service = OnDemandResearch(
                store=research.store,
                parent_budget=research.budget,
                episode_deadline=research.deadline,
                run_id=f"company-{index}",
                cutoff=research.cutoff,
                pit_lane=research.pit_lane,
                templates=(template,),
                clock=lambda: RETRIEVED,
            )
            await service.descriptors()[0].handler({"ts_code": "600000.SH"})
            results = await service.fulfill_pending()
            assert results[0].status == "data_gap"
            assert results[0].snapshot_id is not None
        assert len(transport.requests) == 1

    asyncio.run(run())


def test_news_to_industry_to_new_company_query(tmp_path: Path) -> None:
    base, _ = _setup(tmp_path)
    configs = tuple(
        load_tushare_observation_source(CONFIG.with_name(f"tushare-observation-{name}-v1.json"))
        for name in ("news", "index-member-all", "daily")
    )
    values: tuple[dict[str, object], ...] = (
        {
            "datetime": "2026-08-28 15:00:00",
            "title": "Industry policy",
            "content": "Policy affects industry 801780.SI",
            "channels": "policy",
        },
        {
            "l1_code": "801780.SI",
            "l1_name": "Industry",
            "l2_code": "801781.SI",
            "l2_name": "Subindustry",
            "l3_code": "851781.SI",
            "l3_name": "Group",
            "ts_code": "600000.SH",
            "name": "Company",
            "in_date": "20200101",
            "out_date": None,
            "is_new": "Y",
        },
        {"ts_code": "600000.SH", "trade_date": "20260827"},
    )
    transport = FakeTransport(
        [
            _response(config.fields, [[value.get(name, 1) for name in config.fields]])
            for config, value in zip(configs, values, strict=True)
        ]
    )
    provider = TushareObservationProvider(
        TOKEN, configs, transport=transport, clock=lambda: RETRIEVED
    )
    templates = tuple(
        ResearchSourceTemplate.from_tushare(provider, config.source_id) for config in configs
    )
    service = OnDemandResearch(
        store=base.store,
        parent_budget=base.budget,
        episode_deadline=base.deadline,
        run_id="discovery-0",
        cutoff=base.cutoff,
        pit_lane=base.pit_lane,
        templates=templates,
        clock=lambda: RETRIEVED,
    )

    async def run() -> None:
        nonlocal service
        questions: list[tuple[str, dict[str, object]]] = [
            (
                "lookup_news_events",
                {"start_date": "2026-08-28 00:00:00", "end_date": "2026-08-28 16:00:00"},
            ),
            ("lookup_industry_members", {"l1_code": "801780.SI"}),
        ]
        for index in range(3):
            name, arguments = questions[index]
            tool = next(tool for tool in service.descriptors() if tool.name == name)
            assert (
                cast(dict[str, object], await tool.handler(arguments))["status"]
                == "continuation_required"
            )
            results = await service.fulfill_pending()
            assert results[0].status == "fulfilled"
            cutoff, frozen = service.successor_input(results)
            service = OnDemandResearch(
                store=base.store,
                parent_budget=base.budget,
                episode_deadline=base.deadline,
                run_id=f"discovery-{index + 1}",
                cutoff=cutoff,
                pit_lane=base.pit_lane,
                templates=templates,
                frozen_input=frozen,
                clock=lambda: RETRIEVED,
            )
            if index == 1:
                assert results[0].snapshot_id is not None
                membership = base.store.get(results[0].snapshot_id).observations[0]
                record = cast(dict[str, object], membership.normalized_payload["record"])
                questions.append(
                    (
                        "lookup_stock_prices",
                        {
                            "ts_code": record["ts_code"],
                            "start_date": "20260827",
                            "end_date": "20260827",
                        },
                    )
                )
        assert len(transport.requests) == 3
        assert cast(dict[str, object], transport.requests[-1]["params"])["ts_code"] == "600000.SH"
        assert "lookup_event_context" not in {tool.name for tool in service.descriptors()}

    asyncio.run(run())


def test_zero_model_preparation_corporate_actions_and_calendar(tmp_path: Path) -> None:
    base, _ = _setup(tmp_path)
    configs = tuple(
        load_tushare_observation_source(CONFIG.with_name(f"tushare-observation-{name}-v1.json"))
        for name in ("fund-adj", "fund-div", "dividend", "trade-cal")
    )
    transport = FakeTransport([_response(config.fields, []) for config in configs])
    provider = TushareObservationProvider(
        TOKEN, configs, transport=transport, clock=lambda: RETRIEVED
    )
    templates = tuple(
        ResearchSourceTemplate.from_tushare(provider, config.source_id) for config in configs
    )
    service = OnDemandResearch(
        store=base.store,
        parent_budget=base.budget,
        episode_deadline=base.deadline,
        run_id="prepare-1",
        cutoff=base.cutoff,
        pit_lane=base.pit_lane,
        templates=templates,
        clock=lambda: RETRIEVED,
    )
    questions: tuple[tuple[str, dict[str, object]], ...] = (
        ("lookup_fund_adjustments", PARAMS),
        ("lookup_fund_distributions", {"ts_code": "510300.SH"}),
        ("lookup_company_distributions", {"ts_code": "600000.SH", "ann_date": "20260827"}),
        (
            "lookup_exchange_calendar",
            {"exchange": "SSE", "start_date": "20260801", "end_date": "20260930"},
        ),
    )

    async def run() -> None:
        for name, arguments in questions:
            result = await service.request(name, arguments)
            assert result["status"] == "continuation_required"
        requests = [
            event
            for event in base.budget.journal.events("episode")
            if event.event_type == "research.data.requested"
        ]
        assert len(requests) == 4
        assert all(event.payload["origin"] == "harness_preparation" for event in requests)
        assert transport.requests == []
        results = await service.fulfill_pending()
        assert len(results) == 4
        assert all(
            result.status == "data_gap" and result.snapshot_id is not None for result in results
        )
        assert await service.fulfill_pending() == results
        assert len(transport.requests) == 4
        cutoff, frozen = service.successor_input(results)
        successor = OnDemandResearch(
            store=base.store,
            parent_budget=base.budget,
            episode_deadline=base.deadline,
            run_id="prepare-2",
            cutoff=cutoff,
            pit_lane=base.pit_lane,
            templates=templates,
            frozen_input=frozen,
            clock=lambda: RETRIEVED,
        )
        cached = await successor.request(*questions[0])
        assert cached["status"] == "data_gap" and cached["snapshot_id"] is not None
        future = await successor.request(
            "lookup_fund_adjustments", {**PARAMS, "end_date": "20260829"}
        )
        assert future["error_kind"] == "query_window_after_cutoff"
        with pytest.raises(ValueError, match="exact format"):
            await successor.request("lookup_fund_adjustments", {**PARAMS, "start_date": "2026827"})
        with pytest.raises(ValueError, match="origin"):
            await service.request(*questions[0], origin="agent_tool")
        assert base.budget.summary()["physical_requests"] == 0

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["permission_denied", "response_field_mismatch"])
def test_non_daily_captured_failure_completes_and_replays_without_refetch(
    tmp_path: Path, failure: str
) -> None:
    base, _ = _setup(tmp_path)
    config = load_tushare_observation_source(
        CONFIG.with_name("tushare-observation-fund-div-v1.json")
    )
    body: dict[str, object] = (
        {"code": -2001, "msg": "permission denied"}
        if failure == "permission_denied"
        else _response(("ts_code",), [["510300.SH"]])
    )
    transport = FakeTransport([body])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    service = OnDemandResearch(
        store=base.store,
        parent_budget=base.budget,
        episode_deadline=base.deadline,
        run_id="captured-failure",
        cutoff=base.cutoff,
        pit_lane=base.pit_lane,
        templates=(ResearchSourceTemplate.from_tushare(provider, config.source_id),),
        clock=lambda: RETRIEVED,
    )

    async def run() -> None:
        await service.request("lookup_fund_distributions", {"ts_code": "510300.SH"})
        results = await service.fulfill_pending()
        result = results[0]
        assert result.status == "data_gap"
        assert result.snapshot_id is not None
        assert result.successor_cutoff == RETRIEVED
        snapshot = base.store.get(result.snapshot_id)
        attempt = snapshot.attempts[0]
        assert attempt.error_kind == failure
        assert attempt.retrieved_at == RETRIEVED
        assert attempt.received_count == 0
        assert not snapshot.observations
        assert attempt.raw_response_hash is not None
        raw = base.store.artifacts.get(
            attempt.raw_response_hash, media_type="application/octet-stream"
        ).path.read_bytes()
        assert TOKEN.encode() not in raw
        assert await service.fulfill_pending() == results
        assert len(transport.requests) == 1
        assert any(
            event.event_type == "research.data.received"
            for event in base.budget.journal.events("episode")
        )
        cutoff, frozen = service.successor_input(results)
        successor = OnDemandResearch(
            store=base.store,
            parent_budget=base.budget,
            episode_deadline=base.deadline,
            run_id="captured-failure-successor",
            cutoff=cutoff,
            pit_lane=base.pit_lane,
            templates=tuple(service.templates.values()),
            frozen_input=frozen,
            clock=lambda: RETRIEVED,
        )
        available = await successor.request("lookup_fund_distributions", {"ts_code": "510300.SH"})
        assert available["status"] == "data_gap"
        assert await successor.fulfill_pending() == ()
        assert len(transport.requests) == 1

    asyncio.run(run())


def test_modeled_historical_route_refuses_current_metadata_and_incomplete_session(
    tmp_path: Path,
) -> None:
    from datetime import UTC, datetime

    from market_impact_agent.data_inputs import FrozenDataSnapshotInput
    from tests.test_historical_ashare_inputs import _source

    market = _source(tmp_path)
    journal = RunJournal.authoritative(market.store)
    journal.start_run(
        run_id="historical-episode", config_hash=canonical_hash("history"), created_at=RETRIEVED
    )
    budget = ModelBudget(journal, "historical-episode", 10, 1_000_000)
    templates: list[ResearchSourceTemplate] = []
    transports: list[FakeTransport] = []
    for api in ("daily", "stock_basic", "news"):
        config = load_tushare_observation_source(
            Path(f"examples/providers/tushare-observation-{api.replace('_', '-')}-v1.json")
        )
        transport = FakeTransport([])
        provider = TushareObservationProvider(
            TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
        )
        templates.append(ResearchSourceTemplate.from_tushare(provider, config.source_id))
        transports.append(transport)
    research = OnDemandResearch(
        store=market.store,
        parent_budget=budget,
        episode_deadline=RETRIEVED + timedelta(hours=1),
        run_id="historical-tools",
        cutoff=datetime(2025, 1, 3, 1, 25, tzinfo=UTC),
        pit_lane=DataPITLane.MODELED,
        templates=tuple(templates),
        historical_inputs=market,
        frozen_input=FrozenDataSnapshotInput(frozenset(market.snapshot_ids)),
        clock=lambda: RETRIEVED,
    )

    async def run() -> None:
        metadata = await research.request("lookup_company_profile", {"ts_code": "000001.SZ"})
        assert metadata == {"status": "data_gap", "error_kind": "historical_source_not_projectable"}
        news = await research.request(
            "lookup_news_events",
            {"start_date": "2025-01-02 00:00:00", "end_date": "2025-01-03 00:00:00"},
        )
        assert news == metadata
        for end in ("20250103", "20260905"):
            future = await research.request(
                "lookup_stock_prices",
                {"ts_code": "000001.SZ", "start_date": "20250102", "end_date": end},
            )
            assert future == {
                "status": "data_gap",
                "error_kind": "historical_price_window_after_completed_session",
            }
        assert await research.fulfill_pending() == ()

    asyncio.run(run())
    assert not any(transport.requests for transport in transports)


@pytest.mark.parametrize(
    "invalid",
    [
        {**PARAMS, "start_date": "2026827"},
        {**PARAMS, "start_date": "20260828"},
        {**PARAMS, "ts_code": "510300.SH,510500.SH"},
        {**PARAMS, "limit": 0},
    ],
)
def test_agent_query_validation_is_recoverable_but_preparation_is_strict(
    tmp_path: Path, invalid: dict[str, object]
) -> None:
    research, transport = _setup(tmp_path)
    tool = research.descriptors()[0]

    async def scenario() -> None:
        result = cast(dict[str, object], await tool.handler(invalid))
        assert result["status"] == "validation_error"
        assert result["error_kind"] == "invalid_query_arguments"
        with pytest.raises(ValueError):
            await research.request(tool.name, invalid)
        assert await research.fulfill_pending() == ()
        registry = ToolRegistry(research.store.artifacts)
        registry.register(tool)
        with pytest.raises(PermissionError, match="capability is not allowed"):
            await registry.execute(
                ToolCall(call_id="unauthorized", name=tool.name, arguments=invalid),
                access=ToolAccessContext(
                    allowed_capabilities=frozenset(),
                    allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                    allowed_tools=frozenset({tool.name}),
                ),
            )

    asyncio.run(scenario())
    assert not transport.requests
    assert not any(
        event.event_type == "research.data.requested"
        for event in research.budget.journal.events("episode")
    )
