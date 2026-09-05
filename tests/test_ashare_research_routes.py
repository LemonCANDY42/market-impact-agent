# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest

from market_impact_agent.on_demand_research import (
    OnDemandResearch,
    ResearchQueryValidationError,
    ResearchSourceTemplate,
)
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from tests.test_on_demand_research import _setup
from tests.test_tushare_observation import RETRIEVED, TOKEN, FakeTransport, _response


@pytest.mark.parametrize("api", ["rt_min", "rt_etf_min"])
def test_current_quote_frozen_freshness_uses_bounded_successor_acquisition(
    tmp_path: Path, api: str
) -> None:
    base, _ = _setup(tmp_path)
    config = load_tushare_observation_source(
        Path(f"examples/providers/tushare-observation-{api.replace('_', '-')}-v1.json")
    )
    now = RETRIEVED
    symbol = "600519.SH" if api == "rt_min" else "159919.SZ"

    def row(at: str) -> list[object]:
        return [
            symbol if field == "ts_code" else at if field == "time" else 10
            for field in config.fields
        ]

    transport = FakeTransport(
        [
            _response(config.fields, [row("2026-08-28 16:00:00")]),
            _response(config.fields, [row("2026-08-28 16:03:00")]),
        ]
    )
    provider = TushareObservationProvider(TOKEN, (config,), transport=transport, clock=lambda: now)
    template = ResearchSourceTemplate.from_tushare(provider, config.source_id)
    params: dict[str, object] = {"ts_code": symbol, "freq": "1MIN"}
    with pytest.raises(ResearchQueryValidationError, match="freq=1MIN"):
        template.validate({"ts_code": symbol, "freq": "5MIN"})
    first = OnDemandResearch(
        store=base.store,
        parent_budget=base.budget,
        episode_deadline=base.deadline,
        run_id="quote-initial",
        cutoff=now,
        pit_lane=base.pit_lane,
        templates=(template,),
        clock=lambda: now,
    )

    async def run() -> None:
        nonlocal now
        request = await first.request(template.tool_name, params)
        assert request["status"] == "continuation_required"
        result = await first.fulfill_pending()
        cutoff, frozen = first.successor_input(result)
        fresh = OnDemandResearch(
            store=base.store,
            parent_budget=base.budget,
            episode_deadline=base.deadline,
            run_id="quote-fresh",
            cutoff=cutoff,
            pit_lane=base.pit_lane,
            templates=(template,),
            frozen_input=frozen,
            clock=lambda: now,
        )
        assert (await fresh.request(template.tool_name, params))["status"] == "available"
        assert len(transport.requests) == 1
        now += timedelta(minutes=3)
        stale = OnDemandResearch(
            store=base.store,
            parent_budget=base.budget,
            episode_deadline=base.deadline,
            run_id="quote-next-view",
            cutoff=now,
            pit_lane=base.pit_lane,
            templates=(template,),
            frozen_input=frozen,
            clock=lambda: now,
        )
        queued = await stale.request(template.tool_name, params)
        assert queued["status"] == "continuation_required"
        assert await stale.request(template.tool_name, params) == queued
        assert len(transport.requests) == 1
        second = await stale.fulfill_pending()
        assert second[0].status == "fulfilled"
        assert await stale.fulfill_pending() == second
        assert len(transport.requests) == 2
        # The active frozen view is unchanged by fulfillment.
        assert await stale.request(template.tool_name, params) == queued

    asyncio.run(run())


def test_fund_asset_class_source_template_uses_only_concrete_identity() -> None:
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-fund-basic-v1.json")
    )
    provider = TushareObservationProvider(TOKEN, (config,), transport=FakeTransport([]))
    template = ResearchSourceTemplate.from_tushare(provider, config.source_id)
    assert template.tool_name == "lookup_fund_asset_class"
    assert template.parameters == ("ts_code",)
    template.validate({"ts_code": "159919.SZ"})
    with pytest.raises(ResearchQueryValidationError):
        template.validate({"ts_code": "159919.SZ", "fund_type": "股票型"})
