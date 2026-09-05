# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataPITLane
from market_impact_agent.on_demand_research import OnDemandResearch
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.research_acquisition_runtime import analyze_with_acquisition
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from tests.test_on_demand_research import _setup
from tests.test_pi_runtime import pi_profile
from tests.test_research_thesis_runtime import _answer, _repository


@pytest.mark.parametrize(
    "lane,unknown",
    [
        (DataPITLane.PROSPECTIVE, False),
        (DataPITLane.STRICT, False),
        (DataPITLane.PROSPECTIVE, True),
    ],
)
def test_pi_miss_seals_old_run_and_acquires_only_for_new_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lane: DataPITLane,
    unknown: bool,
) -> None:
    acquisition, transport = _setup(tmp_path / "authority")
    acquisition = OnDemandResearch(
        store=acquisition.store,
        parent_budget=acquisition.budget,
        episode_id="account-review-1",
        episode_deadline=acquisition.deadline,
        run_id="acquisition-research-1",
        cutoff=acquisition.cutoff,
        pit_lane=lane,
        templates=tuple(acquisition.templates.values()),
        clock=acquisition.clock,
    )
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-research-key")

    def installed(_root: Path) -> PiRuntimePermit:
        return PiRuntimePermit(
            canonical_hash(runtime_identity()), (profile.route_identity,), "synthetic-proof"
        )

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec

    async def spawn(program: str, *args: str, **kwargs: Any):
        kwargs["env"]["RESEARCH_ACQUISITION_ANSWER"] = json.dumps(_answer())
        if unknown:
            kwargs["env"]["RESEARCH_GENERATION_UNKNOWN"] = "1"
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("research_acquisition_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    authority = ResearchThesisAuthority(
        acquisition.store,
        experiment_id="study",
        arm_id="luna",
        account_scope="opaque-account-a",
        clock=acquisition.clock,
    )
    inputs = ResearchThesisRunInputs(
        _repository(at=acquisition.cutoff), "INDEX.ETF", "epoch", frozenset({5})
    )

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=acquisition.budget)
        try:
            result = await analyze_with_acquisition(
                authority=authority, provider=provider, inputs=inputs, acquisition=acquisition
            )
            if unknown:
                assert result.status == "incomplete"
                assert len(result.run_ids) == 1
                assert transport.requests == []
                assert acquisition.budget.summary()["unsettled_requests"] == 1
                assert acquisition.budget.summary()["physical_requests"] == 1
                before = acquisition.budget.summary()
                assert (
                    await analyze_with_acquisition(
                        authority=authority,
                        provider=provider,
                        inputs=inputs,
                        acquisition=acquisition,
                    )
                    == result
                )
                assert acquisition.budget.summary() == before
                return
            assert result.status == "completed", result
            if lane is DataPITLane.PROSPECTIVE:
                assert len(result.run_ids) == 2
                assert len(transport.requests) == 1
                old = authority.replay(result.run_ids[0])
                assert old["status"] == "incomplete"
                assert old["reason"] == "ResearchAcquisitionRequired"
                new = cast(dict[str, object], result.terminal["thesis"])
                assert new["as_of"] != acquisition.cutoff.isoformat().replace("+00:00", "Z")
                binding = cast(
                    dict[str, object],
                    authority.store.artifacts.read_json(
                        authority.journal.get_run(result.run_ids[1]).config_hash
                    ),
                )
                selected = cast(
                    dict[str, object],
                    authority.store.artifacts.read_json(
                        cast(str, binding["selected_inputs_artifact_hash"])
                    ),
                )
                assert "data-snapshot-" in json.dumps(selected)
                assert binding["account_scope"] == "opaque-account-a"
                assert binding["arm_id"] == "luna"
                assert acquisition.snapshots == ()
                assert acquisition.budget.summary()["physical_requests"] == 3
            else:
                assert len(result.run_ids) == 1
                assert transport.requests == []
                assert acquisition.budget.summary()["physical_requests"] == 2
            before = acquisition.budget.summary()
            replay = await analyze_with_acquisition(
                authority=authority, provider=provider, inputs=inputs, acquisition=acquisition
            )
            assert replay == result
            assert acquisition.budget.summary() == before
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_explicit_episodes_keep_distinct_deadlines_on_one_parent(tmp_path: Path) -> None:
    initial, _ = _setup(tmp_path)
    first = OnDemandResearch(
        store=initial.store,
        parent_budget=initial.budget,
        episode_id="episode-one",
        episode_deadline=initial.deadline,
        run_id="episode-one.run",
        cutoff=initial.cutoff,
        pit_lane=initial.pit_lane,
        templates=tuple(initial.templates.values()),
    )
    second = OnDemandResearch(
        store=initial.store,
        parent_budget=initial.budget,
        episode_id="episode-two",
        episode_deadline=initial.deadline + timedelta(hours=1),
        run_id="episode-two.run",
        cutoff=initial.cutoff,
        pit_lane=initial.pit_lane,
        templates=tuple(initial.templates.values()),
    )
    assert first.budget is second.budget
    with pytest.raises(ValueError, match="different content"):
        OnDemandResearch(
            store=initial.store,
            parent_budget=initial.budget,
            episode_id="episode-one",
            episode_deadline=second.deadline,
            run_id="new-run",
            cutoff=initial.cutoff,
            pit_lane=initial.pit_lane,
            templates=tuple(initial.templates.values()),
        )


def test_successor_preserves_selected_pack_and_pages_large_news_without_reinjection(
    tmp_path: Path,
) -> None:
    from market_impact_agent.on_demand_research import ResearchSourceTemplate
    from market_impact_agent.research_acquisition_runtime import freeze_acquired_research
    from market_impact_agent.tushare_observation import (
        TushareObservationProvider,
        load_tushare_observation_source,
    )
    from tests.test_on_demand_research import PARAMS
    from tests.test_tushare_observation import RETRIEVED, TOKEN, FakeTransport, _response

    base, prices_transport = _setup(tmp_path)
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-news-v1.json")
    )
    rows: list[list[object]] = [
        [
            "2026-08-28 12:00:00",
            f"unselected-news-{index}:" + "long source text " * 100,
            f"headline-{index}",
            "finance",
        ]
        for index in range(35)
    ]
    transport = FakeTransport([_response(config.fields, rows)])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    templates = (
        *base.templates.values(),
        ResearchSourceTemplate.from_tushare(provider, config.source_id),
    )
    news_args: dict[str, object] = {
        "start_date": "2026-08-28 00:00:00",
        "end_date": "2026-08-28 16:00:00",
    }
    first = OnDemandResearch(
        store=base.store,
        parent_budget=base.budget,
        episode_deadline=base.deadline,
        run_id="large-news",
        cutoff=base.cutoff,
        pit_lane=base.pit_lane,
        templates=templates,
        clock=base.clock,
    )

    async def scenario() -> None:
        await first.request("lookup_news_events", news_args)
        news_results = await first.fulfill_pending()
        cutoff, frozen = first.successor_input(news_results)
        successor = OnDemandResearch(
            store=base.store,
            parent_budget=base.budget,
            episode_deadline=base.deadline,
            run_id="candidate-source",
            cutoff=cutoff,
            pit_lane=base.pit_lane,
            templates=templates,
            frozen_input=frozen,
            clock=base.clock,
        )
        snapshot = successor.snapshots[0]
        original_snapshot = snapshot.to_dict()
        inputs = ResearchThesisRunInputs(
            _repository("510300.SH", at=cutoff), "510300.SH", "epoch", frozenset({1})
        )
        original_selected = await inputs.selected_inputs()
        await successor.request("lookup_fund_prices", PARAMS)
        results = await successor.fulfill_pending()
        final, final_frozen = await freeze_acquired_research(inputs, successor, results)
        selected = await final.selected_inputs()
        assert "unselected-news-" not in json.dumps(selected)
        assert "observation_count" in json.dumps(selected)
        assert len(final_frozen.authorized_snapshot_ids) == 2
        for reference in inputs.repository.evidence_pack.evidence:
            assert await final.repository.read_evidence({"evidence_id": reference.evidence_id}) == (
                await inputs.repository.read_evidence({"evidence_id": reference.evidence_id})
            )
        assert await inputs.selected_inputs() == original_selected
        tool = next(t for t in successor.descriptors() if t.name == "lookup_news_events")
        observations: list[dict[str, object]] = []
        for offset in (0, 20):
            page = cast(
                dict[str, object], await tool.handler({**news_args, "offset": offset, "limit": 20})
            )
            assert page["projection_version"] == "compact-facts-v2"
            assert page["source_api"] == "news"
            observations.extend(cast(list[dict[str, object]], page["observations"]))
        originals = sorted(snapshot.observations, key=lambda item: item.observation_id)
        assert len(observations) == 35
        for projected, original in zip(observations, originals, strict=True):
            assert projected["observation_id"] == original.observation_id
            assert projected["raw_content_hash"] == original.raw_content_hash
            assert projected["times"] == original.times.to_dict()
            assert projected["record"] == original.normalized_payload["record"]
            assert "normalized_payload" not in projected and "provider_version" not in projected
        assert base.store.get(snapshot.snapshot_id).to_dict() == original_snapshot
        assert len(transport.requests) == len(prices_transport.requests) == 1

    asyncio.run(scenario())
