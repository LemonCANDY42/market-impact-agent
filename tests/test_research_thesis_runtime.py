"""Dynamic research authority with the real pi module and synthetic network only."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.dynamic_effectiveness import DatePresentation
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    model_provider_profile_from_dict,
)
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
    reopen_completed_research_thesis,
)
from market_impact_agent.runtime_store import RunStatus

from .test_agent_engine import NOW
from .test_pi_runtime import pi_profile


def _repository(
    target_id: str = "INDEX.ETF",
    *,
    at: datetime = NOW,
    event_id: str = "earnings-1",
) -> FrozenResearchRepository:
    release = {
        "published_at": (at - timedelta(minutes=10)).isoformat(),
        "headline": "Revenue exceeded the point-in-time consensus estimate.",
        "historical_context": (
            "In 2019, 2019年12月份, 2 Feb, 2020, Feb. 2 and Feb. 3 were discussed."
        ),
        "pathogen": "2019-nCov",
        "industry_id": "sw2021_electronics",
        "doi": "10.1111/j.1467-6261.1970.tb00518.x",
        "reference_url": "https://example.test/archive/2020/02/02/release",
        "actual_revenue": 112,
        "prior_consensus": 100,
    }
    market = {
        "as_of": (at - timedelta(minutes=2)).isoformat(),
        "target": target_id,
        "last": 10,
        "five_session_return_before_event": -0.03,
    }
    references = (
        EvidenceReference(
            "release",
            "incremental-fact",
            "official://issuer/release",
            EvidenceTier.OFFICIAL,
            at - timedelta(minutes=10),
            canonical_hash(release),
            "Revenue exceeded the frozen prior consensus.",
        ),
        EvidenceReference(
            "market",
            "priced-in-context",
            "market://index-etf",
            EvidenceTier.REGULATED,
            at - timedelta(minutes=2),
            canonical_hash(market),
            "Pre-event price performance is frozen at the cutoff.",
        ),
    )
    pack = EvidencePack.build(
        event_id=event_id,
        as_of=at,
        research_question=f"What is the likely direction for {target_id}?",
        evidence=references,
        pattern_packs=(),
        allowed_targets=(target_id,),
        data_gaps=("next-quarter management execution is unknown",),
    )
    return FrozenResearchRepository(
        evidence_pack=pack,
        evidence_documents={"release": release, "market": market},
        pattern_packs={},
    )


def _answer() -> dict[str, object]:
    return {
        "horizon_band": "tactical",
        "primary_horizon_sessions": 5,
        "base_case_direction": "up",
        "thesis": "The positive surprise should support near-term earnings revisions.",
        "priced_in_assessment": (
            "The prior decline suggests the positive surprise was not fully priced."
        ),
        "transmission": ["revenue surprise -> earnings revisions -> ETF constituent value"],
        "counter_scenario": "Guidance could show that the beat is not repeatable.",
        "evidence_refs": ["release", "market"],
        "counterevidence_refs": [],
        "invalidation_conditions": ["Management cuts forward revenue guidance."],
        "review_after_sessions": 1,
        "typed_unknowns": ["next-quarter management execution"],
    }


@pytest.fixture
def thesis_network(monkeypatch: pytest.MonkeyPatch) -> tuple[ModelProviderProfile, list[str]]:
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-thesis-key")
    permit = PiRuntimePermit(
        canonical_hash(runtime_identity()),
        (profile.route_identity,),
        "synthetic-thesis-proof",
    )

    def installed(_root: Path) -> PiRuntimePermit:
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec
    spawns: list[str] = []

    async def spawn(program: str, *args: str, **kwargs: Any):
        spawns.append(program)
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(_answer())
        if os.environ.get("ROLE_TOOL_FIXTURE"):
            kwargs["env"]["ROLE_TOOL_FIXTURE"] = os.environ["ROLE_TOOL_FIXTURE"]
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    return profile, spawns


def test_dynamic_research_uses_native_pi_and_replays_without_regeneration(
    tmp_path: Path, thesis_network: tuple[ModelProviderProfile, list[str]]
) -> None:
    profile, spawns = thesis_network

    async def scenario() -> None:
        store = LocalDataSnapshotStore(tmp_path / "harness")
        authority = ResearchThesisAuthority(
            store,
            experiment_id="dynamic-effectiveness-v1",
            arm_id="luna-max",
            clock=lambda: NOW,
        )
        provider = PiRuntimeProvider(profile)
        inputs = ResearchThesisRunInputs(
            _repository(),
            "INDEX.ETF",
            "thesis-epoch-v1",
            frozenset({1, 3, 5, 10}),
        )
        try:
            terminal = await authority.analyze(
                run_id="research-thesis-luna-1", provider=provider, inputs=inputs
            )
            assert terminal["status"] == "completed"
            assert cast(dict[str, object], terminal["thesis"])["base_case_direction"] == "up"
            assert authority.journal.get_run("research-thesis-luna-1").status is RunStatus.COMPLETED
            thesis, source = reopen_completed_research_thesis(
                journal=authority.journal,
                artifact_store=store.artifacts,
                run_id="research-thesis-luna-1",
            )
            assert thesis.primary_horizon_sessions == 5
            assert source["thesis"] == terminal["thesis"]
            replay = await authority.analyze(
                run_id="research-thesis-luna-1", provider=provider, inputs=inputs
            )
            assert replay == terminal
            assert len(spawns) == 1
            changed_payload = profile.to_dict()
            changed_payload["reasoning_effort"] = "high"
            changed_payload["profile_id"] = "model-provider-" + canonical_hash(
                {key: value for key, value in changed_payload.items() if key != "profile_id"}
            )
            changed_provider = PiRuntimeProvider(model_provider_profile_from_dict(changed_payload))
            try:
                with pytest.raises(PermissionError, match="different frozen inputs"):
                    await authority.analyze(
                        run_id="research-thesis-luna-1",
                        provider=changed_provider,
                        inputs=inputs,
                    )
            finally:
                await changed_provider.close()
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_research_thesis_rejects_evidence_after_authority_clock_before_dispatch(
    tmp_path: Path, thesis_network: tuple[ModelProviderProfile, list[str]]
) -> None:
    profile, spawns = thesis_network

    async def scenario() -> None:
        store = LocalDataSnapshotStore(tmp_path / "harness")
        authority = ResearchThesisAuthority(
            store,
            experiment_id="dynamic-effectiveness-v1",
            arm_id="luna-max",
            clock=lambda: NOW,
        )
        provider = PiRuntimeProvider(profile)
        try:
            with pytest.raises(PermissionError, match="after the authority clock"):
                await authority.analyze(
                    run_id="future-evidence",
                    provider=provider,
                    inputs=ResearchThesisRunInputs(
                        _repository(at=NOW + timedelta(days=1)),
                        "INDEX.ETF",
                        "thesis-epoch-v1",
                        frozenset({5}),
                    ),
                )
            assert spawns == []
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_relative_date_presentation_changes_only_structured_time_labels() -> None:
    async def scenario() -> None:
        repository = _repository()
        inputs = ResearchThesisRunInputs(
            repository,
            "INDEX.ETF",
            "memory-check-v1",
            frozenset({5}),
            DatePresentation.RELATIVE_OFFSET,
        )
        selected = await inputs.selected_inputs()
        assert selected["point_in_time_cutoff"] == "T0"
        evidence = cast(list[dict[str, object]], selected["evidence"])
        reference = cast(dict[str, object], evidence[0]["reference"])
        document = cast(dict[str, object], evidence[0]["document"])
        assert reference["available_at"] == "T0"
        assert reference["source_ref"] == "relative-source://withheld"
        assert document["published_at"] == "T0"
        assert document["headline"] == "Revenue exceeded the point-in-time consensus estimate."
        assert "2019" not in cast(str, document["historical_context"])
        assert "2020" not in cast(str, document["historical_context"])
        assert "Feb. 2" not in cast(str, document["historical_context"])
        assert "Feb. 3" not in cast(str, document["historical_context"])
        assert document["pathogen"] == "2019-nCov"
        assert document["industry_id"] == "sw2021_electronics"
        assert document["doi"] == "10.1111/j.1467-6261.1970.tb00518.x"
        assert cast(str, document["reference_url"]).startswith("relative-locator://")
        assert inputs.identity_dict()["as_of"] == NOW.isoformat().replace("+00:00", "Z")

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "presentation", [DatePresentation.TRUE_DATE, DatePresentation.RELATIVE_OFFSET]
)
def test_later_review_reopens_signed_prior_thesis_without_summary_substitution(
    tmp_path: Path,
    thesis_network: tuple[ModelProviderProfile, list[str]],
    presentation: DatePresentation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, spawns = thesis_network

    answer = _answer()
    answer["thesis"] = "The 2020-02-02 release changes the outlook relative to February 1, 2020."
    monkeypatch.setattr(__name__ + "._answer", lambda: answer)

    async def scenario() -> None:
        store = LocalDataSnapshotStore(tmp_path / "harness")
        clock = [NOW]
        authority = ResearchThesisAuthority(
            store,
            experiment_id="cadence-effectiveness-v1",
            arm_id="event-driven",
            clock=lambda: clock[0],
        )
        provider = PiRuntimeProvider(profile)
        try:
            first = await authority.analyze(
                run_id="review-0",
                provider=provider,
                inputs=ResearchThesisRunInputs(
                    _repository(), "INDEX.ETF", "review-sequence-v1", frozenset({5})
                ),
            )
            assert first["status"] == "completed"
            clock[0] = NOW + timedelta(days=1)
            second = await authority.analyze(
                run_id="review-1",
                provider=provider,
                inputs=ResearchThesisRunInputs(
                    _repository(at=NOW + timedelta(days=1), event_id="earnings-1-update"),
                    "INDEX.ETF",
                    "review-sequence-v1",
                    frozenset({5}),
                    presentation,
                ),
                prior_thesis_run_id="review-0",
            )
            assert second["status"] == "completed"
            binding = cast(
                dict[str, object],
                store.artifacts.read_json(authority.journal.get_run("review-1").config_hash),
            )
            prior = cast(dict[str, object], binding["prior_thesis"])
            assert prior["run_id"] == "review-0"
            selected = cast(
                dict[str, object],
                store.artifacts.read_json(cast(str, binding["selected_inputs_artifact_hash"])),
            )
            if presentation is DatePresentation.TRUE_DATE:
                assert selected["prior_thesis"] == prior
            else:
                model_prior = cast(dict[str, object], selected["prior_thesis"])
                original_thesis = cast(dict[str, object], prior["thesis"])
                masked_thesis = cast(dict[str, object], model_prior["thesis"])
                assert original_thesis["as_of"] == NOW.isoformat().replace("+00:00", "Z")
                assert masked_thesis["as_of"] == "T-1 calendar days"
                assert "2020-02-02" in cast(str, original_thesis["thesis"])
                assert "2020" not in cast(str, masked_thesis["thesis"])
                for key in ("terminal_hash", "binding_hash", "journal_hash"):
                    assert model_prior[key] == prior[key]
                frozen = [
                    event
                    for event in authority.journal.events("review-1")
                    if event.event_type == "pi.context.frozen"
                ]
                assert frozen
                native_input = store.artifacts.read_json(
                    cast(str, frozen[0].payload["artifact_hash"])
                )
                assert cast(str, original_thesis["thesis"]) not in json.dumps(native_input)
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "presentation", [DatePresentation.TRUE_DATE, DatePresentation.RELATIVE_OFFSET]
)
def test_research_role_executes_durable_readonly_tool_and_replays(
    tmp_path: Path,
    thesis_network: tuple[ModelProviderProfile, list[str]],
    monkeypatch: pytest.MonkeyPatch,
    presentation: DatePresentation,
) -> None:
    from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect

    monkeypatch.setenv("ROLE_TOOL_FIXTURE", "1")
    profile, spawns = thesis_network
    calls: list[dict[str, object]] = []

    async def read(arguments: dict[str, object]) -> object:
        calls.append(arguments)
        return {
            "fact": "frozen-revenue",
            "evidence_id": "release",
            "as_of": "2020-02-02T00:00:00Z",
            "thesis": "The February 1, 2020 report changes the outlook.",
        }

    descriptor = ToolDescriptor(
        name="read_frozen_fact",
        version="frozen-1",
        description="Read frozen revenue.",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        required_capabilities=frozenset({"evidence.read"}),
        side_effect=ToolSideEffect.READ_ONLY,
        timeout_seconds=2,
        max_result_bytes=1000,
        handler=read,
    )

    async def scenario() -> None:
        store = LocalDataSnapshotStore(tmp_path / "harness")
        authority = ResearchThesisAuthority(
            store, experiment_id="tools", arm_id="luna", clock=lambda: NOW
        )
        provider = PiRuntimeProvider(profile)
        inputs = ResearchThesisRunInputs(
            _repository(), "INDEX.ETF", "epoch", frozenset({5}), presentation
        )
        try:
            result = await authority.analyze(
                run_id="tool-role", provider=provider, inputs=inputs, readonly_tools=(descriptor,)
            )
            assert result["status"] == "completed", result
            assert calls == [{}]
            event = next(
                event
                for event in authority.journal.events("tool-role")
                if event.event_type == "pi.role.tool.completed"
            )
            tool_result = store.artifacts.read_json(cast(str, event.payload["artifact_hash"]))
            if presentation is DatePresentation.RELATIVE_OFFSET:
                assert "2020" not in json.dumps(tool_result)
            else:
                assert "2020-02-02" in json.dumps(tool_result)
            assert (
                len(
                    [
                        event
                        for event in authority.journal.events("tool-role")
                        if event.event_type == "pi.role.tool.completed"
                    ]
                )
                == 1
            )
            assert (
                await authority.analyze(
                    run_id="tool-role",
                    provider=provider,
                    inputs=inputs,
                    readonly_tools=(descriptor,),
                )
                == result
            )
            assert calls == [{}]
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_readonly_role_recovers_after_durable_tool_without_reexecution(
    tmp_path: Path,
    thesis_network: tuple[ModelProviderProfile, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
    from market_impact_agent.pi_execution import PiInvocationContext, execute_pi_once
    from market_impact_agent.runtime_store import ArtifactStore, RunJournal

    from .test_agent_engine import SimulatedCrash

    monkeypatch.setenv("ROLE_TOOL_FIXTURE", "1")
    profile, _ = thesis_network
    calls: list[object] = []

    async def read(arguments: dict[str, object]) -> object:
        calls.append(arguments)
        return {"fact": "frozen-revenue"}

    descriptor = ToolDescriptor(
        name="read_frozen_fact",
        version="frozen-1",
        description="Read frozen revenue.",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        required_capabilities=frozenset({"evidence.read"}),
        side_effect=ToolSideEffect.READ_ONLY,
        timeout_seconds=2,
        max_result_bytes=1000,
        handler=read,
    )

    async def scenario() -> None:
        journal = RunJournal(tmp_path / "run.sqlite3")
        journal.start_run(run_id="recover", config_hash=canonical_hash("fixed"), created_at=NOW)
        context = PiInvocationContext("recover", 1, journal, ArtifactStore(tmp_path / "artifacts"))
        provider = PiRuntimeProvider(profile)
        append = journal.append

        def crash(**kwargs: Any):
            event = append(**kwargs)
            if kwargs["event_type"] == "pi.role.tool.completed":
                raise SimulatedCrash()
            return event

        async def invoke():
            return await execute_pi_once(
                provider,
                context=context,
                messages=({"role": "user", "content": "Inspect frozen revenue."},),
                max_output_tokens=256,
                timeout_seconds=20,
                attempt_observer=lambda _: None,
                readonly_tools=(descriptor,),
            )

        try:
            journal.append = crash
            with pytest.raises(SimulatedCrash):
                await invoke()
            journal.append = append
            result = await invoke()
            assert not result.tool_calls
            assert calls == [{}]
            assert (
                len(
                    [
                        event
                        for event in journal.events("recover")
                        if event.event_type == "model.turn.started"
                    ]
                )
                == 2
            )
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("crash_after_error, tool_limit", [(False, 2), (True, 2), (True, 1)])
def test_native_research_corrects_query_error_with_durable_replay(
    tmp_path: Path,
    thesis_network: tuple[ModelProviderProfile, list[str]],
    monkeypatch: pytest.MonkeyPatch,
    crash_after_error: bool,
    tool_limit: int,
) -> None:
    from dataclasses import replace

    from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
    from market_impact_agent.pi_execution import PiInvocationContext, PiRoleJournal, execute_pi_once
    from market_impact_agent.tushare_observation import (
        TushareObservationProvider,
        load_tushare_observation_source,
    )

    from .test_agent_engine import SimulatedCrash
    from .test_on_demand_research import CONFIG, _setup  # pyright: ignore[reportPrivateUsage]
    from .test_tushare_observation import TOKEN, FakeTransport

    monkeypatch.setenv("ROLE_TOOL_FIXTURE", "query_validation")
    profile, _ = thesis_network
    value = profile.to_dict()
    cast(dict[str, object], value["budget"])["max_tool_calls"] = tool_limit
    value.pop("profile_id")
    value["profile_id"] = f"model-provider-{canonical_hash(value)}"
    profile = model_provider_profile_from_dict(value)
    base, _ = _setup(tmp_path)
    config = load_tushare_observation_source(
        CONFIG.with_name("tushare-observation-stk-limit-v1.json")
    )
    transport = FakeTransport([])
    source = TushareObservationProvider(TOKEN, (config,), transport=transport)
    research = OnDemandResearch(
        store=base.store,
        parent_budget=base.budget,
        episode_deadline=base.deadline,
        run_id="episode",
        cutoff=base.cutoff,
        pit_lane=base.pit_lane,
        templates=(ResearchSourceTemplate.from_tushare(source, config.source_id),),
        clock=base.clock,
    )
    descriptor = research.descriptors()[0]
    executed: list[dict[str, object]] = []

    async def track(arguments: dict[str, object]) -> object:
        executed.append(arguments)
        return await descriptor.handler(arguments)

    async def scenario() -> None:
        authority = ResearchThesisAuthority(
            base.store, experiment_id="query-repair", arm_id="luna", clock=base.clock
        )
        journal = cast(PiRoleJournal, PiRoleJournal.authoritative(base.store))
        journal.bind(
            run_id="episode",
            writer=authority._events,  # pyright: ignore[reportPrivateUsage]
        )
        context = PiInvocationContext("episode", 1, journal, base.store.artifacts, clock=base.clock)
        provider = PiRuntimeProvider(profile, budget=base.budget)
        append = journal.append

        def crash(**kwargs: Any):
            event = append(**kwargs)
            if kwargs["event_type"] == "pi.role.tool.completed":
                raise SimulatedCrash()
            return event

        async def invoke():
            return await execute_pi_once(
                provider,
                context=context,
                messages=({"role": "user", "content": "Inspect the frozen price limits."},),
                max_output_tokens=256,
                timeout_seconds=20,
                attempt_observer=lambda _: None,
                readonly_tools=(replace(descriptor, handler=track),),
            )

        try:
            if crash_after_error:
                journal.append = crash
                with pytest.raises(SimulatedCrash):
                    await invoke()
                journal.append = append
                assert not any(
                    event.event_type == "research.data.requested"
                    for event in journal.events("episode")
                )
            if tool_limit == 1:
                with pytest.raises(PermissionError, match="tool budget"):
                    await invoke()
                assert len(executed) == 1
                assert not any(
                    event.event_type == "research.data.requested"
                    for event in journal.events("episode")
                )
                assert base.budget.summary()["physical_requests"] == 2
                assert not transport.requests
                return
            result = await invoke()
            assert not result.tool_calls
            events = journal.events("episode")
            completed = [e for e in events if e.event_type == "pi.role.tool.completed"]
            assert len(completed) == 2
            saved = cast(
                dict[str, object],
                base.store.artifacts.read_json(cast(str, completed[0].payload["artifact_hash"])),
            )
            error = cast(
                dict[str, object],
                base.store.artifacts.read_json(cast(str, saved["result_artifact_hash"])),
            )
            assert error["status"] == "validation_error"
            assert error["error_kind"] == "invalid_query_arguments"
            assert "do not combine" in cast(str, error["message"])
            assert len(executed) == 2  # Replay does not rerun the rejected query.
            assert sum(e.event_type == "research.data.requested" for e in events) == 1
            assert base.budget.summary()["physical_requests"] == 3
            assert base.budget.summary()["unsettled_requests"] == 0
            assert not transport.requests
            assert await invoke() == result
            assert len(executed) == 2
            assert base.budget.summary()["physical_requests"] == 3
        finally:
            journal.append = append
            await provider.close()

    asyncio.run(scenario())
