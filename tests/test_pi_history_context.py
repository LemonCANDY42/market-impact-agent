"""One cumulative history allowance across direct injection, tools and replay."""

# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.pi_execution import PiInvocationContext, execute_pi_once
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import ArtifactStore, RunJournal

from . import test_research_thesis_runtime as thesis_fixture
from .test_agent_engine import NOW, SimulatedCrash
from .test_research_thesis_runtime import (
    thesis_network,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)


@pytest.fixture
def history_network(
    thesis_network: tuple[ModelProviderProfile, list[str]],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> ModelProviderProfile:
    spawn = asyncio.create_subprocess_exec

    async def wrapped(program: str, *args: str, **kwargs: Any):
        kwargs["env"]["HISTORY_TOOL_CALLS"] = os.environ.get("HISTORY_TOOL_CALLS", "1")
        kwargs["env"]["HISTORY_COMPACTION"] = os.environ.get("HISTORY_COMPACTION", "0")
        return await spawn(
            program,
            "--import",
            str(Path(__file__).with_name("history_context_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", wrapped)
    return thesis_network[0]


@pytest.mark.parametrize(
    "name",
    [
        "read_current_thesis",
        "search_prior_decisions",
        "read_prior_decisions",
        "read_current_account",
    ],
)
@pytest.mark.parametrize("prior_bytes", [5000, 9000])
def test_prior_and_serialized_tools_share_limit_and_replay(
    tmp_path: Path,
    history_network: ModelProviderProfile,
    name: str,
    prior_bytes: int,
) -> None:
    async def scenario() -> None:
        journal = RunJournal(tmp_path / "runs.sqlite3")
        journal.start_run(run_id="history", config_hash=canonical_hash("history"), created_at=NOW)
        context = PiInvocationContext("history", 1, journal, ArtifactStore(tmp_path / "cas"))
        calls: list[object] = []

        async def read(arguments: dict[str, object]) -> object:
            calls.append(arguments)
            return {"history": "x" * 3500}

        descriptor = ToolDescriptor(
            name=name,
            version="v1",
            description="Read authorized context.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            required_capabilities=frozenset({"read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=2,
            max_result_bytes=5000,
            handler=read,
        )
        provider = PiRuntimeProvider(history_network)

        async def invoke(initial: str = "p" * prior_bytes):
            return await execute_pi_once(
                provider,
                context=context,
                messages=({"role": "user", "content": initial},),
                initial_history=initial,
                max_output_tokens=256,
                timeout_seconds=20,
                attempt_observer=lambda _: None,
                readonly_tools=(descriptor,),
            )

        append = journal.append

        def crash(**kwargs: Any):
            event = append(**kwargs)
            if kwargs["event_type"] == "pi.role.tool.completed":
                raise SimulatedCrash()
            return event

        try:
            journal.append = crash
            with pytest.raises(SimulatedCrash):
                await invoke()
            journal.append = append
            if name == "read_current_account" or prior_bytes == 5000:
                assert not (await invoke()).tool_calls
            else:
                with pytest.raises(PermissionError, match="cumulative historical"):
                    await invoke()
                assert (
                    len(
                        [
                            event
                            for event in journal.events("history")
                            if event.event_type == "model.turn.started"
                        ]
                    )
                    == 1
                )
            assert calls == [{}]  # durable tool replay neither refetches nor resets the cap
            with pytest.raises(PermissionError, match="initial history changed"):
                await invoke("")
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_search_summaries_accumulate_and_second_invocation_cannot_reset(
    tmp_path: Path,
    history_network: ModelProviderProfile,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HISTORY_TOOL_CALLS", "3")

    async def scenario() -> None:
        journal = RunJournal(tmp_path / "runs.sqlite3")
        journal.start_run(run_id="search", config_hash=canonical_hash("search"), created_at=NOW)
        context = PiInvocationContext("search", 1, journal, ArtifactStore(tmp_path / "cas"))

        async def search(_: dict[str, object]) -> object:
            return {"hits": [{"summary": "x" * 4500}]}

        descriptor = ToolDescriptor(
            name="search_prior_decisions",
            version="v1",
            description="Search history.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            required_capabilities=frozenset({"read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=2,
            max_result_bytes=6000,
            handler=search,
        )
        provider = PiRuntimeProvider(history_network)

        async def invoke(current: PiInvocationContext):
            return await execute_pi_once(
                provider,
                context=current,
                messages=({"role": "user", "content": "Search prior opinions."},),
                max_output_tokens=256,
                timeout_seconds=20,
                attempt_observer=lambda _: None,
                readonly_tools=(descriptor,),
            )

        try:
            with pytest.raises(PermissionError, match="cumulative historical"):
                await invoke(context)
            starts = [
                event
                for event in journal.events("search")
                if event.event_type == "model.turn.started"
            ]
            assert len(starts) == 3  # third tool result never reaches a fourth model request
            with pytest.raises(PermissionError, match="cumulative historical"):
                await invoke(replace(context, ordinal=2))
            assert [
                event
                for event in journal.events("search")
                if event.event_type == "model.turn.started"
            ] == starts
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_real_prior_thesis_over_cap_stops_update_before_model(
    tmp_path: Path,
    thesis_network: tuple[ModelProviderProfile, list[str]],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    answer = thesis_fixture._answer()
    answer["thesis"] = "unverified prior narrative " * 600
    monkeypatch.setattr(thesis_fixture, "_answer", lambda: answer)

    async def scenario() -> None:
        store = LocalDataSnapshotStore(tmp_path / "harness")
        authority = ResearchThesisAuthority(
            store, experiment_id="study", arm_id="arm", clock=lambda: NOW + timedelta(days=1)
        )
        provider = PiRuntimeProvider(thesis_network[0])
        inputs = ResearchThesisRunInputs(
            thesis_fixture._repository(), "INDEX.ETF", "epoch", frozenset({5})
        )
        try:
            assert (await authority.analyze(run_id="prior", provider=provider, inputs=inputs))[
                "status"
            ] == "completed"
            later = replace(
                inputs, repository=thesis_fixture._repository(at=NOW + timedelta(days=1))
            )
            terminal = await authority.analyze(
                run_id="update", provider=provider, inputs=later, prior_thesis_run_id="prior"
            )
            assert terminal["status"] == "incomplete"
            assert terminal["reason"] == "PermissionError"
            assert not any(
                event.event_type == "model.turn.started"
                for event in authority.journal.events("update")
            )
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["pi.role.history.initial", "pi.role.tool.completed"])
def test_history_accounting_receipts_require_root_authority(tmp_path: Path, kind: str) -> None:
    store = LocalDataSnapshotStore(tmp_path / "source")
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="history-forgery", config_hash=canonical_hash("frozen"), created_at=NOW
    )
    with pytest.raises(PermissionError, match="root-authenticated signer"):
        journal.append(
            run_id="history-forgery",
            event_id="forged-history-receipt",
            event_type=kind,
            observed_at=NOW,
            payload={"bytes": 0},
        )
    assert journal.event("forged-history-receipt") is None


def test_native_compaction_replay_preserves_history_reservations(
    tmp_path: Path,
    history_network: ModelProviderProfile,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from market_impact_agent import pi_execution
    from market_impact_agent.agent_runtime import Utf8TokenEstimator

    monkeypatch.setenv("HISTORY_TOOL_CALLS", "3")
    monkeypatch.setenv("HISTORY_COMPACTION", "1")

    class CompactOnce(Utf8TokenEstimator):
        checked = False

        def count_request(self, messages: Any, tools: Any) -> int:
            if len(messages) == 1 and isinstance(messages[0], dict):
                native = messages[0].get("messages", [])
                if native and native[-1].get("role") == "toolResult" and not self.checked:
                    self.checked = True
                    return 1_000_000
            return super().count_request(messages, tools)

    monkeypatch.setattr(pi_execution, "Utf8TokenEstimator", CompactOnce)

    async def scenario() -> None:
        journal = RunJournal(tmp_path / "runs.sqlite3")
        journal.start_run(
            run_id="compact-history", config_hash=canonical_hash("history"), created_at=NOW
        )
        context = PiInvocationContext(
            "compact-history", 1, journal, ArtifactStore(tmp_path / "cas")
        )
        calls: list[object] = []

        async def search(arguments: dict[str, object]) -> object:
            calls.append(arguments)
            return {"hits": [{"summary": "x" * 3000}]}

        descriptor = ToolDescriptor(
            name="search_prior_decisions",
            version="v1",
            description="Search history.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            required_capabilities=frozenset({"read"}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=2,
            max_result_bytes=4000,
            handler=search,
        )
        provider = PiRuntimeProvider(history_network)

        async def invoke():
            return await execute_pi_once(
                provider,
                context=context,
                messages=({"role": "user", "content": "p" * 4000},),
                initial_history="p" * 4000,
                max_output_tokens=256,
                timeout_seconds=20,
                attempt_observer=lambda _: None,
                readonly_tools=(descriptor,),
            )

        append = journal.append

        def crash(**kwargs: Any):
            event = append(**kwargs)
            if kwargs["event_type"] == "pi.context.compacted":
                raise SimulatedCrash()
            return event

        try:
            journal.append = crash
            with pytest.raises(SimulatedCrash):
                await invoke()
            journal.append = append
            assert len(calls) == 1
            compacted = [
                event
                for event in journal.events("compact-history")
                if event.event_type == "pi.context.compacted"
            ]
            assert len(compacted) == 1
            with pytest.raises(PermissionError, match="cumulative historical"):
                await invoke()
            # The first result is neither forgotten nor charged twice: second
            # result fits, third crosses 12k even after the upstream summary.
            assert len(calls) == 3
            assert [
                event
                for event in journal.events("compact-history")
                if event.event_type == "pi.context.compacted"
            ] == compacted
            starts = [
                event
                for event in journal.events("compact-history")
                if event.event_type == "model.turn.started"
            ]
            summaries = [
                event
                for event in journal.events("compact-history")
                if event.event_type == "pi.context.frozen"
                and event.payload.get("purpose") == "compaction"
            ]
            assert summaries  # upstream may summarize both the cut history and split turn
            assert len(starts) == 3 + len(summaries)
            with pytest.raises(PermissionError, match="cumulative historical"):
                await invoke()
            assert [
                event
                for event in journal.events("compact-history")
                if event.event_type == "model.turn.started"
            ] == starts
        finally:
            await provider.close()

    asyncio.run(scenario())
