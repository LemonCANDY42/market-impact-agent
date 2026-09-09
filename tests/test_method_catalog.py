"""Optional methods through actual role authorities and upstream pi, mocked wire only."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_runtime import SkillRegistry
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.method_catalog import FrozenMethodCatalog
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)

from .test_agent_engine import NOW
from .test_agent_runtime import write_skill  # pyright: ignore[reportPrivateUsage]
from .test_portfolio_review import (
    NativePortfolio,
    _setup,  # pyright: ignore[reportPrivateUsage]
)
from .test_portfolio_review import native_portfolio as native_portfolio
from .test_research_thesis_runtime import (
    _repository,  # pyright: ignore[reportPrivateUsage]
)
from .test_research_thesis_runtime import (
    thesis_network as thesis_network,
)


def catalog(root: Path) -> FrozenMethodCatalog:
    write_skill(
        root,
        name="optional-test-method",
        instructions="ONLY_AFTER_READ_METHOD_BODY",
        capabilities=[],
    )
    write_skill(root, name="unused-method", instructions="NEVER_READ_METHOD_BODY", capabilities=[])
    return FrozenMethodCatalog.freeze(SkillRegistry(root))


def test_freeze_restricts_content_paths_and_capabilities(tmp_path: Path) -> None:
    frozen = catalog(tmp_path / "methods")
    assert "METHOD_BODY" not in json.dumps(frozen.metadata())

    async def read(arguments: dict[str, object]) -> dict[str, object]:
        return cast(dict[str, object], await frozen.read_tool().handler(arguments))

    path = str(frozen.skills[0].manifest.instructions_path)
    assert asyncio.run(read({"path": path}))["body"] == "ONLY_AFTER_READ_METHOD_BODY"
    for foreign in ("/etc/passwd", str(tmp_path / "other-arm/history.json"), path + "/../SKILL.md"):
        with pytest.raises(PermissionError, match="restricted"):
            asyncio.run(read({"path": foreign}))
    Path(path).write_text("mutated")
    with pytest.raises(PermissionError, match="changed"):
        asyncio.run(read({"path": path}))
    with pytest.raises(ValueError, match="differs"):
        FrozenMethodCatalog((replace(frozen.skills[0], instructions="mutated"),))
    write_skill(
        tmp_path / "privileged",
        name="privileged",
        instructions="secret",
        capabilities=["broker.read"],
    )
    with pytest.raises(PermissionError, match="capability"):
        FrozenMethodCatalog.freeze(SkillRegistry(tmp_path / "privileged"))


@pytest.mark.parametrize("wrapped", [False, True, "always"])
def test_research_method_read_is_journaled_and_replay_does_not_reload(
    tmp_path: Path,
    thesis_network: tuple[ModelProviderProfile, list[str]],
    monkeypatch: pytest.MonkeyPatch,
    wrapped: bool | str,
) -> None:
    monkeypatch.setenv("ROLE_TOOL_FIXTURE", "method")
    if wrapped:
        monkeypatch.setenv("ROLE_JSON_WRAPPER_FIXTURE", "always" if wrapped == "always" else "1")
    frozen = catalog(tmp_path / "methods")
    profile, spawns = thesis_network

    async def scenario() -> None:
        store = LocalDataSnapshotStore(tmp_path / "harness")
        authority = ResearchThesisAuthority(
            store, experiment_id="methods", arm_id="B", clock=lambda: NOW, method_catalog=frozen
        )
        inputs = ResearchThesisRunInputs(_repository(), "INDEX.ETF", "epoch", frozenset({5}))
        provider = PiRuntimeProvider(profile)
        try:
            result = await authority.analyze(
                run_id="methods-role", provider=provider, inputs=inputs
            )
            if wrapped == "always":
                assert result["status"] == "incomplete", result
                assert result["reason"] == "ValueError"
                events = authority.journal.events("methods-role")
                assert sum(e.event_type == "pi.role.response.completed" for e in events) == 4
                assert sum(e.event_type == "pi.role.tool.completed" for e in events) == 1
                assert (
                    await authority.analyze(run_id="methods-role", provider=provider, inputs=inputs)
                    == result
                )
                assert len(spawns) == 1
                return
            assert result["status"] == "completed", result
            reads = [
                e
                for e in authority.journal.events("methods-role")
                if e.event_type == "pi.role.tool.completed"
            ]
            assert len(reads) == 1
            assert sum(
                e.event_type == "pi.role.response.completed"
                for e in authority.journal.events("methods-role")
            ) == (3 if wrapped else 2)
            body = store.artifacts.read_json(cast(str, reads[0].payload["artifact_hash"]))
            assert "ONLY_AFTER_READ_METHOD_BODY" in json.dumps(body)
            # Durable replay reopens saved evidence; it does not reread the changed file.
            frozen.skills[0].manifest.instructions_path.write_text("changed after completion")
            assert (
                await authority.analyze(run_id="methods-role", provider=provider, inputs=inputs)
                == result
            )
            assert len(spawns) == 1
            authority.method_catalog = None
            with pytest.raises(PermissionError, match="different frozen inputs"):
                await authority.analyze(run_id="methods-role", provider=provider, inputs=inputs)
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("wrapped", [False, True])
def test_portfolio_method_read_and_group_identity(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
    monkeypatch: pytest.MonkeyPatch,
    wrapped: bool | str,
) -> None:
    monkeypatch.setenv("ROLE_TOOL_FIXTURE", "method")
    if wrapped:
        monkeypatch.setenv("ROLE_JSON_WRAPPER_FIXTURE", "always" if wrapped == "always" else "1")
    profile, _, spawns = native_portfolio
    authority, *_ = _setup(tmp_path)
    before = authority.review_opportunity_id(provider=PiRuntimeProvider(profile))
    authority.method_catalog = catalog(tmp_path / "methods")

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile)
        try:
            assert authority.review_opportunity_id(provider=provider) != before
            result = await authority.review(
                run_id="portfolio-method", provider=provider, research_run_ids=()
            )
            assert result["status"] == "completed", result
            assert (
                len(
                    [
                        e
                        for e in authority.journal.events("portfolio-method")
                        if e.event_type == "pi.role.tool.completed"
                    ]
                )
                == 1
            )
            assert (
                await authority.review(
                    run_id="portfolio-method", provider=provider, research_run_ids=()
                )
                == result
            )
            assert len(spawns) == 1
            assert sum(
                e.event_type == "pi.role.response.completed"
                for e in authority.journal.events("portfolio-method")
            ) == (3 if wrapped else 2)
            authority.method_catalog = None
            with pytest.raises(PermissionError, match="optional capabilities"):
                await authority.review(
                    run_id="portfolio-method", provider=provider, research_run_ids=()
                )
        finally:
            await provider.close()

    asyncio.run(scenario())
