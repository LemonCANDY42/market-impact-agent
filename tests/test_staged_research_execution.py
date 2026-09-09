# pyright: reportPrivateUsage=false
"""One production-shaped panel proves binding, source isolation and paid replay."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.csrc_news import CsrcNewsHTTPResponse
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import runtime_identity
from market_impact_agent.staged_research_execution import (
    create_staged_authorization,
    prepare_stage1_execution,
    qualify_stage1_execution,
    run_stage1_execution,
    stage1_execution_report,
    staged_execution_budget,
)
from market_impact_agent.staged_research_sources import capture_csrc_study_source
from market_impact_agent.staged_research_study import prepare_staged_study

NOW = datetime(2026, 9, 6, 6, tzinfo=UTC)
SOURCE = (
    Path.home() / ".local/state/market-impact-agent/model-runtime/continuous-study-20260905-usd40"
)
INPUTS = Path(".market-impact/continuous-20260905/study/research-inputs-cash-only-inception-v4")


@pytest.fixture(scope="module")
def current_preparation(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if not (SOURCE.is_dir() and INPUTS.is_dir()):
        pytest.skip("private frozen historical sources are absent on this clone")
    output = tmp_path_factory.mktemp("stage1-preparation")
    prepare_staged_study(
        spec_path=Path("examples/research/staged-study-1-v1.json"),
        output_root=output,
        authority_root=SOURCE,
        input_root=INPUTS,
        profile_path=Path("examples/providers/pi-cpa-terra-high-v2.json"),
    )
    return output


def test_new_paid_authorization_is_durable_bounded_and_does_not_reset_spend(tmp_path: Path) -> None:
    first = create_staged_authorization(tmp_path, authorized_at=NOW)
    budget = staged_execution_budget(tmp_path, "paired_research")
    asyncio.run(budget.reserve("unsettled", 500))
    assert create_staged_authorization(tmp_path, authorized_at=NOW) == first
    reopened = staged_execution_budget(tmp_path, "paired_research")
    assert reopened.max_cost_microusd == 140_000_000
    assert sum(scope.max_cost_microusd for scope in reopened.scope_limits) == 140_000_000
    assert reopened.summary()["reserved_microusd"] == 500
    assert reopened.summary()["known_cost_microusd"] == 0
    assert reopened.summary()["physical_requests"] == 1
    revised = {**first, "authorized_at": "2026-09-06T07:00:00+00:00"}
    revised["authorization_hash"] = canonical_hash(
        {key: value for key, value in revised.items() if key != "authorization_hash"}
    )
    (tmp_path / "authorization.json").write_text(json.dumps(revised))
    with pytest.raises(ValueError, match="config"):
        create_staged_authorization(tmp_path, authorized_at=NOW)
    (tmp_path / "authorization.json").write_text(json.dumps(first))
    assert staged_execution_budget(tmp_path, "paired_research").summary()["physical_requests"] == 1
    payload = json.loads((tmp_path / "authorization.json").read_text())
    payload["maximum_cost_microusd"] = 141_000_000
    payload["authorization_hash"] = canonical_hash(
        {key: value for key, value in payload.items() if key != "authorization_hash"}
    )
    (tmp_path / "authorization.json").write_text(json.dumps(payload))
    with pytest.raises(PermissionError, match="USD140"):
        staged_execution_budget(tmp_path, "paired_research")


def test_qualification_blocks_unknown_spend_in_another_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_staged_authorization(tmp_path, authorized_at=NOW)
    asyncio.run(staged_execution_budget(tmp_path, "usability").reserve("unknown", 500))
    profiles = {
        name: load_model_provider_profile(
            Path(f"examples/providers/pi-cpa-{name}-v2.json")
        ).to_dict()
        for name in ("luna-max", "terra-high")
    }

    def registration(*_: object) -> dict[str, object]:
        return {"runtime": runtime_identity(), "profiles": profiles}

    monkeypatch.setattr(
        "market_impact_agent.staged_research_execution._registration",
        registration,
    )
    with pytest.raises(PermissionError, match="before qualification"):
        asyncio.run(qualify_stage1_execution(tmp_path, "panel", tmp_path / "absent.json"))
    assert (
        staged_execution_budget(tmp_path, "route_qualification").summary()["physical_requests"] == 1
    )


class _CsrcWire:
    def get(self, url: str, *, max_response_bytes: int) -> CsrcNewsHTTPResponse:
        assert max_response_bytes > 100
        return CsrcNewsHTTPResponse(
            body=(
                "<html><head><title>Synthetic historical source</title>"
                '<meta name="PubDate" content="2024-02-05 20:00:00"></head>'
                "<body><h2>Synthetic historical source</h2><p>日期:2024-02-05</p>"
                '<div class="TRS_Editor"><p>A synthetic financing policy response.</p></div>'
                "</body></html>"
            ).encode(),
            final_url=url,
            content_type="text/html",
        )


@pytest.mark.parametrize("wire_mode", ["valid", "invalid_one"])
def test_private_source_native_stage1_panel_and_no_dispatch_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, wire_mode: str, current_preparation: Path
) -> None:
    profiles = tuple(
        load_model_provider_profile(Path(f"examples/providers/pi-cpa-{name}-v2.json"))
        for name in ("luna-max", "terra-high")
    )
    for profile in profiles:
        monkeypatch.setenv(profile.credential_env, "synthetic-stage1-key")
    permit = PiRuntimePermit(
        canonical_hash(runtime_identity()),
        tuple(profile.route_identity for profile in profiles),
        "stage1-test-accepted",
    )

    def installed(_root: Path) -> PiRuntimePermit:
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec
    calls: list[str] = []

    async def spawn(program: str, *args: str, **kwargs: Any):
        calls.append(program)
        kwargs["env"]["STAGE1_WIRE_MODE"] = wire_mode
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("stage1_execution_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    create_staged_authorization(tmp_path, authorized_at=NOW)
    store = LocalDataSnapshotStore(tmp_path / "authority")
    supplement = capture_csrc_study_source(
        "https://www.csrc.gov.cn/csrc/c100028/c7461718/content.shtml",
        "cn-2024-broad-rebound",
        datetime(2024, 2, 6, 1, 25, tzinfo=UTC),
        store,
        http_client=_CsrcWire(),
        clock=lambda: NOW,
    )
    with pytest.raises(ValueError, match="event arm requires"):
        prepare_stage1_execution(
            root=tmp_path,
            panel_id="missing",
            preparation_root=current_preparation,
            source_authority_root=SOURCE,
            input_root=INPUTS,
            profiles=profiles,
            supplements={},
        )
    registration = prepare_stage1_execution(
        root=tmp_path,
        panel_id="comparison",
        preparation_root=current_preparation,
        source_authority_root=SOURCE,
        input_root=INPUTS,
        profiles=profiles,
        supplements={"cn-2024-broad-rebound": [supplement]},
    )
    assert calls == []
    assert registration["maximum_panel_cost_microusd"] == 44_064_768
    assert len(cast(list[object], registration["schedule"])) == 8
    assert registration["maximum_acquisition_successors"] == 0
    assert staged_execution_budget(tmp_path, "paired_research").summary()["physical_requests"] == 0
    before = stage1_execution_report(tmp_path, "comparison")
    assert before["registered_arm_opportunities"] == 8
    assert before["complete_groups"] == 0
    assert all(row["status"] == "not_started" for row in cast(list[dict[str, Any]], before["rows"]))

    async def scenario() -> None:
        report = await run_stage1_execution(tmp_path, "comparison")
        assert len(calls) == 8
        assert report["complete_groups"] == (2 if wire_mode == "valid" else 1)
        rows = cast(list[dict[str, Any]], report["rows"])
        assert sum(row["status"] == "completed" for row in rows) == (
            8 if wire_mode == "valid" else 7
        )
        assert sum(row["usage"]["tool_calls"] for row in rows) == 8
        assert all(row["physical_requests"] == 2 for row in rows)
        assert all(row["score"]["hit"] is None for row in rows if row["arm"] == "price_only")
        for model in cast(dict[str, dict[str, Any]], report["models"]).values():
            assert model["registered_opportunities"] == 4
        assert cast(dict[str, int], report["total_authorization_budget"])["unsettled_requests"] == 0
        assert report["investment_effectiveness_accepted"] is False
        replayed = await run_stage1_execution(tmp_path, "comparison")
        assert replayed == report
        assert len(calls) == 8
        # Parent uncertainty stops new dispatch; it is neither free nor a retry invitation.
        budget = staged_execution_budget(tmp_path, "bounded_followup")
        await budget.reserve("unknown-followup", 100)
        with pytest.raises(PermissionError, match="unknown paid request"):
            await run_stage1_execution(tmp_path, "comparison")
        assert len(calls) == 8

    asyncio.run(scenario())
    assert "staged_research_study.py" in cast(dict[str, str], registration["execution_code"])

    def changed_code() -> dict[str, str]:
        return {"staged_research_study.py": "changed"}

    monkeypatch.setattr(
        "market_impact_agent.staged_research_execution._execution_code", changed_code
    )
    with pytest.raises(PermissionError, match="execution build changed"):
        stage1_execution_report(tmp_path, "comparison")
    assert len(calls) == 8
