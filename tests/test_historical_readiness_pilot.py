from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    EvidencePack,
    EvidenceReference,
    JudgmentDecision,
    JudgmentProposal,
    PatternEntry,
    PatternPack,
    PatternPackReference,
    ProposedTransmissionStep,
    canonical_hash,
)
from market_impact_agent.agent_runtime import ModelTurn, ProviderUsage, ToolCall
from market_impact_agent.cli import main
from market_impact_agent.historical_readiness_pilot import (
    HistoricalReadinessAdjudication,
    HistoricalReadinessBrief,
    HistoricalReadinessInputs,
    PreparedHistoricalReadinessPilot,
    prepare_historical_readiness_pilot,
    run_historical_readiness_pilot,
)
from market_impact_agent.method_skills import CPAUsageKeeperPricing, MethodRoutingContext
from market_impact_agent.research import EvidenceTier, TransmissionDirectness
from market_impact_agent.usage_ledger import UsageLedger

NOW = datetime(2026, 9, 2, tzinfo=UTC)
PROFILE = Path("examples/providers/cliproxyapi-luna-xhigh-cpa-v1.json")


def _pricing() -> CPAUsageKeeperPricing:
    return CPAUsageKeeperPricing.from_api_payloads(
        model="gpt-5.6-luna",
        captured_at=NOW,
        version_payload={"version": "v1.14.5"},
        pricing_payload={
            "pricing": [
                {
                    "model": "gpt-5.6-luna",
                    "pricing_style": "openai",
                    "prompt_price_per_1m": 0.2,
                    "completion_price_per_1m": 1.2,
                    "cache_read_price_per_1m": 0.02,
                    "cache_write_price_per_1m": 0.25,
                    "price_multiplier": 1,
                }
            ]
        },
        rules_payload={"model": "gpt-5.6-luna", "rules": []},
    )


@pytest.fixture
def inputs(tmp_path: Path) -> HistoricalReadinessInputs:
    documents = {
        "news": {"text": "Synthetic news only."},
        "context": {"text": "Synthetic market context."},
    }
    pattern = PatternPack.build(
        version="synthetic-v1",
        available_at=NOW,
        entries=(
            PatternEntry(
                "synthetic",
                "test mechanism",
                ("market",),
                ("condition",),
                ("counterexample",),
                ("test-literature",),
            ),
        ),
    )
    pack = EvidencePack.build(
        event_id="synthetic-event",
        as_of=NOW,
        research_question="Should this be proposed up or abstained?",
        evidence=tuple(
            EvidenceReference(
                evidence_id=name,
                claim_id=name,
                source_ref=f"synthetic://{name}",
                source_tier=EvidenceTier.ESTABLISHED_NEWS
                if name == "news"
                else EvidenceTier.PRIMARY,
                available_at=NOW,
                content_hash=canonical_hash(document),
                summary="Synthetic evidence",
            )
            for name, document in documents.items()
        ),
        pattern_packs=(
            PatternPackReference(
                pattern.pack_id, pattern.version, NOW, canonical_hash(pattern.to_dict())
            ),
        ),
        allowed_targets=("research-proxy",),
        data_gaps=("opened modeled-PIT synthetic fixture",),
    )
    declaration: dict[str, object] = {
        "schema_version": "market-impact.method-evidence-declaration.v1",
        "evidence_pack_id": pack.pack_id,
        "evidence_pack_hash": canonical_hash(pack.to_dict()),
        "evidence_types": [
            {
                "evidence_type": "timestamped_narrative_corpus",
                "evidence_refs": ["news"],
                "pattern_pack_refs": [],
            }
        ],
        "outcomes_opened": True,
    }
    declaration["declaration_id"] = "method-evidence-" + canonical_hash(declaration)
    for name, value in (
        ("pack", pack.to_dict()),
        ("documents", {"documents": documents}),
        ("pattern", pattern.to_dict()),
        ("declaration", declaration),
    ):
        (tmp_path / f"{name}.json").write_text(json.dumps(value), encoding="utf-8")
    return HistoricalReadinessInputs(
        tmp_path / "pack.json",
        tmp_path / "documents.json",
        (tmp_path / "pattern.json",),
        tmp_path / "declaration.json",
        Path("examples/research/famous-method-skill-catalog-v1.json"),
        PROFILE,
        Path("skills"),
    )


def _brief() -> HistoricalReadinessBrief:
    return HistoricalReadinessBrief(
        "news", canonical_hash({"text": "Synthetic news only."}), "research-proxy", 1
    )


def _prepare(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    brief: HistoricalReadinessBrief | None = None,
    cap: int = 2_000_000,
) -> PreparedHistoricalReadinessPilot:
    return prepare_historical_readiness_pilot(
        experiment_id="synthetic-pilot",
        state_root=tmp_path / "state",
        inputs=inputs,
        brief=brief or _brief(),
        treatment_skill="narrative-diffusion-assessment",
        routing_context=MethodRoutingContext(
            "unclassified",
            "diffuse",
            ("narrative_diffusion",),
            ("timestamped_narrative_corpus",),
            True,
        ),
        pricing=_pricing(),
        max_total_cost_microusd=cap,
        registered_at=NOW,
    )


class FakeProvider:
    provider_id = "cliproxyapi-openai-compatible"
    model = "gpt-5.6-luna"

    def __init__(self, decisions: tuple[str, ...], *, secret: str = "", horizon: int = 1) -> None:
        self.decisions = decisions
        self.secret = secret
        self.horizon = horizon
        self.calls = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.completed = 0
        self.messages: list[tuple[dict[str, object], ...]] = []

    async def complete(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        assert {cast(dict[str, object], tool["function"])["name"] for tool in tools} == {
            "read_evidence",
            "read_pattern_pack",
        }
        index = self.calls
        self.calls += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        self.messages.append(messages)
        await asyncio.sleep(0)
        try:
            decision = self.decisions[index]
            if decision == "fail":
                raise RuntimeError("provider error with licensed prose " + self.secret)
            candidates = (
                ()
                if decision == "abstain"
                else (
                    CandidateImpact(
                        target_id="research-proxy",
                        direction=CandidateDirection(decision),
                        horizon_sessions=self.horizon,
                        directness=TransmissionDirectness.DIRECT,
                        confidence=0.2,
                        thesis="Licensed prose must not enter summary",
                        evidence_refs=("news",),
                        counterevidence_refs=("context",),
                        invalidation_conditions=("synthetic invalidation",),
                    ),
                )
            )
            proposal = JudgmentProposal(
                event_id="synthetic-event",
                decision=JudgmentDecision.PROPOSE if candidates else JudgmentDecision.ABSTAIN,
                summary=("Licensed prose must not enter summary " + self.secret).strip(),
                transmission_steps=()
                if not candidates
                else (
                    ProposedTransmissionStep(
                        "step",
                        "event",
                        "research-proxy",
                        "synthetic changed variable",
                        TransmissionDirectness.DIRECT,
                        self.horizon,
                        ("news",),
                    ),
                ),
                candidates=candidates,
                blockers=() if candidates else ("evidence gap",),
                unresolved_questions=(),
                stopped_reason="synthetic test complete",
                decision_confidence=0.3,
            )
            assistant: dict[str, object] = {
                "role": "assistant",
                "content": json.dumps(proposal.to_dict()),
            }
            self.completed += 1
            return ModelTurn(
                response_id=f"response-{index}",
                model=self.model,
                assistant_message=assistant,
                tool_calls=(),
                finish_reason="stop",
                usage=ProviderUsage(100, 50),
                raw_response={"message": assistant},
                latency_ms=1,
            )
        finally:
            self.in_flight -= 1


class FullReadProvider(FakeProvider):
    def __init__(self, inputs: HistoricalReadinessInputs, decisions: tuple[str, ...]) -> None:
        super().__init__(decisions)
        self.inputs = inputs

    async def complete(self, **kwargs: object) -> ModelTurn:
        messages = cast(tuple[dict[str, object], ...], kwargs["messages"])
        if not any(message.get("role") == "tool" for message in messages):
            pack = json.loads(self.inputs.evidence_pack_path.read_text())
            calls = tuple(
                ToolCall(
                    f"read-{item['evidence_id']}",
                    "read_evidence",
                    {"evidence_id": item["evidence_id"]},
                )
                for item in pack["evidence"]
            ) + tuple(
                ToolCall(
                    f"read-{item['pack_id']}", "read_pattern_pack", {"pack_id": item["pack_id"]}
                )
                for item in pack["pattern_packs"]
            )
            assistant: dict[str, object] = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": call.call_id,
                        "type": "function",
                        "function": {"name": call.name, "arguments": json.dumps(call.arguments)},
                    }
                    for call in calls
                ],
            }
            return ModelTurn(
                "read",
                self.model,
                assistant,
                calls,
                "tool_calls",
                ProviderUsage(100, 50),
                {"message": assistant},
            )
        return await super().complete(
            messages=messages,
            tools=cast(tuple[dict[str, object], ...], kwargs["tools"]),
            temperature=cast(float, kwargs["temperature"]),
            top_p=cast(float, kwargs["top_p"]),
            max_output_tokens=cast(int, kwargs["max_output_tokens"]),
            timeout_seconds=cast(float, kwargs["timeout_seconds"]),
        )


def _prepare_v2(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    *,
    treatment: str = "none",
    cap: int = 3_000_000,
    judge_profile: Path = Path("examples/providers/cliproxyapi-luna-max-cpa-v1.json"),
    judge_pricing: CPAUsageKeeperPricing | None = None,
) -> PreparedHistoricalReadinessPilot:
    return prepare_historical_readiness_pilot(
        experiment_id="synthetic-adjudication",
        state_root=tmp_path / "state",
        inputs=inputs,
        brief=_brief(),
        treatment_skill=treatment,
        routing_context=MethodRoutingContext(
            "unclassified",
            "diffuse",
            ("narrative_diffusion",),
            ("timestamped_narrative_corpus",),
            True,
        ),
        pricing=_pricing(),
        max_total_cost_microusd=cap,
        registered_at=NOW,
        adjudication=HistoricalReadinessAdjudication(
            "Synthetic broad equity price index; not an executable security.",
            "synthetic-fixture-v1",
            judge_profile,
            judge_pricing or _pricing(),
        ),
    )


def _high_priced_judge(tmp_path: Path) -> tuple[Path, CPAUsageKeeperPricing]:
    profile = json.loads(PROFILE.read_text())
    profile["budget"]["max_estimated_cost_microusd"] = 3_900_000
    profile["pricing"]["input_microusd_per_million_tokens"] = 4_000_000
    profile["pricing"]["output_microusd_per_million_tokens"] = 24_000_000
    profile.pop("profile_id")
    profile["profile_id"] = "model-provider-" + canonical_hash(profile)
    path = tmp_path / "high-priced-judge.json"
    path.write_text(json.dumps(profile), encoding="utf-8")
    return (
        path,
        CPAUsageKeeperPricing.from_api_payloads(
            model="gpt-5.6-luna",
            captured_at=NOW,
            version_payload={"version": "v1.14.5"},
            pricing_payload={
                "pricing": [
                    {
                        "model": "gpt-5.6-luna",
                        "pricing_style": "openai",
                        "prompt_price_per_1m": 4,
                        "completion_price_per_1m": 24,
                        "cache_read_price_per_1m": 0.4,
                        "cache_write_price_per_1m": 5,
                        "price_multiplier": 1,
                    }
                ]
            },
            rules_payload={"model": "gpt-5.6-luna", "rules": []},
        ),
    )


def test_v2_agreement_stops_without_judge_and_keeps_scope_explicit(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    provider, judge = (
        FullReadProvider(inputs, ("up", "abstain", "up", "abstain")),
        FullReadProvider(inputs, ()),
    )
    report = asyncio.run(
        run_historical_readiness_pilot(prepared, provider=provider, judge_provider=judge)
    )
    assert provider.calls == 4 and judge.calls == 0
    assert report["diagnostic_valid"] and report["protocol_complete"]
    assert report["provider_request_count"] == 8
    finals = cast(dict[str, dict[str, object]], report["final_decisions"])
    assert finals["control"]["direction"] == "up"
    assert finals["treatment"]["decision"] == "abstain"
    assert all(item["rule"] == "analyst_agreement_first_terminal" for item in finals.values())
    prompt = json.dumps(provider.messages[0])
    assert "never the sole reason to abstain" in prompt
    assert "Synthetic broad equity price index" in prompt
    registration = json.loads((prepared.directory / "registration.json").read_text())
    assert registration["schema_version"].endswith(".v2")
    assert registration["comparison_scope"] == "repeatability_only_no_added_skill"
    assert registration["provider_profile_hash"] != registration["judge_provider_profile_hash"]
    assert not list(prepared.directory.glob("*-judge-inputs.json"))


@pytest.mark.parametrize("decision", ["up", "down", "abstain"])
def test_v2_judge_can_choose_either_reject_or_synthesize_without_vote(
    inputs: HistoricalReadinessInputs, tmp_path: Path, decision: str
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    provider = FullReadProvider(inputs, ("abstain", "up", "abstain", "abstain"))
    judge = FullReadProvider(inputs, (decision,))
    report = asyncio.run(
        run_historical_readiness_pilot(prepared, provider=provider, judge_provider=judge)
    )
    assert provider.calls == 4 and judge.calls == 1
    assert report["diagnostic_valid"] and report["accounting_complete"]
    assert report["provider_request_count"] == 10
    finals = cast(dict[str, dict[str, object]], report["final_decisions"])
    assert finals["treatment"]["rule"] == "evidence_led_judge"
    assert finals["treatment"]["direction"] == (None if decision == "abstain" else decision)
    assert finals["control"]["decision"] == "abstain"
    task = next(
        json.loads(cast(str, msg["content"])) for msg in judge.messages[0] if msg["role"] == "user"
    )
    instruction = task["research_instruction"]
    assert "Read the original frozen evidence yourself" in instruction
    assert "Do not vote" in instruction
    analyses = json.loads(instruction.split("Untrusted analyst conclusions: ")[1])
    assert [item["decision"] for item in analyses] == ["propose", "abstain"]
    assert all("decision_confidence" not in item for item in analyses)
    assert "confidence" not in analyses[0]["candidates"][0]
    assert "Licensed prose" not in json.dumps(report)
    frozen = json.loads((prepared.directory / "treatment-judge-inputs.json").read_text())
    assert len(frozen["analyst_terminal_hashes"]) == 2
    assert frozen["analyses_hash"] == canonical_hash(analyses)
    records = UsageLedger(prepared.directory / "usage.sqlite3").records()
    assert len(records) == 5
    assert prepared.adjudication is not None
    assert (
        records[-1].record.provider_profile_id
        == json.loads(prepared.adjudication.judge_profile_path.read_text())["profile_id"]
    )
    assert report["promotion_eligible"] is False and report["execution_capability"] == "none"


def test_v2_each_arm_has_own_judge_and_no_recursive_debate(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    judge = FullReadProvider(inputs, ("abstain", "down"))
    report = asyncio.run(
        run_historical_readiness_pilot(
            prepared,
            provider=FullReadProvider(inputs, ("up", "up", "down", "abstain")),
            judge_provider=judge,
        )
    )
    assert judge.calls == 2
    assert len(cast(list[object], report["runs"])) == 6
    assert report["diagnostic_valid"]
    for arm in ("control", "treatment"):
        assert (prepared.directory / f"{arm}-judge-inputs.json").exists()


@pytest.mark.parametrize("failure", ["fail", "unread"])
def test_v2_failed_or_unread_analysis_stops_before_judge(
    inputs: HistoricalReadinessInputs, tmp_path: Path, failure: str
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    provider = (
        FullReadProvider(inputs, ("fail", "up"))
        if failure == "fail"
        else FakeProvider(("up", "down"))
    )
    judge = FullReadProvider(inputs, ())
    report = asyncio.run(
        run_historical_readiness_pilot(prepared, provider=provider, judge_provider=judge)
    )
    assert provider.calls == 2 and provider.in_flight == 0 and judge.calls == 0
    assert report["diagnostic_valid"] is False
    assert report["final_decisions"] == {}
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == 2


def test_v2_failed_judge_is_not_replaced_or_majority_fallback(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    judge = FullReadProvider(inputs, ("fail",))
    report = asyncio.run(
        run_historical_readiness_pilot(
            prepared,
            provider=FullReadProvider(inputs, ("up", "up", "down", "down")),
            judge_provider=judge,
        )
    )
    assert judge.calls == 1
    assert report["diagnostic_valid"] is False and report["final_decisions"] == {}
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == 5
    with pytest.raises(ValueError, match="already dispatched"):
        asyncio.run(
            run_historical_readiness_pilot(
                prepared, provider=FakeProvider(()), judge_provider=judge
            )
        )


def test_v2_judge_binding_and_scope_drift_deny_dispatch(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    assert prepared.adjudication is not None
    provider, judge = FakeProvider(()), FakeProvider(())
    for change in ({"target_description": "Different scope"}, {"judge_profile_path": PROFILE}):
        with pytest.raises(ValueError, match="binding changed"):
            asyncio.run(
                run_historical_readiness_pilot(
                    replace(prepared, adjudication=replace(prepared.adjudication, **change)),
                    provider=provider,
                    judge_provider=judge,
                )
            )
    assert provider.calls == judge.calls == 0


def test_v2_reserves_analyst_and_independent_judge_costs(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="four analysts and two conditional Judges"):
        _prepare_v2(inputs, tmp_path, cap=1_300_000)
    assert not (tmp_path / "state").exists()


def test_v2_preflight_prices_the_actual_mixed_plan(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    judge_profile, judge_pricing = _high_priced_judge(tmp_path)
    prepared = _prepare_v2(
        inputs,
        tmp_path,
        cap=10_000_000,
        judge_profile=judge_profile,
        judge_pricing=judge_pricing,
    )

    registration = json.loads((prepared.directory / "registration.json").read_text())
    preflight = cast(dict[str, object], registration["cost_preflight"])
    assert set(preflight) == {
        "analyst_four_run_estimate",
        "judge_two_run_estimate",
        "mixed_six_run_guarded_microusd",
        "reserved_runtime_caps_microusd",
        "hard_cap_microusd",
    }
    analyst = cast(dict[str, int], preflight["analyst_four_run_estimate"])
    judge = cast(dict[str, int], preflight["judge_two_run_estimate"])
    assert analyst["agent_run_count"] == 4
    assert judge["agent_run_count"] == 2
    mixed_guarded_cost = cast(int, preflight["mixed_six_run_guarded_microusd"])
    assert mixed_guarded_cost == (
        analyst["guarded_max_cost_microusd"] + judge["guarded_max_cost_microusd"]
    )
    assert preflight["reserved_runtime_caps_microusd"] == 8_600_000
    assert mixed_guarded_cost < 10_000_000

    with pytest.raises(ValueError, match="four analysts and two conditional Judges"):
        _prepare_v2(
            inputs,
            tmp_path,
            cap=9_000_000,
            judge_profile=judge_profile,
            judge_pricing=judge_pricing,
        )


def test_v2_does_not_force_expectation_skill_on_category_only_evidence(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    declaration = json.loads(inputs.method_evidence_declaration_path.read_text())
    declaration.pop("declaration_id")
    declaration["evidence_types"] = [
        {"evidence_type": name, "evidence_refs": ["news"], "pattern_pack_refs": []}
        for name in ("reference_class", "new_evidence")
    ]
    declaration["declaration_id"] = "method-evidence-" + canonical_hash(declaration)
    inputs.method_evidence_declaration_path.write_text(json.dumps(declaration))
    with pytest.raises(ValueError, match="content-bound expectation"):
        prepare_historical_readiness_pilot(
            experiment_id="incompatible-method",
            state_root=tmp_path / "state",
            inputs=inputs,
            brief=_brief(),
            treatment_skill="expectations-base-rates",
            routing_context=MethodRoutingContext(
                "unclassified",
                "unavailable",
                ("base_rate_update",),
                ("reference_class", "new_evidence"),
                True,
            ),
            pricing=_pricing(),
            max_total_cost_microusd=3_000_000,
            registered_at=NOW,
            adjudication=HistoricalReadinessAdjudication(
                "Research proxy", "synthetic-v1", PROFILE, _pricing()
            ),
        )
    assert not (tmp_path / "state").exists()


def test_v2_tampered_analyst_artifact_cannot_reach_judge(
    inputs: HistoricalReadinessInputs, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from market_impact_agent import historical_readiness_pilot as module

    prepared = _prepare_v2(inputs, tmp_path)
    original = module.reopen_authoritative_agent_terminal

    def corrupt_then_reopen(**kwargs: object) -> object:
        store = cast(module.ArtifactStore, kwargs["artifact_store"])
        artifact_hash = cast(str, kwargs["terminal_artifact_hash"])
        (store.root / artifact_hash).write_text("{}")
        return original(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(module, "reopen_authoritative_agent_terminal", corrupt_then_reopen)
    judge = FullReadProvider(inputs, ())
    with pytest.raises((ValueError, OSError)):
        asyncio.run(
            run_historical_readiness_pilot(
                prepared,
                provider=FullReadProvider(inputs, ("up", "up", "down", "down")),
                judge_provider=judge,
            )
        )
    assert judge.calls == 0
    assert (prepared.directory / "dispatch.json").exists()
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == 4


def test_v2_wrong_horizon_judge_fails_without_fallback(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)
    judge = FullReadProvider(inputs, ("down",))
    judge.horizon = 2
    report = asyncio.run(
        run_historical_readiness_pilot(
            prepared,
            provider=FullReadProvider(inputs, ("up", "up", "down", "down")),
            judge_provider=judge,
        )
    )
    assert judge.calls == 1
    assert report["diagnostic_valid"] is False and report["final_decisions"] == {}


def test_v2_cancellation_drains_peers_without_later_runs(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    prepared = _prepare_v2(inputs, tmp_path)

    async def exercise() -> dict[str, object]:
        started, release = asyncio.Event(), asyncio.Event()

        class SlowProvider(FullReadProvider):
            async def complete(self, **kwargs: object) -> ModelTurn:
                started.set()
                await release.wait()
                return await super().complete(**kwargs)

        provider = SlowProvider(inputs, ("up", "down"))
        judge = FullReadProvider(inputs, ())
        task = asyncio.create_task(
            run_historical_readiness_pilot(prepared, provider=provider, judge_provider=judge)
        )
        await started.wait()
        task.cancel()
        release.set()
        result = await task
        assert provider.calls == 2 and provider.in_flight == 0 and judge.calls == 0
        return result

    report = asyncio.run(exercise())
    assert report["stop_reason"] == "caller_cancelled_after_peer_drain"
    assert report["final_decisions"] == {}
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == 2


@pytest.mark.parametrize(
    "decisions,expected",
    [
        (("up", "down", "up", "down"), 4),
        (("abstain", "abstain", "abstain", "abstain"), 4),
        (("up", "down", "down", "down", "up", "abstain"), 6),
    ],
)
def test_two_pairs_then_only_disagreement_adds_third(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    decisions: tuple[str, ...],
    expected: int,
) -> None:
    prepared = _prepare(inputs, tmp_path)
    provider = FakeProvider(decisions)
    report = asyncio.run(run_historical_readiness_pilot(prepared, provider=provider))
    assert provider.calls == expected
    assert provider.max_in_flight == 2
    assert report["diagnostic_valid"] is True
    assert report["protocol_complete"] is False
    assert report["promotion_eligible"] is False
    assert report["execution_capability"] == "none"
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == expected
    serialized = json.dumps(report)
    assert "Licensed prose" not in serialized
    assert "Synthetic news only" not in serialized
    assert report["provider_request_count"] == expected
    if decisions[0] == "up":
        assert cast(list[dict[str, object]], report["runs"])[0]["candidate_confidence"] == 0.2
        assert cast(list[dict[str, object]], report["runs"])[0]["decision_confidence"] == 0.3
    # Only the Skill addition differs; both arms use the same explicit brief and neutral question.
    prompts = [json.dumps(messages) for messages in provider.messages]
    assert all("Should this be proposed up or abstained?" not in prompt for prompt in prompts)
    assert all("propose up, propose down, or abstain" in prompt for prompt in prompts)
    assert all("unknown" in prompt and "Shared brief" in prompt for prompt in prompts)
    assert all("opportunity" not in prompt.lower() for prompt in prompts)
    assert "narrative-diffusion-assessment" not in prompts[0]
    assert "narrative-diffusion-assessment" in prompts[1]
    registration = json.loads((prepared.directory / "registration.json").read_text())
    assert registration["evidence_pack_hash"] != registration["derived_evidence_pack_hash"]
    assert registration["derived_input_change"] == "research_question_only"
    assert "Should this be proposed up" in inputs.evidence_pack_path.read_text()


def test_failed_peer_drains_and_records_both_terminals(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
) -> None:
    prepared = _prepare(inputs, tmp_path)
    provider = FakeProvider(("fail", "down"))
    report = asyncio.run(run_historical_readiness_pilot(prepared, provider=provider))
    assert provider.calls == 2
    assert provider.completed == 1
    assert provider.in_flight == 0
    records = UsageLedger(prepared.directory / "usage.sqlite3").records()
    assert len(records) == 2
    assert {item.record.status.value for item in records} == {"failed", "completed"}
    assert report["stop_reason"] == "failed_pair"
    assert report["diagnostic_valid"] is False
    assert report["accounting_complete"] is False
    assert report["recorded_totals_are_lower_bounds"] is True


def test_reservation_and_dispatch_are_exclusive_and_changed_binding_denied(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
) -> None:
    prepared = _prepare(inputs, tmp_path)
    with pytest.raises(ValueError, match="already reserved"):
        _prepare(inputs, tmp_path)
    provider = FakeProvider(("abstain",) * 4)
    with pytest.raises(ValueError, match="binding changed"):
        asyncio.run(
            run_historical_readiness_pilot(
                replace(prepared, brief=replace(prepared.brief, horizon_sessions=2)),
                provider=provider,
            )
        )
    assert provider.calls == 0
    asyncio.run(run_historical_readiness_pilot(prepared, provider=provider))
    with pytest.raises(ValueError, match="already dispatched"):
        asyncio.run(run_historical_readiness_pilot(prepared, provider=provider))
    assert provider.calls == 4


def test_read_coverage_comes_from_executed_journal_tools(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
) -> None:
    prepared = _prepare(inputs, tmp_path)

    class ReadingProvider(FakeProvider):
        async def complete(
            self,
            *,
            messages: tuple[dict[str, object], ...],
            tools: tuple[dict[str, object], ...],
            temperature: float,
            top_p: float,
            max_output_tokens: int,
            timeout_seconds: float,
        ) -> ModelTurn:
            if not any(message.get("role") == "tool" for message in messages):
                pattern_id = json.loads(inputs.pattern_pack_paths[0].read_text())["pack_id"]
                calls = (
                    ToolCall("read-news", "read_evidence", {"evidence_id": "news"}),
                    ToolCall("read-context", "read_evidence", {"evidence_id": "context"}),
                    ToolCall("read-pattern", "read_pattern_pack", {"pack_id": pattern_id}),
                )
                assistant: dict[str, object] = {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": call.call_id,
                            "type": "function",
                            "function": {
                                "name": call.name,
                                "arguments": json.dumps(call.arguments),
                            },
                        }
                        for call in calls
                    ],
                }
                return ModelTurn(
                    "read-response",
                    self.model,
                    assistant,
                    calls,
                    "tool_calls",
                    ProviderUsage(100, 50),
                    {"message": assistant},
                )
            return await super().complete(
                messages=messages,
                tools=tools,
                temperature=temperature,
                top_p=top_p,
                max_output_tokens=max_output_tokens,
                timeout_seconds=timeout_seconds,
            )

    report = asyncio.run(
        run_historical_readiness_pilot(prepared, provider=ReadingProvider(("abstain",) * 4))
    )
    assert report["protocol_complete"] is True
    assert report["provider_request_count"] == 8
    rows = cast(list[dict[str, object]], report["runs"])
    coverage = cast(dict[str, object], rows[0]["read_coverage"])
    assert coverage["evidence_ids_read"] == ["context", "news"]
    assert coverage["evidence_coverage_complete"] is True
    assert coverage["pattern_coverage_complete"] is True


@pytest.mark.parametrize(
    "field,value",
    [
        ("target_id", "outside"),
        ("news_evidence_id", "outside"),
        ("news_content_hash", "0" * 64),
        ("horizon_sessions", 0),
    ],
)
def test_bad_brief_denied_before_reservation(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        _prepare(inputs, tmp_path, replace(_brief(), **{field: value}))
    assert not (tmp_path / "state").exists()


def test_document_drift_and_budget_denied(
    inputs: HistoricalReadinessInputs, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match=r"hard cap|six bounded"):
        _prepare(inputs, tmp_path, cap=1_000_000)
    inputs.evidence_documents_path.write_text(
        json.dumps(
            {
                "documents": {
                    "news": {"text": "changed"},
                    "context": {"text": "Synthetic market context."},
                }
            }
        )
    )
    with pytest.raises(ValueError, match="content hash mismatch"):
        _prepare(inputs, tmp_path)


def test_wrong_horizon_is_invalid_not_an_extra_replica(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
) -> None:
    prepared = _prepare(inputs, tmp_path)
    provider = FakeProvider(("up", "down"), horizon=2)
    report = asyncio.run(run_historical_readiness_pilot(prepared, provider=provider))
    assert provider.calls == 2
    assert report["diagnostic_valid"] is False
    assert report["stop_reason"] == "failed_pair"


def test_protected_secret_redacted_in_error_artifacts_and_summary(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "synthetic-private-api-key"
    monkeypatch.setenv("MARKET_IMPACT_CLIPROXY_API_KEY", secret)
    prepared = _prepare(inputs, tmp_path)
    report = asyncio.run(
        run_historical_readiness_pilot(
            prepared, provider=FakeProvider(("fail", "down"), secret=secret)
        )
    )
    assert secret not in json.dumps(report)
    for path in prepared.directory.rglob("*.json"):
        assert secret not in path.read_text()
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == 2


def test_input_change_after_prepare_denies_dispatch(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
) -> None:
    prepared = _prepare(inputs, tmp_path)
    inputs.evidence_documents_path.write_text(json.dumps({"documents": {}}))
    provider = FakeProvider(("abstain",) * 4)
    with pytest.raises(ValueError, match="exactly match"):
        asyncio.run(run_historical_readiness_pilot(prepared, provider=provider))
    assert provider.calls == 0
    assert not (prepared.directory / "dispatch.json").exists()


def test_caller_cancellation_drains_started_peers_and_stops_after_pair(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepare(inputs, tmp_path)
    provider = FakeProvider(("up", "down"))
    original = provider.complete

    async def scenario() -> dict[str, object]:
        started = asyncio.Event()
        release = asyncio.Event()
        observed = 0

        async def complete(
            *,
            messages: tuple[dict[str, object], ...],
            tools: tuple[dict[str, object], ...],
            temperature: float,
            top_p: float,
            max_output_tokens: int,
            timeout_seconds: float,
        ) -> ModelTurn:
            nonlocal observed
            observed += 1
            if observed == 2:
                started.set()
            await release.wait()
            return await original(
                messages=messages,
                tools=tools,
                temperature=temperature,
                top_p=top_p,
                max_output_tokens=max_output_tokens,
                timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(provider, "complete", complete)
        task = asyncio.create_task(run_historical_readiness_pilot(prepared, provider=provider))
        await started.wait()
        task.cancel()
        release.set()
        return await task

    report = asyncio.run(scenario())
    assert provider.completed == 2
    assert report["stop_reason"] == "caller_cancelled_after_peer_drain"
    assert report["diagnostic_valid"] is False
    assert len(UsageLedger(prepared.directory / "usage.sqlite3").records()) == 2


def test_cli_uses_existing_provider_factory_and_prints_only_sanitized_summary(
    inputs: HistoricalReadinessInputs,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    provider = FakeProvider(("up", "down", "up", "down"))

    def fake_create(*_args: object) -> FakeProvider:
        return provider

    async def fake_pricing(**_kwargs: object) -> CPAUsageKeeperPricing:
        return _pricing()

    monkeypatch.setattr(
        "market_impact_agent.model_provider.ModelProviderFactory.create", fake_create
    )
    monkeypatch.setattr(
        "market_impact_agent.paired_skill_ablation_runner.fetch_cpa_usage_keeper_pricing",
        fake_pricing,
    )
    args = [
        "agent",
        "historical-readiness-pilot-run",
        "--evidence-pack",
        str(inputs.evidence_pack_path),
        "--evidence-documents",
        str(inputs.evidence_documents_path),
        "--pattern-pack",
        str(inputs.pattern_pack_paths[0]),
        "--method-evidence-declaration",
        str(inputs.method_evidence_declaration_path),
        "--method-catalog",
        str(inputs.method_catalog_path),
        "--provider-profile",
        str(inputs.provider_profile_path),
        "--experiment-id",
        "cli-synthetic",
        "--treatment-skill",
        "narrative-diffusion-assessment",
        "--news-evidence-id",
        "news",
        "--news-content-hash",
        _brief().news_content_hash,
        "--target-id",
        "research-proxy",
        "--eligible-horizon-sessions",
        "1",
        "--analysis-need",
        "narrative_diffusion",
        "--narrative-salience",
        "diffuse",
        "--max-total-cost-microusd",
        "2000000",
        "--state-root",
        str(tmp_path / "cli-state"),
        "--skill-root",
        str(inputs.skill_root),
    ]
    assert main(args) == 0
    output = capsys.readouterr()
    assert "Licensed prose" not in output.out
    assert "Synthetic news only" not in output.out
    assert json.loads(output.out)["execution_capability"] == "none"
    assert main(args) == 1
    assert provider.calls == 4
