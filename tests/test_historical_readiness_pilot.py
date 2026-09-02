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

    def fake_pricing(**_kwargs: object) -> CPAUsageKeeperPricing:
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
