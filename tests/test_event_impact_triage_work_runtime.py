import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ModelTurn,
    ProviderUsage,
    SkillRegistry,
    ToolCall,
    Utf8TokenEstimator,
)
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    TriageAgentRole,
    TriageObservationRef,
)
from market_impact_agent.event_impact_triage_runtime import (
    TriageCandidateContent,
    TriageComparisonArm,
)
from market_impact_agent.event_impact_triage_work import (
    TriageWorkManifestPolicy,
    build_event_impact_triage_work_manifest,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EventImpactTriageWorkRunner,
    TriageWorkPhase,
    TriageWorkRunMember,
    build_event_impact_triage_work_execution_plan,
    event_impact_triage_work_execution_plan_from_dict,
)
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.prospective_diagnostic import (
    load_prospective_diagnostic_registration,
)
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 30, 6, tzinfo=UTC)
PROFILE_ALIAS = "cliproxyapi-luna-xhigh-v1"


class StaticResolver:
    def __init__(self, contents: tuple[TriageCandidateContent, ...]) -> None:
        self.contents = contents

    def resolve(
        self, candidate_set: EventImpactTriageCandidateSet
    ) -> tuple[TriageCandidateContent, ...]:
        assert candidate_set.version_ids == tuple(item.version_id for item in self.contents)
        return self.contents


class ScriptedWorkProvider(ModelProvider):
    def __init__(self, *, over_budget: bool = False) -> None:
        self.requests: list[tuple[dict[str, object], ...]] = []
        self.over_budget = over_budget

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

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
        _ = (temperature, top_p, max_output_tokens, timeout_seconds)
        assert tools == ()
        self.requests.append(messages)
        task = next(
            decoded
            for message in reversed(messages)
            if message.get("role") == "user"
            for decoded in (json.loads(str(message["content"])),)
            if "phase" in decoded
        )
        output = self._output(task)
        content = canonical_json_bytes(output).decode()
        usage = (
            ProviderUsage(input_tokens=10_000_000, output_tokens=10_000_000)
            if self.over_budget
            else ProviderUsage(input_tokens=120, output_tokens=80)
        )
        return ModelTurn(
            response_id=f"scripted-{len(self.requests)}",
            model=self.model,
            assistant_message={"role": "assistant", "content": content},
            tool_calls=(),
            finish_reason="stop",
            usage=usage,
            raw_response={"id": f"scripted-{len(self.requests)}", "content": content},
            latency_ms=2.0,
        )

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        phase = str(task["phase"])
        role = str(task["role"])
        phase_input = cast(dict[str, object], task["phase_input"])
        if phase == TriageWorkPhase.MAP.value and role != "coordinator":
            field = {
                "fact_verifier": "fact_findings",
                "transmission_mapper": "transmission_findings",
                "countercase_reviewer": "countercase_findings",
            }[role]
            atoms = cast(list[dict[str, object]], phase_input["atoms"])
            return {
                "manifest_id": phase_input["manifest_id"],
                "work_unit_id": phase_input["work_unit_id"],
                "role": role,
                "atom_findings": [{"atom_id": atom["atom_id"], field: []} for atom in atoms],
            }
        if phase == TriageWorkPhase.MAP.value:
            atoms = cast(list[dict[str, object]], phase_input["atoms"])
            return {
                "manifest_id": phase_input["manifest_id"],
                "work_unit_id": phase_input["work_unit_id"],
                "digests": [
                    {
                        "atom_id": atom["atom_id"],
                        "changed_facts": [],
                        "source_conflicts": [],
                        "transmission_paths": [],
                        "countercases": [],
                        "uncertainty_notes": [],
                        "checkpoint_rule_evidence": [],
                    }
                    for atom in atoms
                ],
            }
        if phase == TriageWorkPhase.PARTITION.value:
            digests = cast(list[dict[str, object]], phase_input["digests"])
            atom_ids = [str(item["atom_id"]) for item in digests]
            cross_unit = [atom_ids[0], atom_ids[-1]]
            singleton_ids = atom_ids[1:-1]
            return {
                "manifest_id": phase_input["manifest_id"],
                "clusters": [
                    {
                        "atom_ids": cross_unit,
                        "merge_state": "merged",
                        "merge_evidence": ["The fixture explicitly links the same event."],
                        "uncertainty_notes": [],
                    },
                    *(
                        {
                            "atom_ids": [atom_id],
                            "merge_state": "merged",
                            "merge_evidence": [],
                            "uncertainty_notes": [],
                        }
                        for atom_id in singleton_ids
                    ),
                ],
            }
        partition_cluster = cast(dict[str, object], phase_input["partition_cluster"])
        versions = cast(list[str], partition_cluster["candidate_version_ids"])
        return {
            "candidate_version_ids": versions,
            "checkpoint_eligibility": "ineligible",
            "recommended_route": "archive",
            "event_archetypes": [],
            "event_stage": "first_observed",
            "changed_facts": [],
            "rule_reasons": ["No registered checkpoint event is supported."],
            "evidence_version_ids": versions,
            "uncertainty_notes": [],
            "countercases": [],
            "transmission_channels": [],
            "affected_entity_refs": [],
            "watch_questions": [],
            "triage_confidence": 0.8,
        }


class SimulatedProcessCrash(BaseException):
    pass


class CrashAfterDispatchProvider(ScriptedWorkProvider):
    async def complete(self, **kwargs: object) -> ModelTurn:
        messages = kwargs.get("messages")
        assert isinstance(messages, tuple)
        self.requests.append(cast(tuple[dict[str, object], ...], messages))
        raise SimulatedProcessCrash


class SlowFirstProvider(ScriptedWorkProvider):
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
        await asyncio.sleep(0.05)
        return await super().complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )


class CorrectionOnceProvider(ScriptedWorkProvider):
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
        if not self.requests:
            self.requests.append(messages)
            content = "{}"
            return ModelTurn(
                response_id="invalid-first",
                model=self.model,
                assistant_message={"role": "assistant", "content": content},
                tool_calls=(),
                finish_reason="stop",
                usage=ProviderUsage(input_tokens=90, output_tokens=20),
                raw_response={"id": "invalid-first", "content": content},
                latency_ms=3.0,
            )
        return await super().complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )


class InvalidResponseProvider(ScriptedWorkProvider):
    def __init__(self, kind: str) -> None:
        super().__init__()
        self.kind = kind

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
        turn = await super().complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        if self.kind == "wrong_model":
            return replace(turn, model="unexpected-model")
        if self.kind == "tool_call":
            return replace(
                turn,
                tool_calls=(ToolCall(call_id="unexpected-1", name="broker", arguments={}),),
            )
        secret = "TOPSECRET"
        return replace(
            turn,
            assistant_message={"role": "assistant", "content": secret},
            raw_response={"id": turn.response_id, "content": secret},
        )


class PointerTamperRunner(EventImpactTriageWorkRunner):
    tampered = False

    def _append_usage(self, member: TriageWorkRunMember) -> None:
        if member.status is RunStatus.COMPLETED and not self.tampered:
            self.tampered = True
            with sqlite3.connect(self.journal.path) as connection:
                connection.execute(
                    "UPDATE runs SET terminal_artifact_id = ? WHERE run_id = ?",
                    ("f" * 64, member.run_id),
                )
        super()._append_usage(member)


def _registration():
    return load_prospective_diagnostic_registration(
        ROOT / "examples" / "research" / "prospective-diagnostic-registration-v3.json"
    )


def _batch(
    count: int,
) -> tuple[EventImpactTriageCandidateSet, tuple[TriageCandidateContent, ...]]:
    registration = _registration()
    observations: list[TriageObservationRef] = []
    contents: list[TriageCandidateContent] = []
    for index in range(1, count + 1):
        payload: dict[str, object] = {
            "record": {
                "headline": f"fixture event {index}",
                "body": "bounded fixture evidence " + "x" * 80,
            }
        }
        observed_at = NOW + timedelta(seconds=index)
        version_id = f"prospective-observation-version-{index:064x}"
        observations.append(
            TriageObservationRef(
                version_id=version_id,
                observation_id=f"source-observation-{1000 + index:064x}",
                first_available_at=observed_at,
                authority_at=observed_at,
                provider_id="fixture-provider",
                provider_version="fixture-v1",
                upstream_source="fixture-source",
                source_ref=f"fixture://news/{index}",
                raw_content_hash=f"{2000 + index:064x}",
                normalized_payload_hash=canonical_hash(payload),
            )
        )
        contents.append(
            TriageCandidateContent(
                version_id=version_id,
                normalized_payload=payload,
                license_scope="private_research_no_redistribution",
            )
        )
    core = {
        "schema_version": "market-impact.event-impact-triage-candidate-set.v1",
        "registration_id": registration.registration_id,
        "checkpoint_key": "next-a-share-policy-event",
        "route_plan_id": "prospective-checkpoint-route-plan-" + "7" * 64,
        "route_admission_id": "prospective-checkpoint-route-admission-" + "8" * 64,
        "readiness_report_id": "prospective-checkpoint-readiness-report-" + "9" * 64,
        "data_snapshot_id": "data-snapshot-" + "a" * 64,
        "admitted_at": "2026-08-30T06:00:00Z",
        "frozen_at": "2026-08-30T07:00:00Z",
        "observations": [item.to_dict() for item in observations],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return (
        EventImpactTriageCandidateSet(
            candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
            registration_id=registration.registration_id,
            checkpoint_key="next-a-share-policy-event",
            route_plan_id="prospective-checkpoint-route-plan-" + "7" * 64,
            route_admission_id="prospective-checkpoint-route-admission-" + "8" * 64,
            readiness_report_id="prospective-checkpoint-readiness-report-" + "9" * 64,
            data_snapshot_id="data-snapshot-" + "a" * 64,
            admitted_at=NOW,
            frozen_at=NOW + timedelta(hours=1),
            observations=tuple(observations),
        ),
        tuple(contents),
    )


def _runtime(
    tmp_path: Path,
    *,
    arm: TriageComparisonArm,
    count: int = 121,
    provider: ScriptedWorkProvider | None = None,
    runner_class: type[EventImpactTriageWorkRunner] = EventImpactTriageWorkRunner,
    secret_values: tuple[str, ...] = (),
):
    candidate_set, contents = _batch(count)
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=contents,
        policy=TriageWorkManifestPolicy(
            max_atoms_per_work_unit=12,
            max_candidate_versions_per_work_unit=12,
            max_estimated_serialized_prompt_utf8_tokens=32_768,
        ),
    )
    skills = SkillRegistry(ROOT / "skills")
    profile = load_builtin_model_provider_profile(PROFILE_ALIAS)
    plan = build_event_impact_triage_work_execution_plan(
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        arm=arm,
        model_profile_alias=PROFILE_ALIAS,
        model_profile=profile,
        skills=skills,
    )
    actual_provider = provider or ScriptedWorkProvider()
    runner = runner_class(
        plan=plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        provider=actual_provider,
        content_resolver=StaticResolver(contents),
        skills=skills,
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "journal.sqlite"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite"),
        secret_values=secret_values,
        clock=lambda: NOW,
    )
    return runner, actual_provider, candidate_set, manifest, plan


@pytest.mark.parametrize("arm", tuple(TriageComparisonArm))
def test_work_runner_covers_121_candidates_crosses_units_and_restarts_without_calls(
    tmp_path: Path, arm: TriageComparisonArm
) -> None:
    runner, provider, candidate_set, manifest, plan = _runtime(tmp_path, arm=arm)
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert len(result.digests) == 121
    assert result.partition is not None
    assert result.proposal is not None
    assert set(
        version_id
        for cluster in result.proposal.clusters
        for version_id in cluster.candidate_version_ids
    ) == set(candidate_set.version_ids)
    first_unit = set(manifest.work_units[0].atom_ids)
    last_unit = set(manifest.work_units[-1].atom_ids)
    assert any(
        set(cluster.atom_ids) & first_unit and set(cluster.atom_ids) & last_unit
        for cluster in result.partition.clusters
    )
    counter = Utf8TokenEstimator()
    for request in provider.requests:
        task = json.loads(str(request[-1]["content"]))
        binding = plan.binding(
            TriageWorkPhase(str(task["phase"])),
            TriageAgentRole(str(task["role"])),
        )
        assert counter.count_request(request, ()) <= binding.max_request_utf8_tokens
        assert "label" not in canonical_json_bytes(task).decode().lower()
    request_count = len(provider.requests)
    reopened = asyncio.run(runner.run())
    assert reopened == result
    assert len(provider.requests) == request_count
    assert len(runner.usage_ledger.records()) == len(result.members)

    assert "label" not in canonical_json_bytes(plan.to_dict()).decode().lower()
    assert event_impact_triage_work_execution_plan_from_dict(plan.to_dict()) == plan


def test_dispatched_ambiguous_unit_is_never_retried(tmp_path: Path) -> None:
    provider = CrashAfterDispatchProvider()
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    assert len(provider.requests) == 1

    result = asyncio.run(runner.run())
    assert result.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert result.proposal is None
    assert result.partition is None
    assert len(provider.requests) == 1
    events = runner.journal.events(result.members[-1].run_id)
    assert [item.event_type for item in events] == [
        "model.request.dispatched",
        "model.request.ambiguous",
    ]


def test_budget_failure_blocks_all_downstream_work(tmp_path: Path) -> None:
    provider = ScriptedWorkProvider(over_budget=True)
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.BUDGET_EXHAUSTED
    assert result.proposal is None
    assert result.partition is None
    assert len(provider.requests) == 1


def test_authority_rejects_tamper_cross_manifest_and_partial_usage(tmp_path: Path) -> None:
    runner, _, candidate_set, manifest, _ = _runtime(
        tmp_path, arm=TriageComparisonArm.BASELINE, count=3
    )
    result = asyncio.run(runner.run())
    assert result.run_evidence is not None
    assert result.partition is not None
    assert result.proposal is not None
    evidence = result.run_evidence
    bad_member = replace(evidence.members[0], terminal_artifact_hash="f" * 64)
    with pytest.raises((FileNotFoundError, ValueError)):
        runner.assert_authoritative_completed_work_run(
            candidate_set=candidate_set,
            work_manifest=manifest,
            digests=result.digests,
            partition=result.partition,
            proposal=result.proposal,
            run_evidence=replace(evidence, members=(bad_member, *evidence.members[1:])),
        )

    other_candidate_set, other_contents = _batch(2)
    other_manifest = build_event_impact_triage_work_manifest(
        candidate_set=other_candidate_set,
        contents=other_contents,
        policy=manifest.policy,
    )
    with pytest.raises(ValueError, match="another frozen input"):
        runner.assert_authoritative_completed_work_run(
            candidate_set=candidate_set,
            work_manifest=other_manifest,
            digests=result.digests,
            partition=result.partition,
            proposal=result.proposal,
            run_evidence=evidence,
        )

    partial_runner = EventImpactTriageWorkRunner(
        plan=runner.plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        provider=runner.provider,
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=UsageLedger(tmp_path / "partial-usage.sqlite"),
        clock=lambda: NOW,
    )
    with pytest.raises(ValueError, match="Usage Ledger"):
        partial_runner.assert_authoritative_completed_work_run(
            candidate_set=candidate_set,
            work_manifest=manifest,
            digests=result.digests,
            partition=result.partition,
            proposal=result.proposal,
            run_evidence=evidence,
        )


def test_concurrent_same_plan_has_exactly_one_provider_owner(tmp_path: Path) -> None:
    provider = SlowFirstProvider()
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
    )
    second = EventImpactTriageWorkRunner(
        plan=plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        provider=provider,
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=runner.usage_ledger,
        clock=lambda: NOW,
    )

    async def run_both():
        first_task = asyncio.create_task(runner.run())
        await asyncio.sleep(0)
        second_task = asyncio.create_task(second.run())
        return await asyncio.gather(first_task, second_task)

    first, concurrent = asyncio.run(run_both())
    assert first.status is RunStatus.COMPLETED
    assert concurrent.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert first.partition is not None
    expected_calls = len(manifest.work_units) + 1 + len(first.partition.clusters)
    assert len(provider.requests) == expected_calls
    first_dispatches = [
        event
        for event in runner.journal.events(first.members[0].run_id)
        if event.event_type == "model.request.dispatched"
    ]
    assert len(first_dispatches) == 1


def test_multiturn_correction_chain_reopens_exactly(tmp_path: Path) -> None:
    provider = CorrectionOnceProvider()
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.COMPLETED
    events = runner.journal.events(result.members[0].run_id)
    assert [item.event_type for item in events] == [
        "model.request.dispatched",
        "model.response.completed",
        "model.request.dispatched",
        "model.response.completed",
        "triage.work.output.validated",
    ]
    second_prompt = cast(
        list[dict[str, object]],
        runner.artifact_store.read_json(str(events[2].payload["prompt_hash"])),
    )
    assert len(second_prompt) >= 4
    assert "validation_error" in str(second_prompt[-1]["content"])
    calls = len(provider.requests)
    reopened = asyncio.run(runner.run())
    assert reopened == result
    assert len(provider.requests) == calls


@pytest.mark.parametrize("kind", ["wrong_model", "tool_call", "secret"])
def test_invalid_provider_response_accounts_usage_before_failure(tmp_path: Path, kind: str) -> None:
    provider = InvalidResponseProvider(kind)
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        secret_values=("TOPSECRET",) if kind == "secret" else (),
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.FAILED
    assert result.members[-1].metrics.input_tokens == 120
    assert result.members[-1].metrics.output_tokens == 80
    usage = runner.usage_ledger.records()[-1].record
    assert usage.metrics == result.members[-1].metrics
    response_types = [item.event_type for item in runner.journal.events(result.members[-1].run_id)]
    expected = "model.response.rejected" if kind == "secret" else "model.response.completed"
    assert expected in response_types
    if kind == "secret":
        assert all(
            b"TOPSECRET" not in path.read_bytes() for path in runner.artifact_store.root.iterdir()
        )
        assert (
            "TOPSECRET"
            not in canonical_json_bytes(
                [item.to_dict() for item in runner.journal.events(result.members[-1].run_id)]
            ).decode()
        )


def test_terminal_pointer_tamper_fails_before_usage_or_downstream(tmp_path: Path) -> None:
    provider = ScriptedWorkProvider()
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        runner_class=PointerTamperRunner,
    )
    with pytest.raises(ValueError, match="terminal pointer"):
        asyncio.run(runner.run())
    assert len(provider.requests) == 1
    assert runner.usage_ledger.records() == ()


def test_corrupt_completed_predecessor_blocks_restart_before_provider(tmp_path: Path) -> None:
    runner, _, _, _, _ = _runtime(tmp_path, arm=TriageComparisonArm.BASELINE, count=2)
    completed = asyncio.run(runner.run())
    first = completed.members[0]
    artifact = runner.artifact_store.get(
        first.terminal_artifact_hash, media_type="application/json"
    )
    artifact.path.write_bytes(b"{}")
    fresh_provider = ScriptedWorkProvider()
    runner.provider = fresh_provider
    with pytest.raises(ValueError, match="artifact content"):
        asyncio.run(runner.run())
    assert fresh_provider.requests == []
