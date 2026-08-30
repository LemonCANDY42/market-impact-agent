# pyright: reportPrivateUsage=false

import asyncio
import json
import sqlite3
from collections.abc import Mapping
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
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.cliproxy_provider import CLIProxyLunaConfig, CLIProxyLunaProvider
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
    _RESERVED_TRIAGE_CONTROL_TOKENS,
    TriageWorkManifestPolicy,
    build_event_impact_triage_work_manifest,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V2,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V3,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V4,
    EventImpactTriageWorkDecisionAuthority,
    EventImpactTriageWorkRunner,
    TriageWorkPhase,
    TriageWorkRoleBinding,
    TriageWorkRunMember,
    build_event_impact_triage_work_execution_plan,
    build_event_impact_triage_work_execution_plan_v3,
    build_event_impact_triage_work_execution_plan_v4,
    event_impact_triage_work_execution_plan_from_dict,
)
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.openai_chat_provider import JsonHttpTransport, OpenAIChatProviderError
from market_impact_agent.prospective_diagnostic import (
    load_prospective_diagnostic_registration,
)
from market_impact_agent.provider_reliability import (
    ProviderGenerationState,
    ProviderHealthStore,
    ProviderRetryDisposition,
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
        prompt_template_id = str(task["prompt_template_id"])
        positional = prompt_template_id.endswith(("-v3", "-v4"))
        typed_classify = prompt_template_id.endswith("-v4")
        phase_input = cast(dict[str, object], task["phase_input"])
        if phase == TriageWorkPhase.MAP.value and role != "coordinator":
            field = {
                "fact_verifier": "fact_findings",
                "transmission_mapper": "transmission_findings",
                "countercase_reviewer": "countercase_findings",
            }[role]
            atoms = cast(list[dict[str, object]], phase_input["atoms"])
            findings: list[dict[str, object]] = [
                ({field: []} if positional else {"atom_id": atom["atom_id"], field: []})
                for atom in atoms
            ]
            return {
                **(
                    {}
                    if positional
                    else {
                        "manifest_id": phase_input["manifest_id"],
                        "work_unit_id": phase_input["work_unit_id"],
                        "role": role,
                    }
                ),
                "atom_findings": findings,
            }
        if phase == TriageWorkPhase.MAP.value:
            atoms = cast(list[dict[str, object]], phase_input["atoms"])
            digests: list[dict[str, object]] = [
                {
                    **({} if positional else {"atom_id": atom["atom_id"]}),
                    "changed_facts": [],
                    "source_conflicts": [],
                    "transmission_paths": [],
                    "countercases": [],
                    "uncertainty_notes": [],
                    "checkpoint_rule_evidence": [],
                }
                for atom in atoms
            ]
            return {
                **(
                    {}
                    if positional
                    else {
                        "manifest_id": phase_input["manifest_id"],
                        "work_unit_id": phase_input["work_unit_id"],
                    }
                ),
                "digests": digests,
            }
        if phase == TriageWorkPhase.PARTITION.value:
            digests = cast(list[dict[str, object]], phase_input["digests"])
            identities = (
                [cast(int, item["atom_ordinal"]) for item in digests]
                if positional
                else [str(item["atom_id"]) for item in digests]
            )
            identity_field = "atom_ordinals" if positional else "atom_ids"
            cross_unit = [identities[0], identities[-1]]
            singleton_ids = identities[1:-1]
            return {
                **({} if positional else {"manifest_id": phase_input["manifest_id"]}),
                "clusters": [
                    {
                        identity_field: cross_unit,
                        "merge_state": "merged",
                        "merge_evidence": ["The fixture explicitly links the same event."],
                        "uncertainty_notes": [],
                    },
                    *(
                        {
                            identity_field: [atom_id],
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
            **({} if typed_classify else {"candidate_version_ids": versions}),
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


class OneFailureTransport(JsonHttpTransport):
    def __init__(self, failure: OpenAIChatProviderError) -> None:
        self.failure = failure
        self.requests: list[dict[str, object]] = []

    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        raise self.failure


class SafeRetryTransport(JsonHttpTransport):
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.rate_limit_emitted = False

    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]:
        assert payload is not None
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        if not self.rate_limit_emitted:
            self.rate_limit_emitted = True
            raise OpenAIChatProviderError(
                "rate limited before generation",
                error_class="http",
                diagnostic_code="rate_limited",
                http_status=429,
                generation_state=ProviderGenerationState.NOT_STARTED,
                retry_disposition=ProviderRetryDisposition.SAFE,
                attempts=1,
            )
        messages = cast(list[dict[str, object]], payload["messages"])
        task = next(
            decoded
            for message in reversed(messages)
            if message.get("role") == "user"
            for decoded in (json.loads(str(message["content"])),)
            if "phase" in decoded
        )
        output = ScriptedWorkProvider()._output(task)
        content = canonical_json_bytes(output).decode()
        response_id = f"safe-retry-{len(self.requests)}"
        return {
            "id": response_id,
            "model": "gpt-5.6-luna",
            "choices": [
                {
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 120, "completion_tokens": 80},
        }


class CrashAfterDispatchProvider(ScriptedWorkProvider):
    async def complete(self, **kwargs: object) -> ModelTurn:
        messages = kwargs.get("messages")
        assert isinstance(messages, tuple)
        self.requests.append(cast(tuple[dict[str, object], ...], messages))
        raise SimulatedProcessCrash


class CrashDuringValidationRunner(EventImpactTriageWorkRunner):
    crashed = False

    def _parse_output(
        self,
        binding: TriageWorkRoleBinding,
        unit_id: str,
        assistant_message: dict[str, object],
    ) -> object:
        if not self.crashed:
            self.crashed = True
            raise SimulatedProcessCrash
        return super()._parse_output(binding, unit_id, assistant_message)


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


class SubstitutedIdentityProvider(ScriptedWorkProvider):
    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if (
            task["phase"] == TriageWorkPhase.MAP.value
            and task["role"] == TriageAgentRole.COORDINATOR.value
            and str(task["prompt_template_id"]).endswith("-v2")
        ):
            digests = cast(list[dict[str, object]], output["digests"])
            digests[-1]["atom_id"] = "event-impact-triage-work-atom-" + "f" * 64
        return output


class RealMalformedDigestProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.malformed_turn = 0

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if (
            task["phase"] == TriageWorkPhase.MAP.value
            and task["role"] == TriageAgentRole.COORDINATOR.value
            and str(task["prompt_template_id"]).endswith("-v2")
        ):
            self.malformed_turn += 1
            digest = cast(list[dict[str, object]], output["digests"])[0]
            if self.malformed_turn == 1:
                digest["checkpoint_rule_evidence"] = {}
            elif self.malformed_turn == 2:
                digest["checkpoint_rule_evidence"] = [1]
            else:
                digest["countercases"] = ["recommended_route"]
        return output


class InvalidOrdinalProvider(ScriptedWorkProvider):
    def __init__(self, kind: str) -> None:
        super().__init__()
        self.kind = kind

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if task["phase"] != TriageWorkPhase.PARTITION.value:
            return output
        clusters = cast(list[dict[str, object]], output["clusters"])
        phase_input = cast(dict[str, object], task["phase_input"])
        count = len(cast(list[object], phase_input["digests"]))
        if self.kind == "bool":
            clusters[0]["atom_ordinals"] = [False]
        elif self.kind == "negative":
            clusters[0]["atom_ordinals"] = [-1]
        elif self.kind == "out_of_range":
            clusters[0]["atom_ordinals"] = [count]
        elif self.kind == "order":
            clusters[0]["atom_ordinals"] = [count - 1, 0]
        elif self.kind == "duplicate":
            clusters[1]["atom_ordinals"] = [0]
        else:
            clusters.pop(1)
        return output


class InvalidPartitionNarrativeProvider(ScriptedWorkProvider):
    def __init__(self, kind: str) -> None:
        super().__init__()
        self.kind = kind

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if task["phase"] != TriageWorkPhase.PARTITION.value:
            return output
        cluster = cast(list[dict[str, object]], output["clusters"])[0]
        if self.kind == "forbidden_control_vocabulary":
            cluster["merge_evidence"] = ["recommended_route"]
        elif self.kind == "invalid_merge_state":
            cluster["merge_state"] = "unclassified"
        elif self.kind == "merged_without_evidence":
            cluster["merge_evidence"] = []
        else:
            cluster["merge_state"] = "needs_review"
        return output


class InvalidV4ClassifyOnceProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.invalid_classify_emitted = False

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if task["phase"] == TriageWorkPhase.CLASSIFY.value and not self.invalid_classify_emitted:
            self.invalid_classify_emitted = True
            output["recommended_route"] = ["archive"]
        return output


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
    dialect: str = "v2",
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
    builder = {
        "v2": build_event_impact_triage_work_execution_plan,
        "v3": build_event_impact_triage_work_execution_plan_v3,
        "v4": build_event_impact_triage_work_execution_plan_v4,
    }[dialect]
    plan = builder(
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
    assert result.partition is not None
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


def test_provider_failure_journals_each_physical_post_with_sanitized_fields(
    tmp_path: Path,
) -> None:
    secret = "SUPERSECRET-PROVIDER-BODY"
    transport = OneFailureTransport(
        OpenAIChatProviderError(
            f"must not persist {secret}",
            error_class="tls",
            diagnostic_code="tls_bad_record_mac",
            http_status=500,
            generation_state=ProviderGenerationState.UNKNOWN,
            retry_disposition=ProviderRetryDisposition.FORBIDDEN,
            attempts=1,
        )
    )
    provider = CLIProxyLunaProvider(
        api_key="dedicated-local-key",
        config=CLIProxyLunaConfig(
            origin="http://127.0.0.1:8317",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            retry_backoff_seconds=0,
        ),
        transport=transport,
        request_id_factory=lambda: "mia-runtime-tls-1",
    )
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=cast(ScriptedWorkProvider, provider),
    )
    runner.provider_health_store = ProviderHealthStore(tmp_path / "provider-health.sqlite")

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert len(transport.requests) == 1
    events = runner.journal.events(result.members[-1].run_id)
    assert sum(item.event_type == "model.request.dispatched" for item in events) == 1
    failed = next(item for item in events if item.event_type == "model.request.failed")
    assert failed.payload["error_class"] == "tls"
    assert failed.payload["diagnostic_code"] == "tls_bad_record_mac"
    assert failed.payload["http_status"] == 500
    assert failed.payload["request_id"] == "mia-runtime-tls-1"
    assert failed.payload["generation_state"] == "unknown"
    assert failed.payload["retry_disposition"] == "forbidden"
    assert failed.payload["attempts"] == 1
    assert result.members[-1].metrics.provider_attempts == 1
    assert result.members[-1].metrics.latency_ms >= 0
    serialized_events = canonical_json_bytes([item.to_dict() for item in events])
    assert secret.encode() not in serialized_events
    assert not runner.provider_health_store.admission(provider.provider_id, now=NOW).allowed


def test_safe_pre_generation_retry_is_authoritative_and_counts_physical_posts(
    tmp_path: Path,
) -> None:
    transport = SafeRetryTransport()
    provider = CLIProxyLunaProvider(
        api_key="dedicated-local-key",
        config=CLIProxyLunaConfig(
            origin="http://127.0.0.1:8317",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            retry_backoff_seconds=0,
        ),
        transport=transport,
        request_id_factory=lambda: "mia-runtime-safe-retry",
    )
    runner, _, _, manifest, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=cast(ScriptedWorkProvider, provider),
        dialect="v4",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    first = result.members[0]
    assert first.metrics.provider_attempts == 2
    events = runner.journal.events(first.run_id)
    assert [item.event_type for item in events[:4]] == [
        "model.request.dispatched",
        "model.request.failed",
        "model.request.dispatched",
        "model.response.completed",
    ]
    assert result.partition is not None
    expected_calls = len(manifest.work_units) + 1 + len(result.partition.clusters)
    assert len(transport.requests) == expected_calls + 1
    calls_before_reopen = len(transport.requests)
    reopened = asyncio.run(runner.run())
    assert reopened.status is RunStatus.COMPLETED
    assert len(transport.requests) == calls_before_reopen


def test_restart_reuses_completed_response_after_validation_crash(tmp_path: Path) -> None:
    runner, provider, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        runner_class=CrashDuringValidationRunner,
        dialect="v4",
    )

    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    assert len(provider.requests) == 1

    recovered = EventImpactTriageWorkRunner(
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
    result = asyncio.run(recovered.run())

    assert result.status is RunStatus.COMPLETED
    assert result.partition is not None
    expected_calls = len(manifest.work_units) + 1 + len(result.partition.clusters)
    assert len(provider.requests) == expected_calls
    first_events = recovered.journal.events(result.members[0].run_id)
    assert [item.event_type for item in first_events].count("model.response.completed") == 1
    assert all(item.event_type != "model.request.ambiguous" for item in first_events)


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


def test_work_decision_authority_compacts_only_after_full_reopening(tmp_path: Path) -> None:
    runner, _, candidate_set, manifest, _ = _runtime(
        tmp_path, arm=TriageComparisonArm.TREATMENT, count=3, dialect="v4"
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.COMPLETED
    assert result.partition is not None
    assert result.proposal is not None
    assert result.run_evidence is not None
    authority = EventImpactTriageWorkDecisionAuthority(
        runner=runner,
        candidate_set=candidate_set,
        work_manifest=manifest,
        digests=result.digests,
        partition=result.partition,
        proposal=result.proposal,
        run_evidence=result.run_evidence,
    )

    evidence = authority.decision_evidence()
    assert evidence.plan_id == runner.plan.plan_id
    assert evidence.work_manifest_id == manifest.manifest_id
    assert evidence.completed_member_count == len(result.members)
    authority.assert_authoritative_completed_triage_work_run(
        candidate_set=candidate_set,
        proposal=result.proposal,
        run_evidence=evidence,
    )
    with pytest.raises(ValueError, match="differs from authoritative reopening"):
        authority.assert_authoritative_completed_triage_work_run(
            candidate_set=candidate_set,
            proposal=result.proposal,
            run_evidence=replace(evidence, authority_receipt_hash="f" * 64),
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


def test_v3_treatment_binds_all_roles_and_121_atoms_by_position(tmp_path: Path) -> None:
    runner, provider, _, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=121,
        dialect="v3",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V3
    assert tuple(item.atom_id for item in result.digests) == tuple(
        item.atom_id for item in manifest.atoms
    )
    assert result.partition is not None
    assert result.proposal is not None
    assert {item.role for item in result.members if item.phase is TriageWorkPhase.MAP} == {
        TriageAgentRole.FACT_VERIFIER,
        TriageAgentRole.TRANSMISSION_MAPPER,
        TriageAgentRole.COUNTERCASE_REVIEWER,
        TriageAgentRole.COORDINATOR,
    }
    first_unit = set(manifest.work_units[0].atom_ids)
    last_unit = set(manifest.work_units[-1].atom_ids)
    assert any(
        set(cluster.atom_ids) & first_unit and set(cluster.atom_ids) & last_unit
        for cluster in result.partition.clusters
    )
    for request in provider.requests:
        task = json.loads(str(request[-1]["content"]))
        assert str(task["prompt_template_id"]).endswith("-v3")
        required = cast(dict[str, object], task["required_output"])
        assert required["contract_version"] == "v3"
        if task["phase"] == TriageWorkPhase.MAP.value:
            assert "atom_id" not in canonical_json_bytes(required).decode()
    call_count = len(provider.requests)
    assert asyncio.run(runner.run()) == result
    assert len(provider.requests) == call_count


def test_v2_substituted_atom_identity_fails_but_v3_binds_semantics(tmp_path: Path) -> None:
    v2_provider = SubstitutedIdentityProvider()
    v2, _, _, _, _ = _runtime(
        tmp_path / "v2",
        arm=TriageComparisonArm.BASELINE,
        count=12,
        provider=v2_provider,
    )
    v2_result = asyncio.run(v2.run())
    assert v2_result.status is RunStatus.FAILED
    assert len(v2_provider.requests) == 3
    assert v2_result.members[-1].metrics.turns == 3

    v3_provider = SubstitutedIdentityProvider()
    v3, _, _, manifest, _ = _runtime(
        tmp_path / "v3",
        arm=TriageComparisonArm.BASELINE,
        count=12,
        provider=v3_provider,
        dialect="v3",
    )
    v3_result = asyncio.run(v3.run())
    assert v3_result.status is RunStatus.COMPLETED
    assert tuple(item.atom_id for item in v3_result.digests) == tuple(
        item.atom_id for item in manifest.atoms
    )


def test_v3_typed_digest_contract_avoids_real_v2_three_turn_failure(tmp_path: Path) -> None:
    v2_provider = RealMalformedDigestProvider()
    v2, _, _, _, _ = _runtime(
        tmp_path / "v2",
        arm=TriageComparisonArm.BASELINE,
        count=12,
        provider=v2_provider,
    )
    failed = asyncio.run(v2.run())
    assert failed.status is RunStatus.FAILED
    assert failed.members[-1].metrics.turns == 3
    v2_corrections = [
        json.loads(str(request[-1]["content"])) for request in v2_provider.requests[1:]
    ]
    assert "checkpoint_rule_evidence must be an array" in v2_corrections[0]["validation_error"]
    assert "must contain trimmed strings" in v2_corrections[1]["validation_error"]

    v3_provider = RealMalformedDigestProvider()
    v3, _, _, manifest, _ = _runtime(
        tmp_path / "v3",
        arm=TriageComparisonArm.BASELINE,
        count=12,
        provider=v3_provider,
        dialect="v3",
    )
    completed = asyncio.run(v3.run())
    assert completed.status is RunStatus.COMPLETED
    assert completed.members[0].metrics.turns == 1
    task = json.loads(str(v3_provider.requests[0][-1]["content"]))
    contract = cast(dict[str, object], task["required_output"])
    serialized_contract = canonical_json_bytes(contract).decode()
    assert '"type":"array"' in serialized_contract
    assert '"max_chars":600' in serialized_contract
    assert "recommended_route" in serialized_contract
    assert tuple(item.atom_id for item in completed.digests) == tuple(
        item.atom_id for item in manifest.atoms
    )


@pytest.mark.parametrize(
    "kind",
    ["bool", "negative", "out_of_range", "order", "duplicate", "missing"],
)
def test_v3_partition_ordinals_fail_closed(tmp_path: Path, kind: str) -> None:
    provider = InvalidOrdinalProvider(kind)
    runner, _, _, manifest, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=3,
        provider=provider,
        dialect="v3",
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.FAILED
    assert result.partition is None
    assert result.members[-1].phase is TriageWorkPhase.PARTITION
    assert result.members[-1].metrics.turns == 3
    corrections = [
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if "output_contract_version" in str(request[-1]["content"])
    ]
    assert corrections
    assert all(
        item["validation_error"] == "invalid_atom_ordinal_coverage_or_order" for item in corrections
    )
    assert all(
        all(atom.atom_id not in canonical_json_bytes(item).decode() for atom in manifest.atoms)
        for item in corrections
    )


def test_v3_partition_contract_describes_an_accepted_payload(tmp_path: Path) -> None:
    runner, provider, _, manifest, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=3,
        dialect="v3",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    partition_task = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.PARTITION.value
    )
    contract = cast(dict[str, object], partition_task["required_output"])
    assert contract["required_fields"] == ["clusters"]
    assert "manifest_id" not in canonical_json_bytes(contract).decode()
    fields = cast(dict[str, dict[str, object]], contract["field_schemas"])
    assert fields["merge_state"] == {
        "type": "string",
        "enum": ["merged", "needs_review"],
    }
    for field in ("merge_evidence", "uncertainty_notes"):
        text_array = fields[field]
        assert text_array["type"] == "array"
        assert text_array["max_items"] == 8
        items = cast(dict[str, object], text_array["items"])
        assert items["type"] == "string"
        assert items["trimmed"] is True
        assert items["min_chars"] == 1
        assert items["max_chars"] == 600
        assert items["forbidden_control_vocabulary"] == list(_RESERVED_TRIAGE_CONTROL_TOKENS)
    assert contract["conditional_requirements"] == [
        {
            "if": {
                "merge_state": {"const": "merged"},
                "atom_ordinals": {"min_items": 2},
            },
            "then": {"merge_evidence": {"min_items": 1}},
        },
        {
            "if": {"merge_state": {"const": "needs_review"}},
            "then": {"uncertainty_notes": {"min_items": 1}},
        },
    ]
    serialized_contract = canonical_json_bytes(contract).decode()
    assert all(atom.atom_id not in serialized_contract for atom in manifest.atoms)


@pytest.mark.parametrize(
    ("kind", "validation_error"),
    [
        ("forbidden_control_vocabulary", "forbidden_control_vocabulary"),
        ("invalid_merge_state", "invalid_merge_state"),
        (
            "merged_without_evidence",
            "merge_evidence_required_for_merged_multi_atom_cluster",
        ),
        (
            "needs_review_without_uncertainty",
            "uncertainty_notes_required_for_needs_review_cluster",
        ),
    ],
)
def test_v3_partition_contract_violations_are_rejected_and_corrected(
    tmp_path: Path, kind: str, validation_error: str
) -> None:
    provider = InvalidPartitionNarrativeProvider(kind)
    runner, _, _, manifest, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=3,
        provider=provider,
        dialect="v3",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.FAILED
    assert result.partition is None
    assert result.members[-1].phase is TriageWorkPhase.PARTITION
    corrections = [
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if "output_contract_version" in str(request[-1]["content"])
    ]
    initial_contract = next(
        task["required_output"]
        for request in provider.requests
        for task in [json.loads(str(request[-1]["content"]))]
        if task.get("phase") == TriageWorkPhase.PARTITION.value
    )
    assert len(corrections) == 2
    assert {item["validation_error"] for item in corrections} == {validation_error}
    assert all(item["required_output"] == initial_contract for item in corrections)
    assert all(
        all(atom.atom_id not in canonical_json_bytes(item).decode() for atom in manifest.atoms)
        for item in corrections
    )


def test_v3_correction_usage_roundtrip_schema_and_parent_revision(tmp_path: Path) -> None:
    provider = CorrectionOnceProvider()
    runner, _, _, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v3",
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.COMPLETED
    assert result.members[0].metrics.turns == 2
    correction = json.loads(str(provider.requests[1][-1]["content"]))
    assert correction["output_contract_version"] == "v3"
    assert correction["validation_error"] == "closed_object_fields_or_binding_invalid"
    assert event_impact_triage_work_execution_plan_from_dict(plan.to_dict()) == plan
    assert not validate_agent_contract(
        plan.to_dict(), "event-impact-triage-work-execution-plan-v3.schema.json"
    )

    payload = plan.to_dict()
    payload["schema_version"] = EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V2
    with pytest.raises(ValueError, match="Plan and role binding revisions"):
        event_impact_triage_work_execution_plan_from_dict(payload)


def test_v4_classify_contract_is_typed_and_harness_binds_cluster_identity(
    tmp_path: Path,
) -> None:
    runner, provider, candidate_set, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=3,
        dialect="v4",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V4
    assert result.proposal is not None
    assert {
        version_id
        for cluster in result.proposal.clusters
        for version_id in cluster.candidate_version_ids
    } == set(candidate_set.version_ids)
    classify_tasks = [
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    ]
    assert classify_tasks
    for task in classify_tasks:
        assert str(task["prompt_template_id"]).endswith("-v4")
        contract = cast(dict[str, object], task["required_output"])
        assert contract["contract_version"] == "v4"
        assert contract["candidate_version_ids_injected_by_harness"] is True
        assert "candidate_version_ids" not in cast(list[str], contract["required_fields"])
        fields = cast(dict[str, dict[str, object]], contract["field_schemas"])
        assert fields["checkpoint_eligibility"]["enum"] == [
            "eligible",
            "ineligible",
            "needs_review",
        ]
        assert fields["recommended_route"]["enum"] == [
            "checkpoint_candidate",
            "event_assessment",
            "attention_watch",
            "archive",
        ]
        assert fields["triage_confidence"] == {
            "type": "number",
            "minimum": 0,
            "maximum": 1,
        }
        for field in (
            "changed_facts",
            "rule_reasons",
            "uncertainty_notes",
            "countercases",
            "affected_entity_refs",
            "watch_questions",
        ):
            assert fields[field]["max_items"] == 8
            assert cast(dict[str, object], fields[field]["items"])["max_chars"] == 600
        for field in ("event_archetypes", "evidence_version_ids", "transmission_channels"):
            assert fields[field]["unique_items"] is True
    assert event_impact_triage_work_execution_plan_from_dict(plan.to_dict()) == plan
    assert not validate_agent_contract(
        plan.to_dict(), "event-impact-triage-work-execution-plan.schema.json"
    )


def test_v4_invalid_classify_scalar_is_corrected_under_same_typed_contract(
    tmp_path: Path,
) -> None:
    provider = InvalidV4ClassifyOnceProvider()
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v4",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    classify_member = next(
        item for item in result.members if item.phase is TriageWorkPhase.CLASSIFY
    )
    assert classify_member.metrics.turns == 2
    correction = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if "output_contract_version" in str(request[-1]["content"])
    )
    assert correction["output_contract_version"] == "v4"
    assert correction["validation_error"] == "closed_output_contract_invalid"
    contract = cast(dict[str, object], correction["required_output"])
    assert contract["contract_version"] == "v4"
    assert contract["candidate_version_ids_injected_by_harness"] is True


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("too_many_narratives", "v4 item limit"),
        ("long_narrative", "v4 character limit"),
        ("untrimmed_narrative", "trimmed strings"),
        ("duplicate_archetype", "event_archetypes must contain unique items"),
        ("duplicate_evidence", "evidence_version_ids must contain unique items"),
        ("duplicate_channel", "transmission_channels must contain unique items"),
    ],
)
def test_v4_classify_parser_rejects_declared_contract_bound_violations(
    tmp_path: Path, kind: str, message: str
) -> None:
    runner, provider, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        dialect="v4",
    )
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.COMPLETED
    assert result.partition is not None
    task = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    )
    output = provider._output(task)
    versions = cast(list[str], output["evidence_version_ids"])
    if kind == "too_many_narratives":
        output["changed_facts"] = [f"fact {index}" for index in range(9)]
    elif kind == "long_narrative":
        output["rule_reasons"] = ["x" * 601]
    elif kind == "untrimmed_narrative":
        output["uncertainty_notes"] = [" not trimmed"]
    elif kind == "duplicate_archetype":
        output["event_archetypes"] = ["policy_regulatory", "policy_regulatory"]
    elif kind == "duplicate_evidence":
        output["evidence_version_ids"] = [versions[0], versions[0]]
    else:
        output["transmission_channels"] = ["policy_access", "policy_access"]

    with pytest.raises((TypeError, ValueError), match=message):
        runner._parse_cluster_proposal(output, result.partition.clusters[0])


def test_v2_plan_prompt_and_output_contract_bytes_remain_frozen(tmp_path: Path) -> None:
    runner, _, _, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
    )
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V2
    assert plan.plan_id == (
        "event-impact-triage-work-execution-plan-"
        "aa2a2d6fd2127a749fb8b4b8aeead48ede7dd67d367360cd3cbde567ad8f168e"
    )
    assert canonical_hash(plan.to_dict()) == (
        "bc985e791ea5891c26d4695153313328b770086d18c70cd9a72b2b64ff85f07d"
    )
    unit = manifest.work_units[0]
    binding = plan.map_bindings[0]
    contents = runner.content_resolver.resolve(runner.candidate_set)
    messages = runner._messages(
        binding,
        runner._map_input(
            unit,
            binding.role,
            {item.version_id: item for item in contents},
            {item.atom_id: item for item in manifest.atoms},
            (),
        ),
    )
    assert canonical_hash(messages) == (
        "32504bceffd2d040256a3ac114782028dafa393506ddbc7f0cf4735d8c101734"
    )
    assert binding.output_contract_hash == (
        "393ca46a135a13c48d8b0388103666b60b860cdacbe1ea3cf7a161804f3dbd7d"
    )
