# pyright: reportPrivateUsage=false

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import market_impact_agent.prospective_triage as prospective_triage
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
from market_impact_agent.event_impact_triage_work_format_recovery import (
    EventImpactTriageWorkFormatRecoveryStore,
)
from market_impact_agent.event_impact_triage_work_replacement import (
    EventImpactTriageWorkReplacementStore,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V2,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V3,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V4,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V5,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V6,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V7,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V8,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V10,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V12,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V13,
    EventImpactTriageWorkDecisionAuthority,
    EventImpactTriageWorkRunner,
    TriageWorkPhase,
    TriageWorkRoleBinding,
    TriageWorkRunMember,
    _output_contract_for_binding,
    build_event_impact_triage_work_execution_plan,
    build_event_impact_triage_work_execution_plan_v3,
    build_event_impact_triage_work_execution_plan_v4,
    build_event_impact_triage_work_execution_plan_v5,
    build_event_impact_triage_work_execution_plan_v6,
    build_event_impact_triage_work_execution_plan_v7,
    build_event_impact_triage_work_execution_plan_v8,
    build_event_impact_triage_work_execution_plan_v9,
    build_event_impact_triage_work_execution_plan_v10,
    build_event_impact_triage_work_execution_plan_v11,
    build_event_impact_triage_work_execution_plan_v12,
    build_event_impact_triage_work_execution_plan_v13,
    event_impact_triage_work_execution_plan_from_dict,
)
from market_impact_agent.model_provider import (
    load_builtin_model_provider_profile,
)
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.prospective_triage import (
    PreparedProspectiveTriageWork,
    ProspectiveTriageActiveBatchStore,
    run_prepared_prospective_triage_work,
)
from market_impact_agent.provider_reliability import (
    ProviderFailure,
    ProviderGenerationState,
    ProviderRetryDisposition,
)
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

from .runtime_fakes import BusinessModelFixture

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


class ScriptedWorkProvider(BusinessModelFixture):
    def __init__(self, *, over_budget: bool = False) -> None:
        self.requests: list[tuple[dict[str, object], ...]] = []
        self.over_budget = over_budget

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

    async def answer(
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
        positional = prompt_template_id.endswith(("-v3", "-v4", "-v5", "-v6", "-v7", "-v8", "-v8m"))
        typed_classify = prompt_template_id.endswith(("-v4", "-v5", "-v6", "-v7", "-v8", "-v8m"))
        ordinal_evidence = prompt_template_id.endswith(("-v5", "-v6", "-v7", "-v8", "-v8m"))
        phase_input = cast(dict[str, object], task["phase_input"])
        if prompt_template_id.endswith(("-v9", "-v10", "-v11")):
            atoms = cast(list[dict[str, object]], phase_input["atoms"])
            return {
                "routes": [
                    {
                        "route": "archive",
                        "changed_fact": "The supplied item reports a fixture fact.",
                        "transmission": None,
                        "watch_for": None,
                    }
                    for _ in atoms
                ]
            }
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
        material_stage_one = prompt_template_id.endswith("-v8m")
        return {
            **({} if typed_classify else {"candidate_version_ids": versions}),
            **({} if material_stage_one else {"checkpoint_eligibility": "ineligible"}),
            "recommended_route": "archive",
            "event_archetypes": [],
            "event_stage": "first_observed",
            "changed_facts": [],
            "rule_reasons": ["No registered checkpoint event is supported."],
            **(
                {"evidence_ordinals": list(range(len(versions)))}
                if ordinal_evidence
                else {"evidence_version_ids": versions}
            ),
            "uncertainty_notes": [],
            "countercases": [],
            "transmission_channels": [],
            "affected_entity_refs": [],
            "watch_questions": [],
            "triage_confidence": 0.8,
        }


class ConcurrencyTrackingProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.active = 0
        self.peak = 0
        self.timeline: list[tuple[str, str]] = []

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        task = next(
            decoded
            for message in reversed(messages)
            if message.get("role") == "user"
            for decoded in (json.loads(str(message["content"])),)
            if "phase" in decoded
        )
        phase = str(task["phase"])
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.timeline.append(("start", phase))
        try:
            await asyncio.sleep(0.01)
            return await super().answer(
                messages=messages,
                tools=tools,
                temperature=temperature,
                top_p=top_p,
                max_output_tokens=max_output_tokens,
                timeout_seconds=timeout_seconds,
            )
        finally:
            self.timeline.append(("end", phase))
            self.active -= 1


class OneConcurrentTerminalFailureProvider(ConcurrencyTrackingProvider):
    def __init__(
        self,
        states: tuple[ProviderGenerationState, ...] = (ProviderGenerationState.RESPONSE_RECEIVED,),
    ) -> None:
        super().__init__()
        self.states = list(states)

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        task = next(
            decoded
            for message in reversed(messages)
            if message.get("role") == "user"
            for decoded in (json.loads(str(message["content"])),)
            if "phase" in decoded
        )
        if self.states and task["phase"] == TriageWorkPhase.MAP.value:
            state = self.states.pop(0)
            await asyncio.sleep(0.01)
            raise ProviderFailure(
                "fixture terminal response failure",
                error_class="invalid_response",
                diagnostic_code="fixture_terminal_response",
                generation_state=state,
                retry_disposition=(
                    ProviderRetryDisposition.FORBIDDEN
                    if state is ProviderGenerationState.UNKNOWN
                    else ProviderRetryDisposition.TERMINAL
                ),
                attempts=1,
            )
        return await super().answer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )


class FailOnceAvailabilityProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.availability_calls = 0

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        _ = timeout_seconds
        self.availability_calls += 1
        if self.availability_calls == 1:
            raise ProviderFailure(
                "fixture model is unavailable",
                error_class="model_unavailable",
                diagnostic_code="model_unavailable",
                generation_state=ProviderGenerationState.RESPONSE_RECEIVED,
                retry_disposition=ProviderRetryDisposition.TERMINAL,
                attempts=1,
            )


class SlowAvailabilityProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.availability_calls = 0

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        _ = timeout_seconds
        self.availability_calls += 1
        await asyncio.sleep(0.01)


class InvalidV5EvidenceOnceProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.invalid_emitted = False

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if (
            task["phase"] == TriageWorkPhase.CLASSIFY.value
            and str(task["prompt_template_id"]).endswith("-v5")
            and not self.invalid_emitted
        ):
            output["evidence_ordinals"] = [999]
            self.invalid_emitted = True
        return output


class MaterialWatchProvider(ScriptedWorkProvider):
    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if task["phase"] != TriageWorkPhase.CLASSIFY.value:
            return output
        assert str(task["prompt_template_id"]).endswith("-v8m")
        assert "checkpoint_eligibility" not in output
        output.update(
            {
                "recommended_route": "attention_watch",
                "event_archetypes": ["issuer_corporate"],
                "changed_facts": ["A frozen issuer fact changed."],
                "rule_reasons": ["Target exposure still requires evidence."],
                "uncertainty_notes": ["The affected tradable target is unresolved."],
                "countercases": ["The event may remain operationally immaterial."],
                "watch_questions": ["Which registered target has direct exposure?"],
                "triage_confidence": 0.7,
            }
        )
        return output


class MixedMaterialIngressProvider(ScriptedWorkProvider):
    def _output(self, task: dict[str, object]) -> dict[str, object]:
        if not str(task["prompt_template_id"]).endswith(("-v9", "-v10", "-v11")):
            return super()._output(task)
        atoms = cast(list[dict[str, object]], cast(dict[str, object], task["phase_input"])["atoms"])
        fixtures = (
            {
                "route": "event_assessment",
                "changed_fact": "A shipping route was interrupted.",
                "transmission": {
                    "event_archetype": "physical_supply_logistics",
                    "channel": "capacity_cost_inventory",
                    "path": "The interruption can raise delivered input costs.",
                },
                "watch_for": None,
            },
            {
                "route": "attention_watch",
                "changed_fact": "A company announced exploratory cooperation.",
                "transmission": None,
                "watch_for": "A binding contract with quantified economics.",
            },
            {
                "route": "archive",
                "changed_fact": "The item repeats a routine calendar notice.",
                "transmission": None,
                "watch_for": None,
            },
        )
        return {"routes": list(fixtures[: len(atoms)])}


class InvalidV6RouteOnceProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.invalid_emitted = False

    def _output(self, task: dict[str, object]) -> dict[str, object]:
        output = super()._output(task)
        if (
            task["phase"] == TriageWorkPhase.CLASSIFY.value
            and str(task["prompt_template_id"]).endswith("-v6")
            and not self.invalid_emitted
        ):
            output.update(
                {
                    "recommended_route": "event_assessment",
                    "event_archetypes": ["issuer_corporate"],
                    "changed_facts": ["A frozen issuer fact changed."],
                    "transmission_channels": [],
                }
            )
            self.invalid_emitted = True
        return output


class OneExtraBracketProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.malformed_emitted = False

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        turn = await super().answer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        task = next(
            decoded
            for message in reversed(messages)
            if message.get("role") == "user"
            for decoded in (json.loads(str(message["content"])),)
            if "phase" in decoded
        )
        if self.malformed_emitted or task["phase"] != TriageWorkPhase.CLASSIFY.value:
            return turn
        content = cast(str, turn.assistant_message["content"])
        malformed = content.replace('],"triage_confidence"', ']],"triage_confidence"', 1)
        assert malformed != content
        self.malformed_emitted = True
        return ModelTurn(
            response_id=turn.response_id,
            model=turn.model,
            assistant_message={"role": "assistant", "content": malformed},
            tool_calls=turn.tool_calls,
            finish_reason=turn.finish_reason,
            usage=turn.usage,
            raw_response={"id": turn.response_id, "content": malformed},
            latency_ms=turn.latency_ms,
            attempts=turn.attempts,
        )


class AlwaysExtraBracketProvider(ScriptedWorkProvider):
    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        turn = await super().answer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        task = next(
            decoded
            for message in reversed(messages)
            if message.get("role") == "user"
            for decoded in (json.loads(str(message["content"])),)
            if "phase" in decoded
        )
        if task["phase"] != TriageWorkPhase.CLASSIFY.value:
            return turn
        content = cast(str, turn.assistant_message["content"])
        malformed = content.replace('],"triage_confidence"', ']],"triage_confidence"', 1)
        assert malformed != content
        return ModelTurn(
            response_id=turn.response_id,
            model=turn.model,
            assistant_message={"role": "assistant", "content": malformed},
            tool_calls=turn.tool_calls,
            finish_reason=turn.finish_reason,
            usage=turn.usage,
            raw_response={"id": turn.response_id, "content": malformed},
            latency_ms=turn.latency_ms,
            attempts=turn.attempts,
        )


class SimulatedProcessCrash(BaseException):
    pass


class CrashAfterDispatchProvider(ScriptedWorkProvider):
    async def answer(self, **kwargs: object) -> ModelTurn:
        messages = kwargs.get("messages")
        assert isinstance(messages, tuple)
        self.requests.append(cast(tuple[dict[str, object], ...], messages))
        raise SimulatedProcessCrash


class InvalidThenCrashProvider(ScriptedWorkProvider):
    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        if self.requests:
            self.requests.append(messages)
            raise SimulatedProcessCrash
        turn = await super().answer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        return replace(
            turn,
            assistant_message={"role": "assistant", "content": "{}"},
            usage=ProviderUsage(input_tokens=120, output_tokens=30_000),
            raw_response={"id": "invalid-before-crash", "content": "{}"},
        )


class RemainingBudgetProvider(ScriptedWorkProvider):
    def __init__(self) -> None:
        super().__init__()
        self.maximum_outputs: list[int] = []

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        self.maximum_outputs.append(max_output_tokens)
        turn = await super().answer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        return replace(
            turn,
            usage=ProviderUsage(input_tokens=120, output_tokens=max_output_tokens),
        )


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
    async def answer(
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
        return await super().answer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )


class CorrectionOnceProvider(ScriptedWorkProvider):
    async def answer(
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
        return await super().answer(
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

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        turn = await super().answer(
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


def _material_registration():
    return load_prospective_diagnostic_registration(
        ROOT / "examples" / "research" / "prospective-diagnostic-registration-v4.json"
    )


def _batch(
    count: int,
    *,
    registration: ProspectiveDiagnosticRegistration | None = None,
    checkpoint_key: str = "next-a-share-policy-event",
) -> tuple[EventImpactTriageCandidateSet, tuple[TriageCandidateContent, ...]]:
    registration = registration or _registration()
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
        "checkpoint_key": checkpoint_key,
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
            checkpoint_key=checkpoint_key,
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
    replacement_store: EventImpactTriageWorkReplacementStore | None = None,
    format_recovery_store: EventImpactTriageWorkFormatRecoveryStore | None = None,
    registration: ProspectiveDiagnosticRegistration | None = None,
    checkpoint_key: str = "next-a-share-policy-event",
    model_profile_alias: str = PROFILE_ALIAS,
):
    registration = registration or _registration()
    candidate_set, contents = _batch(
        count,
        registration=registration,
        checkpoint_key=checkpoint_key,
    )
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
    profile = load_builtin_model_provider_profile(model_profile_alias)
    builder = {
        "v2": build_event_impact_triage_work_execution_plan,
        "v3": build_event_impact_triage_work_execution_plan_v3,
        "v4": build_event_impact_triage_work_execution_plan_v4,
        "v5": build_event_impact_triage_work_execution_plan_v5,
        "v6": build_event_impact_triage_work_execution_plan_v6,
        "v7": build_event_impact_triage_work_execution_plan_v7,
        "v8": build_event_impact_triage_work_execution_plan_v8,
        "v9": build_event_impact_triage_work_execution_plan_v9,
        "v10": build_event_impact_triage_work_execution_plan_v10,
        "v11": build_event_impact_triage_work_execution_plan_v11,
        "v12": build_event_impact_triage_work_execution_plan_v12,
        "v13": build_event_impact_triage_work_execution_plan_v13,
    }[dialect]
    plan = builder(
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=registration,
        arm=arm,
        model_profile_alias=model_profile_alias,
        model_profile=profile,
        skills=skills,
    )
    actual_provider = provider or ScriptedWorkProvider()
    runner = runner_class(
        plan=plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=registration,
        provider=actual_provider,
        content_resolver=StaticResolver(contents),
        skills=skills,
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "journal.sqlite"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite"),
        replacement_store=replacement_store,
        format_recovery_store=format_recovery_store,
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


def test_explicit_replacement_reuses_completed_work_and_preserves_ambiguous_run(
    tmp_path: Path,
) -> None:
    crashing_provider = CrashAfterDispatchProvider()
    replacement_store = EventImpactTriageWorkReplacementStore(
        tmp_path / "replacement-authority.sqlite"
    )
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=crashing_provider,
        dialect="v4",
        replacement_store=replacement_store,
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    blocked = asyncio.run(runner.run())
    assert blocked.status is RunStatus.HUMAN_INPUT_REQUIRED
    original = blocked.members[-1]
    original_record = runner.journal.get_run(original.run_id)
    original_events = runner.journal.events(original.run_id)
    original_usage = tuple(runner.usage_ledger.records())

    grant = replacement_store.authorize_once(
        plan_id=plan.plan_id,
        phase=original.phase.value,
        unit_id=original.unit_id,
        role=original.role.value,
        original_run_id=original.run_id,
        authorized_at=NOW + timedelta(minutes=1),
        journal=runner.journal,
        artifact_store=runner.artifact_store,
        usage_ledger=runner.usage_ledger,
    )
    assert grant.original_run_id == original.run_id
    assert grant.replacement_run_id != original.run_id
    assert (
        replacement_store.authorize_once(
            plan_id=plan.plan_id,
            phase=original.phase.value,
            unit_id=original.unit_id,
            role=original.role.value,
            original_run_id=original.run_id,
            authorized_at=NOW + timedelta(minutes=2),
            journal=runner.journal,
            artifact_store=runner.artifact_store,
            usage_ledger=runner.usage_ledger,
        )
        == grant
    )

    healthy_provider = ScriptedWorkProvider()
    recovered = EventImpactTriageWorkRunner(
        plan=plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        provider=healthy_provider,
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=runner.usage_ledger,
        replacement_store=replacement_store,
        clock=lambda: NOW + timedelta(minutes=3),
    )
    result = asyncio.run(recovered.run())

    assert result.status is RunStatus.COMPLETED
    assert result.proposal is not None
    assert result.run_evidence is not None
    replacement = next(
        member
        for member in result.members
        if member.phase is original.phase and member.unit_id == original.unit_id
    )
    assert replacement.run_id == grant.replacement_run_id
    assert recovered.journal.get_run(original.run_id) == original_record
    assert recovered.journal.events(original.run_id) == original_events
    assert tuple(recovered.usage_ledger.records())[: len(original_usage)] == original_usage
    assert len(healthy_provider.requests) == len(result.members)

    request_count = len(healthy_provider.requests)
    reopened = asyncio.run(recovered.run())
    assert reopened == result
    assert len(healthy_provider.requests) == request_count


def test_replacement_consumes_original_turn_and_token_budgets(tmp_path: Path) -> None:
    replacement_store = EventImpactTriageWorkReplacementStore(
        tmp_path / "replacement-authority.sqlite"
    )
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=InvalidThenCrashProvider(),
        dialect="v6",
        replacement_store=replacement_store,
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    blocked = asyncio.run(runner.run())
    original = blocked.members[-1]
    binding = plan.binding(original.phase, original.role)
    assert original.metrics.turns == 1
    assert original.metrics.output_tokens == 30_000
    grant = replacement_store.authorize_once(
        plan_id=plan.plan_id,
        phase=original.phase.value,
        unit_id=original.unit_id,
        role=original.role.value,
        original_run_id=original.run_id,
        authorized_at=NOW + timedelta(minutes=1),
        journal=runner.journal,
        artifact_store=runner.artifact_store,
        usage_ledger=runner.usage_ledger,
    )

    provider = RemainingBudgetProvider()
    replacement_runner = EventImpactTriageWorkRunner(
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
        replacement_store=replacement_store,
        clock=lambda: NOW + timedelta(minutes=2),
    )
    result = asyncio.run(replacement_runner.run())

    assert result.status is RunStatus.COMPLETED
    assert result.partition is not None
    assert result.proposal is not None
    assert result.run_evidence is not None
    replacement = next(
        member for member in result.members if member.run_id == grant.replacement_run_id
    )
    assert replacement.metrics.turns == 1
    assert replacement.metrics.output_tokens == binding.max_output_tokens - 30_000
    assert provider.maximum_outputs[0] == binding.max_output_tokens - 30_000
    replacement_runner.assert_authoritative_completed_work_run(
        candidate_set=candidate_set,
        work_manifest=manifest,
        digests=result.digests,
        partition=result.partition,
        proposal=result.proposal,
        run_evidence=result.run_evidence,
    )


def test_replacement_cannot_start_before_grant_authority(tmp_path: Path) -> None:
    replacement_store = EventImpactTriageWorkReplacementStore(
        tmp_path / "replacement-authority.sqlite"
    )
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=CrashAfterDispatchProvider(),
        dialect="v6",
        replacement_store=replacement_store,
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    original = asyncio.run(runner.run()).members[-1]
    grant = replacement_store.authorize_once(
        plan_id=plan.plan_id,
        phase=original.phase.value,
        unit_id=original.unit_id,
        role=original.role.value,
        original_run_id=original.run_id,
        authorized_at=NOW + timedelta(minutes=5),
        journal=runner.journal,
        artifact_store=runner.artifact_store,
        usage_ledger=runner.usage_ledger,
    )
    replacement_runner = EventImpactTriageWorkRunner(
        plan=plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        provider=ScriptedWorkProvider(),
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=runner.usage_ledger,
        replacement_store=replacement_store,
        clock=lambda: NOW + timedelta(minutes=4),
    )

    with pytest.raises(ValueError, match="before its Grant authority"):
        asyncio.run(replacement_runner.run())
    with pytest.raises(KeyError):
        replacement_runner.journal.get_run(grant.replacement_run_id)


def test_ambiguous_replacement_cannot_be_replaced_again(tmp_path: Path) -> None:
    replacement_store = EventImpactTriageWorkReplacementStore(
        tmp_path / "replacement-authority.sqlite"
    )
    first_provider = CrashAfterDispatchProvider()
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=first_provider,
        dialect="v4",
        replacement_store=replacement_store,
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    original = asyncio.run(runner.run()).members[-1]
    grant = replacement_store.authorize_once(
        plan_id=plan.plan_id,
        phase=original.phase.value,
        unit_id=original.unit_id,
        role=original.role.value,
        original_run_id=original.run_id,
        authorized_at=NOW + timedelta(minutes=1),
        journal=runner.journal,
        artifact_store=runner.artifact_store,
        usage_ledger=runner.usage_ledger,
    )

    second_provider = CrashAfterDispatchProvider()
    replacement_runner = EventImpactTriageWorkRunner(
        plan=plan,
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=_registration(),
        provider=second_provider,
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=runner.usage_ledger,
        replacement_store=replacement_store,
        clock=lambda: NOW + timedelta(minutes=2),
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(replacement_runner.run())
    blocked = asyncio.run(replacement_runner.run())
    assert blocked.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert blocked.members[-1].run_id == grant.replacement_run_id

    with pytest.raises(ValueError, match="cannot itself be replaced"):
        replacement_store.authorize_once(
            plan_id=plan.plan_id,
            phase=original.phase.value,
            unit_id=original.unit_id,
            role=original.role.value,
            original_run_id=grant.replacement_run_id,
            authorized_at=NOW + timedelta(minutes=3),
            journal=runner.journal,
            artifact_store=runner.artifact_store,
            usage_ledger=runner.usage_ledger,
        )


def test_sealed_legacy_v4_output_contracts_remain_replayable(tmp_path: Path) -> None:
    runner, _, _, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=2,
        dialect="v4",
    )
    _ = runner
    legacy_hashes = {
        (TriageWorkPhase.MAP, TriageAgentRole.FACT_VERIFIER): (
            "fb5ad688740da362518a69165945e832819606f387f619a5cdac8da776ee6f28"
        ),
        (TriageWorkPhase.MAP, TriageAgentRole.TRANSMISSION_MAPPER): (
            "6b4697494acf7f37d5d7be647292e503031468ac68f43e418b7c2cd9793f824d"
        ),
        (TriageWorkPhase.MAP, TriageAgentRole.COUNTERCASE_REVIEWER): (
            "3ac3c57784cb451744a5289d9befb8db7f2269b08b25aac55deee2205b340e69"
        ),
        (TriageWorkPhase.MAP, TriageAgentRole.COORDINATOR): (
            "1c0aad606d692db1f1957db5a354c8c01bb2e6c47c4cc8a5451ea648edc2544b"
        ),
        (TriageWorkPhase.PARTITION, TriageAgentRole.COORDINATOR): (
            "d9dfbac685e4e1448620dbe6b9f26f498a313898f1e22e9d4f2c26bb6dca6989"
        ),
        (TriageWorkPhase.CLASSIFY, TriageAgentRole.COORDINATOR): (
            "7197459407b8bf1e3f5653fb22668888c298a0ab1327cf45309e5ff6d1b013c0"
        ),
    }
    for key, output_contract_hash in legacy_hashes.items():
        legacy = replace(plan.binding(*key), output_contract_hash=output_contract_hash)
        assert canonical_hash(_output_contract_for_binding(legacy)) == output_contract_hash


def test_lazy_availability_failure_stays_nonterminal_until_later_recovery(
    tmp_path: Path,
) -> None:
    provider = FailOnceAvailabilityProvider()
    lazy_provider = provider
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=1,
        provider=cast(ScriptedWorkProvider, lazy_provider),
        dialect="v9",
        registration=_material_registration(),
        checkpoint_key="next-material-a-share-event",
    )

    first = asyncio.run(runner.run())

    assert first.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert provider.availability_calls == 1
    assert provider.requests == []
    assert len(first.members) == 1
    member = first.members[0]
    assert member.metrics.provider_attempts == 0
    record = runner.journal.get_run(member.run_id)
    assert record.status is RunStatus.RUNNING
    assert record.terminal_artifact_id is None
    events = runner.journal.events(member.run_id)
    assert [event.event_type for event in events] == ["provider.preparation.failed"]
    failure = events[0].payload
    assert failure["model_generation_state"] == "not_started"
    assert failure["model_retry_disposition"] == "safe"
    assert failure["provider_attempts"] == 0
    assert failure["preparation_retry_disposition"] == "terminal"
    assert runner.usage_ledger.records() == ()

    recovered = asyncio.run(runner.run())

    assert recovered.status is RunStatus.COMPLETED
    assert provider.availability_calls == 2
    assert len(provider.requests) == 1
    assert runner.journal.get_run(member.run_id).status is RunStatus.COMPLETED
    recovered_events = runner.journal.events(member.run_id)
    assert sum(event.event_type == "model.request.dispatched" for event in recovered_events) == 1
    assert not any(event.event_type == "model.request.ambiguous" for event in recovered_events)
    assert len(runner.usage_ledger.records()) == 1
    assert runner.usage_ledger.records()[0].record.metrics.provider_attempts == 1


def test_recovered_preparation_failure_completes_and_releases_active_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_root = tmp_path / "prospective"
    state_root = tmp_path / "state"
    registration = _material_registration()
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path / "runner",
        arm=TriageComparisonArm.BASELINE,
        count=2,
        dialect="v8",
        registration=registration,
        checkpoint_key="next-material-a-share-event",
    )
    runner._clock = lambda: candidate_set.frozen_at + timedelta(minutes=1)
    protocol_hashes = {
        "readiness": canonical_hash({"fixture": "readiness"}),
        "selection": canonical_hash({"fixture": "selection"}),
        "candidate_set": canonical_hash(candidate_set.to_dict()),
        "work_manifest": canonical_hash(manifest.to_dict()),
        "execution_plan": canonical_hash(plan.to_dict()),
    }
    active_core = {
        "schema_version": "market-impact.prospective-triage-active-batch.v1",
        "registration_id": candidate_set.registration_id,
        "checkpoint_key": candidate_set.checkpoint_key,
        "route_plan_id": candidate_set.route_plan_id,
        "route_admission_id": candidate_set.route_admission_id,
        "readiness_report_id": candidate_set.readiness_report_id,
        "unclassified_candidate_count": len(candidate_set.version_ids),
        "data_snapshot_id": candidate_set.data_snapshot_id,
        "profile_id": plan.model_provider_profile.profile_id,
        "protocol_artifact_hashes": protocol_hashes,
        "created_at": candidate_set.frozen_at.isoformat().replace("+00:00", "Z"),
    }
    prepared = PreparedProspectiveTriageWork(
        active_batch_id=f"prospective-triage-active-batch-{canonical_hash(active_core)}",
        readiness_report_id=candidate_set.readiness_report_id,
        unclassified_candidate_count=len(candidate_set.version_ids),
        selection=cast(
            Any,
            SimpleNamespace(
                selection_id="event-impact-triage-batch-selection-fixture",
                selected_at=candidate_set.frozen_at,
                selected_version_ids=candidate_set.version_ids,
            ),
        ),
        snapshot=cast(Any, SimpleNamespace(snapshot_id=candidate_set.data_snapshot_id)),
        candidate_set=candidate_set,
        manifest=manifest,
        plan=plan,
        profile=plan.model_provider_profile,
        protocol_artifact_hashes=protocol_hashes,
    )
    active_store = ProspectiveTriageActiveBatchStore(run_root)
    active_store.install(prepared, expected_epoch_revision=0)
    lookup = {
        "registration_id": candidate_set.registration_id,
        "checkpoint_key": candidate_set.checkpoint_key,
        "route_plan_id": candidate_set.route_plan_id,
        "route_admission_id": candidate_set.route_admission_id,
    }

    def build_runner(**kwargs: object) -> EventImpactTriageWorkRunner:
        runner.provider = cast(ModelProvider, kwargs["provider"])
        return runner

    monkeypatch.setattr(prospective_triage, "_build_prospective_triage_runner", build_runner)
    provider = FailOnceAvailabilityProvider()

    first = asyncio.run(
        run_prepared_prospective_triage_work(
            prepared=prepared,
            registration=registration,
            state_root=state_root,
            run_root=run_root,
            skill_root=ROOT / "skills",
            provider=provider,
        )
    )

    assert first["status"] == RunStatus.HUMAN_INPUT_REQUIRED.value
    assert provider.availability_calls == 1
    assert provider.requests == []
    assert active_store.active(**lookup) is not None

    recovered = asyncio.run(
        run_prepared_prospective_triage_work(
            prepared=prepared,
            registration=registration,
            state_root=state_root,
            run_root=run_root,
            skill_root=ROOT / "skills",
            provider=provider,
        )
    )

    assert recovered["status"] == RunStatus.COMPLETED.value
    assert provider.availability_calls == 4
    assert active_store.active(**lookup) is None


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


def test_v5_classify_contract_uses_harness_bound_evidence_ordinals(
    tmp_path: Path,
) -> None:
    runner, provider, candidate_set, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=3,
        dialect="v5",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V5
    assert result.proposal is not None
    assert {
        version_id
        for cluster in result.proposal.clusters
        for version_id in cluster.evidence_version_ids
    } <= set(candidate_set.version_ids)
    classify_tasks = [
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    ]
    assert classify_tasks
    for task in classify_tasks:
        contract = cast(dict[str, object], task["required_output"])
        assert contract["contract_version"] == "v5"
        assert contract["candidate_version_ids_injected_by_harness"] is True
        required = cast(list[str], contract["required_fields"])
        assert "evidence_ordinals" in required
        assert "evidence_version_ids" not in required
        fields = cast(dict[str, dict[str, object]], contract["field_schemas"])
        assert fields["evidence_ordinals"] == {
            "type": "array",
            "min_items": 1,
            "unique_items": True,
            "order": "strictly increasing",
            "items": {
                "type": "integer",
                "minimum": 0,
                "maximum": "phase_input.partition_cluster.candidate_version_ids length minus one",
            },
        }
    assert event_impact_triage_work_execution_plan_from_dict(plan.to_dict()) == plan


@pytest.mark.parametrize(
    ("ordinals", "message"),
    [
        ([], "at least one evidence ordinal"),
        ([0, 0], "strictly increasing"),
        ([999], "outside the frozen event cluster"),
        ([True], "non-boolean integers"),
    ],
)
def test_v5_classify_parser_rejects_invalid_evidence_ordinals(
    tmp_path: Path, ordinals: list[object], message: str
) -> None:
    runner, provider, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        dialect="v5",
    )
    result = asyncio.run(runner.run())
    assert result.partition is not None
    task = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    )
    output = provider._output(task)
    output["evidence_ordinals"] = ordinals

    with pytest.raises((TypeError, ValueError), match=message):
        runner._parse_cluster_proposal(output, result.partition.clusters[0])


def test_v5_invalid_evidence_ordinal_gets_actionable_correction(tmp_path: Path) -> None:
    provider = InvalidV5EvidenceOnceProvider()
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v5",
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
    assert correction["output_contract_version"] == "v5"
    assert correction["validation_error"] == "invalid_evidence_ordinal_coverage_or_order"
    assert "strictly increasing evidence_ordinals" in correction["instruction"]


def test_v6_classify_contract_declares_route_invariants(tmp_path: Path) -> None:
    runner, provider, _, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=3,
        dialect="v6",
    )
    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V6
    task = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    )
    contract = cast(dict[str, object], task["required_output"])
    assert contract["contract_version"] == "v6"
    assert contract["candidate_version_ids_injected_by_harness"] is True
    fields = cast(dict[str, dict[str, object]], contract["field_schemas"])
    assert fields["rule_reasons"]["min_items"] == 1
    requirements = cast(list[dict[str, object]], contract["conditional_requirements"])
    assert requirements == [
        {
            "if": {"checkpoint_eligibility": {"const": "eligible"}},
            "then": {
                "recommended_route": {"const": "checkpoint_candidate"},
                "changed_facts": {"min_items": 1},
                "event_archetypes": {"min_items": 1},
            },
        },
        {
            "if": {"checkpoint_eligibility": {"const": "ineligible"}},
            "then": {"recommended_route": {"not_const": "checkpoint_candidate"}},
        },
        {
            "if": {"checkpoint_eligibility": {"const": "needs_review"}},
            "then": {
                "recommended_route": {"enum": ["event_assessment", "attention_watch"]},
                "uncertainty_notes": {"min_items": 1},
            },
        },
        {
            "if": {"recommended_route": {"const": "event_assessment"}},
            "then": {
                "changed_facts": {"min_items": 1},
                "event_archetypes": {"min_items": 1},
                "transmission_channels": {"min_items": 1},
            },
        },
        {
            "if": {"recommended_route": {"const": "attention_watch"}},
            "then": {
                "changed_facts": {"min_items": 1},
                "watch_questions": {"min_items": 1},
            },
        },
    ]


def test_v6_route_violation_gets_actionable_correction(tmp_path: Path) -> None:
    provider = InvalidV6RouteOnceProvider()
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v6",
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
    assert correction["output_contract_version"] == "v6"
    assert correction["validation_error"] == (
        "event_assessment_requires_fact_archetype_and_transmission"
    )
    assert "conditional route requirements" in correction["instruction"]


def test_v7_direct_json_repair_is_bounded_and_reopens_authoritatively(
    tmp_path: Path,
) -> None:
    provider = OneExtraBracketProvider()
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v7",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V7
    assert result.proposal is not None
    assert result.partition is not None
    assert result.run_evidence is not None
    first = next(item for item in result.members if item.phase is TriageWorkPhase.CLASSIFY)
    assert first.metrics.turns == 1
    terminal = cast(
        dict[str, object], runner.artifact_store.read_json(first.terminal_artifact_hash)
    )
    evidence_hash = cast(str, terminal["json_parse_evidence_hash"])
    evidence = cast(dict[str, object], runner.artifact_store.read_json(evidence_hash))
    assert evidence["parser_id"] == "json-repair-0.63.4"
    assert evidence["repair_applied"] is True
    assert cast(list[dict[str, object]], evidence["structural_edits"])[0]["operation"] == "delete"
    assert cast(list[dict[str, object]], evidence["structural_edits"])[0]["token"] == "]"
    runner.assert_authoritative_completed_work_run(
        candidate_set=candidate_set,
        work_manifest=manifest,
        digests=result.digests,
        partition=result.partition,
        proposal=result.proposal,
        run_evidence=result.run_evidence,
    )


def test_v8_material_stage_one_omits_and_derives_checkpoint_eligibility(
    tmp_path: Path,
) -> None:
    provider = MaterialWatchProvider()
    registration = _material_registration()
    runner, _, _, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v8",
        registration=registration,
        checkpoint_key="next-material-a-share-event",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V8
    assert plan.classify_binding is not None
    assert plan.classify_binding.prompt_template_id.endswith("-json-v8m")
    assert result.proposal is not None
    assert all(
        item.checkpoint_eligibility.value == "needs_review"
        and item.recommended_route.value == "attention_watch"
        for item in result.proposal.clusters
    )
    classify_request = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    )
    contract = cast(dict[str, object], classify_request["required_output"])
    assert "checkpoint_eligibility" not in cast(list[str], contract["required_fields"])
    assert "checkpoint_eligibility" not in cast(dict[str, object], contract["field_schemas"])
    assert contract["checkpoint_eligibility"] == ("derived_by_harness_after_route_selection")
    route = cast(
        dict[str, object],
        cast(dict[str, object], contract["field_schemas"])["recommended_route"],
    )
    assert "checkpoint_candidate" not in cast(list[str], route["enum"])
    checkpoint_rule = cast(
        dict[str, object],
        cast(dict[str, object], classify_request["phase_input"])["checkpoint_rule"],
    )
    assert checkpoint_rule["stage_one_authority"] == (
        "harness_derives_provisional_status;final_eligibility_requires_materiality_gate"
    )


def test_v8_direct_checkpoint_keeps_model_eligibility_classification(tmp_path: Path) -> None:
    runner, provider, _, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        dialect="v8",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.classify_binding is not None
    assert plan.classify_binding.prompt_template_id.endswith("-json-v8")
    classify_request = next(
        json.loads(str(request[-1]["content"]))
        for request in provider.requests
        if json.loads(str(request[-1]["content"]))["phase"] == TriageWorkPhase.CLASSIFY.value
    )
    contract = cast(dict[str, object], classify_request["required_output"])
    assert "checkpoint_eligibility" in cast(list[str], contract["required_fields"])


def test_v12_bounds_concurrency_and_preserves_phase_barriers(tmp_path: Path) -> None:
    provider = ConcurrencyTrackingProvider()
    runner, _, _, _, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=25,
        provider=provider,
        dialect="v12",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V12
    assert plan.max_concurrent_model_requests == 3
    assert event_impact_triage_work_execution_plan_from_dict(plan.to_dict()) == plan
    invalid_plan = plan.to_dict()
    invalid_plan["max_concurrent_model_requests"] = 9
    with pytest.raises(ValueError, match="concurrent model request ceiling"):
        event_impact_triage_work_execution_plan_from_dict(invalid_plan)
    assert provider.peak == 3
    first_partition = provider.timeline.index(("start", TriageWorkPhase.PARTITION.value))
    last_map = max(
        index
        for index, event in enumerate(provider.timeline)
        if event == ("end", TriageWorkPhase.MAP.value)
    )
    first_classify = provider.timeline.index(("start", TriageWorkPhase.CLASSIFY.value))
    last_partition = max(
        index
        for index, event in enumerate(provider.timeline)
        if event == ("end", TriageWorkPhase.PARTITION.value)
    )
    assert last_map < first_partition < last_partition < first_classify
    replay = asyncio.run(runner.run())
    assert replay == result


def test_v13_material_ingress_uses_the_same_frozen_concurrency_ceiling(
    tmp_path: Path,
) -> None:
    provider = ConcurrencyTrackingProvider()
    registration = _material_registration()
    runner, _, _, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=25,
        provider=provider,
        dialect="v13",
        registration=registration,
        checkpoint_key="next-material-a-share-event",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V13
    assert plan.max_concurrent_model_requests == 3
    assert plan.max_total_runs == len(manifest.work_units) == 3
    assert provider.peak == 3


def test_v12_waits_for_started_peers_and_does_not_cross_a_failed_phase(
    tmp_path: Path,
) -> None:
    provider = OneConcurrentTerminalFailureProvider()
    runner, _, _, manifest, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=37,
        provider=provider,
        dialect="v12",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.FAILED
    assert len(manifest.work_units) > 3
    assert len(result.members) == 3
    assert sum(item.status is RunStatus.FAILED for item in result.members) == 1
    assert sum(item.status is RunStatus.COMPLETED for item in result.members) == 2
    assert provider.active == 0
    assert all(phase == TriageWorkPhase.MAP.value for _, phase in provider.timeline)
    assert len(runner.usage_ledger.records()) == 3
    requests = len(provider.requests)
    assert asyncio.run(runner.run()) == result
    assert len(provider.requests) == requests
    assert len(runner.usage_ledger.records()) == 3


def test_v12_seals_successful_peer_usage_before_raising_persistent_exception(
    tmp_path: Path,
) -> None:
    provider = ConcurrencyTrackingProvider()
    runner, _, _, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=37,
        provider=provider,
        dialect="v12",
    )
    binding = plan.map_bindings[0]
    runner.journal.start_run(
        run_id=runner._member_run_id(binding, manifest.work_units[0].work_unit_id),
        config_hash="f" * 64,
        created_at=NOW,
    )

    with pytest.raises(ValueError, match="another execution binding"):
        asyncio.run(runner.run())
    first_usage = runner.usage_ledger.records()
    assert len(first_usage) == 2
    assert all(item.record.status is RunStatus.COMPLETED for item in first_usage)
    assert len(provider.requests) == 2
    with pytest.raises(ValueError, match="another execution binding"):
        asyncio.run(runner.run())
    assert runner.usage_ledger.records() == first_usage
    assert len(provider.requests) == 2


def test_v12_ambiguity_takes_precedence_over_other_concurrent_failures(tmp_path: Path) -> None:
    provider = OneConcurrentTerminalFailureProvider(
        (ProviderGenerationState.RESPONSE_RECEIVED, ProviderGenerationState.UNKNOWN)
    )
    runner, _, _, _, _ = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=37,
        provider=provider,
        dialect="v12",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert [item.status for item in result.members] == [
        RunStatus.FAILED,
        RunStatus.HUMAN_INPUT_REQUIRED,
        RunStatus.COMPLETED,
    ]
    assert len(runner.usage_ledger.records()) == 3


@pytest.mark.parametrize(
    ("dialect", "expected_schema"),
    [
        ("v9", EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9),
        ("v10", EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V10),
        ("v11", EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11),
    ],
)
def test_material_ingress_uses_one_positional_call_and_derives_downstream_artifacts(
    tmp_path: Path, dialect: str, expected_schema: str
) -> None:
    provider = MixedMaterialIngressProvider()
    registration = _material_registration()
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.TREATMENT,
        count=3,
        provider=provider,
        dialect=dialect,
        registration=registration,
        checkpoint_key="next-material-a-share-event",
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert plan.schema_version == expected_schema
    assert plan.partition_binding is None
    assert plan.classify_binding is None
    assert plan.max_total_runs == len(manifest.work_units) == 1
    assert len(provider.requests) == 1
    if dialect == "v10":
        system_policy = str(provider.requests[0][0]["content"])
        assert "supplied content itself" in system_policy
        assert "Generic risk appetite" in system_policy
    if dialect == "v11":
        system_policy = str(provider.requests[0][0]["content"])
        assert "checkpoint rule constrains routing" in system_policy
        assert "Do not invent cross-market target links" in system_policy
    assert {item.phase for item in result.members} == {TriageWorkPhase.MAP}
    assert result.partition is not None
    assert result.proposal is not None
    assert result.run_evidence is not None
    assert len(result.partition.clusters) == len(manifest.atoms) == 3
    assert [item.recommended_route.value for item in result.proposal.clusters].count(
        "event_assessment"
    ) == 1
    assert [item.recommended_route.value for item in result.proposal.clusters].count(
        "attention_watch"
    ) == 1
    assert [item.recommended_route.value for item in result.proposal.clusters].count("archive") == 1
    assert all(item.triage_confidence == 0.0 for item in result.proposal.clusters)
    request = json.loads(str(provider.requests[0][-1]["content"]))
    assert request["required_output"]["contract_version"] == dialect
    phase_input = cast(dict[str, object], request["phase_input"])
    expected_input_fields = {"work_unit_ordinal", "atoms"}
    if dialect == "v11":
        expected_input_fields.add("checkpoint_rule")
    assert set(phase_input) == expected_input_fields
    assert all(
        set(item) == {"normalized_payload", "license_scope", "instruction_boundary"}
        for item in cast(list[dict[str, object]], phase_input["atoms"])
    )
    assert "atom_id" not in canonical_json_bytes(request["required_output"]).decode()
    if dialect == "v11":
        checkpoint_rule = cast(dict[str, object], phase_input["checkpoint_rule"])
        assert checkpoint_rule == {
            "eligibility_rule": registration.checkpoint(
                "next-material-a-share-event"
            ).eligibility_rule,
            "exclusion_rules": list(
                registration.checkpoint("next-material-a-share-event").exclusion_rules
            ),
            "target_venues": list(
                registration.checkpoint("next-material-a-share-event").target_venues
            ),
            "allowed_instrument_classes": list(
                registration.checkpoint("next-material-a-share-event").allowed_instrument_classes
            ),
        }
        output_contract = cast(dict[str, object], request["required_output"])
        assert output_contract["required_fields"] == ["routes"]
    assert event_impact_triage_work_execution_plan_from_dict(plan.to_dict()) == plan
    assert not validate_agent_contract(
        plan.to_dict(), "event-impact-triage-work-execution-plan.schema.json"
    )
    runner.assert_authoritative_completed_work_run(
        candidate_set=candidate_set,
        work_manifest=manifest,
        digests=result.digests,
        partition=result.partition,
        proposal=result.proposal,
        run_evidence=result.run_evidence,
    )

    restarted = asyncio.run(runner.run())

    assert restarted == result
    assert len(provider.requests) == 1


def test_v6_exhausted_format_failure_recovers_once_without_provider_call(
    tmp_path: Path,
) -> None:
    provider = AlwaysExtraBracketProvider()
    recovery_store = EventImpactTriageWorkFormatRecoveryStore(tmp_path / "format-recovery.sqlite3")
    runner, _, candidate_set, manifest, plan = _runtime(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
        dialect="v6",
        format_recovery_store=recovery_store,
    )

    blocked = asyncio.run(runner.run())

    assert blocked.status is RunStatus.FAILED
    original = blocked.members[-1]
    assert original.phase is TriageWorkPhase.CLASSIFY
    assert plan.classify_binding is not None
    assert original.metrics.turns == plan.classify_binding.max_turns
    original_record = runner.journal.get_run(original.run_id)
    original_events = runner.journal.events(original.run_id)
    original_usage = tuple(runner.usage_ledger.records())
    provider_request_count = len(provider.requests)

    grant = runner.authorize_format_recovery(
        original_run_id=original.run_id,
        authorized_at=NOW + timedelta(minutes=1),
    )
    assert (
        runner.authorize_format_recovery(
            original_run_id=original.run_id,
            authorized_at=NOW + timedelta(minutes=2),
        )
        == grant
    )

    recovered_runner = EventImpactTriageWorkRunner(
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
        format_recovery_store=recovery_store,
        clock=lambda: NOW + timedelta(minutes=3),
    )
    result = asyncio.run(recovered_runner.run())

    assert result.status is RunStatus.COMPLETED
    assert result.partition is not None
    assert result.proposal is not None
    assert result.run_evidence is not None
    recovered = next(
        member
        for member in result.members
        if member.phase is original.phase and member.unit_id == original.unit_id
    )
    assert recovered.run_id == grant.recovery_run_id
    assert recovered.metrics == original.metrics
    assert len(provider.requests) == provider_request_count
    assert recovered_runner.journal.get_run(original.run_id) == original_record
    assert recovered_runner.journal.events(original.run_id) == original_events
    assert tuple(recovered_runner.usage_ledger.records()) == original_usage
    assert all(item.record.run_id != grant.recovery_run_id for item in original_usage)
    assert (
        sum(
            item.record.status is RunStatus.FAILED
            for item in recovered_runner.usage_ledger.records()
        )
        == 1
    )

    reopened = asyncio.run(recovered_runner.run())
    assert reopened == result
    assert len(provider.requests) == provider_request_count


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
