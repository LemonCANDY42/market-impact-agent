from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import AgentExecutionBinding
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    load_builtin_model_provider_profile,
    model_provider_profile_from_dict,
)
from market_impact_agent.prospective_diagnostic import (
    REASSESSMENT_INITIAL,
    ProspectiveDiagnosticRegistration,
)

PROSPECTIVE_EXECUTION_PLAN_SCHEMA = "market-impact.prospective-execution-plan.v1"
PROSPECTIVE_EXECUTION_PLAN_SCHEMA_V2 = "market-impact.prospective-execution-plan.v2"


@dataclass(frozen=True, slots=True)
class PairedArmExecutionBinding:
    arm: str
    execution_binding: AgentExecutionBinding

    def to_dict(self) -> dict[str, object]:
        return {
            "arm": self.arm,
            "execution_binding": self.execution_binding.to_dict(),
            "execution_binding_hash": self.execution_binding.binding_hash,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveExecutionPlan:
    plan_id: str
    registration_id: str
    model_profile_alias: str
    model_provider_profile: ModelProviderProfile
    arm_bindings: tuple[PairedArmExecutionBinding, ...]
    schema_version: str = PROSPECTIVE_EXECUTION_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in {
            PROSPECTIVE_EXECUTION_PLAN_SCHEMA,
            PROSPECTIVE_EXECUTION_PLAN_SCHEMA_V2,
        }:
            raise ValueError("unsupported prospective execution plan schema")
        if not self.registration_id.startswith("prospective-diagnostic-registration-"):
            raise ValueError("prospective execution plan registration identity is invalid")
        if (
            not self.model_profile_alias
            or self.model_profile_alias != self.model_profile_alias.strip()
        ):
            raise ValueError("prospective execution plan Model Profile alias is invalid")
        arms = tuple(item.arm for item in self.arm_bindings)
        reassessment = self.schema_version == PROSPECTIVE_EXECUTION_PLAN_SCHEMA_V2
        if reassessment and arms != (REASSESSMENT_INITIAL,):
            raise ValueError("reassessment execution plan requires one initial binding")
        if not reassessment and arms != (
            "structured_agent_core",
            "structured_agent_plus_routed_methods",
        ):
            raise ValueError("prospective execution plan requires the frozen paired arms")
        binding_hashes = tuple(item.execution_binding.binding_hash for item in self.arm_bindings)
        if not reassessment and len(set(binding_hashes)) != 2:
            raise ValueError("paired arms require distinct frozen execution surfaces")
        expected_profile = load_builtin_model_provider_profile(self.model_profile_alias)
        if self.model_provider_profile.to_dict() != expected_profile.to_dict():
            raise ValueError("execution plan profile differs from the Harness-bundled alias")
        if reassessment:
            if self.plan_id != self.expected_plan_id:
                raise ValueError("prospective execution plan ID does not match content")
            return
        control = self.arm_bindings[0].execution_binding
        treatment = self.arm_bindings[1].execution_binding
        if (
            treatment.skill_hashes[: len(control.skill_hashes)] != control.skill_hashes
            or len(treatment.skill_hashes) <= len(control.skill_hashes)
            or treatment.runtime_ref != control.runtime_ref
            or treatment.runtime_config_hash != control.runtime_config_hash
            or treatment.tool_manifest_hashes != control.tool_manifest_hashes
            or treatment.tool_surface_hash != control.tool_surface_hash
            or treatment.mcp_server_hashes != control.mcp_server_hashes
            or treatment.context_estimator_id != control.context_estimator_id
            or treatment.compactor_id != control.compactor_id
        ):
            raise ValueError(
                "treatment execution surface must preserve the core surface and add routed methods"
            )
        if self.plan_id != self.expected_plan_id:
            raise ValueError("prospective execution plan ID does not match content")

    @property
    def expected_plan_id(self) -> str:
        return f"prospective-execution-plan-{canonical_hash(self.core_dict())}"

    @property
    def provider_id(self) -> str:
        return self.model_provider_profile.provider_id

    @property
    def model(self) -> str:
        return self.model_provider_profile.model

    def arm_binding(self, arm: str) -> AgentExecutionBinding:
        match = next(
            (item.execution_binding for item in self.arm_bindings if item.arm == arm),
            None,
        )
        if match is None:
            raise KeyError(f"arm is outside the prospective execution plan: {arm}")
        return match

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "model_profile_alias": self.model_profile_alias,
            "model_provider_profile": self.model_provider_profile.to_dict(),
            "arm_bindings": [item.to_dict() for item in self.arm_bindings],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}

    @classmethod
    def build(
        cls,
        *,
        registration: ProspectiveDiagnosticRegistration,
        model_profile_alias: str,
        model_profile: ModelProviderProfile,
        arm_bindings: tuple[PairedArmExecutionBinding, ...],
    ) -> ProspectiveExecutionPlan:
        if model_profile_alias != registration.model_profile_id:
            raise ValueError("execution plan Model Profile alias differs from registration")
        if tuple(item.arm for item in arm_bindings) != registration.paired_arms:
            raise ValueError("execution plan arms differ from registration")
        schema = (
            PROSPECTIVE_EXECUTION_PLAN_SCHEMA_V2
            if registration.reassessment is not None
            else PROSPECTIVE_EXECUTION_PLAN_SCHEMA
        )
        core = {
            "schema_version": schema,
            "registration_id": registration.registration_id,
            "model_profile_alias": model_profile_alias,
            "model_provider_profile": model_profile.to_dict(),
            "arm_bindings": [item.to_dict() for item in arm_bindings],
        }
        return cls(
            plan_id=f"prospective-execution-plan-{canonical_hash(core)}",
            registration_id=registration.registration_id,
            model_profile_alias=model_profile_alias,
            model_provider_profile=model_profile,
            arm_bindings=arm_bindings,
            schema_version=schema,
        )


def prospective_execution_plan_from_dict(value: object) -> ProspectiveExecutionPlan:
    if not isinstance(value, dict):
        raise TypeError("prospective execution plan must be an object")
    payload = cast(dict[object, object], value)
    arm_values = payload.get("arm_bindings")
    if not isinstance(arm_values, list):
        raise TypeError("prospective execution plan arm_bindings must be an array")
    arm_bindings: list[PairedArmExecutionBinding] = []
    for raw_arm_value in cast(list[object], arm_values):
        raw_arm = raw_arm_value
        if not isinstance(raw_arm, dict):
            raise TypeError("prospective execution plan arm binding must be an object")
        raw_arm = cast(dict[object, object], raw_arm)
        raw_binding = raw_arm.get("execution_binding")
        if not isinstance(raw_binding, dict):
            raise TypeError("prospective Agent execution binding must be an object")
        raw_binding = cast(dict[object, object], raw_binding)
        binding = AgentExecutionBinding(
            runtime_ref=_string(raw_binding, "runtime_ref"),
            runtime_config_hash=_string(raw_binding, "runtime_config_hash"),
            prompt_hash=_string(raw_binding, "prompt_hash"),
            skill_hashes=_string_tuple(raw_binding, "skill_hashes"),
            tool_manifest_hashes=_string_tuple(raw_binding, "tool_manifest_hashes"),
            tool_surface_hash=_string(raw_binding, "tool_surface_hash"),
            mcp_server_hashes=_string_tuple(raw_binding, "mcp_server_hashes"),
            context_estimator_id=_string(raw_binding, "context_estimator_id"),
            compactor_id=_string(raw_binding, "compactor_id"),
        )
        if raw_arm.get("execution_binding_hash") != binding.binding_hash:
            raise ValueError("prospective Agent execution binding hash is not exact")
        arm_bindings.append(
            PairedArmExecutionBinding(
                arm=_string(raw_arm, "arm"),
                execution_binding=binding,
            )
        )
    result = ProspectiveExecutionPlan(
        plan_id=_string(payload, "plan_id"),
        registration_id=_string(payload, "registration_id"),
        model_profile_alias=_string(payload, "model_profile_alias"),
        model_provider_profile=model_provider_profile_from_dict(
            payload.get("model_provider_profile")
        ),
        arm_bindings=tuple(arm_bindings),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("prospective execution plan does not match canonical contract")
    return result


def _string(payload: dict[object, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise TypeError(f"prospective execution plan {name} must be non-empty text")
    return value


def _string_tuple(payload: dict[object, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"prospective execution plan {name} must be a string array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"prospective execution plan {name} must be a string array")
    return tuple(cast(list[str], items))
