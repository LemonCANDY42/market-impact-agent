from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import SkillRegistry
from market_impact_agent.agent_study import (
    AgentPhase2Preregistration,
    ExposureRegistry,
)
from market_impact_agent.domain import require_aware

RESEARCH_METHOD_CATALOG_SCHEMA = "market-impact.research-method-catalog.v1"
METHOD_ABLATION_REGISTRATION_SCHEMA = "market-impact.method-ablation-registration.v1"


class MethodLayer(StrEnum):
    EVIDENCE = "evidence"
    EVENT_CONTEXT = "event_context"
    EQUITY_EXPOSURE = "equity_exposure"
    ADVERSARIAL_RISK = "adversarial_risk"
    PATTERN = "pattern"
    FAMILY = "family"


class MethodArm(StrEnum):
    NEUTRAL_EVIDENCE = "neutral_evidence"
    GENERAL_METHODS = "general_methods"
    GENERAL_PATTERN = "general_pattern"
    FAMILY_GUIDED = "family_guided"


@dataclass(frozen=True, slots=True)
class ResearchMethod:
    skill_name: str
    layer: MethodLayer
    asset_classes: tuple[str, ...]
    mechanism_families: tuple[str, ...]
    requires_pattern_pack: bool
    priority: int

    def __post_init__(self) -> None:
        _identifier(self.skill_name, "research method skill_name")
        _unique_identifiers(self.asset_classes, "research method asset_classes")
        _unique_identifiers(self.mechanism_families, "research method mechanism_families")
        if self.priority < 1:
            raise ValueError("research method priority must be positive")
        if self.layer is MethodLayer.FAMILY and not self.mechanism_families:
            raise ValueError("family methods require at least one mechanism family")

    def to_dict(self) -> dict[str, object]:
        return {
            "skill_name": self.skill_name,
            "layer": self.layer.value,
            "asset_classes": list(self.asset_classes),
            "mechanism_families": list(self.mechanism_families),
            "requires_pattern_pack": self.requires_pattern_pack,
            "priority": self.priority,
        }


@dataclass(frozen=True, slots=True)
class ResearchMethodCatalog:
    catalog_id: str
    version: str
    methods: tuple[ResearchMethod, ...]

    def __post_init__(self) -> None:
        _nonempty(self.version, "research method catalog version")
        if not self.methods:
            raise ValueError("research method catalog cannot be empty")
        names = tuple(item.skill_name for item in self.methods)
        if len(names) != len(set(names)):
            raise ValueError("research method catalog skill names must be unique")
        priorities = tuple(item.priority for item in self.methods)
        if len(priorities) != len(set(priorities)):
            raise ValueError("research method catalog priorities must be unique")
        if self.catalog_id != self.expected_catalog_id:
            raise ValueError("research method catalog_id does not match content")

    @property
    def catalog_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_catalog_id(self) -> str:
        return f"research-method-catalog-{self.catalog_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": RESEARCH_METHOD_CATALOG_SCHEMA,
            "version": self.version,
            "methods": [item.to_dict() for item in self.methods],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "catalog_id": self.catalog_id}


@dataclass(frozen=True, slots=True)
class ResearchContext:
    mechanism_family: str
    asset_class: str
    has_pattern_pack: bool

    def __post_init__(self) -> None:
        _identifier(self.mechanism_family, "research context mechanism_family")
        _identifier(self.asset_class, "research context asset_class")


@dataclass(frozen=True, slots=True)
class SkillRoute:
    route_id: str
    arm: MethodArm
    context: ResearchContext
    requested_skills: tuple[str, ...]
    loaded_skills: tuple[str, ...]
    manifest_hashes: tuple[str, ...]
    allowed_capabilities: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _unique_identifiers(self.requested_skills, "route requested_skills")
        _unique_identifiers(self.loaded_skills, "route loaded_skills")
        if len(self.manifest_hashes) != len(self.loaded_skills):
            raise ValueError("route manifest hashes must match loaded skills")
        _unique_nonempty(self.allowed_capabilities, "route allowed_capabilities")
        _unique_identifiers(self.allowed_tools, "route allowed_tools")
        _unique_nonempty(self.reasons, "route reasons")
        if self.route_id != self.expected_route_id:
            raise ValueError("Skill Route route_id does not match content")

    @property
    def route_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_route_id(self) -> str:
        return f"skill-route-{self.route_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.skill-route.v1",
            "arm": self.arm.value,
            "context": {
                "mechanism_family": self.context.mechanism_family,
                "asset_class": self.context.asset_class,
                "has_pattern_pack": self.context.has_pattern_pack,
            },
            "requested_skills": list(self.requested_skills),
            "loaded_skills": list(self.loaded_skills),
            "manifest_hashes": list(self.manifest_hashes),
            "allowed_capabilities": list(self.allowed_capabilities),
            "allowed_tools": list(self.allowed_tools),
            "reasons": list(self.reasons),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "route_id": self.route_id}


class ResearchMethodRouter:
    def __init__(self, *, catalog: ResearchMethodCatalog, skills: SkillRegistry) -> None:
        self.catalog = catalog
        self.skills = skills

    def route(self, *, arm: MethodArm, context: ResearchContext) -> SkillRoute:
        allowed_layers = {
            MethodArm.NEUTRAL_EVIDENCE: {MethodLayer.EVIDENCE},
            MethodArm.GENERAL_METHODS: {
                MethodLayer.EVIDENCE,
                MethodLayer.EVENT_CONTEXT,
                MethodLayer.EQUITY_EXPOSURE,
                MethodLayer.ADVERSARIAL_RISK,
            },
            MethodArm.GENERAL_PATTERN: {
                MethodLayer.EVIDENCE,
                MethodLayer.EVENT_CONTEXT,
                MethodLayer.EQUITY_EXPOSURE,
                MethodLayer.ADVERSARIAL_RISK,
                MethodLayer.PATTERN,
            },
            MethodArm.FAMILY_GUIDED: set(MethodLayer),
        }[arm]
        if MethodLayer.PATTERN in allowed_layers and not context.has_pattern_pack:
            raise ValueError(f"{arm.value} requires a frozen Pattern Pack")
        applicable = tuple(
            method
            for method in sorted(self.catalog.methods, key=lambda item: item.priority)
            if method.layer in allowed_layers
            and (not method.asset_classes or context.asset_class in method.asset_classes)
            and (
                not method.mechanism_families
                or context.mechanism_family in method.mechanism_families
            )
            and (not method.requires_pattern_pack or context.has_pattern_pack)
        )
        if not applicable or applicable[0].layer is not MethodLayer.EVIDENCE:
            raise ValueError("every route must start with a neutral evidence method")
        if arm is MethodArm.FAMILY_GUIDED and not any(
            item.layer is MethodLayer.FAMILY for item in applicable
        ):
            raise ValueError("family-guided arm has no applicable family method")
        requested = tuple(item.skill_name for item in applicable)
        capabilities = frozenset(
            {"evidence.read", "pattern.read"} if context.has_pattern_pack else {"evidence.read"}
        )
        loaded = self.skills.load(requested, allowed_capabilities=capabilities)
        allowed_tools = tuple(
            sorted({tool for item in loaded for tool in item.manifest.allowed_tools})
        )
        selected_capabilities = tuple(
            sorted({cap for item in loaded for cap in item.manifest.required_capabilities})
        )
        reasons = (
            f"arm:{arm.value}",
            f"asset_class:{context.asset_class}",
            f"mechanism_family:{context.mechanism_family}",
            f"pattern_pack:{str(context.has_pattern_pack).lower()}",
        )
        core = {
            "schema_version": "market-impact.skill-route.v1",
            "arm": arm.value,
            "context": {
                "mechanism_family": context.mechanism_family,
                "asset_class": context.asset_class,
                "has_pattern_pack": context.has_pattern_pack,
            },
            "requested_skills": list(requested),
            "loaded_skills": [item.manifest.name for item in loaded],
            "manifest_hashes": [item.manifest.manifest_hash for item in loaded],
            "allowed_capabilities": list(selected_capabilities),
            "allowed_tools": list(allowed_tools),
            "reasons": list(reasons),
        }
        return SkillRoute(
            route_id=f"skill-route-{canonical_hash(core)}",
            arm=arm,
            context=context,
            requested_skills=requested,
            loaded_skills=tuple(item.manifest.name for item in loaded),
            manifest_hashes=tuple(item.manifest.manifest_hash for item in loaded),
            allowed_capabilities=selected_capabilities,
            allowed_tools=allowed_tools,
            reasons=reasons,
        )


@dataclass(frozen=True, slots=True)
class MethodArmSpec:
    arm: MethodArm
    route_id: str
    route_hash: str
    requested_skills: tuple[str, ...]
    allowed_capabilities: tuple[str, ...]
    allowed_tools: tuple[str, ...]

    def __post_init__(self) -> None:
        _sha256(self.route_hash, "method arm route_hash")
        if self.route_id != f"skill-route-{self.route_hash}":
            raise ValueError("method arm route identity is inconsistent")
        _unique_identifiers(self.requested_skills, "method arm requested_skills")
        _unique_nonempty(self.allowed_capabilities, "method arm allowed_capabilities")
        _unique_identifiers(self.allowed_tools, "method arm allowed_tools")

    @classmethod
    def from_route(cls, route: SkillRoute) -> MethodArmSpec:
        return cls(
            arm=route.arm,
            route_id=route.route_id,
            route_hash=route.route_hash,
            requested_skills=route.requested_skills,
            allowed_capabilities=route.allowed_capabilities,
            allowed_tools=route.allowed_tools,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "arm": self.arm.value,
            "route_id": self.route_id,
            "route_hash": self.route_hash,
            "requested_skills": list(self.requested_skills),
            "allowed_capabilities": list(self.allowed_capabilities),
            "allowed_tools": list(self.allowed_tools),
        }


@dataclass(frozen=True, slots=True)
class MethodReplicateProtocol:
    provider_id: str
    model: str
    runtime_ref: str
    assessment_delay_minutes: int
    evidence_cutoff_policy: str
    input_availability_policy: str
    entry_policy: str
    replicate_count: int
    minimum_agreeing_replicates: int
    cross_replicate_memory: bool
    selected_skills: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    allowed_directions: tuple[str, ...]
    eligible_horizons_sessions: tuple[int, ...]
    agreement_fields: tuple[str, ...]
    minimum_candidate_confidence: Decimal
    no_agreement_action: str
    invalid_replicate_action: str
    execution_binding_policy: str

    def __post_init__(self) -> None:
        for name in ("provider_id", "model", "runtime_ref"):
            _nonempty(cast(str, getattr(self, name)), name)
        if self.assessment_delay_minutes != 60:
            raise ValueError("method replicate assessment delay must remain 60 minutes")
        if self.evidence_cutoff_policy != "first_qualifying_visibility_plus_delay":
            raise ValueError("method replicate evidence cutoff policy is invalid")
        if self.input_availability_policy != "available_at_or_before_evidence_cutoff":
            raise ValueError("method replicate input availability policy is invalid")
        if self.entry_policy != "first_executable_xshg_open_strictly_after_evidence_cutoff":
            raise ValueError("method replicate entry policy is invalid")
        if self.replicate_count != 5 or self.minimum_agreeing_replicates != 3:
            raise ValueError("method replicate protocol requires three-of-five agreement")
        if self.cross_replicate_memory:
            raise ValueError("method replicates must not share memory")
        _unique_identifiers(self.selected_skills, "method replicate selected_skills")
        _unique_identifiers(self.allowed_tools, "method replicate allowed_tools")
        if self.allowed_directions != ("up",):
            raise ValueError("current A-share method ablation is long-or-abstain only")
        if self.eligible_horizons_sessions != (1, 3, 10):
            raise ValueError("method replicate horizons do not match the frozen study")
        if self.agreement_fields != ("target_id", "direction", "horizon_sessions"):
            raise ValueError("method replicate agreement fields are invalid")
        if self.minimum_candidate_confidence != Decimal("0.5"):
            raise ValueError("method replicate minimum confidence must remain 0.5")
        if self.no_agreement_action != "abstain" or self.invalid_replicate_action != "abstain":
            raise ValueError("invalid method replicates and disagreements must abstain")
        if self.execution_binding_policy != "exact_hashes_before_first_replicate":
            raise ValueError("method replicate execution binding policy is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "model": self.model,
            "runtime_ref": self.runtime_ref,
            "assessment_delay_minutes": self.assessment_delay_minutes,
            "evidence_cutoff_policy": self.evidence_cutoff_policy,
            "input_availability_policy": self.input_availability_policy,
            "entry_policy": self.entry_policy,
            "replicate_count": self.replicate_count,
            "minimum_agreeing_replicates": self.minimum_agreeing_replicates,
            "cross_replicate_memory": self.cross_replicate_memory,
            "selected_skills": list(self.selected_skills),
            "allowed_tools": list(self.allowed_tools),
            "allowed_directions": list(self.allowed_directions),
            "eligible_horizons_sessions": list(self.eligible_horizons_sessions),
            "agreement_fields": list(self.agreement_fields),
            "minimum_candidate_confidence": str(self.minimum_candidate_confidence),
            "no_agreement_action": self.no_agreement_action,
            "invalid_replicate_action": self.invalid_replicate_action,
            "execution_binding_policy": self.execution_binding_policy,
        }


@dataclass(frozen=True, slots=True)
class MethodAblationRegistration:
    registration_id: str
    registered_at: datetime
    parent_registration_id: str
    parent_registration_hash: str
    provider_profile_id: str
    provider_profile_hash: str
    method_catalog_id: str
    method_catalog_hash: str
    arms: tuple[MethodArmSpec, ...]
    replicate_count: int
    minimum_agreement: int
    run_order: str
    common_inputs: tuple[str, ...]
    all_event_denominator: bool
    outcomes_opened: bool
    execution_capability: str

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "method ablation registered_at")
        for name in (
            "parent_registration_hash",
            "provider_profile_hash",
            "method_catalog_hash",
        ):
            _sha256(cast(str, getattr(self, name)), name)
        expected_arms = tuple(MethodArm)
        if tuple(item.arm for item in self.arms) != expected_arms:
            raise ValueError("method ablation must preserve the four canonical arms")
        if self.replicate_count != 5 or self.minimum_agreement != 3:
            raise ValueError("method ablation requires three-of-five agreement")
        if self.run_order != "interleaved_by_replicate_then_arm":
            raise ValueError("method ablation run order is not frozen")
        required_common = {
            "action_space",
            "evidence_cutoff",
            "evidence_pack",
            "model_provider_profile",
            "output_contract",
            "replicate_budget",
            "target_universe",
        }
        if set(self.common_inputs) != required_common or len(self.common_inputs) != len(
            required_common
        ):
            raise ValueError("method ablation common inputs are incomplete")
        if not self.all_event_denominator:
            raise ValueError("method ablation must use the all-event denominator")
        if self.outcomes_opened:
            raise ValueError("method ablation cannot register opened outcomes")
        if self.execution_capability != "none":
            raise ValueError("method ablation grants no execution capability")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("method ablation registration_id does not match content")

    @property
    def registration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_registration_id(self) -> str:
        return f"method-ablation-{self.registration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": METHOD_ABLATION_REGISTRATION_SCHEMA,
            "registered_at": _timestamp(self.registered_at),
            "parent_registration_id": self.parent_registration_id,
            "parent_registration_hash": self.parent_registration_hash,
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "method_catalog_id": self.method_catalog_id,
            "method_catalog_hash": self.method_catalog_hash,
            "arms": [item.to_dict() for item in self.arms],
            "replicate_count": self.replicate_count,
            "minimum_agreement": self.minimum_agreement,
            "run_order": self.run_order,
            "common_inputs": list(self.common_inputs),
            "all_event_denominator": self.all_event_denominator,
            "outcomes_opened": self.outcomes_opened,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}

    def validate_against(
        self,
        *,
        parent: AgentPhase2Preregistration,
        registry: ExposureRegistry,
        catalog: ResearchMethodCatalog,
        provider_profile_id: str,
        provider_profile_hash: str,
    ) -> None:
        parent.validate_against(registry)
        if self.registered_at < parent.registered_at:
            raise ValueError("method ablation cannot predate its parent registration")
        if (
            self.parent_registration_id != parent.registration_id
            or self.parent_registration_hash != parent.registration_hash
        ):
            raise ValueError("method ablation does not match its parent registration")
        if (
            self.method_catalog_id != catalog.catalog_id
            or self.method_catalog_hash != catalog.catalog_hash
        ):
            raise ValueError("method ablation does not match its Research Method Catalog")
        if (
            self.provider_profile_id != provider_profile_id
            or self.provider_profile_hash != provider_profile_hash
        ):
            raise ValueError("method ablation does not match its Model Provider Profile")


@dataclass(frozen=True, slots=True)
class AblationArmStudy:
    ablation: MethodAblationRegistration
    parent: AgentPhase2Preregistration
    arm: MethodArmSpec

    @property
    def registration_hash(self) -> str:
        return canonical_hash(
            {
                "ablation_registration_hash": self.ablation.registration_hash,
                "arm": self.arm.to_dict(),
                "agent_protocol": self.agent_protocol.to_dict(),
            }
        )

    @property
    def registration_id(self) -> str:
        return f"agent-study-{self.registration_hash}"

    @property
    def agent_protocol(self) -> MethodReplicateProtocol:
        base = self.parent.agent_protocol
        return MethodReplicateProtocol(
            provider_id=base.provider_id,
            model=base.model,
            runtime_ref=base.runtime_ref,
            assessment_delay_minutes=base.assessment_delay_minutes,
            evidence_cutoff_policy=base.evidence_cutoff_policy,
            input_availability_policy=base.input_availability_policy,
            entry_policy=base.entry_policy,
            replicate_count=base.replicate_count,
            minimum_agreeing_replicates=base.minimum_agreeing_replicates,
            cross_replicate_memory=base.cross_replicate_memory,
            selected_skills=self.arm.requested_skills,
            allowed_tools=self.arm.allowed_tools,
            allowed_directions=base.allowed_directions,
            eligible_horizons_sessions=base.eligible_horizons_sessions,
            agreement_fields=base.agreement_fields,
            minimum_candidate_confidence=base.minimum_candidate_confidence,
            no_agreement_action=base.no_agreement_action,
            invalid_replicate_action=base.invalid_replicate_action,
            execution_binding_policy=base.execution_binding_policy,
        )

    def validate_against(self, registry: ExposureRegistry) -> None:
        self.parent.validate_against(registry)


def load_research_method_catalog(path: Path) -> ResearchMethodCatalog:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "Research Method Catalog")
    expected = {"schema_version", "catalog_id", "version", "methods"}
    _closed(payload, expected, "Research Method Catalog")
    if _string(payload, "schema_version") != RESEARCH_METHOD_CATALOG_SCHEMA:
        raise ValueError("unsupported Research Method Catalog schema_version")
    raw_methods = payload.get("methods")
    if not isinstance(raw_methods, list):
        raise TypeError("Research Method Catalog methods must be an array")
    methods = tuple(_method(item) for item in cast(list[object], raw_methods))
    result = ResearchMethodCatalog(
        catalog_id=_string(payload, "catalog_id"),
        version=_string(payload, "version"),
        methods=methods,
    )
    if result.to_dict() != payload:
        raise ValueError("Research Method Catalog does not match canonical contract")
    return result


def load_method_ablation_registration(path: Path) -> MethodAblationRegistration:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "Method Ablation Registration")
    expected = {
        "schema_version",
        "registration_id",
        "registered_at",
        "parent_registration_id",
        "parent_registration_hash",
        "provider_profile_id",
        "provider_profile_hash",
        "method_catalog_id",
        "method_catalog_hash",
        "arms",
        "replicate_count",
        "minimum_agreement",
        "run_order",
        "common_inputs",
        "all_event_denominator",
        "outcomes_opened",
        "execution_capability",
    }
    _closed(payload, expected, "Method Ablation Registration")
    if _string(payload, "schema_version") != METHOD_ABLATION_REGISTRATION_SCHEMA:
        raise ValueError("unsupported Method Ablation Registration schema_version")
    arms_raw = payload.get("arms")
    if not isinstance(arms_raw, list):
        raise TypeError("Method Ablation Registration arms must be an array")
    result = MethodAblationRegistration(
        registration_id=_string(payload, "registration_id"),
        registered_at=datetime.fromisoformat(
            _string(payload, "registered_at").replace("Z", "+00:00")
        ),
        parent_registration_id=_string(payload, "parent_registration_id"),
        parent_registration_hash=_string(payload, "parent_registration_hash"),
        provider_profile_id=_string(payload, "provider_profile_id"),
        provider_profile_hash=_string(payload, "provider_profile_hash"),
        method_catalog_id=_string(payload, "method_catalog_id"),
        method_catalog_hash=_string(payload, "method_catalog_hash"),
        arms=tuple(_arm(item) for item in cast(list[object], arms_raw)),
        replicate_count=_integer(payload, "replicate_count"),
        minimum_agreement=_integer(payload, "minimum_agreement"),
        run_order=_string(payload, "run_order"),
        common_inputs=_string_tuple(payload, "common_inputs"),
        all_event_denominator=_boolean(payload, "all_event_denominator"),
        outcomes_opened=_boolean(payload, "outcomes_opened"),
        execution_capability=_string(payload, "execution_capability"),
    )
    if result.to_dict() != payload:
        raise ValueError("Method Ablation Registration does not match canonical contract")
    return result


def build_arm_studies(
    *, ablation: MethodAblationRegistration, parent: AgentPhase2Preregistration
) -> tuple[AblationArmStudy, ...]:
    return tuple(
        AblationArmStudy(ablation=ablation, parent=parent, arm=arm) for arm in ablation.arms
    )


def _method(value: object) -> ResearchMethod:
    payload = _object(value, "research method")
    _closed(
        payload,
        {
            "skill_name",
            "layer",
            "asset_classes",
            "mechanism_families",
            "requires_pattern_pack",
            "priority",
        },
        "research method",
    )
    return ResearchMethod(
        skill_name=_string(payload, "skill_name"),
        layer=MethodLayer(_string(payload, "layer")),
        asset_classes=_string_tuple(payload, "asset_classes"),
        mechanism_families=_string_tuple(payload, "mechanism_families"),
        requires_pattern_pack=_boolean(payload, "requires_pattern_pack"),
        priority=_integer(payload, "priority"),
    )


def _arm(value: object) -> MethodArmSpec:
    payload = _object(value, "method arm")
    _closed(
        payload,
        {
            "arm",
            "route_id",
            "route_hash",
            "requested_skills",
            "allowed_capabilities",
            "allowed_tools",
        },
        "method arm",
    )
    return MethodArmSpec(
        arm=MethodArm(_string(payload, "arm")),
        route_id=_string(payload, "route_id"),
        route_hash=_string(payload, "route_hash"),
        requested_skills=_string_tuple(payload, "requested_skills"),
        allowed_capabilities=_string_tuple(payload, "allowed_capabilities"),
        allowed_tools=_string_tuple(payload, "allowed_tools"),
    )


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError(f"{label} must have string keys")
    return cast(dict[str, object], mapping)


def _closed(payload: dict[str, object], expected: set[str], label: str) -> None:
    if set(payload) != expected:
        raise ValueError(f"{label} fields are invalid")


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _string_tuple(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], value)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], items))


def _identifier(value: str, label: str) -> None:
    _nonempty(value, label)
    if any(character not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for character in value):
        raise ValueError(f"{label} must be a lowercase identifier")


def _unique_identifiers(values: tuple[str, ...], label: str) -> None:
    for value in values:
        _identifier(value, label)
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")


def _unique_nonempty(values: tuple[str, ...], label: str) -> None:
    for value in values:
        _nonempty(value, label)
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")


def _nonempty(value: str, label: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{label} must be a non-empty trimmed string")


def _sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
