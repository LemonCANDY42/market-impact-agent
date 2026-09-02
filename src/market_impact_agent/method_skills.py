from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.model_provider import ModelProviderProfile

METHOD_SKILL_CATALOG_SCHEMA = "market-impact.method-skill-catalog.v1"
METHOD_EVIDENCE_DECLARATION_SCHEMA = "market-impact.method-evidence-declaration.v1"
PAIRED_SKILL_ABLATION_SCHEMA = "market-impact.method-skill-ablation.v2"
CPA_PRICING_SNAPSHOT_SCHEMA = "market-impact.cpa-pricing-snapshot.v1"

MARKET_STATES = frozenset({"up_fast", "up_mild", "down_fast", "down_mild", "unclassified"})
NARRATIVE_STATES = frozenset(
    {
        "corroborated_obvious",
        "authority_obvious",
        "diffuse",
        "contested",
        "unavailable",
    }
)


@dataclass(frozen=True, slots=True)
class MethodSkill:
    skill_name: str
    lineage: str
    source_refs: tuple[str, ...]
    applicable_market_states: tuple[str, ...]
    applicable_narratives: tuple[str, ...]
    analysis_needs: tuple[str, ...]
    required_evidence: tuple[str, ...]
    prohibited_uses: tuple[str, ...]
    priority: int

    def __post_init__(self) -> None:
        _identifier(self.skill_name, "method Skill name")
        _nonempty(self.lineage, "method Skill lineage")
        _unique(self.source_refs, "method Skill source_refs")
        if not self.source_refs or any(not _is_https_url(item) for item in self.source_refs):
            raise ValueError("method Skill source_refs must contain HTTPS URLs")
        _unique(self.applicable_market_states, "method Skill applicable_market_states")
        if not set(self.applicable_market_states) <= MARKET_STATES:
            raise ValueError("method Skill contains an unsupported market state")
        _unique(self.applicable_narratives, "method Skill applicable_narratives")
        if not set(self.applicable_narratives) <= NARRATIVE_STATES:
            raise ValueError("method Skill contains an unsupported narrative state")
        for name in ("analysis_needs", "required_evidence", "prohibited_uses"):
            values = cast(tuple[str, ...], getattr(self, name))
            _unique(values, f"method Skill {name}")
            if not values:
                raise ValueError(f"method Skill {name} cannot be empty")
        if self.priority < 1:
            raise ValueError("method Skill priority must be positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "skill_name": self.skill_name,
            "lineage": self.lineage,
            "source_refs": list(self.source_refs),
            "applicable_market_states": list(self.applicable_market_states),
            "applicable_narratives": list(self.applicable_narratives),
            "analysis_needs": list(self.analysis_needs),
            "required_evidence": list(self.required_evidence),
            "prohibited_uses": list(self.prohibited_uses),
            "priority": self.priority,
        }


@dataclass(frozen=True, slots=True)
class MethodSkillCatalog:
    catalog_id: str
    version: str
    methods: tuple[MethodSkill, ...]

    def __post_init__(self) -> None:
        _nonempty(self.version, "method Skill catalog version")
        if not self.methods:
            raise ValueError("method Skill catalog cannot be empty")
        names = tuple(item.skill_name for item in self.methods)
        priorities = tuple(item.priority for item in self.methods)
        _unique(names, "method Skill catalog names")
        if len(priorities) != len(set(priorities)):
            raise ValueError("method Skill catalog priorities must be unique")
        if self.catalog_id != self.expected_catalog_id:
            raise ValueError("method Skill catalog_id does not match content")

    @property
    def catalog_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_catalog_id(self) -> str:
        return f"method-skill-catalog-{self.catalog_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": METHOD_SKILL_CATALOG_SCHEMA,
            "version": self.version,
            "methods": [item.to_dict() for item in self.methods],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "catalog_id": self.catalog_id}


@dataclass(frozen=True, slots=True)
class MethodEvidenceBinding:
    evidence_type: str
    evidence_refs: tuple[str, ...]
    pattern_pack_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.evidence_type, "method evidence type")
        _unique(self.evidence_refs, "method evidence refs")
        _unique(self.pattern_pack_refs, "method Pattern Pack refs")
        if not self.evidence_refs and not self.pattern_pack_refs:
            raise ValueError("method evidence binding requires at least one frozen reference")

    def to_dict(self) -> dict[str, object]:
        return {
            "evidence_type": self.evidence_type,
            "evidence_refs": list(self.evidence_refs),
            "pattern_pack_refs": list(self.pattern_pack_refs),
        }


@dataclass(frozen=True, slots=True)
class MethodEvidenceDeclaration:
    declaration_id: str
    evidence_pack_id: str
    evidence_pack_hash: str
    bindings: tuple[MethodEvidenceBinding, ...]
    outcomes_opened: bool

    def __post_init__(self) -> None:
        _sha256(self.evidence_pack_hash, "method evidence pack hash")
        if not self.evidence_pack_id.startswith("evidence-pack-"):
            raise ValueError("method evidence declaration evidence_pack_id is invalid")
        _sha256(
            self.evidence_pack_id.removeprefix("evidence-pack-"),
            "method evidence pack id hash",
        )
        if not self.bindings:
            raise ValueError("method evidence declaration cannot be empty")
        _unique(
            tuple(item.evidence_type for item in self.bindings),
            "method evidence declaration types",
        )
        if self.declaration_id != self.expected_declaration_id:
            raise ValueError("method evidence declaration_id does not match content")

    @property
    def available_evidence(self) -> tuple[str, ...]:
        return tuple(item.evidence_type for item in self.bindings)

    @property
    def declaration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_declaration_id(self) -> str:
        return f"method-evidence-{self.declaration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": METHOD_EVIDENCE_DECLARATION_SCHEMA,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "evidence_types": [item.to_dict() for item in self.bindings],
            "outcomes_opened": self.outcomes_opened,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "declaration_id": self.declaration_id}

    def validate_against(
        self,
        *,
        evidence_pack_id: str,
        evidence_pack_hash: str,
        evidence_ids: frozenset[str],
        pattern_pack_ids: frozenset[str],
        outcomes_opened: bool,
    ) -> None:
        if (
            self.evidence_pack_id != evidence_pack_id
            or self.evidence_pack_hash != evidence_pack_hash
        ):
            raise ValueError("method evidence declaration does not bind the Evidence Pack")
        if self.outcomes_opened != outcomes_opened:
            raise ValueError("method evidence declaration outcome visibility does not match")
        for binding in self.bindings:
            outside_evidence = set(binding.evidence_refs) - evidence_ids
            outside_patterns = set(binding.pattern_pack_refs) - pattern_pack_ids
            if outside_evidence or outside_patterns:
                raise ValueError(
                    "method evidence declaration references content outside the frozen bundle"
                )


@dataclass(frozen=True, slots=True)
class MethodRoutingContext:
    market_state: str
    narrative_salience: str
    analysis_needs: tuple[str, ...]
    available_evidence: tuple[str, ...]
    outcomes_opened: bool

    def __post_init__(self) -> None:
        if self.market_state not in MARKET_STATES:
            raise ValueError("method routing market_state is unsupported")
        if self.narrative_salience not in NARRATIVE_STATES:
            raise ValueError("method routing narrative_salience is unsupported")
        _unique(self.analysis_needs, "method routing analysis_needs")
        _unique(self.available_evidence, "method routing available_evidence")
        if not self.analysis_needs:
            raise ValueError("method routing requires at least one analysis need")

    def to_dict(self) -> dict[str, object]:
        return {
            "market_state": self.market_state,
            "narrative_salience": self.narrative_salience,
            "analysis_needs": list(self.analysis_needs),
            "available_evidence": list(self.available_evidence),
            "outcomes_opened": self.outcomes_opened,
        }


@dataclass(frozen=True, slots=True)
class MethodSkillRoute:
    context: MethodRoutingContext
    selected_skills: tuple[str, ...]
    rejected_methods: tuple[tuple[str, tuple[str, ...]], ...]
    route_id: str

    @property
    def route_hash(self) -> str:
        return canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.method-skill-route.v1",
            "context": self.context.to_dict(),
            "selected_skills": list(self.selected_skills),
            "rejected_methods": [
                {"skill_name": name, "missing_evidence": list(missing)}
                for name, missing in self.rejected_methods
            ],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "route_id": self.route_id}


class MethodSkillRouter:
    def __init__(self, catalog: MethodSkillCatalog) -> None:
        self.catalog = catalog

    def route(self, context: MethodRoutingContext) -> MethodSkillRoute:
        selected: list[str] = []
        rejected: list[tuple[str, tuple[str, ...]]] = []
        needs = set(context.analysis_needs)
        available = set(context.available_evidence)
        for method in sorted(self.catalog.methods, key=lambda item: item.priority):
            if not needs.intersection(method.analysis_needs):
                continue
            if (
                method.applicable_market_states
                and context.market_state not in method.applicable_market_states
            ):
                continue
            if (
                method.applicable_narratives
                and context.narrative_salience not in method.applicable_narratives
            ):
                continue
            missing = tuple(item for item in method.required_evidence if item not in available)
            if missing:
                rejected.append((method.skill_name, missing))
            else:
                selected.append(method.skill_name)
        core = {
            "schema_version": "market-impact.method-skill-route.v1",
            "context": context.to_dict(),
            "selected_skills": selected,
            "rejected_methods": [
                {"skill_name": name, "missing_evidence": list(missing)}
                for name, missing in rejected
            ],
        }
        return MethodSkillRoute(
            context=context,
            selected_skills=tuple(selected),
            rejected_methods=tuple(rejected),
            route_id=f"method-skill-route-{canonical_hash(core)}",
        )


@dataclass(frozen=True, slots=True)
class CPAUsageKeeperPricing:
    schema_version: str
    keeper_version: str
    model: str
    captured_at: datetime
    pricing_style: str
    prompt_microusd_per_million_tokens: int
    completion_microusd_per_million_tokens: int
    cache_read_microusd_per_million_tokens: int
    cache_write_microusd_per_million_tokens: int
    price_multiplier: Decimal
    rules: tuple[tuple[str, str, Decimal], ...]
    source_origin: str = "http://127.0.0.1:8080"

    def __post_init__(self) -> None:
        if self.schema_version != CPA_PRICING_SNAPSHOT_SCHEMA:
            raise ValueError("unsupported CPA pricing snapshot schema_version")
        _nonempty(self.keeper_version, "CPA Usage Keeper version")
        if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", self.keeper_version) is None:
            raise ValueError("CPA Usage Keeper version must be an exact semantic version")
        _nonempty(self.model, "CPA Usage Keeper model")
        require_aware(self.captured_at, "CPA pricing captured_at")
        if self.pricing_style != "openai":
            raise ValueError("the diagnostic currently requires OpenAI-style CPA pricing")
        for name in (
            "prompt_microusd_per_million_tokens",
            "completion_microusd_per_million_tokens",
            "cache_read_microusd_per_million_tokens",
            "cache_write_microusd_per_million_tokens",
        ):
            if cast(int, getattr(self, name)) < 0:
                raise ValueError("CPA token prices cannot be negative")
        if self.price_multiplier <= 0:
            raise ValueError("CPA price_multiplier must be positive")
        if self.source_origin != "http://127.0.0.1:8080":
            raise ValueError("CPA pricing source must remain the pinned local Keeper origin")

    @classmethod
    def from_api_payloads(
        cls,
        *,
        model: str,
        captured_at: datetime,
        version_payload: dict[str, object],
        pricing_payload: dict[str, object],
        rules_payload: dict[str, object],
    ) -> CPAUsageKeeperPricing:
        version = _required_string(version_payload, "version")
        raw_pricing = pricing_payload.get("pricing")
        if not isinstance(raw_pricing, list):
            raise TypeError("CPA pricing response lacks pricing entries")
        pricing_items = cast(list[object], raw_pricing)
        matches: list[dict[str, object]] = []
        for raw_item in pricing_items:
            if not isinstance(raw_item, dict):
                continue
            item = cast(dict[str, object], raw_item)
            if item.get("model") == model:
                matches.append(item)
        if len(matches) != 1:
            raise ValueError("CPA pricing response must contain exactly one configured model")
        entry = matches[0]
        if rules_payload.get("model") != model:
            raise ValueError("CPA pricing rules do not match the requested model")
        raw_rules = rules_payload.get("rules")
        if not isinstance(raw_rules, list):
            raise TypeError("CPA pricing rules response lacks rules")
        rules: list[tuple[str, str, Decimal]] = []
        for raw_rule in cast(list[object], raw_rules):
            if not isinstance(raw_rule, dict):
                raise TypeError("CPA pricing rule must be an object")
            rule = cast(dict[str, object], raw_rule)
            rules.append(
                (
                    _required_string(rule, "key"),
                    _required_string(rule, "value"),
                    _decimal_number(rule, "multiplier"),
                )
            )
        return cls(
            schema_version=CPA_PRICING_SNAPSHOT_SCHEMA,
            keeper_version=version,
            model=model,
            captured_at=captured_at,
            pricing_style=_required_string(entry, "pricing_style"),
            prompt_microusd_per_million_tokens=_usd_to_microusd(
                _decimal_number(entry, "prompt_price_per_1m")
            ),
            completion_microusd_per_million_tokens=_usd_to_microusd(
                _decimal_number(entry, "completion_price_per_1m")
            ),
            cache_read_microusd_per_million_tokens=_usd_to_microusd(
                _decimal_number(entry, "cache_read_price_per_1m")
            ),
            cache_write_microusd_per_million_tokens=_usd_to_microusd(
                _decimal_number(entry, "cache_write_price_per_1m")
            ),
            price_multiplier=_decimal_number(entry, "price_multiplier"),
            rules=tuple(rules),
        )

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def effective_multiplier(self, *, reasoning_effort: str | None) -> Decimal:
        multiplier = self.price_multiplier
        for key, value, rule_multiplier in self.rules:
            if key == "reasoning_effort" and reasoning_effort == value:
                multiplier *= rule_multiplier
            elif key in {"service_tier", "response_service_tier"}:
                raise ValueError(
                    "CPA service-tier pricing requires a Provider Profile field that is not frozen"
                )
            elif key != "reasoning_effort":
                raise ValueError(f"unsupported CPA pricing rule key: {key}")
        return multiplier

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "keeper_version": self.keeper_version,
            "model": self.model,
            "captured_at": self.captured_at.isoformat().replace("+00:00", "Z"),
            "pricing_style": self.pricing_style,
            "prompt_microusd_per_million_tokens": self.prompt_microusd_per_million_tokens,
            "completion_microusd_per_million_tokens": self.completion_microusd_per_million_tokens,
            "cache_read_microusd_per_million_tokens": self.cache_read_microusd_per_million_tokens,
            "cache_write_microusd_per_million_tokens": self.cache_write_microusd_per_million_tokens,
            "price_multiplier": str(self.price_multiplier),
            "rules": [
                {"key": key, "value": value, "multiplier": str(multiplier)}
                for key, value, multiplier in self.rules
            ],
            "source_origin": self.source_origin,
        }


@dataclass(frozen=True, slots=True)
class SkillAblationCostEstimate:
    agent_run_count: int
    provider_request_upper_bound: int
    raw_max_cost_microusd: int
    safety_multiplier: Decimal
    guarded_max_cost_microusd: int
    hard_cap_microusd: int
    within_budget: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "agent_run_count": self.agent_run_count,
            "provider_request_upper_bound": self.provider_request_upper_bound,
            "raw_max_cost_microusd": self.raw_max_cost_microusd,
            "safety_multiplier": str(self.safety_multiplier),
            "guarded_max_cost_microusd": self.guarded_max_cost_microusd,
            "hard_cap_microusd": self.hard_cap_microusd,
            "within_budget": self.within_budget,
        }


def estimate_bounded_agent_run_cost(
    *,
    pricing: CPAUsageKeeperPricing,
    profile: ModelProviderProfile,
    agent_run_count: int,
    safety_multiplier: Decimal,
    max_total_cost_microusd: int,
) -> SkillAblationCostEstimate:
    if isinstance(agent_run_count, bool) or not 1 <= agent_run_count <= 6:
        raise ValueError("cost preflight requires between one and six bounded Agent runs")
    if not Decimal("1") <= safety_multiplier <= Decimal("3"):
        raise ValueError("cost safety_multiplier must be between one and three")
    if not 1 <= max_total_cost_microusd <= 10_000_000:
        raise ValueError("experiment hard cap must be between one micro-USD and $10")
    if pricing.model != profile.model:
        raise ValueError("CPA model does not match the frozen Provider Profile")
    if f"-{pricing.keeper_version}-" not in profile.pricing.pricing_id:
        raise ValueError("CPA version does not match the frozen Provider Profile pricing identity")
    expected_input = profile.pricing.input_microusd_per_million_tokens
    expected_output = profile.pricing.output_microusd_per_million_tokens
    effective_multiplier = pricing.effective_multiplier(reasoning_effort=profile.reasoning_effort)
    effective_input = _ceil_decimal(
        Decimal(pricing.prompt_microusd_per_million_tokens) * effective_multiplier
    )
    effective_output = _ceil_decimal(
        Decimal(pricing.completion_microusd_per_million_tokens) * effective_multiplier
    )
    if (effective_input, effective_output) != (expected_input, expected_output):
        raise ValueError("CPA pricing does not match the frozen Provider Profile")
    effective_cache_read = _ceil_decimal(
        Decimal(pricing.cache_read_microusd_per_million_tokens) * effective_multiplier
    )
    effective_cache_write = _ceil_decimal(
        Decimal(pricing.cache_write_microusd_per_million_tokens) * effective_multiplier
    )
    effective_input_upper_bound = max(
        effective_input,
        effective_cache_read,
        effective_cache_write,
    )
    budget = profile.budget
    per_call = math.ceil(
        (
            budget.max_input_tokens * effective_input_upper_bound
            + budget.max_output_tokens * effective_output
        )
        / 1_000_000
    )
    provider_request_upper_bound = agent_run_count * profile.budget.max_turns * profile.max_attempts
    raw_max = per_call * agent_run_count
    guarded = _ceil_decimal(Decimal(raw_max) * safety_multiplier)
    if guarded > max_total_cost_microusd:
        raise ValueError(
            f"CPA-guarded estimate {guarded} micro-USD exceeds the experiment hard cap"
        )
    return SkillAblationCostEstimate(
        agent_run_count=agent_run_count,
        provider_request_upper_bound=provider_request_upper_bound,
        raw_max_cost_microusd=raw_max,
        safety_multiplier=safety_multiplier,
        guarded_max_cost_microusd=guarded,
        hard_cap_microusd=max_total_cost_microusd,
        within_budget=True,
    )


def estimate_paired_skill_ablation_cost(
    *,
    pricing: CPAUsageKeeperPricing,
    profile: ModelProviderProfile,
    replicate_count: int,
    arm_count: int,
    safety_multiplier: Decimal,
    max_total_cost_microusd: int,
) -> SkillAblationCostEstimate:
    if replicate_count != 3 or arm_count != 2:
        raise ValueError("diagnostic requires exactly three paired replicates")
    return estimate_bounded_agent_run_cost(
        pricing=pricing,
        profile=profile,
        agent_run_count=6,
        safety_multiplier=safety_multiplier,
        max_total_cost_microusd=max_total_cost_microusd,
    )


@dataclass(frozen=True, slots=True)
class PairedSkillAblationRegistration:
    registration_id: str
    experiment_id: str
    registered_at: datetime
    provider_profile_id: str
    provider_profile_hash: str
    method_catalog_id: str
    method_evidence_declaration_id: str
    method_evidence_declaration_hash: str
    evidence_pack_id: str
    evidence_pack_hash: str
    control_skills: tuple[str, ...]
    treatment_skills: tuple[str, ...]
    control_manifest_hashes: tuple[str, ...]
    treatment_manifest_hashes: tuple[str, ...]
    method_route_id: str
    routing_context: MethodRoutingContext
    replicate_count: int
    common_input_hash: str
    pricing: CPAUsageKeeperPricing
    cost_estimate: SkillAblationCostEstimate
    outcomes_opened: bool
    execution_capability: str = "none"

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "method Skill ablation registered_at")
        for name in (
            "provider_profile_hash",
            "method_evidence_declaration_hash",
            "evidence_pack_hash",
            "common_input_hash",
        ):
            _sha256(cast(str, getattr(self, name)), name)
        _unique(self.control_skills, "control Skills")
        _unique(self.treatment_skills, "treatment Skills")
        if len(self.control_manifest_hashes) != len(self.control_skills):
            raise ValueError("control manifest hashes must match control Skills")
        if len(self.treatment_manifest_hashes) != len(self.treatment_skills):
            raise ValueError("treatment manifest hashes must match treatment Skills")
        for value in (*self.control_manifest_hashes, *self.treatment_manifest_hashes):
            _sha256(value, "method Skill manifest hash")
        if self.replicate_count != 3:
            raise ValueError("diagnostic requires exactly three paired replicates")
        if len(self.treatment_skills) != len(self.control_skills) + 1:
            raise ValueError("treatment must add exactly one method Skill")
        if self.treatment_skills[:-1] != self.control_skills:
            raise ValueError("treatment must preserve the complete control Skill prefix")
        if self.treatment_manifest_hashes[:-1] != self.control_manifest_hashes:
            raise ValueError("treatment must preserve the complete control manifest prefix")
        if not self.method_route_id.startswith("method-skill-route-"):
            raise ValueError("method Skill ablation route_id is invalid")
        _sha256(self.method_route_id.removeprefix("method-skill-route-"), "method route hash")
        if self.routing_context.outcomes_opened != self.outcomes_opened:
            raise ValueError("method routing and ablation outcome visibility disagree")
        if self.execution_capability != "none":
            raise ValueError("method Skill ablation cannot expose execution capability")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("method Skill ablation registration_id does not match content")

    @classmethod
    def build(
        cls,
        *,
        experiment_id: str,
        registered_at: datetime,
        provider_profile_id: str,
        provider_profile_hash: str,
        method_catalog_id: str,
        method_evidence_declaration_id: str,
        method_evidence_declaration_hash: str,
        evidence_pack_id: str,
        evidence_pack_hash: str,
        control_skills: tuple[str, ...],
        treatment_skills: tuple[str, ...],
        control_manifest_hashes: tuple[str, ...],
        treatment_manifest_hashes: tuple[str, ...],
        method_route_id: str,
        routing_context: MethodRoutingContext,
        replicate_count: int,
        common_input_hash: str,
        pricing: CPAUsageKeeperPricing,
        cost_estimate: SkillAblationCostEstimate,
        outcomes_opened: bool,
    ) -> PairedSkillAblationRegistration:
        core = {
            "schema_version": PAIRED_SKILL_ABLATION_SCHEMA,
            "experiment_id": experiment_id,
            "registered_at": registered_at.isoformat().replace("+00:00", "Z"),
            "provider_profile_id": provider_profile_id,
            "provider_profile_hash": provider_profile_hash,
            "method_catalog_id": method_catalog_id,
            "method_evidence_declaration_id": method_evidence_declaration_id,
            "method_evidence_declaration_hash": method_evidence_declaration_hash,
            "evidence_pack_id": evidence_pack_id,
            "evidence_pack_hash": evidence_pack_hash,
            "control_skills": list(control_skills),
            "treatment_skills": list(treatment_skills),
            "control_manifest_hashes": list(control_manifest_hashes),
            "treatment_manifest_hashes": list(treatment_manifest_hashes),
            "method_route_id": method_route_id,
            "routing_context": routing_context.to_dict(),
            "replicate_count": replicate_count,
            "run_order": "interleaved_by_replicate_then_arm",
            "common_input_hash": common_input_hash,
            "cpa_pricing": pricing.to_dict(),
            "cost_estimate": cost_estimate.to_dict(),
            "outcomes_opened": outcomes_opened,
            "inference_eligible": False,
            "execution_capability": "none",
        }
        return cls(
            registration_id=f"method-skill-ablation-{canonical_hash(core)}",
            experiment_id=experiment_id,
            registered_at=registered_at,
            provider_profile_id=provider_profile_id,
            provider_profile_hash=provider_profile_hash,
            method_catalog_id=method_catalog_id,
            method_evidence_declaration_id=method_evidence_declaration_id,
            method_evidence_declaration_hash=method_evidence_declaration_hash,
            evidence_pack_id=evidence_pack_id,
            evidence_pack_hash=evidence_pack_hash,
            control_skills=control_skills,
            treatment_skills=treatment_skills,
            control_manifest_hashes=control_manifest_hashes,
            treatment_manifest_hashes=treatment_manifest_hashes,
            method_route_id=method_route_id,
            routing_context=routing_context,
            replicate_count=replicate_count,
            common_input_hash=common_input_hash,
            pricing=pricing,
            cost_estimate=cost_estimate,
            outcomes_opened=outcomes_opened,
        )

    @property
    def added_treatment_skill(self) -> str:
        return self.treatment_skills[-1]

    @property
    def registration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_registration_id(self) -> str:
        return f"method-skill-ablation-{self.registration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": PAIRED_SKILL_ABLATION_SCHEMA,
            "experiment_id": self.experiment_id,
            "registered_at": self.registered_at.isoformat().replace("+00:00", "Z"),
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "method_catalog_id": self.method_catalog_id,
            "method_evidence_declaration_id": self.method_evidence_declaration_id,
            "method_evidence_declaration_hash": self.method_evidence_declaration_hash,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "control_skills": list(self.control_skills),
            "treatment_skills": list(self.treatment_skills),
            "control_manifest_hashes": list(self.control_manifest_hashes),
            "treatment_manifest_hashes": list(self.treatment_manifest_hashes),
            "method_route_id": self.method_route_id,
            "routing_context": self.routing_context.to_dict(),
            "replicate_count": self.replicate_count,
            "run_order": "interleaved_by_replicate_then_arm",
            "common_input_hash": self.common_input_hash,
            "cpa_pricing": self.pricing.to_dict(),
            "cost_estimate": self.cost_estimate.to_dict(),
            "outcomes_opened": self.outcomes_opened,
            "inference_eligible": False,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}


def load_method_skill_catalog(path: Path) -> MethodSkillCatalog:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("method Skill catalog must be a JSON object")
    payload = cast(dict[str, object], raw)
    if payload.get("schema_version") != METHOD_SKILL_CATALOG_SCHEMA:
        raise ValueError("unsupported method Skill catalog schema_version")
    raw_methods = payload.get("methods")
    if not isinstance(raw_methods, list):
        raise TypeError("method Skill catalog methods must be a list")
    methods: list[MethodSkill] = []
    for raw_method in cast(list[object], raw_methods):
        if not isinstance(raw_method, dict):
            raise TypeError("method Skill catalog entry must be an object")
        item = cast(dict[str, object], raw_method)
        methods.append(
            MethodSkill(
                skill_name=_required_string(item, "skill_name"),
                lineage=_required_string(item, "lineage"),
                source_refs=_string_tuple(item, "source_refs"),
                applicable_market_states=_string_tuple(item, "applicable_market_states"),
                applicable_narratives=_string_tuple(item, "applicable_narratives"),
                analysis_needs=_string_tuple(item, "analysis_needs"),
                required_evidence=_string_tuple(item, "required_evidence"),
                prohibited_uses=_string_tuple(item, "prohibited_uses"),
                priority=_required_integer(item, "priority"),
            )
        )
    return MethodSkillCatalog(
        catalog_id=_required_string(payload, "catalog_id"),
        version=_required_string(payload, "version"),
        methods=tuple(methods),
    )


def load_method_evidence_declaration(path: Path) -> MethodEvidenceDeclaration:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("method evidence declaration must be a JSON object")
    payload = cast(dict[str, object], raw)
    if payload.get("schema_version") != METHOD_EVIDENCE_DECLARATION_SCHEMA:
        raise ValueError("unsupported method evidence declaration schema_version")
    raw_bindings = payload.get("evidence_types")
    if not isinstance(raw_bindings, list):
        raise TypeError("method evidence declaration evidence_types must be a list")
    bindings: list[MethodEvidenceBinding] = []
    for raw_binding in cast(list[object], raw_bindings):
        if not isinstance(raw_binding, dict):
            raise TypeError("method evidence declaration binding must be an object")
        binding = cast(dict[str, object], raw_binding)
        bindings.append(
            MethodEvidenceBinding(
                evidence_type=_required_string(binding, "evidence_type"),
                evidence_refs=_string_tuple(binding, "evidence_refs"),
                pattern_pack_refs=_string_tuple(binding, "pattern_pack_refs"),
            )
        )
    outcomes_opened = payload.get("outcomes_opened")
    if not isinstance(outcomes_opened, bool):
        raise TypeError("method evidence declaration outcomes_opened must be boolean")
    return MethodEvidenceDeclaration(
        declaration_id=_required_string(payload, "declaration_id"),
        evidence_pack_id=_required_string(payload, "evidence_pack_id"),
        evidence_pack_hash=_required_string(payload, "evidence_pack_hash"),
        bindings=tuple(bindings),
        outcomes_opened=outcomes_opened,
    )


def _required_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{key} must be a non-empty string")
    return value


def _required_integer(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{key} must be an integer")
    return value


def _string_tuple(payload: dict[str, object], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise TypeError(f"{key} must be a list of strings")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{key} must be a list of strings")
    return tuple(cast(list[str], items))


def _decimal_number(payload: dict[str, object], key: str) -> Decimal:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError(f"{key} must be numeric")
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise TypeError(f"{key} must be numeric") from exc
    if not result.is_finite() or result < 0:
        raise ValueError(f"{key} must be finite and non-negative")
    return result


def _usd_to_microusd(value: Decimal) -> int:
    converted = value * Decimal(1_000_000)
    if converted != converted.to_integral_value():
        raise ValueError("CPA price cannot be represented as whole micro-USD")
    return int(converted)


def _ceil_decimal(value: Decimal) -> int:
    return math.ceil(value)


def _nonempty(value: str, name: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} cannot be empty")


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _identifier(value: str, name: str) -> None:
    normalized_parts = value.replace(".", "-").replace("_", "-").split("-")
    if not value or any(part == "" for part in normalized_parts):
        raise ValueError(f"{name} is invalid")
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789-._")
    if not set(value) <= allowed:
        raise ValueError(f"{name} is invalid")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _is_https_url(value: str) -> bool:
    parsed = urlsplit(value)
    return parsed.scheme == "https" and bool(parsed.netloc) and parsed.username is None
