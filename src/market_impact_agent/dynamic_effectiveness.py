"""Preregistration and deterministic selection rules for the dynamic-horizon study.

This module owns no model loop and no financial state.  It freezes the study
surface consumed by the existing pi runtime and records the rules that must be
applied before outcomes are opened.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware


class CaseRole(StrEnum):
    OPENED_DEVELOPMENT = "opened_development"
    STABILITY_REPEAT = "stability_repeat"
    CADENCE_DEVELOPMENT = "cadence_development"
    UNSEEN_CANARY = "unseen_canary"
    PROSPECTIVE = "prospective"
    MEMORY_SENSITIVITY = "memory_sensitivity"


class DatePresentation(StrEnum):
    TRUE_DATE = "true_date"
    RELATIVE_OFFSET = "relative_offset"


class AnalysisTopology(StrEnum):
    LUNA_MAX = "luna_max"
    TERRA_HIGH = "terra_high"
    SOL_HIGH = "sol_high"
    LUNA_TERRA_WITH_CONDITIONAL_SOL_JUDGE = "luna_terra_conditional_sol_judge"


@dataclass(frozen=True, slots=True)
class StudyBudgetV1:
    route_qualification_microusd: int = 1_000_000
    analysis_ablation_microusd: int = 7_000_000
    portfolio_ablation_microusd: int = 2_500_000
    cadence_ablation_microusd: int = 5_000_000
    unseen_and_prospective_microusd: int = 2_500_000
    recovery_reserve_microusd: int = 2_000_000

    @property
    def total_microusd(self) -> int:
        return sum(
            (
                self.route_qualification_microusd,
                self.analysis_ablation_microusd,
                self.portfolio_ablation_microusd,
                self.cadence_ablation_microusd,
                self.unseen_and_prospective_microusd,
                self.recovery_reserve_microusd,
            )
        )

    def __post_init__(self) -> None:
        if self.total_microusd != 20_000_000:
            raise ValueError("dynamic effectiveness budget must equal USD 20")

    def to_dict(self) -> dict[str, object]:
        return {
            "route_qualification_microusd": self.route_qualification_microusd,
            "analysis_ablation_microusd": self.analysis_ablation_microusd,
            "portfolio_ablation_microusd": self.portfolio_ablation_microusd,
            "cadence_ablation_microusd": self.cadence_ablation_microusd,
            "unseen_and_prospective_microusd": self.unseen_and_prospective_microusd,
            "recovery_reserve_microusd": self.recovery_reserve_microusd,
            "total_microusd": self.total_microusd,
        }


@dataclass(frozen=True, slots=True)
class ModelStudyArm:
    topology: AnalysisTopology
    model: str
    reasoning_effort: str
    provider_profile_id: str
    provider_profile_hash: str
    pricing_id: str

    def __post_init__(self) -> None:
        expected = {
            AnalysisTopology.LUNA_MAX: ("gpt-5.6-luna", "max"),
            AnalysisTopology.TERRA_HIGH: ("gpt-5.6-terra", "high"),
            AnalysisTopology.SOL_HIGH: ("gpt-5.6-sol", "high"),
        }
        if (
            self.topology not in expected
            or (self.model, self.reasoning_effort) != expected[self.topology]
        ):
            raise ValueError("model study arm does not match the preregistered route")
        _sha256(self.provider_profile_hash, "provider_profile_hash")
        for value, name in (
            (self.provider_profile_id, "provider_profile_id"),
            (self.pricing_id, "pricing_id"),
        ):
            _text(value, name)

    def to_dict(self) -> dict[str, object]:
        return {
            "topology": self.topology.value,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "pricing_id": self.pricing_id,
        }


@dataclass(frozen=True, slots=True)
class StudyCase:
    case_id: str
    role: CaseRole
    frozen_input_hash: str
    event_year: int
    category: str

    def __post_init__(self) -> None:
        _text(self.case_id, "case_id")
        _text(self.category, "category")
        _sha256(self.frozen_input_hash, "frozen_input_hash")
        if not 2000 <= self.event_year <= 2100:
            raise ValueError("study case event_year is outside the supported range")

    def to_dict(self) -> dict[str, object]:
        return {
            "case_id": self.case_id,
            "role": self.role.value,
            "frozen_input_hash": self.frozen_input_hash,
            "event_year": self.event_year,
            "category": self.category,
        }


@dataclass(frozen=True, slots=True)
class MemorySensitivityPair:
    case_id: str
    frozen_facts_hash: str
    presentations: tuple[DatePresentation, DatePresentation] = (
        DatePresentation.TRUE_DATE,
        DatePresentation.RELATIVE_OFFSET,
    )

    def __post_init__(self) -> None:
        _text(self.case_id, "case_id")
        _sha256(self.frozen_facts_hash, "frozen_facts_hash")
        if set(self.presentations) != set(DatePresentation):
            raise ValueError("memory sensitivity requires true-date and relative-offset pairing")

    def to_dict(self) -> dict[str, object]:
        return {
            "case_id": self.case_id,
            "frozen_facts_hash": self.frozen_facts_hash,
            "presentations": [item.value for item in self.presentations],
            "facts_unchanged": True,
            "outcomes_hidden": True,
            "inference_scope": "date-label sensitivity; not proof of training-data leakage",
        }


@dataclass(frozen=True, slots=True)
class DynamicEffectivenessRegistrationV1:
    experiment_id: str
    registered_at: datetime
    runtime_identity_hash: str
    model_arms: tuple[ModelStudyArm, ModelStudyArm, ModelStudyArm]
    opened_cases: tuple[StudyCase, ...]
    stability_case_ids: tuple[str, str, str]
    memory_sensitivity_pairs: tuple[MemorySensitivityPair, ...]
    budget: StudyBudgetV1 = field(default_factory=StudyBudgetV1)
    logical_model_concurrency: int = 3
    experiment_concurrency: int = 6
    maximum_physical_requests_per_judgment: int = 4
    transient_retry_limit: int = 1
    consecutive_provider_failure_pause: int = 2

    def __post_init__(self) -> None:
        _text(self.experiment_id, "experiment_id")
        require_aware(self.registered_at, "study registered_at")
        _sha256(self.runtime_identity_hash, "runtime_identity_hash")
        if tuple(item.topology for item in self.model_arms) != (
            AnalysisTopology.LUNA_MAX,
            AnalysisTopology.TERRA_HIGH,
            AnalysisTopology.SOL_HIGH,
        ):
            raise ValueError("study requires Luna max, Terra high and Sol high in fixed order")
        if len({item.pricing_id for item in self.model_arms}) != 3:
            raise ValueError("each model arm requires its own frozen pricing identity")
        if len(self.opened_cases) != 8 or any(
            item.role is not CaseRole.OPENED_DEVELOPMENT for item in self.opened_cases
        ):
            raise ValueError("study requires the eight fixed opened development cases")
        ids = tuple(item.case_id for item in self.opened_cases)
        if len(set(ids)) != len(ids):
            raise ValueError("opened development cases must be unique")
        if self.stability_case_ids != (
            "2018-07-02",
            "2019-01-07",
            "2020-02-03",
        ) or not set(self.stability_case_ids) <= set(ids):
            raise ValueError("study stability cases differ from preregistration")
        for pair in self.memory_sensitivity_pairs:
            if pair.case_id not in ids:
                raise ValueError("memory sensitivity pair must reuse a registered case")
        if (
            self.logical_model_concurrency,
            self.experiment_concurrency,
            self.maximum_physical_requests_per_judgment,
            self.transient_retry_limit,
            self.consecutive_provider_failure_pause,
        ) != (3, 6, 4, 1, 2):
            raise ValueError("study reliability bounds differ from approval")

    @property
    def registration_id(self) -> str:
        return "dynamic-effectiveness-registration-v1-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.dynamic-effectiveness-registration.v1",
            "experiment_id": self.experiment_id,
            "registered_at": _timestamp(self.registered_at),
            "runtime_identity_hash": self.runtime_identity_hash,
            "model_arms": [item.to_dict() for item in self.model_arms],
            "opened_cases": [item.to_dict() for item in self.opened_cases],
            "stability_case_ids": list(self.stability_case_ids),
            "analysis_topologies": [item.value for item in AnalysisTopology],
            "portfolio_scenarios": [
                "bullish_cash",
                "bullish_overconcentrated",
                "bearish_existing_long",
                "low_confidence_rangebound_cash",
            ],
            "cadence_case_roles": [
                "shock_reversal_1",
                "shock_reversal_2",
                "trend_continuation",
                "state_transition_or_noise",
            ],
            "cadence_arms": [
                "dynamic_horizon_one_shot",
                "scheduled_review",
                "material_event_driven_review",
            ],
            "memory_sensitivity_pairs": [item.to_dict() for item in self.memory_sensitivity_pairs],
            "budget": self.budget.to_dict(),
            "logical_model_concurrency": self.logical_model_concurrency,
            "experiment_concurrency": self.experiment_concurrency,
            "maximum_physical_requests_per_judgment": (self.maximum_physical_requests_per_judgment),
            "transient_retry_limit": self.transient_retry_limit,
            "consecutive_provider_failure_pause": self.consecutive_provider_failure_pause,
            "judge_rule": (
                "Luna max and Terra high analyze independently; Sol high runs only on semantic "
                "disagreement, reads original evidence and reasons, and never votes."
            ),
            "outcomes_visible_to_agents": False,
            "live_execution": False,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}


@dataclass(frozen=True, slots=True)
class CadenceMetrics:
    net_return: Decimal
    maximum_drawdown: Decimal
    adverse_excursions: tuple[Decimal, Decimal]
    corrected_reversal_count: int
    trend_error_delta: int
    stressed_incremental_return: Decimal
    turnover: Decimal
    model_cost_microusd: int


def cadence_eligible(candidate: CadenceMetrics, one_shot: CadenceMetrics) -> bool:
    return (
        candidate.net_return >= one_shot.net_return
        and all(
            candidate_value >= baseline_value
            for candidate_value, baseline_value in zip(
                candidate.adverse_excursions, one_shot.adverse_excursions, strict=True
            )
        )
        and candidate.corrected_reversal_count >= 1
        and candidate.trend_error_delta <= 0
        and candidate.stressed_incremental_return >= 0
    )


def choose_review_cadence(
    *,
    one_shot: CadenceMetrics,
    scheduled: CadenceMetrics,
    event_driven: CadenceMetrics,
) -> str:
    eligible = [
        ("scheduled_review", scheduled),
        ("material_event_driven_review", event_driven),
    ]
    passed = [item for item in eligible if cadence_eligible(item[1], one_shot)]
    if not passed:
        return "dynamic_horizon_one_shot"
    return min(
        passed,
        key=lambda item: (
            -item[1].maximum_drawdown,
            item[1].model_cost_microusd,
            item[1].turnover,
            item[0],
        ),
    )[0]


def _text(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
