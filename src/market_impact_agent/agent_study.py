from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.research import EvidenceTier, TransmissionDirectness

EXPOSURE_REGISTRY_SCHEMA = "market-impact.exposure-registry.v1"
AGENT_PHASE2_PREREGISTRATION_SCHEMA = "market-impact.agent-phase2-preregistration.v1"
AGENT_PHASE2_HORIZONS = (1, 3, 10)
AGENT_PHASE2_BASELINES = (
    "commodity_confirmation_3_session",
    "fixed_upstream_3_session",
    "simple_hold_10_session",
    "single_agent_first_valid",
    "target_momentum_3_session",
)
AGENT_PHASE2_REQUIRED_BASELINES = (
    "fixed_upstream_3_session",
    "single_agent_first_valid",
)
AGENT_PHASE2_REQUIRED_MATERIAL_CHANGES = frozenset(
    {
        "agent_replicate_judgment",
        "event_specific_horizon",
        "pre_outcome_exposure_registry",
    }
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")


class TargetRole(StrEnum):
    UPSTREAM_PRODUCER = "upstream_producer"
    INTEGRATED_UPSTREAM = "integrated_upstream"
    INTEGRATED_DOWNSTREAM_CONTROL = "integrated_downstream_control"


@dataclass(frozen=True, slots=True)
class ExposureEntry:
    instrument_id: str
    provider_code: str
    target_role: TargetRole
    directness: TransmissionDirectness
    selection_eligible: bool
    eligible_from: date
    source_refs: tuple[str, ...]
    applicability_conditions: tuple[str, ...]
    offsets: tuple[str, ...]

    def __post_init__(self) -> None:
        _nonempty(self.instrument_id, "instrument_id")
        _nonempty(self.provider_code, "provider_code")
        _unique_nonempty(self.source_refs, "source_refs")
        _unique_nonempty(self.applicability_conditions, "applicability_conditions")
        _unique_nonempty(self.offsets, "offsets")
        if self.selection_eligible and self.target_role is TargetRole.INTEGRATED_DOWNSTREAM_CONTROL:
            raise ValueError("integrated downstream controls cannot be selection eligible")

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "provider_code": self.provider_code,
            "target_role": self.target_role.value,
            "directness": self.directness.value,
            "selection_eligible": self.selection_eligible,
            "eligible_from": self.eligible_from.isoformat(),
            "source_refs": list(self.source_refs),
            "applicability_conditions": list(self.applicability_conditions),
            "offsets": list(self.offsets),
        }


@dataclass(frozen=True, slots=True)
class ExposureRegistry:
    registry_id: str
    as_of: datetime
    mechanism_family: str
    entries: tuple[ExposureEntry, ...]

    def __post_init__(self) -> None:
        require_aware(self.as_of, "exposure registry as_of")
        _identifier(self.mechanism_family, "mechanism_family")
        if len(self.entries) < 2:
            raise ValueError("Exposure Registry requires at least two entries")
        instrument_ids = tuple(item.instrument_id for item in self.entries)
        provider_codes = tuple(item.provider_code for item in self.entries)
        if len(set(instrument_ids)) != len(instrument_ids):
            raise ValueError("Exposure Registry instrument_id values must be unique")
        if len(set(provider_codes)) != len(provider_codes):
            raise ValueError("Exposure Registry provider_code values must be unique")
        if sum(item.selection_eligible for item in self.entries) < 2:
            raise ValueError("Exposure Registry requires at least two selection-eligible targets")
        if self.registry_id != self.expected_registry_id:
            raise ValueError("Exposure Registry registry_id does not match content")

    @property
    def registry_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_registry_id(self) -> str:
        return f"exposure-registry-{self.registry_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": EXPOSURE_REGISTRY_SCHEMA,
            "as_of": _timestamp(self.as_of),
            "mechanism_family": self.mechanism_family,
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registry_id": self.registry_id}


@dataclass(frozen=True, slots=True)
class AccrualPlan:
    opens_after: datetime
    closes_at: datetime
    target_event_count: int
    minimum_separation_days: int
    selection_mode: str
    replacement_policy: str
    shortfall_policy: str

    def __post_init__(self) -> None:
        require_aware(self.opens_after, "accrual opens_after")
        require_aware(self.closes_at, "accrual closes_at")
        if self.closes_at <= self.opens_after:
            raise ValueError("accrual closes_at must be after opens_after")
        if self.target_event_count < 5:
            raise ValueError("prospective holdout requires at least five Accrued Events")
        if self.minimum_separation_days < 10:
            raise ValueError("event separation must be at least ten days")
        if self.selection_mode != "first_eligible":
            raise ValueError("prospective accrual must admit the first eligible events")
        if self.replacement_policy != "never_replace":
            raise ValueError("Accrued Events must never be replaced")
        if self.shortfall_policy != "inconclusive_no_promotion":
            raise ValueError("cohort shortfall must remain inconclusive")

    def to_dict(self) -> dict[str, object]:
        return {
            "opens_after": _timestamp(self.opens_after),
            "closes_at": _timestamp(self.closes_at),
            "target_event_count": self.target_event_count,
            "minimum_separation_days": self.minimum_separation_days,
            "selection_mode": self.selection_mode,
            "replacement_policy": self.replacement_policy,
            "shortfall_policy": self.shortfall_policy,
        }


@dataclass(frozen=True, slots=True)
class EventEligibility:
    mechanism_family: str
    accepted_occurrence_source_tiers: tuple[EvidenceTier, ...]
    required_fields: tuple[str, ...]
    inclusion_rules: tuple[str, ...]
    exclusions: tuple[str, ...]
    missing_critical_data_action: str

    def __post_init__(self) -> None:
        _identifier(self.mechanism_family, "mechanism_family")
        if self.accepted_occurrence_source_tiers != (
            EvidenceTier.OFFICIAL,
            EvidenceTier.PRIMARY,
        ):
            raise ValueError("occurrence evidence must accept exactly official and primary tiers")
        required = {
            "affected_commodity",
            "expected_duration",
            "loss_magnitude",
            "onset_time",
        }
        if set(self.required_fields) != required or len(self.required_fields) != len(required):
            raise ValueError("event eligibility required_fields do not match the frozen protocol")
        _unique_nonempty(self.inclusion_rules, "event eligibility inclusion_rules")
        _unique_nonempty(self.exclusions, "event eligibility exclusions")
        if self.missing_critical_data_action != "retain_and_abstain":
            raise ValueError("missing critical data must retain the event and force abstention")

    def to_dict(self) -> dict[str, object]:
        return {
            "mechanism_family": self.mechanism_family,
            "accepted_occurrence_source_tiers": [
                item.value for item in self.accepted_occurrence_source_tiers
            ],
            "required_fields": list(self.required_fields),
            "inclusion_rules": list(self.inclusion_rules),
            "exclusions": list(self.exclusions),
            "missing_critical_data_action": self.missing_critical_data_action,
        }


@dataclass(frozen=True, slots=True)
class AgentReplicateProtocol:
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
            raise ValueError("Agent assessment delay must remain 60 minutes")
        if self.evidence_cutoff_policy != "first_qualifying_visibility_plus_delay":
            raise ValueError("Agent evidence cutoff policy does not match the frozen protocol")
        if self.input_availability_policy != "available_at_or_before_evidence_cutoff":
            raise ValueError("Agent inputs must not include evidence visible after the cutoff")
        if self.entry_policy != "first_executable_xshg_open_strictly_after_evidence_cutoff":
            raise ValueError("Agent entry policy does not match the frozen protocol")
        if self.replicate_count != 5 or self.minimum_agreeing_replicates != 3:
            raise ValueError("formal Agent study requires three-of-five agreement")
        if self.cross_replicate_memory:
            raise ValueError("Judgment replicates must not share memory")
        if self.selected_skills != ("energy-supply",):
            raise ValueError("Agent study must select only the frozen energy-supply Skill")
        if self.allowed_tools != ("read_evidence", "read_pattern_pack"):
            raise ValueError("Agent study exposes only frozen evidence and Pattern Pack tools")
        if self.allowed_directions != ("up",):
            raise ValueError("current A-share study is long-or-abstain only")
        if self.eligible_horizons_sessions != AGENT_PHASE2_HORIZONS:
            raise ValueError("Agent study horizons do not match the frozen protocol")
        if self.agreement_fields != ("target_id", "direction", "horizon_sessions"):
            raise ValueError("Agent agreement fields do not match the frozen protocol")
        if self.minimum_candidate_confidence != Decimal("0.5"):
            raise ValueError("Agent study minimum confidence must remain 0.5")
        if self.no_agreement_action != "abstain" or self.invalid_replicate_action != "abstain":
            raise ValueError("missing agreement and invalid replicates must force abstention")
        if self.execution_binding_policy != "exact_hashes_before_first_replicate":
            raise ValueError("Agent execution surface must be hash-bound before replicate one")

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
class BaselineSpec:
    baseline_id: str
    rule_ref: str
    horizon_sessions: int | None
    action_space: str

    def __post_init__(self) -> None:
        _identifier(self.baseline_id, "baseline_id")
        _nonempty(self.rule_ref, "baseline rule_ref")
        if self.horizon_sessions is not None and self.horizon_sessions not in AGENT_PHASE2_HORIZONS:
            raise ValueError("baseline horizon must be one of the registered horizons")
        if self.action_space != "long_or_abstain":
            raise ValueError("Agent study baselines must be long-or-abstain")

    def to_dict(self) -> dict[str, object]:
        return {
            "baseline_id": self.baseline_id,
            "rule_ref": self.rule_ref,
            "horizon_sessions": self.horizon_sessions,
            "action_space": self.action_space,
        }


@dataclass(frozen=True, slots=True)
class EvaluationProtocol:
    repeated_backtest_results: int
    all_event_denominator: bool
    missing_critical_data_action: str
    minimum_candidate_coverage_events: int
    report_common_support_view: bool
    require_positive_mean_net_return: bool
    minimum_meaningful_baselines_beaten: int
    required_baselines_to_beat: tuple[str, ...]
    max_single_event_absolute_share: Decimal
    required_metrics: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.repeated_backtest_results != 2:
            raise ValueError("every registered trade requires two deterministic Results")
        if not self.all_event_denominator:
            raise ValueError("primary evaluation must retain every Accrued Event")
        if self.missing_critical_data_action != "abstain_zero_return":
            raise ValueError("missing critical data must contribute an abstention and zero return")
        if self.minimum_candidate_coverage_events < 3:
            raise ValueError("candidate coverage must include at least three events")
        if not self.report_common_support_view:
            raise ValueError("evaluation must report a secondary Common-Support View")
        if not self.require_positive_mean_net_return:
            raise ValueError("candidate mean net return must be positive")
        if self.minimum_meaningful_baselines_beaten < 2:
            raise ValueError("candidate must beat at least two meaningful baselines")
        if self.required_baselines_to_beat != AGENT_PHASE2_REQUIRED_BASELINES:
            raise ValueError("required baselines do not match the frozen protocol")
        if not Decimal("0") < self.max_single_event_absolute_share <= Decimal("0.4"):
            raise ValueError("single-event absolute share must be positive and at most 0.4")
        required_metrics = {
            "calibration",
            "coverage",
            "drawdown",
            "net_return",
            "sharpe",
            "tail_loss",
            "turnover",
        }
        if set(self.required_metrics) != required_metrics or len(self.required_metrics) != len(
            required_metrics
        ):
            raise ValueError("required metrics do not match the frozen protocol")

    def to_dict(self) -> dict[str, object]:
        return {
            "repeated_backtest_results": self.repeated_backtest_results,
            "all_event_denominator": self.all_event_denominator,
            "missing_critical_data_action": self.missing_critical_data_action,
            "minimum_candidate_coverage_events": self.minimum_candidate_coverage_events,
            "report_common_support_view": self.report_common_support_view,
            "require_positive_mean_net_return": self.require_positive_mean_net_return,
            "minimum_meaningful_baselines_beaten": self.minimum_meaningful_baselines_beaten,
            "required_baselines_to_beat": list(self.required_baselines_to_beat),
            "max_single_event_absolute_share": str(self.max_single_event_absolute_share),
            "required_metrics": list(self.required_metrics),
        }


@dataclass(frozen=True, slots=True)
class AgentPhase2Preregistration:
    registration_id: str
    registered_at: datetime
    hypothesis_id: str
    hypothesis_statement: str
    material_changes_from_v2: tuple[str, ...]
    prior_opened_evidence_hashes: tuple[str, ...]
    exposure_registry_id: str
    exposure_registry_hash: str
    accrual: AccrualPlan
    event_eligibility: EventEligibility
    agent_protocol: AgentReplicateProtocol
    baselines: tuple[BaselineSpec, ...]
    evaluation: EvaluationProtocol
    holdout_outcomes_opened: bool
    execution_capability: str

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "registered_at")
        _identifier(self.hypothesis_id, "hypothesis_id")
        _nonempty(self.hypothesis_statement, "hypothesis_statement")
        _unique_nonempty(self.material_changes_from_v2, "material_changes_from_v2")
        if not set(self.material_changes_from_v2) >= AGENT_PHASE2_REQUIRED_MATERIAL_CHANGES:
            raise ValueError("registration does not define every material change from v2")
        _unique_hashes(self.prior_opened_evidence_hashes, "prior_opened_evidence_hashes")
        _nonempty(self.exposure_registry_id, "exposure_registry_id")
        _sha256(self.exposure_registry_hash, "exposure_registry_hash")
        baseline_ids = tuple(sorted(item.baseline_id for item in self.baselines))
        if baseline_ids != AGENT_PHASE2_BASELINES:
            raise ValueError("baseline set does not match the frozen Agent Phase 2 protocol")
        if self.holdout_outcomes_opened:
            raise ValueError("a preregistration cannot contain opened holdout outcomes")
        if self.execution_capability != "none":
            raise ValueError("Agent Phase 2 preregistration grants no execution capability")
        if self.accrual.opens_after <= self.registered_at:
            raise ValueError("prospective accrual must open after registration")
        if self.event_eligibility.mechanism_family != "physical_energy_supply_shock":
            raise ValueError("the first prospective Agent study is physical energy only")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("Agent Phase 2 registration_id does not match content")

    @property
    def registration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_registration_id(self) -> str:
        return f"agent-study-{self.registration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_PHASE2_PREREGISTRATION_SCHEMA,
            "registered_at": _timestamp(self.registered_at),
            "hypothesis_id": self.hypothesis_id,
            "hypothesis_statement": self.hypothesis_statement,
            "material_changes_from_v2": list(self.material_changes_from_v2),
            "prior_opened_evidence_hashes": list(self.prior_opened_evidence_hashes),
            "exposure_registry_id": self.exposure_registry_id,
            "exposure_registry_hash": self.exposure_registry_hash,
            "accrual": self.accrual.to_dict(),
            "event_eligibility": self.event_eligibility.to_dict(),
            "agent_protocol": self.agent_protocol.to_dict(),
            "baselines": [item.to_dict() for item in self.baselines],
            "evaluation": self.evaluation.to_dict(),
            "holdout_outcomes_opened": self.holdout_outcomes_opened,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}

    def validate_against(self, registry: ExposureRegistry) -> None:
        if self.exposure_registry_id != registry.registry_id:
            raise ValueError("preregistration references a different Exposure Registry")
        if self.exposure_registry_hash != registry.registry_hash:
            raise ValueError("preregistration Exposure Registry hash does not match")
        if registry.mechanism_family != self.event_eligibility.mechanism_family:
            raise ValueError("Exposure Registry mechanism family does not match the study")
        if registry.as_of > self.registered_at:
            raise ValueError("Exposure Registry must be frozen no later than registration")
        if any(
            item.selection_eligible and item.eligible_from > self.accrual.opens_after.date()
            for item in registry.entries
        ):
            raise ValueError("selection-eligible targets must exist before accrual opens")


def exposure_registry_from_dict(value: object) -> ExposureRegistry:
    payload = _object(value, "Exposure Registry")
    _closed(
        payload,
        {"schema_version", "registry_id", "as_of", "mechanism_family", "entries"},
        "Exposure Registry",
    )
    if _string(payload, "schema_version") != EXPOSURE_REGISTRY_SCHEMA:
        raise ValueError("unsupported Exposure Registry schema_version")
    entries_payload = _object_list(payload, "entries")
    entries: list[ExposureEntry] = []
    for raw in entries_payload:
        _closed(
            raw,
            {
                "instrument_id",
                "provider_code",
                "target_role",
                "directness",
                "selection_eligible",
                "eligible_from",
                "source_refs",
                "applicability_conditions",
                "offsets",
            },
            "Exposure Registry entry",
        )
        entries.append(
            ExposureEntry(
                instrument_id=_string(raw, "instrument_id"),
                provider_code=_string(raw, "provider_code"),
                target_role=TargetRole(_string(raw, "target_role")),
                directness=TransmissionDirectness(_string(raw, "directness")),
                selection_eligible=_boolean(raw, "selection_eligible"),
                eligible_from=date.fromisoformat(_string(raw, "eligible_from")),
                source_refs=_string_tuple(raw, "source_refs"),
                applicability_conditions=_string_tuple(raw, "applicability_conditions"),
                offsets=_string_tuple(raw, "offsets"),
            )
        )
    return ExposureRegistry(
        registry_id=_string(payload, "registry_id"),
        as_of=_datetime(payload, "as_of"),
        mechanism_family=_string(payload, "mechanism_family"),
        entries=tuple(entries),
    )


def agent_phase2_preregistration_from_dict(value: object) -> AgentPhase2Preregistration:
    payload = _object(value, "Agent Phase 2 preregistration")
    _closed(
        payload,
        {
            "schema_version",
            "registration_id",
            "registered_at",
            "hypothesis_id",
            "hypothesis_statement",
            "material_changes_from_v2",
            "prior_opened_evidence_hashes",
            "exposure_registry_id",
            "exposure_registry_hash",
            "accrual",
            "event_eligibility",
            "agent_protocol",
            "baselines",
            "evaluation",
            "holdout_outcomes_opened",
            "execution_capability",
        },
        "Agent Phase 2 preregistration",
    )
    if _string(payload, "schema_version") != AGENT_PHASE2_PREREGISTRATION_SCHEMA:
        raise ValueError("unsupported Agent Phase 2 preregistration schema_version")
    accrual_raw = _object(payload.get("accrual"), "accrual")
    _closed(
        accrual_raw,
        {
            "opens_after",
            "closes_at",
            "target_event_count",
            "minimum_separation_days",
            "selection_mode",
            "replacement_policy",
            "shortfall_policy",
        },
        "accrual",
    )
    eligibility_raw = _object(payload.get("event_eligibility"), "event_eligibility")
    _closed(
        eligibility_raw,
        {
            "mechanism_family",
            "accepted_occurrence_source_tiers",
            "required_fields",
            "inclusion_rules",
            "exclusions",
            "missing_critical_data_action",
        },
        "event_eligibility",
    )
    agent_raw = _object(payload.get("agent_protocol"), "agent_protocol")
    _closed(
        agent_raw,
        {
            "provider_id",
            "model",
            "runtime_ref",
            "assessment_delay_minutes",
            "evidence_cutoff_policy",
            "input_availability_policy",
            "entry_policy",
            "replicate_count",
            "minimum_agreeing_replicates",
            "cross_replicate_memory",
            "selected_skills",
            "allowed_tools",
            "allowed_directions",
            "eligible_horizons_sessions",
            "agreement_fields",
            "minimum_candidate_confidence",
            "no_agreement_action",
            "invalid_replicate_action",
            "execution_binding_policy",
        },
        "agent_protocol",
    )
    baselines = tuple(_baseline_from_dict(item) for item in _object_list(payload, "baselines"))
    evaluation_raw = _object(payload.get("evaluation"), "evaluation")
    _closed(
        evaluation_raw,
        {
            "repeated_backtest_results",
            "all_event_denominator",
            "missing_critical_data_action",
            "minimum_candidate_coverage_events",
            "report_common_support_view",
            "require_positive_mean_net_return",
            "minimum_meaningful_baselines_beaten",
            "required_baselines_to_beat",
            "max_single_event_absolute_share",
            "required_metrics",
        },
        "evaluation",
    )
    return AgentPhase2Preregistration(
        registration_id=_string(payload, "registration_id"),
        registered_at=_datetime(payload, "registered_at"),
        hypothesis_id=_string(payload, "hypothesis_id"),
        hypothesis_statement=_string(payload, "hypothesis_statement"),
        material_changes_from_v2=_string_tuple(payload, "material_changes_from_v2"),
        prior_opened_evidence_hashes=_string_tuple(payload, "prior_opened_evidence_hashes"),
        exposure_registry_id=_string(payload, "exposure_registry_id"),
        exposure_registry_hash=_string(payload, "exposure_registry_hash"),
        accrual=AccrualPlan(
            opens_after=_datetime(accrual_raw, "opens_after"),
            closes_at=_datetime(accrual_raw, "closes_at"),
            target_event_count=_integer(accrual_raw, "target_event_count"),
            minimum_separation_days=_integer(accrual_raw, "minimum_separation_days"),
            selection_mode=_string(accrual_raw, "selection_mode"),
            replacement_policy=_string(accrual_raw, "replacement_policy"),
            shortfall_policy=_string(accrual_raw, "shortfall_policy"),
        ),
        event_eligibility=EventEligibility(
            mechanism_family=_string(eligibility_raw, "mechanism_family"),
            accepted_occurrence_source_tiers=tuple(
                EvidenceTier(item)
                for item in _string_tuple(
                    eligibility_raw,
                    "accepted_occurrence_source_tiers",
                )
            ),
            required_fields=_string_tuple(eligibility_raw, "required_fields"),
            inclusion_rules=_string_tuple(eligibility_raw, "inclusion_rules"),
            exclusions=_string_tuple(eligibility_raw, "exclusions"),
            missing_critical_data_action=_string(
                eligibility_raw,
                "missing_critical_data_action",
            ),
        ),
        agent_protocol=AgentReplicateProtocol(
            provider_id=_string(agent_raw, "provider_id"),
            model=_string(agent_raw, "model"),
            runtime_ref=_string(agent_raw, "runtime_ref"),
            assessment_delay_minutes=_integer(agent_raw, "assessment_delay_minutes"),
            evidence_cutoff_policy=_string(agent_raw, "evidence_cutoff_policy"),
            input_availability_policy=_string(agent_raw, "input_availability_policy"),
            entry_policy=_string(agent_raw, "entry_policy"),
            replicate_count=_integer(agent_raw, "replicate_count"),
            minimum_agreeing_replicates=_integer(
                agent_raw,
                "minimum_agreeing_replicates",
            ),
            cross_replicate_memory=_boolean(agent_raw, "cross_replicate_memory"),
            selected_skills=_string_tuple(agent_raw, "selected_skills"),
            allowed_tools=_string_tuple(agent_raw, "allowed_tools"),
            allowed_directions=_string_tuple(agent_raw, "allowed_directions"),
            eligible_horizons_sessions=_integer_tuple(
                agent_raw,
                "eligible_horizons_sessions",
            ),
            agreement_fields=_string_tuple(agent_raw, "agreement_fields"),
            minimum_candidate_confidence=Decimal(
                _string(agent_raw, "minimum_candidate_confidence")
            ),
            no_agreement_action=_string(agent_raw, "no_agreement_action"),
            invalid_replicate_action=_string(agent_raw, "invalid_replicate_action"),
            execution_binding_policy=_string(agent_raw, "execution_binding_policy"),
        ),
        baselines=baselines,
        evaluation=EvaluationProtocol(
            repeated_backtest_results=_integer(evaluation_raw, "repeated_backtest_results"),
            all_event_denominator=_boolean(evaluation_raw, "all_event_denominator"),
            missing_critical_data_action=_string(
                evaluation_raw,
                "missing_critical_data_action",
            ),
            minimum_candidate_coverage_events=_integer(
                evaluation_raw,
                "minimum_candidate_coverage_events",
            ),
            report_common_support_view=_boolean(
                evaluation_raw,
                "report_common_support_view",
            ),
            require_positive_mean_net_return=_boolean(
                evaluation_raw,
                "require_positive_mean_net_return",
            ),
            minimum_meaningful_baselines_beaten=_integer(
                evaluation_raw,
                "minimum_meaningful_baselines_beaten",
            ),
            required_baselines_to_beat=_string_tuple(
                evaluation_raw,
                "required_baselines_to_beat",
            ),
            max_single_event_absolute_share=Decimal(
                _string(evaluation_raw, "max_single_event_absolute_share")
            ),
            required_metrics=_string_tuple(evaluation_raw, "required_metrics"),
        ),
        holdout_outcomes_opened=_boolean(payload, "holdout_outcomes_opened"),
        execution_capability=_string(payload, "execution_capability"),
    )


def load_agent_phase2_preregistration(
    registration_path: Path,
    exposure_registry_path: Path,
) -> tuple[AgentPhase2Preregistration, ExposureRegistry]:
    registration = agent_phase2_preregistration_from_dict(
        json.loads(registration_path.read_text(encoding="utf-8"))
    )
    registry = exposure_registry_from_dict(
        json.loads(exposure_registry_path.read_text(encoding="utf-8"))
    )
    registration.validate_against(registry)
    return registration, registry


def _baseline_from_dict(value: dict[str, object]) -> BaselineSpec:
    _closed(
        value,
        {"baseline_id", "rule_ref", "horizon_sessions", "action_space"},
        "baseline",
    )
    raw_horizon = value.get("horizon_sessions")
    if raw_horizon is not None and (
        isinstance(raw_horizon, bool) or not isinstance(raw_horizon, int)
    ):
        raise TypeError("baseline horizon_sessions must be an integer or null")
    return BaselineSpec(
        baseline_id=_string(value, "baseline_id"),
        rule_ref=_string(value, "rule_ref"),
        horizon_sessions=raw_horizon,
        action_space=_string(value, "action_space"),
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object with string keys")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} must be an object with string keys")
    return cast(dict[str, object], value)


def _closed(value: dict[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        raise ValueError(f"{name} fields do not match contract: missing={missing}, extra={extra}")


def _object_list(value: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    raw = value.get(name)
    if not isinstance(raw, list) or not raw:
        raise TypeError(f"{name} must be a non-empty array")
    return tuple(_object(item, f"{name} item") for item in cast(list[object], raw))


def _string(value: dict[str, object], name: str) -> str:
    raw = value.get(name)
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return raw


def _string_tuple(value: dict[str, object], name: str) -> tuple[str, ...]:
    raw = value.get(name)
    if not isinstance(raw, list) or not raw:
        raise TypeError(f"{name} must be a non-empty array")
    items = cast(list[object], raw)
    if any(not isinstance(item, str) or not item or item != item.strip() for item in items):
        raise TypeError(f"{name} must contain non-empty trimmed strings")
    return tuple(cast(str, item) for item in items)


def _integer(value: dict[str, object], name: str) -> int:
    raw = value.get(name)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise TypeError(f"{name} must be an integer")
    return raw


def _integer_tuple(value: dict[str, object], name: str) -> tuple[int, ...]:
    raw = value.get(name)
    if not isinstance(raw, list) or not raw:
        raise TypeError(f"{name} must be a non-empty array")
    items = cast(list[object], raw)
    if any(isinstance(item, bool) or not isinstance(item, int) for item in items):
        raise TypeError(f"{name} must contain integers")
    return tuple(cast(int, item) for item in items)


def _boolean(value: dict[str, object], name: str) -> bool:
    raw = value.get(name)
    if not isinstance(raw, bool):
        raise TypeError(f"{name} must be a boolean")
    return raw


def _datetime(value: dict[str, object], name: str) -> datetime:
    parsed = datetime.fromisoformat(_string(value, name).replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} contains unsupported characters")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if not values or len(set(values)) != len(values):
        raise ValueError(f"{name} must be non-empty and unique")
    for value in values:
        _nonempty(value, name)


def _sha256(value: str, name: str) -> None:
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a SHA-256 hex digest")


def _unique_hashes(values: tuple[str, ...], name: str) -> None:
    if not values or len(set(values)) != len(values):
        raise ValueError(f"{name} must be non-empty and unique")
    for value in values:
        _sha256(value, name)
