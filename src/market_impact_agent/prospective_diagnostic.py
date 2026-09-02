from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability

PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1 = (
    "market-impact.prospective-diagnostic-registration.v1"
)
PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2 = (
    "market-impact.prospective-diagnostic-registration.v2"
)
PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3 = (
    "market-impact.prospective-diagnostic-registration.v3"
)
PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4 = (
    "market-impact.prospective-diagnostic-registration.v4"
)
PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5 = (
    "market-impact.prospective-diagnostic-registration.v5"
)
REASSESSMENT_INITIAL = "reassessment_initial"
REASSESSMENT_PROFILE = "cliproxyapi-luna-max-cpa-retry408-v1"
# Backward-compatible default; v2 must be selected explicitly.
PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA = PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1
_SUPPORTED_REGISTRATION_SCHEMAS = frozenset(
    {
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5,
    }
)

REQUIRED_DIAGNOSTIC_CAPABILITIES = frozenset(
    {
        ObservationCapability.EVENT_REVELATION,
        ObservationCapability.PRIOR_EXPECTATION,
        ObservationCapability.MARKET_CONTEXT,
        ObservationCapability.EXPOSURE_CANDIDATES,
        ObservationCapability.POSITIONING,
        ObservationCapability.MACRO_VINTAGE,
    }
)


class DiagnosticMechanism(StrEnum):
    POLICY_REGULATION = "policy_regulation"
    EARNINGS_EXPECTATION_DELTA = "earnings_expectation_delta"
    MACRO_CYCLE = "macro_cycle"
    MATERIAL_EVENT = "material_event"


class CapabilityApplicability(StrEnum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class DiagnosticCutoffRule:
    timezone: str
    session_boundary: str
    market_close_local: str
    decision_delay_seconds: int

    def __post_init__(self) -> None:
        if self.session_boundary == "harness_now":
            if (self.timezone, self.market_close_local, self.decision_delay_seconds) != (
                "UTC",
                "not_applicable",
                0,
            ):
                raise ValueError("current-time cutoff cannot carry a session-time claim")
            return
        if self.timezone != "Asia/Shanghai":
            raise ValueError("diagnostic cutoff timezone must be Asia/Shanghai")
        if self.session_boundary != "after_market_close":
            raise ValueError("diagnostic checkpoints must use an after-market-close boundary")
        if self.market_close_local != "15:00:00":
            raise ValueError("diagnostic market_close_local must be 15:00:00")
        if not 0 <= self.decision_delay_seconds <= 6 * 60 * 60:
            raise ValueError("diagnostic decision delay must be between zero and six hours")

    def to_dict(self) -> dict[str, object]:
        return {
            "timezone": self.timezone,
            "session_boundary": self.session_boundary,
            "market_close_local": self.market_close_local,
            "decision_delay_seconds": self.decision_delay_seconds,
        }


@dataclass(frozen=True, slots=True)
class DiagnosticCapabilitySlot:
    capability: ObservationCapability
    applicability: CapabilityApplicability
    not_applicable_reason: str | None
    required_route_kinds: tuple[str, ...]
    minimum_data_sources: int
    minimum_observations: int
    poll_interval_seconds: int
    maximum_gap_seconds: int
    maximum_age_seconds: int

    def __post_init__(self) -> None:
        if self.capability not in REQUIRED_DIAGNOSTIC_CAPABILITIES:
            raise ValueError("capability is outside the prospective diagnostic input contract")
        _unique_nonempty(self.required_route_kinds, "diagnostic required_route_kinds")
        if self.applicability is CapabilityApplicability.NOT_APPLICABLE:
            if self.not_applicable_reason is None or not self.not_applicable_reason.strip():
                raise ValueError("not_applicable reason is required")
            if self.required_route_kinds or any(
                (
                    self.minimum_data_sources,
                    self.minimum_observations,
                    self.poll_interval_seconds,
                    self.maximum_gap_seconds,
                    self.maximum_age_seconds,
                )
            ):
                raise ValueError("not_applicable slots cannot carry collection requirements")
            return
        if self.not_applicable_reason is not None:
            raise ValueError("applicable capability slots cannot carry not_applicable reason")
        if not self.required_route_kinds:
            raise ValueError("applicable capability slot needs at least one route kind")
        if not 1 <= self.minimum_data_sources <= len(self.required_route_kinds):
            raise ValueError("minimum_data_sources must fit the registered route kinds")
        if self.minimum_observations < 1:
            raise ValueError("applicable capability slot needs at least one observation")
        if self.poll_interval_seconds < 1:
            raise ValueError("applicable capability slot poll interval must be positive")
        if self.maximum_gap_seconds < self.poll_interval_seconds:
            raise ValueError("maximum gap cannot be shorter than the poll interval")
        if self.maximum_age_seconds < 1:
            raise ValueError("applicable capability slot maximum age must be positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "capability": self.capability.value,
            "applicability": self.applicability.value,
            "not_applicable_reason": self.not_applicable_reason,
            "required_route_kinds": list(self.required_route_kinds),
            "minimum_data_sources": self.minimum_data_sources,
            "minimum_observations": self.minimum_observations,
            "poll_interval_seconds": self.poll_interval_seconds,
            "maximum_gap_seconds": self.maximum_gap_seconds,
            "maximum_age_seconds": self.maximum_age_seconds,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveDiagnosticCheckpoint:
    checkpoint_key: str
    name: str
    mechanism: DiagnosticMechanism
    selection_rule: str
    eligibility_rule: str
    eligibility_source_classes: tuple[str, ...]
    exclusion_rules: tuple[str, ...]
    cutoff: DiagnosticCutoffRule
    capability_slots: tuple[DiagnosticCapabilitySlot, ...]
    target_venues: tuple[str, ...]
    allowed_instrument_classes: tuple[str, ...]
    candidate_horizon_sessions: tuple[int, ...]

    def __post_init__(self) -> None:
        _identifier(self.checkpoint_key, "diagnostic checkpoint_key")
        _trimmed(self.name, "diagnostic checkpoint name")
        if self.selection_rule not in {
            "first_eligible_after_registration",
            "registered_reassessment",
        }:
            raise ValueError("diagnostic selection rule must be first eligible after registration")
        _trimmed(self.eligibility_rule, "diagnostic eligibility rule")
        _unique_nonempty(
            self.eligibility_source_classes,
            "diagnostic eligibility_source_classes",
        )
        _unique_nonempty(self.exclusion_rules, "diagnostic exclusion_rules")
        capabilities = frozenset(item.capability for item in self.capability_slots)
        if capabilities != REQUIRED_DIAGNOSTIC_CAPABILITIES or len(self.capability_slots) != len(
            REQUIRED_DIAGNOSTIC_CAPABILITIES
        ):
            raise ValueError("checkpoint must declare the exact diagnostic capability set")
        _unique_nonempty(self.target_venues, "diagnostic target_venues")
        if not set(self.target_venues) <= {"XSHG", "XSHE"}:
            raise ValueError("diagnostic target venue must be XSHG or XSHE")
        _unique_nonempty(
            self.allowed_instrument_classes,
            "diagnostic allowed_instrument_classes",
        )
        if not set(self.allowed_instrument_classes) <= {"equity", "exchange_traded_fund"}:
            raise ValueError("diagnostic instrument class is unsupported")
        if (
            not self.candidate_horizon_sessions
            or tuple(sorted(set(self.candidate_horizon_sessions)))
            != self.candidate_horizon_sessions
            or any(item < 1 for item in self.candidate_horizon_sessions)
        ):
            raise ValueError("candidate horizons must be unique ascending positive sessions")

    def slot(self, capability: ObservationCapability) -> DiagnosticCapabilitySlot:
        return next(item for item in self.capability_slots if item.capability is capability)

    def to_dict(self) -> dict[str, object]:
        return {
            "checkpoint_key": self.checkpoint_key,
            "name": self.name,
            "mechanism": self.mechanism.value,
            "selection_rule": self.selection_rule,
            "eligibility_rule": self.eligibility_rule,
            "eligibility_source_classes": list(self.eligibility_source_classes),
            "exclusion_rules": list(self.exclusion_rules),
            "cutoff": self.cutoff.to_dict(),
            "capability_slots": [item.to_dict() for item in self.capability_slots],
            "target_venues": list(self.target_venues),
            "allowed_instrument_classes": list(self.allowed_instrument_classes),
            "candidate_horizon_sessions": list(self.candidate_horizon_sessions),
        }


@dataclass(frozen=True, slots=True)
class RegisteredReassessment:
    """Exact old subject and question, not a new event or a parent-Watch resolution."""

    original_registration_id: str
    original_candidate_set_id: str
    original_cluster_id: str
    subject_version_ids: tuple[str, ...]
    research_question: str
    source_acceptance_report_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, prefix in (
            (self.original_registration_id, "prospective-diagnostic-registration-"),
            (self.original_candidate_set_id, "event-impact-triage-candidate-set-"),
            (self.original_cluster_id, "event-impact-triage-cluster-"),
            *((item, "prospective-observation-version-") for item in self.subject_version_ids),
        ):
            suffix = value.removeprefix(prefix)
            if (
                not value.startswith(prefix)
                or len(suffix) != 64
                or any(char not in "0123456789abcdef" for char in suffix)
            ):
                raise ValueError("reassessment requires exact content identities")
        if not self.subject_version_ids or self.subject_version_ids != tuple(
            sorted(set(self.subject_version_ids))
        ):
            raise ValueError("reassessment subject versions must be nonempty sorted and unique")
        _trimmed(self.research_question, "reassessment research question")
        if (
            not self.source_acceptance_report_hashes
            or self.source_acceptance_report_hashes
            != tuple(sorted(set(self.source_acceptance_report_hashes)))
            or any(
                len(item) != 64 or any(char not in "0123456789abcdef" for char in item)
                for item in self.source_acceptance_report_hashes
            )
        ):
            raise ValueError("reassessment requires exact accepted source report hashes")

    def to_dict(self) -> dict[str, object]:
        return {
            "original_registration_id": self.original_registration_id,
            "original_candidate_set_id": self.original_candidate_set_id,
            "original_cluster_id": self.original_cluster_id,
            "subject_version_ids": list(self.subject_version_ids),
            "research_question": self.research_question,
            "source_acceptance_report_hashes": list(self.source_acceptance_report_hashes),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveDiagnosticRegistration:
    registration_id: str
    registered_at: datetime
    checkpoints: tuple[ProspectiveDiagnosticCheckpoint, ...]
    paired_arms: tuple[str, ...]
    replicates_per_arm: int
    model_profile_id: str
    aggregate_model_cost_limit_usd: str
    outcome_opening_rule: str
    stop_conditions: tuple[str, ...]
    go_conditions: tuple[str, ...]
    claim_scope: str
    minimum_replicates_per_arm: int | None = None
    replicate_schedule_rule: str | None = None
    schema_version: str = PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA
    reassessment: RegisteredReassessment | None = None

    def __post_init__(self) -> None:
        if self.schema_version not in _SUPPORTED_REGISTRATION_SCHEMAS:
            raise ValueError("unsupported prospective diagnostic registration schema")
        is_reassessment = self.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5
        if is_reassessment != (self.reassessment is not None):
            raise ValueError("only registration v5 requires a registered reassessment")
        if not is_reassessment and any(
            item.selection_rule != "first_eligible_after_registration"
            or item.cutoff.session_boundary != "after_market_close"
            for item in self.checkpoints
        ):
            raise ValueError("legacy registration requires its original session selection")
        if is_reassessment:
            if len(self.checkpoints) != 1 or self.paired_arms != (REASSESSMENT_INITIAL,):
                raise ValueError("reassessment requires one checkpoint and one initial binding")
            checkpoint = self.checkpoints[0]
            if (
                checkpoint.mechanism is not DiagnosticMechanism.EARNINGS_EXPECTATION_DELTA
                or checkpoint.selection_rule != "registered_reassessment"
                or checkpoint.cutoff.session_boundary != "harness_now"
                or self.replicates_per_arm != 1
                or self.model_profile_id != REASSESSMENT_PROFILE
                or self.aggregate_model_cost_limit_usd != "0.30"
                or self.outcome_opening_rule != "opened_diagnostic_not_blind_or_paired"
            ):
                raise ValueError("reassessment requires the bounded current-time initial Judgment")
            for capability in (
                ObservationCapability.EVENT_REVELATION,
                ObservationCapability.EXPOSURE_CANDIDATES,
            ):
                if (
                    checkpoint.slot(capability).applicability
                    is not CapabilityApplicability.REQUIRED
                ):
                    raise ValueError(
                        "reassessment requires exact event identity and target mapping"
                    )
        if self.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1 and any(
            slot.applicability is CapabilityApplicability.OPTIONAL
            for checkpoint in self.checkpoints
            for slot in checkpoint.capability_slots
        ):
            raise ValueError("prospective diagnostic v1 does not support optional capability slots")
        if self.schema_version in {
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        }:
            for checkpoint in self.checkpoints:
                if (
                    checkpoint.slot(ObservationCapability.EVENT_REVELATION).applicability
                    is not CapabilityApplicability.REQUIRED
                ):
                    raise ValueError(
                        "prospective diagnostic v2-v4 event_revelation must be required"
                    )
        require_aware(self.registered_at, "prospective diagnostic registered_at")
        if self.registered_at.utcoffset() != UTC.utcoffset(self.registered_at):
            raise ValueError("prospective diagnostic registered_at must use UTC")
        maximum_checkpoints = (
            4 if self.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4 else 3
        )
        if not is_reassessment and not 2 <= len(self.checkpoints) <= maximum_checkpoints:
            raise ValueError(
                f"prospective diagnostic requires between two and {maximum_checkpoints} checkpoints"
            )
        keys = tuple(item.checkpoint_key for item in self.checkpoints)
        if len(keys) != len(set(keys)):
            raise ValueError("prospective diagnostic checkpoint keys must be unique")
        mechanisms = tuple(item.mechanism for item in self.checkpoints)
        if len(mechanisms) != len(set(mechanisms)):
            raise ValueError("prospective diagnostic checkpoints must use different mechanisms")
        if not is_reassessment and self.paired_arms != (
            "structured_agent_core",
            "structured_agent_plus_routed_methods",
        ):
            raise ValueError("prospective diagnostic requires the two frozen paired arms")
        if not is_reassessment and self.replicates_per_arm != 3:
            raise ValueError(
                "prospective diagnostic requires exactly three replicates per arm as the maximum"
            )
        if self.schema_version in {
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        }:
            if self.minimum_replicates_per_arm != 2:
                raise ValueError(
                    "prospective diagnostic v3-v4 requires two initial replicates per arm"
                )
            if self.replicate_schedule_rule != (
                "run_two_paired_replicates_then_third_pair_if_either_arm_disagrees"
            ):
                raise ValueError("prospective diagnostic v3-v4 replicate schedule is invalid")
        elif (
            self.minimum_replicates_per_arm is not None or self.replicate_schedule_rule is not None
        ):
            raise ValueError("adaptive replicate fields require prospective diagnostic v3-v4")
        _identifier(self.model_profile_id, "prospective diagnostic model_profile_id")
        if (
            self.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4
            and not self.model_profile_id.endswith("-cpa-v1")
        ):
            raise ValueError("prospective diagnostic v4 requires the CPA-priced Model Profile")
        try:
            cost = Decimal(self.aggregate_model_cost_limit_usd)
        except InvalidOperation as error:
            raise ValueError("aggregate model cost limit must be decimal text") from error
        if cost <= 0 or self.aggregate_model_cost_limit_usd != f"{cost:.2f}":
            raise ValueError("aggregate model cost limit must be positive canonical USD")
        if not is_reassessment and self.outcome_opening_rule != (
            "do_not_open_until_all_paired_judgments_are_sealed"
        ):
            raise ValueError("prospective diagnostic outcome opening rule is invalid")
        _unique_nonempty(self.stop_conditions, "prospective diagnostic stop_conditions")
        _unique_nonempty(self.go_conditions, "prospective diagnostic go_conditions")
        if self.claim_scope != "process_diagnostic_only_no_alpha_or_execution_claim":
            raise ValueError("prospective diagnostic claim scope is invalid")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("prospective diagnostic registration_id does not match content")

    @property
    def expected_registration_id(self) -> str:
        return f"prospective-diagnostic-registration-{canonical_hash(self.core_dict())}"

    def checkpoint(self, checkpoint_key: str) -> ProspectiveDiagnosticCheckpoint:
        match = next(
            (item for item in self.checkpoints if item.checkpoint_key == checkpoint_key),
            None,
        )
        if match is None:
            raise KeyError(f"checkpoint is outside prospective registration: {checkpoint_key}")
        return match

    def core_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "registered_at": _timestamp(self.registered_at),
            "checkpoints": [item.to_dict() for item in self.checkpoints],
            "paired_arms": list(self.paired_arms),
            "replicates_per_arm": self.replicates_per_arm,
            "model_profile_id": self.model_profile_id,
            "aggregate_model_cost_limit_usd": self.aggregate_model_cost_limit_usd,
            "outcome_opening_rule": self.outcome_opening_rule,
            "stop_conditions": list(self.stop_conditions),
            "go_conditions": list(self.go_conditions),
            "claim_scope": self.claim_scope,
        }
        if self.schema_version in {
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        }:
            payload["minimum_replicates_per_arm"] = self.minimum_replicates_per_arm
            payload["replicate_schedule_rule"] = self.replicate_schedule_rule
        if self.reassessment is not None:
            payload["reassessment"] = self.reassessment.to_dict()
        return payload

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}

    @classmethod
    def build(
        cls,
        *,
        registered_at: datetime,
        checkpoints: tuple[ProspectiveDiagnosticCheckpoint, ...],
        paired_arms: tuple[str, ...],
        replicates_per_arm: int,
        model_profile_id: str,
        aggregate_model_cost_limit_usd: str,
        outcome_opening_rule: str,
        stop_conditions: tuple[str, ...],
        go_conditions: tuple[str, ...],
        claim_scope: str,
        minimum_replicates_per_arm: int | None = None,
        replicate_schedule_rule: str | None = None,
        schema_version: str = PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1,
        reassessment: RegisteredReassessment | None = None,
    ) -> ProspectiveDiagnosticRegistration:
        core: dict[str, object] = {
            "schema_version": schema_version,
            "registered_at": _timestamp(registered_at),
            "checkpoints": [item.to_dict() for item in checkpoints],
            "paired_arms": list(paired_arms),
            "replicates_per_arm": replicates_per_arm,
            "model_profile_id": model_profile_id,
            "aggregate_model_cost_limit_usd": aggregate_model_cost_limit_usd,
            "outcome_opening_rule": outcome_opening_rule,
            "stop_conditions": list(stop_conditions),
            "go_conditions": list(go_conditions),
            "claim_scope": claim_scope,
        }
        if schema_version in {
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        }:
            core["minimum_replicates_per_arm"] = minimum_replicates_per_arm
            core["replicate_schedule_rule"] = replicate_schedule_rule
        if reassessment is not None:
            core["reassessment"] = reassessment.to_dict()
        return cls(
            registration_id=(f"prospective-diagnostic-registration-{canonical_hash(core)}"),
            registered_at=registered_at,
            checkpoints=checkpoints,
            paired_arms=paired_arms,
            replicates_per_arm=replicates_per_arm,
            model_profile_id=model_profile_id,
            aggregate_model_cost_limit_usd=aggregate_model_cost_limit_usd,
            outcome_opening_rule=outcome_opening_rule,
            stop_conditions=stop_conditions,
            go_conditions=go_conditions,
            claim_scope=claim_scope,
            minimum_replicates_per_arm=minimum_replicates_per_arm,
            replicate_schedule_rule=replicate_schedule_rule,
            schema_version=schema_version,
            reassessment=reassessment,
        )


def build_reassessment_registration(
    *,
    original_registration: ProspectiveDiagnosticRegistration,
    subject: RegisteredReassessment,
    registered_at: datetime,
) -> ProspectiveDiagnosticRegistration:
    """Declare one opened diagnostic; retain original freshness limits for current context."""
    if original_registration.registration_id != subject.original_registration_id:
        raise ValueError("reassessment subject names another original registration")
    original = next(
        (
            item
            for item in original_registration.checkpoints
            if item.mechanism is DiagnosticMechanism.EARNINGS_EXPECTATION_DELTA
        ),
        None,
    )
    if original is None:
        raise ValueError("original registration has no Earnings checkpoint")
    slots = tuple(
        replace(
            slot,
            applicability=CapabilityApplicability.REQUIRED,
            required_route_kinds=("issuer_event",),
            minimum_data_sources=1,
            minimum_observations=len(subject.subject_version_ids),
        )
        if slot.capability is ObservationCapability.EVENT_REVELATION
        else replace(
            slot,
            applicability=CapabilityApplicability.REQUIRED,
            required_route_kinds=("tradable_instrument_master",),
            minimum_data_sources=1,
            minimum_observations=1,
        )
        if slot.capability is ObservationCapability.EXPOSURE_CANDIDATES
        else replace(
            slot,
            applicability=CapabilityApplicability.OPTIONAL,
            required_route_kinds=("issuer_valuation_context",),
            minimum_data_sources=1,
            minimum_observations=1,
        )
        if slot.capability is ObservationCapability.MARKET_CONTEXT
        else slot
        for slot in original.capability_slots
    )
    checkpoint = replace(
        original,
        name="Current-time read-only Earnings reassessment",
        selection_rule="registered_reassessment",
        eligibility_rule=(
            "Exact registered original subject and accepted target mapping; "
            "economic uncertainty may yield abstention."
        ),
        exclusion_rules=(
            "No fabricated subject, target, receipt, surprise, or execution authority.",
        ),
        cutoff=DiagnosticCutoffRule("UTC", "harness_now", "not_applicable", 0),
        capability_slots=slots,
        candidate_horizon_sessions=(5,),
    )
    return ProspectiveDiagnosticRegistration.build(
        registered_at=registered_at,
        checkpoints=(checkpoint,),
        paired_arms=(REASSESSMENT_INITIAL,),
        replicates_per_arm=1,
        model_profile_id=REASSESSMENT_PROFILE,
        aggregate_model_cost_limit_usd="0.30",
        outcome_opening_rule="opened_diagnostic_not_blind_or_paired",
        stop_conditions=(
            "Stop after one terminal initial Judgment or failure; never silently rerun.",
        ),
        go_conditions=(
            "Exact accepted receipt, target mapping, cutoff and Query Gate permit "
            "one read-only Judgment.",
        ),
        claim_scope="process_diagnostic_only_no_alpha_or_execution_claim",
        schema_version=PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5,
        reassessment=subject,
    )


def load_prospective_diagnostic_registration(
    path: Path,
) -> ProspectiveDiagnosticRegistration:
    return prospective_diagnostic_registration_from_dict(json.loads(path.read_text()))


def prospective_diagnostic_registration_from_dict(
    value: object,
) -> ProspectiveDiagnosticRegistration:
    payload = _object(value, "prospective diagnostic registration")
    schema_version = _string(payload, "schema_version")
    adaptive_fields: set[str] = (
        {"minimum_replicates_per_arm", "replicate_schedule_rule"}
        if schema_version
        in {
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
            PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        }
        else set()
    )
    _exact_keys(
        payload,
        {
            "schema_version",
            "registration_id",
            "registered_at",
            "checkpoints",
            "paired_arms",
            "replicates_per_arm",
            "model_profile_id",
            "aggregate_model_cost_limit_usd",
            "outcome_opening_rule",
            "stop_conditions",
            "go_conditions",
            "claim_scope",
        }
        | adaptive_fields
        | (
            {"reassessment"}
            if schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5
            else set()
        ),
        "prospective diagnostic registration fields",
    )
    registration = ProspectiveDiagnosticRegistration(
        registration_id=_string(payload, "registration_id"),
        registered_at=_datetime(payload.get("registered_at"), "registered_at"),
        checkpoints=tuple(
            _checkpoint_from_dict(item) for item in _list(payload.get("checkpoints"), "checkpoints")
        ),
        paired_arms=_string_tuple(payload.get("paired_arms"), "paired_arms"),
        replicates_per_arm=_integer(payload, "replicates_per_arm"),
        model_profile_id=_string(payload, "model_profile_id"),
        aggregate_model_cost_limit_usd=_string(
            payload,
            "aggregate_model_cost_limit_usd",
        ),
        outcome_opening_rule=_string(payload, "outcome_opening_rule"),
        stop_conditions=_string_tuple(payload.get("stop_conditions"), "stop_conditions"),
        go_conditions=_string_tuple(payload.get("go_conditions"), "go_conditions"),
        claim_scope=_string(payload, "claim_scope"),
        minimum_replicates_per_arm=(
            _integer(payload, "minimum_replicates_per_arm") if adaptive_fields else None
        ),
        replicate_schedule_rule=(
            _string(payload, "replicate_schedule_rule") if adaptive_fields else None
        ),
        schema_version=schema_version,
        reassessment=(
            _reassessment_from_dict(payload.get("reassessment"))
            if schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5
            else None
        ),
    )
    if registration.to_dict() != payload:
        raise ValueError("prospective diagnostic registration is not canonical")
    return registration


def _reassessment_from_dict(value: object) -> RegisteredReassessment:
    payload = _object(value, "registered reassessment")
    result = RegisteredReassessment(
        original_registration_id=_string(payload, "original_registration_id"),
        original_candidate_set_id=_string(payload, "original_candidate_set_id"),
        original_cluster_id=_string(payload, "original_cluster_id"),
        subject_version_ids=_string_tuple(
            payload.get("subject_version_ids"), "subject_version_ids"
        ),
        research_question=_string(payload, "research_question"),
        source_acceptance_report_hashes=_string_tuple(
            payload.get("source_acceptance_report_hashes"), "source_acceptance_report_hashes"
        ),
    )
    if result.to_dict() != payload:
        raise ValueError("registered reassessment is not canonical")
    return result


def _checkpoint_from_dict(value: object) -> ProspectiveDiagnosticCheckpoint:
    payload = _object(value, "prospective diagnostic checkpoint")
    _exact_keys(
        payload,
        {
            "checkpoint_key",
            "name",
            "mechanism",
            "selection_rule",
            "eligibility_rule",
            "eligibility_source_classes",
            "exclusion_rules",
            "cutoff",
            "capability_slots",
            "target_venues",
            "allowed_instrument_classes",
            "candidate_horizon_sessions",
        },
        "checkpoint fields",
    )
    cutoff = _object(payload.get("cutoff"), "diagnostic cutoff")
    _exact_keys(
        cutoff,
        {"timezone", "session_boundary", "market_close_local", "decision_delay_seconds"},
        "cutoff fields",
    )
    return ProspectiveDiagnosticCheckpoint(
        checkpoint_key=_string(payload, "checkpoint_key"),
        name=_string(payload, "name"),
        mechanism=DiagnosticMechanism(_string(payload, "mechanism")),
        selection_rule=_string(payload, "selection_rule"),
        eligibility_rule=_string(payload, "eligibility_rule"),
        eligibility_source_classes=_string_tuple(
            payload.get("eligibility_source_classes"),
            "eligibility_source_classes",
        ),
        exclusion_rules=_string_tuple(payload.get("exclusion_rules"), "exclusion_rules"),
        cutoff=DiagnosticCutoffRule(
            timezone=_string(cutoff, "timezone"),
            session_boundary=_string(cutoff, "session_boundary"),
            market_close_local=_string(cutoff, "market_close_local"),
            decision_delay_seconds=_integer(cutoff, "decision_delay_seconds"),
        ),
        capability_slots=tuple(
            _slot_from_dict(item)
            for item in _list(payload.get("capability_slots"), "capability_slots")
        ),
        target_venues=_string_tuple(payload.get("target_venues"), "target_venues"),
        allowed_instrument_classes=_string_tuple(
            payload.get("allowed_instrument_classes"),
            "allowed_instrument_classes",
        ),
        candidate_horizon_sessions=_integer_tuple(
            payload.get("candidate_horizon_sessions"),
            "candidate_horizon_sessions",
        ),
    )


def _slot_from_dict(value: object) -> DiagnosticCapabilitySlot:
    payload = _object(value, "prospective diagnostic capability slot")
    _exact_keys(
        payload,
        {
            "capability",
            "applicability",
            "not_applicable_reason",
            "required_route_kinds",
            "minimum_data_sources",
            "minimum_observations",
            "poll_interval_seconds",
            "maximum_gap_seconds",
            "maximum_age_seconds",
        },
        "capability slot fields",
    )
    reason = payload.get("not_applicable_reason")
    if reason is not None and not isinstance(reason, str):
        raise ValueError("not_applicable_reason must be string or null")
    return DiagnosticCapabilitySlot(
        capability=ObservationCapability(_string(payload, "capability")),
        applicability=CapabilityApplicability(_string(payload, "applicability")),
        not_applicable_reason=reason,
        required_route_kinds=_string_tuple(
            payload.get("required_route_kinds"),
            "required_route_kinds",
        ),
        minimum_data_sources=_integer(payload, "minimum_data_sources"),
        minimum_observations=_integer(payload, "minimum_observations"),
        poll_interval_seconds=_integer(payload, "poll_interval_seconds"),
        maximum_gap_seconds=_integer(payload, "maximum_gap_seconds"),
        maximum_age_seconds=_integer(payload, "maximum_age_seconds"),
    )


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    payload = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in payload):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], payload)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return cast(list[object], value)


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    values = _list(value, name)
    result = tuple(values)
    if any(not isinstance(item, str) for item in result):
        raise ValueError(f"{name} items must be strings")
    return cast(tuple[str, ...], result)


def _integer_tuple(value: object, name: str) -> tuple[int, ...]:
    values = _list(value, name)
    if any(not isinstance(item, int) or isinstance(item, bool) for item in values):
        raise ValueError(f"{name} items must be integers")
    return cast(tuple[int, ...], tuple(values))


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    _trimmed(value, name)
    if not all(
        character.islower() or character.isdigit() or character in "-_" for character in value
    ):
        raise ValueError(f"{name} must be a lowercase stable identifier")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if any(not item or item != item.strip() for item in values):
        raise ValueError(f"{name} must contain trimmed non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _exact_keys(payload: dict[str, object], expected: set[str], name: str) -> None:
    if set(payload) != expected:
        raise ValueError(f"{name} do not match the canonical contract")
