from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability

PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA = "market-impact.prospective-diagnostic-registration.v1"

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


class CapabilityApplicability(StrEnum):
    REQUIRED = "required"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class DiagnosticCutoffRule:
    timezone: str
    session_boundary: str
    market_close_local: str
    decision_delay_seconds: int

    def __post_init__(self) -> None:
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
            raise ValueError("required capability slots cannot carry not_applicable reason")
        if not self.required_route_kinds:
            raise ValueError("required capability slot needs at least one route kind")
        if not 1 <= self.minimum_data_sources <= len(self.required_route_kinds):
            raise ValueError("minimum_data_sources must fit the registered route kinds")
        if self.minimum_observations < 1:
            raise ValueError("required capability slot needs at least one observation")
        if self.poll_interval_seconds < 1:
            raise ValueError("required capability slot poll interval must be positive")
        if self.maximum_gap_seconds < self.poll_interval_seconds:
            raise ValueError("maximum gap cannot be shorter than the poll interval")
        if self.maximum_age_seconds < 1:
            raise ValueError("required capability slot maximum age must be positive")

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
        if self.selection_rule != "first_eligible_after_registration":
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
    schema_version: str = PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA:
            raise ValueError("unsupported prospective diagnostic registration schema")
        require_aware(self.registered_at, "prospective diagnostic registered_at")
        if self.registered_at.utcoffset() != UTC.utcoffset(self.registered_at):
            raise ValueError("prospective diagnostic registered_at must use UTC")
        if not 2 <= len(self.checkpoints) <= 3:
            raise ValueError("prospective diagnostic requires two or three checkpoints")
        keys = tuple(item.checkpoint_key for item in self.checkpoints)
        if len(keys) != len(set(keys)):
            raise ValueError("prospective diagnostic checkpoint keys must be unique")
        mechanisms = tuple(item.mechanism for item in self.checkpoints)
        if len(mechanisms) != len(set(mechanisms)):
            raise ValueError("prospective diagnostic checkpoints must use different mechanisms")
        if self.paired_arms != (
            "structured_agent_core",
            "structured_agent_plus_routed_methods",
        ):
            raise ValueError("prospective diagnostic requires the two frozen paired arms")
        if self.replicates_per_arm != 3:
            raise ValueError("prospective diagnostic requires exactly three replicates per arm")
        _identifier(self.model_profile_id, "prospective diagnostic model_profile_id")
        try:
            cost = Decimal(self.aggregate_model_cost_limit_usd)
        except InvalidOperation as error:
            raise ValueError("aggregate model cost limit must be decimal text") from error
        if cost <= 0 or self.aggregate_model_cost_limit_usd != f"{cost:.2f}":
            raise ValueError("aggregate model cost limit must be positive canonical USD")
        if self.outcome_opening_rule != ("do_not_open_until_all_paired_judgments_are_sealed"):
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
        return {
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
    ) -> ProspectiveDiagnosticRegistration:
        core = {
            "schema_version": PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA,
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
        )


def load_prospective_diagnostic_registration(
    path: Path,
) -> ProspectiveDiagnosticRegistration:
    return prospective_diagnostic_registration_from_dict(json.loads(path.read_text()))


def prospective_diagnostic_registration_from_dict(
    value: object,
) -> ProspectiveDiagnosticRegistration:
    payload = _object(value, "prospective diagnostic registration")
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
        },
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
        schema_version=_string(payload, "schema_version"),
    )
    if registration.to_dict() != payload:
        raise ValueError("prospective diagnostic registration is not canonical")
    return registration


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
