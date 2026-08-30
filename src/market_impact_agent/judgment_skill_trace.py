from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    JudgmentArtifact,
    canonical_hash,
)
from market_impact_agent.agent_engine import AgentExecutionBinding
from market_impact_agent.domain import require_aware
from market_impact_agent.research_methods import SkillRoute

JUDGMENT_SKILL_TRACE_SCHEMA = "market-impact.judgment-skill-trace.v1"


class SkillOfferDisposition(StrEnum):
    OFFERED = "offered"
    DEPENDENCY_ONLY = "dependency_only"


class SkillRouteDisposition(StrEnum):
    SELECTED = "selected"
    LOADED_DEPENDENCY = "loaded_dependency"
    REJECTED = "rejected"


class AgentReportedSkillUse(StrEnum):
    APPLIED = "applied"
    CONSULTED_NOT_APPLIED = "consulted_not_applied"
    NOT_APPLICABLE = "not_applicable"
    NOT_REPORTED = "not_reported"


@dataclass(frozen=True, slots=True)
class JudgmentSkillTraceEntry:
    skill_name: str
    manifest_hash: str | None
    offer_disposition: SkillOfferDisposition
    route_disposition: SkillRouteDisposition
    loaded: bool
    route_reasons: tuple[str, ...]
    agent_reported_use: AgentReportedSkillUse
    trigger_evidence_refs: tuple[str, ...]
    influenced_proposal_paths: tuple[str, ...]
    agent_rationale: str

    def __post_init__(self) -> None:
        _identifier(self.skill_name, "Skill Trace skill_name")
        if self.manifest_hash is not None:
            _sha256(self.manifest_hash, "Skill Trace manifest_hash")
        _unique_nonempty(self.route_reasons, "Skill Trace route reasons")
        _unique(self.trigger_evidence_refs, "Skill Trace trigger evidence refs")
        _unique(self.influenced_proposal_paths, "Skill Trace influenced proposal paths")
        _nonempty(self.agent_rationale, "Skill Trace Agent rationale")
        route_loads = self.route_disposition in {
            SkillRouteDisposition.SELECTED,
            SkillRouteDisposition.LOADED_DEPENDENCY,
        }
        if self.loaded != route_loads:
            raise ValueError("Skill Trace route and loaded status disagree")
        if self.loaded != (self.manifest_hash is not None):
            raise ValueError("only a loaded Skill may claim an exact manifest hash")
        if (
            self.route_disposition is SkillRouteDisposition.LOADED_DEPENDENCY
            and self.offer_disposition is not SkillOfferDisposition.DEPENDENCY_ONLY
        ):
            raise ValueError("loaded dependency must use dependency-only offer disposition")
        if (
            self.route_disposition is SkillRouteDisposition.SELECTED
            and self.offer_disposition is not SkillOfferDisposition.OFFERED
        ):
            raise ValueError("selected Skill must have been offered")
        if not self.loaded and self.agent_reported_use in {
            AgentReportedSkillUse.APPLIED,
            AgentReportedSkillUse.CONSULTED_NOT_APPLIED,
        }:
            raise ValueError("an unloaded Skill cannot be reported as consulted or applied")
        if (
            self.agent_reported_use is AgentReportedSkillUse.APPLIED
            and not self.influenced_proposal_paths
        ):
            raise ValueError("an applied Skill requires at least one influenced proposal path")
        if (
            self.agent_reported_use is not AgentReportedSkillUse.APPLIED
            and self.influenced_proposal_paths
        ):
            raise ValueError("only an applied Skill may report influenced proposal paths")

    def to_dict(self) -> dict[str, object]:
        return {
            "skill_name": self.skill_name,
            "manifest_hash": self.manifest_hash,
            "offer_disposition": self.offer_disposition.value,
            "route_disposition": self.route_disposition.value,
            "loaded": self.loaded,
            "route_reasons": list(self.route_reasons),
            "agent_reported_use": self.agent_reported_use.value,
            "trigger_evidence_refs": list(self.trigger_evidence_refs),
            "influenced_proposal_paths": list(self.influenced_proposal_paths),
            "agent_rationale": self.agent_rationale,
        }


@dataclass(frozen=True, slots=True)
class JudgmentSkillTrace:
    trace_id: str
    observed_at: datetime
    run_id: str
    judgment_artifact_id: str
    judgment_artifact_hash: str
    route_id: str
    route_hash: str
    execution_binding_hash: str
    entries: tuple[JudgmentSkillTraceEntry, ...]

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "Judgment Skill Trace observed_at")
        _nonempty(self.run_id, "Judgment Skill Trace run_id")
        _prefixed_hash(
            self.judgment_artifact_id,
            "judgment-",
            "Judgment Skill Trace artifact ID",
        )
        for name in (
            "judgment_artifact_hash",
            "route_hash",
            "execution_binding_hash",
        ):
            _sha256(getattr(self, name), f"Judgment Skill Trace {name}")
        _nonempty(self.route_id, "Judgment Skill Trace route_id")
        if not self.entries:
            raise ValueError("Judgment Skill Trace requires at least one Skill entry")
        _unique(tuple(item.skill_name for item in self.entries), "Judgment Skill Trace names")
        _unique(
            tuple(item.manifest_hash for item in self.entries if item.manifest_hash is not None),
            "Judgment Skill Trace manifest hashes",
        )
        if self.trace_id != self.expected_trace_id:
            raise ValueError("Judgment Skill Trace trace_id does not match content")

    @property
    def expected_trace_id(self) -> str:
        return f"judgment-skill-trace-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": JUDGMENT_SKILL_TRACE_SCHEMA,
            "observed_at": _timestamp(self.observed_at),
            "run_id": self.run_id,
            "judgment_artifact_id": self.judgment_artifact_id,
            "judgment_artifact_hash": self.judgment_artifact_hash,
            "route_id": self.route_id,
            "route_hash": self.route_hash,
            "execution_binding_hash": self.execution_binding_hash,
            "entries": [item.to_dict() for item in self.entries],
            "agent_report_authority": "observational_self_report_not_causal_evidence",
            "signal_or_execution_authority": False,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "trace_id": self.trace_id}

    @classmethod
    def build(
        cls,
        *,
        observed_at: datetime,
        artifact: JudgmentArtifact,
        route: SkillRoute,
        execution_binding: AgentExecutionBinding,
        evidence_pack: EvidencePack,
        entries: tuple[JudgmentSkillTraceEntry, ...],
    ) -> JudgmentSkillTrace:
        artifact.validate_against(evidence_pack)
        if observed_at < artifact.finished_at:
            raise ValueError("Judgment Skill Trace cannot predate the Judgment Artifact")
        observed_loaded = {cast(str, item.manifest_hash) for item in entries if item.loaded}
        if observed_loaded != set(artifact.skill_hashes):
            raise ValueError("Judgment Skill Trace loaded Skills differ from the Judgment Artifact")
        observed_binding = AgentExecutionBinding(
            runtime_ref=execution_binding.runtime_ref,
            runtime_config_hash=artifact.runtime_config_hash,
            prompt_hash=artifact.prompt_hash,
            skill_hashes=artifact.skill_hashes,
            tool_manifest_hashes=artifact.tool_manifest_hashes,
            tool_surface_hash=artifact.tool_surface_hash,
            mcp_server_hashes=artifact.mcp_server_hashes,
            context_estimator_id=artifact.context_estimator_id,
            compactor_id=artifact.compactor_id,
        )
        if execution_binding != observed_binding:
            raise ValueError("Judgment Skill Trace binding differs from the Judgment Artifact")
        route_loaded = dict(zip(route.loaded_skills, route.manifest_hashes, strict=True))
        entry_by_name = {item.skill_name: item for item in entries}
        expected_entry_names = set(route.requested_skills) | set(route.loaded_skills)
        if set(entry_by_name) != expected_entry_names:
            raise ValueError("Judgment Skill Trace entries differ from the frozen Skill Route")
        for name, entry in entry_by_name.items():
            if entry.route_reasons != route.reasons:
                raise ValueError("Judgment Skill Trace reasons differ from the Skill Route")
            if name in route_loaded:
                if not entry.loaded or entry.manifest_hash != route_loaded[name]:
                    raise ValueError(
                        "Judgment Skill Trace loaded entry differs from the Skill Route"
                    )
                expected_disposition = (
                    SkillRouteDisposition.SELECTED
                    if name in route.requested_skills
                    else SkillRouteDisposition.LOADED_DEPENDENCY
                )
                if entry.route_disposition is not expected_disposition:
                    raise ValueError(
                        "Judgment Skill Trace disposition differs from the Skill Route"
                    )
            elif (
                entry.route_disposition is not SkillRouteDisposition.REJECTED
                or entry.offer_disposition is not SkillOfferDisposition.OFFERED
                or entry.manifest_hash is not None
            ):
                raise ValueError("Judgment Skill Trace rejected entry differs from the Skill Route")
        evidence_ids = {item.evidence_id for item in evidence_pack.evidence}
        valid_paths = _valid_proposal_paths(artifact)
        for entry in entries:
            if not set(entry.trigger_evidence_refs) <= evidence_ids:
                raise ValueError(
                    "Judgment Skill Trace references evidence outside the Evidence Pack"
                )
            if not set(entry.influenced_proposal_paths) <= valid_paths:
                raise ValueError("Judgment Skill Trace references an unknown proposal path")
        artifact_hash = canonical_hash(artifact.to_dict())
        binding_hash = execution_binding.binding_hash
        core = {
            "schema_version": JUDGMENT_SKILL_TRACE_SCHEMA,
            "observed_at": _timestamp(observed_at),
            "run_id": artifact.run_id,
            "judgment_artifact_id": artifact.artifact_id,
            "judgment_artifact_hash": artifact_hash,
            "route_id": route.route_id,
            "route_hash": route.route_hash,
            "execution_binding_hash": binding_hash,
            "entries": [item.to_dict() for item in entries],
            "agent_report_authority": "observational_self_report_not_causal_evidence",
            "signal_or_execution_authority": False,
        }
        return cls(
            trace_id=f"judgment-skill-trace-{canonical_hash(core)}",
            observed_at=observed_at,
            run_id=artifact.run_id,
            judgment_artifact_id=artifact.artifact_id,
            judgment_artifact_hash=artifact_hash,
            route_id=route.route_id,
            route_hash=route.route_hash,
            execution_binding_hash=binding_hash,
            entries=entries,
        )


def judgment_skill_trace_from_dict(value: object) -> JudgmentSkillTrace:
    payload = _object(value, "Judgment Skill Trace")
    if payload.get("schema_version") != JUDGMENT_SKILL_TRACE_SCHEMA:
        raise ValueError("unsupported Judgment Skill Trace schema_version")
    trace = JudgmentSkillTrace(
        trace_id=_string(payload, "trace_id"),
        observed_at=_datetime(payload, "observed_at"),
        run_id=_string(payload, "run_id"),
        judgment_artifact_id=_string(payload, "judgment_artifact_id"),
        judgment_artifact_hash=_string(payload, "judgment_artifact_hash"),
        route_id=_string(payload, "route_id"),
        route_hash=_string(payload, "route_hash"),
        execution_binding_hash=_string(payload, "execution_binding_hash"),
        entries=tuple(
            _trace_entry(item)
            for item in _object_list(payload.get("entries"), "Skill Trace entries")
        ),
    )
    if trace.to_dict() != payload:
        raise ValueError("Judgment Skill Trace does not match the canonical contract")
    return trace


def _trace_entry(value: object) -> JudgmentSkillTraceEntry:
    payload = _object(value, "Judgment Skill Trace entry")
    return JudgmentSkillTraceEntry(
        skill_name=_string(payload, "skill_name"),
        manifest_hash=_optional_string(payload, "manifest_hash"),
        offer_disposition=_enum(
            SkillOfferDisposition,
            payload.get("offer_disposition"),
            "Skill offer disposition",
        ),
        route_disposition=_enum(
            SkillRouteDisposition,
            payload.get("route_disposition"),
            "Skill route disposition",
        ),
        loaded=_boolean(payload, "loaded"),
        route_reasons=_string_tuple(payload, "route_reasons"),
        agent_reported_use=_enum(
            AgentReportedSkillUse,
            payload.get("agent_reported_use"),
            "Agent-reported Skill use",
        ),
        trigger_evidence_refs=_string_tuple(payload, "trigger_evidence_refs"),
        influenced_proposal_paths=_string_tuple(payload, "influenced_proposal_paths"),
        agent_rationale=_string(payload, "agent_rationale"),
    )


def _valid_proposal_paths(artifact: JudgmentArtifact) -> set[str]:
    proposal = artifact.proposal
    paths = {"/decision", "/summary", "/decision_confidence", "/stopped_reason"}
    paths.update(
        f"/transmission_steps/{index}" for index, _ in enumerate(proposal.transmission_steps)
    )
    paths.update(f"/candidates/{index}" for index, _ in enumerate(proposal.candidates))
    paths.update(f"/blockers/{index}" for index, _ in enumerate(proposal.blockers))
    paths.update(
        f"/unresolved_questions/{index}" for index, _ in enumerate(proposal.unresolved_questions)
    )
    return paths


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} must be a lowercase identifier")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} has an invalid prefix")
    _sha256(value.removeprefix(prefix), name)


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    _unique(values, name)
    if not values:
        raise ValueError(f"{name} cannot be empty")
    for value in values:
        _nonempty(value, name)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], dict(raw))


def _object_list(value: object, name: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, name) for item in cast(Sequence[object], value))


def _string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _optional_string(payload: Mapping[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string or null")
    return value


def _string_tuple(payload: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an array")
    result: list[str] = []
    for item in cast(Sequence[object], value):
        if not isinstance(item, str):
            raise TypeError(f"{name} items must be strings")
        result.append(item)
    return tuple(result)


def _boolean(payload: Mapping[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _datetime(payload: Mapping[str, object], name: str) -> datetime:
    value = _string(payload, name)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(result, name)
    return result.astimezone(UTC)


def _enum[EnumT: StrEnum](enum_type: type[EnumT], value: object, name: str) -> EnumT:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"{name} is unsupported") from exc
