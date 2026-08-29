from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from market_impact_agent.agent_contracts import EvidencePack, canonical_hash
from market_impact_agent.agent_engine import AgentExecutionBinding
from market_impact_agent.data_inputs import FrozenDataSnapshotInput
from market_impact_agent.domain import require_aware
from market_impact_agent.prospective_checkpoint_sets import ProspectiveCheckpointSnapshotSet
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    CapabilityApplicability,
    ProspectiveDiagnosticRegistration,
)

PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA = "market-impact.prospective-query-gate-result.v1"
PROCESS_DIAGNOSTIC_CLAIM_SCOPE = "process_diagnostic_only_no_alpha_or_execution_claim"


@dataclass(frozen=True, slots=True)
class ProspectiveQueryGateResult:
    result_id: str
    registration_id: str
    checkpoint_key: str
    checkpoint_snapshot_set_id: str
    evidence_pack_id: str
    agent_execution_binding_hash: str
    model_profile_id: str
    model_cost_limit_usd: str
    barrier_at: datetime
    evaluated_at: datetime
    authorized_snapshot_ids: tuple[str, ...]
    blocking_required_gaps: tuple[str, ...]
    nonblocking_information_gaps: tuple[str, ...]
    model_run_eligible: bool
    claim_scope: str = PROCESS_DIAGNOSTIC_CLAIM_SCOPE
    historical_pit_claim: bool = False
    strategy_promotion_claim: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA:
            raise ValueError("unsupported prospective Query Gate result schema")
        _strict_utc(self.barrier_at, "Query Gate barrier_at")
        _strict_utc(self.evaluated_at, "Query Gate evaluated_at")
        if self.evaluated_at < self.barrier_at:
            raise ValueError("Query Gate cannot evaluate before the checkpoint barrier")
        if not self.registration_id.startswith("prospective-diagnostic-registration-"):
            raise ValueError("Query Gate registration identity is invalid")
        if not self.checkpoint_snapshot_set_id.startswith("prospective-checkpoint-snapshot-set-"):
            raise ValueError("Query Gate Snapshot Set identity is invalid")
        if not self.evidence_pack_id.startswith("evidence-pack-"):
            raise ValueError("Query Gate Evidence Pack identity is invalid")
        _sha256(self.agent_execution_binding_hash, "Query Gate execution binding hash")
        _trimmed(self.model_profile_id, "Query Gate model profile")
        _canonical_positive_usd(self.model_cost_limit_usd)
        if self.authorized_snapshot_ids != tuple(sorted(set(self.authorized_snapshot_ids))):
            raise ValueError("Query Gate Snapshot IDs must be sorted and unique")
        if self.blocking_required_gaps != tuple(sorted(set(self.blocking_required_gaps))):
            raise ValueError("Query Gate blocking gaps must be sorted and unique")
        if self.nonblocking_information_gaps != tuple(
            sorted(set(self.nonblocking_information_gaps))
        ):
            raise ValueError("Query Gate information gaps must be sorted and unique")
        if set(self.blocking_required_gaps) & set(self.nonblocking_information_gaps):
            raise ValueError("Query Gate gaps cannot be both blocking and nonblocking")
        expected_eligible = bool(self.authorized_snapshot_ids) and not self.blocking_required_gaps
        if self.model_run_eligible != expected_eligible:
            raise ValueError("Query Gate eligibility does not match required inputs")
        if self.claim_scope != PROCESS_DIAGNOSTIC_CLAIM_SCOPE:
            raise ValueError("Query Gate cannot grant an alpha or execution claim")
        if self.historical_pit_claim or self.strategy_promotion_claim or self.execution_capability:
            raise ValueError("Query Gate cannot grant historical, strategy, or execution authority")
        if self.result_id != self.expected_result_id:
            raise ValueError("prospective Query Gate result_id does not match content")

    @property
    def frozen_input(self) -> FrozenDataSnapshotInput:
        if not self.model_run_eligible:
            raise PermissionError("ineligible Query Gate result has no Agent input authority")
        return FrozenDataSnapshotInput(
            authorized_snapshot_ids=frozenset(self.authorized_snapshot_ids)
        )

    @property
    def expected_result_id(self) -> str:
        return f"prospective-query-gate-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "checkpoint_snapshot_set_id": self.checkpoint_snapshot_set_id,
            "evidence_pack_id": self.evidence_pack_id,
            "agent_execution_binding_hash": self.agent_execution_binding_hash,
            "model_profile_id": self.model_profile_id,
            "model_cost_limit_usd": self.model_cost_limit_usd,
            "barrier_at": _timestamp(self.barrier_at),
            "evaluated_at": _timestamp(self.evaluated_at),
            "authorized_snapshot_ids": list(self.authorized_snapshot_ids),
            "blocking_required_gaps": list(self.blocking_required_gaps),
            "nonblocking_information_gaps": list(self.nonblocking_information_gaps),
            "model_run_eligible": self.model_run_eligible,
            "claim_scope": self.claim_scope,
            "historical_pit_claim": self.historical_pit_claim,
            "strategy_promotion_claim": self.strategy_promotion_claim,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "result_id": self.result_id}


def evaluate_prospective_query_gate(
    *,
    registration: ProspectiveDiagnosticRegistration,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    evidence_pack: EvidencePack,
    execution_binding: AgentExecutionBinding,
    model_profile_id: str,
    model_cost_limit_usd: Decimal,
    evaluated_at: datetime,
) -> ProspectiveQueryGateResult:
    if registration.schema_version != PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2:
        raise ValueError("partial-information Query Gate requires a v2 registration")
    checkpoint = registration.checkpoint(snapshot_set.checkpoint_key)
    if snapshot_set.registration_id != registration.registration_id:
        raise ValueError("Query Gate Snapshot Set belongs to a different registration")
    if evidence_pack.as_of != snapshot_set.barrier_at:
        raise ValueError("Query Gate Evidence Pack must use the checkpoint barrier")
    if model_profile_id != registration.model_profile_id:
        raise ValueError("Query Gate model profile differs from the registration")
    canonical_cost = _canonical_positive_usd(model_cost_limit_usd)
    if model_cost_limit_usd > Decimal(registration.aggregate_model_cost_limit_usd):
        raise ValueError("Query Gate model cost exceeds the registered aggregate ceiling")
    _strict_utc(evaluated_at, "Query Gate evaluated_at")

    blocking: list[str] = []
    nonblocking: list[str] = []
    for binding in snapshot_set.capability_bindings:
        slot = checkpoint.slot(binding.capability)
        if (
            binding.applicability is not slot.applicability
            or binding.not_applicable_reason != slot.not_applicable_reason
        ):
            raise ValueError("Query Gate Snapshot Set changed registered applicability")
        if not {item.route_kind for item in binding.routes} <= set(slot.required_route_kinds):
            raise ValueError("Query Gate Snapshot Set contains an unregistered route")
    for gap in snapshot_set.capability_gaps:
        capability_value = gap.split(":", maxsplit=1)[0]
        slot = next(
            item
            for item in checkpoint.capability_slots
            if item.capability.value == capability_value
        )
        binding = next(
            item
            for item in snapshot_set.capability_bindings
            if item.capability.value == capability_value
        )
        gap_kind = gap.split(":", maxsplit=2)[1]
        required_gap_blocks = not binding.routes or gap_kind in {
            "source_diversity",
            "observation_count",
            "observations_stale",
        }
        if slot.applicability is CapabilityApplicability.REQUIRED and required_gap_blocks:
            blocking.append(gap)
        else:
            nonblocking.append(gap)
    for binding in snapshot_set.capability_bindings:
        if not binding.routes and not any(
            gap.startswith(f"{binding.capability.value}:") for gap in (*blocking, *nonblocking)
        ):
            missing_gap = f"{binding.capability.value}:missing_observed_input"
            if binding.applicability is CapabilityApplicability.REQUIRED:
                blocking.append(missing_gap)
            elif binding.applicability is CapabilityApplicability.OPTIONAL:
                nonblocking.append(missing_gap)
    nonblocking.extend(f"evidence_pack:{gap}" for gap in evidence_pack.data_gaps)

    authorized_snapshot_ids = snapshot_set.authorized_snapshot_ids
    blocking_gaps = tuple(sorted(set(blocking)))
    nonblocking_gaps = tuple(sorted(set(nonblocking)))
    core = {
        "schema_version": PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA,
        "registration_id": registration.registration_id,
        "checkpoint_key": checkpoint.checkpoint_key,
        "checkpoint_snapshot_set_id": snapshot_set.snapshot_set_id,
        "evidence_pack_id": evidence_pack.pack_id,
        "agent_execution_binding_hash": execution_binding.binding_hash,
        "model_profile_id": model_profile_id,
        "model_cost_limit_usd": canonical_cost,
        "barrier_at": _timestamp(snapshot_set.barrier_at),
        "evaluated_at": _timestamp(evaluated_at),
        "authorized_snapshot_ids": list(authorized_snapshot_ids),
        "blocking_required_gaps": list(blocking_gaps),
        "nonblocking_information_gaps": list(nonblocking_gaps),
        "model_run_eligible": bool(authorized_snapshot_ids) and not blocking_gaps,
        "claim_scope": PROCESS_DIAGNOSTIC_CLAIM_SCOPE,
        "historical_pit_claim": False,
        "strategy_promotion_claim": False,
        "execution_capability": False,
    }
    return ProspectiveQueryGateResult(
        result_id=f"prospective-query-gate-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint.checkpoint_key,
        checkpoint_snapshot_set_id=snapshot_set.snapshot_set_id,
        evidence_pack_id=evidence_pack.pack_id,
        agent_execution_binding_hash=execution_binding.binding_hash,
        model_profile_id=model_profile_id,
        model_cost_limit_usd=canonical_cost,
        barrier_at=snapshot_set.barrier_at,
        evaluated_at=evaluated_at,
        authorized_snapshot_ids=authorized_snapshot_ids,
        blocking_required_gaps=blocking_gaps,
        nonblocking_information_gaps=nonblocking_gaps,
        model_run_eligible=bool(authorized_snapshot_ids) and not blocking_gaps,
    )


def _canonical_positive_usd(value: Decimal | str) -> str:
    amount = value if isinstance(value, Decimal) else Decimal(value)
    if not amount.is_finite() or amount <= 0:
        raise ValueError("Query Gate model cost must be finite and positive")
    canonical = f"{amount:.2f}"
    if Decimal(canonical) != amount:
        raise ValueError("Query Gate model cost must use whole microusd-compatible cents")
    return canonical


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be lowercase SHA-256 text")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
