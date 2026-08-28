from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import DataPITLane
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability
from market_impact_agent.prospective_collection_runtime import (
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionRuntime,
)

PROSPECTIVE_COLLECTION_TRACER_REPORT_SCHEMA = (
    "market-impact.prospective-collection-tracer-report.v1"
)


class ProspectiveCollectionTracerGate(StrEnum):
    REQUIRED_ROUTE_SET = "required_route_set"
    ACCEPTED_ROUTE_BINDING = "accepted_route_binding"
    TYPED_OPPORTUNITIES = "typed_opportunities"
    ACTUAL_RECEIPT_SNAPSHOTS = "actual_receipt_snapshots"
    INTERVAL_COMPLETENESS = "interval_completeness"
    AUTHORITY_ISOLATION = "authority_isolation"


@dataclass(frozen=True, slots=True)
class ProspectiveCollectionTracerGateResult:
    gate: ProspectiveCollectionTracerGate
    passed: bool
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.passed and self.reasons:
            raise ValueError("passing tracer gate cannot carry reasons")
        if not self.passed and not self.reasons:
            raise ValueError("failing tracer gate requires reasons")
        if len(self.reasons) != len(set(self.reasons)):
            raise ValueError("tracer gate reasons must be unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "gate": self.gate.value,
            "passed": self.passed,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveCollectionTracerRoute:
    job_id: str
    adapter_kind: ProspectiveCollectionAdapterKind
    capability: ObservationCapability
    collection_policy_id: str
    source_acceptance_report_id: str
    opportunity_id: str | None
    scheduled_for: datetime | None
    completed_at: datetime | None
    outcome: str | None
    data_snapshot_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "adapter_kind": self.adapter_kind.value,
            "capability": self.capability.value,
            "collection_policy_id": self.collection_policy_id,
            "source_acceptance_report_id": self.source_acceptance_report_id,
            "opportunity_id": self.opportunity_id,
            "scheduled_for": (
                None if self.scheduled_for is None else _timestamp(self.scheduled_for)
            ),
            "completed_at": None if self.completed_at is None else _timestamp(self.completed_at),
            "outcome": self.outcome,
            "data_snapshot_id": self.data_snapshot_id,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveCollectionTracerReport:
    report_id: str
    evaluated_at: datetime
    routes: tuple[ProspectiveCollectionTracerRoute, ...]
    gates: tuple[ProspectiveCollectionTracerGateResult, ...]
    accepted: bool
    historical_pit_claim: bool = False
    model_authority: bool = False
    host_supervisor_installed: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_COLLECTION_TRACER_REPORT_SCHEMA

    def __post_init__(self) -> None:
        _strict_utc(self.evaluated_at, "tracer evaluated_at")
        expected_gates = tuple(ProspectiveCollectionTracerGate)
        if tuple(item.gate for item in self.gates) != expected_gates:
            raise ValueError("tracer report gates are incomplete or out of order")
        if self.accepted != all(item.passed for item in self.gates):
            raise ValueError("tracer report accepted flag does not match its gates")
        if (
            self.historical_pit_claim
            or self.model_authority
            or self.host_supervisor_installed
            or self.execution_capability
        ):
            raise ValueError(
                "tracer report cannot grant PIT, model, supervisor, or execution authority"
            )
        if self.report_id != self.expected_report_id:
            raise ValueError("tracer report_id does not match content")

    @property
    def expected_report_id(self) -> str:
        return f"prospective-collection-tracer-report-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "evaluated_at": _timestamp(self.evaluated_at),
            "routes": [item.to_dict() for item in self.routes],
            "gates": [item.to_dict() for item in self.gates],
            "accepted": self.accepted,
            "historical_pit_claim": self.historical_pit_claim,
            "model_authority": self.model_authority,
            "host_supervisor_installed": self.host_supervisor_installed,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "report_id": self.report_id}


def qualify_prospective_collection_tracer(
    *,
    runtime: ProspectiveCollectionRuntime,
    job_ids: tuple[str, ...],
    evaluated_at: datetime,
) -> ProspectiveCollectionTracerReport:
    _strict_utc(evaluated_at, "tracer evaluated_at")
    if len(job_ids) != 2 or len(set(job_ids)) != 2:
        raise ValueError("prospective collection tracer requires exactly two distinct jobs")

    routes: list[ProspectiveCollectionTracerRoute] = []
    route_set_reasons: list[str] = []
    binding_reasons: list[str] = []
    opportunity_reasons: list[str] = []
    receipt_reasons: list[str] = []
    interval_reasons: list[str] = []
    isolation_reasons: list[str] = []
    expected = {
        ProspectiveCollectionAdapterKind.CSRC_NEWS: ObservationCapability.EVENT_REVELATION,
        ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION: ObservationCapability.MARKET_CONTEXT,
    }
    seen_kinds: set[ProspectiveCollectionAdapterKind] = set()
    policy_ids: set[str] = set()

    for job_id in sorted(job_ids):
        job = runtime.job(job_id)
        policy = runtime.journal.policy(job.collection_policy_id)
        report = runtime.source_acceptance_report(job_id)
        opportunities = runtime.opportunities(job_id)
        opportunity = opportunities[-1] if opportunities else None
        health = runtime.health(job_id, now=evaluated_at)
        seen_kinds.add(job.adapter_kind)
        policy_ids.add(policy.policy_id)
        if expected.get(job.adapter_kind) is not policy.capability:
            route_set_reasons.append(f"{job_id}:unexpected_capability")
        if (
            not report.accepted
            or report.report_id != job.source_acceptance_report_id
            or report.declaration.capability is not policy.capability
        ):
            binding_reasons.append(f"{job_id}:accepted_route_binding_invalid")
        if opportunity is None:
            opportunity_reasons.append(f"{job_id}:opportunity_missing")
        elif opportunity.outcome not in {
            "success",
            "source_failure",
            "collector_failure",
            "cancelled",
            "missed",
        }:
            opportunity_reasons.append(f"{job_id}:opportunity_not_terminal")
        elif opportunity.completed_at is None:
            opportunity_reasons.append(f"{job_id}:opportunity_completion_missing")
        elif opportunity.completed_at > evaluated_at:
            opportunity_reasons.append(f"{job_id}:opportunity_completed_after_evaluation")

        snapshot_id = None if opportunity is None else opportunity.data_snapshot_id
        snapshot = None
        if opportunity is None or opportunity.outcome != "success" or snapshot_id is None:
            receipt_reasons.append(f"{job_id}:successful_snapshot_missing")
        else:
            try:
                snapshot = runtime.store.get(snapshot_id)
            except (FileNotFoundError, KeyError, ValueError):
                receipt_reasons.append(f"{job_id}:snapshot_storage_invalid")
            if snapshot is not None:
                if (
                    opportunity.completed_at is None
                    or opportunity.completed_at < snapshot.completed_at
                    or any(
                        item.retrieved_at > opportunity.completed_at for item in snapshot.attempts
                    )
                    or any(
                        item.times.retrieved_at > opportunity.completed_at
                        for item in snapshot.observations
                    )
                ):
                    receipt_reasons.append(f"{job_id}:opportunity_completion_precedes_snapshot")
                if snapshot.completed_at > evaluated_at:
                    receipt_reasons.append(f"{job_id}:snapshot_completed_after_evaluation")
                if any(item.retrieved_at > evaluated_at for item in snapshot.attempts) or any(
                    item.times.retrieved_at > evaluated_at
                    or (
                        item.times.available_at is not None
                        and item.times.available_at > evaluated_at
                    )
                    or (item.authority_at is not None and item.authority_at > evaluated_at)
                    for item in snapshot.observations
                ):
                    receipt_reasons.append(f"{job_id}:actual_receipt_after_evaluation")
                if (
                    snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE
                    or not snapshot.coverage_complete
                    or not snapshot.observations
                    or any(
                        item.times.availability_basis is not AvailabilityBasis.ACTUAL_RECEIPT
                        or item.times.available_at != item.times.retrieved_at
                        or item.authority_at != item.times.retrieved_at
                        for item in snapshot.observations
                    )
                ):
                    receipt_reasons.append(f"{job_id}:actual_receipt_contract_invalid")
        if (
            health.missed_opportunities
            or health.source_failures
            or health.collector_failures
            or health.cancelled_opportunities
            or health.incomplete_interval
        ):
            interval_reasons.append(f"{job_id}:interval_incomplete")
        if evaluated_at > health.next_due_at + timedelta(
            seconds=job.misfire_grace_seconds
        ) and not any(item.scheduled_for == health.next_due_at for item in opportunities):
            interval_reasons.append(f"{job_id}:overdue_opportunity_unmaterialized")
        routes.append(
            ProspectiveCollectionTracerRoute(
                job_id=job.job_id,
                adapter_kind=job.adapter_kind,
                capability=policy.capability,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
                opportunity_id=None if opportunity is None else opportunity.opportunity_id,
                scheduled_for=None if opportunity is None else opportunity.scheduled_for,
                completed_at=None if opportunity is None else opportunity.completed_at,
                outcome=None if opportunity is None else opportunity.outcome,
                data_snapshot_id=snapshot_id,
            )
        )

    if seen_kinds != set(expected):
        route_set_reasons.append("required_adapter_set_missing")
    if len(policy_ids) != 2:
        isolation_reasons.append("collection_policies_not_distinct")
    if any(runtime.job(item.job_id).execution_capability for item in routes):
        isolation_reasons.append("job_execution_capability_present")

    reason_sets = (
        route_set_reasons,
        binding_reasons,
        opportunity_reasons,
        receipt_reasons,
        interval_reasons,
        isolation_reasons,
    )
    gates = tuple(
        ProspectiveCollectionTracerGateResult(
            gate=gate,
            passed=not reasons,
            reasons=tuple(dict.fromkeys(reasons)),
        )
        for gate, reasons in zip(ProspectiveCollectionTracerGate, reason_sets, strict=True)
    )
    sorted_routes = tuple(sorted(routes, key=lambda item: item.adapter_kind.value))
    core = {
        "schema_version": PROSPECTIVE_COLLECTION_TRACER_REPORT_SCHEMA,
        "evaluated_at": _timestamp(evaluated_at),
        "routes": [item.to_dict() for item in sorted_routes],
        "gates": [item.to_dict() for item in gates],
        "accepted": all(item.passed for item in gates),
        "historical_pit_claim": False,
        "model_authority": False,
        "host_supervisor_installed": False,
        "execution_capability": False,
    }
    return ProspectiveCollectionTracerReport(
        report_id=f"prospective-collection-tracer-report-{canonical_hash(core)}",
        evaluated_at=evaluated_at,
        routes=sorted_routes,
        gates=gates,
        accepted=all(item.passed for item in gates),
    )


def write_prospective_collection_tracer_report(
    report: ProspectiveCollectionTracerReport,
    *,
    state_root: Path,
) -> Path:
    errors = validate_agent_contract(
        report.to_dict(), "prospective-collection-tracer-report.schema.json"
    )
    if errors:
        raise ValueError("; ".join(errors))
    root = (state_root / "collection-tracers").resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    target = root / f"{report.report_id}.json"
    encoded = (
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode()
    if target.exists():
        if target.read_bytes() != encoded:
            raise ValueError("tracer report identity has conflicting content")
        os.chmod(target, 0o600)
        return target
    descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-tracer-", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
