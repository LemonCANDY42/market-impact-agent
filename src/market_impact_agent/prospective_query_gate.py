from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import EvidencePack, canonical_hash
from market_impact_agent.checkpoint_decision_inputs import (
    checkpoint_decision_input_from_dict,
)
from market_impact_agent.data_inputs import FrozenDataSnapshotInput, LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_checkpoint_sets import (
    CheckpointRouteReconciliation,
    ProspectiveCheckpointSnapshotSet,
    materialize_checkpoint_decision_inputs,
)
from market_impact_agent.prospective_data import ProspectiveDataJournal
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5,
    CapabilityApplicability,
    ProspectiveDiagnosticRegistration,
)
from market_impact_agent.prospective_execution import ProspectiveExecutionPlan
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveTriggerAdmission,
    TriggerAdmissionAuthority,
    TriggerAdmissionKind,
)

PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V4 = "market-impact.prospective-query-gate-result.v4"
PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V5 = "market-impact.prospective-query-gate-result.v5"
# Backward-compatible constructor default; v5 is selected by v4 registrations.
PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA = PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V4
PROCESS_DIAGNOSTIC_CLAIM_SCOPE = "process_diagnostic_only_no_alpha_or_execution_claim"


@dataclass(frozen=True, slots=True)
class ProspectiveQueryGateResult:
    result_id: str
    registration_id: str
    checkpoint_key: str
    checkpoint_snapshot_set_id: str
    evidence_pack_id: str
    evaluation_material_hash: str
    agent_execution_plan_id: str
    agent_execution_plan_hash: str
    model_profile_id: str
    model_cost_limit_usd: str
    barrier_at: datetime
    evaluated_at: datetime
    authorized_snapshot_ids: tuple[str, ...]
    authorized_decision_input_ids: tuple[str, ...]
    blocking_required_gaps: tuple[str, ...]
    nonblocking_information_gaps: tuple[str, ...]
    model_run_eligible: bool
    claim_scope: str = PROCESS_DIAGNOSTIC_CLAIM_SCOPE
    historical_pit_claim: bool = False
    strategy_promotion_claim: bool = False
    execution_capability: bool = False
    trigger_admission_id: str | None = None
    schema_version: str = PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in {
            PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V4,
            PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V5,
        }:
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
        _sha256(self.evaluation_material_hash, "Query Gate evaluation material hash")
        _prefixed_hash(
            self.agent_execution_plan_id,
            "prospective-execution-plan-",
            "Query Gate execution plan ID",
        )
        _sha256(self.agent_execution_plan_hash, "Query Gate execution plan hash")
        _trimmed(self.model_profile_id, "Query Gate model profile")
        _canonical_positive_usd(self.model_cost_limit_usd)
        if self.authorized_snapshot_ids != tuple(sorted(set(self.authorized_snapshot_ids))):
            raise ValueError("Query Gate Snapshot IDs must be sorted and unique")
        if self.authorized_decision_input_ids != tuple(
            sorted(set(self.authorized_decision_input_ids))
        ):
            raise ValueError("Query Gate Decision Input IDs must be sorted and unique")
        for record_id in self.authorized_decision_input_ids:
            _prefixed_hash(
                record_id,
                "checkpoint-decision-input-",
                "Query Gate Decision Input ID",
            )
        if self.blocking_required_gaps != tuple(sorted(set(self.blocking_required_gaps))):
            raise ValueError("Query Gate blocking gaps must be sorted and unique")
        if self.nonblocking_information_gaps != tuple(
            sorted(set(self.nonblocking_information_gaps))
        ):
            raise ValueError("Query Gate information gaps must be sorted and unique")
        if set(self.blocking_required_gaps) & set(self.nonblocking_information_gaps):
            raise ValueError("Query Gate gaps cannot be both blocking and nonblocking")
        expected_eligible = (
            bool(self.authorized_snapshot_ids)
            and bool(self.authorized_decision_input_ids)
            and not self.blocking_required_gaps
        )
        if self.model_run_eligible != expected_eligible:
            raise ValueError("Query Gate eligibility does not match required inputs")
        if self.claim_scope != PROCESS_DIAGNOSTIC_CLAIM_SCOPE:
            raise ValueError("Query Gate cannot grant an alpha or execution claim")
        if self.historical_pit_claim or self.strategy_promotion_claim or self.execution_capability:
            raise ValueError("Query Gate cannot grant historical, strategy, or execution authority")
        if self.schema_version == PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V5:
            if self.trigger_admission_id is None:
                raise ValueError("v5 Query Gate requires a Trigger Admission")
            _prefixed_hash(
                self.trigger_admission_id,
                "prospective-trigger-admission-",
                "Query Gate Trigger Admission ID",
            )
        elif self.trigger_admission_id is not None:
            raise ValueError("legacy Query Gate cannot carry a Trigger Admission")
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
    def frozen_decision_input_ids(self) -> frozenset[str]:
        if not self.model_run_eligible:
            raise PermissionError("ineligible Query Gate result has no Agent input authority")
        return frozenset(self.authorized_decision_input_ids)

    @property
    def expected_result_id(self) -> str:
        return f"prospective-query-gate-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "checkpoint_snapshot_set_id": self.checkpoint_snapshot_set_id,
            "evidence_pack_id": self.evidence_pack_id,
            "evaluation_material_hash": self.evaluation_material_hash,
            "agent_execution_plan_id": self.agent_execution_plan_id,
            "agent_execution_plan_hash": self.agent_execution_plan_hash,
            "model_profile_id": self.model_profile_id,
            "model_cost_limit_usd": self.model_cost_limit_usd,
            "barrier_at": _timestamp(self.barrier_at),
            "evaluated_at": _timestamp(self.evaluated_at),
            "authorized_snapshot_ids": list(self.authorized_snapshot_ids),
            "authorized_decision_input_ids": list(self.authorized_decision_input_ids),
            "blocking_required_gaps": list(self.blocking_required_gaps),
            "nonblocking_information_gaps": list(self.nonblocking_information_gaps),
            "model_run_eligible": self.model_run_eligible,
            "claim_scope": self.claim_scope,
            "historical_pit_claim": self.historical_pit_claim,
            "strategy_promotion_claim": self.strategy_promotion_claim,
            "execution_capability": self.execution_capability,
        }
        if self.schema_version == PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V5:
            payload["trigger_admission_id"] = self.trigger_admission_id
        return payload

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "result_id": self.result_id}


def evaluate_prospective_query_gate(
    *,
    registration: ProspectiveDiagnosticRegistration,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    evidence_pack: EvidencePack,
    decision_inputs: tuple[Mapping[str, object], ...],
    snapshot_store: LocalDataSnapshotStore,
    execution_plan: ProspectiveExecutionPlan,
    model_profile_id: str,
    model_cost_limit_usd: Decimal,
    evaluated_at: datetime,
    trigger_admission: ProspectiveTriggerAdmission | None = None,
    trigger_admission_authority: TriggerAdmissionAuthority | None = None,
) -> ProspectiveQueryGateResult:
    if registration.schema_version not in {
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5,
    }:
        raise ValueError("partial-information Query Gate requires a v2, v3, or v4 registration")
    checkpoint = registration.checkpoint(snapshot_set.checkpoint_key)
    if snapshot_set.registration_id != registration.registration_id:
        raise ValueError("Query Gate Snapshot Set belongs to a different registration")
    if any(
        (binding.tool_manifest.version == "3") != (registration.checkpoint_tool_version == "3")
        for binding in snapshot_set.capability_bindings
        if binding.tool_manifest is not None
    ):
        raise ValueError("Query Gate checkpoint tool version differs from the registration")
    trigger_bound = registration.schema_version in {
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V5,
    }
    if trigger_bound:
        if trigger_admission is None:
            raise ValueError("v4 Query Gate requires a Trigger Admission")
        if trigger_admission_authority is None:
            raise ValueError("v4 Query Gate requires Trigger Admission authority")
        trigger_admission_authority.assert_authoritative(trigger_admission)
        if (
            snapshot_set.trigger_admission_id != trigger_admission.admission_id
            or trigger_admission.registration_id != registration.registration_id
            or trigger_admission.checkpoint_key != snapshot_set.checkpoint_key
        ):
            raise ValueError("Query Gate Trigger Admission does not match its Snapshot Set")
        if trigger_admission.kind is TriggerAdmissionKind.MATERIAL_EVENT and not set(
            evidence_pack.allowed_targets
        ) <= set(trigger_admission.admitted_target_ids):
            raise ValueError("Query Gate Evidence Pack contains a target outside Materiality Gate")
        if registration.reassessment is not None:
            if trigger_admission.kind is not TriggerAdmissionKind.REGISTERED_REASSESSMENT or (
                trigger_admission.registration_artifact_hash
                != canonical_hash(registration.to_dict())
                or snapshot_set.barrier_at != trigger_admission.admitted_at
            ):
                raise ValueError("Query Gate requires the exact registered reassessment")
            from market_impact_agent.prospective_checkpoint_sets import (
                CheckpointRouteSelection,
                reconcile_prospective_checkpoint_snapshot_set,
            )
            from market_impact_agent.source_acceptance import (
                source_route_acceptance_report_from_dict,
            )

            journal = ProspectiveDataJournal(snapshot_store)
            reports = tuple(
                source_route_acceptance_report_from_dict(snapshot_store.artifacts.read_json(item))
                for item in registration.reassessment.source_acceptance_report_hashes
            )
            selections = tuple(
                CheckpointRouteSelection(
                    capability=binding.capability,
                    route_kind=route.route_kind,
                    snapshot_id=route.snapshot_id,
                    collection_policy_id=route.collection_policy_id,
                    source_acceptance_report_id=route.source_acceptance_report_id,
                )
                for binding in snapshot_set.capability_bindings
                for route in binding.routes
            )
            reconstructed = reconcile_prospective_checkpoint_snapshot_set(
                registration=registration,
                checkpoint_key=snapshot_set.checkpoint_key,
                barrier_at=trigger_admission.admitted_at,
                reconciled_at=trigger_admission.admitted_at,
                selections=selections,
                store=snapshot_store,
                journal=journal,
                policies={
                    item.collection_policy_id: journal.policy(item.collection_policy_id)
                    for item in selections
                },
                acceptance_reports={item.report_id: item for item in reports},
                allow_partial=True,
                trigger_admission=trigger_admission,
                trigger_admission_authority=trigger_admission_authority,
            )
            if reconstructed != snapshot_set:
                raise ValueError(
                    "reassessment Snapshot Set differs from exact receipt reconciliation"
                )
            targets = exact_reassessment_targets(
                registration=registration,
                trigger=trigger_admission,
                journal=ProspectiveDataJournal(snapshot_store),
            )
            if evidence_pack.allowed_targets != targets:
                raise ValueError("reassessment Evidence Pack differs from exact issuer mapping")
    elif trigger_admission is not None or trigger_admission_authority is not None:
        raise ValueError("legacy Query Gate cannot bind Trigger Admission authority")
    if evidence_pack.as_of != snapshot_set.barrier_at:
        raise ValueError("Query Gate Evidence Pack must use the checkpoint barrier")
    if model_profile_id != registration.model_profile_id:
        raise ValueError("Query Gate model profile differs from the registration")
    if execution_plan.registration_id != registration.registration_id:
        raise ValueError("Query Gate execution plan belongs to another registration")
    if execution_plan.model_profile_alias != model_profile_id:
        raise ValueError("Query Gate execution plan binds another Model Profile")
    canonical_cost = _canonical_positive_usd(model_cost_limit_usd)
    if model_cost_limit_usd > Decimal(registration.aggregate_model_cost_limit_usd):
        raise ValueError("Query Gate model cost exceeds the registered aggregate ceiling")
    _strict_utc(evaluated_at, "Query Gate evaluated_at")
    if evaluated_at < snapshot_set.reconciled_at:
        raise ValueError("Query Gate cannot predate Snapshot Set reconciliation")

    authorized_decision_input_ids = _validate_evidence_lineage(
        snapshot_set=snapshot_set,
        evidence_pack=evidence_pack,
        decision_inputs=decision_inputs,
        snapshot_store=snapshot_store,
    )

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
    evaluation_material_hash = canonical_hash(
        build_query_gate_evaluation_material(
            registration=registration,
            snapshot_set=snapshot_set,
            trigger_admission=trigger_admission,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
        )
    )
    blocking_gaps = tuple(sorted(set(blocking)))
    nonblocking_gaps = tuple(sorted(set(nonblocking)))
    gate_schema = (
        PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V5
        if trigger_bound
        else PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V4
    )
    core = {
        "schema_version": gate_schema,
        "registration_id": registration.registration_id,
        "checkpoint_key": checkpoint.checkpoint_key,
        "checkpoint_snapshot_set_id": snapshot_set.snapshot_set_id,
        "evidence_pack_id": evidence_pack.pack_id,
        "evaluation_material_hash": evaluation_material_hash,
        "agent_execution_plan_id": execution_plan.plan_id,
        "agent_execution_plan_hash": canonical_hash(execution_plan.to_dict()),
        "model_profile_id": model_profile_id,
        "model_cost_limit_usd": canonical_cost,
        "barrier_at": _timestamp(snapshot_set.barrier_at),
        "evaluated_at": _timestamp(evaluated_at),
        "authorized_snapshot_ids": list(authorized_snapshot_ids),
        "authorized_decision_input_ids": list(authorized_decision_input_ids),
        "blocking_required_gaps": list(blocking_gaps),
        "nonblocking_information_gaps": list(nonblocking_gaps),
        "model_run_eligible": (
            bool(authorized_snapshot_ids)
            and bool(authorized_decision_input_ids)
            and not blocking_gaps
        ),
        "claim_scope": PROCESS_DIAGNOSTIC_CLAIM_SCOPE,
        "historical_pit_claim": False,
        "strategy_promotion_claim": False,
        "execution_capability": False,
    }
    if gate_schema == PROSPECTIVE_QUERY_GATE_RESULT_SCHEMA_V5:
        assert trigger_admission is not None
        core["trigger_admission_id"] = trigger_admission.admission_id
    return ProspectiveQueryGateResult(
        result_id=f"prospective-query-gate-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint.checkpoint_key,
        checkpoint_snapshot_set_id=snapshot_set.snapshot_set_id,
        evidence_pack_id=evidence_pack.pack_id,
        evaluation_material_hash=evaluation_material_hash,
        agent_execution_plan_id=execution_plan.plan_id,
        agent_execution_plan_hash=canonical_hash(execution_plan.to_dict()),
        model_profile_id=model_profile_id,
        model_cost_limit_usd=canonical_cost,
        barrier_at=snapshot_set.barrier_at,
        evaluated_at=evaluated_at,
        authorized_snapshot_ids=authorized_snapshot_ids,
        authorized_decision_input_ids=authorized_decision_input_ids,
        blocking_required_gaps=blocking_gaps,
        nonblocking_information_gaps=nonblocking_gaps,
        model_run_eligible=(
            bool(authorized_snapshot_ids)
            and bool(authorized_decision_input_ids)
            and not blocking_gaps
        ),
        trigger_admission_id=(
            None if trigger_admission is None else trigger_admission.admission_id
        ),
        schema_version=gate_schema,
    )


def build_query_gate_evaluation_material(
    *,
    registration: ProspectiveDiagnosticRegistration,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    decision_inputs: tuple[Mapping[str, object], ...],
    snapshot_store: LocalDataSnapshotStore,
    trigger_admission: ProspectiveTriggerAdmission | None = None,
) -> dict[str, object]:
    canonical_inputs = tuple(
        sorted(
            (checkpoint_decision_input_from_dict(item) for item in decision_inputs),
            key=lambda item: _required_string(item, "record_id"),
        )
    )
    snapshots = tuple(
        snapshot_store.get(snapshot_id) for snapshot_id in snapshot_set.authorized_snapshot_ids
    )
    payload: dict[str, object] = {
        "schema_version": (
            "market-impact.prospective-query-gate-evaluation-material.v2"
            if trigger_admission is not None
            else "market-impact.prospective-query-gate-evaluation-material.v1"
        ),
        "registration": registration.to_dict(),
        "checkpoint_snapshot_set": snapshot_set.to_dict(),
        "decision_inputs": list(canonical_inputs),
        "snapshots": [item.to_dict() for item in snapshots],
    }
    if trigger_admission is not None:
        payload["trigger_admission"] = trigger_admission.to_dict()
    return payload


def exact_reassessment_targets(
    *,
    registration: ProspectiveDiagnosticRegistration,
    trigger: ProspectiveTriggerAdmission,
    journal: ProspectiveDataJournal,
) -> tuple[str, ...]:
    """Require an exact issuer code and effective stock master, never a catalog fallback."""
    from market_impact_agent.prospective_event_assessment import (
        _record_is_effective,  # pyright: ignore[reportPrivateUsage]
        _record_targets,  # pyright: ignore[reportPrivateUsage]
    )

    subject = registration.reassessment
    if subject is None or trigger.observation_version_ids != subject.subject_version_ids:
        raise ValueError("exact reassessment subject is unavailable")
    codes: set[str] = set()
    for version_id in subject.subject_version_ids:
        _, _, observation = journal.version_receipt(version_id, not_after=trigger.admitted_at)
        record = _required_mapping(observation.normalized_payload, "record")
        codes.add(_required_string(record, "ts_code"))
    if len(codes) != 1:
        raise ValueError("reassessment requires one exact issuer across its original subject")
    targets: set[str] = set()
    checkpoint = registration.checkpoint(trigger.checkpoint_key)
    for version_id in trigger.context_version_ids:
        _, _, observation = journal.version_receipt(version_id, not_after=trigger.admitted_at)
        record = dict(_required_mapping(observation.normalized_payload, "record"))
        if record.get("ts_code") not in codes:
            raise ValueError("reassessment context belongs to another issuer")
        api = observation.normalized_payload.get("api_name")
        if api == "stock_basic" and _record_is_effective(
            "stock_basic", record, trigger.admitted_at
        ):
            for target, venue, instrument_class in _record_targets("stock_basic", record):
                if (
                    venue in checkpoint.target_venues
                    and instrument_class in checkpoint.allowed_instrument_classes
                ):
                    targets.add(target)
        elif api == "daily_basic":
            if observation.capability is not ObservationCapability.MARKET_CONTEXT:
                raise ValueError("daily_basic reassessment context must be market_context")
            received_at = observation.times.available_at
            if received_at is None:
                raise ValueError("daily_basic reassessment context lacks its original receipt")
            _validate_reassessment_daily_context(
                trade_date=record.get("trade_date"),
                received_at=received_at,
                cutoff=trigger.admitted_at,
                maximum_age_seconds=checkpoint.slot(
                    ObservationCapability.MARKET_CONTEXT
                ).maximum_age_seconds,
            )
        else:
            raise ValueError("reassessment context requires effective stock_basic or daily_basic")
    if targets != codes:
        raise ValueError("reassessment blocked: no exact accepted issuer-to-target mapping")
    return tuple(sorted(targets))


def _validate_reassessment_daily_context(
    *,
    trade_date: object,
    received_at: datetime,
    cutoff: datetime,
    maximum_age_seconds: int,
) -> None:
    """Validate a selected current daily_basic row, not historical query availability."""
    if (
        not isinstance(trade_date, str)
        or len(trade_date) != 8
        or not trade_date.isascii()
        or not trade_date.isdigit()
    ):
        raise ValueError("reassessment daily_basic requires a valid YYYYMMDD trade_date")
    try:
        trading_date = datetime.strptime(trade_date, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError("reassessment daily_basic requires a valid YYYYMMDD trade_date") from exc
    # Tushare doc_id=32: trade_date is the trading date, close is that day's close,
    # and updates start at 15:00 China time. This is only the earliest possible
    # completed-day boundary, not a fabricated publication/authority timestamp or calendar.
    earliest_completed = datetime.combine(trading_date, time(15), ZoneInfo("Asia/Shanghai"))
    if earliest_completed > received_at or earliest_completed > cutoff:
        raise ValueError("reassessment daily_basic day was not complete at its receipt or cutoff")
    if (cutoff - earliest_completed).total_seconds() > maximum_age_seconds:
        raise ValueError("reassessment daily_basic effective trade_date is stale at its cutoff")


def _validate_evidence_lineage(
    *,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    evidence_pack: EvidencePack,
    decision_inputs: tuple[Mapping[str, object], ...],
    snapshot_store: LocalDataSnapshotStore,
) -> tuple[str, ...]:
    if not snapshot_set.authorized_snapshot_ids:
        if decision_inputs:
            raise ValueError("blocked Query Gate cannot authorize Decision Inputs")
        return ()

    materialized_inputs = {
        _required_string(item, "record_id"): item
        for item in materialize_checkpoint_decision_inputs(snapshot_set, store=snapshot_store)
    }
    authorized_routes: dict[tuple[str, str], tuple[CheckpointRouteReconciliation, ...]] = {}
    capability_by_pair: dict[tuple[str, str], str] = {}
    for binding in snapshot_set.capability_bindings:
        for route in binding.routes:
            for observation_id in route.observation_ids:
                pair = (route.snapshot_id, observation_id)
                authorized_routes[pair] = (*authorized_routes.get(pair, ()), route)
                capability_by_pair[pair] = binding.capability.value

    canonical_inputs: dict[str, dict[str, object]] = {}
    pairs: set[tuple[str, str]] = set()
    for raw in decision_inputs:
        item = checkpoint_decision_input_from_dict(raw)
        record_id = _required_string(item, "record_id")
        if record_id in canonical_inputs:
            raise ValueError("Query Gate Decision Input IDs must be unique")
        if materialized_inputs.get(record_id) != item:
            raise ValueError("Decision Input differs from its frozen Observation projection")
        if item["checkpoint_snapshot_set_id"] != snapshot_set.snapshot_set_id:
            raise ValueError("Decision Input belongs to a different Snapshot Set")
        if item["checkpoint_key"] != snapshot_set.checkpoint_key:
            raise ValueError("Decision Input belongs to a different checkpoint")
        if item["barrier_at"] != _timestamp(snapshot_set.barrier_at):
            raise ValueError("Decision Input barrier differs from the Snapshot Set")
        pair = (
            _required_string(item, "snapshot_id"),
            _required_string(item, "observation_id"),
        )
        if pair not in authorized_routes:
            raise ValueError("Decision Input is not bound to an authorized observation")
        if pair in pairs:
            raise ValueError("Query Gate cannot authorize duplicate observation projections")
        pairs.add(pair)
        if item["capability"] != capability_by_pair[pair]:
            raise ValueError("Decision Input capability differs from its selected route")
        route_kinds = tuple(sorted({route.route_kind for route in authorized_routes[pair]}))
        if item["route_kinds"] != list(route_kinds):
            raise ValueError("Decision Input route kinds differ from the selected route")
        canonical_inputs[record_id] = item

    if not canonical_inputs:
        raise ValueError("eligible prospective Query Gate requires Decision Inputs")
    for evidence in evidence_pack.evidence:
        lineage = evidence.prospective_lineage
        if lineage is None:
            raise ValueError("prospective evidence lacks Snapshot/Observation/Input lineage")
        item = canonical_inputs.get(lineage.checkpoint_decision_input_id)
        if item is None:
            raise ValueError("prospective evidence references an unauthorized Decision Input")
        if (
            item["snapshot_id"] != lineage.snapshot_id
            or item["observation_id"] != lineage.observation_id
        ):
            raise ValueError("prospective evidence lineage identity does not match its input")
        source = _required_mapping(item, "source")
        times = _required_mapping(item, "times")
        if evidence.source_ref != source.get("source_ref"):
            raise ValueError("prospective evidence source_ref differs from its input")
        if evidence.content_hash != source.get("raw_content_hash"):
            raise ValueError("prospective evidence content hash differs from its input")
        if _timestamp(evidence.available_at) != times.get("available_at"):
            raise ValueError("prospective evidence available_at differs from its input")
    return tuple(sorted(canonical_inputs))


def _required_string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} must be non-empty text")
    return value


def _required_mapping(payload: Mapping[str, object], name: str) -> Mapping[str, object]:
    value = payload.get(name)
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, object], value)


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


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
