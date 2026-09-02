from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    ProspectiveEvidenceLineage,
    canonical_hash,
)
from market_impact_agent.agent_engine import AgentExecutionBinding
from market_impact_agent.agent_runtime import ToolSideEffect
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.checkpoint_decision_inputs import project_checkpoint_observation
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    FrozenDataSnapshotInput,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_checkpoint_sets import (
    CheckpointRouteSelection,
    ProspectiveCheckpointSnapshotSet,
    build_checkpoint_tool_descriptors,
    materialize_checkpoint_decision_inputs,
    reconcile_prospective_checkpoint_snapshot_set,
)
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
    prospective_observation_version_id,
)
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    REQUIRED_DIAGNOSTIC_CAPABILITIES,
    CapabilityApplicability,
    DiagnosticCapabilitySlot,
    DiagnosticCutoffRule,
    DiagnosticMechanism,
    ProspectiveDiagnosticCheckpoint,
    ProspectiveDiagnosticRegistration,
)
from market_impact_agent.prospective_execution import (
    PairedArmExecutionBinding,
    ProspectiveExecutionPlan,
)
from market_impact_agent.prospective_query_gate import evaluate_prospective_query_gate
from market_impact_agent.prospective_trigger_admission import (
    PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA,
    ProspectiveTriggerAdmission,
    TriggerAdmissionKind,
)
from market_impact_agent.research import EvidenceTier
from market_impact_agent.source_acceptance import (
    SourceAcceptanceGate,
    SourceAcceptanceGateResult,
    SourceAcceptanceStatus,
    SourceRouteAcceptanceDeclaration,
    SourceRouteAcceptanceReport,
)

REGISTERED = datetime(2026, 8, 28, 5, 0, tzinfo=UTC)
RECEIVED = REGISTERED + timedelta(minutes=30)
BARRIER = REGISTERED + timedelta(hours=1)
WINDOW_START = REGISTERED + timedelta(minutes=25)
MANIFEST_HASH = canonical_hash({"provider": "official-example", "version": "1"})
CONFIG_HASH = canonical_hash({"source": "official-event"})


def _slot(
    capability: ObservationCapability,
    *,
    required: bool,
    required_route_kinds: tuple[str, ...] = ("official_event",),
    maximum_age_seconds: int = 3600,
) -> DiagnosticCapabilitySlot:
    if required:
        return DiagnosticCapabilitySlot(
            capability=capability,
            applicability=CapabilityApplicability.REQUIRED,
            not_applicable_reason=None,
            required_route_kinds=required_route_kinds,
            minimum_data_sources=1,
            minimum_observations=1,
            poll_interval_seconds=60,
            maximum_gap_seconds=3600,
            maximum_age_seconds=maximum_age_seconds,
        )
    return DiagnosticCapabilitySlot(
        capability=capability,
        applicability=CapabilityApplicability.NOT_APPLICABLE,
        not_applicable_reason="Not needed by this contract fixture.",
        required_route_kinds=(),
        minimum_data_sources=0,
        minimum_observations=0,
        poll_interval_seconds=0,
        maximum_gap_seconds=0,
        maximum_age_seconds=0,
    )


def _checkpoint(
    key: str,
    mechanism: DiagnosticMechanism,
    *,
    required_capability: ObservationCapability = ObservationCapability.EVENT_REVELATION,
    required_route_kinds: tuple[str, ...] = ("official_event",),
    maximum_age_seconds: int = 3600,
) -> ProspectiveDiagnosticCheckpoint:
    return ProspectiveDiagnosticCheckpoint(
        checkpoint_key=key,
        name=key.replace("-", " "),
        mechanism=mechanism,
        selection_rule="first_eligible_after_registration",
        eligibility_rule="First eligible source-confirmed event after registration.",
        eligibility_source_classes=("official_source",),
        exclusion_rules=("Exclude events received after cutoff.",),
        cutoff=DiagnosticCutoffRule(
            timezone="Asia/Shanghai",
            session_boundary="after_market_close",
            market_close_local="15:00:00",
            decision_delay_seconds=1800,
        ),
        capability_slots=tuple(
            _slot(
                item,
                required=item is required_capability,
                required_route_kinds=required_route_kinds,
                maximum_age_seconds=maximum_age_seconds,
            )
            for item in sorted(REQUIRED_DIAGNOSTIC_CAPABILITIES, key=lambda value: value.value)
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
        candidate_horizon_sessions=(1, 5, 20),
    )


def _registration(
    *,
    required_capability: ObservationCapability = ObservationCapability.EVENT_REVELATION,
    required_route_kinds: tuple[str, ...] = ("official_event",),
    maximum_age_seconds: int = 3600,
) -> ProspectiveDiagnosticRegistration:
    return ProspectiveDiagnosticRegistration.build(
        registered_at=REGISTERED,
        checkpoints=(
            _checkpoint(
                "policy-event",
                DiagnosticMechanism.POLICY_REGULATION,
                required_capability=required_capability,
                required_route_kinds=required_route_kinds,
                maximum_age_seconds=maximum_age_seconds,
            ),
            _checkpoint(
                "macro-event",
                DiagnosticMechanism.MACRO_CYCLE,
                required_capability=required_capability,
                required_route_kinds=required_route_kinds,
                maximum_age_seconds=maximum_age_seconds,
            ),
        ),
        paired_arms=("structured_agent_core", "structured_agent_plus_routed_methods"),
        replicates_per_arm=3,
        model_profile_id="cliproxyapi-luna-xhigh-v1",
        aggregate_model_cost_limit_usd="20.00",
        outcome_opening_rule="do_not_open_until_all_paired_judgments_are_sealed",
        stop_conditions=("required_snapshot_incomplete",),
        go_conditions=("all_required_slots_reconciled",),
        claim_scope="process_diagnostic_only_no_alpha_or_execution_claim",
    )


def _v2_partial_registration() -> ProspectiveDiagnosticRegistration:
    def checkpoint(key: str, mechanism: DiagnosticMechanism) -> ProspectiveDiagnosticCheckpoint:
        return ProspectiveDiagnosticCheckpoint(
            checkpoint_key=key,
            name=key.replace("-", " "),
            mechanism=mechanism,
            selection_rule="first_eligible_after_registration",
            eligibility_rule="First actual-receipt event after registration.",
            eligibility_source_classes=("observed_source",),
            exclusion_rules=("Exclude observations received after cutoff.",),
            cutoff=DiagnosticCutoffRule(
                timezone="Asia/Shanghai",
                session_boundary="after_market_close",
                market_close_local="15:00:00",
                decision_delay_seconds=1800,
            ),
            capability_slots=tuple(
                DiagnosticCapabilitySlot(
                    capability=capability,
                    applicability=(
                        CapabilityApplicability.REQUIRED
                        if capability is ObservationCapability.EVENT_REVELATION
                        else CapabilityApplicability.OPTIONAL
                    ),
                    not_applicable_reason=None,
                    required_route_kinds=(
                        ("official_event", "established_news")
                        if capability is ObservationCapability.EVENT_REVELATION
                        else (f"{capability.value}_observation",)
                    ),
                    minimum_data_sources=1,
                    minimum_observations=1,
                    poll_interval_seconds=60,
                    maximum_gap_seconds=3600,
                    maximum_age_seconds=3600,
                )
                for capability in sorted(
                    REQUIRED_DIAGNOSTIC_CAPABILITIES,
                    key=lambda item: item.value,
                )
            ),
            target_venues=("XSHG", "XSHE"),
            allowed_instrument_classes=("exchange_traded_fund",),
            candidate_horizon_sessions=(1, 5, 20),
        )

    return ProspectiveDiagnosticRegistration.build(
        registered_at=REGISTERED,
        checkpoints=(
            checkpoint("policy-event-v2", DiagnosticMechanism.POLICY_REGULATION),
            checkpoint("macro-event-v2", DiagnosticMechanism.MACRO_CYCLE),
        ),
        paired_arms=("structured_agent_core", "structured_agent_plus_routed_methods"),
        replicates_per_arm=3,
        model_profile_id="cliproxyapi-luna-xhigh-v1",
        aggregate_model_cost_limit_usd="20.00",
        outcome_opening_rule="do_not_open_until_all_paired_judgments_are_sealed",
        stop_conditions=("structural_query_gate_failed",),
        go_conditions=("actual_receipt_event_trigger_available",),
        claim_scope="process_diagnostic_only_no_alpha_or_execution_claim",
        schema_version=PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    )


def _v4_partial_registration() -> ProspectiveDiagnosticRegistration:
    v2 = _v2_partial_registration()
    return ProspectiveDiagnosticRegistration.build(
        registered_at=v2.registered_at,
        checkpoints=v2.checkpoints,
        paired_arms=v2.paired_arms,
        replicates_per_arm=v2.replicates_per_arm,
        model_profile_id="cliproxyapi-luna-xhigh-cpa-v1",
        aggregate_model_cost_limit_usd=v2.aggregate_model_cost_limit_usd,
        outcome_opening_rule=v2.outcome_opening_rule,
        stop_conditions=v2.stop_conditions,
        go_conditions=v2.go_conditions,
        claim_scope=v2.claim_scope,
        minimum_replicates_per_arm=2,
        replicate_schedule_rule=(
            "run_two_paired_replicates_then_third_pair_if_either_arm_disagrees"
        ),
        schema_version=PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    )


def _trigger_admission(
    registration: ProspectiveDiagnosticRegistration,
    frozen: DataSnapshot,
) -> ProspectiveTriggerAdmission:
    versions = tuple(
        sorted(prospective_observation_version_id(item) for item in frozen.observations)
    )
    core = {
        "schema_version": PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA,
        "kind": TriggerAdmissionKind.CHECKPOINT_ELIGIBLE.value,
        "registration_id": registration.registration_id,
        "checkpoint_key": "policy-event-v2",
        "candidate_set_id": "event-impact-triage-candidate-set-" + "1" * 64,
        "proposal_id": "event-impact-triage-proposal-" + "2" * 64,
        "triage_decision_id": "event-impact-triage-decision-" + "3" * 64,
        "cluster_id": "event-impact-triage-cluster-" + "4" * 64,
        "observation_version_ids": list(versions),
        "event_assessment_id": None,
        "materiality_gate_result_id": None,
        "preceding_materiality_gate_result_ids": [],
        "admitted_target_ids": [],
        "held_target_ids": [],
        "admitted_at": RECEIVED.isoformat().replace("+00:00", "Z"),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return ProspectiveTriggerAdmission(
        admission_id=f"prospective-trigger-admission-{canonical_hash(core)}",
        kind=TriggerAdmissionKind.CHECKPOINT_ELIGIBLE,
        registration_id=registration.registration_id,
        checkpoint_key="policy-event-v2",
        candidate_set_id=cast(str, core["candidate_set_id"]),
        proposal_id=cast(str, core["proposal_id"]),
        triage_decision_id=cast(str, core["triage_decision_id"]),
        cluster_id=cast(str, core["cluster_id"]),
        observation_version_ids=versions,
        event_assessment_id=None,
        materiality_gate_result_id=None,
        preceding_materiality_gate_result_ids=(),
        admitted_target_ids=(),
        held_target_ids=(),
        admitted_at=RECEIVED,
    )


class _TriggerAuthority:
    def __init__(self, admission: ProspectiveTriggerAdmission) -> None:
        self.admission = admission

    def assert_authoritative(self, admission: ProspectiveTriggerAdmission) -> None:
        if admission != self.admission:
            raise ValueError("Trigger Admission differs from durable authority")


def _evidence_pack() -> EvidencePack:
    return EvidencePack.build(
        event_id="policy-event-v2",
        as_of=BARRIER,
        research_question="What can be inferred from the information actually observed?",
        evidence=(
            EvidenceReference(
                evidence_id="prospective-event-observation",
                claim_id="observed-event",
                source_ref="https://official.example/event",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=RECEIVED,
                content_hash=sha256(b"event").hexdigest(),
                summary="An event was observed before the checkpoint barrier.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("510300.XSHG",),
        data_gaps=("prior_expectation unavailable",),
    )


def _lineaged_evidence_pack(
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    frozen: DataSnapshot,
) -> tuple[EvidencePack, dict[str, object]]:
    observation = frozen.observations[0]
    available_at = observation.times.available_at
    assert available_at is not None
    decision_input = project_checkpoint_observation(
        checkpoint_snapshot_set_id=snapshot_set.snapshot_set_id,
        checkpoint_key=snapshot_set.checkpoint_key,
        barrier_at=snapshot_set.barrier_at,
        snapshot_id=frozen.snapshot_id,
        route_kinds=("official_event",),
        observation=observation,
    )
    return (
        EvidencePack.build(
            event_id="policy-event-v2",
            as_of=BARRIER,
            research_question="What can be inferred from the information actually observed?",
            evidence=(
                EvidenceReference(
                    evidence_id="prospective-event-observation",
                    claim_id="observed-event",
                    source_ref=observation.source_ref,
                    source_tier=EvidenceTier.OFFICIAL,
                    available_at=available_at,
                    content_hash=observation.raw_content_hash,
                    summary="An event was observed before the checkpoint barrier.",
                    prospective_lineage=ProspectiveEvidenceLineage(
                        snapshot_id=frozen.snapshot_id,
                        observation_id=observation.observation_id,
                        checkpoint_decision_input_id=cast(str, decision_input["record_id"]),
                    ),
                ),
            ),
            pattern_packs=(),
            allowed_targets=("510300.XSHG",),
            data_gaps=("prior_expectation unavailable",),
        ),
        decision_input,
    )


def _execution_binding() -> AgentExecutionBinding:
    digest = canonical_hash({"fixture": "query-gate"})
    return AgentExecutionBinding(
        runtime_ref="market-impact-agent-runtime-v1",
        runtime_config_hash=digest,
        prompt_hash=digest,
        skill_hashes=(),
        tool_manifest_hashes=(digest,),
        tool_surface_hash=digest,
        mcp_server_hashes=(),
        context_estimator_id="context-estimator-v1",
        compactor_id="compactor-v1",
    )


def _execution_plan(
    registration: ProspectiveDiagnosticRegistration | None = None,
) -> ProspectiveExecutionPlan:
    selected_registration = _v2_partial_registration() if registration is None else registration
    profile_path = (
        "examples/providers/cliproxyapi-luna-xhigh-cpa-v1.json"
        if selected_registration.model_profile_id.endswith("-cpa-v1")
        else "examples/providers/cliproxyapi-luna-xhigh-v1.json"
    )
    profile = load_model_provider_profile(Path(profile_path))
    control = _execution_binding()
    treatment = AgentExecutionBinding(
        runtime_ref=control.runtime_ref,
        runtime_config_hash=control.runtime_config_hash,
        prompt_hash=control.prompt_hash,
        skill_hashes=(canonical_hash({"fixture": "treatment-skill"}),),
        tool_manifest_hashes=control.tool_manifest_hashes,
        tool_surface_hash=control.tool_surface_hash,
        mcp_server_hashes=control.mcp_server_hashes,
        context_estimator_id=control.context_estimator_id,
        compactor_id=control.compactor_id,
    )
    return ProspectiveExecutionPlan.build(
        registration=selected_registration,
        model_profile_alias=selected_registration.model_profile_id,
        model_profile=profile,
        arm_bindings=(
            PairedArmExecutionBinding(
                arm=selected_registration.paired_arms[0], execution_binding=control
            ),
            PairedArmExecutionBinding(
                arm=selected_registration.paired_arms[1], execution_binding=treatment
            ),
        ),
    )


def _source(upstream_source: str = "official-event") -> DataSourceBinding:
    return DataSourceBinding(
        provider_id="official-example",
        provider_version="1",
        upstream_source=upstream_source,
        manifest_hash=MANIFEST_HASH,
        source_config_hash=canonical_hash({"source": upstream_source}),
        required=True,
    )


def _policy(
    *,
    capability: ObservationCapability = ObservationCapability.EVENT_REVELATION,
    sources: tuple[DataSourceBinding, ...] | None = None,
) -> ProspectiveCollectionPolicy:
    return ProspectiveCollectionPolicy.build(
        capability=capability,
        sources=(_source(),) if sources is None else sources,
        window_start=WINDOW_START,
        parameters={"max_items": 20},
        poll_interval_seconds=60,
        maximum_gap_seconds=3600,
    )


def _receipt_snapshot(
    store: LocalDataSnapshotStore,
    policy: ProspectiveCollectionPolicy,
    *,
    observation_sources: tuple[DataSourceBinding, ...] | None = None,
    received_at: datetime = RECEIVED,
    normalized_payload: dict[str, object] | None = None,
) -> DataSnapshot:
    selected_sources = policy.sources if observation_sources is None else observation_sources
    observations: list[SourceObservation] = []
    attempts: list[DataProviderAttempt] = []
    for index, source in enumerate(policy.sources, start=1):
        has_observation = source in selected_sources
        record_hash = store.put_raw(f'{{"source":"{source.upstream_source}"}}'.encode())
        response_hash = store.put_raw(
            f'{{"source":"{source.upstream_source}","count":{int(has_observation)}}}'.encode()
        )
        if has_observation:
            payload = (
                {"headline": "Policy event", "publisher": "Official"}
                if normalized_payload is None
                else normalized_payload
            )
            observations.append(
                SourceObservation.build(
                    capability=policy.capability,
                    provider_id=source.provider_id,
                    provider_version=source.provider_version,
                    upstream_source=source.upstream_source,
                    upstream_record_id=f"event-{index}",
                    source_ref=f"https://official.example/events/{index}",
                    lineage_id=f"{source.source_key}:event-{index}",
                    times=ObservationTimes(
                        occurred_at=received_at - timedelta(minutes=5),
                        published_at=received_at - timedelta(minutes=5),
                        available_at=received_at,
                        source_updated_at=received_at - timedelta(minutes=5),
                        aggregator_fetched_at=None,
                        retrieved_at=received_at,
                        occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
                        availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
                    ),
                    authority_at=received_at,
                    authority_kind="actual_receipt",
                    raw_content_hash=record_hash,
                    normalized_payload=payload,
                    license_scope="private_research_no_redistribution",
                )
            )
        attempts.append(
            DataProviderAttempt(
                provider_id=source.provider_id,
                provider_version=source.provider_version,
                upstream_source=source.upstream_source,
                required=True,
                status=DataFetchStatus.DATA if has_observation else DataFetchStatus.NO_DATA,
                retrieved_at=received_at,
                raw_response_hash=response_hash,
                received_count=int(has_observation),
                accepted_count=int(has_observation),
                rejected_missing_availability=0,
                rejected_after_cutoff=0,
                rejected_missing_authority=0,
                rejected_authority_after_cutoff=0,
                rejected_lane_mismatch=0,
                error_kind=None,
            )
        )
    query = DataQuery.build(
        capability=policy.capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=received_at,
        window_start=policy.window_start,
        source_policy_id=policy.policy_id,
        parameters=policy.parameters,
        sources=policy.sources,
        minimum_data_sources=1,
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [item.to_dict() for item in attempts],
        "observations": [item.to_dict() for item in observations],
        "coverage_complete": True,
        "completed_at": received_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=tuple(attempts),
        observations=tuple(observations),
        coverage_complete=True,
        completed_at=received_at,
    )
    store.put(snapshot)
    return snapshot


def _accepted_report(
    sample_snapshot_id: str,
    *,
    source: DataSourceBinding | None = None,
    capability: ObservationCapability = ObservationCapability.EVENT_REVELATION,
) -> SourceRouteAcceptanceReport:
    accepted_source = _source() if source is None else source
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=accepted_source.provider_id,
        provider_version=accepted_source.provider_version,
        provider_manifest_hash=MANIFEST_HASH,
        source_config_hash=cast(str, accepted_source.source_config_hash),
        upstream_source=accepted_source.upstream_source,
        capability=capability,
        rights_basis_url="https://official.example/terms",
        rights_reviewed_at=REGISTERED,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=False,
        semantic_scope="Official prospective event receipt.",
        revision_strategy="append_only_content_versions",
    )
    gates = tuple(
        SourceAcceptanceGateResult(
            gate=item.value,
            status=SourceAcceptanceStatus.PASS.value,
            reasons=(),
        )
        for item in SourceAcceptanceGate
    )
    core = {
        "schema_version": "market-impact.source-route-acceptance-report.v1",
        "declaration": declaration.to_dict(),
        "rights_evidence": None,
        "data_snapshot_id": sample_snapshot_id,
        "deterministic_replay_snapshot_id": sample_snapshot_id,
        "evaluated_at": RECEIVED.isoformat().replace("+00:00", "Z"),
        "gates": [item.to_dict() for item in gates],
        "accepted": True,
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }
    return SourceRouteAcceptanceReport(
        report_id=f"source-route-acceptance-report-{canonical_hash(core)}",
        declaration=declaration,
        rights_evidence=None,
        data_snapshot_id=sample_snapshot_id,
        deterministic_replay_snapshot_id=sample_snapshot_id,
        evaluated_at=RECEIVED,
        gates=gates,
        accepted=True,
    )


def _fixture(
    tmp_path: Path,
) -> tuple[
    LocalDataSnapshotStore,
    ProspectiveDataJournal,
    ProspectiveCollectionPolicy,
    DataSnapshot,
    SourceRouteAcceptanceReport,
]:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    policy = _policy()
    receipt = _receipt_snapshot(store, policy)
    journal.record_snapshot(receipt, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=BARRIER,
        window_start=policy.window_start,
        minimum_data_sources=1,
        frozen_at=BARRIER + timedelta(seconds=1),
    )
    return store, journal, policy, frozen, _accepted_report(receipt.snapshot_id)


def test_reconcile_builds_non_authoritative_snapshot_set_and_read_only_tool(
    tmp_path: Path,
) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    selection = CheckpointRouteSelection(
        capability=ObservationCapability.EVENT_REVELATION,
        route_kind="official_event",
        snapshot_id=frozen.snapshot_id,
        collection_policy_id=policy.policy_id,
        source_acceptance_report_id=report.report_id,
    )

    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=_registration(),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        selections=(selection,),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports={report.report_id: report},
        reconciled_at=BARRIER + timedelta(seconds=2),
    )

    assert snapshot_set.complete is True
    assert snapshot_set.schema_version == "market-impact.prospective-checkpoint-snapshot-set.v4"
    assert snapshot_set.historical_pit_claim is False
    assert snapshot_set.execution_capability is False
    assert snapshot_set.frozen_input == FrozenDataSnapshotInput(
        authorized_snapshot_ids=frozenset({frozen.snapshot_id})
    )
    assert (
        validate_agent_contract(
            snapshot_set.to_dict(),
            "prospective-checkpoint-snapshot-set.schema.json",
        )
        == ()
    )
    legacy_payload = deepcopy(snapshot_set.to_dict())
    legacy_payload["schema_version"] = "market-impact.prospective-checkpoint-snapshot-set.v1"
    for binding in cast(list[dict[str, object]], legacy_payload["capability_bindings"]):
        manifest = binding["tool_manifest"]
        if isinstance(manifest, dict):
            cast(dict[str, object], manifest)["version"] = "1"
    assert (
        validate_agent_contract(
            legacy_payload,
            "prospective-checkpoint-snapshot-set.schema.json",
        )
        == ()
    )
    cast(
        dict[str, object],
        cast(list[dict[str, object]], legacy_payload["capability_bindings"])[0]["tool_manifest"],
    )["version"] = "2"
    assert validate_agent_contract(
        legacy_payload,
        "prospective-checkpoint-snapshot-set.schema.json",
    )
    descriptors = build_checkpoint_tool_descriptors(
        snapshot_set,
        store=store,
        frozen_input=snapshot_set.frozen_input,
        authorized_decision_input_ids=frozenset(
            cast(str, item["record_id"])
            for item in materialize_checkpoint_decision_inputs(snapshot_set, store=store)
        ),
        required_capability="data.snapshot.read",
    )
    assert [item.name for item in descriptors] == ["lookup_event_revelation"]
    assert descriptors[0].side_effect is ToolSideEffect.READ_ONLY
    assert descriptors[0].version.startswith("2+")

    async def invoke_tool() -> object:
        return await descriptors[0].handler(
            {"filters": {"publisher": "Official"}, "query": "policy", "limit": 5}
        )

    result_value = asyncio.run(invoke_tool())
    assert isinstance(result_value, dict)
    result = cast(dict[str, object], result_value)
    assert result["schema_version"] == "market-impact.checkpoint-data-tool-result.v2"
    assert cast(str, result["result_id"]).startswith("checkpoint-data-tool-result-")
    assert result["checkpoint_snapshot_set_id"] == snapshot_set.snapshot_set_id
    assert "observations" not in result
    records = cast(list[dict[str, object]], result["records"])
    assert len(records) == 1
    assert records[0]["route_kinds"] == ["official_event"]
    assert records[0]["capability"] == "event_revelation"
    assert records[0]["record_type"] == "event_fact"
    assert records[0]["data"] == {
        "event_type": None,
        "headline": "Policy event",
        "industry": None,
        "instrument_code": None,
        "publisher": "Official",
        "source_url": "https://official.example/events/1",
        "statement": None,
    }
    assert records[0]["price_basis"] is None
    assert records[0]["completeness_gaps"] == []
    assert "normalized_payload" not in records[0]
    assert "observation" not in records[0]


def test_reconcile_rejects_missing_registered_route(tmp_path: Path) -> None:
    store, journal, policy, _, report = _fixture(tmp_path)

    with pytest.raises(ValueError, match="registered route kinds"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=(),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports={report.report_id: report},
            reconciled_at=BARRIER + timedelta(seconds=2),
        )


def test_reconcile_rejects_barrier_drift_or_unaccepted_route(tmp_path: Path) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    selection = CheckpointRouteSelection(
        capability=ObservationCapability.EVENT_REVELATION,
        route_kind="official_event",
        snapshot_id=frozen.snapshot_id,
        collection_policy_id=policy.policy_id,
        source_acceptance_report_id=report.report_id,
    )
    with pytest.raises(ValueError, match="barrier"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(),
            checkpoint_key="policy-event",
            barrier_at=BARRIER + timedelta(seconds=1),
            selections=(selection,),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports={report.report_id: report},
            reconciled_at=BARRIER + timedelta(seconds=2),
        )

    object.__setattr__(report, "accepted", False)
    with pytest.raises(ValueError, match="accepted route"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=(selection,),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports={report.report_id: report},
            reconciled_at=BARRIER + timedelta(seconds=2),
        )


def test_reconcile_rejects_post_hoc_not_applicable_selection(tmp_path: Path) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    required_selection = CheckpointRouteSelection(
        capability=ObservationCapability.EVENT_REVELATION,
        route_kind="official_event",
        snapshot_id=frozen.snapshot_id,
        collection_policy_id=policy.policy_id,
        source_acceptance_report_id=report.report_id,
    )
    post_hoc_selection = CheckpointRouteSelection(
        capability=ObservationCapability.PRIOR_EXPECTATION,
        route_kind="official_event",
        snapshot_id=frozen.snapshot_id,
        collection_policy_id=policy.policy_id,
        source_acceptance_report_id=report.report_id,
    )
    with pytest.raises(ValueError, match="not_applicable slot"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=(required_selection, post_hoc_selection),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports={report.report_id: report},
            reconciled_at=BARRIER + timedelta(seconds=2),
        )


def test_tool_binding_rejects_snapshot_outside_frozen_run_input(tmp_path: Path) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    selection = CheckpointRouteSelection(
        capability=ObservationCapability.EVENT_REVELATION,
        route_kind="official_event",
        snapshot_id=frozen.snapshot_id,
        collection_policy_id=policy.policy_id,
        source_acceptance_report_id=report.report_id,
    )
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=_registration(),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        selections=(selection,),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports={report.report_id: report},
        reconciled_at=BARRIER + timedelta(seconds=2),
    )

    with pytest.raises(ValueError, match="enclosing run input"):
        build_checkpoint_tool_descriptors(
            snapshot_set,
            store=store,
            frozen_input=FrozenDataSnapshotInput(
                authorized_snapshot_ids=frozenset({"data-snapshot-other"})
            ),
            authorized_decision_input_ids=frozenset(
                cast(str, item["record_id"])
                for item in materialize_checkpoint_decision_inputs(snapshot_set, store=store)
            ),
            required_capability="data.snapshot.read",
        )


def test_reconcile_rejects_snapshot_sources_without_selected_acceptance(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    selected_source = _source()
    unselected_source = _source("unselected-event")
    policy = _policy(sources=(selected_source, unselected_source))
    receipt = _receipt_snapshot(store, policy)
    journal.record_snapshot(receipt, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=BARRIER,
        window_start=policy.window_start,
        minimum_data_sources=1,
        frozen_at=BARRIER + timedelta(seconds=1),
    )
    report = _accepted_report(receipt.snapshot_id, source=selected_source)
    selection = CheckpointRouteSelection(
        capability=ObservationCapability.EVENT_REVELATION,
        route_kind="official_event",
        snapshot_id=frozen.snapshot_id,
        collection_policy_id=policy.policy_id,
        source_acceptance_report_id=report.report_id,
    )

    with pytest.raises(ValueError, match="unselected or unaccepted sources"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=(selection,),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports={report.report_id: report},
            reconciled_at=BARRIER + timedelta(seconds=2),
        )


def test_reconcile_requires_an_observation_for_each_selected_route(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    official_source = _source()
    secondary_source = _source("secondary-event")
    policy = _policy(sources=(official_source, secondary_source))
    receipt = _receipt_snapshot(
        store,
        policy,
        observation_sources=(official_source,),
        received_at=BARRIER - timedelta(seconds=30),
    )
    journal.record_snapshot(receipt, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=BARRIER,
        window_start=policy.window_start,
        minimum_data_sources=1,
        frozen_at=BARRIER + timedelta(seconds=1),
    )
    selections: list[CheckpointRouteSelection] = []
    reports: dict[str, SourceRouteAcceptanceReport] = {}
    for route_kind, source in (
        ("official_event", official_source),
        ("secondary_event", secondary_source),
    ):
        report = _accepted_report(receipt.snapshot_id, source=source)
        selections.append(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind=route_kind,
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            )
        )
        reports[report.report_id] = report

    with pytest.raises(ValueError, match="each selected route requires an observation"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(
                required_route_kinds=("official_event", "secondary_event"),
                maximum_age_seconds=60,
            ),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=tuple(selections),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports=reports,
            reconciled_at=BARRIER + timedelta(seconds=2),
        )


def test_reconcile_requires_freshness_for_each_selected_route(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    official_source = _source()
    secondary_source = _source("secondary-event")
    policy = _policy(sources=(official_source, secondary_source))
    stale_receipt = _receipt_snapshot(
        store,
        policy,
        received_at=BARRIER - timedelta(seconds=120),
        normalized_payload={"headline": "Earlier event"},
    )
    journal.record_snapshot(stale_receipt, policy=policy)
    fresh_official_receipt = _receipt_snapshot(
        store,
        policy,
        observation_sources=(official_source,),
        received_at=BARRIER - timedelta(seconds=30),
        normalized_payload={"headline": "Updated official event"},
    )
    journal.record_snapshot(fresh_official_receipt, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=BARRIER,
        window_start=policy.window_start,
        minimum_data_sources=1,
        frozen_at=BARRIER + timedelta(seconds=1),
    )
    reports: dict[str, SourceRouteAcceptanceReport] = {}
    selections: list[CheckpointRouteSelection] = []
    for route_kind, source in (
        ("official_event", official_source),
        ("secondary_event", secondary_source),
    ):
        report = _accepted_report(stale_receipt.snapshot_id, source=source)
        reports[report.report_id] = report
        selections.append(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind=route_kind,
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            )
        )

    with pytest.raises(ValueError, match="each selected route requires a fresh observation"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(
                required_route_kinds=("official_event", "secondary_event"),
                maximum_age_seconds=60,
            ),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=tuple(selections),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports=reports,
            reconciled_at=BARRIER + timedelta(seconds=2),
        )


def test_checkpoint_tool_binds_route_kinds_to_each_observation_source(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    official_source = _source("official-event")
    secondary_source = _source("secondary-event")
    policy = _policy(sources=(official_source, secondary_source))
    receipt = _receipt_snapshot(store, policy)
    journal.record_snapshot(receipt, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=BARRIER,
        window_start=policy.window_start,
        minimum_data_sources=2,
        frozen_at=BARRIER + timedelta(seconds=1),
    )
    reports: dict[str, SourceRouteAcceptanceReport] = {}
    selections: list[CheckpointRouteSelection] = []
    for route_kind, source in (
        ("official_event", official_source),
        ("secondary_event", secondary_source),
    ):
        report = _accepted_report(receipt.snapshot_id, source=source)
        reports[report.report_id] = report
        selections.append(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind=route_kind,
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            )
        )
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=_registration(
            required_route_kinds=("official_event", "secondary_event"),
        ),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        selections=tuple(selections),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports=reports,
        reconciled_at=BARRIER + timedelta(seconds=2),
    )
    descriptor = build_checkpoint_tool_descriptors(
        snapshot_set,
        store=store,
        frozen_input=snapshot_set.frozen_input,
        authorized_decision_input_ids=frozenset(
            cast(str, item["record_id"])
            for item in materialize_checkpoint_decision_inputs(snapshot_set, store=store)
        ),
        required_capability="data.snapshot.read",
    )[0]

    async def invoke_tool() -> object:
        return await descriptor.handler({"limit": 10})

    result_value = asyncio.run(invoke_tool())

    assert isinstance(result_value, dict)
    records = cast(list[dict[str, object]], result_value["records"])
    route_kinds_by_source = {
        cast(dict[str, object], record["source"])["upstream_source"]: record["route_kinds"]
        for record in records
    }
    assert route_kinds_by_source == {
        "official-event": ["official_event"],
        "secondary-event": ["secondary_event"],
    }

    materialized = materialize_checkpoint_decision_inputs(snapshot_set, store=store)
    one_authorized_id = cast(str, materialized[0]["record_id"])
    restricted_descriptor = build_checkpoint_tool_descriptors(
        snapshot_set,
        store=store,
        frozen_input=snapshot_set.frozen_input,
        authorized_decision_input_ids=frozenset({one_authorized_id}),
        required_capability="data.snapshot.read",
    )[0]

    async def invoke_restricted_tool() -> object:
        return await restricted_descriptor.handler({"limit": 10})

    restricted_value = asyncio.run(invoke_restricted_tool())
    assert isinstance(restricted_value, dict)
    restricted_records = cast(list[dict[str, object]], restricted_value["records"])
    assert [item["record_id"] for item in restricted_records] == [one_authorized_id]


@pytest.mark.parametrize(
    ("capability", "record", "filters"),
    (
        (
            ObservationCapability.EVENT_REVELATION,
            {
                "title": "Policy impact",
                "channels": "policy",
                "industry_name": "Banks",
                "ts_code": "600000.SH",
            },
            {
                "headline": "Policy impact",
                "event_type": "policy",
                "industry": "Banks",
                "instrument_code": "600000.SH",
            },
        ),
        (
            ObservationCapability.PRIOR_EXPECTATION,
            {
                "ts_code": "600000.SH",
                "report_date": "20260828",
                "author_name": "Synthetic analyst",
                "report_title": "Policy impact",
            },
            {
                "instrument_code": "600000.SH",
                "analyst": "Synthetic analyst",
                "report_date": "20260828",
            },
        ),
        (
            ObservationCapability.MARKET_CONTEXT,
            {
                "api_name": "index_daily",
                "ts_code": "000300.SH",
                "trade_date": "20260828",
            },
            {"index_code": "000300.SH", "trade_date": "20260828"},
        ),
        (
            ObservationCapability.EXPOSURE_CANDIDATES,
            {"ts_code": "510300.SH", "exchange": "SH"},
            {"instrument_code": "510300.SH", "venue": "SH"},
        ),
        (
            ObservationCapability.POSITIONING,
            {"exchange_id": "SSE", "trade_date": "20260828"},
            {"market": "SSE", "trade_date": "20260828"},
        ),
        (
            ObservationCapability.MACRO_VINTAGE,
            {
                "data_api": "cn_cpi",
                "month": "202607",
                "publish_date": "20260809",
            },
            {
                "indicator": "cn_cpi",
                "reference_period": "202607",
                "release_date": "20260809",
            },
        ),
    ),
)
def test_checkpoint_tool_literal_query_and_exact_filters_across_capabilities(
    tmp_path: Path,
    capability: ObservationCapability,
    record: dict[str, object],
    filters: dict[str, str],
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    source = _source(f"tushare-{capability.value.replace('_', '-')}")
    policy = _policy(capability=capability, sources=(source,))
    receipt = _receipt_snapshot(
        store,
        policy,
        normalized_payload={
            "aggregator": "Tushare Pro",
            "upstream_publisher": "Tushare Pro",
            "record": record,
        },
    )
    journal.record_snapshot(receipt, policy=policy)
    frozen = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=BARRIER,
        window_start=policy.window_start,
        minimum_data_sources=1,
        frozen_at=BARRIER + timedelta(seconds=1),
    )
    report = _accepted_report(
        receipt.snapshot_id,
        source=source,
        capability=capability,
    )
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=_registration(required_capability=capability),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        selections=(
            CheckpointRouteSelection(
                capability=capability,
                route_kind="official_event",
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            ),
        ),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports={report.report_id: report},
        reconciled_at=BARRIER + timedelta(seconds=2),
    )
    descriptor = build_checkpoint_tool_descriptors(
        snapshot_set,
        store=store,
        frozen_input=snapshot_set.frozen_input,
        authorized_decision_input_ids=frozenset(
            cast(str, item["record_id"])
            for item in materialize_checkpoint_decision_inputs(snapshot_set, store=store)
        ),
        required_capability="data.snapshot.read",
    )[0]

    async def read_records(arguments: dict[str, object]) -> list[dict[str, object]]:
        result = await descriptor.handler(arguments)
        assert isinstance(result, dict)
        return cast(list[dict[str, object]], result["records"])

    async def exercise_queries() -> None:
        records = await read_records({})
        assert len(records) == 1
        assert await read_records({"publisher": "Tushare Pro", "filters": filters}) == records
        assert await read_records({"query": next(iter(filters.values()))}) == records
        # The legacy tool uses literal search, not a natural-language retrieval plan.
        assert await read_records({"query": "Find exact records for the next five sessions"}) == []
        for field in filters:
            # Every predicate is conjunctive, and "unknown" is a value, not omission.
            assert await read_records({"filters": {**filters, field: "unknown"}}) == [], field
        assert await read_records({"publisher": "Unrelated publisher"}) == []

    asyncio.run(exercise_queries())


def test_industry_membership_projection_is_effective_dated_without_backfill(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    source = _source("tushare-index-member-all")
    policy = _policy(
        capability=ObservationCapability.EXPOSURE_CANDIDATES,
        sources=(source,),
    )
    snapshot = _receipt_snapshot(
        store,
        policy,
        normalized_payload={
            "aggregator": "Tushare Pro",
            "api_name": "index_member_all",
            "upstream_publisher": "Shenwan Hongyuan Research",
            "record": {
                "l1_code": "801010.SI",
                "l1_name": "Agriculture",
                "l2_code": None,
                "l2_name": None,
                "l3_code": None,
                "l3_name": None,
                "ts_code": "600000.SH",
                "name": "Synthetic issuer",
                "in_date": "20260101",
                "out_date": "20261231",
            },
        },
    )

    projected = project_checkpoint_observation(
        checkpoint_snapshot_set_id=("prospective-checkpoint-snapshot-set-" + "1" * 64),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        snapshot_id=snapshot.snapshot_id,
        route_kinds=("effective_industry_membership",),
        observation=snapshot.observations[0],
    )

    assert projected["record_type"] == "industry_membership"
    data = cast(dict[str, object], projected["data"])
    assert data["industry_code"] == "801010.SI"
    assert data["industry_name"] == "Agriculture"
    assert data["taxonomy_level"] == "l1"
    assert data["instrument_code"] == "600000.SH"
    assert data["effective_from"] == "20260101"
    assert data["effective_to"] == "20261231"
    assert data["effective_at_barrier"] is True
    assert projected["completeness_gaps"] == [
        "industry_to_tradable_mapping_missing",
        "taxonomy_version_unverified",
    ]


def test_daily_tradability_limit_projection_binds_date_and_raw_limits(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    policy = _policy(
        capability=ObservationCapability.EXPOSURE_CANDIDATES,
        sources=(_source("tushare-stk-limit"),),
    )
    snapshot = _receipt_snapshot(
        store,
        policy,
        normalized_payload={
            "aggregator": "Tushare Pro",
            "api_name": "stk_limit",
            "upstream_publisher": "Tushare Pro",
            "record": {
                "ts_code": "600000.SH",
                "trade_date": "20260828",
                "pre_close": 10.0,
                "up_limit": 11.0,
                "down_limit": 9.0,
            },
        },
    )

    projected = project_checkpoint_observation(
        checkpoint_snapshot_set_id=("prospective-checkpoint-snapshot-set-" + "3" * 64),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        snapshot_id=snapshot.snapshot_id,
        route_kinds=("daily_tradability_limit",),
        observation=snapshot.observations[0],
    )

    assert projected["record_type"] == "daily_tradability_limit"
    data = cast(dict[str, object], projected["data"])
    assert data["effective_from"] == "20260828"
    assert data["effective_to"] == "20260828"
    assert data["effective_at_barrier"] is True
    assert data["previous_close"] == 10.0
    assert data["upper_price_limit"] == 11.0
    assert data["lower_price_limit"] == 9.0


def test_index_projection_keeps_research_and_execution_price_bases_separate(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    policy = _policy(
        capability=ObservationCapability.MARKET_CONTEXT,
        sources=(_source("tushare-index-daily"),),
    )
    snapshot = _receipt_snapshot(
        store,
        policy,
        normalized_payload={
            "aggregator": "Tushare Pro",
            "api_name": "index_daily",
            "upstream_publisher": "Tushare Pro",
            "record": {
                "ts_code": "000300.SH",
                "trade_date": "20260828",
                "open": 4100.0,
                "high": 4120.0,
                "low": 4090.0,
                "close": 4110.0,
                "pre_close": 4080.0,
                "vol": 12345.0,
                "amount": 67890.0,
            },
        },
    )
    projected = project_checkpoint_observation(
        checkpoint_snapshot_set_id=("prospective-checkpoint-snapshot-set-" + "2" * 64),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        snapshot_id=snapshot.snapshot_id,
        route_kinds=("market_index_price",),
        observation=snapshot.observations[0],
    )

    assert projected == project_checkpoint_observation(
        checkpoint_snapshot_set_id=("prospective-checkpoint-snapshot-set-" + "2" * 64),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        snapshot_id=snapshot.snapshot_id,
        route_kinds=("market_index_price",),
        observation=snapshot.observations[0],
    )
    assert cast(str, projected["record_id"]).startswith("checkpoint-decision-input-")
    assert projected["record_type"] == "index_price_bar"
    assert projected["price_basis"] == {
        "as_of_adjusted": False,
        "execution_basis": None,
        "execution_eligible": False,
        "instrument_type": "price_index",
        "research_basis": "price_index",
        "total_return": False,
    }
    assert projected["completeness_gaps"] == ["total_return_series_missing"]
    assert projected["execution_capability"] is False
    times = cast(dict[str, object], projected["times"])
    assert times["published_at"] == "2026-08-28T05:25:00Z"
    assert times["source_updated_at"] == "2026-08-28T05:25:00Z"
    assert times["available_at"] == "2026-08-28T05:30:00Z"
    assert times["authority_at"] == "2026-08-28T05:30:00Z"
    assert times["retrieved_at"] == "2026-08-28T05:30:00Z"


def test_partial_v2_snapshot_set_keeps_optional_gaps_without_blocking_model_run(
    tmp_path: Path,
) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    registration = _v2_partial_registration()
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=registration,
        checkpoint_key="policy-event-v2",
        barrier_at=BARRIER,
        selections=(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="official_event",
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            ),
        ),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports={report.report_id: report},
        reconciled_at=BARRIER + timedelta(minutes=1),
        allow_partial=True,
    )

    assert snapshot_set.complete is False
    assert len(snapshot_set.capability_gaps) == 6
    assert (
        validate_agent_contract(
            snapshot_set.to_dict(),
            "prospective-checkpoint-snapshot-set.schema.json",
        )
        == ()
    )
    evidence_pack, decision_input = _lineaged_evidence_pack(snapshot_set, frozen)
    gate = evaluate_prospective_query_gate(
        registration=registration,
        snapshot_set=snapshot_set,
        evidence_pack=evidence_pack,
        decision_inputs=(decision_input,),
        snapshot_store=store,
        execution_plan=_execution_plan(),
        model_profile_id=registration.model_profile_id,
        model_cost_limit_usd=Decimal("5.00"),
        evaluated_at=BARRIER + timedelta(minutes=2),
    )

    assert gate.model_run_eligible is True
    assert gate.blocking_required_gaps == ()
    assert len(gate.nonblocking_information_gaps) == 7
    assert (
        "event_revelation:missing_registered_routes:established_news"
        in gate.nonblocking_information_gaps
    )
    assert gate.historical_pit_claim is False
    assert gate.execution_capability is False
    assert gate.frozen_input.authorized_snapshot_ids == frozenset({frozen.snapshot_id})
    assert (
        validate_agent_contract(
            gate.to_dict(),
            "prospective-query-gate-result.schema.json",
        )
        == ()
    )


def test_v4_query_gate_requires_the_exact_trigger_admission(
    tmp_path: Path,
) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    registration = _v4_partial_registration()
    trigger_admission = _trigger_admission(registration, frozen)
    trigger_authority = _TriggerAuthority(trigger_admission)
    with pytest.raises(ValueError, match="Trigger Admission authority"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=registration,
            checkpoint_key="policy-event-v2",
            barrier_at=BARRIER,
            selections=(),
            store=store,
            journal=journal,
            policies={policy.policy_id: policy},
            acceptance_reports={report.report_id: report},
            reconciled_at=BARRIER + timedelta(minutes=1),
            allow_partial=True,
            trigger_admission=trigger_admission,
        )
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=registration,
        checkpoint_key="policy-event-v2",
        barrier_at=BARRIER,
        selections=(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="official_event",
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            ),
        ),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports={report.report_id: report},
        reconciled_at=BARRIER + timedelta(minutes=1),
        allow_partial=True,
        trigger_admission=trigger_admission,
        trigger_admission_authority=trigger_authority,
    )
    evidence_pack, decision_input = _lineaged_evidence_pack(snapshot_set, frozen)

    gate = evaluate_prospective_query_gate(
        registration=registration,
        snapshot_set=snapshot_set,
        trigger_admission=trigger_admission,
        trigger_admission_authority=trigger_authority,
        evidence_pack=evidence_pack,
        decision_inputs=(decision_input,),
        snapshot_store=store,
        execution_plan=_execution_plan(registration),
        model_profile_id=registration.model_profile_id,
        model_cost_limit_usd=Decimal("5.00"),
        evaluated_at=BARRIER + timedelta(minutes=2),
    )

    assert snapshot_set.trigger_admission_id == trigger_admission.admission_id
    assert gate.trigger_admission_id == trigger_admission.admission_id
    assert gate.model_profile_id == "cliproxyapi-luna-xhigh-cpa-v1"
    assert gate.model_run_eligible is True

    unrelated = _trigger_admission(registration, frozen)
    unrelated_core = unrelated.core_dict()
    unrelated_core["cluster_id"] = "event-impact-triage-cluster-" + "9" * 64
    unrelated = replace(
        unrelated,
        admission_id=f"prospective-trigger-admission-{canonical_hash(unrelated_core)}",
        cluster_id=cast(str, unrelated_core["cluster_id"]),
    )
    with pytest.raises(ValueError, match="Trigger Admission"):
        evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=snapshot_set,
            trigger_admission=unrelated,
            trigger_admission_authority=trigger_authority,
            evidence_pack=evidence_pack,
            decision_inputs=(decision_input,),
            snapshot_store=store,
            execution_plan=_execution_plan(registration),
            model_profile_id=registration.model_profile_id,
            model_cost_limit_usd=Decimal("5.00"),
            evaluated_at=BARRIER + timedelta(minutes=2),
        )


def test_snapshot_set_reconciles_multiple_sources_under_one_semantic_route(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    sources = (_source("news-one"), _source("news-two"))
    policies = tuple(_policy(sources=(source,)) for source in sources)
    frozen_snapshots: list[DataSnapshot] = []
    reports: list[SourceRouteAcceptanceReport] = []
    for policy, source in zip(policies, sources, strict=True):
        receipt = _receipt_snapshot(store, policy)
        journal.record_snapshot(receipt, policy=policy)
        frozen_snapshots.append(
            journal.freeze_snapshot(
                policy_id=policy.policy_id,
                not_after=BARRIER,
                window_start=policy.window_start,
                minimum_data_sources=1,
                frozen_at=BARRIER + timedelta(seconds=1),
            )
        )
        reports.append(_accepted_report(receipt.snapshot_id, source=source))

    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=_v2_partial_registration(),
        checkpoint_key="policy-event-v2",
        barrier_at=BARRIER,
        selections=tuple(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="established_news",
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            )
            for policy, frozen, report in zip(policies, frozen_snapshots, reports, strict=True)
        ),
        store=store,
        journal=journal,
        policies={item.policy_id: item for item in policies},
        acceptance_reports={item.report_id: item for item in reports},
        reconciled_at=BARRIER + timedelta(minutes=1),
        allow_partial=True,
    )
    event_binding = next(
        item
        for item in snapshot_set.capability_bindings
        if item.capability is ObservationCapability.EVENT_REVELATION
    )

    assert len(event_binding.routes) == 2
    assert {item.route_kind for item in event_binding.routes} == {"established_news"}
    assert {item.upstream_source for item in event_binding.routes} == {"news-one", "news-two"}


def test_partial_v2_query_gate_blocks_missing_trigger_but_not_optional_information(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    registration = _v2_partial_registration()
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=registration,
        checkpoint_key="policy-event-v2",
        barrier_at=BARRIER,
        selections=(),
        store=store,
        journal=ProspectiveDataJournal(store),
        policies={},
        acceptance_reports={},
        reconciled_at=BARRIER + timedelta(minutes=1),
        allow_partial=True,
    )

    gate = evaluate_prospective_query_gate(
        registration=registration,
        snapshot_set=snapshot_set,
        evidence_pack=_evidence_pack(),
        decision_inputs=(),
        snapshot_store=store,
        execution_plan=_execution_plan(),
        model_profile_id=registration.model_profile_id,
        model_cost_limit_usd=Decimal("5.00"),
        evaluated_at=BARRIER + timedelta(minutes=2),
    )

    assert gate.model_run_eligible is False
    assert gate.blocking_required_gaps == (
        "event_revelation:missing_registered_routes:established_news,official_event",
    )
    assert all(not gap.startswith("event_revelation:") for gap in gate.nonblocking_information_gaps)


def test_query_gate_rejects_evidence_from_an_unrelated_decision_input(
    tmp_path: Path,
) -> None:
    store, journal, policy, frozen, report = _fixture(tmp_path)
    registration = _v2_partial_registration()
    snapshot_set = reconcile_prospective_checkpoint_snapshot_set(
        registration=registration,
        checkpoint_key="policy-event-v2",
        barrier_at=BARRIER,
        selections=(
            CheckpointRouteSelection(
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="official_event",
                snapshot_id=frozen.snapshot_id,
                collection_policy_id=policy.policy_id,
                source_acceptance_report_id=report.report_id,
            ),
        ),
        store=store,
        journal=journal,
        policies={policy.policy_id: policy},
        acceptance_reports={report.report_id: report},
        reconciled_at=BARRIER + timedelta(minutes=1),
        allow_partial=True,
    )
    evidence_pack, decision_input = _lineaged_evidence_pack(snapshot_set, frozen)
    unrelated = deepcopy(decision_input)
    unrelated["snapshot_id"] = "data-snapshot-" + "9" * 64
    unrelated_core = {key: value for key, value in unrelated.items() if key != "record_id"}
    unrelated["record_id"] = f"checkpoint-decision-input-{canonical_hash(unrelated_core)}"

    with pytest.raises(ValueError, match="frozen Observation projection"):
        evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=snapshot_set,
            evidence_pack=evidence_pack,
            decision_inputs=(unrelated,),
            snapshot_store=store,
            execution_plan=_execution_plan(),
            model_profile_id=registration.model_profile_id,
            model_cost_limit_usd=Decimal("5.00"),
            evaluated_at=BARRIER + timedelta(minutes=2),
        )

    forged = deepcopy(decision_input)
    forged_data = cast(dict[str, object], forged["data"])
    forged_data["headline"] = "Fabricated event"
    forged_source = cast(dict[str, object], forged["source"])
    forged_hash = sha256(b"fabricated-event").hexdigest()
    forged_source["raw_content_hash"] = forged_hash
    forged_core = {key: value for key, value in forged.items() if key != "record_id"}
    forged["record_id"] = f"checkpoint-decision-input-{canonical_hash(forged_core)}"
    original_evidence = evidence_pack.evidence[0]
    assert original_evidence.prospective_lineage is not None
    forged_evidence = replace(
        original_evidence,
        content_hash=forged_hash,
        prospective_lineage=replace(
            original_evidence.prospective_lineage,
            checkpoint_decision_input_id=forged["record_id"],
        ),
    )
    forged_pack = EvidencePack.build(
        event_id=evidence_pack.event_id,
        as_of=evidence_pack.as_of,
        research_question=evidence_pack.research_question,
        evidence=(forged_evidence,),
        pattern_packs=evidence_pack.pattern_packs,
        allowed_targets=evidence_pack.allowed_targets,
        data_gaps=evidence_pack.data_gaps,
    )
    with pytest.raises(ValueError, match="frozen Observation projection"):
        evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=snapshot_set,
            evidence_pack=forged_pack,
            decision_inputs=(forged,),
            snapshot_store=store,
            execution_plan=_execution_plan(),
            model_profile_id=registration.model_profile_id,
            model_cost_limit_usd=Decimal("5.00"),
            evaluated_at=BARRIER + timedelta(minutes=2),
        )


def test_v1_snapshot_reconciliation_remains_fail_closed_when_routes_are_missing(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "state")
    with pytest.raises(ValueError, match="selections do not match registered route kinds"):
        reconcile_prospective_checkpoint_snapshot_set(
            registration=_registration(),
            checkpoint_key="policy-event",
            barrier_at=BARRIER,
            selections=(),
            store=store,
            journal=ProspectiveDataJournal(store),
            policies={},
            acceptance_reports={},
            reconciled_at=BARRIER + timedelta(minutes=1),
        )
