from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.checkpoint_market_universe import load_exchange_instrument_rule_set
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.modeled_pit_readiness import (
    _materialize_modeled_pit_readiness_checkpoints,  # pyright: ignore[reportPrivateUsage]
    parse_untrusted_modeled_pit_readiness_checkpoint,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_checkpoint_sets import (
    PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA_V4,
    CheckpointCapabilityBinding,
    CheckpointRouteReconciliation,
    CheckpointToolManifest,
    ProspectiveCheckpointSnapshotSet,
    materialize_checkpoint_decision_inputs,
)
from market_impact_agent.prospective_decision_pipeline import (
    FrozenProspectiveDecisionRefs,
    ProspectiveDecisionPipeline,
)
from market_impact_agent.prospective_diagnostic import (
    REQUIRED_DIAGNOSTIC_CAPABILITIES,
    CapabilityApplicability,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.prospective_trigger_admission import (
    PROSPECTIVE_EVENT_ASSESSMENT_SCHEMA,
    PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA,
    ProspectiveEventAssessmentArtifact,
    ProspectiveTriggerAdmission,
    ProspectiveTriggerAdmissionStore,
    TransmissionPath,
    TriggerAdmissionKind,
)
from market_impact_agent.research import TransmissionChannel
from market_impact_agent.runtime_store import ArtifactStore

ROOT = Path(__file__).parents[1]
BARRIER = datetime(2026, 8, 28, 8, tzinfo=UTC)
REGISTRATION = load_prospective_diagnostic_registration(
    ROOT / "examples/research/prospective-diagnostic-registration-v4.json"
)
CHECKPOINT_KEY = "next-material-a-share-event"
RULE_SET = load_exchange_instrument_rule_set(
    ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
)
_TOOL_NAMES = {
    ObservationCapability.EVENT_REVELATION: "lookup_event_revelation",
    ObservationCapability.PRIOR_EXPECTATION: "lookup_prior_expectation",
    ObservationCapability.MARKET_CONTEXT: "lookup_market_context",
    ObservationCapability.EXPOSURE_CANDIDATES: "lookup_exposure_candidates",
    ObservationCapability.POSITIONING: "lookup_positioning",
    ObservationCapability.MACRO_VINTAGE: "lookup_macro_vintage",
}


def _specs(
    *,
    include_expectation: bool = True,
    price_trade_date: str = "20260828",
) -> tuple[tuple[ObservationCapability, str, str, dict[str, object]], ...]:
    values: list[tuple[ObservationCapability, str, str, dict[str, object]]] = [
        (
            ObservationCapability.EVENT_REVELATION,
            "event",
            "event",
            {"record": {"title": "Outage", "summary": "A material outage was confirmed."}},
        ),
        (
            ObservationCapability.MARKET_CONTEXT,
            "calendar",
            "calendar",
            {
                "api_name": "trade_cal",
                "record": {"exchange": "SSE", "cal_date": "20260828", "is_open": "1"},
            },
        ),
        (
            ObservationCapability.MARKET_CONTEXT,
            "fund",
            "price",
            {
                "api_name": "fund_daily",
                "record": {
                    "ts_code": "512010.SH",
                    "trade_date": price_trade_date,
                    "close": "0.82",
                },
            },
        ),
        (
            ObservationCapability.EXPOSURE_CANDIDATES,
            "instrument",
            "instrument",
            {
                "api_name": "etf_basic",
                "record": {
                    "ts_code": "512010.SH",
                    "name": "Fixture ETF",
                    "exchange": "SSE",
                    "list_status": "L",
                    "list_date": "20200101",
                    "index_code": "801150.SI",
                },
            },
        ),
    ]
    if include_expectation:
        values.append(
            (
                ObservationCapability.PRIOR_EXPECTATION,
                "expectation",
                "expectation",
                {"record": {"eps": "1.25", "quarter": "2026Q3"}},
            )
        )
    return tuple(values)


def _observation(
    capability: ObservationCapability,
    source_id: str,
    payload: dict[str, object],
) -> SourceObservation:
    received = BARRIER - timedelta(minutes=30)
    return SourceObservation.build(
        capability=capability,
        provider_id="fixture-provider",
        provider_version="1",
        upstream_source=source_id,
        upstream_record_id=f"{source_id}:record",
        source_ref=f"fixture://{source_id}",
        lineage_id=f"fixture:{source_id}",
        times=ObservationTimes(
            occurred_at=received - timedelta(minutes=2),
            published_at=received - timedelta(minutes=2),
            available_at=received,
            source_updated_at=received - timedelta(minutes=2),
            aggregator_fetched_at=None,
            retrieved_at=received,
            occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        ),
        authority_at=received,
        authority_kind="actual_receipt",
        raw_content_hash=sha256(source_id.encode()).hexdigest(),
        normalized_payload=payload,
        license_scope="private_research_no_redistribution",
    )


def _data_snapshot(
    store: LocalDataSnapshotStore,
    capability: ObservationCapability,
    source_id: str,
    payload: dict[str, object],
) -> DataSnapshot:
    observation = _observation(capability, source_id, payload)
    digest = sha256(source_id.encode()).hexdigest()
    source = DataSourceBinding(
        provider_id=observation.provider_id,
        provider_version=observation.provider_version,
        upstream_source=observation.upstream_source,
        manifest_hash=digest,
        source_config_hash=digest,
        required=True,
    )
    query = DataQuery.build(
        capability=capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=BARRIER,
        window_start=BARRIER - timedelta(hours=1),
        source_policy_id=f"prospective-collection-policy-{digest}",
        parameters={},
        sources=(source,),
        minimum_data_sources=1,
    )
    attempt = DataProviderAttempt(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        required=True,
        status=DataFetchStatus.DATA,
        retrieved_at=BARRIER,
        raw_response_hash=digest,
        received_count=1,
        accepted_count=1,
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None,
    )
    core: dict[str, object] = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [observation.to_dict()],
        "coverage_complete": True,
        "completed_at": "2026-08-28T08:00:00Z",
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=(attempt,),
        observations=(observation,),
        coverage_complete=True,
        completed_at=BARRIER,
    )
    store.put(snapshot)
    return snapshot


def _snapshot_set(
    store: LocalDataSnapshotStore,
    *,
    include_expectation: bool = True,
    price_trade_date: str = "20260828",
) -> ProspectiveCheckpointSnapshotSet:
    routes: dict[ObservationCapability, list[CheckpointRouteReconciliation]] = {}
    for capability, source_id, route_kind, payload in _specs(
        include_expectation=include_expectation,
        price_trade_date=price_trade_date,
    ):
        snapshot = _data_snapshot(store, capability, source_id, payload)
        observation = snapshot.observations[0]
        digest = sha256(source_id.encode()).hexdigest()
        routes.setdefault(capability, []).append(
            CheckpointRouteReconciliation(
                route_kind=route_kind,
                snapshot_id=snapshot.snapshot_id,
                collection_policy_id=f"prospective-collection-policy-{digest}",
                source_acceptance_report_id=f"source-route-acceptance-report-{digest}",
                provider_id=observation.provider_id,
                provider_version=observation.provider_version,
                upstream_source=observation.upstream_source,
                provider_manifest_hash=digest,
                source_config_hash=digest,
                raw_response_hash=digest,
                observation_ids=(observation.observation_id,),
            )
        )
    bindings: list[CheckpointCapabilityBinding] = []
    for capability in sorted(REQUIRED_DIAGNOSTIC_CAPABILITIES, key=lambda item: item.value):
        selected = tuple(sorted(routes.get(capability, ()), key=lambda item: item.route_kind))
        bindings.append(
            CheckpointCapabilityBinding(
                capability=capability,
                applicability=(
                    CapabilityApplicability.REQUIRED
                    if selected
                    else CapabilityApplicability.NOT_APPLICABLE
                ),
                not_applicable_reason=(None if selected else "not required for fixture"),
                routes=selected,
                tool_manifest=(
                    None
                    if not selected
                    else CheckpointToolManifest(
                        name=_TOOL_NAMES[capability],
                        version="1",
                        snapshot_ids=tuple(sorted(item.snapshot_id for item in selected)),
                        allowed_filter_fields=(),
                    )
                ),
            )
        )
    snapshot_ids = tuple(
        sorted(route.snapshot_id for selected in routes.values() for route in selected)
    )
    core: dict[str, object] = {
        "schema_version": PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA_V4,
        "registration_id": REGISTRATION.registration_id,
        "checkpoint_key": CHECKPOINT_KEY,
        "barrier_at": "2026-08-28T08:00:00Z",
        "reconciled_at": "2026-08-28T08:01:00Z",
        "capability_bindings": [item.to_dict() for item in bindings],
        "authorized_snapshot_ids": list(snapshot_ids),
        "complete": True,
        "historical_pit_claim": False,
        "execution_capability": False,
        "capability_gaps": [],
    }
    return ProspectiveCheckpointSnapshotSet(
        snapshot_set_id=f"prospective-checkpoint-snapshot-set-{canonical_hash(core)}",
        registration_id=REGISTRATION.registration_id,
        checkpoint_key=CHECKPOINT_KEY,
        barrier_at=BARRIER,
        reconciled_at=BARRIER + timedelta(minutes=1),
        capability_bindings=tuple(bindings),
        authorized_snapshot_ids=snapshot_ids,
        complete=True,
        capability_gaps=(),
        schema_version=PROSPECTIVE_CHECKPOINT_SNAPSHOT_SET_SCHEMA_V4,
    )


def _assessment(*, horizon_sessions: int = 1) -> ProspectiveEventAssessmentArtifact:
    evidence_id = "prospective-observation-version-" + "a" * 64
    path = TransmissionPath(
        target_id="512010.SH",
        venue="XSHG",
        instrument_class="exchange_traded_fund",
        channels=(TransmissionChannel.CAPACITY_COST_INVENTORY,),
        causal_steps=("Outage changes expected sector supply.",),
        evidence_version_ids=(evidence_id,),
        horizon_sessions=horizon_sessions,
    )
    core: dict[str, object] = {
        "schema_version": PROSPECTIVE_EVENT_ASSESSMENT_SCHEMA,
        "triage_decision_id": "event-impact-triage-decision-" + "b" * 64,
        "cluster_id": "event-impact-triage-cluster-" + "c" * 64,
        "event_assessment_artifact_hash": "d" * 64,
        "paths": [path.to_dict()],
        "counterevidence": ["Demand response could offset supply pressure."],
        "invalidation_conditions": ["The outage is reversed before the next session."],
        "assessed_at": "2026-08-28T08:02:00Z",
        "position_snapshot": None,
        "historical_analogy_pack": None,
        "historical_pit_claim": False,
        "signal_or_execution_capability": False,
    }
    return ProspectiveEventAssessmentArtifact(
        assessment_id=f"prospective-event-assessment-{canonical_hash(core)}",
        triage_decision_id=cast(str, core["triage_decision_id"]),
        cluster_id=cast(str, core["cluster_id"]),
        event_assessment_artifact_hash="d" * 64,
        paths=(path,),
        counterevidence=("Demand response could offset supply pressure.",),
        invalidation_conditions=("The outage is reversed before the next session.",),
        assessed_at=BARRIER + timedelta(minutes=2),
        position_snapshot=None,
        historical_analogy_pack=None,
    )


def _trigger(assessment: ProspectiveEventAssessmentArtifact) -> ProspectiveTriggerAdmission:
    common: dict[str, object] = {
        "schema_version": PROSPECTIVE_TRIGGER_ADMISSION_SCHEMA,
        "kind": TriggerAdmissionKind.MATERIAL_EVENT.value,
        "registration_id": REGISTRATION.registration_id,
        "checkpoint_key": CHECKPOINT_KEY,
        "candidate_set_id": "event-impact-triage-candidate-set-" + "e" * 64,
        "proposal_id": "event-impact-triage-proposal-" + "f" * 64,
        "triage_decision_id": assessment.triage_decision_id,
        "cluster_id": assessment.cluster_id,
        "observation_version_ids": ["prospective-observation-version-" + "a" * 64],
        "event_assessment_id": assessment.assessment_id,
        "materiality_gate_result_id": "prospective-materiality-gate-" + "1" * 64,
        "preceding_materiality_gate_result_ids": [],
        "admitted_target_ids": ["512010.SH"],
        "held_target_ids": [],
        "admitted_at": "2026-08-28T08:03:00Z",
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return ProspectiveTriggerAdmission(
        admission_id=f"prospective-trigger-admission-{canonical_hash(common)}",
        kind=TriggerAdmissionKind.MATERIAL_EVENT,
        registration_id=REGISTRATION.registration_id,
        checkpoint_key=CHECKPOINT_KEY,
        candidate_set_id=cast(str, common["candidate_set_id"]),
        proposal_id=cast(str, common["proposal_id"]),
        triage_decision_id=assessment.triage_decision_id,
        cluster_id=assessment.cluster_id,
        observation_version_ids=("prospective-observation-version-" + "a" * 64,),
        event_assessment_id=assessment.assessment_id,
        materiality_gate_result_id=cast(str, common["materiality_gate_result_id"]),
        preceding_materiality_gate_result_ids=(),
        admitted_target_ids=("512010.SH",),
        held_target_ids=(),
        admitted_at=BARRIER + timedelta(minutes=3),
    )


def _materialize(
    tmp_path: Path,
    *,
    include_expectation: bool = True,
    price_trade_date: str = "20260828",
    horizon_sessions: int = 1,
) -> tuple[dict[str, object], ...]:
    store = LocalDataSnapshotStore(tmp_path / "snapshots")
    snapshot_set = _snapshot_set(
        store,
        include_expectation=include_expectation,
        price_trade_date=price_trade_date,
    )
    assessment = _assessment(horizon_sessions=horizon_sessions)
    return _materialize_modeled_pit_readiness_checkpoints(
        registration=REGISTRATION,
        snapshot_set=snapshot_set,
        snapshot_store=store,
        trigger=_trigger(assessment),
        assessment=assessment,
        rule_set=RULE_SET,
    )


def test_composition_materialization_is_judgment_ready_but_intent_blocked(
    tmp_path: Path,
) -> None:
    (checkpoint,) = _materialize(tmp_path)

    assert parse_untrusted_modeled_pit_readiness_checkpoint(checkpoint) == checkpoint
    assert validate_agent_contract(checkpoint, "modeled-pit-readiness-checkpoint.schema.json") == ()
    assert checkpoint["judgment_ready"] is True
    assert checkpoint["intent_ready"] is False
    target = cast(dict[str, object], checkpoint["target_state"])
    assert target["decision_time_tradability"] == "unverified"
    assert target["suspension_status"] == "unknown"
    assert target["raw_price_trade_date"] == "20260828"
    assert target["raw_price_execution_eligible"] is False
    assert set(cast(list[str], checkpoint["intent_blockers"])) >= {
        "current_tradability_unverified",
        "executable_raw_price_unavailable",
        "suspension_status_unverified",
    }
    assert checkpoint["hedge_readiness"] == {
        "status": "unavailable",
        "reason_code": "portfolio_exposure_mapping_not_registered",
    }


def test_optional_expectation_absence_is_typed_for_judgment_only(tmp_path: Path) -> None:
    (checkpoint,) = _materialize(tmp_path, include_expectation=False)

    assert checkpoint["prior_expectation"] == {
        "kind": "unknown",
        "reason_code": "no_registered_source",
    }
    assert checkpoint["judgment_ready"] is True
    assert "prior_expectation_unknown" in cast(list[str], checkpoint["judgment_information_gaps"])


def test_future_raw_price_trade_date_is_rejected_even_if_observation_is_available(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="session closes after the checkpoint cutoff"):
        _materialize(tmp_path, price_trade_date="20260829")


def test_arbitrary_horizon_is_rejected_against_durable_registration(tmp_path: Path) -> None:
    with pytest.raises(PermissionError, match="absent from durable preregistration"):
        _materialize(tmp_path, horizon_sessions=2)


def test_caller_universe_and_hedge_facades_do_not_exist(tmp_path: Path) -> None:
    import market_impact_agent.modeled_pit_readiness as modeled

    assert not hasattr(modeled, "ModeledPitReadinessSourceBundle")
    assert not hasattr(modeled, "build_modeled_pit_readiness_checkpoint")
    assert not hasattr(modeled, "evaluate_modeled_pit_readiness")
    assert not hasattr(modeled, "modeled_pit_readiness_checkpoint_from_dict")
    signature = inspect.signature(_materialize_modeled_pit_readiness_checkpoints)
    assert "market_universe_view" not in signature.parameters
    assert "hedge_evidence" not in signature.parameters
    assert "raw_executable_price_basis" not in signature.parameters

    (checkpoint,) = _materialize(tmp_path)
    tampered = cast(dict[str, object], json.loads(json.dumps(checkpoint)))
    cast(dict[str, object], tampered["target_state"])["decision_time_tradability"] = "verified"
    core = {key: item for key, item in tampered.items() if key != "checkpoint_id"}
    tampered["checkpoint_id"] = f"modeled-pit-readiness-checkpoint-{canonical_hash(core)}"
    with pytest.raises(ValueError, match="schema error"):
        parse_untrusted_modeled_pit_readiness_checkpoint(tampered)


def test_caller_rehashed_decision_input_cannot_replace_store_observation(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "snapshots")
    snapshot_set = _snapshot_set(store)
    authentic = materialize_checkpoint_decision_inputs(snapshot_set, store=store)
    tampered = cast(list[dict[str, object]], json.loads(json.dumps(list(authentic))))
    event = next(item for item in tampered if item["capability"] == "event_revelation")
    cast(dict[str, object], event["data"])["statement"] = "forged statement"
    core = {key: item for key, item in event.items() if key != "record_id"}
    event["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    assessment = _assessment()
    (checkpoint,) = _materialize_modeled_pit_readiness_checkpoints(
        registration=REGISTRATION,
        snapshot_set=snapshot_set,
        snapshot_store=store,
        trigger=_trigger(assessment),
        assessment=assessment,
        rule_set=RULE_SET,
    )
    authentic_event_id = next(
        cast(str, item["record_id"])
        for item in authentic
        if item["capability"] == "event_revelation"
    )
    assert checkpoint["new_fact_record_ids"] == [authentic_event_id]
    assert event["record_id"] not in cast(list[str], checkpoint["new_fact_record_ids"])


def test_pipeline_rejects_custom_trigger_authority_before_materialization() -> None:
    pipeline = ProspectiveDecisionPipeline.__new__(ProspectiveDecisionPipeline)
    object.__setattr__(pipeline, "trigger_store", object())
    refs = FrozenProspectiveDecisionRefs(
        registration_hash="a" * 64,
        checkpoint_snapshot_set_hash="b" * 64,
        evidence_pack_hash="c" * 64,
        execution_plan_hash="d" * 64,
        trigger_admission_id="prospective-trigger-admission-" + "e" * 64,
    )

    with pytest.raises(PermissionError, match="concrete durable Trigger store"):
        pipeline.materialize_modeled_pit_readiness(refs=refs)


def _pipeline_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    ProspectiveDecisionPipeline,
    FrozenProspectiveDecisionRefs,
    LocalDataSnapshotStore,
]:
    store = LocalDataSnapshotStore(tmp_path / "snapshots")
    snapshot_set = _snapshot_set(store)
    assessment = _assessment()
    trigger = _trigger(assessment)
    trigger_store = ProspectiveTriggerAdmissionStore(store)

    def reopen_inputs(
        _pipeline: ProspectiveDecisionPipeline,
        _refs: FrozenProspectiveDecisionRefs,
    ) -> tuple[object, ...]:
        return (
            REGISTRATION,
            snapshot_set,
            cast(Any, object()),
            cast(Any, object()),
            trigger,
        )

    def get_context(
        _store: ProspectiveTriggerAdmissionStore,
        admission_id: str,
    ) -> tuple[object, ...]:
        assert admission_id == trigger.admission_id
        return trigger, assessment, None

    monkeypatch.setattr(ProspectiveDecisionPipeline, "_reopen_inputs", reopen_inputs)
    monkeypatch.setattr(ProspectiveTriggerAdmissionStore, "get_context", get_context)
    pipeline = ProspectiveDecisionPipeline.__new__(ProspectiveDecisionPipeline)
    object.__setattr__(pipeline, "frozen_artifacts", ArtifactStore(tmp_path / "frozen"))
    object.__setattr__(pipeline, "snapshot_store", store)
    object.__setattr__(pipeline, "trigger_store", trigger_store)
    object.__setattr__(pipeline, "instrument_rule_set", RULE_SET)
    refs = FrozenProspectiveDecisionRefs(
        registration_hash="a" * 64,
        checkpoint_snapshot_set_hash="b" * 64,
        evidence_pack_hash="c" * 64,
        execution_plan_hash="d" * 64,
        trigger_admission_id=trigger.admission_id,
    )
    return pipeline, refs, store


def test_pipeline_produced_checkpoint_reopens_from_durable_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline, refs, _store = _pipeline_fixture(tmp_path, monkeypatch)

    (checkpoint,) = pipeline.materialize_modeled_pit_readiness(refs=refs)

    assert (
        pipeline.reopen_modeled_pit_readiness(
            refs=refs,
            checkpoint_id=cast(str, checkpoint["checkpoint_id"]),
        )
        == checkpoint
    )


def test_self_hashed_unregistered_checkpoint_cannot_be_reopened_authoritatively(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline, refs, store = _pipeline_fixture(tmp_path, monkeypatch)
    registration, snapshot_set, trigger, assessment = pipeline._reopen_modeled_pit_context(  # pyright: ignore[reportPrivateUsage]
        refs
    )
    (fabricated,) = _materialize_modeled_pit_readiness_checkpoints(
        registration=registration,
        snapshot_set=snapshot_set,
        snapshot_store=store,
        trigger=trigger,
        assessment=assessment,
        rule_set=RULE_SET,
    )
    assert fabricated["judgment_ready"] is True
    assert parse_untrusted_modeled_pit_readiness_checkpoint(fabricated) == fabricated
    store.artifacts.put_json(fabricated)

    with pytest.raises(PermissionError, match="no durable Harness authority"):
        pipeline.reopen_modeled_pit_readiness(
            refs=refs,
            checkpoint_id=cast(str, fabricated["checkpoint_id"]),
        )


def test_rehashed_mutation_and_cross_root_artifact_fail_authoritative_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline, refs, _store = _pipeline_fixture(tmp_path / "first", monkeypatch)
    (checkpoint,) = pipeline.materialize_modeled_pit_readiness(refs=refs)
    forged = cast(dict[str, object], json.loads(json.dumps(checkpoint)))
    gaps = cast(list[str], forged["judgment_information_gaps"])
    gaps.append("prior_expectation_unknown")
    gaps.sort()
    core = {key: item for key, item in forged.items() if key != "checkpoint_id"}
    forged["checkpoint_id"] = f"modeled-pit-readiness-checkpoint-{canonical_hash(core)}"
    assert parse_untrusted_modeled_pit_readiness_checkpoint(forged) == forged
    with pytest.raises(PermissionError, match="absent from current source derivation"):
        pipeline.reopen_modeled_pit_readiness(
            refs=refs,
            checkpoint_id=forged["checkpoint_id"],
        )

    other_pipeline, other_refs, other_store = _pipeline_fixture(
        tmp_path / "second",
        monkeypatch,
    )
    other_store.artifacts.put_json(checkpoint)
    with pytest.raises(PermissionError, match="no durable Harness authority"):
        other_pipeline.reopen_modeled_pit_readiness(
            refs=other_refs,
            checkpoint_id=cast(str, checkpoint["checkpoint_id"]),
        )
