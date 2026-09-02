from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event
from typing import cast

import pytest

from market_impact_agent.agent_contracts import JudgmentDecision, JudgmentProposal, canonical_hash
from market_impact_agent.agent_engine import compose_authoritative_agent_engine
from market_impact_agent.agent_runtime import ModelTurn, SkillRegistry, ToolCall, ToolRegistry
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import (
    DataSnapshot,
    LocalDataSnapshotStore,
    data_snapshot_from_dict,
)
from market_impact_agent.event_impact_triage import (
    CheckpointEligibility,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    TriageClusterProposal,
    TriageObservationRef,
    TriageRoute,
    TriageWorkDecisionEvidence,
    event_impact_triage_candidate_set_from_dict,
)
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_checkpoint_sets import (
    materialize_checkpoint_decision_inputs,
    prospective_checkpoint_snapshot_set_from_dict,
)
from market_impact_agent.prospective_data import (
    ProspectiveDataJournal,
    prospective_observation_version_id,
)
from market_impact_agent.prospective_decision_pipeline import (
    prepare_reassessment_judgment,
    reassessment_inputs,
    run_reassessment_judgment,
)
from market_impact_agent.prospective_diagnostic import (
    REASSESSMENT_PROFILE,
    REASSESSMENT_USD1_PROFILE,
    ProspectiveDiagnosticRegistration,
    RegisteredReassessment,
    build_reassessment_registration,
    prospective_diagnostic_registration_from_dict,
)
from market_impact_agent.prospective_execution import prospective_execution_plan_from_dict
from market_impact_agent.prospective_query_gate import evaluate_prospective_query_gate
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveTriggerAdmissionStore,
    TriggerAdmissionKind,
    prospective_trigger_admission_from_dict,
)
from market_impact_agent.runtime_store import RunStatus
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

from .test_agent_engine import SimulatedCrash, final_turn, tool_turn
from .test_event_impact_triage import RecordingWorkRunAuthority
from .test_prospective_checkpoint_sets import (
    _accepted_report,  # pyright: ignore[reportPrivateUsage]
    _policy,  # pyright: ignore[reportPrivateUsage]
    _receipt_snapshot,  # pyright: ignore[reportPrivateUsage]
    _source,  # pyright: ignore[reportPrivateUsage]
)
from .test_prospective_trigger_admission import (
    _candidate_set,  # pyright: ignore[reportPrivateUsage]
    _registration,  # pyright: ignore[reportPrivateUsage]
    _triage,  # pyright: ignore[reportPrivateUsage]
)

NOW = datetime(2026, 9, 2, 10, tzinfo=UTC)
OLD = datetime(2026, 8, 30, 1, 1, tzinfo=UTC)
State = tuple[
    LocalDataSnapshotStore,
    EventImpactTriageDecisionStore,
    ProspectiveDiagnosticRegistration,
    ProspectiveDiagnosticRegistration,
    tuple[str, ...],
    EventImpactTriageDecision,
]


def prepared_state(
    tmp_path: Path,
    *,
    context_age: timedelta = timedelta(hours=1),
    target: str = "000001.SZ",
    with_unrelated_row: bool = False,
    trade_date: object = "20260901",
    omit_trade_date: bool = False,
    model_profile_id: str = REASSESSMENT_PROFILE,
    checkpoint_tool_version: str = "3",
) -> State:
    store = LocalDataSnapshotStore(tmp_path / "state")
    journal = ProspectiveDataJournal(store)
    original = _registration()
    checkpoint = next(
        item
        for item in original.checkpoints
        if item.mechanism.value == "earnings_expectation_delta"
    )
    snapshots: list[DataSnapshot] = []
    report_hashes: list[str] = []
    contexts: list[str] = []
    for api, capability, received, period in (
        ("forecast_vip", ObservationCapability.EVENT_REVELATION, OLD, "20260930"),
        (
            "forecast_vip",
            ObservationCapability.EVENT_REVELATION,
            OLD + timedelta(seconds=1),
            "20261231",
        ),
        ("stock_basic", ObservationCapability.EXPOSURE_CANDIDATES, NOW - context_age, None),
        ("daily_basic", ObservationCapability.MARKET_CONTEXT, NOW - context_age, None),
    ):
        source = _source(api)
        policy = _policy(capability=capability, sources=(source,))
        snapshot = _receipt_snapshot(
            store,
            policy,
            received_at=received,
            normalized_payload={
                "api_name": api,
                "record": {
                    "ts_code": "000001.SZ" if api == "forecast_vip" else target,
                    "end_date": period,
                    "list_status": "L",
                    "list_date": "20000101",
                    **(
                        {"trade_date": trade_date}
                        if api == "daily_basic" and not omit_trade_date
                        else {}
                    ),
                },
            },
        )
        if with_unrelated_row and api == "stock_basic":
            payload = snapshot.to_dict()
            extra = snapshot.observations[0].core_dict()
            extra["upstream_record_id"] = "unrelated-issuer"
            extra["lineage_id"] = "unrelated-issuer"
            extra["normalized_payload"] = {
                "api_name": "stock_basic",
                "record": {"ts_code": "999999.SZ"},
            }
            extra["observation_id"] = f"source-observation-{canonical_hash(extra)}"
            payload["observations"] = [snapshot.observations[0].to_dict(), extra]
            payload["attempts"] = [
                replace(snapshot.attempts[0], received_count=2, accepted_count=2).to_dict()
            ]
            payload.pop("snapshot_id")
            payload["snapshot_id"] = f"data-snapshot-{canonical_hash(payload)}"
            snapshot = data_snapshot_from_dict(payload)
            store.put(snapshot)
        journal.record_snapshot(snapshot, policy=policy)
        report = _accepted_report(snapshot.snapshot_id, source=source, capability=capability)
        report_hashes.append(store.artifacts.put_json(report.to_dict()).content_hash)
        if capability is ObservationCapability.EVENT_REVELATION:
            snapshots.append(snapshot)
        else:
            contexts.append(prospective_observation_version_id(snapshot.observations[0]))
    base = _candidate_set(original, checkpoint_key=checkpoint.checkpoint_key)
    observations = tuple(
        TriageObservationRef(
            version_id=prospective_observation_version_id(item.observations[0]),
            observation_id=item.observations[0].observation_id,
            first_available_at=cast(datetime, item.observations[0].times.available_at),
            authority_at=cast(datetime, item.observations[0].authority_at),
            provider_id=item.observations[0].provider_id,
            provider_version=item.observations[0].provider_version,
            upstream_source=item.observations[0].upstream_source,
            source_ref=item.observations[0].source_ref,
            raw_content_hash=item.observations[0].raw_content_hash,
            normalized_payload_hash=canonical_hash(item.observations[0].normalized_payload),
        )
        for item in snapshots
    )
    core = base.core_dict()
    core["observations"] = [item.to_dict() for item in observations]
    candidate = event_impact_triage_candidate_set_from_dict(
        {
            **core,
            "candidate_set_id": f"event-impact-triage-candidate-set-{canonical_hash(core)}",
        }
    )
    versions = tuple(sorted(candidate.version_ids))
    template = _triage(original, checkpoint_key=checkpoint.checkpoint_key, needs_review=True)
    cluster = TriageClusterProposal.build(
        candidate_version_ids=versions,
        evidence_version_ids=versions,
        checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
        recommended_route=TriageRoute.ATTENTION_WATCH,
        event_archetypes=template[3].event_archetypes,
        event_stage=template[3].event_stage,
        changed_facts=("Two estimates have different reporting periods.",),
        rule_reasons=("Reporting-period partition is unresolved.",),
        uncertainty_notes=("Revision versus independent reporting periods is unresolved.",),
        watch_questions=("Which reporting period does each forecast describe?",),
        triage_confidence=0.7,
    )
    proposal = EventImpactTriageProposal.build(candidate_set=candidate, clusters=(cluster,))
    evidence = cast(TriageWorkDecisionEvidence, template[2].run_evidence)
    triage = EventImpactTriageDecisionStore(store.root)
    decision = triage.admit_work(
        candidate_set=candidate,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=RecordingWorkRunAuthority(
            candidate.candidate_set_id, proposal.proposal_id, evidence
        ),
        decided_at=evidence.finished_at,
    )
    registration = build_reassessment_registration(
        original_registration=original,
        registered_at=NOW - timedelta(minutes=1),
        model_profile_id=model_profile_id,
        checkpoint_tool_version=checkpoint_tool_version,
        subject=RegisteredReassessment(
            original_registration_id=original.registration_id,
            original_candidate_set_id=candidate.candidate_set_id,
            original_cluster_id=cluster.cluster_id,
            subject_version_ids=versions,
            research_question=(
                "Distinguish forecast periods; assess five-session implications or abstain."
            ),
            source_acceptance_report_hashes=tuple(sorted(set(report_hashes))),
        ),
    )
    return store, triage, original, registration, tuple(sorted(contexts)), decision


def admit(state: State):
    store, triage, original, registration, contexts, _ = state
    candidate, proposal, later, _ = _triage(
        original,
        checkpoint_key=registration.checkpoints[0].checkpoint_key,
        seed_offset=60,
        frozen_after_minutes=30,
    )
    evidence = cast(TriageWorkDecisionEvidence, later.run_evidence)
    triage.admit_work(
        candidate_set=candidate,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=RecordingWorkRunAuthority(
            candidate.candidate_set_id, proposal.proposal_id, evidence
        ),
        decided_at=evidence.finished_at,
    )
    return ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW).record_reassessment(
        registration=registration,
        original_registration=original,
        context_version_ids=contexts,
        triage_authority=triage,
    )


class Provider:
    def __init__(
        self, response: ModelTurn | BaseException, profile_alias: str = REASSESSMENT_PROFILE
    ):
        self.response = response
        self.calls = 0
        self.profile = load_builtin_model_provider_profile(profile_alias)

    @property
    def provider_id(self):
        return self.profile.provider_id

    @property
    def model(self):
        return self.profile.model

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        pass

    async def complete(self, **kwargs: object) -> ModelTurn:
        self.calls += 1
        if isinstance(self.response, BaseException):
            raise self.response
        return replace(self.response, model=self.model)


def engine_for(store: LocalDataSnapshotStore, provider: Provider):
    return compose_authoritative_agent_engine(
        store=store,
        provider=provider,
        config=provider.profile.runtime_config(),
        tool_registry=ToolRegistry(store.artifacts),
        skill_registry=SkillRegistry(Path("skills")),
        clock=lambda: NOW + timedelta(minutes=1),
    )


def abstain_turn(event_id: str):
    return final_turn(
        JudgmentProposal(
            event_id=event_id,
            decision=JudgmentDecision.ABSTAIN,
            summary="The two periods do not establish a comparable revision.",
            transmission_steps=(),
            candidates=(),
            blockers=("Comparable economic change is unknown.",),
            unresolved_questions=("Are these independent reporting-period estimates?",),
            stopped_reason="Economic ambiguity, not delivery provenance.",
        )
    )


@pytest.mark.parametrize("tool_version", ["2", "3"])
def test_exact_old_subject_current_context_and_single_judgment_replay(
    tmp_path: Path,
    tool_version: str,
) -> None:
    # Both budget profiles are exercised through the CLI below; the shared lifecycle
    # needs one integration case, not a second copy for a cost-only configuration.
    state = prepared_state(tmp_path, checkpoint_tool_version=tool_version)
    store, triage, _, registration, _, original_decision = state
    before = triage.get_context(original_decision.candidate_set_id)
    trigger = admit(state)
    assert trigger.kind is TriggerAdmissionKind.REGISTERED_REASSESSMENT
    assert trigger.admitted_at == NOW
    assert prospective_trigger_admission_from_dict(trigger.to_dict()) == trigger
    assert prospective_diagnostic_registration_from_dict(registration.to_dict()) == registration
    assert ("checkpoint_tool_version" in registration.to_dict()) == (tool_version == "3")
    for value, schema in (
        (registration.to_dict(), "prospective-diagnostic-registration.schema.json"),
        (trigger.to_dict(), "prospective-trigger-admission.schema.json"),
    ):
        assert validate_agent_contract(value, schema) == ()
    _, snapshot_set, pack, instruction = reassessment_inputs(store=store, trigger=trigger)
    assert {
        item.tool_manifest.version
        for item in snapshot_set.capability_bindings
        if item.tool_manifest is not None
    } == {tool_version}
    assert ("start each relevant checkpoint tool with {}" in instruction) == (tool_version == "3")
    assert (
        validate_agent_contract(
            snapshot_set.to_dict(), "prospective-checkpoint-snapshot-set.schema.json"
        )
        == ()
    )
    assert pack.as_of == NOW
    assert min(item.available_at for item in pack.evidence) == OLD
    assert pack.allowed_targets == ("000001.SZ",)
    assert "Which reporting period" in instruction
    assert "not financial truth" in instruction
    inputs = materialize_checkpoint_decision_inputs(snapshot_set, store=store)
    assert len(inputs) == 4
    provider = Provider(abstain_turn(pack.event_id))
    engine = engine_for(store, provider)
    refs, gate, request = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    assert gate.model_run_eligible and provider.calls == 0
    # Re-hashing a different tool surface is not authority to change a registered epoch.
    changed = snapshot_set.core_dict()
    for binding in cast(list[dict[str, object]], changed["capability_bindings"]):
        manifest = binding["tool_manifest"]
        if isinstance(manifest, dict):
            manifest["version"] = "2" if tool_version == "3" else "3"
    changed["snapshot_set_id"] = f"prospective-checkpoint-snapshot-set-{canonical_hash(changed)}"
    with pytest.raises(ValueError, match="tool version differs"):
        evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=prospective_checkpoint_snapshot_set_from_dict(changed),
            evidence_pack=pack,
            decision_inputs=inputs,
            snapshot_store=store,
            execution_plan=prospective_execution_plan_from_dict(
                store.artifacts.read_json(refs.execution_plan_hash)
            ),
            model_profile_id=registration.model_profile_id,
            model_cost_limit_usd=Decimal(registration.aggregate_model_cost_limit_usd),
            evaluated_at=NOW,
            trigger_admission=trigger,
            trigger_admission_authority=ProspectiveTriggerAdmissionStore(store),
        )
    for tool in engine.tool_registry.model_tools(request.tool_access):
        function = cast(dict[str, object], tool["function"])
        parameters = cast(dict[str, object], function["parameters"])
        properties = cast(dict[str, object], parameters["properties"])
        assert ("offset" in properties) == (tool_version == "3")
        if tool_version == "2":
            assert properties["query"] == {"type": "string", "minLength": 1}
            assert cast(str, function["description"]).endswith("policies, or Provider versions.")
    assert (
        validate_agent_contract(
            store.artifacts.read_json(refs.execution_plan_hash),
            "prospective-execution-plan.schema.json",
        )
        == ()
    )
    assert request.tool_access.allowed_capabilities == frozenset({"market.read"})
    usage = UsageLedger(store.root / "usage-test.sqlite3")
    result = asyncio.run(
        run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
    )
    assert result.status is RunStatus.COMPLETED
    assert provider.calls == 1
    restarted_provider = Provider(AssertionError("terminal replay dispatched"))
    replay = asyncio.run(
        run_reassessment_judgment(
            store=store,
            refs=refs,
            engine=engine_for(store, restarted_provider),
            usage_ledger=usage,
        )
    )
    assert replay == result
    assert restarted_provider.calls == 0
    assert len(usage.records()) == 1
    assert triage.get_context(original_decision.candidate_set_id) == before
    with sqlite3.connect(store.index_path) as connection:
        assert connection.execute("SELECT count(*) FROM strategy_window_events_v2").fetchone() == (
            0,
        )


@pytest.mark.parametrize(
    "profile_alias,limit",
    [
        (REASSESSMENT_PROFILE, "1.00"),
        (REASSESSMENT_USD1_PROFILE, "0.30"),
        (REASSESSMENT_USD1_PROFILE, "1.01"),
        ("unregistered-profile", "1.00"),
    ],
)
def test_reassessment_rejects_profile_budget_mismatch(
    tmp_path: Path, profile_alias: str, limit: str
) -> None:
    registration = prepared_state(tmp_path)[3]
    with pytest.raises(ValueError, match="bounded current-time"):
        replace(registration, model_profile_id=profile_alias, aggregate_model_cost_limit_usd=limit)
    payload = registration.to_dict()
    payload.update(model_profile_id=profile_alias, aggregate_model_cost_limit_usd=limit)
    assert validate_agent_contract(payload, "prospective-diagnostic-registration.schema.json")


def test_cli_registers_explicit_budget_without_mutating_old_registration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from market_impact_agent.cli import main

    store, triage, original, old, _, _ = prepared_state(tmp_path, checkpoint_tool_version="2")
    assert old.reassessment is not None
    original_path = tmp_path / "original.json"
    question_path = tmp_path / "question.txt"
    new_path = tmp_path / "usd1.json"
    original_path.write_text(json.dumps(original.to_dict()), encoding="utf-8")
    question_path.write_text(old.reassessment.research_question, encoding="utf-8")
    before = triage.get_context(old.reassessment.original_candidate_set_id)
    args = [
        "agent",
        "prospective-reassessment",
        "--action",
        "register",
        "--registration",
        str(new_path),
        "--original-registration",
        str(original_path),
        "--question-file",
        str(question_path),
        "--candidate-set-id",
        old.reassessment.original_candidate_set_id,
        "--cluster-id",
        old.reassessment.original_cluster_id,
        "--state-root",
        str(store.root),
        "--model-profile-alias",
        REASSESSMENT_USD1_PROFILE,
    ]
    for report_hash in old.reassessment.source_acceptance_report_hashes:
        args.extend(["--source-acceptance-report-hash", report_hash])
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["model_calls"] == 0
    new = prospective_diagnostic_registration_from_dict(json.loads(new_path.read_text()))
    assert new.model_profile_id == REASSESSMENT_USD1_PROFILE
    assert new.aggregate_model_cost_limit_usd == "1.00"
    assert new.registration_id != old.registration_id
    assert new.reassessment == old.reassessment
    assert new.checkpoint_tool_version == "3"
    assert "checkpoint_tool_version" not in old.to_dict()
    assert old.aggregate_model_cost_limit_usd == "0.30"
    assert triage.get_context(old.reassessment.original_candidate_set_id) == before
    frozen_bytes = new_path.read_bytes()
    assert main(args) == 1  # Existing registration is never overwritten.
    capsys.readouterr()
    assert new_path.read_bytes() == frozen_bytes
    args[args.index("register")] = "run"
    assert main(args) == 1
    blocked = json.loads(capsys.readouterr().err)
    assert blocked["error_type"] == "ValueError"
    assert new_path.read_bytes() == frozen_bytes


@pytest.mark.parametrize(
    "context_age,target,error",
    [
        (timedelta(days=3), "000001.SZ", "stale"),
        (timedelta(hours=1), "000002.SZ", "another issuer"),
        (timedelta(hours=-1), "000001.SZ", "cutoff-visible"),
    ],
)
def test_invalid_context_blocks_before_provider(
    tmp_path: Path, context_age: timedelta, target: str, error: str
) -> None:
    state = prepared_state(tmp_path, context_age=context_age, target=target)
    with pytest.raises(ValueError, match=error):
        trigger = admit(state)
        reassessment_inputs(store=state[0], trigger=trigger)


@pytest.mark.parametrize(
    "trade_date",
    [
        "20200102",
        "20990102",
        "20260230",
        "202691",
        "2026-09-01",
        "\uff12\uff10\uff12\uff16\uff10\uff19\uff10\uff11",
        None,
        20260901,
    ],
)
def test_daily_context_requires_valid_recent_effective_trading_date(
    tmp_path: Path,
    trade_date: object,
) -> None:
    state = prepared_state(tmp_path, trade_date=trade_date)
    trigger = admit(state)
    provider = Provider(AssertionError("invalid daily context must not dispatch"))
    with pytest.raises(ValueError, match="daily_basic"):
        prepare_reassessment_judgment(
            store=state[0], trigger=trigger, engine=engine_for(state[0], provider)
        )
    assert provider.calls == 0


def test_daily_context_missing_trading_date_is_blocking(tmp_path: Path) -> None:
    state = prepared_state(tmp_path, omit_trade_date=True)
    with pytest.raises(ValueError, match="YYYYMMDD trade_date"):
        reassessment_inputs(store=state[0], trigger=admit(state))


@pytest.mark.parametrize(
    "trade_date,receipt,accepted",
    [
        ("20260902", datetime(2026, 9, 2, 6, 59, 59, tzinfo=UTC), False),
        ("20260902", datetime(2026, 9, 2, 7, tzinfo=UTC), True),
        ("20260902", datetime(2026, 9, 2, 9, tzinfo=UTC), True),
        ("20260901", datetime(2026, 9, 1, 16, tzinfo=UTC), True),
        ("20260902", datetime(2026, 9, 1, 16, tzinfo=UTC), False),
        ("20260831", datetime(2026, 9, 2, 9, tzinfo=UTC), False),
    ],
)
def test_daily_context_uses_china_completed_day_at_original_receipt(
    tmp_path: Path,
    trade_date: str,
    receipt: datetime,
    accepted: bool,
) -> None:
    state = prepared_state(tmp_path, trade_date=trade_date, context_age=NOW - receipt)
    store = state[0]
    trigger = admit(state)
    journal = ProspectiveDataJournal(store)
    before = tuple(
        journal.version_receipt(item, not_after=NOW) for item in trigger.context_version_ids
    )
    if accepted:
        _, _, pack, _ = reassessment_inputs(store=store, trigger=trigger)
        assert pack.as_of == NOW
    else:
        with pytest.raises(ValueError, match="daily_basic"):
            reassessment_inputs(store=store, trigger=trigger)
    assert (
        tuple(journal.version_receipt(item, not_after=NOW) for item in trigger.context_version_ids)
        == before
    )


def test_reassessment_admission_concurrent_single_cutoff_and_exact_restart(tmp_path: Path) -> None:
    state = prepared_state(tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(pool.submit(admit, state) for _ in range(2))
        results = tuple(item.result() for item in futures)
    assert results[0] == results[1]
    store, triage, original, registration, contexts, _ = state
    replay = ProspectiveTriggerAdmissionStore(
        store, clock=lambda: NOW + timedelta(days=1)
    ).record_reassessment(
        registration=registration,
        original_registration=original,
        context_version_ids=contexts,
        triage_authority=triage,
    )
    assert replay == results[0]
    with pytest.raises(ValueError, match="different inputs"):
        ProspectiveTriggerAdmissionStore(store).record_reassessment(
            registration=registration,
            original_registration=original,
            context_version_ids=contexts[:1],
            triage_authority=triage,
        )


@pytest.mark.parametrize(
    "failure", [RuntimeError("provider failed"), SimulatedCrash("lost receipt")]
)
def test_failure_and_interrupted_run_are_accounted_without_silent_rerun(
    tmp_path: Path, failure: BaseException
) -> None:
    state = prepared_state(tmp_path)
    store = state[0]
    trigger = admit(state)
    provider = Provider(failure)
    engine = engine_for(store, provider)
    refs, _, _ = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    usage = UsageLedger(store.root / "usage.sqlite3")
    if isinstance(failure, SimulatedCrash):
        with pytest.raises(SimulatedCrash):
            asyncio.run(
                run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
            )
    else:
        failed = asyncio.run(
            run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
        )
        assert failed.status is RunStatus.FAILED
    restarted = Provider(AssertionError("must not rerun"))
    result = asyncio.run(
        run_reassessment_judgment(
            store=store, refs=refs, engine=engine_for(store, restarted), usage_ledger=usage
        )
    )
    assert result.status in {RunStatus.FAILED, RunStatus.HUMAN_INPUT_REQUIRED}
    assert provider.calls == 1 and restarted.calls == 0
    assert len(usage.records()) == 1


def test_terminal_ledger_crash_reconciles_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = prepared_state(tmp_path)
    store = state[0]
    trigger = admit(state)
    provider = Provider(abstain_turn(trigger.cluster_id))
    engine = engine_for(store, provider)
    refs, _, _ = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    usage = UsageLedger(store.root / "usage.sqlite3")
    append = usage.append
    with monkeypatch.context() as patch:

        def crash(record: UsageRecord):
            raise SimulatedCrash("after terminal before usage")

        patch.setattr(usage, "append", crash)
        with pytest.raises(SimulatedCrash):
            asyncio.run(
                run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
            )
    assert usage.append == append
    replay = Provider(AssertionError("must replay terminal"))
    asyncio.run(
        run_reassessment_judgment(
            store=store, refs=refs, engine=engine_for(store, replay), usage_ledger=usage
        )
    )
    assert provider.calls == 1 and replay.calls == 0 and len(usage.records()) == 1


def test_default_reads_reach_agent_without_exposing_shared_receipt_rows(tmp_path: Path) -> None:
    state = prepared_state(tmp_path, with_unrelated_row=True)
    store = state[0]
    trigger = admit(state)
    counts = {
        "lookup_event_revelation": 2,
        "lookup_exposure_candidates": 1,
        "lookup_market_context": 1,
    }

    class ReadingProvider(Provider):
        async def complete(self, **kwargs: object) -> ModelTurn:
            self.calls += 1
            if self.calls == 1:
                offered = cast(tuple[dict[str, object], ...], kwargs["tools"])
                calls: list[ToolCall] = []
                for tool in offered:
                    function = cast(dict[str, object], tool["function"])
                    name = cast(str, function["name"])
                    if name not in counts:
                        continue
                    assert "Call with {} first" in cast(str, function["description"])
                    parameters = cast(dict[str, object], function["parameters"])
                    assert not parameters.get("required")
                    properties = cast(dict[str, object], parameters["properties"])
                    assert (
                        "NOT a question" in cast(dict[str, str], properties["query"])["description"]
                    )
                    calls.append(ToolCall(call_id=name, name=name, arguments={}))
                assert {call.name for call in calls} == set(counts)
                assistant: dict[str, object] = {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": call.call_id,
                            "type": "function",
                            "function": {
                                "name": call.name,
                                "arguments": json.dumps(call.arguments),
                            },
                        }
                        for call in calls
                    ],
                }
                return replace(
                    tool_turn(1),
                    model=self.model,
                    assistant_message=assistant,
                    tool_calls=tuple(calls),
                    raw_response={"message": assistant},
                )
            assert self.calls == 2
            messages = cast(tuple[dict[str, object], ...], kwargs["messages"])
            returned = [item for item in messages if item.get("role") == "tool"]
            assert len(returned) == 3
            for message in returned:
                envelope = json.loads(cast(str, message["content"]))
                assert envelope["untrusted"]
                payload = envelope["result"]
                expected = counts[cast(str, message["tool_call_id"])]
                assert len(payload["records"]) == expected
                assert payload["page"]["total_available"] == expected
                assert payload["page"]["next_offset"] is None
                assert "999999.SZ" not in json.dumps(payload)
            return replace(abstain_turn(trigger.cluster_id), model=self.model)

    provider = ReadingProvider(AssertionError("scripted reads only"))
    engine = engine_for(store, provider)
    refs, gate, request = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    assert gate.model_run_eligible and provider.calls == 0
    assert request.tool_access.allowed_tools == frozenset(counts)
    assert len(request.evidence_pack.evidence) == 4
    result = asyncio.run(
        run_reassessment_judgment(
            store=store,
            refs=refs,
            engine=engine,
            usage_ledger=UsageLedger(store.root / "usage.sqlite3"),
        )
    )
    assert result.status is RunStatus.COMPLETED
    assert provider.calls == 2


def test_v3_paging_and_explicit_filters_preserve_authorized_record_boundary(tmp_path: Path) -> None:
    from market_impact_agent.prospective_checkpoint_sets import build_checkpoint_tool_descriptors

    state = prepared_state(tmp_path)
    store = state[0]
    trigger = admit(state)
    provider = Provider(AssertionError("tool-only acceptance"))
    engine = engine_for(store, provider)
    _, gate, request = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    _, snapshot_set, _, _ = reassessment_inputs(store=store, trigger=trigger)
    name = "lookup_event_revelation"

    async def read(arguments: dict[str, object], registry: ToolRegistry = engine.tool_registry):
        result = await registry.execute(
            ToolCall(call_id="read", name=name, arguments=arguments), access=request.tool_access
        )
        payload = store.artifacts.read_json(result.result_artifact.content_hash)
        assert isinstance(payload, dict)
        assert json.loads(result.model_content)["result"] == payload
        return cast(dict[str, object], payload)

    default = asyncio.run(read({}))
    assert default["schema_version"] == "market-impact.checkpoint-data-tool-result.v3"
    records = cast(list[dict[str, object]], default["records"])
    assert len(records) == 2
    assert default["page"] == {
        "total_available": 2,
        "total_matched": 2,
        "offset": 0,
        "returned": 2,
        "next_offset": None,
    }
    first = asyncio.run(read({"limit": 1}))
    assert first["records"] == records[:1]
    next_offset = cast(dict[str, object], first["page"])["next_offset"]
    second = asyncio.run(read({"limit": 1, "offset": next_offset}))
    assert second["records"] == records[1:]
    assert cast(dict[str, object], second["page"])["next_offset"] is None
    assert asyncio.run(read({"limit": 1, "offset": next_offset})) == second
    exhausted = asyncio.run(read({"offset": 100}))
    assert exhausted["records"] == []
    assert exhausted["page"] == {
        "total_available": 2,
        "total_matched": 2,
        "offset": 100,
        "returned": 0,
        "next_offset": None,
    }
    # The same exact criteria still work; guessed or natural-language criteria
    # remain empty, with counts that distinguish over-filtering from missing input.
    exact = asyncio.run(read({"query": "000001.sz", "filters": {"instrument_code": "000001.SZ"}}))
    assert exact["records"] == records
    empty_queries: tuple[dict[str, object], ...] = (
        {"query": "Find the issuer's forecast for the next five sessions"},
        {"filters": {"instrument_code": "unknown"}},
        {"query": "000001", "filters": {"instrument_code": "000002.SZ"}},
        {"publisher": "unknown"},
    )
    for arguments in empty_queries:
        empty = asyncio.run(read(arguments))
        assert empty["records"] == []
        assert empty["page"] == {
            "total_available": 2,
            "total_matched": 0,
            "offset": 0,
            "returned": 0,
            "next_offset": None,
        }
    invalid_queries: tuple[dict[str, object], ...] = (
        {"offset": -1},
        {"offset": True},
        {"offset": 1.5},
        {"cutoff": "2099"},
    )
    for arguments in invalid_queries:
        with pytest.raises(ValueError, match="schema validation"):
            asyncio.run(read(arguments))

    # Counts and pages see only this Run's authorized records, not the rest of
    # the same frozen Snapshot (or records from another capability).
    restricted = ToolRegistry(store.artifacts)
    for descriptor in build_checkpoint_tool_descriptors(
        snapshot_set,
        store=store,
        frozen_input=gate.frozen_input,
        authorized_decision_input_ids=frozenset({cast(str, records[1]["record_id"])}),
        required_capability="market.read",
    ):
        restricted.register(descriptor)
    subset = asyncio.run(read({}, restricted))
    assert subset["records"] == records[1:]
    assert subset["page"] == {
        "total_available": 1,
        "total_matched": 1,
        "offset": 0,
        "returned": 1,
        "next_offset": None,
    }
    assert provider.calls == 0


def test_wrong_subject_or_unregistered_source_cannot_prepare(tmp_path: Path) -> None:
    state = prepared_state(tmp_path)
    store, triage, original, registration, contexts, _ = state
    subject = registration.reassessment
    assert subject is not None
    wrong_subject = build_reassessment_registration(
        original_registration=original,
        registered_at=registration.registered_at,
        subject=replace(subject, subject_version_ids=(contexts[0],)),
    )
    with pytest.raises(ValueError, match="original completed subject"):
        ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW).record_reassessment(
            registration=wrong_subject,
            original_registration=original,
            context_version_ids=contexts,
            triage_authority=triage,
        )
    missing_source = build_reassessment_registration(
        original_registration=original,
        registered_at=registration.registered_at,
        subject=replace(
            subject, source_acceptance_report_hashes=subject.source_acceptance_report_hashes[:1]
        ),
    )
    trigger = ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW).record_reassessment(
        registration=missing_source,
        original_registration=original,
        context_version_ids=contexts,
        triage_authority=triage,
    )
    with pytest.raises(ValueError, match="registered accepted source"):
        reassessment_inputs(store=store, trigger=trigger)


def test_initial_dispatch_has_one_inflight_owner(tmp_path: Path) -> None:
    state = prepared_state(tmp_path)
    store = state[0]
    trigger = admit(state)
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingProvider(Provider):
        async def complete(self, **kwargs: object) -> ModelTurn:
            entered.set()
            await release.wait()
            return await super().complete(**kwargs)

    provider = BlockingProvider(abstain_turn(trigger.cluster_id))
    engine = engine_for(store, provider)
    refs, _, _ = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    usage = UsageLedger(store.root / "usage.sqlite3")

    async def race():
        first = asyncio.create_task(
            run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
        )
        await entered.wait()
        with pytest.raises(RuntimeError, match="another caller owns"):
            await run_reassessment_judgment(
                store=store, refs=refs, engine=engine_for(store, provider), usage_ledger=usage
            )
        release.set()
        return await first

    result = asyncio.run(race())
    assert result.status is RunStatus.COMPLETED
    assert provider.calls == 1 and len(usage.records()) == 1


@pytest.mark.parametrize("failed", [False, True])
def test_cli_inspect_and_terminal_replay_need_no_credential_or_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failed: bool,
) -> None:
    from market_impact_agent.cli import main
    from market_impact_agent.model_provider import ModelProviderFactory

    state = prepared_state(tmp_path)
    store, _, original, registration, contexts, _ = state
    trigger = admit(state)
    provider = Provider(
        RuntimeError("private-provider-body") if failed else abstain_turn(trigger.cluster_id)
    )
    engine = engine_for(store, provider)
    refs, _, _ = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    for filename, value in (
        ("registration.json", registration.to_dict()),
        ("original.json", original.to_dict()),
        ("refs.json", asdict(refs)),
    ):
        (tmp_path / filename).write_text(json.dumps(value), encoding="utf-8")
    args = [
        "agent",
        "prospective-reassessment",
        "--registration",
        str(tmp_path / "registration.json"),
        "--state-root",
        str(store.root),
        "--refs",
        str(tmp_path / "refs.json"),
    ]

    def forbidden(*args: object, **kwargs: object):
        raise AssertionError("provider factory is forbidden during inspect/replay")

    monkeypatch.setattr(ModelProviderFactory, "create", forbidden)
    monkeypatch.delenv(provider.profile.credential_env, raising=False)
    assert (
        main(
            [
                *args,
                "--action",
                "prepare",
                "--original-registration",
                str(tmp_path / "original.json"),
                *[value for item in contexts for value in ("--context-version-id", item)],
            ]
        )
        == 0
    )
    assert main([*args, "--action", "inspect"]) == 0
    asyncio.run(
        run_reassessment_judgment(
            store=store,
            refs=refs,
            engine=engine,
            usage_ledger=UsageLedger(store.root / "reassessment-usage.sqlite3"),
        )
    )
    assert main([*args, "--action", "run"]) == (1 if failed else 0)
    output = capsys.readouterr()
    assert "private-provider-body" not in output.out
    assert "end_date" not in output.out
    assert '"terminal_replay": true' in output.out


def test_cli_recovers_interrupted_dispatch_offline_and_accounts_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from market_impact_agent.cli import main
    from market_impact_agent.model_provider import ModelProviderFactory

    state = prepared_state(tmp_path)
    store, _, _, registration, _, _ = state
    trigger = admit(state)
    provider = Provider(SimulatedCrash("lost dispatched turn"))
    engine = engine_for(store, provider)
    refs, _, request = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    usage = UsageLedger(store.root / "reassessment-usage.sqlite3")
    with pytest.raises(SimulatedCrash):
        asyncio.run(
            run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
        )
    assert engine.journal.get_run(request.run_id).status is RunStatus.RUNNING
    assert usage.records() == () and provider.calls == 1
    for filename, value in (
        ("registration.json", registration.to_dict()),
        ("refs.json", asdict(refs)),
    ):
        (tmp_path / filename).write_text(json.dumps(value), encoding="utf-8")
    args = [
        "agent",
        "prospective-reassessment",
        "--action",
        "run",
        "--registration",
        str(tmp_path / "registration.json"),
        "--state-root",
        str(store.root),
        "--refs",
        str(tmp_path / "refs.json"),
    ]

    def forbidden_factory(*args: object, **kwargs: object):
        raise AssertionError("interrupted recovery must not resolve a provider")

    environment_get = os.environ.get

    def offline_environment(key: str, default: str | None = None):
        if key == provider.profile.credential_env:
            raise AssertionError("interrupted recovery must not read credentials")
        return environment_get(key, default)

    monkeypatch.setattr(ModelProviderFactory, "create", forbidden_factory)
    monkeypatch.setattr(os.environ, "get", offline_environment)
    assert main(args) == 1
    recovered = json.loads(capsys.readouterr().out)
    assert recovered["status"] == RunStatus.HUMAN_INPUT_REQUIRED.value
    assert not recovered["terminal_replay"]
    assert recovered["terminal_artifact_hash"]
    assert engine.journal.get_run(request.run_id).status is RunStatus.HUMAN_INPUT_REQUIRED
    records = usage.records()
    assert len(records) == 1
    assert main(args) == 1
    replay = json.loads(capsys.readouterr().out)
    assert replay["status"] == RunStatus.HUMAN_INPUT_REQUIRED.value
    assert replay["terminal_replay"]
    assert replay["terminal_artifact_hash"] == recovered["terminal_artifact_hash"]
    assert usage.records() == records
    assert provider.calls == 1


@pytest.mark.parametrize("profile_alias", [REASSESSMENT_PROFILE, REASSESSMENT_USD1_PROFILE])
def test_cli_fresh_dispatch_resolves_exact_profile_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_alias: str,
) -> None:
    from market_impact_agent.cli import main
    from market_impact_agent.model_provider import ModelProviderFactory, ModelProviderProfile

    state = prepared_state(tmp_path, model_profile_id=profile_alias)
    store, _, _, registration, _, _ = state
    trigger = admit(state)
    provider = Provider(abstain_turn(trigger.cluster_id), profile_alias)
    refs, _, _ = prepare_reassessment_judgment(
        store=store, trigger=trigger, engine=engine_for(store, provider)
    )
    for filename, value in (
        ("registration.json", registration.to_dict()),
        ("refs.json", asdict(refs)),
    ):
        (tmp_path / filename).write_text(json.dumps(value), encoding="utf-8")
    resolved: list[ModelProviderProfile] = []

    def create_exact(factory: ModelProviderFactory, profile: ModelProviderProfile):
        assert profile == provider.profile
        assert not resolved
        resolved.append(profile)
        return provider

    monkeypatch.setattr(ModelProviderFactory, "create", create_exact)
    monkeypatch.setenv(provider.profile.credential_env, "synthetic-reassessment-test-credential")
    args = [
        "agent",
        "prospective-reassessment",
        "--action",
        "run",
        "--registration",
        str(tmp_path / "registration.json"),
        "--state-root",
        str(store.root),
        "--refs",
        str(tmp_path / "refs.json"),
    ]
    assert main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == RunStatus.COMPLETED.value
    assert not result["terminal_replay"]
    assert resolved == [provider.profile] and provider.calls == 1
    assert main(args) == 0
    replay = json.loads(capsys.readouterr().out)
    assert replay["terminal_replay"]
    assert resolved == [provider.profile] and provider.calls == 1
    assert len(UsageLedger(store.root / "reassessment-usage.sqlite3").records()) == 1


@pytest.mark.parametrize(
    "slow_stage", ["snapshot_raw", "snapshot_json", "usage_snapshot", "usage_cas"]
)
def test_collector_preparation_does_not_block_signed_provider_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slow_stage: str
) -> None:
    from market_impact_agent.openai_chat_provider import (
        OpenAIChatCompatibleProvider,
        OpenAIChatProviderConfig,
    )

    from .test_agent_engine import ObservedFixtureTransport, observed_response
    from .test_prospective_collection_runtime import (
        START,
        _runtime,  # pyright: ignore[reportPrivateUsage]
        _snapshot,  # pyright: ignore[reportPrivateUsage]
    )

    state = prepared_state(tmp_path)
    store = state[0]
    trigger = admit(state)
    collection_store, runtime, policy, job = _runtime(tmp_path)
    assert runtime.index_path == store.index_path
    snapshot = _snapshot(collection_store, policy=policy, retrieved_at=START)
    profile = load_builtin_model_provider_profile(REASSESSMENT_PROFILE)
    transport = ObservedFixtureTransport(
        [observed_response(replace(abstain_turn(trigger.cluster_id), model=profile.model))]
    )
    provider = OpenAIChatCompatibleProvider(
        api_key="synthetic-only",
        provider_id=profile.provider_id,
        provider_label="Fixture",
        config=OpenAIChatProviderConfig(
            origin="https://fixture.invalid",
            model=profile.model,
            api_path="/chat/completions",
            models_path="/models",
            max_attempts=1,
            retry_backoff_seconds=0,
        ),
        completion_parameters={},
        transport=transport,
    )
    engine = compose_authoritative_agent_engine(
        store=store,
        provider=provider,
        config=profile.runtime_config(),
        tool_registry=ToolRegistry(store.artifacts),
        skill_registry=SkillRegistry(Path("skills")),
        clock=lambda: NOW + timedelta(minutes=1),
    )
    refs, _, _ = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    entered, release = Event(), Event()

    def pause_preparation() -> None:
        entered.set()
        assert release.wait(timeout=20)

    if slow_stage == "snapshot_raw":
        original_get = collection_store.artifacts.get

        def slow_get(content_hash: str, *, media_type: str):
            if content_hash == snapshot.attempts[0].raw_response_hash and not entered.is_set():
                pause_preparation()
            return original_get(content_hash, media_type=media_type)

        monkeypatch.setattr(collection_store.artifacts, "get", slow_get)
    elif slow_stage == "snapshot_json":
        from market_impact_agent import prospective_data

        original_json = prospective_data.canonical_json_bytes

        def slow_json(value: object):
            if isinstance(value, dict) and cast(dict[str, object], value).get("observation_id") == (
                snapshot.observations[0].observation_id
            ):
                pause_preparation()
            return original_json(cast(object, value))

        monkeypatch.setattr(prospective_data, "canonical_json_bytes", slow_json)
    elif slow_stage == "usage_snapshot":
        original_build = runtime._build_usage_record  # pyright: ignore[reportPrivateUsage]

        def slow_build(**kwargs: object):
            pause_preparation()
            return original_build(**kwargs)  # pyright: ignore[reportArgumentType]

        monkeypatch.setattr(runtime, "_build_usage_record", slow_build)
    else:
        original_put = collection_store.artifacts.put_json

        def slow_put(value: object):
            if isinstance(value, dict) and cast(dict[str, object], value).get("schema_version") == (
                "market-impact.prospective-collection-usage-record.v2"
            ):
                pause_preparation()
            return original_put(cast(object, value))

        monkeypatch.setattr(collection_store.artifacts, "put_json", slow_put)

    usage = UsageLedger(store.root / "contention-test-usage.sqlite3")
    with ThreadPoolExecutor(max_workers=1) as pool:
        collection = None

        def start_collection() -> None:
            nonlocal collection
            collection = pool.submit(
                runtime.run_due,
                job.job_id,
                now=START,
                collector=lambda _policy, _config, _due: snapshot,
            )
            assert entered.wait(timeout=5)

        transport.before_request = start_collection
        try:
            result = asyncio.run(
                run_reassessment_judgment(store=store, refs=refs, engine=engine, usage_ledger=usage)
            )
            assert result.status is RunStatus.COMPLETED
            assert collection is not None and not collection.done()
            # Reading the authoritative Journal also verifies its signed event chain.
            attempts = [
                event.event_type
                for event in engine.journal.events(result.run_id)
                if event.event_type.startswith("model.attempt.")
            ]
            assert attempts == ["model.attempt.dispatched", "model.attempt.succeeded"]
            assert transport.calls == 1 and len(usage.records()) == 1
        finally:
            release.set()
        assert collection is not None
        assert collection.result(timeout=5).outcome == "success"
    assert len(runtime.usage_records(job.job_id)) == 1


def test_reassessment_cli_sqlite_failure_is_sanitized_without_fabricated_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from market_impact_agent import prospective_decision_pipeline
    from market_impact_agent.cli import main

    state = prepared_state(tmp_path)
    store, _, _, registration, _, _ = state
    trigger = admit(state)
    provider = Provider(AssertionError("failed storage must not dispatch"))
    engine = engine_for(store, provider)
    refs, _, _ = prepare_reassessment_judgment(store=store, trigger=trigger, engine=engine)
    for filename, value in (
        ("registration.json", registration.to_dict()),
        ("refs.json", asdict(refs)),
    ):
        (tmp_path / filename).write_text(json.dumps(value), encoding="utf-8")

    async def failed_storage(**kwargs: object):
        raise sqlite3.OperationalError("private diagnostic SQL body")

    monkeypatch.setattr(prospective_decision_pipeline, "run_reassessment_judgment", failed_storage)
    assert (
        main(
            [
                "agent",
                "prospective-reassessment",
                "--action",
                "run",
                "--registration",
                str(tmp_path / "registration.json"),
                "--state-root",
                str(store.root),
                "--refs",
                str(tmp_path / "refs.json"),
            ]
        )
        == 1
    )
    output = capsys.readouterr()
    assert not output.out
    assert json.loads(output.err) == {
        "status": "blocked",
        "error_type": "OperationalError",
        "stage": "run_replay",
        "execution_capability": False,
    }
    assert engine.journal.records() == ()
    assert UsageLedger(store.root / "reassessment-usage.sqlite3").records() == ()
    assert provider.calls == 0
