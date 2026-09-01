from __future__ import annotations

import hmac
import inspect
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import market_impact_agent.runtime_store as runtime_store_module
import market_impact_agent.strategy_validation as strategy_validation_module
from market_impact_agent.agent_contracts import (
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    canonical_hash,
    canonical_json_bytes,
)
from market_impact_agent.agent_engine import reopen_authoritative_agent_terminal
from market_impact_agent.backtests import (
    StrategyBacktestArm,
    StrategyBacktestOutcomeMissing,
    StrategyBacktestOutcomeReceipt,
    StrategyBacktestRequestTemplate,
    StrategyBacktestVariant,
    strategy_backtest_cost_model_hash,
    strategy_backtest_fill_model_hash,
    strategy_backtest_outcome_from_dict,
    strategy_backtest_universe_hash,
)
from market_impact_agent.cli import build_parser
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.nautilus_backtest import NautilusBacktestBridge
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveTriggerAdmissionStore,
    StrategyAdmissionCaseMapping,
)
from market_impact_agent.runtime_store import RunJournal, RunStatus
from market_impact_agent.strategy_validation import (
    ProspectiveCohortCase,
    ProspectiveValidationCohort,
    StrategyBaselineDefinition,
    StrategyCaseDefinition,
    StrategyCaseMeasurementWriter,
    StrategyCaseRole,
    StrategyCaseRunPlan,
    StrategyEvidenceLane,
    StrategyMeasurementArtifact,
    StrategyValidationAuthorityStore,
    StrategyValidationDisposition,
    StrategyValidationProgram,
    StrategyValidationRegistration,
    _portfolio_from_receipts,  # pyright: ignore[reportPrivateUsage]
    bind_strategy_case_run_plan,
    start_strategy_case_run,
    write_strategy_case_terminal,
)
from tests.test_nautilus_backtest import (
    SNAPSHOT_PATH,
    _authoritative_source_snapshot,  # pyright: ignore[reportPrivateUsage]
)
from tests.test_nautilus_backtest import request as backtest_request
from tests.test_nautilus_backtest import signal as backtest_signal

NOW = datetime(2026, 9, 2, tzinfo=UTC)
HASH = "a" * 64
RUN_SPEC_HASH = "d" * 64
SKILL_HASHES: tuple[str, ...] = ()
TOOL_MANIFEST_HASHES: tuple[str, ...] = ()


def _variant(
    arm: StrategyBacktestArm,
    *,
    baseline_id: str | None = None,
    target_selection_ref: str = "manual-integration-fixture:synthetic.v1",
    strategy_ref: str = "event-impact-hold.v1",
) -> StrategyBacktestVariant:
    template = backtest_request()
    return StrategyBacktestVariant.build(
        arm=arm,
        baseline_id=baseline_id,
        strategy_ref=strategy_ref,
        target_selection_ref=target_selection_ref,
        request_template=StrategyBacktestRequestTemplate.from_request(template),
        simulation=template.simulation,
    )


class _TestPrivilegedEventSink:
    """Test-only direct writer for malformed/root-mismatch journal attacks."""

    def __init__(
        self,
        *,
        store: LocalDataSnapshotStore,
        journal: RunJournal,
        signing_store: LocalDataSnapshotStore | None = None,
    ) -> None:
        self._journal = journal
        self._authority_id = store.harness_authority_id
        key_store = store if signing_store is None else signing_store
        self._key = (key_store.root / ".harness-event-hmac.key").read_bytes()

    def append(
        self,
        *,
        run_id: str,
        event_id: str,
        event_type: str,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> str:
        payload_json = canonical_json_bytes(payload).decode()
        payload_hash = sha256(payload_json.encode()).hexdigest()
        observed = observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        with sqlite3.connect(self._journal.path) as connection:
            connection.row_factory = sqlite3.Row
            previous = connection.execute(
                "SELECT event_hash FROM events WHERE run_id = ? ORDER BY sequence DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            previous_hash = None if previous is None else str(previous["event_hash"])
            next_row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM events"
            ).fetchone()
            assert next_row is not None
            sequence = int(next_row["next_sequence"])
            event_hash = sha256(
                canonical_json_bytes(
                    {
                        "run_id": run_id,
                        "event_id": event_id,
                        "event_type": event_type,
                        "observed_at": observed,
                        "payload_hash": payload_hash,
                        "previous_hash": previous_hash,
                    }
                )
            ).hexdigest()
            signing_bytes = canonical_json_bytes(
                {
                    "schema_version": "market-impact.privileged-runtime-event-signature.v1",
                    "harness_authority_id": self._authority_id,
                    "sequence": sequence,
                    "run_id": run_id,
                    "event_id": event_id,
                    "event_type": event_type,
                    "observed_at": observed,
                    "payload": payload,
                    "previous_hash": previous_hash,
                }
            )
            signature = hmac.new(self._key, signing_bytes, sha256).hexdigest()
            connection.execute(
                """
                INSERT INTO events(
                    sequence, run_id, event_id, event_type, observed_at, payload_json,
                    payload_hash, previous_hash, event_hash,
                    signer_authority_id, privileged_signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    run_id,
                    event_id,
                    event_type,
                    observed,
                    payload_json,
                    payload_hash,
                    previous_hash,
                    event_hash,
                    self._authority_id,
                    signature,
                ),
            )
        return event_id


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


def _registration(
    *,
    source_snapshot_id: str | None = None,
    universe_hash: str = "e" * 64,
    cost_model_hash: str = "f" * 64,
    fill_model_hash: str = "1" * 64,
) -> StrategyValidationRegistration:
    development = tuple(
        StrategyCaseDefinition(
            case_id=f"development-{index:02d}",
            root_event_id=f"development-root-{index:02d}",
            regime=f"development-regime-{index % 4}",
            role=StrategyCaseRole.DEVELOPMENT,
        )
        for index in range(8)
    )
    holdout = tuple(
        StrategyCaseDefinition(
            case_id=f"holdout-{index:02d}",
            root_event_id=f"holdout-root-{index:02d}",
            regime=f"regime-{index % 6}",
            role=StrategyCaseRole.HISTORICAL_HOLDOUT,
            source_snapshot_id=source_snapshot_id,
        )
        for index in range(24)
    )
    candidate = _variant(StrategyBacktestArm.CANDIDATE)
    baseline = _variant(
        StrategyBacktestArm.PRIMARY_BASELINE,
        baseline_id="cash",
        target_selection_ref="manual-integration-fixture:baseline.v1",
        strategy_ref="cash-no-action.v1",
    )
    return StrategyValidationRegistration.build(
        strategy_epoch_id="strategy-epoch-v2",
        program=StrategyValidationProgram.HISTORICAL_STRICT,
        model_profile_hash=HASH,
        prompt_hash="b" * 64,
        skill_catalog_hash=canonical_hash(list(SKILL_HASHES)),
        tool_manifest_hash=canonical_hash(list(TOOL_MANIFEST_HASHES)),
        universe_hash=universe_hash,
        cost_model_hash=cost_model_hash,
        fill_model_hash=fill_model_hash,
        candidate_variant=candidate,
        primary_baseline_id="cash",
        baseline_definitions=(
            StrategyBaselineDefinition("cash", "2" * 64, baseline.configuration_hash, baseline),
        ),
        development_selection_evidence_hash="4" * 64,
        case_definitions=tuple(sorted((*development, *holdout), key=lambda item: item.case_id)),
        created_at=NOW,
    )


def _measurement(case_id: str, arm: str) -> StrategyMeasurementArtifact:
    candidate = arm == "candidate"
    return StrategyMeasurementArtifact(
        case_id=case_id,
        arm=arm,
        outcome_receipt_id=f"strategy-backtest-outcome-{'a' * 64}",
        outcome_receipt_hash="a" * 64,
        net_return=Decimal("0.04" if candidate else "0.01"),
        absolute_pnl=Decimal("100"),
        portfolio_net_return=Decimal("0.20" if candidate else "0.10"),
        max_drawdown=Decimal("0.08" if candidate else "0.10"),
        cvar95=Decimal("0.08" if candidate else "0.10"),
        sharpe=Decimal("1.4" if candidate else "1.0"),
        sortino=Decimal("1.8" if candidate else "1.2"),
        stressed_net_return=Decimal("0.04" if candidate else "-0.01"),
        turnover=Decimal("2.5" if candidate else "1.0"),
        adverse_excursion=Decimal("0.04" if candidate else "0.08"),
        liquidity_cost=Decimal("0.002" if candidate else "0.003"),
        avoided_loss=Decimal("0.05" if candidate else "0"),
        false_avoidance_opportunity_cost=Decimal("0.01" if candidate else "0"),
        nonempty_execution=True,
    )


def _run(
    store: LocalDataSnapshotStore,
    registration: StrategyValidationRegistration,
    case_id: str,
    *,
    run_id: str,
    started_at: datetime,
    status: RunStatus = RunStatus.COMPLETED,
    measurements: bool = True,
    fabrication: str | None = None,
) -> str:
    if fabrication not in {
        None,
        "empty_transcript",
        "zero_metrics",
        "unowned_zero_metrics",
    }:
        raise ValueError("unsupported strategy test fabrication")
    plan = StrategyCaseRunPlan.build(
        store=store,
        registration=registration,
        run_id=run_id,
        case_id=case_id,
    )
    journal = RunJournal.authoritative(store)
    start_strategy_case_run(
        journal=journal,
        artifact_store=store.artifacts,
        run_id=run_id,
        plan=plan,
        config_hash=RUN_SPEC_HASH,
        created_at=started_at,
    )
    sink = _TestPrivilegedEventSink(store=store, journal=journal)
    sink.append(
        run_id=run_id,
        event_id=f"{run_id}.started",
        event_type="run.started",
        observed_at=started_at,
        payload={
            "config_hash": RUN_SPEC_HASH,
            "provider_id": "fixture-provider",
            "model": "fixture-model",
            "strategy_plan_artifact_hash": plan.plan_hash,
        },
    )
    # Ordinary run-authority tests intentionally omit economic outcomes. The
    # promotion-capable measurement path is exercised through the real bridge below.
    _ = measurements
    finished_at = started_at + timedelta(minutes=1)
    if status is RunStatus.COMPLETED:
        proposal = JudgmentProposal(
            event_id=f"event-{case_id}",
            decision=JudgmentDecision.ABSTAIN,
            summary="No promotion signal is proposed by this validation fixture.",
            transmission_steps=(),
            candidates=(),
            blockers=("validation fixture abstention",),
            unresolved_questions=(),
            stopped_reason="fixture completed without a proposed signal",
        )
        context_before: list[dict[str, object]] = (
            []
            if fabrication is not None
            else [
                {
                    "entry_id": f"{run_id}.policy",
                    "role": "system",
                    "kind": "policy",
                    "content": "fixture policy",
                    "pinned": True,
                    "untrusted": False,
                    "artifact_hash": None,
                    "tool_call_id": None,
                    "provider_fields": {},
                },
                {
                    "entry_id": f"{run_id}.task",
                    "role": "user",
                    "kind": "task",
                    "content": "fixture task",
                    "pinned": True,
                    "untrusted": False,
                    "artifact_hash": None,
                    "tool_call_id": None,
                    "provider_fields": {},
                },
            ]
        )
        context_before_artifact = store.artifacts.put_json(context_before)
        assistant = store.artifacts.put_json(
            {
                "role": "assistant",
                "content": json.dumps(proposal.to_dict(), separators=(",", ":"), sort_keys=True),
            }
        )
        raw_response = store.artifacts.put_json({"response_id": f"response-{run_id}"})
        turn_payload: dict[str, object] = {
            "response_id": f"response-{run_id}",
            "provider_id": "fixture-provider",
            "model": "fixture-model",
            "assistant_artifact_hash": assistant.content_hash,
            "raw_response_artifact_hash": raw_response.content_hash,
            "tool_calls": [],
            "finish_reason": "stop",
            "usage": {
                "input_tokens": 0 if fabrication in {"zero_metrics", "unowned_zero_metrics"} else 1,
                "output_tokens": 0
                if fabrication in {"zero_metrics", "unowned_zero_metrics"}
                else 1,
            },
            "latency_ms": 1.0,
            "attempts": 1,
            "estimated_cost_microusd": 0,
            "tool_surface_hash": "0" * 64,
            "tool_manifest_hashes": list(TOOL_MANIFEST_HASHES),
            "mcp_binding_hashes": [],
            "context_before_turn_hash": context_before_artifact.content_hash,
        }
        if fabrication == "unowned_zero_metrics":
            with pytest.raises(PermissionError, match="root-authenticated signer"):
                journal.append(
                    run_id=run_id,
                    event_id=f"{run_id}.turn.1",
                    event_type="model.turn.completed",
                    observed_at=started_at + timedelta(seconds=10),
                    payload=turn_payload,
                )
            assert [event.event_type for event in journal.events(run_id)] == ["run.started"]
            return ""
        sink.append(
            run_id=run_id,
            event_id=f"{run_id}.turn.1",
            event_type="model.turn.completed",
            observed_at=started_at + timedelta(seconds=10),
            payload=turn_payload,
        )
        metrics = {
            "turns": 1,
            "tool_calls": 0,
            "input_tokens": 0 if fabrication == "zero_metrics" else 1,
            "output_tokens": 0 if fabrication == "zero_metrics" else 1,
            "result_bytes": 0,
            "latency_ms": 1.0,
            "provider_attempts": 1,
            "estimated_cost_microusd": 0,
        }
        metrics_artifact = store.artifacts.put_json(metrics)
        assistant_context_entry: dict[str, object] = {
            "entry_id": f"{run_id}.assistant.1",
            "role": "assistant",
            "kind": "turn",
            "content": json.dumps(proposal.to_dict(), separators=(",", ":"), sort_keys=True),
            "pinned": False,
            "untrusted": False,
            "artifact_hash": assistant.content_hash,
            "tool_call_id": None,
            "provider_fields": {},
        }
        transcript_entries: list[dict[str, object]] = (
            [] if fabrication is not None else [*context_before, assistant_context_entry]
        )
        transcript = store.artifacts.put_json(transcript_entries)
        validation_event_id = sink.append(
            run_id=run_id,
            event_id=f"{run_id}.proposal.validated",
            event_type="judgment.validated",
            observed_at=started_at + timedelta(seconds=20),
            payload={
                "proposal_hash": canonical_hash(proposal.to_dict()),
                "transcript_hash": transcript.content_hash,
                "metrics_hash": metrics_artifact.content_hash,
                "metrics": metrics,
            },
        )
        validation = journal.event(validation_event_id)
        assert validation is not None
        judgment_value = JudgmentArtifact.build(
            run_id=run_id,
            evidence_pack_id=f"evidence-pack-{case_id}",
            provider_id="fixture-provider",
            model="fixture-model",
            runtime_config_hash=HASH,
            prompt_hash="b" * 64,
            skill_hashes=SKILL_HASHES,
            tool_manifest_hashes=TOOL_MANIFEST_HASHES,
            tool_surface_hash="0" * 64,
            mcp_server_hashes=(),
            context_estimator_id="fixture-counter",
            compactor_id="fixture-compactor",
            journal_hash=validation.event_hash,
            transcript_hash=transcript.content_hash,
            raw_response_hash=raw_response.content_hash,
            started_at=started_at,
            finished_at=finished_at,
            proposal=proposal,
        )
        terminal_artifact = store.artifacts.put_json(judgment_value.to_dict())
        judgment_artifact_hash: str | None = terminal_artifact.content_hash
    else:
        metrics = {
            "turns": 0,
            "tool_calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "result_bytes": 0,
            "latency_ms": 0.0,
            "provider_attempts": 0,
            "estimated_cost_microusd": 0,
        }
        error_payload: dict[str, object] = {
            "status": status.value,
            "finished_at": finished_at.isoformat().replace("+00:00", "Z"),
            "error_class": "FixtureFailure",
            "message": "fixture terminal disposition",
            "metrics": metrics,
        }
        terminal_event_id = sink.append(
            run_id=run_id,
            event_id=f"{run_id}.terminal.failed",
            event_type="run.failed",
            observed_at=finished_at,
            payload=error_payload,
        )
        terminal_event = journal.event(terminal_event_id)
        assert terminal_event is not None
        terminal_artifact = store.artifacts.put_json(
            {
                "schema_version": "market-impact.agent-run-error.v1",
                "run_id": run_id,
                "journal_hash": terminal_event.event_hash,
                **error_payload,
            }
        )
        judgment_artifact_hash = None
    terminal = (
        write_strategy_case_terminal(
            journal=journal,
            artifact_store=store.artifacts,
            run_id=run_id,
            status=status,
            finished_at=finished_at,
            run_terminal_artifact_hash=terminal_artifact.content_hash,
            judgment_artifact_hash=judgment_artifact_hash,
        )
        if fabrication is None
        else None
    )
    if fabrication is not None:
        expected_error = (
            "zero token usage"
            if fabrication == "zero_metrics"
            else "no authoritative pre-turn context"
        )
        with pytest.raises(ValueError, match=expected_error):
            write_strategy_case_terminal(
                journal=journal,
                artifact_store=store.artifacts,
                run_id=run_id,
                status=status,
                finished_at=finished_at,
                run_terminal_artifact_hash=terminal_artifact.content_hash,
                judgment_artifact_hash=judgment_artifact_hash,
            )
        assert journal.get_run(run_id).status is RunStatus.RUNNING
        return ""
    assert terminal is not None
    assert journal.get_run(run_id).status is status
    journal.finish(
        run_id=run_id,
        status=status,
        finished_at=finished_at,
        terminal_artifact_id=terminal_artifact.content_hash,
    )
    return terminal.terminal_id


def test_authority_identity_is_stable_and_legacy_journal_is_ineligible(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    reopened = LocalDataSnapshotStore(tmp_path / "authority")

    assert reopened.harness_authority_id == store.harness_authority_id
    assert RunJournal.authoritative(store).harness_authority_id == store.harness_authority_id
    with pytest.raises(ValueError, match="legacy path"):
        RunJournal(tmp_path / "legacy.sqlite3").start_run(
            run_id="legacy-run",
            config_hash=HASH,
            created_at=NOW,
            strategy_plan_artifact_hash=HASH,
        )


def test_measurement_writer_accepts_only_actual_bridge_receipt_ids(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    source = _authoritative_source_snapshot(store)
    template = backtest_request(data_snapshot_id=source.snapshot_id)
    registration = _registration(
        source_snapshot_id=source.snapshot_id,
        universe_hash=strategy_backtest_universe_hash(template.instrument_ids),
        cost_model_hash=strategy_backtest_cost_model_hash(template.simulation),
        fill_model_hash=strategy_backtest_fill_model_hash(template.simulation),
    )
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    definition = registration.evaluation_cases[0]
    plan = authority.build_case_run_plan(
        registration_id=registration.registration_id,
        run_id="actual-outcome-run",
        case_id=definition.case_id,
    )
    assert plan.evidence_lane is StrategyEvidenceLane.RETROSPECTIVE
    assert plan.evidence_unavailable_reason == "strict_qualification_lineage_owner_unavailable"
    with pytest.raises(ValueError, match="Strict-PIT plan construction is unavailable"):
        replace(plan, evidence_lane=StrategyEvidenceLane.STRICT_PIT)
    start_strategy_case_run(
        journal=RunJournal.authoritative(store),
        artifact_store=store.artifacts,
        run_id=plan.run_id,
        plan=plan,
        config_hash=RUN_SPEC_HASH,
        created_at=NOW,
    )
    bridge = NautilusBacktestBridge(
        SNAPSHOT_PATH,
        snapshot_store=store,
        artifact_store=store.artifacts,
    )
    request = backtest_request(
        data_snapshot_id=source.snapshot_id,
        bound_signal=backtest_signal(event_id=definition.root_event_id),
    )
    candidate = bridge.run_strategy_outcome(
        request,
        case_id=definition.case_id,
        variant=registration.candidate_variant,
    )
    assert not isinstance(candidate, StrategyBacktestOutcomeMissing)
    baseline_request = replace(
        request,
        strategy_ref=registration.primary_baseline.variant.strategy_ref,
        target_selection_ref=registration.primary_baseline.variant.target_selection_ref,
    )
    baseline = bridge.run_strategy_outcome(
        baseline_request,
        case_id=definition.case_id,
        variant=registration.primary_baseline.variant,
    )
    assert not isinstance(baseline, StrategyBacktestOutcomeMissing)
    writer = StrategyCaseMeasurementWriter(store)
    writer.record(
        run_id=plan.run_id,
        candidate_receipt_id=candidate.receipt_id,
        baseline_receipt_id=baseline.receipt_id,
        measured_at=NOW + timedelta(seconds=30),
    )
    with store.authority_transaction() as connection:
        rows = connection.execute(
            "SELECT arm, artifact_hash FROM strategy_case_measurements_v2 "
            "WHERE run_id = ? ORDER BY arm",
            (plan.run_id,),
        ).fetchall()
    assert len(rows) == 2
    measured = [
        cast(dict[str, object], store.artifacts.read_json(str(row["artifact_hash"])))
        for row in rows
    ]
    assert {item["outcome_receipt_id"] for item in measured} == {
        candidate.receipt_id,
        baseline.receipt_id,
    }
    assert {item["net_return"] for item in measured} == {
        str(candidate.net_return),
        str(baseline.net_return),
    }

    with pytest.raises(ValueError, match="frozen case, variant"):
        writer.record(
            run_id=plan.run_id,
            candidate_receipt_id=candidate.receipt_id,
            baseline_receipt_id=candidate.receipt_id,
            measured_at=NOW + timedelta(seconds=31),
        )

    with pytest.raises(KeyError, match="unknown strategy backtest outcome receipt"):
        writer.record(
            run_id=plan.run_id,
            candidate_receipt_id="strategy-backtest-outcome-" + "9" * 64,
            baseline_receipt_id=baseline.receipt_id,
            measured_at=NOW + timedelta(seconds=31),
        )
    legacy = bridge.run(request)
    with pytest.raises(KeyError, match="unknown strategy backtest outcome receipt"):
        writer.record(
            run_id=plan.run_id,
            candidate_receipt_id=legacy.result_hash,
            baseline_receipt_id=baseline.receipt_id,
            measured_at=NOW + timedelta(seconds=31),
        )
    with pytest.raises(TypeError):
        cast(Any, writer.record)(
            run_id=plan.run_id,
            candidate=_measurement(definition.case_id, "candidate"),
            baseline=_measurement(definition.case_id, "primary_baseline"),
            measured_at=NOW + timedelta(seconds=31),
        )
    with pytest.raises(ValueError, match="only come from"):
        RunJournal(
            store.index_path,
            harness_authority_id=store.harness_authority_id,
        )


def test_portfolio_aggregation_requires_complete_marks_on_overlapping_paths(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    source = _authoritative_source_snapshot(store)
    template = backtest_request(data_snapshot_id=source.snapshot_id)
    registration = _registration(
        source_snapshot_id=source.snapshot_id,
        universe_hash=strategy_backtest_universe_hash(template.instrument_ids),
        cost_model_hash=strategy_backtest_cost_model_hash(template.simulation),
        fill_model_hash=strategy_backtest_fill_model_hash(template.simulation),
    )
    first_definition, second_definition = registration.evaluation_cases[:2]
    bridge = NautilusBacktestBridge(
        SNAPSHOT_PATH,
        snapshot_store=store,
        artifact_store=store.artifacts,
    )
    candidate_requests = (
        backtest_request(
            data_snapshot_id=source.snapshot_id,
            bound_signal=backtest_signal(event_id=first_definition.root_event_id),
        ),
        replace(
            backtest_request(
                data_snapshot_id=source.snapshot_id,
                bound_signal=backtest_signal(event_id=second_definition.root_event_id),
            ),
            start_at=datetime(2026, 9, 1, 1, tzinfo=UTC),
        ),
    )
    candidates = tuple(
        bridge.run_strategy_outcome(
            request,
            case_id=definition.case_id,
            variant=registration.candidate_variant,
        )
        for definition, request in zip(
            (first_definition, second_definition), candidate_requests, strict=True
        )
    )
    baseline_requests = tuple(
        replace(
            request,
            strategy_ref=registration.primary_baseline.variant.strategy_ref,
            target_selection_ref=registration.primary_baseline.variant.target_selection_ref,
        )
        for request in candidate_requests
    )
    baselines = tuple(
        bridge.run_strategy_outcome(
            request,
            case_id=definition.case_id,
            variant=registration.primary_baseline.variant,
        )
        for definition, request in zip(
            (first_definition, second_definition), baseline_requests, strict=True
        )
    )
    assert all(not isinstance(item, StrategyBacktestOutcomeMissing) for item in candidates)
    assert all(not isinstance(item, StrategyBacktestOutcomeMissing) for item in baselines)
    candidate_receipts = cast(tuple[StrategyBacktestOutcomeReceipt, ...], candidates)
    baseline_receipts = cast(tuple[StrategyBacktestOutcomeReceipt, ...], baselines)

    metrics, reason = _portfolio_from_receipts(
        registration,
        [
            (first_definition, candidate_receipts[0]),
            (second_definition, candidate_receipts[1]),
        ],
        [
            (first_definition, baseline_receipts[0]),
            (second_definition, baseline_receipts[1]),
        ],
        artifact_store=store.artifacts,
    )

    assert metrics is None
    assert reason == "portfolio_aggregation_incomplete_adverse_excursion_coverage"


def test_portfolio_execution_metrics_use_common_capital_active_weights(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    source = _authoritative_source_snapshot(store)
    template = backtest_request(data_snapshot_id=source.snapshot_id)
    registration = _registration(
        source_snapshot_id=source.snapshot_id,
        universe_hash=strategy_backtest_universe_hash(template.instrument_ids),
        cost_model_hash=strategy_backtest_cost_model_hash(template.simulation),
        fill_model_hash=strategy_backtest_fill_model_hash(template.simulation),
    )
    first_definition, second_definition = registration.evaluation_cases[:2]
    bridge = NautilusBacktestBridge(
        SNAPSHOT_PATH,
        snapshot_store=store,
        artifact_store=store.artifacts,
    )

    def actual_receipt(
        definition: StrategyCaseDefinition, *, baseline: bool
    ) -> StrategyBacktestOutcomeReceipt:
        request = backtest_request(
            data_snapshot_id=source.snapshot_id,
            bound_signal=backtest_signal(event_id=definition.root_event_id),
        )
        variant = registration.candidate_variant
        if baseline:
            request = replace(
                request,
                strategy_ref=registration.primary_baseline.variant.strategy_ref,
                target_selection_ref=registration.primary_baseline.variant.target_selection_ref,
            )
            variant = registration.primary_baseline.variant
        outcome = bridge.run_strategy_outcome(
            request,
            case_id=definition.case_id,
            variant=variant,
        )
        assert not isinstance(outcome, StrategyBacktestOutcomeMissing)
        return outcome

    candidates = (
        actual_receipt(first_definition, baseline=False),
        actual_receipt(second_definition, baseline=False),
    )
    baselines = (
        actual_receipt(first_definition, baseline=True),
        actual_receipt(second_definition, baseline=True),
    )
    start = candidates[0].capital_path[0].observed_at
    entry = candidates[0].fills[0].filled_at
    exit_at = candidates[0].fills[-1].filled_at

    def rewritten(
        receipt: StrategyBacktestOutcomeReceipt,
        *,
        quantity: str,
        price: str,
        available: str,
        adverse: tuple[str, str, str],
    ) -> StrategyBacktestOutcomeReceipt:
        payload = receipt.to_dict()
        payload.pop("receipt_id")
        payload["capital_path"] = [
            {"observed_at": start.isoformat().replace("+00:00", "Z"), "equity": "1000000"},
            {"observed_at": entry.isoformat().replace("+00:00", "Z"), "equity": "1000000"},
            {"observed_at": exit_at.isoformat().replace("+00:00", "Z"), "equity": "1000000"},
        ]
        payload["fills"] = [
            {
                "side": "buy",
                "filled_at": entry.isoformat().replace("+00:00", "Z"),
                "quantity": quantity,
                "price": price,
                "commission": "0",
                "available_liquidity_quantity": available,
            },
            {
                "side": "sell",
                "filled_at": exit_at.isoformat().replace("+00:00", "Z"),
                "quantity": quantity,
                "price": price,
                "commission": "0",
                "available_liquidity_quantity": available,
            },
        ]
        payload["adverse_excursion_path"] = [
            {
                "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
                "adverse_excursion": value,
            }
            for observed_at, value in zip((start, entry, exit_at), adverse, strict=True)
        ]
        payload["adverse_excursion"] = max(adverse, key=Decimal)
        payload["receipt_id"] = "strategy-backtest-outcome-" + canonical_hash(payload)
        return strategy_backtest_outcome_from_dict(payload)

    heterogeneous = (
        rewritten(
            candidates[0],
            quantity="10",
            price="100",
            available="100",
            adverse=("0", "0.1", "0.05"),
        ),
        rewritten(
            candidates[1],
            quantity="40",
            price="50",
            available="80",
            adverse=("0", "0.3", "0.4"),
        ),
    )

    duplicate_normal_payload = candidates[0].to_dict()
    duplicate_normal_payload.pop("receipt_id")
    normal_adverse = cast(
        list[dict[str, object]], duplicate_normal_payload["adverse_excursion_path"]
    )
    normal_adverse.insert(1, dict(normal_adverse[1]))
    duplicate_normal_payload["receipt_id"] = "strategy-backtest-outcome-" + canonical_hash(
        duplicate_normal_payload
    )
    with pytest.raises(ValueError, match="strictly chronological"):
        strategy_backtest_outcome_from_dict(duplicate_normal_payload)

    metrics, reason = _portfolio_from_receipts(
        registration,
        list(zip((first_definition, second_definition), heterogeneous, strict=True)),
        list(zip((first_definition, second_definition), baselines, strict=True)),
        artifact_store=store.artifacts,
    )

    assert reason is None
    assert metrics is not None
    assert metrics.candidate_turnover == Decimal("0.003")
    assert metrics.candidate_liquidity_utilization * Decimal(14) == Decimal(3)
    assert metrics.candidate_adverse_excursion == Decimal("0.225")
    assert metrics.candidate_max_drawdown == 0

    missing_payload = heterogeneous[0].to_dict()
    missing_payload.pop("receipt_id")
    missing_fills = cast(list[dict[str, object]], missing_payload["fills"])
    missing_fills[0]["available_liquidity_quantity"] = None
    missing_metrics = cast(list[dict[str, object]], missing_payload["missing_metrics"])
    missing_metrics.append({"name": "liquidity", "reason": "source_unavailable"})
    missing_metrics.sort(key=lambda item: cast(str, item["name"]))
    missing_payload["receipt_id"] = "strategy-backtest-outcome-" + canonical_hash(missing_payload)
    missing_liquidity = strategy_backtest_outcome_from_dict(missing_payload)

    missing, missing_reason = _portfolio_from_receipts(
        registration,
        [
            (first_definition, missing_liquidity),
            (second_definition, heterogeneous[1]),
        ],
        list(zip((first_definition, second_definition), baselines, strict=True)),
        artifact_store=store.artifacts,
    )
    assert missing is None
    assert missing_reason == "portfolio_aggregation_missing_liquidity_observation"

    for coverage_gap in ("before_first_mark", "after_last_mark", "interior_gap"):
        incomplete_payload = heterogeneous[0].to_dict()
        incomplete_payload.pop("receipt_id")
        adverse_points = cast(list[dict[str, object]], incomplete_payload["adverse_excursion_path"])
        if coverage_gap == "before_first_mark":
            adverse_points[:] = [adverse_points[-1]]
        elif coverage_gap == "after_last_mark":
            adverse_points[:] = adverse_points[:-1]
        else:
            midpoint = entry + (exit_at - entry) / 2
            capital_points = cast(list[dict[str, object]], incomplete_payload["capital_path"])
            capital_points.insert(
                -1,
                {
                    "observed_at": midpoint.isoformat().replace("+00:00", "Z"),
                    "equity": "1000000",
                },
            )
        incomplete_payload["adverse_excursion"] = max(
            (cast(str, point["adverse_excursion"]) for point in adverse_points), key=Decimal
        )
        incomplete_payload["receipt_id"] = "strategy-backtest-outcome-" + canonical_hash(
            incomplete_payload
        )
        incomplete_adverse = strategy_backtest_outcome_from_dict(incomplete_payload)

        missing, missing_reason = _portfolio_from_receipts(
            registration,
            [
                (first_definition, incomplete_adverse),
                (second_definition, heterogeneous[1]),
            ],
            list(zip((first_definition, second_definition), baselines, strict=True)),
            artifact_store=store.artifacts,
        )
        assert missing is None
        assert missing_reason == "portfolio_aggregation_incomplete_adverse_excursion_coverage"

    stress_payload = cast(
        dict[str, object],
        store.artifacts.read_json(candidates[0].stress_evidence_artifact_hash or ""),
    )
    stress_adverse = cast(list[dict[str, object]], stress_payload["adverse_excursion_path"])
    stress_adverse.pop()
    incomplete_stress_artifact = store.artifacts.put_json(stress_payload)
    incomplete_stress_payload = candidates[0].to_dict()
    incomplete_stress_payload.pop("receipt_id")
    incomplete_stress_payload["stress_evidence_artifact_hash"] = (
        incomplete_stress_artifact.content_hash
    )
    incomplete_stress_payload["receipt_id"] = "strategy-backtest-outcome-" + canonical_hash(
        incomplete_stress_payload
    )
    incomplete_stress = strategy_backtest_outcome_from_dict(incomplete_stress_payload)

    missing, missing_reason = _portfolio_from_receipts(
        registration,
        [(first_definition, incomplete_stress), (second_definition, candidates[1])],
        list(zip((first_definition, second_definition), baselines, strict=True)),
        artifact_store=store.artifacts,
    )
    assert missing is None
    assert missing_reason == "portfolio_aggregation_incomplete_stress_adverse_excursion_coverage"

    duplicate_stress_payload = cast(
        dict[str, object],
        store.artifacts.read_json(candidates[0].stress_evidence_artifact_hash or ""),
    )
    duplicate_stress_adverse = cast(
        list[dict[str, object]], duplicate_stress_payload["adverse_excursion_path"]
    )
    duplicate_stress_adverse.insert(1, dict(duplicate_stress_adverse[1]))
    duplicate_stress_artifact = store.artifacts.put_json(duplicate_stress_payload)
    duplicate_stress_receipt_payload = candidates[0].to_dict()
    duplicate_stress_receipt_payload.pop("receipt_id")
    duplicate_stress_receipt_payload["stress_evidence_artifact_hash"] = (
        duplicate_stress_artifact.content_hash
    )
    duplicate_stress_receipt_payload["receipt_id"] = "strategy-backtest-outcome-" + canonical_hash(
        duplicate_stress_receipt_payload
    )
    duplicate_stress = strategy_backtest_outcome_from_dict(duplicate_stress_receipt_payload)

    missing, missing_reason = _portfolio_from_receipts(
        registration,
        [(first_definition, duplicate_stress), (second_definition, candidates[1])],
        list(zip((first_definition, second_definition), baselines, strict=True)),
        artifact_store=store.artifacts,
    )
    assert missing is None
    assert missing_reason == "portfolio_aggregation_invalid_stress_adverse_excursion_path"


def test_evaluator_accepts_only_registration_id_and_missing_actuals_are_inconclusive(
    tmp_path: Path,
) -> None:
    assert tuple(inspect.signature(StrategyValidationAuthorityStore.evaluate).parameters) == (
        "self",
        "registration_id",
    )
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    for index, definition in enumerate(registration.evaluation_cases):
        _run(
            store,
            registration,
            definition.case_id,
            run_id=f"run-{definition.case_id}",
            started_at=NOW + timedelta(minutes=index),
            measurements=index != 0,
        )

    seal = authority.seal_run_set(registration.registration_id, sealed_at=NOW + timedelta(days=1))
    report = authority.evaluate(registration.registration_id)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.harness_authority_id == store.harness_authority_id
    assert report.run_set_seal_hash == seal.seal_hash
    assert "missing_actual_measurement:holdout-00" in report.reasons
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas/strategy-validation-report-v2.schema.json").read_text(
            encoding="utf-8"
        )
    )
    cast(
        Validator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    ).validate(report.to_dict())


def test_invalid_measurement_case_is_typed_inconclusive_evidence(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    definition = registration.evaluation_cases[0]
    run_id = "invalid-measurement-case"
    _run(store, registration, definition.case_id, run_id=run_id, started_at=NOW)
    terminal = cast(Any, authority)._terminals(registration.registration_id)[0]
    plan = cast(Any, authority)._plan(run_id)
    candidate = store.artifacts.put_json(_measurement("different-case", "candidate").to_dict())
    baseline = store.artifacts.put_json(
        _measurement(definition.case_id, "primary_baseline").to_dict()
    )
    terminal_payload = terminal.to_dict()
    terminal_payload.update(
        candidate_measurement_artifact_hash=candidate.content_hash,
        candidate_measurement_artifact_path=str(candidate.path),
        baseline_measurement_artifact_hash=baseline.content_hash,
        baseline_measurement_artifact_path=str(baseline.path),
    )
    terminal_payload.pop("terminal_id")
    terminal_payload["terminal_id"] = "strategy-case-terminal-" + canonical_hash(terminal_payload)
    rebound_terminal = cast(Any, strategy_validation_module)._strategy_terminal_from_dict(
        terminal_payload
    )

    pair, reason = cast(Any, authority)._measurements(
        rebound_terminal,
        definition.case_id,
        plan=plan,
        registration=registration,
    )

    assert pair is None
    assert reason == "candidate_measurement_case_mismatch"


def test_invalid_measurement_ownership_and_divergence_are_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    definition = registration.evaluation_cases[0]
    run_id = "invalid-measurement-ownership"
    _run(store, registration, definition.case_id, run_id=run_id, started_at=NOW)
    terminal = cast(Any, authority)._terminals(registration.registration_id)[0]
    plan = cast(Any, authority)._plan(run_id)
    candidate_measurement = _measurement(definition.case_id, "candidate")
    baseline_measurement = _measurement(definition.case_id, "primary_baseline")
    candidate = store.artifacts.put_json(candidate_measurement.to_dict())
    baseline = store.artifacts.put_json(baseline_measurement.to_dict())
    terminal_payload = terminal.to_dict()
    terminal_payload.update(
        candidate_measurement_artifact_hash=candidate.content_hash,
        candidate_measurement_artifact_path=str(candidate.path),
        baseline_measurement_artifact_hash=baseline.content_hash,
        baseline_measurement_artifact_path=str(baseline.path),
    )
    terminal_payload.pop("terminal_id")
    terminal_payload["terminal_id"] = "strategy-case-terminal-" + canonical_hash(terminal_payload)
    rebound_terminal = cast(Any, strategy_validation_module)._strategy_terminal_from_dict(
        terminal_payload
    )
    receipt = object()

    def reopen_outcome(*_args: object) -> tuple[object, object]:
        return receipt, object()

    monkeypatch.setattr(
        strategy_validation_module,
        "reopen_strategy_backtest_outcome",
        reopen_outcome,
    )

    def reject_owner(**_: object) -> None:
        raise ValueError("fixture ownership mismatch")

    monkeypatch.setattr(strategy_validation_module, "_verify_receipt_for_plan", reject_owner)
    pair, reason = cast(Any, authority)._measurements(
        rebound_terminal,
        definition.case_id,
        plan=plan,
        registration=registration,
    )
    assert pair is None
    assert reason == "candidate_outcome_receipt_ownership_mismatch"

    def accept_owner(**_kwargs: object) -> None:
        return None

    def divergent_measurement(_receipt: object) -> StrategyMeasurementArtifact:
        return replace(candidate_measurement, net_return=Decimal("0.05"))

    monkeypatch.setattr(strategy_validation_module, "_verify_receipt_for_plan", accept_owner)
    monkeypatch.setattr(
        strategy_validation_module,
        "_measurement_from_receipt",
        divergent_measurement,
    )
    pair, reason = cast(Any, authority)._measurements(
        rebound_terminal,
        definition.case_id,
        plan=plan,
        registration=registration,
    )
    assert pair is None
    assert reason == "candidate_measurement_receipt_divergence"


def test_evaluator_surfaces_precise_invalid_measurement_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    for index, definition in enumerate(registration.evaluation_cases):
        _run(
            store,
            registration,
            definition.case_id,
            run_id=f"run-{definition.case_id}",
            started_at=NOW + timedelta(minutes=index),
        )
    authority.seal_run_set(registration.registration_id, sealed_at=NOW + timedelta(days=1))
    original = StrategyValidationAuthorityStore._measurements  # pyright: ignore[reportPrivateUsage]
    invalid_case_id = registration.evaluation_cases[0].case_id

    def typed_invalid(
        self: StrategyValidationAuthorityStore,
        terminal: object,
        case_id: str,
        **kwargs: object,
    ) -> object:
        if case_id == invalid_case_id:
            return None, "candidate_measurement_arm_mismatch"
        return original(self, cast(Any, terminal), case_id, **cast(Any, kwargs))

    monkeypatch.setattr(StrategyValidationAuthorityStore, "_measurements", typed_invalid)

    report = authority.evaluate(registration.registration_id)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert f"candidate_measurement_arm_mismatch:{invalid_case_id}" in report.reasons


def test_earliest_terminal_retry_is_selected_and_cross_root_cannot_revalidate(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    first_case = registration.evaluation_cases[0]
    earliest = _run(
        store,
        registration,
        first_case.case_id,
        run_id="earliest-failed",
        started_at=NOW,
        status=RunStatus.FAILED,
        measurements=False,
    )
    _run(
        store,
        registration,
        first_case.case_id,
        run_id="later-favorable",
        started_at=NOW + timedelta(hours=1),
    )
    for index, definition in enumerate(registration.evaluation_cases[1:], start=1):
        _run(
            store,
            registration,
            definition.case_id,
            run_id=f"run-{definition.case_id}",
            started_at=NOW + timedelta(minutes=index),
        )
    seal = authority.seal_run_set(registration.registration_id, sealed_at=NOW + timedelta(days=1))
    report = authority.evaluate(registration.registration_id)

    assert seal.selected_terminal_ids[0] == earliest
    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    other = StrategyValidationAuthorityStore(LocalDataSnapshotStore(tmp_path / "other"))
    with pytest.raises(ValueError, match="different Harness authority"):
        other.revalidate(report)


def test_requested_tool_budget_failure_replays_and_seals_as_genuine_failure(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    definition = registration.evaluation_cases[0]
    run_id = "two-requested-max-one"
    plan = StrategyCaseRunPlan.build(
        store=store,
        registration=registration,
        run_id=run_id,
        case_id=definition.case_id,
    )
    journal = RunJournal.authoritative(store)
    start_strategy_case_run(
        journal=journal,
        artifact_store=store.artifacts,
        run_id=run_id,
        plan=plan,
        config_hash=RUN_SPEC_HASH,
        created_at=NOW,
    )
    sink = _TestPrivilegedEventSink(store=store, journal=journal)
    sink.append(
        run_id=run_id,
        event_id=f"{run_id}.started",
        event_type="run.started",
        observed_at=NOW,
        payload={
            "config_hash": RUN_SPEC_HASH,
            "provider_id": "fixture-provider",
            "model": "fixture-model",
            "strategy_plan_artifact_hash": plan.plan_hash,
        },
    )
    context = store.artifacts.put_json([{"role": "user", "content": "fixture"}])
    assistant = store.artifacts.put_json({"role": "assistant", "content": ""})
    raw = store.artifacts.put_json({"response_id": "budget-response"})
    sink.append(
        run_id=run_id,
        event_id=f"{run_id}.turn.1",
        event_type="model.turn.completed",
        observed_at=NOW + timedelta(seconds=1),
        payload={
            "response_id": "budget-response",
            "provider_id": "fixture-provider",
            "model": "fixture-model",
            "assistant_artifact_hash": assistant.content_hash,
            "raw_response_artifact_hash": raw.content_hash,
            "tool_calls": [
                {"call_id": "call-1", "name": "read_evidence", "arguments": {}},
                {"call_id": "call-2", "name": "read_evidence", "arguments": {}},
            ],
            "finish_reason": "tool_calls",
            "usage": {"input_tokens": 10, "output_tokens": 5},
            "latency_ms": 0.0,
            "attempts": 1,
            "estimated_cost_microusd": 0,
            "tool_surface_hash": "0" * 64,
            "tool_manifest_hashes": [],
            "mcp_binding_hashes": [],
            "context_before_turn_hash": context.content_hash,
        },
    )
    metrics: dict[str, object] = {
        "turns": 1,
        "tool_calls": 2,
        "input_tokens": 10,
        "output_tokens": 5,
        "result_bytes": 0,
        "latency_ms": 0.0,
        "provider_attempts": 1,
        "estimated_cost_microusd": 0,
    }
    finished_at = NOW + timedelta(seconds=2)
    error_payload: dict[str, object] = {
        "status": RunStatus.BUDGET_EXHAUSTED.value,
        "finished_at": finished_at.isoformat().replace("+00:00", "Z"),
        "error_class": "_BudgetExceeded",
        "message": "run exceeded its tool-call budget",
        "metrics": metrics,
    }
    terminal_event_id = sink.append(
        run_id=run_id,
        event_id=f"{run_id}.terminal.failed",
        event_type="run.failed",
        observed_at=finished_at,
        payload=error_payload,
    )
    terminal_event = journal.event(terminal_event_id)
    assert terminal_event is not None
    artifact = store.artifacts.put_json(
        {
            "schema_version": "market-impact.agent-run-error.v1",
            "run_id": run_id,
            "journal_hash": terminal_event.event_hash,
            **error_payload,
        }
    )
    terminal = write_strategy_case_terminal(
        journal=journal,
        artifact_store=store.artifacts,
        run_id=run_id,
        status=RunStatus.BUDGET_EXHAUSTED,
        finished_at=finished_at,
        run_terminal_artifact_hash=artifact.content_hash,
        judgment_artifact_hash=None,
    )
    assert terminal is not None
    journal.finish(
        run_id=run_id,
        status=RunStatus.BUDGET_EXHAUSTED,
        finished_at=finished_at,
        terminal_artifact_id=artifact.content_hash,
    )
    assert (
        reopen_authoritative_agent_terminal(
            journal=journal,
            artifact_store=store.artifacts,
            run_id=run_id,
            status=RunStatus.BUDGET_EXHAUSTED,
            finished_at=finished_at,
            terminal_artifact_hash=artifact.content_hash,
        )
        is None
    )
    for index, remaining in enumerate(registration.evaluation_cases[1:], start=1):
        _run(
            store,
            registration,
            remaining.case_id,
            run_id=f"remaining-{remaining.case_id}",
            started_at=NOW + timedelta(minutes=index),
        )
    seal = authority.seal_run_set(registration.registration_id, sealed_at=NOW + timedelta(days=1))
    assert terminal.terminal_id in seal.selected_terminal_ids


def test_strategy_evaluate_cli_has_no_outcome_metric_or_selector_overrides() -> None:
    parser = build_parser()
    parsed = parser.parse_args(
        [
            "agent",
            "strategy-evaluate",
            "--state-root",
            "/tmp/authority",
            "--registration-id",
            "strategy-validation-registration-" + "a" * 64,
        ]
    )

    assert set(vars(parsed)) == {
        "command",
        "agent_command",
        "state_root",
        "registration_id",
    }


def test_orphan_prebound_plan_blocks_run_set_seal(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    plan = StrategyCaseRunPlan.build(
        store=store,
        registration=registration,
        run_id="crashed-before-run-row",
        case_id=registration.evaluation_cases[0].case_id,
    )
    bind_strategy_case_run_plan(
        journal=RunJournal.authoritative(store),
        artifact_store=store.artifacts,
        run_id=plan.run_id,
        plan=plan,
    )

    with pytest.raises(ValueError, match="missing or unfinished"):
        authority.seal_run_set(registration.registration_id, sealed_at=NOW)


def test_eventless_or_non_judgment_terminal_cannot_claim_agent_completion(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    case_id = registration.evaluation_cases[0].case_id
    plan = StrategyCaseRunPlan.build(
        store=store,
        registration=registration,
        run_id="forged-agent-completion",
        case_id=case_id,
    )
    journal = RunJournal.authoritative(store)
    start_strategy_case_run(
        journal=journal,
        artifact_store=store.artifacts,
        run_id=plan.run_id,
        plan=plan,
        config_hash=RUN_SPEC_HASH,
        created_at=NOW,
    )
    arbitrary = store.artifacts.put_json({"run_id": plan.run_id, "actual": True})
    with pytest.raises(ValueError, match="no authoritative Run Journal events"):
        write_strategy_case_terminal(
            journal=journal,
            artifact_store=store.artifacts,
            run_id=plan.run_id,
            status=RunStatus.COMPLETED,
            finished_at=NOW + timedelta(minutes=1),
            run_terminal_artifact_hash=arbitrary.content_hash,
            judgment_artifact_hash=arbitrary.content_hash,
        )

    proposal = JudgmentProposal(
        event_id=f"event-{case_id}",
        decision=JudgmentDecision.ABSTAIN,
        summary="No signal.",
        transmission_steps=(),
        candidates=(),
        blockers=("no authoritative model completion",),
        unresolved_questions=(),
        stopped_reason="eventless forged completion",
    )
    transcript = store.artifacts.put_json([])
    raw_response = store.artifacts.put_json({"response_id": "forged"})
    forged = JudgmentArtifact.build(
        run_id=plan.run_id,
        evidence_pack_id="evidence-pack-forged",
        provider_id="fixture-provider",
        model="fixture-model",
        runtime_config_hash=HASH,
        prompt_hash=registration.prompt_hash,
        skill_hashes=SKILL_HASHES,
        tool_manifest_hashes=TOOL_MANIFEST_HASHES,
        tool_surface_hash="0" * 64,
        mcp_server_hashes=(),
        context_estimator_id="fixture-counter",
        compactor_id="fixture-compactor",
        journal_hash=journal.journal_hash(plan.run_id),
        transcript_hash=transcript.content_hash,
        raw_response_hash=raw_response.content_hash,
        started_at=NOW,
        finished_at=NOW + timedelta(minutes=1),
        proposal=proposal,
    )
    forged_artifact = store.artifacts.put_json(forged.to_dict())
    with pytest.raises(ValueError, match="no authoritative Run Journal events"):
        write_strategy_case_terminal(
            journal=journal,
            artifact_store=store.artifacts,
            run_id=plan.run_id,
            status=RunStatus.COMPLETED,
            finished_at=NOW + timedelta(minutes=1),
            run_terminal_artifact_hash=forged_artifact.content_hash,
            judgment_artifact_hash=forged_artifact.content_hash,
        )
    assert journal.get_run(plan.run_id).status is RunStatus.RUNNING


def test_zero_run_registration_initializes_journal_schema_but_cannot_seal(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    with store.authority_transaction() as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'runs'"
            ).fetchone()
            is not None
        )
    with pytest.raises(ValueError, match="every registered evaluation case"):
        authority.seal_run_set(registration.registration_id, sealed_at=NOW)


def test_all_24_unowned_fabricated_event_rows_cannot_create_a_run_set(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    journal = RunJournal.authoritative(store)
    assert not hasattr(runtime_store_module, "_AGENT_ENGINE_EVENT_TOKEN")
    assert not hasattr(journal, "append_privileged")
    assert not hasattr(journal, "_event_signer_for_agent_engine")
    assert not any(
        term in name
        for name in dir(journal)
        if not name.startswith("_")
        for term in ("signer", "sink", "key")
    )
    assert not any(
        term in name
        for name in dir(store)
        if not name.startswith("_")
        for term in ("signer", "sink", "key")
    )
    for index, definition in enumerate(registration.evaluation_cases):
        _run(
            store,
            registration,
            definition.case_id,
            run_id=f"fabricated-{definition.case_id}",
            started_at=NOW + timedelta(minutes=index),
            measurements=False,
            fabrication="unowned_zero_metrics",
        )
    with store.authority_transaction() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM strategy_case_terminals_v2").fetchone()[0] == 0
        )
    with pytest.raises(ValueError, match="missing or unfinished"):
        authority.seal_run_set(registration.registration_id, sealed_at=NOW + timedelta(days=1))


def test_all_24_eventless_failures_cannot_write_terminals_or_seal(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    journal = RunJournal.authoritative(store)
    for index, definition in enumerate(registration.evaluation_cases):
        run_id = f"eventless-failure-{definition.case_id}"
        started_at = NOW + timedelta(minutes=index)
        finished_at = started_at + timedelta(seconds=1)
        plan = StrategyCaseRunPlan.build(
            store=store,
            registration=registration,
            run_id=run_id,
            case_id=definition.case_id,
        )
        start_strategy_case_run(
            journal=journal,
            artifact_store=store.artifacts,
            run_id=run_id,
            plan=plan,
            config_hash=RUN_SPEC_HASH,
            created_at=started_at,
        )
        artifact = store.artifacts.put_json(
            {
                "schema_version": "market-impact.agent-run-error.v1",
                "run_id": run_id,
                "status": RunStatus.FAILED.value,
                "journal_hash": RUN_SPEC_HASH,
                "finished_at": finished_at.isoformat().replace("+00:00", "Z"),
                "error_class": "FabricatedFailure",
                "message": "caller-authored terminal",
                "metrics": {
                    "turns": 0,
                    "tool_calls": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "result_bytes": 0,
                    "latency_ms": 0.0,
                    "provider_attempts": 0,
                    "estimated_cost_microusd": 0,
                },
            }
        )
        with pytest.raises(ValueError, match="no authoritative Run Journal events"):
            write_strategy_case_terminal(
                journal=journal,
                artifact_store=store.artifacts,
                run_id=run_id,
                status=RunStatus.FAILED,
                finished_at=finished_at,
                run_terminal_artifact_hash=artifact.content_hash,
                judgment_artifact_hash=None,
            )
    with store.authority_transaction() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM strategy_case_terminals_v2").fetchone()[0] == 0
        )
    with pytest.raises(ValueError, match="missing or unfinished"):
        authority.seal_run_set(registration.registration_id, sealed_at=NOW + timedelta(days=1))


def test_privileged_event_signatures_are_root_bound_and_tamper_evident(
    tmp_path: Path,
) -> None:
    registration = _registration()
    stores = (
        LocalDataSnapshotStore(tmp_path / "first"),
        LocalDataSnapshotStore(tmp_path / "second"),
    )
    key_paths = tuple(store.root / ".harness-event-hmac.key" for store in stores)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in key_paths)
    assert key_paths[0].read_bytes() != key_paths[1].read_bytes()
    first_key = key_paths[0].read_bytes()
    LocalDataSnapshotStore(tmp_path / "first")
    assert key_paths[0].read_bytes() == first_key
    journals: list[RunJournal] = []
    for index, store in enumerate(stores):
        StrategyValidationAuthorityStore(store).register(registration)
        plan = StrategyCaseRunPlan.build(
            store=store,
            registration=registration,
            run_id=f"root-bound-{index}",
            case_id=registration.evaluation_cases[index].case_id,
        )
        journal = RunJournal.authoritative(store)
        start_strategy_case_run(
            journal=journal,
            artifact_store=store.artifacts,
            run_id=plan.run_id,
            plan=plan,
            config_hash=RUN_SPEC_HASH,
            created_at=NOW,
        )
        journals.append(journal)

    mismatched_sink = _TestPrivilegedEventSink(
        store=stores[1],
        journal=journals[1],
        signing_store=stores[0],
    )
    mismatched_sink.append(
        run_id="root-bound-1",
        event_id="root-bound-1.checkpoint.1",
        event_type="context.checkpointed",
        observed_at=NOW + timedelta(seconds=1),
        payload={"checkpoint_artifact_hash": "a" * 64, "checkpoint_id": "checkpoint-1"},
    )
    with pytest.raises(ValueError, match="signature is invalid"):
        journals[1].events("root-bound-1")

    event_id = _TestPrivilegedEventSink(store=stores[0], journal=journals[0]).append(
        run_id="root-bound-0",
        event_id="root-bound-0.checkpoint.1",
        event_type="context.checkpointed",
        observed_at=NOW + timedelta(seconds=1),
        payload={"checkpoint_artifact_hash": "a" * 64, "checkpoint_id": "checkpoint-1"},
    )
    with sqlite3.connect(stores[0].index_path) as connection:
        connection.execute(
            "UPDATE events SET privileged_signature = ? WHERE event_id = ?",
            ("0" * 64, event_id),
        )
    with pytest.raises(ValueError, match="signature is invalid"):
        journals[0].events("root-bound-0")


def test_owned_journal_events_reject_zero_token_completion_metrics(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    _run(
        store,
        registration,
        registration.evaluation_cases[0].case_id,
        run_id="fabricated-zero-metrics",
        started_at=NOW,
        measurements=False,
        fabrication="zero_metrics",
    )


def test_positive_metrics_cannot_authorize_an_empty_reconstructed_transcript(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    registration = _registration()
    authority = StrategyValidationAuthorityStore(store)
    authority.register(registration)
    _run(
        store,
        registration,
        registration.evaluation_cases[0].case_id,
        run_id="fabricated-empty-transcript",
        started_at=NOW,
        measurements=False,
        fabrication="empty_transcript",
    )


def test_empty_prospective_window_and_unbound_registration_fail_closed(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    trigger_store = ProspectiveTriggerAdmissionStore(store)
    window_id = trigger_store.open_strategy_window(
        strategy_epoch_id="prospective-epoch-v2",
        qualification_policy_hash="a" * 64,
        opened_at=NOW - timedelta(days=2),
        cutoff_at=NOW - timedelta(days=1),
        registration_mapping=(
            StrategyAdmissionCaseMapping(
                registration_id="prospective-diagnostic-registration-" + "b" * 64,
                case_id="prospective-00",
                root_event_id="prospective-root-00",
                regime="regime-0",
            ),
        ),
    )
    with pytest.raises(ValueError, match="empty admission denominator"):
        trigger_store.seal_strategy_window(window_id, sealed_at=NOW)

    prospective_cases = tuple(
        StrategyCaseDefinition(
            case_id=f"prospective-{index:02d}",
            root_event_id=f"prospective-root-{index:02d}",
            regime=f"regime-{index % 4}",
            role=StrategyCaseRole.PROSPECTIVE_CONFIRMATION,
        )
        for index in range(30)
    )
    development = tuple(
        StrategyCaseDefinition(
            case_id=f"development-p-{index:02d}",
            root_event_id=f"development-p-root-{index:02d}",
            regime=f"development-regime-{index % 4}",
            role=StrategyCaseRole.DEVELOPMENT,
        )
        for index in range(8)
    )
    legacy_cohort = ProspectiveValidationCohort.build(
        strategy_epoch_id="prospective-epoch-v2",
        qualification_window_id="prospective-qualification-window-" + "c" * 64,
        qualification_policy_hash="d" * 64,
        qualification_window_open_at=NOW - timedelta(days=3),
        cohort_cutoff_at=NOW - timedelta(days=2),
        sealed_at=NOW - timedelta(days=1),
        append_only_journal_hash="e" * 64,
        qualification_digest_hash="f" * 64,
        eligible_cases=tuple(
            ProspectiveCohortCase(item.case_id, item.root_event_id) for item in prospective_cases
        ),
    )
    candidate_variant = _variant(StrategyBacktestArm.CANDIDATE)
    baseline_variant = _variant(
        StrategyBacktestArm.PRIMARY_BASELINE,
        baseline_id="cash",
        target_selection_ref="manual-integration-fixture:baseline.v1",
        strategy_ref="cash-no-action.v1",
    )
    registration = StrategyValidationRegistration.build(
        strategy_epoch_id="prospective-epoch-v2",
        program=StrategyValidationProgram.PROSPECTIVE_CONFIRMATION,
        model_profile_hash="a" * 64,
        prompt_hash="b" * 64,
        skill_catalog_hash="c" * 64,
        tool_manifest_hash="d" * 64,
        universe_hash="e" * 64,
        cost_model_hash="f" * 64,
        fill_model_hash="1" * 64,
        candidate_variant=candidate_variant,
        primary_baseline_id="cash",
        baseline_definitions=(
            StrategyBaselineDefinition(
                "cash",
                "2" * 64,
                baseline_variant.configuration_hash,
                baseline_variant,
            ),
        ),
        development_selection_evidence_hash="4" * 64,
        case_definitions=tuple(
            sorted((*development, *prospective_cases), key=lambda item: item.case_id)
        ),
        prospective_cohort=legacy_cohort,
        created_at=NOW,
    )
    with pytest.raises(ValueError, match="exactly one non-stale strategy window"):
        StrategyValidationAuthorityStore(store).register(registration)
