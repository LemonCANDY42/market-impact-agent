from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import (
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    canonical_hash,
)
from market_impact_agent.agent_engine import RunMetrics
from market_impact_agent.agent_ensemble import (
    AgentEnsembleDecision,
    EnsembleDisposition,
    EnsembleReason,
    ReplicateAssessment,
    ReplicateOutcome,
)
from market_impact_agent.backtests import (
    BacktestRequest,
    BacktestResult,
    BacktestRunStatus,
    canonical_backtest_request_hash,
)
from market_impact_agent.method_development_evaluation import (
    evaluate_method_development_case,
)
from market_impact_agent.method_development_runner import (
    DevelopmentState,
    MethodDevelopmentCase,
    load_method_development_case,
)
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

_MATERIALIZE = "__materialize_private_state__"


@dataclass(frozen=True)
class _Metric:
    name: str
    value: Decimal


@dataclass(frozen=True)
class _Manifest:
    request_hash: str
    engine_config_hash: str
    run_id: str


@dataclass(frozen=True)
class _Result:
    status: BacktestRunStatus
    result_hash: str
    metrics: tuple[_Metric, ...]
    manifest: _Manifest


def _decision(
    case: MethodDevelopmentCase,
    state: DevelopmentState,
    arm_index: int,
    experiment_id: str,
    binding_hash: str,
) -> dict[str, object]:
    arm_name = case.arm_bindings[arm_index].arm.value
    ensemble_run_id = f"{experiment_id}.{state.state_id}.{arm_name}"
    assessments = tuple(
        ReplicateAssessment(
            replicate_index=index,
            run_id=f"{ensemble_run_id}.replicate-{index}",
            run_status=RunStatus.COMPLETED,
            outcome=ReplicateOutcome.ABSTAIN,
            reason="agent_abstained",
            terminal_artifact_hash=f"{arm_index * 10 + index + 100:064x}",
            judgment_artifact_id=f"judgment-{arm_index * 10 + index + 200:064x}",
            execution_binding_hash=binding_hash,
            vote=None,
        )
        for index in range(1, 6)
    )
    core = {
        "schema_version": "market-impact.agent-ensemble-decision.v1",
        "ensemble_run_id": ensemble_run_id,
        "registration_id": case.case_id,
        "registration_hash": case.case_hash,
        "evidence_pack_id": state.evidence_pack_id,
        "evidence_pack_hash": state.evidence_pack_hash,
        "provider_id": "minimax-openai-compatible",
        "model": "MiniMax-M3",
        "runtime_ref": case.runtime_ref,
        "frozen_execution_binding_hash": binding_hash,
        "replicate_count": 5,
        "minimum_agreement": 3,
        "assessments": [item.to_dict() for item in assessments],
        "disposition": "abstain",
        "reason": "no_three_of_five_agreement",
        "selected_vote": None,
        "agreement_count": 0,
        "agreeing_judgment_artifact_ids": [],
        "execution_capability": "none",
    }
    return AgentEnsembleDecision(
        decision_id=f"agent-ensemble-{canonical_hash(core)}",
        ensemble_run_id=cast(str, core["ensemble_run_id"]),
        registration_id=case.case_id,
        registration_hash=case.case_hash,
        evidence_pack_id=state.evidence_pack_id,
        evidence_pack_hash=state.evidence_pack_hash,
        provider_id="minimax-openai-compatible",
        model="MiniMax-M3",
        runtime_ref=case.runtime_ref,
        frozen_execution_binding_hash=binding_hash,
        replicate_count=5,
        minimum_agreement=3,
        assessments=assessments,
        disposition=EnsembleDisposition.ABSTAIN,
        reason=EnsembleReason.NO_THREE_OF_FIVE_AGREEMENT,
        selected_vote=None,
        agreement_count=0,
        agreeing_judgment_artifact_ids=(),
    ).to_dict()


def _report(case: MethodDevelopmentCase, state: DevelopmentState) -> dict[str, object]:
    experiment_id = f"development-{state.state_id}"
    arms: list[dict[str, object]] = []
    for arm_index, frozen_arm in enumerate(case.arm_bindings):
        name = frozen_arm.arm.value
        execution_binding: dict[str, object] = {
            "runtime_ref": case.runtime_ref,
            "runtime_config_hash": f"{100 + arm_index:064x}",
            "prompt_hash": f"{200 + arm_index:064x}",
            "skill_hashes": list(frozen_arm.manifest_hashes),
            "tool_manifest_hashes": [
                f"{300 + arm_index * 10 + index:064x}"
                for index, _ in enumerate(frozen_arm.allowed_tools)
            ],
            "tool_surface_hash": f"{400 + arm_index:064x}",
            "mcp_server_hashes": [],
            "context_estimator_id": "utf8-byte-v1",
            "compactor_id": "deterministic-context-v1",
        }
        binding_hash = canonical_hash(execution_binding)
        decision = _decision(case, state, arm_index, experiment_id, binding_hash)
        totals = {
            "turns": 5,
            "tool_calls": 5,
            "input_tokens": 500,
            "output_tokens": 100,
            "result_bytes": 500,
            "latency_ms": 50,
            "provider_attempts": 5,
            "estimated_cost_microusd": 50 + arm_index,
        }
        replicate_run_ids = [
            f"{experiment_id}.{state.state_id}.{name}.replicate-{index}"
            for index in range(1, case.replicate_count + 1)
        ]
        arms.append(
            {
                "arm": name,
                "route_id": frozen_arm.route_id,
                "requested_skills": list(frozen_arm.requested_skills),
                "allowed_capabilities": list(frozen_arm.allowed_capabilities),
                "allowed_tools": list(frozen_arm.allowed_tools),
                "execution_binding_run_id": (
                    f"{experiment_id}.{state.state_id}.{name}.binding-preflight"
                ),
                "execution_binding": execution_binding,
                "execution_binding_hash": binding_hash,
                "execution_binding_identity_hash": canonical_hash(
                    {
                        "experiment_id": experiment_id,
                        "state_id": state.state_id,
                        "arm": name,
                        "preflight_run_id": (
                            f"{experiment_id}.{state.state_id}.{name}.binding-preflight"
                        ),
                        "binding_hash": binding_hash,
                    }
                ),
                "decision": decision,
                "decision_artifact_hash": canonical_hash(decision),
                "run_statuses": ["completed"] * 5,
                "totals": totals,
                "totals_binding_hash": canonical_hash(
                    {
                        "experiment_id": experiment_id,
                        "state_id": state.state_id,
                        "arm": name,
                        "replicate_run_ids": replicate_run_ids,
                        "totals": totals,
                    }
                ),
            }
        )
    core: dict[str, object] = {
        "schema_version": "market-impact.method-development-report.v1",
        "experiment_id": experiment_id,
        "case_id": case.case_id,
        "case_hash": case.case_hash,
        "state_id": state.state_id,
        "evidence_pack_id": state.evidence_pack_id,
        "evidence_pack_hash": state.evidence_pack_hash,
        "provider_profile_id": case.provider_profile_id,
        "provider_profile_hash": case.provider_profile_hash,
        "arms": arms,
        "usage_ledger_hash": f"{500 + len(state.state_id):064x}",
        "outcomes_used_by_agent": False,
        "outcomes_known_to_builder": True,
        "identity_masked": True,
        "inference_eligible": False,
        "claim_scope": "opened_development_diagnostic_only",
        "broker_reachability": False,
        "execution_capability": "none",
    }
    return {**core, "report_id": f"method-development-report-{canonical_hash(core)}"}


def _runner_output(report: dict[str, object]) -> dict[str, object]:
    return {
        **report,
        "report_artifact_hash": _MATERIALIZE,
        "state_directory": _MATERIALIZE,
    }


def _rehash_report(report: dict[str, object]) -> None:
    core = {key: value for key, value in report.items() if key != "report_id"}
    report["report_id"] = f"method-development-report-{canonical_hash(core)}"


def _rehash_arm(report: dict[str, object], arm_index: int = 0) -> None:
    arm = cast(list[dict[str, object]], report["arms"])[arm_index]
    decision = cast(dict[str, object], arm["decision"])
    decision_core = {key: value for key, value in decision.items() if key != "decision_id"}
    decision["decision_id"] = f"agent-ensemble-{canonical_hash(decision_core)}"
    arm["decision_artifact_hash"] = canonical_hash(decision)
    _rehash_report(report)


def _write_reports(
    tmp_path: Path,
    case: MethodDevelopmentCase,
    *,
    attack: dict[str, object] | None = None,
    recovery: dict[str, object] | None = None,
) -> dict[str, Path]:
    payloads = {
        "attack": attack or _runner_output(_report(case, case.state("attack"))),
        "recovery": recovery or _runner_output(_report(case, case.state("recovery"))),
    }
    paths: dict[str, Path] = {}
    for state_id, payload in payloads.items():
        source = deepcopy(payload)
        report = {
            key: value
            for key, value in source.items()
            if key
            not in {
                "report_artifact_hash",
                "state_directory",
                "unexpected",
            }
        }
        materialized = _materialize_private_state(tmp_path, report)
        output = {**materialized}
        if source.get("report_artifact_hash") not in (None, _MATERIALIZE):
            output["report_artifact_hash"] = source["report_artifact_hash"]
        if source.get("state_directory") not in (None, _MATERIALIZE):
            output["state_directory"] = source["state_directory"]
        for key in set(source) - set(output):
            output[key] = source[key]
        path = tmp_path / f"{state_id}-report.json"
        path.write_text(json.dumps(output), encoding="utf-8")
        paths[state_id] = path
    return paths


def _materialize_private_state(
    tmp_path: Path,
    report: dict[str, object],
) -> dict[str, object]:
    experiment_id = cast(str, report["experiment_id"])
    state_directory = tmp_path / "private-state" / canonical_hash(experiment_id)
    artifact_store = ArtifactStore(state_directory / "artifacts")
    usage_ledger = UsageLedger(state_directory / "usage.sqlite3")
    for arm_index, arm in enumerate(cast(list[dict[str, object]], report["arms"])):
        arm_name = cast(str, arm["arm"])
        execution_binding = cast(dict[str, object], arm["execution_binding"])
        binding_hash = cast(str, arm["execution_binding_hash"])
        assert artifact_store.put_json(execution_binding).content_hash == binding_hash
        decision = cast(dict[str, object], arm["decision"])
        assessments = cast(list[dict[str, object]], decision["assessments"])
        for replicate_index, assessment in enumerate(assessments, start=1):
            run_id = f"{experiment_id}.{report['state_id']}.{arm_name}.replicate-{replicate_index}"
            run_directory = state_directory / "runs" / arm_name / f"replicate-{replicate_index}"
            journal = RunJournal(run_directory / "run.sqlite3")
            started_at = datetime(2026, 8, 27, 4, replicate_index, tzinfo=UTC)
            journal.start_run(
                run_id=run_id,
                config_hash=cast(str, execution_binding["runtime_config_hash"]),
                created_at=started_at,
            )
            judgment = JudgmentArtifact.build(
                run_id=run_id,
                evidence_pack_id=cast(str, report["evidence_pack_id"]),
                provider_id="minimax-openai-compatible",
                model="MiniMax-M3",
                runtime_config_hash=cast(str, execution_binding["runtime_config_hash"]),
                prompt_hash=cast(str, execution_binding["prompt_hash"]),
                skill_hashes=tuple(cast(list[str], execution_binding["skill_hashes"])),
                tool_manifest_hashes=tuple(
                    cast(list[str], execution_binding["tool_manifest_hashes"])
                ),
                tool_surface_hash=cast(str, execution_binding["tool_surface_hash"]),
                mcp_server_hashes=(),
                context_estimator_id=cast(str, execution_binding["context_estimator_id"]),
                compactor_id=cast(str, execution_binding["compactor_id"]),
                journal_hash=journal.journal_hash(run_id),
                transcript_hash=f"{1000 + arm_index * 10 + replicate_index:064x}",
                raw_response_hash=f"{2000 + arm_index * 10 + replicate_index:064x}",
                started_at=started_at,
                finished_at=started_at + timedelta(seconds=1),
                proposal=JudgmentProposal(
                    event_id=f"opened-{report['state_id']}",
                    decision=JudgmentDecision.ABSTAIN,
                    summary="Synthetic persisted abstention.",
                    transmission_steps=(),
                    candidates=(),
                    blockers=("insufficient evidence",),
                    unresolved_questions=(),
                    stopped_reason="synthetic fixture completed",
                ),
            )
            stored = ArtifactStore(run_directory / "artifacts").put_json(judgment.to_dict())
            journal.finish(
                run_id=run_id,
                status=RunStatus.COMPLETED,
                finished_at=started_at + timedelta(seconds=1),
                terminal_artifact_id=stored.content_hash,
            )
            assessment["terminal_artifact_hash"] = stored.content_hash
            assessment["judgment_artifact_id"] = judgment.artifact_id
            per_run_cost = (
                cast(dict[str, int], arm["totals"])["estimated_cost_microusd"] - 4
                if replicate_index == 1
                else 1
            )
            metrics = RunMetrics(
                turns=1,
                tool_calls=1,
                input_tokens=100,
                output_tokens=20,
                result_bytes=100,
                latency_ms=10,
                provider_attempts=1,
                estimated_cost_microusd=per_run_cost,
            )
            usage_ledger.append(
                UsageRecord(
                    experiment_id=experiment_id,
                    arm_id=arm_name,
                    run_id=run_id,
                    recorded_at=journal.get_run(run_id).updated_at,
                    status=RunStatus.COMPLETED,
                    provider_profile_id=cast(str, report["provider_profile_id"]),
                    provider_profile_hash=cast(str, report["provider_profile_hash"]),
                    execution_binding_hash=binding_hash,
                    terminal_artifact_hash=stored.content_hash,
                    run_journal_hash=journal.journal_hash(run_id),
                    metrics=metrics,
                )
            )
        decision_core = {key: value for key, value in decision.items() if key != "decision_id"}
        decision["decision_id"] = f"agent-ensemble-{canonical_hash(decision_core)}"
        stored_decision = artifact_store.put_json(decision)
        arm["decision_artifact_hash"] = stored_decision.content_hash
    report["usage_ledger_hash"] = usage_ledger.ledger_hash
    _rehash_report(report)
    stored_report = artifact_store.put_json(report)
    return {
        **report,
        "report_artifact_hash": stored_report.content_hash,
        "state_directory": state_directory.as_posix(),
    }


def _persist_forged_report(
    path: Path,
    mutate: Callable[[dict[str, object]], None],
) -> None:
    output = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
    state_directory = Path(cast(str, output["state_directory"]))
    report = {
        key: value
        for key, value in output.items()
        if key not in {"report_artifact_hash", "state_directory"}
    }
    mutate(report)
    artifact_store = ArtifactStore(state_directory / "artifacts")
    for arm in cast(list[dict[str, object]], report["arms"]):
        decision = cast(dict[str, object], arm["decision"])
        decision_core = {key: value for key, value in decision.items() if key != "decision_id"}
        decision["decision_id"] = f"agent-ensemble-{canonical_hash(decision_core)}"
        arm["decision_artifact_hash"] = artifact_store.put_json(decision).content_hash
    _rehash_report(report)
    report_hash = artifact_store.put_json(report).content_hash
    path.write_text(
        json.dumps(
            {
                **report,
                "report_artifact_hash": report_hash,
                "state_directory": state_directory.as_posix(),
            }
        ),
        encoding="utf-8",
    )


def _evaluate(
    tmp_path: Path,
    report_paths: dict[str, Path],
    replay_runner: Callable[[BacktestRequest, Path], BacktestResult],
    *,
    recovery_request_path: Path | None = None,
    snapshot_preflight: Callable[[Path], object] = lambda path: path,
) -> dict[str, object]:
    return evaluate_method_development_case(
        case_path=Path("examples/calibration/method-development-abqaiq-v1.json"),
        attack_report_path=report_paths["attack"],
        recovery_report_path=report_paths["recovery"],
        attack_backtest_request_path=Path(
            "examples/backtests/real-abqaiq-601857-attack-state-request-v1.json"
        ),
        recovery_backtest_request_path=recovery_request_path
        or Path("examples/backtests/real-abqaiq-601857-recovery-state-request-v1.json"),
        attack_data_snapshot_path=tmp_path / "attack-snapshot",
        recovery_data_snapshot_path=tmp_path / "recovery-snapshot",
        evaluation_id="synthetic-opened-evaluation",
        state_root=tmp_path / "evaluation",
        replay_runner=replay_runner,
        snapshot_preflight=snapshot_preflight,
    )


def test_evaluation_opens_repeated_outcomes_after_bound_judgments(tmp_path: Path) -> None:
    case_path = Path("examples/calibration/method-development-abqaiq-v1.json")
    case = load_method_development_case(case_path)
    report_paths = _write_reports(tmp_path, case)
    run_counts: dict[str, int] = {"attack": 0, "recovery": 0}

    def fake_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        state_id = "attack" if "attack" in path.name else "recovery"
        run_counts[state_id] += 1
        result = _Result(
            status=BacktestRunStatus.COMPLETED,
            result_hash=("a" * 64 if state_id == "attack" else "b" * 64),
            metrics=(
                _Metric("gross_return", Decimal("0.01")),
                _Metric("net_return", Decimal("-0.01")),
            ),
            manifest=_Manifest(
                request_hash=canonical_backtest_request_hash(request),
                engine_config_hash=("c" * 64 if state_id == "attack" else "d" * 64),
                run_id=f"{state_id}-{run_counts[state_id]}",
            ),
        )
        return cast(BacktestResult, result)

    result = _evaluate(tmp_path, report_paths, fake_replay)

    assert run_counts == {"attack": 2, "recovery": 2}
    diagnosis = cast(dict[str, object], result["diagnosis"])
    assert diagnosis["all_ensemble_actions_abstained"] is True
    assert diagnosis["fixed_long_net_negative_in_both_states"] is True
    assert diagnosis["method_ranking_supported"] is False
    assert result["inference_eligible"] is False
    states = cast(list[dict[str, object]], result["states"])
    assert states[0]["repeated_backtest_result_hash"] == "a" * 64
    assert states[1]["repeated_backtest_result_hash"] == "b" * 64


def test_evaluation_accepts_stored_report_artifact_path(tmp_path: Path) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report_paths = _write_reports(tmp_path, case)
    attack_output = cast(
        dict[str, object], json.loads(report_paths["attack"].read_text(encoding="utf-8"))
    )
    report_paths["attack"] = (
        Path(cast(str, attack_output["state_directory"]))
        / "artifacts"
        / cast(str, attack_output["report_artifact_hash"])
    )
    calls = 0

    def fake_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        nonlocal calls
        calls += 1
        state_id = "attack" if "attack" in path.name else "recovery"
        return cast(
            BacktestResult,
            _Result(
                status=BacktestRunStatus.COMPLETED,
                result_hash="a" * 64 if state_id == "attack" else "b" * 64,
                metrics=(
                    _Metric("gross_return", Decimal("0.01")),
                    _Metric("net_return", Decimal("-0.01")),
                ),
                manifest=_Manifest(
                    request_hash=canonical_backtest_request_hash(request),
                    engine_config_hash="c" * 64,
                    run_id=f"{state_id}-{calls}",
                ),
            ),
        )

    result = _evaluate(tmp_path, report_paths, fake_replay)

    assert calls == 4
    assert result["outcomes_opened_after_judgments"] is True


@pytest.mark.parametrize(
    "tamper",
    ["judgment_artifact_id", "run_completion", "usage_ledger_hash", "costs"],
)
def test_evaluation_rejects_self_rehashed_claims_not_backed_by_private_state(
    tmp_path: Path,
    tamper: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report_paths = _write_reports(tmp_path, case)
    output = cast(dict[str, object], json.loads(report_paths["attack"].read_text(encoding="utf-8")))
    if tamper == "judgment_artifact_id":

        def mutate(report: dict[str, object]) -> None:
            arm = cast(list[dict[str, object]], report["arms"])[0]
            decision = cast(dict[str, object], arm["decision"])
            assessment = cast(list[dict[str, object]], decision["assessments"])[0]
            assessment["judgment_artifact_id"] = f"judgment-{'f' * 64}"

        _persist_forged_report(report_paths["attack"], mutate)
    elif tamper == "run_completion":
        state_directory = Path(cast(str, output["state_directory"]))
        run_id = "development-attack.attack.neutral_evidence.replicate-1"
        journal_path = state_directory / "runs" / "neutral_evidence" / "replicate-1" / "run.sqlite3"
        with sqlite3.connect(journal_path) as connection:
            connection.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?",
                (RunStatus.FAILED.value, run_id),
            )
    elif tamper == "usage_ledger_hash":
        _persist_forged_report(
            report_paths["attack"],
            lambda report: report.__setitem__("usage_ledger_hash", "f" * 64),
        )
    else:

        def mutate(report: dict[str, object]) -> None:
            arm = cast(list[dict[str, object]], report["arms"])[0]
            totals = cast(dict[str, int], arm["totals"])
            totals["estimated_cost_microusd"] += 1000
            arm["totals_binding_hash"] = canonical_hash(
                {
                    "experiment_id": report["experiment_id"],
                    "state_id": report["state_id"],
                    "arm": arm["arm"],
                    "replicate_run_ids": [
                        f"{report['experiment_id']}.{report['state_id']}.{arm['arm']}."
                        f"replicate-{index}"
                        for index in range(1, 6)
                    ],
                    "totals": totals,
                }
            )

        _persist_forged_report(report_paths["attack"], mutate)
    replay_calls = 0

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        nonlocal replay_calls
        _ = (request, path)
        replay_calls += 1
        raise AssertionError("outcomes must not open for fabricated private evidence")

    with pytest.raises((FileNotFoundError, ValueError), match="method development"):
        _evaluate(tmp_path, report_paths, forbidden_replay)

    assert replay_calls == 0


@pytest.mark.parametrize("failure", ["missing", "corrupt"])
def test_evaluation_preflights_both_snapshots_before_any_replay(
    tmp_path: Path,
    failure: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report_paths = _write_reports(tmp_path, case)
    attack_snapshot = tmp_path / "attack-snapshot"
    recovery_snapshot = tmp_path / "recovery-snapshot"
    attack_snapshot.mkdir()
    (attack_snapshot / "validated").write_text("ok", encoding="utf-8")
    if failure == "corrupt":
        recovery_snapshot.mkdir()
        (recovery_snapshot / "validated").write_text("corrupt", encoding="utf-8")
    preflight_calls: list[Path] = []
    replay_calls = 0

    def preflight(path: Path) -> object:
        preflight_calls.append(path)
        marker = (path / "validated").read_text(encoding="utf-8")
        if marker != "ok":
            raise ValueError("corrupt recovery snapshot")
        return path

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        nonlocal replay_calls
        _ = (request, path)
        replay_calls += 1
        raise AssertionError("no replay may run before both snapshots pass preflight")

    with pytest.raises((FileNotFoundError, ValueError)):
        _evaluate(
            tmp_path,
            report_paths,
            forbidden_replay,
            snapshot_preflight=preflight,
        )

    assert preflight_calls == [attack_snapshot, recovery_snapshot]
    assert replay_calls == 0


@pytest.mark.parametrize("failure", ["missing", "incomplete", "tampered"])
def test_evaluation_preflights_recovery_report_before_any_outcome_opening(
    tmp_path: Path,
    failure: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report_paths = _write_reports(tmp_path, case)
    if failure == "missing":
        report_paths["recovery"].unlink()
    elif failure == "incomplete":
        recovery = _report(case, case.state("recovery"))
        cast(list[dict[str, object]], recovery["arms"]).pop()
        _rehash_report(recovery)
        report_paths = _write_reports(
            tmp_path,
            case,
            recovery=_runner_output(recovery),
        )
    else:
        recovery_output = _runner_output(_report(case, case.state("recovery")))
        recovery_output["report_artifact_hash"] = "0" * 64
        report_paths = _write_reports(tmp_path, case, recovery=recovery_output)
    replay_calls = 0

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        nonlocal replay_calls
        _ = (request, path)
        replay_calls += 1
        raise AssertionError("no replay may run before both states pass preflight")

    with pytest.raises((FileNotFoundError, ValueError)):
        _evaluate(tmp_path, report_paths, forbidden_replay)

    assert replay_calls == 0


def test_evaluation_preflights_recovery_request_before_any_outcome_opening(
    tmp_path: Path,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report_paths = _write_reports(tmp_path, case)
    request = json.loads(
        Path("examples/backtests/real-abqaiq-601857-recovery-state-request-v1.json").read_text(
            encoding="utf-8"
        )
    )
    request["data_snapshot_id"] = "tampered-recovery-snapshot"
    tampered_path = tmp_path / "tampered-recovery-request.json"
    tampered_path.write_text(json.dumps(request), encoding="utf-8")
    replay_calls = 0

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        nonlocal replay_calls
        _ = (request, path)
        replay_calls += 1
        raise AssertionError("no replay may run before both requests pass preflight")

    with pytest.raises(ValueError, match="Backtest Request"):
        _evaluate(
            tmp_path,
            report_paths,
            forbidden_replay,
            recovery_request_path=tampered_path,
        )

    assert replay_calls == 0


@pytest.mark.parametrize("run_status", ["failed", "budget_exhausted"])
def test_evaluation_rejects_incomplete_run_status_before_outcome_opening(
    tmp_path: Path,
    run_status: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report = _report(case, case.state("attack"))
    cast(list[dict[str, object]], report["arms"])[0]["run_statuses"] = [
        run_status,
        "completed",
        "completed",
        "completed",
        "completed",
    ]
    _rehash_report(report)
    paths = _write_reports(tmp_path, case, attack=_runner_output(report))

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        _ = (request, path)
        raise AssertionError("outcomes must not open for an incomplete Agent state")

    with pytest.raises(ValueError, match="requires five completed run statuses"):
        _evaluate(tmp_path, paths, forbidden_replay)


@pytest.mark.parametrize(
    "tamper",
    [
        "registration_hash",
        "evidence_pack_id",
        "provider_id",
        "model",
        "runtime_ref",
        "execution_binding",
        "assessment_status",
    ],
)
def test_evaluation_rejects_nested_cross_binding_tamper_before_outcome_opening(
    tmp_path: Path,
    tamper: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report = deepcopy(_report(case, case.state("attack")))
    arm = cast(list[dict[str, object]], report["arms"])[0]
    decision = cast(dict[str, object], arm["decision"])
    if tamper == "registration_hash":
        decision["registration_hash"] = "f" * 64
    elif tamper == "evidence_pack_id":
        decision["evidence_pack_id"] = "evidence-pack-other"
    elif tamper == "provider_id":
        decision["provider_id"] = "other-provider"
    elif tamper == "model":
        decision["model"] = "OtherModel"
    elif tamper == "runtime_ref":
        decision["runtime_ref"] = "other-runtime"
    elif tamper == "execution_binding":
        decision["frozen_execution_binding_hash"] = "e" * 64
        for assessment in cast(list[dict[str, object]], decision["assessments"]):
            assessment["execution_binding_hash"] = "e" * 64
    elif tamper == "assessment_status":
        assessment = cast(list[dict[str, object]], decision["assessments"])[0]
        assessment.update(
            {
                "run_status": "failed",
                "outcome": "invalid",
                "reason": "run_failed",
                "terminal_artifact_hash": None,
                "judgment_artifact_id": None,
                "execution_binding_hash": None,
            }
        )
    _rehash_arm(report)
    paths = _write_reports(tmp_path, case, attack=_runner_output(report))

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        _ = (request, path)
        raise AssertionError("outcomes must not open for cross-bound Agent decisions")

    with pytest.raises(ValueError, match="method development"):
        _evaluate(tmp_path, paths, forbidden_replay)


@pytest.mark.parametrize(
    "tamper",
    [
        "route_id",
        "requested_skills",
        "ensemble_run_id",
        "assessment_run_id",
        "assessment_order",
        "cross_arm_decision",
        "cross_arm_binding",
        "cross_state_binding",
        "cross_arm_totals",
    ],
)
def test_evaluation_rejects_arm_identity_tamper_before_outcome_opening(
    tmp_path: Path,
    tamper: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    report = deepcopy(_report(case, case.state("attack")))
    arms = cast(list[dict[str, object]], report["arms"])
    arm = arms[0]
    decision = cast(dict[str, object], arm["decision"])
    if tamper == "route_id":
        arm["route_id"] = arms[1]["route_id"]
    elif tamper == "requested_skills":
        arm["requested_skills"] = arms[1]["requested_skills"]
    elif tamper == "ensemble_run_id":
        decision["ensemble_run_id"] = "other-experiment.attack.neutral_evidence"
        _rehash_arm(report)
    elif tamper == "assessment_run_id":
        cast(list[dict[str, object]], decision["assessments"])[0]["run_id"] = (
            "other-experiment.attack.neutral_evidence.replicate-1"
        )
        _rehash_arm(report)
    elif tamper == "assessment_order":
        assessments = cast(list[dict[str, object]], decision["assessments"])
        assessments[0], assessments[1] = assessments[1], assessments[0]
        _rehash_arm(report)
    elif tamper == "cross_arm_decision":
        arm["decision"] = arms[1]["decision"]
        arm["decision_artifact_hash"] = arms[1]["decision_artifact_hash"]
    elif tamper == "cross_arm_binding":
        arm["execution_binding_run_id"] = arms[1]["execution_binding_run_id"]
        arm["execution_binding"] = arms[1]["execution_binding"]
        arm["execution_binding_hash"] = arms[1]["execution_binding_hash"]
        decision["frozen_execution_binding_hash"] = arms[1]["execution_binding_hash"]
        for assessment in cast(list[dict[str, object]], decision["assessments"]):
            assessment["execution_binding_hash"] = arms[1]["execution_binding_hash"]
        _rehash_arm(report)
    elif tamper == "cross_state_binding":
        recovery_arm = cast(
            list[dict[str, object]],
            _report(case, case.state("recovery"))["arms"],
        )[0]
        arm["execution_binding_run_id"] = recovery_arm["execution_binding_run_id"]
        arm["execution_binding"] = recovery_arm["execution_binding"]
        arm["execution_binding_hash"] = recovery_arm["execution_binding_hash"]
        arm["execution_binding_identity_hash"] = recovery_arm["execution_binding_identity_hash"]
        decision["frozen_execution_binding_hash"] = recovery_arm["execution_binding_hash"]
        for assessment in cast(list[dict[str, object]], decision["assessments"]):
            assessment["execution_binding_hash"] = recovery_arm["execution_binding_hash"]
        _rehash_arm(report)
    else:
        arm["totals"] = arms[1]["totals"]
        arm["totals_binding_hash"] = arms[1]["totals_binding_hash"]
    _rehash_report(report)
    paths = _write_reports(tmp_path, case, attack=_runner_output(report))
    replay_calls = 0

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        nonlocal replay_calls
        _ = (request, path)
        replay_calls += 1
        raise AssertionError("outcomes must not open for arm identity tamper")

    with pytest.raises(ValueError, match=r"method development|replicate assessments"):
        _evaluate(tmp_path, paths, forbidden_replay)

    assert replay_calls == 0


@pytest.mark.parametrize("tamper", ["artifact_hash", "extra_key"])
def test_evaluation_rejects_tampered_runner_output(
    tmp_path: Path,
    tamper: str,
) -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )
    output = _runner_output(_report(case, case.state("attack")))
    if tamper == "artifact_hash":
        output["report_artifact_hash"] = "0" * 64
    else:
        output["unexpected"] = True
    paths = _write_reports(tmp_path, case, attack=output)

    def forbidden_replay(request: BacktestRequest, path: Path) -> BacktestResult:
        _ = (request, path)
        raise AssertionError("outcomes must not open for tampered runner output")

    with pytest.raises(ValueError, match="method development"):
        _evaluate(tmp_path, paths, forbidden_replay)
