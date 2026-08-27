from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    canonical_hash,
    canonical_json_bytes,
    judgment_artifact_from_dict,
)
from market_impact_agent.agent_ensemble import (
    ReplicateOutcome,
    agent_ensemble_decision_from_dict,
    execution_binding_hash,
)
from market_impact_agent.backtests import (
    BacktestRequest,
    BacktestResult,
    BacktestRunStatus,
    backtest_request_from_dict,
    canonical_backtest_request_hash,
)
from market_impact_agent.method_development_runner import (
    DevelopmentArmBinding,
    DevelopmentState,
    MethodDevelopmentCase,
    load_method_development_case,
)
from market_impact_agent.runtime_store import ArtifactStore, RunStatus
from market_impact_agent.tushare_replay import (
    load_validated_tushare_modeled_open,
    run_validated_tushare_replay,
)

ReplayRunner = Callable[[BacktestRequest, Path], BacktestResult]
SnapshotPreflight = Callable[[Path], object]

_REPORT_FIELDS = {
    "schema_version",
    "experiment_id",
    "case_id",
    "case_hash",
    "state_id",
    "evidence_pack_id",
    "evidence_pack_hash",
    "provider_profile_id",
    "provider_profile_hash",
    "arms",
    "usage_ledger_hash",
    "outcomes_used_by_agent",
    "outcomes_known_to_builder",
    "identity_masked",
    "inference_eligible",
    "claim_scope",
    "broker_reachability",
    "execution_capability",
    "report_id",
}
_RUNNER_OUTPUT_FIELDS = {"report_artifact_hash", "state_directory"}
_ARM_FIELDS = {
    "arm",
    "route_id",
    "requested_skills",
    "allowed_capabilities",
    "allowed_tools",
    "execution_binding_run_id",
    "execution_binding",
    "execution_binding_hash",
    "execution_binding_identity_hash",
    "decision",
    "decision_artifact_hash",
    "run_statuses",
    "totals",
    "totals_binding_hash",
}
_EXECUTION_BINDING_FIELDS = {
    "runtime_ref",
    "runtime_config_hash",
    "prompt_hash",
    "skill_hashes",
    "tool_manifest_hashes",
    "tool_surface_hash",
    "mcp_server_hashes",
    "context_estimator_id",
    "compactor_id",
}
_TOTAL_FIELDS = {
    "turns",
    "tool_calls",
    "input_tokens",
    "output_tokens",
    "result_bytes",
    "latency_ms",
    "provider_attempts",
    "estimated_cost_microusd",
}
_USAGE_RECORD_FIELDS = {
    "schema_version",
    "experiment_id",
    "arm_id",
    "run_id",
    "recorded_at",
    "status",
    "provider_profile_id",
    "provider_profile_hash",
    "execution_binding_hash",
    "terminal_artifact_hash",
    "run_journal_hash",
    "metrics",
}


@dataclass(frozen=True, slots=True)
class _AuthenticatedMethodReport:
    report: dict[str, object]
    artifact_hash: str
    state_directory: Path


def evaluate_method_development_case(
    *,
    case_path: Path,
    attack_report_path: Path,
    recovery_report_path: Path,
    attack_backtest_request_path: Path,
    recovery_backtest_request_path: Path,
    attack_data_snapshot_path: Path,
    recovery_data_snapshot_path: Path,
    evaluation_id: str,
    state_root: Path,
    replay_runner: ReplayRunner = run_validated_tushare_replay,
    snapshot_preflight: SnapshotPreflight = load_validated_tushare_modeled_open,
) -> dict[str, object]:
    case = load_method_development_case(case_path)
    report_paths = {
        "attack": attack_report_path,
        "recovery": recovery_report_path,
    }
    request_paths = {
        "attack": attack_backtest_request_path,
        "recovery": recovery_backtest_request_path,
    }
    snapshot_paths = {
        "attack": attack_data_snapshot_path,
        "recovery": recovery_data_snapshot_path,
    }
    validated_inputs: list[tuple[DevelopmentState, dict[str, object], BacktestRequest, Path]] = []
    for state in case.states:
        authenticated = _normalize_method_report(report_paths[state.state_id])
        method_report = authenticated.report
        request_payload = _read_object(request_paths[state.state_id])
        request = backtest_request_from_dict(request_payload)
        _validate_state_inputs(case, state, method_report, request)
        _authenticate_private_state(case, state, authenticated)
        validated_inputs.append((state, method_report, request, snapshot_paths[state.state_id]))

    for _, _, _, snapshot_path in validated_inputs:
        snapshot_preflight(snapshot_path)

    state_rows: list[dict[str, object]] = []
    for state, method_report, request, snapshot_path in validated_inputs:
        first = replay_runner(request, snapshot_path)
        second = replay_runner(request, snapshot_path)
        _validate_repeated_results(first, second)
        state_rows.append(
            _state_row(
                state=state,
                method_report=method_report,
                first=first,
                second=second,
            )
        )
    update_rows = _evidence_update(tuple(state_rows))
    arm_costs = _arm_costs(tuple(state_rows))
    report_core = {
        "schema_version": "market-impact.method-development-evaluation.v1",
        "evaluation_id": evaluation_id,
        "case_id": case.case_id,
        "case_hash": case.case_hash,
        "independent_unit": case.independent_unit,
        "states": state_rows,
        "evidence_update": update_rows,
        "arm_provider_costs_microusd": arm_costs,
        "diagnosis": {
            "all_ensemble_actions_abstained": all(
                arm["ensemble_disposition"] == "abstain"
                for state in state_rows
                for arm in _object_array(state, "arms")
            ),
            "all_recovery_replicates_abstained": all(
                arm["proposal_replicates"] == 0 for arm in _object_array(state_rows[1], "arms")
            ),
            "fixed_long_net_negative_in_both_states": all(
                Decimal(_string(_object(state.get("fixed_long"), "fixed_long"), "net_return")) < 0
                for state in state_rows
            ),
            "method_ranking_supported": False,
            "alpha_claim_supported": False,
            "prospective_claim_supported": False,
        },
        "outcomes_opened_after_judgments": True,
        "inference_eligible": False,
        "claim_scope": "opened_development_diagnostic_only",
        "execution_capability": "none",
    }
    report = {
        **report_core,
        "evaluation_report_id": (f"method-development-evaluation-{canonical_hash(report_core)}"),
    }
    artifact_store = ArtifactStore(state_root / canonical_hash(evaluation_id) / "artifacts")
    stored = artifact_store.put_json(
        report,
        media_type="application/vnd.market-impact.method-development-evaluation+json",
    )
    return {
        **report,
        "evaluation_artifact_hash": stored.content_hash,
        "state_directory": (state_root / canonical_hash(evaluation_id)).as_posix(),
    }


def _validate_state_inputs(
    case: MethodDevelopmentCase,
    state: DevelopmentState,
    report: dict[str, object],
    request: BacktestRequest,
) -> None:
    report_core = {key: value for key, value in report.items() if key != "report_id"}
    if (
        _string(report, "schema_version") != "market-impact.method-development-report.v1"
        or _string(report, "report_id")
        != f"method-development-report-{canonical_hash(report_core)}"
        or _string(report, "case_id") != case.case_id
        or _string(report, "case_hash") != case.case_hash
        or _string(report, "state_id") != state.state_id
        or _string(report, "evidence_pack_id") != state.evidence_pack_id
        or _string(report, "evidence_pack_hash") != state.evidence_pack_hash
        or _string(report, "provider_profile_id") != case.provider_profile_id
        or _string(report, "provider_profile_hash") != case.provider_profile_hash
        or _boolean(report, "outcomes_used_by_agent")
        or _boolean(report, "inference_eligible")
        or not _boolean(report, "outcomes_known_to_builder")
        or not _boolean(report, "identity_masked")
        or _boolean(report, "broker_reachability")
        or _string(report, "claim_scope") != "opened_development_diagnostic_only"
        or _string(report, "execution_capability") != "none"
    ):
        raise ValueError("method development report does not match frozen state")
    if (
        request.request_id != state.backtest_request_id
        or canonical_backtest_request_hash(request) != state.backtest_request_hash
        or request.data_snapshot_id != state.data_snapshot_id
        or request.instrument_ids != (state.actual_target_id,)
        or request.horizons_sessions != case.eligible_horizons_sessions
    ):
        raise ValueError("development Backtest Request does not match frozen state")
    arms = _object_array(report, "arms")
    if tuple(_string(item, "arm") for item in arms) != (
        "neutral_evidence",
        "general_methods",
        "general_pattern",
        "family_guided",
    ):
        raise ValueError("method development report arms are incomplete or reordered")
    for arm in arms:
        _validate_arm(case, state, report, arm)


def _normalize_method_report(path: Path) -> _AuthenticatedMethodReport:
    if path.is_symlink() or not path.is_file():
        raise FileNotFoundError(f"method development report is not a regular file: {path}")
    payload = _read_object(path)
    runner_fields = set(payload) & _RUNNER_OUTPUT_FIELDS
    if runner_fields:
        if runner_fields != _RUNNER_OUTPUT_FIELDS or set(payload) != (
            _REPORT_FIELDS | _RUNNER_OUTPUT_FIELDS
        ):
            raise ValueError("method development runner output fields are invalid")
        report = {key: value for key, value in payload.items() if key in _REPORT_FIELDS}
        artifact_hash = _string(payload, "report_artifact_hash")
        if artifact_hash != canonical_hash(report):
            raise ValueError("method development report artifact hash does not match report")
        state_directory = _validated_state_directory(
            Path(_string(payload, "state_directory")), report
        )
        _validate_artifact_payload(
            state_directory / "artifacts",
            artifact_hash,
            report,
            "method development report",
        )
        return _AuthenticatedMethodReport(report, artifact_hash, state_directory)
    if set(payload) != _REPORT_FIELDS:
        raise ValueError("method development report fields are invalid")
    artifact_hash = canonical_hash(payload)
    resolved_path = path.resolve(strict=True)
    if resolved_path.parent.name != "artifacts" or resolved_path.name != artifact_hash:
        raise ValueError("stored method development report path is not content addressed")
    state_directory = _validated_state_directory(resolved_path.parent.parent, payload)
    _validate_artifact_payload(
        state_directory / "artifacts",
        artifact_hash,
        payload,
        "method development report",
    )
    return _AuthenticatedMethodReport(payload, artifact_hash, state_directory)


def _validated_state_directory(path: Path, report: dict[str, object]) -> Path:
    raw = path.as_posix()
    if not raw or raw != raw.strip() or path.is_symlink() or not path.is_dir():
        raise ValueError("method development state_directory is invalid")
    resolved = path.resolve(strict=True)
    if resolved.name != canonical_hash(_string(report, "experiment_id")):
        raise ValueError("method development state_directory identity is invalid")
    artifact_root = resolved / "artifacts"
    if artifact_root.is_symlink() or not artifact_root.is_dir():
        raise ValueError("method development artifact store is invalid")
    return resolved


def _authenticate_private_state(
    case: MethodDevelopmentCase,
    state: DevelopmentState,
    authenticated: _AuthenticatedMethodReport,
) -> None:
    report = authenticated.report
    state_directory = authenticated.state_directory
    artifact_root = state_directory / "artifacts"
    ledger_records, ledger_hash = _read_usage_ledger(state_directory / "usage.sqlite3")
    if ledger_hash != _string(report, "usage_ledger_hash"):
        raise ValueError("method development Usage Ledger hash does not match report")

    expected_run_ids: list[str] = []
    for arm in _object_array(report, "arms"):
        arm_name = _string(arm, "arm")
        binding_hash = _string(arm, "execution_binding_hash")
        execution_binding = _object(arm.get("execution_binding"), "execution_binding")
        _validate_artifact_payload(
            artifact_root,
            binding_hash,
            execution_binding,
            "method development execution binding",
        )
        decision = _object(arm.get("decision"), "decision")
        _validate_artifact_payload(
            artifact_root,
            _string(arm, "decision_artifact_hash"),
            decision,
            "method development decision",
        )
        assessments = _object_array(decision, "assessments")
        totals: dict[str, int | float] = {name: 0 for name in _TOTAL_FIELDS}
        for replicate_index, assessment in enumerate(assessments, start=1):
            run_id = _string(assessment, "run_id")
            expected_run_ids.append(run_id)
            try:
                usage = ledger_records[run_id]
            except KeyError as exc:
                raise ValueError(
                    f"method development Usage Ledger is missing run: {run_id}"
                ) from exc
            terminal_hash = _string(assessment, "terminal_artifact_hash")
            run_directory = state_directory / "runs" / arm_name / f"replicate-{replicate_index}"
            journal = _read_run_journal(run_directory / "run.sqlite3", run_id)
            if (
                _string(usage, "experiment_id") != _string(report, "experiment_id")
                or _string(usage, "arm_id") != arm_name
                or _string(usage, "run_id") != run_id
                or _string(usage, "status") != RunStatus.COMPLETED.value
                or _string(usage, "provider_profile_id") != case.provider_profile_id
                or _string(usage, "provider_profile_hash") != case.provider_profile_hash
                or _string(usage, "execution_binding_hash") != binding_hash
                or _string(usage, "terminal_artifact_hash") != terminal_hash
                or _string(usage, "run_journal_hash") != journal["journal_hash"]
                or _string(usage, "recorded_at") != journal["updated_at"]
                or journal["status"] != RunStatus.COMPLETED.value
                or journal["terminal_artifact_id"] != terminal_hash
            ):
                raise ValueError("method development persisted run evidence does not match report")
            judgment_payload = _read_artifact_payload(
                run_directory / "artifacts",
                terminal_hash,
                "method development judgment",
            )
            judgment = judgment_artifact_from_dict(judgment_payload)
            if (
                judgment.run_id != run_id
                or judgment.evidence_pack_id != state.evidence_pack_id
                or judgment.provider_id != case.provider_id
                or judgment.model != case.model
                or judgment.artifact_id != _string(assessment, "judgment_artifact_id")
                or judgment.journal_hash != journal["journal_hash"]
                or execution_binding_hash(judgment, runtime_ref=case.runtime_ref) != binding_hash
            ):
                raise ValueError(
                    "method development persisted Judgment Artifact does not match assessment"
                )
            metrics = _object(usage.get("metrics"), "usage metrics")
            if set(metrics) != _TOTAL_FIELDS:
                raise ValueError("method development Usage Ledger metrics are invalid")
            for name in _TOTAL_FIELDS:
                totals[name] += _number(metrics, name)
        if totals != _object(arm.get("totals"), "totals"):
            raise ValueError("method development report totals do not match Usage Ledger")

    if len(expected_run_ids) != len(set(expected_run_ids)) or set(ledger_records) != set(
        expected_run_ids
    ):
        raise ValueError("method development Usage Ledger run set does not match report")


def _validate_artifact_payload(
    artifact_root: Path,
    content_hash: str,
    expected: object,
    label: str,
) -> None:
    payload = _read_artifact_payload(artifact_root, content_hash, label)
    if payload != expected:
        raise ValueError(f"{label} content does not match report")


def _read_artifact_payload(
    artifact_root: Path,
    content_hash: str,
    label: str,
) -> object:
    _sha256_hex(content_hash, f"{label} hash")
    if artifact_root.is_symlink() or not artifact_root.is_dir():
        raise ValueError(f"{label} store is invalid")
    artifact_path = artifact_root / content_hash
    if artifact_path.is_symlink() or not artifact_path.is_file():
        raise FileNotFoundError(f"{label} artifact is missing: {content_hash}")
    payload_bytes = artifact_path.read_bytes()
    if sha256(payload_bytes).hexdigest() != content_hash:
        raise ValueError(f"{label} artifact content hash is invalid")
    payload = json.loads(payload_bytes)
    if canonical_json_bytes(payload) != payload_bytes:
        raise ValueError(f"{label} artifact is not canonical JSON")
    return payload


def _read_usage_ledger(path: Path) -> tuple[dict[str, dict[str, object]], str]:
    rows = _read_sqlite_rows(
        path,
        "SELECT sequence, run_id, payload_json, payload_hash, previous_hash, record_hash "
        "FROM usage_records ORDER BY sequence",
        "method development Usage Ledger",
    )
    records: dict[str, dict[str, object]] = {}
    record_hashes: list[str] = []
    previous_hash: str | None = None
    for row in rows:
        payload_json = cast(str, row["payload_json"])
        payload = _json_object(payload_json, "stored Usage Record")
        if (
            set(payload) != _USAGE_RECORD_FIELDS
            or payload.get("schema_version") != "market-impact.usage-record.v1"
            or payload_json != canonical_json_bytes(payload).decode()
        ):
            raise ValueError("stored Usage Record is not canonical JSON")
        payload_hash = canonical_hash(payload)
        stored_previous = cast(str | None, row["previous_hash"])
        record_hash = canonical_hash(
            {"payload_hash": payload_hash, "previous_hash": stored_previous}
        )
        run_id = _string(payload, "run_id")
        if (
            cast(str, row["run_id"]) != run_id
            or cast(str, row["payload_hash"]) != payload_hash
            or stored_previous != previous_hash
            or cast(str, row["record_hash"]) != record_hash
            or run_id in records
        ):
            raise ValueError("method development Usage Ledger hash chain is invalid")
        records[run_id] = payload
        record_hashes.append(record_hash)
        previous_hash = record_hash
    return records, canonical_hash(
        {
            "schema_version": "market-impact.usage-ledger.v1",
            "record_hashes": record_hashes,
        }
    )


def _read_run_journal(path: Path, run_id: str) -> dict[str, str | None]:
    run_rows = _read_sqlite_rows(
        path,
        "SELECT run_id, status, config_hash, updated_at, terminal_artifact_id FROM runs",
        "method development run journal",
    )
    if len(run_rows) != 1 or cast(str, run_rows[0]["run_id"]) != run_id:
        raise ValueError("method development run journal identity is invalid")
    run = run_rows[0]
    config_hash = cast(str, run["config_hash"])
    _sha256_hex(config_hash, "method development run config hash")
    event_rows = _read_sqlite_rows(
        path,
        "SELECT run_id, event_id, event_type, observed_at, payload_json, payload_hash, "
        "previous_hash, event_hash FROM events ORDER BY sequence",
        "method development run journal",
    )
    previous_hash: str | None = None
    for row in event_rows:
        payload_json = cast(str, row["payload_json"])
        payload = _json_object(payload_json, "run journal event")
        if payload_json != canonical_json_bytes(payload).decode():
            raise ValueError("run journal event payload is not canonical JSON")
        payload_hash = sha256(payload_json.encode()).hexdigest()
        stored_previous = cast(str | None, row["previous_hash"])
        event_hash = sha256(
            canonical_json_bytes(
                {
                    "run_id": cast(str, row["run_id"]),
                    "event_id": cast(str, row["event_id"]),
                    "event_type": cast(str, row["event_type"]),
                    "observed_at": cast(str, row["observed_at"]),
                    "payload_hash": payload_hash,
                    "previous_hash": stored_previous,
                }
            )
        ).hexdigest()
        if (
            cast(str, row["run_id"]) != run_id
            or cast(str, row["payload_hash"]) != payload_hash
            or stored_previous != previous_hash
            or cast(str, row["event_hash"]) != event_hash
        ):
            raise ValueError("method development run journal hash chain is invalid")
        previous_hash = event_hash
    return {
        "status": cast(str, run["status"]),
        "updated_at": cast(str, run["updated_at"]),
        "terminal_artifact_id": cast(str | None, run["terminal_artifact_id"]),
        "journal_hash": previous_hash or config_hash,
    }


def _read_sqlite_rows(
    path: Path,
    query: str,
    label: str,
) -> list[sqlite3.Row]:
    if path.is_symlink() or not path.is_file():
        raise FileNotFoundError(f"{label} is not a regular file: {path}")
    uri = f"{path.resolve(strict=True).as_uri()}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            return connection.execute(query).fetchall()
    except sqlite3.DatabaseError as exc:
        raise ValueError(f"{label} is invalid") from exc


def _json_object(payload_json: str, label: str) -> dict[str, object]:
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise TypeError(f"{label} must be a string-keyed object")
    raw = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{label} must be a string-keyed object")
    return cast(dict[str, object], raw)


def _sha256_hex(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _validate_arm(
    case: MethodDevelopmentCase,
    state: DevelopmentState,
    report: dict[str, object],
    arm: dict[str, object],
) -> None:
    if set(arm) != _ARM_FIELDS:
        raise ValueError("method development arm fields are invalid")
    totals = _object(arm.get("totals"), "totals")
    if set(totals) != _TOTAL_FIELDS:
        raise ValueError("method development totals fields are invalid")
    arm_name = _string(arm, "arm")
    frozen_arm = case.arm_binding(arm_name)
    experiment_id = _string(report, "experiment_id")
    if (
        _string(arm, "route_id") != frozen_arm.route_id
        or tuple(_string_array(arm, "requested_skills")) != frozen_arm.requested_skills
        or tuple(_string_array(arm, "allowed_capabilities")) != frozen_arm.allowed_capabilities
        or tuple(_string_array(arm, "allowed_tools")) != frozen_arm.allowed_tools
    ):
        raise ValueError("method development arm route does not match frozen treatment")
    run_statuses = _string_array(arm, "run_statuses")
    if run_statuses != [RunStatus.COMPLETED.value] * case.replicate_count:
        raise ValueError("method development arm requires five completed run statuses")
    decision = agent_ensemble_decision_from_dict(arm.get("decision"))
    expected_ensemble_run_id = f"{experiment_id}.{state.state_id}.{arm_name}"
    expected_run_ids = tuple(
        f"{expected_ensemble_run_id}.replicate-{index}"
        for index in range(1, case.replicate_count + 1)
    )
    execution_binding = _object(arm.get("execution_binding"), "execution_binding")
    _validate_execution_binding(
        case=case,
        arm=frozen_arm,
        payload=execution_binding,
    )
    binding_hash = _string(arm, "execution_binding_hash")
    preflight_run_id = f"{expected_ensemble_run_id}.binding-preflight"
    if (
        _string(arm, "decision_artifact_hash") != canonical_hash(decision.to_dict())
        or decision.ensemble_run_id != expected_ensemble_run_id
        or decision.registration_id != case.case_id
        or decision.registration_hash != case.case_hash
        or decision.evidence_pack_id != state.evidence_pack_id
        or decision.evidence_pack_hash != state.evidence_pack_hash
        or decision.provider_id != case.provider_id
        or decision.model != case.model
        or decision.runtime_ref != case.runtime_ref
        or decision.replicate_count != case.replicate_count
        or decision.minimum_agreement != case.minimum_agreement
        or _string(arm, "execution_binding_run_id") != preflight_run_id
        or canonical_hash(execution_binding) != binding_hash
        or _string(arm, "execution_binding_identity_hash")
        != canonical_hash(
            {
                "experiment_id": experiment_id,
                "state_id": state.state_id,
                "arm": arm_name,
                "preflight_run_id": preflight_run_id,
                "binding_hash": binding_hash,
            }
        )
        or decision.frozen_execution_binding_hash != binding_hash
    ):
        raise ValueError("method development decision does not match frozen state")
    if tuple(item.run_id for item in decision.assessments) != expected_run_ids or any(
        assessment.run_status is not RunStatus.COMPLETED
        or assessment.outcome is ReplicateOutcome.INVALID
        or assessment.judgment_artifact_id is None
        or assessment.execution_binding_hash != binding_hash
        for assessment in decision.assessments
    ):
        raise ValueError("method development decision contains an invalid replicate")
    if _string(arm, "totals_binding_hash") != canonical_hash(
        {
            "experiment_id": experiment_id,
            "state_id": state.state_id,
            "arm": arm_name,
            "replicate_run_ids": list(expected_run_ids),
            "totals": totals,
        }
    ):
        raise ValueError("method development totals do not match frozen arm runs")


def _validate_execution_binding(
    *,
    case: MethodDevelopmentCase,
    arm: DevelopmentArmBinding,
    payload: dict[str, object],
) -> None:
    if set(payload) != _EXECUTION_BINDING_FIELDS:
        raise ValueError("method development execution binding fields are invalid")
    if (
        _string(payload, "runtime_ref") != case.runtime_ref
        or tuple(_string_array(payload, "skill_hashes")) != arm.manifest_hashes
        or len(_string_array(payload, "tool_manifest_hashes")) != len(arm.allowed_tools)
        or _string_array(payload, "mcp_server_hashes")
    ):
        raise ValueError("method development execution binding does not match frozen arm")
    for name in (
        "runtime_config_hash",
        "prompt_hash",
        "tool_surface_hash",
    ):
        value = _string(payload, name)
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError(f"method development execution binding {name} is invalid")
    for value in _string_array(payload, "tool_manifest_hashes"):
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("method development tool manifest hash is invalid")
    for name in ("context_estimator_id", "compactor_id"):
        value = _string(payload, name)
        if not value or value != value.strip():
            raise ValueError(f"method development execution binding {name} is invalid")


def _validate_repeated_results(first: BacktestResult, second: BacktestResult) -> None:
    if first.status is not BacktestRunStatus.COMPLETED or second.status is not (
        BacktestRunStatus.COMPLETED
    ):
        raise ValueError("development outcome replay did not complete")
    if (
        first.result_hash != second.result_hash
        or first.metrics != second.metrics
        or first.manifest.request_hash != second.manifest.request_hash
        or first.manifest.engine_config_hash != second.manifest.engine_config_hash
    ):
        raise ValueError("development outcome replay is not deterministic")


def _state_row(
    *,
    state: DevelopmentState,
    method_report: dict[str, object],
    first: BacktestResult,
    second: BacktestResult,
) -> dict[str, object]:
    metrics = {item.name: item for item in first.metrics}
    gross = metrics.get("gross_return")
    net = metrics.get("net_return")
    if gross is None or net is None:
        raise ValueError("one-session development replay lacks return metrics")
    arms: list[dict[str, object]] = []
    for arm in _object_array(method_report, "arms"):
        decision = _object(arm.get("decision"), "decision")
        assessments = _object_array(decision, "assessments")
        proposal_count = sum(_string(item, "outcome") == "vote" for item in assessments)
        disposition = _string(decision, "disposition")
        selected_vote = decision.get("selected_vote")
        if disposition == "propose":
            vote = _object(selected_vote, "selected_vote")
            if (
                _string(vote, "target_id") != state.target_alias
                or _string(vote, "direction") != "up"
                or _integer(vote, "horizon_sessions") != 1
            ):
                raise ValueError("development ensemble selected an unscorable vote")
            policy_net_return = net.value
        elif disposition == "abstain" and selected_vote is None:
            policy_net_return = Decimal("0")
        else:
            raise ValueError("development ensemble disposition is inconsistent")
        totals = _object(arm.get("totals"), "totals")
        arms.append(
            {
                "arm": _string(arm, "arm"),
                "proposal_replicates": proposal_count,
                "abstention_replicates": len(assessments) - proposal_count,
                "ensemble_disposition": disposition,
                "selected_vote": selected_vote,
                "policy_net_return": str(policy_net_return),
                "provider_cost_microusd": _integer(totals, "estimated_cost_microusd"),
            }
        )
    return {
        "state_id": state.state_id,
        "actual_cutoff": state.actual_cutoff.isoformat().replace("+00:00", "Z"),
        "actual_target_id": state.actual_target_id,
        "method_report_id": _string(method_report, "report_id"),
        "method_report_hash": canonical_hash(method_report),
        "usage_ledger_hash": _string(method_report, "usage_ledger_hash"),
        "repeated_backtest_result_hash": first.result_hash,
        "backtest_request_hash": first.manifest.request_hash,
        "engine_config_hash": first.manifest.engine_config_hash,
        "replay_run_ids": [first.manifest.run_id, second.manifest.run_id],
        "replay_deterministic": True,
        "fixed_long": {
            "gross_return": str(gross.value),
            "net_return": str(net.value),
        },
        "arms": arms,
        "licensed_market_metrics_private": True,
    }


def _evidence_update(states: tuple[dict[str, object], ...]) -> list[dict[str, object]]:
    if tuple(_string(item, "state_id") for item in states) != ("attack", "recovery"):
        raise ValueError("development evaluation requires attack then recovery state")
    attack = {
        _string(item, "arm"): _integer(item, "proposal_replicates")
        for item in _object_array(states[0], "arms")
    }
    recovery = {
        _string(item, "arm"): _integer(item, "proposal_replicates")
        for item in _object_array(states[1], "arms")
    }
    if set(attack) != set(recovery):
        raise ValueError("development state arms do not match")
    return [
        {
            "arm": arm,
            "attack_proposal_replicates": attack[arm],
            "recovery_proposal_replicates": recovery[arm],
            "proposal_replicate_change": recovery[arm] - attack[arm],
        }
        for arm in attack
    ]


def _arm_costs(states: tuple[dict[str, object], ...]) -> list[dict[str, object]]:
    totals: dict[str, int] = {}
    for state in states:
        for arm in _object_array(state, "arms"):
            name = _string(arm, "arm")
            totals[name] = totals.get(name, 0) + _integer(arm, "provider_cost_microusd")
    return [{"arm": name, "total": total} for name, total in totals.items()]


def _read_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"expected JSON object: {path}")
    raw = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"expected string-keyed JSON object: {path}")
    return cast(dict[str, object], raw)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], raw)


def _object_array(payload: dict[str, object], name: str) -> list[dict[str, object]]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return [_object(item, name) for item in cast(list[object], value)]


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _number(payload: dict[str, object], name: str) -> int | float:
    value = payload.get(name)
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _string_array(payload: dict[str, object], name: str) -> list[str]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a string array")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{name} must be a string array")
    return cast(list[str], value)
