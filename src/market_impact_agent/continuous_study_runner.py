"""Preparation-only authority for the frozen continuous coverage study.

This module records the study and the evidence it still needs.  It deliberately
does not open a Provider, retrieve a source, reserve a model request, or expose
an execution path.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import ProviderUsage
from market_impact_agent.continuous_study import (
    CONTINUOUS_STUDY_COVERAGE_MATRIX_SCHEMA,
    build_continuous_study_coverage_matrix,
    build_continuous_study_registration,
    coverage_report,
    load_pinned_regime_panels,
    load_prior_usage_audit_binding,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.market_regimes import RegimePanel, load_market_regime_dataset
from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.pi_runtime import shared_admission_root
from market_impact_agent.runtime_store import RunJournal

_STUDY_SCOPE_ID = "continuous-study-20260905-usd40"
_REGISTRATION_PATH = "registration.json"
_COVERAGE_PATH = "coverage.json"
_COVERAGE_MATRIX_PATH = "coverage-matrix.json"
_DAILY_INPUT_INVENTORY_PATH = "daily-input-inventory.json"
_PREPARATION_PATH = "preparation.json"
_DAILY_INPUTS_DIRECTORY = "daily-inputs"
_COST_ESTIMATES_DIRECTORY = "cost-estimates"
_CADENCE_ARMS = ("expiry_only", "scheduled", "event")
_BUDGET_STAGES = (
    "route_qualification",
    "analysis_coverage",
    "portfolio_coverage",
    "rolling",
    "unseen_and_prospective",
    "recovery",
)
_PRIOR_REQUESTS = 98
_PRIOR_KNOWN_MICROUSD = 5_356_905
_PRIOR_RESERVED_MICROUSD = 11_769
_PRIOR_UNSETTLED_REQUESTS = 1
_MAX_COST_MICROUSD = 40_000_000
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_FROZEN_PROFILE_PATHS = {
    "model-provider-dddfa35322a03a3bfe92f1186a1ec04fc77075d9f6038de16ade304f85f8add0": (
        _PROJECT_ROOT / "examples/providers/pi-cpa-luna-max-v2.json"
    ),
    "model-provider-7d3c04afa0b04a1a6466da7918d585c7e7df630857016b99504b35a973d34034": (
        _PROJECT_ROOT / "examples/providers/pi-cpa-terra-high-v2.json"
    ),
    "model-provider-c963b814206d3fcbd9e596ac5486a06c5300c1c7d9157dcc6f816ec1b056d129": (
        _PROJECT_ROOT / "examples/providers/pi-cpa-sol-high-v2.json"
    ),
}


def prepare_continuous_study(
    root: Path,
    *,
    dataset_path: Path,
    panel_root: Path,
    prior_usage_audit_path: Path,
) -> dict[str, object]:
    """Freeze registration and pending input requirements without model activity."""

    root = _secure_root(root)
    preparation_path = root / _PREPARATION_PATH
    if preparation_path.exists():
        try:
            prepared = _load_prepared(root)
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            # The completion marker can survive a crash or interrupted file restore.
            # Rebuild from the same offline pinned inputs, then only write missing
            # artifacts or verify byte-equivalent immutable artifacts.
            prepared = None
        if prepared is not None:
            budget = study_budget(root, "rolling")
            return _prepare_result(prepared, budget, replayed=True)

    panels = load_pinned_regime_panels(panel_root)
    audit = load_prior_usage_audit_binding(prior_usage_audit_path)
    registration = build_continuous_study_registration(
        load_market_regime_dataset(dataset_path), panels, prior_usage_audit=audit
    )
    registration_value = registration.to_dict()
    coverage_value: dict[str, object] = {
        "schema_version": "market-impact.continuous-study-coverage.v1",
        "registration_id": registration.registration_id,
        "coverage": coverage_report(registration),
        "coverage_windows": [item.to_dict() for item in registration.coverage_windows],
        "coverage_complete": False,
        "completion_status": "registered_pending_daily_inputs_and_requalification",
        "labels_access": "evaluation_only",
        "labels_are_model_inputs": False,
        "model_or_network_invocation": False,
        "broker_access": False,
    }
    inventory_value = _daily_input_inventory(registration_value, panels.selection_panel.panel)
    preparation_value: dict[str, object] = {
        "schema_version": "market-impact.continuous-study-preparation.v1",
        "study_scope_id": _STUDY_SCOPE_ID,
        "registration_id": registration.registration_id,
        "registration_content_hash": canonical_hash(registration_value),
        "coverage_content_hash": canonical_hash(coverage_value),
        "daily_input_inventory_content_hash": canonical_hash(inventory_value),
        "provider_dispatch_permitted": False,
        "broker_access": False,
        "coverage_complete": False,
    }
    _write_or_verify_json(root / _REGISTRATION_PATH, registration_value)
    _write_or_verify_json(root / _COVERAGE_PATH, coverage_value)
    _write_or_verify_json(root / _DAILY_INPUT_INVENTORY_PATH, inventory_value)
    _write_or_verify_json(preparation_path, preparation_value)
    prepared = _load_prepared(root)
    budget = study_budget(root, "rolling")
    return _prepare_result(prepared, budget, replayed=False)


def study_budget(root: Path, stage: str) -> ModelBudget:
    """Return one stage view over the one machine/project study budget journal."""

    if stage not in _BUDGET_STAGES:
        raise ValueError(f"unknown continuous study budget stage: {stage}")
    prepared = _load_prepared(root)
    pointer = _admission_pointer(prepared)
    scope_root = _secure_root(shared_admission_root())
    pointer_path = scope_root / f"{_STUDY_SCOPE_ID}.json"
    _write_or_verify_pointer(pointer_path, pointer)
    stored_pointer = _read_object(pointer_path)
    if stored_pointer != pointer:
        raise ValueError("shared continuous study authorization differs from this registration")
    store = LocalDataSnapshotStore(scope_root / _STUDY_SCOPE_ID)
    journal = RunJournal.authoritative(store)
    owner_run_id = f"{_STUDY_SCOPE_ID}.{_registration_digest(prepared['registration'])}"
    journal.start_run(
        run_id=owner_run_id,
        config_hash=canonical_hash(pointer),
        created_at=datetime.now(UTC),
    )
    return ModelBudget(
        journal=journal,
        owner_run_id=owner_run_id,
        # This is deliberately not a batch ceiling.  A one-micro-USD physical
        # request is the smallest paid request ModelBudget can admit, so this
        # guard cannot reduce the authorized USD 40 cost scope.
        max_requests=_PRIOR_REQUESTS + _MAX_COST_MICROUSD,
        max_cost_microusd=_MAX_COST_MICROUSD,
        prior_requests=_PRIOR_REQUESTS,
        prior_cost_microusd=_PRIOR_KNOWN_MICROUSD,
        prior_reserved_microusd=_PRIOR_RESERVED_MICROUSD,
        prior_unsettled_requests=_PRIOR_UNSETTLED_REQUESTS,
        scope_limits=_budget_scopes(),
        scope=stage,
    )


def preflight_continuous_study(root: Path) -> dict[str, object]:
    """Inspect local source-proof and cost-estimate manifests without contacting a Provider."""

    prepared = _load_prepared(root)
    registration = prepared["registration"]
    inventory = prepared["inventory"]
    rolling_budget = study_budget(root, "rolling")
    authority_store = LocalDataSnapshotStore(rolling_budget.journal.path.parent)
    if (
        authority_store.index_path != rolling_budget.journal.path
        or authority_store.harness_authority_id != rolling_budget.journal.harness_authority_id
    ):
        raise ValueError("continuous study preflight must reopen the budget authority store")
    window_status: dict[str, dict[str, object]] = {}
    total_available = 0
    total_missing = 0
    total_unverified = 0
    for requirement in _list_of_objects(inventory, "daily_input_requirements"):
        window_id = _string(requirement, "window_id")
        trade_dates = _list_of_strings(requirement, "trade_dates")
        state = {"available": 0, "missing": 0, "unverified": 0}
        for trade_date in trade_dates:
            path = root / _DAILY_INPUTS_DIRECTORY / window_id / f"{trade_date}.json"
            manifest_state = _daily_manifest_state(
                path,
                registration_id=_string(registration, "registration_id"),
                window_id=window_id,
                trade_date=trade_date,
                authority_store=authority_store,
            )
            state[manifest_state] += 1
        total_available += state["available"]
        total_missing += state["missing"]
        total_unverified += state["unverified"]
        window_status[window_id] = {
            **state,
            "required": len(trade_dates),
            "current_status": _requirement_status(state),
        }

    cost_status = {"available": 0, "missing": 0, "unverified": 0}
    estimated_by_stage: dict[str, int] = {stage: 0 for stage in _BUDGET_STAGES}
    for observation in _list_of_objects(inventory, "planned_observations"):
        state, estimate = _cost_estimate_state(
            root / _COST_ESTIMATES_DIRECTORY / f"{_string(observation, 'observation_id')}.json",
            registration_id=_string(registration, "registration_id"),
            observation=observation,
            authority_store=authority_store,
        )
        cost_status[state] += 1
        if estimate is not None:
            estimated_by_stage[_string(observation, "budget_stage")] += estimate

    budget_remaining: dict[str, int] = {}
    budget_checks: dict[str, str] = {}
    for stage in _BUDGET_STAGES:
        budget = rolling_budget if stage == "rolling" else study_budget(root, stage)
        limit = next(item for item in budget.scope_limits if item.name == stage)
        used = budget.scope_summary()
        remaining = (
            limit.max_cost_microusd - used["known_cost_microusd"] - used["reserved_microusd"]
        )
        budget_remaining[stage] = remaining
        budget_checks[stage] = (
            "within_registered_scope"
            if estimated_by_stage[stage] <= remaining
            else "estimated_cost_exceeds_registered_scope"
        )

    evidence_ready = (
        total_missing == 0
        and total_unverified == 0
        and cost_status["missing"] == 0
        and cost_status["unverified"] == 0
        and all(value == "within_registered_scope" for value in budget_checks.values())
    )
    return {
        "schema_version": "market-impact.continuous-study-preflight.v1",
        "registration_id": _string(registration, "registration_id"),
        "daily_inputs": {
            "available": total_available,
            "missing": total_missing,
            "unverified": total_unverified,
            "windows": window_status,
        },
        "cost_estimates": {
            **cost_status,
            "estimated_microusd_by_stage": estimated_by_stage,
            "remaining_microusd_by_stage": budget_remaining,
            "scope_checks": budget_checks,
        },
        "evidence_ready_for_root_requalification": evidence_ready,
        "root_requalification_status": "required_before_any_provider_dispatch",
        "stage_passed": evidence_ready,
        "coverage_complete": False,
        "provider_dispatch_permitted": False,
        "model_or_network_invocation": False,
        "broker_access": False,
    }


def report_continuous_study(root: Path) -> dict[str, object]:
    """Report every original window and its current local evidence status."""

    prepared = _load_prepared(root)
    coverage = prepared["coverage"]
    inventory = prepared["inventory"]
    preflight = preflight_continuous_study(root)
    daily_status = _object(preflight["daily_inputs"])
    window_status = _object(daily_status["windows"])
    baseline = _object(_object(coverage["coverage"])["baseline_input_inventory"])
    windows: list[dict[str, object]] = []
    for original_window in _list_of_objects(coverage, "coverage_windows"):
        window_id = _string(original_window, "window_id")
        current = _object(window_status[window_id])
        missing_current: list[dict[str, object]] = []
        if _integer(current, "missing"):
            missing_current.append(
                {
                    "gap_type": "missing_daily_input_manifest",
                    "required": _integer(current, "missing"),
                }
            )
        if _integer(current, "unverified"):
            missing_current.append(
                {
                    "gap_type": "unverified_daily_source_proof",
                    "required": _integer(current, "unverified"),
                }
            )
        windows.append(
            {
                "original_window": original_window,
                "full_baseline": baseline,
                "missing_current": missing_current,
                "current_status": _string(current, "current_status"),
                "execution_eligibility": "not_evaluated_no_execution_authority",
                "labels_access": "evaluation_only",
                "labels_are_model_inputs": False,
            }
        )
    return {
        "schema_version": "market-impact.continuous-study-report.v1",
        "registration_id": _string(prepared["registration"], "registration_id"),
        "coverage_denominator": len(windows),
        "windows": windows,
        "planned_observation_denominator": _planned_observation_count(inventory),
        "pending_observation_denominator": sum(
            item.get("status") == "pending"
            for item in _list_of_objects(inventory, "planned_observations")
        ),
        "cost_estimate_status": preflight["cost_estimates"],
        "coverage_complete": False,
        "completion_claim": (
            "A schema-valid registration records planned coverage only; it does not prove all "
            "eighteen windows have complete daily source evidence."
        ),
        "provider_dispatch_permitted": False,
        "broker_access": False,
    }


def freeze_continuous_study_coverage_matrix(
    root: Path,
    *,
    dataset_path: Path,
    panel_root: Path,
    prior_usage_audit_path: Path,
) -> dict[str, object]:
    """Write the separately immutable evaluator-only coverage-matrix artifact.

    Rebuilding the registration from its original pinned inputs before writing
    prevents this optional report from becoming a second registration authority.
    The existing registration, coverage, and preparation files are read only.
    """

    root = root.resolve()
    prepared = _load_prepared(root)
    dataset = load_market_regime_dataset(dataset_path)
    registration = build_continuous_study_registration(
        dataset,
        load_pinned_regime_panels(panel_root),
        prior_usage_audit=load_prior_usage_audit_binding(prior_usage_audit_path),
    )
    if registration.to_dict() != prepared["registration"]:
        raise ValueError("coverage matrix inputs do not reproduce the frozen registration")
    matrix = build_continuous_study_coverage_matrix(registration, dataset)
    path = root / _COVERAGE_MATRIX_PATH
    replayed = path.exists()
    _write_or_verify_json(path, matrix)
    return {
        "status": "replayed_immutable_coverage_matrix" if replayed else "frozen_coverage_matrix",
        "coverage_matrix_id": _string(matrix, "coverage_matrix_id"),
        "coverage_denominator": _integer(matrix, "coverage_denominator"),
        "deep_selection_denominator": len(_list_of_objects(matrix, "deep_selection")),
        "dimension_gap_count": len(_list_of_objects(matrix, "dimension_gaps")),
        "artifact_path": str(path),
        "evaluator_only": True,
        "labels_are_model_inputs": False,
        "model_or_network_invocation": False,
        "broker_access": False,
    }


def report_continuous_study_coverage_matrix(root: Path) -> dict[str, object]:
    """Load and integrity-check the separately frozen coverage-matrix artifact."""

    prepared = _load_prepared(root)
    matrix = _read_object(root.resolve() / _COVERAGE_MATRIX_PATH)
    matrix_id = _string(matrix, "coverage_matrix_id")
    core = {key: value for key, value in matrix.items() if key != "coverage_matrix_id"}
    if (
        matrix_id != f"continuous-study-coverage-matrix-{canonical_hash(core)}"
        or matrix.get("schema_version") != CONTINUOUS_STUDY_COVERAGE_MATRIX_SCHEMA
        or matrix.get("registration_id") != prepared["registration"].get("registration_id")
        or matrix.get("registration_content_hash") != canonical_hash(prepared["registration"])
        or _integer(matrix, "coverage_denominator") != 18
        or len(_list_of_objects(matrix, "rows")) != 18
        or len(_list_of_objects(matrix, "deep_selection")) != 8
        or matrix.get("evaluator_only") is not True
        or matrix.get("labels_access") != "evaluation_only"
        or matrix.get("labels_are_model_inputs") is not False
        or matrix.get("model_or_network_invocation") is not False
        or matrix.get("broker_access") is not False
    ):
        raise ValueError("continuous study coverage matrix changed or lost evaluator-only bounds")
    return matrix


def _daily_input_inventory(
    registration: dict[str, object], panel: RegimePanel
) -> dict[str, object]:
    primary_series = next(
        (item for item in panel.series if item.series_id == "000300.SH"),
        None,
    )
    if primary_series is None:
        raise ValueError("continuous study selection panel has no CSI 300 series")
    panel_dates = tuple(_trade_date(row) for row in primary_series.rows)
    daily_requirements: list[dict[str, object]] = []
    coverage_windows = _list_of_objects(registration, "coverage_windows")
    for window in coverage_windows:
        decision_session = date.fromisoformat(_string(window, "decision_session"))
        outcome_window_end = date.fromisoformat(_string(window, "outcome_window_end"))
        required_dates = [
            session.isoformat()
            for session in panel_dates
            if decision_session <= session <= outcome_window_end
        ]
        if not required_dates:
            raise ValueError("registered coverage window has no panel sessions")
        daily_requirements.append(
            {
                "window_id": _string(window, "window_id"),
                "trade_dates": required_dates,
                "daily_input_manifest_path": (
                    f"{_DAILY_INPUTS_DIRECTORY}/{_string(window, 'window_id')}/{{trade_date}}.json"
                ),
                "source_proof_requirement": (
                    "exact source_snapshot_ids, rule_artifact_hashes, symbol, modeled_policy, "
                    "preopen cutoff, and source_record_hashes replayed through "
                    "HistoricalAShareInputs"
                ),
                "public_label_access": "evaluation_only",
                "labels_are_model_inputs": False,
                "current_status": "pending",
            }
        )
    observations: list[dict[str, object]] = []
    profiles = _list_of_objects(registration, "model_profiles")
    for cell in _list_of_objects(registration, "deep_cells"):
        for profile in profiles:
            for cadence in _CADENCE_ARMS:
                observation_id = canonical_hash(
                    {
                        "registration_id": _string(registration, "registration_id"),
                        "deep_cell_id": _string(cell, "cell_id"),
                        "profile": profile,
                        "cadence": cadence,
                    }
                )
                observations.append(
                    {
                        "observation_id": observation_id,
                        "deep_cell_id": _string(cell, "cell_id"),
                        "coverage_window_id": _string(cell, "coverage_window_id"),
                        "cadence": cadence,
                        "budget_stage": "rolling",
                        "profile": profile,
                        "cost_estimate_manifest_path": (
                            f"{_COST_ESTIMATES_DIRECTORY}/{observation_id}.json"
                        ),
                        "cost_estimate_requirement": (
                            "authority input_artifact_hash with byte-bound input tokens, frozen "
                            "output reserve, and bounded physical_review_count"
                        ),
                        "status": "pending",
                        "public_label_access": "evaluation_only",
                        "labels_are_model_inputs": False,
                        "provider_dispatch_permitted": False,
                    }
                )
    if len(observations) != 72:
        raise ValueError("continuous study must retain all 72 pending observations")
    return {
        "schema_version": "market-impact.continuous-study-daily-input-inventory.v1",
        "registration_id": _string(registration, "registration_id"),
        "daily_input_requirements": daily_requirements,
        "planned_observations": observations,
        "planned_observation_denominator": len(observations),
        "pending_observation_denominator": len(observations),
        "labels_access": "evaluation_only",
        "labels_are_model_inputs": False,
        "model_or_network_invocation": False,
        "broker_access": False,
    }


def _daily_manifest_state(
    path: Path,
    *,
    registration_id: str,
    window_id: str,
    trade_date: str,
    authority_store: LocalDataSnapshotStore,
) -> str:
    if not path.is_file() or path.is_symlink():
        return "missing"
    try:
        value = _read_object(path)
        snapshot_ids = tuple(_list_of_strings(value, "source_snapshot_ids"))
        rule_artifact_hashes = tuple(_list_of_strings(value, "rule_artifact_hashes"))
        policy_value = _object(value["modeled_policy"])
        expected_cutoff = _preopen_cutoff(trade_date)
        source = HistoricalAShareInputs(
            store=authority_store,
            snapshot_ids=snapshot_ids,
            rule_artifact_hashes=rule_artifact_hashes,
            policy=ModeledHistoricalPolicy(
                policy_id=_string(policy_value, "policy_id"),
                daily_open_volume_fraction=Decimal(
                    _string(policy_value, "daily_open_volume_fraction")
                ),
                lane=_string(policy_value, "lane"),
                opening_tick_validity_microseconds=_integer(
                    policy_value, "opening_tick_validity_microseconds"
                ),
            ),
        )
        evidence = source.reopen_security(_string(value, "symbol"), expected_cutoff)
        source_record_hashes = tuple(sorted(_list_of_strings(value, "source_record_hashes")))
        if (
            value.get("schema_version") != "market-impact.continuous-study-daily-input-manifest.v1"
            or value.get("registration_id") != registration_id
            or value.get("window_id") != window_id
            or value.get("trade_date") != trade_date
            or value.get("preopen_cutoff") != expected_cutoff.isoformat()
            or evidence is None
            or evidence.gaps
            or evidence.cutoff != expected_cutoff
            or evidence.source_record_hashes != source_record_hashes
            or not snapshot_ids
            or not rule_artifact_hashes
        ):
            return "unverified"
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return "unverified"
    return "available"


def _cost_estimate_state(
    path: Path,
    *,
    registration_id: str,
    observation: dict[str, object],
    authority_store: LocalDataSnapshotStore,
) -> tuple[str, int | None]:
    if not path.is_file() or path.is_symlink():
        return "missing", None
    try:
        value = _read_object(path)
        profile = _object(observation["profile"])
        estimate = _integer(value, "estimated_cost_microusd")
        frozen_profile = _frozen_profile(profile)
        input_artifact = authority_store.artifacts.get(
            _string(value, "input_artifact_hash"), media_type="application/json"
        )
        input_tokens = _integer(value, "bound_input_tokens")
        output_reserve_tokens = _integer(value, "output_reserve_tokens")
        review_count = _integer(value, "physical_review_count")
        recomputed = (
            frozen_profile.pricing.estimate_microusd(
                ProviderUsage(
                    input_tokens=input_artifact.size_bytes, output_tokens=output_reserve_tokens
                )
            )
            * review_count
        )
        if (
            value.get("schema_version") != "market-impact.continuous-study-cost-estimate.v1"
            or value.get("registration_id") != registration_id
            or value.get("observation_id") != observation["observation_id"]
            or value.get("budget_stage") != observation["budget_stage"]
            or value.get("provider_profile_id") != profile["provider_profile_id"]
            or value.get("provider_profile_hash") != profile["provider_profile_hash"]
            or value.get("pricing_id") != profile["pricing_id"]
            or input_tokens != input_artifact.size_bytes
            or output_reserve_tokens != frozen_profile.reserved_output_tokens
            or input_tokens > frozen_profile.context_window_tokens - output_reserve_tokens
            or not 1 <= review_count <= frozen_profile.max_attempts
            or estimate != recomputed
        ):
            return "unverified", None
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return "unverified", None
    return "available", estimate


def _frozen_profile(binding: dict[str, object]):
    profile_id = _string(binding, "provider_profile_id")
    path = _FROZEN_PROFILE_PATHS.get(profile_id)
    if path is None:
        raise ValueError("continuous study observation has no frozen provider profile")
    profile = load_model_provider_profile(path)
    if (
        profile.profile_id != profile_id
        or profile.profile_hash != binding.get("provider_profile_hash")
        or profile.pricing.pricing_id != binding.get("pricing_id")
    ):
        raise ValueError("frozen provider profile differs from the registration binding")
    return profile


def _preopen_cutoff(trade_date: str) -> datetime:
    session = date.fromisoformat(trade_date)
    return datetime.combine(session, time(9), tzinfo=_SHANGHAI)


def _load_prepared(root: Path) -> dict[str, dict[str, object]]:
    root = root.resolve()
    preparation = _read_object(root / _PREPARATION_PATH)
    registration = _read_object(root / _REGISTRATION_PATH)
    coverage = _read_object(root / _COVERAGE_PATH)
    inventory = _read_object(root / _DAILY_INPUT_INVENTORY_PATH)
    registration_id = _string(registration, "registration_id")
    if (
        preparation.get("schema_version") != "market-impact.continuous-study-preparation.v1"
        or preparation.get("study_scope_id") != _STUDY_SCOPE_ID
        or preparation.get("registration_id") != registration_id
        or preparation.get("registration_content_hash") != canonical_hash(registration)
        or preparation.get("coverage_content_hash") != canonical_hash(coverage)
        or preparation.get("daily_input_inventory_content_hash") != canonical_hash(inventory)
        or preparation.get("provider_dispatch_permitted") is not False
        or preparation.get("broker_access") is not False
    ):
        raise ValueError("continuous study preparation changed after registration")
    registration_core = {
        key: value for key, value in registration.items() if key != "registration_id"
    }
    if registration_id != f"continuous-study-registration-{canonical_hash(registration_core)}":
        raise ValueError("continuous study registration identity changed")
    if (
        coverage.get("registration_id") != registration_id
        or coverage.get("coverage_complete") is not False
        or coverage.get("labels_access") != "evaluation_only"
        or coverage.get("labels_are_model_inputs") is not False
        or inventory.get("registration_id") != registration_id
        or inventory.get("labels_access") != "evaluation_only"
        or inventory.get("labels_are_model_inputs") is not False
        or inventory.get("model_or_network_invocation") is not False
        or inventory.get("broker_access") is not False
        or _planned_observation_count(inventory) != 72
    ):
        raise ValueError("continuous study prepared files do not retain the frozen boundaries")
    for observation in _list_of_objects(inventory, "planned_observations"):
        if (
            observation.get("status") != "pending"
            or observation.get("public_label_access") != "evaluation_only"
            or observation.get("labels_are_model_inputs") is not False
            or observation.get("provider_dispatch_permitted") is not False
        ):
            raise ValueError(
                "continuous study observation changed from pending evaluation-only state"
            )
    return {
        "preparation": preparation,
        "registration": registration,
        "coverage": coverage,
        "inventory": inventory,
    }


def _admission_pointer(prepared: dict[str, dict[str, object]]) -> dict[str, object]:
    registration = prepared["registration"]
    return {
        "schema_version": "market-impact.continuous-study-budget-admission.v1",
        "study_scope_id": _STUDY_SCOPE_ID,
        "registration_id": _string(registration, "registration_id"),
        "registration_content_hash": canonical_hash(registration),
        "maximum_future_physical_requests": _MAX_COST_MICROUSD,
        "physical_request_guard": (
            "non-shrinking operational guard: one paid micro-USD request per authorized "
            "micro-USD; it is not a continuous-study batch ceiling"
        ),
        "maximum_cost_microusd": _MAX_COST_MICROUSD,
        "prior_requests": _PRIOR_REQUESTS,
        "prior_known_cost_microusd": _PRIOR_KNOWN_MICROUSD,
        "prior_reserved_microusd": _PRIOR_RESERVED_MICROUSD,
        "prior_unsettled_requests": _PRIOR_UNSETTLED_REQUESTS,
        "scope_limits": [item.to_dict() for item in _budget_scopes()],
    }


def _budget_scopes() -> tuple[ModelBudgetScope, ...]:
    return (
        ModelBudgetScope("route_qualification", 1_000_000, 85_194),
        ModelBudgetScope("analysis_coverage", 9_000_000, 4_870_788, 11_769),
        ModelBudgetScope("portfolio_coverage", 2_500_000, 400_923),
        ModelBudgetScope("rolling", 22_000_000),
        ModelBudgetScope("unseen_and_prospective", 2_500_000),
        ModelBudgetScope("recovery", 3_000_000),
    )


def _prepare_result(
    prepared: dict[str, dict[str, object]], budget: ModelBudget, *, replayed: bool
) -> dict[str, object]:
    inventory = prepared["inventory"]
    return {
        "status": "replayed_immutable_preparation" if replayed else "prepared",
        "registration_id": _string(prepared["registration"], "registration_id"),
        "coverage_denominator": len(_list_of_objects(prepared["coverage"], "coverage_windows")),
        "planned_observation_denominator": _planned_observation_count(inventory),
        "pending_observation_denominator": _integer(inventory, "pending_observation_denominator"),
        "budget_parent_summary": budget.summary(),
        "coverage_complete": False,
        "model_calls": 0,
        "provider_dispatch_permitted": False,
        "broker_access": False,
    }


def _planned_observation_count(inventory: dict[str, object]) -> int:
    observations = _list_of_objects(inventory, "planned_observations")
    if _integer(inventory, "planned_observation_denominator") != len(observations):
        raise ValueError("planned observation denominator does not match the inventory")
    return len(observations)


def _registration_digest(registration: dict[str, object]) -> str:
    registration_id = _string(registration, "registration_id")
    return registration_id.removeprefix("continuous-study-registration-")


def _requirement_status(state: dict[str, int]) -> str:
    if state["unverified"]:
        return "unverified_source_proof"
    if state["missing"]:
        return "missing_daily_inputs"
    return "daily_inputs_preflighted_pending_root_requalification"


def _write_or_verify_pointer(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    payload = canonical_json_bytes(value)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            existing = _read_object(path)
            if existing != value:
                raise ValueError("shared continuous study authorization is already bound") from None
        else:
            os.chmod(path, 0o600)
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_new_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            descriptor = None
            output.write(canonical_json_bytes(value))
            output.flush()
            os.fsync(output.fileno())
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _write_or_verify_json(path: Path, value: dict[str, object]) -> None:
    if path.exists():
        if _read_object(path) != value:
            raise ValueError(f"immutable continuous study artifact differs: {path}")
        return
    _write_new_json(path, value)


def _secure_root(path: Path) -> Path:
    path = path.expanduser().resolve()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _read_object(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise FileNotFoundError(path)
    value: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"expected a JSON object: {path}")
    return cast(dict[str, object], value)


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected JSON object")
    return cast(dict[str, object], value)


def _list_of_objects(value: dict[str, object], key: str) -> list[dict[str, object]]:
    raw = value.get(key)
    if not isinstance(raw, list):
        raise TypeError(f"{key} must be a JSON object list")
    items = cast(list[object], raw)
    if not all(isinstance(item, dict) for item in items):
        raise TypeError(f"{key} must be a JSON object list")
    return [cast(dict[str, object], item) for item in items]


def _list_of_strings(value: dict[str, object], key: str) -> list[str]:
    raw = value.get(key)
    if not isinstance(raw, list):
        raise TypeError(f"{key} must be a JSON string list")
    items = cast(list[object], raw)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"{key} must be a JSON string list")
    return cast(list[str], items)


def _string(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def _integer(value: dict[str, object], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return item


def _trade_date(row: dict[str, object]) -> date:
    raw = _string(row, "trade_date")
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    return date.fromisoformat(raw)


def load_prepared_continuous_registration(root: Path) -> dict[str, object]:
    """Reopen the immutable prepared study; exposes no evaluation labels."""
    return _load_prepared(root)["registration"]


def continuous_study_scope(
    registration_id: str, coverage_window_id: str, profile_arm: str, cadence: str
) -> tuple[str, str]:
    """One deterministic case/model experiment and cadence-specific research arm."""
    if cadence not in {"coverage", "expiry_only", "scheduled", "event"}:
        raise ValueError("unregistered continuous cadence")
    experiment = "continuous-case-" + canonical_hash(
        {"registration_id": registration_id, "window": coverage_window_id, "profile": profile_arm}
    )
    return experiment, experiment + ":" + cadence
