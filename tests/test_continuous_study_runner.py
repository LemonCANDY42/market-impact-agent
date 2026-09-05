from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_impact_agent import continuous_study_runner
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.cli import main
from market_impact_agent.continuous_study_runner import (
    preflight_continuous_study,
    prepare_continuous_study,
    report_continuous_study,
    study_budget,
)

from .test_continuous_study import require_private_continuous_study_inputs

_DATASET = Path("examples/research/market-regime-dataset-v1.json")
_PANELS = Path(".market-impact/regime")
_AUDIT = Path(".market-impact/continuous-20260905/prior-budget-audit.json")


def test_prepare_cli_replays_one_shared_budget_and_reports_pending_gaps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    require_private_continuous_study_inputs()
    shared_root = tmp_path / "shared-model-admission"
    monkeypatch.setattr(continuous_study_runner, "shared_admission_root", lambda: shared_root)
    root = tmp_path / "first-study"
    command = [
        "agent",
        "continuous-study",
        "prepare",
        "--state-root",
        str(root),
        "--dataset",
        str(_DATASET),
        "--panel-root",
        str(_PANELS),
        "--prior-usage-audit",
        str(_AUDIT),
    ]

    assert main(command) == 0
    prepared = json.loads(capsys.readouterr().out)
    assert prepared["model_calls"] == 0
    assert prepared["coverage_denominator"] == 18
    assert prepared["planned_observation_denominator"] == 72
    assert prepared["pending_observation_denominator"] == 72
    assert prepared["coverage_complete"] is False

    assert main(command) == 0
    replayed = json.loads(capsys.readouterr().out)
    assert replayed["status"] == "replayed_immutable_preparation"
    first_budget = study_budget(root, "rolling")
    assert first_budget.max_requests == 40_000_098

    registration_bytes = (root / "registration.json").read_bytes()
    (root / "coverage.json").unlink()
    (root / "daily-input-inventory.json").unlink()
    (root / "preparation.json").unlink()
    recovered = prepare_continuous_study(
        root,
        dataset_path=_DATASET,
        panel_root=_PANELS,
        prior_usage_audit_path=_AUDIT,
    )
    assert recovered["status"] == "prepared"
    assert (root / "registration.json").read_bytes() == registration_bytes

    second_root = tmp_path / "second-study"
    prepare_continuous_study(
        second_root,
        dataset_path=_DATASET,
        panel_root=_PANELS,
        prior_usage_audit_path=_AUDIT,
    )
    second_budget = study_budget(second_root, "rolling")
    assert first_budget.journal.path == second_budget.journal.path
    assert first_budget.owner_run_id == second_budget.owner_run_id
    assert second_budget.summary() == {
        "physical_requests": 98,
        "known_cost_microusd": 5_356_905,
        "reserved_microusd": 11_769,
        "unsettled_requests": 1,
    }

    preflight = preflight_continuous_study(root)
    assert preflight["stage_passed"] is False
    assert preflight["coverage_complete"] is False
    assert preflight["daily_inputs"]["missing"] > 0  # type: ignore[index,operator]
    assert preflight["cost_estimates"]["missing"] == 72  # type: ignore[index]

    registration = json.loads((root / "registration.json").read_text())
    inventory = json.loads((root / "daily-input-inventory.json").read_text())
    requirement = inventory["daily_input_requirements"][0]
    fake_proof = {"source": "self-attested"}
    daily_path = (
        root / "daily-inputs" / requirement["window_id"] / f"{requirement['trade_dates'][0]}.json"
    )
    daily_path.parent.mkdir(parents=True)
    daily_path.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.continuous-study-daily-input-manifest.v1",
                "registration_id": registration["registration_id"],
                "window_id": requirement["window_id"],
                "trade_date": requirement["trade_dates"][0],
                "source_proof": fake_proof,
                "source_proof_hash": canonical_hash(fake_proof),
            }
        )
    )
    observation = inventory["planned_observations"][0]
    cost_path = root / "cost-estimates" / f"{observation['observation_id']}.json"
    cost_path.parent.mkdir(parents=True)
    cost_path.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.continuous-study-cost-estimate.v1",
                "registration_id": registration["registration_id"],
                "observation_id": observation["observation_id"],
                "budget_stage": observation["budget_stage"],
                "provider_profile_id": observation["profile"]["provider_profile_id"],
                "provider_profile_hash": observation["profile"]["provider_profile_hash"],
                "pricing_id": observation["profile"]["pricing_id"],
                "estimated_cost_microusd": 1,
                "estimate_basis": {"claimed": "self-attested"},
            }
        )
    )
    preflight = preflight_continuous_study(root)
    assert preflight["daily_inputs"]["unverified"] >= 1  # type: ignore[index,operator]
    assert preflight["cost_estimates"]["unverified"] >= 1  # type: ignore[index,operator]
    report = report_continuous_study(root)
    assert report["coverage_complete"] is False
    assert report["pending_observation_denominator"] == 72
    assert len(report["windows"]) == 18  # type: ignore[arg-type]
    first_window = report["windows"][0]  # type: ignore[index]
    assert set(first_window) >= {  # type: ignore[arg-type]
        "original_window",
        "full_baseline",
        "missing_current",
        "current_status",
        "execution_eligibility",
    }
    assert first_window["execution_eligibility"] == "not_evaluated_no_execution_authority"  # type: ignore[index]
