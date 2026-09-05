from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    PatternPackReference,
    canonical_hash,
    pattern_pack_from_dict,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.dynamic_effectiveness_runner import (
    accept_dynamic_route_qualification,
    load_dynamic_effectiveness_study,
    portfolio_actions_complete,
    prepare_dynamic_effectiveness_study,
    prepare_dynamic_route_qualification,
    run_dynamic_route_qualification,
    run_opened_analysis_ablation,
    run_portfolio_ablation,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import runtime_identity
from market_impact_agent.research import EvidenceTier
from market_impact_agent.runtime_store import RunJournal

_INPUTS = (
    "cn-2018-bear-market/2018-07-02",
    "cn-2019-q1-fast-rebound/2019-01-07",
    "cn-2020-covid-closure-shock/2020-02-03",
    "cn-2020-covid-closure-shock/2020-03-23",
    "cn-2021-index-flat-sector-rotation/2021-07-01",
    "cn-2021-index-flat-sector-rotation/2021-12-01",
    "cn-2024-policy-melt-up/2024-09-24",
    "cn-2024-post-rally-whipsaw/2024-10-09",
)


def _profiles():
    root = Path(__file__).parents[1] / "examples" / "providers"
    return tuple(
        load_model_provider_profile(root / name)
        for name in (
            "pi-cpa-luna-max-v2.json",
            "pi-cpa-terra-high-v2.json",
            "pi-cpa-sol-high-v2.json",
        )
    )


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    pattern_source = (
        Path(__file__).parents[1] / "examples" / "agent" / "market_regime" / "pattern-pack.json"
    )
    pattern_path = tmp_path / "pattern-pack.json"
    pattern_path.write_bytes(pattern_source.read_bytes())
    pattern = pattern_pack_from_dict(json.loads(pattern_path.read_text()))
    inputs_root = tmp_path / "inputs"
    for input_ref in _INPUTS:
        at = datetime.fromisoformat(input_ref.rsplit("/", maxsplit=1)[-1]).replace(
            hour=1, minute=25, tzinfo=UTC
        )
        release = {
            "published_at": (at - timedelta(hours=1)).isoformat(),
            "fact": "Policy and market evidence available before the cutoff.",
        }
        market = {
            "as_of": (at - timedelta(minutes=5)).isoformat(),
            "fact": "The proxy declined three percent over the prior five sessions.",
        }
        evidence = (
            EvidenceReference(
                "release",
                "incremental-fact",
                f"regime-manifest://{input_ref}/release",
                EvidenceTier.OFFICIAL,
                at - timedelta(hours=1),
                canonical_hash(release),
                "Frozen release evidence.",
            ),
            EvidenceReference(
                "market",
                "priced-in-context",
                f"regime-manifest://{input_ref}/market",
                EvidenceTier.REGULATED,
                at - timedelta(minutes=5),
                canonical_hash(market),
                "Frozen market context.",
            ),
        )
        pack = EvidencePack.build(
            event_id=f"event-{canonical_hash(input_ref)[:16]}",
            as_of=at,
            research_question="Old opened development question.",
            evidence=evidence,
            pattern_packs=(
                PatternPackReference(
                    pattern.pack_id,
                    pattern.version,
                    pattern.available_at,
                    canonical_hash(pattern.to_dict()),
                ),
            ),
            allowed_targets=("broad-market-a",),
            data_gaps=("This is Modeled-PIT, not Strict-PIT.",),
        )
        destination = inputs_root / input_ref
        destination.mkdir(parents=True)
        (destination / "evidence-pack.json").write_text(
            json.dumps(pack.to_dict()), encoding="utf-8"
        )
        (destination / "evidence-documents.json").write_text(
            json.dumps({"documents": {"release": release, "market": market}}),
            encoding="utf-8",
        )
    return inputs_root, pattern_path


def _answer() -> dict[str, object]:
    return {
        "horizon_band": "tactical",
        "primary_horizon_sessions": 5,
        "base_case_direction": "up",
        "thesis": "The incremental fact supports a near-term repricing.",
        "priced_in_assessment": "The prior decline did not fully price the positive fact.",
        "transmission": ["incremental fact -> revisions -> proxy value"],
        "counter_scenario": "The fact may prove transitory.",
        "evidence_refs": ["release", "market"],
        "counterevidence_refs": [],
        "invalidation_conditions": ["A later point-in-time release reverses the fact."],
        "review_after_sessions": 1,
        "typed_unknowns": ["future execution"],
    }


def test_study_freezes_exact_profiles_inputs_and_context_policy(tmp_path: Path) -> None:
    inputs_root, pattern_path = _inputs(tmp_path)
    profiles = cast(tuple[Any, Any, Any], _profiles())
    value = prepare_dynamic_effectiveness_study(
        tmp_path / "study",
        inputs_root=inputs_root,
        pattern_pack_path=pattern_path,
        profiles=profiles,
        registered_at=datetime(2026, 9, 4, tzinfo=UTC),
    )

    assert value == load_dynamic_effectiveness_study(tmp_path / "study")
    assert len(cast(list[object], cast(dict[str, object], value["study"])["opened_cases"])) == 8
    for raw in cast(dict[str, dict[str, object]], value["profiles"]).values():
        assert raw["context_window_tokens"] == 272_000
        assert raw["compaction_trigger_tokens"] == 258_000

    first = inputs_root / _INPUTS[0] / "evidence-documents.json"
    first.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="changed"):
        asyncio.run(
            run_opened_analysis_ablation(
                tmp_path / "study",
                inputs_root=inputs_root,
                pattern_pack_path=pattern_path,
            )
        )


def test_opened_runner_uses_three_reused_pi_workers_without_unneeded_judge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs_root, pattern_path = _inputs(tmp_path)
    profiles = cast(tuple[Any, Any, Any], _profiles())
    for profile in profiles:
        monkeypatch.setenv(profile.credential_env, "synthetic-study-key")
    permit = PiRuntimePermit(
        canonical_hash(runtime_identity()),
        tuple(profile.route_identity for profile in profiles),
        "synthetic-study-proof",
    )

    def installed(_root: Path) -> PiRuntimePermit:
        return permit

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec
    spawns: list[str] = []

    async def spawn(program: str, *args: str, **kwargs: Any):
        spawns.append(program)
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(_answer())
        kwargs["env"]["DYNAMIC_STUDY_FIXTURE"] = "1"
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    study = tmp_path / "study"
    prepare_dynamic_effectiveness_study(
        study,
        inputs_root=inputs_root,
        pattern_pack_path=pattern_path,
        profiles=profiles,
        registered_at=datetime(2026, 9, 4, tzinfo=UTC),
    )

    report = asyncio.run(
        run_opened_analysis_ablation(
            study,
            inputs_root=inputs_root,
            pattern_pack_path=pattern_path,
        )
    )

    results = cast(list[dict[str, object]], report["results"])
    assert len(results) == 34  # 24 base + 9 stability + 1 date-presentation pair.
    assert all(item["status"] == "completed" for item in results)
    assert not any(item["repetition"] == "conditional-judge" for item in results)
    assert cast(dict[str, int], report["budget"])["physical_requests"] == 34
    assert len(spawns) == 3
    assert report["outcomes_opened"] is False
    relative = next(item for item in results if item["repetition"] == "memory-sensitivity")
    runs = LocalDataSnapshotStore(study / "analysis-runs")
    record = RunJournal.authoritative(runs).get_run(cast(str, relative["run_id"]))
    binding = cast(dict[str, object], runs.artifacts.read_json(record.config_hash))
    selected = runs.artifacts.read_json(cast(str, binding["selected_inputs_artifact_hash"]))
    rendered = json.dumps(selected)
    assert "2020-02-03" not in rendered
    assert "cn-2020-covid-closure-shock" not in rendered

    portfolio = asyncio.run(run_portfolio_ablation(study))
    portfolio_results = cast(list[dict[str, object]], portfolio["results"])
    assert len(portfolio_results) == 12
    assert all(item["status"] == "completed" for item in portfolio_results)
    assert all(item["within_preregistered_reasonable_actions"] for item in portfolio_results)
    assert portfolio["same_bullish_thesis_reused"] is True
    assert all(cast(dict[str, bool], portfolio["same_viewpoint_account_differentiation"]).values())
    assert cast(dict[str, int], portfolio["budget"])["physical_requests"] == 12
    assert len(spawns) == 6  # Three reused analysis workers plus three portfolio workers.
    assert portfolio["mock_execution"] is False


def test_portfolio_completion_requires_exact_nonempty_matrix() -> None:
    assert portfolio_actions_complete([]) is False
    skipped = [
        {
            "scenario_id": "bullish-cash",
            "topology": "luna_max",
            "status": "not_run_provider_paused",
        }
    ]
    assert portfolio_actions_complete(cast(list[dict[str, object]], skipped)) is False


def test_route_acceptance_rejects_self_hashed_report_without_signed_runs(
    tmp_path: Path,
) -> None:
    verification = {
        "runtime": runtime_identity(),
        "checks": {
            name: "passed"
            for name in (
                "ruff",
                "format",
                "pyright",
                "pytest",
                "typescript",
                "node_tests",
                "production_entry",
                "independent_review",
            )
        },
        "evidence_refs": ["synthetic-offline-review"],
    }
    verification_path = tmp_path / "verification.json"
    verification_path.write_text(json.dumps(verification))
    root = tmp_path / "qualification"
    registration = prepare_dynamic_route_qualification(
        root,
        profiles=cast(tuple[Any, Any, Any], _profiles()),
        verification_path=verification_path,
        registered_at=datetime(2026, 9, 4, tzinfo=UTC),
    )
    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-route-qualification-report.v1",
        "registration_hash": registration["registration_hash"],
        "runtime": registration["runtime"],
        "cases": [],
        "budget": {"unsettled_requests": 0},
        "stage_passed": True,
        "reconciled": True,
        "execution_capability": False,
        "live_execution": False,
    }
    report["report_hash"] = canonical_hash(report)
    (root / "qualification-report.json").write_text(json.dumps(report))

    with pytest.raises(ValueError, match="authoritative terminal"):
        accept_dynamic_route_qualification(root)


def test_failed_route_qualification_remains_replayable_without_becoming_accepted(
    tmp_path: Path,
) -> None:
    verification = {
        "runtime": runtime_identity(),
        "checks": {
            name: "passed"
            for name in (
                "ruff",
                "format",
                "pyright",
                "pytest",
                "typescript",
                "node_tests",
                "production_entry",
                "independent_review",
            )
        },
        "evidence_refs": ["synthetic-offline-review"],
    }
    verification_path = tmp_path / "verification.json"
    verification_path.write_text(json.dumps(verification))
    root = tmp_path / "qualification"
    registration = prepare_dynamic_route_qualification(
        root,
        profiles=cast(tuple[Any, Any, Any], _profiles()),
        verification_path=verification_path,
        registered_at=datetime(2026, 9, 4, tzinfo=UTC),
    )
    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-route-qualification-report.v1",
        "registration_hash": registration["registration_hash"],
        "runtime": registration["runtime"],
        "cases": [],
        "budget": {"unsettled_requests": 0},
        "stage_passed": False,
        "reconciled": True,
        "execution_capability": False,
        "live_execution": False,
    }
    report["report_hash"] = canonical_hash(report)
    (root / "qualification-report.json").write_text(json.dumps(report))

    assert asyncio.run(run_dynamic_route_qualification(root)) == report
    with pytest.raises(ValueError, match="authoritative terminal"):
        accept_dynamic_route_qualification(root)
