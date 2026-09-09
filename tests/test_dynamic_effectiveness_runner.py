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
        "event_support": "supported",
        "expectations": "The prior consensus was lower.",
        "revision_conclusion": "Initial frozen analysis.",
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


def test_route_qualification_shares_parent_scope_and_preserves_old_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sqlite3
    from dataclasses import replace

    from market_impact_agent.dynamic_effectiveness_runner import (
        _verified_qualification_report,  # pyright: ignore[reportPrivateUsage]
    )
    from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope
    from market_impact_agent.runtime_store import RunStatus

    profiles = cast(tuple[Any, Any, Any], _profiles())
    for profile in profiles:
        monkeypatch.setenv(profile.credential_env, "synthetic-study-key")
    original = asyncio.create_subprocess_exec

    async def spawn(program: str, *args: str, **kwargs: Any):
        answer = _answer()
        answer["evidence_refs"] = ["synthetic-release", "synthetic-market"]
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(answer)
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    parent = LocalDataSnapshotStore(tmp_path / "parent")
    journal = RunJournal.authoritative(parent)
    journal.start_run(
        run_id="study", config_hash=canonical_hash("study-40-dollar"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "study",
        500,
        40_000_000,
        prior_requests=20,
        prior_cost_microusd=5_356_905,
        prior_reserved_microusd=11_769,
        prior_unsettled_requests=1,
        scope_limits=(
            ModelBudgetScope("route_qualification", 1_000_000, 85_194),
            ModelBudgetScope("study", 39_000_000, 5_271_711, 11_769),
        ),
        scope="route_qualification",
    )
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
    prepare_dynamic_route_qualification(
        root,
        profiles=profiles,
        verification_path=verification_path,
        registered_at=datetime.now(UTC),
        shared_budget=budget,
    )
    with pytest.raises(ValueError, match="ancestry changed"):
        asyncio.run(
            run_dynamic_route_qualification(root, shared_budget=replace(budget, max_requests=501))
        )
    report = asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget))
    assert report["stage_passed"] is True, report
    assert report["reconciled"] is True
    batch = cast(dict[str, int], report["budget"])
    assert batch["physical_requests"] == 3
    assert batch["unsettled_requests"] == 0
    assert budget.summary()["reserved_microusd"] == 11_769
    assert budget.summary()["unsettled_requests"] == 1
    assert budget.summary()["known_cost_microusd"] == 5_356_905 + batch["known_cost_microusd"]
    assert budget.scope_summary()["known_cost_microusd"] == 85_194 + batch["known_cost_microusd"]
    assert journal.get_run("study").status is RunStatus.RUNNING
    assert _verified_qualification_report(root, require_passed=True) == report
    assert asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget)) == report
    with pytest.raises(RuntimeError, match="registered study stage"):
        asyncio.run(budget.reserve("outside-stage", 1_000_000))
    with sqlite3.connect(journal.path) as connection:
        connection.execute(
            "UPDATE runs SET config_hash = ? WHERE run_id = ?",
            (canonical_hash("different-parent-authorization"), "study"),
        )
    with pytest.raises(ValueError, match="ancestry changed"):
        _verified_qualification_report(root, require_passed=True)


def _qualification_verification(tmp_path: Path) -> Path:
    path = tmp_path / "verification.json"
    path.write_text(
        json.dumps(
            {
                "runtime": runtime_identity(),
                "checks": dict.fromkeys(
                    (
                        "ruff",
                        "format",
                        "pyright",
                        "pytest",
                        "typescript",
                        "node_tests",
                        "production_entry",
                        "independent_review",
                    ),
                    "passed",
                ),
                "evidence_refs": ["synthetic-offline-review"],
            }
        )
    )
    return path


def test_qualification_panel_registration_rejects_mismatches(tmp_path: Path) -> None:
    from market_impact_agent.dynamic_effectiveness import AnalysisTopology
    from market_impact_agent.dynamic_effectiveness_runner import load_dynamic_route_qualification

    luna, terra, sol = _profiles()
    panel = (AnalysisTopology.LUNA_MAX, AnalysisTopology.TERRA_HIGH)
    verification = _qualification_verification(tmp_path)
    root = tmp_path / "qualification"
    for invalid_panel, invalid_profiles in (
        ((panel[0], panel[0]), (luna, terra)),
        ((panel[0],), (luna,)),
        (panel, (luna, sol)),
        (panel, (luna, luna)),
        (panel, (luna, terra, sol)),
        ((panel[0], cast(AnalysisTopology, "unknown")), (luna, terra)),
    ):
        with pytest.raises(ValueError, match=r"panel|profiles"):
            prepare_dynamic_route_qualification(
                root,
                profiles=invalid_profiles,
                route_panel=invalid_panel,
                verification_path=verification,
            )
    registered = prepare_dynamic_route_qualification(
        root,
        profiles=(terra, luna),
        route_panel=panel,
        verification_path=verification,
    )
    assert registered["route_panel"] == [item.value for item in panel]
    assert (
        prepare_dynamic_route_qualification(
            root,
            profiles=(luna, terra),
            route_panel=panel,
            verification_path=verification,
        )
        == registered
    )
    with pytest.raises(ValueError, match="cannot change"):
        prepare_dynamic_route_qualification(
            root,
            profiles=(luna, terra),
            route_panel=tuple(reversed(panel)),
            verification_path=verification,
        )
    swapped = dict(registered)
    swapped["profiles"] = {panel[0].value: terra.to_dict(), panel[1].value: luna.to_dict()}
    swapped["registration_hash"] = canonical_hash(
        {key: value for key, value in swapped.items() if key != "registration_hash"}
    )
    (root / "qualification-registration.json").write_text(json.dumps(swapped))
    with pytest.raises(ValueError, match="wrong route"):
        load_dynamic_route_qualification(root)
    # Rehashing a malformed panel must not make it a valid registration.
    registered["route_panel"] = [panel[0].value, panel[0].value]
    registered["registration_hash"] = canonical_hash(
        {key: value for key, value in registered.items() if key != "registration_hash"}
    )
    (root / "qualification-registration.json").write_text(json.dumps(registered))
    with pytest.raises(ValueError, match="route panel"):
        load_dynamic_route_qualification(root)


@pytest.mark.parametrize("shared_parent", [True, False])
def test_two_route_qualification_native_v2_group_replay_and_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    shared_parent: bool,
) -> None:
    import sqlite3
    from dataclasses import replace

    from market_impact_agent.dynamic_effectiveness import AnalysisTopology
    from market_impact_agent.dynamic_effectiveness_runner import (
        _verified_qualification_report,  # pyright: ignore[reportPrivateUsage]
    )
    from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope
    from market_impact_agent.pi_deployment import installed_permit

    profiles = _profiles()[:2]
    panel = (AnalysisTopology.LUNA_MAX, AnalysisTopology.TERRA_HIGH)
    for profile in profiles:
        monkeypatch.setenv(profile.credential_env, "synthetic-study-key")
    monkeypatch.setattr(
        "market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path / "installed"
    )
    original = asyncio.create_subprocess_exec
    spawned = 0

    async def spawn(program: str, *args: str, **kwargs: Any):
        nonlocal spawned
        spawned += 1
        answer = _answer()
        answer["evidence_refs"] = ["synthetic-release", "synthetic-market"]
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(answer)
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    store = LocalDataSnapshotStore(tmp_path / "parent")
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="new-study", config_hash=canonical_hash("140-dollar"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "new-study",
        2000,
        140_000_000,
        scope_limits=(
            ModelBudgetScope("route_qualification", 1_000_000),
            ModelBudgetScope("study", 139_000_000),
        ),
        scope="route_qualification",
    )
    root = tmp_path / "qualification"
    registration = prepare_dynamic_route_qualification(
        root,
        profiles=profiles,
        route_panel=panel,
        verification_path=_qualification_verification(tmp_path),
        shared_budget=budget if shared_parent else None,
    )
    report = asyncio.run(
        run_dynamic_route_qualification(root, shared_budget=budget if shared_parent else None)
    )
    assert report["stage_passed"] is True, report
    assert spawned == 2
    cases = cast(list[dict[str, Any]], report["cases"])
    assert [item["topology"] for item in cases] == [item.value for item in panel]
    owner = f"dynamic-route-qualification-{registration['registration_hash']}"
    if not shared_parent:
        journal = RunJournal.authoritative(LocalDataSnapshotStore(root / "authority"))
        budget = ModelBudget(
            journal,
            owner,
            12,
            1_000_000,
            scope_limits=(ModelBudgetScope("route_qualification", 1_000_000),),
            scope="route_qualification",
        )
    groups = [
        event
        for event in journal.events(budget.owner_run_id)
        if event.event_type == "pi.budget.group_admitted"
    ]
    assert len(groups) == 1
    assert groups[0].payload["members"] == [
        {"member_id": item.value, "max_requests": 6, "max_cost_microusd": 500_000} for item in panel
    ]
    reservations = [
        event
        for event in journal.events(budget.owner_run_id)
        if event.event_type == "pi.budget.reserved"
    ]
    assert len(reservations) == 2
    # These reservations come from real pi token admission, including 4096 output tokens.
    assert all(
        0 < cast(int, event.payload["reserved_microusd"]) < 500_000 for event in reservations
    )
    assert {event.payload["group_member_id"] for event in reservations} == {
        item.value for item in panel
    }
    assert _verified_qualification_report(root, require_passed=True) == report
    # Resume after business terminals exist but before the report is published.
    (root / "qualification-report.json").unlink()
    assert (
        asyncio.run(
            run_dynamic_route_qualification(root, shared_budget=budget if shared_parent else None)
        )
        == report
    )
    assert spawned == 2
    assert (
        asyncio.run(
            run_dynamic_route_qualification(root, shared_budget=budget if shared_parent else None)
        )
        == report
    )
    accept_dynamic_route_qualification(root)
    permit = installed_permit(tmp_path / "installed")
    assert permit is not None
    assert set(permit.route_identities) == {profile.route_identity for profile in profiles}
    for case in cases:
        case_store = LocalDataSnapshotStore(root / "cases" / case["topology"])
        terminal = case_store.artifacts.read_json(case["terminal_hash"])
        assert "research-thesis.v2" in json.dumps(terminal)
    # A rehashed presentation report cannot change the authoritative route panel.
    changed = dict(report, cases=list(reversed(cases))[:1])
    changed["report_hash"] = canonical_hash(
        {key: value for key, value in changed.items() if key != "report_hash"}
    )
    (root / "qualification-report.json").write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="authoritative terminal"):
        _verified_qualification_report(root, require_passed=True)
    (root / "qualification-report.json").write_text(json.dumps(report))
    # Shared parent has ample budget; the route group remains the hard limit.
    grouped = replace(budget, group_id=owner)

    async def exhaust() -> None:
        luna = grouped.for_group_member(panel[0].value)
        for number in range(5):
            key = f"synthetic-extra-{number}"
            await luna.reserve(key, 1)
            luna.settle(key, cost_microusd=1, evidence_ref=canonical_hash(key))
        with pytest.raises(RuntimeError, match="group member allowance"):
            await luna.reserve("seventh", 1)
        with pytest.raises(RuntimeError, match="group member allowance"):
            await grouped.for_group_member(panel[1].value).reserve("over-cost", 500_000)

    if shared_parent:
        asyncio.run(exhaust())
    else:
        with pytest.raises(ValueError, match="cannot append to a terminal run"):
            asyncio.run(grouped.for_group_member(panel[0].value).reserve("after-terminal", 1))
    # Removing allocation ancestry must fail replay, even with intact native cases.
    with sqlite3.connect(journal.path) as connection:
        connection.execute("DELETE FROM events WHERE event_id = ?", (groups[0].event_id,))
    with pytest.raises(ValueError, match=r"group admission|journal hash chain"):
        _verified_qualification_report(root, require_passed=True)


def test_panel_group_admission_fails_before_any_route_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from market_impact_agent.dynamic_effectiveness import AnalysisTopology
    from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope

    async def unexpected_spawn(*args: Any, **kwargs: Any) -> None:
        pytest.fail("an unfunded qualification must not dispatch any route")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_spawn)
    journal = RunJournal.authoritative(LocalDataSnapshotStore(tmp_path / "parent"))
    journal.start_run(
        run_id="parent", config_hash=canonical_hash("parent"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "parent",
        2000,
        140_000_000,
        prior_cost_microusd=1,
        scope_limits=(ModelBudgetScope("route_qualification", 1_000_000, 1),),
        scope="route_qualification",
    )
    root = tmp_path / "qualification"
    prepare_dynamic_route_qualification(
        root,
        profiles=_profiles()[:2],
        route_panel=(AnalysisTopology.LUNA_MAX, AnalysisTopology.TERRA_HIGH),
        verification_path=_qualification_verification(tmp_path),
        shared_budget=budget,
    )
    with pytest.raises(RuntimeError, match="cannot fund the complete group"):
        asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget))
    assert not journal.events("parent")
    assert not (root / "cases").exists()


def test_explicit_qualification_recovery_preserves_failure_and_original_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace

    from market_impact_agent.dynamic_effectiveness import AnalysisTopology
    from market_impact_agent.dynamic_effectiveness_runner import (
        _verified_qualification_report,  # pyright: ignore[reportPrivateUsage]
        load_dynamic_route_qualification,
    )
    from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope

    profiles = _profiles()[:2]
    panel = (AnalysisTopology.LUNA_MAX, AnalysisTopology.TERRA_HIGH)
    for profile in profiles:
        monkeypatch.setenv(profile.credential_env, "synthetic-study-key")
    monkeypatch.setattr(
        "market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path / "installed"
    )
    original = asyncio.create_subprocess_exec
    spawned = 0

    async def spawn(program: str, *args: str, **kwargs: Any):
        nonlocal spawned
        spawned += 1
        answer = _answer()
        answer["evidence_refs"] = ["synthetic-release", "synthetic-market"]
        if spawned == 2:
            answer["typed_unknowns"] = {"invalid": "object instead of array"}
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(answer)
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    journal = RunJournal.authoritative(LocalDataSnapshotStore(tmp_path / "parent"))
    journal.start_run(
        run_id="study", config_hash=canonical_hash("140-dollar"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "study",
        2000,
        140_000_000,
        scope_limits=(ModelBudgetScope("route_qualification", 1_000_000),),
        scope="route_qualification",
    )
    verification = _qualification_verification(tmp_path)
    prior_root = tmp_path / "attempt-1"
    first = prepare_dynamic_route_qualification(
        prior_root,
        profiles=profiles,
        route_panel=panel,
        verification_path=verification,
        shared_budget=budget,
    )
    failed = asyncio.run(run_dynamic_route_qualification(prior_root, shared_budget=budget))
    assert failed["stage_passed"] is False
    assert failed["reconciled"] is True
    first_bytes = (prior_root / "qualification-report.json").read_bytes()
    root = tmp_path / "attempt-2"
    for wrong_budget, wrong_profiles, wrong_panel in (
        (replace(budget, max_requests=2001), profiles, panel),
        (budget, (profiles[0], _profiles()[2]), (panel[0], AnalysisTopology.SOL_HIGH)),
    ):
        with pytest.raises(
            ValueError, match=r"same panel, profiles and shared parent|parent model budget changed"
        ):
            prepare_dynamic_route_qualification(
                root,
                profiles=wrong_profiles,
                route_panel=wrong_panel,
                verification_path=verification,
                shared_budget=wrong_budget,
                prior_qualification_root=prior_root,
            )
    # An unreconciled predecessor is not an authorization to regenerate.
    unknown = dict(failed, reconciled=False)
    unknown["report_hash"] = canonical_hash(
        {key: value for key, value in unknown.items() if key != "report_hash"}
    )
    (prior_root / "qualification-report.json").write_text(json.dumps(unknown))
    with pytest.raises(ValueError, match="failed reconciled"):
        prepare_dynamic_route_qualification(
            root,
            profiles=profiles,
            route_panel=panel,
            verification_path=verification,
            shared_budget=budget,
            prior_qualification_root=prior_root,
        )
    (prior_root / "qualification-report.json").write_bytes(first_bytes)
    second = prepare_dynamic_route_qualification(
        root,
        profiles=profiles,
        route_panel=panel,
        verification_path=verification,
        shared_budget=budget,
        prior_qualification_root=prior_root,
    )
    assert second["registration_hash"] != first["registration_hash"]
    assert (
        cast(dict[str, object], second["prior_qualification"])["report_hash"]
        == failed["report_hash"]
    )
    assert load_dynamic_route_qualification(root) == second
    assert (
        prepare_dynamic_route_qualification(
            root,
            profiles=profiles,
            route_panel=panel,
            verification_path=verification,
            shared_budget=budget,
            prior_qualification_root=prior_root,
        )
        == second
    )
    sibling = tmp_path / "sibling-attempt"
    with pytest.raises(ValueError, match="single recovery attempt"):
        prepare_dynamic_route_qualification(
            sibling,
            profiles=profiles,
            route_panel=panel,
            verification_path=verification,
            shared_budget=budget,
            prior_qualification_root=prior_root,
        )
    with pytest.raises(ValueError, match="single recovery attempt"):
        asyncio.run(run_dynamic_route_qualification(sibling, shared_budget=budget))
    assert spawned == 2
    claims = [
        event
        for event in journal.events("study")
        if event.event_type == "qualification.recovery.claimed"
    ]
    assert len(claims) == 1
    assert claims[0].payload["registration_hash"] == second["registration_hash"]
    assert claims[0].payload["root"] == str(root.resolve())
    with pytest.raises(ValueError, match="cannot change"):
        prepare_dynamic_route_qualification(
            root,
            profiles=profiles,
            route_panel=panel,
            verification_path=verification,
            shared_budget=budget,
        )
    report = asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget))
    assert report["stage_passed"] is True, report
    assert spawned == 4
    assert (prior_root / "qualification-report.json").read_bytes() == first_bytes
    assert asyncio.run(run_dynamic_route_qualification(prior_root, shared_budget=budget)) == failed
    assert _verified_qualification_report(root, require_passed=True) == report
    assert asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget)) == report
    assert spawned == 4
    events = journal.events("study")
    assert len([event for event in events if event.event_type == "pi.budget.group_admitted"]) == 1
    reservations = [event for event in events if event.event_type == "pi.budget.reserved"]
    assert len(reservations) == 4
    assert {event.payload["group_id"] for event in reservations} == {
        f"dynamic-route-qualification-{first['registration_hash']}"
    }
    assert budget.summary()["known_cost_microusd"] == (
        cast(dict[str, int], failed["budget"])["known_cost_microusd"]
        + cast(dict[str, int], report["budget"])["known_cost_microusd"]
    )
    accept_dynamic_route_qualification(root)
    with pytest.raises(ValueError, match="failed reconciled"):
        prepare_dynamic_route_qualification(
            tmp_path / "attempt-3",
            profiles=profiles,
            route_panel=panel,
            verification_path=verification,
            shared_budget=budget,
            prior_qualification_root=root,
        )
    # A passed original run cannot become the predecessor of a recovery.
    passed = dict(failed, stage_passed=True)
    passed["report_hash"] = canonical_hash(
        {key: value for key, value in passed.items() if key != "report_hash"}
    )
    (prior_root / "qualification-report.json").write_text(json.dumps(passed))
    with pytest.raises(ValueError, match="authoritative terminal"):
        prepare_dynamic_route_qualification(
            tmp_path / "passed-recovery",
            profiles=profiles,
            route_panel=panel,
            verification_path=verification,
            shared_budget=budget,
            prior_qualification_root=prior_root,
        )
    with pytest.raises(ValueError):
        load_dynamic_route_qualification(root)


@pytest.mark.parametrize("legacy_first_recovery", [False, True])
def test_qualification_successor_chain_preserves_original_budget_and_prevents_forks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    legacy_first_recovery: bool,
) -> None:
    from dataclasses import replace

    import market_impact_agent.dynamic_effectiveness_runner as runner
    from market_impact_agent.dynamic_effectiveness import AnalysisTopology
    from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope

    profiles = _profiles()[:2]
    panel = (AnalysisTopology.LUNA_MAX, AnalysisTopology.TERRA_HIGH)
    for profile in profiles:
        monkeypatch.setenv(profile.credential_env, "synthetic-study-key")
    monkeypatch.setattr(
        "market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path / "installed"
    )
    original_spawn = asyncio.create_subprocess_exec
    spawned = 0

    async def spawn(program: str, *args: str, **kwargs: Any):
        nonlocal spawned
        spawned += 1
        answer = _answer()
        answer["evidence_refs"] = ["synthetic-release", "synthetic-market"]
        if spawned in {2, 4}:
            answer["typed_unknowns"] = {"invalid": "array required"}
        kwargs["env"]["PORTFOLIO_FIXTURE_ANSWER"] = json.dumps(answer)
        return await original_spawn(
            program,
            "--import",
            str(Path(__file__).with_name("portfolio_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    journal = RunJournal.authoritative(LocalDataSnapshotStore(tmp_path / "parent"))
    journal.start_run(
        run_id="study", config_hash=canonical_hash("140-dollar"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "study",
        2000,
        140_000_000,
        scope_limits=(ModelBudgetScope("route_qualification", 1_000_000),),
        scope="route_qualification",
    )
    verification = _qualification_verification(tmp_path)
    roots = [tmp_path / f"attempt-{number}" for number in range(1, 4)]
    registrations: list[dict[str, object]] = []
    reports: list[dict[str, object]] = []
    originals: list[bytes] = []
    original_reference = runner._qualification_recovery_reference  # pyright: ignore[reportPrivateUsage]

    def legacy_reference(*args: Any, **kwargs: Any) -> dict[str, object]:
        kwargs["legacy"] = True
        return original_reference(*args, **kwargs)

    for index, root in enumerate(roots):
        with monkeypatch.context() as patch:
            if index == 1 and legacy_first_recovery:
                patch.setattr(runner, "_qualification_recovery_reference", legacy_reference)
            registration = prepare_dynamic_route_qualification(
                root,
                profiles=profiles,
                route_panel=panel,
                verification_path=verification,
                shared_budget=budget,
                prior_qualification_root=roots[index - 1] if index else None,
            )
        registrations.append(registration)
        report = asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget))
        reports.append(report)
        originals.append((root / "qualification-report.json").read_bytes())
        assert report["stage_passed"] is (index == 2), report
        assert report["reconciled"] is True
    assert spawned == 6
    assert [len(cast(list[object], report["cases"])) for report in reports] == [2, 2, 2]
    final_ref = cast(dict[str, object], registrations[2]["prior_qualification"])
    assert final_ref["registration_hash"] == registrations[1]["registration_hash"]
    assert final_ref["group_registration_hash"] == registrations[0]["registration_hash"]
    assert final_ref["schema_version"] == "market-impact.qualification-recovery-reference.v2"
    if legacy_first_recovery:
        assert "schema_version" not in cast(
            dict[str, object], registrations[1]["prior_qualification"]
        )
    for index, root in enumerate(roots):
        assert (root / "qualification-report.json").read_bytes() == originals[index]
        assert (
            asyncio.run(run_dynamic_route_qualification(root, shared_budget=budget))
            == reports[index]
        )
    assert spawned == 6
    accept_dynamic_route_qualification(roots[2])
    events = journal.events("study")
    assert sum(event.event_type == "pi.budget.group_admitted" for event in events) == 1
    claims = [event for event in events if event.event_type == "qualification.recovery.claimed"]
    assert len(claims) == 2
    assert len({event.event_id for event in claims}) == 2
    assert budget.summary()["physical_requests"] == 6
    assert budget.summary()["known_cost_microusd"] == sum(
        cast(dict[str, int], report["budget"])["known_cost_microusd"] for report in reports
    )
    for index in (0, 1):
        with pytest.raises(ValueError, match="single recovery attempt"):
            prepare_dynamic_route_qualification(
                tmp_path / f"fork-{index}",
                profiles=profiles,
                route_panel=panel,
                verification_path=verification,
                shared_budget=budget,
                prior_qualification_root=roots[index],
            )
    assert spawned == 6
    # All generations consume the original member ceilings, never a fresh allowance.
    group_id = f"dynamic-route-qualification-{registrations[0]['registration_hash']}"

    async def exhaust() -> None:
        member = replace(budget, group_id=group_id).for_group_member(panel[0].value)
        for number in range(3):
            await member.reserve(f"extra-{number}", 1)
        with pytest.raises(RuntimeError, match="group member allowance"):
            await member.reserve("seventh-physical-request", 1)

    asyncio.run(exhaust())
    # Changing immediate ancestry while retaining the original group cannot pass replay.
    changed = dict(registrations[2])
    changed["prior_qualification"] = dict(
        final_ref, group_registration_hash=canonical_hash("other")
    )
    changed["registration_hash"] = canonical_hash(
        {key: value for key, value in changed.items() if key != "registration_hash"}
    )
    (roots[2] / "qualification-registration.json").write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="ancestry changed"):
        runner.load_dynamic_route_qualification(roots[2])
