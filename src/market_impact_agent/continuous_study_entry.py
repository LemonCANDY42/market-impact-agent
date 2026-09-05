"""Local production entry for frozen study inputs; source acquisition stays separate."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash, evidence_pack_from_dict
from market_impact_agent.continuous_baselines import registered_baseline_windows
from market_impact_agent.continuous_decision import ReviewFrame
from market_impact_agent.continuous_experiment import (
    FrozenContinuousWindow,
    prepare_continuous_experiment,
    registered_frame_cutoff,
    run_continuous_experiment,
)
from market_impact_agent.continuous_portfolio_runtime import build_continuous_review_frame
from market_impact_agent.continuous_research_inputs import continuous_event_facts
from market_impact_agent.continuous_study import (
    build_continuous_study_registration,
    load_pinned_regime_panels,
    load_prior_usage_audit_binding,
)
from market_impact_agent.continuous_study_runner import study_budget
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.market_regimes import load_market_regime_dataset
from market_impact_agent.model_provider import load_model_provider_profile


async def continuous_study_entry(
    *,
    action: str,
    study_root: Path,
    input_root: Path,
    dataset_path: Path,
    panel_root: Path,
    prior_usage_audit_path: Path,
) -> dict[str, object]:
    if action not in {"prepare-experiment", "run"}:
        raise ValueError("unknown continuous experiment action")
    panels = load_pinned_regime_panels(panel_root)
    registration = build_continuous_study_registration(
        load_market_regime_dataset(dataset_path),
        panels,
        prior_usage_audit=load_prior_usage_audit_binding(prior_usage_audit_path),
    )
    budget = study_budget(study_root, "rolling")
    store = LocalDataSnapshotStore(budget.journal.path.parent)
    windows: list[FrozenContinuousWindow] = []
    # Windows in one frozen batch usually share the exact same source version.
    # Reuse that verified immutable view within this entry; a new entry or any
    # changed source/policy binding still constructs and verifies a fresh graph.
    source_views: dict[str, HistoricalAShareInputs] = {}
    for registered in registered_baseline_windows(registration, panels.selection_panel.panel):
        path = input_root / f"{registered.window.window_id}.json"
        if not path.exists():
            continue
        manifest = cast(dict[str, object], json.loads(path.read_text()))
        if (
            manifest.get("registration_id") != registration.registration_id
            or manifest.get("window_id") != registered.window.window_id
        ):
            raise PermissionError("continuous input belongs to another registration or case")
        records = cast(list[dict[str, object]], manifest["records"])
        if not records:
            continue
        snapshots = tuple(cast(list[str], records[0]["market_snapshot_ids"]))
        rules = tuple(cast(list[str], records[0]["rule_artifact_hashes"]))
        halt_hashes = tuple(cast(list[str], records[0].get("fund_halt_artifact_hashes", [])))
        policy = cast(dict[str, object], manifest["source_policy"])
        source_identity = canonical_hash(
            {"snapshots": snapshots, "rules": rules, "fund_halts": halt_hashes, "policy": policy}
        )
        market = source_views.get(source_identity)
        if market is None:
            market = HistoricalAShareInputs(
                store=store,
                snapshot_ids=snapshots,
                rule_artifact_hashes=rules,
                fund_halt_artifact_hashes=halt_hashes,
                policy=ModeledHistoricalPolicy(
                    str(policy["policy_id"]),
                    Decimal(str(policy["daily_open_volume_fraction"])),
                    limit_basis=str(policy.get("limit_basis", "reported_stk_limit")),
                ),
            )
            source_views[source_identity] = market
        repositories: list[FrozenResearchRepository] = []
        frames: list[ReviewFrame] = []
        for record in records:
            if (
                tuple(cast(list[str], record["market_snapshot_ids"])) != snapshots
                or tuple(cast(list[str], record["rule_artifact_hashes"])) != rules
                or tuple(cast(list[str], record.get("fund_halt_artifact_hashes", [])))
                != halt_hashes
            ):
                raise PermissionError("window requires one immutable source binding")
            artifact = cast(
                dict[str, object], store.artifacts.read_json(str(record["research_artifact_hash"]))
            )
            pack = evidence_pack_from_dict(artifact["evidence_pack"])
            expected_cutoff = registered_frame_cutoff(date.fromisoformat(str(record["trade_date"])))
            if (
                pack.as_of != expected_cutoff
                or datetime.fromisoformat(str(record["cutoff"])) != expected_cutoff
            ):
                raise PermissionError(
                    "research artifact differs from its registered pre-open cutoff"
                )
            repository = FrozenResearchRepository(
                evidence_pack=pack,
                evidence_documents=cast(dict[str, object], artifact["documents"]),
                pattern_packs={},
            )
            repositories.append(repository)
            frames.append(
                build_continuous_review_frame(
                    repository=repository,
                    market=market,
                    new_fact_ids=await continuous_event_facts(repository),
                )
            )
        windows.append(
            FrozenContinuousWindow(
                registered.window.window_id,
                tuple(frames),
                tuple(repositories),
                market,
                registered.sessions,
                tuple(cast(list[str], manifest["candidate_symbols"])),
            )
        )
    project = Path(__file__).resolve().parents[2]
    profiles = tuple(
        load_model_provider_profile(project / f"examples/providers/pi-cpa-{name}-v2.json")
        for name in ("luna-max", "terra-high", "sol-high")
    )
    if action == "prepare-experiment":
        return await prepare_continuous_experiment(
            study_root=study_root,
            registration=registration,
            selection_panel=panels.selection_panel.panel,
            windows=tuple(windows),
            profiles=profiles,
        )
    # Preserve the existing provider manifest/source identities and range caches.
    # Acquisition remains limited to explicitly permitted historical price routes.
    from market_impact_agent.prospective_discovery_entry import discovery_source_templates

    templates = tuple(
        item for item in discovery_source_templates() if item.api_name in {"daily", "fund_daily"}
    )
    entry_binding = {
        "registration_id": registration.registration_id,
        "source_templates": sorted(item.template_id for item in templates),
        "window_frames": {
            window.window_id: [frame.to_dict() for frame in window.frames] for window in windows
        },
        "parent_budget": budget.binding,
    }
    event_id = f"{budget.owner_run_id}.research-entry.{canonical_hash(entry_binding)}"
    existing = budget.journal.event(event_id)
    if existing is None:
        now = datetime.now(UTC)
        budget.journal.append(
            run_id=budget.owner_run_id,
            event_id=event_id,
            event_type="continuous.research.entry.binding",
            observed_at=now,
            payload={"binding": entry_binding, "deadline": (now + timedelta(days=1)).isoformat()},
        )
        existing = budget.journal.event(event_id)
    if existing is None or existing.payload["binding"] != entry_binding:
        raise PermissionError("continuous acquisition entry differs from its frozen authority")
    return await run_continuous_experiment(
        study_root=study_root,
        registration=registration,
        selection_panel=panels.selection_panel.panel,
        windows=tuple(windows),
        profiles=profiles,
        historical_research_templates=templates,
        research_episode_deadline=datetime.fromisoformat(str(existing.payload["deadline"])),
    )
