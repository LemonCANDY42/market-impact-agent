"""Registered Stage 1 model/evidence comparisons on the existing research runtime.

This owner freezes a complete, matched panel before dispatch. It does not acquire
evidence during a Run, create a trading account, or implement another Agent loop.
The separate USD authorization survives panel revisions and qualification attempts.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    canonical_hash,
    evidence_pack_from_dict,
)
from market_impact_agent.agent_runtime import ProviderUsage
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_budget import ModelBudget, ModelBudgetGroupMember, ModelBudgetScope
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    model_provider_profile_from_dict,
)
from market_impact_agent.offline_authority import ReadOnlyDataSnapshotStore
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.research_thesis_runtime import (
    RESEARCH_THESIS_V2_PROMPT,
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.staged_research_sources import reopen_csrc_study_source
from market_impact_agent.staged_research_study import (
    FrozenDirectionBand,
    _market,  # pyright: ignore[reportPrivateUsage]
    _prepare_window,  # pyright: ignore[reportPrivateUsage]
    score_direction_opportunity,
    summarize_direction_opportunities,
)
from market_impact_agent.usage_ledger import UsageLedger

_AUTHORIZATION = "market-impact.staged-paid-authorization.v1"
_REGISTRATION = "market-impact.stage1-execution.v1"
_REPORT = "market-impact.stage1-execution-report.v1"
_ARMS = ("price_only", "price_and_event")
_SCOPES = (
    ModelBudgetScope("route_qualification", 1_000_000),
    ModelBudgetScope("paired_research", 45_000_000),
    ModelBudgetScope("usability", 34_000_000),
    ModelBudgetScope("bounded_followup", 60_000_000),
)
_QUESTION = (
    "Assess the specified target using only the supplied cutoff-visible evidence. "
    "Choose a five-trading-session primary direction, including unknown when justified. "
    "Distinguish event support from the price forecast, explain what expectations may "
    "already be priced, and identify the evidence that distinguishes your base case "
    "from the strongest counter-scenario. Do not use remembered later historical outcomes. "
    "No new candidate or account decision is requested in this fixed-target experiment."
)


def create_staged_authorization(root: Path, *, authorized_at: datetime) -> dict[str, object]:
    """Materialize this user's explicit new USD 140 authorization, never an old balance."""
    if authorized_at.tzinfo is None:
        raise ValueError("paid authorization requires an aware timestamp")
    root = root.resolve()
    path = root / "authorization.json"
    if path.exists():
        existing = _read_hashed(path, "authorization_hash")
        _validate_authorization(existing)
        store = LocalDataSnapshotStore(root / "authority")
        artifact = store.artifacts.put_json(existing)
        RunJournal.authoritative(store).start_run(
            run_id=_owner(existing),
            config_hash=artifact.content_hash,
            created_at=datetime.fromisoformat(str(existing["authorized_at"])),
        )
        return existing
    core: dict[str, object] = {
        "schema_version": _AUTHORIZATION,
        "authorized_at": authorized_at.isoformat(),
        "maximum_cost_microusd": 140_000_000,
        "maximum_physical_requests": 512,
        "prior_cost_microusd": 0,
        "prior_requests": 0,
        "scopes": [scope.to_dict() for scope in _SCOPES],
        "purpose": "Stage 1 complete comparisons and bounded Luna/Terra usability validation",
        "authorization_basis": "User explicitly approved a NEW USD140 ceiling on 2026-09-06",
        "paper_execution": False,
        "live_execution": False,
    }
    value = {**core, "authorization_hash": canonical_hash(core)}
    _write_new(path, value)
    store = LocalDataSnapshotStore(root / "authority")
    artifact = store.artifacts.put_json(value)
    RunJournal.authoritative(store).start_run(
        run_id=_owner(value), config_hash=artifact.content_hash, created_at=authorized_at
    )
    return value


def staged_execution_budget(root: Path, scope: str) -> ModelBudget:
    authorization = _read_hashed(root / "authorization.json", "authorization_hash")
    _validate_authorization(authorization)
    store = LocalDataSnapshotStore(root / "authority")
    journal = RunJournal.authoritative(store)
    owner = _owner(authorization)
    record = journal.get_run(owner)
    if record.status.terminal or store.artifacts.read_json(record.config_hash) != authorization:
        raise PermissionError("paid authorization differs from its open authoritative parent")
    return ModelBudget(
        journal=journal,
        owner_run_id=owner,
        max_requests=cast(int, authorization["maximum_physical_requests"]),
        max_cost_microusd=cast(int, authorization["maximum_cost_microusd"]),
        scope_limits=_SCOPES,
        scope=scope,
    )


def prepare_stage1_execution(
    *,
    root: Path,
    panel_id: str,
    preparation_root: Path,
    source_authority_root: Path,
    input_root: Path,
    profiles: tuple[ModelProviderProfile, ...],
    supplements: Mapping[str, Sequence[str]],
    budget_scope: str = "paired_research",
) -> dict[str, object]:
    """Freeze eight research Runs: 2 windows x 2 evidence arms x 2 models.

    The old preparation and authority remain read-only. New publisher captures live
    in the new authority; only verified additions can close a preparation source gap.
    """
    panel_root = _panel_root(root, panel_id)
    budget = staged_execution_budget(root, budget_scope)
    if budget_scope not in {"paired_research", "bounded_followup"}:
        raise ValueError("a research panel requires a registered research scope")
    by_model = {profile.model: profile for profile in profiles}
    if len(profiles) != 2 or set(by_model) != {"gpt-5.6-luna", "gpt-5.6-terra"}:
        raise ValueError("Stage 1 model pilot requires one Luna and one Terra profile")
    if by_model["gpt-5.6-luna"].reasoning_effort != "max" or (
        by_model["gpt-5.6-terra"].reasoning_effort != "high"
    ):
        raise ValueError("pilot retains Luna max and Terra high effort")
    preparation = _read_hashed(preparation_root / "preparation.json", "preparation_hash")
    if preparation.get("stage") != 1 or preparation.get("arms") != list(_ARMS):
        raise ValueError("execution requires a Stage 1 two-arm preparation")
    spec = _object(preparation["spec"])
    if canonical_hash(spec) != preparation["spec_hash"]:
        raise ValueError("preparation specification changed")
    registered_windows = _objects(spec["windows"])
    if not set(supplements) <= {str(window["window_id"]) for window in registered_windows}:
        raise ValueError("source supplement belongs to an unregistered window")
    source_store = ReadOnlyDataSnapshotStore(source_authority_root)
    store = LocalDataSnapshotStore(root / "authority")
    windows: list[dict[str, object]] = []
    for window_spec, frozen_window in zip(
        registered_windows, _objects(preparation["windows"]), strict=True
    ):
        window = _prepare_window(
            window_spec, spec=spec, stage=1, input_root=input_root, store=source_store
        )
        if window != frozen_window:
            raise ValueError("historical source changed after offline preparation")
        opportunity = _objects(window["opportunities"])[0]
        allowed_gap = "qualified_event_evidence_missing"
        if window["gaps"] or any(
            gap != allowed_gap for gap in cast(list[str], opportunity["gaps"])
        ):
            raise ValueError("Stage 1 lacks qualified prices, calendar, or frozen bands")
        windows.append(
            _execution_window(
                window=window,
                opportunity=opportunity,
                source_store=source_store,
                store=store,
                supplement_hashes=tuple(supplements.get(str(window["window_id"]), ())),
            )
        )
    call_graph = {model: _run_ceiling(profile) for model, profile in sorted(by_model.items())}
    panel_ceiling = sum(int(item["maximum_cost_microusd"]) * 4 for item in call_graph.values())
    scope_cap = next(scope.max_cost_microusd for scope in _SCOPES if scope.name == budget_scope)
    if panel_ceiling > scope_cap:
        raise ValueError("registered scope cannot cover this complete model/evidence panel")
    core: dict[str, object] = {
        "schema_version": _REGISTRATION,
        "panel_id": panel_id,
        "authorization_hash": _read_hashed(root / "authorization.json", "authorization_hash")[
            "authorization_hash"
        ],
        "budget_binding": budget.binding,
        "budget_scope": budget_scope,
        "preparation_hash": preparation["preparation_hash"],
        "source_authority_id": source_store.harness_authority_id,
        "source_authority_root": str(source_authority_root.resolve()),
        "source_input_root": str(input_root.resolve()),
        "profiles": {model: profile.to_dict() for model, profile in sorted(by_model.items())},
        "runtime": runtime_identity(),
        "execution_code": _execution_code(),
        "research_prompt_hash": canonical_hash(RESEARCH_THESIS_V2_PROMPT),
        "research_question": _QUESTION,
        "windows": windows,
        "call_graph": call_graph,
        "maximum_panel_cost_microusd": panel_ceiling,
        "maximum_role_runs": 8,
        "maximum_acquisition_successors": 0,
        "maximum_semantic_repairs": 0,
        "primary_horizon_sessions": 5,
        "auxiliary_horizon_sessions": 1,
        "auxiliary_interpretation": "H1 movement diagnostic of the same H5 forecast",
        "schedule": _schedule(windows),
        "model_selection_rule": {
            "luna_requires_all_four_completed": True,
            "source_grounding_review_required": True,
            "critical_unsupported_claims_allowed": 0,
            "relative_quality": (
                "No material loss on matched evidence/expectations/counter-scenario review"
            ),
            "lower_observed_cost_required": True,
            "historical_outcome_not_a_promotion_gate": True,
            "status": "development_default_only_not_investment_effectiveness",
        },
        "paper_execution": False,
        "live_execution": False,
    }
    value = {**core, "registration_hash": canonical_hash(core)}
    _write_new(panel_root / "registration.json", value)
    store.artifacts.put_json(value)
    return value


async def run_stage1_execution(root: Path, panel_id: str) -> dict[str, object]:
    """Execute registered immutable Runs; terminal replay never dispatches again."""
    registration = _registration(root, panel_id)
    budget = staged_execution_budget(root, str(registration["budget_scope"]))
    if budget.binding != registration["budget_binding"]:
        raise PermissionError("execution budget ancestry changed")
    if budget.summary()["unsettled_requests"]:
        raise PermissionError("an unknown paid request requires reconciliation before new dispatch")
    profiles = {
        model: model_provider_profile_from_dict(_object(profile))
        for model, profile in _object(registration["profiles"]).items()
    }
    from market_impact_agent.pi_deployment import installed_permit
    from market_impact_agent.pi_runtime import shared_admission_root

    permit = installed_permit(shared_admission_root())
    if (
        permit is None
        or permit.build_hash != canonical_hash(runtime_identity())
        or any(
            profile.route_identity not in permit.route_identities for profile in profiles.values()
        )
    ):
        raise PermissionError("current build and both model routes require accepted qualification")
    store = LocalDataSnapshotStore(root / "authority")
    windows = {str(window["window_id"]): window for window in _objects(registration["windows"])}
    # Protect all eight arms up front; qualification and other scopes cannot steal them.
    graph = _object(registration["call_graph"])
    schedule = _objects(registration["schedule"])
    members = tuple(
        ModelBudgetGroupMember(
            str(item["member_id"]),
            _integer(_object(graph[str(item["model"])])["maximum_physical_requests"]),
            _integer(_object(graph[str(item["model"])])["maximum_cost_microusd"]),
        )
        for item in schedule
    )
    # Reopen immutable source views once for this invocation, not after every model turn.
    outcomes = _registered_outcomes(registration)
    grouped = await budget.admit_group(
        group_id="stage1-panel-" + str(registration["registration_hash"]),
        members=members,
        call_graph_hash=canonical_hash({"schedule": schedule, "graph": graph}),
    )
    for item in schedule:
        window = windows[str(item["window_id"])]
        package = _object(_object(window["packages"])[str(item["arm"])])
        repository = _repository(store, str(package["repository_hash"]))
        tools = tuple(
            tool for tool in repository.tool_descriptors() if tool.name == "read_evidence"
        )
        if [tool.manifest_hash for tool in tools] != package["tool_hashes"]:
            raise PermissionError("registered native evidence tool surface changed")
        profile = profiles[str(item["model"])]
        provider = PiRuntimeProvider(
            profile, budget=grouped.for_group_member(str(item["member_id"]))
        )
        authority = ResearchThesisAuthority(
            store,
            experiment_id=str(registration["registration_hash"]),
            arm_id=str(item["model"]) + ":" + str(item["arm"]),
        )
        try:
            await authority.analyze(
                run_id=_run_id(registration, item),
                provider=provider,
                inputs=ResearchThesisRunInputs(
                    repository=repository,
                    target_id=str(window["target_id"]),
                    thesis_epoch="stage1-model-pilot-v1",
                    allowed_horizons=frozenset({5}),
                    research_question=str(registration["research_question"]),
                ),
                readonly_tools=tools,
            )
        finally:
            await provider.close()
        # A completed or failed terminal is retained; peers still finish their registered arm.
        _write_report(root, panel_id, _execution_report(root, registration, outcomes))
        if grouped.summary()["unsettled_requests"]:
            break
    report = _execution_report(root, registration, outcomes)
    _write_report(root, panel_id, report)
    return report


async def qualify_stage1_execution(
    root: Path,
    panel_id: str,
    verification_path: Path,
    *,
    prior_qualification_root: Path | None = None,
) -> dict[str, object]:
    """Reuse the bounded production route qualification under this same paid parent."""
    from market_impact_agent.dynamic_effectiveness import AnalysisTopology
    from market_impact_agent.dynamic_effectiveness_runner import (
        accept_dynamic_route_qualification,
        prepare_dynamic_route_qualification,
        run_dynamic_route_qualification,
    )

    registration = _registration(root, panel_id)
    profiles = tuple(
        model_provider_profile_from_dict(_object(value))
        for value in _object(registration["profiles"]).values()
    )
    budget = staged_execution_budget(root, "route_qualification")
    if budget.summary()["unsettled_requests"]:
        raise PermissionError(
            "an unknown paid request requires reconciliation before qualification"
        )
    qualification_root = root / "qualification"
    if prior_qualification_root is not None:
        if prior_qualification_root.resolve().parent != root.resolve():
            raise PermissionError("qualification predecessor must belong to this pilot")
        predecessor = _read_hashed(
            prior_qualification_root / "qualification-registration.json", "registration_hash"
        )
        qualification_root = root / (
            "qualification-recovery"
            if prior_qualification_root.resolve() == (root / "qualification").resolve()
            else "qualification-after-" + str(predecessor["registration_hash"])
        )
    prepare_dynamic_route_qualification(
        qualification_root,
        profiles=profiles,
        verification_path=verification_path,
        shared_budget=budget,
        route_panel=(AnalysisTopology.LUNA_MAX, AnalysisTopology.TERRA_HIGH),
        prior_qualification_root=prior_qualification_root,
    )
    report = await run_dynamic_route_qualification(qualification_root, shared_budget=budget)
    if report["stage_passed"] is True and report["reconciled"] is True:
        accept_dynamic_route_qualification(qualification_root)
    return report


def stage1_execution_report(root: Path, panel_id: str) -> dict[str, object]:
    """Reopen signed native results and score every registered opportunity."""
    registration = _registration(root, panel_id)
    return _execution_report(root, registration, _registered_outcomes(registration))


def _registered_outcomes(registration: Mapping[str, object]) -> dict[str, dict[str, object]]:
    source = ReadOnlyDataSnapshotStore(Path(str(registration["source_authority_root"])))
    input_root = Path(str(registration["source_input_root"]))
    return {
        str(window["window_id"]): _window_outcomes(window, source, input_root)
        for window in _objects(registration["windows"])
    }


def _execution_report(
    root: Path,
    registration: Mapping[str, object],
    outcomes: Mapping[str, dict[str, object]],
) -> dict[str, object]:
    store = LocalDataSnapshotStore(root / "authority")
    journal = RunJournal.authoritative(store)
    budget = staged_execution_budget(root, str(registration["budget_scope"]))
    windows = {str(window["window_id"]): window for window in _objects(registration["windows"])}
    usage_records = {
        entry.record.run_id: entry.record for entry in UsageLedger(store.index_path).records()
    }
    rows: list[dict[str, object]] = []
    for item in _objects(registration["schedule"]):
        window = windows[str(item["window_id"])]
        run_id = _run_id(registration, item)
        try:
            record = journal.get_run(run_id)
        except KeyError:
            record = None
        terminal: dict[str, object] | None = None
        if record is not None and record.status.terminal:
            binding = _object(store.artifacts.read_json(record.config_hash))
            package = _object(_object(window["packages"])[str(item["arm"])])
            expected_inputs = ResearchThesisRunInputs(
                repository=_repository(store, str(package["repository_hash"])),
                target_id=str(window["target_id"]),
                thesis_epoch="stage1-model-pilot-v1",
                allowed_horizons=frozenset({5}),
                research_question=str(registration["research_question"]),
            )
            if (
                binding["inputs"] != expected_inputs.identity_dict()
                or binding["profile"] != _object(registration["profiles"])[str(item["model"])]
                or binding["experiment_id"] != registration["registration_hash"]
                or binding["arm_id"] != str(item["model"]) + ":" + str(item["arm"])
                or binding["readonly_tool_hashes"]
                != sorted(cast(list[str], package["tool_hashes"]))
                or _object(binding["budget_owner"])["binding"] != registration["budget_binding"]
            ):
                raise PermissionError("research Run differs from its registered comparison arm")
            terminal = ResearchThesisAuthority(
                store,
                experiment_id=str(registration["registration_hash"]),
                arm_id=str(item["model"]) + ":" + str(item["arm"]),
            ).replay(run_id)
        status = (
            "not_started"
            if record is None
            else "system_failed"
            if terminal is None or terminal["status"] != "completed"
            else "completed"
        )
        thesis = _object(terminal["thesis"]) if status == "completed" and terminal else None
        direction = str(thesis["base_case_direction"]) if thesis else None
        outcome = outcomes[str(item["window_id"])]
        band = _band(_object(window["h5_band"]))
        score = score_direction_opportunity(
            prediction=direction,
            realized_return=Decimal(str(outcome["h5_return"])),
            band=band,
            status=status,
        )
        usage = usage_records.get(run_id)
        if terminal and usage is None:
            raise PermissionError("research terminal lacks its unique Usage record")
        if status == "completed":
            from market_impact_agent.dynamic_effectiveness_runner import (
                _verify_qualification_native_budget,  # pyright: ignore[reportPrivateUsage]
            )

            _verify_qualification_native_budget(
                store,
                run_id,
                budget,
                model_provider_profile_from_dict(
                    _object(_object(registration["profiles"])[str(item["model"])])
                ),
            )
        costs = _run_cost(budget, run_id)
        rows.append(
            {
                **item,
                "run_id": run_id,
                "status": status,
                "reason": None if terminal is None else terminal.get("reason"),
                "input_package_hash": _object(_object(window["packages"])[str(item["arm"])])[
                    "repository_hash"
                ],
                "terminal_hash": None if record is None else record.terminal_artifact_id,
                "journal_hash": None if record is None else journal.journal_hash(run_id),
                "thesis": thesis,
                "score": score,
                "outcome": outcome,
                "usage": None if usage is None else usage.metrics.to_dict(),
                **costs,
            }
        )
    complete_groups: list[str] = []
    for window in windows:
        group_rows = [row for row in rows if row["window_id"] == window]
        if all(row["status"] == "completed" for row in group_rows):
            complete_groups.append(window)
    models = {
        model: {
            **summarize_direction_opportunities(
                [_object(row["score"]) for row in rows if row["model"] == model]
            ),
            "known_cost_microusd": sum(
                _integer(row["known_cost_microusd"]) for row in rows if row["model"] == model
            ),
            "physical_requests": sum(
                _integer(row["physical_requests"]) for row in rows if row["model"] == model
            ),
        }
        for model in _object(registration["profiles"])
    }
    core: dict[str, object] = {
        "schema_version": _REPORT,
        "stage": 1,
        "registration_hash": registration["registration_hash"],
        "rows": rows,
        "models": models,
        "registered_arm_opportunities": len(rows),
        "complete_groups": len(complete_groups),
        "complete_group_windows": complete_groups,
        "independent_window_count": len(windows),
        "total_authorization_budget": budget.summary(),
        "panel_cost_microusd": sum(_integer(row["known_cost_microusd"]) for row in rows),
        "panel_reserved_microusd": sum(_integer(row["reserved_microusd"]) for row in rows),
        "model_selection": "pending_source_grounding_review"
        if len(complete_groups) == len(windows)
        else "incomplete_comparison",
        "investment_effectiveness_accepted": False,
        "sample_role": "opened_historical_development",
        "paper_execution": False,
        "live_execution": False,
    }
    return {**core, "report_hash": canonical_hash(core)}


def _execution_window(
    *,
    window: Mapping[str, object],
    opportunity: Mapping[str, object],
    source_store: ReadOnlyDataSnapshotStore,
    store: LocalDataSnapshotStore,
    supplement_hashes: tuple[str, ...],
) -> dict[str, object]:
    original = _object(source_store.artifacts.read_json(str(opportunity["research_artifact_hash"])))
    pack = evidence_pack_from_dict(original["evidence_pack"])
    documents = _object(original["documents"])
    packages = _object(opportunity["arm_input_packages"])
    additions = [reopen_csrc_study_source(store, ref, pack.as_of) for ref in supplement_hashes]
    if len(set(supplement_hashes)) != len(supplement_hashes) or any(
        addition.window_id != window["window_id"] for addition in additions
    ):
        raise ValueError("source supplements are duplicated or belong to another window")
    result: dict[str, object] = {}
    for arm in _ARMS:
        reference_packages = _objects(_object(packages[arm])["reference_packages"])
        allowed_ids = {
            str(_object(item["reference"])["evidence_id"]) for item in reference_packages
        }
        references = tuple(ref for ref in pack.evidence if ref.evidence_id in allowed_ids)
        selected_documents = {ref.evidence_id: documents[ref.evidence_id] for ref in references}
        if arm == "price_and_event":
            references += tuple(addition.reference for addition in additions)
            selected_documents.update(
                {addition.reference.evidence_id: addition.document for addition in additions}
            )
            if not additions and not any(
                _has_event_text(document) for document in selected_documents.values()
            ):
                raise ValueError("event arm requires source-backed event text, not only metadata")
        gaps = (
            "Historical development evidence uses modeled PIT, not original historical receipt; "
            "later remembered outcomes must not inform this forecast.",
            "This fixed-target study establishes neither trading eligibility nor account action.",
        ) + (
            ("No event evidence is supplied in this price-only arm.",)
            if arm == "price_only"
            else ()
        )
        frozen_pack = EvidencePack.build(
            event_id="stage1-event-"
            + canonical_hash(
                {"cutoff": pack.as_of.isoformat(), "target": opportunity["target_id"]}
            ),
            as_of=pack.as_of,
            research_question=_QUESTION,
            evidence=references,
            pattern_packs=(),
            allowed_targets=(str(opportunity["target_id"]),),
            data_gaps=gaps,
        )
        payload = {"evidence_pack": frozen_pack.to_dict(), "documents": selected_documents}
        repository_hash = store.artifacts.put_json(payload).content_hash
        repository = _repository(store, repository_hash)
        result[arm] = {
            "repository_hash": repository_hash,
            "reference_hashes": [reference.content_hash for reference in references],
            "tool_hashes": [
                tool.manifest_hash
                for tool in repository.tool_descriptors()
                if tool.name == "read_evidence"
            ],
        }
    h5_band = next(band for band in _objects(opportunity["bands"]) if band["horizon_sessions"] == 5)
    return {
        "window_id": window["window_id"],
        "target_id": opportunity["target_id"],
        "cutoff": opportunity["cutoff"],
        "sessions": window["registered_sessions"],
        "manifest_hash": window["manifest_hash"],
        "original_opportunity_id": opportunity["opportunity_id"],
        "h5_band": h5_band,
        "supplement_hashes": list(supplement_hashes),
        "packages": result,
    }


def _window_outcomes(
    window: Mapping[str, object], source: ReadOnlyDataSnapshotStore, input_root: Path
) -> dict[str, object]:
    manifest = _object(json.loads((input_root / (str(window["window_id"]) + ".json")).read_text()))
    if canonical_hash(manifest) != window["manifest_hash"]:
        raise PermissionError("outcome source manifest changed after registration")
    market = _market(source, manifest, _objects(manifest["records"])[0])
    sessions = [date.fromisoformat(str(value)) for value in cast(list[str], window["sessions"])]
    cutoff = datetime.fromisoformat(str(window["cutoff"]))
    target = str(window["target_id"])
    projection = market.research_series(target, cutoff, limit=21)
    rows = _objects(projection["rows"])
    prior = Decimal(str(rows[-1]["cutoff_adjusted_close"]))
    last_date = date.fromisoformat(str(rows[-1]["trade_date"]))
    # A single end-of-window adjustment basis avoids corporate-action inconsistencies.
    end_cutoff = datetime.combine(sessions[-1], datetime.max.time(), tzinfo=cutoff.tzinfo)
    full = market.research_series(target, end_cutoff, limit=80)
    all_rows = {str(row["trade_date"]): row for row in _objects(full["rows"])}
    prior_common = Decimal(str(all_rows[last_date.isoformat()]["cutoff_adjusted_close"]))
    first = Decimal(str(all_rows[sessions[0].isoformat()]["cutoff_adjusted_close"]))
    last = Decimal(str(all_rows[sessions[-1].isoformat()]["cutoff_adjusted_close"]))
    momentum = prior / Decimal(str(rows[-6]["cutoff_adjusted_close"])) - 1
    return {
        "h5_return": str(last / prior_common - 1),
        "h1_return": str(first / prior_common - 1),
        "five_session_momentum": str(momentum),
        "momentum_direction": "up" if momentum > 0 else "down" if momentum < 0 else "rangebound",
        "source_projection_hash": canonical_hash(full),
        "basis": "same-adjustment-basis close-to-close; not executable strategy return",
    }


def _run_cost(budget: ModelBudget, run_id: str) -> dict[str, int]:
    reserved: dict[str, int] = {}
    settled: dict[str, int] = {}
    for event in budget.journal.events(budget.owner_run_id):
        key = event.payload.get("request_key")
        if not isinstance(key, str) or not key.startswith(run_id + ".pi-invocation."):
            continue
        if event.event_type == "pi.budget.reserved":
            reserved[key] = cast(int, event.payload["reserved_microusd"])
        elif event.event_type == "pi.budget.settled":
            settled[key] = cast(int, event.payload["estimated_cost_microusd"])
    if not settled.keys() <= reserved.keys():
        raise PermissionError("research charge lacks its parent reservation")
    return {
        "physical_requests": len(reserved),
        "known_cost_microusd": sum(settled.values()),
        "reserved_microusd": sum(value for key, value in reserved.items() if key not in settled),
        "unsettled_requests": len(reserved.keys() - settled.keys()),
    }


def _run_ceiling(profile: ModelProviderProfile) -> dict[str, int]:
    requests = profile.budget.max_turns * profile.max_attempts
    charge = profile.pricing.estimate_microusd(
        ProviderUsage(
            min(
                profile.budget.max_input_tokens,
                profile.context_window_tokens - profile.reserved_output_tokens,
            ),
            min(profile.budget.max_output_tokens, profile.reserved_output_tokens),
        )
    )
    return {"maximum_physical_requests": requests, "maximum_cost_microusd": requests * charge}


def _schedule(windows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    # Counterbalance serial provider order; every model sees exactly the same input.
    for index, window in enumerate(windows):
        for arm_index, arm in enumerate(_ARMS):
            models = ("gpt-5.6-luna", "gpt-5.6-terra")
            if (index + arm_index) % 2:
                models = tuple(reversed(models))
            for model in models:
                identity = {"window_id": window["window_id"], "model": model, "arm": arm}
                result.append({**identity, "member_id": canonical_hash(identity)})
    return result


def _repository(store: LocalDataSnapshotStore, artifact_hash: str) -> FrozenResearchRepository:
    payload = _object(store.artifacts.read_json(artifact_hash))
    return FrozenResearchRepository(
        evidence_pack=evidence_pack_from_dict(payload["evidence_pack"]),
        evidence_documents=_object(payload["documents"]),
        pattern_packs={},
    )


def _has_event_text(document: object) -> bool:
    if not isinstance(document, dict):
        return False
    value = cast(dict[str, object], document)
    return any(
        isinstance(row, dict)
        and bool(
            cast(dict[str, object], row).get("article_excerpt")
            or cast(dict[str, object], row).get("transcript_excerpt")
        )
        for row in cast(list[object], value.get("records", []))
        + cast(list[object], value.get("articles", []))
    )


def _band(value: Mapping[str, object]) -> FrozenDirectionBand:
    band = FrozenDirectionBand(
        str(value["target_id"]),
        datetime.fromisoformat(str(value["cutoff"])),
        _integer(value["horizon_sessions"]),
        str(value["source_projection_hash"]),
        tuple(date.fromisoformat(str(day)) for day in cast(list[str], value["close_dates"])),
        Decimal(str(value["daily_return_standard_deviation"])),
        Decimal(str(value["half_width"])),
    )
    if band.to_dict() != value:
        raise PermissionError("registered direction band changed")
    return band


def _execution_code() -> dict[str, str]:
    return {
        name: sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in (
            "staged_research_execution.py",
            "staged_research_sources.py",
            "staged_research_study.py",
            "dynamic_effectiveness_runner.py",
            "research_thesis_runtime.py",
            "decision_thesis.py",
            "frozen_research.py",
        )
    }


def _validate_authorization(value: Mapping[str, object]) -> None:
    if (
        value.get("schema_version") != _AUTHORIZATION
        or value.get("maximum_cost_microusd") != 140_000_000
        or (
            value.get("maximum_physical_requests") != 512
            or value.get("scopes") != [scope.to_dict() for scope in _SCOPES]
            or value.get("prior_cost_microusd") != 0
            or value.get("prior_requests") != 0
            or value.get("paper_execution") is not False
            or value.get("live_execution") is not False
        )
    ):
        raise PermissionError("authorization differs from the explicit new USD140 scope")


def _owner(_authorization: Mapping[str, object]) -> str:
    # Metadata revisions must never mint a fresh budget inside this authority.
    return "staged-paid-20260906-usd140"


def _panel_root(root: Path, panel_id: str) -> Path:
    if not panel_id or any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in panel_id
    ):
        raise ValueError("panel identity must be a simple local name")
    return root.resolve() / "panels" / panel_id


def _registration(root: Path, panel_id: str) -> dict[str, object]:
    registration = _read_hashed(
        _panel_root(root, panel_id) / "registration.json", "registration_hash"
    )
    if registration.get("schema_version") != _REGISTRATION:
        raise ValueError("unsupported execution registration")
    if registration["runtime"] != runtime_identity() or (
        registration["execution_code"] != _execution_code()
        or registration["research_prompt_hash"] != canonical_hash(RESEARCH_THESIS_V2_PROMPT)
    ):
        raise PermissionError(
            "execution build changed; use the frozen build or register a new panel"
        )
    authorization = _read_hashed(root / "authorization.json", "authorization_hash")
    if registration["authorization_hash"] != authorization["authorization_hash"]:
        raise PermissionError("research panel belongs to another paid authorization")
    store = LocalDataSnapshotStore(root / "authority")
    if store.artifacts.read_json(canonical_hash(registration)) != registration:
        raise PermissionError("research panel differs from its content-addressed registration")
    return registration


def _run_id(registration: Mapping[str, object], item: Mapping[str, object]) -> str:
    return "stage1-" + str(registration["registration_hash"]) + "." + str(item["member_id"])


def _write_report(root: Path, panel_id: str, report: Mapping[str, object]) -> None:
    # Each progress state is immutable; the friendly report is a replaceable projection.
    store = LocalDataSnapshotStore(root / "authority")
    store.artifacts.put_json(dict(report))
    path = _panel_root(root, panel_id) / "report.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def _read_hashed(path: Path, field: str) -> dict[str, object]:
    value = _object(json.loads(path.read_text()))
    if canonical_hash({key: item for key, item in value.items() if key != field}) != value.get(
        field
    ):
        raise PermissionError("immutable artifact hash changed: " + field)
    return value


def _write_new(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise PermissionError("immutable execution file already has different content")
        return
    with path.open("x") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    path.chmod(0o600)


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected a JSON object")
    return cast(dict[str, object], value)


def _objects(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise TypeError("expected a JSON object list")
    return [_object(item) for item in cast(list[object], value)]


def _integer(value: object) -> int:
    if type(value) is not int:
        raise TypeError("expected a JSON integer")
    return value
