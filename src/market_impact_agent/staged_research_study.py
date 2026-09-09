"""Offline preparation for paired staged research studies.

The study specification owns the denominator. Source reopening may prove an
opportunity ready or attach a gap, but it can never remove a registered session.
Preparation opens the existing Harness authority read-only and does not construct
a Provider, ModelBudget, broker, or trading account.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash, evidence_pack_from_dict
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.offline_authority import (
    ReadOnlyDataSnapshotStore,
    open_read_only_runtime_authority,
)
from market_impact_agent.portfolio_review import PORTFOLIO_REVIEW_PROMPT_V3
from market_impact_agent.research_thesis_runtime import RESEARCH_THESIS_V2_PROMPT

_SPEC_VERSION = "market-impact.staged-research-study.v1"
_PREPARATION_VERSION = "market-impact.staged-research-preparation.v1"
_REPORT_VERSION = "market-impact.staged-research-report.v1"
_BAND_VERSION = "market-impact.frozen-direction-band.v1"
_INPUT_PACKAGE_VERSION = "market-impact.staged-research-input-package.v1"
_ACCOUNT_BINDING_VERSION = "market-impact.staged-initial-account-binding.v1"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PROFILE_ID = "model-provider-7d3c04afa0b04a1a6466da7918d585c7e7df630857016b99504b35a973d34034"
_RESEARCH_CONTRACT = "market-impact.research-thesis-inputs.v2"
_PORTFOLIO_CONTRACT = "market-impact.portfolio-review-binding.v6"
_CONTINUOUS_PROTOCOL = "research-continuous-v2"
_STAGES = {
    1: ("event_evidence", ("price_only", "price_and_event")),
    2: ("target_selection", ("fixed_seed", "dynamic_candidates")),
    3: ("continuous_review", ("expiry_only", "scheduled", "event")),
}
_DEFAULT_INITIAL_CASH = Decimal("100000")


class _WindowSourceMissing(FileNotFoundError):
    def __init__(self, scope: str) -> None:
        super().__init__(scope)
        self.scope = scope


class _WindowReadinessGap(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class FrozenDirectionBand:
    target_id: str
    cutoff: datetime
    horizon_sessions: int
    source_projection_hash: str
    close_dates: tuple[date, ...]
    daily_return_standard_deviation: Decimal
    half_width: Decimal

    def to_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "schema_version": _BAND_VERSION,
            "target_id": self.target_id,
            "cutoff": self.cutoff.isoformat(),
            "horizon_sessions": self.horizon_sessions,
            "source_projection_hash": self.source_projection_hash,
            "close_dates": [day.isoformat() for day in self.close_dates],
            "daily_return_standard_deviation": str(self.daily_return_standard_deviation),
            "half_width": str(self.half_width),
            "formula": "0.5 * sample_std(last_20_completed_daily_returns) * sqrt(H)",
            "rangebound_boundary": "inclusive",
            "price_basis": "cutoff_adjusted_close",
        }
        return {**core, "band_id": "direction-band-" + canonical_hash(core)}


def freeze_direction_band(
    projection: Mapping[str, object],
    *,
    cutoff: datetime,
    horizon_sessions: int,
    calendar_sessions: Sequence[date],
) -> FrozenDirectionBand:
    """Freeze +/-0.5 sample standard deviations from 20 pre-cutoff returns."""
    if cutoff.tzinfo is None or horizon_sessions not in {1, 3, 5, 10, 20, 60}:
        raise ValueError("direction band requires an aware cutoff and registered horizon")
    if datetime.fromisoformat(_string(projection, "cutoff")) != cutoff:
        raise ValueError("direction band projection belongs to another cutoff")
    if projection.get("gaps"):
        raise ValueError("direction band requires complete price and adjustment evidence")
    rows = _objects(projection.get("rows"))
    dates = [date.fromisoformat(_string(row, "trade_date")) for row in rows]
    if dates != sorted(set(dates)) or len(dates) < 21:
        raise ValueError("direction band requires 21 ordered distinct closes")
    if any(datetime.combine(day, time(15), _SHANGHAI) >= cutoff for day in dates):
        raise ValueError("direction band cannot consume future or incomplete session prices")
    past_calendar = [
        day for day in calendar_sessions if datetime.combine(day, time(15), _SHANGHAI) < cutoff
    ]
    if (
        list(calendar_sessions) != sorted(set(calendar_sessions))
        or dates[-21:] != past_calendar[-21:]
    ):
        raise ValueError("direction band history is not consecutive on the source calendar")
    selected = rows[-21:]
    closes = [Decimal(_string(row, "cutoff_adjusted_close")) for row in selected]
    if any(not value.is_finite() or value <= 0 for value in closes):
        raise ValueError("direction band requires positive finite adjusted closes")
    if any(
        not row.get("source_record_hash") or not row.get("factor_record_hash") for row in selected
    ):
        raise ValueError("direction band lacks exact price/factor source provenance")
    with localcontext() as context:
        context.prec = 28
        returns = [current / prior - 1 for prior, current in pairwise(closes)]
        mean = sum(returns, Decimal(0)) / Decimal(20)
        variance = sum(((value - mean) ** 2 for value in returns), Decimal(0)) / Decimal(19)
        standard_deviation = variance.sqrt()
        half_width = Decimal("0.5") * standard_deviation * Decimal(horizon_sessions).sqrt()
    return FrozenDirectionBand(
        _string(projection, "symbol"),
        cutoff,
        horizon_sessions,
        canonical_hash(dict(projection)),
        tuple(dates[-21:]),
        standard_deviation,
        half_width,
    )


def score_direction_opportunity(
    *,
    prediction: str | None,
    realized_return: Decimal | None,
    band: FrozenDirectionBand,
    status: str = "completed",
) -> dict[str, object]:
    """Keep every registered status in coverage while scoring only concrete directions."""
    allowed_statuses = {
        "completed",
        "not_started",
        "source_insufficient",
        "system_failed",
        "budget_stopped",
    }
    allowed_predictions = {None, "up", "down", "rangebound", "unknown"}
    if status not in allowed_statuses or prediction not in allowed_predictions:
        raise ValueError("unregistered direction outcome")
    if status != "completed" and prediction is not None:
        raise ValueError("incomplete direction opportunity cannot carry a prediction")
    if status == "completed" and prediction is None:
        raise ValueError("completed direction opportunity requires a prediction")
    if realized_return is not None and not realized_return.is_finite():
        raise ValueError("direction outcome must be finite")
    label = (
        None
        if realized_return is None
        else (
            "rangebound"
            if abs(realized_return) <= band.half_width
            else "up"
            if realized_return > 0
            else "down"
        )
    )
    response_covered = status == "completed" and prediction is not None
    direction_covered = status == "completed" and prediction in {
        "up",
        "down",
        "rangebound",
    }
    scored = direction_covered and label is not None
    return {
        "status": status,
        "prediction": prediction,
        "realized_return": None if realized_return is None else str(realized_return),
        "realized_direction": label,
        "band_id": band.to_dict()["band_id"],
        "response_covered": response_covered,
        "direction_covered": direction_covered,
        "scored": scored,
        "hit": prediction == label if scored else None,
        "unknown_direction": response_covered and prediction == "unknown",
    }


def summarize_direction_opportunities(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    denominator = len(rows)
    responses = sum(row.get("response_covered") is True for row in rows)
    covered = sum(row.get("direction_covered") is True for row in rows)
    scored = sum(row.get("scored") is True for row in rows)
    hits = sum(row.get("hit") is True for row in rows)
    return {
        "registered_opportunities": denominator,
        "completed_responses": responses,
        "response_completion_rate": (
            None if not denominator else str(Decimal(responses) / denominator)
        ),
        "direction_covered": covered,
        "direction_coverage": None if not denominator else str(Decimal(covered) / denominator),
        "direction_scored": scored,
        "direction_hits": hits,
        "conditional_hit_rate": None if not scored else str(Decimal(hits) / scored),
        "unknown_direction": sum(row.get("unknown_direction") is True for row in rows),
        "status_counts": dict(sorted(Counter(str(row.get("status")) for row in rows).items())),
        "coverage_denominator_policy": (
            "all_registered_opportunities_including_source_system_and_budget_failures"
        ),
        "independent_samples": "distinct_windows_only_not_repeated_dates",
        "investment_effectiveness_accepted": False,
    }


def staged_call_graph(spec: Mapping[str, object], profile_path: Path) -> dict[str, object]:
    """Bound all possible role Runs without creating a budget authorization."""
    stage = _stage(spec)
    profile = load_model_provider_profile(profile_path)
    if (
        profile.profile_id != _PROFILE_ID
        or profile.model != "gpt-5.6-terra"
        or profile.reasoning_effort != "high"
    ):
        raise ValueError("preparation must retain the frozen Terra high profile")
    budget = profile.budget
    physical_input_cap = min(
        budget.max_input_tokens,
        profile.context_window_tokens - profile.reserved_output_tokens,
    )
    physical_output_cap = min(budget.max_output_tokens, profile.reserved_output_tokens)
    from market_impact_agent.agent_runtime import ProviderUsage

    physical_request_token_cap_microusd = profile.pricing.estimate_microusd(
        ProviderUsage(physical_input_cap, physical_output_cap)
    )
    physical_requests_per_run = budget.max_turns * profile.max_attempts
    token_envelope_per_run = physical_requests_per_run * physical_request_token_cap_microusd
    enforced_run_ceiling = budget.max_estimated_cost_microusd or token_envelope_per_run
    from market_impact_agent.pi_deployment import installed_permit
    from market_impact_agent.pi_runtime import runtime_identity, shared_admission_root

    runtime = runtime_identity()
    permit = installed_permit(shared_admission_root())
    runtime_route_accepted = bool(
        permit is not None
        and permit.build_hash == canonical_hash(runtime)
        and profile.route_identity in permit.route_identities
    )

    research_runs_per_review = 3
    portfolio_runs_per_review = 0 if stage == 1 else 3
    opportunities_per_window = 1 if stage in {1, 2} else 10
    reviews_per_arm = opportunities_per_window * 2
    runs_per_arm = reviews_per_arm * (research_runs_per_review + portfolio_runs_per_review)
    members = [
        {
            "member_id": arm,
            "registered_review_opportunities": reviews_per_arm,
            "maximum_role_runs": runs_per_arm,
            "maximum_physical_requests": runs_per_arm * physical_requests_per_run,
            "maximum_cost_microusd": runs_per_arm * token_envelope_per_run,
            "successful_usage_cap_diagnostic_microusd": (runs_per_arm * enforced_run_ceiling),
        }
        for arm in _STAGES[stage][1]
    ]
    surface = {
        "profile_id": profile.profile_id,
        "profile_hash": profile.profile_hash,
        "profile_document_hash": canonical_hash(profile.to_dict()),
        "profile_runtime": profile.runtime,
        "runtime_identity": runtime,
        "runtime_identity_hash": canonical_hash(runtime),
        "installed_runtime_permit_build_hash": None if permit is None else permit.build_hash,
        "runtime_route_accepted": runtime_route_accepted,
        "runtime_route_status": (
            "accepted_current_build"
            if runtime_route_accepted
            else "route_qualification_pending_current_build"
        ),
        "research_prompt_hash": canonical_hash(RESEARCH_THESIS_V2_PROMPT),
        "portfolio_prompt_hash": canonical_hash(PORTFOLIO_REVIEW_PROMPT_V3),
        "tool_surface_status": "opportunity_scoped_descriptors_require_execution_registration",
    }
    core: dict[str, object] = {
        "runtime_surface": {**surface, "surface_hash": canonical_hash(surface)},
        "profile_limits": budget.to_dict(),
        "research_runs_per_review": research_runs_per_review,
        "maximum_acquisition_successors_per_review": 2,
        "portfolio_runs_per_review": portfolio_runs_per_review,
        "maximum_portfolio_projection_recoveries_per_review": 0 if stage == 1 else 1,
        "maximum_rotation_destination_reviews_per_review": 0 if stage == 1 else 1,
        "physical_requests_per_run": physical_requests_per_run,
        "physical_input_token_cap": physical_input_cap,
        "physical_output_token_cap": physical_output_cap,
        "physical_request_token_cap_microusd": physical_request_token_cap_microusd,
        "token_envelope_per_run_microusd": token_envelope_per_run,
        "successful_model_usage_cap_per_run_microusd": enforced_run_ceiling,
        "successful_usage_cap_excludes_unsettled_attempt_reservations": True,
        "members": members,
        "group_maximum_role_runs": sum(
            cast(int, member["maximum_role_runs"]) for member in members
        ),
        "group_maximum_physical_requests": sum(
            cast(int, member["maximum_physical_requests"]) for member in members
        ),
        "group_max_cost_microusd": sum(
            cast(int, member["maximum_cost_microusd"]) for member in members
        ),
        "group_successful_usage_cap_diagnostic_microusd": sum(
            cast(int, member["successful_usage_cap_diagnostic_microusd"]) for member in members
        ),
        "funding_unit": "complete_all_arm_group_for_one_registered_window",
        "budget_authorization_created": False,
        "historical_reference": {
            "source": "CONTINUOUS_STUDY_RETROSPECTIVE_20260906.md",
            "terra_rolling_known_microusd": 4_968_002,
            "terra_rolling_requests": 165,
            "median_terra_paid_run_microusd": 25_785,
            "comparable_complete_group_estimate": False,
        },
    }
    return {**core, "call_graph_hash": canonical_hash(core)}


def load_staged_study_spec(path: Path) -> dict[str, object]:
    spec = _object(json.loads(path.read_text()))
    if spec.get("schema_version") != _SPEC_VERSION:
        raise ValueError("unsupported staged study specification")
    stage = _stage(spec)
    _string(spec, "study_name")
    if spec.get("profile_id") != _PROFILE_ID:
        raise ValueError("staged study requires frozen Terra profile")
    if (
        spec.get("research_contract") != _RESEARCH_CONTRACT
        or spec.get("portfolio_contract") != _PORTFOLIO_CONTRACT
        or spec.get("continuous_protocol") != _CONTINUOUS_PROTOCOL
    ):
        raise ValueError("staged study contracts differ from the accepted runtime")
    _string(spec, "research_question")
    fixed = _strings(spec.get("fixed_seed_symbols"))
    if len(fixed) != len(set(fixed)) or not fixed:
        raise ValueError("fixed seed symbols must be unique")
    windows = _objects(spec.get("windows"))
    if len(windows) != 2 or len({_string(window, "window_id") for window in windows}) != 2:
        raise ValueError("each stage prepares exactly two distinct windows")
    # Reversal labels describe the original development selection, not an
    # authority invariant. Source-first follow-ups must not inspect outcomes just
    # to satisfy an outcome-dependent sample requirement.
    _string(spec, "selection_policy")
    expected_count = 10 if stage == 3 else 5
    for window in windows:
        if window.get("sample_role") != "development":
            raise ValueError("inspected candidate windows must remain development data")
        sessions = [date.fromisoformat(value) for value in _strings(window.get("sessions"))]
        start = date.fromisoformat(_string(window, "decision_session"))
        end = date.fromisoformat(_string(window, "observation_end"))
        if (
            len(sessions) != expected_count
            or sessions != sorted(set(sessions))
            or sessions[0] != start
            or sessions[-1] != end
            or window.get("session_count") != expected_count
            or not _strings(window.get("source_refs"))
        ):
            raise ValueError("window must freeze its exact ordered session list")
        target = _string(window, "target_id")
        if target not in fixed:
            raise ValueError("window target must be one of the fixed seed symbols")
        if stage == 2 and window.get("business_exposure_proof_required") is not True:
            raise ValueError("target-selection stage requires historical exposure proof")
    if spec.get("candidate_limit") != 5:
        raise ValueError("new candidate upper bound must be five, separate from holdings")
    if spec.get("primary_evaluation_horizon") != (None if stage == 3 else 5):
        raise ValueError("research stages freeze five-session primary evaluation")
    if spec.get("auxiliary_evaluation_horizon") != (None if stage == 3 else 1):
        raise ValueError("research stages freeze one-session auxiliary evaluation")
    if stage == 3:
        seed = _object(spec.get("initial_account_policy"))
        if seed != {
            "engine": "historical-streaming-account.v1",
            "initial_cash": "100000",
            "seed_symbol": "510300.SH",
            "allocation_fraction": "0.5",
        }:
            raise ValueError("continuous stage requires the accepted initial account policy")
    if spec.get("predecessor_result_hash") is not None:
        _sha256_text(_string(spec, "predecessor_result_hash"), "predecessor result")
    return spec


def prepare_staged_study(
    *,
    spec_path: Path,
    output_root: Path,
    authority_root: Path,
    input_root: Path,
    profile_path: Path,
) -> dict[str, object]:
    spec = load_staged_study_spec(spec_path)
    store = ReadOnlyDataSnapshotStore(authority_root)
    stage = _stage(spec)
    windows: list[dict[str, object]] = []
    for window in _objects(spec["windows"]):
        try:
            prepared_window = _prepare_window(
                window, spec=spec, stage=stage, input_root=input_root, store=store
            )
        except FileNotFoundError as exc:
            scope = exc.scope if isinstance(exc, _WindowSourceMissing) else "market_source_cas"
            prepared_window = _unavailable_source_window(
                window,
                spec=spec,
                stage=stage,
                gap="window_source_artifact_missing:" + scope,
            )
        windows.append(prepared_window)
    gaps = _predecessor_gaps(spec, store)
    for window in windows:
        gaps.extend(_strings(window["gaps"]))
        for opportunity in _objects(window["opportunities"]):
            gaps.extend(_strings(opportunity["gaps"]))
    gaps = sorted(set(gaps))
    call_graph = staged_call_graph(spec, profile_path)
    runtime_surface = _object(call_graph["runtime_surface"])
    execution_gaps = ["separate_paid_authorization_required"]
    if runtime_surface["runtime_route_accepted"] is not True:
        execution_gaps.append("runtime_route_qualification_pending_current_build")
    core: dict[str, object] = {
        "schema_version": _PREPARATION_VERSION,
        "spec": spec,
        "spec_hash": canonical_hash(spec),
        "stage": stage,
        "stage_name": _STAGES[stage][0],
        "arms": list(_STAGES[stage][1]),
        "harness_authority_id": store.harness_authority_id,
        "windows": windows,
        "registered_opportunities": sum(
            len(_objects(window["opportunities"])) for window in windows
        ),
        "call_graph": call_graph,
        "preparation_complete": True,
        "source_ready": not gaps,
        "ready": False,
        "gaps": gaps,
        "execution_gaps": execution_gaps,
        "provider_dispatch_permitted": False,
        "broker_access": False,
        "paid_authorization_created": False,
        "account_mutations": 0,
        "execution_configuration_status": (
            "prepared_pending_separate_paid_authorization"
            if stage == 1
            else "pending_predecessor_result_and_separate_paid_authorization"
        ),
        "investment_effectiveness_accepted": False,
    }
    preparation = {**core, "preparation_hash": canonical_hash(core)}
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    output_root.chmod(0o700)
    _write_immutable(output_root / "preparation.json", preparation)
    _write_immutable(output_root / "report-template.json", staged_report(preparation))
    return preparation


def preflight_staged_study(
    *,
    spec_path: Path,
    output_root: Path,
    authority_root: Path,
    input_root: Path,
    profile_path: Path,
) -> dict[str, object]:
    """Reopen exact inputs and compare with the immutable preparation."""
    frozen = _read_preparation(output_root)
    current = prepare_staged_study(
        spec_path=spec_path,
        output_root=output_root,
        authority_root=authority_root,
        input_root=input_root,
        profile_path=profile_path,
    )
    if current != frozen:
        raise ValueError("source or specification changed after preparation")
    return {"status": "ready" if current["ready"] else "not_ready", **current}


def report_staged_study(output_root: Path, *, spec_path: Path | None = None) -> dict[str, object]:
    preparation = _read_preparation(output_root)
    if spec_path is not None and preparation["spec_hash"] != canonical_hash(
        load_staged_study_spec(spec_path)
    ):
        raise ValueError("report specification differs from frozen preparation")
    report = staged_report(preparation)
    frozen = _object(json.loads((output_root / "report-template.json").read_text()))
    if report != frozen:
        raise ValueError("staged report template changed after preparation")
    return report


def staged_report(preparation: Mapping[str, object]) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    arms = _strings(preparation["arms"])
    for window in _objects(preparation["windows"]):
        for opportunity in _objects(window["opportunities"]):
            packages = _object(opportunity["arm_input_packages"])
            for arm in arms:
                package = _object(packages[arm])
                rows.append(
                    {
                        "window_id": window["window_id"],
                        "opportunity_id": opportunity["opportunity_id"],
                        "cutoff": opportunity["cutoff"],
                        "target_id": opportunity["target_id"],
                        "arm": arm,
                        "input_package_hash": package["input_package_hash"],
                        "status": "not_started",
                        "research_status": "not_started",
                        "account_review_status": "not_started",
                        "execution_readiness": "not_assessed",
                        "research_run_id": None,
                        "portfolio_run_id": None,
                        "signed_thesis_ref": None,
                        "signed_position_ref": None,
                        "direction": None,
                        "direction_hit": None,
                        "transmission_evidence": [],
                        "target_selection_reason": None,
                        "net_return": None,
                        "maximum_drawdown": None,
                        "cash_exposure": None,
                        "turnover": None,
                        "fees": None,
                        "unfilled": None,
                        "known_cost_microusd": 0,
                        "unknown_reservation_microusd": 0,
                        "complete_group_difference": None,
                    }
                )
    return {
        "schema_version": _REPORT_VERSION,
        "preparation_hash": preparation["preparation_hash"],
        "stage": preparation["stage"],
        "registered_opportunities": preparation["registered_opportunities"],
        "registered_arm_opportunities": len(rows),
        "complete_groups": 0,
        "rows": rows,
        "direction_coverage": "all_registered_opportunities_including_failures",
        "comparison_status": "no_complete_group",
        "performance_difference": None,
        "independent_window_count": len(_objects(preparation["windows"])),
        "investment_effectiveness_accepted": False,
    }


def _prepare_window(
    window: Mapping[str, object],
    *,
    spec: Mapping[str, object],
    stage: int,
    input_root: Path,
    store: ReadOnlyDataSnapshotStore,
) -> dict[str, object]:
    window_id = _string(window, "window_id")
    target = _string(window, "target_id")
    sessions = [date.fromisoformat(value) for value in _strings(window["sessions"])]
    review_days = sessions if stage == 3 else sessions[:1]
    result: dict[str, object] = {
        "window_id": window_id,
        "sample_role": "development",
        "registered_sessions": [day.isoformat() for day in sessions],
        "source_refs": window["source_refs"],
    }
    path = input_root / f"{window_id}.json"
    if not path.is_file() or path.is_symlink():
        return _unproven_window(
            result,
            spec=spec,
            stage=stage,
            target=target,
            review_days=review_days,
            gap="frozen_source_manifest_missing",
        )
    manifest = _object(json.loads(path.read_text()))
    if manifest.get("window_id") != window_id:
        raise ValueError("source manifest belongs to another window")
    records = _objects(manifest.get("records"))
    if not records:
        return _unproven_window(
            result,
            spec=spec,
            stage=stage,
            target=target,
            review_days=review_days,
            gap="frozen_source_records_missing",
            manifest=manifest,
        )
    by_day = {_string(record, "trade_date"): record for record in records}
    if len(by_day) != len(records):
        raise ValueError("duplicate source cutoff records are not independent opportunities")
    source = records[0]
    market = _market(store, manifest, source)
    window_gaps: list[str] = []
    calendar = market.calendar_window(
        "SSE" if target.endswith(".SH") else "SZSE", sessions[0], sessions[-1]
    )
    source_sessions = _strings(calendar["sessions"])
    if calendar["missing_dates"] or source_sessions != [day.isoformat() for day in sessions]:
        window_gaps.append("registered_sessions_not_exactly_source_confirmed")
    account_binding: dict[str, object] | None = None
    if stage == 3:
        try:
            account_binding = _initial_account_binding(
                input_root=input_root,
                market=market,
                manifest=manifest,
                first_session=sessions[0],
                target=target,
                store=store,
            )
        except _WindowReadinessGap as exc:
            window_gaps.append("initial_account_binding_not_ready:" + str(exc))
    opportunities = [
        _prepare_opportunity(
            spec=spec,
            window_id=window_id,
            day=day,
            sessions=sessions,
            target=target,
            stage=stage,
            manifest=manifest,
            source=source,
            row=by_day.get(day.isoformat()),
            market=market,
            store=store,
            account_binding=account_binding,
        )
        for day in review_days
    ]
    return {
        **result,
        "manifest_hash": canonical_hash(manifest),
        "calendar": calendar,
        "source_policy": manifest["source_policy"],
        "market_snapshot_ids": source["market_snapshot_ids"],
        "rule_artifact_hashes": source["rule_artifact_hashes"],
        **({"initial_account_binding": account_binding} if account_binding else {}),
        "opportunities": opportunities,
        "ready": not window_gaps and all(not item["gaps"] for item in opportunities),
        "gaps": sorted(set(window_gaps)),
    }


def _prepare_opportunity(
    *,
    spec: Mapping[str, object],
    window_id: str,
    day: date,
    sessions: Sequence[date],
    target: str,
    stage: int,
    manifest: Mapping[str, object],
    source: Mapping[str, object],
    row: Mapping[str, object] | None,
    market: HistoricalAShareInputs,
    store: ReadOnlyDataSnapshotStore,
    account_binding: Mapping[str, object] | None,
) -> dict[str, object]:
    cutoff = datetime.combine(day, time(9, 25), _SHANGHAI).astimezone(UTC)
    gaps: list[str] = []
    pack_hash: str | None = None
    reference_packages: list[dict[str, object]] = []
    event_metadata: list[dict[str, object]] = []
    if row is None:
        gaps.append("cutoff_research_input_missing")
    else:
        if datetime.fromisoformat(_string(row, "cutoff")) != cutoff:
            raise ValueError("research record differs from registered preopen cutoff")
        if (
            row["market_snapshot_ids"] != source["market_snapshot_ids"]
            or row["rule_artifact_hashes"] != source["rule_artifact_hashes"]
        ):
            raise ValueError("window source versions differ; freeze a coherent input manifest")
        pack_hash = _string(row, "research_artifact_hash")
        try:
            artifact = _object(store.artifacts.read_json(pack_hash))
        except FileNotFoundError as exc:
            raise _WindowSourceMissing("research_artifact") from exc
        else:
            pack = evidence_pack_from_dict(artifact["evidence_pack"])
            if pack.as_of != cutoff or target not in pack.allowed_targets:
                raise ValueError("research pack differs from registered cutoff or target")
            documents = _object(artifact["documents"])
            for reference in pack.evidence:
                if reference.evidence_id not in documents:
                    raise ValueError("research source document is missing")
                document = documents[reference.evidence_id]
                if (
                    canonical_hash(document) != reference.content_hash
                    or reference.available_at > cutoff
                ):
                    raise ValueError("research source content or PIT binding changed")
                source_hash = _source_artifact_hash(reference.source_ref)
                source_available = True
                if source_hash is not None:
                    try:
                        store.artifacts.read_bytes(source_hash)
                    except FileNotFoundError as exc:
                        raise _WindowSourceMissing("reference_source_artifact") from exc
                metadata = (
                    _event_metadata(reference.source_ref, document, cutoff)
                    if source_available
                    else []
                )
                event_metadata.extend(metadata)
                reference_packages.append(
                    {
                        "reference": reference.to_dict(),
                        "document_hash": canonical_hash(document),
                        "source_artifact_hash": source_hash,
                        "source_artifact_available": source_available,
                        "source_metadata": metadata,
                        "projection_class": (
                            "price"
                            if reference.claim_id == "completed-session-price-history"
                            else "event_context"
                        ),
                    }
                )
    price_references = [item for item in reference_packages if item["projection_class"] == "price"]
    event_references = [
        item
        for item in reference_packages
        if item["projection_class"] == "event_context" and item["source_metadata"]
    ]
    if not price_references:
        gaps.append("price_input_projection_missing")
    if stage == 1 and not event_references:
        gaps.append("qualified_event_evidence_missing")
    if stage == 2:
        gaps.append("historical_industry_or_company_exposure_difference_not_source_proven")

    remaining = len(sessions) - sessions.index(day)
    allowed_horizons = (
        [1, 5]
        if stage in {1, 2}
        else [horizon for horizon in (1, 3, 5, 10) if horizon <= remaining]
    )
    bands: list[dict[str, object]] = []
    projection = market.research_series(target, cutoff, limit=21)
    history_rows = _objects(projection["rows"])
    if not history_rows:
        gaps.append("direction_band_not_ready:price history missing")
    elif projection.get("gaps"):
        gaps.append("direction_band_not_ready:price or adjustment evidence incomplete")
    elif len(history_rows) < 21:
        gaps.append("direction_band_not_ready:21 completed closes unavailable")
    else:
        history_start = date.fromisoformat(_string(history_rows[0], "trade_date"))
        history_calendar = market.calendar_window(
            "SSE" if target.endswith(".SH") else "SZSE",
            history_start,
            day - timedelta(days=1),
        )
        if history_calendar["missing_dates"]:
            gaps.append("direction_band_not_ready:price history calendar missing")
        else:
            calendar_sessions = [
                date.fromisoformat(value) for value in _strings(history_calendar["sessions"])
            ]
            bands = [
                freeze_direction_band(
                    projection,
                    cutoff=cutoff,
                    horizon_sessions=horizon,
                    calendar_sessions=calendar_sessions,
                ).to_dict()
                for horizon in allowed_horizons
            ]

    if stage == 3:
        for symbol in _strings(manifest.get("candidate_symbols", [target])):
            session = market.session(symbol, day)
            if symbol == target and not session.execution_ready:
                gaps.extend("account_execution_source:" + gap for gap in session.gaps)
        if account_binding is None:
            gaps.append("initial_account_binding_missing")

    packages = _arm_input_packages(
        spec=spec,
        stage=stage,
        window_id=window_id,
        cutoff=cutoff,
        target=target,
        allowed_horizons=allowed_horizons,
        price_references=price_references,
        event_references=event_references,
        all_references=reference_packages,
        research_artifact_hash=pack_hash,
        initial_account_binding=account_binding,
    )
    core: dict[str, object] = {
        "window_id": window_id,
        "cutoff": cutoff.isoformat(),
        "target_id": target,
        "research_artifact_hash": pack_hash,
        "bands": bands,
        "event_evidence_metadata": event_metadata,
        "gaps": sorted(set(gaps)),
        "allowed_horizons": allowed_horizons,
        "candidate_permission": (
            "registered_native_tools_up_to_five_new_securities" if stage > 1 else "fixed_target"
        ),
        "arm_input_packages": packages,
    }
    return {**core, "opportunity_id": "study-opportunity-" + canonical_hash(core)}


def _arm_input_packages(
    *,
    spec: Mapping[str, object],
    stage: int,
    window_id: str,
    cutoff: datetime,
    target: str,
    allowed_horizons: Sequence[int],
    price_references: Sequence[Mapping[str, object]],
    event_references: Sequence[Mapping[str, object]],
    all_references: Sequence[Mapping[str, object]],
    research_artifact_hash: str | None,
    initial_account_binding: Mapping[str, object] | None,
) -> dict[str, object]:
    common: dict[str, object] = {
        "schema_version": _INPUT_PACKAGE_VERSION,
        "window_id": window_id,
        "cutoff": cutoff.isoformat(),
        "target_id": target,
        "research_question": spec["research_question"],
        "allowed_horizons": list(allowed_horizons),
        "research_contract": spec["research_contract"],
        "portfolio_contract": spec["portfolio_contract"],
        "profile_id": spec["profile_id"],
        "research_prompt_hash": canonical_hash(RESEARCH_THESIS_V2_PROMPT),
        "portfolio_prompt_hash": canonical_hash(PORTFOLIO_REVIEW_PROMPT_V3),
        "research_artifact_hash": research_artifact_hash,
        **(
            {"initial_account_binding_hash": initial_account_binding["binding_hash"]}
            if initial_account_binding is not None
            else {}
        ),
    }
    common_hash = canonical_hash(common)
    packages: dict[str, object] = {}
    for arm in _STAGES[stage][1]:
        if stage == 1:
            references = list(price_references)
            if arm == "price_and_event":
                references.extend(event_references)
            projection = arm
        else:
            references = list(all_references)
            projection = "same_frozen_source_with_" + arm
        core = {
            **common,
            "common_pairing_hash": common_hash,
            "arm": arm,
            "projection": projection,
            "reference_packages": references,
            "reference_list_hash": canonical_hash(references),
        }
        packages[arm] = {**core, "input_package_hash": canonical_hash(core)}
    return packages


def _unproven_window(
    result: Mapping[str, object],
    *,
    spec: Mapping[str, object],
    stage: int,
    target: str,
    review_days: Sequence[date],
    gap: str,
    manifest: Mapping[str, object] | None = None,
) -> dict[str, object]:
    opportunities: list[dict[str, object]] = []
    for index, day in enumerate(review_days):
        cutoff = datetime.combine(day, time(9, 25), _SHANGHAI).astimezone(UTC)
        allowed_horizons = (
            [1, 5]
            if stage < 3
            else [horizon for horizon in (1, 3, 5, 10) if horizon <= len(review_days) - index]
        )
        packages = _arm_input_packages(
            spec=spec,
            stage=stage,
            window_id=cast(str, result["window_id"]),
            cutoff=cutoff,
            target=target,
            allowed_horizons=allowed_horizons,
            price_references=(),
            event_references=(),
            all_references=(),
            research_artifact_hash=None,
            initial_account_binding=None,
        )
        core: dict[str, object] = {
            "window_id": result["window_id"],
            "cutoff": cutoff.isoformat(),
            "target_id": target,
            "research_artifact_hash": None,
            "bands": [],
            "event_evidence_metadata": [],
            "gaps": [gap],
            "allowed_horizons": allowed_horizons,
            "candidate_permission": (
                "registered_native_tools_up_to_five_new_securities" if stage > 1 else "fixed_target"
            ),
            "arm_input_packages": packages,
        }
        opportunities.append(
            {**core, "opportunity_id": "study-opportunity-" + canonical_hash(core)}
        )
    return {
        **result,
        **({"manifest_hash": canonical_hash(manifest)} if manifest is not None else {}),
        "ready": False,
        "gaps": [gap],
        "opportunities": opportunities,
    }


def _unavailable_source_window(
    window: Mapping[str, object],
    *,
    spec: Mapping[str, object],
    stage: int,
    gap: str,
) -> dict[str, object]:
    sessions = [date.fromisoformat(value) for value in _strings(window["sessions"])]
    return _unproven_window(
        {
            "window_id": _string(window, "window_id"),
            "sample_role": "development",
            "registered_sessions": [day.isoformat() for day in sessions],
            "source_refs": window["source_refs"],
            "source_availability": "unavailable",
        },
        spec=spec,
        stage=stage,
        target=_string(window, "target_id"),
        review_days=sessions if stage == 3 else sessions[:1],
        gap=gap,
    )


def _initial_account_binding(
    *,
    input_root: Path,
    market: HistoricalAShareInputs,
    manifest: Mapping[str, object],
    first_session: date,
    target: str,
    store: ReadOnlyDataSnapshotStore,
) -> dict[str, object]:
    policy = _object(manifest["source_policy"])
    inception = datetime.fromisoformat(_string(policy, "cash_only_inception_at"))
    if inception.tzinfo is None or inception.date() >= first_session:
        raise _WindowReadinessGap("source policy has no pre-window cash-only inception")
    seed = market.session(target, inception.astimezone(_SHANGHAI).date())
    if not seed.execution_ready or seed.spec is None or seed.bar is None:
        raise _WindowReadinessGap("source-backed opening seed is incomplete")
    quantity = (
        _DEFAULT_INITIAL_CASH / Decimal(2) / seed.bar.open // seed.spec.lot_size
    ) * seed.spec.lot_size
    commission = max(
        seed.spec.minimum_commission,
        quantity * seed.bar.open * seed.spec.commission_rate,
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
    cash = (_DEFAULT_INITIAL_CASH - quantity * seed.bar.open - commission).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_EVEN
    )
    nav = cash + quantity * seed.bar.close
    economic_seed = {
        "nav": str(nav),
        "cash": str(cash),
        "positions": {target: str(quantity)},
    }
    source_inputs = {
        "seed_session": inception.astimezone(_SHANGHAI).date().isoformat(),
        "initial_cash": str(_DEFAULT_INITIAL_CASH),
        "allocation_fraction": "0.5",
        "instrument_spec": _json_value(asdict(seed.spec)),
        "bar": _json_value(asdict(seed.bar)),
        "source_record_hashes": list(seed.source_record_hashes),
        "expected_opening_quantity": str(quantity),
        "expected_commission": str(commission),
        "economic_seed": economic_seed,
        "initial_account_hash": canonical_hash(economic_seed),
        "market_snapshot_ids": list(market.snapshot_ids),
    }
    signed_receipt = _matching_signed_seed_receipt(
        input_root.parent / "account-engine",
        source_inputs,
        target,
        store=store,
        cutoff=datetime.combine(first_session, time(9, 25), _SHANGHAI).astimezone(UTC),
    )
    core: dict[str, object] = {
        "schema_version": _ACCOUNT_BINDING_VERSION,
        "engine": "historical-streaming-account.v1",
        "source_inputs": source_inputs,
        "reopened_signed_account_receipt": signed_receipt,
        "comparison_policy": "each_arm_must_reopen_this_identical_economic_seed",
        "new_account_created": False,
    }
    return {**core, "binding_hash": canonical_hash(core)}


def _matching_signed_seed_receipt(
    account_root: Path,
    source_inputs: Mapping[str, object],
    target: str,
    *,
    store: ReadOnlyDataSnapshotStore,
    cutoff: datetime,
) -> dict[str, object]:
    if not account_root.is_dir() or account_root.is_symlink():
        raise _WindowReadinessGap("prior account-engine source directory is missing")
    expected_spec = _object(source_inputs["instrument_spec"])
    expected_bar = _object(source_inputs["bar"])
    expected_quantity = source_inputs["expected_opening_quantity"]
    expected_inception = expected_bar["session_open_at"]
    journal_prefixes: dict[str, str] = {}
    for path in sorted(account_root.glob("*.jsonl")):
        if path.is_symlink() or not path.is_file():
            continue
        lines = path.read_text().splitlines()
        if len(lines) < 2:
            continue
        try:
            header = _object(json.loads(lines[0]))
            opening = _object(json.loads(lines[1]))
            specs = _objects(header.get("specs"))
            bars = _object(opening.get("bars"))
            intents = _objects(opening.get("intents"))
            inception = _object(header.get("cash_only_inception"))
        except (json.JSONDecodeError, ValueError):
            continue
        comparable_spec = {
            key: value for key, value in expected_spec.items() if key != "source_ref"
        }
        if (
            header.get("schema") != "historical-streaming-account.v1"
            or header.get("cash") != str(_DEFAULT_INITIAL_CASH)
            or inception.get("at") != expected_inception
            or len(specs) != 1
            or {key: value for key, value in specs[0].items() if key != "source_ref"}
            != comparable_spec
            or bars.get(target) != expected_bar
            or len(intents) != 1
            or intents[0].get("instrument_id") != target
            or intents[0].get("quantity") != expected_quantity
            or intents[0].get("signal_id") != "historical-opening-allocation"
        ):
            continue
        account_reference = header.get("account")
        if not isinstance(account_reference, str):
            continue
        journal_prefixes[account_reference] = sha256(
            (lines[0] + "\n" + lines[1] + "\n").encode()
        ).hexdigest()
    if not journal_prefixes:
        raise _WindowReadinessGap("no existing engine journal reopens the source-backed seed")
    authority = open_read_only_runtime_authority(store.root)
    receipts: list[dict[str, object]] = []
    for event in authority.events:
        if event.event_type != "portfolio.review.frozen" or event.observed_at != cutoff:
            continue
        binding_hash = event.payload.get("binding_hash")
        if not isinstance(binding_hash, str):
            continue
        binding = _object(authority.store.artifacts.read_json(binding_hash))
        inputs = _object(binding.get("inputs"))
        if datetime.fromisoformat(_string(inputs, "cutoff").replace("Z", "+00:00")) != cutoff:
            continue
        account_state = _object(inputs.get("account_state"))
        account_reference = account_state.get("account_reference_hash")
        if not isinstance(account_reference, str) or account_reference not in journal_prefixes:
            continue
        _validate_signed_seed_inputs(
            inputs,
            source_inputs=source_inputs,
            target=target,
            account_reference=account_reference,
            cutoff=cutoff,
        )
        receipt = {
            "journal_prefix_hash": journal_prefixes[account_reference],
            "signed_event_hash": event.event_hash,
            "signed_binding_hash": binding_hash,
            "account_state_hash": canonical_hash(account_state),
            "position_snapshot_hash": canonical_hash(inputs["position_snapshot"]),
            "exposure_view_hash": canonical_hash(inputs["exposure_view"]),
            "cutoff": cutoff.isoformat(),
        }
        receipts.append({**receipt, "receipt_hash": canonical_hash(receipt)})
    if not receipts:
        raise _WindowReadinessGap(
            "no signed completed account receipt proves the source-backed seed"
        )
    return min(receipts, key=lambda item: cast(str, item["receipt_hash"]))


def _validate_signed_seed_inputs(
    inputs: Mapping[str, object],
    *,
    source_inputs: Mapping[str, object],
    target: str,
    account_reference: str,
    cutoff: datetime,
) -> None:
    state = _object(inputs["account_state"])
    position = _object(inputs["position_snapshot"])
    exposure = _object(inputs["exposure_view"])
    view = _object(inputs["authorized_view"])
    mandate = _object(inputs["mandate"])
    expected = _object(source_inputs["economic_seed"])
    expected_bar = _object(source_inputs["bar"])
    cash = _objects(state.get("cash"))
    positions = _objects(state.get("positions"))
    fills = _objects(state.get("recent_fills"))
    marked = _objects(exposure.get("marked_positions"))
    if (
        state.get("complete") is not True
        or state.get("environment") != "backtest"
        or state.get("missing_sections") != []
        or state.get("reconciliation_gaps") != []
        or state.get("open_orders") != []
        or state.get("account_reference_hash") != account_reference
        or datetime.fromisoformat(_string(state, "as_of").replace("Z", "+00:00"))
        != datetime.fromisoformat(_string(expected_bar, "session_close_at"))
        or len(cash) != 1
        or cash[0].get("currency") != "CNY"
        or cash[0].get("available") != expected["cash"]
        or cash[0].get("settled") != expected["cash"]
        or len(positions) != 1
        or positions[0].get("target_id") != target
        or positions[0].get("quantity") != _object(expected["positions"])[target]
        or positions[0].get("side") != "buy"
        or len(fills) != 1
        or fills[0].get("order_reference") != "historical-opening-510300"
        or fills[0].get("target_id") != target
        or fills[0].get("quantity") != _object(expected["positions"])[target]
        or datetime.fromisoformat(_string(fills[0], "filled_at").replace("Z", "+00:00"))
        != datetime.fromisoformat(_string(expected_bar, "session_open_at"))
        or position.get("account_reference_hash") != account_reference
        or position.get("account_state_snapshot_id") != state.get("snapshot_id")
        or position.get("complete") is not True
        or position.get("observation_gaps") != []
        or len(marked) != 1
        or marked[0].get("instrument_id") != target
        or marked[0].get("quantity") != _object(expected["positions"])[target]
        or Decimal(str(marked[0].get("raw_price"))) != Decimal(_string(expected_bar, "close"))
        or Decimal(str(exposure.get("current_gross_exposure")))
        != Decimal(str(_object(expected["positions"])[target]))
        * Decimal(_string(expected_bar, "close"))
        or Decimal(str(expected["cash"])) + Decimal(str(exposure.get("current_gross_exposure")))
        != Decimal(str(expected["nav"]))
        or datetime.fromisoformat(_string(view, "cutoff").replace("Z", "+00:00")) != cutoff
        or sorted(_strings(view.get("data_snapshot_ids")))
        != sorted(_strings(source_inputs["market_snapshot_ids"]))
        or view.get("position_snapshot_id") != position.get("snapshot_id")
        or mandate.get("account_id") != account_reference
    ):
        raise ValueError("signed account receipt differs from source-backed seed")


def _event_metadata(source_ref: str, document: object, cutoff: datetime) -> list[dict[str, object]]:
    """Use dated source records, never evidence-ID naming or fetch time guesses."""
    if not isinstance(document, dict):
        return []
    value = cast(dict[str, object], document)
    result: list[dict[str, object]] = []
    for field in ("records", "articles"):
        for row in _objects(value.get(field, [])):
            published = row.get("published_at")
            available = row.get("available_at")
            if not isinstance(published, str) or not isinstance(available, str):
                continue
            publication = datetime.fromisoformat(published.replace("Z", "+00:00"))
            visibility = datetime.fromisoformat(available.replace("Z", "+00:00"))
            if (
                publication.tzinfo is None
                or visibility.tzinfo is None
                or publication > visibility
                or visibility > cutoff
            ):
                raise ValueError("event source times are not PIT eligible")
            if (
                not row.get("source_ref")
                or not row.get("content_hash")
                or not row.get("evidence_record_id")
            ):
                continue
            result.append(
                {
                    "source_ref": row["source_ref"],
                    "projection_source_ref": source_ref,
                    "evidence_record_id": row["evidence_record_id"],
                    "content_hash": row["content_hash"],
                    "published_at": publication.isoformat(),
                    "available_at": visibility.isoformat(),
                    "age_seconds": int((cutoff - publication).total_seconds()),
                    "coverage": "dated_publisher_record",
                    "payload_status": row.get("payload_status"),
                    "historical_authentication": "modeled_pit_not_strict_receipt",
                }
            )
    return result


def _predecessor_gaps(spec: Mapping[str, object], store: ReadOnlyDataSnapshotStore) -> list[str]:
    stage = _stage(spec)
    if stage == 1:
        return []
    predecessor = spec.get("predecessor_result_hash")
    if predecessor is None:
        return ["preceding_stage_result_not_yet_selected_execution_version_pending"]
    try:
        prior = _object(store.artifacts.read_json(cast(str, predecessor)))
    except FileNotFoundError:
        return ["preceding_stage_result_artifact_missing"]
    gaps: list[str] = []
    if prior.get("stage") != stage - 1 or prior.get("complete_groups", 0) == 0:
        gaps.append("preceding_stage_result_has_no_complete_comparable_group")
    gaps.append("execution_configuration_requires_result_based_registration")
    return gaps


def _market(
    store: ReadOnlyDataSnapshotStore,
    manifest: Mapping[str, object],
    record: Mapping[str, object],
) -> HistoricalAShareInputs:
    policy = _object(manifest["source_policy"])
    return HistoricalAShareInputs(
        store=store,
        snapshot_ids=tuple(_strings(record["market_snapshot_ids"])),
        rule_artifact_hashes=tuple(_strings(record["rule_artifact_hashes"])),
        fund_halt_artifact_hashes=tuple(_strings(record.get("fund_halt_artifact_hashes", []))),
        policy=ModeledHistoricalPolicy(
            _string(policy, "policy_id"),
            Decimal(_string(policy, "daily_open_volume_fraction")),
            limit_basis=str(policy.get("limit_basis", "reported_stk_limit")),
            review_timing=str(policy.get("review_timing", "preopen")),
            cash_only_inception_at=(
                None
                if policy.get("cash_only_inception_at") is None
                else datetime.fromisoformat(str(policy["cash_only_inception_at"]))
            ),
        ),
    )


def _read_preparation(output_root: Path) -> dict[str, object]:
    value = _object(json.loads((output_root / "preparation.json").read_text()))
    core = {key: item for key, item in value.items() if key != "preparation_hash"}
    if value.get("schema_version") != _PREPARATION_VERSION or value.get(
        "preparation_hash"
    ) != canonical_hash(core):
        raise ValueError("staged preparation content hash changed")
    return value


def _source_artifact_hash(source_ref: str) -> str | None:
    if not source_ref.startswith("sha256:"):
        return None
    return _sha256_text(source_ref.removeprefix("sha256:"), "source artifact")


def _stage(spec: Mapping[str, object]) -> int:
    value = spec.get("stage")
    if type(value) is not int or value not in _STAGES:
        raise ValueError("study stage must be 1, 2 or 3")
    return value


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) for key in cast(dict[object, object], value)
    ):
        raise ValueError("expected JSON object")
    return cast(dict[str, object], value)


def _objects(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError("expected JSON object list")
    return [_object(item) for item in cast(list[object], value)]


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item or item != item.strip():
        raise ValueError(f"expected trimmed {key}")
    return item


def _strings(value: object) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in cast(list[object], value)
    ):
        raise ValueError("expected strings")
    return cast(list[str], value)


def _sha256_text(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must have a SHA-256 digest")
    return value


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in cast(dict[str, Any], value).items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in cast(Sequence[Any], value)]
    return value


def _write_immutable(path: Path, value: object) -> None:
    payload = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if path.exists():
        if path.read_text() != payload:
            raise ValueError(f"frozen preparation differs: {path.name}")
        return
    with path.open("x") as stream:
        stream.write(payload)
    path.chmod(0o600)
