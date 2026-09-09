"""Read-only posthoc measurements for a completed continuous-study batch.

The entry deliberately accepts every private location from its caller.  It never
starts an Agent, calls a Provider, or opens the Harness state for writing.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.offline_authority import (
    ReadOnlyDataSnapshotStore,
    open_read_only_runtime_authority,
)
from market_impact_agent.usage_ledger import UsageRecord


@dataclass(frozen=True, slots=True)
class ContinuousRetrospectivePaths:
    """Explicit private inputs for one completed-study retrospective."""

    report_path: Path
    runtime_store_root: Path
    preflight_artifact_hash: str


@dataclass(frozen=True, slots=True)
class ContinuousRetrospectiveResult:
    """Private, posthoc measurements.  Callers choose whether and where to persist them."""

    costs: dict[str, object]
    theses: list[dict[str, object]]
    thesis_summary: list[dict[str, object]]
    proxy_price_maps: dict[str, dict[str, dict[str, object]]]
    proxy_outcomes: list[dict[str, object]]
    proxy_outcome_summary: dict[str, object]
    matched_momentum: list[dict[str, object]]


def verify_continuous_retrospective_inputs(runtime_store_root: Path) -> None:
    """Verify the signed Run Journal and hash-chained Usage Ledger in read-only mode."""

    open_read_only_runtime_authority(runtime_store_root)


def summarize_usage(records: Sequence[UsageRecord]) -> dict[str, int | float | None]:
    """Preserve the completed-study denominator: every terminal record is counted."""

    paid = [record for record in records if record.metrics.provider_attempts]
    return {
        "terminal_runs": len(records),
        "paid_terminal_runs": len(paid),
        "estimated_cost_microusd": sum(
            record.metrics.estimated_cost_microusd for record in records
        ),
        "input_tokens": sum(record.metrics.input_tokens for record in records),
        "output_tokens": sum(record.metrics.output_tokens for record in records),
        "latency_ms": sum(record.metrics.latency_ms for record in records),
        "provider_attempts": sum(record.metrics.provider_attempts for record in records),
        "tool_calls": sum(record.metrics.tool_calls for record in records),
        "turns": sum(record.metrics.turns for record in records),
        "median_paid_run_cost_microusd": median(
            [record.metrics.estimated_cost_microusd for record in paid]
        )
        if paid
        else None,
        "median_paid_run_latency_ms": median([record.metrics.latency_ms for record in paid])
        if paid
        else None,
    }


def run_continuous_retrospective(
    paths: ContinuousRetrospectivePaths,
) -> ContinuousRetrospectiveResult:
    """Reopen one frozen batch and calculate the private retrospective diagnostics.

    The returned values retain private run/source identities for a caller-owned
    local result.  This function does not write those values to disk.
    """

    runtime_root = paths.runtime_store_root
    report = _load_report(paths.report_path)
    _verify_report_identity(report)
    authority = open_read_only_runtime_authority(runtime_root)

    store = authority.store
    preflight = _artifact_object(store, paths.preflight_artifact_hash, "preflight")
    initial_runs, episode_rows = _report_runs(report)
    usage_records = [stored.record for stored in authority.usage_records]

    def run_row(run_id: str) -> dict[str, object] | None:
        if run_id in initial_runs:
            return initial_runs[run_id]
        research_run = run_id.removesuffix(".portfolio") + ".research"
        if run_id.endswith(".portfolio") and research_run in initial_runs:
            return initial_runs[research_run]
        return episode_rows.get(run_id.split(".review.")[0])

    current_usage = [record for record in usage_records if run_row(record.run_id) is not None]
    cost_groups: defaultdict[tuple[str, ...], list[UsageRecord]] = defaultdict(list)
    for record in current_usage:
        row = _required_run_row(run_row(record.run_id), record.run_id)
        stage = "initial" if record.run_id.startswith("continuous-initial-") else "rolling"
        role = "research" if ".research" in record.run_id else "portfolio"
        profile_arm = _string(row, "profile_arm")
        window_id = _string(row, "window_id")
        dimensions = (
            ("stage_model", stage, profile_arm),
            ("stage_role", stage, role),
            (("rolling_window" if stage == "rolling" else "initial_window"), window_id),
            ("stage_model_role", stage, profile_arm, role),
        )
        for dimension in dimensions:
            cost_groups[dimension].append(record)
    costs: dict[str, object] = {
        "current_identity_runs": summarize_usage(current_usage),
        "groups": [
            {"dimensions": list(key), **summarize_usage(records)}
            for key, records in cost_groups.items()
        ],
        "cumulative_attribution": _cumulative_attribution(report, current_usage),
    }

    thesis_events = sorted(
        (event.sequence, event.run_id, event.payload)
        for event in authority.events
        if event.event_type == "research.thesis.validated"
    )
    theses = _read_theses(
        store=store,
        thesis_events=thesis_events,
        run_row=run_row,
        initial_runs=initial_runs,
    )
    profiles = sorted({cast(str, thesis["profile_arm"]) for thesis in theses})
    thesis_summary = _thesis_summary(theses, profiles)
    source_windows = {
        _string(item, "window_id"): item for item in _object_list(preflight, "source_windows")
    }
    outcome_maps = _outcome_maps(store, theses, source_windows)
    outcomes = _proxy_outcomes(theses, source_windows, outcome_maps)
    proxy_outcome_summary: dict[str, object] = {
        "posthoc": True,
        "independent_sample_claim": False,
        "investment_effectiveness_accepted": False,
        "groups": _proxy_summary(outcomes, profiles),
    }
    matched_momentum = _matched_momentum(theses, outcomes, profiles)
    return ContinuousRetrospectiveResult(
        costs=costs,
        theses=theses,
        thesis_summary=thesis_summary,
        proxy_price_maps=outcome_maps,
        proxy_outcomes=outcomes,
        proxy_outcome_summary=proxy_outcome_summary,
        matched_momentum=matched_momentum,
    )


def _read_theses(
    *,
    store: ReadOnlyDataSnapshotStore,
    thesis_events: Sequence[tuple[int, str, dict[str, object]]],
    run_row: Callable[[str], dict[str, object] | None],
    initial_runs: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for sequence, run_id, payload in thesis_events:
        row = run_row(run_id)
        if row is None:
            continue
        terminal_hash = _string(payload, "terminal_hash")
        binding_hash = _string(payload, "binding_hash")
        terminal = _artifact_object(store, terminal_hash, "thesis terminal")
        binding = _artifact_object(store, binding_hash, "thesis binding")
        selected = _artifact_object(
            store,
            _string(binding, "selected_inputs_artifact_hash"),
            "selected inputs",
        )
        thesis = _mapping(terminal.get("thesis"), "terminal thesis")
        evidence = _object_list(selected, "evidence")
        reference_ids = [
            _string(_mapping(item.get("reference"), "evidence reference"), "evidence_id")
            for item in evidence
        ]
        prices = [
            _mapping(item.get("document"), "price evidence document")
            for item in evidence
            if _string(
                _mapping(item.get("reference"), "evidence reference"), "evidence_id"
            ).startswith("price-")
        ]
        context_ids = [item for item in reference_ids if not item.startswith("price-")]
        prior = binding.get("prior_thesis")
        prior_thesis = _mapping(prior, "prior thesis").get("thesis") if prior is not None else None
        prior_mapping = _mapping(prior_thesis, "prior thesis") if prior_thesis is not None else None
        rows.append(
            {
                "sequence": sequence,
                "run_id": run_id,
                "terminal_hash": terminal_hash,
                "binding_hash": binding_hash,
                "selected_inputs_hash": _string(binding, "selected_inputs_artifact_hash"),
                "window_id": _string(row, "window_id"),
                "profile_arm": _string(row, "profile_arm"),
                "cadence": row.get("cadence"),
                "stage": "initial" if run_id in initial_runs else "rolling",
                "target_id": _string(
                    _mapping(binding.get("inputs"), "binding inputs"), "target_id"
                ),
                "as_of": _string(thesis, "as_of"),
                "horizon": thesis["primary_horizon_sessions"],
                "direction": _string(thesis, "base_case_direction"),
                "review_after": thesis["review_after_sessions"],
                "prior_direction": None
                if prior_mapping is None
                else prior_mapping.get("base_case_direction"),
                "prior_horizon": None
                if prior_mapping is None
                else prior_mapping.get("primary_horizon_sessions"),
                "context_ids": context_ids,
                "source_gaps": selected.get("data_gaps", []),
                "price_symbols": [item.get("symbol") for item in prices],
                "last_input_price_dates": {
                    item.get("symbol"): _last_input_date(item) for item in prices
                },
                "price_input_rows": {item.get("symbol"): item.get("rows", []) for item in prices},
                "thesis": thesis,
                "last5_input_momentum": {
                    item.get("symbol"): str(
                        Decimal(str(_price_rows(item)[-1][2]))
                        / Decimal(str(_price_rows(item)[-6][2]))
                        - 1
                    )
                    for item in prices
                    if len(_price_rows(item)) >= 6
                    and _price_rows(item)[-1][2] is not None
                    and _price_rows(item)[-6][2] is not None
                },
            }
        )
    if len(rows) != len({cast(str, row["run_id"]) for row in rows}):
        raise ValueError("validated thesis records must have unique run IDs")
    if len(rows) != 211:
        raise ValueError("completed-study thesis denominator differs from 211")
    return rows


def _thesis_summary(
    theses: Sequence[dict[str, object]], profiles: Sequence[str]
) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    for stage in ("initial", "rolling"):
        for profile in profiles:
            group = [
                thesis
                for thesis in theses
                if thesis["stage"] == stage and thesis["profile_arm"] == profile
            ]
            summaries.append(
                {
                    "stage": stage,
                    "profile": profile,
                    "theses": len(group),
                    "directions": dict(Counter(thesis["direction"] for thesis in group)),
                    "horizons": dict(Counter(thesis["horizon"] for thesis in group)),
                    "review_after": dict(Counter(thesis["review_after"] for thesis in group)),
                    "price_only": sum(not thesis["context_ids"] for thesis in group),
                    "direction_changes": sum(
                        thesis["prior_direction"] is not None
                        and thesis["direction"] != thesis["prior_direction"]
                        for thesis in group
                    ),
                    "with_prior": sum(thesis["prior_direction"] is not None for thesis in group),
                }
            )
    return summaries


def _outcome_maps(
    store: ReadOnlyDataSnapshotStore,
    theses: Sequence[dict[str, object]],
    source_windows: dict[str, dict[str, object]],
) -> dict[str, dict[str, dict[str, object]]]:
    maps: dict[str, dict[str, dict[str, object]]] = {}
    for window_id in sorted({cast(str, thesis["window_id"]) for thesis in theses}):
        source = source_windows[window_id]
        policy = _mapping(source.get("policy"), "source-window policy")
        market = HistoricalAShareInputs(
            store=store,
            snapshot_ids=tuple(_string_list(source, "snapshot_ids")),
            rule_artifact_hashes=tuple(_string_list(source, "rule_artifact_hashes")),
            fund_halt_artifact_hashes=tuple(
                _string_list(source, "fund_halt_artifact_hashes", required=False)
            ),
            policy=ModeledHistoricalPolicy(
                _string(policy, "policy_id"),
                Decimal(_string(policy, "daily_open_volume_fraction")),
                limit_basis=_string(policy, "limit_basis"),
                review_timing=str(policy.get("review_timing", "preopen")),
                cash_only_inception_at=_optional_datetime(policy.get("cash_only_inception_at")),
            ),
        )
        calendar = _string_list(
            _mapping(source.get("preflight_qualification"), "preflight qualification"),
            "matched_execution_sessions",
        )
        endpoints = [
            calendar[calendar.index(_as_of_day(thesis)) + _integer(thesis, "horizon") - 1]
            for thesis in theses
            if thesis["window_id"] == window_id
            and calendar.index(_as_of_day(thesis)) + _integer(thesis, "horizon") - 1 < len(calendar)
        ]
        cutoff = datetime.fromisoformat(max(endpoints) + "T16:00:00+08:00")
        series = {
            symbol: market.research_series(symbol, cutoff, limit=252)
            for symbol in ("510300.SH", "510500.SH")
        }
        maps[window_id] = {
            symbol: {
                _string(row, "trade_date"): row["cutoff_adjusted_close"]
                for row in _object_list(values, "rows")
            }
            for symbol, values in series.items()
        }
    return maps


def _proxy_outcomes(
    theses: Sequence[dict[str, object]],
    source_windows: dict[str, dict[str, object]],
    outcome_maps: dict[str, dict[str, dict[str, object]]],
) -> list[dict[str, object]]:
    outcomes: list[dict[str, object]] = []
    for thesis in theses:
        source = source_windows[_string(thesis, "window_id")]
        calendar = _string_list(
            _mapping(source.get("preflight_qualification"), "preflight qualification"),
            "matched_execution_sessions",
        )
        as_of = _as_of_day(thesis)
        if as_of not in calendar:
            raise ValueError("research cutoff not on frozen calendar")
        index = calendar.index(as_of)
        horizon_end = index + _integer(thesis, "horizon") - 1
        result = {
            key: thesis[key]
            for key in (
                "run_id",
                "terminal_hash",
                "window_id",
                "profile_arm",
                "cadence",
                "stage",
                "as_of",
                "horizon",
                "direction",
            )
        }
        result.update(
            status="missing_registered_horizon",
            returns={},
            endpoint=None,
            directional_hit=None,
            momentum_agreement=None,
        )
        if horizon_end < len(calendar):
            endpoint = calendar[horizon_end]
            returns: dict[str, str] = {}
            for symbol in ("510300.SH", "510500.SH"):
                baseline_date = _mapping(thesis["last_input_price_dates"], "input price dates").get(
                    symbol
                )
                if not isinstance(baseline_date, str) or baseline_date >= as_of:
                    continue
                values = outcome_maps[_string(thesis, "window_id")][symbol]
                start, end = values.get(baseline_date), values.get(endpoint)
                if start is not None and end is not None:
                    returns[symbol] = str(Decimal(str(end)) / Decimal(str(start)) - 1)
            if len(returns) == 2:
                result.update(status="proxy_observed", returns=returns, endpoint=endpoint)
                signs = {_sign(Decimal(value)) for value in returns.values()}
                result["proxy_sign"] = next(iter(signs)) if len(signs) == 1 else None
                direction = _string(thesis, "direction")
                if direction in ("up", "down") and result["proxy_sign"] in (-1, 1):
                    result["directional_hit"] = (1 if direction == "up" else -1) == result[
                        "proxy_sign"
                    ]
                momentum = _mapping(thesis["last5_input_momentum"], "input momentum")
                if len(momentum) == 2:
                    momentum_signs = {_sign(Decimal(str(value))) for value in momentum.values()}
                    if len(momentum_signs) == 1 and direction in ("up", "down"):
                        result["momentum_agreement"] = (1 if direction == "up" else -1) == next(
                            iter(momentum_signs)
                        )
        outcomes.append(result)
    return outcomes


def _proxy_summary(
    outcomes: Sequence[dict[str, object]], profiles: Sequence[str]
) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    for stage in ("initial", "rolling"):
        for profile in profiles:
            group = [
                outcome
                for outcome in outcomes
                if outcome["stage"] == stage and outcome["profile_arm"] == profile
            ]
            summaries.append(
                {
                    "stage": stage,
                    "profile": profile,
                    "n": len(group),
                    "observed": sum(outcome["status"] == "proxy_observed" for outcome in group),
                    "directional_assessable": sum(
                        outcome["directional_hit"] is not None for outcome in group
                    ),
                    "directional_hits": sum(
                        outcome["directional_hit"] is True for outcome in group
                    ),
                    "rangebound": sum(outcome["direction"] == "rangebound" for outcome in group),
                    "momentum_assessable": sum(
                        outcome["momentum_agreement"] is not None for outcome in group
                    ),
                    "momentum_agreement": sum(
                        outcome["momentum_agreement"] is True for outcome in group
                    ),
                }
            )
    return summaries


def _matched_momentum(
    theses: Sequence[dict[str, object]],
    outcomes: Sequence[dict[str, object]],
    profiles: Sequence[str],
) -> list[dict[str, object]]:
    by_run = {_string(thesis, "run_id"): thesis for thesis in theses}
    groups: list[dict[str, object]] = []
    for stage in ("initial", "rolling"):
        for profile in profiles:
            pairs: list[dict[str, object]] = []
            for outcome in outcomes:
                if (
                    outcome["stage"] != stage
                    or outcome["profile_arm"] != profile
                    or outcome["directional_hit"] is None
                ):
                    continue
                momentum = _mapping(
                    by_run[_string(outcome, "run_id")]["last5_input_momentum"], "input momentum"
                )
                signs = {_sign(Decimal(str(value))) for value in momentum.values()}
                if len(momentum) == 2 and len(signs) == 1 and next(iter(signs)) != 0:
                    pairs.append(
                        {
                            "run_id": outcome["run_id"],
                            "model_hit": outcome["directional_hit"],
                            "momentum_hit": next(iter(signs)) == outcome["proxy_sign"],
                        }
                    )
            groups.append(
                {
                    "stage": stage,
                    "model": profile,
                    "n": len(pairs),
                    "model_hits": sum(pair["model_hit"] is True for pair in pairs),
                    "momentum_hits": sum(pair["momentum_hit"] is True for pair in pairs),
                    "pairs": pairs,
                }
            )
    return groups


def _report_runs(
    report: dict[str, object],
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    episode_rows = {
        _string(row, "episode_id"): row
        for row in _object_list(report, "rolling")
        if row.get("episode_id")
    }
    initial_runs: dict[str, dict[str, object]] = {}
    for row in _object_list(report, "initial"):
        decision = _mapping(row.get("decision", {}), "initial decision")
        research_run = decision.get("research_run_id")
        if not research_run and row.get("continuation_ref"):
            research_run = _string(row, "continuation_ref").removesuffix(".portfolio") + ".research"
        if isinstance(research_run, str):
            initial_runs[research_run] = row
    if len(initial_runs) != 30:
        raise ValueError("completed-study initial Run denominator differs from 30")
    return initial_runs, episode_rows


def _cumulative_attribution(
    report: dict[str, object], current_usage: Sequence[UsageRecord]
) -> dict[str, object]:
    """Keep known spend separate from the report's unresolved reservation."""

    budget = _mapping(report.get("budget"), "completed-study budget")
    cumulative_known_cost = _integer(budget, "known_cost_microusd")
    cumulative_requests = _integer(budget, "physical_requests")
    reserved_microusd = _integer(budget, "reserved_microusd")
    unsettled_requests = _integer(budget, "unsettled_requests")
    current = summarize_usage(current_usage)
    current_known_cost = _integer(current, "estimated_cost_microusd")
    current_requests = _integer(current, "provider_attempts")
    prior_known_cost = cumulative_known_cost - current_known_cost
    prior_requests = cumulative_requests - current_requests
    if prior_known_cost < 0 or prior_requests < 0:
        raise ValueError("current identity usage exceeds completed-study cumulative budget")
    return {
        "current_identity": {
            "known_cost_microusd": current_known_cost,
            "physical_requests": current_requests,
        },
        "cumulative_report": {
            "known_cost_microusd": cumulative_known_cost,
            "physical_requests": cumulative_requests,
        },
        "prior_identities": {
            "known_cost_microusd": prior_known_cost,
            "physical_requests": prior_requests,
        },
        "unsettled_reservation": {
            "reserved_microusd": reserved_microusd,
            "unsettled_requests": unsettled_requests,
            "included_in_known_spend": False,
        },
    }


def _load_report(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("continuous-study report must be a real file")
    report_path = path.resolve()
    import json

    value = json.loads(report_path.read_text(encoding="utf-8"))
    return _mapping(value, "continuous-study report")


def _verify_report_identity(report: dict[str, object]) -> None:
    artifact_hash = _string(report, "artifact_hash")
    body = {key: value for key, value in report.items() if key != "artifact_hash"}
    if canonical_hash(body) != artifact_hash:
        raise ValueError("continuous-study report content does not match its artifact identity")


def _artifact_object(
    store: ReadOnlyDataSnapshotStore, content_hash: str, label: str
) -> dict[str, object]:
    return _mapping(store.artifacts.read_json(content_hash), label)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object")
    raw = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in raw):
        raise TypeError(f"{label} must be an object")
    return cast(dict[str, object], raw)


def _object_list(value: dict[str, object], key: str) -> list[dict[str, object]]:
    raw = value.get(key)
    if not isinstance(raw, list):
        raise TypeError(f"{key} must be a list")
    return [_mapping(item, key) for item in cast(list[object], raw)]


def _string_list(value: dict[str, object], key: str, *, required: bool = True) -> list[str]:
    raw = value.get(key)
    if raw is None and not required:
        return []
    if not isinstance(raw, list):
        raise TypeError(f"{key} must be a list of text")
    items = cast(list[object], raw)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"{key} must be a list of text")
    return cast(list[str], items)


def _string(value: dict[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str):
        raise TypeError(f"{key} must be text")
    return result


def _integer(value: Mapping[str, object], key: str) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int):
        raise TypeError(f"{key} must be an integer")
    return result


def _required_run_row(row: dict[str, object] | None, run_id: str) -> dict[str, object]:
    if row is None:
        raise ValueError(f"usage record {run_id} is outside the completed-study report")
    return row


def _as_of_day(thesis: dict[str, object]) -> str:
    return _string(thesis, "as_of")[:10]


def _optional_datetime(value: object) -> datetime | None:
    return datetime.fromisoformat(value) if isinstance(value, str) else None


def _price_rows(value: dict[str, object]) -> list[list[object]]:
    raw = value.get("rows", [])
    if not isinstance(raw, list):
        raise TypeError("price evidence rows must be a list of rows")
    rows = cast(list[object], raw)
    if not all(isinstance(row, list) for row in rows):
        raise TypeError("price evidence rows must be a list of rows")
    return cast(list[list[object]], rows)


def _last_input_date(value: dict[str, object]) -> object:
    rows = _price_rows(value)
    return rows[-1][0] if rows else None


def _sign(value: Decimal) -> int:
    return (value > 0) - (value < 0)
