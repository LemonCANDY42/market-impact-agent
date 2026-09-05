"""Open dynamic-horizon development outcomes after forecasts are immutable.

This module is intentionally separate from the Agent runner.  No model process
or frozen decision input receives a panel path or any value produced here.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.dynamic_effectiveness_runner import (
    load_dynamic_effectiveness_study,
    load_verified_opened_analysis_report,
)
from market_impact_agent.market_regimes import RegimeSeries, validate_regime_panel


def open_dynamic_analysis_outcomes(
    root: Path,
    *,
    panel_directory: Path,
    index_proxy: str,
    reviewed_at: datetime,
) -> dict[str, object]:
    """Score immutable opened-development forecasts at their declared horizons.

    ``reviewed_at`` is the explicit boundary after which the outcomes may be
    opened.  It is recorded but never passed back into an Agent context.
    """

    destination = root / "opened-analysis-outcomes.json"
    if reviewed_at.tzinfo is None or reviewed_at.utcoffset() is None:
        raise ValueError("outcome review time must be timezone-aware")
    registration = load_dynamic_effectiveness_study(root)
    analysis = load_verified_opened_analysis_report(root)
    panel = validate_regime_panel(panel_directory)
    series = _market_series(panel.panel.series, index_proxy)

    if destination.is_file():
        stored = _verified_report(destination)
        stored_reviewed_at = _datetime(_string(stored, "reviewed_at"))
        expected = _build_outcome_report(
            registration=registration,
            analysis=analysis,
            panel_id=panel.panel_id,
            panel_hash=panel.panel_hash,
            index_proxy=index_proxy,
            series=series,
            reviewed_at=stored_reviewed_at,
        )
        if stored != expected:
            raise ValueError("dynamic outcome report differs from its frozen authorities")
        return stored

    report = _build_outcome_report(
        registration=registration,
        analysis=analysis,
        panel_id=panel.panel_id,
        panel_hash=panel.panel_hash,
        index_proxy=index_proxy,
        series=series,
        reviewed_at=reviewed_at,
    )
    _write_new(destination, report)
    return report


def _build_outcome_report(
    *,
    registration: dict[str, object],
    analysis: dict[str, object],
    panel_id: str,
    panel_hash: str,
    index_proxy: str,
    series: RegimeSeries,
    reviewed_at: datetime,
) -> dict[str, object]:
    """Recompute the report from signed analysis and the registered price panel."""

    scored: list[dict[str, object]] = []
    for row in cast(list[dict[str, object]], analysis["results"]):
        thesis = row.get("thesis")
        if row.get("status") != "completed" or not isinstance(thesis, dict):
            scored.append(
                {
                    "run_id": row.get("run_id"),
                    "case_id": row.get("case_id"),
                    "topology": row.get("topology"),
                    "repetition": row.get("repetition"),
                    "status": "unscored_system_failure",
                }
            )
            continue
        scored.append(score_dynamic_result(row, cast(dict[str, object], thesis), series))

    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-opened-analysis-outcomes.v1",
        "registration_hash": registration["registration_hash"],
        "analysis_report_hash": canonical_hash(analysis),
        "reviewed_at": reviewed_at.isoformat(),
        "panel_id": panel_id,
        "panel_hash": panel_hash,
        "index_proxy": index_proxy,
        "results": scored,
        "topology_summaries": _topology_summaries(scored),
        "conditional_topology": _conditional_topology(scored),
        "memory_sensitivity": summarize_memory_sensitivity(scored),
        "pre_post_2026_comparison": {
            "status": "insufficient_post_2026_outcomes",
            "pre_2026_scored": sum(
                row.get("status") == "scored" and int(cast(str, row["case_id"])[:4]) < 2026
                for row in scored
                if isinstance(row.get("case_id"), str)
            ),
            "post_2026_scored": 0,
            "inference_scope": (
                "descriptive era comparison only; cannot establish training-data leakage"
            ),
        },
        "outcomes_visible_to_agents": False,
        "claims": (
            "Opened correlated Modeled-PIT development diagnostics only. Dynamic-horizon "
            "direction/path agreement is not executable PnL, a Skill increment, calibrated "
            "accuracy, or Alpha evidence."
        ),
        "promotion": False,
        "live_execution": False,
    }
    report["report_hash"] = canonical_hash(report)
    return report


def score_dynamic_result(
    row: dict[str, object], thesis: dict[str, object], series: RegimeSeries
) -> dict[str, object]:
    case_id = _string(row, "case_id")
    horizon = thesis.get("primary_horizon_sessions")
    direction = thesis.get("base_case_direction")
    if not isinstance(horizon, int) or horizon not in {1, 3, 5, 10, 20, 60}:
        raise ValueError("completed thesis has an invalid primary horizon")
    if direction not in {"up", "down", "rangebound"}:
        raise ValueError("completed thesis has an invalid direction")
    start = date.fromisoformat(case_id)
    prices = sorted(
        (
            item
            for item in series.rows
            if date.fromisoformat(cast(str, item["trade_date"])) >= start
        ),
        key=lambda item: cast(str, item["trade_date"]),
    )[:horizon]
    if len(prices) != horizon or date.fromisoformat(cast(str, prices[0]["trade_date"])) != start:
        raise ValueError(f"price panel cannot satisfy {case_id} horizon {horizon}")
    opening = Decimal(str(prices[0]["open"]))
    if opening <= 0:
        raise ValueError("opening price must be positive")
    path = (Decimal(1), *(Decimal(str(item["close"])) / opening for item in prices))
    realized_return = path[-1] - 1
    sign = Decimal(1) if direction == "up" else Decimal(-1) if direction == "down" else Decimal(0)
    signed_return = sign * realized_return
    adverse = (
        min(path) - 1
        if direction == "up"
        else 1 - max(path)
        if direction == "down"
        else -max(abs(value - 1) for value in path)
    )
    peak = path[0]
    drawdown = Decimal(0)
    for value in path:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1)
    return {
        "run_id": row["run_id"],
        "case_id": case_id,
        "topology": row["topology"],
        "repetition": row["repetition"],
        "date_presentation": row.get("date_presentation", "true_date"),
        "status": "scored",
        "direction": direction,
        "primary_horizon_sessions": horizon,
        "window": [cast(str, prices[0]["trade_date"]), cast(str, prices[-1]["trade_date"])],
        "daily_path": [
            {
                "session_offset": offset,
                "trade_date": cast(str, item["trade_date"]),
                "open": str(item["open"]),
                "close": str(item["close"]),
                "close_return_from_initial_open": str(Decimal(str(item["close"])) / opening - 1),
            }
            for offset, item in enumerate(prices, start=1)
        ],
        "index_return": str(realized_return),
        "signed_return": str(signed_return),
        "directional_adverse_excursion": str(adverse),
        "close_sampled_max_drawdown": str(drawdown),
    }


def _topology_summaries(rows: list[dict[str, object]]) -> dict[str, object]:
    summaries: dict[str, object] = {}
    for topology in ("luna_max", "terra_high", "sol_high"):
        base = [
            row
            for row in rows
            if row.get("status") == "scored"
            and row.get("topology") == topology
            and row.get("repetition") == "base"
        ]
        repeats = {
            cast(str, row["case_id"]): row
            for row in rows
            if row.get("status") == "scored"
            and row.get("topology") == topology
            and row.get("repetition") == "stability-repeat"
        }
        stable = [
            row
            for row in base
            if row["case_id"] in repeats
            and row["direction"] == repeats[cast(str, row["case_id"])]["direction"]
            and row["primary_horizon_sessions"]
            == repeats[cast(str, row["case_id"])]["primary_horizon_sessions"]
        ]
        summaries[topology] = {
            "fixed_denominator": 8,
            "scored_base_count": len(base),
            "signed_return_sum": str(
                sum((Decimal(cast(str, row["signed_return"])) for row in base), Decimal(0))
            ),
            "stability_denominator": 3,
            "stable_direction_and_horizon_count": len(stable),
        }
    return summaries


def _conditional_topology(rows: list[dict[str, object]]) -> dict[str, object]:
    selected: list[dict[str, object]] = []
    cases = sorted(
        {
            cast(str, row["case_id"])
            for row in rows
            if row.get("status") == "scored" and row.get("repetition") == "base"
        }
    )
    for case_id in cases:
        base = {
            cast(str, row["topology"]): row
            for row in rows
            if row.get("status") == "scored"
            and row.get("case_id") == case_id
            and row.get("repetition") == "base"
        }
        judge = next(
            (
                row
                for row in rows
                if row.get("status") == "scored"
                and row.get("case_id") == case_id
                and row.get("repetition") == "conditional-judge"
            ),
            None,
        )
        luna, terra = base.get("luna_max"), base.get("terra_high")
        if luna is None or terra is None:
            continue
        agrees = (
            luna["direction"],
            luna["primary_horizon_sessions"],
        ) == (terra["direction"], terra["primary_horizon_sessions"])
        chosen = luna if agrees else judge
        if chosen is not None:
            selected.append(chosen)
    return {
        "fixed_denominator": 8,
        "scored_count": len(selected),
        "signed_return_sum": str(
            sum((Decimal(cast(str, row["signed_return"])) for row in selected), Decimal(0))
        ),
        "selection_rule": (
            "use the common Luna/Terra conclusion when direction and horizon agree; "
            "otherwise use the preregistered Sol Judge result, never a vote"
        ),
    }


def summarize_memory_sensitivity(rows: list[dict[str, object]]) -> dict[str, object]:
    true_date = next(
        (
            row
            for row in rows
            if row.get("case_id") == "2020-02-03"
            and row.get("topology") == "luna_max"
            and row.get("repetition") == "base"
            and row.get("status") == "scored"
        ),
        None,
    )
    relative = next(
        (
            row
            for row in rows
            if row.get("case_id") == "2020-02-03"
            and row.get("topology") == "luna_max"
            and row.get("repetition") == "memory-sensitivity"
            and row.get("status") == "scored"
        ),
        None,
    )
    return {
        "pair_complete": true_date is not None and relative is not None,
        "direction_and_horizon_match": (
            None
            if true_date is None or relative is None
            else (
                true_date["direction"],
                true_date["primary_horizon_sessions"],
            )
            == (relative["direction"], relative["primary_horizon_sessions"])
        ),
        "inference_scope": "date-label sensitivity only; not proof of training-data leakage",
    }


def _market_series(series: tuple[RegimeSeries, ...], proxy: str) -> RegimeSeries:
    matches = [item for item in series if item.kind == "market" and item.tushare_code == proxy]
    if len(matches) != 1:
        raise ValueError("registered market proxy is not unique in the panel")
    return matches[0]


def _read_object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain an object")
    return cast(dict[str, object], value)


def _verified_report(path: Path) -> dict[str, object]:
    value = _read_object(path)
    core = {key: item for key, item in value.items() if key != "report_hash"}
    if canonical_hash(core) != value.get("report_hash"):
        raise ValueError("dynamic outcome report changed after creation")
    return value


def _write_new(path: Path, value: dict[str, object]) -> None:
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def _string(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be non-empty text")
    return item


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("dynamic outcome timestamp must be timezone-aware")
    return parsed
