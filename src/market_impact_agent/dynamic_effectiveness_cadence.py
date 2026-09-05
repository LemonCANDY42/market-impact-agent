"""Zero-cost evidence preflight for continuous-review experiments."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.decision_thesis import ReviewCadence, ReviewScheduleV1
from market_impact_agent.dynamic_effectiveness_runner import load_dynamic_effectiveness_study
from market_impact_agent.market_regimes import RegimeSeries, validate_regime_panel

_CADENCE_CASES = (
    (
        "shock_reversal_1",
        (
            "cn-2020-covid-closure-shock/2020-02-03",
            "cn-2020-covid-closure-shock/2020-03-02",
            "cn-2020-covid-closure-shock/2020-03-23",
        ),
    ),
    (
        "shock_reversal_2",
        (
            "cn-2024-post-rally-whipsaw/2024-10-09",
            "cn-2024-post-rally-whipsaw/2024-11-18",
            "cn-2024-post-rally-whipsaw/2024-12-30",
        ),
    ),
    (
        "trend_continuation",
        (
            "cn-2024-policy-melt-up/2024-09-24",
            "cn-2024-policy-melt-up/2024-09-30",
            "cn-2024-policy-melt-up/2024-10-08",
        ),
    ),
    (
        "state_transition_or_noise",
        (
            "cn-2019-q1-fast-rebound/2019-01-07",
            "cn-2019-q1-fast-rebound/2019-02-25",
            "cn-2019-q1-fast-rebound/2019-04-08",
        ),
    ),
)


def preflight_cadence_evidence(
    root: Path,
    *,
    inputs_root: Path,
    panel_directory: Path,
    index_proxy: str,
) -> dict[str, object]:
    """Prove whether exact scheduled review points exist before spending model budget."""

    destination = root / "cadence-evidence-preflight.json"
    if destination.is_file():
        return _verified(destination)
    registration = load_dynamic_effectiveness_study(root)
    panel = validate_regime_panel(panel_directory)
    series = _market_series(panel.panel.series, index_proxy)
    sessions = [date.fromisoformat(cast(str, row["trade_date"])) for row in series.rows]
    session_index = {value: index for index, value in enumerate(sessions)}
    required_model_review_offsets = sorted(
        {
            point.session_offset
            for horizon in (1, 3, 5, 10, 20, 60)
            for point in ReviewScheduleV1.build(
                root_event_id="dynamic-cadence-preflight",
                thesis_epoch="dynamic-cadence-preflight-v1",
                primary_horizon_sessions=horizon,
                cadence=ReviewCadence.SCHEDULED,
                future_trading_sessions=tuple(
                    date(2030, 1, 1) + timedelta(days=offset) for offset in range(1, 61)
                ),
                created_at=datetime(2030, 1, 1, tzinfo=UTC),
            ).review_points
        }
    )
    required_continuous_offsets = list(range(1, 61))
    cases: list[dict[str, object]] = []
    for role, refs in _CADENCE_CASES:
        snapshots = [_snapshot(inputs_root / ref, ref) for ref in refs]
        initial_date = date.fromisoformat(cast(str, snapshots[0]["case_id"]))
        if initial_date not in session_index:
            raise ValueError(f"initial cadence date is missing from market panel: {initial_date}")
        initial_index = session_index[initial_date]
        observed_offsets: list[int] = []
        for snapshot in snapshots[1:]:
            current = date.fromisoformat(cast(str, snapshot["case_id"]))
            if current not in session_index or session_index[current] <= initial_index:
                raise ValueError("cadence successor must be a later market session")
            observed_offsets.append(session_index[current] - initial_index)
        missing_model_reviews = [
            offset for offset in required_model_review_offsets if offset not in observed_offsets
        ]
        missing_continuous = [
            offset for offset in required_continuous_offsets if offset not in observed_offsets
        ]
        cases.append(
            {
                "role": role,
                "initial": snapshots[0],
                "successors": snapshots[1:],
                "observed_successor_offsets": observed_offsets,
                "required_continuous_session_offsets": required_continuous_offsets,
                "missing_continuous_session_offsets": missing_continuous,
                "required_model_review_offsets": required_model_review_offsets,
                "missing_model_review_offsets": missing_model_reviews,
                "one_shot_ready": not missing_continuous,
                "continuous_daily_path_ready": not missing_continuous,
                "scheduled_review_ready": not missing_model_reviews and not missing_continuous,
                "event_driven_candidate_count": len(observed_offsets),
                "event_driven_within_wake_limit": len(observed_offsets) <= 3,
            }
        )
    scheduled_ready = all(cast(bool, item["scheduled_review_ready"]) for item in cases)
    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-cadence-evidence-preflight.v1",
        "registration_hash": registration["registration_hash"],
        "panel_id": panel.panel_id,
        "panel_hash": panel.panel_hash,
        "index_proxy": index_proxy,
        "cases": cases,
        "one_shot_ready": all(cast(bool, item["continuous_daily_path_ready"]) for item in cases),
        "scheduled_review_ready": scheduled_ready,
        "event_driven_candidate_count_within_wake_limit": all(
            cast(bool, item["event_driven_within_wake_limit"]) for item in cases
        ),
        "event_driven_run_ready": scheduled_ready,
        "paid_cadence_run_admitted": scheduled_ready,
        "model_requests": 0,
        "blocker": (
            None
            if scheduled_ready
            else (
                "the full D1-through-D60 Modeled-PIT market/account path is not available; "
                "sparse checkpoints cannot be relabeled as continuous scheduled trading"
            )
        ),
        "live_execution": False,
    }
    report["report_hash"] = canonical_hash(report)
    _write_new(destination, report)
    return report


def _snapshot(path: Path, ref: str) -> dict[str, object]:
    pack_path = path / "evidence-pack.json"
    documents_path = path / "evidence-documents.json"
    raw_pack: object = json.loads(pack_path.read_text(encoding="utf-8"))
    raw_documents: object = json.loads(documents_path.read_text(encoding="utf-8"))
    if not isinstance(raw_pack, dict) or not isinstance(raw_documents, dict):
        raise ValueError("cadence inputs must contain frozen JSON objects")
    pack = cast(dict[str, object], raw_pack)
    documents = cast(dict[str, object], raw_documents)
    case_id = ref.rsplit("/", maxsplit=1)[-1]
    as_of = pack.get("as_of")
    if not isinstance(as_of, str) or not as_of.startswith(case_id):
        raise ValueError("cadence input date and frozen cutoff disagree")
    return {
        "case_id": case_id,
        "input_ref": ref,
        "evidence_pack_hash": canonical_hash(pack),
        "evidence_documents_hash": canonical_hash(documents),
    }


def _market_series(series: tuple[RegimeSeries, ...], proxy: str) -> RegimeSeries:
    matches = [item for item in series if item.kind == "market" and item.tushare_code == proxy]
    if len(matches) != 1:
        raise ValueError("registered market proxy is not unique in the panel")
    return matches[0]


def _verified(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("cadence preflight must be an object")
    report = cast(dict[str, object], value)
    core = {key: item for key, item in report.items() if key != "report_hash"}
    if canonical_hash(core) != report.get("report_hash"):
        raise ValueError("cadence evidence preflight changed after creation")
    return report


def _write_new(path: Path, value: dict[str, object]) -> None:
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
