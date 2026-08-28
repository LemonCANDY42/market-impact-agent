from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from market_impact_agent.cli import (
    build_parser,
    main,
    run_due_prospective_collection_jobs,
)


def test_collection_cli_freezes_registration_and_one_shot_worker_arguments() -> None:
    register = build_parser().parse_args(
        [
            "data",
            "collection-register",
            "--adapter-kind",
            "tushare_observation",
            "--source-config",
            "examples/providers/tushare-observation-index-daily-v1.json",
            "--acceptance-report",
            ".market-impact/data-inputs/source-acceptance/example.json",
            "--parameters-json",
            '{"end_date":"20270828","start_date":"20260828","ts_code":"000300.SH"}',
            "--window-start",
            "2026-08-28T14:00:00Z",
            "--starts-at",
            "2026-08-28T14:05:00Z",
            "--poll-interval-seconds",
            "86400",
            "--maximum-gap-seconds",
            "172800",
            "--misfire-grace-seconds",
            "300",
        ]
    )
    run = build_parser().parse_args(
        [
            "data",
            "collection-run-due",
            "--job-id",
            "prospective-collection-job-" + "a" * 64,
            "--now",
            "2026-08-28T14:05:00Z",
        ]
    )
    tracer = build_parser().parse_args(
        [
            "data",
            "collection-qualify-tracer",
            "--job-id",
            "prospective-collection-job-" + "a" * 64,
            "--job-id",
            "prospective-collection-job-" + "b" * 64,
            "--evaluated-at",
            "2026-08-28T14:10:00Z",
        ]
    )

    assert register.data_command == "collection-register"
    assert register.adapter_kind == "tushare_observation"
    assert register.poll_interval_seconds == 86400
    assert register.maximum_jitter_seconds == 0
    assert run.data_command == "collection-run-due"
    assert run.now == datetime(2026, 8, 28, 14, 5, tzinfo=UTC)
    assert tracer.data_command == "collection-qualify-tracer"
    assert len(tracer.job_id) == 2


def test_one_shot_worker_and_health_are_safe_when_no_jobs_exist(
    tmp_path: Path,
    capsys: object,
) -> None:
    result = run_due_prospective_collection_jobs(
        state_root=tmp_path / "state",
        now=datetime(2026, 8, 28, 14, 5, tzinfo=UTC),
    )

    assert result["job_count"] == 0
    assert result["results"] == []
    assert result["execution_capability"] is False

    exit_code = main(
        [
            "data",
            "collection-health",
            "--state-root",
            (tmp_path / "state").as_posix(),
            "--now",
            "2026-08-28T14:05:00Z",
        ]
    )
    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert exit_code == 0
    assert output["job_count"] == 0
    assert output["health"] == []
    assert output["execution_capability"] is False
