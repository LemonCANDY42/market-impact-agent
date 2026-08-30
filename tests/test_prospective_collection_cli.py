from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

import market_impact_agent.cli as cli_module
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
            "--maximum-state-bytes",
            "10000000000",
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
    service = build_parser().parse_args(
        [
            "data",
            "collection-service-run",
            "--state-root",
            "/tmp/market-impact-state",
            "--environment-file",
            "/tmp/market-impact.env",
            "--maximum-state-bytes",
            "10000000000",
        ]
    )
    supervisor = build_parser().parse_args(
        [
            "data",
            "collection-supervisor-plan",
            "--host-name",
            "research-mac",
            "--host-uid",
            "501",
            "--service-definition-path",
            "/tmp/Library/LaunchAgents/com.lemoncandy42.market-impact-agent.collection.plist",
            "--executable-path",
            "/tmp/repo/.venv/bin/market-impact",
            "--working-directory",
            "/tmp/repo",
            "--state-root",
            "/tmp/state",
            "--environment-file",
            "/tmp/config/collection.env",
            "--stdout-path",
            "/tmp/logs/collection.log",
            "--stderr-path",
            "/tmp/logs/collection.err.log",
            "--maximum-state-bytes",
            "10000000000",
        ]
    )

    assert register.data_command == "collection-register"
    assert register.adapter_kind == "tushare_observation"
    assert register.poll_interval_seconds == 86400
    assert register.maximum_jitter_seconds == 0
    assert run.data_command == "collection-run-due"
    assert run.now == datetime(2026, 8, 28, 14, 5, tzinfo=UTC)
    assert run.maximum_state_bytes == 10_000_000_000
    assert tracer.data_command == "collection-qualify-tracer"
    assert len(tracer.job_id) == 2
    assert service.data_command == "collection-service-run"
    assert service.environment_file == Path("/tmp/market-impact.env")
    assert service.maximum_state_bytes == 10_000_000_000
    assert supervisor.data_command == "collection-supervisor-plan"
    assert supervisor.invocation_interval_seconds == 60


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

    run_exit_code = main(
        [
            "data",
            "collection-run-due",
            "--state-root",
            (tmp_path / "state").as_posix(),
            "--now",
            "2026-08-28T14:05:00Z",
            "--maximum-state-bytes",
            "10000000000",
        ]
    )
    run_output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert run_exit_code == 0
    assert run_output["job_count"] == 0
    assert run_output["execution_capability"] is False

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


def test_service_worker_loads_token_from_private_file_without_echoing_it(
    tmp_path: Path,
    capsys: object,
) -> None:
    environment_file = tmp_path / "collection.env"
    environment_file.write_text("TUSHARE_TOKEN=do-not-print-me\n", encoding="utf-8")
    environment_file.chmod(0o600)

    exit_code = main(
        [
            "data",
            "collection-service-run",
            "--state-root",
            (tmp_path / "state").as_posix(),
            "--environment-file",
            environment_file.as_posix(),
            "--maximum-state-bytes",
            "10000000000",
        ]
    )
    output_text = cast(str, capsys.readouterr().out)  # type: ignore[attr-defined]
    output = json.loads(output_text)

    assert exit_code == 0
    assert output["job_count"] == 0
    assert output["service_environment_loaded"] is True
    assert "do-not-print-me" not in output_text


def test_service_worker_forwards_state_budget_to_due_worker(
    tmp_path: Path,
    capsys: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment_file = tmp_path / "collection.env"
    environment_file.write_text("TUSHARE_TOKEN=do-not-print-me\n", encoding="utf-8")
    environment_file.chmod(0o600)
    received: dict[str, Any] = {}

    def fake_run_due(**kwargs: Any) -> dict[str, object]:
        received.update(kwargs)
        return {"job_count": 0, "results": [], "execution_capability": False}

    monkeypatch.setattr(cli_module, "run_due_prospective_collection_jobs", fake_run_due)

    exit_code = main(
        [
            "data",
            "collection-service-run",
            "--state-root",
            (tmp_path / "state").as_posix(),
            "--environment-file",
            environment_file.as_posix(),
            "--maximum-state-bytes",
            "123456",
        ]
    )
    capsys.readouterr()  # type: ignore[attr-defined]

    assert exit_code == 0
    assert received["maximum_state_bytes"] == 123456


def test_interactive_due_worker_forwards_required_state_budget(
    tmp_path: Path,
    capsys: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, Any] = {}

    def fake_run_due(**kwargs: Any) -> dict[str, object]:
        received.update(kwargs)
        return {"job_count": 0, "results": [], "execution_capability": False}

    monkeypatch.setattr(cli_module, "run_due_prospective_collection_jobs", fake_run_due)

    exit_code = main(
        [
            "data",
            "collection-run-due",
            "--state-root",
            (tmp_path / "state").as_posix(),
            "--maximum-state-bytes",
            "654321",
        ]
    )
    capsys.readouterr()  # type: ignore[attr-defined]

    assert exit_code == 0
    assert received["maximum_state_bytes"] == 654321


@pytest.mark.parametrize("command", ["collection-run-due", "collection-service-run"])
def test_due_workers_accept_healthy_terminal_no_data(
    tmp_path: Path,
    capsys: object,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    def fake_run_due(**_kwargs: Any) -> dict[str, object]:
        return {
            "job_count": 1,
            "results": [{"outcome": "no_data"}],
            "execution_capability": False,
        }

    monkeypatch.setattr(cli_module, "run_due_prospective_collection_jobs", fake_run_due)
    arguments = [
        "data",
        command,
        "--state-root",
        (tmp_path / "state").as_posix(),
        "--maximum-state-bytes",
        "654321",
    ]
    if command == "collection-service-run":
        environment_file = tmp_path / "collection.env"
        environment_file.write_text("TUSHARE_TOKEN=do-not-print-me\n", encoding="utf-8")
        environment_file.chmod(0o600)
        arguments.extend(["--environment-file", environment_file.as_posix()])

    exit_code = main(arguments)
    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert exit_code == 0
    assert output["results"] == [{"outcome": "no_data"}]


def test_due_workers_reject_nonpositive_state_budget_without_jobs(
    tmp_path: Path,
    capsys: object,
) -> None:
    exit_code = main(
        [
            "data",
            "collection-run-due",
            "--state-root",
            (tmp_path / "state").as_posix(),
            "--maximum-state-bytes",
            "0",
        ]
    )
    error = json.loads(capsys.readouterr().err)  # type: ignore[attr-defined]

    assert exit_code == 1
    assert "positive" in error["error"]


def test_state_backup_verify_and_restore_cli_round_trip(
    tmp_path: Path,
    capsys: object,
) -> None:
    state_root = tmp_path / "state"
    from market_impact_agent.data_inputs import LocalDataSnapshotStore

    LocalDataSnapshotStore(state_root).put_raw(b"receipt")
    backup_parent = tmp_path / "backups"

    backup_exit = main(
        [
            "data",
            "state-backup",
            "--state-root",
            state_root.as_posix(),
            "--backup-parent",
            backup_parent.as_posix(),
        ]
    )
    backup_output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert backup_exit == 0

    verify_exit = main(["data", "state-verify-backup", "--backup", backup_output["backup_path"]])
    verify_output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert verify_exit == 0
    assert verify_output["verified"] is True

    restore_exit = main(
        [
            "data",
            "state-restore",
            "--backup",
            backup_output["backup_path"],
            "--destination",
            (tmp_path / "restored").as_posix(),
        ]
    )
    restore_output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert restore_exit == 0
    assert restore_output["restored"] is True
    assert restore_output["manifest_id"] == backup_output["manifest_id"]
