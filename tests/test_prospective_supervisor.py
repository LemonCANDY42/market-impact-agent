import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.prospective_supervisor import (
    ProspectiveSupervisorGate,
    ProspectiveSupervisorGateResult,
    ProspectiveSupervisorPlan,
    ProspectiveSupervisorReceipt,
    assert_clean_supervisor_environment,
    load_supervisor_environment,
    render_launchd_plist,
    write_supervisor_receipt,
)


def _plan(tmp_path: Path) -> ProspectiveSupervisorPlan:
    return ProspectiveSupervisorPlan.build(
        host_name="research-mac",
        host_uid=501,
        launchd_label="com.lemoncandy42.market-impact-agent.collection",
        service_definition_path=(
            tmp_path / "Library/LaunchAgents/com.lemoncandy42.market-impact-agent.collection.plist"
        ),
        executable_path=tmp_path / "repo/.venv/bin/market-impact",
        working_directory=tmp_path / "repo",
        state_root=tmp_path / "state",
        environment_file=tmp_path / "config/collection.env",
        stdout_path=tmp_path / "logs/collection.log",
        stderr_path=tmp_path / "logs/collection.err.log",
        invocation_interval_seconds=60,
        maximum_state_bytes=10_000_000_000,
        notification_policy="health_log_only",
    )


def test_supervisor_plan_is_content_identified_schema_valid_and_secret_free(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)

    assert plan.plan_id == plan.expected_plan_id
    assert validate_agent_contract(plan.to_dict(), "prospective-supervisor-plan.schema.json") == ()

    plist = render_launchd_plist(plan)
    assert plist["Label"] == plan.launchd_label
    assert plist["ProgramArguments"] == [
        "/usr/bin/env",
        "-i",
        "PATH=/usr/bin:/bin:/usr/sbin:/sbin",
        "PYTHONUNBUFFERED=1",
        plan.executable_path.as_posix(),
        "data",
        "collection-service-run",
        "--state-root",
        plan.state_root.as_posix(),
        "--environment-file",
        plan.environment_file.as_posix(),
        "--maximum-state-bytes",
        "10000000000",
        "--maximum-concurrent-opportunities",
        "4",
        "--require-clean-environment",
    ]
    assert plist["StartInterval"] == 60
    assert plist["RunAtLoad"] is False
    assert plist["Disabled"] is True
    assert plist["ProcessType"] == "Background"
    assert "EnvironmentVariables" not in plist
    assert plan.process_environment_isolation == "clear_then_allowlist"
    assert plan.process_environment == {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "PYTHONUNBUFFERED": "1",
    }
    assert "TUSHARE_TOKEN" not in repr(plist)
    assert plan.execution_capability is False
    assert plan.maximum_concurrent_opportunities == 4
    assert plan.disabled_install_commands == (
        (
            "launchctl",
            "disable",
            "gui/501/com.lemoncandy42.market-impact-agent.collection",
        ),
    )
    assert plan.activation_commands == (
        (
            "launchctl",
            "enable",
            "gui/501/com.lemoncandy42.market-impact-agent.collection",
        ),
        (
            "launchctl",
            "bootstrap",
            "gui/501",
            plan.service_definition_path.as_posix(),
        ),
    )
    assert plan.rollback_commands == (
        (
            "launchctl",
            "bootout",
            "gui/501",
            plan.service_definition_path.as_posix(),
        ),
        (
            "launchctl",
            "disable",
            "gui/501/com.lemoncandy42.market-impact-agent.collection",
        ),
        ("/bin/rm", "--", plan.service_definition_path.as_posix()),
    )


def test_supervisor_plan_rejects_relative_or_overlapping_sensitive_paths(tmp_path: Path) -> None:
    plan = _plan(tmp_path)

    with pytest.raises(ValueError, match="absolute"):
        ProspectiveSupervisorPlan.build(
            host_name=plan.host_name,
            host_uid=plan.host_uid,
            launchd_label=plan.launchd_label,
            service_definition_path=plan.service_definition_path,
            executable_path=Path(".venv/bin/market-impact"),
            working_directory=plan.working_directory,
            state_root=plan.state_root,
            environment_file=plan.environment_file,
            stdout_path=plan.stdout_path,
            stderr_path=plan.stderr_path,
            invocation_interval_seconds=60,
            maximum_state_bytes=plan.maximum_state_bytes,
            notification_policy=plan.notification_policy,
        )

    with pytest.raises(ValueError, match="outside the state root"):
        ProspectiveSupervisorPlan.build(
            host_name=plan.host_name,
            host_uid=plan.host_uid,
            launchd_label=plan.launchd_label,
            service_definition_path=plan.service_definition_path,
            executable_path=plan.executable_path,
            working_directory=plan.working_directory,
            state_root=plan.state_root,
            environment_file=plan.state_root / "collection.env",
            stdout_path=plan.stdout_path,
            stderr_path=plan.stderr_path,
            invocation_interval_seconds=60,
            maximum_state_bytes=plan.maximum_state_bytes,
            notification_policy=plan.notification_policy,
        )


def test_supervisor_environment_requires_private_permissions_and_known_keys(
    tmp_path: Path,
) -> None:
    path = tmp_path / "collection.env"
    path.write_text("TUSHARE_TOKEN=secret-value\n", encoding="utf-8")
    path.chmod(0o600)

    assert load_supervisor_environment(path, state_root=tmp_path / "state") == {
        "TUSHARE_TOKEN": "secret-value"
    }

    path.chmod(0o644)
    with pytest.raises(PermissionError, match="0600"):
        load_supervisor_environment(path, state_root=tmp_path / "state")

    path.chmod(0o600)
    path.write_text("TUSHARE_TOKEN=secret-value\nUNEXPECTED=value\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported"):
        load_supervisor_environment(path, state_root=tmp_path / "state")


def test_supervisor_environment_accepts_registered_shell_export_format(tmp_path: Path) -> None:
    path = tmp_path / "secrets.env"
    path.write_text("export TUSHARE_TOKEN=secret-value\n", encoding="utf-8")
    path.chmod(0o600)

    assert load_supervisor_environment(path, state_root=tmp_path / "state") == {
        "TUSHARE_TOKEN": "secret-value"
    }


def test_supervisor_process_environment_must_be_clean_and_allowlisted() -> None:
    assert_clean_supervisor_environment(
        {
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            "PYTHONUNBUFFERED": "1",
            "LC_CTYPE": "UTF-8",
            "__CF_USER_TEXT_ENCODING": "0x1F7:0x0:0x0",
        }
    )

    with pytest.raises(RuntimeError, match="not isolated"):
        assert_clean_supervisor_environment(
            {
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "PYTHONUNBUFFERED": "1",
                "UNRELATED_SECRET": "must-not-be-visible",
            }
        )


@pytest.mark.parametrize("reason", ("", " leading", "trailing ", "nul\x00reason"))
def test_supervisor_gate_reasons_must_be_nonempty_trimmed_and_nul_free(reason: str) -> None:
    with pytest.raises(ValueError, match="reason"):
        ProspectiveSupervisorGateResult(
            gate=ProspectiveSupervisorGate.DISABLED_INSTALL,
            passed=False,
            reasons=(reason,),
        )


def test_supervisor_receipt_is_content_identified_schema_valid_and_private(
    tmp_path: Path,
) -> None:
    gates = tuple(
        ProspectiveSupervisorGateResult(gate=gate, passed=True, reasons=())
        for gate in ProspectiveSupervisorGate
    )
    receipt = ProspectiveSupervisorReceipt.build(
        accepted_at=datetime(2026, 8, 28, 14, 30, tzinfo=UTC),
        supervisor_plan_id="prospective-supervisor-plan-" + "a" * 64,
        source_commit="b" * 40,
        host_name="research-mac",
        host_uid=501,
        launchd_label="com.lemoncandy42.market-impact-agent.collection",
        service_definition_hash="c" * 64,
        runtime_evidence_hash="d" * 64,
        machine_registry_hash="e" * 64,
        observed_successful_run_count=4,
        gates=gates,
    )

    assert receipt.receipt_id == receipt.expected_receipt_id
    assert receipt.accepted is True
    assert (
        validate_agent_contract(receipt.to_dict(), "prospective-supervisor-receipt.schema.json")
        == ()
    )
    path = write_supervisor_receipt(receipt, state_root=tmp_path / "state")
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text(encoding="utf-8")) == receipt.to_dict()
    assert "secret" not in path.read_text(encoding="utf-8").lower()

    path.chmod(0o644)
    assert write_supervisor_receipt(receipt, state_root=tmp_path / "state") == path
    assert path.stat().st_mode & 0o777 == 0o600

    authoritative_state = tmp_path / "authoritative-state"
    authoritative_state.mkdir()
    linked_state = tmp_path / "linked-state"
    linked_state.symlink_to(authoritative_state, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        write_supervisor_receipt(receipt, state_root=linked_state)

    state_with_linked_parent = tmp_path / "state-with-linked-parent"
    state_with_linked_parent.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (state_with_linked_parent / "operations").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        write_supervisor_receipt(receipt, state_root=state_with_linked_parent)

    state_with_linked_receipt = tmp_path / "state-with-linked-receipt"
    receipt_root = state_with_linked_receipt / "operations" / "supervisor-receipts"
    receipt_root.mkdir(parents=True)
    outside_receipt = outside / "receipt.json"
    outside_receipt.write_bytes(path.read_bytes())
    linked_receipt = receipt_root / f"{receipt.receipt_id}.json"
    linked_receipt.symlink_to(outside_receipt)
    with pytest.raises(ValueError, match="symlink"):
        write_supervisor_receipt(receipt, state_root=state_with_linked_receipt)


def test_supervisor_rejects_environment_paths_resolving_inside_state_root(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir()
    secret = state_root / "collection.env"
    secret.write_text("TUSHARE_TOKEN=secret-value\n", encoding="utf-8")
    secret.chmod(0o600)
    linked_parent = tmp_path / "linked-config"
    linked_parent.symlink_to(state_root, target_is_directory=True)
    plan = _plan(tmp_path)

    with pytest.raises(ValueError, match=r"symlink|outside the state root"):
        ProspectiveSupervisorPlan.build(
            host_name=plan.host_name,
            host_uid=plan.host_uid,
            launchd_label=plan.launchd_label,
            service_definition_path=plan.service_definition_path,
            executable_path=plan.executable_path,
            working_directory=plan.working_directory,
            state_root=state_root,
            environment_file=linked_parent / "collection.env",
            stdout_path=plan.stdout_path,
            stderr_path=plan.stderr_path,
            invocation_interval_seconds=plan.invocation_interval_seconds,
            maximum_state_bytes=plan.maximum_state_bytes,
            notification_policy=plan.notification_policy,
        )

    with pytest.raises(ValueError, match=r"symlink|outside the state root"):
        load_supervisor_environment(linked_parent / "collection.env", state_root=state_root)


def test_supervisor_environment_rejects_final_and_ancestor_symlinks(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir()
    config = tmp_path / "config"
    config.mkdir()
    secret = config / "collection.env"
    secret.write_text("TUSHARE_TOKEN=secret-value\n", encoding="utf-8")
    secret.chmod(0o600)
    final_link = tmp_path / "collection.env"
    final_link.symlink_to(secret)
    parent_link = tmp_path / "config-link"
    parent_link.symlink_to(config, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        load_supervisor_environment(final_link, state_root=state_root)
    with pytest.raises(ValueError, match="symlink"):
        load_supervisor_environment(parent_link / "collection.env", state_root=state_root)
