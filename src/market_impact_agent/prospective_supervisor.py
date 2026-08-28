from __future__ import annotations

import plistlib
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from market_impact_agent.agent_contracts import canonical_hash

PROSPECTIVE_SUPERVISOR_PLAN_SCHEMA = "market-impact.prospective-supervisor-plan.v1"
_ALLOWED_ENVIRONMENT_KEYS = frozenset({"TUSHARE_TOKEN"})


@dataclass(frozen=True, slots=True)
class ProspectiveSupervisorPlan:
    plan_id: str
    host_name: str
    host_uid: int
    launchd_label: str
    service_definition_path: Path
    executable_path: Path
    working_directory: Path
    state_root: Path
    environment_file: Path
    stdout_path: Path
    stderr_path: Path
    invocation_interval_seconds: int
    maximum_state_bytes: int
    notification_policy: str
    enabled_after_install: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_SUPERVISOR_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_SUPERVISOR_PLAN_SCHEMA:
            raise ValueError("unsupported prospective supervisor plan schema")
        _nonempty(self.host_name, "host name")
        if self.host_uid < 1:
            raise ValueError("host UID must be positive")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]+", self.launchd_label) is None:
            raise ValueError("launchd label contains unsupported characters")
        for name in (
            "executable_path",
            "service_definition_path",
            "working_directory",
            "state_root",
            "environment_file",
            "stdout_path",
            "stderr_path",
        ):
            path = getattr(self, name)
            if not path.is_absolute():
                raise ValueError(f"{name} must be absolute")
        canonical_state_root = _canonical_path(self.state_root, "supervisor state root")
        canonical_environment_file = _canonical_path(
            self.environment_file,
            "supervisor environment file",
        )
        if self.state_root != canonical_state_root:
            raise ValueError("supervisor state root must use its canonical path")
        if self.environment_file != canonical_environment_file:
            raise ValueError("supervisor environment file must use its canonical path")
        if canonical_environment_file.is_relative_to(canonical_state_root):
            raise ValueError("supervisor environment file must stay outside the state root")
        if self.stdout_path == self.stderr_path:
            raise ValueError("supervisor stdout and stderr paths must differ")
        if self.invocation_interval_seconds < 10:
            raise ValueError("supervisor invocation interval must be at least 10 seconds")
        if self.maximum_state_bytes < 1:
            raise ValueError("supervisor maximum state bytes must be positive")
        if self.notification_policy not in {"health_log_only", "failed_runs_only"}:
            raise ValueError("unsupported supervisor notification policy")
        if self.enabled_after_install:
            raise ValueError("a pre-install supervisor plan cannot claim enabled state")
        if self.execution_capability:
            raise ValueError("prospective collection supervisor cannot grant execution capability")
        if self.plan_id != self.expected_plan_id:
            raise ValueError("prospective supervisor plan_id does not match content")
        if self.service_definition_path.name != f"{self.launchd_label}.plist":
            raise ValueError("service definition filename must match the launchd label")

    @property
    def expected_plan_id(self) -> str:
        return f"prospective-supervisor-plan-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "host_name": self.host_name,
            "host_uid": self.host_uid,
            "launchd_label": self.launchd_label,
            "service_definition_path": self.service_definition_path.as_posix(),
            "executable_path": self.executable_path.as_posix(),
            "working_directory": self.working_directory.as_posix(),
            "state_root": self.state_root.as_posix(),
            "environment_file": self.environment_file.as_posix(),
            "stdout_path": self.stdout_path.as_posix(),
            "stderr_path": self.stderr_path.as_posix(),
            "invocation_interval_seconds": self.invocation_interval_seconds,
            "maximum_state_bytes": self.maximum_state_bytes,
            "notification_policy": self.notification_policy,
            "enabled_after_install": self.enabled_after_install,
            "execution_capability": self.execution_capability,
            "install_command": list(self.install_command),
            "enable_command": list(self.enable_command),
            "rollback_commands": [list(item) for item in self.rollback_commands],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}

    @property
    def install_command(self) -> tuple[str, ...]:
        return (
            "launchctl",
            "bootstrap",
            f"gui/{self.host_uid}",
            self.service_definition_path.as_posix(),
        )

    @property
    def enable_command(self) -> tuple[str, ...]:
        return (
            "launchctl",
            "enable",
            f"gui/{self.host_uid}/{self.launchd_label}",
        )

    @property
    def rollback_commands(self) -> tuple[tuple[str, ...], ...]:
        return (
            (
                "launchctl",
                "bootout",
                f"gui/{self.host_uid}",
                self.service_definition_path.as_posix(),
            ),
            (
                "launchctl",
                "disable",
                f"gui/{self.host_uid}/{self.launchd_label}",
            ),
            ("/bin/rm", "--", self.service_definition_path.as_posix()),
        )

    @classmethod
    def build(
        cls,
        *,
        host_name: str,
        host_uid: int,
        launchd_label: str,
        service_definition_path: Path,
        executable_path: Path,
        working_directory: Path,
        state_root: Path,
        environment_file: Path,
        stdout_path: Path,
        stderr_path: Path,
        invocation_interval_seconds: int,
        maximum_state_bytes: int,
        notification_policy: str,
    ) -> ProspectiveSupervisorPlan:
        state_root = _canonical_path(state_root, "supervisor state root")
        environment_file = _canonical_path(
            environment_file,
            "supervisor environment file",
        )
        core = {
            "schema_version": PROSPECTIVE_SUPERVISOR_PLAN_SCHEMA,
            "host_name": host_name,
            "host_uid": host_uid,
            "launchd_label": launchd_label,
            "service_definition_path": service_definition_path.as_posix(),
            "executable_path": executable_path.as_posix(),
            "working_directory": working_directory.as_posix(),
            "state_root": state_root.as_posix(),
            "environment_file": environment_file.as_posix(),
            "stdout_path": stdout_path.as_posix(),
            "stderr_path": stderr_path.as_posix(),
            "invocation_interval_seconds": invocation_interval_seconds,
            "maximum_state_bytes": maximum_state_bytes,
            "notification_policy": notification_policy,
            "enabled_after_install": False,
            "execution_capability": False,
            "install_command": [
                "launchctl",
                "bootstrap",
                f"gui/{host_uid}",
                service_definition_path.as_posix(),
            ],
            "enable_command": [
                "launchctl",
                "enable",
                f"gui/{host_uid}/{launchd_label}",
            ],
            "rollback_commands": [
                [
                    "launchctl",
                    "bootout",
                    f"gui/{host_uid}",
                    service_definition_path.as_posix(),
                ],
                [
                    "launchctl",
                    "disable",
                    f"gui/{host_uid}/{launchd_label}",
                ],
                ["/bin/rm", "--", service_definition_path.as_posix()],
            ],
        }
        return cls(
            plan_id=f"prospective-supervisor-plan-{canonical_hash(core)}",
            host_name=host_name,
            host_uid=host_uid,
            launchd_label=launchd_label,
            service_definition_path=service_definition_path,
            executable_path=executable_path,
            working_directory=working_directory,
            state_root=state_root,
            environment_file=environment_file,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            invocation_interval_seconds=invocation_interval_seconds,
            maximum_state_bytes=maximum_state_bytes,
            notification_policy=notification_policy,
        )


def render_launchd_plist(plan: ProspectiveSupervisorPlan) -> dict[str, object]:
    """Render a secret-free launchd definition for the Harness one-shot worker."""

    return {
        "Label": plan.launchd_label,
        "ProgramArguments": [
            plan.executable_path.as_posix(),
            "data",
            "collection-service-run",
            "--state-root",
            plan.state_root.as_posix(),
            "--environment-file",
            plan.environment_file.as_posix(),
            "--maximum-state-bytes",
            str(plan.maximum_state_bytes),
        ],
        "WorkingDirectory": plan.working_directory.as_posix(),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
        "ProcessType": "Background",
        "StartInterval": plan.invocation_interval_seconds,
        "RunAtLoad": False,
        "Disabled": True,
        "ThrottleInterval": 10,
        "Umask": 63,
        "StandardOutPath": plan.stdout_path.as_posix(),
        "StandardErrorPath": plan.stderr_path.as_posix(),
    }


def render_launchd_plist_bytes(plan: ProspectiveSupervisorPlan) -> bytes:
    return plistlib.dumps(render_launchd_plist(plan), fmt=plistlib.FMT_XML, sort_keys=True)


def load_supervisor_environment(path: Path, *, state_root: Path) -> dict[str, str]:
    """Load the narrow secret boundary without storing values in Harness artifacts."""

    canonical_state_root = _canonical_path(state_root, "supervisor state root")
    canonical_path = _canonical_path(path, "supervisor environment file")
    if canonical_path.is_relative_to(canonical_state_root):
        raise ValueError("supervisor environment file must stay outside the state root")
    if not canonical_path.is_file():
        raise ValueError("supervisor environment file must be a regular file")
    mode = stat.S_IMODE(canonical_path.stat().st_mode)
    if mode & 0o077:
        raise PermissionError("supervisor environment file must use private 0600 permissions")
    result: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        canonical_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"invalid supervisor environment line {line_number}")
        key, value = line.split("=", 1)
        if key not in _ALLOWED_ENVIRONMENT_KEYS:
            raise ValueError(f"unsupported supervisor environment key: {key}")
        if not value or "\x00" in value or "\n" in value:
            raise ValueError(f"invalid supervisor environment value: {key}")
        if key in result:
            raise ValueError(f"duplicate supervisor environment key: {key}")
        result[key] = value
    if "TUSHARE_TOKEN" not in result:
        raise ValueError("supervisor environment is missing TUSHARE_TOKEN")
    return result


def environment_with_supervisor_secrets(
    base: Mapping[str, str],
    secrets: Mapping[str, str],
) -> dict[str, str]:
    environment = dict(base)
    for key in _ALLOWED_ENVIRONMENT_KEYS:
        environment.pop(key, None)
    environment.update(secrets)
    return environment


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip() or "\x00" in value:
        raise ValueError(f"{name} must be non-empty and trimmed")


def _canonical_path(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise ValueError(f"{name} cannot contain symlinks: {current}")
    return path.resolve(strict=False)
