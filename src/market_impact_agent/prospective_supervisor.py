from __future__ import annotations

import os
import plistlib
import re
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes

PROSPECTIVE_SUPERVISOR_PLAN_SCHEMA = "market-impact.prospective-supervisor-plan.v3"
PROSPECTIVE_SUPERVISOR_RECEIPT_SCHEMA = "market-impact.prospective-supervisor-receipt.v1"
_ALLOWED_ENVIRONMENT_KEYS = frozenset({"TUSHARE_TOKEN"})
_CLEAN_PROCESS_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
    "PYTHONUNBUFFERED": "1",
}
_ALLOWED_SYSTEM_PROCESS_ENVIRONMENT_KEYS = frozenset({"LC_CTYPE", "__CF_USER_TEXT_ENCODING"})


class ProspectiveSupervisorGate(StrEnum):
    DISABLED_INSTALL = "disabled_install"
    ENVIRONMENT_ISOLATION = "environment_isolation"
    SERVICE_LIFECYCLE = "service_lifecycle"
    FAILURE_RECOVERY = "failure_recovery"
    HEALTH_VISIBILITY = "health_visibility"
    LOG_REDACTION = "log_redaction"
    ROLLBACK_REINSTALL = "rollback_reinstall"
    MACHINE_REGISTRY = "machine_registry"


@dataclass(frozen=True, slots=True)
class ProspectiveSupervisorGateResult:
    gate: ProspectiveSupervisorGate
    passed: bool
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        for reason in self.reasons:
            _nonempty(reason, "supervisor gate reason")
        if self.passed and self.reasons:
            raise ValueError("passing supervisor gate cannot carry reasons")
        if not self.passed and not self.reasons:
            raise ValueError("failing supervisor gate requires reasons")
        if len(self.reasons) != len(set(self.reasons)):
            raise ValueError("supervisor gate reasons must be unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "gate": self.gate.value,
            "passed": self.passed,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveSupervisorReceipt:
    receipt_id: str
    accepted_at: datetime
    supervisor_plan_id: str
    source_commit: str
    host_name: str
    host_uid: int
    launchd_label: str
    service_definition_hash: str
    runtime_evidence_hash: str
    machine_registry_hash: str
    observed_successful_run_count: int
    gates: tuple[ProspectiveSupervisorGateResult, ...]
    accepted: bool
    historical_pit_claim: bool = False
    model_authority: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_SUPERVISOR_RECEIPT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_SUPERVISOR_RECEIPT_SCHEMA:
            raise ValueError("unsupported prospective supervisor receipt schema")
        _strict_utc(self.accepted_at, "supervisor receipt accepted_at")
        if (
            re.fullmatch(r"prospective-supervisor-plan-[0-9a-f]{64}", self.supervisor_plan_id)
            is None
        ):
            raise ValueError("supervisor receipt plan ID is invalid")
        if re.fullmatch(r"[0-9a-f]{40}", self.source_commit) is None:
            raise ValueError("supervisor receipt source commit is invalid")
        _nonempty(self.host_name, "supervisor receipt host name")
        if self.host_uid < 1:
            raise ValueError("supervisor receipt host UID must be positive")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]+", self.launchd_label) is None:
            raise ValueError("supervisor receipt launchd label is invalid")
        for value, name in (
            (self.service_definition_hash, "service definition hash"),
            (self.runtime_evidence_hash, "runtime evidence hash"),
            (self.machine_registry_hash, "machine registry hash"),
        ):
            _sha256(value, name)
        if self.observed_successful_run_count < 1:
            raise ValueError("supervisor receipt requires at least one successful run")
        if tuple(item.gate for item in self.gates) != tuple(ProspectiveSupervisorGate):
            raise ValueError("supervisor receipt gates are incomplete or out of order")
        if self.accepted != all(item.passed for item in self.gates):
            raise ValueError("supervisor receipt accepted flag does not match its gates")
        if self.historical_pit_claim or self.model_authority or self.execution_capability:
            raise ValueError("supervisor receipt cannot grant PIT, model, or execution authority")
        if self.receipt_id != self.expected_receipt_id:
            raise ValueError("supervisor receipt ID does not match content")

    @property
    def expected_receipt_id(self) -> str:
        return f"prospective-supervisor-receipt-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "accepted_at": _timestamp(self.accepted_at),
            "supervisor_plan_id": self.supervisor_plan_id,
            "source_commit": self.source_commit,
            "host_name": self.host_name,
            "host_uid": self.host_uid,
            "launchd_label": self.launchd_label,
            "service_definition_hash": self.service_definition_hash,
            "runtime_evidence_hash": self.runtime_evidence_hash,
            "machine_registry_hash": self.machine_registry_hash,
            "observed_successful_run_count": self.observed_successful_run_count,
            "gates": [item.to_dict() for item in self.gates],
            "accepted": self.accepted,
            "historical_pit_claim": self.historical_pit_claim,
            "model_authority": self.model_authority,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "receipt_id": self.receipt_id}

    @classmethod
    def build(
        cls,
        *,
        accepted_at: datetime,
        supervisor_plan_id: str,
        source_commit: str,
        host_name: str,
        host_uid: int,
        launchd_label: str,
        service_definition_hash: str,
        runtime_evidence_hash: str,
        machine_registry_hash: str,
        observed_successful_run_count: int,
        gates: tuple[ProspectiveSupervisorGateResult, ...],
    ) -> ProspectiveSupervisorReceipt:
        core: dict[str, object] = {
            "schema_version": PROSPECTIVE_SUPERVISOR_RECEIPT_SCHEMA,
            "accepted_at": _timestamp(accepted_at),
            "supervisor_plan_id": supervisor_plan_id,
            "source_commit": source_commit,
            "host_name": host_name,
            "host_uid": host_uid,
            "launchd_label": launchd_label,
            "service_definition_hash": service_definition_hash,
            "runtime_evidence_hash": runtime_evidence_hash,
            "machine_registry_hash": machine_registry_hash,
            "observed_successful_run_count": observed_successful_run_count,
            "gates": [item.to_dict() for item in gates],
            "accepted": all(item.passed for item in gates),
            "historical_pit_claim": False,
            "model_authority": False,
            "execution_capability": False,
        }
        return cls(
            receipt_id=f"prospective-supervisor-receipt-{canonical_hash(core)}",
            accepted_at=accepted_at,
            supervisor_plan_id=supervisor_plan_id,
            source_commit=source_commit,
            host_name=host_name,
            host_uid=host_uid,
            launchd_label=launchd_label,
            service_definition_hash=service_definition_hash,
            runtime_evidence_hash=runtime_evidence_hash,
            machine_registry_hash=machine_registry_hash,
            observed_successful_run_count=observed_successful_run_count,
            gates=gates,
            accepted=all(item.passed for item in gates),
        )


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
            "process_environment_isolation": self.process_environment_isolation,
            "process_environment": self.process_environment,
            "enabled_after_install": self.enabled_after_install,
            "execution_capability": self.execution_capability,
            "disabled_install_commands": [list(item) for item in self.disabled_install_commands],
            "activation_commands": [list(item) for item in self.activation_commands],
            "rollback_commands": [list(item) for item in self.rollback_commands],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}

    @property
    def process_environment_isolation(self) -> str:
        return "clear_then_allowlist"

    @property
    def process_environment(self) -> dict[str, str]:
        return dict(_CLEAN_PROCESS_ENVIRONMENT)

    @property
    def disabled_install_commands(self) -> tuple[tuple[str, ...], ...]:
        return (
            (
                "launchctl",
                "disable",
                f"gui/{self.host_uid}/{self.launchd_label}",
            ),
        )

    @property
    def activation_commands(self) -> tuple[tuple[str, ...], ...]:
        return (
            (
                "launchctl",
                "enable",
                f"gui/{self.host_uid}/{self.launchd_label}",
            ),
            (
                "launchctl",
                "bootstrap",
                f"gui/{self.host_uid}",
                self.service_definition_path.as_posix(),
            ),
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
            "process_environment_isolation": "clear_then_allowlist",
            "process_environment": dict(_CLEAN_PROCESS_ENVIRONMENT),
            "enabled_after_install": False,
            "execution_capability": False,
            "disabled_install_commands": [
                [
                    "launchctl",
                    "disable",
                    f"gui/{host_uid}/{launchd_label}",
                ],
            ],
            "activation_commands": [
                [
                    "launchctl",
                    "enable",
                    f"gui/{host_uid}/{launchd_label}",
                ],
                [
                    "launchctl",
                    "bootstrap",
                    f"gui/{host_uid}",
                    service_definition_path.as_posix(),
                ],
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
            "/usr/bin/env",
            "-i",
            *(f"{key}={value}" for key, value in plan.process_environment.items()),
            plan.executable_path.as_posix(),
            "data",
            "collection-service-run",
            "--state-root",
            plan.state_root.as_posix(),
            "--environment-file",
            plan.environment_file.as_posix(),
            "--maximum-state-bytes",
            str(plan.maximum_state_bytes),
            "--require-clean-environment",
        ],
        "WorkingDirectory": plan.working_directory.as_posix(),
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


def write_supervisor_receipt(
    receipt: ProspectiveSupervisorReceipt,
    *,
    state_root: Path,
) -> Path:
    canonical_state_root = _canonical_path(state_root, "supervisor receipt state root")
    root = _canonical_path(
        canonical_state_root / "operations" / "supervisor-receipts",
        "supervisor receipt directory",
    )
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root = _canonical_path(root, "supervisor receipt directory")
    path = _canonical_path(
        root / f"{receipt.receipt_id}.json",
        "supervisor receipt path",
    )
    if not path.is_relative_to(canonical_state_root):
        raise ValueError("supervisor receipt must stay within the authoritative state root")
    payload = canonical_json_bytes(receipt.to_dict())
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("supervisor receipt identity has conflicting content")
        path.chmod(0o600)
        return path
    descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-supervisor-receipt-", dir=root)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        return path
    finally:
        temporary.unlink(missing_ok=True)


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
        if line.startswith("export "):
            line = line.removeprefix("export ")
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


def assert_clean_supervisor_environment(environment: Mapping[str, str]) -> None:
    """Fail closed unless launchd cleared inherited host secrets before Python started."""

    unexpected_keys = (
        set(environment)
        - set(_CLEAN_PROCESS_ENVIRONMENT)
        - set(_ALLOWED_SYSTEM_PROCESS_ENVIRONMENT_KEYS)
    )
    if (
        any(environment.get(key) != value for key, value in _CLEAN_PROCESS_ENVIRONMENT.items())
        or unexpected_keys
    ):
        raise RuntimeError("supervisor process environment is not isolated")


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


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _strict_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _canonical_path(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise ValueError(f"{name} cannot contain symlinks: {current}")
    return path.resolve(strict=False)
