"""Source-checkout deployment and frozen route admission for the pi runtime."""

from __future__ import annotations

import fcntl
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import canonical_hash

if TYPE_CHECKING:
    from market_impact_agent.model_provider import ModelProviderProfile


@dataclass(frozen=True, slots=True)
class PiRuntimePermit:
    build_hash: str
    route_identities: tuple[str, ...]
    evidence_hash: str
    # Empty means accepted production. Acceptance calls instead bind exact Run
    # identities and the single registered, shared parent budget.
    run_ids: tuple[str, ...] = ()
    budget_owner: str | None = None

    def authorize(
        self,
        profile: ModelProviderProfile,
        build: dict[str, object],
        invocation_id: str,
        budget_owner: str,
    ) -> None:
        if (
            self.build_hash != canonical_hash(build)
            or profile.route_identity not in self.route_identities
        ):
            raise PermissionError("pi build/model route has not passed runtime acceptance")
        if self.run_ids and not any(
            invocation_id == run_id or invocation_id.startswith(f"{run_id}.pi-invocation.")
            for run_id in self.run_ids
        ):
            raise PermissionError("runtime acceptance permit does not authorize this Run")
        if self.budget_owner is not None and self.budget_owner != budget_owner:
            raise PermissionError("runtime acceptance request has another budget authority")


def installed_permit(root: Path) -> PiRuntimePermit | None:
    path = root / "accepted-pi-runtime.json"
    if not path.is_file():
        return None
    value = cast(dict[str, object], json.loads(path.read_text()))
    core = {key: item for key, item in value.items() if key != "acceptance_hash"}
    if canonical_hash(core) != value.get("acceptance_hash"):
        raise ValueError("installed pi acceptance record has changed")
    routes = cast(list[str], value["route_identities"])
    route_evidence = value.get("route_evidence")
    if route_evidence is None:
        if value.get("schema_version") is not None:
            raise ValueError("runtime acceptance schema does not match its stored shape")
        # v1 compatibility: one qualification artifact owned the complete route set.
        evidence_hash = cast(str, value["evidence_hash"])
        qualified = _qualified_routes_from_evidence(
            root, evidence_hash, cast(str, value["build_hash"])
        )
        if routes != qualified:
            raise ValueError("runtime acceptance does not match its qualified build/routes")
        permit_evidence_hash = evidence_hash
    else:
        if value.get("schema_version") != "market-impact.accepted-pi-runtime.v2" or not isinstance(
            route_evidence, dict
        ):
            raise ValueError("runtime route evidence does not cover the accepted routes")
        mapping = cast(dict[str, object], route_evidence)
        if set(mapping) != set(routes):
            raise ValueError("runtime route evidence does not cover the accepted routes")
        for route, evidence_hash_value in mapping.items():
            if not isinstance(evidence_hash_value, str):
                raise ValueError("invalid runtime acceptance evidence identity")
            qualified = _qualified_routes_from_evidence(
                root, evidence_hash_value, cast(str, value["build_hash"])
            )
            if route not in qualified:
                raise ValueError("runtime route is not present in its qualification evidence")
        permit_evidence_hash = canonical_hash(mapping)
    if value["execution_capability"] is not False or routes != sorted(set(routes)):
        raise ValueError("runtime acceptance does not match its qualified build/routes")
    return PiRuntimePermit(
        cast(str, value["build_hash"]),
        tuple(routes),
        permit_evidence_hash,
    )


def _qualified_routes_from_evidence(root: Path, evidence_hash: str, build_hash: str) -> list[str]:
    if len(evidence_hash) != 64 or any(c not in "0123456789abcdef" for c in evidence_hash):
        raise ValueError("invalid runtime acceptance evidence identity")
    evidence_path = root / "acceptance-artifacts" / evidence_hash
    if evidence_path.is_symlink() or not evidence_path.is_file():
        raise ValueError("runtime acceptance evidence is unavailable")
    evidence_bytes = evidence_path.read_bytes()
    if sha256(evidence_bytes).hexdigest() != evidence_hash:
        raise ValueError("runtime acceptance evidence changed")
    evidence = json.loads(evidence_bytes)
    registration, report = evidence["registration"], evidence["report"]
    from market_impact_agent.model_provider import model_provider_profile_from_dict

    routes = sorted(
        model_provider_profile_from_dict(profile).route_identity
        for profile in registration["profiles"].values()
    )
    if (
        report["stage_passed"] is not True
        or report["registration_hash"] != registration["registration_hash"]
        or build_hash != canonical_hash(registration["runtime"])
    ):
        raise ValueError("runtime acceptance does not match its qualification evidence")
    return routes


def runtime_doctor(profiles: tuple[ModelProviderProfile, ...] = ()) -> dict[str, object]:
    """Read-only and safe to print: never probe a model or disclose env values."""
    from market_impact_agent.pi_runtime import (
        PI_RUNTIME_ROOT,
        model_concurrency_limit,
        runtime_identity,
        shared_admission_root,
    )

    node = shutil.which("node")
    version = None
    if node is not None:
        version = subprocess.check_output([node, "--version"], text=True, timeout=10).strip()
    version_ok = bool(
        version and tuple(int(item) for item in version.lstrip("v").split(".")) >= (22, 19, 0)
    )
    package = json.loads((PI_RUNTIME_ROOT / "package.json").read_text())
    lock = json.loads((PI_RUNTIME_ROOT / "package-lock.json").read_text())
    dependencies: dict[str, object] = {}
    for name, expected in package["dependencies"].items():
        installed = PI_RUNTIME_ROOT / "node_modules" / name / "package.json"
        observed = json.loads(installed.read_text())["version"] if installed.is_file() else None
        locked = lock["packages"].get(f"node_modules/{name}", {})
        dependencies[name] = {
            "expected": expected,
            "installed": observed,
            "locked_integrity_present": bool(locked.get("integrity")),
            "matches": observed == expected == locked.get("version"),
        }
    root = shared_admission_root()
    ancestor = root
    while not ancestor.exists():
        ancestor = ancestor.parent
    permit = installed_permit(root)
    build = runtime_identity()
    routes: list[dict[str, object]] = []
    for profile in profiles:
        accepted = False
        if permit is not None:
            try:
                permit.authorize(profile, build, "doctor-read-only", "doctor-read-only")
            except PermissionError:
                pass
            else:
                accepted = True
        routes.append(
            {
                "profile_id": profile.profile_id,
                "route_identity": profile.route_identity,
                "model": profile.model,
                "native_api": profile.native_api,
                "effort": profile.reasoning_effort,
                "context_window_tokens": profile.context_window_tokens,
                "compaction_trigger_tokens": profile.effective_compaction_trigger_tokens,
                "reserved_output_tokens": profile.reserved_output_tokens,
                "context_policy_valid": (
                    profile.effective_compaction_trigger_tokens + profile.reserved_output_tokens
                    <= profile.context_window_tokens
                ),
                "credential_present": bool(os.environ.get(profile.credential_env)),
                "runtime_accepted": accepted,
            }
        )
    return {
        "python": platform.python_version(),
        "python_compatible": (3, 13) <= sys.version_info[:2] < (3, 15),
        "platform": platform.system(),
        "node_version": version,
        "node_compatible": version_ok,
        "dependencies": dependencies,
        "build": build,
        "model_admission_root": str(root),
        "admission_directory_writable": os.access(ancestor, os.W_OK),
        "same_model_concurrency": model_concurrency_limit(),
        "routes": routes,
        "network_requests": 0,
        "ready": version_ok
        and (3, 13) <= sys.version_info[:2] < (3, 15)
        and all(cast(dict[str, object], value)["matches"] for value in dependencies.values()),
    }


def prepare_runtime() -> dict[str, object]:
    from market_impact_agent.pi_runtime import PI_RUNTIME_ROOT, shared_admission_root

    npm = shutil.which("npm")
    if npm is None:
        raise RuntimeError("Node/npm is required; install Node >=22.19 before preparing pi")
    root = shared_admission_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / "runtime-build.lock").open("a+b") as lease:
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                "pi workers are still active; drain before preparing dependencies"
            ) from None
        subprocess.run([npm, "ci", "--ignore-scripts"], cwd=PI_RUNTIME_ROOT, check=True)
        subprocess.run([npm, "run", "check"], cwd=PI_RUNTIME_ROOT, check=True)
    return runtime_doctor()


async def accept_runtime(root: Path, skill_root: Path) -> dict[str, object]:
    """Reopen every terminal and reconcile before replacing the local admission pointer."""
    from market_impact_agent.pi_canary import run_pi_canary

    report = await run_pi_canary(root, skill_root, replay_only=True)
    if not report["stage_passed"]:
        raise ValueError("runtime qualification failed; no production route admission")
    registration = cast(
        dict[str, object], json.loads((root / "pi-canary-registration.json").read_text())
    )
    return install_runtime_acceptance(registration=registration, report=report)


def install_runtime_acceptance(
    *, registration: dict[str, object], report: dict[str, object]
) -> dict[str, object]:
    """Install a fully replayable qualification as the sole local pi admission.

    Qualification coordinators may differ, but the evidence contract is one: the
    exact current build, all frozen Profiles, an immutable registration hash and a
    completed reconciled report.  This function does not run or probe any model.
    """

    from market_impact_agent.model_provider import model_provider_profile_from_dict
    from market_impact_agent.pi_runtime import runtime_identity, shared_admission_root
    from market_impact_agent.runtime_store import ArtifactStore

    core = {key: item for key, item in registration.items() if key != "registration_hash"}
    if (
        canonical_hash(core) != registration.get("registration_hash")
        or report.get("stage_passed") is not True
        or report.get("registration_hash") != registration.get("registration_hash")
        or report.get("runtime") != registration.get("runtime")
        or registration.get("runtime") != runtime_identity()
        or report.get("reconciled") is not True
    ):
        raise ValueError("runtime qualification evidence is incomplete or changed")
    profiles = cast(dict[str, dict[str, object]], registration["profiles"])
    target = shared_admission_root()
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    artifacts = ArtifactStore(target / "acceptance-artifacts")
    evidence = artifacts.put_json({"registration": registration, "report": report})
    new_routes = sorted(
        model_provider_profile_from_dict(profile).route_identity for profile in profiles.values()
    )
    route_evidence: dict[str, str] = {}
    path = target / "accepted-pi-runtime.json"
    with (target / "runtime-build.lock").open("a+b") as lease:
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("drain active pi workers before runtime cutover") from None
        current_build_hash = canonical_hash(runtime_identity())
        if path.exists():
            previous = cast(dict[str, object], json.loads(path.read_text()))
            previous_core = {
                key: item for key, item in previous.items() if key != "acceptance_hash"
            }
            if canonical_hash(previous_core) != previous.get("acceptance_hash"):
                raise ValueError("installed pi acceptance record has changed")
            previous_build_hash = cast(str, previous["build_hash"])
            if previous_build_hash == current_build_hash:
                try:
                    previous_permit = installed_permit(target)
                except ValueError:
                    # The v1 record predates route-identity versioning. Re-derive
                    # only the exact routes contained in immutable same-build
                    # evidence; never translate them into a current Profile.
                    if "route_evidence" in previous or previous.get("schema_version") is not None:
                        raise
                    previous_hash = cast(str, previous["evidence_hash"])
                    previous_routes = _qualified_routes_from_evidence(
                        target, previous_hash, current_build_hash
                    )
                    route_evidence.update({route: previous_hash for route in previous_routes})
                    previous_permit = None
                if previous_permit is not None:
                    previous_mapping = previous.get("route_evidence")
                    if isinstance(previous_mapping, dict):
                        route_evidence.update(cast(dict[str, str], previous_mapping))
                    else:
                        previous_hash = cast(str, previous["evidence_hash"])
                        route_evidence.update(
                            {route: previous_hash for route in previous_permit.route_identities}
                        )
            else:
                previous_evidence = previous.get("route_evidence")
                if previous.get("schema_version") == "market-impact.accepted-pi-runtime.v2":
                    if not isinstance(previous_evidence, dict):
                        raise ValueError(
                            "runtime acceptance schema does not match its stored shape"
                        )
                    # v2 explicitly binds each route to its evidence. Validate
                    # the mapping even when retiring the old build.
                    installed_permit(target)
                else:
                    if previous.get("schema_version") is not None:
                        raise ValueError(
                            "runtime acceptance schema does not match its stored shape"
                        )
                    # v1 used an older route-identity derivation. Reopen its
                    # immutable qualification without inheriting those routes.
                    _qualified_routes_from_evidence(
                        target,
                        cast(str, previous["evidence_hash"]),
                        previous_build_hash,
                    )
            artifacts.put_json(previous)
        route_evidence.update({route: evidence.content_hash for route in new_routes})
        record: dict[str, object] = {
            "schema_version": "market-impact.accepted-pi-runtime.v2",
            "build_hash": current_build_hash,
            "route_identities": sorted(route_evidence),
            "route_evidence": dict(sorted(route_evidence.items())),
            "execution_capability": False,
        }
        record["acceptance_hash"] = canonical_hash(record)
        descriptor, name = tempfile.mkstemp(prefix="accepted-pi-", dir=target)
        try:
            with os.fdopen(descriptor, "w") as output:
                json.dump(record, output, indent=2, sort_keys=True)
                output.flush()
                os.fsync(output.fileno())
            os.replace(name, path)
            directory = os.open(target, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(name).unlink(missing_ok=True)
    return {"accepted": True, **record, "live_enabled": False}
