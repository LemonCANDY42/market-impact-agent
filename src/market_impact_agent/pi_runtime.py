"""Process adapter only. pi owns the generic loop; Python owns durable callbacks.

No Provider parsing, credentials in RPC, default tools, or alternate runtime fallback.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import shutil
from collections.abc import Awaitable, Callable
from contextlib import suppress
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ModelTurn

if TYPE_CHECKING:
    from market_impact_agent.model_budget import ModelBudget
    from market_impact_agent.model_provider import ModelProviderProfile
    from market_impact_agent.pi_deployment import PiRuntimePermit
    from market_impact_agent.pi_execution import PiInvocationContext
    from market_impact_agent.provider_reliability import ProviderAttemptObserver

PI_RUNTIME: dict[str, object] = {
    "adapter": "market-impact-pi-v2",
    "upstream": "0.84.4",
    "revision": "b79e4cc834970cca69daebffab7df1da7d1e52c4",
}
PI_RUNTIME_ROOT = Path(__file__).resolve().parents[2] / "runtime" / "pi"
MAX_FRAME_BYTES = 4_000_000
Callback = Callable[[str, dict[str, object]], Awaitable[dict[str, object]]]


def runtime_identity() -> dict[str, object]:
    """Freeze exact local adapter and dependency lock, not just a friendly version."""
    files = {
        "lock": PI_RUNTIME_ROOT / "package-lock.json",
        "worker": PI_RUNTIME_ROOT / "src" / "worker.ts",
        "stdio": PI_RUNTIME_ROOT / "src" / "stdio.ts",
        "loop_adapter": PI_RUNTIME_ROOT / "src" / "runtime.ts",
        "process_adapter": Path(__file__),
        "authority_callbacks": Path(__file__).with_name("pi_execution.py"),
        "profile_contract": Path(__file__).with_name("model_provider.py"),
        "context_contract": Path(__file__).with_name("agent_runtime.py"),
        "answer_parser": Path(__file__).with_name("model_json.py"),
        "budget": Path(__file__).with_name("model_budget.py"),
        "deployment": Path(__file__).with_name("pi_deployment.py"),
        "engine": Path(__file__).with_name("agent_engine.py"),
        "reliability": Path(__file__).with_name("provider_reliability.py"),
    }
    return {
        **PI_RUNTIME,
        "files": {name: sha256(path.read_bytes()).hexdigest() for name, path in files.items()},
    }


def shared_admission_root() -> Path:
    """One machine/project scope, deliberately independent of experiment state roots."""
    configured = os.environ.get("MARKET_IMPACT_MODEL_STATE_ROOT")
    return (
        Path(configured).expanduser()
        if configured
        else Path.home() / ".local" / "state" / "market-impact-agent" / "model-runtime"
    ).resolve()


def model_concurrency_limit(explicit: int | None = None) -> int:
    raw = (
        explicit
        if explicit is not None
        else os.environ.get("MARKET_IMPACT_MODEL_MAX_CONCURRENT_REQUESTS", "3")
    )
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("MARKET_IMPACT_MODEL_MAX_CONCURRENT_REQUESTS must be an integer") from exc
    if not 1 <= value <= 3:
        raise ValueError(
            "model concurrency must be between one and the authorized maximum of three"
        )
    return value


class SharedSlots:
    """Advisory OS leases shared by all project workers on this host.

    Kernel releases locks on exit. There is no lease database or expiry race.
    """

    def __init__(self, root: Path, *, namespace: str, identity: object, limit: int) -> None:
        if not namespace or namespace != namespace.strip():
            raise ValueError("slot namespace must be nonempty trimmed text")
        if isinstance(limit, bool) or limit < 1:
            raise ValueError("slot capacity must be positive")
        self.root = root / namespace / canonical_hash(identity)
        self.limit = limit
        self.handle: BinaryIO | None = None
        self._configuration: BinaryIO | None = None

    async def acquire(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        configuration = (self.root / "capacity.lock").open("a+b")
        self._configuration = configuration
        try:
            while True:
                try:
                    fcntl.flock(configuration, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    await asyncio.sleep(0.02)
                    continue
                configuration.seek(0)
                capacity = configuration.read()
                if capacity == str(self.limit).encode():
                    break
                try:
                    # Only EX may change capacity; every active holder retains SH.
                    # A failed upgrade releases SH before retrying first-use setup.
                    fcntl.flock(configuration, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    fcntl.flock(configuration, fcntl.LOCK_UN)
                    if capacity:
                        raise RuntimeError("model capacity differs from active workers") from None
                    await asyncio.sleep(0.02)
                    continue
                configuration.seek(0)
                configuration.truncate()
                configuration.write(str(self.limit).encode())
                configuration.flush()
                fcntl.flock(configuration, fcntl.LOCK_SH)
                break
            while self.handle is None:
                for slot in range(self.limit):
                    handle = (self.root / f"{slot}.lock").open("a+b")
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        handle.close()
                    else:
                        self.handle = handle
                        return
                await asyncio.sleep(0.02)
        except BaseException:
            self.release()
            raise

    def release(self) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None
        if self._configuration is not None:
            fcntl.flock(self._configuration, fcntl.LOCK_UN)
            self._configuration.close()
            self._configuration = None


class ModelSlots(SharedSlots):
    """One machine/project limit shared by aliases of the same logical model."""

    def __init__(self, root: Path, model: str, limit: int = 3) -> None:
        super().__init__(
            root,
            namespace="model-slots",
            identity={"model": model},
            limit=model_concurrency_limit(limit),
        )


class ExperimentSlots(SharedSlots):
    """A separate bounded experiment-wide limit across multiple model routes."""

    def __init__(self, root: Path, experiment_id: str, limit: int = 6) -> None:
        if isinstance(limit, bool) or not 1 <= limit <= 6:
            raise ValueError("experiment concurrency must be between one and six")
        super().__init__(
            root,
            namespace="experiment-slots",
            identity={"experiment": experiment_id},
            limit=limit,
        )


class PiRuntimeProvider:
    def __init__(
        self,
        profile: ModelProviderProfile,
        *,
        dispatch_allowed: bool = True,
        budget: ModelBudget | None = None,
        permit: PiRuntimePermit | None = None,
    ) -> None:
        from market_impact_agent.pi_deployment import installed_permit

        if dispatch_allowed:
            _ = profile.native_api  # Archived profile decoding grants no model dispatch.
        self.profile = profile
        self.dispatch_allowed = dispatch_allowed
        self.budget = budget
        self._credential = os.environ.get(profile.credential_env) if dispatch_allowed else None
        self.profile_identity = profile.profile_hash
        self.admission_root = shared_admission_root()
        self.permit = permit or installed_permit(self.admission_root)
        self.max_concurrent_requests = model_concurrency_limit()
        self.runtime_identity = runtime_identity()
        self._process: asyncio.subprocess.Process | None = None
        self._build_lease: BinaryIO | None = None
        self._lock = asyncio.Lock()

    @property
    def provider_id(self) -> str:
        return self.profile.provider_id

    @property
    def model(self) -> str:
        return self.profile.model

    def context_identity(
        self, run_id: str, tools: list[dict[str, object]], messages: list[dict[str, object]]
    ) -> dict[str, str]:
        """Cache may share public prefixes, never private conversation state."""
        runtime = cast(dict[str, object], self.profile.runtime)
        return {
            "conversationId": "conversation-"
            + canonical_hash({"run": run_id, "route": self.profile.route_identity}),
            "cacheKey": "prefix-"
            + canonical_hash(
                {
                    "namespace": runtime["cache_namespace"],
                    "route": self.profile.route_identity,
                    "system": [message for message in messages if message.get("role") == "system"],
                    "tools": tools,
                }
            ),
        }

    def assert_frozen(self) -> None:
        if (
            self.runtime_identity != runtime_identity()
            or self.profile.profile_hash != self.profile_identity
        ):
            raise RuntimeError("pi adapter/dependencies changed; create a new runtime epoch")

    def authorize_dispatch(self, invocation_id: str, budget_owner: str) -> None:
        if not self.dispatch_allowed or self.permit is None:
            raise PermissionError("pi requires an accepted route or bounded acceptance permit")
        self.permit.authorize(self.profile, self.runtime_identity, invocation_id, budget_owner)

    async def assert_model_available(self, *, timeout_seconds: float = 30) -> None:
        """Configuration readiness only; the first native response proves identity.

        Do not add a separate custom discovery Provider or spend an untracked model call.
        """
        if not self.dispatch_allowed or not self._credential:
            raise RuntimeError("configured pi credential environment is unavailable")
        await asyncio.wait_for(self._start(), timeout=timeout_seconds)

    async def run_once(
        self,
        *,
        context: PiInvocationContext,
        messages: tuple[dict[str, object], ...],
        max_output_tokens: int,
        timeout_seconds: float,
        attempt_observer: ProviderAttemptObserver,
    ) -> ModelTurn:
        from market_impact_agent.pi_execution import execute_pi_once

        return await execute_pi_once(
            self,
            context=context,
            messages=messages,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
            attempt_observer=attempt_observer,
        )

    async def _start(self) -> asyncio.subprocess.Process:
        if self._process is not None and self._process.returncode is None:
            return self._process
        node = shutil.which("node")
        if node is None or not (PI_RUNTIME_ROOT / "node_modules").is_dir():
            raise RuntimeError("pi runtime requires Node and npm ci in runtime/pi")
        self.admission_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        lease = (self.admission_root / "runtime-build.lock").open("a+b")
        try:
            fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BaseException:
            lease.close()
            raise RuntimeError("pi dependencies are being prepared; no worker start") from None
        self._build_lease = lease
        # No global config discovery, shell evaluation, or inherited unrelated credentials.
        env = {"PATH": os.defpath, "NODE_NO_WARNINGS": "1"}
        key = self._credential
        if key:
            env[self.profile.credential_env] = key
        try:
            process = await asyncio.create_subprocess_exec(
                node,
                "--experimental-strip-types",
                str(self.entry_point()),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
                limit=MAX_FRAME_BYTES + 1,
            )
            self._process = process
            ready = await asyncio.wait_for(self._read(process), timeout=15)
            if ready != {"type": "ready", "runtime": PI_RUNTIME}:
                raise RuntimeError("pi worker version handshake mismatch")
            return process
        except BaseException:
            await self.close()
            raise

    async def execute(self, payload: dict[str, object], callback: Callback) -> dict[str, object]:
        async with self._lock:
            _ = self.profile.native_api
            self.assert_frozen()
            try:
                process = await self._start()
                await self._send(process, {"type": "run", "payload": payload})
                while True:
                    frame = await self._read(process)
                    if frame.get("type") == "done":
                        return cast(dict[str, object], frame["result"])
                    if frame.get("type") != "callback":
                        raise RuntimeError("pi worker stopped without an accepted terminal")
                    method, body = frame.get("method"), frame.get("payload")
                    if not isinstance(method, str) or not isinstance(body, dict):
                        raise RuntimeError("invalid pi callback")
                    result = await callback(method, cast(dict[str, object], body))
                    await self._send(
                        process, {"type": "reply", "id": frame["id"], "payload": result}
                    )
            except BaseException:
                await self.close()
                raise

    def entry_point(self) -> Path:
        return PI_RUNTIME_ROOT / "src" / "worker.ts"

    async def close(self) -> None:
        process, self._process = self._process, None
        lease, self._build_lease = self._build_lease, None
        try:
            await self._close_process(process)
        finally:
            if lease is not None:
                fcntl.flock(lease, fcntl.LOCK_UN)
                lease.close()

    async def _close_process(self, process: asyncio.subprocess.Process | None) -> None:
        if process is None:
            return
        if process.returncode is None:
            with suppress(BrokenPipeError, ConnectionResetError):
                await self._send(process, {"type": "cancel"})
            if process.stdin is not None:
                process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except TimeoutError:
                if process.returncode is None:
                    process.kill()
                await process.wait()

    @staticmethod
    async def _send(process: asyncio.subprocess.Process, value: dict[str, object]) -> None:
        frame = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        if len(frame) > MAX_FRAME_BYTES:
            raise ValueError("pi IPC frame exceeds bound")
        if process.stdin is None:
            raise RuntimeError("pi stdin is unavailable")
        process.stdin.write(frame)
        await process.stdin.drain()

    @staticmethod
    async def _read(process: asyncio.subprocess.Process) -> dict[str, object]:
        if process.stdout is None:
            raise RuntimeError("pi stdout is unavailable")
        frame = await process.stdout.readline()
        if not frame or len(frame) > MAX_FRAME_BYTES:
            raise RuntimeError("pi worker exited or exceeded IPC bound")
        value = json.loads(frame)
        if not isinstance(value, dict):
            raise RuntimeError("pi worker frame must be an object")
        return cast(dict[str, object], value)
