"""Production AgentEngine + real Node/pi; only network I/O is replaced."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent import pi_deployment
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import CancellationToken
from market_impact_agent.agent_runtime import Utf8TokenEstimator
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import (
    load_builtin_model_provider_profile,
    model_provider_profile_from_dict,
)
from market_impact_agent.pi_canary import (
    _physical_intervals,  # pyright: ignore[reportPrivateUsage]
    _preserves_observed_duration,  # pyright: ignore[reportPrivateUsage]
    _QualificationProvider,  # pyright: ignore[reportPrivateUsage]
    prepare_pi_canary,
    prepare_pi_canary_followup,
    prepare_pi_canary_repair,
    run_pi_canary,
)
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_execution import PiInvocationContext, native_turn, native_usage
from market_impact_agent.pi_runtime import (
    PI_RUNTIME_ROOT,
    ExperimentSlots,
    ModelSlots,
    PiRuntimeProvider,
    runtime_identity,
)
from market_impact_agent.provider_reliability import ProviderAttemptEvent
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus

from .test_agent_engine import SimulatedCrash, make_engine, request


@pytest.mark.parametrize("boundary", ["pi.response.received", "pi.role.response.completed"])
def test_single_turn_role_replays_native_completion_not_a_new_request(
    tmp_path: Path, offline_network: None, boundary: str
):
    async def scenario():
        profile = pi_profile()
        provider = PiRuntimeProvider(profile)
        journal = RunJournal(tmp_path / "runs.sqlite3")
        journal.start_run(
            run_id="role", config_hash=canonical_hash("frozen"), created_at=datetime.now(UTC)
        )
        context = PiInvocationContext("role", 1, journal, ArtifactStore(tmp_path / "artifacts"))
        events: list[ProviderAttemptEvent] = []
        original = journal.append
        crashed = False

        def crash_after(**kwargs: Any):
            nonlocal crashed
            event = original(**kwargs)
            if kwargs["event_type"] == boundary and not crashed:
                crashed = True
                raise SimulatedCrash()
            return event

        journal.append = crash_after
        arguments = {
            "context": context,
            "messages": ({"role": "user", "content": "Classify the frozen input."},),
            "max_output_tokens": 256,
            "timeout_seconds": 20,
            "attempt_observer": events.append,
        }
        try:
            with pytest.raises(SimulatedCrash):
                await provider.run_once(**arguments)  # pyright: ignore[reportArgumentType]
            journal.append = original
            response = await provider.run_once(**arguments)  # pyright: ignore[reportArgumentType]
            assert response.tool_calls == ()
            assert response.usage.input_tokens == 100
            assert sum(event.phase.value == "dispatched" for event in events) == 1
            assert (
                ModelBudget(
                    journal,
                    "role",
                    profile.budget.max_turns * profile.max_attempts,
                    profile.budget.max_estimated_cost_microusd,
                ).summary()["unsettled_requests"]
                == 0
            )
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_separate_experiment_workers_share_parent_request_budget(
    tmp_path: Path, offline_network: None
):
    async def scenario():
        profile = pi_profile()
        journal = RunJournal(tmp_path / "parent.sqlite3")
        journal.start_run(
            run_id="parent", config_hash=canonical_hash("fixed"), created_at=datetime.now(UTC)
        )
        budget = ModelBudget(journal, "parent", 4, 3_000_000, 1, 7)
        providers = [PiRuntimeProvider(profile, budget=budget) for _ in range(2)]
        engines = [
            make_engine(tmp_path / str(i), p, handler_calls=[], config=profile.runtime_config())
            for i, p in enumerate(providers)
        ]
        try:
            results = await asyncio.gather(
                *(engine.run(request(f"child-{i}")) for i, engine in enumerate(engines))
            )
            assert {result.status for result in results} == {
                RunStatus.COMPLETED,
                RunStatus.BUDGET_EXHAUSTED,
            }
            assert budget.summary() == {
                "physical_requests": 4,
                "known_cost_microusd": 139,
                "reserved_microusd": 0,
                "unsettled_requests": 0,
            }
        finally:
            await asyncio.gather(*(p.close() for p in providers))

    asyncio.run(scenario())


def test_single_turn_role_corrections_preserve_question_answer_instruction_order(
    tmp_path: Path, offline_network: None
):
    async def scenario():
        provider = PiRuntimeProvider(pi_profile())
        journal = RunJournal(tmp_path / "roles.sqlite3")
        artifacts = ArtifactStore(tmp_path / "artifacts")
        journal.start_run(
            run_id="role", config_hash=canonical_hash("role"), created_at=datetime.now(UTC)
        )
        messages: list[dict[str, object]] = [
            {"role": "user", "content": "Original frozen question"}
        ]
        attempts: list[ProviderAttemptEvent] = []
        try:
            for sequence in (1, 2, 3):
                context = PiInvocationContext("role", sequence, journal, artifacts)
                arguments: dict[str, Any] = dict(
                    context=context,
                    messages=tuple(messages),
                    max_output_tokens=256,
                    timeout_seconds=20,
                    attempt_observer=attempts.append,
                )
                turn = await provider.run_once(**arguments)
                frozen = next(
                    event
                    for event in journal.events("role")
                    if event.event_id == f"{context.invocation_id}.pi.input.1"
                )
                native = cast(
                    dict[str, Any], artifacts.read_json(cast(str, frozen.payload["artifact_hash"]))
                )
                history = native["context"]["messages"]
                assert [message["role"] for message in history] == ["user"] + [
                    "assistant",
                    "user",
                ] * (sequence - 1)
                assert history[0]["content"] == "Original frozen question"
                assert history[-1]["content"] == messages[-1]["content"]
                if sequence > 1:
                    assert await provider.run_once(**arguments) == turn
                messages.extend(
                    [
                        turn.assistant_message,
                        {
                            "role": "user",
                            "content": f"Correction {sequence}: preserve the new instruction.",
                        },
                    ]
                )
            assert sum(event.phase.value == "dispatched" for event in attempts) == 3
        finally:
            await provider.close()

    asyncio.run(scenario())


def pi_profile():
    value = load_builtin_model_provider_profile("pi-cpa-luna-max-v2").to_dict()
    value.update(
        adapter_kind="pi-openai-responses",
        api_path="/v1/responses",
        credential_env="PI_FIXTURE_KEY",
    )
    value.pop("profile_id")
    value["profile_id"] = f"model-provider-{canonical_hash(value)}"
    return model_provider_profile_from_dict(value)


@pytest.fixture
def offline_network(monkeypatch: pytest.MonkeyPatch):
    original = asyncio.create_subprocess_exec
    monkeypatch.setenv("PI_FIXTURE_KEY", "synthetic-fixture-key")

    def fixture_permit(_root: Path) -> PiRuntimePermit:
        return PiRuntimePermit(
            canonical_hash(runtime_identity()),
            (
                pi_profile().route_identity,
                load_builtin_model_provider_profile("pi-minimax-m3-v2").route_identity,
            ),
            "synthetic-offline-route-proof",
        )

    monkeypatch.setattr(pi_deployment, "installed_permit", fixture_permit)

    async def spawn(program: str, *args: str, **kwargs: Any):
        return await original(
            program,
            "--import",
            str(PI_RUNTIME_ROOT / "test" / "fixture-network.ts"),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)


def test_real_pi_loop_consumes_tool_result_and_replays_terminal(
    tmp_path: Path, offline_network: None
):
    async def scenario():
        profile = pi_profile()
        provider = PiRuntimeProvider(profile)
        calls: list[str] = []
        engine = make_engine(
            tmp_path, provider, handler_calls=calls, config=profile.runtime_config()
        )
        try:
            result = await engine.run(request())
            assert result.terminal_store_hash is not None and result.metrics is not None
            if result.status != RunStatus.COMPLETED:
                pytest.fail(str(engine.artifact_store.read_json(result.terminal_store_hash)))
            assert calls == ["official-outage"]
            assert result.metrics.turns == 2
            assert result.metrics.provider_attempts == 2
            assert result.metrics.input_tokens == 200
            assert result.metrics.output_tokens == 40
            assert result.judgment is not None
            assert result.judgment.proposal.summary == "Frozen outage evidence was read."
            await provider.close()
            replay = await engine.run(request())
            assert replay.terminal_store_hash == result.terminal_store_hash
            assert calls == ["official-outage"]
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_configured_compatible_model_uses_the_same_native_loop_after_explicit_admission(
    tmp_path: Path,
    offline_network: None,
):
    async def scenario():
        raw = pi_profile().to_dict()
        raw["model"] = "synthetic-compatible-model"
        raw.pop("profile_id")
        raw["profile_id"] = f"model-provider-{canonical_hash(raw)}"
        profile = model_provider_profile_from_dict(raw)
        unaccepted = PiRuntimeProvider(profile)
        with pytest.raises(PermissionError, match="has not passed"):
            unaccepted.authorize_dispatch("compatible", "compatible")
        # This test-only permit never installs production model acceptance.
        provider = PiRuntimeProvider(
            profile,
            permit=PiRuntimePermit(
                canonical_hash(runtime_identity()),
                (profile.route_identity,),
                "synthetic-offline-extension-proof",
                run_ids=("compatible",),
            ),
        )
        calls: list[str] = []
        engine = make_engine(
            tmp_path, provider, handler_calls=calls, config=profile.runtime_config()
        )
        try:
            result = await engine.run(request("compatible"))
            assert result.status is RunStatus.COMPLETED
            assert result.judgment is not None and result.judgment.model == profile.model
            assert calls == ["official-outage"]
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "boundary",
    ["pi.response.received", "model.turn.completed", "tool.call.completed", "model.turn.started"],
)
def test_pi_crash_recovery_has_no_duplicate_dispatch_or_tool(
    tmp_path: Path, offline_network: None, boundary: str
):
    async def scenario():
        profile = pi_profile()
        provider = PiRuntimeProvider(profile)
        calls: list[str] = []
        engine = make_engine(
            tmp_path, provider, handler_calls=calls, config=profile.runtime_config()
        )
        original = engine.journal.append
        crashed = False

        def crash_after(**kwargs: Any):
            nonlocal crashed
            event = original(**kwargs)
            if kwargs["event_type"] == boundary and not crashed:
                crashed = True
                raise SimulatedCrash()
            return event

        engine.journal.append = crash_after
        try:
            with pytest.raises(SimulatedCrash):
                await engine.run(request())
            engine.journal.append = original
            result = await engine.run(request())
            assert result.terminal_store_hash is not None and result.metrics is not None
            if boundary == "model.turn.started":
                assert result.status == RunStatus.HUMAN_INPUT_REQUIRED
                assert calls == []
            else:
                if result.status != RunStatus.COMPLETED:
                    pytest.fail(str(engine.artifact_store.read_json(result.terminal_store_hash)))
                assert calls == ["official-outage"]
                assert result.metrics.provider_attempts == 2
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_usage_total_includes_cache_once_and_unknown_stays_unknown():
    usage = native_usage(
        {
            "input_tokens": 100,
            "output_tokens": 20,
            "input_tokens_details": {"cached_tokens": 60},
            "output_tokens_details": {"reasoning_tokens": 8},
        }
    )
    assert usage.input_tokens == 100 and usage.output_tokens == 20
    assert usage.cache_read_tokens == 60
    assert usage.cache_write_tokens is None
    assert native_usage({"prompt_tokens": 100, "completion_tokens": 20}).cache_read_tokens is None
    with pytest.raises(ValueError, match="unavailable"):
        native_usage(None)


def test_pi_cancellation_joins_child_before_releasing_request_slot(
    tmp_path: Path, offline_network: None, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("PI_FIXTURE_KEY", "hang")

    async def scenario():
        profile = pi_profile()
        provider = PiRuntimeProvider(profile)
        engine = make_engine(tmp_path, provider, handler_calls=[], config=profile.runtime_config())
        dispatched = asyncio.Event()
        original = engine._observe_attempt  # pyright: ignore[reportPrivateUsage]

        def observe(run_id: str, turn_number: int, event: ProviderAttemptEvent):
            original(run_id, turn_number, event)
            dispatched.set()

        engine._observe_attempt = observe  # pyright: ignore[reportPrivateUsage]
        cancellation = CancellationToken()
        task = asyncio.create_task(engine.run(request(), cancellation=cancellation))
        try:
            await asyncio.wait_for(dispatched.wait(), 5)
            cancellation.cancel()
            result = await asyncio.wait_for(task, 5)
            assert result.status == RunStatus.CANCELLED
            assert provider._process is None  # pyright: ignore[reportPrivateUsage]
            leases = [ModelSlots(provider.admission_root, provider.model) for _ in range(3)]
            await asyncio.wait_for(asyncio.gather(*(lease.acquire() for lease in leases)), 1)
            for lease in leases:
                lease.release()
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_shared_model_slots_release_and_cancel_without_extra_admission(tmp_path: Path):
    async def scenario():
        leases = [ModelSlots(tmp_path, "same-model") for _ in range(4)]
        await asyncio.gather(*(lease.acquire() for lease in leases[:3]))
        task = asyncio.create_task(leases[3].acquire())
        await asyncio.sleep(0)
        assert leases[3].handle is None
        leases[0].release()
        await asyncio.wait_for(task, 1)
        assert leases[3].handle is not None
        for lease in leases:
            lease.release()

    asyncio.run(scenario())


def test_shared_experiment_slots_bound_six_and_release_waiter(tmp_path: Path):
    async def scenario():
        leases = [ExperimentSlots(tmp_path, "study-v1") for _ in range(7)]
        await asyncio.gather(*(lease.acquire() for lease in leases[:6]))
        task = asyncio.create_task(leases[6].acquire())
        await asyncio.sleep(0)
        assert leases[6].handle is None
        leases[0].release()
        await asyncio.wait_for(task, 1)
        assert leases[6].handle is not None
        for lease in leases:
            lease.release()

    asyncio.run(scenario())


def test_qualification_physical_intervals_exclude_retry_backoff(tmp_path: Path):
    journal = RunJournal(tmp_path / "attempts.sqlite3")
    start = datetime(2026, 9, 3, tzinfo=UTC)
    journal.start_run(run_id="retry", config_hash=canonical_hash("retry"), created_at=start)
    expected: list[tuple[datetime, datetime]] = []
    for attempt, begin, end, phase in ((1, 0, 0.5, "failed"), (2, 3, 4, "succeeded")):
        dispatch = journal.append(
            run_id="retry",
            event_id=f"attempt.{attempt}.dispatch",
            event_type="model.attempt.dispatched",
            observed_at=start + timedelta(seconds=begin),
            payload={"physical_attempt": attempt, "request_id": "same-logical-request"},
        )
        journal.append(
            run_id="retry",
            event_id=f"attempt.{attempt}.{phase}",
            event_type=f"model.attempt.{phase}",
            observed_at=start + timedelta(seconds=end),
            payload={"dispatch_event_hash": dispatch.event_hash},
        )
        expected.append((dispatch.observed_at, start + timedelta(seconds=end)))
    intervals = _physical_intervals(journal.events("retry"))
    assert intervals == expected
    # Another worker during [1, 2] does not overlap either physical attempt;
    # pairing the first dispatch with the later success would falsely pass.
    assert not any(
        max(begin, start + timedelta(seconds=1)) < min(end, start + timedelta(seconds=2))
        for begin, end in intervals
    )
    assert any(
        max(begin, start + timedelta(seconds=3.5)) < min(end, start + timedelta(seconds=5))
        for begin, end in intervals
    )


@pytest.mark.parametrize("qualification", ["original", "followup", "repair"])
def test_fixed_cutover_qualification_uses_both_routes_and_reopens_without_dispatch(
    tmp_path: Path, offline_network: None, monkeypatch: pytest.MonkeyPatch, qualification: str
):
    import json

    from market_impact_agent.pi_canary import _prior_usage  # pyright: ignore[reportPrivateUsage]
    from market_impact_agent.pi_deployment import accept_runtime

    original_init = _QualificationProvider.__init__
    original_execute = PiRuntimeProvider.execute
    focused_followup = qualification != "original"
    repaired = False

    def initialize(self: _QualificationProvider, *args: Any, **kwargs: Any):
        original_init(self, *args, **kwargs)
        if (
            focused_followup
            and self.case["case"] == "cpa-concurrent-2"
            and not self.case.get("continuation_only")
        ):
            self._credential = "synthetic-cpa-extra-search"  # pyright: ignore[reportPrivateUsage]

    # An external-I/O barrier makes concurrency verification deterministic;
    # the real pi loop, admission and durable request events remain in use.
    barrier: asyncio.Barrier | None = None

    async def execute(self: PiRuntimeProvider, payload: dict[str, object], callback: Any):
        nonlocal barrier

        async def controlled(method: str, body: dict[str, object]):
            nonlocal barrier
            result = await callback(method, body)
            if (
                isinstance(self, _QualificationProvider)
                and self.case.get("continuation_only")
                and self.case["route"] == "cpa"
                and method == "attempt_start"
                and body["number"] == 1
            ):
                if barrier is None:
                    barrier = asyncio.Barrier(2)
                await asyncio.wait_for(barrier.wait(), 5)
            if (
                qualification == "repair"
                and not repaired
                and isinstance(self, _QualificationProvider)
                and self.case.get("continuation_only")
                and method == "model_completed"
            ):
                raise ValueError("synthetic prior adapter projection failure after receipt")
            return result

        return await original_execute(self, payload, controlled)

    monkeypatch.setattr(_QualificationProvider, "__init__", initialize)
    monkeypatch.setattr(PiRuntimeProvider, "execute", execute)

    monkeypatch.setenv("MARKET_IMPACT_CLIPROXY_API_KEY", "synthetic-cpa")
    monkeypatch.setenv("MINIMAX_API_KEY", "synthetic-minimax")
    prior: dict[str, object] = {
        "requests": 8,
        "cost_microusd": 25_153,
        "union_hash": canonical_hash("prior"),
        "roots": [str(tmp_path / "old-a"), str(tmp_path / "old-b")],
    }

    def fixed_prior(_roots: tuple[Path, ...]) -> dict[str, object]:
        if len(_roots) > 2:
            extra = _prior_usage(_roots[2:])
            return {
                "requests": 8 + cast(int, extra["requests"]),
                "cost_microusd": 25_153 + cast(int, extra["cost_microusd"]),
                "union_hash": canonical_hash([prior, extra]),
                "roots": [str(path.resolve()) for path in _roots],
            }
        return prior

    monkeypatch.setattr("market_impact_agent.pi_canary._prior_usage", fixed_prior)
    verification = tmp_path / "verified.json"
    verification.write_text(
        json.dumps(
            {
                "runtime": runtime_identity(),
                "checks": {
                    key: "passed"
                    for key in (
                        "ruff",
                        "format",
                        "pyright",
                        "pytest",
                        "typescript",
                        "node_tests",
                        "clean_source_install",
                        "independent_review",
                    )
                },
                "evidence_refs": ["synthetic-offline-verification-only"],
            }
        )
    )
    roots = tuple(Path(path) for path in cast(list[str], prior["roots"]))
    registration = prepare_pi_canary(tmp_path, prior_roots=roots, verification=verification)
    assert prepare_pi_canary(tmp_path, prior_roots=roots, verification=verification) == registration
    other = tmp_path / "another-final-batch"
    with pytest.raises(PermissionError, match="another final batch"):
        prepare_pi_canary(other, prior_roots=roots, verification=verification)
    skill_root = Path(__file__).resolve().parents[1] / "skills"
    with pytest.raises(PermissionError, match="another final batch"):
        asyncio.run(run_pi_canary(other, skill_root))
    result = asyncio.run(run_pi_canary(tmp_path, skill_root))
    if focused_followup:
        assert not result["stage_passed"]
        assert result["not_run"] == ["cpa-cancelled"]
        failed_store = LocalDataSnapshotStore(tmp_path / "cases" / "cpa-concurrent-2")
        failed_journal = RunJournal.authoritative(failed_store)
        failed_run = f"pi-cutover-{registration['registration_hash']}.cpa-concurrent-2"
        assert failed_journal.event(f"{failed_run}.turn.3.started") is None
        assert failed_journal.event(f"{failed_run}.turn.3.interrupted") is None
        terminal = failed_journal.event(f"{failed_run}.terminal.failed")
        assert terminal is not None and terminal.payload["error_class"] == "_BudgetExceeded"
        parent_bytes = (tmp_path / "pi-canary-report.json").read_bytes()
        successor = tmp_path / "focused"
        frozen = asyncio.run(
            prepare_pi_canary_followup(
                successor,
                parent_root=tmp_path,
                verification=verification,
                skill_root=skill_root,
            )
        )
        assert frozen["prior"]["requests"] == 34  # pyright: ignore[reportIndexIssue]
        assert (
            asyncio.run(
                prepare_pi_canary_followup(
                    successor,
                    parent_root=tmp_path,
                    verification=verification,
                    skill_root=skill_root,
                )
            )
            == frozen
        )
        with pytest.raises(PermissionError, match="another final batch"):
            asyncio.run(
                prepare_pi_canary_followup(
                    tmp_path / "second-focused",
                    parent_root=tmp_path,
                    verification=verification,
                    skill_root=skill_root,
                )
            )
        result = asyncio.run(run_pi_canary(successor, skill_root))
        if qualification == "repair":
            assert not result["stage_passed"]
            assert result["not_run"] == ["cpa-cancelled"]
            assert cast(dict[str, int], result["usage"])["physical_requests"] == 36
            failed_bytes = (successor / "pi-canary-report.json").read_bytes()
            # An explicitly different build; never mutate the prior registration.
            previous_build = runtime_identity()
            updated = {
                **previous_build,
                "files": {
                    **cast(dict[str, str], previous_build["files"]),
                    "process_adapter": canonical_hash("synthetic repaired process adapter"),
                },
            }
            monkeypatch.setattr("market_impact_agent.pi_canary.runtime_identity", lambda: updated)
            monkeypatch.setattr("market_impact_agent.pi_runtime.runtime_identity", lambda: updated)
            verified = json.loads(verification.read_text())
            verified.update(
                runtime=updated,
                reviewed_changed_files=["process_adapter"],
                retained_report_hash=cast(dict[str, object], result["inherited_report"])[
                    "report_hash"
                ],
            )
            verification.write_text(json.dumps(verified))
            repair_root = tmp_path / "repair"
            repaired = True
            barrier = None  # Each asyncio.run owns a distinct event loop.
            repair = prepare_pi_canary_repair(
                repair_root, parent_root=successor, verification=verification
            )
            assert (
                prepare_pi_canary_repair(
                    repair_root, parent_root=successor, verification=verification
                )
                == repair
            )
            with pytest.raises(PermissionError, match="another final batch"):
                prepare_pi_canary_repair(
                    tmp_path / "duplicate-repair", parent_root=successor, verification=verification
                )
            result = asyncio.run(run_pi_canary(repair_root, skill_root))
            assert result["stage_passed"], result
            assert cast(dict[str, int], result["usage"])["physical_requests"] == 45
            assert result["concurrent_requests_overlapped"] is True
            assert (successor / "pi-canary-report.json").read_bytes() == failed_bytes
            assert (tmp_path / "pi-canary-report.json").read_bytes() == parent_bytes
            monkeypatch.delenv("MARKET_IMPACT_CLIPROXY_API_KEY")
            monkeypatch.delenv("MINIMAX_API_KEY")
            assert asyncio.run(run_pi_canary(repair_root, skill_root, replay_only=True)) == result
            assert asyncio.run(accept_runtime(repair_root, skill_root))["accepted"]
            with pytest.raises(ValueError, match="closed failed focused"):
                prepare_pi_canary_repair(
                    tmp_path / "recursive-repair",
                    parent_root=repair_root,
                    verification=verification,
                )
            return
        assert result["stage_passed"], result
        assert result["concurrent_requests_overlapped"] is True
        assert cast(dict[str, int], result["usage"])["physical_requests"] == 36
        assert (tmp_path / "pi-canary-report.json").read_bytes() == parent_bytes
        assert result["inherited_report"]["stage_passed"] is False  # pyright: ignore[reportIndexIssue]
        monkeypatch.delenv("MARKET_IMPACT_CLIPROXY_API_KEY")
        monkeypatch.delenv("MINIMAX_API_KEY")
        assert asyncio.run(run_pi_canary(successor, skill_root, replay_only=True)) == result
        accepted = asyncio.run(accept_runtime(successor, skill_root))
        assert accepted["accepted"]
        with pytest.raises(ValueError, match="only one focused"):
            asyncio.run(
                prepare_pi_canary_followup(
                    tmp_path / "recursive",
                    parent_root=successor,
                    verification=verification,
                    skill_root=skill_root,
                )
            )
        return
    if not result["stage_passed"]:
        pytest.fail(str(result))
    assert result["runtime_accepted"] is False
    rows = cast(list[dict[str, object]], result["cases"])
    assert len(rows) == 13
    assert all(row["passed"] for row in rows)
    assert [row["compactions"] for row in rows if row["mode"] == "compression"] == [2, 2]
    assert all(row["retained_compaction_evidence"] for row in rows if row["mode"] == "compression")
    # 16 cache/tool calls, six compression/continuation, four concurrency calls.
    assert cast(dict[str, int], result["usage"])["physical_requests"] == 8 + 26
    monkeypatch.delenv("MARKET_IMPACT_CLIPROXY_API_KEY")
    monkeypatch.delenv("MINIMAX_API_KEY")
    assert asyncio.run(run_pi_canary(tmp_path, skill_root, replay_only=True)) == result
    accepted = asyncio.run(accept_runtime(tmp_path, skill_root))
    assert accepted["accepted"] and accepted["live_enabled"] is False
    # No implicit replay compatibility framework for an upgraded build.
    monkeypatch.setattr("market_impact_agent.pi_canary.runtime_identity", lambda: {"changed": True})
    with pytest.raises(ValueError, match="runtime changed"):
        asyncio.run(run_pi_canary(tmp_path, skill_root))


@pytest.mark.parametrize(
    ("summary", "valid"),
    [
        ("The outage lasts 18 hours, later duration remains unknown.", True),
        ("本次中断持续 18 小时。", True),
        ("The outage lasts 180 hours.", False),
        ("The outage lasts 118 hours.", False),
        ("The outage lasts 18.5 hours.", False),
        ("The estimate was 18 hours, now 180 hours.", False),
        ("The outage was reported on the 18th.", False),
    ],
)
def test_qualification_does_not_accept_a_numeric_substring(summary: str, valid: bool):
    assert _preserves_observed_duration(summary) is valid


def test_pi_answer_repair_keeps_original_and_does_not_extract_reasoning():
    def turn(text: str):
        return native_turn(
            {
                "message": {
                    "model": "test-model",
                    "stopReason": "stop",
                    "content": [{"type": "text", "text": text}],
                },
                "response_models": ["test-model"],
                "raw_usage": {"input_tokens": 100, "output_tokens": 20},
                "latency_ms": 1,
                "attempts": 1,
            },
            "test-model",
        )

    repaired = turn('{"summary":"literal evidence",}')
    assert repaired.assistant_message["content"] == '{"summary":"literal evidence"}'
    evidence = cast(dict[str, object], repaired.raw_response["answer_parse_evidence"])
    assert evidence["repair_applied"] is True
    message = cast(dict[str, object], repaired.raw_response["message"])
    blocks = cast(list[dict[str, object]], message["content"])
    assert cast(str, blocks[0]["text"]).endswith(",}")
    unseparated = '<think>Not the answer</think>{"summary":"literal evidence"}'
    assert turn(unseparated).assistant_message["content"] == unseparated


@pytest.mark.parametrize(
    "mode,expected,attempts",
    [
        ("received408", RunStatus.COMPLETED, 3),
        ("repeated408", RunStatus.FAILED, 2),
        ("rate-limit", RunStatus.COMPLETED, 3),
        ("quota", RunStatus.FAILED, 1),
        ("unclassified429", RunStatus.FAILED, 1),
        ("broken-stream", RunStatus.FAILED, 1),
        ("model-fallback", RunStatus.HUMAN_INPUT_REQUIRED, 1),
        ("bad-arguments", RunStatus.COMPLETED, 2),
        ("empty-once", RunStatus.COMPLETED, 3),
        ("empty-always", RunStatus.FAILED, 3),
    ],
)
def test_pi_physical_failure_policy(
    tmp_path: Path,
    offline_network: None,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    expected: RunStatus,
    attempts: int,
):
    monkeypatch.setenv("PI_FIXTURE_KEY", mode)

    async def scenario():
        profile = pi_profile()
        provider = PiRuntimeProvider(profile)
        engine = make_engine(tmp_path, provider, handler_calls=[], config=profile.runtime_config())
        try:
            result = await engine.run(request())
            assert result.status == expected
            assert result.metrics is not None
            assert result.metrics.provider_attempts == attempts
            if mode == "broken-stream":
                assert engine.journal.event("run-1.turn.1.interrupted") is not None
            if mode.startswith("empty-"):
                events = engine.journal.events("run-1")
                assert not any(event.event_type == "model.turn.interrupted" for event in events)
                assert sum(
                    event.event_type == "judgment.contract_correction" for event in events
                ) == (1 if mode == "empty-once" else 2)
                assert sum(event.event_type == "model.turn.completed" for event in events) == 3
            replay = await engine.run(request())
            assert replay.terminal_store_hash == result.terminal_store_hash
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["config-drift", "minimum-output"])
def test_pi_pre_dispatch_budget_boundaries(tmp_path: Path, offline_network: None, kind: str):
    async def scenario():
        profile = pi_profile()
        if kind == "minimum-output":
            raw = profile.to_dict()
            raw["reserved_output_tokens"] = 1
            raw.pop("profile_id")
            raw["profile_id"] = f"model-provider-{canonical_hash(raw)}"
            profile = model_provider_profile_from_dict(raw)
        config = profile.runtime_config()
        if kind == "config-drift":
            config = replace(config, budget=replace(config.budget, max_output_tokens=1))
        provider = PiRuntimeProvider(profile)
        engine = make_engine(tmp_path, provider, handler_calls=[], config=config)
        try:
            result = await engine.run(request())
            assert result.status != RunStatus.COMPLETED
            assert not any(
                event.event_type == "model.attempt.dispatched"
                for event in engine.journal.events("run-1")
            )
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("crash", [False, True])
@pytest.mark.parametrize("summary", ["normal", "empty-summary", "whitespace-summary"])
def test_pi_summary_shares_budget_and_committed_compaction_replays(
    tmp_path: Path,
    offline_network: None,
    monkeypatch: pytest.MonkeyPatch,
    crash: bool,
    summary: str,
):
    monkeypatch.setenv("PI_FIXTURE_KEY", summary)

    class CompactOnce(Utf8TokenEstimator):
        checked = False

        def count_request(self, messages: Any, tools: Any) -> int:
            if len(messages) == 1 and isinstance(messages[0], dict):
                native = messages[0].get("messages", [])
                if native and native[-1].get("role") == "toolResult" and not self.checked:
                    self.checked = True
                    return 1_000_000
            return super().count_request(messages, tools)

    async def scenario():
        profile = pi_profile()
        provider = PiRuntimeProvider(profile)
        calls: list[str] = []
        engine = make_engine(
            tmp_path,
            provider,
            handler_calls=calls,
            config=profile.runtime_config(),
            counter=CompactOnce(),
        )
        append = engine.journal.append

        def crash_after(**kwargs: Any):
            event = append(**kwargs)
            if (summary == "normal" and kwargs["event_type"] == "pi.context.compacted") or (
                summary != "normal"
                and kwargs["event_type"] == "model.turn.completed"
                and kwargs["event_id"] == "run-1.turn.2"
            ):
                raise SimulatedCrash()
            return event

        try:
            if crash:
                engine.journal.append = crash_after
                with pytest.raises(SimulatedCrash):
                    await engine.run(request())
                engine.journal.append = append
                engine.token_counter = CompactOnce()
            result = await engine.run(request())
            assert result.metrics is not None and result.terminal_store_hash is not None
            if summary != "normal":
                assert result.status is RunStatus.FAILED
                assert result.metrics.turns == result.metrics.provider_attempts == 2
                events = engine.journal.events("run-1")
                assert not any(event.event_type == "pi.context.compacted" for event in events)
                assert not any(event.event_id == "run-1.turn.3.started" for event in events)
                assert calls == ["official-outage"]
                assert "compaction requires completed nonempty text" in str(
                    engine.artifact_store.read_json(result.terminal_store_hash)
                )
                replay = await engine.run(request())
                assert replay.terminal_store_hash == result.terminal_store_hash
                return
            if result.status != RunStatus.COMPLETED:
                pytest.fail(str(engine.artifact_store.read_json(result.terminal_store_hash)))
            assert calls == ["official-outage"]
            assert result.metrics.turns == 3 and result.metrics.provider_attempts == 3
            assert result.metrics.input_tokens == 300
            assert (
                len(
                    [
                        event
                        for event in engine.journal.events("run-1")
                        if event.event_type == "pi.context.compacted"
                    ]
                )
                == 1
            )
            replay = await engine.run(request())
            assert replay.terminal_store_hash == result.terminal_store_hash
        finally:
            await provider.close()

    asyncio.run(scenario())
