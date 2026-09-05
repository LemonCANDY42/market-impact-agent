"""Harness-owned callbacks for the upstream pi loop (not another Agent loop)."""

# This module is the AgentEngine's implementation, not an external consumer of
# its private authority. Keeping the callback body here avoids duplicating it.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ContextEntry,
    ModelProvider,
    ModelTurn,
    ProviderUsage,
    ToolCall,
    ToolExecutionResult,
    Utf8TokenEstimator,
)
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_json import load_model_json
from market_impact_agent.pi_runtime import PI_RUNTIME, ModelSlots
from market_impact_agent.provider_reliability import (
    ProviderAttemptEvent,
    ProviderAttemptObserver,
    ProviderAttemptPhase,
    ProviderFailure,
    ProviderGenerationState,
    ProviderRetryDisposition,
    http_diagnostic_code,
    http_generation_state,
    http_retry_disposition,
    retry_after_seconds,
    retry_is_safe,
)
from market_impact_agent.runtime_store import RunJournal, RuntimeEvent

if TYPE_CHECKING:
    from market_impact_agent.agent_engine import (
        AgentEngine,
        AgentRunRequest,
        AgentRunResult,
        CancellationToken,
        _ExecutionSurface,
        _MutableMetrics,
    )
    from market_impact_agent.agent_runtime import LoadedSkill, RuntimeConfig, TokenCounter
    from market_impact_agent.runtime_store import ArtifactStore, RunRecord


class PrivilegedPiEventWriter(Protocol):
    """Narrow callback surface needed by a pi role on an authoritative Run."""

    def append(
        self,
        *,
        run_id: str,
        event_id: str,
        event_type: str,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> RuntimeEvent: ...


class PiRoleJournal(RunJournal):
    """Bind public pi callbacks to the Harness' signed event writer.

    The adapter contains no business state and is shared by every single-turn
    role.  Business terminals are still written by their owning authority.
    """

    writer: PrivilegedPiEventWriter | None = None
    bound_run_id: str | None = None

    def bind(self, *, run_id: str, writer: PrivilegedPiEventWriter) -> None:
        if self.writer is not None or self.bound_run_id is not None:
            raise ValueError("pi role journal is already bound")
        self.writer = writer
        self.bound_run_id = run_id

    def append(
        self,
        *,
        run_id: str,
        event_id: str,
        event_type: str,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> RuntimeEvent:
        if (
            self.writer is None
            or run_id != self.bound_run_id
            or event_type
            not in {
                "pi.context.frozen",
                "model.turn.started",
                "pi.response.received",
                "pi.role.response.completed",
                "pi.agent.ended",
                "pi.budget.reserved",
                "pi.budget.settled",
            }
        ):
            raise PermissionError("pi role journal has no root-authenticated event writer")
        return self.writer.append(
            run_id=run_id,
            event_id=event_id,
            event_type=event_type,
            observed_at=observed_at,
            payload=payload,
        )


def _stable_context(value: object) -> object:
    # pi summary prompts include creation timestamps; timestamps are receipt metadata,
    # never LLM content. Keep originals, excluding only message.timestamp for comparison.
    if isinstance(value, dict):
        return {
            key: _stable_context(item)
            for key, item in cast(dict[str, object], value).items()
            if not (key == "timestamp" and "role" in value)
        }
    if isinstance(value, list):
        return [_stable_context(item) for item in cast(list[object], value)]
    return value


def native_usage(raw: object) -> ProviderUsage:
    if not isinstance(raw, dict):
        raise ValueError("Provider usage unavailable; cannot assert zero usage")
    usage = cast(dict[str, object], raw)
    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens"))
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens"))
    if type(input_tokens) is not int or type(output_tokens) is not int:
        raise ValueError("Provider input/output usage unavailable")
    details = usage.get("input_tokens_details", usage.get("prompt_tokens_details"))
    fields = cast(dict[str, object], details) if isinstance(details, dict) else {}
    cached = fields.get("cached_tokens", usage.get("cached_tokens"))
    written = fields.get("cache_write_tokens")
    return ProviderUsage(
        input_tokens,
        output_tokens,
        cache_read_tokens=cached if type(cached) is int else None,
        cache_write_tokens=written if type(written) is int else None,
    )


def _call_id(value: object) -> str:
    return f"pi-call-{canonical_hash(value)}"


def native_turn(payload: dict[str, object], model: str) -> ModelTurn:
    message = cast(dict[str, object], payload["message"])
    if (
        message.get("model") != model
        or payload.get("response_models") != [model]
        or message.get("responseModel", model) != model
    ):
        raise ValueError("pi returned a different model identity")
    content = cast(list[dict[str, object]], message["content"])
    calls = tuple(
        ToolCall(
            call_id=_call_id(block["id"]),
            name=cast(str, block["name"]),
            arguments=cast(dict[str, object], block["arguments"]),
        )
        for block in content
        if block["type"] == "toolCall"
    )
    assistant: dict[str, object] = {
        "role": "assistant",
        "content": "".join(
            cast(str, block["text"]) for block in content if block["type"] == "text"
        ),
    }
    if calls:
        assistant["tool_calls"] = [
            {
                "id": call.call_id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": json.dumps(call.arguments, separators=(",", ":")),
                },
            }
            for call in calls
        ]
    raw_response = payload
    if not calls and message["stopReason"] == "stop":
        try:
            parsed = load_model_json(cast(str, assistant["content"]))
        except ValueError:
            # Leave malformed/ambiguous content intact for the normal business
            # correction or abstention path; never extract answers from reasoning.
            pass
        else:
            assistant["content"] = json.dumps(
                parsed.value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            )
            raw_response = {**payload, "answer_parse_evidence": parsed.evidence.to_dict()}
    return ModelTurn(
        response_id=cast(str, message.get("responseId") or f"pi-{canonical_hash(message)}"),
        model=model,
        assistant_message=assistant,
        tool_calls=calls,
        finish_reason=cast(str, message["stopReason"]),
        usage=native_usage(payload.get("raw_usage")),
        raw_response=raw_response,
        latency_ms=float(cast(float, payload["latency_ms"])),
        attempts=cast(int, payload["attempts"]),
    )


def _validate_compaction_turn(turn: ModelTurn, purpose: object) -> None:
    # Response receipt/usage is authoritative even when its summary is unusable.
    # Apply the same semantic gate after a fresh receipt AND completed-turn replay.
    if purpose == "compaction" and (
        turn.tool_calls
        or turn.finish_reason != "stop"
        or not str(turn.assistant_message.get("content") or "").strip()
    ):
        raise ValueError("compaction requires completed nonempty text, not tool calls")


class PiRequestBoundary:
    """Shared physical-call authority for every pi role, including summaries.

    State here is in-memory only. Durable ownership remains the caller's Journal;
    callers supply their existing signed writes and business projections.
    """

    def __init__(
        self,
        *,
        provider: ModelProvider,
        invocation_id: str,
        journal: RunJournal,
        artifacts: ArtifactStore,
        config: RuntimeConfig,
        counter: TokenCounter,
        metrics: _MutableMetrics,
        append: Callable[[str, str, dict[str, object]], None],
        load: Callable[[RuntimeEvent], ModelTurn],
        commit: Callable[[int, ModelTurn], object],
        record: Callable[[ModelTurn, int], None],
        observe: Callable[[int, ProviderAttemptEvent], None],
        check: Callable[[], None],
        check_identity: Callable[[], None],
        sanitize: Callable[[ModelTurn], ModelTurn],
        budget: ModelBudget,
    ) -> None:
        self.provider, self.profile = provider, provider.profile
        self.invocation_id, self.journal, self.artifacts = invocation_id, journal, artifacts
        self.config, self.counter, self.metrics = config, counter, metrics
        self.budget = budget
        self.reserved_cost = 0
        self.append, self.load, self.commit, self.record = append, load, commit, record
        self.observe, self.check, self.check_identity, self.sanitize = (
            observe,
            check,
            check_identity,
            sanitize,
        )
        self.current_number = 0
        self.regeneration_used: set[int] = set()
        self.slots = ModelSlots(
            provider.admission_root,
            cast(str, (provider.profile.runtime or {}).get("quota_model", provider.profile.model)),
            provider.max_concurrent_requests,
        )

    async def callback(self, method: str, payload: dict[str, object]) -> dict[str, object] | None:
        from market_impact_agent.agent_engine import _BudgetExceeded, _ModelTurnInterrupted

        self.check()
        if method == "context_check":
            estimate = self.counter.count_request(
                (cast(dict[str, object], payload["context"]),), ()
            )
            return {"compact": estimate >= self.profile.effective_compaction_trigger_tokens}
        if method == "model_admit":
            number = cast(int, payload["number"])
            if payload["runtime"] != PI_RUNTIME or number != self.metrics.turns + 1:
                raise ValueError("pi request runtime/order differs from durable owner")
            self.current_number = number
            event_id = f"{self.invocation_id}.turn.{number}"
            frozen_id = f"{self.invocation_id}.pi.input.{number}"
            original = self.journal.event(frozen_id)
            context_hash = canonical_hash(_stable_context(payload["context"]))
            budget_binding = {
                "owner": self.budget.owner_run_id,
                "limits": self.budget.binding,
            }
            if original is not None:
                if (
                    original.payload["context_hash"] != context_hash
                    or original.payload["purpose"] != payload["purpose"]
                    or original.payload["budget"] != budget_binding
                ):
                    raise ValueError("pi replay context differs from frozen request")
            else:
                artifact = self.artifacts.put_json(payload)
                self.append(
                    f"pi.input.{number}",
                    "pi.context.frozen",
                    {
                        "artifact_hash": artifact.content_hash,
                        "context_hash": context_hash,
                        "purpose": payload["purpose"],
                        "runtime": PI_RUNTIME,
                        "budget": budget_binding,
                    },
                )
            existing = self.journal.event(event_id)
            if existing is not None:
                turn = self.load(existing)
                self.record(turn, number)
                _validate_compaction_turn(turn, payload["purpose"])
                return {"replay": turn.raw_response["message"]}
            received = self.journal.event(f"{self.invocation_id}.pi.response.{number}")
            if received is not None:
                # A response may be durable while its business projection is not.
                # Rebuild that projection; never issue another physical request.
                raw = cast(
                    dict[str, object],
                    self.artifacts.read_json(cast(str, received.payload["artifact_hash"])),
                )
                if raw["number"] != number or raw["purpose"] != payload["purpose"]:
                    raise ValueError("durable pi response is not bound to this request")
                turn = self.commit_received(raw)
                return {"replay": turn.raw_response["message"]}
            if self.journal.event(f"{event_id}.started") is not None:
                raise _ModelTurnInterrupted("pi request has no durable completion; no regeneration")
            if not self.provider.dispatch_allowed:
                raise PermissionError("read-only pi binding cannot dispatch a model request")
            self.provider.authorize_dispatch(self.invocation_id, self.budget.owner_run_id)
            if number > self.config.budget.max_turns:
                raise _BudgetExceeded("run exhausted its model-turn budget including summaries")
            estimate = self.counter.count_request(
                (cast(dict[str, object], payload["context"]),), ()
            )
            if estimate + self.profile.reserved_output_tokens > self.profile.context_window_tokens:
                raise _BudgetExceeded("pi context exceeds frozen model window")
            if self.metrics.input_tokens + estimate > self.config.budget.max_input_tokens:
                raise _BudgetExceeded("pi request lacks input budget")
            maximum = min(
                self.config.reserved_output_tokens,
                self.config.budget.max_output_tokens - self.metrics.output_tokens,
            )
            if self.config.budget.max_estimated_cost_microusd is not None:
                maximum = min(
                    maximum,
                    self.profile.pricing.affordable_output_tokens(
                        remaining_microusd=self.config.budget.max_estimated_cost_microusd
                        - self.metrics.estimated_cost_microusd,
                        estimated_input_tokens=estimate,
                    ),
                )
            minimum = 16 if self.profile.adapter_kind == "pi-openai-responses" else 1
            if maximum < minimum:
                raise _BudgetExceeded("pi request lacks output/cost budget")
            self.reserved_cost = self.profile.pricing.estimate_microusd(
                ProviderUsage(estimate, maximum)
            )
            await self.slots.acquire()
            self.check()
            self.check_identity()
            self.append(
                f"turn.{number}.started",
                "model.turn.started",
                {"turn_number": number, "attempt_observation": "physical"},
            )
            return {"max_output": maximum}
        if method == "attempt_start":
            number, attempt = cast(int, payload["number"]), cast(int, payload["attempt"])
            if number != self.current_number or not 1 <= attempt <= self.profile.max_attempts:
                raise ValueError("physical attempt is outside its admitted request")
            await self.budget.reserve(
                f"{self.invocation_id}:{number}:{attempt}", self.reserved_cost
            )
            self.observe(
                number,
                ProviderAttemptEvent(
                    request_id=f"{self.invocation_id}:{number}",
                    method="POST",
                    physical_attempt=attempt,
                    phase=ProviderAttemptPhase.DISPATCHED,
                    elapsed_latency_ms=0,
                ),
            )
            return {}
        if method == "attempt_end":
            number, attempt = cast(int, payload["number"]), cast(int, payload["attempt"])
            status = cast(int | None, payload["status"])
            code = (
                http_diagnostic_code(status, str(payload.get("error_code", "")))
                if status is not None
                else "transport_unknown"
            )
            if (
                status == 429
                and code == "rate_limited"
                and payload.get("error_code")
                not in {"rate_limited", "rate_limit_exceeded", "too_many_requests"}
            ):
                code = "http_429_unclassified"
            state = (
                http_generation_state("POST", status, code)
                if status is not None
                else ProviderGenerationState.UNKNOWN
            )
            disposition = (
                http_retry_disposition("POST", status, code, state)
                if status is not None
                else ProviderRetryDisposition.FORBIDDEN
            )
            failure = ProviderFailure(
                "Provider request failed",
                error_class=code,
                diagnostic_code=code,
                http_status=status,
                generation_state=state,
                retry_disposition=disposition,
                attempts=attempt,
                elapsed_latency_ms=cast(float, payload["latency_ms"]),
            )
            regenerate = (
                status == 408
                and self.profile.retry_received_408_once
                and number not in self.regeneration_used
            )
            retry = (
                retry_is_safe("POST", failure) or regenerate
            ) and attempt < self.profile.max_attempts
            if regenerate and retry:
                self.regeneration_used.add(number)
                failure.retry_disposition = ProviderRetryDisposition.AUTHORIZED_REGENERATION
            self.observe(
                number,
                ProviderAttemptEvent(
                    request_id=f"{self.invocation_id}:{number}",
                    method="POST",
                    physical_attempt=attempt,
                    phase=ProviderAttemptPhase.FAILED,
                    elapsed_latency_ms=failure.elapsed_latency_ms,
                    failure=failure,
                ),
            )
            if state is ProviderGenerationState.NOT_STARTED:
                self.budget.settle(
                    f"{self.invocation_id}:{number}:{attempt}",
                    cost_microusd=0,
                    evidence_ref=f"provider-rejection:{canonical_hash(payload)}",
                )
            if not retry:
                raise failure
            delay = max(
                1.0 if regenerate else 0, self.profile.retry_backoff_seconds * 2 ** (attempt - 1)
            )
            retry_after = retry_after_seconds(cast(str | None, payload.get("retry_after")))
            if retry_after is not None:
                delay = max(delay, min(60.0, retry_after))
            return {"retry": True, "delay_ms": delay * 1000}
        if method == "model_completed":
            number = cast(int, payload["number"])
            if number != self.current_number:
                raise ValueError("pi response has no matching admission")
            # Preserve even malformed/error responses before validation, never print raw content.
            artifact = self.artifacts.put_json(payload)
            self.append(
                f"pi.response.{number}",
                "pi.response.received",
                {
                    "artifact_hash": artifact.content_hash,
                    "request_id": f"{self.invocation_id}:{number}",
                },
            )
            self.commit_received(payload)
            return {}
        return None

    def commit_received(self, payload: dict[str, object]) -> ModelTurn:
        from market_impact_agent.agent_engine import _ModelTurnInterrupted

        number = cast(int, payload["number"])
        native = cast(dict[str, object], payload["message"])
        if native.get("stopReason") in {"error", "aborted"}:
            raise _ModelTurnInterrupted(
                "pi returned an incomplete response; inspect native artifact"
            )
        try:
            turn = self.sanitize(native_turn(payload, self.profile.model))
        except ValueError as exc:
            raise _ModelTurnInterrupted(str(exc)) from exc
        self.observe(
            number,
            ProviderAttemptEvent(
                request_id=f"{self.invocation_id}:{number}",
                method="POST",
                physical_attempt=turn.attempts,
                phase=ProviderAttemptPhase.SUCCEEDED,
                elapsed_latency_ms=turn.latency_ms,
            ),
        )
        self.budget.settle(
            f"{self.invocation_id}:{number}:{turn.attempts}",
            cost_microusd=self.profile.pricing.estimate_microusd(turn.usage),
            evidence_ref=canonical_hash(payload),
        )
        self.commit(number, turn)
        self.record(turn, number)
        self.slots.release()
        _validate_compaction_turn(turn, payload["purpose"])
        return turn


@dataclass(frozen=True, slots=True)
class PiInvocationContext:
    """Bind a role invocation to its existing business Run, not a second Run."""

    run_id: str
    ordinal: int
    journal: RunJournal
    artifacts: ArtifactStore
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    secret_values: tuple[str, ...] = ()

    @property
    def invocation_id(self) -> str:
        if self.ordinal < 1:
            raise ValueError("pi invocation ordinal must be positive")
        self.journal.get_run(self.run_id)
        return f"{self.run_id}.pi-invocation.{self.ordinal}"


async def execute_pi_once(
    provider: ModelProvider,
    *,
    context: PiInvocationContext,
    messages: tuple[dict[str, object], ...],
    max_output_tokens: int,
    timeout_seconds: float,
    attempt_observer: ProviderAttemptObserver,
) -> ModelTurn:
    """A zero-tool, single-turn role uses the same pi loop and physical boundary.

    The calling role retains its validator and business terminal. Its native reply
    is durable before returning, including when business validation later fails.
    """
    from market_impact_agent.agent_engine import _MutableMetrics, sanitized_model_turn

    invocation_id = context.invocation_id
    profile = provider.profile
    maximum = min(max_output_tokens, profile.reserved_output_tokens)
    if maximum < 1:
        raise ValueError("pi role requires a positive output allowance")
    config = replace(
        profile.runtime_config(),
        reserved_output_tokens=maximum,
        budget=replace(profile.budget, max_turns=1, max_output_tokens=maximum),
    )
    metrics = _MutableMetrics()
    result: ModelTurn | None = None

    def append(suffix: str, kind: str, payload: dict[str, object]) -> None:
        context.journal.append(
            run_id=context.run_id,
            event_id=f"{invocation_id}.{suffix}",
            event_type=kind,
            observed_at=context.clock(),
            payload=payload,
        )

    def record(turn: ModelTurn, number: int) -> None:
        nonlocal result
        result = turn
        metrics.turns = number
        metrics.input_tokens += turn.usage.input_tokens
        metrics.output_tokens += turn.usage.output_tokens
        metrics.estimated_cost_microusd += profile.pricing.estimate_microusd(turn.usage)

    def commit(number: int, turn: ModelTurn) -> None:
        artifact = context.artifacts.put_json(turn.raw_response)
        append(
            f"turn.{number}",
            "pi.role.response.completed",
            {
                "artifact_hash": artifact.content_hash,
                "runtime": provider.runtime_identity,
            },
        )

    def load(event: RuntimeEvent) -> ModelTurn:
        if event.payload["runtime"] != provider.runtime_identity:
            raise ValueError("unfinished role belongs to another pi runtime build")
        raw = context.artifacts.read_json(cast(str, event.payload["artifact_hash"]))
        return native_turn(cast(dict[str, object], raw), profile.model)

    def observe(number: int, event: ProviderAttemptEvent) -> None:
        _ = number
        attempt_observer(event)

    # Preserve pi's native continuation rather than coercing assistant/thinking
    # records into user text. Historical projection-only contexts cannot dispatch.
    fixed: list[dict[str, object]] = []
    native: list[dict[str, object]] = []
    assistant_number = 0
    for message in messages:
        if message["role"] == "assistant":
            assistant_number += 1
            prior = context.journal.event(
                f"{context.run_id}.pi-invocation.{assistant_number}.turn.1"
            )
            if prior is None:
                raise ValueError("role continuation has no durable native pi message")
            turn = load(prior)
            if turn.assistant_message != message:
                raise ValueError("role continuation differs from its native response")
            native.append(cast(dict[str, object], turn.raw_response["message"]))
        elif message["role"] == "user":
            native.append({"role": "user", "content": message["content"], "timestamp": 0})
        elif message["role"] == "system" and not assistant_number:
            fixed.append(message)
        else:
            raise PermissionError("single-turn role cannot import tools or alter system policy")

    # pi appends current prompts AFTER ordered native history. Only trailing
    # user messages are current prompts; the original question belongs before
    # its saved assistant answer, not after a later corrective instruction.
    pending: list[dict[str, object]] = []
    while native and native[-1]["role"] == "user":
        message = native.pop()
        pending.append({"role": "user", "content": message["content"]})
    fixed.extend(reversed(pending))

    boundary = PiRequestBoundary(
        provider=provider,
        invocation_id=invocation_id,
        journal=context.journal,
        artifacts=context.artifacts,
        config=config,
        counter=Utf8TokenEstimator(),
        metrics=metrics,
        append=append,
        load=load,
        commit=commit,
        record=record,
        observe=observe,
        check=lambda: None,
        check_identity=provider.assert_frozen,
        sanitize=lambda turn: sanitized_model_turn(turn, context.secret_values),
        budget=provider.budget
        or ModelBudget(
            context.journal,
            context.run_id,
            profile.budget.max_turns * profile.max_attempts,
            profile.budget.max_estimated_cost_microusd,
        ),
    )

    async def callback(method: str, payload: dict[str, object]) -> dict[str, object]:
        serialized = json.dumps(payload, ensure_ascii=False)
        if any(value and value in serialized for value in context.secret_values):
            raise PermissionError("pi role response contains protected secret material")
        value = await boundary.callback(method, payload)
        if value is not None:
            return value
        if method == "turn_end":
            if result is None or result.tool_calls:
                raise PermissionError("single-turn role has no authorized final response")
            return {"stop": True}
        if method == "agent_end":
            append("ended", "pi.agent.ended", {"runtime": provider.runtime_identity})
            return {}
        raise PermissionError("single-turn role has no delegated tool capability")

    try:
        await asyncio.wait_for(
            provider.execute(
                {
                    "runId": invocation_id,
                    **provider.context_identity(context.run_id, [], fixed),
                    "profile": profile.to_dict(),
                    "messages": fixed,
                    "nativeMessages": native,
                    "tools": [],
                },
                callback,
            ),
            timeout=min(timeout_seconds, profile.budget.max_wall_seconds),
        )
    finally:
        boundary.slots.release()
    if result is None:
        raise RuntimeError("pi role ended without a durable response")
    return result


async def execute_pi(
    engine: AgentEngine,
    *,
    provider: ModelProvider,
    request: AgentRunRequest,
    loaded_skills: tuple[LoadedSkill, ...],
    prompt_entries: tuple[ContextEntry, ...],
    prompt_hash: str,
    surface: _ExecutionSurface,
    record: RunRecord,
    cancellation: CancellationToken,
    metrics: _MutableMetrics,
) -> AgentRunResult:
    # Existing business validators/terminal writer stay owned by AgentEngine.
    from market_impact_agent.agent_engine import (
        _ModelTurnInterrupted,
        _proposal_from_assistant,
        assistant_context_entry,
        contract_correction_entry,
        enforce_run_budgets,
        sanitized_model_turn,
        tool_context_entry,
    )

    entries = list(prompt_entries)
    profile = provider.profile
    if engine.config != profile.runtime_config():
        raise ValueError("pi RuntimeConfig must match its frozen registered Provider Profile")
    corrections = 0
    current_number = 0
    current_turn: ModelTurn | None = None
    completed_result: AgentRunResult | None = None
    final_proposal = None

    def append(suffix: str, kind: str, payload: dict[str, object]) -> None:
        engine._append_privileged_event(
            run_id=request.run_id,
            event_id=f"{request.run_id}.{suffix}",
            event_type=kind,
            observed_at=engine._now(),
            payload=payload,
        )

    def record_turn(turn: ModelTurn, number: int) -> None:
        nonlocal current_turn
        current_turn = turn
        engine._record_turn_metrics(metrics, turn)
        metrics.tool_calls += len(turn.tool_calls)
        entries.append(assistant_context_entry(request.run_id, number, turn))
        enforce_run_budgets(metrics, engine.config)

    boundary = PiRequestBoundary(
        provider=provider,
        invocation_id=request.run_id,
        journal=engine.journal,
        artifacts=engine.artifact_store,
        config=engine.config,
        counter=engine.token_counter,
        metrics=metrics,
        append=append,
        load=lambda event: engine._load_turn(event, surface=surface),
        commit=lambda number, turn: engine._store_turn(
            request.run_id, number, turn, surface=surface, context_before_turn=tuple(entries)
        ),
        record=record_turn,
        observe=lambda number, event: engine._observe_attempt(request.run_id, number, event),
        check=lambda: engine._check_cancel(cancellation),
        check_identity=engine._validate_active_provider_identity,
        sanitize=lambda turn: sanitized_model_turn(turn, engine.secret_values),
        budget=provider.budget
        or ModelBudget(
            engine.journal,
            request.run_id,
            engine.config.budget.max_turns * profile.max_attempts,
            engine.config.budget.max_estimated_cost_microusd,
            append=append,
            check_cancel=lambda: engine._check_cancel(cancellation),
        ),
    )

    async def callback(method: str, payload: dict[str, object]) -> dict[str, object]:
        nonlocal current_number, current_turn, corrections, completed_result, final_proposal
        engine._check_cancel(cancellation)
        engine._assert_no_secret(payload)
        response = await boundary.callback(method, payload)
        current_number = boundary.current_number
        if response is not None:
            return response
        if method == "tool":
            if current_turn is None:
                raise ValueError("tool precedes a durable model response")
            call = ToolCall(
                _call_id(payload["call_id"]),
                cast(str, payload["name"]),
                cast(dict[str, object], payload["arguments"]),
            )
            if call not in current_turn.tool_calls or current_turn.finish_reason == "length":
                raise PermissionError("tool is not a complete authorized model call")
            result = await engine._execute_or_replay_tool(
                run_id=request.run_id, call=call, access=request.tool_access, surface=surface
            )
            metrics.result_bytes += result.result_artifact.size_bytes
            enforce_run_budgets(metrics, engine.config)
            entries.append(tool_context_entry(request.run_id, current_number, result))
            return {"content": result.model_content}
        if method == "tool_message":
            # pi can produce argument/unknown-tool errors without executing Python.
            native = cast(dict[str, object], payload["message"])
            tool_event_id = f"{request.run_id}.tool.{_call_id(native['toolCallId'])}"
            if engine.journal.event(tool_event_id) is None:
                if current_turn is None or native.get("isError") is not True:
                    raise ValueError("pi tool result has no authorized completion")
                call = next(
                    (
                        item
                        for item in current_turn.tool_calls
                        if item.call_id == _call_id(native["toolCallId"])
                    ),
                    None,
                )
                if call is None:
                    raise PermissionError("pi rejected an unbound tool call")
                # Unknown/unoffered capabilities still stop. Argument errors on an
                # offered tool remain ordinary upstream tool errors, not empty data.
                engine.tool_registry.manifest_hash(call.name, request.tool_access)
                content = cast(list[dict[str, object]], native["content"])
                text = "\n".join(
                    cast(str, item["text"]) for item in content if item["type"] == "text"
                )
                artifact = engine.artifact_store.put_json(native)
                result = ToolExecutionResult(call.call_id, call.name, artifact, text, True, False)
                engine._store_tool_result(
                    request.run_id, call, result, access=request.tool_access, surface=surface
                )
                entries.append(tool_context_entry(request.run_id, current_number, result))
                metrics.result_bytes += artifact.size_bytes
                enforce_run_budgets(metrics, engine.config)
            return {}
        if method == "turn_end":
            if current_turn is None:
                raise ValueError("turn completed without a durable model response")
            if current_turn.tool_calls:
                return {"stop": False}
            try:
                proposal = _proposal_from_assistant(current_turn)
                proposal.validate_against(request.evidence_pack)
            except (TypeError, ValueError) as exc:
                if corrections >= 2:
                    raise ValueError(
                        "model failed Judgment contract after two corrections"
                    ) from exc
                corrections += 1
                correction = contract_correction_entry(
                    request=request, correction_number=corrections, error=exc
                )
                entries.append(correction)
                append(
                    f"contract-correction.{corrections}",
                    "judgment.contract_correction",
                    {
                        "correction_number": corrections,
                        "error_class": type(exc).__name__,
                        "error": engine._redacted_message(str(exc)),
                        "invalid_response_hash": current_turn.raw_response_hash,
                    },
                )
                return {"stop": False, "correction": correction.content}
            final_proposal = proposal
            return {"stop": True}
        if method == "agent_end":
            # v0.84.4 no longer calls prepareNextTurn on a terminal turn.
            if final_proposal is None or current_turn is None:
                raise ValueError("pi ended without a validated Judgment")
            append(
                "pi.ended", "pi.agent.ended", {"call_number": metrics.turns, "runtime": PI_RUNTIME}
            )
            completed_result = engine._finish_judgment(
                request=request,
                loaded_skills=loaded_skills,
                prompt_hash=prompt_hash,
                surface=surface,
                record=record,
                metrics=metrics,
                proposal=final_proposal,
                entries=tuple(entries),
                last_raw_response_hash=current_turn.raw_response_hash,
            )
            return {}
        if method == "compaction_lookup":
            event = engine.journal.event(f"{request.run_id}.pi.compaction.{payload['number']}")
            if event is None:
                return {}
            for number in range(metrics.turns + 1, cast(int, event.payload["call_number"]) + 1):
                turn_event = engine.journal.event(f"{request.run_id}.turn.{number}")
                if turn_event is None:
                    raise ValueError("pi compaction has no durable summary response")
                record_turn(engine._load_turn(turn_event, surface=surface), number)
            return {
                "entry": engine.artifact_store.read_json(cast(str, event.payload["entry_hash"])),
                "call_number": event.payload["call_number"],
            }
        if method == "compaction_commit":
            artifact = engine.artifact_store.put_json(payload["entry"])
            append(
                f"pi.compaction.{payload['number']}",
                "pi.context.compacted",
                {
                    "entry_hash": artifact.content_hash,
                    "call_number": payload["call_number"],
                },
            )
            return {}
        raise PermissionError("unregistered pi callback")

    try:
        await provider.execute(
            {
                "runId": request.run_id,
                **provider.context_identity(
                    request.run_id,
                    list(surface.model_tools),
                    [entry.to_message() for entry in prompt_entries],
                ),
                "profile": profile.to_dict(),
                "messages": [entry.to_message() for entry in prompt_entries],
                "tools": list(surface.model_tools),
            },
            callback,
        )
    except Exception:
        # A lost final IPC reply cannot replace a committed successful terminal.
        if completed_result is None:
            raise
    finally:
        boundary.slots.release()
    if completed_result is None:
        raise _ModelTurnInterrupted("pi exited without a durable Harness terminal")
    return completed_result
