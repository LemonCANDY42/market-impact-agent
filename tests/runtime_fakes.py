"""Scripted pi event double for financial-contract unit tests, never production.

No HTTP parsing, retries, compaction, or protocol implementation lives here.
Those behaviors are covered by test_pi_runtime through the actual pinned Node
entrypoint with only network I/O replaced. This double keeps financial fixtures
independent of native wire syntax and of a paid model's choice of answer.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ModelTurn, RuntimeConfig
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.pi_execution import PiInvocationContext
from market_impact_agent.pi_runtime import PI_RUNTIME, runtime_identity
from market_impact_agent.provider_reliability import (
    ProviderAttemptEvent,
    ProviderAttemptObserver,
    ProviderAttemptPhase,
)


class _Profile:
    def __init__(self, config: RuntimeConfig) -> None:
        self.config = config
        self.model = config.model
        self.provider_id = config.provider_id
        self.pricing = config.pricing
        self.budget = config.budget
        self.reserved_output_tokens = config.reserved_output_tokens
        self.context_window_tokens = config.context_window_tokens
        self.runtime = {"quota_model": config.model}
        self.max_attempts = 1
        self.adapter_kind = "pi-openai-completions"
        self.credential_env = "BUSINESS_FIXTURE_UNUSED_KEY"

    def runtime_config(self) -> RuntimeConfig:
        return self.config

    def to_dict(self) -> dict[str, object]:
        return self.config.to_dict()


class BusinessModelFixture:
    """The answer is supplied by a domain test; Harness callbacks are real."""

    @property
    def runtime_identity(self) -> dict[str, object]:
        return runtime_identity()

    dispatch_allowed = True
    budget: ModelBudget | None = None
    max_concurrent_requests = 3
    profile: ModelProviderProfile

    def bind_runtime(self, config: RuntimeConfig) -> None:
        current = getattr(self, "profile", None)
        if isinstance(current, ModelProviderProfile) and current.runtime_config() == config:
            return
        self.profile = cast(ModelProviderProfile, _Profile(config))

    @property
    def admission_root(self) -> Path:
        return Path(os.environ["MARKET_IMPACT_MODEL_STATE_ROOT"])

    def assert_frozen(self) -> None:
        pass

    def authorize_dispatch(self, invocation_id: str, budget_owner: str) -> None:
        pass

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        pass

    async def close(self) -> None:
        pass

    def context_identity(
        self, run_id: str, tools: list[dict[str, object]], messages: list[dict[str, object]]
    ) -> dict[str, str]:
        return {"conversationId": run_id, "cacheKey": canonical_hash(tools)}

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        raise NotImplementedError

    async def run_once(
        self,
        *,
        context: PiInvocationContext,
        messages: tuple[dict[str, object], ...],
        max_output_tokens: int,
        timeout_seconds: float,
        attempt_observer: ProviderAttemptObserver,
    ) -> ModelTurn:
        # Unit role tests isolate their own validator/ledger from the runtime.
        # Actual zero-tool pi continuation/recovery is separately integration tested.
        attempt_observer(
            ProviderAttemptEvent(
                request_id=f"fixture:{context.ordinal}",
                method="POST",
                physical_attempt=1,
                phase=ProviderAttemptPhase.DISPATCHED,
                elapsed_latency_ms=0,
            )
        )
        turn = await self.answer(
            messages=messages,
            tools=(),
            temperature=1.0,
            top_p=1.0,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        attempt_observer(
            ProviderAttemptEvent(
                request_id=f"fixture:{context.ordinal}",
                method="POST",
                physical_attempt=1,
                phase=ProviderAttemptPhase.SUCCEEDED,
                elapsed_latency_ms=turn.latency_ms,
            )
        )
        return turn

    async def execute(
        self,
        payload: dict[str, object],
        callback: Callable[[str, dict[str, object]], Awaitable[dict[str, object]]],
    ) -> dict[str, object]:
        messages = list(cast(list[dict[str, object]], payload["messages"]))
        tools = tuple(cast(list[dict[str, object]], payload["tools"]))
        # A bounded event script, not an alternative production loop. The only
        # sequencing decision is the real Harness's turn_end stop/correction reply.
        for number in range(1, self.profile.budget.max_turns + 2):
            admitted = await callback(
                "model_admit",
                {
                    "runtime": PI_RUNTIME,
                    "number": number,
                    "purpose": "decision",
                    "context": {"messages": list(messages), "tools": list(tools)},
                },
            )
            replay = admitted.get("replay")
            if replay is None:
                await callback("attempt_start", {"number": number, "attempt": 1})
                turn = await self.answer(
                    messages=tuple(messages),
                    tools=tools,
                    temperature=self.profile.runtime_config().temperature,
                    top_p=self.profile.runtime_config().top_p,
                    max_output_tokens=cast(int, admitted["max_output"]),
                    timeout_seconds=self.profile.budget.max_wall_seconds,
                )
                native = {
                    "role": "assistant",
                    "model": turn.model,
                    "responseId": turn.response_id,
                    "stopReason": turn.finish_reason,
                    "timestamp": 0,
                    "content": [
                        {"type": "text", "text": turn.assistant_message.get("content") or ""},
                        *(
                            {
                                "type": "toolCall",
                                "id": call.call_id,
                                "name": call.name,
                                "arguments": call.arguments,
                            }
                            for call in turn.tool_calls
                        ),
                    ],
                }
                await callback(
                    "model_completed",
                    {
                        "number": number,
                        "purpose": "decision",
                        "message": native,
                        "response_models": [turn.model],
                        "latency_ms": turn.latency_ms,
                        "attempts": turn.attempts,
                        "raw_usage": {
                            "input_tokens": turn.usage.input_tokens,
                            "output_tokens": turn.usage.output_tokens,
                        },
                    },
                )
            else:
                native = cast(dict[str, Any], replay)
            content = cast(list[dict[str, Any]], native["content"])
            messages.append(
                {
                    "role": "assistant",
                    "content": "".join(item["text"] for item in content if item["type"] == "text"),
                }
            )
            for item in content:
                if item["type"] != "toolCall":
                    continue
                response = await callback(
                    "tool",
                    {
                        "call_id": item["id"],
                        "name": item["name"],
                        "arguments": item["arguments"],
                    },
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": item["id"],
                        "name": item["name"],
                        "content": response["content"],
                    }
                )
            result = await callback("turn_end", {})
            if result.get("stop"):
                await callback("agent_end", {})
                return {}
            if result.get("correction"):
                messages.append({"role": "user", "content": result["correction"]})
        raise AssertionError("bounded domain fixture did not reach a Harness terminal")
