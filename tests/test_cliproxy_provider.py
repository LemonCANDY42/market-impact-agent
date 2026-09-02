import asyncio
import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import ClassVar, cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.cliproxy_provider import (
    CLIProxyLunaConfig,
    CLIProxyLunaProvider,
    CLIProxyProviderError,
)
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    load_model_provider_profile,
)
from market_impact_agent.openai_chat_provider import (
    JsonHttpTransport,
    PinnedUrllibJsonTransport,
)
from market_impact_agent.provider_reliability import (
    ProviderGenerationState,
    ProviderRetryDisposition,
)

PROFILE = Path("examples/providers/cliproxyapi-luna-xhigh-v1.json")
CPA_PROFILE = Path("examples/providers/cliproxyapi-luna-xhigh-cpa-v1.json")
CPA_MAX_PROFILE = Path("examples/providers/cliproxyapi-luna-max-cpa-v1.json")


class FixtureTransport(JsonHttpTransport):
    def __init__(self, responses: list[dict[str, object] | CLIProxyProviderError]) -> None:
        self.responses = responses
        self.requests: list[dict[str, object]] = []

    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, CLIProxyProviderError):
            raise response
        return response


class JsonServerHandler(BaseHTTPRequestHandler):
    hits: ClassVar[list[tuple[str, str | None]]] = []
    server_label = ""
    response_status = 500

    def do_GET(self) -> None:
        type(self).hits.append((self.path, self.headers.get("Authorization")))
        body = json.dumps({"served_by": type(self).server_label}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        type(self).hits.append((self.path, self.headers.get("Authorization")))
        body = json.dumps({"error": type(self).server_label}).encode()
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        _ = (format, args)


@contextmanager
def json_server(
    label: str, *, status: int = 500
) -> Generator[tuple[ThreadingHTTPServer, type[JsonServerHandler]]]:
    class Handler(JsonServerHandler):
        hits: ClassVar[list[tuple[str, str | None]]] = []
        server_label = label
        response_status = status

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, Handler
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _completion() -> dict[str, object]:
    return {
        "id": "chatcmpl-luna-1",
        "model": "gpt-5.6-luna",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-luna-1",
                            "type": "function",
                            "function": {
                                "name": "read_evidence",
                                "arguments": json.dumps({"evidence_id": "ev-1"}),
                            },
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 21, "completion_tokens": 13, "total_tokens": 34},
    }


def _provider(
    transport: JsonHttpTransport, *, reasoning_effort: str = "xhigh"
) -> CLIProxyLunaProvider:
    return CLIProxyLunaProvider(
        api_key="dedicated-local-key",
        config=CLIProxyLunaConfig(
            origin="http://127.0.0.1:8317",
            model="gpt-5.6-luna",
            reasoning_effort=reasoning_effort,
            retry_backoff_seconds=0,
        ),
        transport=transport,
    )


def test_cliproxy_profile_freezes_luna_xhigh_and_zero_marginal_pricing() -> None:
    payload = json.loads(PROFILE.read_text(encoding="utf-8"))
    profile = load_model_provider_profile(PROFILE)

    assert validate_agent_contract(payload, "model-provider-profile.schema.json") == ()
    assert profile.profile_id == f"model-provider-{profile.profile_hash}"
    assert profile.model == "gpt-5.6-luna"
    assert profile.reasoning_effort == "xhigh"
    assert profile.credential_env == "MARKET_IMPACT_CLIPROXY_API_KEY"
    assert profile.pricing.input_microusd_per_million_tokens == 0


def test_cliproxy_cpa_max_profile_has_a_new_identity_and_cost_cap() -> None:
    xhigh_payload = json.loads(CPA_PROFILE.read_text(encoding="utf-8"))
    max_payload = json.loads(CPA_MAX_PROFILE.read_text(encoding="utf-8"))
    xhigh_profile = load_model_provider_profile(CPA_PROFILE)
    max_profile = load_model_provider_profile(CPA_MAX_PROFILE)

    assert validate_agent_contract(xhigh_payload, "model-provider-profile.schema.json") == ()
    assert validate_agent_contract(max_payload, "model-provider-profile.schema.json") == ()
    assert xhigh_profile.reasoning_effort == "xhigh"
    assert max_profile.reasoning_effort == "max"
    assert xhigh_profile.profile_id == f"model-provider-{xhigh_profile.profile_hash}"
    assert max_profile.profile_id == f"model-provider-{max_profile.profile_hash}"
    assert max_profile.profile_id != xhigh_profile.profile_id
    assert max_profile.budget.max_turns == xhigh_profile.budget.max_turns
    assert max_profile.budget.max_tool_calls == xhigh_profile.budget.max_tool_calls
    assert max_profile.budget.max_input_tokens == xhigh_profile.budget.max_input_tokens
    assert max_profile.budget.max_output_tokens == xhigh_profile.budget.max_output_tokens
    assert max_profile.budget.max_wall_seconds == xhigh_profile.budget.max_wall_seconds
    assert max_profile.budget.max_result_bytes == xhigh_profile.budget.max_result_bytes
    xhigh_cost_cap = xhigh_profile.budget.max_estimated_cost_microusd
    assert xhigh_cost_cap is not None
    assert max_profile.budget.max_estimated_cost_microusd == xhigh_cost_cap * 3 // 2
    assert max_profile.pricing == xhigh_profile.pricing


def test_cliproxy_factory_uses_dedicated_environment_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = load_model_provider_profile(PROFILE)
    monkeypatch.setenv("MARKET_IMPACT_CLIPROXY_API_KEY", "dedicated-local-key")

    selected = ModelProviderFactory.with_builtin_adapters().create(profile)

    assert selected.provider_id == "cliproxyapi-openai-compatible"
    assert selected.model == "gpt-5.6-luna"


def test_exact_loopback_transport_never_sends_credentials_to_environment_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        json_server("target") as (target, TargetHandler),
        json_server("proxy") as (
            proxy,
            ProxyHandler,
        ),
    ):
        target_origin = f"http://127.0.0.1:{target.server_port}"
        proxy_origin = f"http://127.0.0.1:{proxy.server_port}"
        monkeypatch.setenv("http_proxy", proxy_origin)
        monkeypatch.setenv("HTTP_PROXY", proxy_origin)
        monkeypatch.delenv("no_proxy", raising=False)
        monkeypatch.delenv("NO_PROXY", raising=False)

        response = PinnedUrllibJsonTransport(
            allowed_origin=target_origin,
            provider_label="CLIProxyAPI",
        ).request_json(
            method="GET",
            url=f"{target_origin}/v1/models",
            headers={"Authorization": "Bearer dedicated-local-key"},
            payload=None,
            timeout_seconds=2,
        )

    assert response == {"served_by": "target"}
    assert TargetHandler.hits == [("/v1/models", "Bearer dedicated-local-key")]
    assert ProxyHandler.hits == []


def test_http_500_body_recognizes_tls_bad_record_mac_without_persisting_body() -> None:
    with json_server("tls: bad record MAC SECRET-BODY") as (server, Handler):
        origin = f"http://127.0.0.1:{server.server_port}"
        with pytest.raises(CLIProxyProviderError) as captured:
            PinnedUrllibJsonTransport(
                allowed_origin=origin,
                provider_label="CLIProxyAPI",
            ).request_json(
                method="POST",
                url=f"{origin}/v1/chat/completions",
                headers={"Authorization": "Bearer dedicated-local-key"},
                payload={"model": "gpt-5.6-luna"},
                timeout_seconds=2,
            )

    assert Handler.hits == [("/v1/chat/completions", "Bearer dedicated-local-key")]
    assert captured.value.error_class == "tls"
    assert captured.value.diagnostic_code == "tls_bad_record_mac"
    assert captured.value.generation_state is ProviderGenerationState.UNKNOWN
    assert captured.value.retry_disposition is ProviderRetryDisposition.FORBIDDEN
    assert "SECRET-BODY" not in str(captured.value)


def test_http_408_incomplete_upstream_stream_is_ambiguous_and_forbidden() -> None:
    body = (
        "stream error: stream disconnected before completion: "
        "stream closed before response.completed"
    )
    with json_server(body, status=408) as (server, Handler):
        origin = f"http://127.0.0.1:{server.server_port}"
        with pytest.raises(CLIProxyProviderError) as captured:
            PinnedUrllibJsonTransport(
                allowed_origin=origin,
                provider_label="CLIProxyAPI",
            ).request_json(
                method="POST",
                url=f"{origin}/v1/chat/completions",
                headers={"Authorization": "Bearer dedicated-local-key"},
                payload={"model": "gpt-5.6-luna"},
                timeout_seconds=2,
            )

    assert Handler.hits == [("/v1/chat/completions", "Bearer dedicated-local-key")]
    assert captured.value.error_class == "http"
    assert captured.value.diagnostic_code == "upstream_stream_incomplete"
    assert captured.value.generation_state is ProviderGenerationState.UNKNOWN
    assert captured.value.retry_disposition is ProviderRetryDisposition.FORBIDDEN


def test_generic_http_408_generation_post_is_ambiguous_and_forbidden() -> None:
    with json_server("request timeout", status=408) as (server, Handler):
        origin = f"http://127.0.0.1:{server.server_port}"
        with pytest.raises(CLIProxyProviderError) as captured:
            PinnedUrllibJsonTransport(
                allowed_origin=origin,
                provider_label="CLIProxyAPI",
            ).request_json(
                method="POST",
                url=f"{origin}/v1/chat/completions",
                headers={"Authorization": "Bearer dedicated-local-key"},
                payload={"model": "gpt-5.6-luna"},
                timeout_seconds=2,
            )

    assert Handler.hits == [("/v1/chat/completions", "Bearer dedicated-local-key")]
    assert captured.value.diagnostic_code == "http_408"
    assert captured.value.generation_state is ProviderGenerationState.UNKNOWN
    assert captured.value.retry_disposition is ProviderRetryDisposition.FORBIDDEN


@pytest.mark.parametrize(
    "diagnostic_code", ["auth_unavailable", "authentication_failed", "quota_exhausted"]
)
def test_http_5xx_explicit_pre_generation_diagnostics_are_terminal(
    diagnostic_code: str,
) -> None:
    with json_server(diagnostic_code) as (server, Handler):
        origin = f"http://127.0.0.1:{server.server_port}"
        with pytest.raises(CLIProxyProviderError) as captured:
            PinnedUrllibJsonTransport(
                allowed_origin=origin,
                provider_label="CLIProxyAPI",
            ).request_json(
                method="POST",
                url=f"{origin}/v1/chat/completions",
                headers={"Authorization": "Bearer dedicated-local-key"},
                payload={"model": "gpt-5.6-luna"},
                timeout_seconds=2,
            )

    assert Handler.hits == [("/v1/chat/completions", "Bearer dedicated-local-key")]
    assert captured.value.error_class == "http"
    assert captured.value.diagnostic_code == diagnostic_code
    assert captured.value.generation_state is ProviderGenerationState.NOT_STARTED
    assert captured.value.retry_disposition is ProviderRetryDisposition.TERMINAL


def test_cliproxy_completion_sends_xhigh_and_preserves_tool_calls() -> None:
    transport = FixtureTransport([_completion()])
    selected = _provider(transport)

    turn = asyncio.run(
        selected.complete(
            messages=({"role": "user", "content": "inspect ev-1"},),
            tools=(
                {
                    "type": "function",
                    "function": {
                        "name": "read_evidence",
                        "description": "Read evidence.",
                        "parameters": {"type": "object"},
                    },
                },
            ),
            temperature=0.1,
            top_p=0.95,
            max_output_tokens=256,
            timeout_seconds=5,
        )
    )

    assert turn.model == "gpt-5.6-luna"
    assert turn.tool_calls[0].arguments == {"evidence_id": "ev-1"}
    assert transport.requests[0]["payload"] == {
        "model": "gpt-5.6-luna",
        "messages": [{"role": "user", "content": "inspect ev-1"}],
        "temperature": 0.1,
        "top_p": 0.95,
        "max_tokens": 256,
        "stream": False,
        "reasoning_effort": "xhigh",
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "read_evidence",
                    "description": "Read evidence.",
                    "parameters": {"type": "object"},
                },
            }
        ],
        "tool_choice": "auto",
    }
    headers = cast(dict[str, str], transport.requests[0]["headers"])
    assert headers["X-Market-Impact-Request-Id"].startswith("mia-")


def test_cliproxy_completion_sends_max_unchanged_to_the_gateway() -> None:
    transport = FixtureTransport([_completion()])
    selected = _provider(transport, reasoning_effort="max")

    asyncio.run(
        selected.complete(
            messages=({"role": "user", "content": "inspect ev-1"},),
            tools=(),
            temperature=0.1,
            top_p=0.95,
            max_output_tokens=256,
            timeout_seconds=5,
        )
    )

    assert transport.requests[0]["payload"] == {
        "model": "gpt-5.6-luna",
        "messages": [{"role": "user", "content": "inspect ev-1"}],
        "temperature": 0.1,
        "top_p": 0.95,
        "max_tokens": 256,
        "stream": False,
        "reasoning_effort": "max",
    }


@pytest.mark.parametrize(
    ("error_class", "diagnostic_code"),
    [
        ("http", "upstream_server_error"),
        ("tls", "tls_bad_record_mac"),
    ],
)
def test_ambiguous_completion_failure_has_exactly_one_project_gateway_post(
    error_class: str, diagnostic_code: str
) -> None:
    failure = CLIProxyProviderError(
        "sanitized gateway failure",
        error_class=error_class,
        diagnostic_code=diagnostic_code,
        http_status=500 if error_class == "http" else None,
        generation_state=ProviderGenerationState.UNKNOWN,
        retry_disposition=ProviderRetryDisposition.FORBIDDEN,
        attempts=1,
    )
    transport = FixtureTransport([failure, _completion()])
    selected = CLIProxyLunaProvider(
        api_key="dedicated-local-key",
        config=CLIProxyLunaConfig(
            origin="http://127.0.0.1:8317",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            retry_backoff_seconds=0,
        ),
        transport=transport,
        request_id_factory=lambda: "mia-incident-1",
    )

    with pytest.raises(CLIProxyProviderError) as captured:
        asyncio.run(
            selected.complete(
                messages=({"role": "user", "content": "inspect ev-1"},),
                tools=(),
                temperature=0.1,
                top_p=0.95,
                max_output_tokens=256,
                timeout_seconds=5,
            )
        )

    assert len(transport.requests) == 1
    assert transport.requests[0]["method"] == "POST"
    headers = cast(dict[str, str], transport.requests[0]["headers"])
    assert headers["X-Market-Impact-Request-Id"] == "mia-incident-1"
    assert captured.value.generation_state is ProviderGenerationState.UNKNOWN
    assert captured.value.attempts == 1


def test_explicit_429_rejection_retries_with_retry_after_and_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def record_delay(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr("market_impact_agent.openai_chat_provider.asyncio.sleep", record_delay)
    rejected = CLIProxyProviderError(
        "rate limited before generation",
        error_class="http",
        diagnostic_code="rate_limited",
        http_status=429,
        generation_state=ProviderGenerationState.NOT_STARTED,
        retry_disposition=ProviderRetryDisposition.SAFE,
        retry_after_seconds=1.5,
        attempts=1,
    )
    transport = FixtureTransport([rejected, _completion()])
    selected = CLIProxyLunaProvider(
        api_key="dedicated-local-key",
        config=CLIProxyLunaConfig(
            origin="http://127.0.0.1:8317",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            retry_backoff_seconds=0.25,
        ),
        transport=transport,
        request_id_factory=lambda: "mia-rate-limit-1",
    )

    turn = asyncio.run(
        selected.complete(
            messages=({"role": "user", "content": "inspect ev-1"},),
            tools=(),
            temperature=0.1,
            top_p=0.95,
            max_output_tokens=256,
            timeout_seconds=5,
        )
    )

    assert turn.attempts == 2
    assert delays == [1.5]
    assert len(transport.requests) == 2
    assert {
        cast(dict[str, str], request["headers"])["X-Market-Impact-Request-Id"]
        for request in transport.requests
    } == {"mia-rate-limit-1"}


@pytest.mark.parametrize(
    ("origin", "model", "effort"),
    [
        ("http://localhost:8317", "gpt-5.6-luna", "xhigh"),
        ("http://127.0.0.1:8317/", "gpt-5.6-luna", "xhigh"),
        ("https://127.0.0.1:8317", "gpt-5.6-luna", "xhigh"),
        ("http://127.0.0.1:8317", "gpt-5.6-terra", "xhigh"),
        ("http://127.0.0.1:8317", "gpt-5.6-luna", "high"),
        ("http://127.0.0.1:8317", "gpt-5.6-luna", "low"),
    ],
)
def test_cliproxy_rejects_origin_model_or_effort_substitution(
    origin: str, model: str, effort: str
) -> None:
    with pytest.raises(ValueError):
        CLIProxyLunaConfig(origin=origin, model=model, reasoning_effort=effort)
