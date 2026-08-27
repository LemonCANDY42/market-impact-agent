import asyncio
import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import ClassVar

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

PROFILE = Path("examples/providers/cliproxyapi-luna-xhigh-v1.json")


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

    def do_GET(self) -> None:
        type(self).hits.append((self.path, self.headers.get("Authorization")))
        body = json.dumps({"served_by": type(self).server_label}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        _ = (format, args)


@contextmanager
def json_server(
    label: str,
) -> Generator[tuple[ThreadingHTTPServer, type[JsonServerHandler]]]:
    class Handler(JsonServerHandler):
        hits: ClassVar[list[tuple[str, str | None]]] = []
        server_label = label

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


def _provider(transport: JsonHttpTransport) -> CLIProxyLunaProvider:
    return CLIProxyLunaProvider(
        api_key="dedicated-local-key",
        config=CLIProxyLunaConfig(
            origin="http://127.0.0.1:8317",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
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


@pytest.mark.parametrize(
    ("origin", "model", "effort"),
    [
        ("http://localhost:8317", "gpt-5.6-luna", "xhigh"),
        ("http://127.0.0.1:8317/", "gpt-5.6-luna", "xhigh"),
        ("https://127.0.0.1:8317", "gpt-5.6-luna", "xhigh"),
        ("http://127.0.0.1:8317", "gpt-5.6-terra", "xhigh"),
        ("http://127.0.0.1:8317", "gpt-5.6-luna", "high"),
    ],
)
def test_cliproxy_rejects_origin_model_or_effort_substitution(
    origin: str, model: str, effort: str
) -> None:
    with pytest.raises(ValueError):
        CLIProxyLunaConfig(origin=origin, model=model, reasoning_effort=effort)
