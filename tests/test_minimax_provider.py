import asyncio
import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import ClassVar

import pytest

import market_impact_agent.minimax_provider as minimax_provider_module
from market_impact_agent.minimax_provider import (
    JsonHttpTransport,
    MiniMaxOpenAIProvider,
    MiniMaxProviderConfig,
    MiniMaxProviderError,
    UrllibJsonTransport,
)


class FixtureTransport(JsonHttpTransport):
    def __init__(self, responses: list[dict[str, object] | MiniMaxProviderError]) -> None:
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
        if isinstance(response, MiniMaxProviderError):
            raise response
        return response


class RedirectTestHandler(BaseHTTPRequestHandler):
    hits: ClassVar[list[str]] = []
    redirect_status: int | None = None
    redirect_location = ""

    def do_GET(self) -> None:
        type(self).hits.append(self.path)
        status = type(self).redirect_status
        if status is not None and self.path == "/start":
            self.send_response(status)
            self.send_header("Location", type(self).redirect_location)
            self.end_headers()
            self.wfile.write(b"Bearer redirect-secret redirect-secret")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, format: str, *args: object) -> None:
        _ = (format, args)


@contextmanager
def redirect_server() -> Generator[tuple[ThreadingHTTPServer, type[RedirectTestHandler]]]:
    class Handler(RedirectTestHandler):
        hits: ClassVar[list[str]] = []

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, Handler
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def completion_response(*, model: str = "MiniMax-M3") -> dict[str, object]:
    assistant: dict[str, object] = {
        "role": "assistant",
        "content": None,
        "reasoning_details": [{"type": "reasoning.text", "text": "inspect evidence"}],
        "tool_calls": [
            {
                "id": "call-1",
                "type": "function",
                "function": {
                    "name": "read_evidence",
                    "arguments": json.dumps({"evidence_id": "ev-1"}),
                },
            }
        ],
    }
    return {
        "id": "response-1",
        "model": model,
        "choices": [{"index": 0, "message": assistant, "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 13, "completion_tokens": 8, "total_tokens": 21},
    }


def provider(transport: JsonHttpTransport) -> MiniMaxOpenAIProvider:
    return MiniMaxOpenAIProvider(
        api_key="super-secret-key",
        config=MiniMaxProviderConfig(
            base_url="https://api.minimaxi.com",
            model="MiniMax-M3",
            retry_backoff_seconds=0,
        ),
        transport=transport,
    )


def test_minimax_preserves_tool_turn_and_explicit_request_contract() -> None:
    transport = FixtureTransport([completion_response()])
    selected = provider(transport)

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
            temperature=1,
            top_p=0.95,
            max_output_tokens=256,
            timeout_seconds=5,
        )
    )

    assert turn.model == "MiniMax-M3"
    assert turn.tool_calls[0].arguments == {"evidence_id": "ev-1"}
    assert turn.assistant_message["reasoning_details"]
    assert turn.usage.input_tokens == 13
    request = transport.requests[0]
    assert request["url"] == "https://api.minimaxi.com/v1/chat/completions"
    assert request["payload"] == {
        "model": "MiniMax-M3",
        "messages": [{"role": "user", "content": "inspect ev-1"}],
        "temperature": 1,
        "top_p": 0.95,
        "max_tokens": 256,
        "stream": False,
        "reasoning_split": True,
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


def test_minimax_model_preflight_and_substitution_fail_closed() -> None:
    transport = FixtureTransport([{"data": [{"id": "MiniMax-M3"}]}])
    asyncio.run(provider(transport).assert_model_available(timeout_seconds=5))

    substituted = FixtureTransport([completion_response(model="MiniMax-M2")])
    with pytest.raises(MiniMaxProviderError, match="unexpected model") as raised:
        asyncio.run(
            provider(substituted).complete(
                messages=({"role": "user", "content": "test"},),
                tools=(),
                temperature=1,
                top_p=1,
                max_output_tokens=10,
                timeout_seconds=5,
            )
        )
    assert raised.value.error_class == "model_substitution"


def test_minimax_retries_only_retryable_errors_and_redacts_secret() -> None:
    transport = FixtureTransport(
        [
            MiniMaxProviderError(
                "temporary super-secret-key",
                error_class="http_503",
                retryable=True,
                attempts=1,
            ),
            completion_response(),
        ]
    )
    turn = asyncio.run(
        provider(transport).complete(
            messages=({"role": "user", "content": "test"},),
            tools=(),
            temperature=1,
            top_p=1,
            max_output_tokens=10,
            timeout_seconds=5,
        )
    )
    assert turn.attempts == 2

    failed = FixtureTransport(
        [
            MiniMaxProviderError(
                "bad super-secret-key",
                error_class="http_401",
                retryable=False,
                attempts=1,
            )
        ]
    )
    with pytest.raises(MiniMaxProviderError) as raised:
        asyncio.run(
            provider(failed).complete(
                messages=({"role": "user", "content": "test"},),
                tools=(),
                temperature=1,
                top_p=1,
                max_output_tokens=10,
                timeout_seconds=5,
            )
        )
    assert "super-secret-key" not in str(raised.value)
    assert raised.value.attempts == 1


def test_minimax_environment_requires_all_explicit_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINIMAX_API_KEY", raising=False)
    monkeypatch.setenv("MINIMAX_BASE_URL", "https://api.minimaxi.com")
    monkeypatch.setenv("MINIMAX_MODEL", "MiniMax-M3")

    with pytest.raises(ValueError, match="MINIMAX_API_KEY"):
        MiniMaxOpenAIProvider.from_environment()


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.minimaxi.com.evil.test",
        "https://api.minimaxi.com@evil.test",
        "https://user@api.minimaxi.com",
        "https://api.minimaxi.com:443",
        "https://api.minimaxi.com/",
        "https://api.minimaxi.com/v1",
        "https://api.minimax.io",
        "http://api.minimaxi.com",
    ],
)
def test_minimax_rejects_every_origin_except_exact_pinned_china_origin(
    base_url: str,
) -> None:
    with pytest.raises(ValueError, match="exactly match"):
        MiniMaxProviderConfig(base_url=base_url, model="MiniMax-M3")


def test_environment_provider_sends_credential_only_to_pinned_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FixtureTransport([{"data": [{"id": "MiniMax-M3"}]}])
    monkeypatch.setenv("MINIMAX_API_KEY", "environment-secret")
    monkeypatch.setenv("MINIMAX_BASE_URL", "https://api.minimaxi.com")
    monkeypatch.setenv("MINIMAX_MODEL", "MiniMax-M3")

    selected = MiniMaxOpenAIProvider.from_environment(transport=transport)
    asyncio.run(selected.assert_model_available(timeout_seconds=5))

    request = transport.requests[0]
    assert request["url"] == "https://api.minimaxi.com/v1/models"
    assert request["headers"] == {
        "Authorization": "Bearer environment-secret",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("target_origin", ["same", "cross"])
def test_credential_request_rejects_all_redirects_without_forwarding_authorization(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    target_origin: str,
) -> None:
    with (
        redirect_server() as (origin_server, origin_handler),
        redirect_server() as (target_server, target_handler),
    ):
        origin = f"http://127.0.0.1:{origin_server.server_port}"
        target = f"http://127.0.0.1:{target_server.server_port}"
        origin_handler.redirect_status = status
        origin_handler.redirect_location = (
            f"{origin}/target" if target_origin == "same" else f"{target}/target"
        )
        monkeypatch.setattr(minimax_provider_module, "MINIMAX_CHINA_ORIGIN", origin)

        with pytest.raises(MiniMaxProviderError) as raised:
            UrllibJsonTransport().request_json(
                method="GET",
                url=f"{origin}/start",
                headers={"Authorization": "Bearer redirect-secret"},
                payload=None,
                timeout_seconds=2,
            )

        assert raised.value.error_class == f"http_{status}"
        assert raised.value.retryable is False
        assert "redirect-secret" not in str(raised.value)
        assert origin_handler.hits == ["/start"]
        assert target_handler.hits == []
