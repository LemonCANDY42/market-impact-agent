from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from market_impact_agent.agent_runtime import ModelTurn, ProviderUsage, ToolCall

MINIMAX_CHINA_ORIGIN = "https://api.minimaxi.com"


class JsonHttpTransport(Protocol):
    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class MiniMaxProviderConfig:
    base_url: str
    model: str
    api_path: str = "/v1/chat/completions"
    models_path: str = "/v1/models"
    max_attempts: int = 2
    retry_backoff_seconds: float = 0.25

    def __post_init__(self) -> None:
        if self.base_url != MINIMAX_CHINA_ORIGIN:
            raise ValueError(
                f"MiniMax base_url must exactly match the pinned China origin: "
                f"{MINIMAX_CHINA_ORIGIN}"
            )
        if not self.model or self.model != self.model.strip():
            raise ValueError("MiniMax model must be a non-empty trimmed string")
        for name in ("api_path", "models_path"):
            path = cast(str, getattr(self, name))
            if not path.startswith("/") or "?" in path or "#" in path:
                raise ValueError(f"MiniMax {name} must be an absolute URL path")
        if self.max_attempts < 1 or self.max_attempts > 3:
            raise ValueError("MiniMax max_attempts must be between one and three")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or self.retry_backoff_seconds < 0
            or self.retry_backoff_seconds > 5
        ):
            raise ValueError("MiniMax retry_backoff_seconds must be finite and between 0 and 5")

    def endpoint(self, path: str) -> str:
        return urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))


class MiniMaxProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_class: str,
        retryable: bool,
        attempts: int,
    ) -> None:
        super().__init__(message)
        self.error_class = error_class
        self.retryable = retryable
        self.attempts = attempts


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        _ = (req, fp, code, msg, headers, newurl)
        return None


class UrllibJsonTransport:
    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]:
        if any(name.lower() == "authorization" for name in headers):
            _assert_pinned_credential_url(url)
        body = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
        request = Request(url, data=body, headers=dict(headers), method=method)
        try:
            with build_opener(_NoRedirectHandler()).open(
                request, timeout=timeout_seconds
            ) as response:
                response_body = response.read()
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")[:2000]
            raise MiniMaxProviderError(
                f"MiniMax HTTP {exc.code}: "
                f"{_bounded_error_text(_redact_header_values(error_body, headers))}",
                error_class=f"http_{exc.code}",
                retryable=exc.code == 429 or 500 <= exc.code < 600,
                attempts=1,
            ) from exc
        except TimeoutError as exc:
            raise MiniMaxProviderError(
                "MiniMax request timed out",
                error_class="timeout",
                retryable=True,
                attempts=1,
            ) from exc
        except URLError as exc:
            raise MiniMaxProviderError(
                "MiniMax transport failed",
                error_class="transport",
                retryable=True,
                attempts=1,
            ) from exc
        try:
            decoded = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise MiniMaxProviderError(
                "MiniMax returned invalid JSON",
                error_class="invalid_json",
                retryable=False,
                attempts=1,
            ) from exc
        if not isinstance(decoded, dict):
            raise MiniMaxProviderError(
                "MiniMax returned a non-object response",
                error_class="invalid_response",
                retryable=False,
                attempts=1,
            )
        return cast(dict[str, object], decoded)


class MiniMaxOpenAIProvider:
    def __init__(
        self,
        *,
        api_key: str,
        config: MiniMaxProviderConfig,
        transport: JsonHttpTransport | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("MiniMax API key is required")
        self._api_key = api_key
        self.config = config
        self._transport = transport or UrllibJsonTransport()

    @classmethod
    def from_environment(
        cls,
        *,
        transport: JsonHttpTransport | None = None,
    ) -> MiniMaxOpenAIProvider:
        required = {
            "MINIMAX_API_KEY": os.environ.get("MINIMAX_API_KEY", ""),
            "MINIMAX_BASE_URL": os.environ.get("MINIMAX_BASE_URL", ""),
            "MINIMAX_MODEL": os.environ.get("MINIMAX_MODEL", ""),
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            raise ValueError(f"MiniMax environment is incomplete: {', '.join(missing)}")
        return cls(
            api_key=required["MINIMAX_API_KEY"],
            config=MiniMaxProviderConfig(
                base_url=required["MINIMAX_BASE_URL"],
                model=required["MINIMAX_MODEL"],
            ),
            transport=transport,
        )

    @property
    def provider_id(self) -> str:
        return "minimax-openai-compatible"

    @property
    def model(self) -> str:
        return self.config.model

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        response, _attempts = await self._request_with_retry(
            method="GET",
            path=self.config.models_path,
            payload=None,
            timeout_seconds=timeout_seconds,
        )
        items = _required_list(response, "data")
        available = {
            value
            for item in items
            if isinstance(item, dict)
            and isinstance((value := cast(dict[object, object], item).get("id")), str)
        }
        if self.model not in available:
            raise MiniMaxProviderError(
                f"Configured MiniMax model is unavailable: {self.model}",
                error_class="model_unavailable",
                retryable=False,
                attempts=1,
            )

    async def complete(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        if not messages:
            raise ValueError("MiniMax completion requires at least one message")
        if not 0 < temperature <= 1 or not 0 < top_p <= 1:
            raise ValueError("MiniMax temperature and top_p must be in (0, 1]")
        if max_output_tokens < 1:
            raise ValueError("MiniMax max_output_tokens must be positive")
        payload: dict[str, object] = {
            "model": self.model,
            "messages": list(messages),
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_output_tokens,
            "stream": False,
            "reasoning_split": True,
        }
        if tools:
            payload["tools"] = list(tools)
            payload["tool_choice"] = "auto"
        started = time.monotonic()
        response, attempts = await self._request_with_retry(
            method="POST",
            path=self.config.api_path,
            payload=payload,
            timeout_seconds=timeout_seconds,
        )
        latency_ms = (time.monotonic() - started) * 1000
        return _parse_completion(
            response,
            expected_model=self.model,
            latency_ms=latency_ms,
            attempts=attempts,
        )

    async def _request_with_retry(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> tuple[dict[str, object], int]:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("MiniMax timeout_seconds must be finite and positive")
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                response = await asyncio.to_thread(
                    self._transport.request_json,
                    method=method,
                    url=self.config.endpoint(path),
                    headers=headers,
                    payload=payload,
                    timeout_seconds=timeout_seconds,
                )
                return response, attempt
            except MiniMaxProviderError as exc:
                if not exc.retryable or attempt >= self.config.max_attempts:
                    raise MiniMaxProviderError(
                        _redact_secret(str(exc), self._api_key),
                        error_class=exc.error_class,
                        retryable=exc.retryable,
                        attempts=attempt,
                    ) from exc
                if self.config.retry_backoff_seconds:
                    await asyncio.sleep(self.config.retry_backoff_seconds * attempt)
        raise AssertionError("bounded MiniMax retry loop did not return")


def _parse_completion(
    response: dict[str, object],
    *,
    expected_model: str,
    latency_ms: float,
    attempts: int,
) -> ModelTurn:
    response_id = _required_string(response, "id")
    model = _required_string(response, "model")
    if model != expected_model:
        raise MiniMaxProviderError(
            f"MiniMax returned an unexpected model: {model}",
            error_class="model_substitution",
            retryable=False,
            attempts=attempts,
        )
    choices = _required_list(response, "choices")
    if len(choices) != 1 or not isinstance(choices[0], dict):
        raise MiniMaxProviderError(
            "MiniMax completion must contain exactly one choice",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    choice = cast(dict[str, object], choices[0])
    message = _required_mapping(choice, "message")
    if message.get("role") != "assistant":
        raise MiniMaxProviderError(
            "MiniMax choice did not contain an assistant message",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    tool_calls = tuple(
        _parse_tool_call(item, attempts) for item in _optional_list(message, "tool_calls")
    )
    finish_reason = _required_string(choice, "finish_reason")
    usage = _required_mapping(response, "usage")
    return ModelTurn(
        response_id=response_id,
        model=model,
        assistant_message=message,
        tool_calls=tool_calls,
        finish_reason=finish_reason,
        usage=ProviderUsage(
            input_tokens=_required_integer(usage, "prompt_tokens"),
            output_tokens=_required_integer(usage, "completion_tokens"),
        ),
        raw_response=response,
        latency_ms=latency_ms,
        attempts=attempts,
    )


def _parse_tool_call(value: object, attempts: int) -> ToolCall:
    if not isinstance(value, dict):
        raise MiniMaxProviderError(
            "MiniMax tool call must be an object",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    payload = cast(dict[str, object], value)
    call_id = _required_string(payload, "id")
    if payload.get("type") != "function":
        raise MiniMaxProviderError(
            "MiniMax tool call type must be function",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    function = _required_mapping(payload, "function")
    name = _required_string(function, "name")
    arguments_text = _required_string(function, "arguments")
    try:
        arguments = json.loads(arguments_text)
    except json.JSONDecodeError as exc:
        raise MiniMaxProviderError(
            "MiniMax tool arguments are invalid JSON",
            error_class="invalid_tool_arguments",
            retryable=False,
            attempts=attempts,
        ) from exc
    if not isinstance(arguments, dict):
        raise MiniMaxProviderError(
            "MiniMax tool arguments must be an object with string keys",
            error_class="invalid_tool_arguments",
            retryable=False,
            attempts=attempts,
        )
    raw_arguments = cast(dict[object, object], arguments)
    if any(not isinstance(key, str) for key in raw_arguments):
        raise MiniMaxProviderError(
            "MiniMax tool arguments must be an object with string keys",
            error_class="invalid_tool_arguments",
            retryable=False,
            attempts=attempts,
        )
    return ToolCall(
        call_id=call_id,
        name=name,
        arguments=cast(dict[str, object], arguments),
    )


def _required_mapping(value: Mapping[str, object], key: str) -> dict[str, object]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise MiniMaxProviderError(
            f"MiniMax response field must be an object: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    raw_item = cast(dict[object, object], item)
    if any(not isinstance(name, str) for name in raw_item):
        raise MiniMaxProviderError(
            f"MiniMax response field must be an object: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return cast(dict[str, object], item)


def _required_list(value: Mapping[str, object], key: str) -> list[object]:
    item = value.get(key)
    if not isinstance(item, list):
        raise MiniMaxProviderError(
            f"MiniMax response field must be an array: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return cast(list[object], item)


def _optional_list(value: Mapping[str, object], key: str) -> list[object]:
    item = value.get(key)
    if item is None:
        return []
    return _required_list(value, key)


def _required_string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise MiniMaxProviderError(
            f"MiniMax response field must be a non-empty string: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return item


def _required_integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise MiniMaxProviderError(
            f"MiniMax response field must be a non-negative integer: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return item


def _redact_secret(value: str, secret: str) -> str:
    return value.replace(secret, "[REDACTED]")


def _bounded_error_text(value: str) -> str:
    normalized = " ".join(value.split())
    return normalized or "empty error response"


def _redact_header_values(value: str, headers: Mapping[str, str]) -> str:
    cleaned = value
    for name, header_value in headers.items():
        if name.lower() != "authorization":
            continue
        cleaned = cleaned.replace(header_value, "[REDACTED]")
        scheme, separator, credential = header_value.partition(" ")
        _ = scheme
        if separator and credential:
            cleaned = cleaned.replace(credential, "[REDACTED]")
    return cleaned


def _assert_pinned_credential_url(url: str) -> None:
    parsed = urlsplit(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if (
        origin != MINIMAX_CHINA_ORIGIN
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise MiniMaxProviderError(
            "MiniMax credential-bearing request rejected an unpinned origin",
            error_class="credential_origin",
            retryable=False,
            attempts=1,
        )
