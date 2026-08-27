from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import BaseHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from market_impact_agent.agent_runtime import ModelTurn, ProviderUsage, ToolCall


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


class OpenAIChatProviderError(RuntimeError):
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


class PinnedUrllibJsonTransport:
    def __init__(self, *, allowed_origin: str, provider_label: str) -> None:
        _validate_origin(allowed_origin, "allowed_origin")
        _nonempty(provider_label, "provider_label")
        self._allowed_origin = allowed_origin
        self._provider_label = provider_label
        self._disable_environment_proxies = urlsplit(allowed_origin).hostname in {
            "127.0.0.1",
            "::1",
        }

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
            self._assert_pinned_credential_url(url)
        body = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
        request = Request(url, data=body, headers=dict(headers), method=method)
        handlers: list[BaseHandler] = [_NoRedirectHandler()]
        if self._disable_environment_proxies:
            handlers.insert(0, ProxyHandler({}))
        try:
            with build_opener(*handlers).open(request, timeout=timeout_seconds) as response:
                response_body = response.read()
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")[:2000]
            raise OpenAIChatProviderError(
                f"{self._provider_label} HTTP {exc.code}: "
                f"{_bounded_error_text(_redact_header_values(error_body, headers))}",
                error_class=f"http_{exc.code}",
                retryable=exc.code == 429 or 500 <= exc.code < 600,
                attempts=1,
            ) from exc
        except TimeoutError as exc:
            raise OpenAIChatProviderError(
                f"{self._provider_label} request timed out",
                error_class="timeout",
                retryable=True,
                attempts=1,
            ) from exc
        except URLError as exc:
            raise OpenAIChatProviderError(
                f"{self._provider_label} transport failed",
                error_class="transport",
                retryable=True,
                attempts=1,
            ) from exc
        try:
            decoded = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise OpenAIChatProviderError(
                f"{self._provider_label} returned invalid JSON",
                error_class="invalid_json",
                retryable=False,
                attempts=1,
            ) from exc
        if not isinstance(decoded, dict):
            raise OpenAIChatProviderError(
                f"{self._provider_label} returned a non-object response",
                error_class="invalid_response",
                retryable=False,
                attempts=1,
            )
        return cast(dict[str, object], decoded)

    def _assert_pinned_credential_url(self, url: str) -> None:
        parsed = urlsplit(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if (
            origin != self._allowed_origin
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise OpenAIChatProviderError(
                f"{self._provider_label} credential-bearing request rejected an unpinned origin",
                error_class="credential_origin",
                retryable=False,
                attempts=1,
            )


@dataclass(frozen=True, slots=True)
class OpenAIChatProviderConfig:
    origin: str
    model: str
    api_path: str
    models_path: str
    max_attempts: int
    retry_backoff_seconds: float

    def __post_init__(self) -> None:
        _validate_origin(self.origin, "origin")
        _nonempty(self.model, "model")
        for name in ("api_path", "models_path"):
            path = cast(str, getattr(self, name))
            if not path.startswith("/") or "?" in path or "#" in path:
                raise ValueError(f"{name} must be an absolute URL path")
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("max_attempts must be between one and three")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or not 0 <= self.retry_backoff_seconds <= 5
        ):
            raise ValueError("retry_backoff_seconds must be finite and between zero and five")

    def endpoint(self, path: str) -> str:
        return urljoin(self.origin.rstrip("/") + "/", path.lstrip("/"))


class OpenAIChatCompatibleProvider:
    def __init__(
        self,
        *,
        api_key: str,
        provider_id: str,
        provider_label: str,
        config: OpenAIChatProviderConfig,
        completion_parameters: Mapping[str, object],
        transport: JsonHttpTransport,
    ) -> None:
        _nonempty(api_key, "API key")
        _nonempty(provider_id, "provider_id")
        _nonempty(provider_label, "provider_label")
        self._api_key = api_key
        self._provider_id = provider_id
        self._provider_label = provider_label
        self._config = config
        self._completion_parameters = dict(completion_parameters)
        self._transport = transport

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def model(self) -> str:
        return self._config.model

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        response, _attempts = await self._request_with_retry(
            method="GET",
            path=self._config.models_path,
            payload=None,
            timeout_seconds=timeout_seconds,
        )
        items = _required_list(response, "data", self._provider_label)
        available = {
            value
            for item in items
            if isinstance(item, dict)
            and isinstance((value := cast(dict[object, object], item).get("id")), str)
        }
        if self.model not in available:
            raise OpenAIChatProviderError(
                f"Configured {self._provider_label} model is unavailable: {self.model}",
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
            raise ValueError(f"{self._provider_label} completion requires at least one message")
        if not 0 < temperature <= 1 or not 0 < top_p <= 1:
            raise ValueError(f"{self._provider_label} temperature and top_p must be in (0, 1]")
        if max_output_tokens < 1:
            raise ValueError(f"{self._provider_label} max_output_tokens must be positive")
        payload: dict[str, object] = {
            "model": self.model,
            "messages": list(messages),
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_output_tokens,
            "stream": False,
            **self._completion_parameters,
        }
        if tools:
            payload["tools"] = list(tools)
            payload["tool_choice"] = "auto"
        started = time.monotonic()
        response, attempts = await self._request_with_retry(
            method="POST",
            path=self._config.api_path,
            payload=payload,
            timeout_seconds=timeout_seconds,
        )
        latency_ms = (time.monotonic() - started) * 1000
        return _parse_completion(
            response,
            expected_model=self.model,
            provider_label=self._provider_label,
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
            raise ValueError(f"{self._provider_label} timeout_seconds must be finite and positive")
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        for attempt in range(1, self._config.max_attempts + 1):
            try:
                response = await asyncio.to_thread(
                    self._transport.request_json,
                    method=method,
                    url=self._config.endpoint(path),
                    headers=headers,
                    payload=payload,
                    timeout_seconds=timeout_seconds,
                )
                return response, attempt
            except OpenAIChatProviderError as exc:
                if not exc.retryable or attempt >= self._config.max_attempts:
                    raise OpenAIChatProviderError(
                        _redact_secret(str(exc), self._api_key),
                        error_class=exc.error_class,
                        retryable=exc.retryable,
                        attempts=attempt,
                    ) from exc
                if self._config.retry_backoff_seconds:
                    await asyncio.sleep(self._config.retry_backoff_seconds * attempt)
        raise AssertionError("bounded OpenAI-compatible retry loop did not return")


def _parse_completion(
    response: dict[str, object],
    *,
    expected_model: str,
    provider_label: str,
    latency_ms: float,
    attempts: int,
) -> ModelTurn:
    response_id = _required_string(response, "id", provider_label)
    model = _required_string(response, "model", provider_label)
    if model != expected_model:
        raise OpenAIChatProviderError(
            f"{provider_label} returned an unexpected model: {model}",
            error_class="model_substitution",
            retryable=False,
            attempts=attempts,
        )
    choices = _required_list(response, "choices", provider_label)
    if len(choices) != 1 or not isinstance(choices[0], dict):
        raise OpenAIChatProviderError(
            f"{provider_label} completion must contain exactly one choice",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    choice = cast(dict[str, object], choices[0])
    message = _required_mapping(choice, "message", provider_label)
    if message.get("role") != "assistant":
        raise OpenAIChatProviderError(
            f"{provider_label} choice did not contain an assistant message",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    tool_calls = tuple(
        _parse_tool_call(item, attempts, provider_label)
        for item in _optional_list(message, "tool_calls", provider_label)
    )
    finish_reason = _required_string(choice, "finish_reason", provider_label)
    usage = _required_mapping(response, "usage", provider_label)
    return ModelTurn(
        response_id=response_id,
        model=model,
        assistant_message=message,
        tool_calls=tool_calls,
        finish_reason=finish_reason,
        usage=ProviderUsage(
            input_tokens=_required_integer(usage, "prompt_tokens", provider_label),
            output_tokens=_required_integer(usage, "completion_tokens", provider_label),
        ),
        raw_response=response,
        latency_ms=latency_ms,
        attempts=attempts,
    )


def _parse_tool_call(value: object, attempts: int, provider_label: str) -> ToolCall:
    if not isinstance(value, dict):
        raise OpenAIChatProviderError(
            f"{provider_label} tool call must be an object",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    payload = cast(dict[str, object], value)
    call_id = _required_string(payload, "id", provider_label)
    if payload.get("type") != "function":
        raise OpenAIChatProviderError(
            f"{provider_label} tool call type must be function",
            error_class="invalid_response",
            retryable=False,
            attempts=attempts,
        )
    function = _required_mapping(payload, "function", provider_label)
    name = _required_string(function, "name", provider_label)
    arguments_text = _required_string(function, "arguments", provider_label)
    try:
        arguments = json.loads(arguments_text)
    except json.JSONDecodeError as exc:
        raise OpenAIChatProviderError(
            f"{provider_label} tool arguments are invalid JSON",
            error_class="invalid_tool_arguments",
            retryable=False,
            attempts=attempts,
        ) from exc
    if not isinstance(arguments, dict):
        raise OpenAIChatProviderError(
            f"{provider_label} tool arguments must be an object with string keys",
            error_class="invalid_tool_arguments",
            retryable=False,
            attempts=attempts,
        )
    raw_arguments = cast(dict[object, object], arguments)
    if any(not isinstance(key, str) for key in raw_arguments):
        raise OpenAIChatProviderError(
            f"{provider_label} tool arguments must be an object with string keys",
            error_class="invalid_tool_arguments",
            retryable=False,
            attempts=attempts,
        )
    return ToolCall(call_id=call_id, name=name, arguments=cast(dict[str, object], arguments))


def _required_mapping(
    value: Mapping[str, object], key: str, provider_label: str
) -> dict[str, object]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise OpenAIChatProviderError(
            f"{provider_label} response field must be an object: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    raw_item = cast(dict[object, object], item)
    if any(not isinstance(name, str) for name in raw_item):
        raise OpenAIChatProviderError(
            f"{provider_label} response field must be an object: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return cast(dict[str, object], item)


def _required_list(value: Mapping[str, object], key: str, provider_label: str) -> list[object]:
    item = value.get(key)
    if not isinstance(item, list):
        raise OpenAIChatProviderError(
            f"{provider_label} response field must be an array: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return cast(list[object], item)


def _optional_list(value: Mapping[str, object], key: str, provider_label: str) -> list[object]:
    item = value.get(key)
    return [] if item is None else _required_list(value, key, provider_label)


def _required_string(value: Mapping[str, object], key: str, provider_label: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise OpenAIChatProviderError(
            f"{provider_label} response field must be a non-empty string: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return item


def _required_integer(value: Mapping[str, object], key: str, provider_label: str) -> int:
    item = value.get(key)
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise OpenAIChatProviderError(
            f"{provider_label} response field must be a non-negative integer: {key}",
            error_class="invalid_response",
            retryable=False,
            attempts=1,
        )
    return item


def _validate_origin(value: str, name: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{name} must be an exact HTTP(S) origin")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


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
        _scheme, separator, credential = header_value.partition(" ")
        if separator and credential:
            cleaned = cleaned.replace(credential, "[REDACTED]")
    return cleaned
