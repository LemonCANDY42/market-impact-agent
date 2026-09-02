from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Protocol, cast
from urllib.parse import urljoin, urlsplit
from uuid import uuid4

import httpx2

from market_impact_agent.agent_runtime import ModelTurn, ProviderUsage, ToolCall
from market_impact_agent.provider_reliability import (
    ProviderAttemptEvent,
    ProviderAttemptObserver,
    ProviderAttemptPhase,
    ProviderFailure,
    ProviderGenerationState,
    ProviderRetryDisposition,
)


class JsonHttpTransport(Protocol):
    async def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]: ...


class OpenAIChatProviderError(ProviderFailure):
    pass


class PinnedHttpxJsonTransport:
    """One cancellable physical request; Harness owns retries and durable state."""

    def __init__(self, *, allowed_origin: str, provider_label: str) -> None:
        _validate_origin(allowed_origin, "allowed_origin")
        _nonempty(provider_label, "provider_label")
        self._allowed_origin = allowed_origin
        self._provider_label = provider_label
        self._disable_environment_proxies = urlsplit(allowed_origin).hostname in {
            "127.0.0.1",
            "::1",
        }

    async def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]:
        self._assert_pinned_url(url)
        body = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
        try:
            # Request-scoped ownership avoids pools bound to a different event loop
            # and closes the socket before cancellation releases the Harness lease.
            async with (
                asyncio.timeout(timeout_seconds),
                httpx2.AsyncClient(
                    follow_redirects=False,
                    trust_env=not self._disable_environment_proxies,
                    timeout=timeout_seconds,
                ) as client,
            ):
                response = await client.request(method, url, headers=headers, content=body)
                response.raise_for_status()
        except httpx2.HTTPStatusError as exc:
            status = exc.response.status_code
            error_body = exc.response.content.decode("utf-8", errors="replace")[:2000]
            diagnostic_code = _http_diagnostic_code(status, error_body)
            generation_state = _http_generation_state(method, status, diagnostic_code)
            raise OpenAIChatProviderError(
                f"{self._provider_label} request was rejected with HTTP {status}",
                error_class=("tls" if diagnostic_code == "tls_bad_record_mac" else "http"),
                diagnostic_code=diagnostic_code,
                http_status=status,
                generation_state=generation_state,
                retry_disposition=_http_retry_disposition(
                    method, status, diagnostic_code, generation_state
                ),
                retry_after_seconds=_retry_after_seconds(exc.response.headers.get("Retry-After")),
                attempts=1,
            ) from exc
        except (TimeoutError, httpx2.TimeoutException) as exc:
            raise OpenAIChatProviderError(
                f"{self._provider_label} request timed out",
                error_class="timeout",
                diagnostic_code="request_timeout",
                generation_state=_transport_generation_state(method),
                retry_disposition=_transport_retry_disposition(method),
                attempts=1,
            ) from exc
        except httpx2.RequestError as exc:
            tls_bad_record_mac = _is_tls_bad_record_mac(exc)
            raise OpenAIChatProviderError(
                f"{self._provider_label} transport failed",
                error_class="tls" if tls_bad_record_mac else "transport",
                diagnostic_code=("tls_bad_record_mac" if tls_bad_record_mac else "transport_error"),
                generation_state=_transport_generation_state(method),
                retry_disposition=_transport_retry_disposition(method),
                attempts=1,
            ) from exc
        try:
            decoded = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise OpenAIChatProviderError(
                f"{self._provider_label} returned invalid JSON",
                error_class="invalid_json",
                diagnostic_code="invalid_json_response",
                generation_state=ProviderGenerationState.RESPONSE_RECEIVED,
                retry_disposition=ProviderRetryDisposition.TERMINAL,
                attempts=1,
            ) from exc
        if not isinstance(decoded, dict):
            raise OpenAIChatProviderError(
                f"{self._provider_label} returned a non-object response",
                error_class="invalid_response",
                diagnostic_code="non_object_response",
                generation_state=ProviderGenerationState.RESPONSE_RECEIVED,
                retry_disposition=ProviderRetryDisposition.TERMINAL,
                attempts=1,
            )
        return cast(dict[str, object], decoded)

    def _assert_pinned_url(self, url: str) -> None:
        parsed = urlsplit(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if (
            origin != self._allowed_origin
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise OpenAIChatProviderError(
                f"{self._provider_label} request rejected an unpinned origin",
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
    retry_received_408_once: bool = False

    def __post_init__(self) -> None:
        if not isinstance(cast(object, self.retry_received_408_once), bool):
            raise TypeError("retry_received_408_once must be boolean")
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
        request_id_factory: Callable[[], str] | None = None,
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
        self._request_id_factory = request_id_factory or (lambda: f"mia-{uuid4().hex}")

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def model(self) -> str:
        return self._config.model

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        response, _attempts, _request_id = await self._request_with_retry(
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
                diagnostic_code="model_unavailable",
                generation_state=ProviderGenerationState.RESPONSE_RECEIVED,
                retry_disposition=ProviderRetryDisposition.TERMINAL,
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
        return await self.complete_with_observer(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
            attempt_observer=None,
        )

    async def complete_with_observer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
        attempt_observer: ProviderAttemptObserver | None,
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
        response, attempts, request_id = await self._request_with_retry(
            method="POST",
            path=self._config.api_path,
            payload=payload,
            timeout_seconds=timeout_seconds,
            attempt_observer=attempt_observer,
        )
        latency_ms = (time.monotonic() - started) * 1000
        try:
            return _parse_completion(
                response,
                expected_model=self.model,
                provider_label=self._provider_label,
                latency_ms=latency_ms,
                attempts=attempts,
            )
        except ProviderFailure as exc:
            raise OpenAIChatProviderError(
                str(exc),
                error_class=exc.error_class,
                diagnostic_code=exc.diagnostic_code,
                http_status=exc.http_status,
                request_id=request_id,
                generation_state=ProviderGenerationState.RESPONSE_RECEIVED,
                retry_disposition=ProviderRetryDisposition.TERMINAL,
                retry_after_seconds=exc.retry_after_seconds,
                attempts=attempts,
                elapsed_latency_ms=latency_ms,
            ) from exc

    async def _request_with_retry(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, object] | None,
        timeout_seconds: float,
        attempt_observer: ProviderAttemptObserver | None = None,
    ) -> tuple[dict[str, object], int, str]:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError(f"{self._provider_label} timeout_seconds must be finite and positive")
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        request_id = self._request_id_factory()
        _nonempty(request_id, "request_id")
        headers["X-Market-Impact-Request-Id"] = request_id
        started = time.monotonic()
        regeneration_used = False
        for attempt in range(1, self._config.max_attempts + 1):
            attempt_started = time.monotonic()
            _observe(
                attempt_observer,
                ProviderAttemptEvent(
                    request_id=request_id,
                    method=method,
                    physical_attempt=attempt,
                    phase=ProviderAttemptPhase.DISPATCHED,
                    elapsed_latency_ms=0.0,
                ),
            )
            try:
                response = await self._transport.request_json(
                    method=method,
                    url=self._config.endpoint(path),
                    headers=headers,
                    payload=payload,
                    timeout_seconds=(
                        max(0.001, timeout_seconds - (time.monotonic() - started))
                        if self._config.retry_received_408_once
                        else timeout_seconds
                    ),
                )
                _observe(
                    attempt_observer,
                    ProviderAttemptEvent(
                        request_id=request_id,
                        method=method,
                        physical_attempt=attempt,
                        phase=ProviderAttemptPhase.SUCCEEDED,
                        elapsed_latency_ms=(time.monotonic() - attempt_started) * 1000,
                    ),
                )
                return response, attempt, request_id
            except OpenAIChatProviderError as exc:
                regenerate = (
                    self._config.retry_received_408_once
                    and method.upper() == "POST"
                    and not regeneration_used
                    and exc.http_status == 408
                    and exc.generation_state is ProviderGenerationState.UNKNOWN
                    and exc.diagnostic_code in {"http_408", "upstream_stream_incomplete"}
                )
                delay = self._config.retry_backoff_seconds * (2 ** (attempt - 1))
                if regenerate:
                    delay = max(1.0, delay)
                if exc.retry_after_seconds is not None:
                    delay = max(delay, min(exc.retry_after_seconds, 60.0))
                retry = (_retry_is_safe(method, exc) or regenerate) and (
                    attempt < self._config.max_attempts
                )
                if self._config.retry_received_408_once:
                    retry = retry and (time.monotonic() - started + delay < timeout_seconds)
                contextual = OpenAIChatProviderError(
                    _redact_secret(str(exc), self._api_key),
                    error_class=exc.error_class,
                    diagnostic_code=exc.diagnostic_code,
                    http_status=exc.http_status,
                    request_id=request_id,
                    generation_state=exc.generation_state,
                    retry_disposition=(
                        ProviderRetryDisposition.AUTHORIZED_REGENERATION
                        if regenerate and retry
                        else exc.retry_disposition
                    ),
                    retry_after_seconds=exc.retry_after_seconds,
                    attempts=attempt,
                    elapsed_latency_ms=(time.monotonic() - started) * 1000,
                )
                _observe(
                    attempt_observer,
                    ProviderAttemptEvent(
                        request_id=request_id,
                        method=method,
                        physical_attempt=attempt,
                        phase=ProviderAttemptPhase.FAILED,
                        elapsed_latency_ms=(time.monotonic() - attempt_started) * 1000,
                        failure=contextual,
                    ),
                )
                if not retry:
                    raise contextual from exc
                regeneration_used = regeneration_used or regenerate
                if delay:
                    await asyncio.sleep(delay)
                if (
                    self._config.retry_received_408_once
                    and time.monotonic() - started >= timeout_seconds
                ):
                    raise contextual from exc
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


def _http_diagnostic_code(status: int, body: str) -> str:
    normalized = " ".join(body.lower().split())
    if "bad record mac" in normalized or "decryption failed or bad record mac" in normalized:
        return "tls_bad_record_mac"
    if (
        "stream disconnected before completion" in normalized
        or "stream closed before response.completed" in normalized
    ):
        return "upstream_stream_incomplete"
    if any(
        marker in normalized
        for marker in (
            "auth_unavailable",
            "auth unavailable",
            "authentication unavailable",
            "no available credential",
            "no available account",
            "no auth available",
        )
    ):
        return "auth_unavailable"
    if "authentication_failed" in normalized or "authentication failed" in normalized:
        return "authentication_failed"
    if (
        "quota_exhausted" in normalized
        or "quota exhausted" in normalized
        or "insufficient_quota" in normalized
        or "quota" in normalized
    ):
        return "quota_exhausted"
    if status in {401, 403, 407}:
        return "authentication_failed"
    if status == 429:
        return "rate_limited"
    if 500 <= status < 600:
        return "upstream_server_error"
    return f"http_{status}"


def _http_generation_state(
    method: str, status: int, diagnostic_code: str
) -> ProviderGenerationState:
    if diagnostic_code == "upstream_stream_incomplete":
        return ProviderGenerationState.UNKNOWN
    if method.upper() == "POST" and status == 408:
        return ProviderGenerationState.UNKNOWN
    if method.upper() != "POST" or status < 500:
        return ProviderGenerationState.NOT_STARTED
    if diagnostic_code in {"auth_unavailable", "authentication_failed", "quota_exhausted"}:
        return ProviderGenerationState.NOT_STARTED
    return ProviderGenerationState.UNKNOWN


def _http_retry_disposition(
    method: str,
    status: int,
    diagnostic_code: str,
    generation_state: ProviderGenerationState,
) -> ProviderRetryDisposition:
    if diagnostic_code in {"auth_unavailable", "authentication_failed", "quota_exhausted"}:
        return ProviderRetryDisposition.TERMINAL
    if method.upper() == "GET" and (status == 408 or status == 429 or 500 <= status < 600):
        return ProviderRetryDisposition.SAFE
    if (
        method.upper() == "POST"
        and status == 429
        and diagnostic_code == "rate_limited"
        and generation_state is ProviderGenerationState.NOT_STARTED
    ):
        return ProviderRetryDisposition.SAFE
    if generation_state is ProviderGenerationState.UNKNOWN:
        return ProviderRetryDisposition.FORBIDDEN
    return ProviderRetryDisposition.TERMINAL


def _transport_generation_state(method: str) -> ProviderGenerationState:
    return (
        ProviderGenerationState.NOT_STARTED
        if method.upper() == "GET"
        else ProviderGenerationState.UNKNOWN
    )


def _transport_retry_disposition(method: str) -> ProviderRetryDisposition:
    return (
        ProviderRetryDisposition.SAFE
        if method.upper() == "GET"
        else ProviderRetryDisposition.FORBIDDEN
    )


def _retry_is_safe(method: str, failure: ProviderFailure) -> bool:
    if failure.retry_disposition is not ProviderRetryDisposition.SAFE:
        return False
    if method.upper() != "POST":
        return True
    return (
        failure.generation_state is ProviderGenerationState.NOT_STARTED
        and failure.http_status == 429
        and failure.diagnostic_code == "rate_limited"
    )


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if target.tzinfo is None:
            target = target.replace(tzinfo=UTC)
        seconds = (target - datetime.now(UTC)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def _is_tls_bad_record_mac(error: httpx2.RequestError) -> bool:
    text = str(error).lower()
    return "bad record mac" in text or "decryption failed or bad record mac" in text


def _observe(observer: ProviderAttemptObserver | None, event: ProviderAttemptEvent) -> None:
    if observer is not None:
        observer(event)
