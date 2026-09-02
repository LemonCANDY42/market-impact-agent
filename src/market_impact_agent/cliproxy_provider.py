from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast
from urllib.parse import urljoin

from market_impact_agent.openai_chat_provider import (
    JsonHttpTransport,
    OpenAIChatCompatibleProvider,
    OpenAIChatProviderConfig,
    OpenAIChatProviderError,
    PinnedUrllibJsonTransport,
)

CLIPROXY_LOCAL_ORIGIN = "http://127.0.0.1:8317"
CLIPROXY_LUNA_MODEL = "gpt-5.6-luna"
CLIPROXY_LUNA_REASONING_EFFORTS = frozenset({"xhigh", "max"})
CLIProxyProviderError = OpenAIChatProviderError


@dataclass(frozen=True, slots=True)
class CLIProxyLunaConfig:
    origin: str
    model: str
    reasoning_effort: str
    api_path: str = "/v1/chat/completions"
    models_path: str = "/v1/models"
    max_attempts: int = 2
    retry_backoff_seconds: float = 0.25
    retry_received_408_once: bool = False

    def __post_init__(self) -> None:
        if self.origin != CLIPROXY_LOCAL_ORIGIN:
            raise ValueError(
                f"CLIProxyAPI origin must exactly match the local loopback origin: "
                f"{CLIPROXY_LOCAL_ORIGIN}"
            )
        if self.model != CLIPROXY_LUNA_MODEL:
            raise ValueError(f"CLIProxyAPI model must remain {CLIPROXY_LUNA_MODEL}")
        if self.reasoning_effort not in CLIPROXY_LUNA_REASONING_EFFORTS:
            raise ValueError(
                "CLIProxyAPI reasoning_effort must be one of "
                f"{', '.join(sorted(CLIPROXY_LUNA_REASONING_EFFORTS))}"
            )
        for name in ("api_path", "models_path"):
            path = cast(str, getattr(self, name))
            if not path.startswith("/") or "?" in path or "#" in path:
                raise ValueError(f"CLIProxyAPI {name} must be an absolute URL path")
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("CLIProxyAPI max_attempts must be between one and three")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or not 0 <= self.retry_backoff_seconds <= 5
        ):
            raise ValueError("CLIProxyAPI retry_backoff_seconds must be finite and between 0 and 5")

    def endpoint(self, path: str) -> str:
        return urljoin(self.origin.rstrip("/") + "/", path.lstrip("/"))


class CLIProxyUrllibJsonTransport(PinnedUrllibJsonTransport):
    def __init__(self) -> None:
        super().__init__(allowed_origin=CLIPROXY_LOCAL_ORIGIN, provider_label="CLIProxyAPI")


class CLIProxyLunaProvider(OpenAIChatCompatibleProvider):
    """Project-to-gateway adapter; gateway-internal retries are outside project proof."""

    def __init__(
        self,
        *,
        api_key: str,
        config: CLIProxyLunaConfig,
        transport: JsonHttpTransport | None = None,
        request_id_factory: Callable[[], str] | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("CLIProxyAPI project API key is required")
        self.config = config
        super().__init__(
            api_key=api_key,
            provider_id="cliproxyapi-openai-compatible",
            provider_label="CLIProxyAPI",
            config=OpenAIChatProviderConfig(
                origin=config.origin,
                model=config.model,
                api_path=config.api_path,
                models_path=config.models_path,
                max_attempts=config.max_attempts,
                retry_backoff_seconds=config.retry_backoff_seconds,
                retry_received_408_once=config.retry_received_408_once,
            ),
            completion_parameters={"reasoning_effort": config.reasoning_effort},
            transport=transport or CLIProxyUrllibJsonTransport(),
            request_id_factory=request_id_factory,
        )
