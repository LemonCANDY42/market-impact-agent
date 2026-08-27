from __future__ import annotations

import math
import os
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

MINIMAX_CHINA_ORIGIN = "https://api.minimaxi.com"
MiniMaxProviderError = OpenAIChatProviderError


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
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("MiniMax max_attempts must be between one and three")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or not 0 <= self.retry_backoff_seconds <= 5
        ):
            raise ValueError("MiniMax retry_backoff_seconds must be finite and between 0 and 5")

    def endpoint(self, path: str) -> str:
        return urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))


class UrllibJsonTransport(PinnedUrllibJsonTransport):
    def __init__(self) -> None:
        super().__init__(allowed_origin=MINIMAX_CHINA_ORIGIN, provider_label="MiniMax")


class MiniMaxOpenAIProvider(OpenAIChatCompatibleProvider):
    def __init__(
        self,
        *,
        api_key: str,
        config: MiniMaxProviderConfig,
        transport: JsonHttpTransport | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("MiniMax API key is required")
        self.config = config
        super().__init__(
            api_key=api_key,
            provider_id="minimax-openai-compatible",
            provider_label="MiniMax",
            config=OpenAIChatProviderConfig(
                origin=config.base_url,
                model=config.model,
                api_path=config.api_path,
                models_path=config.models_path,
                max_attempts=config.max_attempts,
                retry_backoff_seconds=config.retry_backoff_seconds,
            ),
            completion_parameters={"reasoning_split": True},
            transport=transport or UrllibJsonTransport(),
        )

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
