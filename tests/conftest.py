"""Offline tests never share model leases with paid workers."""

from pathlib import Path
from typing import Any

import pytest

from market_impact_agent.agent_engine import AgentEngine
from market_impact_agent.agent_runtime import RuntimeConfig

from .runtime_fakes import BusinessModelFixture


@pytest.fixture(autouse=True)
def model_fixture_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKET_IMPACT_MODEL_STATE_ROOT", str(tmp_path / "model-admission"))
    original = AgentEngine.__init__

    def initialize(self: AgentEngine, **kwargs: Any) -> None:
        provider = kwargs["provider"]
        if isinstance(provider, BusinessModelFixture):
            config: RuntimeConfig = kwargs["config"]
            provider.bind_runtime(config)
        original(self, **kwargs)

    monkeypatch.setattr(AgentEngine, "__init__", initialize)
