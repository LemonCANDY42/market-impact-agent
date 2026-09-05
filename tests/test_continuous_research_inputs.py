# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.continuous_research_inputs import (
    continuous_event_facts,
    continuous_research_repository,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from tests.test_historical_ashare_inputs import _capture


def test_research_ignores_future_factors_and_does_not_require_trading_rules(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path)
    symbol = "510300.SH"
    prices = _capture(
        store,
        "fund_daily",
        {"ts_code": symbol, "start_date": "20250102", "end_date": "20250106"},
        [
            dict(ts_code=symbol, trade_date=day, close=close, vol=1000)
            for day, close in (("20250102", 10), ("20250103", 5), ("20250106", 999))
        ],
    )
    factors = _capture(
        store,
        "fund_adj",
        {"ts_code": symbol, "start_date": "20250102", "end_date": "20250106"},
        [
            dict(ts_code=symbol, trade_date=day, adj_factor=factor)
            for day, factor in (("20250102", 1), ("20250103", 2), ("20250106", 100))
        ],
    )
    market = HistoricalAShareInputs(
        store=store,
        snapshot_ids=(prices, factors),
        rule_artifact_hashes=(),
        policy=ModeledHistoricalPolicy("fixture-research-only", Decimal(".001")),
    )
    cutoff = datetime(2025, 1, 6, 1, 25, tzinfo=UTC)
    projection = market.research_series(symbol, cutoff)
    rows = cast(list[dict[str, object]], projection["rows"])
    assert [row["raw_close"] for row in rows] == ["10", "5"]
    assert [row["cutoff_adjusted_close"] for row in rows] == ["5", "5"]
    assert market.instrument_spec(symbol, cutoff) is None
    repository = asyncio.run(
        continuous_research_repository(
            market=market, cutoff=cutoff, event_scope="private-case-label", symbols=(symbol,)
        )
    )
    assert "private-case-label" not in str(repository.evidence_pack.to_dict())
    assert repository.evidence_pack.as_of == cutoff
    assert any("coverage is missing" in gap for gap in repository.evidence_pack.data_gaps)
    # Raw halving with its sourced factor adjustment is not a market-move trigger.
    assert asyncio.run(continuous_event_facts(repository)) == ()
