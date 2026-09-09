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
    price_reference = repository.evidence_pack.evidence[0]
    loaded = cast(
        dict[str, object],
        asyncio.run(repository.read_evidence({"evidence_id": price_reference.evidence_id})),
    )
    document = cast(dict[str, object], loaded["document"])
    assert document["fields"] == [
        "trade_date",
        "raw_close",
        "cutoff_adjusted_close",
        "volume_lots",
    ]
    semantics = cast(dict[str, str], document["field_semantics"])
    assert semantics["raw_close"] == (
        "actual quoted session close; do not mix it with adjusted closes"
    )
    assert "consistent adjusted-close basis" in semantics["cutoff_adjusted_close"]
    proof_hash = cast(str, document["source_projection_hash"])
    assert store.artifacts.read_json(proof_hash) == projection
    # Raw halving with its sourced factor adjustment is not a market-move trigger.
    assert asyncio.run(continuous_event_facts(repository)) == ()


def test_fact_projection_uses_source_records_not_evidence_names() -> None:
    from dataclasses import replace

    from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
    from market_impact_agent.frozen_research import FrozenResearchRepository
    from market_impact_agent.research import EvidenceTier

    cutoff = datetime(2025, 1, 6, 1, 25, tzinfo=UTC)
    published = datetime(2025, 1, 3, 0, tzinfo=UTC)
    document: dict[str, object] = {
        "records": [
            {
                "evidence_record_id": "publisher-version-a",
                "published_at": published.isoformat(),
                "available_at": published.isoformat(),
            }
        ],
        "retrieved_at": cutoff.isoformat(),
    }
    reference = EvidenceReference(
        "arbitrary-source-name",
        "source-publication",
        "source://publisher/version-a",
        EvidenceTier.OFFICIAL,
        published,
        canonical_hash(document),
        "Dated publisher record",
    )

    def repository(ref: EvidenceReference, value: dict[str, object]):
        return FrozenResearchRepository(
            evidence_pack=EvidencePack.build(
                event_id="event",
                as_of=cutoff,
                research_question="Review the source.",
                evidence=(ref,),
                pattern_packs=(),
                allowed_targets=("510300.SH",),
                data_gaps=(),
            ),
            evidence_documents={ref.evidence_id: value},
            pattern_packs={},
        )

    first = asyncio.run(continuous_event_facts(repository(reference, document)))
    renamed = asyncio.run(
        continuous_event_facts(
            repository(replace(reference, evidence_id="price-not-an-event"), document)
        )
    )
    assert first == renamed == ("qualified-fact:" + canonical_hash("publisher-version-a"),)
    receipt_only: dict[str, object] = {
        "retrieved_at": cutoff.isoformat(),
        "source_api": "stock_basic",
    }
    ref = replace(reference, content_hash=canonical_hash(receipt_only), available_at=cutoff)
    assert asyncio.run(continuous_event_facts(repository(ref, receipt_only))) == ()
