from __future__ import annotations

from datetime import UTC, datetime

from market_impact_agent.regime_positioning_evidence import build_margin_evidence_records
from market_impact_agent.tushare import TushareTable, tushare_table_content_hash


def test_margin_rows_become_versioned_next_morning_positioning_evidence() -> None:
    fields = (
        "trade_date",
        "exchange_id",
        "rzye",
        "rzmre",
        "rzche",
        "rqye",
        "rqmcl",
        "rzrqye",
    )
    rows = (
        ("20240923", "SSE", 750_000.0, 10_000.0, 9_000.0, 5_000.0, 200.0, 755_000.0),
        ("20240923", "SZSE", 680_000.0, 8_000.0, 7_500.0, 4_000.0, 100.0, 684_000.0),
    )
    params = {"start_date": "20240920", "end_date": "20240923"}
    table = TushareTable(
        endpoint="https://api.tushare.pro",
        api_name="margin",
        params=tuple(sorted(params.items())),
        fields=fields,
        rows=rows,
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        content_hash=tushare_table_content_hash(
            api_name="margin",
            params=params,
            fields=fields,
            rows=rows,
        ),
    )

    records = build_margin_evidence_records(
        table,
        case_keys=("cn-2024-policy-melt-up",),
    )

    assert len(records) == 2
    assert {item.publisher_id for item in records} == {"sse", "szse"}
    assert all(item.source_id == "exchange-positioning-flow" for item in records)
    assert all(item.provider_id == "tushare-http" for item in records)
    assert all(item.published_at == datetime(2024, 9, 23, 7, tzinfo=UTC) for item in records)
    assert all(item.available_at == datetime(2024, 9, 24, 1, tzinfo=UTC) for item in records)
    assert all(item.authority_at == table.retrieved_at for item in records)
    assert records[0].content_hash != records[1].content_hash
