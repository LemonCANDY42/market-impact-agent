from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.regime_evidence import (
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
    RegimeEvidenceRecord,
)
from market_impact_agent.tushare import TUSHARE_PROVIDER_ID, TushareTable

_MARGIN_LATENCY_MODEL_ID = "tushare-margin-daily-0900-v1"
_MARGIN_LATENCY_MODEL_HASH = canonical_hash(
    {
        "model_id": _MARGIN_LATENCY_MODEL_ID,
        "source": "Tushare permission table states margin updates daily at 09:00",
        "published_at": "trade_date 15:00 Asia/Shanghai",
        "available_at": "next calendar date 09:00 Asia/Shanghai",
    }
)


def build_margin_evidence_records(
    table: TushareTable,
    *,
    case_keys: tuple[str, ...],
) -> tuple[RegimeEvidenceRecord, ...]:
    expected_fields = (
        "trade_date",
        "exchange_id",
        "rzye",
        "rzmre",
        "rzche",
        "rqye",
        "rqmcl",
        "rzrqye",
    )
    if table.api_name != "margin" or table.fields != expected_fields:
        raise ValueError("positioning evidence requires the canonical margin table")
    indexes = {field: table.fields.index(field) for field in table.fields}
    records: list[RegimeEvidenceRecord] = []
    for row in table.rows:
        trade_date = datetime.strptime(str(row[indexes["trade_date"]]), "%Y%m%d").date()
        exchange_id = str(row[indexes["exchange_id"]])
        publisher_id = exchange_id.casefold()
        published_at = datetime.combine(
            trade_date,
            time(15),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).astimezone(UTC)
        available_at = datetime.combine(
            trade_date + timedelta(days=1),
            time(9),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).astimezone(UTC)
        row_payload = dict(zip(table.fields, row, strict=True))
        content_hash = canonical_hash(
            {
                "api_name": table.api_name,
                "params": dict(table.params),
                "row": row_payload,
            }
        )
        source_ref = f"tushare://margin/{exchange_id}/{trade_date.isoformat()}"
        records.append(
            RegimeEvidenceRecord.build(
                case_keys=case_keys,
                category="positioning_or_expectations",
                source_id="exchange-positioning-flow",
                provider_id=TUSHARE_PROVIDER_ID,
                publisher_id=publisher_id,
                source_ref=source_ref,
                claim_id=f"margin-{exchange_id}-{trade_date.isoformat()}",
                lineage_id=f"margin-{exchange_id}-{trade_date.isoformat()}",
                title=f"{exchange_id} margin summary for {trade_date.isoformat()}",
                occurred_at=published_at,
                published_at=published_at,
                source_updated_at=None,
                available_at=available_at,
                availability_basis=RegimeEvidenceAvailabilityBasis.MODELED_LATENCY,
                latency_model_id=_MARGIN_LATENCY_MODEL_ID,
                latency_model_hash=_MARGIN_LATENCY_MODEL_HASH,
                authority_kind=RegimeEvidenceAuthorityKind.PROVIDER_VERSION,
                authority_id=f"tushare-margin-table-{table.content_hash}",
                authority_at=table.retrieved_at,
                authority_hash=canonical_hash(
                    {
                        "provider_id": TUSHARE_PROVIDER_ID,
                        "table_hash": table.content_hash,
                        "source_ref": source_ref,
                        "content_hash": content_hash,
                        "latency_model_hash": _MARGIN_LATENCY_MODEL_HASH,
                    }
                ),
                content_hash=content_hash,
                supersedes_id=None,
                license_scope="private_licensed",
            )
        )
    return tuple(records)
