from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    RegimeSeries,
    ValidatedRegimePanel,
)
from market_impact_agent.regime_evidence import (
    RegimeCheckpoint,
    RegimeEvidenceAuthorityKind,
    RegimeEvidenceAvailabilityBasis,
    RegimeEvidenceRecord,
)

_PRICE_LATENCY_MODEL_ID = "cn-daily-price-next-day-0800-v1"
_PRICE_LATENCY_MODEL_HASH = canonical_hash(
    {
        "model_id": _PRICE_LATENCY_MODEL_ID,
        "published_at": "trade_date 15:00 Asia/Shanghai",
        "available_at": "next calendar date 08:00 Asia/Shanghai",
        "decision_rule": "only rows strictly before the checkpoint session",
    }
)


def panel_authority_source_ref(
    *,
    source: str,
    tushare_code: str,
    case_key: str,
    checkpoint_date: date,
) -> str:
    return (
        f"tushare://{source}/{tushare_code}?case={case_key}"
        f"&checkpoint={checkpoint_date.isoformat()}"
    )


def panel_series_as_of_hash(series: RegimeSeries, checkpoint_date: date) -> str:
    rows = tuple(row for row in series.rows if _row_date(row) < checkpoint_date)
    return canonical_hash(
        {
            "series_id": series.series_id,
            "kind": series.kind,
            "tushare_code": series.tushare_code,
            "source": series.source,
            "return_basis": series.return_basis,
            "checkpoint_date": checkpoint_date.isoformat(),
            "rows": list(rows),
        }
    )


def build_panel_authority_records(
    validated_panel: ValidatedRegimePanel,
    *,
    market_case: MarketRegimeCase,
    checkpoints: tuple[RegimeCheckpoint, ...],
) -> tuple[RegimeEvidenceRecord, ...]:
    panel = validated_panel.panel
    series_by_id = {item.series_id: item for item in panel.series}
    by_code = {item.tushare_code: item for item in panel.series}
    for proxy_id, code in panel.proxy_resolution:
        if code in by_code:
            series_by_id[proxy_id] = by_code[code]
    records: list[RegimeEvidenceRecord] = []
    for checkpoint in checkpoints:
        series_specs = ((market_case.primary_market_index, "market_price", "tushare-market"),)
        industry_specs = tuple(
            (proxy_id, "industry_price", "tushare-industry")
            for proxy_id in market_case.required_industry_proxies
        )
        for series_id, category, source_id in (*series_specs, *industry_specs):
            series = series_by_id.get(series_id)
            if series is None:
                continue
            rows = tuple(row for row in series.rows if _row_date(row) < checkpoint.session_date)
            if not rows:
                continue
            last_trade_date = max(_row_date(row) for row in rows)
            published_at = _local(last_trade_date, time(15)).astimezone(UTC)
            available_at = _local(last_trade_date + timedelta(days=1), time(8)).astimezone(UTC)
            content_hash = panel_series_as_of_hash(series, checkpoint.session_date)
            source_ref = panel_authority_source_ref(
                source=series.source,
                tushare_code=series.tushare_code,
                case_key=market_case.case_key,
                checkpoint_date=checkpoint.session_date,
            )
            authority_hash = canonical_hash(
                {
                    "panel_id": validated_panel.panel_id,
                    "panel_hash": validated_panel.panel_hash,
                    "provider_id": panel.provider_id,
                    "provider_version": panel.provider_version,
                    "source_ref": source_ref,
                    "content_hash": content_hash,
                    "latency_model_hash": _PRICE_LATENCY_MODEL_HASH,
                }
            )
            records.append(
                RegimeEvidenceRecord.build(
                    case_keys=(market_case.case_key,),
                    category=category,
                    source_id=source_id,
                    provider_id=panel.provider_id,
                    publisher_id="tushare",
                    source_ref=source_ref,
                    claim_id=(
                        f"{market_case.case_key}-{checkpoint.session_date.isoformat()}-{series_id}"
                    ),
                    lineage_id=f"{market_case.case_key}-{series_id}",
                    title=f"{series_id} price rows available before {checkpoint.session_date}",
                    occurred_at=None,
                    published_at=published_at,
                    source_updated_at=None,
                    available_at=available_at,
                    availability_basis=RegimeEvidenceAvailabilityBasis.MODELED_LATENCY,
                    latency_model_id=_PRICE_LATENCY_MODEL_ID,
                    latency_model_hash=_PRICE_LATENCY_MODEL_HASH,
                    authority_kind=RegimeEvidenceAuthorityKind.PROVIDER_VERSION,
                    authority_id=validated_panel.panel_id,
                    authority_at=panel.retrieved_at,
                    authority_hash=authority_hash,
                    content_hash=content_hash,
                    supersedes_id=None,
                    license_scope="private_licensed",
                )
            )
    return tuple(records)


def _row_date(row: dict[str, object]) -> date:
    value = row.get("trade_date")
    if not isinstance(value, str):
        raise TypeError("regime price row trade_date must be a string")
    return date.fromisoformat(value) if "-" in value else datetime.strptime(value, "%Y%m%d").date()


def _local(value: date, value_time: time) -> datetime:
    return datetime.combine(value, value_time, tzinfo=ZoneInfo("Asia/Shanghai"))
