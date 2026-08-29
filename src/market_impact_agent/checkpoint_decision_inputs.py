from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import SourceObservation
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability

CHECKPOINT_DECISION_INPUT_SCHEMA = "market-impact.checkpoint-decision-input.v1"
_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "record_id",
        "checkpoint_snapshot_set_id",
        "checkpoint_key",
        "barrier_at",
        "snapshot_id",
        "route_kinds",
        "capability",
        "record_type",
        "observation_id",
        "source",
        "times",
        "data",
        "price_basis",
        "completeness_gaps",
        "historical_pit_claim",
        "evidence_promoted",
        "execution_capability",
    }
)


def checkpoint_decision_input_from_dict(value: object) -> dict[str, object]:
    """Validate and canonically reload one decision-input projection."""

    schema_errors = _schema_errors(value)
    if schema_errors:
        raise ValueError(
            "checkpoint decision input does not conform to its schema: " + "; ".join(schema_errors)
        )
    payload = _string_object(value, "checkpoint decision input")
    if frozenset(payload) != _RECORD_FIELDS:
        raise ValueError("checkpoint decision input fields are incomplete or unknown")
    if payload["schema_version"] != CHECKPOINT_DECISION_INPUT_SCHEMA:
        raise ValueError("unsupported checkpoint decision input schema")
    record_id = _prefixed_hash(
        payload["record_id"],
        "checkpoint-decision-input-",
        "checkpoint decision input record_id",
    )
    _prefixed_hash(
        payload["checkpoint_snapshot_set_id"],
        "prospective-checkpoint-snapshot-set-",
        "checkpoint decision input Snapshot Set ID",
    )
    _required_string(payload["checkpoint_key"], "checkpoint decision input checkpoint key")
    _prefixed_hash(
        payload["snapshot_id"],
        "data-snapshot-",
        "checkpoint decision input Snapshot ID",
    )
    _required_string(payload["record_type"], "checkpoint decision input record type")
    _prefixed_hash(
        payload["observation_id"],
        "source-observation-",
        "checkpoint decision input observation ID",
    )
    ObservationCapability(
        _required_string(payload["capability"], "checkpoint decision input capability")
    )
    route_kinds = _string_list(payload["route_kinds"], "checkpoint decision input route kinds")
    if (
        not route_kinds
        or route_kinds != sorted(set(route_kinds))
        or any(item != item.strip() for item in route_kinds)
    ):
        raise ValueError("checkpoint decision input route kinds must be sorted and unique")
    gaps = _string_list(payload["completeness_gaps"], "checkpoint decision input gaps")
    if gaps != sorted(set(gaps)):
        raise ValueError("checkpoint decision input gaps must be sorted and unique")
    _string_object(payload["source"], "checkpoint decision input source")
    times = _string_object(payload["times"], "checkpoint decision input times")
    _string_object(payload["data"], "checkpoint decision input data")
    if payload["price_basis"] is not None:
        _string_object(payload["price_basis"], "checkpoint decision input price basis")
    if any(
        payload[name] is not False
        for name in ("historical_pit_claim", "evidence_promoted", "execution_capability")
    ):
        raise ValueError("checkpoint decision input cannot grant evidence or execution authority")

    barrier_at = _parsed_timestamp(payload["barrier_at"], "checkpoint decision input barrier_at")
    available_at = _parsed_timestamp(
        times.get("available_at"),
        "checkpoint decision input available_at",
    )
    authority_at = _parsed_timestamp(
        times.get("authority_at"),
        "checkpoint decision input authority_at",
    )
    retrieved_at = _parsed_timestamp(
        times.get("retrieved_at"),
        "checkpoint decision input retrieved_at",
    )
    if available_at > barrier_at or authority_at > barrier_at:
        raise ValueError("checkpoint decision input is not authoritative at its barrier")
    if available_at > authority_at or authority_at > retrieved_at:
        raise ValueError("checkpoint decision input time ordering is invalid")

    core = {key: item for key, item in payload.items() if key != "record_id"}
    if record_id != f"checkpoint-decision-input-{canonical_hash(core)}":
        raise ValueError("checkpoint decision input record_id does not match content")
    canonical = json.loads(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return cast(dict[str, object], canonical)


def project_checkpoint_observation(
    *,
    checkpoint_snapshot_set_id: str,
    checkpoint_key: str,
    barrier_at: datetime,
    snapshot_id: str,
    route_kinds: tuple[str, ...],
    observation: SourceObservation,
) -> dict[str, object]:
    """Build a deterministic provider-neutral view over one frozen observation."""

    _prefixed_hash(
        checkpoint_snapshot_set_id,
        "prospective-checkpoint-snapshot-set-",
        "checkpoint decision input Snapshot Set ID",
    )
    _prefixed_hash(
        snapshot_id,
        "data-snapshot-",
        "checkpoint decision input Snapshot ID",
    )
    if not checkpoint_key or checkpoint_key != checkpoint_key.strip():
        raise ValueError("checkpoint decision input requires a checkpoint key")
    require_aware(barrier_at, "checkpoint decision input barrier_at")
    if barrier_at.utcoffset() != UTC.utcoffset(barrier_at):
        raise ValueError("checkpoint decision input barrier_at must use UTC")
    if (
        not route_kinds
        or route_kinds != tuple(sorted(set(route_kinds)))
        or any(not item or item != item.strip() for item in route_kinds)
    ):
        raise ValueError("checkpoint decision input route kinds must be sorted and unique")
    if observation.times.available_at is None or observation.times.available_at > barrier_at:
        raise ValueError("checkpoint decision input observation is unavailable at the barrier")
    if observation.authority_at is None or observation.authority_at > barrier_at:
        raise ValueError("checkpoint decision input observation lacks barrier-time authority")

    record_type, data, price_basis, gaps = _project_payload(
        observation,
        barrier_at=barrier_at,
    )
    core = {
        "schema_version": CHECKPOINT_DECISION_INPUT_SCHEMA,
        "checkpoint_snapshot_set_id": checkpoint_snapshot_set_id,
        "checkpoint_key": checkpoint_key,
        "barrier_at": _timestamp(barrier_at),
        "snapshot_id": snapshot_id,
        "route_kinds": list(route_kinds),
        "capability": observation.capability.value,
        "record_type": record_type,
        "observation_id": observation.observation_id,
        "source": {
            "provider_id": observation.provider_id,
            "provider_version": observation.provider_version,
            "upstream_source": observation.upstream_source,
            "upstream_record_id": observation.upstream_record_id,
            "source_ref": observation.source_ref,
            "lineage_id": observation.lineage_id,
            "raw_content_hash": observation.raw_content_hash,
            "license_scope": observation.license_scope,
        },
        "times": {
            **observation.times.to_dict(),
            "authority_at": _timestamp(observation.authority_at),
            "authority_kind": observation.authority_kind,
        },
        "data": data,
        "price_basis": price_basis,
        "completeness_gaps": list(gaps),
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }
    return {
        **core,
        "record_id": f"checkpoint-decision-input-{canonical_hash(core)}",
    }


def _project_payload(
    observation: SourceObservation,
    *,
    barrier_at: datetime,
) -> tuple[str, dict[str, object], dict[str, object] | None, tuple[str, ...]]:
    payload = observation.normalized_payload
    record = _record(payload)
    if observation.capability is ObservationCapability.EVENT_REVELATION:
        return (
            "event_fact",
            {
                "event_type": _first(payload, record, "event_type", "channels"),
                "headline": _first(payload, record, "headline", "title"),
                "industry": _first(payload, record, "industry", "industry_name"),
                "instrument_code": _first(payload, record, "instrument_code", "ts_code"),
                "publisher": _first(payload, record, "publisher", "upstream_publisher"),
                "source_url": _first(payload, record, "url") or observation.source_ref,
                "statement": _first(payload, record, "summary", "content"),
            },
            None,
            (),
        )
    if observation.capability is ObservationCapability.PRIOR_EXPECTATION:
        reported_metrics = {
            canonical: record[source]
            for source, canonical in _EXPECTATION_METRICS.items()
            if source in record and record[source] is not None
        }
        gaps = ["consensus_not_derived", "reported_metric_units_unverified"]
        if not reported_metrics:
            gaps.append("forecast_values_missing")
        return (
            "forecast_observation",
            {
                "analyst": _first(payload, record, "analyst", "author_name"),
                "forecast_institution": _first(payload, record, "org_name"),
                "forecast_period": _first(payload, record, "quarter"),
                "instrument_code": _first(payload, record, "instrument_code", "ts_code"),
                "issuer_name": _first(payload, record, "name"),
                "publisher": _first(payload, record, "publisher", "upstream_publisher"),
                "rating": _first(payload, record, "rating"),
                "report_date": _first(payload, record, "report_date"),
                "report_title": _first(payload, record, "report_title"),
                "report_type": _first(payload, record, "report_type"),
                "reported_metrics": reported_metrics,
            },
            None,
            tuple(sorted(gaps)),
        )
    if observation.capability is ObservationCapability.MARKET_CONTEXT:
        api_name = _string(_first(payload, record, "api_name"))
        publisher = _first(payload, record, "publisher", "upstream_publisher")
        if api_name == "trade_cal" or "cal_date" in record:
            return (
                "trading_calendar_session",
                {
                    "is_open": _first(payload, record, "is_open"),
                    "market": _first(payload, record, "market", "exchange"),
                    "pretrade_date": _first(payload, record, "pretrade_date"),
                    "publisher": publisher,
                    "trade_date": _first(payload, record, "trade_date", "cal_date"),
                },
                None,
                (),
            )
        instrument_code = _first(payload, record, "instrument_code", "ts_code")
        explicit_index_code = _first(payload, record, "index_code")
        basis, gaps = _market_price_basis(api_name)
        return (
            _market_record_type(api_name),
            {
                "amount": _first(payload, record, "amount"),
                "change": _first(payload, record, "change"),
                "close": _first(payload, record, "close"),
                "high": _first(payload, record, "high"),
                "index_code": (
                    explicit_index_code
                    if explicit_index_code is not None
                    else instrument_code
                    if api_name == "index_daily"
                    else None
                ),
                "instrument_code": instrument_code,
                "low": _first(payload, record, "low"),
                "market": _first(payload, record, "market", "exchange"),
                "open": _first(payload, record, "open"),
                "pct_change": _first(payload, record, "pct_change", "pct_chg"),
                "pre_close": _first(payload, record, "pre_close"),
                "publisher": publisher,
                "trade_date": _first(payload, record, "trade_date"),
                "volume": _first(payload, record, "volume", "vol"),
            },
            basis,
            gaps,
        )
    if observation.capability is ObservationCapability.EXPOSURE_CANDIDATES:
        api_name = _string(_first(payload, record, "api_name"))
        is_pcf_constituent = api_name in {"etf_sh_cons", "etf_sz_cons"}
        industry_code, industry_name, taxonomy_level, taxonomy_gap = _industry_taxonomy(
            payload,
            record,
            api_name=api_name,
        )
        trade_date = _first(payload, record, "trade_date")
        effective_from = (
            trade_date
            if api_name == "stk_limit" or is_pcf_constituent
            else _first(payload, record, "in_date", "list_date")
        )
        effective_to = (
            trade_date
            if api_name == "stk_limit" or is_pcf_constituent
            else _first(payload, record, "out_date", "delist_date")
        )
        taxonomy_source = _first(payload, record, "src")
        exposure_data = {
            "effective_at_barrier": _effective_at_barrier(
                effective_from,
                effective_to,
                barrier_at=barrier_at,
            ),
            "effective_from": effective_from,
            "effective_to": effective_to,
            "index_code": _first(payload, record, "index_code"),
            "industry_code": industry_code,
            "industry_name": industry_name,
            "instrument_class": _instrument_class(api_name),
            "instrument_code": _first(payload, record, "instrument_code", "ts_code"),
            "instrument_name": _first(payload, record, "instrument_name", "name", "csname"),
            "list_status": _first(payload, record, "list_status"),
            "lower_price_limit": _first(payload, record, "lower_price_limit", "down_limit"),
            "previous_close": _first(payload, record, "previous_close", "pre_close"),
            "publisher": _first(payload, record, "publisher", "upstream_publisher"),
            "taxonomy_level": taxonomy_level,
            "taxonomy_source": taxonomy_source,
            "trade_date": trade_date,
            "upper_price_limit": _first(payload, record, "upper_price_limit", "up_limit"),
            "venue": _first(payload, record, "venue", "exchange"),
        }
        if api_name in {"index_classify", "index_member_all"}:
            exposure_data["taxonomy_family"] = "shenwan"
        if is_pcf_constituent:
            exposure_data.update(
                {
                    "cash_premium_rate": _first(payload, record, "cpr"),
                    "cash_substitution_amount": _first(payload, record, "sca"),
                    "component_exchange": _first(payload, record, "exchange"),
                    "constituent_code": _first(payload, record, "con_code"),
                    "constituent_name": _first(payload, record, "con_name"),
                    "constituent_quantity": _first(payload, record, "qty"),
                    "redemption_cash_component": _first(payload, record, "red_cc"),
                    "replacement_ratio": _first(payload, record, "rdr"),
                    "subscription_cash_component": _first(payload, record, "sub_cc"),
                    "substitution_flag": _first(payload, record, "sub_flag"),
                }
            )
        return (
            _exposure_record_type(api_name),
            exposure_data,
            None,
            _exposure_gaps(api_name, taxonomy_gap=taxonomy_gap),
        )
    if observation.capability is ObservationCapability.POSITIONING:
        return (
            "margin_financing_snapshot",
            {
                "financing_balance": _first(payload, record, "financing_balance", "rzye"),
                "financing_purchases": _first(payload, record, "financing_purchases", "rzmre"),
                "financing_repayments": _first(payload, record, "financing_repayments", "rzche"),
                "instrument_code": _first(payload, record, "instrument_code", "ts_code"),
                "market": _first(payload, record, "market", "exchange_id", "exchange"),
                "publisher": _first(payload, record, "publisher", "upstream_publisher"),
                "securities_lending_balance": _first(
                    payload, record, "securities_lending_balance", "rqye"
                ),
                "securities_lending_sales": _first(
                    payload, record, "securities_lending_sales", "rqmcl"
                ),
                "total_margin_balance": _first(payload, record, "total_margin_balance", "rzrqye"),
                "trade_date": _first(payload, record, "trade_date"),
                "unit": None,
            },
            None,
            (
                "publication_cadence_unverified",
                "reported_units_unverified",
                "revision_policy_unverified",
            ),
        )
    if observation.capability is ObservationCapability.MACRO_VINTAGE:
        return (
            "macro_release_schedule",
            {
                "indicator": _first(payload, record, "indicator", "data_api"),
                "issuing_organization": _first(payload, record, "issuing_org"),
                "original_release_observation_id": None,
                "publisher": _first(payload, record, "publisher", "upstream_publisher"),
                "reference_period": _first(payload, record, "reference_period", "month", "quarter"),
                "release_date": _first(payload, record, "release_date", "publish_date"),
                "release_title": _first(payload, record, "release_title", "title"),
                "revision_lineage": [],
            },
            None,
            ("original_release_missing", "revision_lineage_missing"),
        )
    return (
        observation.capability.value,
        {"fields": record or payload},
        None,
        ("provider_neutral_projection_unavailable",),
    )


def _record(payload: Mapping[str, object]) -> dict[str, object]:
    value = payload.get("record")
    if not isinstance(value, dict):
        return {}
    untyped = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in untyped):
        raise ValueError("checkpoint decision input record keys must be strings")
    return cast(dict[str, object], untyped)


def _first(
    payload: Mapping[str, object],
    record: Mapping[str, object],
    *names: str,
) -> object | None:
    for name in names:
        if name in payload:
            value = payload[name]
            if value is not None:
                return value
        if name in record:
            value = record[name]
            if value is not None:
                return value
    return None


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parsed_timestamp(value: object, name: str) -> datetime:
    text = _required_string(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a timestamp") from exc
    require_aware(parsed, name)
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{name} must use UTC")
    return parsed.astimezone(UTC)


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _prefixed_hash(value: object, prefix: str, name: str) -> str:
    text = _required_string(value, name)
    if re.fullmatch(rf"{re.escape(prefix)}[0-9a-f]{{64}}", text) is None:
        raise ValueError(f"{name} is invalid")
    return text


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a string array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) or not item or item != item.strip() for item in items):
        raise ValueError(f"{name} must be a string array")
    return cast(list[str], items)


def _string_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    untyped = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in untyped):
        raise ValueError(f"{name} must use string keys")
    return cast(dict[str, object], untyped)


def _market_record_type(api_name: str | None) -> str:
    if api_name == "index_daily":
        return "index_price_bar"
    if api_name == "fund_daily":
        return "fund_price_bar"
    return "market_price_bar"


def _exposure_record_type(api_name: str | None) -> str:
    record_types: dict[str, str] = {
        "etf_basic": "tradable_instrument_mapping",
        "stock_basic": "tradable_instrument",
        "index_classify": "industry_taxonomy",
        "index_member_all": "industry_membership",
        "stk_limit": "daily_tradability_limit",
        "etf_sh_cons": "etf_basket_constituent",
        "etf_sz_cons": "etf_basket_constituent",
    }
    if api_name is None:
        return "instrument_reference"
    return record_types.get(api_name, "instrument_reference")


def _instrument_class(api_name: str | None) -> str | None:
    if api_name == "etf_basic":
        return "exchange_traded_fund"
    if api_name in {"etf_sh_cons", "etf_sz_cons"}:
        return "exchange_traded_fund"
    if api_name in {"stock_basic", "index_member_all", "stk_limit"}:
        return "equity"
    return None


def _exposure_gaps(
    api_name: str | None,
    *,
    taxonomy_gap: str | None,
) -> tuple[str, ...]:
    if api_name == "index_member_all":
        return tuple(
            sorted(
                {
                    "industry_to_tradable_mapping_missing",
                    "taxonomy_version_unverified",
                    *(() if taxonomy_gap is None else (taxonomy_gap,)),
                }
            )
        )
    if api_name == "index_classify":
        return ("effective_taxonomy_interval_unverified", "tradable_exposure_mapping_missing")
    if api_name in {"etf_sh_cons", "etf_sz_cons"}:
        return (
            "basket_publication_time_unverified",
            "basket_revision_lineage_missing",
            "basket_weight_missing",
        )
    return (
        "decision_time_tradability_unverified",
        "lot_size_missing",
        "tick_size_missing",
    )


def _industry_taxonomy(
    payload: Mapping[str, object],
    record: Mapping[str, object],
    *,
    api_name: str | None,
) -> tuple[object | None, object | None, object | None, str | None]:
    if api_name != "index_member_all":
        return (
            _first(payload, record, "industry_code"),
            _first(payload, record, "industry_name"),
            _first(payload, record, "taxonomy_level", "level"),
            None,
        )

    any_level_value = False
    for level in ("l3", "l2", "l1"):
        code = _first(payload, record, f"{level}_code")
        name = _first(payload, record, f"{level}_name")
        any_level_value = any_level_value or code is not None or name is not None
        if code is not None and name is not None:
            return code, name, level, None
    return (
        None,
        None,
        None,
        (
            "industry_taxonomy_pair_incomplete"
            if any_level_value
            else "industry_taxonomy_pair_missing"
        ),
    )


def _effective_at_barrier(
    start: object | None,
    end: object | None,
    *,
    barrier_at: datetime,
) -> bool | None:
    start_date = _source_date(start)
    if start_date is None:
        return None
    end_date = _source_date(end)
    barrier_date = barrier_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
    return start_date <= barrier_date and (end_date is None or barrier_date <= end_date)


def _source_date(value: object | None) -> date | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("checkpoint decision input effective date must be a string")
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    raise ValueError("checkpoint decision input effective date is invalid")


def _market_price_basis(
    api_name: str | None,
) -> tuple[dict[str, object], tuple[str, ...]]:
    if api_name == "index_daily":
        return (
            {
                "as_of_adjusted": False,
                "execution_basis": None,
                "execution_eligible": False,
                "instrument_type": "price_index",
                "research_basis": "price_index",
                "total_return": False,
            },
            ("total_return_series_missing",),
        )
    if api_name == "fund_daily":
        return (
            {
                "as_of_adjusted": False,
                "execution_basis": "raw_tradable_price",
                "execution_eligible": False,
                "instrument_type": "exchange_traded_fund",
                "research_basis": "raw_unadjusted_price",
                "total_return": False,
            },
            ("as_of_adjusted_research_series_missing",),
        )
    return (
        {
            "as_of_adjusted": False,
            "execution_basis": None,
            "execution_eligible": False,
            "instrument_type": "unknown",
            "research_basis": "unknown",
            "total_return": False,
        },
        ("instrument_price_basis_unverified",),
    )


def _string(value: object | None) -> str | None:
    return value if isinstance(value, str) and value else None


class _ContractValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


@lru_cache(maxsize=1)
def _decision_input_validator() -> _ContractValidator:
    package_root = Path(__file__).resolve().parent
    installed = package_root / "schemas" / "checkpoint-decision-input.schema.json"
    path = (
        installed
        if installed.is_file()
        else package_root.parents[1] / "schemas" / "checkpoint-decision-input.schema.json"
    )
    schema = _string_object(
        json.loads(path.read_text(encoding="utf-8")),
        "checkpoint decision input schema",
    )
    return cast(
        _ContractValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )


def _schema_errors(value: object) -> tuple[str, ...]:
    errors = sorted(
        _decision_input_validator().iter_errors(value),
        key=lambda item: (item.json_path, item.message),
    )
    return tuple(f"{item.json_path}: {item.message}" for item in errors)


_EXPECTATION_METRICS = {
    "op_rt": "operating_revenue",
    "op_pr": "operating_profit",
    "tp": "target_price",
    "np": "net_profit",
    "eps": "earnings_per_share",
    "pe": "price_earnings_ratio",
    "rd": "research_and_development",
    "roe": "return_on_equity",
    "ev_ebitda": "enterprise_value_to_ebitda",
    "max_price": "target_price_high",
    "min_price": "target_price_low",
    "imp_dg": "forecast_change",
}
