from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.checkpoint_decision_inputs import (
    checkpoint_decision_input_from_dict,
)
from market_impact_agent.domain import require_aware

CHECKPOINT_MARKET_UNIVERSE_SCHEMA = "market-impact.checkpoint-market-universe-view.v1"
EXCHANGE_INSTRUMENT_RULE_SET_SCHEMA = "market-impact.exchange-instrument-rule-set.v1"

_RULE_SET_FIELDS = frozenset(
    {
        "schema_version",
        "rule_set_id",
        "effective_from",
        "source_documents",
        "rules",
        "historical_pit_claim",
        "execution_capability",
    }
)
_VIEW_FIELDS = frozenset(
    {
        "schema_version",
        "view_id",
        "checkpoint_snapshot_set_id",
        "checkpoint_key",
        "barrier_at",
        "input_record_ids",
        "instrument_rule_set_id",
        "target_venues",
        "allowed_instrument_classes",
        "market_inputs",
        "instruments",
        "industry_exposures",
        "completeness_gaps",
        "historical_pit_claim",
        "evidence_promoted",
        "execution_capability",
        "model_call_authorized",
    }
)
_VENUES = frozenset({"XSHG", "XSHE"})
_INSTRUMENT_CLASSES = frozenset({"equity", "exchange_traded_fund"})
_VENUE_ALIASES = {
    "SSE": "XSHG",
    "SH": "XSHG",
    "XSHG": "XSHG",
    "SZSE": "XSHE",
    "SZ": "XSHE",
    "XSHE": "XSHE",
}


@dataclass(frozen=True, slots=True)
class ExchangeInstrumentRule:
    rule_key: str
    venue: str
    instrument_class: str
    buy_lot_size: int
    price_tick: float
    currency: str
    scope: str
    exceptions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExchangeInstrumentRuleSet:
    rule_set_id: str
    effective_from: date
    source_documents: tuple[dict[str, object], ...]
    rules: tuple[ExchangeInstrumentRule, ...]


def load_exchange_instrument_rule_set(path: Path) -> ExchangeInstrumentRuleSet:
    """Load one content-identified, secret-free exchange rule set."""

    payload = _object(json.loads(path.read_text(encoding="utf-8")), "instrument rule set")
    if frozenset(payload) != _RULE_SET_FIELDS:
        raise ValueError("instrument rule set fields are incomplete or unknown")
    if payload["schema_version"] != EXCHANGE_INSTRUMENT_RULE_SET_SCHEMA:
        raise ValueError("unsupported instrument rule set schema")
    rule_set_id = _prefixed_hash(
        payload["rule_set_id"],
        "exchange-instrument-rule-set-",
        "instrument rule set ID",
    )
    core = {key: value for key, value in payload.items() if key != "rule_set_id"}
    if rule_set_id != f"exchange-instrument-rule-set-{canonical_hash(core)}":
        raise ValueError("instrument rule set ID does not match content")
    if payload["historical_pit_claim"] is not False or payload["execution_capability"] is not False:
        raise ValueError("instrument rule set cannot grant historical or execution authority")

    effective_from = _source_date(payload["effective_from"], "instrument rule effective_from")
    raw_sources = _object_list(payload["source_documents"], "instrument rule sources")
    if not raw_sources:
        raise ValueError("instrument rule set requires source documents")
    source_documents: list[dict[str, object]] = []
    for source in raw_sources:
        if frozenset(source) != frozenset(
            {
                "venue",
                "issuer",
                "notice_id",
                "published_on",
                "effective_from",
                "source_ref",
                "rule_references",
            }
        ):
            raise ValueError("instrument rule source fields are incomplete or unknown")
        venue = _required_string(source["venue"], "instrument rule source venue")
        if venue not in _VENUES:
            raise ValueError("instrument rule source venue is unsupported")
        _required_string(source["issuer"], "instrument rule source issuer")
        _required_string(source["notice_id"], "instrument rule source notice ID")
        _source_date(source["published_on"], "instrument rule source published_on")
        source_effective = _source_date(
            source["effective_from"], "instrument rule source effective_from"
        )
        if source_effective != effective_from:
            raise ValueError("instrument rule source effective date mismatch")
        source_ref = _required_string(source["source_ref"], "instrument rule source ref")
        if not source_ref.startswith("https://"):
            raise ValueError("instrument rule source ref must use HTTPS")
        references = _string_tuple(source["rule_references"], "instrument rule references")
        if not references:
            raise ValueError("instrument rule source requires rule references")
        source_documents.append(source)

    raw_rules = _object_list(payload["rules"], "instrument rules")
    if not raw_rules:
        raise ValueError("instrument rule set requires rules")
    rules: list[ExchangeInstrumentRule] = []
    keys: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    for value in raw_rules:
        if frozenset(value) != frozenset(
            {
                "rule_key",
                "venue",
                "instrument_class",
                "buy_lot_size",
                "price_tick",
                "currency",
                "scope",
                "exceptions",
            }
        ):
            raise ValueError("instrument rule fields are incomplete or unknown")
        rule_key = _required_string(value["rule_key"], "instrument rule key")
        venue = _required_string(value["venue"], "instrument rule venue")
        instrument_class = _required_string(value["instrument_class"], "instrument rule class")
        if venue not in _VENUES or instrument_class not in _INSTRUMENT_CLASSES:
            raise ValueError("instrument rule venue or class is unsupported")
        pair = (venue, instrument_class)
        if rule_key in keys or pair in pairs:
            raise ValueError("instrument rules must have unique keys and venue/class pairs")
        keys.add(rule_key)
        pairs.add(pair)
        buy_lot_size = value["buy_lot_size"]
        if not isinstance(buy_lot_size, int) or isinstance(buy_lot_size, bool) or buy_lot_size <= 0:
            raise ValueError("instrument rule buy_lot_size must be a positive integer")
        price_tick = value["price_tick"]
        if (
            not isinstance(price_tick, (int, float))
            or isinstance(price_tick, bool)
            or price_tick <= 0
        ):
            raise ValueError("instrument rule price_tick must be positive")
        exceptions = _string_tuple(value["exceptions"], "instrument rule exceptions")
        rules.append(
            ExchangeInstrumentRule(
                rule_key=rule_key,
                venue=venue,
                instrument_class=instrument_class,
                buy_lot_size=buy_lot_size,
                price_tick=float(price_tick),
                currency=_required_string(value["currency"], "instrument rule currency"),
                scope=_required_string(value["scope"], "instrument rule scope"),
                exceptions=exceptions,
            )
        )
    return ExchangeInstrumentRuleSet(
        rule_set_id=rule_set_id,
        effective_from=effective_from,
        source_documents=tuple(source_documents),
        rules=tuple(rules),
    )


def build_checkpoint_market_universe_view(
    *,
    decision_inputs: Iterable[Mapping[str, object]],
    rule_set: ExchangeInstrumentRuleSet,
    target_venues: tuple[str, ...],
    allowed_instrument_classes: tuple[str, ...],
) -> dict[str, object]:
    """Join frozen decision inputs without creating a new source or authority."""

    inputs = tuple(checkpoint_decision_input_from_dict(dict(value)) for value in decision_inputs)
    if not inputs:
        raise ValueError("checkpoint market universe view requires decision inputs")
    _unique_allowed(target_venues, _VENUES, "target venues")
    _unique_allowed(
        allowed_instrument_classes,
        _INSTRUMENT_CLASSES,
        "allowed instrument classes",
    )
    snapshot_set_ids = {cast(str, item["checkpoint_snapshot_set_id"]) for item in inputs}
    checkpoint_keys = {cast(str, item["checkpoint_key"]) for item in inputs}
    barriers = {cast(str, item["barrier_at"]) for item in inputs}
    if len(snapshot_set_ids) != 1:
        raise ValueError("checkpoint market universe view requires one checkpoint Snapshot Set")
    if len(checkpoint_keys) != 1 or len(barriers) != 1:
        raise ValueError("checkpoint market universe inputs must share one checkpoint barrier")
    barrier_text = next(iter(barriers))
    barrier_at = _timestamp(barrier_text, "checkpoint market universe barrier")
    barrier_date = barrier_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
    rules_active = barrier_date >= rule_set.effective_from
    active_rules = (
        {(item.venue, item.instrument_class): item for item in rule_set.rules}
        if rules_active
        else {}
    )

    market_inputs = _market_inputs(inputs)
    instruments = _instruments(
        inputs,
        market_inputs=market_inputs,
        active_rules=active_rules,
        target_venues=target_venues,
        allowed_instrument_classes=allowed_instrument_classes,
    )
    industry_exposures, industry_exposure_gaps = _industry_exposures(
        inputs,
        instruments=instruments,
    )
    gaps = {
        *(() if rules_active else ("instrument_rule_not_effective_at_barrier",)),
        *industry_exposure_gaps,
        "calendar_sequence_completeness_unverified",
        "corporate_action_semantics_unverified",
        "market_breadth_missing",
        "market_liquidity_missing",
        "market_volatility_missing",
    }
    market_record_types = {cast(str, item["record_type"]) for item in market_inputs}
    for record_type, gap in (
        ("trading_calendar_session", "trading_calendar_missing"),
        ("index_price_bar", "market_index_price_missing"),
        ("fund_price_bar", "etf_price_missing"),
    ):
        if record_type not in market_record_types:
            gaps.add(gap)
    if not instruments:
        gaps.add("tradable_instrument_candidates_missing")
    if not industry_exposures:
        gaps.add("industry_to_tradable_mapping_missing")
    if not any(
        item["record_type"] == "industry_membership"
        and cast(dict[str, object], item["data"]).get("effective_at_barrier") is True
        for item in inputs
    ):
        gaps.add("effective_industry_membership_missing")
    for item in (*market_inputs, *instruments, *industry_exposures):
        gaps.update(cast(list[str], item["completeness_gaps"]))

    core = {
        "schema_version": CHECKPOINT_MARKET_UNIVERSE_SCHEMA,
        "checkpoint_snapshot_set_id": next(iter(snapshot_set_ids)),
        "checkpoint_key": next(iter(checkpoint_keys)),
        "barrier_at": barrier_text,
        "input_record_ids": sorted(cast(str, item["record_id"]) for item in inputs),
        "instrument_rule_set_id": rule_set.rule_set_id,
        "target_venues": list(target_venues),
        "allowed_instrument_classes": list(allowed_instrument_classes),
        "market_inputs": market_inputs,
        "instruments": instruments,
        "industry_exposures": industry_exposures,
        "completeness_gaps": sorted(gaps),
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
        "model_call_authorized": False,
    }
    result = {**core, "view_id": f"checkpoint-market-universe-view-{canonical_hash(core)}"}
    return checkpoint_market_universe_view_from_dict(result)


def checkpoint_market_universe_view_from_dict(value: object) -> dict[str, object]:
    """Validate and canonically reload one Checkpoint Market Universe View."""

    schema_errors = _schema_errors(value)
    if schema_errors:
        raise ValueError(
            "checkpoint market universe view does not conform to its schema: "
            + "; ".join(schema_errors)
        )
    payload = _object(value, "checkpoint market universe view")
    if frozenset(payload) != _VIEW_FIELDS:
        raise ValueError("checkpoint market universe view fields are incomplete or unknown")
    if payload["schema_version"] != CHECKPOINT_MARKET_UNIVERSE_SCHEMA:
        raise ValueError("unsupported checkpoint market universe view schema")
    view_id = _prefixed_hash(
        payload["view_id"],
        "checkpoint-market-universe-view-",
        "checkpoint market universe view ID",
    )
    if any(
        payload[name] is not False
        for name in (
            "historical_pit_claim",
            "evidence_promoted",
            "execution_capability",
            "model_call_authorized",
        )
    ):
        raise ValueError("checkpoint market universe view cannot grant authority")
    core = {key: item for key, item in payload.items() if key != "view_id"}
    if view_id != f"checkpoint-market-universe-view-{canonical_hash(core)}":
        raise ValueError("checkpoint market universe view_id does not match content")
    return cast(
        dict[str, object],
        json.loads(json.dumps(payload, sort_keys=True, separators=(",", ":"))),
    )


def _market_inputs(inputs: tuple[dict[str, object], ...]) -> list[dict[str, object]]:
    values: list[dict[str, object]] = []
    for item in inputs:
        if item["capability"] != "market_context":
            continue
        data = cast(dict[str, object], item["data"])
        values.append(
            {
                "record_id": item["record_id"],
                "record_type": item["record_type"],
                "snapshot_id": item["snapshot_id"],
                "index_code": data.get("index_code"),
                "instrument_code": data.get("instrument_code"),
                "market": data.get("market"),
                "trade_date": data.get("trade_date"),
                "price_basis": item["price_basis"],
                "completeness_gaps": item["completeness_gaps"],
            }
        )
    return sorted(
        values, key=lambda item: (cast(str, item["record_type"]), cast(str, item["record_id"]))
    )


def _instruments(
    inputs: tuple[dict[str, object], ...],
    *,
    market_inputs: list[dict[str, object]],
    active_rules: Mapping[tuple[str, str], ExchangeInstrumentRule],
    target_venues: tuple[str, ...],
    allowed_instrument_classes: tuple[str, ...],
) -> list[dict[str, object]]:
    limits_by_code: dict[str, list[str]] = {}
    for item in inputs:
        if item["record_type"] != "daily_tradability_limit":
            continue
        data = cast(dict[str, object], item["data"])
        code = _optional_string(data.get("instrument_code"))
        if code is not None and data.get("effective_at_barrier") is True:
            limits_by_code.setdefault(code, []).append(cast(str, item["record_id"]))
    price_versions_by_code: dict[str, list[dict[str, object]]] = {}
    for market_input in market_inputs:
        code = _optional_string(market_input.get("instrument_code"))
        if code is not None and market_input["record_type"] == "fund_price_bar":
            price_versions_by_code.setdefault(code, []).append(market_input)
    prices_by_code = {
        code: max(
            versions,
            key=lambda value: (
                _sortable_source_date(value.get("trade_date")),
                cast(str, value["record_id"]),
            ),
        )
        for code, versions in price_versions_by_code.items()
    }

    master_versions: dict[
        str,
        list[tuple[dict[str, object], dict[str, object], str, str]],
    ] = {}
    for item in inputs:
        if item["record_type"] not in {"tradable_instrument_mapping", "tradable_instrument"}:
            continue
        data = cast(dict[str, object], item["data"])
        code = _optional_string(data.get("instrument_code"))
        instrument_class = _optional_string(data.get("instrument_class"))
        venue = _normalize_venue(data.get("venue"), code=code)
        if (
            code is None
            or instrument_class not in allowed_instrument_classes
            or venue not in target_venues
        ):
            continue
        master_versions.setdefault(code, []).append((item, data, venue, instrument_class))

    values: list[dict[str, object]] = []
    for code in sorted(master_versions):
        versions = master_versions[code]
        item, data, venue, instrument_class = max(
            versions,
            key=lambda value: (
                cast(str, cast(dict[str, object], value[0]["times"])["authority_at"]),
                cast(str, value[0]["record_id"]),
            ),
        )
        effective_at_barrier = data.get("effective_at_barrier")
        list_status = _optional_string(data.get("list_status"))
        research_eligible = effective_at_barrier is True and list_status == "L"
        rule = active_rules.get((venue, instrument_class))
        gaps = set(cast(list[str], item["completeness_gaps"]))
        if rule is not None:
            gaps.discard("lot_size_missing")
            gaps.discard("tick_size_missing")
        else:
            gaps.add("instrument_rule_not_effective_at_barrier")
        if research_eligible:
            gaps.discard("decision_time_tradability_unverified")
            gaps.add("suspension_status_unverified")
            decision_time_tradability = "unverified"
        else:
            decision_time_tradability = "ineligible"
        price_versions = price_versions_by_code.get(code, ())
        if code not in prices_by_code:
            gaps.add("raw_price_at_barrier_missing")
        elif len(price_versions) > 1:
            gaps.add("multiple_raw_price_records_present")
        if len(versions) > 1:
            gaps.add("instrument_master_versions_present")
        related_ids = {
            *(cast(str, value[0]["record_id"]) for value in versions),
            *limits_by_code.get(code, ()),
            *(cast(str, value["record_id"]) for value in price_versions),
        }
        values.append(
            {
                "instrument_code": code,
                "instrument_name": data.get("instrument_name"),
                "instrument_class": instrument_class,
                "venue": venue,
                "index_code": data.get("index_code"),
                "list_status": data.get("list_status"),
                "effective_from": data.get("effective_from"),
                "effective_to": data.get("effective_to"),
                "effective_at_barrier": effective_at_barrier,
                "buy_lot_size": None if rule is None else rule.buy_lot_size,
                "price_tick": None if rule is None else rule.price_tick,
                "currency": None if rule is None else rule.currency,
                "rule_key": None if rule is None else rule.rule_key,
                "rule_exceptions": [] if rule is None else list(rule.exceptions),
                "research_eligible": research_eligible,
                "decision_time_tradability": decision_time_tradability,
                "raw_price_record_id": (
                    None
                    if code not in prices_by_code
                    else cast(str, prices_by_code[code]["record_id"])
                ),
                "daily_limit_record_ids": sorted(limits_by_code.get(code, ())),
                "input_record_ids": sorted(related_ids),
                "completeness_gaps": sorted(gaps),
            }
        )
    return sorted(values, key=lambda item: cast(str, item["instrument_code"]))


def _industry_exposures(
    inputs: tuple[dict[str, object], ...],
    *,
    instruments: list[dict[str, object]],
) -> tuple[list[dict[str, object]], set[str]]:
    taxonomy_versions_by_index_source: dict[tuple[str, str], list[dict[str, object]]] = {}
    taxonomy_identities_by_index_family: dict[tuple[str, str], set[tuple[str, str]]] = {}
    memberships_by_industry_source: dict[tuple[str, str], list[dict[str, object]]] = {}
    memberships_by_industry_family: dict[tuple[str, str], list[dict[str, object]]] = {}
    memberships_by_industry: dict[str, list[dict[str, object]]] = {}
    memberships_by_instrument: dict[str, list[dict[str, object]]] = {}
    basket_constituents_by_etf: dict[str, list[dict[str, object]]] = {}
    taxonomy_indices_without_source: set[str] = set()
    membership_industries_without_source: set[str] = set()
    for item in inputs:
        data = cast(dict[str, object], item["data"])
        if item["record_type"] == "industry_taxonomy":
            index_code = _optional_string(data.get("index_code"))
            taxonomy_family = _optional_string(data.get("taxonomy_family"))
            taxonomy_source = _optional_string(data.get("taxonomy_source"))
            if index_code is not None and taxonomy_source is not None:
                taxonomy_identity = (index_code, taxonomy_source)
                taxonomy_versions_by_index_source.setdefault(taxonomy_identity, []).append(item)
                if taxonomy_family is not None:
                    taxonomy_identities_by_index_family.setdefault(
                        (index_code, taxonomy_family), set()
                    ).add(taxonomy_identity)
            elif index_code is not None:
                taxonomy_indices_without_source.add(index_code)
        elif item["record_type"] == "industry_membership":
            industry_code = _optional_string(data.get("industry_code"))
            taxonomy_family = _optional_string(data.get("taxonomy_family"))
            taxonomy_source = _optional_string(data.get("taxonomy_source"))
            member_code = _optional_string(data.get("instrument_code"))
            if member_code is not None:
                memberships_by_instrument.setdefault(member_code, []).append(item)
            if industry_code is not None and taxonomy_source is not None:
                membership_identity = (industry_code, taxonomy_source)
                memberships_by_industry_source.setdefault(membership_identity, []).append(item)
                memberships_by_industry.setdefault(industry_code, []).append(item)
            elif industry_code is not None:
                membership_industries_without_source.add(industry_code)
            if industry_code is not None and taxonomy_family is not None:
                memberships_by_industry_family.setdefault(
                    (industry_code, taxonomy_family), []
                ).append(item)
        elif item["record_type"] == "etf_basket_constituent":
            etf_code = _optional_string(data.get("instrument_code"))
            constituent_code = _optional_string(data.get("constituent_code"))
            if (
                etf_code is not None
                and constituent_code is not None
                and data.get("effective_at_barrier") is True
            ):
                basket_constituents_by_etf.setdefault(etf_code, []).append(item)

    values: list[dict[str, object]] = []
    join_gaps: set[str] = set()
    for instrument in instruments:
        if instrument["research_eligible"] is not True:
            continue
        index_code = _optional_string(instrument.get("index_code"))
        if index_code is not None and index_code in taxonomy_indices_without_source:
            join_gaps.add("taxonomy_version_unverified")
        for (taxonomy_index_code, taxonomy_source), taxonomy_versions in sorted(
            taxonomy_versions_by_index_source.items()
        ):
            if index_code is None or taxonomy_index_code != index_code:
                continue
            taxonomy = max(
                taxonomy_versions,
                key=lambda value: (
                    cast(str, cast(dict[str, object], value["times"])["authority_at"]),
                    cast(str, value["record_id"]),
                ),
            )
            taxonomy_data = cast(dict[str, object], taxonomy["data"])
            industry_code = _optional_string(taxonomy_data.get("industry_code")) or index_code
            exact_memberships = [
                item
                for item in memberships_by_industry_source.get((industry_code, taxonomy_source), ())
                if cast(dict[str, object], item["data"]).get("effective_at_barrier") is True
            ]
            taxonomy_family = _optional_string(taxonomy_data.get("taxonomy_family"))
            family_memberships = (
                [
                    item
                    for item in memberships_by_industry_family.get(
                        (industry_code, taxonomy_family), ()
                    )
                    if cast(dict[str, object], item["data"]).get("effective_at_barrier") is True
                    and _optional_string(
                        cast(dict[str, object], item["data"]).get("taxonomy_source")
                    )
                    is None
                ]
                if taxonomy_family is not None
                else []
            )
            memberships = [*exact_memberships, *family_memberships]
            mismatched_memberships = [
                item
                for item in memberships_by_industry.get(industry_code, ())
                if _optional_string(cast(dict[str, object], item["data"]).get("taxonomy_source"))
                != taxonomy_source
                and cast(dict[str, object], item["data"]).get("effective_at_barrier") is True
            ]
            if mismatched_memberships:
                join_gaps.add("taxonomy_source_mismatch")
            if industry_code in membership_industries_without_source:
                join_gaps.add("taxonomy_version_unverified")
            if not memberships:
                continue
            gaps = {
                *cast(list[str], taxonomy["completeness_gaps"]),
                *cast(list[str], instrument["completeness_gaps"]),
            }
            for membership in memberships:
                gaps.update(cast(list[str], membership["completeness_gaps"]))
            gaps.discard("tradable_exposure_mapping_missing")
            gaps.discard("industry_to_tradable_mapping_missing")
            if not family_memberships:
                gaps.discard("taxonomy_version_unverified")
            gaps.discard("effective_taxonomy_interval_unverified")
            gaps.update({"taxonomy_effective_interval_unverified", "rebalance_lineage_missing"})
            if len(taxonomy_versions) > 1:
                gaps.add("taxonomy_versions_present")
            input_record_ids = {
                *cast(list[str], instrument["input_record_ids"]),
                *(cast(str, value["record_id"]) for value in taxonomy_versions),
                *(cast(str, item["record_id"]) for item in memberships),
            }
            values.append(
                {
                    "taxonomy_source": taxonomy_source,
                    "taxonomy_version": taxonomy_source,
                    "taxonomy_level": taxonomy_data.get("taxonomy_level"),
                    "industry_code": industry_code,
                    "industry_name": taxonomy_data.get("industry_name"),
                    "index_code": index_code,
                    "instrument_code": instrument["instrument_code"],
                    "mapping_basis": "etf_index_code_exact",
                    "observed_at_barrier": True,
                    "effective_at_barrier": None,
                    "constituent_count": len(memberships),
                    "input_record_ids": sorted(input_record_ids),
                    "completeness_gaps": sorted(gaps),
                }
            )

        instrument_code = cast(str, instrument["instrument_code"])
        basket_rows = basket_constituents_by_etf.get(instrument_code, ())
        matched_by_industry_source: dict[
            tuple[str, str], list[tuple[dict[str, object], dict[str, object], bool]]
        ] = {}
        for basket in basket_rows:
            basket_data = cast(dict[str, object], basket["data"])
            constituent_code = _optional_string(basket_data.get("constituent_code"))
            if constituent_code is None:
                continue
            for membership in memberships_by_instrument.get(constituent_code, ()):
                membership_data = cast(dict[str, object], membership["data"])
                if membership_data.get("effective_at_barrier") is not True:
                    continue
                industry_code = _optional_string(membership_data.get("industry_code"))
                taxonomy_family = _optional_string(membership_data.get("taxonomy_family"))
                taxonomy_source = _optional_string(membership_data.get("taxonomy_source"))
                if industry_code is None:
                    continue
                source_exact = taxonomy_source is not None
                identities: set[tuple[str, str]]
                if taxonomy_source is not None:
                    identities = {(industry_code, taxonomy_source)}
                elif taxonomy_family is not None:
                    identities = taxonomy_identities_by_index_family.get(
                        (industry_code, taxonomy_family), set()
                    )
                else:
                    identities = set()
                if len(identities) != 1:
                    if any(
                        taxonomy_code == industry_code
                        for taxonomy_code, _source in taxonomy_versions_by_index_source
                    ):
                        join_gaps.add(
                            "taxonomy_version_unverified"
                            if taxonomy_source is None
                            else "taxonomy_source_mismatch"
                        )
                    continue
                identity = next(iter(identities))
                if identity not in taxonomy_versions_by_index_source:
                    join_gaps.add("taxonomy_source_mismatch")
                    continue
                matched_by_industry_source.setdefault(identity, []).append(
                    (basket, membership, source_exact)
                )

        for (industry_code, taxonomy_source), matches in sorted(matched_by_industry_source.items()):
            taxonomy_versions = taxonomy_versions_by_index_source[(industry_code, taxonomy_source)]
            taxonomy = max(
                taxonomy_versions,
                key=lambda value: (
                    cast(str, cast(dict[str, object], value["times"])["authority_at"]),
                    cast(str, value["record_id"]),
                ),
            )
            taxonomy_data = cast(dict[str, object], taxonomy["data"])
            gaps = {
                *cast(list[str], taxonomy["completeness_gaps"]),
                *cast(list[str], instrument["completeness_gaps"]),
            }
            input_record_ids = {
                *cast(list[str], instrument["input_record_ids"]),
                *(cast(str, value["record_id"]) for value in taxonomy_versions),
            }
            constituent_codes: set[str] = set()
            used_family_join = False
            for basket, membership, source_exact in matches:
                gaps.update(cast(list[str], basket["completeness_gaps"]))
                gaps.update(cast(list[str], membership["completeness_gaps"]))
                used_family_join = used_family_join or not source_exact
                input_record_ids.add(cast(str, basket["record_id"]))
                input_record_ids.add(cast(str, membership["record_id"]))
                constituent_code = _optional_string(
                    cast(dict[str, object], basket["data"]).get("constituent_code")
                )
                if constituent_code is not None:
                    constituent_codes.add(constituent_code)
            gaps.discard("tradable_exposure_mapping_missing")
            gaps.discard("industry_to_tradable_mapping_missing")
            if not used_family_join:
                gaps.discard("taxonomy_version_unverified")
            gaps.discard("effective_taxonomy_interval_unverified")
            gaps.update({"taxonomy_effective_interval_unverified", "rebalance_lineage_missing"})
            if len(taxonomy_versions) > 1:
                gaps.add("taxonomy_versions_present")
            values.append(
                {
                    "taxonomy_source": taxonomy_source,
                    "taxonomy_version": taxonomy_source,
                    "taxonomy_level": taxonomy_data.get("taxonomy_level"),
                    "industry_code": industry_code,
                    "industry_name": taxonomy_data.get("industry_name"),
                    "index_code": taxonomy_data.get("index_code") or industry_code,
                    "instrument_code": instrument_code,
                    "mapping_basis": "daily_pcf_constituent_exact",
                    "observed_at_barrier": True,
                    "effective_at_barrier": None,
                    "constituent_count": len(constituent_codes),
                    "input_record_ids": sorted(input_record_ids),
                    "completeness_gaps": sorted(gaps),
                }
            )
    return (
        sorted(
            values,
            key=lambda item: (
                cast(str, item["industry_code"]),
                cast(str, item["instrument_code"]),
                cast(str, item["taxonomy_source"]),
                cast(str, item["mapping_basis"]),
            ),
        ),
        join_gaps,
    )


def _normalize_venue(value: object, *, code: str | None) -> str | None:
    venue = _optional_string(value)
    if venue is not None:
        normalized = _VENUE_ALIASES.get(venue.upper())
        if normalized is not None:
            return normalized
    if code is not None and code.endswith(".SH"):
        return "XSHG"
    if code is not None and code.endswith(".SZ"):
        return "XSHE"
    return None


def _unique_allowed(values: tuple[str, ...], allowed: frozenset[str], name: str) -> None:
    if not values or len(values) != len(set(values)) or any(item not in allowed for item in values):
        raise ValueError(f"checkpoint market universe {name} are invalid")


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _sortable_source_date(value: object) -> date:
    if not isinstance(value, str):
        return date.min
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    return date.min


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _prefixed_hash(value: object, prefix: str, name: str) -> str:
    text = _required_string(value, name)
    if re.fullmatch(rf"{re.escape(prefix)}[0-9a-f]{{64}}", text) is None:
        raise ValueError(f"{name} is invalid")
    return text


def _source_date(value: object, name: str) -> date:
    text = _required_string(value, name)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD") from exc


def _timestamp(value: object, name: str) -> datetime:
    text = _required_string(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a timestamp") from exc
    require_aware(parsed, name)
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{name} must use UTC")
    return parsed.astimezone(UTC)


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) or not item or item != item.strip() for item in items):
        raise ValueError(f"{name} must contain non-empty strings")
    result = cast(tuple[str, ...], tuple(items))
    if len(result) != len(set(result)):
        raise ValueError(f"{name} must be unique")
    return result


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    untyped = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in untyped):
        raise ValueError(f"{name} must use string keys")
    return cast(dict[str, object], untyped)


def _object_list(value: object, name: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return [_object(item, name) for item in cast(list[object], value)]


class _SchemaValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


def _schema_errors(value: object) -> list[str]:
    validator = _view_schema_validator()
    return sorted(error.message for error in validator.iter_errors(value))


@lru_cache(maxsize=1)
def _view_schema_validator() -> _SchemaValidator:
    module_root = Path(__file__).resolve().parent
    installed = module_root / "schemas" / "checkpoint-market-universe-view.schema.json"
    schema_path = (
        installed
        if installed.exists()
        else module_root.parents[1] / "schemas" / "checkpoint-market-universe-view.schema.json"
    )
    schema = _object(
        json.loads(schema_path.read_text(encoding="utf-8")),
        "checkpoint market universe schema",
    )
    return cast(
        _SchemaValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
