"""Harness-owned generic A-share qualification from captured source facts.

No symbol list grants eligibility. Qualification selects one supported venue/class
regime and writes a concrete immutable rule for existing account consumers.
"""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import RunJournal, RuntimeEvent
from market_impact_agent.streaming_nautilus_account import HistoricalInstrumentSpec

if TYPE_CHECKING:
    from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs


@dataclass(frozen=True)
class AShareSecurityQualification:
    symbol: str
    spec: HistoricalInstrumentSpec | None
    qualification_artifact_hash: str
    rule_artifact_hash: str | None
    source_record_hashes: tuple[str, ...]
    gaps: tuple[str, ...]

    @property
    def qualified(self) -> bool:
        return self.spec is not None and not self.gaps


@dataclass(frozen=True)
class SourceBackedAShareRulePolicy:
    """Reference to a root-signed Harness acceptance; raw source receipts are not approval."""

    acceptance_event_id: str
    policy_artifact_hash: str
    policy_id: str
    accepted_at: datetime
    effective_from: datetime
    effective_until: datetime | None
    source_artifact_hashes: tuple[str, ...]
    source_urls: tuple[str, ...]

    @classmethod
    def from_accepted_event(
        cls, store: LocalDataSnapshotStore, event_id: str
    ) -> SourceBackedAShareRulePolicy:
        journal = RunJournal.authoritative(store)
        event = journal.event(event_id)
        if event is None or event.event_type != "ashare.rule_policy.accepted":
            raise ValueError("signed A-share rule policy acceptance missing")
        digest = str(event.payload.get("policy_artifact_hash", ""))
        policy = cast(dict[str, Any], store.artifacts.read_json(digest))
        if (
            policy.get("schema_version") != "market-impact.ashare-rule-policy.v1"
            or policy.get("policy_id") != _POLICY_ID
            or policy.get("harness_authority_id") != store.harness_authority_id
            or policy.get("numeric_rules") != _NUMERIC_RULES
            or policy.get("synthetic_fee_policy") != _SYNTHETIC_FEE_POLICY
        ):
            raise ValueError("generic A-share accepted policy differs from registered revision")
        start = datetime.fromisoformat(policy["effective_from"])
        end = (
            None
            if policy["effective_until"] is None
            else datetime.fromisoformat(policy["effective_until"])
        )
        _validate_interval(start, end)
        sources = _source_receipts(store, tuple(policy["source_receipt_event_ids"]), event.run_id)
        if policy.get("sources") != sources or event.observed_at < max(
            datetime.fromisoformat(str(item["retrieved_at"])) for item in sources
        ):
            raise ValueError("accepted A-share policy source receipt binding changed")
        return cls(
            event_id,
            digest,
            _POLICY_ID,
            event.observed_at,
            start,
            end,
            tuple(str(item["raw_hash"]) for item in sources),
            tuple(str(item["url"]) for item in sources),
        )

    def assert_sources(self, inputs: HistoricalAShareInputs, cutoff: datetime) -> None:
        require_aware(cutoff, "qualification cutoff")
        reopened = self.from_accepted_event(inputs.store, self.acceptance_event_id)
        if reopened != self:
            raise ValueError("generic A-share policy fields differ from accepted authority")
        if cutoff < self.effective_from or (
            self.effective_until is not None and cutoff >= self.effective_until
        ):
            raise ValueError("generic A-share source policy is not effective at cutoff")


_POLICY_ID = "source-backed-ashare-normal-regime-2026-v1"
_NUMERIC_RULES = {
    "equity": {
        "lot_size": 100,
        "price_increment": "0.01",
        "price_limit_ratio": "0.10",
        "sell_stamp_tax_rate": "0.0005",
    },
    "exchange_traded_fund": {
        "lot_size": 100,
        "price_increment": "0.001",
        "price_limit_ratio": "0.10",
        "sell_stamp_tax_rate": "0",
    },
}
_SYNTHETIC_FEE_POLICY = {
    "policy_id": "synthetic-account-fee-policy-v1",
    "basis": "SYNTHETIC",
    "commission_rate": "0.0003",
    "minimum_commission": "5",
}
_SOURCE_URLS = frozenset(
    {
        "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml",
        "https://investor.szse.cn/lawrules/rule/trade/t20260424_620190.html",
        "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/10816482/files/959da0158c65434daa8a43a6e32be7ba.docx",
        "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/10816482/files/f6bd756cdd8248f49b7795f62e73237f.docx",
        "https://docs.static.szse.cn/www/lawrules/rule/trade/W020260424690713155663.pdf",
        "https://tushare.pro/document/2?doc_id=385",
        "https://tushare.pro/document/2?doc_id=19",
        "https://shanxi.chinatax.gov.cn/web/detail/sx-11400-545-1780448",
    }
)


def _validate_interval(start: datetime, end: datetime | None) -> None:
    require_aware(start, "rule policy effective_from")
    if end is not None:
        require_aware(end, "rule policy effective_until")
    if start != datetime(2026, 7, 5, 16, tzinfo=UTC) or (end is not None and end <= start):
        raise ValueError("2026 generic rule revision is not effective for this interval")


def _source_receipts(
    store: LocalDataSnapshotStore, ids: tuple[str, ...], run_id: str | None = None
) -> list[dict[str, object]]:
    journal = RunJournal.authoritative(store)
    events: dict[str, RuntimeEvent] = {
        event.event_id: event for event in (() if run_id is None else journal.events(run_id))
    }
    sources: list[dict[str, object]] = []
    for event_id in ids:
        event = events.get(event_id)
        if event is None:
            event = journal.event(event_id)
            if event is not None:
                events.update({item.event_id: item for item in journal.events(event.run_id)})
        if event is None or event.event_type != "research.public.received":
            raise ValueError("public source receipt missing")
        value = event.payload
        url = value.get("url")
        parsed = urlparse(str(url))
        if (
            parsed.scheme != "https"
            or parsed.hostname
            not in {
                "www.sse.com.cn",
                "investor.szse.cn",
                "docs.static.szse.cn",
                "www.szse.cn",
                "tushare.pro",
                "shanxi.chinatax.gov.cn",
            }
            or value.get("http_status") != 200
        ):
            raise ValueError("unsupported generic policy source receipt")
        received = datetime.fromisoformat(str(value["retrieved_at"]))
        require_aware(received, "source actual receipt")
        if received > event.observed_at:
            raise ValueError("source receipt time exceeds journal observation")
        raw = store.artifacts.get(str(value["raw_hash"]), media_type="application/octet-stream")
        if not raw.size_bytes:
            raise ValueError("empty rule source document")
        sources.append(dict(value))
    if not {str(item["url"]) for item in sources} >= _SOURCE_URLS:
        raise ValueError("generic A-share policy source coverage missing")
    return sources


def accept_ashare_rule_policy(
    *,
    store: LocalDataSnapshotStore,
    run_id: str,
    source_receipt_event_ids: tuple[str, ...],
    effective_from: datetime,
    effective_until: datetime | None,
    accepted_at: datetime,
) -> SourceBackedAShareRulePolicy:
    """Harness-only composition after review of the named effective source revision.

    This function is deliberately not an Agent tool. The root signs the fixed
    policy and explicit source bindings, using the existing journal signing key.
    """
    from market_impact_agent.agent_engine import (
        _PrivilegedEventSink,  # pyright: ignore[reportPrivateUsage]
    )

    _validate_interval(effective_from, effective_until)
    require_aware(accepted_at, "rule policy accepted_at")
    sources = _source_receipts(store, source_receipt_event_ids, run_id)
    if any(datetime.fromisoformat(str(item["retrieved_at"])) > accepted_at for item in sources):
        raise ValueError("rule policy cannot be accepted before source receipt")
    artifact = store.artifacts.put_json(
        {
            "schema_version": "market-impact.ashare-rule-policy.v1",
            "policy_id": _POLICY_ID,
            "harness_authority_id": store.harness_authority_id,
            "effective_from": effective_from.isoformat(),
            "effective_until": None if effective_until is None else effective_until.isoformat(),
            "source_receipt_event_ids": list(source_receipt_event_ids),
            "sources": sources,
            "numeric_rules": _NUMERIC_RULES,
            "synthetic_fee_policy": _SYNTHETIC_FEE_POLICY,
        }
    )
    journal = RunJournal.authoritative(store)
    event_id = "ashare-rule-policy-accepted-" + artifact.content_hash
    if journal.event(event_id) is None:
        key_path = store.root / ".harness-event-hmac.key"
        if key_path.is_symlink() or not key_path.is_file():
            raise ValueError("Harness signing key unavailable")
        key = key_path.read_bytes()
        sink = _PrivilegedEventSink(
            journal=journal,
            authority_id=store.harness_authority_id,
            signer=lambda payload: hmac.new(key, payload, sha256).hexdigest(),
        )
        sink.append(
            run_id=run_id,
            event_id=event_id,
            event_type="ashare.rule_policy.accepted",
            observed_at=accepted_at,
            payload={"policy_artifact_hash": artifact.content_hash},
        )
    return SourceBackedAShareRulePolicy.from_accepted_event(store, event_id)


def qualify_ashare_security(
    inputs: HistoricalAShareInputs,
    symbol: str,
    cutoff: datetime,
    policy: SourceBackedAShareRulePolicy,
    *,
    historical: bool = False,
) -> AShareSecurityQualification:
    """Resolve identity/regime only; quotes, halts and actions remain session gates."""
    require_aware(cutoff, "qualification cutoff")
    gaps: set[str] = set()
    hashes: set[str] = set()
    spec = None
    rule_hash = None
    instrument_class: str | None = None
    try:
        policy.assert_sources(inputs, cutoff)
        hashes.update(policy.source_artifact_hashes)
        hashes.add(policy.policy_artifact_hash)
        if not historical and policy.accepted_at > cutoff:
            gaps.add("generic_rule_policy_accepted_after_cutoff")
    except (ValueError, FileNotFoundError, KeyError):
        gaps.add("generic_rule_source_authority_unverified")
    valid_symbol = len(symbol) == 9 and symbol[:6].isdigit() and symbol[6:] in {".SH", ".SZ"}
    if not valid_symbol:
        gaps.add("security_identity_unverified")
    else:
        # Verification of CAS/config/normalized rows belongs to the existing reader.
        tables = inputs._tables()  # pyright: ignore[reportPrivateUsage]
        observed: dict[str, datetime] = {}

        def identity_rows(api: str) -> tuple[tuple[Mapping[str, Any], str], ...]:
            candidates: list[tuple[Mapping[str, Any], str, datetime]] = []
            for table in tables:
                if table.api != api:
                    continue
                receipt_times = {
                    item.raw_content_hash: item.times.retrieved_at
                    for item in table.snapshot.observations
                }
                candidates.extend(
                    (row, digest, receipt_times[digest])
                    for row, digest in table.rows
                    if row.get("ts_code") == symbol
                )
            visible = [item for item in candidates if item[2] <= cutoff]
            chosen = visible or candidates
            if not chosen:
                return ()
            latest = max(item[2] for item in chosen)
            distinct: dict[str, tuple[Mapping[str, Any], str]] = {}
            for row, digest, at in chosen:
                if at == latest:
                    observed[digest] = at
                    distinct[digest] = row, digest
            return tuple(distinct.values())

        stock = identity_rows("stock_basic")
        etf = identity_rows("etf_basic")
        if bool(stock) == bool(etf) or len(stock or etf) != 1:
            gaps.add("security_identity_missing_or_conflicting")
        else:
            row, digest = (stock or etf)[0]
            hashes.add(digest)
            if observed[digest] > cutoff:
                gaps.add(
                    "historical_regime_not_point_in_time"
                    if historical
                    else "security_identity_received_after_cutoff"
                )
            venue_codes = (
                {"SSE", "SH", "XSHG"} if symbol.endswith(".SH") else {"SZSE", "SZ", "XSHE"}
            )
            if row.get("exchange") not in venue_codes:
                gaps.add("security_exchange_identity_mismatch")
            listed = str(row.get("list_date") or "")
            try:
                listed_at = datetime.strptime(listed, "%Y%m%d").date()
                if listed_at > cutoff.date():
                    gaps.add("security_not_yet_listed")
            except ValueError:
                gaps.add("listing_date_unverified")
            if row.get("list_status") != "L":
                gaps.add("listing_status_unverified")
            if stock:
                instrument_class = "equity"
                prefixes = (
                    ("600", "601", "603", "605")
                    if symbol.endswith(".SH")
                    else ("000", "001", "002", "003")
                )
                if not symbol.startswith(prefixes):
                    gaps.add("equity_board_unaccepted")
                name = str(row.get("name") or "").upper()
                if not name or any(marker in name for marker in ("ST", "退")):
                    gaps.add("equity_special_regime_unaccepted")
            else:
                instrument_class = "exchange_traded_fund"
                funds = identity_rows("fund_basic")
                if row.get("etf_type") != "境内" or not row.get("index_code"):
                    gaps.add("etf_domestic_tracker_identity_unverified")
                if len(funds) != 1:
                    gaps.add("etf_equity_class_source_missing_or_conflicting")
                else:
                    fund, fund_hash = funds[0]
                    hashes.add(fund_hash)
                    if fund.get("fund_type") != "股票型":
                        gaps.add("etf_asset_class_unaccepted")
                    if fund.get("market") != "E" or fund.get("status") != "L":
                        gaps.add("etf_fund_listing_identity_unverified")
                    if fund.get("list_date") != row.get("list_date"):
                        gaps.add("etf_listing_date_sources_conflict")
                    if observed[fund_hash] > cutoff:
                        gaps.add(
                            "historical_regime_not_point_in_time"
                            if historical
                            else "etf_class_received_after_cutoff"
                        )
    if not gaps and instrument_class is not None:
        is_etf = instrument_class == "exchange_traded_fund"
        rule: dict[str, Any] = {
            "schema_version": "market-impact.ashare-qualified-rule.v1",
            "version": policy.policy_id,
            "symbol": symbol,
            "instrument_class": instrument_class,
            "effective_from": policy.effective_from.isoformat(),
            "effective_until": None
            if policy.effective_until is None
            else policy.effective_until.isoformat(),
            "regime_basis": "static_identity_requires_reported_session_regime_validation",
            "policy_artifact_hash": policy.policy_artifact_hash,
            "source_artifact_hash": policy.source_artifact_hashes[0],
            "source_url": policy.source_urls[0],
            "qualification_source_record_hashes": sorted(hashes),
            "lot_size": 100,
            "price_increment": "0.001" if is_etf else "0.01",
            "price_limit_ratio": "0.10",
            "commission_rate": "0.0003",
            "minimum_commission": "5",
            "sell_stamp_tax_rate": "0" if is_etf else "0.0005",
            "commission_basis": "synthetic-account-fee-policy-v1",
        }
        rule_hash = inputs.store.artifacts.put_json(rule).content_hash
        hashes.add(rule_hash)
        spec = HistoricalInstrumentSpec(
            target_id=symbol,
            instrument_class=instrument_class,
            source_ref="sha256:" + rule_hash,
            price_increment=Decimal(cast(str, rule["price_increment"])),
            lot_size=100,
            price_limit_ratio=Decimal("0.10"),
            commission_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            sell_stamp_tax_rate=Decimal("0" if is_etf else "0.0005"),
        )
    artifact = inputs.store.artifacts.put_json(
        {
            "schema_version": "market-impact.ashare-security-qualification.v1",
            "policy_id": policy.policy_id,
            "symbol": symbol,
            "cutoff": cutoff.isoformat(),
            "lane": "modeled_historical" if historical else "actual_receipt",
            "instrument_class": instrument_class,
            "rule_artifact_hash": rule_hash,
            "source_record_hashes": sorted(hashes),
            "gaps": sorted(gaps),
        }
    )
    return AShareSecurityQualification(
        symbol, spec, artifact.content_hash, rule_hash, tuple(sorted(hashes)), tuple(sorted(gaps))
    )
