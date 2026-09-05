"""Reopen captured Tushare evidence for explicitly modeled historical simulation.

Actual receipt times are never rewritten. This read-only adapter projects historical
row dates under a named modeled policy; it does not mint StrictPIT DataSnapshots.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from threading import Lock
from types import MappingProxyType
from typing import Any, cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.dynamic_ashare_admission import HistoricalSecurityEvidence
from market_impact_agent.nautilus_backtest import AShareDailyBar
from market_impact_agent.sse_fund_suspension import (
    VerifiedFundSuspensions,
    read_sse_fund_suspensions,
)
from market_impact_agent.streaming_nautilus_account import (
    HistoricalCorporateAction,
    HistoricalInstrumentSpec,
)
from market_impact_agent.tushare_observation import tushare_observation_source_from_dict

_SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class ModeledHistoricalPolicy:
    policy_id: str
    daily_open_volume_fraction: Decimal
    lane: str = "modeled_pit"
    opening_tick_validity_microseconds: int = 1
    limit_basis: str = "reported_stk_limit"

    def to_dict(self) -> dict[str, object]:
        """Preserve legacy serialized identities when the new basis is not selected."""
        result: dict[str, object] = {
            "policy_id": self.policy_id,
            "daily_open_volume_fraction": str(self.daily_open_volume_fraction),
            "lane": self.lane,
            "opening_tick_validity_microseconds": self.opening_tick_validity_microseconds,
        }
        if self.limit_basis != "reported_stk_limit":
            result["limit_basis"] = self.limit_basis
        return result

    def __post_init__(self) -> None:
        if self.limit_basis not in {"reported_stk_limit", "qualified_seed_etf_exchange_rule_v1"}:
            raise ValueError("unsupported modeled historical limit basis")
        if self.opening_tick_validity_microseconds != 1:
            raise ValueError("modeled venue facts cover exactly the opening tick")
        if not self.policy_id or self.lane != "modeled_pit":
            raise ValueError("an explicit modeled-PIT policy is required; StrictPIT is unsupported")
        if (
            not self.daily_open_volume_fraction.is_finite()
            or not 0 < self.daily_open_volume_fraction <= Decimal("0.01")
        ):
            raise ValueError(
                "daily opening liquidity proxy must be positive and at most 1% of volume"
            )


@dataclass(frozen=True)
class HistoricalSessionInputs:
    spec: HistoricalInstrumentSpec | None
    bar: AShareDailyBar | None
    corporate_actions: tuple[HistoricalCorporateAction, ...]
    source_record_hashes: tuple[str, ...]
    gaps: tuple[str, ...]
    policy_id: str
    price_basis: str = "raw_unadjusted"
    liquidity_basis: str = "modeled_fraction_of_reported_daily_volume_not_observed_open_book"

    limit_diagnostics: Mapping[str, object] | None = None

    @property
    def execution_ready(self) -> bool:
        return self.spec is not None and self.bar is not None and not self.gaps


@dataclass(frozen=True)
class _Table:
    api: str
    snapshot: DataSnapshot
    rows: tuple[tuple[dict[str, Any], str], ...]
    hashes: tuple[str, ...]


class HistoricalAShareInputs:
    """Use only explicit immutable snapshot/rule references from this Harness root.

    Rule artifacts have schema ``market-impact.historical-ashare-rule.v1`` and
    carry symbol, effective_from/until, version, instrument_class, source_artifact_hash,
    source_url, lot_size, price_increment, price_limit_ratio and the three fee fields.
    They are Harness-reviewed effective-dated normalizations of source evidence;
    current stock_basic/etf_basic classification is not their replacement.
    """

    def __init__(
        self,
        *,
        store: LocalDataSnapshotStore,
        snapshot_ids: tuple[str, ...],
        rule_artifact_hashes: tuple[str, ...],
        policy: ModeledHistoricalPolicy,
        fund_halt_artifact_hashes: tuple[str, ...] = (),
    ) -> None:
        self.store = store
        self.snapshot_ids = tuple(dict.fromkeys(snapshot_ids))
        self.rule_artifact_hashes = rule_artifact_hashes
        self.policy = policy
        self.fund_halt_artifact_hashes = tuple(dict.fromkeys(fund_halt_artifact_hashes))
        self._verified_tables: tuple[_Table, ...] | None = None
        self._table_lock = Lock()
        self._fund_halt_lock = Lock()
        self._verified_fund_halts: VerifiedFundSuspensions | None = None
        self._row_index: Mapping[
            tuple[str, str | None], tuple[tuple[Mapping[str, Any], str], ...]
        ] = MappingProxyType({})

    def with_snapshots(self, snapshot_ids: tuple[str, ...]) -> HistoricalAShareInputs:
        return HistoricalAShareInputs(
            store=self.store,
            snapshot_ids=self.snapshot_ids + snapshot_ids,
            rule_artifact_hashes=self.rule_artifact_hashes,
            policy=self.policy,
            fund_halt_artifact_hashes=self.fund_halt_artifact_hashes,
        )

    def _fund_halt(
        self,
        symbol: str,
        session: date,
        halted: bool | None,
        gaps: set[str],
        hashes: set[str],
    ) -> bool | None:
        if not self.fund_halt_artifact_hashes:
            return halted
        with self._fund_halt_lock:
            if self._verified_fund_halts is None:
                self._verified_fund_halts = read_sse_fund_suspensions(
                    store=self.store, artifact_hashes=self.fund_halt_artifact_hashes
                )
            verified = self._verified_fund_halts
        evidence = verified.project(symbol=symbol, session=session)
        hashes.update(evidence.source_record_hashes)
        gaps.update(evidence.gaps)
        if evidence.halted is False:
            gaps.discard("halt_status_unverified")
            if halted is True:
                gaps.add("fund_halt_sources_conflict")
                return True
            return False
        return True if halted is True else None

    def research_query_gap(
        self, api: str, arguments: Mapping[str, object], cutoff: datetime
    ) -> str | None:
        """Authorize only dated raw price research through a completed registered session."""
        if api not in {"daily", "fund_daily"}:
            return "historical_source_not_projectable"
        symbol = arguments.get("ts_code")
        if (
            not isinstance(symbol, str)
            or len(symbol) != 9
            or not symbol[:6].isdigit()
            or symbol[6:] not in {".SH", ".SZ"}
        ):
            return "historical_symbol_invalid"
        exchange = "SSE" if symbol.endswith(".SH") else "SZSE"
        completed = [
            _day(row["cal_date"])
            for row, _ in self._rows("trade_cal", None)
            if row.get("exchange") == exchange
            and str(row.get("is_open")) == "1"
            and _at(_day(row["cal_date"]), time(15)) < cutoff
        ]
        if not completed:
            return "historical_completed_session_missing"
        try:
            start, end = _day(arguments["start_date"]), _day(arguments["end_date"])
        except (KeyError, ValueError, TypeError):
            return "historical_price_window_invalid"
        if start > end or end > max(completed):
            return "historical_price_window_after_completed_session"
        return None

    def research_series(
        self, symbol: str, cutoff: datetime, *, limit: int = 61
    ) -> dict[str, object]:
        """Reopen past price facts without requiring trading eligibility.

        Adjustment uses only factors dated through the last completed session;
        the original price remains explicit and is the sole execution price.
        Receipt times retain their actual values in this modeled historical lane.
        """
        require_aware(cutoff, "research series cutoff")
        if not 1 <= limit <= 252:
            raise ValueError("research series must request 1..252 completed sessions")
        tables = self._tables()
        rows: dict[date, tuple[dict[str, Any], str]] = {}
        factors: dict[date, tuple[dict[str, Any], str]] = {}
        for table in tables:
            if table.api not in {"daily", "fund_daily", "adj_factor", "fund_adj"}:
                continue
            target = factors if table.api in {"adj_factor", "fund_adj"} else rows
            for row, digest in table.rows:
                if row.get("ts_code") != symbol:
                    continue
                day = _day(row["trade_date"])
                if _at(day, time(15)) > cutoff:
                    continue
                if day in target and target[day][0] != row:
                    raise ValueError("research series contains unresolved source revisions")
                target[day] = row, digest
        selected = sorted(rows)[-limit:]
        base = None if not selected else factors.get(selected[-1])
        base_factor = None if base is None else _decimal(base[0].get("adj_factor"))
        values: list[dict[str, object]] = []
        for day in selected:
            row, digest = rows[day]
            factor = factors.get(day)
            factor_value = None if factor is None else _decimal(factor[0].get("adj_factor"))
            close = _decimal(row.get("close"))
            adjusted = (
                close * factor_value / base_factor
                if close is not None and factor_value is not None and base_factor
                else None
            )
            values.append(
                {
                    "trade_date": day.isoformat(),
                    "raw_close": None if close is None else str(close),
                    "cutoff_adjusted_close": None if adjusted is None else str(adjusted),
                    "volume_lots": row.get("vol"),
                    "amount_thousand_cny": row.get("amount"),
                    "source_record_hash": digest,
                    "factor_record_hash": None if factor is None else factor[1],
                }
            )
        return {
            "symbol": symbol,
            "cutoff": cutoff.isoformat(),
            "pit_lane": self.policy.lane,
            "policy_id": self.policy.policy_id,
            "strict_pit_accepted": False,
            "rows": values,
            "adjustment_base_record_hash": None if base is None else base[1],
            "gaps": ["raw_price_history_missing"]
            if not values
            else ["adjustment_factor_history_incomplete"]
            if any(row["cutoff_adjusted_close"] is None for row in values)
            else [],
            "source_snapshots": list(self.snapshot_ids),
        }

    def _tables(self) -> tuple[_Table, ...]:
        # One verified immutable graph per frozen source binding. New bindings
        # (including with_snapshots(())) independently recheck every CAS artifact.
        with self._table_lock:
            if self._verified_tables is None:
                tables = self._read_tables()
                self._row_index = _index_rows(tables)
                self._verified_tables = tables
            return self._verified_tables

    def _rows(self, api: str, symbol: str | None) -> tuple[tuple[Mapping[str, Any], str], ...]:
        self._tables()
        return self._row_index.get((api, symbol), ())

    def _one(self, api: str, symbol: str, day: date) -> tuple[Mapping[str, Any], str] | None:
        day_text = day.strftime("%Y%m%d")
        rows = [pair for pair in self._rows(api, symbol) if pair[0].get("trade_date") == day_text]
        if len(rows) > 1:
            raise ValueError(f"conflicting captured historical rows: {api}/{symbol}/{day}")
        return rows[0] if rows else None

    def _read_tables(self) -> tuple[_Table, ...]:
        tables: list[_Table] = []
        for snapshot_id in self.snapshot_ids:
            snapshot = self.store.get(snapshot_id)
            for binding, attempt in zip(snapshot.query.sources, snapshot.attempts, strict=True):
                if (
                    not attempt.status.completed
                    or attempt.raw_response_hash is None
                    or attempt.accepted_count != attempt.received_count
                ):
                    continue
                if binding.source_config_hash is None:
                    raise ValueError("historical source requires immutable source configuration")
                config = tushare_observation_source_from_dict(
                    self.store.artifacts.read_json(binding.source_config_hash)
                )
                if config.source_id != binding.upstream_source:
                    raise ValueError("source configuration identity mismatch")
                self.store.artifacts.get(
                    attempt.raw_response_hash, media_type="application/octet-stream"
                )
                rows: list[tuple[dict[str, Any], str]] = []
                hashes = {binding.source_config_hash, attempt.raw_response_hash}
                for observation in snapshot.observations:
                    if observation.upstream_source != binding.upstream_source:
                        continue
                    raw = cast(
                        dict[str, Any], self.store.artifacts.read_json(observation.raw_content_hash)
                    )
                    if raw.get("fields") != list(config.fields):
                        raise ValueError(
                            "raw observation fields differ from captured source contract"
                        )
                    row = dict(zip(config.fields, raw["values"], strict=True))
                    normalized = observation.normalized_payload
                    if (
                        normalized.get("api_name") != config.api_name
                        or normalized.get("record") != row
                    ):
                        raise ValueError(
                            "normalized historical observation differs from raw CAS record"
                        )
                    rows.append((row, observation.raw_content_hash))
                    hashes.add(observation.raw_content_hash)
                tables.append(_Table(config.api_name, snapshot, tuple(rows), tuple(sorted(hashes))))
        return tuple(tables)

    def _rule(
        self, symbol: str, cutoff: datetime
    ) -> tuple[HistoricalInstrumentSpec | None, tuple[str, ...]]:
        matches: list[tuple[HistoricalInstrumentSpec, tuple[str, ...]]] = []
        for artifact_hash in self.rule_artifact_hashes:
            rule = cast(dict[str, Any], self.store.artifacts.read_json(artifact_hash))
            if rule.get("schema_version") != "market-impact.historical-ashare-rule.v1":
                raise ValueError("unsupported historical trading rule schema")
            if rule["symbol"] != symbol:
                continue
            start, end = (
                datetime.fromisoformat(rule["effective_from"]),
                datetime.fromisoformat(rule["effective_until"]),
            )
            require_aware(start, "historical rule effective_from")
            require_aware(end, "historical rule effective_until")
            if not start <= cutoff < end:
                continue
            source_hash = rule["source_artifact_hash"]
            self.store.artifacts.get(source_hash, media_type="application/octet-stream")
            if not rule.get("version") or not str(rule.get("source_url", "")).startswith(
                "https://"
            ):
                raise ValueError("effective-dated trading rule requires a version and source URL")
            spec = HistoricalInstrumentSpec(
                target_id=symbol,
                instrument_class=rule["instrument_class"],
                source_ref="sha256:" + artifact_hash,
                price_increment=Decimal(rule["price_increment"]),
                lot_size=rule["lot_size"],
                price_limit_ratio=Decimal(rule["price_limit_ratio"]),
                commission_rate=Decimal(rule["commission_rate"]),
                minimum_commission=Decimal(rule["minimum_commission"]),
                sell_stamp_tax_rate=Decimal(rule["sell_stamp_tax_rate"]),
            )
            matches.append((spec, (artifact_hash, source_hash)))
        if len(matches) > 1:
            raise ValueError("overlapping historical trading rule versions")
        return matches[0] if matches else (None, ())

    def instrument_spec(self, symbol: str, cutoff: datetime) -> HistoricalInstrumentSpec | None:
        """Reopen only the effective source-backed trading/fee rule, never outcome bars."""
        return self._rule(symbol, cutoff)[0]

    def _qualified_limits(
        self,
        symbol: str,
        day: date,
        spec: HistoricalInstrumentSpec,
        gaps: set[str],
        hashes: set[str],
    ) -> tuple[Decimal | None, Decimal | None, Mapping[str, object]]:
        """One modeled reference for admission and the existing execution formula.

        Today's pre_close is a qualification check only. The reference is the raw
        close of the exact preceding calendar session; today's OHLC is never read.
        """
        if (
            symbol not in {"510300.SH", "510500.SH"}
            or spec.instrument_class != "exchange_traded_fund"
            or spec.price_increment != Decimal("0.001")
            or spec.price_limit_ratio != Decimal("0.1")
        ):
            gaps.add("qualified_limit_instrument_or_regime_unsupported")
        rule = cast(
            dict[str, Any], self.store.artifacts.read_json(spec.source_ref.removeprefix("sha256:"))
        )
        raw_qualification = rule.get("qualified_limit_reference")
        qualification = (
            cast(dict[str, object], raw_qualification)
            if isinstance(raw_qualification, dict)
            else {}
        )
        if (
            qualification.get("schema_version")
            != "market-impact.qualified-seed-etf-limit-reference.v1"
            or qualification.get("normal_session_assumption") is not True
            or qualification.get("domestic_equity_etf") is not True
            or not str(qualification.get("identity_source_url", "")).startswith("https://")
            or not qualification.get("identity_source_artifact_hash")
            or not qualification.get("listing_date")
        ):
            gaps.add("qualified_limit_identity_and_normal_session_basis_missing")
        else:
            identity_hash = str(qualification["identity_source_artifact_hash"])
            self.store.artifacts.get(identity_hash, media_type="application/octet-stream")
            hashes.add(identity_hash)
            listing = date.fromisoformat(str(qualification["listing_date"]))
            basics = self._rows("etf_basic", symbol)
            if (
                listing >= day
                or len(basics) != 1
                or not basics[0][0].get("list_date")
                or _day(basics[0][0]["list_date"]) != listing
            ):
                gaps.add("qualified_limit_historical_listing_mismatch")
        calendar = [
            pair
            for pair in self._rows("trade_cal", None)
            if pair[0].get("exchange") == "SSE"
            and pair[0].get("cal_date") == day.strftime("%Y%m%d")
        ]
        prior_day = None
        if len(calendar) == 1 and str(calendar[0][0].get("is_open")) == "1":
            hashes.add(calendar[0][1])
            if calendar[0][0].get("pretrade_date"):
                prior_day = _day(calendar[0][0]["pretrade_date"])
        if prior_day is None or prior_day >= day:
            gaps.add("qualified_limit_prior_calendar_session_missing")
            prior_day = None
        prior = self._one("fund_daily", symbol, prior_day) if prior_day else None
        current = self._one("fund_daily", symbol, day)
        reference = _decimal(prior[0].get("close")) if prior else None
        declared = _decimal(current[0].get("pre_close")) if current else None
        if prior:
            hashes.add(prior[1])
        # The current record is provenance of the pre_close qualification, not an
        # Agent-visible outcome observation. No current OHLC fields enter diagnostics.
        if current:
            hashes.add(current[1])
        tick = spec.price_increment
        if reference is None or reference <= 0 or reference % tick != 0:
            gaps.add("qualified_limit_prior_raw_close_invalid")
            reference = None
        if declared is None or declared <= 0 or declared % tick != 0:
            gaps.add("qualified_limit_session_pre_close_invalid")
        if reference is not None and declared is not None and reference != declared:
            gaps.add("qualified_limit_prior_close_pre_close_mismatch")
        factors = (
            self._one("fund_adj", symbol, prior_day) if prior_day else None,
            self._one("fund_adj", symbol, day),
        )
        values = [_decimal(pair[0].get("adj_factor")) if pair else None for pair in factors]
        hashes.update(pair[1] for pair in factors if pair)
        if any(value is None or value <= 0 for value in values):
            gaps.add("qualified_limit_factor_coverage_invalid")
        elif values[0] != values[1]:
            gaps.add("qualified_limit_factor_discontinuity")
        if not _complete_scope(self._tables(), "fund_div", symbol, day, full_symbol=True):
            gaps.add("corporate_action_event_coverage_missing")
        for row, digest in self._rows("fund_div", symbol):
            # Exclude reference-changing sessions even when capture announcement
            # dating is incomplete or later revised. No cash settlement exception.
            ex = _day(row["ex_date"]) if row.get("ex_date") else None
            pay = _day(row["pay_date"]) if row.get("pay_date") else None
            if ex is None or pay is None:
                gaps.add("qualified_limit_corporate_action_dates_unverified")
                hashes.add(digest)
            elif ex <= day <= max(ex, pay):
                gaps.add("qualified_limit_corporate_action_reference_excluded")
                hashes.add(digest)
        lower = upper = None
        if reference is not None:
            lower = (reference * (1 - spec.price_limit_ratio) / tick).quantize(
                Decimal(1), rounding="ROUND_HALF_UP"
            ) * tick
            upper = (reference * (1 + spec.price_limit_ratio) / tick).quantize(
                Decimal(1), rounding="ROUND_HALF_UP"
            ) * tick
        reported = self._one("stk_limit", symbol, day)
        if reported:
            hashes.add(reported[1])
        reported_lower = _decimal(reported[0].get("down_limit")) if reported else None
        reported_upper = _decimal(reported[0].get("up_limit")) if reported else None
        reported_prior = _decimal(reported[0].get("pre_close")) if reported else None
        diagnostics: dict[str, object] = {
            "limit_basis": self.policy.limit_basis,
            "pit_lane": "modeled_pit",
            "strict_pit_accepted": False,
            "normal_session_assumption": True,
            "normal_session_assumption_scope": "seed_domestic_equity_etf_no_known_exception",
            "reference_basis": "exact_prior_calendar_session_raw_close",
            "prior_session": prior_day.isoformat() if prior_day else None,
            "reference_close": str(reference) if reference is not None else None,
            "derived_lower_limit": str(lower) if lower is not None else None,
            "derived_upper_limit": str(upper) if upper is not None else None,
            "reported_source_record_hash": reported[1] if reported else None,
            "reported_pre_close": str(reported_prior) if reported_prior is not None else None,
            "reported_lower_limit": str(reported_lower) if reported_lower is not None else None,
            "reported_upper_limit": str(reported_upper) if reported_upper is not None else None,
            "reported_minus_derived_lower": str(reported_lower - lower)
            if reported_lower is not None and lower is not None
            else None,
            "reported_minus_derived_upper": str(reported_upper - upper)
            if reported_upper is not None and upper is not None
            else None,
            "comparison_status": "agrees"
            if reported_lower == lower
            and reported_upper == upper
            and lower is not None
            and upper is not None
            and reported_prior in {None, reference}
            else "unresolved_reported_comparison",
        }
        return lower, upper, MappingProxyType(diagnostics)

    def reopen_security(self, symbol: str, cutoff: datetime) -> HistoricalSecurityEvidence | None:
        """Preopen view: prior completed close/volume plus modeled 09:00 venue facts.

        session() is executor-only and may include the entire future trading day.
        This method never exposes that day's closing price or daily volume.
        """
        require_aware(cutoff, "historical security cutoff")
        day = cutoff.astimezone(_SHANGHAI).date()
        spec, rule_hashes = self._rule(symbol, cutoff)
        if spec is None:
            return None
        tables = self._tables()
        api = "fund_daily" if spec.instrument_class == "exchange_traded_fund" else "daily"
        visible = [
            (row, digest)
            for row, digest in self._rows(api, symbol)
            if _at(_day(row["trade_date"]), time(15)) < cutoff and _day(row["trade_date"]) < day
        ]
        visible.sort(key=lambda pair: str(pair[0]["trade_date"]))
        raw = visible[-1][0] if visible else None
        if raw is not None:
            self._one(api, symbol, _day(raw["trade_date"]))
        hashes = set(rule_hashes)
        gaps: set[str] = set()
        if visible:
            hashes.add(visible[-1][1])
        else:
            gaps.add("prior_completed_raw_close_missing")
        # Publication-time assumptions are part of the explicit modeled policy,
        # never changes to captured actual receipt or retrospective StrictPIT.
        if not _at(day, time(9)) <= cutoff < _at(day, time(9, 30)):
            gaps.add("outside_modeled_preopen_venue_fact_window")
        calendar = [
            (row, digest)
            for row, digest in self._rows("trade_cal", None)
            if row.get("cal_date") == day.strftime("%Y%m%d")
            and row.get("exchange") == ("SSE" if symbol.endswith(".SH") else "SZSE")
        ]
        if len(calendar) != 1 or str(calendar[0][0].get("is_open")) != "1":
            gaps.add("trading_calendar_session_unverified")
        elif raw is None or calendar[0][0].get("pretrade_date") != raw.get("trade_date"):
            gaps.add("prior_close_not_previous_trading_session")
        else:
            hashes.add(calendar[0][1])
        basic_rows = self._rows("etf_basic" if api == "fund_daily" else "stock_basic", symbol)
        if len(basic_rows) != 1:
            gaps.add("historical_listing_identity_missing_or_conflicting")
        else:
            basic, digest = basic_rows[0]
            hashes.add(digest)
            listed = _day(basic["list_date"]) if basic.get("list_date") else None
            delisted = _day(basic["delist_date"]) if basic.get("delist_date") else None
            if listed is None or listed > day or (delisted is not None and delisted <= day):
                gaps.add("security_not_listed_in_session")
            if basic.get("exchange") not in (
                {"SSE", "SH", "XSHG"} if symbol.endswith(".SH") else {"SZSE", "SZ", "XSHE"}
            ):
                gaps.add("historical_listing_exchange_mismatch")
        halts = [
            (row, digest)
            for row, digest in self._rows("suspend_d", symbol)
            if row.get("trade_date") == day.strftime("%Y%m%d")
        ]
        halted = None
        if any(row.get("suspend_timing") for row, _ in halts):
            gaps.add("intraday_halt_not_preopen_authority")
        if any(
            row.get("suspend_type") == "S" and not row.get("suspend_timing") for row, _ in halts
        ):
            halted = True
        elif any(
            row.get("suspend_type") == "R" and not row.get("suspend_timing") for row, _ in halts
        ) or (api == "daily" and _complete_scope(tables, "suspend_d", symbol, day)):
            halted = False
        else:
            gaps.add("halt_status_unverified")
        hashes.update(digest for _, digest in halts)
        if api == "fund_daily":
            halted = self._fund_halt(symbol, day, halted, gaps, hashes)
        limits = self._one("stk_limit", symbol, day)
        lower = _decimal(limits[0].get("down_limit")) if limits else None
        upper = _decimal(limits[0].get("up_limit")) if limits else None
        if limits:
            hashes.add(limits[1])
        else:
            gaps.add("daily_limits_unverified")
        limit_diagnostics = None
        if self.policy.limit_basis == "qualified_seed_etf_exchange_rule_v1":
            gaps.discard("daily_limits_unverified")
            lower, upper, limit_diagnostics = self._qualified_limits(
                symbol, day, spec, gaps, hashes
            )
        action_api = "fund_div" if api == "fund_daily" else "dividend"
        if not _complete_scope(tables, action_api, symbol, day, full_symbol=True):
            gaps.add("corporate_action_event_coverage_missing")
        for row, digest in self._rows(action_api, symbol):
            if row.get("ann_date") and _day(row["ann_date"]) > day:
                continue
            ex = _day(row["ex_date"]) if row.get("ex_date") else None
            pay = _day(row["pay_date"]) if row.get("pay_date") else None
            if ex is not None and pay is not None and ex <= day <= pay:
                gaps.add("corporate_action_pending_settlement")
                hashes.add(digest)
        for table in tables:
            if table.api in {"trade_cal", "suspend_d", "stk_limit", action_api}:
                hashes.update(table.hashes)
        amount = _decimal(raw.get("amount")) if raw else None
        return HistoricalSecurityEvidence(
            symbol=symbol,
            venue=spec.venue,
            instrument_class=spec.instrument_class,
            cutoff=cutoff,
            effective_from=_at(day, time(9)),
            effective_until=_at(
                day, time(9, 30, 0, self.policy.opening_tick_validity_microseconds)
            ),
            raw_price=_decimal(raw.get("close")) if raw else None,
            raw_price_observed_at=_at(_day(raw["trade_date"]), time(15)) if raw else None,
            halted=halted,
            lower_limit=lower,
            upper_limit=upper,
            turnover=amount * 1000 if amount is not None else None,
            corporate_action_status=(
                "modeled_normal_session_assumption" if limit_diagnostics is not None else "none"
            )
            if not any("corporate_action" in gap for gap in gaps)
            else None,
            buy_lot_size=spec.lot_size,
            price_tick=spec.price_increment,
            source_record_hashes=tuple(sorted(hashes)),
            gaps=tuple(sorted(gaps)),
            limit_diagnostics=limit_diagnostics,
        )

    def session(self, symbol: str, session: date) -> HistoricalSessionInputs:
        if len(symbol) != 9 or not symbol[:6].isdigit() or symbol[6:] not in {".SH", ".SZ"}:
            raise ValueError("historical inputs require a concrete Tushare symbol")
        opened, closed = _at(session, time(9, 30)), _at(session, time(15))
        spec, rule_hashes = self._rule(symbol, opened)
        if spec is None:
            return HistoricalSessionInputs(
                None, None, (), (), ("historical_trading_rule_missing",), self.policy.policy_id
            )
        tables = self._tables()
        hashes = set(rule_hashes)
        gaps: set[str] = set()
        etf = spec.instrument_class == "exchange_traded_fund"
        api = "fund_daily" if etf else "daily"
        daily = self._one(api, symbol, session)
        if daily is None:
            gaps.add("raw_daily_session_missing")
        else:
            hashes.add(daily[1])
        basics = self._rows("etf_basic" if etf else "stock_basic", symbol)
        if len(basics) != 1:
            gaps.add("historical_listing_identity_missing_or_conflicting")
        else:
            basic, digest = basics[0]
            hashes.add(digest)
            listed = _day(basic["list_date"]) if basic.get("list_date") else None
            delisted = _day(basic["delist_date"]) if basic.get("delist_date") else None
            if listed is None or listed > session or (delisted is not None and delisted <= session):
                gaps.add("security_not_listed_in_session")
            exchange = basic.get("exchange")
            if exchange not in (
                {"SSE", "SH", "XSHG"} if symbol.endswith(".SH") else {"SZSE", "SZ", "XSHE"}
            ):
                gaps.add("historical_listing_exchange_mismatch")
        calendar = [
            (row, digest)
            for row, digest in self._rows("trade_cal", None)
            if row.get("cal_date") == session.strftime("%Y%m%d")
            and row.get("exchange") == ("SSE" if symbol.endswith(".SH") else "SZSE")
        ]
        if len(calendar) != 1 or str(calendar[0][0].get("is_open")) != "1":
            gaps.add("trading_calendar_session_unverified")
        else:
            hashes.add(calendar[0][1])
        halts = [
            (row, digest)
            for row, digest in self._rows("suspend_d", symbol)
            if row.get("trade_date") == session.strftime("%Y%m%d")
        ]
        halted: bool | None = None
        if any(row.get("suspend_type") == "S" for row, _ in halts):
            halted = True
        elif any(
            row.get("suspend_type") == "R" and not row.get("suspend_timing") for row, _ in halts
        ) or (not etf and _complete_scope(tables, "suspend_d", symbol, session)):
            halted = False
        else:
            gaps.add("halt_status_unverified")
        hashes.update(digest for _, digest in halts)
        if etf:
            halted = self._fund_halt(symbol, session, halted, gaps, hashes)
        limits = self._one("stk_limit", symbol, session)
        if limits is None:
            gaps.add("daily_limits_unverified")
        else:
            hashes.add(limits[1])
            if daily is not None:
                prior = _decimal(daily[0].get("pre_close"))
                limit_prior = _decimal(limits[0].get("pre_close"))
                if prior is not None and limit_prior is not None and prior != limit_prior:
                    gaps.add("daily_limit_previous_close_mismatch")
                if prior is not None:
                    tick = spec.price_increment
                    expected_up = (prior * (1 + spec.price_limit_ratio) / tick).quantize(
                        Decimal(1), rounding="ROUND_HALF_UP"
                    ) * tick
                    expected_down = (prior * (1 - spec.price_limit_ratio) / tick).quantize(
                        Decimal(1), rounding="ROUND_HALF_UP"
                    ) * tick
                    if (
                        _decimal(limits[0].get("up_limit")) != expected_up
                        or _decimal(limits[0].get("down_limit")) != expected_down
                    ):
                        gaps.add("effective_rule_daily_limit_mismatch")
        limit_diagnostics = None
        if self.policy.limit_basis == "qualified_seed_etf_exchange_rule_v1":
            gaps.difference_update(
                {
                    "daily_limits_unverified",
                    "daily_limit_previous_close_mismatch",
                    "effective_rule_daily_limit_mismatch",
                }
            )
            _, _, limit_diagnostics = self._qualified_limits(symbol, session, spec, gaps, hashes)
        # Stable factors are anomaly evidence only; they never create cash or shares.
        factor_api = "fund_adj" if etf else "adj_factor"
        factor = self._one(factor_api, symbol, session)
        prior_factors = [
            (row, digest)
            for row, digest in self._rows(factor_api, symbol)
            if _day(row["trade_date"]) < session
        ]
        prior_factors.sort(key=lambda pair: str(pair[0]["trade_date"]))
        changed_factor = False
        if factor is None or not prior_factors:
            gaps.add("corporate_action_factor_coverage_missing")
        else:
            hashes.update((factor[1], prior_factors[-1][1]))
            changed_factor = _decimal(factor[0].get("adj_factor")) != _decimal(
                prior_factors[-1][0].get("adj_factor")
            )
        action_api = "fund_div" if etf else "dividend"
        if not _complete_scope(tables, action_api, symbol, session, full_symbol=True):
            gaps.add("corporate_action_event_coverage_missing")
        actions: list[HistoricalCorporateAction] = []
        payment_effects: dict[str, Decimal] = {}
        conflicting_payments: set[str] = set()
        relevant: list[Mapping[str, Any]] = []
        for row, digest in self._rows(action_api, symbol):
            if row.get("ann_date") and _day(row["ann_date"]) > session:
                continue
            if row.get("div_proc") != "实施":
                continue
            ex_date = _day(row["ex_date"]) if row.get("ex_date") else None
            pay_date = _day(row["pay_date"]) if row.get("pay_date") else None
            if ex_date is None or pay_date is None:
                gaps.add("corporate_action_dates_unverified")
                continue
            if ex_date <= session <= pay_date:
                relevant.append(row)
                hashes.add(digest)
                if not etf or row.get("div_proc") != "实施":
                    gaps.add("corporate_action_settlement_unaccepted")
                elif ex_date != pay_date or not row.get("record_date"):
                    gaps.add("corporate_action_delayed_payment_or_entitlement_unaccepted")
                elif pay_date == session:
                    # Preserve payment terms; reopen record-date holdings in the engine.
                    cash = _decimal(row.get("div_cash"))
                    if cash is None or cash <= 0:
                        gaps.add("corporate_action_cash_amount_unverified")
                    else:
                        payment_id = "source-dividend-" + canonical_hash(
                            {
                                "symbol": symbol,
                                "record_date": row["record_date"],
                                "ex_date": row["ex_date"],
                                "pay_date": row["pay_date"],
                            }
                        )
                        if payment_id in payment_effects:
                            if payment_effects[payment_id] != cash:
                                gaps.add("corporate_action_conflicting_payment_effects")
                                conflicting_payments.add(payment_id)
                            continue
                        payment_effects[payment_id] = cash
                        actions.append(
                            HistoricalCorporateAction(
                                action_id=payment_id,
                                target_id=symbol,
                                kind="cash_dividend",
                                effective_at=opened,
                                source_ref="sha256:" + digest,
                                cash_per_share=cash,
                                entitlement_at=_at(_day(row["record_date"]), time(15)),
                            )
                        )
        actions = [action for action in actions if action.action_id not in conflicting_payments]
        if changed_factor and not relevant:
            gaps.add("corporate_action_unexplained_factor_change")
        bar = None
        if daily is not None:
            row = daily[0]
            prices = [
                _decimal(row.get(field)) for field in ("pre_close", "open", "high", "low", "close")
            ]
            volume = _decimal(row.get("vol"))
            if any(price is None or price <= 0 for price in prices) or volume is None or volume < 0:
                gaps.add("raw_daily_values_invalid")
            else:
                shares = int(volume * 100)
                proxy = int(Decimal(shares) * self.policy.daily_open_volume_fraction)
                bar = AShareDailyBar(
                    session_open_at=opened,
                    session_close_at=closed,
                    previous_close=cast(Decimal, prices[0]),
                    open=cast(Decimal, prices[1]),
                    high=cast(Decimal, prices[2]),
                    low=cast(Decimal, prices[3]),
                    close=cast(Decimal, prices[4]),
                    volume=shares,
                    open_bid_quantity=proxy if halted is False else 0,
                    open_ask_quantity=proxy if halted is False else 0,
                    suspended=halted is True,
                )
        for table in tables:
            if table.api in {api, "trade_cal", "suspend_d", "stk_limit", action_api, factor_api}:
                hashes.update(table.hashes)
        return HistoricalSessionInputs(
            spec,
            bar,
            tuple(actions),
            tuple(sorted(hashes)),
            tuple(sorted(gaps)),
            self.policy.policy_id,
            limit_diagnostics=limit_diagnostics,
        )


def _index_rows(
    tables: tuple[_Table, ...],
) -> Mapping[tuple[str, str | None], tuple[tuple[Mapping[str, Any], str], ...]]:
    # Keep original first-insertion order and last identical-row provenance.
    deduplicated: dict[str, dict[str, tuple[Mapping[str, Any], str]]] = {}
    for table in tables:
        rows = deduplicated.setdefault(table.api, {})
        for row, digest in table.rows:
            rows[canonical_hash(row)] = MappingProxyType(dict(row)), digest
    index: dict[tuple[str, str | None], tuple[tuple[Mapping[str, Any], str], ...]] = {}
    for api, rows in deduplicated.items():
        index[api, None] = tuple(rows.values())
        symbols: dict[str, list[tuple[Mapping[str, Any], str]]] = {}
        for pair in rows.values():
            symbol = pair[0].get("ts_code")
            if isinstance(symbol, str):
                symbols.setdefault(symbol, []).append(pair)
        index.update(((api, symbol), tuple(pairs)) for symbol, pairs in symbols.items())
    return MappingProxyType(index)


def _complete_scope(
    tables: tuple[_Table, ...], api: str, symbol: str, day: date, *, full_symbol: bool = False
) -> bool:
    for table in tables:
        if table.api != api:
            continue
        params = table.snapshot.query.parameters
        if params.get("ts_code") != symbol:
            continue
        if full_symbol:
            if set(params) == {"ts_code"}:
                return True
        elif params.get("trade_date") == day.strftime("%Y%m%d") or (
            params.get("start_date")
            and params.get("end_date")
            and _day(params["start_date"]) <= day <= _day(params["end_date"])
        ):
            return True
    return False


def _decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    result = Decimal(str(value))
    return result if result.is_finite() else None


def _day(value: object) -> date:
    return datetime.strptime(str(value), "%Y%m%d").date()


def _at(day: date, hour: time) -> datetime:
    return datetime.combine(day, hour, _SHANGHAI).astimezone(UTC)
