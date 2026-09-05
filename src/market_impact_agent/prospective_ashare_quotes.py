"""Current A-share quote qualification over the existing durable snapshot store.

This is a local Mock price authority, not a broker fill or an entitlement to a
new data service. Missing permissions and out-of-session data remain gaps.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from market_impact_agent.dynamic_ashare_admission import HistoricalSecurityEvidence
from market_impact_agent.historical_ashare_inputs import (
    _at,  # pyright: ignore[reportPrivateUsage]
    _complete_scope,  # pyright: ignore[reportPrivateUsage]
    _day,  # pyright: ignore[reportPrivateUsage]
    _decimal,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.prospective_ashare_inputs import ProspectiveAShareInputs

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_QUOTE_AGE = timedelta(minutes=2)


class ExecutableProspectiveAShareInputs(ProspectiveAShareInputs):
    """Require actual current receipts, normal limits and a recent traded minute."""

    def with_snapshots(self, snapshot_ids: tuple[str, ...]) -> ExecutableProspectiveAShareInputs:
        return ExecutableProspectiveAShareInputs(
            store=self.store,
            snapshot_ids=self.snapshot_ids + snapshot_ids,
            qualification_policy=self.qualification_policy,
            policy=self.policy,
            fund_halt_artifact_hashes=self.fund_halt_artifact_hashes,
        )

    def reopen_security(self, symbol: str, cutoff: datetime) -> HistoricalSecurityEvidence:
        reference = super().reopen_security(symbol, cutoff)
        qualification = self.qualification(symbol, cutoff)
        spec = qualification.spec
        gaps = set(qualification.gaps)
        hashes = set(reference.source_record_hashes)
        local = cutoff.astimezone(_SHANGHAI)
        day = local.date()
        stamp = day.strftime("%Y%m%d")
        regular = time(9, 31) <= local.time() < time(11, 30) or time(13, 1) <= local.time() < time(
            14, 57
        )
        if not regular:
            gaps.add("outside_supported_continuous_session")
        # A captured empty response proves absence only within its complete exact
        # query scope and actual receipt time. Never use a future/backfilled table.
        tables = tuple(
            table
            for table in self._tables()
            if table.snapshot.completed_at <= cutoff
            and all(item.times.retrieved_at <= cutoff for item in table.snapshot.observations)
        )

        def rows(api: str) -> list[tuple[dict[str, object], str]]:
            unique = {
                digest: row
                for table in tables
                if table.api == api
                for row, digest in table.rows
                if api == "trade_cal" or row.get("ts_code") == symbol
            }
            return [(row, digest) for digest, row in unique.items()]

        venue = "SSE" if symbol.endswith(".SH") else "SZSE"
        calendar = [
            (row, digest) for row, digest in rows("trade_cal") if row.get("exchange") == venue
        ]
        today = [(row, digest) for row, digest in calendar if row.get("cal_date") == stamp]
        if len(today) != 1 or str(today[0][0].get("is_open")) != "1":
            gaps.add("current_trading_session_unverified")
        hashes.update(digest for _, digest in calendar)
        etf = spec is not None and spec.instrument_class == "exchange_traded_fund"
        quote_api = "rt_etf_min" if etf else "rt_min"
        quotes: list[tuple[datetime, dict[str, object], str]] = []
        for table in tables:
            if table.api != quote_api or table.snapshot.query.parameters != {
                "ts_code": symbol,
                "freq": "1MIN",
            }:
                continue
            hashes.update(table.hashes)
            for row, digest in table.rows:
                if row.get("ts_code") != symbol:
                    continue
                at = datetime.fromisoformat(str(row["time"]))
                if at.tzinfo is None:
                    at = at.replace(tzinfo=_SHANGHAI)
                if at <= cutoff:
                    quotes.append((at, row, digest))
        quotes.sort(key=lambda item: item[0])
        observed = None
        price = turnover = None
        if not quotes:
            gaps.add("fresh_intraday_quote_missing")
        else:
            observed, quote, digest = quotes[-1]
            hashes.add(digest)
            if len({item[2] for item in quotes if item[0] == observed}) != 1:
                gaps.add("current_quote_conflict")
            if observed.astimezone(_SHANGHAI).date() != day or cutoff - observed >= _MAX_QUOTE_AGE:
                gaps.add("current_quote_stale")
            price, turnover = _decimal(quote.get("close")), _decimal(quote.get("amount"))
            volume = _decimal(quote.get("vol"))
            if volume is None or volume <= 0 or turnover is None or turnover <= 0:
                gaps.add("current_quote_no_trade")
        limits = [
            (row, digest) for row, digest in rows("stk_limit") if row.get("trade_date") == stamp
        ]
        lower = upper = None
        if len(limits) != 1:
            gaps.add("current_reported_limits_missing_or_conflicting")
        else:
            row, digest = limits[0]
            hashes.add(digest)
            lower, upper, prior = (
                _decimal(row.get(key)) for key in ("down_limit", "up_limit", "pre_close")
            )
            if spec is None or prior is None or prior <= 0:
                gaps.add("current_limit_regime_unverified")
            else:
                tick = spec.price_increment
                expected = tuple(
                    (prior * ratio / tick).quantize(Decimal(1), rounding=ROUND_HALF_UP) * tick
                    for ratio in (Decimal("0.9"), Decimal("1.1"))
                )
                if (lower, upper) != expected:
                    gaps.add("current_limit_regime_unsupported")
        if not etf:
            basic = rows("stock_basic")
            listed = (
                _day(basic[0][0]["list_date"])
                if len(basic) == 1 and basic[0][0].get("list_date")
                else None
            )
            prior_sessions = {
                _day(row["cal_date"])
                for row, _ in calendar
                if str(row.get("is_open")) == "1"
                and listed is not None
                and listed <= _day(row["cal_date"]) < day
            }
            if len(prior_sessions) < 5:
                gaps.add("initial_listing_unrestricted_sessions_unverified")
        # A current traded minute establishes tradability only at that observation.
        # Explicit suspension evidence still wins, including an intraday halt.
        halted = None
        halt_rows = [
            (row, digest) for row, digest in rows("suspend_d") if row.get("trade_date") == stamp
        ]
        hashes.update(digest for _, digest in halt_rows)
        if any(row.get("suspend_type") == "S" for row, _ in halt_rows):
            halted = True
        elif _complete_scope(tables, "suspend_d", symbol, day) and quotes:
            halted = False
        else:
            gaps.add("current_halt_status_unverified")
        action_api = "fund_div" if etf else "dividend"
        action_tables = tuple(
            table
            for table in tables
            if table.snapshot.completed_at.astimezone(_SHANGHAI).date() == day
        )
        if not _complete_scope(action_tables, action_api, symbol, day, full_symbol=True):
            gaps.add("current_corporate_action_coverage_missing")
        for row, digest in rows(action_api):
            dates = [
                _day(row[key]) for key in ("ex_date", "pay_date", "div_listdate") if row.get(key)
            ]
            if dates and min(dates) <= day <= max(dates):
                gaps.add("current_corporate_action_requires_reconciliation")
                hashes.add(digest)
        factor_api = "fund_adj" if etf else "adj_factor"
        previous = today[0][0].get("pretrade_date") if len(today) == 1 else None
        factors = rows(factor_api)
        current_factors = [
            (row, digest) for row, digest in factors if row.get("trade_date") == stamp
        ]
        prior_factors = [
            (row, digest) for row, digest in factors if row.get("trade_date") == previous
        ]
        if len(current_factors) != 1 or len(prior_factors) != 1:
            gaps.add("current_corporate_action_factor_coverage_missing")
        else:
            before, after = (
                _decimal(items[0][0].get("adj_factor"))
                for items in (prior_factors, current_factors)
            )
            hashes.update((prior_factors[0][1], current_factors[0][1]))
            if before is None or after is None or before <= 0 or after != before:
                gaps.add("current_corporate_action_factor_change_unresolved")
        for table in tables:
            if table.api in {"suspend_d", action_api}:
                hashes.update(table.hashes)
        return replace(
            reference,
            raw_price=price,
            raw_price_observed_at=observed,
            turnover=turnover,
            effective_from=cutoff,
            effective_until=min(
                observed + _MAX_QUOTE_AGE,
                _at(day, time(11, 30) if local.time() < time(12) else time(14, 57)),
            )
            if observed is not None and regular and cutoff - observed < _MAX_QUOTE_AGE
            else cutoff + timedelta(microseconds=1),
            lower_limit=lower,
            upper_limit=upper,
            halted=halted,
            corporate_action_status=None
            if any("corporate_action" in gap for gap in gaps)
            else "none",
            source_record_hashes=tuple(sorted(hashes)),
            gaps=tuple(sorted(gaps)),
            limit_diagnostics={
                "valuation_basis": "actual_received_one_minute_trade",
                "qualification_artifact_hash": qualification.qualification_artifact_hash,
                "static_qualification_ready": qualification.qualified,
                "execution_session_ready": not gaps,
                "maximum_quote_age_seconds": 120,
                "limit_basis": "reported_limits_matching_qualified_normal_regime",
            },
        )
