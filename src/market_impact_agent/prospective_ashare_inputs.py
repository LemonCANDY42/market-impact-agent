"""Actual-receipt A-share research qualification and reference valuation.

Completed daily closes are reference marks, never fresh executable intraday quotes.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from market_impact_agent.ashare_security_qualification import (
    AShareSecurityQualification,
    SourceBackedAShareRulePolicy,
    qualify_ashare_security,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.dynamic_ashare_admission import HistoricalSecurityEvidence
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
    _at,  # pyright: ignore[reportPrivateUsage]
    _day,  # pyright: ignore[reportPrivateUsage]
    _decimal,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.streaming_nautilus_account import HistoricalInstrumentSpec


class ProspectiveAShareInputs(HistoricalAShareInputs):
    """Reuse the frozen source graph and successor acquisition lifecycle."""

    def __init__(
        self,
        *,
        store: LocalDataSnapshotStore,
        snapshot_ids: tuple[str, ...],
        qualification_policy: SourceBackedAShareRulePolicy,
        rule_artifact_hashes: tuple[str, ...] = (),
        policy: ModeledHistoricalPolicy | None = None,
        fund_halt_artifact_hashes: tuple[str, ...] = (),
    ) -> None:
        if rule_artifact_hashes:
            raise ValueError("prospective qualification requires generic source policy")
        super().__init__(
            store=store,
            snapshot_ids=snapshot_ids,
            rule_artifact_hashes=(),
            policy=policy
            or ModeledHistoricalPolicy("prospective-reference-only-v1", Decimal("0.01")),
            fund_halt_artifact_hashes=fund_halt_artifact_hashes,
            qualification_policy=qualification_policy,
        )
        self.qualification_policy: SourceBackedAShareRulePolicy = qualification_policy

    def with_snapshots(self, snapshot_ids: tuple[str, ...]) -> ProspectiveAShareInputs:
        return ProspectiveAShareInputs(
            store=self.store,
            snapshot_ids=self.snapshot_ids + snapshot_ids,
            qualification_policy=self.qualification_policy,
            policy=self.policy,
            fund_halt_artifact_hashes=self.fund_halt_artifact_hashes,
        )

    def qualification(self, symbol: str, cutoff: datetime) -> AShareSecurityQualification:
        return qualify_ashare_security(self, symbol, cutoff, self.qualification_policy)

    def _rule(
        self, symbol: str, cutoff: datetime
    ) -> tuple[HistoricalInstrumentSpec | None, tuple[str, ...]]:
        result = self.qualification(symbol, cutoff)
        return result.spec, (*result.source_record_hashes, result.qualification_artifact_hash)

    def reopen_security(self, symbol: str, cutoff: datetime) -> HistoricalSecurityEvidence:
        qualification = self.qualification(symbol, cutoff)
        spec = qualification.spec
        hashes = set(qualification.source_record_hashes)
        hashes.add(qualification.qualification_artifact_hash)
        gaps = set(qualification.gaps)
        gaps.add("fresh_intraday_quote_missing")
        api = "fund_daily" if spec and spec.instrument_class == "exchange_traded_fund" else "daily"
        tables = self._tables()
        received: dict[str, datetime] = {}
        for table in tables:
            for row in table.snapshot.observations:
                received[row.raw_content_hash] = min(
                    received.get(row.raw_content_hash, row.times.retrieved_at),
                    row.times.retrieved_at,
                )
        rows = [
            pair
            for pair in self._rows(api, symbol)
            if received[pair[1]] <= cutoff and _at(_day(pair[0]["trade_date"]), time(15)) <= cutoff
        ]
        rows.sort(key=lambda pair: str(pair[0]["trade_date"]))
        price = None
        turnover = None
        observed_at = None
        if rows:
            row, digest = rows[-1]
            duplicates = [pair for pair in rows if pair[0]["trade_date"] == row["trade_date"]]
            if len(duplicates) != 1:
                gaps.add("reference_mark_source_conflict")
            else:
                hashes.add(digest)
                price = _decimal(row.get("close"))
                amount = _decimal(row.get("amount"))
                turnover = amount * 1000 if amount is not None else None
                observed_at = _at(_day(row["trade_date"]), time(15))
                exchange = "SSE" if symbol.endswith(".SH") else "SZSE"
                current_day = cutoff.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
                if not any(
                    cal.get("exchange") == exchange
                    and cal.get("cal_date") == current_day
                    and received[cal_hash] <= cutoff
                    for cal, cal_hash in self._rows("trade_cal", None)
                ):
                    gaps.add("current_calendar_coverage_missing")
                sessions = [
                    _day(cal["cal_date"])
                    for cal, cal_hash in self._rows("trade_cal", None)
                    if cal.get("exchange") == exchange
                    and str(cal.get("is_open")) == "1"
                    and received[cal_hash] <= cutoff
                    and _at(_day(cal["cal_date"]), time(15)) <= cutoff
                ]
                if not sessions or max(sessions) != _day(row["trade_date"]):
                    gaps.add("reference_mark_not_latest_completed_session")
        else:
            gaps.add("completed_reference_mark_missing")
        evidence = HistoricalSecurityEvidence(
            symbol=symbol,
            venue="XSHG" if symbol.endswith(".SH") else "XSHE",
            instrument_class=spec.instrument_class if spec else "unqualified",
            cutoff=cutoff,
            effective_from=cutoff,
            effective_until=cutoff + timedelta(microseconds=1),
            raw_price=price,
            raw_price_observed_at=observed_at,
            halted=None,
            lower_limit=None,
            upper_limit=None,
            turnover=turnover,
            corporate_action_status=None,
            buy_lot_size=spec.lot_size if spec else None,
            price_tick=spec.price_increment if spec else None,
            source_record_hashes=tuple(sorted(hashes)),
            gaps=tuple(sorted(gaps)),
        )
        return replace(
            evidence,
            limit_diagnostics={
                "valuation_basis": "completed_daily_reference_not_executable_quote",
                "qualification_artifact_hash": qualification.qualification_artifact_hash,
                "static_qualification_ready": qualification.qualified,
                "execution_session_ready": False,
            },
        )
