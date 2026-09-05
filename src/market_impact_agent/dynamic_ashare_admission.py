"""Harness-owned modeled-PIT admission; discovery never grants execution authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import TradingMandateV3, require_aware


@dataclass(frozen=True, slots=True)
class HistoricalSecurityEvidence:
    symbol: str
    venue: str
    instrument_class: str
    cutoff: datetime
    effective_from: datetime
    effective_until: datetime
    raw_price: Decimal | None
    halted: bool | None
    lower_limit: Decimal | None
    upper_limit: Decimal | None
    turnover: Decimal | None
    corporate_action_status: str | None
    buy_lot_size: int | None
    price_tick: Decimal | None
    source_record_hashes: tuple[str, ...]
    gaps: tuple[str, ...] = ()
    raw_price_observed_at: datetime | None = None
    limit_diagnostics: Mapping[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "cutoff": self.cutoff.isoformat(),
            "effective_from": self.effective_from.isoformat(),
            "effective_until": self.effective_until.isoformat(),
            "raw_price": None if self.raw_price is None else str(self.raw_price),
            "raw_price_observed_at": None
            if self.raw_price_observed_at is None
            else self.raw_price_observed_at.isoformat(),
            "halted": self.halted,
            "lower_limit": None if self.lower_limit is None else str(self.lower_limit),
            "upper_limit": None if self.upper_limit is None else str(self.upper_limit),
            "turnover": None if self.turnover is None else str(self.turnover),
            "corporate_action_status": self.corporate_action_status,
            "buy_lot_size": self.buy_lot_size,
            "price_tick": None if self.price_tick is None else str(self.price_tick),
            "source_record_hashes": list(self.source_record_hashes),
            "gaps": list(self.gaps),
            **(
                {"limit_diagnostics": dict(self.limit_diagnostics)}
                if self.limit_diagnostics is not None
                else {}
            ),
        }


class HistoricalSecurityEvidenceAuthority(Protocol):
    def reopen_security(self, symbol: str, cutoff: datetime) -> HistoricalSecurityEvidence | None:
        """Reopen source records, verifying identity, modeled PIT and source hashes."""
        ...


@dataclass(frozen=True, slots=True)
class SecurityAdmission:
    symbol: str
    evidence: HistoricalSecurityEvidence | None
    gaps: tuple[str, ...]

    @property
    def execution_ready(self) -> bool:
        return not self.gaps

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "research_eligible": True,
            "execution_ready": self.execution_ready,
            "evidence": None if self.evidence is None else self.evidence.to_dict(),
            "gaps": list(self.gaps),
        }


@dataclass(frozen=True, slots=True)
class DynamicUniverseBinding:
    securities: tuple[SecurityAdmission, ...]
    mandate: TradingMandateV3

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.dynamic-ashare-universe.v1",
            "securities": [item.to_dict() for item in self.securities],
            "mandate": self.mandate.to_dict(),
            "live_capability": False,
        }


class DynamicAShareAdmission:
    def __init__(self, authority: HistoricalSecurityEvidenceAuthority) -> None:
        self.authority = authority

    def discover(self, symbols: tuple[str, ...], cutoff: datetime) -> tuple[SecurityAdmission, ...]:
        require_aware(cutoff, "dynamic universe cutoff")
        result: list[SecurityAdmission] = []
        for symbol in sorted(set(symbols)):
            if len(symbol) != 9 or not symbol[:6].isdigit() or symbol[6:] not in {".SH", ".SZ"}:
                raise ValueError("discovery requires concrete A-share symbols")
            evidence = self.authority.reopen_security(symbol, cutoff)
            gaps: set[str] = set()
            if evidence is None:
                gaps.add("historical_security_evidence_missing")
            else:
                gaps.update(evidence.gaps)
                for at in (evidence.cutoff, evidence.effective_from, evidence.effective_until):
                    require_aware(at, "security evidence time")
                if (
                    evidence.symbol != symbol
                    or evidence.venue != ("XSHG" if symbol.endswith(".SH") else "XSHE")
                    or evidence.instrument_class not in {"equity", "exchange_traded_fund"}
                ):
                    gaps.add("historical_identity_unverified")
                if (
                    evidence.cutoff != cutoff
                    or not evidence.effective_from <= cutoff < evidence.effective_until
                ):
                    gaps.add("historical_evidence_not_current")
                if not evidence.source_record_hashes or any(
                    len(item) != 64 or any(char not in "0123456789abcdef" for char in item)
                    for item in evidence.source_record_hashes
                ):
                    gaps.add("source_record_authority_missing")
                for value, name in (
                    (evidence.raw_price, "raw_price"),
                    (evidence.lower_limit, "lower_limit"),
                    (evidence.upper_limit, "upper_limit"),
                    (evidence.turnover, "liquidity"),
                    (evidence.price_tick, "price_tick"),
                ):
                    if value is None or not value.is_finite() or value <= 0:
                        gaps.add(name + "_unverified")
                if evidence.halted is not False:
                    gaps.add("halted" if evidence.halted is True else "halt_status_unverified")
                if evidence.corporate_action_status not in {
                    "none",
                    "cash_dividend_applied",
                } and not (
                    evidence.corporate_action_status == "modeled_normal_session_assumption"
                    and evidence.limit_diagnostics is not None
                    and evidence.limit_diagnostics.get("limit_basis")
                    == "qualified_seed_etf_exchange_rule_v1"
                    and evidence.limit_diagnostics.get("normal_session_assumption") is True
                ):
                    gaps.add("corporate_action_semantics_unverified")
                if type(evidence.buy_lot_size) is not int or evidence.buy_lot_size <= 0:
                    gaps.add("trading_rules_unverified")
                if not gaps and not (
                    evidence.lower_limit <= evidence.raw_price <= evidence.upper_limit  # type: ignore[operator]
                ):
                    gaps.add("raw_price_outside_daily_limits")
            result.append(SecurityAdmission(symbol, evidence, tuple(sorted(gaps))))
        return tuple(result)

    def bind(
        self,
        symbols: tuple[str, ...],
        cutoff: datetime,
        mandate_template: TradingMandateV3,
    ) -> DynamicUniverseBinding:
        if not mandate_template.valid_from <= cutoff < mandate_template.valid_until:
            raise PermissionError("dynamic universe mandate template is stale")
        securities = self.discover(symbols, cutoff)
        allowed = frozenset(item.symbol for item in securities if item.execution_ready)
        if not allowed:
            raise PermissionError(
                "no discovered security has complete historical admission evidence"
            )
        core = {
            "securities": [item.to_dict() for item in securities],
            "account_id": mandate_template.account_id,
            "harness_authority_id": mandate_template.harness_authority_id,
            "cutoff": cutoff.isoformat(),
            "policy": {
                key: value
                for key, value in mandate_template.to_dict().items()
                if key not in {"mandate_id", "universe_binding_hash", "allowed_instruments"}
            },
        }
        binding_hash = canonical_hash(core)
        mandate = replace(
            mandate_template,
            mandate_id="dynamic-ashare-mandate-" + binding_hash,
            universe_binding_hash=binding_hash,
            allowed_instruments=allowed,
        )
        return DynamicUniverseBinding(securities, mandate)

    def assert_current(self, binding: DynamicUniverseBinding, cutoff: datetime) -> None:
        rebuilt = self.bind(
            tuple(item.symbol for item in binding.securities), cutoff, binding.mandate
        )
        if rebuilt != binding:
            raise PermissionError("dynamic universe source evidence or mandate binding changed")
