"""Source-derived local Mock fills; provider receipts remain economic authority."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import ExecutionReceipt, ExecutionStatus
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
from market_impact_agent.providers import MockExecutionProvider

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PREFIX = "prospective-source-fill-"


@dataclass(frozen=True)
class ProspectiveMockFillResult:
    receipt: ExecutionReceipt | None
    gaps: tuple[str, ...] = ()
    evidence_artifact_hash: str | None = None


def _next_open(
    market: ExecutableProspectiveAShareInputs, symbol: str, at: datetime
) -> tuple[datetime | None, tuple[str, ...]]:
    """Prove every intervening calendar day, including closures, from actual receipts."""
    exchange = "SSE" if symbol.endswith(".SH") else "SZSE"
    rows: dict[str, dict[str, tuple[dict[str, object], str]]] = {}
    for table in market._tables():  # pyright: ignore[reportPrivateUsage]
        if (
            table.api != "trade_cal"
            or table.snapshot.completed_at > at
            or any(item.times.retrieved_at > at for item in table.snapshot.observations)
        ):
            continue
        for row, digest in table.rows:
            if row.get("exchange") == exchange:
                rows.setdefault(str(row["cal_date"]), {})[digest] = (dict(row), digest)
    day = at.astimezone(_SHANGHAI).date()
    candidate = day + timedelta(days=1)
    hashes: list[str] = []
    while candidate.strftime("%Y%m%d") in rows:
        values = list(rows[candidate.strftime("%Y%m%d")].values())
        if len(values) != 1:
            return None, ()
        row, digest = values[0]
        hashes.append(digest)
        if str(row.get("is_open")) == "1":
            if row.get("pretrade_date") != day.strftime("%Y%m%d"):
                return None, ()
            return datetime.combine(candidate, time(9, 30), _SHANGHAI), tuple(hashes)
        if str(row.get("is_open")) != "0":
            return None, ()
        candidate += timedelta(days=1)
    return None, ()


def record_prospective_mock_fill(
    provider: MockExecutionProvider,
    market: ExecutableProspectiveAShareInputs,
    client_order_id: str,
) -> ProspectiveMockFillResult:
    """Fill an accepted full market order once using later frozen source evidence.

    No caller-supplied price, quantity, fee or T+1 timestamp is accepted. This is
    an explicit simulation policy, not a claim about broker fills or liquidity.
    """
    at = provider._clock()  # pyright: ignore[reportPrivateUsage]
    with provider._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        row = connection.execute(
            "SELECT * FROM mock_execution_receipts WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        if row is None or row["order_json"] is None:
            return ProspectiveMockFillResult(None, ("accepted_durable_order_missing",))
        receipt = provider._durable_receipt(connection, row)  # pyright: ignore[reportPrivateUsage]
        if receipt.status is ExecutionStatus.FILLED and len(receipt.fill_ids) == 1:
            fill_id = receipt.fill_ids[0]
            if fill_id.startswith(_PREFIX):
                digest = fill_id.removeprefix(_PREFIX)
                # The immutable authority survives a crash after provider mutation.
                market.store.artifacts.read_json(digest)
                return ProspectiveMockFillResult(receipt, evidence_artifact_hash=digest)
        if receipt.status is not ExecutionStatus.ACCEPTED or receipt.fill_ids:
            return ProspectiveMockFillResult(None, ("unfilled_accepted_order_required",))
        order = cast(dict[str, object], json.loads(row["order_json"]))
        if canonical_hash(order) != row["order_hash"]:
            return ProspectiveMockFillResult(None, ("accepted_order_content_mismatch",))
        configuration = connection.execute(
            "SELECT payload_json FROM mock_account_configuration"
        ).fetchone()
        if configuration is None or json.loads(configuration[0])["cash"][0]["currency"] != "CNY":
            return ProspectiveMockFillResult(None, ("cny_mock_account_required",))
    if order["order_kind"] != "market":
        return ProspectiveMockFillResult(None, ("market_order_required",))
    if at >= datetime.fromisoformat(str(order["expires_at"])):
        return ProspectiveMockFillResult(None, ("accepted_order_expired",))
    symbol = str(order["instrument_id"])
    admission = DynamicAShareAdmission(market).discover((symbol,), at)[0]
    if not admission.execution_ready or admission.evidence is None:
        return ProspectiveMockFillResult(None, admission.gaps)
    evidence = admission.evidence
    assert evidence.raw_price is not None and evidence.raw_price_observed_at is not None
    if evidence.raw_price_observed_at <= max(
        receipt.observed_at, datetime.fromisoformat(str(order["created_at"]))
    ):
        return ProspectiveMockFillResult(None, ("post_submission_quote_required",))
    qualification = market.qualification(symbol, at)
    spec = qualification.spec
    if not qualification.qualified or spec is None:
        return ProspectiveMockFillResult(None, qualification.gaps)
    sellable_at, calendar_hashes = None, ()
    if order["side"] == "buy":
        sellable_at, calendar_hashes = _next_open(market, symbol, at)
        if sellable_at is None:
            return ProspectiveMockFillResult(None, ("next_open_trading_date_unverified",))
    quantity = Decimal(str(order["quantity"]))
    notional = quantity * evidence.raw_price
    fee = max(spec.minimum_commission, notional * spec.commission_rate)
    if order["side"] == "sell":
        fee += notional * spec.sell_stamp_tax_rate
    fee = fee.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    authority = market.store.artifacts.put_json(
        {
            "schema_version": "market-impact.prospective-mock-fill.v1",
            "client_order_id": client_order_id,
            "order_hash": canonical_hash(order),
            "policy": "full-order-later-traded-minute-qualified-fees-t1-v1",
            "observed_at": at.isoformat(),
            "snapshot_ids": list(market.snapshot_ids),
            "security": evidence.to_dict(),
            "qualification_artifact_hash": qualification.qualification_artifact_hash,
            "fee_rule_ref": spec.source_ref,
            "fee": str(fee),
            "quantity": str(quantity),
            "sellable_at": sellable_at.isoformat() if sellable_at is not None else None,
            "calendar_source_record_hashes": list(calendar_hashes),
        }
    )
    try:
        receipt = provider.record_simulated_fill(
            client_order_id,
            fill_id=_PREFIX + authority.content_hash,
            quantity=quantity,
            price=evidence.raw_price,
            fee=fee,
            sellable_at=sellable_at,
        )
    except (PermissionError, ValueError) as exc:
        return ProspectiveMockFillResult(None, ("provider_fill_refused:" + str(exc),))
    return ProspectiveMockFillResult(receipt, evidence_artifact_hash=authority.content_hash)
