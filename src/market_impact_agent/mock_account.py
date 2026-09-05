"""Deterministic projection of simulated Provider facts; not an account ledger."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import cast

from market_impact_agent.account_state import (
    AccountPosition,
    AccountStateSnapshot,
    CashBalance,
    OpenOrder,
    OpenOrderStatus,
    RecentFill,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import ExecutionStatus, Side, TradingEnvironment, require_aware
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.providers import MockExecutionProvider, ReconciliationSnapshot


def configure_simulated_account(
    provider: MockExecutionProvider,
    *,
    seed: str,
    cash: tuple[CashBalance, ...],
    positions: tuple[AccountPosition, ...],
    instruments: Mapping[str, tuple[str, str]],
    opened_at: datetime,
) -> None:
    require_aware(opened_at, "opened_at")
    if not seed or len(cash) != 1 or cash[0].currency != "USD":
        raise ValueError("Mock account requires a synthetic seed and one USD opening balance")
    if len({item.target_id for item in positions}) != len(positions):
        raise ValueError("Mock opening account requires one position per instrument")
    if any(len(value) != 2 or not all(value) for value in instruments.values()):
        raise ValueError("Mock instrument metadata requires explicit venue and class")
    if any(item.side is not Side.BUY or item.target_id not in instruments for item in positions):
        raise ValueError(
            "Mock opening positions must be long and have explicit instrument metadata"
        )
    if any(
        instruments[item.target_id] != (item.venue, item.instrument_class) for item in positions
    ):
        raise ValueError("Mock opening position identity differs from configured instrument")
    payload = {
        "schema_version": "market-impact.simulated-account-opening.v1",
        "seed": seed,
        "cash": [item.to_dict() for item in cash],
        "positions": [item.to_dict() for item in positions],
        "instruments": {key: list(value) for key, value in sorted(instruments.items())},
        "opened_at": opened_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "provenance": "synthetic_mock_configuration_not_broker_facts",
    }
    serialized = json.dumps(payload, sort_keys=True)
    with provider._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT payload_json FROM mock_account_configuration"
        ).fetchone()
        if existing is not None:
            if existing[0] != serialized:
                raise PermissionError("Mock opening account configuration is immutable")
            return
        if connection.execute("SELECT 1 FROM mock_execution_receipts LIMIT 1").fetchone():
            raise PermissionError("Mock account must be configured before any order")
        connection.execute("INSERT INTO mock_account_configuration VALUES (1, ?)", (serialized,))


def simulated_account_snapshot(
    provider: MockExecutionProvider,
    *,
    price_bases: Mapping[str, PriceBasis],
) -> AccountStateSnapshot:
    observed_at = provider._clock()  # pyright: ignore[reportPrivateUsage]
    with provider._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        connection.execute("BEGIN")
        row = connection.execute("SELECT payload_json FROM mock_account_configuration").fetchone()
        if row is None:
            raise PermissionError("Mock has no configured simulated account")
        config = cast(dict[str, object], json.loads(row[0]))
        orders = connection.execute(
            "SELECT * FROM mock_execution_receipts ORDER BY client_order_id"
        ).fetchall()
        fills = connection.execute("SELECT * FROM mock_execution_fills ORDER BY fill_id").fetchall()
        receipts = tuple(provider._durable_receipt(connection, row) for row in orders)  # pyright: ignore[reportPrivateUsage]
    opening = datetime.fromisoformat(cast(str, config["opened_at"]).replace("Z", "+00:00"))
    if observed_at < opening:
        raise PermissionError("Mock account cannot be observed before opening")
    instruments = cast(dict[str, list[str]], config["instruments"])
    cash = cast(list[dict[str, str]], config["cash"])[0]
    available, settled = Decimal(cash["available"]), Decimal(cash["settled"])
    quantities = {
        item["target_id"]: Decimal(item["quantity"])
        for item in cast(list[dict[str, str]], config["positions"])
    }
    order_payloads: dict[str, dict[str, str]] = {}
    for row in orders:
        if row["order_json"] is None:
            raise PermissionError("Mock account order has no exact quantity/side provenance")
        payload = cast(dict[str, str], json.loads(row["order_json"]))
        if (
            canonical_hash(payload) != row["order_hash"]
            or payload["instrument_id"] not in instruments
        ):
            raise PermissionError(
                "Mock account order identity differs or lacks instrument metadata"
            )
        order_payloads[row["client_order_id"]] = payload
    recent: list[RecentFill] = []
    receipt_by_id = {item.client_order_id: item for item in receipts}
    for fill in fills:
        order = order_payloads[fill["client_order_id"]]
        instrument = order["instrument_id"]
        quantity, price = Decimal(fill["quantity"]), Decimal(fill["price"])
        side = Side(order["side"])
        signed = quantity if side is Side.BUY else -quantity
        quantities[instrument] = quantities.get(instrument, Decimal(0)) + signed
        available -= signed * price
        settled -= signed * price
        venue, instrument_class = instruments[instrument]
        receipt = receipt_by_id[fill["client_order_id"]]
        assert receipt.provider_order_id is not None
        recent.append(
            RecentFill(
                fill["fill_id"],
                receipt.provider_order_id,
                instrument,
                venue,
                instrument_class,
                side,
                quantity,
                datetime.fromisoformat(fill["observed_at"].replace("Z", "+00:00")),
            )
        )
    marked: dict[str, Decimal] = {}
    for instrument, quantity in quantities.items():
        if quantity < 0:
            raise PermissionError("Mock account short-position projection is not accepted")
        if not quantity:
            continue
        basis = price_bases.get(instrument)
        if (
            basis is None
            or basis.instrument_id != instrument
            or basis.currency != "USD"
            or basis.unit != "per_share"
            or basis.basis_kind not in {"raw_reference_quote", "reference_quote"}
            or not basis.observed_at <= observed_at < basis.valid_until
        ):
            raise PermissionError("Mock account requires an explicit current simulated raw mark")
        marked[instrument] = quantity * basis.price
    equity = settled + sum(marked.values(), Decimal(0))
    if equity <= 0:
        raise PermissionError("Mock account has no positive marked equity")
    positions = tuple(
        AccountPosition(
            instrument,
            instruments[instrument][0],
            instruments[instrument][1],
            Side.BUY,
            quantities[instrument],
            marked[instrument] / equity,
            None,
        )
        for instrument in sorted(marked)
    )
    open_orders: list[OpenOrder] = []
    for receipt in receipts:
        if receipt.status not in {ExecutionStatus.ACCEPTED, ExecutionStatus.PARTIALLY_FILLED}:
            continue
        order = order_payloads[receipt.client_order_id]
        assert receipt.provider_order_id is not None
        open_orders.append(
            OpenOrder(
                receipt.provider_order_id,
                order["instrument_id"],
                instruments[order["instrument_id"]][0],
                instruments[order["instrument_id"]][1],
                Side(order["side"]),
                Decimal(order["quantity"]) - receipt.filled_quantity,
                OpenOrderStatus.WORKING,
                datetime.fromisoformat(order["created_at"].replace("Z", "+00:00")),
            )
        )
    reconciliation = ReconciliationSnapshot.build(
        provider_id=provider.manifest.provider_id,
        observed_at=observed_at,
        complete=True,
        receipts=receipts,
    )
    seed = cast(str, config["seed"])
    return capture_account_state_snapshot(
        provider=provider.manifest,
        account_reference="simulated:" + seed,
        account_reference_key=sha256(("synthetic-only:" + seed).encode()).digest(),
        environment=TradingEnvironment.PAPER,
        as_of=observed_at,
        reconciled_at=observed_at,
        reconciliation_reference=reconciliation.snapshot_id,
        cash=(CashBalance("USD", available, settled),),
        positions=positions,
        open_orders=tuple(open_orders),
        recent_fills=tuple(recent),
        recent_fills_since=opening,
    )
