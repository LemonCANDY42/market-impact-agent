from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from market_impact_agent.account_state import CashBalance
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import (
    ExecutionStatus,
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.providers import (
    MockExecutionProvider,
    SubmissionCapabilityRejected,
    _issue_submission_capability,  # pyright: ignore[reportPrivateUsage]
)

AT = datetime(2026, 9, 3, 2, tzinfo=UTC)
TARGET = "510300.SH"


def test_cny_durable_fees_sellability_dynamic_registration_and_ack(tmp_path: Path) -> None:
    clock = [AT]
    path = tmp_path / "mock.sqlite3"
    provider = MockExecutionProvider(path, clock=lambda: clock[0])

    def configure(authority: dict[str, str] | None = None) -> None:
        provider.configure_simulated_account(
            seed="cny-native",
            cash=(CashBalance("CNY", Decimal(100000), Decimal(100000)),),
            positions=(),
            instruments={},
            opened_at=AT,
            opening_authority=authority,
        )

    authority = {
        "version": "cny-local-mock.v1",
        "source_reference": "opening-evidence",
        "opening_inventory": "overnight_sellable",
    }
    with pytest.raises(ValueError, match="CNY opening"):
        configure()
    configure(authority)
    provider.register_simulated_instrument(
        target_id=TARGET,
        venue="XSHG",
        instrument_class="exchange_traded_fund",
        qualification_hash="a" * 64,
    )
    provider.register_simulated_instrument(
        target_id=TARGET,
        venue="XSHG",
        instrument_class="exchange_traded_fund",
        qualification_hash="b" * 64,
    )
    with pytest.raises(PermissionError, match="identity is immutable"):
        provider.register_simulated_instrument(
            target_id=TARGET,
            venue="XSHE",
            instrument_class="exchange_traded_fund",
            qualification_hash="c" * 64,
        )
    provider.bind_submission_validator(lambda capability: True)

    def submit(identity: str, side: Side):
        order = OrderIntent(
            identity,
            "signal",
            "synthetic",
            TradingEnvironment.PAPER,
            TARGET,
            side,
            Decimal(100),
            OrderKind.MARKET,
            clock[0],
            clock[0] + timedelta(minutes=1),
        )
        return provider.submit(
            _issue_submission_capability(
                order=order,
                submission_id=identity,
                provider_id=provider.manifest.provider_id,
                provider_version=provider.manifest.provider_version,
                order_hash=canonical_hash(order.to_dict()),
                mandate_hash="a" * 64,
                price_basis_hash="b" * 64,
                policy_evaluation_hash="c" * 64,
                approval_hash="d" * 64,
            )
        )

    def snapshot():
        basis = PriceBasis(
            TARGET,
            "CNY",
            "per_share",
            "raw_reference_quote",
            Decimal(10),
            "synthetic",
            "1",
            clock[0],
            clock[0] + timedelta(minutes=1),
        )
        return provider.simulated_account_snapshot(price_bases={TARGET: basis})

    assert submit("buy", Side.BUY).status is ExecutionStatus.ACCEPTED
    ack_snapshot = provider.reconcile()
    clock[0] += timedelta(seconds=1)
    delayed_account = provider.simulated_account_snapshot(
        price_bases={}, reconciliation_snapshot=ack_snapshot
    )
    assert delayed_account.as_of == ack_snapshot.observed_at
    assert delayed_account.reconciliation_reference == ack_snapshot.snapshot_id
    assert snapshot().positions == ()
    assert snapshot().cash == (CashBalance("CNY", Decimal(100000), Decimal(100000)),)
    with pytest.raises(ValueError, match="explicit fee"):
        provider.record_simulated_fill(
            "buy", fill_id="fill-buy", quantity=Decimal(100), price=Decimal(10)
        )
    next_open = AT + timedelta(days=1)

    def fill_buy():
        return provider.record_simulated_fill(
            "buy",
            fill_id="fill-buy",
            quantity=Decimal(100),
            price=Decimal(10),
            fee=Decimal(5),
            sellable_at=next_open,
        )

    with pytest.raises(PermissionError, match="including fees"):
        provider.record_simulated_fill(
            "buy",
            fill_id="too-expensive",
            quantity=Decimal(100),
            price=Decimal(1000),
            fee=Decimal(5),
            sellable_at=next_open,
        )
    receipt = fill_buy()
    with pytest.raises(PermissionError, match="exact current durable facts"):
        provider.simulated_account_snapshot(price_bases={}, reconciliation_snapshot=ack_snapshot)
    with pytest.raises(ValueError, match="different content"):
        provider.record_simulated_fill(
            "buy",
            fill_id="fill-buy",
            quantity=Decimal(100),
            price=Decimal(10),
            fee=Decimal(6),
            sellable_at=next_open,
        )
    assert fill_buy() == receipt
    assert snapshot().cash == (CashBalance("CNY", Decimal(98995), Decimal(98995)),)
    assert provider.simulated_sellable_quantity(TARGET) == 0
    with pytest.raises(SubmissionCapabilityRejected, match="settled inventory"):
        submit("same-day-sell", Side.SELL)
    provider = MockExecutionProvider(path, clock=lambda: clock[0])
    provider.bind_submission_validator(lambda capability: True)
    configure(authority)
    assert fill_buy() == receipt
    clock[0] = next_open
    assert provider.simulated_sellable_quantity(TARGET) == 100
    assert submit("sell", Side.SELL).status is ExecutionStatus.ACCEPTED
    with pytest.raises(SubmissionCapabilityRejected, match="settled inventory"):
        submit("second-sell", Side.SELL)
    provider.record_simulated_fill(
        "sell", fill_id="fill-sell", quantity=Decimal(100), price=Decimal(10), fee=Decimal(5)
    )
    assert snapshot().positions == ()
    assert snapshot().cash == (CashBalance("CNY", Decimal(99990), Decimal(99990)),)
