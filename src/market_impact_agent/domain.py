from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum


class TradingEnvironment(StrEnum):
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderKind(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class ApprovalMode(StrEnum):
    DISABLED = "disabled"
    MANUAL_EACH = "manual_each"
    TIMEBOXED = "timeboxed"
    POLICY_AUTO = "policy_auto"
    AUTONOMOUS = "autonomous"


class HardPolicyOutcome(StrEnum):
    DENY = "deny"
    REQUIRE_MANUAL = "require_manual"
    ELIGIBLE = "eligible"


class ExecutionStatus(StrEnum):
    ACCEPTED = "accepted"
    PENDING_CANCEL = "pending_cancel"
    CANCELED = "canceled"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


def require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class SignalIntent:
    signal_id: str
    event_id: str
    instrument_id: str
    side: Side
    valid_from: datetime
    expires_at: datetime
    evidence_refs: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]

    def __post_init__(self) -> None:
        require_aware(self.valid_from, "valid_from")
        require_aware(self.expires_at, "expires_at")
        if self.expires_at <= self.valid_from:
            raise ValueError("expires_at must be after valid_from")
        if not self.evidence_refs:
            raise ValueError("signal intents require at least one evidence reference")
        if not self.invalidation_conditions:
            raise ValueError("signal intents require at least one invalidation condition")
        if len(self.evidence_refs) != len(set(self.evidence_refs)):
            raise ValueError("signal intent evidence references must be unique")
        if len(self.invalidation_conditions) != len(set(self.invalidation_conditions)):
            raise ValueError("signal intent invalidation conditions must be unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.signal-intent.v1",
            "signal_id": self.signal_id,
            "event_id": self.event_id,
            "instrument_id": self.instrument_id,
            "side": self.side.value,
            "valid_from": _timestamp(self.valid_from),
            "expires_at": _timestamp(self.expires_at),
            "evidence_refs": list(self.evidence_refs),
            "invalidation_conditions": list(self.invalidation_conditions),
        }


@dataclass(frozen=True, slots=True)
class OrderIntent:
    client_order_id: str
    signal_id: str
    account_id: str
    environment: TradingEnvironment
    instrument_id: str
    side: Side
    quantity: Decimal
    order_kind: OrderKind
    created_at: datetime
    expires_at: datetime
    limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        require_aware(self.created_at, "created_at")
        require_aware(self.expires_at, "expires_at")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("quantity must be finite and positive")
        if self.order_kind is OrderKind.LIMIT and self.limit_price is None:
            raise ValueError("limit orders require limit_price")
        if self.order_kind is OrderKind.MARKET and self.limit_price is not None:
            raise ValueError("market orders cannot set limit_price")
        if self.limit_price is not None and (
            not self.limit_price.is_finite() or self.limit_price <= 0
        ):
            raise ValueError("limit_price must be finite and positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.order-intent.v1",
            "client_order_id": self.client_order_id,
            "signal_id": self.signal_id,
            "account_id": self.account_id,
            "environment": self.environment.value,
            "instrument_id": self.instrument_id,
            "side": self.side.value,
            "quantity": str(self.quantity),
            "order_kind": self.order_kind.value,
            "limit_price": str(self.limit_price) if self.limit_price is not None else None,
            "created_at": _timestamp(self.created_at),
            "expires_at": _timestamp(self.expires_at),
        }


@dataclass(frozen=True, slots=True)
class TradingMandate:
    mandate_id: str
    account_id: str
    environment: TradingEnvironment
    approval_mode: ApprovalMode
    valid_from: datetime
    expires_at: datetime
    allowed_instruments: frozenset[str]
    allowed_sides: frozenset[Side]
    max_order_notional: Decimal

    def __post_init__(self) -> None:
        require_aware(self.valid_from, "valid_from")
        require_aware(self.expires_at, "expires_at")
        if self.expires_at <= self.valid_from:
            raise ValueError("expires_at must be after valid_from")
        if not self.allowed_instruments:
            raise ValueError("allowed_instruments must not be empty")
        if not self.allowed_sides:
            raise ValueError("allowed_sides must not be empty")
        if not self.max_order_notional.is_finite() or self.max_order_notional <= 0:
            raise ValueError("max_order_notional must be finite and positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.trading-mandate.v1",
            "mandate_id": self.mandate_id,
            "account_id": self.account_id,
            "environment": self.environment.value,
            "approval_mode": self.approval_mode.value,
            "valid_from": _timestamp(self.valid_from),
            "expires_at": _timestamp(self.expires_at),
            "allowed_instruments": sorted(self.allowed_instruments),
            "allowed_sides": sorted(item.value for item in self.allowed_sides),
            "max_order_notional": str(self.max_order_notional),
        }


@dataclass(frozen=True, slots=True)
class HardPolicyDecision:
    outcome: HardPolicyOutcome
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    client_order_id: str
    provider_order_id: str | None
    status: ExecutionStatus
    observed_at: datetime
    filled_quantity: Decimal = Decimal(0)
    fill_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "observed_at")
        if self.provider_order_id is not None and (
            not self.provider_order_id or self.provider_order_id != self.provider_order_id.strip()
        ):
            raise ValueError("provider_order_id must be a non-empty trimmed string")
        if (
            self.status
            in {
                ExecutionStatus.ACCEPTED,
                ExecutionStatus.PENDING_CANCEL,
                ExecutionStatus.CANCELED,
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
            }
            and self.provider_order_id is None
        ):
            raise ValueError("broker-observed order state requires provider_order_id")
        if not self.filled_quantity.is_finite() or self.filled_quantity < 0:
            raise ValueError("filled_quantity must be finite and non-negative")
        if len(set(self.fill_ids)) != len(self.fill_ids) or any(
            not item or item != item.strip() for item in self.fill_ids
        ):
            raise ValueError("fill_ids must be unique, non-empty strings")
        object.__setattr__(self, "fill_ids", tuple(sorted(self.fill_ids)))
        if (self.filled_quantity > 0) != bool(self.fill_ids):
            raise ValueError("filled_quantity and fill_ids must be present together")
        if (
            self.status
            in {
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
            }
            and self.filled_quantity == 0
        ):
            raise ValueError("filled order status requires fill evidence")
        if (
            self.status
            in {
                ExecutionStatus.ACCEPTED,
                ExecutionStatus.REJECTED,
            }
            and self.filled_quantity > 0
        ):
            raise ValueError("unfilled terminal or accepted status cannot carry fill evidence")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
