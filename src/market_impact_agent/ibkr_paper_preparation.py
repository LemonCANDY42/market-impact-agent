"""Offline IBKR Paper preparation artifacts.

This module deliberately has no dependency on the concrete ``TradingNode``
runtime.  It validates the static portion of that runtime's supported scope
and records the still-required evidence.  Creating a preparation does not
connect to IBKR, construct a Nautilus node, obtain credentials, or authorize
an order.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import (
    ApprovalMode,
    Side,
    TradingEnvironment,
    TradingMandateV2,
)
from market_impact_agent.ibkr_nautilus_execution import (
    IbkrNautilusInstrumentRoute,
    hash_ibkr_nautilus_instrument_routes,
)
from market_impact_agent.ibkr_nautilus_paper import (
    IBKR_NAUTILUS_PAPER_PROVIDER_ID,
    IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
    IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
    IBKR_NAUTILUS_VERSION,
)

IBKR_PAPER_PREPARATION_SCHEMA = "market-impact.ibkr-paper-preparation.v1"
_PAPER_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_PAPER_MARKETS = frozenset({"HK", "US"})
_REQUIRED_SCENARIOS = (
    "account_reconciliation",
    "ambiguous_acknowledgement",
    "cancel",
    "disconnect",
    "duplicate_fill",
    "external_order",
    "gateway_restart",
    "partial_fill",
    "process_restart",
    "replace",
    "submit",
)
_MANDATE_FIELDS = frozenset(
    {
        "schema_version",
        "mandate_id",
        "account_id",
        "harness_authority_id",
        "environment",
        "approval_mode",
        "valid_from",
        "valid_until",
        "allowed_instruments",
        "allowed_instrument_classes",
        "allowed_sides",
        "currency",
        "gross_exposure_limit",
        "minimum_net_exposure",
        "maximum_net_exposure",
        "maximum_position_count",
        "maximum_single_position_fraction",
        "daily_turnover_limit",
        "daily_submission_limit",
        "daily_loss_kill_threshold",
        "strategy_peak_drawdown_kill_threshold",
        "kill_on_unknown_ack",
        "kill_on_stale_account_snapshot",
        "kill_on_incomplete_order_coverage",
        "kill_on_reconciliation_difference",
        "kill_on_provider_loss",
    }
)


@dataclass(frozen=True, slots=True)
class IbkrPaperStaticConfiguration:
    """The only static network configuration that the candidate supports."""

    host: str = "127.0.0.1"
    port: int = 4002
    client_id: int = 0
    fetch_all_open_orders: bool = True
    time_in_force: str = "DAY"

    def __post_init__(self) -> None:
        if self.host not in _PAPER_HOSTS:
            raise ValueError("IBKR Paper preparation only accepts a loopback Gateway")
        if self.port != 4002:
            raise ValueError("IBKR Paper preparation requires Paper Gateway port 4002")
        if self.client_id != 0:
            raise ValueError("IBKR Paper preparation requires client ID 0")
        if not self.fetch_all_open_orders:
            raise ValueError("IBKR Paper preparation requires fetch_all_open_orders")
        if self.time_in_force != "DAY":
            raise ValueError("IBKR Paper preparation supports DAY only")

    def to_dict(self) -> dict[str, object]:
        return {
            "gateway_host": self.host,
            "gateway_port": self.port,
            "client_id": self.client_id,
            "fetch_all_open_orders": self.fetch_all_open_orders,
            "time_in_force": self.time_in_force,
        }


@dataclass(frozen=True, slots=True)
class IbkrPaperPreparation:
    """A content-addressed offline plan for future, separately accepted Paper work."""

    preparation_id: str
    mandate_source_hash: str
    mandate: TradingMandateV2
    instrument_routes: Mapping[str, IbkrNautilusInstrumentRoute]
    configuration: IbkrPaperStaticConfiguration

    @property
    def mandate_hash(self) -> str:
        return canonical_hash(self.mandate.to_dict())

    @property
    def instrument_routes_hash(self) -> str:
        return hash_ibkr_nautilus_instrument_routes(self.instrument_routes)

    @property
    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": IBKR_PAPER_PREPARATION_SCHEMA,
            "mandate_source_hash": self.mandate_source_hash,
            "trading_mandate_id": self.mandate.mandate_id,
            "trading_mandate_hash": self.mandate_hash,
            "harness_authority_id": self.mandate.harness_authority_id,
            "instrument_routes_hash": self.instrument_routes_hash,
            "instrument_ids": sorted(self.instrument_routes),
            "markets": sorted({route.market for route in self.instrument_routes.values()}),
            "static_configuration": self.configuration.to_dict(),
            "runtime_contract": _runtime_contract(),
            "per_order_checklist": _per_order_checklist(),
            "fault_matrix": _fault_matrix(),
            "pending_acceptance_scenarios": list(_REQUIRED_SCENARIOS),
        }

    def to_dict(self) -> dict[str, object]:
        core = self.core_dict
        return {
            "preparation_id": self.preparation_id,
            **core,
            "preparation_valid": True,
            "execution_accepted": False,
            "execution_status": "pending_real_ibkr_paper_acceptance",
            "network_calls": False,
            "broker_actions": False,
            "runtime_driver": {
                **cast(dict[str, object], core["runtime_contract"]),
                "constructed": False,
                "broker_connected": False,
                "accepted_for_external_execution": False,
                "verified_capabilities": [],
                "provider_enabled": False,
            },
            "not_provided": [
                "broker_credentials",
                "broker_account_identifier",
                "account_reference_key",
                "manual_order_binding",
                "broker_order_submission",
                "broker_order_cancellation",
            ],
        }


def prepare_ibkr_paper(
    *,
    mandate: TradingMandateV2,
    mandate_source_hash: str,
    instrument_routes: Mapping[str, str],
    configuration: IbkrPaperStaticConfiguration | None = None,
) -> IbkrPaperPreparation:
    """Validate static inputs and issue a plan without touching IBKR or Nautilus runtime APIs."""

    if not _is_sha256(mandate_source_hash):
        raise ValueError("mandate_source_hash must be a SHA-256 hash")
    if mandate.environment is not TradingEnvironment.PAPER:
        raise ValueError("IBKR Paper preparation requires a Paper Trading Mandate")
    if mandate.approval_mode is not ApprovalMode.MANUAL_EACH:
        raise ValueError("IBKR Paper preparation preserves exact per-order manual approval")
    _assert_current_paper_risk_controls(mandate)
    routes = _routes_from_mapping(instrument_routes, mandate=mandate)
    static_configuration = configuration or IbkrPaperStaticConfiguration()
    partial = IbkrPaperPreparation(
        preparation_id="",
        mandate_source_hash=mandate_source_hash,
        mandate=mandate,
        instrument_routes=routes,
        configuration=static_configuration,
    )
    return IbkrPaperPreparation(
        preparation_id="ibkr-paper-preparation-" + canonical_hash(partial.core_dict),
        mandate_source_hash=mandate_source_hash,
        mandate=mandate,
        instrument_routes=routes,
        configuration=static_configuration,
    )


def prepare_ibkr_paper_from_mandate_path(
    *,
    mandate_path: Path,
    instrument_routes: Mapping[str, str],
    configuration: IbkrPaperStaticConfiguration | None = None,
) -> IbkrPaperPreparation:
    """Bind a plan to exact mandate source bytes, while never serializing its account scope."""

    source = _read_mandate_source(mandate_path)
    mandate = trading_mandate_v2_from_dict(json.loads(source))
    return prepare_ibkr_paper(
        mandate=mandate,
        mandate_source_hash=hashlib.sha256(source).hexdigest(),
        instrument_routes=instrument_routes,
        configuration=configuration,
    )


def write_ibkr_paper_preparation(
    preparation: IbkrPaperPreparation,
    destination: Path,
) -> Path:
    """Write one private immutable artifact; a pre-existing destination always fails closed."""

    if destination.is_symlink():
        raise ValueError("preparation destination must not be a symbolic link")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = (json.dumps(preparation.to_dict(), indent=2, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    finally:
        if descriptor != -1:
            os.close(descriptor)
    return destination


def trading_mandate_v2_from_dict(payload: object) -> TradingMandateV2:
    """Parse exactly the versioned Paper-mandate shape used by the preparation command."""

    if not isinstance(payload, dict):
        raise TypeError("Trading Mandate source must be a JSON object")
    fields = cast(dict[str, object], payload)
    if frozenset(fields) != _MANDATE_FIELDS:
        raise ValueError("Trading Mandate v2 source fields are invalid")
    if fields["schema_version"] != "market-impact.trading-mandate.v2":
        raise ValueError("IBKR Paper preparation requires Trading Mandate v2")
    environment = _enum_value(TradingEnvironment, fields["environment"], "environment")
    if environment is not TradingEnvironment.PAPER:
        raise ValueError("IBKR Paper preparation requires a Paper Trading Mandate")
    mandate = TradingMandateV2(
        mandate_id=_required_string(fields, "mandate_id"),
        account_id=_required_string(fields, "account_id"),
        harness_authority_id=_required_string(fields, "harness_authority_id"),
        environment=environment,
        approval_mode=_enum_value(ApprovalMode, fields["approval_mode"], "approval_mode"),
        valid_from=_timestamp(fields["valid_from"], "valid_from"),
        valid_until=_timestamp(fields["valid_until"], "valid_until"),
        allowed_instruments=_string_set(fields["allowed_instruments"], "allowed_instruments"),
        allowed_instrument_classes=_string_set(
            fields["allowed_instrument_classes"], "allowed_instrument_classes"
        ),
        allowed_sides=frozenset(
            _enum_value(Side, value, "allowed_sides")
            for value in _string_list(fields["allowed_sides"], "allowed_sides")
        ),
        currency=_required_string(fields, "currency"),
        gross_exposure_limit=_decimal(fields["gross_exposure_limit"], "gross_exposure_limit"),
        minimum_net_exposure=_decimal(fields["minimum_net_exposure"], "minimum_net_exposure"),
        maximum_net_exposure=_decimal(fields["maximum_net_exposure"], "maximum_net_exposure"),
        maximum_position_count=_positive_int(
            fields["maximum_position_count"], "maximum_position_count"
        ),
        maximum_single_position_fraction=_decimal(
            fields["maximum_single_position_fraction"], "maximum_single_position_fraction"
        ),
        daily_turnover_limit=_decimal(fields["daily_turnover_limit"], "daily_turnover_limit"),
        daily_submission_limit=_positive_int(
            fields["daily_submission_limit"], "daily_submission_limit"
        ),
        daily_loss_kill_threshold=_decimal(
            fields["daily_loss_kill_threshold"], "daily_loss_kill_threshold"
        ),
        strategy_peak_drawdown_kill_threshold=_decimal(
            fields["strategy_peak_drawdown_kill_threshold"], "strategy_peak_drawdown_kill_threshold"
        ),
        kill_on_unknown_ack=_bool(fields["kill_on_unknown_ack"], "kill_on_unknown_ack"),
        kill_on_stale_account_snapshot=_bool(
            fields["kill_on_stale_account_snapshot"], "kill_on_stale_account_snapshot"
        ),
        kill_on_incomplete_order_coverage=_bool(
            fields["kill_on_incomplete_order_coverage"], "kill_on_incomplete_order_coverage"
        ),
        kill_on_reconciliation_difference=_bool(
            fields["kill_on_reconciliation_difference"], "kill_on_reconciliation_difference"
        ),
        kill_on_provider_loss=_bool(fields["kill_on_provider_loss"], "kill_on_provider_loss"),
    )
    if mandate.approval_mode is not ApprovalMode.MANUAL_EACH:
        raise ValueError("IBKR Paper preparation preserves exact per-order manual approval")
    return mandate


def _assert_current_paper_risk_controls(mandate: TradingMandateV2) -> None:
    if not all(
        (
            mandate.kill_on_unknown_ack,
            mandate.kill_on_stale_account_snapshot,
            mandate.kill_on_incomplete_order_coverage,
            mandate.kill_on_reconciliation_difference,
            mandate.kill_on_provider_loss,
        )
    ):
        raise ValueError("IBKR Paper preparation requires all versioned risk kill predicates")


def _routes_from_mapping(
    values: Mapping[str, str], *, mandate: TradingMandateV2
) -> dict[str, IbkrNautilusInstrumentRoute]:
    if not values:
        raise ValueError("IBKR Paper preparation requires at least one instrument route")
    if set(values) != set(mandate.allowed_instruments):
        raise ValueError("instrument routes must cover exactly the Trading Mandate instruments")
    routes = {
        instrument_id: IbkrNautilusInstrumentRoute(
            nautilus_instrument_id=instrument_id,
            market=market,
        )
        for instrument_id, market in values.items()
    }
    if any(route.market not in _PAPER_MARKETS for route in routes.values()):
        raise ValueError("IBKR Paper preparation supports only US or HK instrument routes")
    return dict(sorted(routes.items()))


def _per_order_checklist() -> list[dict[str, object]]:
    return [
        {
            "step": "reconcile_current_account_scope",
            "required_receipt": "account_state_snapshot_id",
            "requirement": (
                "complete, current account, position, open-order, and execution coverage"
            ),
            "failure_action": "block_submission_and_escalate",
        },
        {
            "step": "bind_current_exposure_and_price",
            "required_receipts": ["portfolio_exposure_view_id", "price_basis_hash"],
            "requirement": "same-root current risk view and raw executable-side price basis",
            "failure_action": "block_submission",
        },
        {
            "step": "bind_order_to_policy_and_mandate",
            "required_receipts": [
                "order_intent_hash",
                "policy_evaluation_hash",
                "trading_mandate_hash",
                "mandate_binding_hash",
            ],
            "requirement": "exact source-bound mandate and unchanged versioned risk controls",
            "failure_action": "block_submission",
        },
        {
            "step": "obtain_exact_human_approval",
            "required_receipt": "approval_hash",
            "requirement": "manual approval for this exact order intent remains required",
            "failure_action": "leave_operation_pending_approval",
        },
        {
            "step": "prove_provider_scope_before_mutation",
            "required_receipts": [
                "ibkr_nautilus_paper_provider_acceptance_id",
                "accepted_paper_provider_capability_id",
                "autonomous_paper_provider_lease_id",
            ],
            "requirement": (
                "current sealed acceptance binds configuration, anonymous account scope, "
                "routes, market order, and DAY"
            ),
            "failure_action": "block_submission",
        },
        {
            "step": "reconcile_after_any_transport_or_broker_response",
            "required_receipt": "complete_reconciliation_hash",
            "requirement": "transport success alone is never an accepted order or fill",
            "failure_action": "hold_unknown_state_and_kill_new_submissions",
        },
    ]


def _runtime_contract() -> dict[str, object]:
    try:
        installed_nautilus = version("nautilus-trader")
        nautilus_ibapi_version = version("nautilus_ibapi")
    except PackageNotFoundError as exc:
        raise RuntimeError(
            "IBKR Paper preparation requires the pinned Nautilus dependencies"
        ) from exc
    if installed_nautilus != IBKR_NAUTILUS_VERSION:
        raise RuntimeError(
            "IBKR Paper preparation requires NautilusTrader "
            f"{IBKR_NAUTILUS_VERSION}, found {installed_nautilus}"
        )
    if not nautilus_ibapi_version or nautilus_ibapi_version != nautilus_ibapi_version.strip():
        raise RuntimeError(
            "IBKR Paper preparation requires a valid Nautilus IB API package version"
        )
    return {
        "implementation": "market_impact_agent.ibkr_nautilus_runtime.IbkrNautilusPaperRuntime",
        "runtime_version": IBKR_NAUTILUS_PAPER_RUNTIME_VERSION,
        "nautilus_version": IBKR_NAUTILUS_VERSION,
        "nautilus_ibapi_version": nautilus_ibapi_version,
        "provider_id": IBKR_NAUTILUS_PAPER_PROVIDER_ID,
        "provider_version": IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
        "declared_capabilities": ["paper_execution"],
    }


def _fault_matrix() -> list[dict[str, str]]:
    return [
        {
            "fault": "incomplete_or_stale_account_coverage",
            "required_evidence": "fresh complete account state snapshot",
            "response": "block every new submission and preserve reconciliation",
        },
        {
            "fault": "manual_tws_order_not_covered",
            "required_evidence": "client-ID-0 external order observation and reconciliation",
            "response": "block mutation until exact external-order coverage is proven",
        },
        {
            "fault": "client_id_0_scope_collision",
            "required_evidence": "exclusive API client scope observation",
            "response": "do not start the command runtime or submit",
        },
        {
            "fault": "submit_or_acknowledgement_ambiguous",
            "required_evidence": "sealed ambiguous-acknowledgement scenario and reconciliation",
            "response": "mark unknown, kill new submissions, reconcile",
        },
        {
            "fault": "disconnect_or_gateway_restart",
            "required_evidence": "sealed disconnect and gateway-restart scenarios",
            "response": "invalidate session scope and require fresh reconciliation",
        },
        {
            "fault": "partial_or_duplicate_fill",
            "required_evidence": "sealed partial-fill and duplicate-fill scenarios",
            "response": "preserve cumulative monotonic fill state and reconcile",
        },
        {
            "fault": "process_restart",
            "required_evidence": "sealed process-restart scenario and durable outbox recovery",
            "response": "reopen exact durable operation without redispatch",
        },
        {
            "fault": "cancel_or_replace_failure",
            "required_evidence": "sealed cancel and replace scenarios plus exact order identity",
            "response": "retain the original order state until reconciliation proves cancellation",
        },
    ]


def _read_mandate_source(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Trading Mandate source must be a regular file")
    source = path.read_bytes()
    if not source or len(source) > 1024 * 1024:
        raise ValueError("Trading Mandate source must contain between 1 byte and 1 MiB")
    return source


def _required_string(fields: Mapping[str, object], name: str) -> str:
    value = fields[name]
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"Trading Mandate {name} must be non-empty trimmed text")
    return value


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Trading Mandate {name} must be non-empty trimmed strings")
    items = cast(list[object], value)
    if not items or any(
        not isinstance(item, str) or not item or item != item.strip() for item in items
    ):
        raise ValueError(f"Trading Mandate {name} must be non-empty trimmed strings")
    strings = cast(list[str], items)
    if len(strings) != len(set(strings)):
        raise ValueError(f"Trading Mandate {name} must be unique")
    return strings


def _string_set(value: object, name: str) -> frozenset[str]:
    return frozenset(_string_list(value, name))


def _enum_value[T: Side | ApprovalMode | TradingEnvironment](
    enum_type: type[T], value: object, name: str
) -> T:
    if not isinstance(value, str):
        raise TypeError(f"Trading Mandate {name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"Trading Mandate {name} is invalid") from exc


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"Trading Mandate {name} must be an ISO 8601 string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Trading Mandate {name} is not an ISO 8601 timestamp") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"Trading Mandate {name} must include a timezone")
    return result.astimezone(UTC)


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise TypeError(f"Trading Mandate {name} must be a decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"Trading Mandate {name} must be decimal") from exc
    if not result.is_finite():
        raise ValueError(f"Trading Mandate {name} must be finite")
    return result


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"Trading Mandate {name} must be a positive integer")
    return value


def _bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"Trading Mandate {name} must be a boolean")
    return value


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)
