from __future__ import annotations

import hashlib
import json
import socket
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.cli import main
from market_impact_agent.domain import (
    ApprovalMode,
    Side,
    TradingEnvironment,
    TradingMandateV2,
)
from market_impact_agent.ibkr_paper_preparation import (
    IbkrPaperStaticConfiguration,
    prepare_ibkr_paper,
    prepare_ibkr_paper_from_mandate_path,
    write_ibkr_paper_preparation,
)

NOW = datetime(2026, 9, 5, 8, tzinfo=UTC)


def _mandate(*, environment: TradingEnvironment = TradingEnvironment.PAPER) -> TradingMandateV2:
    return TradingMandateV2(
        mandate_id="ibkr-paper-2026-09-05",
        account_id="account-ref-" + "a" * 64,
        harness_authority_id="harness-authority-" + "b" * 64,
        environment=environment,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=NOW,
        valid_until=NOW + timedelta(hours=8),
        allowed_instruments=frozenset({"SPY.ARCA"}),
        allowed_instrument_classes=frozenset({"unlevered_exchange_traded_fund"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        currency="USD",
        gross_exposure_limit=Decimal("10000"),
        minimum_net_exposure=Decimal("-10000"),
        maximum_net_exposure=Decimal("10000"),
        maximum_position_count=10,
        maximum_single_position_fraction=Decimal("1"),
        daily_turnover_limit=Decimal("50000"),
        daily_submission_limit=50,
        daily_loss_kill_threshold=Decimal("300"),
        strategy_peak_drawdown_kill_threshold=Decimal("1000"),
    )


def _write_mandate(path: Path, mandate: TradingMandateV2) -> bytes:
    source = (json.dumps(mandate.to_dict(), sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(source)
    return source


def test_preparation_is_offline_source_bound_and_pending_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mandate_path = tmp_path / "mandate.json"
    source = _write_mandate(mandate_path, _mandate())

    def fail_network(*_: object, **__: object) -> object:
        raise AssertionError("offline preparation must not open a network connection")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.delitem(sys.modules, "market_impact_agent.ibkr_nautilus_runtime", raising=False)

    preparation = prepare_ibkr_paper_from_mandate_path(
        mandate_path=mandate_path,
        instrument_routes={"SPY.ARCA": "US"},
    )
    payload = preparation.to_dict()

    assert payload["preparation_valid"] is True
    assert payload["execution_accepted"] is False
    assert payload["execution_status"] == "pending_real_ibkr_paper_acceptance"
    assert payload["network_calls"] is False
    assert payload["broker_actions"] is False
    assert payload["mandate_source_hash"] == hashlib.sha256(source).hexdigest()
    assert payload["static_configuration"] == {
        "gateway_host": "127.0.0.1",
        "gateway_port": 4002,
        "client_id": 0,
        "fetch_all_open_orders": True,
        "time_in_force": "DAY",
    }
    assert payload["runtime_driver"] == {
        "implementation": "market_impact_agent.ibkr_nautilus_runtime.IbkrNautilusPaperRuntime",
        "runtime_version": "0.2.0-candidate",
        "nautilus_version": "1.231.0",
        "nautilus_ibapi_version": version("nautilus_ibapi"),
        "provider_id": "ibkr-nautilus-paper",
        "provider_version": "0.2.0-candidate",
        "declared_capabilities": ["paper_execution"],
        "constructed": False,
        "broker_connected": False,
        "accepted_for_external_execution": False,
        "verified_capabilities": [],
        "provider_enabled": False,
    }
    assert "market_impact_agent.ibkr_nautilus_runtime" not in sys.modules
    checklist = cast(list[object], payload["per_order_checklist"])
    fault_matrix = cast(list[object], payload["fault_matrix"])
    not_provided = cast(list[str], payload["not_provided"])
    assert len(checklist) == 6
    assert len(fault_matrix) == 8
    assert "account-ref-" not in json.dumps(payload)
    assert "broker_credentials" in not_provided


def test_preparation_rejects_live_mandate_source(tmp_path: Path) -> None:
    mandate_path = tmp_path / "mandate.json"
    payload = _mandate().to_dict()
    payload["environment"] = "live"
    mandate_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="requires a Paper Trading Mandate"):
        prepare_ibkr_paper_from_mandate_path(
            mandate_path=mandate_path,
            instrument_routes={"SPY.ARCA": "US"},
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"host": "192.0.2.10"}, "loopback Gateway"),
        ({"port": 7497}, "port 4002"),
        ({"client_id": 1}, "client ID 0"),
    ],
)
def test_static_configuration_rejects_non_paper_gateway_scope(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        IbkrPaperStaticConfiguration(**kwargs)  # type: ignore[arg-type]


def test_preparation_preserves_exact_manual_approval_and_route_scope() -> None:
    mandate = _mandate()

    with pytest.raises(ValueError, match="exactly the Trading Mandate instruments"):
        prepare_ibkr_paper(
            mandate=mandate,
            mandate_source_hash="c" * 64,
            instrument_routes={"QQQ.NASDAQ": "US"},
        )

    with pytest.raises(ValueError, match="exact per-order manual approval"):
        prepare_ibkr_paper(
            mandate=replace(mandate, approval_mode=ApprovalMode.AUTONOMOUS),
            mandate_source_hash="c" * 64,
            instrument_routes={"SPY.ARCA": "US"},
        )


def test_preparation_artifact_is_immutable(tmp_path: Path) -> None:
    preparation = prepare_ibkr_paper(
        mandate=_mandate(),
        mandate_source_hash="c" * 64,
        instrument_routes={"SPY.ARCA": "US"},
    )
    destination = tmp_path / "ibkr-paper-preparation.json"

    assert write_ibkr_paper_preparation(preparation, destination) == destination
    assert json.loads(destination.read_text())["preparation_id"] == preparation.preparation_id
    with pytest.raises(FileExistsError):
        write_ibkr_paper_preparation(preparation, destination)


def test_cli_emits_an_offline_immutable_preparation_artifact(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    mandate_path = tmp_path / "mandate.json"
    _write_mandate(mandate_path, _mandate())
    artifact_path = tmp_path / "preparation.json"

    assert (
        main(
            [
                "ibkr-paper",
                "prepare",
                "--mandate",
                str(mandate_path),
                "--instrument-route",
                "SPY.ARCA=US",
                "--output",
                str(artifact_path),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["execution_accepted"] is False
    assert payload["artifact_path"] == artifact_path.as_posix()
    assert json.loads(artifact_path.read_text())["preparation_id"] == payload["preparation_id"]
