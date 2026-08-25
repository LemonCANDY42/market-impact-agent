import json
from pathlib import Path

import pytest

from market_impact_agent.cli import main, status_payload


def test_status_is_fail_closed() -> None:
    payload = status_payload()
    assert payload["live_trading"] == "disabled"
    providers = payload["providers"]
    assert isinstance(providers, list)
    assert providers[0]["provider_id"] == "mock-execution"


def test_validate_provider_command(tmp_path: Path) -> None:
    source = Path("examples/providers/veighna-external-bridge.json")
    target = tmp_path / "provider.json"
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    assert main(["provider", "validate", str(target)]) == 0


def test_validate_provider_reports_parse_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "provider.json"
    target.write_text("not-json", encoding="utf-8")

    assert main(["provider", "validate", str(target)]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.err)["valid"] is False


def test_validate_provider_rejects_malicious_live_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "malicious-live.json"
    target.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.provider-manifest.v1",
                "provider_id": "malicious-live",
                "provider_version": "1.0.0",
                "transport": "http",
                "environments": ["live"],
                "declared_capabilities": ["live_execution"],
                "verified_capabilities": ["live_execution"],
                "markets": ["US"],
                "order_types": ["market"],
                "supports_streaming": "false",
                "supports_reconciliation": "false",
                "enabled": "true",
                "trust_tier": "live_validated",
            }
        ),
        encoding="utf-8",
    )

    assert main(["provider", "validate", str(target)]) == 1
    captured = capsys.readouterr()
    assert "must be a boolean" in json.loads(captured.err)["error"]


def test_validate_event_rejects_future_evidence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Path("examples/events/synthetic-energy-supply-shock.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["first_publication_time"] = "2026-08-24T02:06:00Z"
    target = tmp_path / "future-evidence.json"
    target.write_text(json.dumps(payload), encoding="utf-8")

    assert main(["event", "validate", str(target)]) == 1
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["errors"] == ["first_publication_time must not be after as_of"]
