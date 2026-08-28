from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from market_impact_agent.cli import accept_tushare_observation_source, main
from market_impact_agent.tushare_observation import load_tushare_observation_source

TOKEN = "private-test-token"
RETRIEVED = datetime(2026, 8, 28, 12, 30, tzinfo=UTC)
CONFIG_PATH = Path("examples/providers/tushare-observation-index-daily-v1.json")


class FakeTransport:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.calls = 0

    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
        assert endpoint == "https://api.tushare.pro"
        assert TOKEN.encode() in body
        assert timeout_seconds > 0
        self.calls += 1
        return self.response


def test_accept_tushare_route_persists_actual_receipt_and_replays_from_storage(
    tmp_path: Path,
) -> None:
    config = load_tushare_observation_source(CONFIG_PATH)
    response = json.dumps(
        {
            "code": 0,
            "msg": None,
            "data": {
                "fields": list(config.fields),
                "items": [
                    [
                        "000300.SH",
                        "20260828",
                        4000.0,
                        4050.0,
                        3990.0,
                        4030.0,
                        3980.0,
                        50.0,
                        1.2563,
                        1000000.0,
                        2000000.0,
                    ]
                ],
            },
        },
        separators=(",", ":"),
    ).encode()
    transport = FakeTransport(response)

    result = accept_tushare_observation_source(
        token=TOKEN,
        source_config_path=CONFIG_PATH,
        parameters={
            "ts_code": "000300.SH",
            "start_date": "20260828",
            "end_date": "20260828",
        },
        window_start=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
        poll_interval_seconds=86400,
        maximum_gap_seconds=172800,
        state_root=tmp_path / "state",
        provider_timeout_seconds=5.0,
        transport=transport,
        rights_fetcher=lambda url: (url, b"Tushare owner private-use terms"),
        clock=lambda: RETRIEVED,
    )

    assert result["accepted"] is True
    assert result["coverage_complete"] is True
    assert result["observation_count"] == 1
    assert result["pit_lane"] == "prospective"
    assert result["historical_pit_claim"] is False
    assert result["execution_capability"] is False
    assert cast(str, result["source_route_acceptance_report_id"]).startswith(
        "source-route-acceptance-report-"
    )
    assert cast(str, result["collection_policy_id"]).startswith("prospective-collection-policy-")
    assert transport.calls == 1
    for path in (tmp_path / "state").rglob("*"):
        if path.is_file():
            assert TOKEN.encode() not in path.read_bytes()


def test_cli_requires_environment_token_before_tushare_acceptance(
    monkeypatch: object,
    capsys: object,
) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)  # type: ignore[attr-defined]

    result = main(
        [
            "data",
            "accept-tushare-observation",
            "--source-config",
            CONFIG_PATH.as_posix(),
            "--parameters-json",
            '{"end_date":"20260828","start_date":"20260828","ts_code":"000300.SH"}',
            "--window-start",
            "2026-08-28T12:00:00Z",
            "--poll-interval-seconds",
            "86400",
            "--maximum-gap-seconds",
            "172800",
        ]
    )

    assert result == 1
    assert json.loads(capsys.readouterr().err) == {  # type: ignore[attr-defined]
        "accepted": False,
        "error": "TUSHARE_TOKEN is not configured",
    }
