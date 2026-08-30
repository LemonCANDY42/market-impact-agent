from __future__ import annotations

import json

import pytest

from market_impact_agent.cli import main


@pytest.mark.parametrize("version", ["v1", "v2", "v3"])
def test_cli_validates_frozen_prospective_diagnostic_registration(
    capsys: pytest.CaptureFixture[str],
    version: str,
) -> None:
    assert (
        main(
            [
                "data",
                "validate-prospective-diagnostic",
                "--registration",
                f"examples/research/prospective-diagnostic-registration-{version}.json",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)

    assert output["valid"] is True
    assert output["registration_id"].startswith("prospective-diagnostic-registration-")
    assert output["checkpoint_keys"] == [
        "next-a-share-policy-event",
        "next-a-share-earnings-surprise",
        "next-nbs-cpi-ppi-release",
    ]
    assert output["model_calls_authorized"] is False
    assert output["execution_capability"] is False
