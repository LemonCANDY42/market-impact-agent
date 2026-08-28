from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from market_impact_agent.prospective_diagnostic import (
    REQUIRED_DIAGNOSTIC_CAPABILITIES,
    load_prospective_diagnostic_registration,
    prospective_diagnostic_registration_from_dict,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/research/prospective-diagnostic-registration-v1.json"


def _payload() -> dict[str, object]:
    return json.loads(EXAMPLE.read_text())


def test_example_registration_round_trips_and_freezes_three_distinct_mechanisms() -> None:
    registration = load_prospective_diagnostic_registration(EXAMPLE)

    assert registration.to_dict() == _payload()
    assert len(registration.checkpoints) == 3
    assert len({item.mechanism for item in registration.checkpoints}) == 3
    assert registration.replicates_per_arm == 3
    assert registration.aggregate_model_cost_limit_usd == "20.00"
    assert registration.outcome_opening_rule == (
        "do_not_open_until_all_paired_judgments_are_sealed"
    )
    for checkpoint in registration.checkpoints:
        assert checkpoint.selection_rule == "first_eligible_after_registration"
        assert checkpoint.cutoff.session_boundary == "after_market_close"
        assert {item.capability for item in checkpoint.capability_slots} == (
            REQUIRED_DIAGNOSTIC_CAPABILITIES
        )


def test_registration_rejects_missing_capability_slot() -> None:
    payload = _payload()
    checkpoint = payload["checkpoints"][0]  # type: ignore[index]
    checkpoint["capability_slots"].pop()  # type: ignore[index,union-attr]

    with pytest.raises(ValueError, match="exact diagnostic capability set"):
        prospective_diagnostic_registration_from_dict(payload)


def test_registration_rejects_post_registration_provider_bindings() -> None:
    payload = _payload()
    slot = payload["checkpoints"][0]["capability_slots"][0]  # type: ignore[index]
    slot["provider_id"] = "tushare-pro"  # type: ignore[index]

    with pytest.raises(ValueError, match="capability slot fields"):
        prospective_diagnostic_registration_from_dict(payload)


def test_registration_requires_not_applicable_reason_and_zero_collection_minima() -> None:
    payload = _payload()
    slot = payload["checkpoints"][0]["capability_slots"][0]  # type: ignore[index]
    slot.update(  # type: ignore[union-attr]
        {
            "applicability": "not_applicable",
            "not_applicable_reason": None,
            "required_route_kinds": [],
            "minimum_data_sources": 0,
            "minimum_observations": 0,
            "poll_interval_seconds": 0,
            "maximum_gap_seconds": 0,
            "maximum_age_seconds": 0,
        }
    )

    with pytest.raises(ValueError, match="not_applicable reason"):
        prospective_diagnostic_registration_from_dict(payload)


def test_registration_rejects_noncanonical_identity_or_nonpaired_design() -> None:
    identity_payload = _payload()
    identity_payload["registration_id"] = "prospective-diagnostic-registration-" + "0" * 64
    with pytest.raises(ValueError, match="registration_id does not match content"):
        prospective_diagnostic_registration_from_dict(identity_payload)

    design_payload = deepcopy(_payload())
    design_payload["replicates_per_arm"] = 2
    with pytest.raises(ValueError, match="exactly three replicates"):
        prospective_diagnostic_registration_from_dict(design_payload)


def test_registration_rejects_duplicate_mechanisms_or_outcome_leakage() -> None:
    mechanisms = _payload()
    mechanisms["checkpoints"][1]["mechanism"] = mechanisms["checkpoints"][0][  # type: ignore[index]
        "mechanism"
    ]
    with pytest.raises(ValueError, match="different mechanisms"):
        prospective_diagnostic_registration_from_dict(mechanisms)

    leakage = _payload()
    leakage["outcome_opening_rule"] = "open_after_each_checkpoint"
    with pytest.raises(ValueError, match="outcome opening rule"):
        prospective_diagnostic_registration_from_dict(leakage)
