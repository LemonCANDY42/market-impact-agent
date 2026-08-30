from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pytest

from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
    REQUIRED_DIAGNOSTIC_CAPABILITIES,
    CapabilityApplicability,
    DiagnosticCapabilitySlot,
    DiagnosticCutoffRule,
    DiagnosticMechanism,
    ProspectiveDiagnosticCheckpoint,
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
    prospective_diagnostic_registration_from_dict,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/research/prospective-diagnostic-registration-v1.json"
V3_EXAMPLE = ROOT / "examples/research/prospective-diagnostic-registration-v3.json"


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


def _v2_checkpoint(key: str, mechanism: DiagnosticMechanism) -> ProspectiveDiagnosticCheckpoint:
    return ProspectiveDiagnosticCheckpoint(
        checkpoint_key=key,
        name=key,
        mechanism=mechanism,
        selection_rule="first_eligible_after_registration",
        eligibility_rule="First actual-receipt event after registration.",
        eligibility_source_classes=("observed_source",),
        exclusion_rules=("Exclude observations received after the barrier.",),
        cutoff=DiagnosticCutoffRule(
            timezone="Asia/Shanghai",
            session_boundary="after_market_close",
            market_close_local="15:00:00",
            decision_delay_seconds=1800,
        ),
        capability_slots=tuple(
            DiagnosticCapabilitySlot(
                capability=capability,
                applicability=(
                    CapabilityApplicability.REQUIRED
                    if capability.value == "event_revelation"
                    else CapabilityApplicability.OPTIONAL
                ),
                not_applicable_reason=None,
                required_route_kinds=(f"{capability.value}_observation",),
                minimum_data_sources=1,
                minimum_observations=1,
                poll_interval_seconds=60,
                maximum_gap_seconds=3600,
                maximum_age_seconds=86400,
            )
            for capability in sorted(
                REQUIRED_DIAGNOSTIC_CAPABILITIES,
                key=lambda item: item.value,
            )
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
        candidate_horizon_sessions=(1, 5, 20),
    )


def test_v2_registration_allows_optional_information_but_requires_event_trigger() -> None:
    registration = ProspectiveDiagnosticRegistration.build(
        registered_at=datetime.fromisoformat("2026-08-29T00:00:00+00:00"),
        checkpoints=(
            _v2_checkpoint("policy-event-v2", DiagnosticMechanism.POLICY_REGULATION),
            _v2_checkpoint("macro-event-v2", DiagnosticMechanism.MACRO_CYCLE),
        ),
        paired_arms=("structured_agent_core", "structured_agent_plus_routed_methods"),
        replicates_per_arm=3,
        model_profile_id="cliproxyapi-luna-xhigh-v1",
        aggregate_model_cost_limit_usd="20.00",
        outcome_opening_rule="do_not_open_until_all_paired_judgments_are_sealed",
        stop_conditions=("structural_query_gate_failed",),
        go_conditions=("actual_receipt_event_trigger_available",),
        claim_scope="process_diagnostic_only_no_alpha_or_execution_claim",
        schema_version=PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    )

    assert registration.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2
    assert all(
        checkpoint.slot(next(iter(REQUIRED_DIAGNOSTIC_CAPABILITIES))).applicability
        in {CapabilityApplicability.REQUIRED, CapabilityApplicability.OPTIONAL}
        for checkpoint in registration.checkpoints
    )

    missing_trigger = deepcopy(registration.to_dict())
    for checkpoint in missing_trigger["checkpoints"]:  # type: ignore[index]
        for slot in checkpoint["capability_slots"]:  # type: ignore[index]
            if slot["capability"] == "event_revelation":
                slot["applicability"] = "optional"
    core = {key: value for key, value in missing_trigger.items() if key != "registration_id"}
    from market_impact_agent.agent_contracts import canonical_hash

    missing_trigger["registration_id"] = (
        f"prospective-diagnostic-registration-{canonical_hash(core)}"
    )
    with pytest.raises(ValueError, match="event_revelation must be required"):
        prospective_diagnostic_registration_from_dict(missing_trigger)


def test_v1_registration_rejects_optional_capability_semantics() -> None:
    payload = _payload()
    payload["schema_version"] = PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V1
    payload["checkpoints"][0]["capability_slots"][1]["applicability"] = "optional"  # type: ignore[index]
    core = {key: value for key, value in payload.items() if key != "registration_id"}
    from market_impact_agent.agent_contracts import canonical_hash

    payload["registration_id"] = f"prospective-diagnostic-registration-{canonical_hash(core)}"
    with pytest.raises(ValueError, match="v1 does not support optional"):
        prospective_diagnostic_registration_from_dict(payload)


def test_v3_example_freezes_adaptive_pairs_and_partial_information_semantics() -> None:
    registration = load_prospective_diagnostic_registration(V3_EXAMPLE)

    assert registration.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3
    assert registration.minimum_replicates_per_arm == 2
    assert registration.replicates_per_arm == 3
    assert registration.replicate_schedule_rule == (
        "run_two_paired_replicates_then_third_pair_if_either_arm_disagrees"
    )
    for checkpoint in registration.checkpoints:
        assert (
            checkpoint.slot(
                next(
                    capability
                    for capability in REQUIRED_DIAGNOSTIC_CAPABILITIES
                    if capability.value == "event_revelation"
                )
            ).applicability
            is CapabilityApplicability.REQUIRED
        )
        assert all(
            slot.applicability is CapabilityApplicability.OPTIONAL
            for slot in checkpoint.capability_slots
            if slot.capability.value != "event_revelation"
        )
