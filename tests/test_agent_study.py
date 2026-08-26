import json
from copy import deepcopy
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_study import (
    agent_phase2_preregistration_from_dict,
    exposure_registry_from_dict,
    load_agent_phase2_preregistration,
)

REGISTRATION_PATH = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY_PATH = Path("examples/research/a-share-energy-exposure-registry-v1.json")


def _payload(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _content_id(payload: dict[str, object], field: str, prefix: str) -> str:
    core = deepcopy(payload)
    core.pop(field)
    return f"{prefix}{canonical_hash(core)}"


def test_canonical_agent_study_is_content_bound_and_prospective() -> None:
    registration_payload = _payload(REGISTRATION_PATH)
    registry_payload = _payload(REGISTRY_PATH)

    assert (
        validate_agent_contract(
            registration_payload,
            "agent-phase2-preregistration.schema.json",
        )
        == ()
    )
    assert validate_agent_contract(registry_payload, "exposure-registry.schema.json") == ()

    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )

    assert registration.registration_id == registration.expected_registration_id
    assert registry.registry_id == registry.expected_registry_id
    assert registration.registered_at < registration.accrual.opens_after
    assert registration.holdout_outcomes_opened is False
    assert registration.execution_capability == "none"
    assert registration.evaluation.all_event_denominator is True
    assert registration.event_eligibility.missing_critical_data_action == "retain_and_abstain"
    assert sum(item.selection_eligible for item in registry.entries) == 2


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        (
            "event_eligibility",
            "missing_critical_data_action",
            "drop_event",
            "missing critical data",
        ),
        (
            "agent_protocol",
            "cross_replicate_memory",
            True,
            "must not share memory",
        ),
        (
            "evaluation",
            "all_event_denominator",
            False,
            "retain every Accrued Event",
        ),
    ],
)
def test_study_rejects_protocol_mutations_that_could_improve_reported_results(
    section: str,
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _payload(REGISTRATION_PATH)
    nested = payload[section]
    assert isinstance(nested, dict)
    cast(dict[str, object], nested)[field] = value
    payload["registration_id"] = _content_id(payload, "registration_id", "agent-study-")

    with pytest.raises(ValueError, match=message):
        agent_phase2_preregistration_from_dict(payload)

    assert validate_agent_contract(payload, "agent-phase2-preregistration.schema.json")


def test_study_rejects_opened_holdout_and_content_id_drift() -> None:
    opened = _payload(REGISTRATION_PATH)
    opened["holdout_outcomes_opened"] = True
    opened["registration_id"] = _content_id(opened, "registration_id", "agent-study-")

    with pytest.raises(ValueError, match="opened holdout"):
        agent_phase2_preregistration_from_dict(opened)
    assert validate_agent_contract(opened, "agent-phase2-preregistration.schema.json")

    drifted = _payload(REGISTRY_PATH)
    entries = drifted["entries"]
    assert isinstance(entries, list)
    first = cast(list[object], entries)[0]
    assert isinstance(first, dict)
    cast(dict[str, object], first)["provider_code"] = "tampered"

    with pytest.raises(ValueError, match="registry_id does not match content"):
        exposure_registry_from_dict(drifted)


def test_study_rejects_registry_frozen_after_registration() -> None:
    registry_payload = _payload(REGISTRY_PATH)
    registry_payload["as_of"] = "2026-08-26T08:00:00Z"
    registry_payload["registry_id"] = _content_id(
        registry_payload,
        "registry_id",
        "exposure-registry-",
    )
    registry = exposure_registry_from_dict(registry_payload)

    registration_payload = _payload(REGISTRATION_PATH)
    registration_payload["exposure_registry_id"] = registry.registry_id
    registration_payload["exposure_registry_hash"] = registry.registry_hash
    registration_payload["registration_id"] = _content_id(
        registration_payload,
        "registration_id",
        "agent-study-",
    )
    registration = agent_phase2_preregistration_from_dict(registration_payload)

    with pytest.raises(ValueError, match="no later than registration"):
        registration.validate_against(registry)
